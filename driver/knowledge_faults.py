"""The knowledge scenarios: incidents in small worlds, each decided by one thing about the environment that is idiosyncratic
and thus tricky to uncover.

A problem deploys its services (knowledge_services/, one module each) into namespaces of its own, seeds their state,
and plays the scenario out:

    support_handoff_failed   a ticket offer published before the agent's desk reconnected, so it was lost
    meeting_runner_dlq       a user's dead Google grant, seen only in mailbox-sync, which logs users by email
    payments_core_5xx        one admin's sweep through admin-api, which logs admins by email
    label_backlog            a cut-off carrier service-area table parks orders; how far from normal, only a baseline says
    orders_drop_edge         the CDN's WAF blocks the Android app; only the CDN's own log stream in Loki shows it
    checkout_fx_quota        a flag makes search drain the shop's shared FX quota; only the flag provider's audit stream shows who turned it on
    couriers_offline         ShiftBoard's rota export stopped, so newly booked shifts never open; only a baseline says it should change

`inject_steady` deploys the same services and plays only their steady traffic, with none of the incident's timeline,
so the agent can learn the world as it runs every day, before an incident.

The oracle is stateless: it reads the services' own counters through the API server's service proxy.
"""

import contextlib
import hashlib
import http.client
import http.server
import json
import os
import random
import re
import socket
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar

# The services' source, one module per service beside the code they share (common.py). Each service ships only
# its own module and common.py, in a ConfigMap of its own, so reading one service's code shows no other's.
SERVICES_DIR = Path(__file__).with_name("knowledge_services")
SHARED_MODULE = "common.py"
# A mirror of Docker Hub's official images, which rate-limits anonymous pulls less.
IMAGE = os.environ.get("SCENARIO_IMAGE", "mirror.gcr.io/library/python:3.12-slim")
PORT = 8080
ROLLOUT_TIMEOUT_S = 300
# Where the problems push the logs of systems outside the cluster (the CDN, the flag provider): a Loki service,
# `namespace/service:port`, reached through a port-forward.
LOKI_SERVICE = os.environ.get("SCENARIO_LOKI", "observe/loki:3100")


# ---------------------------------------------------------------------------------------------------
# The cluster: kubectl on the current context
# ---------------------------------------------------------------------------------------------------


def kubectl(*args: str, stdin: str | None = None, check: bool = True) -> str:
    result = subprocess.run(["kubectl", *args], input=stdin, capture_output=True, text=True)
    if check and result.returncode != 0:
        raise RuntimeError(f"kubectl {' '.join(args[:4])} failed: {result.stderr[-1500:]}")
    return result.stdout


@dataclass(frozen=True)
class Svc:
    name: str
    namespace: str
    env: dict[str, str]
    # Hostnames outside the cluster the service reaches, and the address each resolves to here: (ip, hostname).
    host_aliases: tuple[tuple[str, str], ...] = ()
    replicas: int = 1
    # A CPU limit, for a service whose work is CPU-bound; others run without one.
    cpu_limit: str = ""

    @property
    def module(self) -> str:
        return f"{self.name.replace('-', '_')}.py"

    @property
    def source_configmap(self) -> str:
        return f"{self.name}-src"


def _manifests(namespaces: tuple[str, ...], services: tuple[Svc, ...]) -> list[dict]:
    items: list[dict] = [{"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": ns}} for ns in namespaces]
    for svc in services:
        labels = {"app": svc.name, "app.kubernetes.io/name": svc.name}
        items.append(
            {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": svc.source_configmap, "namespace": svc.namespace, "labels": labels},
                "data": {module: (SERVICES_DIR / module).read_text() for module in (SHARED_MODULE, svc.module)},
            }
        )
        env = [{"name": k, "value": v} for k, v in {"PORT": str(PORT), **svc.env}.items()]
        container = {
            "name": svc.name,
            "image": IMAGE,
            "imagePullPolicy": "IfNotPresent",
            "command": ["python", "-u", f"/srv/{svc.module}"],
            "env": env,
            "ports": [{"containerPort": PORT, "name": "http"}],
            "readinessProbe": {"httpGet": {"path": "/healthz", "port": PORT}, "periodSeconds": 2},
            "resources": {
                "requests": {"cpu": "20m", "memory": "48Mi"},
                "limits": {"memory": "160Mi", **({"cpu": svc.cpu_limit} if svc.cpu_limit else {})},
            },
            "volumeMounts": [{"name": "src", "mountPath": "/srv"}],
        }
        items.append(
            {
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {"name": svc.name, "namespace": svc.namespace, "labels": labels},
                "spec": {
                    "replicas": svc.replicas,
                    "selector": {"matchLabels": {"app": svc.name}},
                    "template": {
                        "metadata": {"labels": labels},
                        "spec": {
                            "containers": [container],
                            "volumes": [{"name": "src", "configMap": {"name": svc.source_configmap}}],
                            **(
                                {"hostAliases": [{"ip": ip, "hostnames": [host]} for ip, host in svc.host_aliases]}
                                if svc.host_aliases
                                else {}
                            ),
                        },
                    },
                },
            }
        )
        items.append(
            {
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {"name": svc.name, "namespace": svc.namespace, "labels": labels},
                "spec": {"selector": {"app": svc.name}, "ports": [{"name": "http", "port": PORT, "targetPort": PORT}]},
            }
        )
    return items


def url(svc: str, namespace: str) -> str:
    return f"http://{svc}.{namespace}.svc.cluster.local:{PORT}"


class KubeCluster:
    def __init__(self) -> None:
        self._forwards: list[subprocess.Popen] = []

    def deploy(self, namespaces: tuple[str, ...], services: tuple[Svc, ...]) -> None:
        # Where the cluster runs Calico, its CNI kubeconfig's token can be long expired on a cluster restored from a
        # snapshot: no new pod gets a network ("connection is unauthorized") until calico-node writes a fresh one,
        # which it does when it restarts. A cluster without Calico has nothing to restart.
        if (
            subprocess.run(
                ["kubectl", "-n", "kube-system", "get", "daemonset/calico-node"], capture_output=True
            ).returncode
            == 0
        ):
            kubectl("-n", "kube-system", "rollout", "restart", "daemonset/calico-node")
            kubectl(
                "-n", "kube-system", "rollout", "status", "daemonset/calico-node", f"--timeout={ROLLOUT_TIMEOUT_S}s"
            )
        kubectl(
            "apply",
            "-f",
            "-",
            stdin=json.dumps({"apiVersion": "v1", "kind": "List", "items": _manifests(namespaces, services)}),
        )
        for svc in services:
            kubectl("-n", svc.namespace, "rollout", "status", f"deploy/{svc.name}", f"--timeout={ROLLOUT_TIMEOUT_S}s")

    # A local port forwarded to the service, for this process's traffic.
    def forward(self, svc: str, namespace: str, port: int = PORT, ready: str = "/healthz") -> str:
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            local = s.getsockname()[1]
        proc = subprocess.Popen(
            ["kubectl", "-n", namespace, "port-forward", f"svc/{svc}", f"{local}:{port}"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self._forwards.append(proc)
        base = f"http://127.0.0.1:{local}"
        deadline = time.time() + 60
        while time.time() < deadline:
            with (
                contextlib.suppress(OSError, http.client.HTTPException),
                urllib.request.urlopen(f"{base}{ready}", timeout=5) as response,  # noqa: S310 (our port-forward)
            ):
                if response.status == 200:
                    return base
            time.sleep(0.5)
        raise RuntimeError(f"port-forward to {namespace}/{svc} never answered")

    def get(self, svc: str, namespace: str, path: str) -> dict | None:
        raw = f"/api/v1/namespaces/{namespace}/services/http:{svc}:{PORT}/proxy{path}"
        result = subprocess.run(["kubectl", "get", "--raw", raw], capture_output=True, text=True)
        return json.loads(result.stdout) if result.returncode == 0 and result.stdout.strip() else None

    def delete(self, namespaces: tuple[str, ...]) -> None:
        for proc in self._forwards:
            proc.terminate()
        kubectl("delete", "namespace", *namespaces, "--ignore-not-found", "--wait=true", "--timeout=180s", check=False)


def http_json(method: str, address: str, body: object = None, headers: dict[str, str] | None = None):
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(address, data=data, method=method, headers={"Content-Type": "application/json"})  # noqa: S310
    for key, value in (headers or {}).items():
        request.add_header(key, value)
    try:
        with urllib.request.urlopen(request, timeout=10) as response:  # noqa: S310 (http to our own services)
            raw = response.read()
            return response.status, json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        return e.code, {}
    except (urllib.error.URLError, TimeoutError, ConnectionError, http.client.HTTPException):
        return 599, {}


def _hex24(*parts: object) -> str:
    return hashlib.sha1("-".join(map(str, parts)).encode(), usedforsecurity=False).hexdigest()[:24]


class Oracle:
    def __init__(self, healthy) -> None:
        self._healthy = healthy

    def evaluate(self) -> dict:
        return {"success": self._healthy()}


@dataclass(frozen=True)
class AppInfo:
    name: str
    description: str


class KnowledgeProblem:
    """What a runner needs of a problem: namespace, app, root_cause, inject/recover and the mitigation oracle."""

    namespaces: tuple[str, ...] = ()
    services: tuple[Svc, ...] = ()
    app = AppInfo("", "")
    root_cause = ""
    report = ""

    def __init__(self, cluster=None) -> None:
        self.cluster = cluster or KubeCluster()
        self.stop = threading.Event()
        self.rng = random.Random(1832)
        self.mitigation_oracle = Oracle(self.healthy)

    @property
    def namespace(self) -> str:
        return self.namespaces[0]

    def inject_fault(self) -> None:
        self.cluster.deploy(self.namespaces, self.services)
        self.scenario(self.background())

    # The world as it runs every day: the services and their steady traffic, with no incident and none of the
    # incident's people acting in it.
    def inject_steady(self) -> None:
        self.cluster.deploy(self.namespaces, self.services)
        self.background()

    def recover_fault(self) -> None:
        self.stop.set()
        self.cluster.delete(self.namespaces)

    # Starts the steady traffic and returns when it started.
    def background(self) -> float:
        raise NotImplementedError

    # The incident's timeline, from when the steady traffic started.
    def scenario(self, start: float) -> None:
        raise NotImplementedError

    def healthy(self) -> bool:
        raise NotImplementedError

    def every(self, low_s: float, high_s: float, action) -> None:
        def loop() -> None:
            while not self.stop.wait(self.rng.uniform(low_s, high_s)):
                with contextlib.suppress(Exception):
                    action()

        threading.Thread(target=loop, daemon=True).start()

    def at(self, start: float, offset_s: float, action) -> None:
        def run() -> None:
            if not self.stop.wait(max(0.0, start + offset_s - time.time())):
                action()

        threading.Thread(target=run, daemon=True).start()


# ---------------------------------------------------------------------------------------------------
# The support desk (support_handoff_failed)
# ---------------------------------------------------------------------------------------------------

SUPPORT, REALTIME = "support", "realtime"
COMPANY = "wellnessdemo"
TARGET_AGENT = "rcallan@acme.example"
TARGET_CONVERSATION = "CAb132d8d1457b10e718d26b238786e4f6"
OTHER_AGENTS = {
    "jdiaz@acme.example": "healthdemo",
    "mokafor@acme.example": "wellnessdemo",
    "lchen@acme.example": "acmeretail",
    "spatel@acme.example": "healthdemo",
}


class Desk:
    """One agent's desk in a browser: fetches its tasks, holds its SSE channel, heartbeats, accepts offers."""

    def __init__(self, problem: "SupportHandoff", agent: str, company: str) -> None:
        self.problem, self.agent, self.company = problem, agent, company
        # A login starts a session; its heartbeat and channel threads stop when a later expire or login ends it.
        self.session = 0
        self.conn: http.client.HTTPConnection | None = None

    @property
    def channel(self) -> str:
        return f"{self.company}_{self.agent}_agent_events-{self.agent}"

    def login(self, subscribe_after_s: float = 0.0) -> None:
        self.session += 1
        session = self.session
        http_json("GET", f"{self.problem.routing}/agents/{urllib.parse.quote(self.agent)}/tasks")
        time.sleep(subscribe_after_s)
        threading.Thread(target=self._listen, args=(session,), daemon=True).start()
        threading.Thread(target=self._heartbeat, args=(session,), daemon=True).start()

    def expire(self) -> None:
        self.session += 1
        if self.conn is not None and self.conn.sock is not None:
            with contextlib.suppress(OSError):
                self.conn.sock.shutdown(socket.SHUT_RDWR)

    def _live(self, session: int) -> bool:
        return session == self.session and not self.problem.stop.is_set()

    def _heartbeat(self, session: int) -> None:
        while self._live(session):
            http_json("POST", f"{self.problem.routing}/agents/{urllib.parse.quote(self.agent)}/heartbeat", {})
            self.problem.stop.wait(10)

    def _listen(self, session: int) -> None:
        parsed = urllib.parse.urlsplit(self.problem.push_api)
        query = urllib.parse.urlencode({"channel": self.channel, "externalId": self.agent})
        while self._live(session):
            conn = self.conn = http.client.HTTPConnection(parsed.hostname, parsed.port, timeout=60)
            try:
                conn.request("GET", f"/connect/sse?{query}")
                response = conn.getresponse()
                while self._live(session):
                    line = response.fp.readline()
                    if not line:
                        break
                    if line.startswith(b"data: "):
                        self._on_message(json.loads(line[6:]))
            except (OSError, http.client.HTTPException, ValueError):
                pass
            finally:
                conn.close()
            self.problem.stop.wait(2)

    def _on_message(self, message: dict) -> None:
        if message.get("type") != "TICKET_PREASSIGNED":
            return

        def accept() -> None:
            time.sleep(self.problem.rng.uniform(4, 13))
            http_json("POST", f"{self.problem.routing}/tasks/{message['taskId']}/accept", {"agentId": self.agent})

        threading.Thread(target=accept, daemon=True).start()


class SupportHandoff(KnowledgeProblem):
    namespaces = (SUPPORT, REALTIME)
    services = (
        Svc("push-hub", REALTIME, {}),
        Svc("push-api", REALTIME, {"PUSH_HUB_URL": url("push-hub", REALTIME)}),
        Svc(
            "ticketing",
            SUPPORT,
            {
                "ROUTING_URL": url("routing", SUPPORT),
                "PUSH_API_URL": url("push-api", REALTIME),
                "ASSIGNMENT_TIMEOUT_S": "180",
            },
        ),
        Svc(
            "routing", SUPPORT, {"TICKETING_URL": url("ticketing", SUPPORT), "PUSH_API_URL": url("push-api", REALTIME)}
        ),
        Svc("llm-bot", SUPPORT, {"TICKETING_URL": url("ticketing", SUPPORT)}),
    )
    app = AppInfo(
        "support-desk",
        "A support desk: llm-bot answers customers and hands conversations to human agents through ticketing and "
        "routing.",
    )
    report = (
        f"Cleric today there was a demo done using {COMPANY} company marker for HITL and it failed to connect: the "
        f"conversation was handed off to a human agent and nobody picked it up.\nThe conversation id is "
        f"{TARGET_CONVERSATION}\nLook into the logs to check what caused it. Check llm-bot, ticketing and routing, all "
        f"in namespace {SUPPORT}."
    )
    # What the grader compares an answer against: the fault and what a correct answer must show, with the scenario's
    # own setting (the desk reconnecting) named as not the fault.
    root_cause = (
        f"[fault_spec] component=the delivery of the preassigned offer to agent {TARGET_AGENT}'s event channel "
        f"(routing publishing it, through push-api and push-hub); namespace={REALTIME} || routing published the "
        f"ticket's preassigned offer for agent {TARGET_AGENT} to the agent's event channel through push-api and "
        "push-hub less than a second before the agent's desk, reconnecting after its session expired, opened its "
        "Server-Sent Events connection to that channel. push-hub had no receivers for the channel (receivers=0) and "
        "does not replay, so the offer never reached the desk and the ticket timed out unassigned. The fault spans "
        "that delivery path: a diagnosis may place it in routing, push-api or push-hub, provided it shows, from "
        "push-api's or push-hub's logs, that the offer was published before the desk's channel was open; a diagnosis "
        "that does not show that has not localized it. The agent's expired session and reconnect are the setting, not "
        "the fault: ticket assignment and the agent's acceptance worked as designed, and llm-bot and ticketing worked."
    )

    def background(self) -> float:
        self.routing = self.cluster.forward("routing", SUPPORT)
        self.push_api = self.cluster.forward("push-api", REALTIME)
        self.bot = self.cluster.forward("llm-bot", SUPPORT)
        self.desks = {agent: Desk(self, agent, company) for agent, company in OTHER_AGENTS.items()}
        for desk in self.desks.values():
            desk.login()
        self.every(
            20, 40, lambda: self.handoff(f"CA{self.rng.getrandbits(128):032x}", self.rng.choice(list(OTHER_AGENTS)))
        )
        return time.time()

    def handoff(self, conversation: str, agent: str) -> None:
        company = self.desks[agent].company
        http_json(
            "POST", f"{self.bot}/conversations/{conversation}/handoff", {"company_marker": company, "queue": agent}
        )

    def scenario(self, start: float) -> None:
        target = self.desks[TARGET_AGENT] = Desk(self, TARGET_AGENT, COMPANY)
        target.login()
        # The agent's earlier ticket, offered and accepted while the desk was connected.
        self.at(start, 12, lambda: self.handoff(f"CA{self.rng.getrandbits(128):032x}", TARGET_AGENT))
        # The desk's session lapses: no heartbeats, no channel; routing expires it about 30 s later.
        self.at(start, 45, target.expire)
        # The demo's conversation is handed to the agent while the desk is away: routing queues it.
        self.at(start, 130, lambda: self.handoff(TARGET_CONVERSATION, TARGET_AGENT))
        # The desk comes back: it fetches its tasks, which makes routing send the queued offer at once, and only
        # then opens its channel.
        self.at(start, 157, lambda: target.login(subscribe_after_s=0.4))

    def healthy(self) -> bool:
        stats = self.cluster.get("ticketing", SUPPORT, "/stats")
        return stats is None or stats.get("escalated", 0) == 0


# ---------------------------------------------------------------------------------------------------
# The workspace suite (meeting_runner_dlq, payments_core_5xx)
# ---------------------------------------------------------------------------------------------------

WORKSPACE, MAIL, IDENTITY, BACKOFFICE = "workspace", "mail", "identity", "backoffice"
DOMAIN = "lanternhq.example"
FIRST = [
    "alex",
    "sam",
    "jordan",
    "taylor",
    "morgan",
    "casey",
    "riley",
    "jamie",
    "avery",
    "quinn",
    "dana",
    "robin",
    "drew",
    "kai",
    "noor",
    "lena",
    "omar",
    "priya",
]
LAST = [
    "kim",
    "riley",
    "park",
    "chen",
    "silva",
    "novak",
    "shah",
    "ito",
    "haddad",
    "moreau",
    "keller",
    "okafor",
    "rossi",
    "brandt",
    "lund",
    "vega",
]


def _people(n: int) -> list[dict]:
    people, seen = [], set()
    for i in range(n):
        first, last = FIRST[i % len(FIRST)], LAST[(i // len(FIRST)) % len(LAST)]
        email = f"{first}.{last}@{DOMAIN}"
        if email in seen:
            email = f"{first}.{last}{i}@{DOMAIN}"
        seen.add(email)
        people.append({"id": _hex24("user", i), "email": email, "name": f"{first.title()} {last.title()}"})
    return people


USERS = _people(600)
GRANT_USER = next(u for u in USERS if u["email"] == f"morgan.riley@{DOMAIN}")
SWEEP_ADMIN = next(u for u in USERS if u["email"] == f"dana.kim@{DOMAIN}")
ADMINS = [SWEEP_ADMIN, *(u for u in USERS[::20] if u is not SWEEP_ADMIN)][:30]
WORKSPACES = [_hex24("workspace", i) for i in range(2000)]
_billing = random.Random(7)
MANUAL_BILLING = _billing.sample(WORKSPACES, 60)
NO_METADATA = [next(w for w in WORKSPACES if w not in MANUAL_BILLING)]
_grants = random.Random(11)
REVOKED = {GRANT_USER["id"], *(u["id"] for u in _grants.sample(USERS, 110))}
# Users whose Google refusal is something other than a dead grant, so calendar-api's code names no single cause.
LIMITED_SCOPES = {u["id"] for u in _grants.sample(USERS, 60) if u["id"] not in REVOKED}
EXTERNAL_MAILBOXES = [
    {"email": f"{_hex24('ext', i)[:8]}@{['gmail.com', 'outlook.com', 'example.org'][i % 3]}", "revoked": i % 5 == 0}
    for i in range(240)
]


class WorkspaceSuite(KnowledgeProblem):
    namespaces = (WORKSPACE, MAIL, IDENTITY, BACKOFFICE)
    services = (
        Svc("accounts", IDENTITY, {}),
        Svc("mailbox-sync", MAIL, {"REFRESH_BATCH": "40", "REFRESH_EVERY_S": "10"}),
        Svc("calendar-api", WORKSPACE, {}),
        Svc("meeting-runner", WORKSPACE, {"CALENDAR_API_URL": url("calendar-api", WORKSPACE)}),
        Svc("payments-core", WORKSPACE, {}),
        Svc("subscriptions-api", WORKSPACE, {"PAYMENTS_CORE_URL": url("payments-core", WORKSPACE)}),
        Svc(
            "admin-api",
            BACKOFFICE,
            {"PAYMENTS_CORE_URL": url("payments-core", WORKSPACE), "ACCOUNTS_URL": url("accounts", IDENTITY)},
        ),
    )
    app = AppInfo(
        "workspace-suite",
        "A meetings and billing suite: meeting-runner joins users' scheduled meetings, payments-core serves paid "
        "subscriptions to the app and to the admin console's API, admin-api.",
    )

    def background(self) -> float:
        seeds = {
            ("accounts", IDENTITY): {"users": USERS},
            ("mailbox-sync", MAIL): {
                "mailboxes": self.rng.sample(
                    [{"email": u["email"], "revoked": u["id"] in REVOKED} for u in USERS] + EXTERNAL_MAILBOXES,
                    len(USERS) + len(EXTERNAL_MAILBOXES),
                )
            },
            ("calendar-api", WORKSPACE): {
                "google_errors": {
                    **dict.fromkeys(sorted(LIMITED_SCOPES), "insufficientPermissions"),
                    **dict.fromkeys(sorted(REVOKED), "invalid_grant"),
                }
            },
            ("payments-core", WORKSPACE): {"manual_billing": MANUAL_BILLING, "no_metadata": NO_METADATA},
            ("subscriptions-api", WORKSPACE): {
                "reads": [
                    (u["id"], w)
                    for u, w in zip(
                        self.rng.sample(USERS, 300),
                        self.rng.sample([w for w in WORKSPACES if w not in MANUAL_BILLING + NO_METADATA], 300),
                        strict=True,
                    )
                ]
            },
        }
        for (svc, ns), seed in seeds.items():
            base = self.cluster.forward(svc, ns)
            status, _ = http_json("POST", f"{base}/internal/seed", seed)
            if status != 200:
                raise RuntimeError(f"seeding {ns}/{svc} failed: {status}")
        self.runner = self.cluster.forward("meeting-runner", WORKSPACE)
        self.admin = self.cluster.forward("admin-api", BACKOFFICE)
        self.accounts = self.cluster.forward("accounts", IDENTITY)
        healthy_users = [u for u in USERS if u["id"] not in REVOKED | LIMITED_SCOPES]
        # Support staff open any workspace, manual-billing ones included: payments-core answers those 500 all day.
        console_workspaces = [w for w in WORKSPACES if w not in NO_METADATA]

        def meeting() -> None:
            user = self.rng.choice(healthy_users)
            self.schedule(user["id"], time.time())

        def admin_read() -> None:
            admin = self.rng.choice(ADMINS[1:])
            workspace = self.rng.choice(console_workspaces)
            for what in self.rng.sample(["trials", "subscriptions", "billing-profile"], self.rng.randint(1, 3)):
                self.admin_read(admin["email"], workspace, what)

        def sign_in() -> None:
            self.sign_in(self.rng.choice(USERS)["id"], self.rng.choice(["google", "google", "password"]))

        self.every(15, 30, meeting)
        self.every(0.5, 1.5, admin_read)
        self.every(3, 8, sign_in)
        return time.time()

    def scenario(self, start: float) -> None:
        # The suite's users who matter sign in like anyone else, so accounts' log holds their id beside their email.
        for user, offset in ((GRANT_USER, 20), (GRANT_USER, 110), (SWEEP_ADMIN, 15)):
            self.at(start, offset, lambda user=user: self.sign_in(user["id"], "password"))
        self.incident(start)

    def sign_in(self, user_id: str, provider: str) -> None:
        http_json("POST", f"{self.accounts}/sessions", {"user_id": user_id, "provider": provider})

    def schedule(self, user_id: str, at: float, meeting_id: str | None = None, event_id: str | None = None) -> None:
        letters = "abcdefghijklmnopqrstuvwxyz"
        meeting_id = meeting_id or "-".join("".join(self.rng.choice(letters) for _ in range(n)) for n in (3, 4, 3))
        event_id = event_id or "".join(self.rng.choice(letters + "0123456789") for _ in range(26))
        task = {"meetingId": meeting_id, "eventId": event_id, "userId": user_id, "at": at}
        http_json("POST", f"{self.runner}/schedule", {"tasks": [task]})

    def admin_read(self, email: str, workspace: str, what: str) -> None:
        http_json(
            "GET", f"{self.admin}/account-admin/v1/workspaces/{workspace}/{what}", headers={"X-Actor-Email": email}
        )

    def incident(self, start: float) -> None:
        raise NotImplementedError


class MeetingRunnerDlq(WorkspaceSuite):
    """A user's dead Google grant, seen only in mailbox-sync, which logs users by email."""

    report = (
        "Triggered: [meeting-runner] DLQ not empty (production) — max meeting-runner-dlq depth >= 1. A scheduled "
        f"meeting task failed processing and is in meeting-runner-dlq. Service: meeting-runner, namespace {WORKSPACE}."
    )
    root_cause = (
        f"[fault_spec] component=calendar-api; namespace={WORKSPACE} || The failing task's user, {GRANT_USER['id']} "
        f"({GRANT_USER['email']}), has a dead Google OAuth grant: mailbox-sync logs that user's mailbox as logged out "
        "(invalid_grant). Every Google call calendar-api makes for the user fails with permanent_google_api_failure "
        "(500), so meeting-runner's task for the user failed three times and was dead-lettered. Retrying cannot "
        "succeed until the user reconnects their Google account."
    )

    def incident(self, start: float) -> None:
        # mailbox-sync's round robin logs every mailbox about every 3 minutes; the user's task falls due after it
        # has logged the user's at least once.
        self.at(
            start,
            150,
            lambda: self.schedule(GRANT_USER["id"], time.time(), "lhe-sdnc-fio", "3kq9v2m7c1x8rb4n6tj0wslpe5"),
        )

    def healthy(self) -> bool:
        dlq = self.cluster.get("meeting-runner", WORKSPACE, "/dlq")
        return dlq is None or dlq.get("depth", 0) == 0


class PaymentsCore5xx(WorkspaceSuite):
    """One admin's sweep through admin-api, which logs admins by email."""

    report = (
        "[billing] Payments Core requests are failing (production): payments-core answered more than 40 requests "
        "with 5xx in the last 15 minutes, all on GET /v1/paid-subscriptions. Every payments-core replica is running "
        f"and ready. Namespace {WORKSPACE}."
    )
    # What the grader compares an answer against: one person's activity, which is not a fault in any service.
    root_cause = (
        f"[fault_spec] component=admin-api (one admin's bulk reads through it); namespace={BACKOFFICE} || One admin, "
        f"{SWEEP_ADMIN['email']} (user id {SWEEP_ADMIN['id']} on payments-core's errors), read about 437 workspaces "
        "through admin-api in under seven minutes (about 945 audited reads). The reads of manual-billing workspaces "
        "reached payments-core's paid-subscriptions endpoint, which answers those business-rule rejections "
        "(ManualBillingError, NormalityCheckError) with 500, so the 5xx alert fired. The cause is one person's "
        "activity, not a fault in any service: a diagnosis localizes it by naming the admin and their reads through "
        "admin-api. Contributing: payments-core answering business-rule rejections with 500. Affected: payments-core's "
        "error rate only; payments-core is otherwise healthy."
    )
    SWEEP_S = 400

    def incident(self, start: float) -> None:
        rng = random.Random(945)
        others = rng.sample([w for w in WORKSPACES if w not in MANUAL_BILLING + NO_METADATA], 401)
        failing = MANUAL_BILLING[:35] + NO_METADATA
        swept = failing + others
        reads = [("trials", w) for w in swept] + [("trials", rng.choice(swept)) for _ in range(474 - len(swept))]
        reads += [("subscriptions", w) for w in failing] + [
            ("subscriptions", rng.choice(MANUAL_BILLING[:35])) for _ in range(13)
        ]
        reads += [("subscriptions", NO_METADATA[0])] + [("subscriptions", w) for w in rng.sample(others, 328 - 50)]
        reads += [("billing-profile", w) for w in rng.sample(swept, 143)]
        rng.shuffle(reads)

        def sweep() -> None:
            gap = self.SWEEP_S / len(reads)
            for what, workspace in reads:
                if self.stop.is_set():
                    return
                self.admin_read(SWEEP_ADMIN["email"], workspace, what)
                time.sleep(gap * rng.uniform(0.3, 1.7))

        self.at(start, 60, lambda: threading.Thread(target=sweep, daemon=True).start())

    def healthy(self) -> bool:
        stats = self.cluster.get("payments-core", WORKSPACE, "/stats")
        return stats is None or stats.get("errors_15m", 0) < 40


# ---------------------------------------------------------------------------------------------------
# The storefront (label_backlog, orders_drop_edge, checkout_fx_quota)
# ---------------------------------------------------------------------------------------------------

SHOP, FULFIL, PLATFORM, MARKETING, OBSERVE = "shop", "fulfil", "platform", "marketing", "observe"
ENGINEERS = [f"{name}@kestrel.example" for name in ("r.alvarez", "m.chen", "s.okoye", "j.lindqvist", "p.haas")]
EXPERIMENT_BOT = "exp-scheduler@kestrel.example"
CURRENCIES = ["USD"] * 5 + ["EUR"] * 4 + ["GBP"] * 3 + ["CAD", "CAD", "AUD", "JPY", "CHF", "SEK", "MXN", "NZD"]
_areas = random.Random(41)
# The postcodes the carrier delivers to, and the table the label_backlog world loads from its start: the carrier's
# file cut off at 82% of its rows, so the highest postcodes are missing.
SERVICE_AREA = sorted(f"{p:05d}" for p in _areas.sample(range(1001, 99951), 38906))
TRUNCATED_SERVICE_AREA = SERVICE_AREA[: len(SERVICE_AREA) * 82 // 100]
_served = set(SERVICE_AREA)
_unserved = [f"{p:05d}" for p in range(1001, 99951) if f"{p:05d}" not in _served]
CLIENTS = {
    "web": "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Safari/605.1.15",
    "android": "KestrelShop/5.14.0 (Android 15; Pixel 8; okhttp/4.12.0)",
    "ios": "KestrelShop/5.14.0 (iPhone; iOS 18.1; Scale/3.00)",
}
WEB_SHOPPERS = [
    {
        "id": f"shp_{_hex24('shopper', i)[:16]}",
        "currency": CURRENCIES[i % len(CURRENCIES)],
        # One shopper in a hundred lives where the carrier does not deliver.
        "postcode": _areas.choice(_unserved) if i % 100 < 1 else _areas.choice(SERVICE_AREA),
        "client": _areas.choices(["android", "ios", "web"], weights=[40, 10, 50])[0],
        "ip": f"{_areas.randrange(24, 223)}.{_areas.randrange(256)}.{_areas.randrange(256)}.{_areas.randrange(1, 255)}",
        "country": _areas.choice(["US"] * 8 + ["CA", "GB"]),
    }
    for i in range(2400)
]
# Shoppers recs-builder keeps recommendations for: the web shop's and the apps'.
ACTIVE_SHOPPERS = [s["id"] for s in WEB_SHOPPERS] + [f"shp_{_hex24('app-shopper', i)[:16]}" for i in range(6600)]
SEARCHES = ["jacket", "boots", "lamp", "mug", "scarf", "backpack", "desk", "kettle", "rug", "tent", "watch", "chair"]
SETTINGS = {
    "pricing": {"promo.autumn_sale": False, "promo.discount_pct": 10},
    "checkout": {"ui.express_pay": True},
    "search": {"ranking.boost_new_arrivals": 1.2},
    "storefront-api": {"banner.text": "", "session.ttl_s": 7200},
    "shipping-labels": {"carrier.retry_backoff_ms": 1000, "carrier.max_attempts": 5},
}
# Everyday runtime-setting changes, made from the admin console by the shop's engineers.
EVERYDAY_CHANGES = [
    ("storefront-api", "banner.text", ["Free returns until Nov 30", "New: winter boots", ""]),
    ("search", "ranking.boost_new_arrivals", [1.1, 1.2, 1.3, 1.4]),
    ("pricing", "promo.discount_pct", [10, 12, 15]),
    ("checkout", "ui.express_pay", [True, False]),
    ("shipping-labels", "carrier.retry_backoff_ms", [800, 1000, 1200]),
]
# The experiment platform's bot moves traffic between experiment arms all day.
EXPERIMENTS = [
    ("search", "experiments.ranking-v3.allocation"),
    ("search", "experiments.typo-tolerance.allocation"),
    ("storefront-api", "experiments.new-pdp.allocation"),
    ("storefront-api", "experiments.sticky-cart.allocation"),
    ("checkout", "experiments.one-page-checkout.allocation"),
    ("pricing", "experiments.price-display.allocation"),
    ("pricing", "experiments.bundle-discounts.allocation"),
    ("shipping-labels", "experiments.label-batching.allocation"),
]
# The feature flags the shop evaluates (key -> what reads it), from the flag provider. The product team flips them
# from the provider's dashboard; its audit log reaches Loki as a stream of its own. The services in the cluster get
# theirs through flag-relay; the mobile app's backend (MOBILE_BACKEND), which runs outside the cluster, uses the
# provider's SDK directly.
MOBILE_BACKEND = "mobile-bff"
FLAGS = {
    "storefront-holiday-theme": ["storefront-api"],
    "storefront-guest-checkout": ["storefront-api"],
    "checkout-apple-pay": ["checkout"],
    "checkout-address-autocomplete": ["checkout"],
    "search-semantic-ranking": ["search"],
    "search-local-currency-prices": ["search"],
    "checkout-bundle-badges": ["checkout"],
    "mobile-wishlist-sync": [MOBILE_BACKEND],
    "mobile-dark-mode": [MOBILE_BACKEND],
}
# The flag checkout_fx_quota's incident turns on, and who turns it on; the product team never flips it day to day.
FX_FLAG, FX_ACTOR = "search-local-currency-prices", "t.nakamura@kestrel.example"
EVERYDAY_FLAGS = [key for key in FLAGS if key != FX_FLAG]
# The FX rates provider, outside the cluster (served by this process, see host_address), and the shop's API
# key there: the mobile backend holds the same key, so the plan's quota is shared with it.
FX_PROVIDER_HOST, FX_PROVIDER_PORT = "api.ratesprovider.example", 18471
FX_KEY, FX_PLAN_PER_MIN = "kestrel-shop", 40
PRODUCT_TEAM = [f"{name}@kestrel.example" for name in ("a.ferreira", "k.brooks", "l.nguyen")]
FLAG_RELAY = url("flag-relay", PLATFORM)
CDN_ZONE = "shop.kestrel.example"
BAD_BOTS = ["python-requests/2.31.0", "Scrapy/2.11 (+https://scrapy.org)", "Go-http-client/1.1", "curl/8.4.0"]
# The WAF rule a managed-ruleset update turned on in the orders_drop_edge world, and the one that always runs.
BOT_RULE = ("100201", "Known bad bot user agent")
APP_RULE = ("100917", "Anomalous mobile SDK request fingerprint")


# Where pods reach a server in this process: SCENARIO_HOST_ADDRESS when set (an address of this machine the cluster's
# pods can route to), else the gateway of the nodes' network, which is the host of a kind cluster.
def host_address() -> str:
    if address := os.environ.get("SCENARIO_HOST_ADDRESS"):
        return address
    node_ip = kubectl(
        "get", "nodes", "-o", "jsonpath={.items[0].status.addresses[?(@.type=='InternalIP')].address}"
    ).strip()
    return ".".join([*node_ip.split(".")[:3], "1"])


# Every service sends a sample of its calls as OTLP client spans: to SCENARIO_OTLP_ENDPOINT when set (empty sends
# none), else to a Jaeger agent in namespace observe.
def _tracing() -> dict[str, str]:
    endpoint = os.environ.get("SCENARIO_OTLP_ENDPOINT", f"http://jaeger-agent.{OBSERVE}.svc.cluster.local:4318")
    return {"OTEL_EXPORTER_OTLP_ENDPOINT": endpoint, "OTEL_TRACES_SAMPLER_ARG": "0.1"} if endpoint else {}


class FxProvider:
    """The FX rates provider outside the cluster: this process serves its API on this machine, which pods reach at
    host_address(). It meters each API key against its plan (a token bucket, FX_PLAN_PER_MIN with a
    small burst). The shop's key is also held by the mobile app's backend, also outside the cluster, which this class
    plays: a rates refresh a minute."""

    RATES: ClassVar[dict[str, float]] = {
        c: round(0.5 + 3.7 * i, 4)
        for i, c in enumerate(["EUR", "GBP", "CAD", "AUD", "JPY", "CHF", "SEK", "NOK", "MXN", "BRL", "NZD", "SGD"])
    }

    def __init__(self, stop: threading.Event) -> None:
        self.stop, self.lock = stop, threading.Lock()
        self.burst = FX_PLAN_PER_MIN / 6
        self.tokens, self.last = self.burst, time.time()
        self.address = host_address()
        provider = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: object) -> None:  # noqa: A002 (the base class's signature)
                pass

            def do_GET(self) -> None:
                status, body = provider.serve(self.path, self.headers.get("X-Api-Key", ""))
                raw = json.dumps(body).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

        server = http.server.ThreadingHTTPServer(("0.0.0.0", FX_PROVIDER_PORT), Handler)  # noqa: S104 (pods reach it)
        server.daemon_threads = True
        threading.Thread(target=server.serve_forever, daemon=True).start()
        threading.Thread(target=self._mobile_backend, daemon=True).start()

    def _admit(self) -> bool:
        now = time.time()
        with self.lock:
            self.tokens = min(self.burst, self.tokens + (now - self.last) * FX_PLAN_PER_MIN / 60)
            self.last = now
            if self.tokens < 1:
                return False
            self.tokens -= 1
            return True

    def serve(self, path: str, key: str) -> tuple[int, dict]:
        if key != FX_KEY:
            return 401, {"error": "invalid_api_key"}
        if not self._admit():
            message = f"Plan limit of {FX_PLAN_PER_MIN} requests per minute reached for this API key"
            return 429, {"error": "rate_limit_exceeded", "message": message, "limit_per_min": FX_PLAN_PER_MIN}
        if path.startswith("/v1/rates/USD/"):
            quote = path.rsplit("/", 1)[-1]
            if quote not in self.RATES:
                return 404, {"error": "unknown_currency"}
            return 200, {"base": "USD", "quote": quote, "rate": self.RATES[quote]}
        return 200, {"base": "USD", "rates": self.RATES}

    # The mobile backend on the same key: its rates refresh once a minute.
    def _mobile_backend(self) -> None:
        while not self.stop.wait(60):
            self._admit()


class Storefront(KnowledgeProblem):
    namespaces = (SHOP, FULFIL, PLATFORM, MARKETING)
    services = (
        Svc("settings", PLATFORM, {}),
        Svc("flag-relay", PLATFORM, {}),
        Svc("registry", PLATFORM, {}),
        Svc(
            "fx-gateway",
            PLATFORM,
            {"FX_PROVIDER_URL": f"http://{FX_PROVIDER_HOST}:{FX_PROVIDER_PORT}", "FX_API_KEY_ID": FX_KEY},
        ),
        Svc("carrier-gw", PLATFORM, {}),
        Svc("sessions-cache", PLATFORM, {"MAX_BYTES": str(24 * 1024 * 1024)}),
        Svc("catalog-api", SHOP, {}),
        Svc(
            "search",
            SHOP,
            {
                "CATALOG_URL": url("catalog-api", SHOP),
                "FX_GATEWAY_URL": url("fx-gateway", PLATFORM),
                "SETTINGS_URL": url("settings", PLATFORM),
                "FLAG_RELAY_URL": FLAG_RELAY,
            },
        ),
        Svc("payments", SHOP, {}),
        Svc(
            "pricing",
            SHOP,
            {"FX_GATEWAY_URL": url("fx-gateway", PLATFORM), "SETTINGS_URL": url("settings", PLATFORM)},
        ),
        Svc("orders", FULFIL, {}),
        Svc("address-validation", FULFIL, {}),
        Svc(
            "shipping-labels",
            FULFIL,
            {
                "ORDERS_URL": url("orders", FULFIL),
                "ADDRESS_VALIDATION_URL": url("address-validation", FULFIL),
                "CARRIER_GW_URL": url("carrier-gw", PLATFORM),
                "SETTINGS_URL": url("settings", PLATFORM),
            },
        ),
        Svc(
            "checkout",
            SHOP,
            {
                "PRICING_URL": url("pricing", SHOP),
                "PAYMENTS_URL": url("payments", SHOP),
                "ORDERS_URL": url("orders", FULFIL),
                "SETTINGS_URL": url("settings", PLATFORM),
                "FLAG_RELAY_URL": FLAG_RELAY,
            },
        ),
        Svc(
            "storefront-api",
            SHOP,
            {
                "SESSION_STORE_URL": url("sessions-cache", PLATFORM),
                "SEARCH_URL": url("search", SHOP),
                "CHECKOUT_URL": url("checkout", SHOP),
                "SETTINGS_URL": url("settings", PLATFORM),
                "FLAG_RELAY_URL": FLAG_RELAY,
            },
        ),
        # recs-builder finds its store through the registry, which has pointed p13n-store at sessions-cache since
        # the old p13n cache was retired.
        Svc(
            "recs-builder",
            MARKETING,
            {"CATALOG_URL": url("catalog-api", SHOP), "REGISTRY_URL": url("registry", PLATFORM), "MODEL": "v6"},
        ),
        Svc("feed-exporter", MARKETING, {"CATALOG_URL": url("catalog-api", SHOP)}),
    )
    app = AppInfo(
        "storefront",
        "An online shop: storefront-api serves the web shop and the apps behind a CDN, search and checkout serve it, "
        "orders and shipping-labels ship what is bought, and marketing's jobs feed recommendations and ad feeds from "
        "the catalog.",
    )
    # The carrier's service-area table the incident's world loads from its start.
    service_area = SERVICE_AREA
    TRACING: ClassVar[dict[str, str]] = _tracing()

    def _deployed(self) -> tuple[Svc, ...]:
        return tuple(
            Svc(
                svc.name,
                svc.namespace,
                {**svc.env, **self.TRACING, "OTEL_SERVICE_NAME": svc.name},
                ((self.provider.address, FX_PROVIDER_HOST),) if svc.name == "fx-gateway" else (),
            )
            for svc in self.services
        )

    def inject_fault(self) -> None:
        self.provider = FxProvider(self.stop)
        self.cluster.deploy(self.namespaces, self._deployed())
        self.scenario(self.background(self.service_area))

    def inject_steady(self) -> None:
        self.provider = FxProvider(self.stop)
        self.cluster.deploy(self.namespaces, self._deployed())
        self.background(SERVICE_AREA)

    def background(self, service_area: list[str]) -> float:
        self.settings = self.cluster.forward("settings", PLATFORM)
        self.relay = self.cluster.forward("flag-relay", PLATFORM)
        self.validation = self.cluster.forward("address-validation", FULFIL)
        self.carrier = self.cluster.forward("carrier-gw", PLATFORM)
        self.storefront = self.cluster.forward("storefront-api", SHOP)
        self.orders = self.cluster.forward("orders", FULFIL)
        loki_ns, loki_svc = LOKI_SERVICE.split(":")[0].split("/")
        self.loki = self.cluster.forward(loki_svc, loki_ns, port=int(LOKI_SERVICE.split(":")[1]), ready="/ready")
        self.lock = threading.Lock()
        self.pushed: dict[tuple, list[list[str]]] = {}
        threading.Thread(target=self._push_logs, daemon=True).start()
        seeds = {
            ("settings", PLATFORM): {"settings": SETTINGS},
            ("recs-builder", MARKETING): {"shoppers": ACTIVE_SHOPPERS},
            ("registry", PLATFORM): {
                "names": {
                    "p13n-store": f"sessions-cache.{PLATFORM}",
                    "catalog": f"catalog-api.{SHOP}",
                    "orders": f"orders.{FULFIL}",
                    "settings": f"settings.{PLATFORM}",
                    "fx-rates": f"fx-gateway.{PLATFORM}",
                }
            },
        }
        for (svc, ns), seed in seeds.items():
            status, _ = http_json("POST", f"{self.cluster.forward(svc, ns)}/internal/seed", seed)
            if status != 200:
                raise RuntimeError(f"seeding {ns}/{svc} failed: {status}")
        self.service_area_file = 0
        self.publish_service_area(service_area)
        self.sessions: dict[str, str] = {}
        self.current_settings = {service: dict(values) for service, values in SETTINGS.items()}
        self.flags = dict.fromkeys(FLAGS, False)
        for key in FLAGS:
            self.stream_flag(key)
        self.blocked_clients: set[str] = set()
        self.shoppers(4)
        self.every(0.6, 1.8, self.bad_bot)
        self.every(180, 360, self.everyday_flag)
        self.every(100, 200, self.everyday_change)
        self.every(6, 14, self.experiment_change)
        self.every(300, 480, lambda: self.publish_service_area(service_area))
        self.every(240, 360, self.carrier_peak)
        self.every(15, 25, self.customer_service)
        return time.time()

    # Customer service works parked orders from the console, oldest first, about three a minute: it contacts the
    # shopper and corrects the address or cancels the order.
    def customer_service(self) -> None:
        status, body = http_json("GET", f"{self.orders}/v1/orders/parked?limit=1")
        for order in body.get("orders", []) if status == 200 else []:
            resolution = self.rng.choice(["cancelled: outside delivery area", "address corrected"])
            http_json("POST", f"{self.orders}/v1/orders/{order['orderId']}/resolve", {"resolution": resolution})

    def shoppers(self, n: int) -> None:
        for _ in range(n):
            self.every(0.6, 1.4, self.visit)

    def publish_service_area(self, postcodes: list[str]) -> None:
        self.service_area_file += 1
        stamp = time.strftime("%Y%m%dT%H%M", time.gmtime())
        table = {"file": f"sftp://drop.parcelco.example/kestrel/service_area_{stamp}.csv", "postcodes": postcodes}
        status, _ = http_json("POST", f"{self.validation}/internal/service-area", table)
        if status != 200:
            raise RuntimeError(f"publishing the service-area table failed: {status}")

    def carrier_peak(self) -> None:
        http_json("POST", f"{self.carrier}/internal/peak", {"seconds": self.rng.uniform(60, 100)})

    # A runtime-setting change from the admin console: the settings service applies it, and its controller records it
    # as a Kubernetes event on the settings Deployment.
    def change_setting(self, service: str, key: str, value: object, actor: str, comment: str = "") -> None:
        with self.lock:
            previous = self.current_settings.setdefault(service, {}).get(key)
            self.current_settings[service][key] = value
        change = {"key": key, "value": value, "actor": actor, "comment": comment}
        http_json("POST", f"{self.settings}/v1/settings/{service}", change)
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        event = {
            "apiVersion": "v1",
            "kind": "Event",
            "metadata": {"generateName": "settings.", "namespace": PLATFORM},
            "involvedObject": {
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "name": "settings",
                "namespace": PLATFORM,
            },
            "reason": "SettingChanged",
            "message": f"{service}: {key} {json.dumps(previous)} -> {json.dumps(value)} by {actor}"
            + (f" ({comment})" if comment else ""),
            "type": "Normal",
            "source": {"component": "settings-controller"},
            "firstTimestamp": now,
            "lastTimestamp": now,
            "count": 1,
        }
        kubectl("create", "-f", "-", stdin=json.dumps(event), check=False)

    def everyday_change(self) -> None:
        service, key, values = self.rng.choice(EVERYDAY_CHANGES)
        value = self.rng.choice([v for v in values if v != self.current_settings[service].get(key)])
        self.change_setting(service, key, value, self.rng.choice(ENGINEERS))

    def experiment_change(self) -> None:
        service, key = self.rng.choice(EXPERIMENTS)
        self.change_setting(service, key, self.rng.choice(range(0, 101, 5)), EXPERIMENT_BOT, "scheduled ramp")

    # One shopper's visit, through the CDN: signs in when it has no session (or its session is gone), searches, and
    # sometimes buys.
    def visit(self) -> None:
        shopper = self.rng.choice(WEB_SHOPPERS)
        for _ in range(self.rng.randint(1, 3)):
            if self._as(shopper, "GET", f"/api/search?q={self.rng.choice(SEARCHES)}") is None:
                return
        if self.rng.random() < 0.45:
            items = [
                {"sku": f"sku-{self.rng.randrange(12000):05d}", "price_usd": round(self.rng.uniform(8, 300), 2)}
                for _ in range(self.rng.randint(1, 3))
            ]
            address = {
                "line1": f"{self.rng.randint(1, 9999)} Main St",
                "postcode": shopper["postcode"],
                "country": "US",
            }
            self._as(shopper, "POST", "/api/checkout", {"items": items, "address": address})

    def _as(self, shopper: dict, method: str, path: str, body: object = None) -> int | None:
        for _ in range(2):
            with self.lock:
                sid = self.sessions.get(shopper["id"])
            if sid is None:
                status, session = self._edge(
                    shopper, "POST", "/api/login", {"shopper": shopper["id"], "currency": shopper["currency"]}
                )
                if status != 200:
                    return None
                sid = session["sid"]
                with self.lock:
                    self.sessions[shopper["id"]] = sid
            status, _ = self._edge(shopper, method, path, body, {"X-Session": sid})
            if status == 403:
                return None
            if status != 401:
                return status
            with self.lock:
                self.sessions.pop(shopper["id"], None)
        return None

    # A request through the CDN: its WAF may block it at the edge; otherwise it reaches storefront-api. Either way
    # the CDN logs it.
    def _edge(self, shopper: dict, method: str, path: str, body: object, headers: dict | None = None):
        blocked = shopper["client"] in self.blocked_clients
        if blocked:
            # The app waits on the edge's answer and its retries as long as a real request would take.
            self.stop.wait(self.rng.uniform(0.3, 0.8))
        status, result = (403, {}) if blocked else http_json(method, f"{self.storefront}{path}", body, headers)
        self._cdn_log(
            shopper["ip"], shopper["country"], CLIENTS[shopper["client"]], method, path, status, blocked, APP_RULE
        )
        return status, result

    def bad_bot(self) -> None:
        ip = f"{self.rng.randrange(24, 223)}.{self.rng.randrange(256)}.{self.rng.randrange(256)}.{self.rng.randrange(1, 255)}"
        path = self.rng.choice(["/api/search?q=" + self.rng.choice(SEARCHES), "/api/login", "/wp-login.php", "/.env"])
        self._cdn_log(
            ip, self.rng.choice(["US", "NL", "SG", "DE"]), self.rng.choice(BAD_BOTS), "GET", path, 403, True, BOT_RULE
        )

    def _cdn_log(
        self, ip: str, country: str, agent: str, method: str, path: str, status: int, blocked: bool, rule: tuple
    ) -> None:
        line = {
            "EdgeStartTimestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "RayID": f"{self.rng.getrandbits(64):016x}",
            "ClientIP": ip,
            "ClientCountry": country.lower(),
            "ClientRequestHost": CDN_ZONE,
            "ClientRequestMethod": method,
            "ClientRequestPath": path.split("?")[0],
            "ClientRequestUserAgent": agent,
            "EdgeColoCode": self.rng.choice(["IAD", "ORD", "SJC", "DFW", "LHR", "YYZ"]),
            "EdgeResponseStatus": status,
            "OriginResponseStatus": 0 if blocked else status,
            "SecurityAction": "block" if blocked else "",
            "SecurityRuleID": rule[0] if blocked else "",
            "SecurityRuleDescription": rule[1] if blocked else "",
        }
        self.push_log({"source": "cdn", "zone": CDN_ZONE, "dataset": "http_requests"}, line)

    # Systems outside the cluster (the CDN, the flag provider) push their logs into Loki as streams of their own: no
    # namespace or pod labels.
    def push_log(self, labels: dict[str, str], line: dict) -> None:
        with self.lock:
            key = tuple(sorted(labels.items()))
            self.pushed.setdefault(key, []).append([str(time.time_ns()), json.dumps(line, separators=(",", ":"))])

    def _push_logs(self) -> None:
        while not self.stop.wait(2):
            with self.lock:
                pushed, self.pushed = self.pushed, {}
            streams = [{"stream": dict(key), "values": values} for key, values in pushed.items()]
            if streams:
                http_json("POST", f"{self.loki}/loki/api/v1/push", {"streams": streams})

    # A flag's state as the provider streams it to the relay, for the services in the cluster; the mobile backend,
    # outside it, takes its flags from the provider directly.
    def stream_flag(self, key: str) -> None:
        if MOBILE_BACKEND in FLAGS[key]:
            return
        flag = {"key": key, "on": self.flags[key], "services": FLAGS[key]}
        http_json("POST", f"{self.relay}/internal/stream", flag)

    # A flag flipped in the provider's dashboard: the provider streams it to its SDKs and writes its audit log.
    def flip_flag(self, key: str, on: bool, member: str, comment: str = "") -> None:
        self.flags[key] = on
        self.stream_flag(key)
        verb = "turned on" if on else "turned off"
        self.push_log(
            {"source": "feature-flags", "project": "kestrel-shop", "environment": "production"},
            {
                "date": int(time.time() * 1000),
                "kind": "flag",
                "key": key,
                "title": f"{member} {verb} the flag {key} in Production",
                "member": {"email": member},
                "action": "updateOn",
                "previous": not on,
                "current": on,
                "comment": comment,
            },
        )

    def everyday_flag(self) -> None:
        key = self.rng.choice(EVERYDAY_FLAGS)
        self.flip_flag(key, not self.flags[key], self.rng.choice(PRODUCT_TEAM))


class LabelBacklog(Storefront):
    """A carrier service-area table missing its last fifth parks about one order in six from the world's start, where
    one in a hundred is usual and customer service keeps up, under the carrier's routine 429s: the baseline (what is
    normal for each service, which an incident's own window cannot show)."""

    service_area = TRUNCATED_SERVICE_AREA

    @property
    def namespace(self) -> str:
        return FULFIL

    report = (
        "Triggered: [fulfil] orders waiting for a shipping label > 150 (production) — the label queue in orders "
        "keeps growing. Service: orders, namespace fulfil."
    )
    root_cause = (
        f"[fault_spec] component=address-validation; namespace={FULFIL} || The carrier's service-area table that "
        f"address-validation loads has {len(TRUNCATED_SERVICE_AREA)} rows in place of the usual {len(SERVICE_AREA)} "
        "(the published file is cut off, missing the highest postcodes), so address-validation rejects about one "
        "address in six as outside_service_area against the usual one in a hundred; shipping-labels parks those "
        "orders in orders, faster than customer service resolves them, so parked orders pile up in the label queue "
        "on top of the routine bursts of the carrier's peak windows. The carrier's 429s in shipping-labels' logs are "
        "its routine peak-window throttling, retried successfully, and not the cause."
    )

    def scenario(self, start: float) -> None:
        for offset in (60, 290, 520):
            self.at(start, offset, self.carrier_peak)

    def healthy(self) -> bool:
        stats = self.cluster.get("orders", FULFIL, "/stats")
        return stats is None or stats.get("awaiting_label", 0) < 150


class OrdersDropEdge(Storefront):
    """The CDN's WAF blocks the shop's Android app after a managed-ruleset update, so orders fall while every service
    is healthy: the logs convention (the CDN's request logs are a Loki stream of their own)."""

    report = (
        "Triggered: [shop] orders placed per minute 30% below forecast for 10 minutes (production). "
        "Service: checkout, namespace shop."
    )
    root_cause = (
        f"[fault_spec] component=cdn; namespace={SHOP} || The CDN's WAF began blocking (403 at the edge) every request "
        f"from the shop's Android app (user agent KestrelShop/... Android) under managed rule {APP_RULE[0]} "
        f'"{APP_RULE[1]}", so those shoppers never reach storefront-api: orders fall by about the app\'s share of '
        "traffic while every service is healthy. The blocks show only in the CDN's request logs, pushed to Loki as "
        '{source="cdn"}.'
    )

    def scenario(self, start: float) -> None:
        self.at(start, 150, lambda: self.blocked_clients.add("android"))

    def healthy(self) -> bool:
        stats = self.cluster.get("checkout", SHOP, "/stats")
        if stats is None or stats.get("orders_prev_5m", 0) < 100:
            return True
        return stats["orders_5m"] >= 0.75 * stats["orders_prev_5m"]


class CheckoutFxQuota(Storefront):
    """A feature flag made search look up an FX rate for every search, using up the FX quota checkout's pricing
    shares, during a sale that looks like the reason: the events convention (the flag provider's audit log is a Loki
    stream of its own)."""

    report = (
        "Triggered: [shop] checkout order failures (production) — checkout answered 5xx for more than 5% of "
        "POST /v1/orders over the last 10 minutes. Service: checkout, namespace shop."
    )
    # In step with the fact: the fault is the flag change and who made it, as the provider's audit log has it.
    root_cause = (
        f"[fault_spec] component=the flag provider: {FX_ACTOR}'s change turning on {FX_FLAG} (search's flag), seen "
        f"in its audit log; namespace={SHOP} || {FX_ACTOR} turned on the feature flag {FX_FLAG} in the flag "
        "provider's dashboard shortly before the failures began, with no deploy; the change is recorded only in the "
        'provider\'s audit log, the Loki stream {source="feature-flags"}. With it on, search asked fx-gateway for a '
        f"rate on every non-USD search and used up the shop's shared FX quota ({FX_PLAN_PER_MIN} requests a minute on "
        f"key {FX_KEY}), so pricing's rate refresh got 429, its rates went stale, and checkout failed non-USD orders. "
        "A diagnosis has localized the fault only when it names that change and who made it; one that infers the flag "
        "is on from search's traffic or code, without saying who turned it on, has found where the failures come from "
        "but not the change behind them. The autumn sale, pricing, fx-gateway and the FX provider are not the cause."
    )

    def scenario(self, start: float) -> None:
        sale = "Autumn sale: 20% off everything"
        self.at(
            start, 60, lambda: self.change_setting("storefront-api", "banner.text", sale, ENGINEERS[0], "sale live")
        )
        self.at(start, 70, lambda: self.change_setting("pricing", "promo.autumn_sale", True, ENGINEERS[0], "sale live"))
        self.at(start, 90, lambda: self.shoppers(3))
        flip = (FX_FLAG, True, FX_ACTOR, "show local prices during the sale")
        self.at(start, 120, lambda: self.flip_flag(*flip))

    def healthy(self) -> bool:
        stats = self.cluster.get("checkout", SHOP, "/stats")
        return stats is None or stats.get("errors_10m", 0) < 30


# ---------------------------------------------------------------------------------------------------
# The delivery platform (couriers_offline)
# ---------------------------------------------------------------------------------------------------

EATS, FLEET, PARTNERS = "eats", "fleet", "partners"
ORDERLY_HOST, SHIFTBOARD_HOST = "api.orderly.example", "api.shiftboard.example"
PAYENGINE_HOST = "api.payengine.example"
PARTNER_PORT = 18472
PARTNER_KEYS = {"orderly": "ol_live_4f1c9e2a7d", "shiftboard": "sb_larkspur_91d2", "payengine": "pe_larkspur_3b70"}
# The courier fee table the pay engine quotes from (base, per minute of pickup, in cents).
FEE_TABLE = ("ft-5c21", 300, 25)
ETA_MODEL = "eta-gbm-2026.10.3"
ROTA_FIRST_VERSION = 88207
_larkspur = random.Random(77)
_cuisines = ["thai", "pizza", "sushi", "burgers", "indian", "mexican", "salads", "pho", "korean", "bakery", "greek"]
_first = ["Golden", "Little", "Blue", "Lucky", "Old Town", "Corner", "Sunny", "Red", "Green Leaf", "Harbor", "Union"]
_second = ["Kitchen", "Garden", "Grill", "House", "Express", "Canteen", "Bistro", "Bowl", "Table", "Spoon", "Oven"]
# The restaurants and the POS each runs, all connected through Orderly: 50 on PrepPro, 70 on TillTech.
_restaurant_ids = _larkspur.sample(range(1000, 9999), 120)
RESTAURANTS = [
    {
        "id": f"r_{rid}",
        "name": f"{_larkspur.choice(_first)} {_larkspur.choice(_second)}",
        "cuisine": _larkspur.choice(_cuisines),
        "pos": "preppro" if i < 50 else "tilltech",
    }
    for i, rid in enumerate(_restaurant_ids)
]
POS = {r["id"]: r["pos"] for r in RESTAURANTS}
# Customers order most from a few restaurants and rarely from the rest.
_popularity = _larkspur.sample(RESTAURANTS, len(RESTAURANTS))
POPULARITY = [1 / (rank + 1) ** 0.8 for rank in range(len(_popularity))]
CUSTOMERS = [f"c_{_hex24('customer', i)[:12]}" for i in range(3000)]
CUSTOMER_APPS = [
    "Larkspur/12.4.0 (iPhone; iOS 18.1)",
    "Larkspur/12.4.0 (Android 15; Pixel 8)",
    "Larkspur/12.3.2 (Android 14)",
]
# Couriers who book their slots days ahead, and couriers who pick slots up in the half hour before they start.
COURIERS = [f"cr_{10000 + i}" for i in range(140)]
FLEX_COURIERS = [f"cr_{20000 + i}" for i in range(230)]
COURIER_APPS = ["LarkspurCourier/8.3.1 (Android 14; SM-A546B)", "LarkspurCourier/8.3.1 (iPhone; iOS 18.1)"]
ZONES = ["north", "central", "riverside", "east", "university"]
DISHES = ["noodles", "curry", "wrap", "bowl", "burger", "salad", "dumplings", "tacos", "pizza", "soup", "rice", "cake"]
STAFF = [f"{name}@{rest}" for name, rest in (("manager", "kitchen"), ("frontdesk", "kitchen"), ("owner", "kitchen"))]


def menu_items(restaurant: str) -> list[dict]:
    rng = random.Random(restaurant)
    return [
        {"id": f"{restaurant}_i{k}", "name": f"{rng.choice(DISHES)} #{k}", "priceCents": rng.randrange(450, 2400, 50)}
        for k in range(rng.randint(18, 30))
    ]


# Couriers' shifts around the world's start, as ShiftBoard records their bookings (`booked`): slots of 15-30 minutes
# with breaks between, which some couriers book days ahead and the rest in the half hour before the slot, so that about
# 275 couriers are on shift at any time, about 120 of them on slots booked days ahead.
def rota_schedule(start: float) -> list[dict]:
    rng = random.Random(5)
    shifts = []
    for couriers, lead in ((COURIERS, (86400, 3 * 86400)), (FLEX_COURIERS, (300, 1800))):
        for courier in couriers:
            t = start - 7200 + rng.uniform(0, 3600)
            while t < start + 14400:
                length = rng.uniform(900, 1800)
                shift = {"courier": courier, "start": round(t), "end": round(t + length), "zone": rng.choice(ZONES)}
                shifts.append({**shift, "booked": round(t - rng.uniform(*lead))})
                t += length + rng.uniform(480, 1500)
    return shifts


class PartnerApis:
    """The partners' APIs outside the cluster, served by this process and told apart by the Host
    header: Orderly's menu API (it fetches each menu from the restaurant's POS, a few at a time per POS vendor; the
    rest queue) and ShiftBoard's rota API (the rota as ShiftBoard last exported it)."""

    MENU = re.compile(r"^/v1/menus/(r_\d{4})$")

    def __init__(self, world: "Delivery") -> None:
        self.world, self.rng = world, random.Random(9)
        self.address = host_address()
        self.slots = {"preppro": threading.Semaphore(4), "tilltech": threading.Semaphore(4)}
        partners = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: object) -> None:  # noqa: A002 (the base class's signature)
                pass

            def do_GET(self) -> None:
                status, body = partners.serve(self.headers.get("Host", ""), self.path, self.headers)
                raw = json.dumps(body).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

        server = http.server.ThreadingHTTPServer(("0.0.0.0", PARTNER_PORT), Handler)  # noqa: S104 (pods reach it)
        server.daemon_threads = True
        threading.Thread(target=server.serve_forever, daemon=True).start()

    def serve(self, host: str, path: str, headers) -> tuple[int, dict]:
        host = host.split(":")[0]
        if host == SHIFTBOARD_HOST:
            if headers.get("X-Api-Key") != PARTNER_KEYS["shiftboard"]:
                return 401, {"error": "invalid api key"}
            return (200, self.world.rota()) if path.startswith("/v2/rota") else (404, {"error": "not found"})
        if host == PAYENGINE_HOST:
            if headers.get("Authorization") != f"Bearer {PARTNER_KEYS['payengine']}":
                return 401, {"error": "unauthorized"}
            query = urllib.parse.parse_qs(urllib.parse.urlsplit(path).query)
            table, base, per_minute = FEE_TABLE
            pickup_s = float(query.get("pickupEtaS", ["300"])[0])
            return 200, {"feeCents": round(base + per_minute * pickup_s / 60), "feeTable": table, "currency": "USD"}
        if host != ORDERLY_HOST:
            return 404, {"error": "unknown host"}
        if headers.get("Authorization") != f"Bearer {PARTNER_KEYS['orderly']}":
            return 401, {"error": "unauthorized"}
        match = self.MENU.match(path)
        if match is None or match[1] not in POS:
            return 404, {"error": "no such menu"}
        pos = POS[match[1]]
        with self.slots[pos]:
            time.sleep(min(8.0, max(0.4, self.rng.lognormvariate(0.59, 0.5))))
            if pos == "tilltech" and self.rng.random() < 0.01:
                return 503, {"error": "upstream unavailable"}
        return 200, self.world.partner_menu(match[1])


class Delivery(KnowledgeProblem):
    namespaces = (EATS, FLEET, PARTNERS, PLATFORM)
    services = (
        Svc("billing", EATS, {}),
        Svc("deliveries", EATS, {}),
        Svc("eta", PLATFORM, {"MODEL_VERSION": ETA_MODEL}, cpu_limit="300m"),
        Svc("menu", EATS, {"POS_HUB_URL": url("pos-hub", PARTNERS), "MENU_CACHE_TTL_S": "150"}),
        Svc("menu-warmer", EATS, {"MENU_URL": url("menu", EATS), "WARM_EVERY_S": "45"}),
        Svc(
            "restaurant-portal", PARTNERS, {"ETA_URL": url("eta", PLATFORM), "DELIVERIES_URL": url("deliveries", EATS)}
        ),
        Svc(
            "pos-hub",
            PARTNERS,
            {
                "DELIVERIES_URL": url("deliveries", EATS),
                "MENU_URL": url("menu", EATS),
                "RESTAURANT_PORTAL_URL": url("restaurant-portal", PARTNERS),
                "ORDERLY_API_URL": f"http://{ORDERLY_HOST}:{PARTNER_PORT}",
                "ORDERLY_API_KEY": PARTNER_KEYS["orderly"],
            },
        ),
        Svc(
            "app-api",
            EATS,
            {
                "MENU_URL": url("menu", EATS),
                "ETA_URL": url("eta", PLATFORM),
                "BILLING_URL": url("billing", EATS),
                "DELIVERIES_URL": url("deliveries", EATS),
            },
        ),
        Svc("courier-api", FLEET, {"DELIVERIES_URL": url("deliveries", EATS)}),
        Svc(
            "dispatch",
            FLEET,
            {
                "DELIVERIES_URL": url("deliveries", EATS),
                "COURIER_API_URL": url("courier-api", FLEET),
                "ETA_URL": url("eta", PLATFORM),
                "PAYENGINE_URL": f"http://{PAYENGINE_HOST}:{PARTNER_PORT}",
                "PAYENGINE_API_KEY": PARTNER_KEYS["payengine"],
            },
        ),
        Svc(
            "rota-sync",
            FLEET,
            {
                "SHIFTBOARD_URL": f"http://{SHIFTBOARD_HOST}:{PARTNER_PORT}",
                "SHIFTBOARD_API_KEY": PARTNER_KEYS["shiftboard"],
                "COURIER_API_URL": url("courier-api", FLEET),
                "ROTA_POLL_S": "120",
            },
        ),
    )
    app = AppInfo(
        "delivery",
        "Larkspur, a food delivery platform: app-api serves the customer app and menu its restaurants' menus, deliveries "
        "tracks each order, pos-hub and restaurant-portal serve the restaurants, dispatch and courier-api assign orders "
        "to couriers, and eta estimates the times they are quoted.",
    )
    TRACING = Storefront.TRACING
    # What the incident's world changes; its steady world, which discovery learns, changes none of them.
    rota_frozen = False

    def _deployed(self, incident: bool) -> tuple[Svc, ...]:
        aliases = {
            "pos-hub": ((self.partners.address, ORDERLY_HOST),),
            "rota-sync": ((self.partners.address, SHIFTBOARD_HOST),),
            "dispatch": ((self.partners.address, PAYENGINE_HOST),),
        }
        return tuple(
            Svc(
                svc.name,
                svc.namespace,
                {**svc.env, **self.TRACING, "OTEL_SERVICE_NAME": svc.name},
                aliases.get(svc.name, ()),
                svc.replicas,
                svc.cpu_limit,
            )
            for svc in self.services
        )

    # The world's state before its services start: rota-sync fetches the rota as soon as it runs.
    def _prepare(self, incident: bool) -> None:
        self.start = time.time()
        self.schedule = rota_schedule(self.start)
        self.menu_versions = {r["id"]: self.rng.randint(12, 60) for r in RESTAURANTS}
        self.rota_version = ROTA_FIRST_VERSION
        self.frozen = incident and self.rota_frozen
        # The items each restaurant's kitchen has run out of, as its POS knows them.
        self.sold_out: dict[str, set[str]] = {r["id"]: set() for r in RESTAURANTS}
        self.published: dict | None = None
        if not self.frozen:
            self.publish_rota(self.start)
        self.partners = PartnerApis(self)

    def inject_fault(self) -> None:
        self._prepare(incident=True)
        self.cluster.deploy(self.namespaces, self._deployed(incident=True))
        self.scenario(self.background(incident=True))

    def inject_steady(self) -> None:
        self._prepare(incident=False)
        self.cluster.deploy(self.namespaces, self._deployed(incident=False))
        self.background(incident=False)

    def background(self, incident: bool) -> float:
        self.lock = threading.Lock()
        self.app_api = self.cluster.forward("app-api", EATS)
        self.hub = self.cluster.forward("pos-hub", PARTNERS)
        self.portal = self.cluster.forward("restaurant-portal", PARTNERS)
        self.couriers = self.cluster.forward("courier-api", FLEET)
        seeds = {
            ("app-api", EATS): {"restaurants": [{k: r[k] for k in ("id", "name", "cuisine")} for r in RESTAURANTS]},
            ("pos-hub", PARTNERS): {"restaurants": POS},
            ("menu-warmer", EATS): {"restaurants": [{"id": r["id"], "open": True} for r in RESTAURANTS]},
        }
        for (svc, ns), seed in seeds.items():
            status, _ = http_json("POST", f"{self.cluster.forward(svc, ns)}/internal/seed", seed)
            if status != 200:
                raise RuntimeError(f"seeding {ns}/{svc} failed: {status}")
        self.ready_at: dict[str, float] = {}
        self.answered: set[str] = set()
        self.going_online: set[str] = set()
        self.online: list[str] = []
        self.customers(12)
        self.every(10, 20, self.menu_edit)
        self.every(2, 4, self.availability_change)
        self.every(15, 30, self.staff_dashboard)
        self.every(4, 6, self.courier_apps)
        self.every(2, 3, self.pings)
        self.every(1.5, 2.5, self.answer_offers)
        if not self.frozen:
            self.every(60, 90, lambda: self.publish_rota(time.time()))
        return time.time()

    # The rota as ShiftBoard exports it: a new version with each round of bookings, holding the shifts booked by then
    # that start from 90 minutes before to four hours after it.
    def publish_rota(self, at: float) -> None:
        shifts = [
            {k: v for k, v in s.items() if k != "booked"}
            for s in self.schedule
            if s["booked"] <= at and at - 5400 <= s["start"] <= at + 14400
        ]
        published = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(at))
        self.published = {"version": str(self.rota_version), "publishedAt": published, "shifts": shifts}
        self.rota_version += 1

    # What ShiftBoard serves. A frozen rota is the export it made 90 minutes before rota-sync first asks: it holds
    # the shifts booked by then, and none booked since; the slots in it that were booked in the half hour before they
    # start have all ended by the time rota-sync asks, so the couriers on shift hold steady at those who book ahead.
    def rota(self) -> dict:
        if self.published is None:
            self.publish_rota(time.time() - 5400)
        return self.published

    def partner_menu(self, restaurant: str) -> dict:
        version = f"{restaurant}-v{self.menu_versions[restaurant]}"
        return {"restaurant": restaurant, "menuVersion": version, "items": menu_items(restaurant)}

    def customers(self, n: int) -> None:
        for _ in range(n):
            self.every(1.0, 2.5, self.visit)

    # One customer browsing the app: the restaurants near them, a few menus, and sometimes an order of a dish the menu
    # shows as available, most often one of the restaurant's popular dishes.
    def visit(self) -> None:
        customer = self.rng.choice(CUSTOMERS)
        headers = {"X-Customer": customer, "User-Agent": self.rng.choice(CUSTOMER_APPS)}
        if self.rng.random() < 0.3:
            http_json("GET", f"{self.app_api}/v1/restaurants", headers=headers)
        restaurant, menu = None, {}
        for _ in range(self.rng.randint(1, 3)):
            restaurant = self.rng.choices(_popularity, weights=POPULARITY)[0]["id"]
            _, menu = http_json("GET", f"{self.app_api}/v1/restaurants/{restaurant}/menu", headers=headers)
        items = [(k, i["id"]) for k, i in enumerate(menu.get("items", [])) if i.get("available", True)]
        if restaurant and items and self.rng.random() < 0.11:
            item = self.rng.choices([i for _, i in items], weights=[4 if k < 6 else 1 for k, _ in items])[0]
            order = {
                "customer": customer,
                "restaurant": restaurant,
                "items": [item],
                "amountCents": self.rng.randrange(1500, 6000, 10),
            }
            status, placed = http_json("POST", f"{self.app_api}/v1/orders", order, headers)
            if status == 201:
                self.restaurant_takes(placed["orderId"], restaurant, item)

    # The restaurant's POS accepts the order (or rejects it: the dish has run out, or, rarely, the kitchen is too busy)
    # and marks it ready when cooked.
    def restaurant_takes(self, order: str, restaurant: str, item: str) -> None:
        pos, now = POS[restaurant], time.time()
        event = {"orderId": order, "restaurant": restaurant}
        reason = (
            "item unavailable"
            if item in self.sold_out[restaurant]
            else "kitchen too busy"
            if self.rng.random() < 0.02
            else None
        )
        if reason:
            rejected = {**event, "event": "order.rejected", "reason": reason, "item": item}
            self.at(now, self.rng.uniform(20, 60), lambda: self.webhook(pos, rejected))
            return
        # Most kitchens accept within a minute; some take a few, a tablet left unattended for longer.
        accepted = min(600.0, max(5.0, self.rng.lognormvariate(3.7, 0.9)))
        ready = self.rng.uniform(60, 120)
        self.ready_at[order] = now + accepted + ready
        self.at(now, accepted, lambda: self.webhook(pos, {**event, "event": "order.accepted"}))
        self.at(now, accepted + ready, lambda: self.webhook(pos, {**event, "event": "order.ready"}))

    # Orderly delivers the restaurant's POS event to pos-hub as a webhook.
    def webhook(self, pos: str, event: dict) -> None:
        headers = {"User-Agent": "Orderly-Webhooks/4.1"}
        http_json("POST", f"{self.hub}/webhooks/orderly", {**event, "pos": pos}, headers)

    # A restaurant edits its menu in its POS: a new version, and a menu.updated webhook.
    def menu_edit(self) -> None:
        restaurant = self.rng.choice(RESTAURANTS)["id"]
        self.menu_versions[restaurant] += 1
        self.menu_updated(restaurant)

    def menu_updated(self, restaurant: str) -> None:
        version = f"{restaurant}-v{self.menu_versions[restaurant]}"
        self.webhook(POS[restaurant], {"event": "menu.updated", "restaurant": restaurant, "menuVersion": version})

    # A kitchen runs out of one of its popular dishes, marks it sold out in its POS, and restocks it 4-15 minutes later.
    # Orderly sends each change to pos-hub as an item.availability webhook, unless its availability feed is down.
    def availability_change(self) -> None:
        restaurant = self.rng.choices(_popularity, weights=POPULARITY)[0]["id"]
        item = self.rng.choice(menu_items(restaurant)[:6])["id"]
        if item in self.sold_out[restaurant]:
            return
        self.stock(restaurant, item, available=False)
        self.at(time.time(), self.rng.uniform(240, 900), lambda: self.stock(restaurant, item, available=True))

    def stock(self, restaurant: str, item: str, available: bool) -> None:
        (self.sold_out[restaurant].discard if available else self.sold_out[restaurant].add)(item)
        event = {"event": "item.availability", "restaurant": restaurant, "item": item, "available": available}
        self.webhook(POS[restaurant], event)

    def staff_dashboard(self) -> None:
        restaurant = self.rng.choice(RESTAURANTS)["id"]
        headers = {"X-Staff": f"{self.rng.choice(STAFF)}-{restaurant[2:]}.example"}
        http_json("GET", f"{self.portal}/v1/dashboard/{restaurant}", headers=headers)

    # The courier apps: a courier whose shift has opened goes online within a minute; online couriers send their
    # location, and answer offers (most accept, some decline, a few let them expire).
    def courier_apps(self) -> None:
        status, body = http_json("GET", f"{self.couriers}/v1/shifts/open")
        if status != 200:
            return
        self.online = body["online"]
        for courier in set(body["couriers"]) - set(body["online"]) - self.going_online:
            self.going_online.add(courier)
            self.at(time.time(), self.rng.uniform(5, 60), lambda c=courier: self.go_online(c))

    def go_online(self, courier: str) -> None:
        headers = {"User-Agent": COURIER_APPS[hash(courier) % len(COURIER_APPS)]}
        http_json("POST", f"{self.couriers}/v1/couriers/{courier}/online", {}, headers)
        self.going_online.discard(courier)

    def pings(self) -> None:
        for courier in self.rng.sample(self.online, min(6, len(self.online))):
            http_json("POST", f"{self.couriers}/v1/couriers/{courier}/location", {"lat": 51.5, "lng": -0.1})

    def answer_offers(self) -> None:
        status, body = http_json("GET", f"{self.couriers}/v1/offers")
        for offer in body.get("offers", []) if status == 200 else []:
            if offer["offerId"] in self.answered:
                continue
            self.answered.add(offer["offerId"])
            # Most couriers take an offer that pays what they expect for the trip, about $3.50 or more, and few take
            # one that pays less.
            fee = offer.get("feeCents", 500)
            accept = 0.9 if fee >= 350 else 0.3
            roll, now = self.rng.random(), time.time()
            if roll < accept:
                self.at(now, self.rng.uniform(3, 10), lambda o=offer: self.deliver(o))
            elif roll < accept + (1 - accept) * 0.6:
                reason = self.rng.choice(["too far", "vehicle issue", "on a break"])
                decline = f"{self.couriers}/v1/offers/{offer['offerId']}/decline"
                self.at(now, self.rng.uniform(2, 6), lambda u=decline, r=reason: http_json("POST", u, {"reason": r}))

    def deliver(self, offer: dict) -> None:
        status, _ = http_json("POST", f"{self.couriers}/v1/offers/{offer['offerId']}/accept", {})
        if status != 200:
            return
        now = time.time()
        pickup = max(self.ready_at.pop(offer["order"], now), now) + self.rng.uniform(30, 90)
        drop = pickup + self.rng.uniform(60, 150)
        step = f"{self.couriers}/v1/deliveries/{offer['order']}"
        body = {"courier": offer["courier"]}
        self.at(now, pickup - now, lambda: http_json("POST", f"{step}/picked-up", body))
        self.at(now, drop - now, lambda: http_json("POST", f"{step}/delivered", body))


class CouriersOffline(Delivery):
    """ShiftBoard's rota export stopped 90 minutes before the incident: rota-sync gets the same rota on every fetch,
    which holds the slots couriers booked days ahead but none of the many booked since, so far fewer couriers are on
    shift than usual and orders wait for one. That the rota normally changes every couple of minutes, and how many
    couriers are normally on shift, is in rota-sync's and courier-api's baselines."""

    rota_frozen = True

    @property
    def namespace(self) -> str:
        return FLEET

    report = (
        "Triggered: [fleet] orders waiting for a courier > 60 for 5 minutes (production). "
        "Service: dispatch, namespace fleet."
    )
    # What the grader compares an answer against: the fault is in ShiftBoard's feed, both stale and far smaller.
    root_cause = (
        f"[fault_spec] component=ShiftBoard's rota export, as rota-sync fetches it; namespace={FLEET} || ShiftBoard "
        "keeps serving the rota export it made 90 minutes before the incident: the same version, with the same shifts, "
        "on every fetch, where a new version normally comes every couple of minutes as couriers book slots. The slots "
        "booked since are missing and never open, so the rota holds far fewer shifts than usual (about 100 open where "
        "about 270 usually are), and orders wait for a courier. The fault is in ShiftBoard's rota feed, which is stale "
        "and so far smaller than usual: a diagnosis localizes it by placing the fault in that feed. Demand, dispatch and "
        "the couriers are not the cause, and rota-sync and courier-api work correctly on the rota they are given."
    )

    def scenario(self, start: float) -> None:
        pass

    def healthy(self) -> bool:
        stats = self.cluster.get("dispatch", FLEET, "/stats")
        return stats is None or stats.get("waiting", 0) < 60


PROBLEMS: dict[str, type[KnowledgeProblem]] = {
    "support_handoff_failed": SupportHandoff,
    "meeting_runner_dlq": MeetingRunnerDlq,
    "payments_core_5xx": PaymentsCore5xx,
    "label_backlog": LabelBacklog,
    "orders_drop_edge": OrdersDropEdge,
    "checkout_fx_quota": CheckoutFxQuota,
    "couriers_offline": CouriersOffline,
}


def namespaces(problem_id: str) -> list[str]:
    return list(PROBLEMS[problem_id].namespaces)
