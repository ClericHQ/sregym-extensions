#!/usr/bin/env python3
"""Run Cleric's knowledge scenarios on a Kubernetes cluster from this machine.

Each scenario is an incident in one of four small worlds (a support desk, a workspace suite, an online shop, a food
delivery platform). A world first runs its everyday traffic, healthy, so Cleric can learn it; then each incident
is started on its own, investigated, and stopped. This process plays everything outside the cluster (the users, the
partners, the CDN), so it must keep running while a world or incident is up, and it must not run where Cleric can
read it: it holds the answer key.

    python3 scenario.py list
    python3 scenario.py steady storefront       # deploy a world with its healthy traffic, for discovery
    python3 scenario.py start checkout_fx_quota # start an incident; prints the alert to give Cleric
    python3 scenario.py status
    python3 scenario.py stop                    # take down whatever is running

Needs python3 (3.11+) and kubectl with an admin context on the cluster. Settings, from the environment:
SCENARIO_HOST_ADDRESS (an address of this machine the cluster's pods can reach; partners are served from here),
SCENARIO_LOKI (the Loki service the CDN and flag-provider logs are pushed to, default observe/loki:3100),
SCENARIO_OTLP_ENDPOINT (where services send traces; empty sends none) and SCENARIO_IMAGE (the services' Python
image, default mirror.gcr.io/library/python:3.12-slim).
"""

import json
import os
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE / "driver"))

STATE = HERE / "state"
# Each world: the problem whose steady traffic is the world's everyday traffic, and its incidents.
WORLDS = {
    "support-desk": ("support_handoff_failed", ["support_handoff_failed"]),
    "workspace-suite": ("meeting_runner_dlq", ["payments_core_5xx"]),
    "storefront": ("label_backlog", ["orders_drop_edge", "checkout_fx_quota"]),
    "delivery": ("couriers_offline", ["couriers_offline"]),
}
INCIDENTS = {problem: world for world, (_, problems) in WORLDS.items() for problem in problems}
# How long an incident may take to reach its alert condition, and the world to come back down.
ORACLE_TIMEOUT_S = 600
START_TIMEOUT_S = 900


def _active() -> dict | None:
    path = STATE / "active.json"
    if not path.exists():
        return None
    active = json.loads(path.read_text())
    try:
        os.kill(active["pid"], 0)
    except ProcessLookupError:
        return {**active, "dead": True}
    return active


def _say(message: str) -> None:
    print(f"{time.strftime('%H:%M:%S')} {message}", flush=True)


def _serve(problem: str, control: Path, steady: bool) -> None:
    from knowledge_faults import PROBLEMS  # shipped beside this script, in driver/

    world = PROBLEMS[problem]()
    started = time.time()
    _say(f"deploying {', '.join(world.namespaces)}")
    (world.inject_steady if steady else world.inject_fault)()
    _say("deployed; traffic running")
    live = False
    if not steady:
        while time.time() - started < ORACLE_TIMEOUT_S and not live:
            live = not world.mitigation_oracle.evaluate()["success"]
            if not live:
                time.sleep(5)
        _say(f"alert condition {'holds' if live else 'never held'} after {time.time() - started:.0f}s")
    truth = {
        "problem": problem,
        "steady": steady,
        "report": None if steady else world.report,
        "root_cause": "" if steady else world.root_cause,
        "namespaces": list(world.namespaces),
        "fault_live": live,
        "started_at": started,
        "inject_s": round(time.time() - started, 1),
    }
    (control / "fault.json").write_text(json.dumps(truth, indent=2))
    while not (control / "recover").exists():
        time.sleep(1)
    world.recover_fault()
    _say("taken down")
    (control / "recovered.json").write_text(json.dumps({"recovered_at": time.time()}))


def _launch(problem: str, label: str, steady: bool) -> tuple[dict, Path]:
    if (active := _active()) and not active.get("dead"):
        sys.exit(f"{active['name']} is still running: run `scenario.py stop` first")
    control = STATE / f"{label}-{int(time.time())}"
    control.mkdir(parents=True)
    log = (control / "driver.log").open("w")
    args = [sys.executable, "-u", str(Path(__file__).resolve()), "_serve", problem, str(control)]
    proc = subprocess.Popen(
        [*args, *(["--steady"] if steady else [])],
        stdout=log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    (STATE / "active.json").write_text(json.dumps({"name": label, "pid": proc.pid, "control": str(control)}))
    started = time.time()
    deadline, said = started + START_TIMEOUT_S, started
    waiting = "deploying the world" if steady else "deploying the world and waiting for the alert condition"
    print(f"{waiting} (up to {START_TIMEOUT_S // 60} min); progress in {control / 'driver.log'}", flush=True)
    while not (control / "fault.json").exists():
        if proc.poll() is not None:
            sys.exit(f"the driver exited before {label} was up; see {control / 'driver.log'}")
        if time.time() > deadline:
            sys.exit(f"{label} was not up after {START_TIMEOUT_S}s; see {control / 'driver.log'}")
        if time.time() - said >= 30:
            said = time.time()
            print(f"  still {waiting.split(' and ')[-1]}: {said - started:.0f}s", flush=True)
        time.sleep(2)
    return json.loads((control / "fault.json").read_text()), control


def steady(world: str) -> None:
    if world not in WORLDS:
        sys.exit(f"unknown world {world!r}: one of {', '.join(WORLDS)}")
    truth, _ = _launch(WORLDS[world][0], f"steady-{world}", steady=True)
    print(f"{world} is up with its everyday traffic, namespaces {', '.join(truth['namespaces'])}.")
    print("Leave it running while Cleric's discovery learns it, then `scenario.py stop`.")


def start(problem: str) -> None:
    if problem not in INCIDENTS:
        sys.exit(f"unknown scenario {problem!r}: one of {', '.join(INCIDENTS)}")
    truth, control = _launch(problem, problem, steady=False)
    if not truth["fault_live"]:
        print(f"warning: {problem}'s alert condition did not hold within {ORACLE_TIMEOUT_S}s", file=sys.stderr)
    print(f"{problem} is live. Give Cleric this alert, word for word:\n")
    print(truth["report"])
    print(f"\nThe answer key is in {control / 'fault.json'}; keep it away from Cleric.")


def stop() -> None:
    active = _active()
    if active is None:
        print("nothing is running")
        return
    control = Path(active["control"])
    if not active.get("dead"):
        (control / "recover").touch()
        deadline = time.time() + START_TIMEOUT_S
        while not (control / "recovered.json").exists() and time.time() < deadline:
            time.sleep(2)
    (STATE / "active.json").unlink()
    print(f"{active['name']} stopped" + (" (its driver had already exited)" if active.get("dead") else ""))


def status() -> None:
    active = _active()
    if active is None:
        print("nothing is running")
        return
    state = "driver exited" if active.get("dead") else "running"
    print(f"{active['name']}: {state}, control {active['control']}")


def main(argv: list[str]) -> None:
    if not argv or argv[0] in ("-h", "--help", "help"):
        print(__doc__)
        return
    command, rest = argv[0], argv[1:]
    if command == "list":
        for world, (_, problems) in WORLDS.items():
            print(f"{world:16} {', '.join(problems)}")
    elif command == "steady" and len(rest) == 1:
        steady(rest[0])
    elif command == "start" and len(rest) == 1:
        start(rest[0])
    elif command == "stop":
        stop()
    elif command == "status":
        status()
    elif command == "_serve":
        _serve(rest[0], Path(rest[1]), "--steady" in rest)
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main(sys.argv[1:])
