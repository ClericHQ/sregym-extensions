"""What every service here shares: structured logging, HTTP between services, and the request routing.

A service is a module beside this one that defines a Service subclass and runs `serve(TheService)`; PORT is
its HTTP port. Each service loads its records through POST /internal/seed.
"""

# Plain HTTP between in-cluster services, each listening on its pod's interfaces.
# ruff: noqa: S310, S104

import json
import os
import random
import re
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = int(os.environ.get("PORT", "8080"))
POD = os.environ.get("HOSTNAME", "")
_out = threading.Lock()


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _emit(line: str) -> None:
    with _out:
        sys.stdout.write(line + "\n")
        sys.stdout.flush()


def log_json(level: str, message: str, **fields: object) -> None:
    _emit(json.dumps({"timestamp": _now(), "level": level, "message": message, **fields}, separators=(",", ":")))


def log_text(level: str, text: str) -> None:
    _emit(f"{_now()} {level.upper():5} {text}")


def log_logfmt(level: str, message: str, **fields: object) -> None:
    pairs = " ".join(f"{k}={json.dumps(v) if isinstance(v, str) and ' ' in v else v}" for k, v in fields.items())
    _emit(f'time={_now()} level={level} msg="{message}" {pairs}')


def call(
    method: str,
    url: str,
    body: object = None,
    headers: dict[str, str] | None = None,
    timeout: float = 5,
    peer: str | None = None,
):
    started = time.time()
    status, result = _call(method, url, body, headers, timeout)
    _tracer.client_span(method, url, status, started, peer)
    return status, result


def _call(method: str, url: str, body: object, headers: dict[str, str] | None, timeout: float):
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    for key, value in (headers or {}).items():
        request.add_header(key, value)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
            return response.status, json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, json.loads(raw) if raw else {}
        except ValueError:
            return e.code, {"raw": raw.decode(errors="replace")}
    except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
        return 599, {"error": str(e)}


class _Tracer:
    """Sampled OpenTelemetry client spans for outgoing calls, exported over OTLP/HTTP (OTEL_EXPORTER_OTLP_ENDPOINT)
    as OTEL_SERVICE_NAME; off without an endpoint. Each span names the service called (peer.service)."""

    def __init__(self) -> None:
        self.endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "")
        self.service = os.environ.get("OTEL_SERVICE_NAME", "")
        self.ratio = float(os.environ.get("OTEL_TRACES_SAMPLER_ARG", "0.1"))
        self.lock = threading.Lock()
        self.spans: list[dict] = []
        if self.endpoint:
            threading.Thread(target=self._export, daemon=True).start()

    def client_span(self, method: str, url: str, status: int, started: float, peer: str | None) -> None:
        if not self.endpoint or random.random() >= self.ratio:
            return
        parts = urllib.parse.urlsplit(url)
        host = parts.hostname or ""
        peer = peer or host.split(".")[0]
        attributes = {
            "peer.service": peer,
            "http.method": method,
            "http.status_code": status,
            "net.peer.name": host,
            "net.peer.port": parts.port or 80,
        }
        span = {
            "traceId": os.urandom(16).hex(),
            "spanId": os.urandom(8).hex(),
            "name": f"{method} {peer}",
            "kind": 3,
            "startTimeUnixNano": str(int(started * 1e9)),
            "endTimeUnixNano": str(int(time.time() * 1e9)),
            "attributes": [{"key": k, "value": _otlp_value(v)} for k, v in attributes.items()],
            "status": {"code": 2 if status >= 500 else 1},
        }
        with self.lock:
            self.spans.append(span)

    def _export(self) -> None:
        resource = {"attributes": [{"key": "service.name", "value": {"stringValue": self.service}}]}
        while True:
            time.sleep(5)
            with self.lock:
                spans, self.spans = self.spans, []
            if spans:
                body = {"resourceSpans": [{"resource": resource, "scopeSpans": [{"spans": spans}]}]}
                _call("POST", f"{self.endpoint}/v1/traces", body, None, 5)


def _otlp_value(value: object) -> dict:
    return {"intValue": str(value)} if isinstance(value, int) else {"stringValue": str(value)}


_tracer = _Tracer()


def resolve(name: str) -> tuple[str, str]:
    """A logical service name's address from the service registry (REGISTRY_URL): (base URL, the service's own name)."""
    while True:
        status, body = _call("GET", f"{os.environ['REGISTRY_URL']}/v1/resolve/{name}", None, None, 5)
        if status == 200:
            return f"http://{body['address']}", body["service"]
        time.sleep(2)


class Flags:
    """This service's feature flags, as the flag provider's relay (FLAG_RELAY_URL) serves them; defaults until the
    first poll, and without a relay."""

    def __init__(self, service: str, defaults: dict[str, bool], every_s: float = 5) -> None:
        self.url = os.environ.get("FLAG_RELAY_URL", "")
        self.service, self.every_s = service, every_s
        self.values = dict(defaults)
        if self.url:
            threading.Thread(target=self._poll, daemon=True).start()

    def enabled(self, flag: str) -> bool:
        return bool(self.values.get(flag))

    def _poll(self) -> None:
        while True:
            status, body = _call("GET", f"{self.url}/sdk/latest-flags?service={self.service}", None, None, 5)
            if status == 200:
                self.values = {**self.values, **body.get("flags", {})}
            time.sleep(self.every_s)


def ulid() -> str:
    alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
    stamp, chars = int(time.time() * 1000), []
    for _ in range(10):
        chars.append(alphabet[stamp % 32])
        stamp //= 32
    return "".join(reversed(chars)) + "".join(random.choice(alphabet) for _ in range(16))


Handler = Callable[["Request", re.Match], "tuple[int, object] | None"]


class Service:
    """A service's routes: (method, path pattern, handler(request, match) -> (status, body) or None if it streamed)."""

    def __init__(self) -> None:
        self.routes: list[tuple[str, re.Pattern, Handler]] = []
        self.state: dict = {}
        self.route("POST", r"/internal/seed", self._seed)
        self.route("GET", r"/healthz", lambda r, m: (200, {"ok": True}))

    def route(self, method: str, pattern: str, handler: Handler) -> None:
        self.routes.append((method, re.compile(f"^{pattern}$"), handler))

    def _seed(self, request: "Request", match: re.Match):
        records = request.json()
        self.state.update(records)
        self.seeded()
        return 200, {"seeded": sorted(records)}

    def seeded(self) -> None:
        pass

    def start(self) -> None:
        pass


class Request(BaseHTTPRequestHandler):
    service: Service
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002 (BaseHTTPRequestHandler's signature)
        pass

    def json(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        return json.loads(self.rfile.read(length)) if length else {}

    @property
    def path_only(self) -> str:
        return urllib.parse.urlsplit(self.path).path

    @property
    def query(self) -> dict[str, str]:
        return {k: v[0] for k, v in urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query).items()}

    def send_json(self, status: int, body: object) -> None:
        raw = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _dispatch(self, method: str) -> None:
        for route_method, pattern, handler in self.service.routes:
            if route_method == method and (match := pattern.match(self.path_only)):
                result = handler(self, match)
                if result is not None:
                    self.send_json(*result)
                return
        self.send_json(404, {"error": "not found"})

    def do_GET(self) -> None:
        self._dispatch("GET")

    def do_POST(self) -> None:
        self._dispatch("POST")


class Settings:
    """This service's runtime settings, pulled from the settings service (SETTINGS_URL) every few seconds.

    Until the first pull, and without SETTINGS_URL, a service runs on its defaults.
    """

    def __init__(self, service: str, defaults: dict, every_s: float = 10) -> None:
        self.url = os.environ.get("SETTINGS_URL", "")
        self.service, self.every_s = service, every_s
        self.values = dict(defaults)
        if self.url:
            threading.Thread(target=self._poll, daemon=True).start()

    def get(self, key: str):
        return self.values.get(key)

    def _poll(self) -> None:
        while True:
            status, body = call("GET", f"{self.url}/v1/settings/{self.service}")
            if status == 200:
                self.values = {**self.values, **body.get("values", {})}
            time.sleep(self.every_s)


def agent_channel(company: str, agent: str) -> str:
    return f"{company}_{agent}_agent_events-{agent}"


def serve(service_class: type[Service]) -> None:
    service = service_class()
    handler = type("Handler", (Request,), {"service": service})
    server = ThreadingHTTPServer(("0.0.0.0", PORT), handler)
    server.daemon_threads = True
    service.start()
    log_text("info", f"{service_class.__name__} listening on :{PORT}")
    server.serve_forever()
