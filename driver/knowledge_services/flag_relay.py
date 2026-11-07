import threading
import time

from common import Request, Service, log_json, serve


class FlagRelay(Service):
    """Relay proxy for the feature-flag provider: holds the production environment's flags, kept current by the
    provider's streaming connection, and serves each service the flags it evaluates. Flags are changed in the
    provider's dashboard, not here."""

    def __init__(self) -> None:
        super().__init__()
        self.lock = threading.Lock()
        self.flags: dict[str, dict] = {}
        self.route("GET", r"/sdk/latest-flags", self.latest)
        self.route("POST", r"/internal/stream", self.stream)

    def start(self) -> None:
        threading.Thread(target=self._heartbeat, daemon=True).start()

    def _heartbeat(self) -> None:
        while True:
            time.sleep(60)
            with self.lock:
                count = len(self.flags)
            log_json("info", "stream healthy", environment="production", flags=count)

    # The provider's stream: one flag's full state after a change in its dashboard.
    def stream(self, request: Request, match):
        flag = request.json()
        with self.lock:
            self.flags[flag["key"]] = flag
        return 200, {}

    def latest(self, request: Request, match):
        service = request.query.get("service", "")
        with self.lock:
            flags = {key: f["on"] for key, f in self.flags.items() if service in f.get("services", [])}
        return 200, {"flags": flags}


if __name__ == "__main__":
    serve(FlagRelay)
