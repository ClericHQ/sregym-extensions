import os
import threading
import time

from common import Service, call, log_json, serve

SHIFTBOARD = os.environ["SHIFTBOARD_URL"]
SHIFTBOARD_KEY = os.environ["SHIFTBOARD_API_KEY"]
COURIERS = os.environ["COURIER_API_URL"]
POLL_S = float(os.environ.get("ROTA_POLL_S", "120"))
OPEN_AHEAD_S = 300


class RotaSync(Service):
    """Brings the courier rota from ShiftBoard, where Larkspur's fleet planners schedule couriers' shifts, into
    courier-api: every ROTA_POLL_S it fetches the published rota, opens the shifts starting within the next five
    minutes and closes the shifts that have ended."""

    def __init__(self) -> None:
        super().__init__()
        self.version: str | None = None
        self.rota: list[dict] = []
        self.opened: dict[str, float] = {}

    def start(self) -> None:
        threading.Thread(target=self._run, daemon=True).start()

    # A sync that could not reach courier-api is retried shortly rather than at the next poll.
    def _run(self) -> None:
        while True:
            time.sleep(POLL_S if self._sync() else 10)

    def _sync(self) -> bool:
        status, body = call("GET", f"{SHIFTBOARD}/v2/rota", headers={"X-Api-Key": SHIFTBOARD_KEY}, timeout=15)
        if status != 200:
            log_json("warn", "rota fetch failed", status=status)
        else:
            if body["version"] != self.version:
                log_json(
                    "info", "rota updated", version=body["version"], previous=self.version, shifts=len(body["shifts"])
                )
            self.version, self.rota = body["version"], body["shifts"]
        now = time.time()
        due = [
            s
            for s in self.rota
            if s["start"] <= now + OPEN_AHEAD_S and s["end"] > now and self.opened.get(s["courier"]) != s["end"]
        ]
        ended = [c for c, end in self.opened.items() if end <= now]
        if not due and not ended:
            log_json("info", "shifts synced", opened=0, closed=0, open=len(self.opened))
            return True
        body = {"open": due, "close": ended}
        status, _ = call("POST", f"{COURIERS}/internal/shifts", body, {"X-Client": "rota-sync"})
        if status != 200:
            log_json("warn", "shift sync failed", status=status)
            return False
        for courier in ended:
            del self.opened[courier]
        for shift in due:
            self.opened[shift["courier"]] = shift["end"]
        log_json("info", "shifts synced", opened=len(due), closed=len(ended), open=len(self.opened))
        return True


if __name__ == "__main__":
    serve(RotaSync)
