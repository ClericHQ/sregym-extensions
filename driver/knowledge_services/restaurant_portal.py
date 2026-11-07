import os
import threading
import time

from common import Request, Service, call, log_json, serve

ETA = os.environ["ETA_URL"]
WINDOWS = ("now", "+15m", "+30m", "+45m", "+60m", "+90m")


class RestaurantPortal(Service):
    """The restaurant dashboard's backend. It shows each restaurant the prep times customers are quoted, which the
    ETA model estimates per time window from the restaurant's current menu: when a restaurant's menu changes
    (menu.updated from pos-hub), its prep times are refreshed. A restaurant waiting for a refresh is refreshed once,
    however many updates arrive meanwhile."""

    WORKERS = 12

    def __init__(self) -> None:
        super().__init__()
        self.lock = threading.Condition()
        self.pending: dict[str, str | None] = {}
        self.minute = {"refreshes": 0, "dashboards": 0}
        self.route("POST", r"/internal/menu-updated", self.menu_updated)
        self.route("GET", r"/v1/dashboard/(?P<id>[^/]+)", self.dashboard)

    def start(self) -> None:
        for _ in range(self.WORKERS):
            threading.Thread(target=self._work, daemon=True).start()
        threading.Thread(target=self._summary, daemon=True).start()

    def menu_updated(self, request: Request, match):
        body = request.json()
        with self.lock:
            self.pending[body["restaurant"]] = body.get("menuVersion")
            self.lock.notify()
        return 202, {}

    def _work(self) -> None:
        while True:
            with self.lock:
                while not self.pending:
                    self.lock.wait()
                restaurant, version = self.pending.popitem()
            started, failed = time.time(), 0
            for window in WINDOWS:
                status, _ = call(
                    "POST",
                    f"{ETA}/v1/prep-time",
                    {"restaurant": restaurant, "window": window},
                    {"X-Client": "restaurant-portal"},
                    timeout=10,
                )
                failed += status != 200
            with self.lock:
                self.minute["refreshes"] += 1
            log_json(
                "info" if not failed else "warn",
                "prep times refreshed",
                restaurant=restaurant,
                menuVersion=version,
                windows=len(WINDOWS),
                failed=failed,
                ms=round((time.time() - started) * 1000),
            )

    def dashboard(self, request: Request, match):
        with self.lock:
            self.minute["dashboards"] += 1
        log_json("info", "dashboard viewed", restaurant=match["id"], staff=request.headers.get("X-Staff", ""))
        return 200, {"restaurant": match["id"]}

    def _summary(self) -> None:
        while True:
            time.sleep(60)
            with self.lock:
                minute, self.minute = self.minute, dict.fromkeys(self.minute, 0)
                pending = len(self.pending)
            log_json(
                "info",
                "portal activity in the last minute",
                prepTimeRefreshes=minute["refreshes"],
                dashboardViews=minute["dashboards"],
                refreshesPending=pending,
            )


if __name__ == "__main__":
    serve(RestaurantPortal)
