import os
import threading
import time

from common import Request, Service, call, log_json, serve

MENU = os.environ["MENU_URL"]
ETA = os.environ["ETA_URL"]
BILLING = os.environ["BILLING_URL"]
DELIVERIES = os.environ["DELIVERIES_URL"]
WINDOW_S = 300


class AppApi(Service):
    """The customer app's backend: the restaurants near the customer, a restaurant's menu, and checkout (the card is
    authorised, the ETA model quotes the delivery time promised, and the order is placed with deliveries)."""

    HEADERS = {"X-Client": "app-api"}  # noqa: RUF012 (never mutated)

    def __init__(self) -> None:
        super().__init__()
        self.lock = threading.Lock()
        self.menu_times: list[tuple[float, int]] = []
        self.route("GET", r"/v1/restaurants", self.restaurants)
        self.route("GET", r"/v1/restaurants/(?P<id>[^/]+)/menu", self.menu)
        self.route("POST", r"/v1/orders", self.order)
        self.route("GET", r"/stats", self.stats)

    def _log(self, request: Request, route: str, status: int, started: float, **fields) -> int:
        ms = round((time.time() - started) * 1000)
        log_json(
            "info" if status < 500 else "error",
            "request",
            method=request.command,
            route=route,
            status=status,
            ms=ms,
            **fields,
        )
        return ms

    def restaurants(self, request: Request, match):
        started = time.time()
        listing = [{k: r[k] for k in ("id", "name", "cuisine")} for r in self.state.get("restaurants", [])]
        self._log(request, "GET /v1/restaurants", 200, started, customer=request.headers.get("X-Customer", ""))
        return 200, {"restaurants": listing}

    def menu(self, request: Request, match):
        started = time.time()
        status, menu = call("GET", f"{MENU}/v1/menus/{match['id']}", headers=self.HEADERS, timeout=10)
        status = 200 if status == 200 else 502
        ms = self._log(request, "GET /v1/restaurants/{id}/menu", status, started, restaurant=match["id"])
        with self.lock:
            self.menu_times.append((time.time(), ms))
        return status, menu if status == 200 else {"error": "menu unavailable"}

    def order(self, request: Request, match):
        started, body = time.time(), request.json()
        customer, restaurant = body["customer"], body["restaurant"]
        status, auth = call(
            "POST",
            f"{BILLING}/v1/authorize",
            {"customer": customer, "amountCents": body.get("amountCents")},
            self.HEADERS,
        )
        if status != 200:
            self._log(request, "POST /v1/orders", 402, started, customer=customer, reason=auth.get("reason"))
            return 402, {"error": "payment declined"}
        status, estimate = call(
            "POST", f"{ETA}/v1/eta", {"kind": "delivery_promise", "restaurant": restaurant}, self.HEADERS, timeout=3
        )
        promise_s = 330 + (estimate.get("seconds", 600) % 120) if status == 200 else 600
        status, placed = call(
            "POST",
            f"{DELIVERIES}/v1/orders",
            {"customer": customer, "restaurant": restaurant, "promiseS": promise_s},
            self.HEADERS,
        )
        status = 201 if status == 201 else 502
        self._log(request, "POST /v1/orders", status, started, customer=customer, restaurant=restaurant)
        return status, placed

    # Menu latency over the last five minutes.
    def stats(self, request: Request, match):
        now = time.time()
        with self.lock:
            self.menu_times = [(t, ms) for t, ms in self.menu_times if now - t < WINDOW_S]
            times = sorted(ms for _, ms in self.menu_times)
        return 200, {"menu_requests_5m": len(times), "menu_p95_ms_5m": times[int(len(times) * 0.95)] if times else 0}


if __name__ == "__main__":
    serve(AppApi)
