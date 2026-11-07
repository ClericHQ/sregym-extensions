import random
import threading
import time

from common import Request, Service, log_json, serve


class Orders(Service):
    """Fulfilment state of placed orders. An order waits for a shipping label, which shipping-labels buys, claiming
    orders a few at a time; one it cannot ship is parked and still waits for its label until customer service resolves
    it from the console (a corrected address, or a cancellation)."""

    CLAIM_S = 30

    def __init__(self) -> None:
        super().__init__()
        self.lock = threading.Lock()
        self.orders: dict[str, dict] = {}
        self.route("POST", r"/v1/orders", self.create)
        self.route("GET", r"/v1/orders/awaiting-label", self.claim)
        self.route("POST", r"/v1/orders/(?P<order>ord_[0-9a-f]+)/label", self.label)
        self.route("POST", r"/v1/orders/(?P<order>ord_[0-9a-f]+)/park", self.park)
        self.route("GET", r"/v1/orders/parked", self.parked)
        self.route("POST", r"/v1/orders/(?P<order>ord_[0-9a-f]+)/resolve", self.resolve)
        self.route("GET", r"/stats", self.stats)

    def start(self) -> None:
        threading.Thread(target=self._gauge, daemon=True).start()

    def _waiting(self) -> list[dict]:
        return [o for o in self.orders.values() if o["status"] == "awaiting_label"]

    def _gauge(self) -> None:
        while True:
            time.sleep(60)
            with self.lock:
                cutoff = time.time() - 600
                self.orders = {
                    k: o for k, o in self.orders.items() if o["status"] == "awaiting_label" or o["created"] > cutoff
                }
                waiting = self._waiting()
                parked = sum(o["parked"] for o in waiting)
                oldest = min((o["created"] for o in waiting), default=time.time())
            log_json("info", "label queue", depth=len(waiting), parked=parked, oldestAgeS=round(time.time() - oldest))

    def create(self, request: Request, match):
        order = request.json()
        with self.lock:
            self.orders[order["orderId"]] = {
                **order,
                "status": "awaiting_label",
                "parked": False,
                "claimed_until": 0.0,
                "created": time.time(),
            }
        if random.random() < 0.2:
            log_json("info", "order received", orderId=order["orderId"])
        return 201, {"orderId": order["orderId"]}

    def claim(self, request: Request, match):
        limit, now = int(request.query.get("limit", "5")), time.time()
        with self.lock:
            free = [o for o in self._waiting() if not o["parked"] and o["claimed_until"] < now]
            free.sort(key=lambda o: o["created"])
            for order in free[:limit]:
                order["claimed_until"] = now + self.CLAIM_S
            claimed = [{"orderId": o["orderId"], "address": o["address"]} for o in free[:limit]]
        return 200, {"orders": claimed}

    def label(self, request: Request, match):
        body = request.json()
        with self.lock:
            order = self.orders.get(match["order"])
            if order is None:
                return 404, {"error": "no such order"}
            order["status"], order["tracking"] = "labelled", body.get("tracking")
        return 200, {}

    def park(self, request: Request, match):
        body = request.json()
        with self.lock:
            order = self.orders.get(match["order"])
            if order is None:
                return 404, {"error": "no such order"}
            order["parked"], order["park_reason"] = True, body.get("reason")
        return 200, {}

    def parked(self, request: Request, match):
        limit = int(request.query.get("limit", "10"))
        with self.lock:
            parked = sorted((o for o in self._waiting() if o["parked"]), key=lambda o: o["created"])[:limit]
            return 200, {"orders": [{"orderId": o["orderId"], "reason": o.get("park_reason")} for o in parked]}

    def resolve(self, request: Request, match):
        body = request.json()
        with self.lock:
            order = self.orders.get(match["order"])
            if order is None:
                return 404, {"error": "no such order"}
            order["status"] = "resolved"
        log_json("info", "parked order resolved", orderId=match["order"], resolution=body.get("resolution"))
        return 200, {}

    def stats(self, request: Request, match):
        now = time.time()
        with self.lock:
            waiting = self._waiting()
            return 200, {
                "awaiting_label": len(waiting),
                "parked": sum(o["parked"] for o in waiting),
                "oldest_age_s": round(now - min((o["created"] for o in waiting), default=now)),
            }


if __name__ == "__main__":
    serve(Orders)
