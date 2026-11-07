import threading
import time

from common import Request, Service, log_json, serve, ulid

OPEN = ("placed", "accepted", "ready", "courier_assigned", "picked_up")
STEPS = (*OPEN, "delivered")


class Deliveries(Service):
    """Every order from placement to drop-off: placed, accepted by the restaurant, ready for pickup, a courier
    assigned, picked up, delivered (or cancelled). Each order carries the delivery time app-api promised the customer
    when it was placed."""

    def __init__(self) -> None:
        super().__init__()
        self.lock = threading.Lock()
        self.orders: dict[str, dict] = {}
        self.counts = {"placed": 0, "delivered": 0, "late": 0, "cancelled": 0}
        self.accept_times: list[float] = []
        self.route("POST", r"/v1/orders", self.create)
        self.route("GET", r"/v1/orders", self.list)
        self.route("POST", r"/v1/orders/(?P<id>[^/]+)/status", self.status)
        self.route("GET", r"/stats", self.stats)

    def start(self) -> None:
        threading.Thread(target=self._summary, daemon=True).start()

    def create(self, request: Request, match):
        body, now = request.json(), time.time()
        order = {
            "orderId": f"ord_{ulid()[-12:].lower()}",
            "customer": body["customer"],
            "restaurant": body["restaurant"],
            "status": "placed",
            "placed": now,
            "promised": now + float(body.get("promiseS", 900)),
            "courier": None,
        }
        with self.lock:
            self.orders[order["orderId"]] = order
            self.counts["placed"] += 1
        log_json(
            "info",
            "order placed",
            orderId=order["orderId"],
            restaurant=order["restaurant"],
            promisedMin=round((order["promised"] - now) / 60, 1),
        )
        return 201, {"orderId": order["orderId"], "promised": order["promised"]}

    # Orders the restaurant has accepted that have no courier yet, oldest first.
    def list(self, request: Request, match):
        wanted = set(request.query.get("status", "accepted,ready").split(","))
        limit, now = int(request.query.get("limit", "50")), time.time()
        with self.lock:
            orders = [o for o in self.orders.values() if o["status"] in wanted and not o["courier"]]
        orders.sort(key=lambda o: o["placed"])
        return 200, {
            "orders": [
                {
                    "orderId": o["orderId"],
                    "restaurant": o["restaurant"],
                    "status": o["status"],
                    "ageS": now - o["placed"],
                }
                for o in orders[:limit]
            ],
            "total": len(orders),
        }

    def status(self, request: Request, match):
        body, now = request.json(), time.time()
        with self.lock:
            order = self.orders.get(match["id"])
            if order is None:
                return 404, {"error": "unknown order"}
            previous = order["status"]
            # A webhook that arrives after a later one does not move the order back.
            if previous in STEPS and body["status"] in STEPS and STEPS.index(body["status"]) < STEPS.index(previous):
                return 200, {}
            order["status"] = body["status"]
            if body.get("courier"):
                order["courier"] = body["courier"]
            late = order["status"] == "delivered" and now > order["promised"]
            if order["status"] == "delivered":
                self.counts["delivered"] += 1
                self.counts["late"] += late
            if order["status"] == "cancelled":
                self.counts["cancelled"] += 1
        fields = {"orderId": match["id"], "status": body["status"], "restaurant": order["restaurant"]}
        if body.get("courier"):
            fields["courier"] = body["courier"]
        if body.get("reason"):
            fields["reason"] = body["reason"]
        if previous == "placed" and order["status"] == "accepted":
            fields["acceptedAfterS"] = round(now - order["placed"])
            with self.lock:
                self.accept_times.append(now - order["placed"])
        if order["status"] == "delivered":
            fields["minutesFromPlaced"] = round((now - order["placed"]) / 60, 1)
            fields["late"] = late
        log_json("info", "order status", **fields)
        return 200, {}

    def _late_open(self, now: float) -> int:
        return sum(1 for o in self.orders.values() if o["status"] in OPEN and now > o["promised"])

    def stats(self, request: Request, match):
        now = time.time()
        with self.lock:
            return 200, {
                "late_open": self._late_open(now),
                "awaiting_acceptance": sum(1 for o in self.orders.values() if o["status"] == "placed"),
                "open": sum(o["status"] in OPEN for o in self.orders.values()),
            }

    def _summary(self) -> None:
        while True:
            time.sleep(60)
            now = time.time()
            with self.lock:
                counts, self.counts = self.counts, dict.fromkeys(self.counts, 0)
                accepts, self.accept_times = sorted(self.accept_times), []
                open_orders = sum(o["status"] in OPEN for o in self.orders.values())
                awaiting = sum(1 for o in self.orders.values() if o["status"] == "placed")
                late_open = self._late_open(now)
                for order_id in [
                    i for i, o in self.orders.items() if o["status"] not in OPEN and now - o["placed"] > 3600
                ]:
                    del self.orders[order_id]
            log_json(
                "info",
                "orders in the last minute",
                placed=counts["placed"],
                delivered=counts["delivered"],
                deliveredLate=counts["late"],
                cancelled=counts["cancelled"],
                open=open_orders,
                awaitingAcceptance=awaiting,
                acceptedP50S=round(accepts[len(accepts) // 2]) if accepts else None,
                pastPromiseOpen=late_open,
            )


if __name__ == "__main__":
    serve(Deliveries)
