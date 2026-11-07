import os
import threading
import time
import uuid

from common import Request, Service, Settings, call, log_json, serve


class Checkout(Service):
    """Places an order: prices the cart with pricing, authorizes the payment with payments and hands the order to
    orders for fulfilment."""

    def __init__(self) -> None:
        super().__init__()
        self.pricing, self.payments, self.orders = (
            os.environ["PRICING_URL"],
            os.environ["PAYMENTS_URL"],
            os.environ["ORDERS_URL"],
        )
        self.settings = Settings("checkout", {"ui.express_pay": True})
        self.lock = threading.Lock()
        self.outcomes: list[tuple[float, int]] = []
        self.route("POST", r"/v1/orders", self.place)
        self.route("GET", r"/stats", self.stats)

    def _done(self, status: int, body: dict) -> tuple[int, dict]:
        with self.lock:
            self.outcomes.append((time.time(), status))
        return status, body

    def place(self, request: Request, match):
        body = request.json()
        order_id = f"ord_{uuid.uuid4().hex[:14]}"
        status, quote = call(
            "POST",
            f"{self.pricing}/v1/quote",
            {"items": body["items"], "currency": body["currency"]},
        )
        if status != 200:
            log_json("error", "order failed", orderId=order_id, step="quote", status=status, error=quote.get("error"))
            return self._done(502, {"error": "pricing_unavailable"})
        status, auth = call(
            "POST", f"{self.payments}/v1/authorizations", {"amount": quote["total"], "currency": quote["currency"]}
        )
        if status == 402:
            log_json("info", "order declined", orderId=order_id, reason=auth.get("declined"))
            return self._done(402, {"error": "payment_declined"})
        if status != 201:
            log_json("error", "order failed", orderId=order_id, step="authorize", status=status)
            return self._done(502, {"error": "payments_unavailable"})
        order = {"orderId": order_id, "address": body["address"], "items": body["items"]}
        status, _ = call("POST", f"{self.orders}/v1/orders", order)
        if status != 201:
            log_json("error", "order failed", orderId=order_id, step="fulfilment", status=status)
            return self._done(502, {"error": "orders_unavailable"})
        log_json("info", "order placed", orderId=order_id, total=quote["total"], currency=quote["currency"])
        return self._done(201, {"orderId": order_id})

    def stats(self, request: Request, match):
        now = time.time()
        with self.lock:
            self.outcomes = [o for o in self.outcomes if o[0] > now - 600]
            recent = list(self.outcomes)
        return 200, {
            "orders_10m": len(recent),
            "errors_10m": sum(status >= 500 for _, status in recent),
            "orders_5m": sum(t > now - 300 for t, _ in recent),
            "orders_prev_5m": sum(t <= now - 300 for t, _ in recent),
        }


if __name__ == "__main__":
    serve(Checkout)
