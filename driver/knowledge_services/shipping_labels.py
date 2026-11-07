import os
import random
import threading
import time

from common import Service, Settings, call, log_json, serve


class ShippingLabels(Service):
    """Buys a shipping label from the carrier (through carrier-gw) for each order waiting for one in orders.

    The address is checked with address-validation first; an order whose address cannot be delivered to is parked
    in orders for customer service. The carrier throttles at peak; throttled requests are retried with backoff.
    """

    def __init__(self) -> None:
        super().__init__()
        self.orders = os.environ["ORDERS_URL"]
        self.validation = os.environ["ADDRESS_VALIDATION_URL"]
        self.carrier = os.environ["CARRIER_GW_URL"]
        self.workers = int(os.environ.get("WORKERS", "1"))
        self.settings = Settings("shipping-labels", {"carrier.retry_backoff_ms": 1000, "carrier.max_attempts": 5})

    def start(self) -> None:
        for _ in range(self.workers):
            threading.Thread(target=self._work, daemon=True).start()

    def _work(self) -> None:
        while True:
            status, body = call("GET", f"{self.orders}/v1/orders/awaiting-label?limit=5")
            orders = body.get("orders", []) if status == 200 else []
            for order in orders:
                self._ship(order)
            if not orders:
                time.sleep(1)

    def _ship(self, order: dict) -> None:
        order_id, address = order["orderId"], order["address"]
        status, verdict = call("POST", f"{self.validation}/v1/validate", address)
        if status == 200 and not verdict.get("deliverable"):
            call("POST", f"{self.orders}/v1/orders/{order_id}/park", {"reason": verdict.get("reason")})
            log_json("warn", "order parked", orderId=order_id, reason=verdict.get("reason"), country=address["country"])
            return
        attempts = int(self.settings.get("carrier.max_attempts"))
        for attempt in range(1, attempts + 1):
            status, label = call("POST", f"{self.carrier}/v1/labels", {"orderId": order_id, "address": address})
            if status == 201:
                call("POST", f"{self.orders}/v1/orders/{order_id}/label", {"tracking": label["tracking"]})
                if random.random() < 0.25:
                    log_json("info", "label purchased", orderId=order_id, tracking=label["tracking"])
                return
            backoff = int(self.settings.get("carrier.retry_backoff_ms"))
            log_json(
                "error",
                "carrier label request failed",
                orderId=order_id,
                status=status,
                attempt=f"{attempt}/{attempts}",
                retryInMs=backoff if attempt < attempts else None,
            )
            time.sleep(backoff / 1000)


if __name__ == "__main__":
    serve(ShippingLabels)
