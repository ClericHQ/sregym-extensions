import os
import queue
import threading
import time

from common import Request, Service, call, log_json, serve

DELIVERIES = os.environ["DELIVERIES_URL"]
MENU = os.environ["MENU_URL"]
PORTAL = os.environ["RESTAURANT_PORTAL_URL"]
ORDERLY = os.environ["ORDERLY_API_URL"]
ORDERLY_KEY = os.environ["ORDERLY_API_KEY"]
ORDER_EVENTS = {"order.accepted": "accepted", "order.ready": "ready", "order.rejected": "cancelled"}


class PosHub(Service):
    """Larkspur's side of the restaurants' point-of-sale integrations. Every restaurant's POS (PrepPro or TillTech) is
    connected through Orderly, a POS aggregator, which sends the restaurant's events here as webhooks: an order
    accepted, ready or rejected (to deliveries), item availability (to menu), and menu.updated (menu drops its cached
    copy, restaurant-portal refreshes the restaurant's prep times). Webhooks are answered at once and handled by
    workers. Menus are fetched from Orderly's API."""

    WORKERS = 8

    def __init__(self) -> None:
        super().__init__()
        self.lock = threading.Lock()
        self.jobs: queue.Queue = queue.Queue()
        self.minute: dict[str, dict[str, int]] = {}
        self.route("POST", r"/webhooks/orderly", self.webhook)
        self.route("GET", r"/v1/pos-menus/(?P<id>[^/]+)", self.menu)

    def start(self) -> None:
        for _ in range(self.WORKERS):
            threading.Thread(target=self._work, daemon=True).start()
        threading.Thread(target=self._summary, daemon=True).start()

    def webhook(self, request: Request, match):
        event = request.json()
        kind, pos = event.get("event", "?"), event.get("pos", "?")
        fields = {k: event[k] for k in ("restaurant", "orderId", "menuVersion", "item", "available") if k in event}
        log_json("info", "webhook received", source="orderly", pos=pos, event=kind, **fields)
        with self.lock:
            per_pos = self.minute.setdefault(pos, {})
            per_pos[kind] = per_pos.get(kind, 0) + 1
        self.jobs.put(event)
        return 202, {}

    def _work(self) -> None:
        while True:
            event = self.jobs.get()
            kind, restaurant = event.get("event"), event.get("restaurant")
            headers = {"X-Client": "pos-hub"}
            if kind in ORDER_EVENTS:
                body = {"status": ORDER_EVENTS[kind]}
                if kind == "order.rejected":
                    body["reason"] = event.get("reason", "rejected by restaurant")
                call("POST", f"{DELIVERIES}/v1/orders/{event['orderId']}/status", body, headers)
            elif kind == "item.availability":
                body = {"restaurant": restaurant, "item": event["item"], "available": event["available"]}
                call("POST", f"{MENU}/internal/availability", body, headers)
            elif kind == "menu.updated":
                call("POST", f"{MENU}/internal/invalidate", {"restaurant": restaurant}, headers)
                body = {"restaurant": restaurant, "menuVersion": event.get("menuVersion")}
                call("POST", f"{PORTAL}/internal/menu-updated", body, headers, timeout=30)

    def menu(self, request: Request, match):
        restaurant = match["id"]
        pos = self.state.get("restaurants", {}).get(restaurant)
        if pos is None:
            return 404, {"error": "unknown restaurant"}
        started = time.time()
        headers = {"Authorization": f"Bearer {ORDERLY_KEY}"}
        status, menu = call("GET", f"{ORDERLY}/v1/menus/{restaurant}", headers=headers, timeout=8)
        ms = round((time.time() - started) * 1000)
        level = "info" if status == 200 else "warn"
        log_json(level, "menu fetched from orderly", pos=pos, restaurant=restaurant, status=status, ms=ms)
        return (200, menu) if status == 200 else (502, {"error": f"orderly returned {status}"})

    def _summary(self) -> None:
        while True:
            time.sleep(60)
            with self.lock:
                minute, self.minute = self.minute, {}
            by_event: dict[str, int] = {}
            for events in minute.values():
                for kind, count in events.items():
                    by_event[kind] = by_event.get(kind, 0) + count
            log_json(
                "info",
                "webhooks in the last minute",
                total=sum(by_event.values()),
                byEvent=by_event,
                byPos=minute,
                queued=self.jobs.qsize(),
            )


if __name__ == "__main__":
    serve(PosHub)
