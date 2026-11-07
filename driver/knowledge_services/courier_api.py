import os
import random
import threading
import time

from common import Request, Service, call, log_json, serve, ulid

DELIVERIES = os.environ["DELIVERIES_URL"]
OFFER_TTL_S = 30


class CourierApi(Service):
    """The courier app's backend. A courier can go online during a shift that is open here (rota-sync opens and
    closes shifts from the rota); online couriers send their location, get delivery offers from dispatch, and accept
    or decline them. When a courier's shift closes they go offline, after the delivery they are on."""

    def __init__(self) -> None:
        super().__init__()
        self.lock = threading.Lock()
        self.shifts: dict[str, dict] = {}
        self.couriers: dict[str, str] = {}  # courier -> idle | offered | on_delivery
        self.closing: set[str] = set()
        self.offers: dict[str, dict] = {}
        self.minute = dict.fromkeys(["shiftsOpened", "shiftsClosed", "pings", "offers", "accepted", "declined"], 0)
        self.fees: list[int] = []
        self.route("POST", r"/internal/shifts", self.sync_shifts)
        self.route("GET", r"/v1/shifts/open", self.open_shifts)
        self.route("POST", r"/v1/couriers/(?P<id>[^/]+)/online", self.online)
        self.route("POST", r"/v1/couriers/(?P<id>[^/]+)/location", self.location)
        self.route("GET", r"/v1/couriers/idle", self.idle)
        self.route("POST", r"/v1/offers", self.offer)
        self.route("GET", r"/v1/offers", self.pending)
        self.route("POST", r"/v1/offers/(?P<id>[^/]+)/(?P<answer>accept|decline)", self.answer)
        self.route("POST", r"/v1/deliveries/(?P<order>[^/]+)/(?P<step>picked-up|delivered)", self.delivery)

    def start(self) -> None:
        threading.Thread(target=self._expire, daemon=True).start()
        threading.Thread(target=self._summary, daemon=True).start()

    def sync_shifts(self, request: Request, match):
        body = request.json()
        with self.lock:
            for shift in body.get("open", []):
                self.shifts[shift["courier"]] = shift
                self.minute["shiftsOpened"] += 1
            for courier in body.get("close", []):
                self.shifts.pop(courier, None)
                self.minute["shiftsClosed"] += 1
                if self.couriers.get(courier) == "idle":
                    del self.couriers[courier]
                elif courier in self.couriers:
                    self.closing.add(courier)
        for shift in body.get("open", []):
            log_json("info", "shift opened", courier=shift["courier"], zone=shift.get("zone"), ends=shift.get("end"))
        for courier in body.get("close", []):
            log_json("info", "shift closed", courier=courier)
        return 200, {}

    def open_shifts(self, request: Request, match):
        with self.lock:
            return 200, {"couriers": sorted(self.shifts), "online": sorted(self.couriers)}

    def online(self, request: Request, match):
        courier = match["id"]
        with self.lock:
            if courier not in self.shifts:
                return 409, {"error": "no open shift"}
            self.couriers.setdefault(courier, "idle")
        log_json("info", "courier online", courier=courier, app=request.headers.get("User-Agent", ""))
        return 200, {}

    def location(self, request: Request, match):
        with self.lock:
            self.minute["pings"] += 1
        return 200, {}

    def idle(self, request: Request, match):
        limit = int(request.query.get("limit", "3"))
        with self.lock:
            idle = [c for c, state in self.couriers.items() if state == "idle" and c not in self.closing]
        return 200, {"couriers": random.sample(idle, min(limit, len(idle)))}

    def offer(self, request: Request, match):
        body = request.json()
        with self.lock:
            if self.couriers.get(body["courier"]) != "idle":
                return 409, {"error": "courier not available"}
            self.couriers[body["courier"]] = "offered"
            offer = {**body, "offerId": f"off_{ulid()[-10:].lower()}", "sent": time.time()}
            self.offers[offer["offerId"]] = offer
            self.minute["offers"] += 1
            if body.get("feeCents") is not None:
                self.fees.append(body["feeCents"])
        log_json(
            "info",
            "offer sent",
            offerId=offer["offerId"],
            order=body["order"],
            courier=body["courier"],
            pickupEtaS=body.get("pickupEtaS"),
            feeCents=body.get("feeCents"),
        )
        return 201, {"offerId": offer["offerId"]}

    def pending(self, request: Request, match):
        with self.lock:
            return 200, {"offers": [dict(o) for o in self.offers.values()]}

    def answer(self, request: Request, match):
        reason = request.json().get("reason", "")
        with self.lock:
            offer = self.offers.pop(match["id"], None)
            if offer is None:
                return 404, {"error": "no such offer"}
            accepted = match["answer"] == "accept"
            if accepted:
                self.couriers[offer["courier"]] = "on_delivery"
            else:
                self._free(offer["courier"])
            self.minute["accepted" if accepted else "declined"] += 1
        if accepted:
            call(
                "POST",
                f"{DELIVERIES}/v1/orders/{offer['order']}/status",
                {"status": "courier_assigned", "courier": offer["courier"]},
                {"X-Client": "courier-api"},
            )
            log_json("info", "offer accepted", offerId=match["id"], order=offer["order"], courier=offer["courier"])
        else:
            log_json(
                "info",
                "offer declined",
                offerId=match["id"],
                order=offer["order"],
                courier=offer["courier"],
                reason=reason,
            )
        return 200, {}

    def delivery(self, request: Request, match):
        courier = request.json()["courier"]
        status = "picked_up" if match["step"] == "picked-up" else "delivered"
        call(
            "POST",
            f"{DELIVERIES}/v1/orders/{match['order']}/status",
            {"status": status, "courier": courier},
            {"X-Client": "courier-api"},
        )
        if status == "delivered":
            with self.lock:
                self._free(courier)
        return 200, {}

    # A courier done with an offer or a delivery: idle again, or offline if their shift has closed. Holds the lock.
    def _free(self, courier: str) -> None:
        if courier in self.closing:
            self.closing.discard(courier)
            self.couriers.pop(courier, None)
        elif courier in self.couriers:
            self.couriers[courier] = "idle"

    def _expire(self) -> None:
        while True:
            time.sleep(5)
            now = time.time()
            with self.lock:
                expired = [o for o in self.offers.values() if now - o["sent"] > OFFER_TTL_S]
                for offer in expired:
                    del self.offers[offer["offerId"]]
                    if self.couriers.get(offer["courier"]) == "offered":
                        self._free(offer["courier"])
                    self.minute["declined"] += 1
            for offer in expired:
                log_json(
                    "info", "offer expired", offerId=offer["offerId"], order=offer["order"], courier=offer["courier"]
                )

    def _summary(self) -> None:
        while True:
            time.sleep(60)
            with self.lock:
                minute, self.minute = self.minute, dict.fromkeys(self.minute, 0)
                fees, self.fees = self.fees, []
                states = list(self.couriers.values())
                open_shifts = len(self.shifts)
            log_json(
                "info",
                "couriers in the last minute",
                online=len(states),
                idle=states.count("idle"),
                onDelivery=states.count("on_delivery"),
                openShifts=open_shifts,
                **minute,
                avgOfferFeeCents=round(sum(fees) / len(fees)) if fees else None,
            )


if __name__ == "__main__":
    serve(CourierApi)
