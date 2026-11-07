import contextlib
import os
import threading
import time

from common import Request, Service, call, log_json, serve

DELIVERIES = os.environ["DELIVERIES_URL"]
COURIERS = os.environ["COURIER_API_URL"]
ETA = os.environ["ETA_URL"]
PAYENGINE = os.environ["PAYENGINE_URL"]
PAYENGINE_KEY = os.environ["PAYENGINE_API_KEY"]
ROUND_S = 4
OFFER_HOLD_S = 35


class Dispatch(Service):
    """Matches orders the restaurant has accepted with couriers. Every few seconds it takes the unassigned orders,
    oldest first, asks courier-api for idle couriers near each restaurant, has the ETA model estimate each candidate's
    time to the restaurant, and offers the order to the quickest, with the fee finance's pay engine quotes for the
    trip. An order with an open offer waits for the courier's answer."""

    def __init__(self) -> None:
        super().__init__()
        self.headers = {"X-Client": "dispatch"}
        self.lock = threading.Lock()
        self.offered: dict[str, float] = {}
        self.last = {"waiting": 0, "oldestWaitS": 0}
        self.minute = {"rounds": 0, "offers": 0, "noCourier": 0, "etaFailed": 0}
        self.route("GET", r"/stats", self.stats)

    def start(self) -> None:
        threading.Thread(target=self._loop, daemon=True).start()
        threading.Thread(target=self._summary, daemon=True).start()

    def _loop(self) -> None:
        while True:
            started = time.time()
            with contextlib.suppress(Exception):
                self._round()
            time.sleep(max(0.5, ROUND_S - (time.time() - started)))

    def _round(self) -> None:
        started = time.time()
        status, body = call("GET", f"{DELIVERIES}/v1/orders?status=accepted,ready&limit=50", headers=self.headers)
        if status != 200:
            log_json("warn", "could not list unassigned orders", status=status)
            return
        orders = body["orders"]
        with self.lock:
            self.last = {"waiting": body["total"], "oldestWaitS": round(orders[0]["ageS"]) if orders else 0}
            self.offered = {o: t for o, t in self.offered.items() if started - t < OFFER_HOLD_S}
            todo = [o for o in orders if o["orderId"] not in self.offered][:8]
        offers = no_courier = eta_failed = 0
        eta_s = 0.0
        for order in todo:
            status, idle = call(
                "GET", f"{COURIERS}/v1/couriers/idle?restaurant={order['restaurant']}&limit=3", headers=self.headers
            )
            candidates = idle.get("couriers", []) if status == 200 else []
            if not candidates:
                no_courier += 1
                continue
            best = None
            for courier in candidates:
                eta_started = time.time()
                status, estimate = call(
                    "POST",
                    f"{ETA}/v1/eta",
                    {"kind": "courier_pickup", "courier": courier, "restaurant": order["restaurant"]},
                    self.headers,
                    timeout=2,
                )
                eta_s += time.time() - eta_started
                if status != 200:
                    eta_failed += 1
                    continue
                if best is None or estimate["seconds"] < best[1]:
                    best = (courier, estimate["seconds"])
            if best is None:
                continue
            # The courier's fee for the trip, from finance's pay engine.
            status, quote = call(
                "GET",
                f"{PAYENGINE}/v1/quote?pickupEtaS={best[1]}",
                headers={"Authorization": f"Bearer {PAYENGINE_KEY}"},
                timeout=3,
            )
            if status != 200:
                log_json("warn", "fee quote failed", order=order["orderId"], status=status)
                continue
            body = {"order": order["orderId"], "courier": best[0], "pickupEtaS": best[1], "feeCents": quote["feeCents"]}
            status, _ = call("POST", f"{COURIERS}/v1/offers", body, self.headers)
            if status == 201:
                offers += 1
                with self.lock:
                    self.offered[order["orderId"]] = time.time()
        with self.lock:
            self.minute["rounds"] += 1
            self.minute["offers"] += offers
            self.minute["noCourier"] += no_courier
            self.minute["etaFailed"] += eta_failed
        log_json(
            "info",
            "assignment round",
            unassigned=self.last["waiting"],
            considered=len(todo),
            offered=offers,
            noIdleCourier=no_courier,
            etaFailures=eta_failed,
            ms=round((time.time() - started) * 1000),
            etaMs=round(eta_s * 1000),
        )

    def stats(self, request: Request, match):
        with self.lock:
            return 200, dict(self.last)

    def _summary(self) -> None:
        while True:
            time.sleep(60)
            with self.lock:
                minute, self.minute = self.minute, dict.fromkeys(self.minute, 0)
                last = dict(self.last)
            log_json(
                "info",
                "dispatch in the last minute",
                **minute,
                unassigned=last["waiting"],
                oldestWaitS=last["oldestWaitS"],
            )


if __name__ == "__main__":
    serve(Dispatch)
