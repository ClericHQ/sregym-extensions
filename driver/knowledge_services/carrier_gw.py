import random
import threading
import time
import uuid

from common import Request, Service, log_json, serve


class CarrierGateway(Service):
    """Gateway to the parcel carrier's label API. During the carrier's peak windows it throttles a share of
    requests, which the gateway passes on as 429."""

    def __init__(self) -> None:
        super().__init__()
        self.lock = threading.Lock()
        self.throttled_until = 0.0
        self.route("POST", r"/v1/labels", self.create)
        self.route("POST", r"/internal/peak", self.peak)

    def peak(self, request: Request, match):
        seconds = float(request.json().get("seconds", 60))
        with self.lock:
            self.throttled_until = time.time() + seconds
        return 200, {}

    def create(self, request: Request, match):
        body = request.json()
        time.sleep(random.uniform(0.03, 0.09))
        if time.time() < self.throttled_until and random.random() < 0.7:
            log_json("warn", "carrier throttled label request", orderId=body.get("orderId"), status=429)
            return 429, {"error": "Too Many Requests"}
        return 201, {"tracking": f"1Z{uuid.uuid4().hex[:16].upper()}"}


if __name__ == "__main__":
    serve(CarrierGateway)
