import os
import threading
import time

from common import Request, Service, log_json, serve

MODEL = os.environ.get("MODEL_VERSION", "eta-gbm-2026.10.3")


def _calibrate(target_s: float) -> int:
    n, spent = 20000, 0.0
    while spent < 0.005:
        started = time.process_time()
        sum(i * i for i in range(n))
        spent = time.process_time() - started
        n *= 2
    return int(n / 2 * target_s / spent)


class Eta(Service):
    """Time estimates from the ETA model: a courier's time to a restaurant (dispatch), the delivery time promised at
    checkout (app-api), and a restaurant's prep time by time window (restaurant-portal). Every estimate runs the
    model; callers name themselves in X-Client."""

    def __init__(self) -> None:
        super().__init__()
        self.lock = threading.Lock()
        self.minute: list[tuple[str, float]] = []
        self.route("POST", r"/v1/eta", self.estimate)
        self.route("POST", r"/v1/prep-time", self.estimate)

    def start(self) -> None:
        # The model's cost per estimate on this CPU, measured once at start.
        self.work = _calibrate(0.03)
        log_json("info", "loaded ETA model", model=MODEL, features=412, trees=1800)
        threading.Thread(target=self._summary, daemon=True).start()

    def estimate(self, request: Request, match):
        started = time.time()
        body = request.json()
        caller = request.headers.get("X-Client", "unknown")
        kind = body.get("kind", "prep_time" if request.path_only.endswith("prep-time") else "eta")
        sum(i * i for i in range(self.work))
        seconds = 60 + (hash((kind, body.get("restaurant"), body.get("courier"))) % 900)
        ms = round((time.time() - started) * 1000)
        with self.lock:
            self.minute.append((caller, ms))
        log_json("info", "estimate", kind=kind, caller=caller, ms=ms)
        return 200, {"seconds": seconds, "model": MODEL}

    def _summary(self) -> None:
        while True:
            time.sleep(60)
            with self.lock:
                minute, self.minute = self.minute, []
            by_caller: dict[str, int] = {}
            for caller, _ in minute:
                by_caller[caller] = by_caller.get(caller, 0) + 1
            times = sorted(ms for _, ms in minute) or [0]
            log_json(
                "info",
                "estimates in the last minute",
                total=len(minute),
                byCaller=by_caller,
                p50Ms=times[len(times) // 2],
                p95Ms=times[int(len(times) * 0.95)],
                model=MODEL,
            )


if __name__ == "__main__":
    serve(Eta)
