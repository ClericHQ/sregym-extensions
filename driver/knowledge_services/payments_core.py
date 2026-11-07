import random
import threading
import time

from common import Request, Service, log_json, serve


class PaymentsCore(Service):
    """Paid subscriptions per workspace. Its business-rule errors escape to the error handler, which answers 500."""

    def __init__(self) -> None:
        super().__init__()
        self.errors: list[float] = []
        self.lock = threading.Lock()
        self.route("GET", r"/v1/paid-subscriptions/(?P<workspace>[0-9a-f]{24})", self.paid_subscriptions)
        self.route("GET", r"/stats", self.stats)

    def paid_subscriptions(self, request: Request, match):
        workspace, user = match["workspace"], request.headers.get("X-User-Id", "")
        url = request.path
        failure = None
        if workspace in set(self.state.get("manual_billing", [])):
            failure = (
                "ManualBillingError",
                "Workspace is configured for manual billing; you can't obtain paid subscriptions for this workspace "
                "through this API",
            )
        elif workspace in set(self.state.get("no_metadata", [])):
            failure = ("NormalityCheckError", "This subscription has no workspaceId metadata")
        if failure is None:
            if random.random() < 0.05:
                log_json("info", "request completed", url=url, status=200, durationMs=random.randint(20, 90))
            return 200, {"workspaceId": workspace, "subscriptions": [{"plan": "team", "seats": random.randint(2, 40)}]}
        kind, message = failure
        log_json("error", message, userId=user, url=url, **{"error.kind": kind, "error.extra.workspaceId": workspace})
        log_json("error", f"{kind}: {message}", userId=user, url=url, status=500, handler="errorHandler")
        with self.lock:
            self.errors.append(time.time())
        return 500, {"error": kind, "message": message}

    def stats(self, request: Request, match):
        with self.lock:
            recent = sum(t > time.time() - 900 for t in self.errors)
        return 200, {"errors_15m": recent}


if __name__ == "__main__":
    serve(PaymentsCore)
