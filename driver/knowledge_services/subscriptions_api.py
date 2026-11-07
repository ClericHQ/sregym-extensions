import os
import random
import threading
import time

from common import Service, call, log_json, serve


class SubscriptionsApi(Service):
    """Reads paid subscriptions for the workspaces its users open in the app."""

    def __init__(self) -> None:
        super().__init__()
        self.payments = os.environ["PAYMENTS_CORE_URL"]

    def start(self) -> None:
        threading.Thread(target=self._loop, daemon=True).start()

    def _loop(self) -> None:
        while True:
            time.sleep(random.uniform(0.5, 1.5))
            pairs = self.state.get("reads", [])
            if not pairs:
                continue
            user, workspace = random.choice(pairs)
            status, _ = call("GET", f"{self.payments}/v1/paid-subscriptions/{workspace}", headers={"X-User-Id": user})
            if status >= 500:
                log_json("error", "getPaidSubscriptions failed", workspaceId=workspace, status=status)


if __name__ == "__main__":
    serve(SubscriptionsApi)
