import random
import time
import uuid

from common import Request, Service, log_json, serve

DECLINES = ["insufficient_funds", "do_not_honor", "expired_card", "card_velocity_exceeded"]


class Payments(Service):
    """Authorizes card payments with the acquirer. A decline is an answer (402), not a failure."""

    def __init__(self) -> None:
        super().__init__()
        self.route("POST", r"/v1/authorizations", self.authorize)

    def authorize(self, request: Request, match):
        body = request.json()
        time.sleep(random.uniform(0.04, 0.12))
        if random.random() < 0.04:
            reason = random.choice(DECLINES)
            log_json(
                "info",
                "authorization declined",
                reason=reason,
                amount=body.get("amount"),
                currency=body.get("currency"),
            )
            return 402, {"declined": reason}
        return 201, {"authorizationId": f"auth_{uuid.uuid4().hex[:16]}"}


if __name__ == "__main__":
    serve(Payments)
