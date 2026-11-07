import random

from common import Request, Service, log_json, serve, ulid

DECLINES = ["insufficient_funds", "do_not_honor", "expired_card", "card_velocity_exceeded"]


class Billing(Service):
    """Authorises the customer's card for an order's total before the order is placed."""

    def __init__(self) -> None:
        super().__init__()
        self.route("POST", r"/v1/authorize", self.authorize)

    def authorize(self, request: Request, match):
        body = request.json()
        if random.random() < 0.03:
            reason = random.choice(DECLINES)
            log_json("info", "authorization declined", customer=body.get("customer"), reason=reason)
            return 402, {"approved": False, "reason": reason}
        auth = f"auth_{ulid()[-10:].lower()}"
        log_json(
            "info",
            "authorization approved",
            customer=body.get("customer"),
            authId=auth,
            amountCents=body.get("amountCents"),
        )
        return 200, {"approved": True, "authId": auth}


if __name__ == "__main__":
    serve(Billing)
