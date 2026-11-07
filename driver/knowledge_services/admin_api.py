import os
import urllib.error
import urllib.parse
import urllib.request
import uuid
from typing import ClassVar

from common import Request, Service, call, log_json, serve


class AdminApi(Service):
    """The admin console's API. Records every admin action as an info-level audit event."""

    ROUTES: ClassVar[dict[str, tuple[str, str]]] = {
        "trials": ("GET /account-admin/v1/workspaces/:workspaceId/trials", "account-admin.trials.read"),
        "subscriptions": (
            "GET /account-admin/v1/workspaces/:workspaceId/subscriptions",
            "account-admin.subscriptions.read",
        ),
        "billing-profile": (
            "GET /account-admin/v1/workspaces/:workspaceId/billing-profile",
            "account-admin.billing-profile.read",
        ),
    }

    def __init__(self) -> None:
        super().__init__()
        self.payments, self.accounts = os.environ["PAYMENTS_CORE_URL"], os.environ["ACCOUNTS_URL"]
        self.actor_ids: dict[str, str] = {}
        self.route("GET", r"/account-admin/v1/workspaces/(?P<workspace>[0-9a-f]{24})/(?P<what>[a-z-]+)", self.read)

    def _actor_id(self, email: str) -> str | None:
        if email not in self.actor_ids:
            status, user = call("GET", f"{self.accounts}/users?email={urllib.parse.quote(email)}")
            if status != 200:
                return None
            self.actor_ids[email] = user["id"]
        return self.actor_ids[email]

    def _subscriptions(self, workspace: str, actor_id: str, request_id: str) -> tuple[int, dict]:
        for _ in range(3):
            status, body = call(
                "GET",
                f"{self.payments}/v1/paid-subscriptions/{workspace}?includeSeatEconomics=true",
                headers={"X-User-Id": actor_id},
            )
            if status < 500:
                return status, body
            # Manual billing is an answer, not a failure: the console shows the workspace as invoiced.
            if body.get("error") == "ManualBillingError":
                return 200, {"billing": "manual"}
        log_json(
            "error",
            "payments-core read unavailable",
            error="Failed to get paid subscriptions",
            downstream="payments-core",
            requestId=request_id,
        )
        return 502, {"error": "payments-core read unavailable"}

    def read(self, request: Request, match):
        what, workspace = match["what"], match["workspace"]
        if what not in self.ROUTES:
            return 404, {"error": "no such route"}
        email, request_id = request.headers.get("X-Actor-Email", ""), uuid.uuid4().hex
        actor_id = self._actor_id(email)
        if actor_id is None:
            return 403, {"error": "unknown admin"}
        status, body = (200, {"workspaceId": workspace})
        if what == "subscriptions":
            status, body = self._subscriptions(workspace, actor_id, request_id)
        route, action = self.ROUTES[what]
        log_json(
            "info",
            "account-admin.audit",
            actor={"email": email},
            subject={"workspaceId": workspace},
            route=route,
            action=action,
            outcome="succeeded" if status < 400 else "failed",
            requestId=request_id,
        )
        return status, body


if __name__ == "__main__":
    serve(AdminApi)
