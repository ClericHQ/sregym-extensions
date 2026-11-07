import uuid

from common import Request, Service, log_json, log_text, serve


class Accounts(Service):
    """The user directory and sign-in: a user's id, email and name. Logs each session it issues."""

    def __init__(self) -> None:
        super().__init__()
        self.route("GET", r"/users/(?P<id>[0-9a-f]{24})", self.by_id)
        self.route("GET", r"/users", self.by_email)
        self.route("POST", r"/sessions", self.sign_in)

    def seeded(self) -> None:
        users = self.state.get("users", [])
        self.ids = {u["id"]: u for u in users}
        self.emails = {u["email"]: u for u in users}

    def _access(self, request: Request, status: int) -> None:
        log_text("info", f'{request.client_address[0]} "GET {request.path} HTTP/1.1" {status}')

    def by_id(self, request: Request, match):
        user = getattr(self, "ids", {}).get(match["id"])
        self._access(request, 200 if user else 404)
        return (200, user) if user else (404, {"error": "no such user"})

    def by_email(self, request: Request, match):
        user = getattr(self, "emails", {}).get(request.query.get("email", ""))
        self._access(request, 200 if user else 404)
        return (200, user) if user else (404, {"error": "no such user"})

    def sign_in(self, request: Request, match):
        body = request.json()
        user = getattr(self, "ids", {}).get(body.get("user_id", ""))
        if user is None:
            log_json("warning", "sign-in rejected", reason="unknown user")
            return 401, {"error": "unknown user"}
        log_json(
            "info",
            "session issued",
            user_id=user["id"],
            email=user["email"],
            provider=body.get("provider", "password"),
            session_id=uuid.uuid4().hex[:16],
        )
        return 200, {"session": "issued"}


if __name__ == "__main__":
    serve(Accounts)
