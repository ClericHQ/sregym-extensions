import json
import os
import secrets
import threading
import time

from common import Request, Service, Settings, call, log_json, serve


class StorefrontApi(Service):
    """The web shop's backend: signs shoppers in, keeps their sessions in the session store (SESSION_STORE_URL),
    and serves search and checkout to signed-in shoppers. A request whose session is gone is answered 401 and the
    shopper is sent back to sign in."""

    def __init__(self) -> None:
        super().__init__()
        self.sessions = os.environ["SESSION_STORE_URL"]
        self.search_url, self.checkout_url = os.environ["SEARCH_URL"], os.environ["CHECKOUT_URL"]
        self.settings = Settings("storefront-api", {"banner.text": "", "session.ttl_s": 7200})
        self.lock = threading.Lock()
        self.outcomes: list[tuple[float, int]] = []
        self.route("POST", r"/api/login", self.login)
        self.route("GET", r"/api/search", self.search)
        self.route("POST", r"/api/checkout", self.checkout)
        self.route("GET", r"/stats", self.stats)

    def _record(self, status: int, body: dict) -> tuple[int, dict]:
        with self.lock:
            self.outcomes.append((time.time(), status))
        return status, body

    def _session(self, request: Request) -> dict | None:
        sid = request.headers.get("X-Session", "")
        status, body = call("GET", f"{self.sessions}/kv/sess:{sid}")
        if status == 200:
            return json.loads(body["value"])
        log_json("warn", "session not found", path=request.path_only, sid=sid[:8], status=status)
        return None

    def login(self, request: Request, match):
        body = request.json()
        sid = secrets.token_hex(16)
        session = {
            "shopper": body["shopper"],
            "currency": body.get("currency", "USD"),
            "cart": [],
            "since": time.time(),
        }
        session["profile"] = {"segments": ["returning"], "recent": [f"sku-{i:05d}" for i in range(0, 1200, 37)]}
        call(
            "POST",
            f"{self.sessions}/kv/sess:{sid}",
            {"value": json.dumps(session), "ttl_s": self.settings.get("session.ttl_s")},
        )
        return self._record(200, {"sid": sid})

    def search(self, request: Request, match):
        session = self._session(request)
        if session is None:
            return self._record(401, {"error": "session_expired"})
        query = f"q={request.query.get('q', '')}&currency={session['currency']}"
        status, body = call("GET", f"{self.search_url}/v1/search?{query}", timeout=4)
        return self._record(200 if status == 200 else 503, body)

    def checkout(self, request: Request, match):
        session = self._session(request)
        if session is None:
            return self._record(401, {"error": "session_expired"})
        body = request.json()
        order = {"items": body["items"], "currency": session["currency"], "address": body["address"]}
        status, result = call("POST", f"{self.checkout_url}/v1/orders", order, timeout=8)
        return self._record(status, result)

    def stats(self, request: Request, match):
        cutoff = time.time() - 600
        with self.lock:
            self.outcomes = [o for o in self.outcomes if o[0] > cutoff]
            recent = list(self.outcomes)
        return 200, {"requests_10m": len(recent), "unauthorized_10m": sum(s == 401 for _, s in recent)}


if __name__ == "__main__":
    serve(StorefrontApi)
