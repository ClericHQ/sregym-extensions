import os

from common import Request, Service, call, log_json, serve


class FxGateway(Service):
    """The shop's gateway to its FX rates provider (FX_PROVIDER_URL). Calls go out under the shop's API key
    (FX_API_KEY_ID); the provider meters the key against its plan and answers 429 past it, which the gateway passes
    on to the caller."""

    def __init__(self) -> None:
        super().__init__()
        self.provider, self.key = os.environ["FX_PROVIDER_URL"], os.environ["FX_API_KEY_ID"]
        self.route("GET", r"/v1/rates", self.all_rates)
        self.route("GET", r"/v1/rates/USD/(?P<quote>[A-Z]{3})", self.rate)

    def _provider(self, request: Request, path: str) -> tuple[int, dict]:
        status, body = call("GET", f"{self.provider}{path}", headers={"X-Api-Key": self.key}, peer="fx-provider")
        if status == 429:
            log_json(
                "warn",
                "provider rate limited",
                client=request.headers.get("X-Client", "unknown"),
                key=self.key,
                status=429,
                provider=body,
            )
        elif status != 200:
            log_json("error", "provider request failed", status=status, path=path)
        return status, body

    def all_rates(self, request: Request, match):
        return self._provider(request, "/v1/rates?base=USD")

    def rate(self, request: Request, match):
        return self._provider(request, f"/v1/rates/USD/{match['quote']}")


if __name__ == "__main__":
    serve(FxGateway)
