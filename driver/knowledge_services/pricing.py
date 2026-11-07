import os
import threading
import time

from common import Request, Service, Settings, call, log_json, serve


class Pricing(Service):
    """Quotes an order's total in the shopper's currency. Prices are in USD; FX rates come from fx-gateway, refreshed
    once a minute. Rates older than MAX_RATE_AGE_S are not used: a quote in another currency then fails."""

    def __init__(self) -> None:
        super().__init__()
        self.fx = os.environ["FX_GATEWAY_URL"]
        self.max_age_s = float(os.environ.get("MAX_RATE_AGE_S", "120"))
        self.settings = Settings("pricing", {"promo.autumn_sale": False, "promo.discount_pct": 10})
        self.lock = threading.Lock()
        self.rates: dict[str, float] = {}
        self.fetched = 0.0
        self.route("POST", r"/v1/quote", self.quote)

    def start(self) -> None:
        threading.Thread(target=self._refresh, daemon=True).start()

    def _refresh(self) -> None:
        while True:
            status, body = call("GET", f"{self.fx}/v1/rates", headers={"X-Client": "pricing"})
            if status == 200:
                with self.lock:
                    self.rates, self.fetched = body["rates"], time.time()
            else:
                log_json("warn", "fx rates refresh failed", status=status)
            # Retries quickly only until the first rates arrive; after that a failed refresh waits half a minute.
            time.sleep(60 if status == 200 else 30 if self.fetched else 5)

    def quote(self, request: Request, match):
        body = request.json()
        currency = body.get("currency", "USD")
        subtotal = sum(item["price_usd"] * item.get("qty", 1) for item in body.get("items", []))
        if self.settings.get("promo.autumn_sale"):
            subtotal *= 1 - self.settings.get("promo.discount_pct") / 100
        if currency == "USD":
            return 200, {"currency": currency, "total": round(subtotal, 2)}
        with self.lock:
            age, rate = time.time() - self.fetched, self.rates.get(currency)
        if rate is None or age > self.max_age_s:
            log_json("error", "quote failed: FX rates stale", currency=currency, rateAgeS=round(age))
            return 502, {"error": "fx_unavailable"}
        return 200, {"currency": currency, "total": round(subtotal * rate, 2)}


if __name__ == "__main__":
    serve(Pricing)
