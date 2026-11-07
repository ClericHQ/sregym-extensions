import os
import threading
import time

from common import Flags, Request, Service, Settings, call, log_json, serve


class Search(Service):
    """Product search for the storefront: queries catalog-api and ranks the hits, boosting new arrivals, and boosting
    close title matches too while the search-semantic-ranking flag is on. With the search-local-currency-prices flag
    on, it shows prices in the shopper's currency at fx-gateway's rate, and in USD when fx-gateway has no rate for it."""

    def __init__(self) -> None:
        super().__init__()
        self.catalog, self.fx = os.environ["CATALOG_URL"], os.environ["FX_GATEWAY_URL"]
        self.settings = Settings("search", {"ranking.boost_new_arrivals": 1.2})
        self.flags = Flags("search", {"search-semantic-ranking": False, "search-local-currency-prices": False})
        self.lock = threading.Lock()
        self.outcomes: list[tuple[float, int]] = []
        self.route("GET", r"/v1/search", self.search)
        self.route("GET", r"/stats", self.stats)

    def _rank(self, hits: list[dict], query: str) -> list[dict]:
        boost = float(self.settings.get("ranking.boost_new_arrivals"))
        semantic = self.flags.enabled("search-semantic-ranking")

        def score(hit: dict) -> float:
            return hit["price_usd"] * boost + (1000 if semantic and query in hit["title"] else 0)

        return sorted(hits, key=lambda hit: -score(hit))

    def _local(self, hits: list[dict], currency: str) -> list[dict]:
        if currency == "USD" or not self.flags.enabled("search-local-currency-prices"):
            return hits
        status, body = call("GET", f"{self.fx}/v1/rates/USD/{currency}", headers={"X-Client": "search"})
        if status != 200:
            return hits
        return [{**hit, "price": round(hit["price_usd"] * body["rate"], 2), "currency": currency} for hit in hits]

    def search(self, request: Request, match):
        query, currency = request.query.get("q", ""), request.query.get("currency", "USD")
        status, body = call("GET", f"{self.catalog}/v1/products/search?q={query}", timeout=3)
        if status != 200:
            log_json("error", "catalog lookup failed", q=query, status=status)
            result = (503, {"error": "search_unavailable"})
        else:
            result = (200, {"q": query, "hits": self._local(self._rank(body["products"], query), currency)})
        with self.lock:
            self.outcomes.append((time.time(), result[0]))
        return result

    def stats(self, request: Request, match):
        cutoff = time.time() - 600
        with self.lock:
            self.outcomes = [o for o in self.outcomes if o[0] > cutoff]
            recent = list(self.outcomes)
        return 200, {"searches_10m": len(recent), "errors_10m": sum(s >= 500 for _, s in recent)}


if __name__ == "__main__":
    serve(Search)
