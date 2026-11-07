import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from common import Request, Service, call, log_json, serve

USER_AGENT = "feed-exporter/2.4"


class FeedExporter(Service):
    """Keeps the marketing feeds (shopping ads, affiliates) in step with the catalog: exports products updated
    since the last run every few minutes, and on request re-exports the whole catalog for each locale, streaming
    its shards from catalog-api in parallel."""

    def __init__(self) -> None:
        super().__init__()
        self.catalog = os.environ["CATALOG_URL"]
        self.every_s = float(os.environ.get("INCREMENTAL_EVERY_S", "180"))
        self.route("POST", r"/internal/export", self.full)

    def start(self) -> None:
        threading.Thread(target=self._incremental, daemon=True).start()

    def _get(self, path: str, timeout: float) -> tuple[int, dict]:
        return call("GET", f"{self.catalog}{path}", headers={"User-Agent": USER_AGENT}, timeout=timeout)

    def _incremental(self) -> None:
        while True:
            time.sleep(self.every_s)
            since, products = int(time.time() - self.every_s), 0
            for page in (1, 2, 3):
                status, body = self._get(f"/v1/products?updated_since={since}&page={page}&limit=200", 10)
                if status != 200:
                    log_json("warn", "incremental export page failed", page=page, status=status)
                    break
                products += len(body.get("products", []))
            log_json("info", "incremental export done", products=products, feeds=["google-shopping", "affiliates"])

    def _shard(self, locale: str, shard: int, of: int) -> None:
        for _ in range(5):
            status, _ = self._get(f"/v1/products/export?shard={shard}&of={of}&locale={locale}", 300)
            if status == 200:
                return
            time.sleep(2)
        log_json("error", "export shard failed", locale=locale, shard=shard, of=of)

    def full(self, request: Request, match):
        body = request.json()
        workers, locales = int(body.get("workers", 4)), body.get("locales", ["en-US"])

        def run() -> None:
            log_json("info", "full export started", locales=locales, shards=workers)
            started = time.time()
            for locale in locales:
                with ThreadPoolExecutor(workers) as pool:
                    list(pool.map(lambda shard, locale=locale: self._shard(locale, shard, workers), range(workers)))
                log_json("info", "locale exported", locale=locale, shards=workers)
            log_json("info", "full export done", locales=len(locales), tookS=round(time.time() - started))

        threading.Thread(target=run, daemon=True).start()
        return 202, {"started": True}


if __name__ == "__main__":
    serve(FeedExporter)
