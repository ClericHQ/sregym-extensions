import json
import os
import random
import threading
import time

from common import Service, call, log_json, log_text, resolve, serve

# How many candidates each model keeps per shopper, with their ranking features.
CANDIDATES = {"v6": 20, "v7": 400}


class RecsBuilder(Service):
    """Keeps active shoppers' product recommendations fresh: every few seconds it scores a batch of recently active
    shoppers against catalog-api's search and stores each shopper's list in the p13n store, which the apps read."""

    def __init__(self) -> None:
        super().__init__()
        self.catalog = os.environ["CATALOG_URL"]
        self.model = os.environ.get("MODEL", "v6")
        self.batch = int(os.environ.get("BATCH", "25"))
        self.every_s = float(os.environ.get("REFRESH_EVERY_S", "10"))

    def start(self) -> None:
        threading.Thread(target=self._refresh, daemon=True).start()

    def _products(self) -> list[str]:
        query = random.choice(["jacket", "boots", "lamp", "mug", "scarf", "backpack", "desk", "kettle"])
        status, body = call("GET", f"{self.catalog}/v1/products/search?q={query}")
        return [p["sku"] for p in body.get("products", [])] if status == 200 else []

    def _refresh(self) -> None:
        store, store_service = resolve(os.environ.get("P13N_STORE", "p13n-store"))
        log_text("info", f"p13n store resolved to {store.removeprefix('http://')}")
        candidates = CANDIDATES.get(self.model, 20)
        while True:
            time.sleep(self.every_s)
            shoppers = self.state.get("shoppers", [])
            if not shoppers:
                continue
            skus = self._products() or ["sku-00000"]
            for shopper in random.sample(shoppers, min(self.batch, len(shoppers))):
                items = [
                    {
                        "sku": random.choice(skus),
                        "score": round(random.random(), 4),
                        "f": [round(random.random(), 3) for _ in range(8)],
                    }
                    for _ in range(candidates)
                ]
                value = json.dumps({"model": self.model, "items": items})
                call("POST", f"{store}/kv/p13n:{shopper}", {"value": value, "ttl_s": 6 * 3600}, peer=store_service)
            log_json("info", "recommendations refreshed", model=self.model, shoppers=self.batch)


if __name__ == "__main__":
    serve(RecsBuilder)
