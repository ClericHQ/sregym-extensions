import random
import time

from common import Request, Service, log_json, serve

WORDS = ["jacket", "boots", "lamp", "mug", "scarf", "backpack", "desk", "kettle", "rug", "tent", "watch", "chair"]
ADJECTIVES = ["wool", "leather", "oak", "linen", "steel", "cotton", "ceramic", "canvas", "walnut", "recycled"]
PRODUCTS = [
    {
        "sku": f"sku-{i:05d}",
        "title": f"{ADJECTIVES[i % len(ADJECTIVES)]} {WORDS[(i // 7) % len(WORDS)]}",
        "price_usd": round(5 + (i * 37 % 400) + 0.99, 2),
    }
    for i in range(12000)
]


class CatalogApi(Service):
    """The product catalog: search for the storefront and recommendations, pages of recently updated products, and
    a streamed export of the whole catalog, in shards, for marketing feeds."""

    def __init__(self) -> None:
        super().__init__()
        self.route("GET", r"/v1/products/search", self.search)
        self.route("GET", r"/v1/products", self.page)
        self.route("GET", r"/v1/products/export", self.export)

    def search(self, request: Request, match):
        query, started = request.query.get("q", ""), time.time()
        time.sleep(random.uniform(0.01, 0.04))
        hits = [p for p in PRODUCTS[random.randrange(0, 11000) :][:400] if query in str(p["title"])][:10]
        if random.random() < 0.1:
            log_json("info", "search", q=query, results=len(hits), tookMs=round((time.time() - started) * 1000))
        return 200, {"products": hits}

    def page(self, request: Request, match):
        page, limit = int(request.query.get("page", "1")), int(request.query.get("limit", "200"))
        time.sleep(random.uniform(0.1, 0.3))
        return 200, {"page": page, "products": PRODUCTS[(page - 1) * limit : page * limit]}

    def export(self, request: Request, match):
        shard, of = int(request.query.get("shard", "0")), int(request.query.get("of", "1"))
        products = PRODUCTS[shard::of]
        # Streams the shard a chunk at a time, enriching each product with stock and media from their stores.
        for _ in range(0, len(products), 25):
            time.sleep(random.uniform(0.4, 0.8))
        return 200, {"shard": shard, "of": of, "locale": request.query.get("locale", "en-US"), "count": len(products)}


if __name__ == "__main__":
    serve(CatalogApi)
