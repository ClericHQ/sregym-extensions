import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from common import Request, Service, call, log_json, serve

POS_HUB = os.environ["POS_HUB_URL"]
TTL_S = float(os.environ.get("MENU_CACHE_TTL_S", "180"))


class Menu(Service):
    """Restaurant menus for the customer app. A menu comes from the restaurant's POS through pos-hub and is cached
    for MENU_CACHE_TTL_S; a menu.updated webhook drops it. Orderly's menus carry no stock: which items are sold out
    comes only from item availability webhooks, kept here and applied to every menu served. POST /internal/warm loads
    a list of menus into the cache ahead of requests."""

    def __init__(self) -> None:
        super().__init__()
        self.lock = threading.Lock()
        self.sold_out: dict[str, set[str]] = {}
        self.cache: dict[str, tuple[float, dict]] = {}
        self.minute: list[tuple[bool, int]] = []
        self.pool = ThreadPoolExecutor(max_workers=6)
        self.route("GET", r"/v1/menus/(?P<id>[^/]+)", self.get)
        self.route("POST", r"/internal/warm", self.warm)
        self.route("POST", r"/internal/invalidate", self.invalidate)
        self.route("POST", r"/internal/availability", self.availability)
        self.route("GET", r"/stats", self.stats)

    def start(self) -> None:
        threading.Thread(target=self._summary, daemon=True).start()

    def _fetch(self, restaurant: str) -> dict | None:
        status, menu = call("GET", f"{POS_HUB}/v1/pos-menus/{restaurant}", headers={"X-Client": "menu"}, timeout=10)
        if status != 200:
            log_json("warn", "menu fetch failed", restaurant=restaurant, status=status)
            return None
        with self.lock:
            self.cache[restaurant] = (time.time() + TTL_S, menu)
        return menu

    def get(self, request: Request, match):
        started, restaurant = time.time(), match["id"]
        with self.lock:
            expires, menu = self.cache.get(restaurant, (0.0, None))
        hit = menu is not None and expires > started
        if not hit:
            menu = self._fetch(restaurant)
        ms = round((time.time() - started) * 1000)
        with self.lock:
            self.minute.append((hit, ms))
        log_json("info", "menu served", restaurant=restaurant, cache="hit" if hit else "miss", ms=ms)
        if menu is None:
            return 502, {"error": "menu unavailable"}
        with self.lock:
            sold_out = set(self.sold_out.get(restaurant, ()))
        items = [{**item, "available": item["id"] not in sold_out} for item in menu.get("items", [])]
        return 200, {**menu, "items": items}

    def warm(self, request: Request, match):
        restaurants = request.json().get("restaurants", [])
        started = time.time()
        loaded = sum(1 for menu in self.pool.map(self._fetch, restaurants) if menu is not None)
        seconds = round(time.time() - started, 1)
        log_json("info", "menus warmed", requested=len(restaurants), loaded=loaded, seconds=seconds)
        return 200, {"loaded": loaded}

    def invalidate(self, request: Request, match):
        restaurant = request.json()["restaurant"]
        with self.lock:
            self.cache.pop(restaurant, None)
        return 200, {}

    def availability(self, request: Request, match):
        body = request.json()
        with self.lock:
            sold_out = self.sold_out.setdefault(body["restaurant"], set())
            if body["available"]:
                sold_out.discard(body["item"])
            else:
                sold_out.add(body["item"])
        return 200, {}

    def stats(self, request: Request, match):
        with self.lock:
            return 200, {"entries": len(self.cache)}

    def _summary(self) -> None:
        while True:
            time.sleep(60)
            now = time.time()
            with self.lock:
                minute, self.minute = self.minute, []
                fresh = sum(1 for expires, _ in self.cache.values() if expires > now)
                sold_out = sum(len(items) for items in self.sold_out.values())
            hits = sum(1 for hit, _ in minute if hit)
            times = sorted(ms for _, ms in minute) or [0]
            log_json(
                "info",
                "menu requests in the last minute",
                requests=len(minute),
                cacheHits=hits,
                cacheMisses=len(minute) - hits,
                hitRatio=round(hits / len(minute), 3) if minute else None,
                p95Ms=times[int(len(times) * 0.95)],
                cachedMenus=fresh,
                soldOutItems=sold_out,
            )


if __name__ == "__main__":
    serve(Menu)
