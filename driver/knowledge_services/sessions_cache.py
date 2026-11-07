import os
import threading
import time
from collections import OrderedDict

from common import Request, Service, log_logfmt, serve


class KeyValueCache(Service):
    """An in-memory key-value cache with a memory budget (MAX_BYTES). Past the budget it evicts the least recently
    used keys; entries also expire after their TTL."""

    def __init__(self) -> None:
        super().__init__()
        self.max_bytes = int(os.environ.get("MAX_BYTES", str(32 * 1024 * 1024)))
        self.lock = threading.Lock()
        self.items: OrderedDict[str, tuple[str, float]] = OrderedDict()
        self.bytes = 0
        self.counters = {"hits": 0, "misses": 0, "sets": 0, "evictions": 0, "expired": 0}
        self.route("GET", r"/kv/(?P<key>[A-Za-z0-9:_.-]+)", self.get)
        self.route("POST", r"/kv/(?P<key>[A-Za-z0-9:_.-]+)", self.put)

    def start(self) -> None:
        threading.Thread(target=self._report, daemon=True).start()

    def _drop(self, key: str) -> None:
        value, _ = self.items.pop(key)
        self.bytes -= len(key) + len(value)

    def get(self, request: Request, match):
        key = match["key"]
        with self.lock:
            entry = self.items.get(key)
            if entry is not None and entry[1] < time.time():
                self._drop(key)
                self.counters["expired"] += 1
                entry = None
            if entry is None:
                self.counters["misses"] += 1
                return 404, {"error": "not found"}
            self.items.move_to_end(key)
            self.counters["hits"] += 1
            return 200, {"value": entry[0]}

    def put(self, request: Request, match):
        key, body = match["key"], request.json()
        value, expires = body["value"], time.time() + float(body.get("ttl_s", 3600))
        with self.lock:
            if key in self.items:
                self._drop(key)
            self.items[key] = (value, expires)
            self.bytes += len(key) + len(value)
            self.counters["sets"] += 1
            while self.bytes > self.max_bytes and self.items:
                self._drop(next(iter(self.items)))
                self.counters["evictions"] += 1
        return 200, {}

    def _report(self) -> None:
        while True:
            time.sleep(30)
            with self.lock:
                counters, self.counters = self.counters, dict.fromkeys(self.counters, 0)
                items, used = len(self.items), self.bytes
            log_logfmt("info", "stats", items=items, bytes=used, max_bytes=self.max_bytes, **counters)


if __name__ == "__main__":
    serve(KeyValueCache)
