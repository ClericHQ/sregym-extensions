import threading
import time

from common import Request, Service, log_json, serve


class AddressValidation(Service):
    """Checks that the carrier delivers to a shipping address: its postcode must be in the carrier's service-area
    table. The carrier publishes the table to our SFTP drop a few times an hour, and each publish is loaded on
    arrival."""

    def __init__(self) -> None:
        super().__init__()
        self.lock = threading.Lock()
        self.postcodes: frozenset[str] = frozenset()
        self.file, self.loaded = "", 0.0
        self.route("POST", r"/v1/validate", self.validate)
        self.route("POST", r"/internal/service-area", self.load)

    def start(self) -> None:
        threading.Thread(target=self._gauge, daemon=True).start()

    def _gauge(self) -> None:
        while True:
            time.sleep(60)
            with self.lock:
                rows, file, loaded = len(self.postcodes), self.file, self.loaded
            log_json("info", "service-area table in use", rows=rows, file=file, loadedAgoS=round(time.time() - loaded))

    def load(self, request: Request, match):
        table = request.json()
        postcodes = frozenset(table["postcodes"])
        with self.lock:
            self.postcodes, self.file, self.loaded = postcodes, table["file"], time.time()
        log_json("info", "service-area table loaded", file=table["file"], rows=len(postcodes))
        return 200, {"rows": len(postcodes)}

    def validate(self, request: Request, match):
        address = request.json()
        if address.get("postcode") not in self.postcodes:
            return 200, {"deliverable": False, "reason": "outside_service_area"}
        return 200, {"deliverable": True}


if __name__ == "__main__":
    serve(AddressValidation)
