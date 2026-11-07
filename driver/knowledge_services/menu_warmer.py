import os
import threading
import time

from common import Service, call, log_json, serve

MENU = os.environ["MENU_URL"]
EVERY_S = float(os.environ.get("WARM_EVERY_S", "60"))


class MenuWarmer(Service):
    """Keeps the menus of open restaurants loaded in menu's cache, so customers browsing at mealtimes don't wait on
    the restaurants' POS systems: every WARM_EVERY_S it asks menu to load the menus of every restaurant open now."""

    def start(self) -> None:
        threading.Thread(target=self._run, daemon=True).start()

    def _run(self) -> None:
        while True:
            restaurants = [r["id"] for r in self.state.get("restaurants", []) if r.get("open", True)]
            if not restaurants:
                time.sleep(2)
                continue
            started = time.time()
            status, body = call(
                "POST", f"{MENU}/internal/warm", {"restaurants": restaurants}, {"X-Client": "menu-warmer"}, 55
            )
            log_json(
                "info" if status == 200 else "warn",
                "warm run",
                restaurants=len(restaurants),
                loaded=body.get("loaded") if status == 200 else 0,
                status=status,
                seconds=round(time.time() - started, 1),
            )
            time.sleep(EVERY_S)


if __name__ == "__main__":
    serve(MenuWarmer)
