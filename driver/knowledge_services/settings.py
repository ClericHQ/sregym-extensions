import threading
import uuid

from common import Request, Service, log_json, serve


class SettingsStore(Service):
    """Runtime settings per service, changed by engineers from the admin console without a deploy.

    Each service polls its own settings. Every change is recorded as an info-level `setting changed` event with
    who made it, the previous value and the new one.
    """

    def __init__(self) -> None:
        super().__init__()
        self.lock = threading.Lock()
        self.revision = 0
        self.route("GET", r"/v1/settings/(?P<service>[a-z0-9-]+)", self.read)
        self.route("POST", r"/v1/settings/(?P<service>[a-z0-9-]+)", self.change)

    def read(self, request: Request, match):
        values = self.state.get("settings", {}).get(match["service"], {})
        return 200, {"service": match["service"], "revision": self.revision, "values": values}

    def change(self, request: Request, match):
        body = request.json()
        service, key, value, actor = match["service"], body["key"], body["value"], body.get("actor", "")
        with self.lock:
            settings = self.state.setdefault("settings", {}).setdefault(service, {})
            previous = settings.get(key)
            settings[key] = value
            self.revision += 1
            revision = self.revision
        log_json(
            "info",
            "setting changed",
            service=service,
            key=key,
            previous=previous,
            value=value,
            actor=actor,
            comment=body.get("comment", ""),
            revision=revision,
            changeId=f"chg_{uuid.uuid4().hex[:12]}",
        )
        return 200, {"revision": revision}


if __name__ == "__main__":
    serve(SettingsStore)
