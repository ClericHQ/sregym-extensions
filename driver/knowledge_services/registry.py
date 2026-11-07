import socket

from common import PORT, Request, Service, log_text, serve


class Registry(Service):
    """Service discovery by logical name: services ask for a capability ("p13n-store", "catalog") and get the address
    of whichever service provides it today, so a backend can move without a redeploy of its clients."""

    def __init__(self) -> None:
        super().__init__()
        self.route("GET", r"/v1/resolve/(?P<name>[a-z0-9-]+)", self.resolve)

    def seeded(self) -> None:
        log_text("info", f"registry loaded {len(self.state.get('names', {}))} names")

    def resolve(self, request: Request, match):
        target = self.state.get("names", {}).get(match["name"])
        if target is None:
            return 404, {"error": "unknown name"}
        service, namespace = target.split(".")
        address = socket.gethostbyname(f"{service}.{namespace}.svc.cluster.local")
        return 200, {"service": service, "address": f"{address}:{PORT}"}


if __name__ == "__main__":
    serve(Registry)
