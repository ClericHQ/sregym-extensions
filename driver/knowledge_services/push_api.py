# ruff: noqa: S310 (plain HTTP between in-cluster services)
import os
import urllib.error
import urllib.parse
import urllib.request

from common import POD, Request, Service, call, log_json, serve


class PushApi(Service):
    """Publishes through push-hub and serves desks their SSE channel, relayed from push-hub."""

    def __init__(self) -> None:
        super().__init__()
        self.hub = os.environ["PUSH_HUB_URL"]
        self.route("POST", r"/publish", self.publish)
        self.route("GET", r"/connect/sse", self.connect)

    def publish(self, request: Request, match):
        body = request.json()
        status, _ = call("POST", f"{self.hub}/publish", body)
        if status >= 400:
            log_json("error", "failed to publish message to push-hub", channel=body.get("channel"), status=status)
            return 502, {"error": "publish failed"}
        log_json("info", "message published to push-hub", channel=body.get("channel"), pod=POD)
        return 200, {"published": True}

    def connect(self, request: Request, match):
        channel, external_id = request.query.get("channel", ""), request.query.get("externalId", "")
        log_json(
            "info",
            f"new subscription coming via Server-sent-events from user with externalId {external_id}",
            externalId=external_id,
            pod=POD,
        )
        log_json("info", f"connecting via sse to channel {channel}", channel=channel, pod=POD)
        upstream = urllib.request.Request(
            f"{self.hub}/subscribe?channel={urllib.parse.quote(channel)}",
            headers={"X-Forwarded-Host": "push-api.realtime.svc/push-api"},
        )
        try:
            with urllib.request.urlopen(upstream, timeout=60) as stream:
                request.send_response(200)
                request.send_header("Content-Type", "text/event-stream")
                request.send_header("Cache-Control", "no-cache")
                request.end_headers()
                for line in stream:
                    request.wfile.write(line)
                    request.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError, urllib.error.URLError):
            pass
        finally:
            log_json("info", "sse connection closed", channel=channel, pod=POD)
            request.close_connection = True
        return None


if __name__ == "__main__":
    serve(PushApi)
