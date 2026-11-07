import json
import queue
import threading

from common import Request, Service, log_text, serve


class PushHub(Service):
    """Fans a publish out to whoever is subscribed to the channel at that moment; keeps nothing."""

    def __init__(self) -> None:
        super().__init__()
        self.subscribers: dict[str, list[queue.Queue]] = {}
        self.lock = threading.Lock()
        self.route("GET", r"/subscribe", self.subscribe)
        self.route("POST", r"/publish", self.publish)

    def subscribe(self, request: Request, match):
        channel, origin = request.query.get("channel", ""), request.headers.get("X-Forwarded-Host", "push-api")
        inbox: queue.Queue = queue.Queue()
        with self.lock:
            self.subscribers.setdefault(channel, []).append(inbox)
        log_text("info", f"[handler] subscribe channel={channel} from=http://{origin}/connect/sse")
        request.send_response(200)
        request.send_header("Content-Type", "text/event-stream")
        request.send_header("Cache-Control", "no-cache")
        request.end_headers()
        try:
            request.wfile.write(b"retry: 3000\n\n")
            request.wfile.flush()
            while True:
                try:
                    message = inbox.get(timeout=15)
                    request.wfile.write(f"data: {json.dumps(message)}\n\n".encode())
                except queue.Empty:
                    request.wfile.write(b": keep-alive\n\n")
                request.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass
        finally:
            with self.lock:
                self.subscribers.get(channel, []).remove(inbox)
            log_text("info", f"[handler] unsubscribe channel={channel}")
            request.close_connection = True
        return None

    def publish(self, request: Request, match):
        body = request.json()
        channel = body["channel"]
        with self.lock:
            inboxes = list(self.subscribers.get(channel, []))
        for inbox in inboxes:
            inbox.put(body.get("message"))
        log_text("info", f"[handler] publish channel={channel} receivers={len(inboxes)}")
        return 200, {"receivers": len(inboxes)}


if __name__ == "__main__":
    serve(PushHub)
