import os
import threading
import time

from common import Request, Service, call, log_json, serve


class MeetingRunner(Service):
    """Runs each scheduled meeting task when it falls due; a task that fails three times is dead-lettered."""

    MAX_ATTEMPTS = 3
    RETRY_AFTER_S = 60

    def __init__(self) -> None:
        super().__init__()
        self.calendar = os.environ["CALENDAR_API_URL"]
        self.tasks: list[dict] = []
        self.dlq: list[dict] = []
        self.lock = threading.Lock()
        self.route("POST", r"/schedule", self.schedule)
        self.route("GET", r"/dlq", self.dead_letters)

    def start(self) -> None:
        threading.Thread(target=self._loop, daemon=True).start()

    def schedule(self, request: Request, match):
        with self.lock:
            for task in request.json()["tasks"]:
                self.tasks.append({**task, "attempt": 1})
        return 200, {"scheduled": True}

    def dead_letters(self, request: Request, match):
        with self.lock:
            return 200, {"queue": "meeting-runner-dlq", "depth": len(self.dlq)}

    def _loop(self) -> None:
        while True:
            time.sleep(1)
            with self.lock:
                due = [t for t in self.tasks if t["at"] <= time.time()]
                self.tasks = [t for t in self.tasks if t["at"] > time.time()]
            for task in due:
                threading.Thread(target=self._run, args=(task,), daemon=True).start()

    def _run(self, task: dict) -> None:
        context = {"meetingId": task["meetingId"], "calendarEventId": task["eventId"], "userId": task["userId"]}
        log_json("info", "Processing scheduled task", meetingId=task["meetingId"], attempt=task["attempt"])
        status, body = call("GET", f"{self.calendar}/v1/users/{task['userId']}/events/{task['eventId']}")
        if status < 400:
            log_json("info", "meeting bot dispatched", meetingId=task["meetingId"])
            return
        log_json("error", "Error fetching calendar event", **context, error={"status": status, "responseBody": body})
        log_json(
            "error",
            "Error fetching calendar event Error response from calendar service: 500",
            **context,
            error={"kind": "CalendarServiceError", "status": status},
        )
        log_json(
            "error",
            "Error processing scheduled task",
            **context,
            error={"kind": "CalendarServiceError", "message": f"Error response from calendar service: {status}"},
        )
        if task["attempt"] >= self.MAX_ATTEMPTS:
            log_json(
                "error",
                "Invoke Error CalendarServiceError: Error response from calendar service: 500",
                userId=task["userId"],
                attempt=task["attempt"],
            )
            log_json(
                "warning", "task moved to dead-letter queue", queue="meeting-runner-dlq", meetingId=task["meetingId"]
            )
            with self.lock:
                self.dlq.append(task)
            return
        with self.lock:
            self.tasks.append({**task, "attempt": task["attempt"] + 1, "at": time.time() + self.RETRY_AFTER_S})


if __name__ == "__main__":
    serve(MeetingRunner)
