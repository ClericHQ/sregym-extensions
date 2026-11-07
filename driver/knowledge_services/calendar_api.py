import random

from common import Request, Service, log_json, serve


class CalendarApi(Service):
    """Reads a user's calendar events from Google Calendar with the user's stored OAuth credentials."""

    # Google refuses these for good (a dead grant, scopes the user never granted, an event gone): retrying with the
    # same credentials cannot succeed, so the caller gets permanent_google_api_failure.
    PERMANENT = frozenset({"invalid_grant", "insufficientPermissions", "forbidden", "notFound", "deleted"})

    def __init__(self) -> None:
        super().__init__()
        self.route("GET", r"/v1/users/(?P<user>[0-9a-f]{24})/events/(?P<event>[^/]+)", self.event)

    # Google Calendar's events.get with the user's credentials, as the Google client reports it: None, or the error
    # reason Google gave.
    def _google_events_get(self, user: str, event: str) -> str | None:
        return self.state.get("google_errors", {}).get(user)

    def event(self, request: Request, match):
        user, event = match["user"], match["event"]
        reason = self._google_events_get(user, event)
        if reason in self.PERMANENT:
            log_json(
                "error",
                "Error fetching event by ID: permanent_google_api_failure",
                name="InternalServerError",
                userId=user,
                eventId=event,
                statusCode=500,
            )
            return 500, {"message": "InternalServerError: permanent_google_api_failure"}
        if reason is not None:
            log_json("warning", "transient Google error fetching event", userId=user, eventId=event)
            return 503, {"message": "ServiceUnavailable"}
        log_json("info", "fetched calendar event", userId=user, eventId=event, durationMs=random.randint(40, 220))
        return 200, {"id": event, "status": "confirmed"}


if __name__ == "__main__":
    serve(CalendarApi)
