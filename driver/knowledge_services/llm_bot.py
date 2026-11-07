import os

from common import Request, Service, call, log_json, serve


class LlmBot(Service):
    """Hands a conversation to a human agent when the bot cannot help."""

    def __init__(self) -> None:
        super().__init__()
        self.ticketing = os.environ["TICKETING_URL"]
        self.route("POST", r"/conversations/(?P<conversation>[^/]+)/handoff", self.handoff)

    def handoff(self, request: Request, match):
        body = request.json()
        conversation = match["conversation"]
        log_json(
            "info",
            "conversation escalated to human agent",
            conversation_id=conversation,
            company_marker=body["company_marker"],
            intent=body.get("intent", "speak_to_agent"),
        )
        status, reply = call(
            "POST",
            f"{self.ticketing}/tickets",
            {"conversation_id": conversation, "company_marker": body["company_marker"], "queue": body["queue"]},
        )
        if status >= 400:
            log_json("error", "HITL ticket creation failed", conversation_id=conversation, status=status)
            return 502, {"error": "handoff failed"}
        log_json("info", "HITL ticket created", conversation_id=conversation, ticket_id=reply.get("ticket_id"))
        return 200, reply


if __name__ == "__main__":
    serve(LlmBot)
