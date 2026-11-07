import os
import threading
import time

from common import Request, Service, agent_channel, call, log_json, serve, ulid


class Ticketing(Service):
    """Opens a ticket per handed-off conversation and escalates it when no agent takes it in time."""

    def __init__(self) -> None:
        super().__init__()
        self.routing, self.push = os.environ["ROUTING_URL"], os.environ["PUSH_API_URL"]
        self.timeout_s = float(os.environ.get("ASSIGNMENT_TIMEOUT_S", "180"))
        self.tickets: dict[str, dict] = {}
        self.lock = threading.Lock()
        self.route("POST", r"/tickets", self.create)
        self.route("POST", r"/tickets/(?P<ticket>[^/]+)/assign", self.assign)
        self.route("GET", r"/stats", self.stats)

    def _publish(self, ticket: dict, event: str) -> None:
        message_type = f"hitl-{ticket['company_marker']}--ticketId-{ticket['ticket_id']}"
        status, _ = call("POST", f"{self.push}/publish", {"channel": message_type, "message": {"event": event}})
        log_json(
            "info" if status < 400 else "error",
            "external.push_api.publish",
            message_type=message_type,
            event=event,
            status="ok" if status < 400 else "failed",
        )

    def create(self, request: Request, match):
        body = request.json()
        ticket = {
            "ticket_id": ulid(),
            "external_conversation_id": f"{body['conversation_id']}_int",
            "company_marker": body["company_marker"],
            "queue": body["queue"],
            "status": "CREATED",
            "created": time.time(),
        }
        log_json(
            "info",
            "ticket.create.attempt",
            external_conversation_id=ticket["external_conversation_id"],
            company_marker=ticket["company_marker"],
        )
        with self.lock:
            self.tickets[ticket["ticket_id"]] = ticket
        log_json(
            "info",
            "ticket.created",
            ticket_id=ticket["ticket_id"],
            external_conversation_id=ticket["external_conversation_id"],
            company_marker=ticket["company_marker"],
            status="CREATED",
        )
        log_json(
            "info", "ticket_assignment_timer scheduled", ticket_id=ticket["ticket_id"], delay_seconds=self.timeout_s
        )
        threading.Timer(self.timeout_s, self._timeout, args=(ticket["ticket_id"],)).start()
        self._publish(ticket, "created")
        call(
            "POST",
            f"{self.routing}/tasks",
            {"taskId": ticket["ticket_id"], "queueName": ticket["queue"], "companyMarker": ticket["company_marker"]},
        )
        return 200, {"ticket_id": ticket["ticket_id"]}

    def assign(self, request: Request, match):
        agent = request.json().get("agentId")
        with self.lock:
            ticket = self.tickets.get(match["ticket"])
            if ticket is None or ticket["status"] != "CREATED":
                return 409, {"error": "ticket is not open"}
            ticket["status"], ticket["agent"] = "ASSIGNED", agent
        log_json("info", "ticket.assigned", ticket_id=match["ticket"], agent_id=agent, transition="CREATED -> ASSIGNED")
        self._publish(ticket, "assigned")
        return 200, {"assigned": True}

    def _timeout(self, ticket_id: str) -> None:
        with self.lock:
            ticket = self.tickets[ticket_id]
            if ticket["status"] != "CREATED":
                return
            ticket["status"] = "ESCALATED"
        log_json(
            "warning",
            "ticket.timeout.unassigned",
            ticket_id=ticket_id,
            reason="TicketClosedReason.ASSIGNMENT_TIMEOUT",
            transition="CREATED -> ESCALATED",
            external_conversation_id=ticket["external_conversation_id"],
        )
        self._publish(ticket, "escalated")
        message = {"type": "TICKET_CLOSED", "taskId": ticket_id}
        call(
            "POST",
            f"{self.push}/publish",
            {"channel": agent_channel(ticket["company_marker"], ticket["queue"]), "message": message},
        )

    def stats(self, request: Request, match):
        with self.lock:
            tickets = list(self.tickets.values())
        return 200, {
            "created": len(tickets),
            "assigned": sum(t["status"] == "ASSIGNED" for t in tickets),
            "escalated": sum(t["status"] == "ESCALATED" for t in tickets),
        }


if __name__ == "__main__":
    serve(Ticketing)
