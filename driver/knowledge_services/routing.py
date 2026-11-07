import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

from common import Request, Service, agent_channel, call, log_text, serve


class Routing(Service):
    """Queues each ticket for its agent and offers it once the agent's desk is connected."""

    HEARTBEAT_TIMEOUT_S = 30

    def __init__(self) -> None:
        super().__init__()
        self.push, self.ticketing = os.environ["PUSH_API_URL"], os.environ["TICKETING_URL"]
        self.agents: dict[str, dict] = {}
        self.pending: dict[str, list[dict]] = {}
        self.lock = threading.Lock()
        self.route("POST", r"/tasks", self.add_task)
        self.route("POST", r"/tasks/(?P<task>[^/]+)/accept", self.accept)
        self.route("POST", r"/agents/(?P<agent>[^/]+)/heartbeat", self.heartbeat)
        self.route("GET", r"/agents/(?P<agent>[^/]+)/tasks", self.agent_tasks)

    def start(self) -> None:
        threading.Thread(target=self._expire, daemon=True).start()

    def _expire(self) -> None:
        while True:
            time.sleep(5)
            with self.lock:
                expired = [
                    a
                    for a, s in self.agents.items()
                    if s["online"] and time.time() - s["seen"] > self.HEARTBEAT_TIMEOUT_S
                ]
                for agent in expired:
                    self.agents[agent]["online"] = False
            for agent in expired:
                log_text("warn", f"AgentManager::startAgentConsumer heartbeat expired agentId={agent}")

    def _offer(self, task: dict) -> None:
        agent, company = task["queueName"], task["companyMarker"]
        log_text(
            "info",
            "AgentManager::SendTicketPreassignedEventToAgent sending ticket preassigned event to agent "
            f"agentId={agent} taskId={task['taskId']}",
        )
        message = {"type": "TICKET_PREASSIGNED", "taskId": task["taskId"], "companyMarker": company}
        call("POST", f"{self.push}/publish", {"channel": agent_channel(company, agent), "message": message})

    def add_task(self, request: Request, match):
        task = request.json()
        agent = task["queueName"]
        log_text(
            "info", f"TaskManager::AddTask successfully added task to queue taskId={task['taskId']} queueName={agent}"
        )
        with self.lock:
            online = self.agents.get(agent, {}).get("online", False)
            if not online:
                self.pending.setdefault(agent, []).append(task)
        if online:
            self._offer(task)
        return 200, {"queued": True}

    def heartbeat(self, request: Request, match):
        agent = urllib.parse.unquote(match["agent"])
        with self.lock:
            state = self.agents.setdefault(agent, {"online": False, "seen": 0})
            state["seen"] = time.time()
        return 200, {"ok": True}

    def agent_tasks(self, request: Request, match):
        agent = urllib.parse.unquote(match["agent"])
        log_text("info", f"AgentManager::GetAgentTasks getting tasks for agent agentId={agent}")
        with self.lock:
            self.agents[agent] = {"online": True, "seen": time.time()}
            waiting = self.pending.pop(agent, [])
        for task in waiting:
            self._offer(task)
        return 200, {"tasks": [t["taskId"] for t in waiting]}

    def accept(self, request: Request, match):
        agent = request.json().get("agentId", "")
        log_text(
            "info", f"AgentManager::HandleAgentAcceptTask agent accepted task agentId={agent} taskId={match['task']}"
        )
        call("POST", f"{self.ticketing}/tickets/{match['task']}/assign", {"agentId": agent})
        return 200, {"accepted": True}


if __name__ == "__main__":
    serve(Routing)
