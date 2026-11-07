import os
import random
import threading
import time

from common import Service, log_logfmt, serve


class MailboxSync(Service):
    """Refreshes each connected mailbox's history from Google, round robin."""

    def __init__(self) -> None:
        super().__init__()
        self.batch = int(os.environ.get("REFRESH_BATCH", "40"))
        self.every_s = float(os.environ.get("REFRESH_EVERY_S", "10"))

    def start(self) -> None:
        threading.Thread(target=self._loop, daemon=True).start()

    def _loop(self) -> None:
        cursor = 0
        while True:
            time.sleep(self.every_s)
            mailboxes = self.state.get("mailboxes", [])
            if not mailboxes:
                continue
            for i in range(self.batch):
                mailbox = mailboxes[(cursor + i) % len(mailboxes)]
                if mailbox.get("revoked"):
                    log_logfmt(
                        "error",
                        "[ALARM-USED] User was logged out",
                        logger="GoogleAPIs",
                        mailbox=mailbox["email"],
                        errMessage="invalid_grant",
                        errCode=400,
                        action="cron.refreshMailboxHistory",
                    )
                elif random.random() < 0.2:
                    log_logfmt(
                        "info",
                        "mailbox history refreshed",
                        logger="sync",
                        mailbox=mailbox["email"],
                        messages=random.randint(0, 40),
                        action="cron.refreshMailboxHistory",
                    )
            cursor = (cursor + self.batch) % len(mailboxes)


if __name__ == "__main__":
    serve(MailboxSync)
