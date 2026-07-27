"""Best-effort SMTP notification for validated opportunities and risks."""

from __future__ import annotations

import os
import smtplib
import ssl
from dataclasses import dataclass
from email.header import Header
from email.mime.text import MIMEText
from email.utils import formataddr, parseaddr


@dataclass(frozen=True)
class EmailOutcome:
    sent: bool
    detail: str


@dataclass(frozen=True)
class EmailNotifier:
    recipient: str | None

    @property
    def configured(self) -> bool:
        return bool(
            self.recipient
            and os.environ.get("FUNDLAB_SMTP_HOST")
            and os.environ.get("FUNDLAB_SMTP_USER")
            and os.environ.get("FUNDLAB_SMTP_PASSWORD")
        )

    def send(self, subject: str, body: str, *, message_id: str | None = None) -> EmailOutcome:
        if not self.recipient:
            return EmailOutcome(False, "no recipient configured (agent.notify.email_to)")
        if "\r" in self.recipient or "\n" in self.recipient:
            return EmailOutcome(False, "invalid recipient")
        _, parsed_recipient = parseaddr(self.recipient)
        if not parsed_recipient or "@" not in parsed_recipient:
            return EmailOutcome(False, "invalid recipient")
        host = os.environ.get("FUNDLAB_SMTP_HOST", "")
        user = os.environ.get("FUNDLAB_SMTP_USER", "")
        password = os.environ.get("FUNDLAB_SMTP_PASSWORD", "")
        if not (host and user and password):
            return EmailOutcome(
                False,
                "SMTP environment not configured "
                "(FUNDLAB_SMTP_HOST/FUNDLAB_SMTP_USER/FUNDLAB_SMTP_PASSWORD)",
            )
        try:
            port = int(os.environ.get("FUNDLAB_SMTP_PORT", "465"))
        except ValueError:
            return EmailOutcome(False, "FUNDLAB_SMTP_PORT is not an integer")
        message = MIMEText(body, "plain", "utf-8")
        message["Subject"] = Header(subject.replace("\r", " ").replace("\n", " "), "utf-8")
        message["From"] = formataddr((str(Header("FundLab Agent", "utf-8")), user))
        message["To"] = parsed_recipient
        if message_id:
            message["Message-ID"] = f"<{message_id}@fundlab.local>"
        try:
            with smtplib.SMTP_SSL(
                host,
                port,
                context=ssl.create_default_context(),
                timeout=30,
            ) as smtp:
                smtp.login(user, password)
                smtp.sendmail(user, [parsed_recipient], message.as_string())
        except Exception as exc:  # noqa: BLE001 - notification cannot invalidate a decision
            return EmailOutcome(False, f"{type(exc).__name__}: {exc}")
        return EmailOutcome(True, f"sent to {parsed_recipient}")
