from __future__ import annotations

import os
from pathlib import Path

import pytest

from fundlab.common.local_env import load_local_environment
from fundlab.settings import load_foundation_settings


def test_local_environment_loads_values_without_overriding_process(tmp_path, monkeypatch):
    source = tmp_path / ".env.local"
    source.write_text(
        "# local only\n"
        "FUNDLAB_SMTP_HOST=smtp.qq.com\n"
        "FUNDLAB_SMTP_PORT='465'\n"
        "FUNDLAB_SMTP_USER=file@qq.com\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("FUNDLAB_SMTP_USER", "process@qq.com")
    monkeypatch.delenv("FUNDLAB_SMTP_HOST", raising=False)
    monkeypatch.delenv("FUNDLAB_SMTP_PORT", raising=False)

    load_local_environment(source)

    assert os.environ["FUNDLAB_SMTP_HOST"] == "smtp.qq.com"
    assert os.environ["FUNDLAB_SMTP_PORT"] == "465"
    assert os.environ["FUNDLAB_SMTP_USER"] == "process@qq.com"


def test_local_environment_rejects_malformed_entries_without_echoing_secret(
    tmp_path, monkeypatch,
):
    source = tmp_path / ".env.local"
    source.write_text("SMTP PASSWORD secret-value\n", encoding="utf-8")
    monkeypatch.delenv("SMTP", raising=False)

    with pytest.raises(ValueError) as raised:
        load_local_environment(source)

    assert "secret-value" not in str(raised.value)


def test_local_environment_rejects_unrelated_process_configuration(tmp_path):
    source = tmp_path / ".env.local"
    source.write_text("PATH=untrusted\n", encoding="utf-8")

    with pytest.raises(ValueError, match="Unsupported local environment name"):
        load_local_environment(source)


def test_notification_recipient_defaults_to_smtp_user_and_allows_override(monkeypatch):
    config = Path(__file__).resolve().parents[2] / "config" / "fundlab.yaml"
    monkeypatch.setenv("FUNDLAB_SMTP_USER", "sender@qq.com")
    monkeypatch.delenv("FUNDLAB_EMAIL_TO", raising=False)

    assert load_foundation_settings(config).agent.notify.email_to == "sender@qq.com"

    monkeypatch.setenv("FUNDLAB_EMAIL_TO", "receiver@example.com")
    assert load_foundation_settings(config).agent.notify.email_to == "receiver@example.com"
