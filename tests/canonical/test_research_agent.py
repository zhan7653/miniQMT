import json

import pytest

from fundlab.agent.research_agent import ResearchAgent, ResearchAgentError


class _Response:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def read(self):
        return json.dumps(self.payload).encode("utf-8")


def test_research_agent_continues_after_read_only_tool(monkeypatch):
    monkeypatch.setenv("TEST_AGENT_KEY", "secret")
    responses = iter(
        [
            {
                "id": "r1",
                "status": "completed",
                "output": [
                    {
                        "type": "function_call",
                        "name": "lookup",
                        "call_id": "c1",
                        "arguments": "{}",
                    }
                ],
            },
            {
                "id": "r2",
                "status": "completed",
                "output": [
                    {
                        "type": "message",
                        "content": [{"type": "output_text", "text": "verified"}],
                    }
                ],
            },
        ]
    )
    seen = []

    def open_fn(request, timeout):
        seen.append(json.loads(request.data.decode("utf-8")))
        return _Response(next(responses))

    agent = ResearchAgent(
        base_url="https://example.test/v1",
        model="gpt-6-astra",
        api_key_env="TEST_AGENT_KEY",
        open_fn=open_fn,
        tools={
            "lookup": {
                "description": "read",
                "parameters": {
                    "type": "object",
                    "properties": {},
                    "additionalProperties": False,
                },
                "callable": lambda: {"value": 3},
                "write": False,
            }
        },
    )
    result = agent.research("check", max_turns=2)
    assert result["research_turns"] == 2
    assert seen[1]["input"][-1]["type"] == "function_call_output"


def test_research_agent_rejects_write_tool(monkeypatch):
    monkeypatch.setenv("TEST_AGENT_KEY", "secret")
    agent = ResearchAgent(
        base_url="https://example.test/v1",
        api_key_env="TEST_AGENT_KEY",
        tools={"write": {"callable": lambda: None, "write": True}},
    )
    with pytest.raises(ValueError, match="write-capable"):
        agent.research("check")


def test_research_agent_rejects_undeclared_callable_tool(monkeypatch):
    monkeypatch.setenv("TEST_AGENT_KEY", "secret")
    agent = ResearchAgent(
        base_url="https://example.test/v1",
        api_key_env="TEST_AGENT_KEY",
        tools={"lookup": lambda: {"ok": True}},
    )
    with pytest.raises(ValueError, match="write: false"):
        agent.research("check")


def test_research_agent_requires_credential(monkeypatch):
    monkeypatch.delenv("TEST_AGENT_KEY", raising=False)
    agent = ResearchAgent(
        base_url="https://example.test/v1", api_key_env="TEST_AGENT_KEY"
    )
    with pytest.raises(ResearchAgentError, match="missing credential"):
        agent.research("check")
