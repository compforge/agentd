import json
from types import SimpleNamespace

import httpx
import pytest
from perf_harness import Case, Outcome, Target

from agentd_perf.managed_agent import ManagedAgentWorkload, _build_workload


@pytest.mark.asyncio
async def test_turn_uses_bounded_session_pool_and_waits_for_idle() -> None:
    requests: list[httpx.Request] = []
    event_polls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal event_polls
        requests.append(request)
        if request.method == "POST" and request.url.path == "/v1/agents":
            return httpx.Response(200, json={"id": "agent-1"})
        if request.method == "POST" and request.url.path == "/v1/environments":
            return httpx.Response(200, json={"id": "environment-1"})
        if request.method == "POST" and request.url.path == "/v1/sessions":
            return httpx.Response(200, json={"id": "session-1"})
        if request.method == "POST" and request.url.path.endswith("/events"):
            return httpx.Response(200, json={"data": [{"type": "user.message"}]})
        if request.method == "GET" and request.url.path.endswith("/events"):
            event_polls += 1
            data = [{"type": "user.message"}]
            if event_polls > 1:
                data.append(
                    {
                        "type": "agent.message",
                        "content": [{"type": "text", "text": "AGENTD_PERF_OK"}],
                    }
                )
            return httpx.Response(200, json={"data": data})
        if request.method == "GET" and request.url.path == "/v1/sessions/session-1":
            return httpx.Response(200, json={"id": "session-1", "status": "idle"})
        return httpx.Response(404)

    workload = ManagedAgentWorkload(pool_size=1, timeout_s=1, poll_interval_s=0.001)
    target = Target("http://agentd", headers={"x-api-key": "test"})
    ctx = SimpleNamespace(target=target)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        ctx.client = client
        await workload.setup(ctx)
        outcome = await workload.fire(
            target,
            client,
            Case(
                "sandbox_turn",
                {"prompt": "run it", "expected_text": "AGENTD_PERF_OK"},
            ),
            "run-1",
        )

    assert workload.judge(outcome).ok
    assert outcome.meta["session_id"] == "session-1"
    assert set(outcome.metrics) == {"accept_ms", "complete_ms"}
    assert {metric.name for metric in workload.describe()} == {"accept_ms", "complete_ms"}
    agent_payload = json.loads(requests[0].content)
    assert agent_payload["tools"] == [{"type": "agent_toolset_20260401", "configs": []}]
    assert "For every other request, do not use tools" in agent_payload["system"]
    sent = next(
        request
        for request in requests
        if request.method == "POST" and request.url.path.endswith("/events")
    )
    assert json.loads(sent.content)["events"][0]["content"][0]["text"] == "run it"


def test_judge_rejects_an_unexpected_agent_message() -> None:
    workload = ManagedAgentWorkload()

    verdict = workload.judge(Outcome(status=200, duration_ms=1, meta={"expected_text_seen": False}))

    assert not verdict.ok
    assert verdict.error_kind == "unexpected_response"


@pytest.mark.asyncio
async def test_turn_reports_session_error_without_waiting_for_agent_message() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and request.url.path == "/v1/agents":
            return httpx.Response(200, json={"id": "agent-1"})
        if request.method == "POST" and request.url.path == "/v1/environments":
            return httpx.Response(200, json={"id": "environment-1"})
        if request.method == "POST" and request.url.path == "/v1/sessions":
            return httpx.Response(200, json={"id": "session-1"})
        if request.method == "POST" and request.url.path.endswith("/events"):
            return httpx.Response(200, json={"data": [{"id": "input-1", "type": "user.message"}]})
        if request.method == "GET" and request.url.path.endswith("/events"):
            return httpx.Response(
                200,
                json={
                    "data": [
                        {"id": "input-1", "type": "user.message"},
                        {
                            "type": "session.error",
                            "error": {
                                "type": "runtime_error",
                                "message": "checkpoint revision conflict",
                            },
                        },
                        {
                            "type": "session.status_idle",
                            "stop_reason": {"type": "retries_exhausted"},
                        },
                    ]
                },
            )
        if request.method == "GET" and request.url.path == "/v1/sessions/session-1":
            return httpx.Response(200, json={"id": "session-1", "status": "idle"})
        return httpx.Response(404)

    workload = ManagedAgentWorkload(pool_size=1, timeout_s=1, poll_interval_s=0.001)
    target = Target("http://agentd", headers={"x-api-key": "test"})
    ctx = SimpleNamespace(target=target)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        ctx.client = client
        await workload.setup(ctx)
        outcome = await workload.fire(
            target,
            client,
            Case("failed_turn", {"prompt": "run it", "expected_text": "AGENTD_PERF_OK"}),
            "run-1",
        )

    verdict = workload.judge(outcome)
    assert not verdict.ok
    assert verdict.error_kind == "session_runtime_error"
    assert outcome.meta["stop_reason_type"] == "retries_exhausted"
    assert outcome.meta["agent_message_seen"] is False
    assert outcome.duration_ms < 1000


def test_build_workload_reads_profile() -> None:
    workload = _build_workload({"model": "test-model", "pool_size": 3, "use_sandbox_tools": False})

    assert workload.model == "test-model"
    assert workload.pool_size == 3
    assert not workload.use_sandbox_tools
