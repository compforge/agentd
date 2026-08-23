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
                data.extend(
                    [
                        {
                            "type": "agent.message",
                            "content": [{"type": "text", "text": "AGENTD_PERF_OK"}],
                        },
                        {
                            "type": "session.status_idle",
                            "stop_reason": {"type": "end_turn"},
                        },
                    ]
                )
            return httpx.Response(200, json={"data": data})
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
    assert set(outcome.metrics) == {
        "accept_ms",
        "complete_ms",
        "send_reconciled",
        "transport_error_count",
        "transport_retry_count",
    }
    assert {metric.name for metric in workload.describe()} == {
        "accept_ms",
        "complete_ms",
        "event_reconcile_ms",
        "send_reconciled",
        "transport_error_count",
        "transport_retry_count",
    }
    agent_payload = json.loads(requests[0].content)
    assert agent_payload["tools"] == [{"type": "agent_toolset_20260401", "configs": []}]
    assert "For every other request, do not use tools" in agent_payload["system"]
    sent = next(
        request
        for request in requests
        if request.method == "POST" and request.url.path.endswith("/events")
    )
    assert json.loads(sent.content)["events"][0]["content"][0]["text"] == "run it"
    assert not any(
        request.method == "GET" and request.url.path == "/v1/sessions/session-1"
        for request in requests
    )


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
                                "retry_status": {"type": "exhausted"},
                            },
                        },
                        {
                            "type": "session.status_idle",
                            "stop_reason": {"type": "retries_exhausted"},
                        },
                    ]
                },
            )
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
    assert outcome.meta["session_error_retry_status"] == "exhausted"
    assert outcome.meta["agent_message_seen"] is False
    assert outcome.duration_ms < 1000


@pytest.mark.asyncio
async def test_event_history_read_retries_without_failing_the_turn() -> None:
    event_polls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal event_polls
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
            if event_polls == 1:
                raise httpx.ReadError("response body was lost", request=request)
            return httpx.Response(
                200,
                json={
                    "data": [
                        {"type": "user.message"},
                        {
                            "type": "agent.message",
                            "content": [{"type": "text", "text": "AGENTD_PERF_OK"}],
                        },
                        {
                            "type": "session.status_idle",
                            "stop_reason": {"type": "end_turn"},
                        },
                    ]
                },
            )
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
            Case("plain_turn", {"prompt": "reply", "expected_text": "AGENTD_PERF_OK"}),
            "run-1",
        )

    assert workload.judge(outcome).ok
    assert outcome.metrics["transport_retry_count"] == 1
    assert outcome.metrics["transport_error_count"] == 1
    assert outcome.meta["transport_failures"] == [
        {
            "operation": "list_events",
            "path": "/v1/sessions/session-1/events",
            "attempt": 1,
            "error": "ReadError",
            "detail": "ReadError('response body was lost')",
        }
    ]


@pytest.mark.asyncio
async def test_lost_send_response_is_reconciled_without_replaying_the_post() -> None:
    send_count = 0
    event_polls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal event_polls, send_count
        if request.method == "POST" and request.url.path == "/v1/agents":
            return httpx.Response(200, json={"id": "agent-1"})
        if request.method == "POST" and request.url.path == "/v1/environments":
            return httpx.Response(200, json={"id": "environment-1"})
        if request.method == "POST" and request.url.path == "/v1/sessions":
            return httpx.Response(200, json={"id": "session-1"})
        if request.method == "POST" and request.url.path.endswith("/events"):
            send_count += 1
            raise httpx.ReadError("response was lost after commit", request=request)
        if request.method == "GET" and request.url.path.endswith("/events"):
            event_polls += 1
            if event_polls < 3:
                return httpx.Response(200, json={"data": []})
            return httpx.Response(
                200,
                json={
                    "data": [
                        {"type": "user.message"},
                        {
                            "type": "agent.message",
                            "content": [{"type": "text", "text": "AGENTD_PERF_OK"}],
                        },
                        {
                            "type": "session.status_idle",
                            "stop_reason": {"type": "end_turn"},
                        },
                    ]
                },
            )
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
            Case("plain_turn", {"prompt": "reply", "expected_text": "AGENTD_PERF_OK"}),
            "run-1",
        )

    assert workload.judge(outcome).ok
    assert send_count == 1
    assert event_polls == 3
    assert outcome.meta["send_reconciled"] is True
    assert outcome.metrics["send_reconciled"] == 1
    assert outcome.metrics["event_reconcile_ms"] >= 0
    assert outcome.metrics["transport_error_count"] == 1


@pytest.mark.asyncio
async def test_unconfirmed_send_is_not_replayed_and_retires_the_session_slot() -> None:
    send_count = 0
    event_polls = 0
    events: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal event_polls, events, send_count
        if request.method == "POST" and request.url.path == "/v1/agents":
            return httpx.Response(200, json={"id": "agent-1"})
        if request.method == "POST" and request.url.path == "/v1/environments":
            return httpx.Response(200, json={"id": "environment-1"})
        if request.method == "POST" and request.url.path == "/v1/sessions":
            return httpx.Response(200, json={"id": "session-1"})
        if request.method == "POST" and request.url.path.endswith("/events"):
            send_count += 1
            if send_count == 1:
                events = [
                    {"type": "user.message"},
                    {
                        "type": "agent.message",
                        "content": [{"type": "text", "text": "AGENTD_PERF_OK"}],
                    },
                    {
                        "type": "session.status_idle",
                        "stop_reason": {"type": "end_turn"},
                    },
                ]
                return httpx.Response(200, json={"data": [{"type": "user.message"}]})
            raise httpx.ConnectError("connection failed", request=request)
        if request.method == "GET" and request.url.path.endswith("/events"):
            event_polls += 1
            return httpx.Response(200, json={"data": events})
        return httpx.Response(404)

    workload = ManagedAgentWorkload(
        pool_size=1,
        timeout_s=1,
        poll_interval_s=0.001,
        send_reconcile_timeout_s=0.01,
    )
    target = Target("http://agentd", headers={"x-api-key": "test"})
    ctx = SimpleNamespace(target=target)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        ctx.client = client
        await workload.setup(ctx)
        first = await workload.fire(
            target,
            client,
            Case("plain_turn", {"prompt": "reply", "expected_text": "AGENTD_PERF_OK"}),
            "run-1",
        )
        outcome = await workload.fire(
            target,
            client,
            Case("plain_turn", {"prompt": "reply", "expected_text": "AGENTD_PERF_OK"}),
            "run-2",
        )

    assert workload.judge(first).ok
    assert not workload.judge(outcome).ok
    assert workload.judge(outcome).error_kind == "ambiguous_send"
    assert send_count == 2
    assert event_polls > 1
    assert workload._available.empty()


@pytest.mark.asyncio
async def test_retrying_session_error_waits_for_the_final_turn_events() -> None:
    event_polls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal event_polls
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
            data = [
                {"type": "user.message"},
                {
                    "type": "session.error",
                    "error": {
                        "type": "model_overloaded_error",
                        "message": "retrying model request",
                        "retry_status": {"type": "retrying"},
                    },
                },
            ]
            if event_polls > 1:
                data.extend(
                    [
                        {
                            "type": "agent.message",
                            "content": [{"type": "text", "text": "AGENTD_PERF_OK"}],
                        },
                        {
                            "type": "session.status_idle",
                            "stop_reason": {"type": "end_turn"},
                        },
                    ]
                )
            return httpx.Response(200, json={"data": data})
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
            Case("plain_turn", {"prompt": "reply", "expected_text": "AGENTD_PERF_OK"}),
            "run-1",
        )

    assert workload.judge(outcome).ok
    assert event_polls == 2


def test_build_workload_reads_profile() -> None:
    workload = _build_workload(
        {
            "model": "test-model",
            "pool_size": 3,
            "send_reconcile_timeout_s": 7,
            "use_sandbox_tools": False,
        }
    )

    assert workload.model == "test-model"
    assert workload.pool_size == 3
    assert workload.send_reconcile_timeout_s == 7
    assert not workload.use_sandbox_tools
