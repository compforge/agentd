import json
from types import SimpleNamespace

import httpx
import pytest
from perf_harness import Case, Target

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
            Case("sandbox_turn", {"prompt": "run it"}),
            "run-1",
        )

    assert workload.judge(outcome).ok
    assert outcome.meta["session_id"] == "session-1"
    assert set(outcome.metrics) == {"accept_ms", "complete_ms"}
    agent_payload = json.loads(requests[0].content)
    assert agent_payload["tools"] == [{"type": "agent_toolset_20260401", "configs": []}]
    sent = next(
        request
        for request in requests
        if request.method == "POST" and request.url.path.endswith("/events")
    )
    assert json.loads(sent.content)["events"][0]["content"][0]["text"] == "run it"


def test_build_workload_reads_profile() -> None:
    workload = _build_workload({"model": "test-model", "pool_size": 3, "use_sandbox_tools": False})

    assert workload.model == "test-model"
    assert workload.pool_size == 3
    assert not workload.use_sandbox_tools
