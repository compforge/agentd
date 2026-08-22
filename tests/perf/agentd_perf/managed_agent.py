from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass

import httpx
from perf_harness import MetricFamily, Outcome, TrialContext, Verdict, Workload, register_workload


@dataclass
class SessionSlot:
    session_id: str
    agent_messages: int = 0
    session_errors: int = 0
    idle_events: int = 0


class ManagedAgentWorkload(Workload):
    """Measure one complete public Event-to-idle managed-agent turn."""

    name = "managed_agent"

    def __init__(
        self,
        *,
        model: str = "claude-sonnet-4-6",
        pool_size: int = 10,
        timeout_s: float = 180.0,
        poll_interval_s: float = 0.5,
        use_sandbox_tools: bool = True,
    ) -> None:
        if pool_size < 1:
            raise ValueError("managed_agent pool_size must be >= 1")
        if timeout_s <= 0 or poll_interval_s <= 0:
            raise ValueError("managed_agent timeout and poll interval must be positive")
        self.model = model
        self.pool_size = pool_size
        self.timeout_s = timeout_s
        self.poll_interval_s = poll_interval_s
        self.use_sandbox_tools = use_sandbox_tools
        self._available: asyncio.Queue[SessionSlot] = asyncio.Queue()

    async def setup(self, ctx: TrialContext) -> None:
        self._available = asyncio.Queue()
        prefix = f"agentd-perf-{uuid.uuid4().hex[:12]}"
        tools = []
        system = "Do not use tools. Answer every request exactly AGENTD_PERF_OK."
        if self.use_sandbox_tools:
            tools = [{"type": "agent_toolset_20260401", "configs": []}]
            system = (
                "When the user explicitly asks for a sandbox check, call bash exactly once with "
                "command `printf AGENTD_PERF_SANDBOX_OK`, then answer exactly AGENTD_PERF_OK. "
                "For every other request, do not use tools and answer exactly AGENTD_PERF_OK."
            )
        agent = await self._required_post(
            ctx.client,
            ctx.target.base_url + "/v1/agents",
            {
                "name": prefix,
                "model": self.model,
                "system": system,
                "tools": tools,
                "metadata": {"perf_run": prefix},
            },
            ctx.target.headers,
        )
        environment = await self._required_post(
            ctx.client,
            ctx.target.base_url + "/v1/environments",
            {
                "name": prefix,
                "config": {"type": "cloud", "networking": {"type": "unrestricted"}},
                "metadata": {"perf_run": prefix},
            },
            ctx.target.headers,
        )
        for index in range(self.pool_size):
            session = await self._required_post(
                ctx.client,
                ctx.target.base_url + "/v1/sessions",
                {
                    "agent": agent["id"],
                    "environment_id": environment["id"],
                    "title": f"{prefix}-{index}",
                    "metadata": {"perf_run": prefix},
                },
                ctx.target.headers,
            )
            self._available.put_nowait(SessionSlot(session_id=session["id"]))

    async def fire(self, target, client: httpx.AsyncClient, case, run_id: str) -> Outcome:
        started = time.monotonic()
        try:
            slot = await asyncio.wait_for(self._available.get(), timeout=self.timeout_s)
        except asyncio.TimeoutError:
            return Outcome(
                status=None,
                duration_ms=(time.monotonic() - started) * 1000,
                meta={"case_id": case.id, "run_id": run_id, "exc": "SessionPoolExhausted"},
            )
        accepted_at = started
        nbytes = 0
        reusable = False
        meta: dict[str, object] = {
            "case_id": case.id,
            "run_id": run_id,
            "session_id": slot.session_id,
        }
        try:
            response = await client.post(
                target.base_url + f"/v1/sessions/{slot.session_id}/events",
                json={
                    "events": [
                        {
                            "type": "user.message",
                            "content": [
                                {
                                    "type": "text",
                                    "text": str(case.input.get("prompt", "Reply with OK.")),
                                }
                            ],
                        }
                    ]
                },
                headers=target.headers,
                timeout=self.timeout_s,
            )
            accepted_at = time.monotonic()
            nbytes += len(response.content)
            if not response.is_success:
                meta["body"] = response.text[:512]
                return Outcome(
                    status=response.status_code,
                    duration_ms=(accepted_at - started) * 1000,
                    nbytes=nbytes,
                    meta=meta,
                )

            deadline = started + self.timeout_s
            while time.monotonic() < deadline:
                events = await client.get(
                    target.base_url + f"/v1/sessions/{slot.session_id}/events",
                    headers=target.headers,
                    timeout=self.timeout_s,
                )
                nbytes += len(events.content)
                if not events.is_success:
                    meta["body"] = events.text[:512]
                    return Outcome(
                        status=events.status_code,
                        duration_ms=(time.monotonic() - started) * 1000,
                        nbytes=nbytes,
                        meta=meta,
                    )
                data = events.json().get("data", [])
                messages = sum(event.get("type") == "agent.message" for event in data)
                errors = [event for event in data if event.get("type") == "session.error"]
                idle_events = [
                    event for event in data if event.get("type") == "session.status_idle"
                ]
                has_new_message = messages > slot.agent_messages
                has_new_error = len(errors) > slot.session_errors
                has_new_idle = len(idle_events) > slot.idle_events
                if has_new_message:
                    latest_message = next(
                        event for event in reversed(data) if event.get("type") == "agent.message"
                    )
                    expected_text = str(case.input.get("expected_text", ""))
                    if expected_text:
                        meta["expected_text_seen"] = expected_text in str(
                            latest_message.get("content", "")
                        )
                session = await client.get(
                    target.base_url + f"/v1/sessions/{slot.session_id}",
                    headers=target.headers,
                    timeout=self.timeout_s,
                )
                nbytes += len(session.content)
                if not session.is_success:
                    meta["body"] = session.text[:512]
                    return Outcome(
                        status=session.status_code,
                        duration_ms=(time.monotonic() - started) * 1000,
                        nbytes=nbytes,
                        meta=meta,
                    )
                status = session.json().get("status")
                if status == "terminated":
                    meta["session_status"] = status
                    return Outcome(
                        status=200,
                        duration_ms=(time.monotonic() - started) * 1000,
                        nbytes=nbytes,
                        meta=meta,
                    )
                # An accepted input may briefly observe the Session's previous
                # idle state. A new output or turn-scoped idle Event proves that
                # this turn, rather than the previous one, has settled.
                if status == "idle" and (has_new_message or has_new_idle):
                    completed_at = time.monotonic()
                    meta["session_status"] = status
                    meta["agent_message_seen"] = has_new_message
                    if has_new_error:
                        error = errors[-1].get("error", {})
                        if isinstance(error, dict):
                            meta["session_error_type"] = str(error.get("type", "runtime_error"))
                            meta["session_error_message"] = str(error.get("message", ""))[:512]
                    if has_new_idle:
                        stop_reason = idle_events[-1].get("stop_reason", {})
                        if isinstance(stop_reason, dict):
                            meta["stop_reason_type"] = str(stop_reason.get("type", ""))
                    if has_new_message and not has_new_error:
                        slot.agent_messages = messages
                        slot.session_errors = len(errors)
                        slot.idle_events = len(idle_events)
                        reusable = True
                    return Outcome(
                        status=200,
                        duration_ms=(completed_at - started) * 1000,
                        nbytes=nbytes,
                        metrics={
                            "accept_ms": (accepted_at - started) * 1000,
                            "complete_ms": (completed_at - started) * 1000,
                        },
                        meta=meta,
                    )
                await asyncio.sleep(self.poll_interval_s)
            meta["exc"] = "CompletionTimeout"
            return Outcome(
                status=None,
                duration_ms=(time.monotonic() - started) * 1000,
                nbytes=nbytes,
                meta=meta,
            )
        except Exception as exc:  # noqa: BLE001 - load generators record transport failures
            meta["exc"] = type(exc).__name__
            meta["exc_detail"] = str(exc)
            return Outcome(
                status=None,
                duration_ms=(time.monotonic() - started) * 1000,
                nbytes=nbytes,
                meta=meta,
            )
        finally:
            # A failed turn has an ambiguous completion state. Do not let a
            # delayed response be mistaken for the next request on this slot.
            if reusable:
                self._available.put_nowait(slot)

    def judge(self, outcome: Outcome) -> Verdict:
        verdict = super().judge(outcome)
        if not verdict.ok:
            return verdict
        if outcome.meta.get("session_status") == "terminated":
            return Verdict(False, "session_terminated")
        if error_type := outcome.meta.get("session_error_type"):
            return Verdict(False, f"session_{error_type}")
        if outcome.meta.get("agent_message_seen") is False:
            stop_reason = outcome.meta.get("stop_reason_type")
            return Verdict(
                False, f"session_{stop_reason}" if stop_reason else "missing_agent_message"
            )
        if outcome.meta.get("expected_text_seen") is False:
            return Verdict(False, "unexpected_response")
        return verdict

    def describe(self) -> list[MetricFamily]:
        return [
            MetricFamily(
                name="accept_ms",
                unit="ms",
                side="request",
                value_kind="distribution",
                source="client",
                description="Time from dispatch until agentd durably accepts the input Event.",
            ),
            MetricFamily(
                name="complete_ms",
                unit="ms",
                side="request",
                value_kind="distribution",
                source="client",
                description="Time from dispatch until the Agent output is durable and Session is idle.",
            ),
        ]

    async def _required_post(
        self,
        client: httpx.AsyncClient,
        url: str,
        payload: dict,
        headers: dict[str, str],
    ) -> dict:
        response = await client.post(url, json=payload, headers=headers, timeout=self.timeout_s)
        response.raise_for_status()
        value = response.json()
        if not isinstance(value, dict) or not value.get("id"):
            raise RuntimeError(f"agentd setup response from {url} has no id")
        return value


def _build_workload(config: dict) -> ManagedAgentWorkload:
    return ManagedAgentWorkload(
        model=str(config.get("model", "claude-sonnet-4-6")),
        pool_size=int(config.get("pool_size", 10)),
        timeout_s=float(config.get("timeout_s", 180.0)),
        poll_interval_s=float(config.get("poll_interval_s", 0.5)),
        use_sandbox_tools=bool(config.get("use_sandbox_tools", True)),
    )


register_workload("managed_agent", _build_workload)
