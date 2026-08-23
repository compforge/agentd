from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass, field

import httpx
from perf_harness import MetricFamily, Outcome, TrialContext, Verdict, Workload, register_workload


@dataclass
class SessionSlot:
    session_id: str
    user_messages: int = 0
    agent_messages: int = 0
    session_errors: int = 0
    idle_events: int = 0


@dataclass
class TransportStats:
    retry_count: int = 0
    error_count: int = 0
    send_reconciled: int = 0
    event_reconcile_ms: float | None = None
    failures: list[dict[str, object]] = field(default_factory=list)

    def record(
        self,
        operation: str,
        path: str,
        attempt: int,
        *,
        error: BaseException | None = None,
        response: httpx.Response | None = None,
    ) -> None:
        self.error_count += 1
        failure: dict[str, object] = {
            "operation": operation,
            "path": path,
            "attempt": attempt,
        }
        if error is not None:
            failure["error"] = type(error).__name__
            failure["detail"] = repr(error)[:512]
        if response is not None:
            failure["error"] = "HTTPStatusError"
            failure["status"] = response.status_code
            if request_id := response.headers.get("request-id"):
                failure["request_id"] = request_id
        self.failures.append(failure)
        if len(self.failures) > 8:
            self.failures.pop(0)

    def metrics(self) -> dict[str, float]:
        values = {
            "transport_retry_count": float(self.retry_count),
            "transport_error_count": float(self.error_count),
            "send_reconciled": float(self.send_reconciled),
        }
        if self.event_reconcile_ms is not None:
            values["event_reconcile_ms"] = self.event_reconcile_ms
        return values


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
        deadline = started + self.timeout_s
        transport = TransportStats()
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

        def finish(
            status: int | None,
            *,
            at: float | None = None,
            metrics: dict[str, float] | None = None,
        ) -> Outcome:
            if transport.failures:
                meta["transport_failures"] = transport.failures
            values = transport.metrics()
            values.update(metrics or {})
            return Outcome(
                status=status,
                duration_ms=((at or time.monotonic()) - started) * 1000,
                nbytes=nbytes,
                metrics=values,
                meta=meta,
            )

        try:
            events_url = target.base_url + f"/v1/sessions/{slot.session_id}/events"
            events_path = httpx.URL(events_url).path
            send_failure: BaseException | httpx.Response | None = None
            initial_events: list[dict] | None = None
            try:
                response = await client.post(
                    events_url,
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
                    timeout=max(deadline - time.monotonic(), 0.001),
                )
            except httpx.TransportError as exc:
                transport.record("send_events", events_path, 1, error=exc)
                send_failure = exc
            else:
                accepted_at = time.monotonic()
                nbytes += len(response.content)
                if response.is_success:
                    pass
                elif self._retryable_status(response.status_code):
                    transport.record("send_events", events_path, 1, response=response)
                    send_failure = response
                else:
                    meta["body"] = response.text[:512]
                    return finish(response.status_code, at=accepted_at)

            if send_failure is not None:
                reconcile_started = time.monotonic()
                try:
                    history, history_bytes = await self._list_events(
                        client,
                        events_url,
                        target.headers,
                        deadline,
                        transport,
                        operation="reconcile_send",
                    )
                finally:
                    transport.event_reconcile_ms = (time.monotonic() - reconcile_started) * 1000
                nbytes += history_bytes
                if not history.is_success:
                    meta["body"] = history.text[:512]
                    return finish(history.status_code)
                initial_events = self._event_data(history)
                user_messages = self._count(initial_events, "user.message")
                if user_messages <= slot.user_messages:
                    meta["exc"] = "ambiguous_send"
                    return finish(None)

                # A lost POST response cannot be replayed safely because the API has no
                # idempotency key. Durable Event History is the delivery confirmation.
                transport.send_reconciled = 1
                meta["send_reconciled"] = True
                accepted_at = time.monotonic()

            while time.monotonic() < deadline:
                if initial_events is None:
                    events, history_bytes = await self._list_events(
                        client,
                        events_url,
                        target.headers,
                        deadline,
                        transport,
                        operation="list_events",
                    )
                    nbytes += history_bytes
                    if not events.is_success:
                        meta["body"] = events.text[:512]
                        return finish(events.status_code)
                    data = self._event_data(events)
                else:
                    data = initial_events
                    initial_events = None

                user_messages = self._count(data, "user.message")
                messages = self._count(data, "agent.message")
                errors = [event for event in data if event.get("type") == "session.error"]
                idle_events = [
                    event for event in data if event.get("type") == "session.status_idle"
                ]
                has_new_message = messages > slot.agent_messages
                has_new_idle = len(idle_events) > slot.idle_events
                new_errors = errors[slot.session_errors :]
                settled_errors = [
                    event for event in new_errors if self._retry_status(event) != "retrying"
                ]
                if has_new_message:
                    latest_message = next(
                        event for event in reversed(data) if event.get("type") == "agent.message"
                    )
                    expected_text = str(case.input.get("expected_text", ""))
                    if expected_text:
                        meta["expected_text_seen"] = expected_text in str(
                            latest_message.get("content", "")
                        )
                if settled_errors:
                    meta["agent_message_seen"] = has_new_message
                    self._record_session_error(meta, settled_errors[-1])
                    return finish(200)

                # A new idle Event, rather than the eventually consistent Session read
                # model, proves that this exact turn has settled.
                if has_new_idle:
                    completed_at = time.monotonic()
                    meta["session_status"] = "idle"
                    meta["agent_message_seen"] = has_new_message
                    stop_reason = idle_events[-1].get("stop_reason", {})
                    if isinstance(stop_reason, dict):
                        meta["stop_reason_type"] = str(stop_reason.get("type", ""))
                    if has_new_message:
                        slot.user_messages = user_messages
                        slot.agent_messages = messages
                        slot.session_errors = len(errors)
                        slot.idle_events = len(idle_events)
                        reusable = True
                    return finish(
                        200,
                        at=completed_at,
                        metrics={
                            "accept_ms": (accepted_at - started) * 1000,
                            "complete_ms": (completed_at - started) * 1000,
                        },
                    )
                await asyncio.sleep(self.poll_interval_s)
            meta["exc"] = "CompletionTimeout"
            return finish(None)
        except Exception as exc:  # noqa: BLE001 - load generators record transport failures
            meta["exc"] = type(exc).__name__
            meta["exc_detail"] = str(exc)
            return finish(None)
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
            MetricFamily(
                name="transport_retry_count",
                unit="count",
                side="request",
                value_kind="distribution",
                source="client",
                description="Safe Event History read retries made during one Turn.",
            ),
            MetricFamily(
                name="transport_error_count",
                unit="count",
                side="request",
                value_kind="distribution",
                source="client",
                description="Transient HTTP transport and retryable response errors observed.",
            ),
            MetricFamily(
                name="event_reconcile_ms",
                unit="ms",
                side="request",
                value_kind="distribution",
                source="client",
                description="Time spent confirming an ambiguous Event send from durable history.",
            ),
            MetricFamily(
                name="send_reconciled",
                unit="count",
                side="request",
                value_kind="distribution",
                source="client",
                description="Whether an ambiguous Event send was confirmed without replay.",
            ),
        ]

    async def _list_events(
        self,
        client: httpx.AsyncClient,
        url: str,
        headers: dict[str, str],
        deadline: float,
        transport: TransportStats,
        *,
        operation: str,
    ) -> tuple[httpx.Response, int]:
        attempt = 0
        nbytes = 0
        path = httpx.URL(url).path
        while True:
            attempt += 1
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"{operation} exceeded the Turn deadline")
            try:
                response = await client.get(
                    url,
                    headers=headers,
                    timeout=max(remaining, 0.001),
                )
            except httpx.TransportError as exc:
                transport.record(operation, path, attempt, error=exc)
                if not await self._wait_for_retry(attempt, deadline, transport):
                    raise
                continue

            nbytes += len(response.content)
            if not self._retryable_status(response.status_code):
                return response, nbytes

            transport.record(operation, path, attempt, response=response)
            if not await self._wait_for_retry(attempt, deadline, transport):
                return response, nbytes

    @staticmethod
    async def _wait_for_retry(
        attempt: int,
        deadline: float,
        transport: TransportStats,
    ) -> bool:
        delay = min(0.1 * (2 ** min(attempt - 1, 4)), 1.0)
        if deadline - time.monotonic() <= delay:
            return False
        transport.retry_count += 1
        await asyncio.sleep(delay)
        return True

    @staticmethod
    def _retryable_status(status: int) -> bool:
        return status == 429 or status >= 500

    @staticmethod
    def _event_data(response: httpx.Response) -> list[dict]:
        data = response.json().get("data", [])
        if not isinstance(data, list):
            raise TypeError("agentd Event History data is not a list")
        return [event for event in data if isinstance(event, dict)]

    @staticmethod
    def _count(events: list[dict], event_type: str) -> int:
        return sum(event.get("type") == event_type for event in events)

    @staticmethod
    def _retry_status(event: dict) -> str:
        error = event.get("error", {})
        if not isinstance(error, dict):
            return ""
        retry_status = error.get("retry_status", {})
        if isinstance(retry_status, dict):
            return str(retry_status.get("type", ""))
        return str(retry_status or "")

    @classmethod
    def _record_session_error(cls, meta: dict[str, object], event: dict) -> None:
        error = event.get("error", {})
        if not isinstance(error, dict):
            return
        meta["session_error_type"] = str(error.get("type", "runtime_error"))
        meta["session_error_message"] = str(error.get("message", ""))[:512]
        if retry_status := cls._retry_status(event):
            meta["session_error_retry_status"] = retry_status

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
