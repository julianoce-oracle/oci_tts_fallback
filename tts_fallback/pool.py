from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass
from typing import Any

from .errors import CancelledInFlight, NoHealthyConnectionError
from .types import EndpointConfig, Timeouts, utc_now
from .xai_oci import XaiOciConnection, XaiOciEndpoint


@dataclass(frozen=True)
class PoolConfig:
    size: int = 3
    failure_threshold: int = 3
    cooldown_s: float = 30.0
    reconnect_initial_s: float = 0.25
    reconnect_max_s: float = 5.0
    jitter_ratio: float = 0.2


@dataclass
class ConnectionSlot:
    slot_id: str
    connection: XaiOciConnection | None = None
    state: str = "idle"
    leased: bool = False
    failure_count: int = 0
    use_count: int = 0
    last_error: str | None = None
    connected_at_utc: str | None = None
    updated_at_utc: str | None = None
    reconnect_task: asyncio.Task[None] | None = None

    @property
    def is_ready(self) -> bool:
        return (
            self.state == "ready"
            and not self.leased
            and self.connection is not None
            and self.connection.is_open
        )


class WarmWebSocketPool:
    def __init__(
        self,
        *,
        endpoint: EndpointConfig,
        pool: PoolConfig | None = None,
        timeouts: Timeouts | None = None,
    ) -> None:
        self.config = pool or PoolConfig()
        self.timeouts = timeouts or Timeouts()
        self.endpoint = XaiOciEndpoint(endpoint, self.timeouts)
        self._slots = [ConnectionSlot(slot_id=f"ws-{index + 1}") for index in range(self.config.size)]
        self._condition = asyncio.Condition()
        self._closed = False

    async def start(self) -> None:
        if self.config.size < 1:
            raise ValueError("pool size must be at least 1")
        await asyncio.gather(*(self._connect_slot(slot) for slot in self._slots))

    async def wait_until_ready(self, *, min_ready: int = 1, timeout_s: float | None = None) -> None:
        deadline = None if timeout_s is None else asyncio.get_running_loop().time() + timeout_s
        async with self._condition:
            while self.ready_count < min_ready:
                if self._closed:
                    raise NoHealthyConnectionError("pool is closed")
                if deadline is None:
                    await self._condition.wait()
                    continue

                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    raise NoHealthyConnectionError(
                        f"timed out waiting for {min_ready} ready websocket connection(s)"
                    )
                try:
                    await asyncio.wait_for(self._condition.wait(), timeout=remaining)
                except asyncio.TimeoutError as exc:
                    raise NoHealthyConnectionError(self._no_healthy_error_message()) from exc

    @property
    def ready_count(self) -> int:
        return sum(1 for slot in self._slots if slot.is_ready)


    def _no_healthy_error_message(self) -> str:
        details = [
            f"{slot.slot_id}: state={slot.state}, failures={slot.failure_count}, last_error={slot.last_error}"
            for slot in self._slots
            if slot.last_error
        ]
        if not details:
            return "timed out waiting for a healthy websocket connection"
        return "timed out waiting for a healthy websocket connection; " + "; ".join(details)

    def snapshot(self) -> list[dict[str, Any]]:
        return [
            {
                "slotId": slot.slot_id,
                "state": slot.state,
                "leased": slot.leased,
                "failureCount": slot.failure_count,
                "useCount": slot.use_count,
                "lastError": slot.last_error,
                "connectedAtUtc": slot.connected_at_utc,
                "updatedAtUtc": slot.updated_at_utc,
            }
            for slot in self._slots
        ]

    async def acquire(self, *, timeout_s: float | None = None) -> ConnectionSlot:
        timeout_s = self.timeouts.acquire_s if timeout_s is None else timeout_s
        deadline = asyncio.get_running_loop().time() + timeout_s

        async with self._condition:
            while True:
                if self._closed:
                    raise NoHealthyConnectionError("pool is closed")

                for slot in self._slots:
                    if slot.is_ready:
                        slot.leased = True
                        slot.state = "leased"
                        slot.use_count += 1
                        slot.updated_at_utc = utc_now()
                        return slot

                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    raise NoHealthyConnectionError("no healthy websocket connection is available")
                try:
                    await asyncio.wait_for(self._condition.wait(), timeout=remaining)
                except asyncio.TimeoutError as exc:
                    raise NoHealthyConnectionError(self._no_healthy_error_message()) from exc

    async def release(
        self,
        slot: ConnectionSlot,
        *,
        healthy: bool,
        error: BaseException | None = None,
    ) -> None:
        if healthy and slot.connection is not None and slot.connection.is_open:
            async with self._condition:
                slot.state = "ready"
                slot.leased = False
                slot.failure_count = 0
                slot.last_error = None
                slot.updated_at_utc = utc_now()
                self._condition.notify_all()
            return

        await self._fail_slot(slot, error or CancelledInFlight("released unhealthy in-flight socket"))

    async def aclose(self) -> None:
        self._closed = True
        for slot in self._slots:
            if slot.reconnect_task is not None:
                slot.reconnect_task.cancel()
            if slot.connection is not None:
                await slot.connection.close()
            slot.connection = None
            slot.state = "closed"
            slot.leased = False
            slot.updated_at_utc = utc_now()
        async with self._condition:
            self._condition.notify_all()

    async def _connect_slot(self, slot: ConnectionSlot) -> None:
        if self._closed:
            return

        async with self._condition:
            slot.state = "connecting"
            slot.leased = False
            slot.updated_at_utc = utc_now()
            self._condition.notify_all()

        try:
            connection = await self.endpoint.connect(slot.slot_id)
        except Exception as exc:
            await self._fail_slot(slot, exc, close_existing=False)
            return

        async with self._condition:
            slot.connection = connection
            slot.state = "ready"
            slot.leased = False 
            slot.last_error = None
            slot.connected_at_utc = utc_now()
            slot.updated_at_utc = slot.connected_at_utc
            self._condition.notify_all()

    async def _fail_slot(
        self,
        slot: ConnectionSlot,
        error: BaseException,
        *,
        close_existing: bool = True,
    ) -> None:
        if close_existing and slot.connection is not None:
            await slot.connection.close()

        async with self._condition:
            slot.connection = None
            slot.state = "cooling_down"
            slot.leased = False
            slot.failure_count += 1
            slot.last_error = f"{type(error).__name__}: {error}"
            slot.updated_at_utc = utc_now()
            self._schedule_reconnect_locked(slot)
            self._condition.notify_all()

    def _schedule_reconnect_locked(self, slot: ConnectionSlot) -> None:
        if self._closed:
            return
        if slot.reconnect_task is not None and not slot.reconnect_task.done():
            return

        delay = self._reconnect_delay(slot.failure_count)
        slot.reconnect_task = asyncio.create_task(self._reconnect_after(slot, delay))

    async def _reconnect_after(self, slot: ConnectionSlot, delay_s: float) -> None:
        try:
            await asyncio.sleep(delay_s)
            async with self._condition:
                slot.reconnect_task = None
            await self._connect_slot(slot)
        except asyncio.CancelledError:
            raise

    def _reconnect_delay(self, failure_count: int) -> float:
        if failure_count >= self.config.failure_threshold:
            base = self.config.cooldown_s
        else:
            power = max(0, failure_count - 1)
            base = min(self.config.reconnect_max_s, self.config.reconnect_initial_s * (2**power))

        jitter = base * self.config.jitter_ratio
        if jitter <= 0:
            return base
        return max(0.0, base + random.uniform(-jitter, jitter))

