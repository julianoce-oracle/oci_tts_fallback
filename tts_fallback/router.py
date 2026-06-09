from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

from .errors import (
    CancelledInFlight,
    NoHealthyConnectionError,
    PartialSynthesisError,
    SynthesisFailedError,
)
from .pool import ConnectionSlot, PoolConfig, WarmWebSocketPool
from .types import EndpointConfig, FallbackEvent, Timeouts, elapsed_ms, monotonic_ns, new_request_id


class FallbackMode:
    SEQUENTIAL = "sequential"
    HEDGED = "hedged"


AttemptStartHook = Callable[[ConnectionSlot, int], Awaitable[None]]


class FallbackTTS:
    def __init__(
        self,
        *,
        endpoint: EndpointConfig,
        pool: PoolConfig | None = None,
        timeouts: Timeouts | None = None,
    ) -> None:
        self.pool = WarmWebSocketPool(endpoint=endpoint, pool=pool, timeouts=timeouts)

    async def __aenter__(self) -> "FallbackTTS":
        await self.start()
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        await self.aclose()

    async def start(self) -> None:
        await self.pool.start()

    async def wait_until_ready(self, *, min_ready: int = 1, timeout_s: float | None = None) -> None:
        await self.pool.wait_until_ready(min_ready=min_ready, timeout_s=timeout_s)

    async def aclose(self) -> None:
        await self.pool.aclose()

    async def stream(
        self,
        text: str,
        *,
        mode: str = FallbackMode.SEQUENTIAL,
        hedges: int = 2,
        max_attempts: int | None = None,
        restart_on_midstream_failure: bool = False,
        on_attempt_start: AttemptStartHook | None = None,
    ) -> AsyncIterator[FallbackEvent]:
        request_id = new_request_id()
        if mode == FallbackMode.SEQUENTIAL:
            async for event in self._stream_sequential(
                request_id,
                text,
                max_attempts=max_attempts,
                restart_on_midstream_failure=restart_on_midstream_failure,
                on_attempt_start=on_attempt_start,
            ):
                yield event
            return

        if mode == FallbackMode.HEDGED:
            async for event in self._stream_hedged(
                request_id,
                text,
                hedges=hedges,
                on_attempt_start=on_attempt_start,
            ):
                yield event
            return

        raise ValueError(f"unknown fallback mode: {mode}")

    async def _stream_sequential(
        self,
        request_id: str,
        text: str,
        *,
        max_attempts: int | None,
        restart_on_midstream_failure: bool,
        on_attempt_start: AttemptStartHook | None,
    ) -> AsyncIterator[FallbackEvent]:
        max_attempts = max_attempts or self.pool.config.size
        last_error: BaseException | None = None

        for attempt in range(1, max_attempts + 1):
            slot = await self.pool.acquire()
            emitted_audio = False
            chunk_index = 0
            started_ns = monotonic_ns()
            yield FallbackEvent(
                type="attempt_started",
                request_id=request_id,
                connection_id=slot.slot_id,
                attempt=attempt,
                meta={"mode": FallbackMode.SEQUENTIAL},
            )

            try:
                if on_attempt_start is not None:
                    await on_attempt_start(slot, attempt)

                if slot.connection is None:
                    raise NoHealthyConnectionError("leased slot has no websocket connection")

                async for chunk in slot.connection.stream_audio(text):
                    chunk_index += 1
                    if not emitted_audio:
                        emitted_audio = True
                        yield FallbackEvent(
                            type="winner",
                            request_id=request_id,
                            connection_id=slot.slot_id,
                            attempt=attempt,
                            elapsed_ms=elapsed_ms(started_ns, monotonic_ns()),
                            meta={"mode": FallbackMode.SEQUENTIAL},
                        )

                    yield FallbackEvent(
                        type="audio",
                        request_id=request_id,
                        connection_id=slot.slot_id,
                        attempt=attempt,
                        chunk_index=chunk_index,
                        audio=chunk,
                    )

                await self.pool.release(slot, healthy=True)
                yield FallbackEvent(
                    type="completed",
                    request_id=request_id,
                    connection_id=slot.slot_id,
                    attempt=attempt,
                    elapsed_ms=elapsed_ms(started_ns, monotonic_ns()),
                    meta={"chunks": chunk_index, "mode": FallbackMode.SEQUENTIAL},
                )
                return
            except Exception as exc:
                last_error = exc
                await self.pool.release(slot, healthy=False, error=exc)
                yield FallbackEvent(
                    type="attempt_failed",
                    request_id=request_id,
                    connection_id=slot.slot_id,
                    attempt=attempt,
                    message=f"{type(exc).__name__}: {exc}",
                    elapsed_ms=elapsed_ms(started_ns, monotonic_ns()),
                    meta={"chunksBeforeFailure": chunk_index, "mode": FallbackMode.SEQUENTIAL},
                )

                if emitted_audio and not restart_on_midstream_failure:
                    raise PartialSynthesisError(
                        "websocket failed after audio was emitted; refusing to restart full text by default"
                    ) from exc

        raise SynthesisFailedError(f"all fallback attempts failed: {last_error}") from last_error

    async def _stream_hedged(
        self,
        request_id: str,
        text: str,
        *,
        hedges: int,
        on_attempt_start: AttemptStartHook | None,
    ) -> AsyncIterator[FallbackEvent]:
        if hedges < 1:
            raise ValueError("hedges must be at least 1")

        slots: list[ConnectionSlot] = []
        for _ in range(hedges):
            try:
                slots.append(await self.pool.acquire())
            except NoHealthyConnectionError:
                if not slots:
                    raise
                break

        queue: asyncio.Queue[tuple[str, ConnectionSlot, Any]] = asyncio.Queue()
        tasks: dict[str, asyncio.Task[None]] = {}
        released: set[str] = set()
        winner_id: str | None = None
        winner_started_ns: int | None = None
        active_ids = {slot.slot_id for slot in slots}
        emitted_by_winner = False

        async def release_once(slot: ConnectionSlot, *, healthy: bool, error: BaseException | None = None) -> None:
            if slot.slot_id in released:
                return
            released.add(slot.slot_id)
            await self.pool.release(slot, healthy=healthy, error=error)

        async def worker(slot: ConnectionSlot) -> None:
            chunk_index = 0
            try:
                if slot.connection is None:
                    raise NoHealthyConnectionError("leased slot has no websocket connection")
                async for chunk in slot.connection.stream_audio(text):
                    chunk_index += 1
                    await queue.put(("audio", slot, (chunk_index, chunk)))
                await queue.put(("done", slot, {"chunks": chunk_index}))
            except Exception as exc:
                await queue.put(("error", slot, exc))

        try:
            for attempt, slot in enumerate(slots, start=1):
                yield FallbackEvent(
                    type="attempt_started",
                    request_id=request_id,
                    connection_id=slot.slot_id,
                    attempt=attempt,
                    meta={"mode": FallbackMode.HEDGED},
                )
                if on_attempt_start is not None:
                    await on_attempt_start(slot, attempt)
                tasks[slot.slot_id] = asyncio.create_task(worker(slot))

            while active_ids:
                kind, slot, payload = await queue.get()
                slot_id = slot.slot_id

                if kind == "audio":
                    chunk_index, chunk = payload
                    if winner_id is None:
                        winner_id = slot_id
                        winner_started_ns = monotonic_ns()
                        yield FallbackEvent(
                            type="winner",
                            request_id=request_id,
                            connection_id=slot_id,
                            attempt=slots.index(slot) + 1,
                            meta={"mode": FallbackMode.HEDGED, "cancelledLosers": sorted(active_ids - {slot_id})},
                        )

                        for loser_id in sorted(active_ids - {slot_id}):
                            task = tasks.get(loser_id)
                            if task is not None:
                                task.cancel()
                            loser = next(candidate for candidate in slots if candidate.slot_id == loser_id)
                            await release_once(
                                loser,
                                healthy=False,
                                error=CancelledInFlight("hedged request lost race and was closed"),
                            )
                            active_ids.discard(loser_id)

                    if slot_id == winner_id:
                        emitted_by_winner = True
                        yield FallbackEvent(
                            type="audio",
                            request_id=request_id,
                            connection_id=slot_id,
                            chunk_index=chunk_index,
                            audio=chunk,
                        )
                    continue

                if kind == "done":
                    active_ids.discard(slot_id)
                    if slot_id == winner_id:
                        await release_once(slot, healthy=True)
                        yield FallbackEvent(
                            type="completed",
                            request_id=request_id,
                            connection_id=slot_id,
                            elapsed_ms=elapsed_ms(winner_started_ns, monotonic_ns()),
                            meta={"chunks": payload.get("chunks"), "mode": FallbackMode.HEDGED},
                        )
                        return

                    await release_once(slot, healthy=True)
                    if winner_id is None and not active_ids:
                        raise SynthesisFailedError("all hedged websocket attempts finished without audio")
                    continue

                if kind == "error":
                    active_ids.discard(slot_id)
                    await release_once(slot, healthy=False, error=payload)
                    yield FallbackEvent(
                        type="attempt_failed",
                        request_id=request_id,
                        connection_id=slot_id,
                        attempt=slots.index(slot) + 1,
                        message=f"{type(payload).__name__}: {payload}",
                        meta={"mode": FallbackMode.HEDGED},
                    )

                    if slot_id == winner_id and emitted_by_winner:
                        raise PartialSynthesisError("winning hedged websocket failed after audio was emitted") from payload
                    if winner_id is None and not active_ids:
                        raise SynthesisFailedError("all hedged websocket attempts failed") from payload
        finally:
            for task in tasks.values():
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks.values(), return_exceptions=True)
            for slot in slots:
                if slot.slot_id not in released:
                    await release_once(
                        slot,
                        healthy=False,
                        error=CancelledInFlight("stream ended before hedged socket was released"),
                    )

