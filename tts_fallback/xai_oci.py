from __future__ import annotations

import asyncio
import base64
import inspect
import json
import os
from collections.abc import AsyncIterator
from urllib.parse import urlencode

try:
    import websockets
except ModuleNotFoundError:  # pragma: no cover - depends on local environment
    websockets = None  # type: ignore[assignment]

from .errors import (
    ChunkTimeout,
    ConnectionClosedError,
    ConnectionOpenError,
    FirstAudioTimeout,
    MissingApiKeyError,
    ProtocolError,
    ProviderError,
)
from .types import EndpointConfig, Timeouts


def _connect_header_arg() -> str:
    websockets_module = _require_websockets()
    return (
        "additional_headers"
        if "additional_headers" in inspect.signature(websockets_module.connect).parameters
        else "extra_headers"
    )


def _require_websockets() -> object:
    if websockets is None:
        raise ConnectionOpenError("missing dependency: install the `websockets` package")
    return websockets


def _is_connection_closed(exc: BaseException) -> bool:
    if websockets is None:
        return False
    return isinstance(exc, websockets.exceptions.ConnectionClosed)


def _status_code_from_exception(exc: BaseException) -> int | None:
    for attr in ("status_code", "status"):
        value = getattr(exc, attr, None)
        if isinstance(value, int):
            return value
    response = getattr(exc, "response", None)
    for attr in ("status_code", "status"):
        value = getattr(response, attr, None)
        if isinstance(value, int):
            return value
    return None


class XaiOciEndpoint:
    def __init__(self, config: EndpointConfig, timeouts: Timeouts) -> None:
        self.config = config
        self.timeouts = timeouts

    def stream_url(self) -> str:
        params: dict[str, str | int] = {
            "language": self.config.language,
            "voice": self.config.voice,
            "codec": self.config.codec,
            "sample_rate": self.config.sample_rate,
            **self.config.extra_query,
        }
        if self.config.codec == "mp3" and self.config.bit_rate:
            params["bit_rate"] = self.config.bit_rate
        return f"{self.config.url}?{urlencode(params)}"

    def api_key(self) -> str:
        api_key = self.config.api_key or os.getenv(self.config.api_key_env)
        if not api_key:
            raise MissingApiKeyError(f"{self.config.api_key_env} is not set")
        return api_key

    async def connect(self, connection_id: str) -> "XaiOciConnection":
        websockets_module = _require_websockets()
        headers = {"Authorization": f"Bearer {self.api_key()}"}
        try:
            websocket = await websockets_module.connect(
                self.stream_url(),
                **{_connect_header_arg(): headers},
                open_timeout=self.timeouts.connect_s,
                close_timeout=self.timeouts.close_s,
                ping_interval=self.timeouts.ping_interval_s,
                ping_timeout=self.timeouts.ping_timeout_s,
            )
        except Exception as exc:
            status_code = _status_code_from_exception(exc)
            raise ConnectionOpenError(
                f"failed to open xAI OCI websocket: {type(exc).__name__}: {exc}"
            ) from exc

        return XaiOciConnection(connection_id=connection_id, websocket=websocket, timeouts=self.timeouts)


class XaiOciConnection:
    def __init__(self, *, connection_id: str, websocket: object, timeouts: Timeouts) -> None:
        self.connection_id = connection_id
        self.websocket = websocket
        self.timeouts = timeouts

    @property
    def close_code(self) -> int | None:
        value = getattr(self.websocket, "close_code", None)
        return value if isinstance(value, int) else None

    @property
    def is_open(self) -> bool:
        closed = getattr(self.websocket, "closed", None)
        if isinstance(closed, bool):
            return not closed

        state = getattr(self.websocket, "state", None)
        state_name = getattr(state, "name", None)
        if isinstance(state_name, str):
            return state_name == "OPEN"

        return True

    async def close(self) -> None:
        try:
            close = getattr(self.websocket, "close")
            result = close()
            if inspect.isawaitable(result):
                await result
        except Exception:
            pass

    async def stream_audio(self, text: str) -> AsyncIterator[bytes]:
        await self._send_json({"type": "text.delta", "delta": text})
        await self._send_json({"type": "text.done"})

        saw_audio = False
        while True:
            timeout = self.timeouts.chunk_s if saw_audio else self.timeouts.first_audio_s
            try:
                message = await asyncio.wait_for(self.websocket.recv(), timeout=timeout)
            except asyncio.TimeoutError as exc:
                if saw_audio:
                    raise ChunkTimeout(f"timed out after {timeout}s waiting for next audio chunk") from exc
                raise FirstAudioTimeout(f"timed out after {timeout}s waiting for first audio") from exc
            except Exception as exc:
                if not _is_connection_closed(exc):
                    raise
                raise ConnectionClosedError(
                    f"websocket closed while receiving audio ({exc.code})",
                    close_code=exc.code,
                    reason=exc.reason or None,
                ) from exc

            if not isinstance(message, str):
                raise ProtocolError(f"expected text websocket message, got {type(message).__name__}")

            try:
                event = json.loads(message)
            except json.JSONDecodeError as exc:
                raise ProtocolError("received non-json websocket message") from exc

            event_type = event.get("type")
            if event_type == "audio.delta":
                try:
                    chunk = base64.b64decode(event.get("delta", ""))
                except Exception as exc:
                    raise ProtocolError("received invalid base64 audio delta") from exc
                saw_audio = True
                yield chunk
                continue

            if event_type == "audio.done":
                return

            if event_type == "error":
                message_text = event.get("message") or json.dumps(event, sort_keys=True)
                raise ProviderError(
                    message_text,
                    status_code=event.get("status") or event.get("status_code"),
                    body=json.dumps(event, sort_keys=True),
                )

            raise ProtocolError(f"unexpected websocket event: {json.dumps(event, sort_keys=True)}")

    async def _send_json(self, payload: dict[str, object]) -> None:
        try:
            await self.websocket.send(json.dumps(payload))
        except Exception as exc:
            if not _is_connection_closed(exc):
                raise
            raise ConnectionClosedError(
                f"websocket closed while sending text ({exc.code})",
                close_code=exc.code,
                reason=exc.reason or None,
            ) from exc

