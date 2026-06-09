from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


XAI_OCI_TTS_URL = "wss://inference.generativeai.us-chicago-1.oci.oraclecloud.com/xai/v1/tts"


def new_request_id() -> str:
    return uuid.uuid4().hex


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def monotonic_ns() -> int:
    return time.perf_counter_ns()


def elapsed_ms(start_ns: int | None, end_ns: int | None) -> float | None:
    if start_ns is None or end_ns is None:
        return None
    return round((end_ns - start_ns) / 1_000_000, 3)


@dataclass(frozen=True)
class EndpointConfig:
    url: str = XAI_OCI_TTS_URL
    api_key: str | None = None
    api_key_env: str = "OCI_GENAI_API_KEY"
    voice: str = "ara"
    language: str = "pt-BR"
    codec: str = "mp3"
    sample_rate: int = 24000
    bit_rate: int | None = 128000
    extra_query: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class Timeouts:
    connect_s: float = 5.0
    first_audio_s: float = 1.5
    chunk_s: float = 10.0
    acquire_s: float = 3.0
    close_s: float = 3.0
    ping_interval_s: float | None = None
    ping_timeout_s: float | None = None


@dataclass(frozen=True)
class FallbackEvent:
    type: str
    request_id: str
    connection_id: str | None = None
    attempt: int | None = None
    audio: bytes | None = None
    chunk_index: int | None = None
    message: str | None = None
    elapsed_ms: float | None = None
    meta: dict[str, Any] = field(default_factory=dict)

    def to_log_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "type": self.type,
            "requestId": self.request_id,
            "connectionId": self.connection_id,
            "attempt": self.attempt,
            "chunkIndex": self.chunk_index,
            "message": self.message,
            "elapsedMs": self.elapsed_ms,
            "meta": self.meta,
        }
        if self.audio is not None:
            payload["audioBytes"] = len(self.audio)
        return {key: value for key, value in payload.items() if value is not None}

