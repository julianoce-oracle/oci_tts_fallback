from __future__ import annotations

import asyncio
import json
import os
import threading
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Protocol
from xml.sax.saxutils import escape

from .errors import MissingApiKeyError, ProviderError, SynthesisFailedError
from .types import FallbackEvent, elapsed_ms, monotonic_ns, new_request_id


class HttpTTSProvider(Protocol):
    name: str

    async def stream_synthesize(self, text: str) -> AsyncIterator[bytes]:
        ...

    async def synthesize(self, text: str) -> bytes:
        ...


@dataclass(frozen=True)
class ElevenLabsConfig:
    api_key: str | None = None
    api_key_env: str = "ELEVENLABS_API_KEY"
    voice_id: str = ""
    model_id: str = "eleven_multilingual_v2"
    language_code: str | None = "pt"
    output_format: str = "mp3_44100_128"
    base_url: str = "https://api.elevenlabs.io/v1/text-to-speech"
    timeout_s: float = 30.0
    chunk_size: int = 8192


@dataclass(frozen=True)
class MicrosoftSpeechConfig:
    api_key: str | None = None
    api_key_env: str = "MICROSOFT_SPEECH_KEY"
    region: str = "eastus"
    endpoint: str | None = None
    voice: str = "pt-BR-FranciscaNeural"
    language: str = "pt-BR"
    output_format: str = "audio-24khz-48kbitrate-mono-mp3"
    timeout_s: float = 30.0
    chunk_size: int = 8192


class ElevenLabsProvider:
    name = "elevenlabs"

    def __init__(self, config: ElevenLabsConfig) -> None:
        self.config = config

    async def synthesize(self, text: str) -> bytes:
        audio = bytearray()
        async for chunk in self.stream_synthesize(text):
            audio.extend(chunk)
        return bytes(audio)

    async def stream_synthesize(self, text: str) -> AsyncIterator[bytes]:
        api_key = self.config.api_key or os.getenv(self.config.api_key_env)
        if not api_key:
            raise MissingApiKeyError(f"{self.config.api_key_env} is not set")
        if not self.config.voice_id:
            raise ProviderError("ELEVENLABS_VOICE_ID is not set")

        query = urllib.parse.urlencode({"output_format": self.config.output_format})
        url = f"{self.config.base_url.rstrip('/')}/{self.config.voice_id}/stream?{query}"
        body: dict[str, object] = {
            "text": text,
            "model_id": self.config.model_id,
        }
        if self.config.language_code:
            body["language_code"] = self.config.language_code

        async for chunk in _post_byte_stream(
            url,
            headers={
                "Content-Type": "application/json",
                "xi-api-key": api_key,
            },
            body=json.dumps(body).encode("utf-8"),
            timeout_s=self.config.timeout_s,
            provider_name=self.name,
            chunk_size=self.config.chunk_size,
        ):
            yield chunk


class MicrosoftSpeechProvider:
    name = "microsoft"

    def __init__(self, config: MicrosoftSpeechConfig) -> None:
        self.config = config

    async def synthesize(self, text: str) -> bytes:
        audio = bytearray()
        async for chunk in self.stream_synthesize(text):
            audio.extend(chunk)
        return bytes(audio)

    async def stream_synthesize(self, text: str) -> AsyncIterator[bytes]:
        api_key = self.config.api_key or os.getenv(self.config.api_key_env)
        if not api_key:
            raise MissingApiKeyError(f"{self.config.api_key_env} is not set")

        endpoint = self.config.endpoint or (
            f"https://{self.config.region}.tts.speech.microsoft.com/cognitiveservices/v1"
        )
        ssml = (
            f"<speak version='1.0' xml:lang='{escape(self.config.language)}'>"
            f"<voice xml:lang='{escape(self.config.language)}' name='{escape(self.config.voice)}'>"
            f"{escape(text)}"
            "</voice></speak>"
        )

        async for chunk in _post_byte_stream(
            endpoint,
            headers={
                "Content-Type": "application/ssml+xml",
                "Ocp-Apim-Subscription-Key": api_key,
                "X-Microsoft-OutputFormat": self.config.output_format,
                "User-Agent": "tts-fallback",
            },
            body=ssml.encode("utf-8"),
            timeout_s=self.config.timeout_s,
            provider_name=self.name,
            chunk_size=self.config.chunk_size,
        ):
            yield chunk


def build_provider_chain_from_env(order: str | None = None) -> list[HttpTTSProvider]:
    configured_order = order if order is not None else os.getenv("PROVIDER_FALLBACK_ORDER", "")
    providers: list[HttpTTSProvider] = []

    for raw_name in configured_order.split(","):
        name = raw_name.strip().lower()
        if not name:
            continue
        if name == "microsoft":
            api_key = os.getenv("MICROSOFT_SPEECH_KEY")
            if not api_key:
                continue
            providers.append(
                MicrosoftSpeechProvider(
                    MicrosoftSpeechConfig(
                        api_key=api_key,
                        region=os.getenv("MICROSOFT_SPEECH_REGION", "eastus"),
                        endpoint=os.getenv("MICROSOFT_SPEECH_ENDPOINT") or None,
                        voice=os.getenv("MICROSOFT_SPEECH_VOICE", "pt-BR-FranciscaNeural"),
                        language=os.getenv("MICROSOFT_SPEECH_LANGUAGE", "pt-BR"),
                        output_format=os.getenv(
                            "MICROSOFT_SPEECH_OUTPUT_FORMAT",
                            "audio-24khz-48kbitrate-mono-mp3",
                        ),
                        timeout_s=_float_env("MICROSOFT_SPEECH_TIMEOUT", 30.0),
                        chunk_size=_int_env("MICROSOFT_SPEECH_CHUNK_SIZE", 8192),
                    )
                )
            )
        elif name == "elevenlabs":
            api_key = os.getenv("ELEVENLABS_API_KEY")
            voice_id = os.getenv("ELEVENLABS_VOICE_ID", "")
            if not api_key or not voice_id:
                continue
            providers.append(
                ElevenLabsProvider(
                    ElevenLabsConfig(
                        api_key=api_key,
                        voice_id=voice_id,
                        model_id=os.getenv("ELEVENLABS_MODEL_ID", "eleven_multilingual_v2"),
                        language_code=os.getenv("ELEVENLABS_LANGUAGE_CODE", "pt") or None,
                        output_format=os.getenv("ELEVENLABS_OUTPUT_FORMAT", "mp3_44100_128"),
                        base_url=os.getenv(
                            "ELEVENLABS_BASE_URL",
                            "https://api.elevenlabs.io/v1/text-to-speech",
                        ),
                        timeout_s=_float_env("ELEVENLABS_TIMEOUT", 30.0),
                        chunk_size=_int_env("ELEVENLABS_CHUNK_SIZE", 8192),
                    )
                )
            )
        else:
            raise ValueError(f"unknown provider in PROVIDER_FALLBACK_ORDER: {name}")

    return providers


async def stream_provider_fallback(
    text: str,
    providers: list[HttpTTSProvider],
    *,
    request_id: str | None = None,
) -> AsyncIterator[FallbackEvent]:
    request_id = request_id or new_request_id()
    last_error: BaseException | None = None

    for attempt, provider in enumerate(providers, start=1):
        started_ns = monotonic_ns()
        chunk_count = 0
        byte_count = 0
        yielded_winner = False
        yield FallbackEvent(
            type="attempt_started",
            request_id=request_id,
            connection_id=provider.name,
            attempt=attempt,
            meta={"mode": "provider_fallback", "provider": provider.name, "streaming": True},
        )
        try:
            async for chunk in provider.stream_synthesize(text):
                if not chunk:
                    continue
                chunk_count += 1
                byte_count += len(chunk)

                if not yielded_winner:
                    yielded_winner = True
                    yield FallbackEvent(
                        type="winner",
                        request_id=request_id,
                        connection_id=provider.name,
                        attempt=attempt,
                        elapsed_ms=elapsed_ms(started_ns, monotonic_ns()),
                        meta={"mode": "provider_fallback", "provider": provider.name, "streaming": True},
                    )

                yield FallbackEvent(
                    type="audio",
                    request_id=request_id,
                    connection_id=provider.name,
                    attempt=attempt,
                    chunk_index=chunk_count,
                    audio=chunk,
                    meta={"mode": "provider_fallback", "provider": provider.name, "streaming": True},
                )

            if chunk_count == 0:
                raise ProviderError(f"{provider.name} returned no audio chunks")

            yield FallbackEvent(
                type="completed",
                request_id=request_id,
                connection_id=provider.name,
                attempt=attempt,
                elapsed_ms=elapsed_ms(started_ns, monotonic_ns()),
                meta={
                    "mode": "provider_fallback",
                    "provider": provider.name,
                    "streaming": True,
                    "chunks": chunk_count,
                    "bytes": byte_count,
                },
            )
            return
        except Exception as exc:
            last_error = exc
            yield FallbackEvent(
                type="attempt_failed",
                request_id=request_id,
                connection_id=provider.name,
                attempt=attempt,
                message=f"{type(exc).__name__}: {exc}",
                elapsed_ms=elapsed_ms(started_ns, monotonic_ns()),
                meta={"mode": "provider_fallback", "provider": provider.name, "streaming": True},
            )

    raise SynthesisFailedError(f"all provider fallbacks failed: {last_error}") from last_error


async def _post_byte_stream(
    url: str,
    *,
    headers: dict[str, str],
    body: bytes,
    timeout_s: float,
    provider_name: str,
    chunk_size: int,
) -> AsyncIterator[bytes]:
    loop = asyncio.get_running_loop()
    queue: asyncio.Queue[bytes | BaseException | None] = asyncio.Queue()
    request = urllib.request.Request(url, data=body, headers=headers, method="POST")
    read_size = max(1, chunk_size)

    def publish(item: bytes | BaseException | None) -> None:
        loop.call_soon_threadsafe(queue.put_nowait, item)

    def worker() -> None:
        try:
            with urllib.request.urlopen(request, timeout=timeout_s) as response:
                while True:
                    chunk = response.read(read_size)
                    if not chunk:
                        break
                    publish(chunk)
        except urllib.error.HTTPError as exc:
            error_body = exc.read().decode("utf-8", errors="replace")[:2000]
            publish(
                ProviderError(
                    f"{provider_name} returned HTTP {exc.code}: {error_body}",
                    status_code=exc.code,
                    body=error_body,
                )
            )
        except urllib.error.URLError as exc:
            publish(ProviderError(f"{provider_name} request failed: {exc.reason}"))
        except Exception as exc:  # pragma: no cover - defensive bridge from thread to async loop.
            publish(ProviderError(f"{provider_name} streaming request failed: {exc}"))
        finally:
            publish(None)

    threading.Thread(target=worker, name=f"{provider_name}-tts-stream", daemon=True).start()

    while True:
        item = await queue.get()
        if item is None:
            break
        if isinstance(item, BaseException):
            raise item
        yield item


def _float_env(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default
