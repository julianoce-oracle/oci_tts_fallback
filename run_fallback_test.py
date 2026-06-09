from __future__ import annotations

import argparse
import asyncio
import importlib.util
import json
import os
import sys
from pathlib import Path
from typing import Any

from tts_fallback.errors import SynthesisFailedError
from tts_fallback import (
    AudioCache,
    CacheConfig,
    EndpointConfig,
    FallbackEvent,
    FallbackMode,
    FallbackTTS,
    PoolConfig,
    Timeouts,
    build_provider_chain_from_env,
    codec_extension,
    is_cacheable,
    load_env_file,
    load_static_lines,
    stream_provider_fallback,
)


DEFAULT_TEXT = "Analisando a sua fatura atual em relacao a passada, o valor variou em R$ 226,11."
PROJECT_ROOT = Path(__file__).resolve().parent


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Test fallback across warm xAI OCI TTS WebSocket connections."
    )
    parser.add_argument("--env-file", default=str(PROJECT_ROOT / ".env"))
    parser.add_argument("--text", default=DEFAULT_TEXT)
    parser.add_argument("--voice", default="c8x2ieiocufs")
    parser.add_argument("--language", default="pt-BR")
    parser.add_argument("--codec", choices=["mp3", "pcm"], default="mp3")
    parser.add_argument("--sample-rate", type=int, default=24000)
    parser.add_argument("--bit-rate", type=int, default=128000)
    parser.add_argument("--api-key-env", default="OCI_GENAI_API_KEY")
    parser.add_argument("--pool-size", type=int, default=3)
    parser.add_argument("--mode", choices=[FallbackMode.SEQUENTIAL, FallbackMode.HEDGED], default=FallbackMode.SEQUENTIAL)
    parser.add_argument("--hedges", type=int, default=2)
    parser.add_argument("--connect-timeout", type=float, default=5.0)
    parser.add_argument("--first-audio-timeout", type=float, default=1.5)
    parser.add_argument("--chunk-timeout", type=float, default=10.0)
    parser.add_argument("--acquire-timeout", type=float, default=3.0)
    parser.add_argument("--output", default="xai-oci-fallback-output.mp3")
    parser.add_argument("--cache-dir", default=None)
    parser.add_argument("--cache-mode", choices=["off", "static", "all"], default=None)
    parser.add_argument("--static-lines-file", default=None)
    parser.add_argument("--recovery-cache-text", default=None)
    parser.add_argument("--recovery-retries", type=int, default=None)
    parser.add_argument("--disable-recovery-cache", action="store_true")
    parser.add_argument("--precache-static", action="store_true")
    parser.add_argument("--refresh-cache", action="store_true")
    parser.add_argument("--list-static-lines", action="store_true")
    parser.add_argument(
        "--provider-fallback-order",
        default=None,
        help="Comma-separated provider order used if xAI OCI does not produce audio. Defaults to PROVIDER_FALLBACK_ORDER; empty disables provider fallback.",
    )
    parser.add_argument(
        "--disable-provider-fallback",
        action="store_true",
        help="Only test xAI OCI WebSocket fallback; do not fall back to Microsoft or ElevenLabs.",
    )
    parser.add_argument(
        "--fault",
        choices=["none", "close-first-attempt"],
        default="none",
        help="Inject a deterministic failure to verify fallback behavior.",
    )
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--debug", action="store_true", help="Show Python traceback instead of a compact JSON error.")
    return parser


async def run(args: argparse.Namespace) -> int:
    load_env_file(args.env_file)

    cache_mode = args.cache_mode or os.getenv("TTS_CACHE_MODE", "static")
    provider_fallback_order = args.provider_fallback_order if args.provider_fallback_order is not None else os.getenv("PROVIDER_FALLBACK_ORDER", "")
    cache_dir = Path(args.cache_dir or os.getenv("TTS_CACHE_DIR", str(PROJECT_ROOT / "cache" / "audio")))
    static_lines_file = args.static_lines_file or os.getenv("TTS_CACHE_STATIC_LINES_FILE") or None
    static_lines = load_static_lines(static_lines_file)
    cache = AudioCache(CacheConfig(cache_dir=cache_dir, mode=cache_mode, static_lines_file=Path(static_lines_file) if static_lines_file else None))

    if args.list_static_lines:
        print(json.dumps({"staticLines": static_lines}, indent=2, ensure_ascii=False))
        return 0

    if args.precache_static:
        return await precache_static_lines(args, cache=cache, cache_mode=cache_mode, static_lines=static_lines)

    result = await synthesize_text(args.text, args, cache=cache, cache_mode=cache_mode, static_lines=static_lines)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(result["audio"])

    summary = {
        "output": str(output_path),
        "bytes": len(result["audio"]),
        "mode": args.mode,
        "poolSize": args.pool_size,
        "hedges": args.hedges if args.mode == FallbackMode.HEDGED else None,
        "fault": args.fault,
        "cache": {
            "mode": cache_mode,
            "dir": str(cache.cache_dir),
            "cacheable": result["cacheable"],
            "hit": result["cacheHit"],
            "saved": result["cacheSaved"],
            "key": result["cacheKey"],
        },
        "recovery": result["recovery"],
        "completedOriginal": result["completedOriginal"],
        "primaryError": result["primaryError"],
        "providerFallbackEnabled": bool(provider_fallback_order.strip()) and not args.disable_provider_fallback,
        "providerFallbackOrder": provider_fallback_order or None,
        "eventCounts": result["eventCounts"],
        "events": result["events"],
    }
    print(json.dumps(summary, indent=2, sort_keys=True, ensure_ascii=False))
    return 0


async def precache_static_lines(
    args: argparse.Namespace,
    *,
    cache: AudioCache,
    cache_mode: str,
    static_lines: list[str],
) -> int:
    records: list[dict[str, Any]] = []
    extension = codec_extension(args.codec)
    primary_profile = xai_cache_profile(args)
    for index, line in enumerate(static_lines, start=1):
        if not args.refresh_cache:
            cached = cache.get(line, profile=primary_profile, extension=extension)
            if cached:
                records.append(
                    {
                        "index": index,
                        "text": line,
                        "bytes": len(cached.audio),
                        "cacheHit": True,
                        "cacheSaved": False,
                        "cacheKey": cached.key,
                        "primaryError": None,
                    }
                )
                continue

        result = await synthesize_text(
            line,
            args,
            cache=cache,
            cache_mode="all" if args.refresh_cache else cache_mode,
            static_lines=static_lines,
            force_refresh=True,
        )
        records.append(
            {
                "index": index,
                "text": line,
                "bytes": len(result["audio"]),
                "cacheHit": result["cacheHit"],
                "cacheSaved": result["cacheSaved"],
                "cacheKey": result["cacheKey"],
                "primaryError": result["primaryError"],
            }
        )

    print(json.dumps({"cacheDir": str(cache.cache_dir), "records": records}, indent=2, ensure_ascii=False))
    return 0


async def synthesize_text(
    text: str,
    args: argparse.Namespace,
    *,
    cache: AudioCache,
    cache_mode: str,
    static_lines: list[str],
    force_refresh: bool = False,
) -> dict[str, Any]:
    endpoint = EndpointConfig(
        api_key_env=args.api_key_env,
        voice=args.voice,
        language=args.language,
        codec=args.codec,
        sample_rate=args.sample_rate,
        bit_rate=args.bit_rate,
    )
    timeouts = Timeouts(
        connect_s=args.connect_timeout,
        first_audio_s=args.first_audio_timeout,
        chunk_s=args.chunk_timeout,
        acquire_s=args.acquire_timeout,
    )
    pool = PoolConfig(size=args.pool_size)
    extension = codec_extension(args.codec)
    cacheable = is_cacheable(text, mode=cache_mode, static_lines=static_lines)

    audio = bytearray()
    synthesis_audio = bytearray()
    event_counts: dict[str, int] = {}
    events: list[dict[str, Any]] = []
    primary_error: str | None = None
    last_recovery_error: str | None = None
    cache_hit = False
    cache_saved = False
    cache_key: str | None = None
    winning_profile: dict[str, Any] | None = None
    winning_source: str | None = None
    fault_injected = False
    recovery_cache_text = args.recovery_cache_text or os.getenv("TTS_RECOVERY_CACHE_TEXT", "Por favor, aguarde um momento.")
    recovery_retries = args.recovery_retries
    if recovery_retries is None:
        recovery_retries = _int_env("TTS_RECOVERY_RETRIES", 1)
    recovery_retries = max(0, recovery_retries)
    recovery_audio_played = False
    recovery_cache_hit = False
    recovery_cache_key: str | None = None

    def record(event: FallbackEvent) -> None:
        event_counts[event.type] = event_counts.get(event.type, 0) + 1
        if event.type in {
            "attempt_started",
            "attempt_failed",
            "winner",
            "completed",
            "cache_hit",
            "cache_miss",
            "cache_saved",
            "recovery_retry",
        }:
            events.append(event.to_log_dict())
        if args.verbose:
            print(json.dumps(event.to_log_dict(), sort_keys=True, ensure_ascii=False), flush=True)

    async def on_attempt_start(slot: Any, attempt_number: int) -> None:
        nonlocal fault_injected
        if args.fault == "close-first-attempt" and not fault_injected:
            fault_injected = True
            if slot.connection is not None:
                await slot.connection.close()

    async def consume(event_stream: Any, *, capture: bytearray) -> None:
        nonlocal winning_profile, winning_source
        async for event in event_stream:
            record(event)
            if event.type == "audio" and event.audio:
                audio.extend(event.audio)
                capture.extend(event.audio)
            elif event.type == "winner":
                provider = event.meta.get("provider")
                if provider:
                    winning_source = str(provider)
                    winning_profile = provider_cache_profile(str(provider), args)
                else:
                    winning_source = "xai-oci"
                    winning_profile = xai_cache_profile(args)

    async def try_xai_once(*, recovery_attempt: int | None = None) -> tuple[bytes, str | None]:
        capture = bytearray()
        if recovery_attempt is not None:
            record(
                FallbackEvent(
                    type="recovery_retry",
                    request_id="recovery",
                    connection_id="xai-oci",
                    attempt=recovery_attempt,
                    message="tentando recuperar xAI OCI apos audio de espera",
                    meta={"recoveryAttempt": recovery_attempt},
                )
            )

        xai_api_key = endpoint.api_key or os.getenv(endpoint.api_key_env)
        if not xai_api_key:
            return b"", f"MissingApiKeyError: {endpoint.api_key_env} não está configurada"
        if importlib.util.find_spec("websockets") is None:
            return b"", "MissingDependencyError: pacote `websockets` não está instalado. Rode `pip install -r requirements.txt`."

        try:
            async with FallbackTTS(endpoint=endpoint, pool=pool, timeouts=timeouts) as client:
                await client.wait_until_ready(min_ready=1, timeout_s=args.connect_timeout + args.acquire_timeout)
                await consume(
                    client.stream(
                        text,
                        mode=args.mode,
                        hedges=args.hedges,
                        on_attempt_start=on_attempt_start if args.fault != "none" else None,
                    ),
                    capture=capture,
                )
        except Exception as exc:
            if capture:
                raise
            return b"", f"{type(exc).__name__}: {exc}"

        return bytes(capture), None

    primary_profile = xai_cache_profile(args)

    first_audio, primary_error = await try_xai_once()
    if first_audio:
        synthesis_audio.extend(first_audio)

    if primary_error and not synthesis_audio:
        if recovery_cache_text and not args.disable_recovery_cache and not force_refresh:
            recovery_profile = primary_profile
            cached = cache.get(recovery_cache_text, profile=recovery_profile, extension=extension)
            if cached:
                audio.extend(cached.audio)
                recovery_audio_played = True
                recovery_cache_hit = True
                cache_hit = True
                cache_key = cached.key
                recovery_cache_key = cached.key
                record(
                    FallbackEvent(
                        type="cache_hit",
                        request_id="cache",
                        connection_id="xai-oci-cache",
                        message="audio de espera servido do cache local; tentando recuperar websocket em seguida",
                        meta={
                            "path": str(cached.path),
                            "source": cached.metadata.get("source"),
                            "fallbackReason": primary_error,
                            "recoveryText": recovery_cache_text,
                            "role": "recovery_wait",
                        },
                    )
                )
            else:
                record(
                    FallbackEvent(
                        type="cache_miss",
                        request_id="cache",
                        connection_id="xai-oci-cache",
                        message="audio de espera nao encontrado no cache local",
                        meta={
                            "profile": recovery_profile,
                            "fallbackReason": primary_error,
                            "recoveryText": recovery_cache_text,
                            "role": "recovery_wait",
                        },
                    )
                )

        for retry_index in range(1, recovery_retries + 1):
            retry_audio, last_recovery_error = await try_xai_once(recovery_attempt=retry_index)
            if retry_audio:
                synthesis_audio.extend(retry_audio)
                break

        if not synthesis_audio:
            providers = [] if args.disable_provider_fallback else build_provider_chain_from_env(args.provider_fallback_order)
            if providers:
                provider_capture = bytearray()
                await consume(stream_provider_fallback(text, providers), capture=provider_capture)
                synthesis_audio.extend(provider_capture)
            elif recovery_audio_played:
                return build_result(
                    audio,
                    event_counts,
                    events,
                    primary_error,
                    cacheable,
                    cache_hit,
                    cache_saved,
                    cache_key,
                    completed_original=False,
                    recovery={
                        "audioPlayed": recovery_audio_played,
                        "cacheHit": recovery_cache_hit,
                        "cacheKey": recovery_cache_key,
                        "text": recovery_cache_text,
                        "retries": recovery_retries,
                        "lastError": last_recovery_error,
                    },
                )
            else:
                raise SynthesisFailedError(
                    "xAI OCI não gerou áudio, não havia áudio de espera em cache e nenhum provider externo opcional está configurado. "
                    f"Causa xAI: {primary_error.rstrip('.')}. "
                    "Pré-gere o áudio de espera em cache ou ative PROVIDER_FALLBACK_ORDER=microsoft/elevenlabs."
                )

    if cacheable and synthesis_audio:
        profile = winning_profile or primary_profile
        source = winning_source or "xai-oci"
        cached = cache.put(
            text,
            bytes(synthesis_audio),
            profile=profile,
            extension=extension,
            source=source,
            metadata={"primaryError": primary_error, "mode": args.mode},
        )
        cache_saved = True
        cache_key = cached.key
        record(
            FallbackEvent(
                type="cache_saved",
                request_id="cache",
                connection_id=f"{source}-cache",
                message="audio sintetizado salvo no cache local",
                meta={"path": str(cached.path), "source": source},
            )
        )

    return build_result(
        audio,
        event_counts,
        events,
        primary_error,
        cacheable,
        cache_hit,
        cache_saved,
        cache_key,
        completed_original=bool(synthesis_audio),
        recovery={
            "audioPlayed": recovery_audio_played,
            "cacheHit": recovery_cache_hit,
            "cacheKey": recovery_cache_key,
            "text": recovery_cache_text if recovery_audio_played else None,
            "retries": recovery_retries if recovery_audio_played else 0,
            "lastError": last_recovery_error,
        },
    )


def build_result(
    audio: bytearray,
    event_counts: dict[str, int],
    events: list[dict[str, Any]],
    primary_error: str | None,
    cacheable: bool,
    cache_hit: bool,
    cache_saved: bool,
    cache_key: str | None,
    *,
    completed_original: bool,
    recovery: dict[str, Any],
) -> dict[str, Any]:
    return {
        "audio": bytes(audio),
        "eventCounts": event_counts,
        "events": events,
        "primaryError": primary_error,
        "cacheable": cacheable,
        "cacheHit": cache_hit,
        "cacheSaved": cache_saved,
        "cacheKey": cache_key,
        "completedOriginal": completed_original,
        "recovery": recovery,
    }


def xai_cache_profile(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "provider": "xai-oci",
        "voice": args.voice,
        "language": args.language,
        "codec": args.codec,
        "sampleRate": args.sample_rate,
        "bitRate": args.bit_rate if args.codec == "mp3" else None,
    }


def provider_cache_profile(provider: str, args: argparse.Namespace) -> dict[str, Any]:
    normalized = provider.lower()
    if normalized == "microsoft":
        return {
            "provider": "microsoft",
            "voice": os.getenv("MICROSOFT_SPEECH_VOICE", "pt-BR-FranciscaNeural"),
            "language": os.getenv("MICROSOFT_SPEECH_LANGUAGE", "pt-BR"),
            "outputFormat": os.getenv("MICROSOFT_SPEECH_OUTPUT_FORMAT", "audio-24khz-48kbitrate-mono-mp3"),
            "region": os.getenv("MICROSOFT_SPEECH_REGION", "eastus"),
        }
    if normalized == "elevenlabs":
        return {
            "provider": "elevenlabs",
            "voiceId": os.getenv("ELEVENLABS_VOICE_ID", ""),
            "modelId": os.getenv("ELEVENLABS_MODEL_ID", "eleven_multilingual_v2"),
            "languageCode": os.getenv("ELEVENLABS_LANGUAGE_CODE", "pt"),
            "outputFormat": os.getenv("ELEVENLABS_OUTPUT_FORMAT", "mp3_44100_128"),
        }
    return {"provider": normalized}


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value >= 0 else default


def main() -> int:
    args = build_parser().parse_args()
    try:
        return asyncio.run(run(args))
    except Exception as exc:
        if args.debug:
            raise
        print(
            json.dumps(
                {
                    "error": {
                        "type": type(exc).__name__,
                        "message": str(exc),
                    }
                },
                indent=2,
                ensure_ascii=False,
            ),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
