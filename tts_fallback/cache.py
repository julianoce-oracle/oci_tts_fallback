from __future__ import annotations

import hashlib
import json
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .types import utc_now


DEFAULT_STATIC_LINES_PT = [
    "Bem-vindo de volta.",
    "Por favor, aguarde um momento.",
    "Um momento, por favor.",
    "Estou verificando isso agora.",
    "Obrigado pela paciência.",
    "Pode repetir, por favor?",
    "Não consegui processar sua solicitação agora.",
    "Sua solicitação foi concluída.",
    "Estamos transferindo seu atendimento.",
    "Ainda estou aqui.",
]


@dataclass(frozen=True)
class CacheConfig:
    cache_dir: Path = Path("cache/audio")
    mode: str = "static"
    static_lines_file: Path | None = None


@dataclass(frozen=True)
class CachedAudio:
    key: str
    path: Path
    meta_path: Path
    audio: bytes
    metadata: dict[str, Any] = field(default_factory=dict)


class AudioCache:
    def __init__(self, config: CacheConfig) -> None:
        self.config = config
        self.cache_dir = Path(config.cache_dir).expanduser().resolve()
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def get(self, text: str, *, profile: dict[str, Any], extension: str) -> CachedAudio | None:
        key = self.cache_key(text, profile=profile)
        audio_path = self.audio_path(key, extension)
        if not audio_path.exists():
            return None

        meta_path = self.meta_path(key)
        metadata: dict[str, Any] = {}
        if meta_path.exists():
            try:
                metadata = json.loads(meta_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                metadata = {}

        return CachedAudio(
            key=key,
            path=audio_path,
            meta_path=meta_path,
            audio=audio_path.read_bytes(),
            metadata=metadata,
        )

    def put(
        self,
        text: str,
        audio: bytes,
        *,
        profile: dict[str, Any],
        extension: str,
        source: str,
        metadata: dict[str, Any] | None = None,
    ) -> CachedAudio:
        key = self.cache_key(text, profile=profile)
        audio_path = self.audio_path(key, extension)
        meta_path = self.meta_path(key)

        tmp_audio_path = audio_path.with_suffix(audio_path.suffix + ".tmp")
        tmp_audio_path.write_bytes(audio)
        tmp_audio_path.replace(audio_path)

        payload = {
            "key": key,
            "source": source,
            "text": text,
            "normalizedText": normalize_text(text),
            "profile": profile,
            "audioFile": str(audio_path),
            "bytes": len(audio),
            "createdAtUtc": utc_now(),
            "metadata": metadata or {},
        }
        tmp_meta_path = meta_path.with_suffix(meta_path.suffix + ".tmp")
        tmp_meta_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        tmp_meta_path.replace(meta_path)

        return CachedAudio(
            key=key,
            path=audio_path,
            meta_path=meta_path,
            audio=audio,
            metadata=payload,
        )

    def cache_key(self, text: str, *, profile: dict[str, Any]) -> str:
        payload = {
            "version": 1,
            "normalizedText": normalize_text(text),
            "profile": profile,
        }
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def audio_path(self, key: str, extension: str) -> Path:
        clean_ext = extension.strip().lower().lstrip(".") or "bin"
        return self.cache_dir / f"{key}.{clean_ext}"

    def meta_path(self, key: str) -> Path:
        return self.cache_dir / f"{key}.json"


def normalize_text(text: str) -> str:
    compact = " ".join(text.strip().split()).casefold()
    normalized = unicodedata.normalize("NFKD", compact)
    return "".join(char for char in normalized if not unicodedata.combining(char))


def load_static_lines(path: str | Path | None = None) -> list[str]:
    lines = list(DEFAULT_STATIC_LINES_PT)
    if path:
        lines_path = Path(path).expanduser()
        if lines_path.exists():
            for raw_line in lines_path.read_text(encoding="utf-8").splitlines():
                line = raw_line.strip()
                if line and not line.startswith("#"):
                    lines.append(line)

    seen: set[str] = set()
    deduped: list[str] = []
    for line in lines:
        normalized = normalize_text(line)
        if normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(line)
    return deduped


def is_cacheable(text: str, *, mode: str, static_lines: list[str]) -> bool:
    normalized_mode = mode.strip().lower()
    if normalized_mode == "off":
        return False
    if normalized_mode == "all":
        return True
    if normalized_mode != "static":
        raise ValueError("cache mode must be one of: off, static, all")

    static_set = {normalize_text(line) for line in static_lines}
    return normalize_text(text) in static_set


def codec_extension(codec: str) -> str:
    normalized = codec.strip().lower()
    if normalized in {"mp3", "pcm", "wav", "ogg"}:
        return normalized
    return "bin"
