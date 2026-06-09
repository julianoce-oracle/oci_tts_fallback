from __future__ import annotations


class TTSFallbackError(Exception):
    """Base error for the fallback library."""


class MissingApiKeyError(TTSFallbackError):
    pass


class ConnectionOpenError(TTSFallbackError):
    pass


class NoHealthyConnectionError(TTSFallbackError):
    pass


class ProviderError(TTSFallbackError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        body: str | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.body = body


class ProtocolError(TTSFallbackError):
    pass


class FirstAudioTimeout(TTSFallbackError):
    pass


class ChunkTimeout(TTSFallbackError):
    pass


class ConnectionClosedError(TTSFallbackError):
    def __init__(self, message: str, *, close_code: int | None = None, reason: str | None = None) -> None:
        super().__init__(message)
        self.close_code = close_code
        self.reason = reason


class PartialSynthesisError(TTSFallbackError):
    pass


class SynthesisFailedError(TTSFallbackError):
    pass


class CancelledInFlight(TTSFallbackError):
    pass

