# TTS Fallback

Uma biblioteca Python para deixar TTS mais resiliente em aplicações de voz em tempo real.

A lib usa **xAI OCI via WebSocket** como provedor principal. Se o WebSocket falhar antes de produzir áudio, ela pode:

- tentar outro WebSocket quente;
- tocar uma frase curta de espera a partir do cache local;
- tentar recuperar o WebSocket principal;
- cair para Microsoft Azure Speech ou ElevenLabs em modo streaming.

A ideia não é trocar de voz no meio da fala. A ideia é evitar silêncio e recuperar com segurança.

## Modelo Mental

Pense no fluxo assim:

```text
1. tente falar pelo xAI OCI
2. se ainda não saiu áudio, é seguro tentar outra rota
3. se demorar ou cair, toque uma frase de espera cacheada
4. tente recuperar o WebSocket principal
5. se não recuperar, use outro provider streaming
6. se a fala original já começou, não reinicie a frase automaticamente
```

A regra principal é simples:

```text
Depois que uma origem começou a emitir áudio, ela é a dona daquela fala.
```

Isso evita áudio duplicado quando uma tentativa atrasada responde depois.

## Instalação

```bash
pip install -r requirements.txt
```

Para usar localmente sem empacotar a lib:

```bash
cd /home/ubuntu/tts-fallback2
python3 seu_script.py
```

ou:

```bash
PYTHONPATH=/home/ubuntu/tts-fallback2 python3 seu_script.py
```

Configure a chave do provedor principal:

```text
OCI_GENAI_API_KEY=...
```

## Quick Start

```python
import asyncio
from pathlib import Path

from tts_fallback import EndpointConfig, FallbackMode, FallbackTTS, PoolConfig, Timeouts


async def main() -> None:
    endpoint = EndpointConfig(
        api_key_env="OCI_GENAI_API_KEY",
        voice="c8x2ieiocufs",
        language="pt-BR",
        codec="mp3",
    )

    audio = bytearray()

    async with FallbackTTS(
        endpoint=endpoint,
        pool=PoolConfig(size=3),
        timeouts=Timeouts(first_audio_s=1.5, chunk_s=10.0),
    ) as tts:
        await tts.wait_until_ready(min_ready=1, timeout_s=8.0)

        async for event in tts.stream("Por favor, aguarde um momento.", mode=FallbackMode.SEQUENTIAL):
            if event.type == "audio" and event.audio:
                audio.extend(event.audio)
            else:
                print(event.to_log_dict())

    Path("/tmp/tts-output.mp3").write_bytes(audio)


asyncio.run(main())
```

## Eventos

A lib emite eventos para você conectar com seu player, logs ou métricas.

| Evento | Quando aparece | O que fazer |
| --- | --- | --- |
| `attempt_started` | Uma tentativa começou | Log/métrica |
| `winner` | A primeira origem entregou áudio válido | Marcar rota vencedora |
| `audio` | Chegou um chunk de áudio | Enviar ao player/cliente |
| `completed` | A fala terminou | Fechar a resposta |
| `attempt_failed` | Uma tentativa falhou | Log/métrica |
| `cache_hit` | A frase de espera saiu do cache | Tocar espera e continuar recuperação |
| `recovery_retry` | A lib tentou recuperar xAI depois da espera | Log/métrica |
| `cache_saved` | Um áudio foi salvo para uso futuro | Log/métrica |

## Padrões De Uso

### 1. Sequential: modo padrão e conservador

Use quando você quer tentar um WebSocket saudável por vez.

```python
async for event in tts.stream(text, mode=FallbackMode.SEQUENTIAL):
    if event.type == "audio":
        player.write(event.audio)
```

Se o primeiro socket falhar **antes** do primeiro áudio, outro socket pode assumir. Se a fala já começou, a lib evita reiniciar a frase por padrão.

Bom para:

- fluxo normal de produção;
- menor custo;
- evitar duplicação de fala.

### 2. Hedged: menor latência, maior custo

Use quando p95/p99 de latência importa muito.

```python
async for event in tts.stream(text, mode=FallbackMode.HEDGED, hedges=2):
    if event.type == "audio":
        player.write(event.audio)
```

No hedged, a mesma frase vai para mais de um WebSocket. O primeiro que emitir áudio vence; os outros são cancelados ou ignorados.

Isso é o que antes estava descrito como `winner-takes-stream`: não é uma feature separada para você configurar, é a regra de segurança que faz o hedged não duplicar áudio.

Bom para:

- reduzir cauda de latência;
- lidar com sockets lentos;
- planos premium ou chamadas críticas.

### 3. Frase de espera em cache

O cache não substitui o texto original. Ele toca uma frase curta enquanto a aplicação tenta recuperar o WebSocket.

Exemplo:

```text
Por favor, aguarde um momento.
```

Fluxo recomendado:

```text
xAI falhou antes de áudio
-> tocar frase de espera cacheada
-> tentar xAI de novo
-> se recuperar, falar o texto original
-> se não recuperar, cair para provider externo
```

Variáveis úteis:

```text
TTS_RECOVERY_CACHE_TEXT=Por favor, aguarde um momento.
TTS_RECOVERY_RETRIES=1
TTS_CACHE_MODE=static
TTS_CACHE_DIR=/home/ubuntu/tts-fallback2/cache/audio
```

### 4. Provider fallback streaming

Microsoft e ElevenLabs entram quando:

```text
xAI não produziu nenhum áudio
a frase de espera já foi tocada ou não estava disponível
a recuperação do xAI não funcionou
```

Ambos são consumidos como streaming pela lib: cada bloco recebido vira um evento `audio`.

Configuração Microsoft:

```text
PROVIDER_FALLBACK_ORDER=microsoft
MICROSOFT_SPEECH_KEY=...
MICROSOFT_SPEECH_REGION=eastus
MICROSOFT_SPEECH_VOICE=pt-BR-FranciscaNeural
MICROSOFT_SPEECH_OUTPUT_FORMAT=audio-24khz-48kbitrate-mono-mp3
MICROSOFT_SPEECH_CHUNK_SIZE=8192
```

Configuração ElevenLabs:

```text
PROVIDER_FALLBACK_ORDER=elevenlabs
ELEVENLABS_API_KEY=...
ELEVENLABS_VOICE_ID=...
ELEVENLABS_MODEL_ID=eleven_multilingual_v2
ELEVENLABS_OUTPUT_FORMAT=mp3_44100_128
ELEVENLABS_CHUNK_SIZE=8192
```

Ordem com os dois:

```text
PROVIDER_FALLBACK_ORDER=microsoft,elevenlabs
```

## O Que A Lib Protege

| Situação | Proteção |
| --- | --- |
| WebSocket lento para começar | `first_audio_timeout` + tentativa em outro socket |
| Socket fecha antes do áudio | fallback sequencial |
| Socket responde devagar | hedged request |
| Endpoint fica temporariamente indisponível | frase de espera cacheada + retry |
| xAI não recupera | Microsoft/ElevenLabs streaming |
| Tentativa atrasada responde depois | apenas a origem vencedora continua |
| Stream trava no meio da fala | `PartialSynthesisError` para evitar duplicação |

## Configuração Completa

### xAI OCI

```text
OCI_GENAI_API_KEY=...
```

### Timeouts principais

Configurados via `Timeouts` no código:

```python
Timeouts(
    connect_s=5.0,
    first_audio_s=1.5,
    chunk_s=10.0,
    acquire_s=3.0,
)
```

### Cache

```text
TTS_CACHE_MODE=static
TTS_CACHE_DIR=/home/ubuntu/tts-fallback2/cache/audio
TTS_CACHE_STATIC_LINES_FILE=
TTS_RECOVERY_CACHE_TEXT=Por favor, aguarde um momento.
TTS_RECOVERY_RETRIES=1
```

Modos de cache:

```text
off     não usa cache
static  só usa frases estáticas conhecidas
all     permite cachear qualquer texto
```

Use `static` por padrão para evitar armazenar textos dinâmicos sensíveis.

## Testes Com CLI

`run_fallback_test.py` é só um utilitário para validar comportamento. A lib em si deve ser importada pelo seu app.

### Testar fluxo normal

```bash
python3 run_fallback_test.py \
  --mode sequential \
  --pool-size 3 \
  --cache-mode off \
  --text "Teste normal com xAI OCI." \
  --output /tmp/xai-normal.mp3 \
  --verbose
```

Esperado:

```text
attempt_started
winner
audio
completed
```

### Testar falha antes do áudio

```bash
python3 run_fallback_test.py \
  --mode sequential \
  --pool-size 3 \
  --cache-mode off \
  --fault close-first-attempt \
  --text "Teste de fallback sequencial." \
  --output /tmp/sequential.mp3 \
  --verbose
```

Esperado:

```text
attempt_failed
winner
completed
```

### Testar hedged

```bash
python3 run_fallback_test.py \
  --mode hedged \
  --pool-size 3 \
  --hedges 2 \
  --cache-mode off \
  --text "Teste hedged." \
  --output /tmp/hedged.mp3 \
  --verbose
```

Esperado: dois `attempt_started`, um `winner`, um `completed`.

### Testar frase de espera cacheada

Este teste força o xAI a falhar usando uma variável inexistente. Se a frase de espera já estiver no cache, ela toca e a lib tenta recuperar depois.

```bash
python3 run_fallback_test.py \
  --api-key-env MISSING_OCI_KEY \
  --disable-provider-fallback \
  --text "Texto original que deveria vir depois da espera." \
  --output /tmp/recovery-wait.mp3 \
  --verbose
```

Esperado:

```text
cache_hit
recovery_retry
completedOriginal: false
recovery.audioPlayed: true
```

`completedOriginal: false` significa que só a espera foi gerada nesse teste, porque a chave continua ausente.

### Testar provider fallback

```bash
python3 run_fallback_test.py \
  --api-key-env MISSING_OCI_KEY \
  --cache-mode off \
  --provider-fallback-order microsoft,elevenlabs \
  --text "Teste de provider fallback." \
  --output /tmp/provider-fallback.mp3 \
  --verbose
```

Esperado: `winner` no provider que responder primeiro dentro da ordem configurada e múltiplos eventos `audio` se a resposta vier em vários chunks.

### Testar falha no meio do stream

```bash
python3 run_fallback_test.py \
  --mode sequential \
  --pool-size 1 \
  --cache-mode off \
  --chunk-timeout 0.05 \
  --disable-provider-fallback \
  --text "Texto longo para gerar vários chunks de áudio." \
  --output /tmp/chunk-timeout.mp3 \
  --verbose \
  --debug
```

Esperado:

```text
winner
audio
attempt_failed: ChunkTimeout
PartialSynthesisError
```

Isso é intencional: se o usuário já ouviu parte da fala, a lib não reinicia automaticamente a frase inteira.

## Troubleshooting

### `PartialSynthesisError`

A fala original já tinha começado e o stream falhou. A lib parou para evitar duplicar áudio.

### `cache_miss` na frase de espera

A frase definida em `TTS_RECOVERY_CACHE_TEXT` não está pré-gerada para o mesmo perfil de voz. Gere essa frase uma vez com xAI funcionando ou rode o pré-cache das frases estáticas.

### Provider externo não entra

Por padrão, provider externo fica desativado:

```text
PROVIDER_FALLBACK_ORDER=
```

Configure `microsoft`, `elevenlabs` ou `microsoft,elevenlabs` e preencha as chaves correspondentes.