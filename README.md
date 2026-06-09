# TTS Fallback

Biblioteca standalone para implementar uma camada de resiliência de TTS em nível de aplicação usando xAI OCI via WebSocket como provedor primário.

Ela foi pensada para aplicações de voz em tempo real que precisam continuar respondendo quando o WebSocket primário fica lento, fecha antes do áudio, não entrega o primeiro chunk ou o provedor principal fica indisponível.

Endpoint primário:

```text
wss://inference.generativeai.us-chicago-1.oci.oraclecloud.com/xai/v1/tts
```


## Instalação

Instale as dependências:

```bash
pip install -r requirements.txt
```

Para importar a lib em scripts locais, execute a partir da raiz do projeto ou adicione o diretório ao `PYTHONPATH`:

```bash
cd /home/ubuntu/tts-fallback
python3 seu_script.py
```

ou:

```bash
PYTHONPATH=/home/ubuntu/tts-fallback python3 seu_script.py
```

Configure a chave primária da xAI OCI no ambiente ou em um arquivo `.env` carregado pela sua aplicação:

```text
OCI_GENAI_API_KEY=...
```

Se quiser provider fallback externo, configure também Microsoft ou ElevenLabs:

```text
PROVIDER_FALLBACK_ORDER=microsoft
MICROSOFT_SPEECH_KEY=...
MICROSOFT_SPEECH_REGION=eastus
MICROSOFT_SPEECH_VOICE=pt-BR-FranciscaNeural
```

ou:

```text
PROVIDER_FALLBACK_ORDER=elevenlabs
ELEVENLABS_API_KEY=...
ELEVENLABS_VOICE_ID=...
```

## Uso Como Biblioteca

O ponto principal da lib é `FallbackTTS`. Ele abre um pool de WebSockets xAI OCI, emite eventos de síntese e entrega chunks de áudio conforme chegam.

Exemplo mínimo:

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
        sample_rate=24000,
        bit_rate=128000,
    )

    audio = bytearray()

    async with FallbackTTS(
        endpoint=endpoint,
        pool=PoolConfig(size=3),
        timeouts=Timeouts(
            connect_s=5.0,
            first_audio_s=1.5,
            chunk_s=10.0,
            acquire_s=3.0,
        ),
    ) as tts:
        await tts.wait_until_ready(min_ready=1, timeout_s=8.0)

        async for event in tts.stream(
            "Por favor, aguarde um momento.",
            mode=FallbackMode.SEQUENTIAL,
        ):
            if event.type == "audio" and event.audio:
                audio.extend(event.audio)
            else:
                print(event.to_log_dict())

    Path("/tmp/tts-output.mp3").write_bytes(audio)


asyncio.run(main())
```

Eventos importantes:

```text
attempt_started  uma tentativa começou em um socket/provider
winner           uma origem começou a entregar áudio válido
audio            chunk de áudio recebido
completed        síntese concluída
attempt_failed   uma tentativa falhou
cache_hit        áudio local usado como fallback de recuperação
cache_saved      áudio salvo para possível recuperação futura
```

## Política Recomendada

A política recomendada para produto é:

```text
1. xAI OCI via WebSocket é a primeira opção.
2. O pool tenta manter conexões saudáveis.
3. Se um socket falha antes do primeiro áudio, outro socket pode assumir.
4. Se nenhum áudio saiu do xAI OCI, toca uma frase curta de espera vinda do cache local.
5. Depois da frase de espera, tenta recuperar o xAI OCI e sintetizar o texto original novamente.
6. Se ainda não houver WebSocket funcional, pode cair para Microsoft ou ElevenLabs.
7. Se algum áudio do texto original já saiu, não reinicia a frase inteira automaticamente.
```

O ponto mais importante é a fronteira entre antes e depois do primeiro áudio:

```text
antes do primeiro áudio: é seguro tentar outra rota
depois do primeiro áudio: reiniciar pode duplicar fala para o usuário
```

Por isso, falhas no meio do stream geram `PartialSynthesisError` por padrão.

## Implementando Fallback Sequencial

Use `FallbackMode.SEQUENTIAL` quando quiser o comportamento mais conservador: tentar um socket e, se ele falhar antes do primeiro chunk de áudio, tentar outro socket saudável do pool.

```python
from tts_fallback import FallbackMode

async for event in tts.stream(
    text,
    mode=FallbackMode.SEQUENTIAL,
):
    if event.type == "audio":
        player.write(event.audio)
```

Esse modo cobre:

```text
falha ao conectar
first audio timeout
evento de erro do provider
close inesperado antes de áudio
mensagem inválida do protocolo
```

Se a falha acontece depois que o áudio já começou, a lib não reinicia o texto por padrão.

## Implementando Hedged Request

Use `FallbackMode.HEDGED` quando latência p95/p99 for mais importante que custo. A lib envia a mesma frase para mais de um WebSocket e usa o primeiro que devolver áudio.

```python
async for event in tts.stream(
    text,
    mode=FallbackMode.HEDGED,
    hedges=2,
):
    if event.type == "winner":
        print("socket vencedor", event.connection_id)
    elif event.type == "audio":
        player.write(event.audio)
```

Depois que existe um vencedor:

```text
chunks do vencedor são emitidos
sockets perdedores são fechados
perdedores são repostos pelo reconnect
áudio atrasado dos perdedores é ignorado
```

## Implementando Cache Como Fallback De Recuperação

O cache local não deve ser usado como primeira opção e também não deve substituir o texto original. Ele serve para tocar uma frase curta de espera enquanto a aplicação tenta recuperar um WebSocket funcional.

Use o cache assim:

```text
1. tente xAI OCI normalmente para o texto original
2. se xAI OCI não produzir nenhum áudio, toque uma frase cacheada de espera
3. depois da frase de espera, tente xAI OCI novamente para o texto original
4. se recuperar, entregue o áudio original depois da espera
5. se não recuperar, caia para provider externo ou encerre com erro controlado
```

Exemplo de frase de espera:

```text
Por favor, aguarde um momento.
```

Exemplo de integração:

```python
from pathlib import Path

from tts_fallback import (
    AudioCache,
    CacheConfig,
    EndpointConfig,
    FallbackMode,
    FallbackTTS,
    codec_extension,
)


def xai_cache_profile(endpoint: EndpointConfig) -> dict[str, object]:
    return {
        "provider": "xai-oci",
        "voice": endpoint.voice,
        "language": endpoint.language,
        "codec": endpoint.codec,
        "sampleRate": endpoint.sample_rate,
        "bitRate": endpoint.bit_rate if endpoint.codec == "mp3" else None,
    }


async def try_xai(text: str, tts: FallbackTTS) -> bytes:
    audio = bytearray()
    async for event in tts.stream(text, mode=FallbackMode.SEQUENTIAL):
        if event.type == "audio" and event.audio:
            audio.extend(event.audio)
    return bytes(audio)


async def synthesize_with_wait_recovery(text: str, tts: FallbackTTS, endpoint: EndpointConfig) -> bytes:
    cache = AudioCache(CacheConfig(cache_dir=Path("cache/audio"), mode="static"))
    output = bytearray()

    try:
        audio = await try_xai(text, tts)
        if audio:
            return audio
    except Exception:
        # Se já houve áudio parcial do texto original, deixe a exceção subir no seu app.
        pass

    wait = cache.get(
        "Por favor, aguarde um momento.",
        profile=xai_cache_profile(endpoint),
        extension=codec_extension(endpoint.codec),
    )
    if wait:
        output.extend(wait.audio)
        # No app real, envie este áudio ao player imediatamente.

    recovered_audio = await try_xai(text, tts)
    if recovered_audio:
        output.extend(recovered_audio)
        return bytes(output)

    # Aqui você pode chamar Microsoft/ElevenLabs streaming ou retornar erro controlado.
    return bytes(output)
```


Modos de cache:

```text
off     nunca lê nem grava cache
static  cacheia somente frases estáticas conhecidas
all     cacheia qualquer texto de entrada
```

O padrão recomendado é `static`, porque evita gravar texto dinâmico com dados sensíveis.

Frases estáticas padrão:

```text
Bem-vindo de volta.
Por favor, aguarde um momento.
Um momento, por favor.
Estou verificando isso agora.
Obrigado pela paciência.
Pode repetir, por favor?
Não consegui processar sua solicitação agora.
Sua solicitação foi concluída.
Estamos transferindo seu atendimento.
Ainda estou aqui.
```

## Implementando Provider Fallback

Microsoft e ElevenLabs são fallbacks externos opcionais e também são consumidos em modo streaming pela lib. Eles só devem entrar se xAI OCI não produzir nenhum áudio e se não houver cache local de recuperação aplicável.

```python
from tts_fallback import build_provider_chain_from_env, stream_provider_fallback


async def synthesize_with_provider_fallback(text: str) -> bytes:
    providers = build_provider_chain_from_env("microsoft,elevenlabs")
    audio = bytearray()

    async for event in stream_provider_fallback(text, providers):
        if event.type == "audio" and event.audio:
            # Envie este chunk imediatamente ao player/cliente.
            audio.extend(event.audio)
        else:
            print(event.to_log_dict())

    return bytes(audio)
```

Provider fallback agora emite chunks de áudio conforme a resposta HTTP vai chegando. Mesmo assim, ele só é acionado antes de qualquer áudio do xAI OCI sair; se o primário já começou a falar, trocar para outro provider ainda poderia duplicar a frase.

Ajuste o tamanho dos chunks por variável de ambiente:

```text
MICROSOFT_SPEECH_CHUNK_SIZE=8192
ELEVENLABS_CHUNK_SIZE=8192
```

Para ElevenLabs, a lib chama o endpoint `/stream` do Text to Speech. Para Microsoft, a lib lê a resposta do endpoint REST em blocos incrementais usando um formato de saída streaming, por exemplo `audio-24khz-48kbitrate-mono-mp3`.

## Ordem Completa Recomendada

Em uma aplicação real, a ordem completa pode ficar assim:

```text
1. xAI OCI sequential ou hedged para o texto original
2. se falhar antes do primeiro áudio, tocar frase de espera do cache local
3. depois da espera, tentar xAI OCI novamente para o texto original
4. se ainda não recuperar, Microsoft Azure Speech streaming
5. se Microsoft falhar, ElevenLabs streaming
6. se tudo falhar, retornar erro controlado para o app
```

Para frases críticas de atendimento, pré-gere o cache antes de produção. Para texto dinâmico, prefira não cachear ou use uma política explícita de privacidade.

## Fallbacks Implementados

| Mecanismo | O que faz | Quando entra | O que protege |
| --- | --- | --- | --- |
| Pool de WebSockets quentes | Mantém conexões xAI OCI abertas e prontas | Antes da síntese | Latência de abertura de conexão e falhas isoladas de socket |
| Fallback sequencial | Tenta outro socket saudável se o primeiro falhar antes do áudio | Antes do primeiro chunk | Erro de conexão, timeout inicial, close prematuro, erro do provider |
| Hedged request | Envia a mesma frase para mais de um socket e usa o primeiro áudio válido | Em modo `hedged` | Picos de latência e sockets lentos |
| First-audio timeout | Limita quanto tempo esperar pelo primeiro áudio | Antes do áudio começar | Socket conectado mas travado/mudo |
| Chunk timeout | Limita quanto tempo esperar entre chunks depois que a fala começou | Durante o stream | Stream parado no meio da fala |
| Reconnect com backoff | Fecha sockets ruins e reconecta em background com espera progressiva | Depois de falhas | Loop agressivo de reconexão e instabilidade temporária |
| Cooldown tipo circuit breaker | Aumenta a espera depois de falhas repetidas no mesmo slot | Depois de falhas repetidas | Saturação, auth ruim, rate limit ou endpoint instável |
| Winner-takes-stream | Só uma origem pode emitir áudio para cada fala | Sequential, hedged e provider fallback | Áudio duplicado ou respostas atrasadas conflitantes |
| Cache local de recuperação | Toca uma frase curta de espera já gerada | Depois que xAI OCI falha sem produzir áudio | Silêncio enquanto a aplicação tenta recuperar um WebSocket funcional |
| Provider fallback streaming | Usa Microsoft Azure Speech ou ElevenLabs e emite chunks conforme chegam | Depois que xAI OCI falha sem áudio e cache não resolve | Outage ou falha total do provedor primário com menor tempo até o primeiro áudio |

## Configuração

### xAI OCI

```text
OCI_GENAI_API_KEY
```

### Cache

```text
TTS_CACHE_MODE=static
TTS_CACHE_DIR=/home/ubuntu/tts-fallback/cache/audio
TTS_CACHE_STATIC_LINES_FILE=
TTS_RECOVERY_CACHE_TEXT=Por favor, aguarde um momento.
TTS_RECOVERY_RETRIES=1
```

### Provider Fallback Opcional

```text
PROVIDER_FALLBACK_ORDER=
```

Deixe vazio para não usar Microsoft/ElevenLabs. Preencha apenas quando quiser fallback externo, por exemplo `microsoft`, `elevenlabs` ou `microsoft,elevenlabs`.

### Microsoft Azure Speech Opcional

Use somente se `PROVIDER_FALLBACK_ORDER` incluir `microsoft`.

```text
MICROSOFT_SPEECH_KEY
MICROSOFT_SPEECH_REGION=eastus
MICROSOFT_SPEECH_ENDPOINT=
MICROSOFT_SPEECH_VOICE=pt-BR-FranciscaNeural
MICROSOFT_SPEECH_LANGUAGE=pt-BR
MICROSOFT_SPEECH_OUTPUT_FORMAT=audio-24khz-48kbitrate-mono-mp3
MICROSOFT_SPEECH_TIMEOUT=30
MICROSOFT_SPEECH_CHUNK_SIZE=8192
```

### ElevenLabs Opcional

Use somente se `PROVIDER_FALLBACK_ORDER` incluir `elevenlabs`.

```text
ELEVENLABS_API_KEY
ELEVENLABS_VOICE_ID
ELEVENLABS_MODEL_ID=eleven_multilingual_v2
ELEVENLABS_LANGUAGE_CODE=pt
ELEVENLABS_OUTPUT_FORMAT=mp3_44100_128
ELEVENLABS_BASE_URL=https://api.elevenlabs.io/v1/text-to-speech
ELEVENLABS_TIMEOUT=30
ELEVENLABS_CHUNK_SIZE=8192
```

## Testes Com CLI

O arquivo `run_fallback_test.py` é um utilitário de validação. Ele mostra os eventos em JSON e ajuda a provar cada fallback sem você precisar escrever código novo.

O CLI carrega `/home/ubuntu/tts-fallback/.env` por padrão. Para usar outro arquivo:

```bash
python3 run_fallback_test.py --env-file /caminho/para/.env
```

### Testar xAI OCI normal

```bash
python3 run_fallback_test.py \
  --mode sequential \
  --pool-size 3 \
  --cache-mode off \
  --text "Teste normal com xAI OCI." \
  --output /tmp/xai-normal.mp3 \
  --verbose
```

Eventos esperados:

```text
attempt_started
winner
completed
```

### Testar fallback sequencial

Força falha no primeiro WebSocket para provar que outro socket assume:

```bash
python3 run_fallback_test.py \
  --mode sequential \
  --pool-size 3 \
  --cache-mode off \
  --fault close-first-attempt \
  --text "Teste de fallback sequencial antes do áudio." \
  --output /tmp/sequential-fallback.mp3 \
  --verbose
```

Eventos esperados:

```text
attempt_started
attempt_failed
attempt_started
winner
completed
```

### Testar hedged request

```bash
python3 run_fallback_test.py \
  --mode hedged \
  --pool-size 3 \
  --hedges 2 \
  --cache-mode off \
  --text "Teste de hedged request." \
  --output /tmp/hedged.mp3 \
  --verbose
```

Eventos esperados:

```text
attempt_started
attempt_started
winner
completed
```

### Testar cache como fallback de recuperação

Este teste força o primário xAI OCI a falhar usando uma variável de ambiente inexistente. O cache deve tocar uma frase de espera e, em seguida, o runner deve registrar uma nova tentativa de recuperação do xAI OCI.

```bash
python3 run_fallback_test.py \
  --api-key-env MISSING_OCI_KEY \
  --disable-provider-fallback \
  --text "Por favor, aguarde um momento." \
  --output /tmp/cache-fallback.mp3 \
  --verbose
```

Eventos esperados:

```text
cache_hit
recovery_retry
```

A mensagem deve indicar:

```text
audio de espera servido do cache local; tentando recuperar websocket em seguida
```

Se o WebSocket ainda não recuperar e providers externos estiverem desabilitados, o resumo deve mostrar:

```text
completedOriginal: false
recovery.audioPlayed: true
```

### Testar first-audio timeout

```bash
python3 run_fallback_test.py \
  --mode sequential \
  --pool-size 3 \
  --cache-mode off \
  --first-audio-timeout 0.05 \
  --disable-provider-fallback \
  --text "Teste de timeout antes do primeiro áudio." \
  --output /tmp/first-audio-timeout.mp3 \
  --verbose
```

Evento esperado quando o provedor demora mais que o limite:

```text
attempt_failed: FirstAudioTimeout
```

### Testar chunk timeout

```bash
python3 run_fallback_test.py \
  --mode sequential \
  --pool-size 1 \
  --cache-mode off \
  --chunk-timeout 0.05 \
  --disable-provider-fallback \
  --text "Texto longo para gerar vários chunks de áudio e testar timeout no meio do stream." \
  --output /tmp/chunk-timeout.mp3 \
  --verbose \
  --debug
```

Eventos esperados:

```text
winner
audio
attempt_failed: ChunkTimeout
PartialSynthesisError
```

Esse comportamento é intencional: se o usuário já recebeu áudio, a biblioteca evita reiniciar a frase inteira automaticamente para não gerar fala duplicada.

### Testar provider fallback

Microsoft:

```bash
python3 run_fallback_test.py \
  --api-key-env MISSING_OCI_KEY \
  --cache-mode off \
  --provider-fallback-order microsoft \
  --text "Teste de fallback para Microsoft." \
  --output /tmp/provider-microsoft.mp3 \
  --verbose
```

ElevenLabs:

```bash
python3 run_fallback_test.py \
  --api-key-env MISSING_OCI_KEY \
  --cache-mode off \
  --provider-fallback-order elevenlabs \
  --text "Teste de fallback para ElevenLabs." \
  --output /tmp/provider-elevenlabs.mp3 \
  --verbose
```

Cadeia Microsoft e depois ElevenLabs:

```bash
python3 run_fallback_test.py \
  --api-key-env MISSING_OCI_KEY \
  --cache-mode off \
  --provider-fallback-order microsoft,elevenlabs \
  --text "Teste de cadeia de providers." \
  --output /tmp/provider-chain.mp3 \
  --verbose
```

## Troubleshooting

### `PartialSynthesisError`

Significa que o WebSocket falhou depois de emitir algum áudio. A lib não reinicia automaticamente a frase inteira para evitar duplicação no player.

### `cache_miss` depois de falha do xAI OCI

A frase de espera configurada em `TTS_RECOVERY_CACHE_TEXT` não tinha arquivo local para o mesmo perfil de voz/provedor. Gere essa frase uma vez com xAI OCI funcionando ou rode pré-cache das frases estáticas.

### Nenhum provider externo é usado

Provider fallback externo fica desativado por padrão:

```text
PROVIDER_FALLBACK_ORDER=
```

Configure `PROVIDER_FALLBACK_ORDER=microsoft`, `elevenlabs` ou `microsoft,elevenlabs` e preencha as chaves correspondentes.

## O Que Ainda Não Está Implementado

A biblioteca cobre o núcleo de fallback em aplicação, mas ainda não implementa itens operacionais de produto completo:

```text
API HTTP/gateway para produção
dashboards de SLA
alertas automáticos
health check sintético contínuo
multi-região OCI
política por tenant/cliente
reserva de capacidade
relatório mensal de disponibilidade
integração direta com LiveKit
```

## Observações

O caminho xAI OCI é streaming via WebSocket. Microsoft e ElevenLabs são consumidos por HTTP streaming nesta lib: a resposta é lida em chunks e cada chunk vira um evento `audio`. Isso melhora tempo até o primeiro áudio no fallback externo, mas ainda não preserva continuidade se o xAI OCI já tiver começado a falar.