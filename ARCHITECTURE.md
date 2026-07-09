# Arquitetura — Snakemake ⇄ Dart Desktop Bridge

Integração em tempo real entre workflows [Snakemake 9+](https://snakemake.readthedocs.io/)
e uma aplicação **Dart Desktop**, sem servidor intermediário: a aplicação
embarca um servidor WebSocket e o plugin de logging do Snakemake conecta
diretamente nele.

## Visão geral

```
┌─────────────────────┐                        ┌──────────────────────────────┐
│  Snakemake          │                        │  Dart Desktop                │
│  ┌───────────────┐  │   WebSocket (ws://)    │  ┌────────────────────────┐  │
│  │ logger plugin │──┼────────────────────────┼─>│ servidor WS embarcado  │  │
│  │ (cliente WS)  │<─┼───── comandos ─────────┼──│ (shelf, 127.0.0.1)     │  │
│  └───────────────┘  │                        │  └───────────┬────────────┘  │
└─────────▲───────────┘                        │       stream de eventos      │
          │                                    │              ▼               │
          └───────── Process.start() ──────────│   reducer → estado da UI     │
                 (o app lança o Snakemake)     └──────────────────────────────┘
```

Decisões estruturais:

1. **O app Dart é o servidor; o plugin é o cliente.** Uma única conexão
   WebSocket persistente e bidirecional. O app faz *bind* em `127.0.0.1` numa
   porta efêmera — sem polling, sem conflito de porta, sem exposição de rede.
2. **O app lança o Snakemake** (`Process.start`). Isso resolve:
   - *Descoberta de porta*: o app monta a linha de comando com
     `--logger-dart-address ws://127.0.0.1:<porta>`.
   - *Ciclo de vida*: o servidor já está de pé antes do primeiro evento.
   - *Cancelamento*: logger plugins do Snakemake são passivos (não controlam o
     scheduler). Cancelar o run = o app enviar `SIGTERM` ao processo filho que
     ele mesmo criou. Comandos via WebSocket ficam para operações finas
     (replay, ping).
3. **Modo "attach" também funciona**: o usuário pode rodar o Snakemake à mão
   no terminal apontando para um app já aberto em porta fixa. O buffer +
   reconexão do plugin cobrem o app abrir/fechar durante o run.

## Componentes

| Componente | Diretório | Papel |
| --- | --- | --- |
| `snakemake-logger-plugin-dart` | `python/` | Plugin Python (`--logger dart`). Traduz `LogEvent`s em mensagens JSON e as entrega via WebSocket, de forma assíncrona e resiliente. |
| `snakemake_bridge` (Dart) | `dart/` | Módulo de referência para o app: servidor WS embarcado, modelos de eventos (sealed classes), reducer de estado e launcher do processo Snakemake. |

### Plugin Python — camadas internas

```
LogRecord (Snakemake) ──> EventTranslator ──> WsTransport ──> WebSocket
                          (events.py)         (transport.py)
```

- **`events.py` — `EventTranslator`**: converte cada `LogRecord` num par
  `(type, payload)` JSON-serializável. Dispatch por tabela (mesmo padrão do
  plugin panoptes), com acesso defensivo via `getattr` — mudanças de versão do
  Snakemake nunca quebram o plugin. Registros sem `event` (logs comuns
  nível ≥ INFO) viram eventos `log`, alimentando um painel de console na UI.
- **`transport.py` — `WsTransport`**: cliente WebSocket numa *thread* própria.
  - `emit()` do Snakemake **nunca bloqueia**: eventos entram numa fila em
    memória (`queue.Queue`, descarta com aviso se cheia) e a thread de
    trabalho os entrega.
  - **Reconexão** com backoff exponencial (0,5 s → 10 s).
  - **Buffer de replay** (`deque`, últimos 100 000 eventos) para reentrega
    após reconexão ou quando o app conecta tarde.
  - **Flush no encerramento**: `close()` espera a fila esvaziar (até
    `flush_timeout`) antes de derrubar a conexão, para o app receber o final
    do run.

## Protocolo

Todas as mensagens são frames de texto JSON.

### Envelope (plugin → app)

```jsonc
{
  "v": 1,                          // versão do schema
  "seq": 42,                       // sequencial monotônico por run (0 = hello)
  "ts": "2026-07-03T14:00:00+00:00", // ISO-8601 UTC
  "run_id": "3f2b...",             // uuid gerado pelo plugin no início do run
  "type": "job_finished",          // ver tabela abaixo
  "payload": { }                   // corpo específico do tipo
}
```

**Entrega é *at-least-once***: após reconexão ou `replay`, eventos podem
chegar duplicados. O app **deduplica por `seq`** (ignora `seq` ≤ último visto).

### Tipos de evento (plugin → app)

| `type` | Quando | Campos principais do `payload` |
| --- | --- | --- |
| `hello` | Primeiro frame de cada conexão | `pid`, `schema`, `run_id` |
| `workflow_started` | Início do workflow | `snakefile`, `workdir` |
| `rulegraph` | DAG de regras disponível | `rulegraph` (node-link, ver abaixo) |
| `run_info` | Estatística inicial de jobs | `stats` (`{rule: count}`), `total` |
| `job_info` | Job agendado | `job_id`, `rule_name`, `wildcards`, `input`, `output`, `log`, `resources`, `threads`, `shellcmd`, `reason`, `priority`, `is_checkpoint` |
| `job_started` | Job(s) começou(aram) | `job_ids` |
| `job_finished` | Job concluído | `job_id` |
| `job_error` | Job falhou | `job_id`, `rule_name` |
| `group_info` / `group_error` | Grupos de jobs | `group_id`, `jobs` |
| `shellcmd` | Comando shell executado | `job_id`, `shellcmd` |
| `progress` | Progresso global | `done`, `total` |
| `resources_info` | Recursos do run | `nodes`, `cores`, `provided_resources` |
| `error` | Erro de workflow | `message`, `rule`, `location`, `traceback` |
| `log` | Log comum (sem `LogEvent`) | `level`, `message` |
| `pong` | Resposta a `ping` | — |
| `bye` | Encerramento limpo do run | — |

#### Observações de um run real (Snakemake 9.23)

- **`rulegraph`** chega em formato *node-link* (estilo d3): índices de
  `links.source`/`links.target` apontam para a lista `nodes`:

  ```json
  {
    "nodes": [{"rule": "prepare"}, {"rule": "analyze"}, {"rule": "all"}],
    "links": [
      {"source": 0, "target": 1, "sourcerule": "prepare", "targetrule": "analyze"},
      {"source": 1, "target": 2, "sourcerule": "analyze", "targetrule": "all"}
    ]
  }
  ```

- **Ordem dos eventos por job**: `job_started` chega **antes** de `job_info`
  (o scheduler anuncia o lote antes dos detalhes de cada job). O reducer deve
  criar o job no primeiro evento e completar `rule_name` depois.
- `resources_info` é emitido mais de uma vez no início do run.

### Comandos (app → plugin)

```jsonc
{ "type": "command", "cmd": "replay", "since_seq": 42 }  // reenvia buffer com seq > 42
{ "type": "command", "cmd": "ping" }                     // responde com "pong"
```

Fluxo de reconexão: ao (re)conectar, o plugin envia `hello`; o app responde
`replay` com o último `seq` que processou; o plugin reenvia o que faltar e a
fila corrente segue normalmente.

## Segurança

- O servidor **sempre** faz bind em `127.0.0.1` (nunca `0.0.0.0`).
- **Token por run**: o app gera um token aleatório, passa ao plugin via env
  var `SNAKEMAKE_LOGGER_DART_TOKEN`, e o plugin o envia no header
  `Authorization: Bearer <token>` do handshake WebSocket. O app rejeita
  conexões sem o token — impede que outro processo local injete eventos
  falsos na UI.

## Modos de falha

| Cenário | Comportamento |
| --- | --- |
| App fechado / porta inacessível | Plugin acumula no buffer, tenta reconectar com backoff. Workflow **nunca** é afetado. |
| Conexão cai no meio do run | Reconexão + `replay` recuperam os eventos perdidos. |
| Fila cheia (app travado por muito tempo) | Eventos novos são descartados com `warning` no log do Snakemake — nunca bloqueia o run. |
| Snakemake termina | `close()` faz flush da fila (até `flush_timeout` s) e envia `bye`. |
| App precisa cancelar o run | Fora do protocolo: `Process.kill(SIGTERM)` no processo filho. |

## Fluxo típico (app lança o run)

```
App                                    Plugin (dentro do Snakemake)
 │ 1. abre servidor WS em porta efêmera
 │ 2. gera token
 │ 3. Process.start("snakemake",
 │      --logger dart,
 │      --logger-dart-address ws://127.0.0.1:PORT,
 │      env: SNAKEMAKE_LOGGER_DART_TOKEN)
 │                                       │ 4. conecta com Authorization header
 │ <────────────── hello ────────────────│
 │ ── replay(since_seq=0) ──────────────>│
 │ <── workflow_started, rulegraph, ... ─│ 5. eventos em tempo real
 │ <── progress, job_*, ... ─────────────│
 │ <────────────── bye ──────────────────│ 6. fim do run + flush
 │ 7. exit code do processo confirma o desfecho
```

## Uso rápido

Plugin (instalado no mesmo ambiente do Snakemake):

```bash
pip install -e python/
snakemake --cores 4 \
    --logger dart \
    --logger-dart-address ws://127.0.0.1:8765
```

App Dart (ver `dart/`):

```dart
final server = WorkflowServer(token: token);
await server.start();                       // porta efêmera em server.boundPort
server.events.listen(state.apply);          // reducer → UI
final proc = await SnakemakeRunner.start(
  workflowDir: dir, serverPort: server.boundPort, token: token);
```

## Evolução prevista

- `v` no envelope permite evoluir o schema sem quebrar apps antigos; o app
  trata tipos desconhecidos como `UnknownEvent` (forward-compatible).
- Persistência da fila em disco (`.jsonl`) para sobreviver a crash do plugin.
- Métricas por job (CPU/RAM) como novo tipo de evento.
