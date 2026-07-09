# snakemake-logger-plugin-dart

Plugin de logging do [Snakemake 9+](https://snakemake.readthedocs.io/) que
transmite os eventos do workflow em tempo real, via **WebSocket**, para uma
aplicação desktop (Dart) que embarca o servidor — contraparte do pacote Dart
[`snakemake_bridge`](https://pub.dev/packages/snakemake_bridge). Arquitetura e
protocolo:
[ARCHITECTURE.md](https://github.com/karlaycosta/snakemake-dart/blob/main/ARCHITECTURE.md).

## Instalação

```bash
pip install snakemake-logger-plugin-dart
# ou, a partir do código-fonte:
pip install -e .
# Snakemake passa a listar:
snakemake --help | grep logger-dart
```

## Uso

```bash
snakemake \
    --cores 4 \
    --logger dart \
    --logger-dart-address ws://127.0.0.1:8765
```

### Configurações

| Flag | Env var | Default | Descrição |
| --- | --- | --- | --- |
| `--logger-dart-address` | `SNAKEMAKE_LOGGER_DART_ADDRESS` | _(obrigatório)_ | URL `ws://` do servidor embarcado no app. |
| `--logger-dart-token` | `SNAKEMAKE_LOGGER_DART_TOKEN` | — | Bearer token exigido pelo app (prefira a env var; não aparece no `ps`). |
| `--logger-dart-flush-timeout` | — | `5.0` | Segundos máximos, no encerramento, aguardando a entrega dos eventos pendentes. |

## Garantias

- `emit()` **nunca bloqueia** o Snakemake: eventos vão para uma fila drenada
  por uma thread própria.
- Queda de conexão / app fechado **nunca** afetam o workflow: reconexão com
  backoff + buffer de replay (o app pede `replay` ao receber `hello`).
- Entrega *at-least-once* — o consumidor deduplica pelo `seq` do envelope.

## Desenvolvimento

```bash
pip install -e '.[dev]'
pytest
```
