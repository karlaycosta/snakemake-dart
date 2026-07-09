# Snakemake ⇄ Dart Desktop Bridge

Monitoramento de workflows [Snakemake 9+](https://snakemake.readthedocs.io/)
em tempo real dentro de uma aplicação Dart Desktop, **sem servidor
intermediário**: o app embarca um servidor WebSocket e o plugin de logging do
Snakemake conecta diretamente nele.

- 📐 **[ARCHITECTURE.md](ARCHITECTURE.md)** — arquitetura, protocolo, segurança
  e modos de falha.
- 🛠️ **[INSTALL.md](INSTALL.md)** — como compilar/instalar o plugin no
  Snakemake, rodar os testes (Python e Dart) e validar ponta-a-ponta.
- 🐍 **[python/](python/)** —
  [`snakemake-logger-plugin-dart`](https://pypi.org/project/snakemake-logger-plugin-dart/)
  (PyPI): cliente WebSocket assíncrono e resiliente, `--logger dart`.
- 🎯 **[dart/](dart/)** —
  [`snakemake_bridge`](https://pub.dev/packages/snakemake_bridge) (pub.dev):
  servidor embarcado, modelos de eventos tipados, reducer de estado e launcher
  do processo.

Baseado na estrutura do
[snakemake-logger-plugin-panoptes](https://github.com/panoptes-organization/snakemake-logger-plugin-panoptes),
substituindo HTTP unidirecional por um canal WebSocket bidirecional com
replay, e o servidor panoptes pelo próprio app.

## Demonstração rápida (sem Snakemake)

```bash
# terminal 1 — "app"
cd dart && dart pub get
SNAKEMAKE_LOGGER_DART_TOKEN=dev dart run example/monitor_cli.dart
# LISTENING port=NNNNN ...

# terminal 2 — um run real usaria: snakemake --logger dart \
#   --logger-dart-address ws://127.0.0.1:NNNNN
```

## Estado dos testes

- `python/`: 12 testes (tradução de eventos + integração de transporte com
  servidor WS real: envelope, replay, token, não-bloqueio).
- `dart/`: 6 testes (rejeição sem token, dedupe por `seq` + reducer,
  forward-compat de tipos desconhecidos).
- E2E validado: handler Python real → servidor Dart real → estado final
  `finished, jobs=2, progress=1.0`.
