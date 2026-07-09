# Snakemake ⇄ Dart Desktop Bridge

Monitoramento de workflows [Snakemake 9+](https://snakemake.readthedocs.io/)
em tempo real dentro de uma aplicação Dart Desktop, **sem servidor
intermediário**: o app embarca um servidor WebSocket e o plugin de logging do
Snakemake conecta diretamente nele.

O monorepo tem duas metades: do lado Python, o plugin
[`snakemake-logger-plugin-dart`](python/) — um cliente WebSocket assíncrono e
resiliente, ativado com `--logger dart` e publicado no
[PyPI](https://pypi.org/project/snakemake-logger-plugin-dart/) —; do lado
Dart, o pacote [`snakemake_bridge`](dart/), publicado no
[pub.dev](https://pub.dev/packages/snakemake_bridge), com o servidor
embarcado, os modelos de eventos tipados, o reducer de estado e o launcher do
processo. A [arquitetura](ARCHITECTURE.md) documenta o protocolo, a segurança
e os modos de falha, e o [guia de instalação](INSTALL.md) mostra como
compilar/instalar o plugin no Snakemake, rodar os testes (Python e Dart) e
validar a ponte ponta-a-ponta.

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
