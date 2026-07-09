# Changelog

## 0.1.0

- Versão inicial.
- Plugin de logging para Snakemake 9+ (`--logger dart`): traduz `LogEvent`s em
  payloads JSON e os entrega via WebSocket a um app desktop.
- `emit()` nunca bloqueia o Snakemake (fila drenada por thread própria).
- Reconexão com backoff, buffer de replay e entrega *at-least-once*
  (deduplicação pelo `seq` do envelope).
- Autenticação por bearer token (`--logger-dart-token` ou
  `SNAKEMAKE_LOGGER_DART_TOKEN`).
