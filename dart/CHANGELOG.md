# Changelog

## 0.1.0

- Versão inicial.
- `WorkflowServer`: servidor WebSocket embarcado (`shelf`), bind em
  `127.0.0.1`, autenticação por token, deduplicação por `seq` e pedido de
  `replay` a cada reconexão.
- `workflow_events.dart`: modelos tipados dos eventos do Snakemake (sealed
  classes; tipos desconhecidos viram `UnknownEvent`).
- `WorkflowRunState`: reducer de eventos → estado consultável do run (jobs,
  progresso, DAG, logs, erros).
- `SnakemakeRunner`: lança e gerencia o processo `snakemake`.
- Exemplo `example/monitor_cli.dart`: monitor headless para teste no terminal.
