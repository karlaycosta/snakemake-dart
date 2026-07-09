# snakemake_bridge (Dart)

Módulo para embarcar um monitor de workflows Snakemake numa
aplicação **Dart Desktop**. Contraparte do plugin Python
[`snakemake-logger-plugin-dart`](https://pypi.org/project/snakemake-logger-plugin-dart/).
Arquitetura e protocolo:
[ARCHITECTURE.md](https://github.com/karlaycosta/snakemake-dart/blob/main/ARCHITECTURE.md).

## Instalação

```yaml
dependencies:
  snakemake_bridge: ^0.1.0
```

## Conteúdo

| Arquivo | Papel |
| --- | --- |
| `lib/src/workflow_server.dart` | Servidor WebSocket embarcado (`shelf`), bind em `127.0.0.1`, valida token, deduplica por `seq`, pede `replay` a cada reconexão. |
| `lib/src/workflow_events.dart` | Modelos tipados dos eventos (sealed classes, sem codegen; tipos desconhecidos viram `UnknownEvent`). |
| `lib/src/workflow_state.dart` | Reducer: stream de eventos → estado consultável do run (jobs, progresso, DAG, logs, erros). |
| `lib/src/snakemake_runner.dart` | Lança e é dono do processo `snakemake` (cancelamento = SIGTERM). |
| `example/monitor_cli.dart` | "App" headless mínimo para testar a ponte pelo terminal. |

## Uso no app

```dart
final token = WorkflowServer.generateToken();
final server = WorkflowServer(token: token);
await server.start();

final state = WorkflowRunState();
server.events.listen((event) {
  if (state.apply(event)) notifyUi(); // ChangeNotifier / Riverpod / Bloc
});

final runner = await SnakemakeRunner.start(
  workflowDir: '/caminho/do/workflow',
  serverPort: server.boundPort,
  token: token,
);
// cancelar: await runner.cancel();
// desfecho: await runner.exitCode;
```

## Teste rápido sem Snakemake

```bash
SNAKEMAKE_LOGGER_DART_TOKEN=dev dart run example/monitor_cli.dart
# em outro terminal, rode o Snakemake (ou o plugin) apontando para a porta impressa
```

## Testes

```bash
dart pub get
dart test
```
