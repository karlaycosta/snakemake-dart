/// Ponte desktop Snakemake ⇄ Dart.
///
/// Fiação típica:
/// ```dart
/// final token = WorkflowServer.generateToken();
/// final server = WorkflowServer(token: token);
/// await server.start();
///
/// final state = WorkflowRunState();
/// server.events.listen((event) {
///   if (state.apply(event)) notifyUi();
/// });
///
/// final runner = await SnakemakeRunner.start(
///   workflowDir: '/path/to/workflow',
///   serverPort: server.boundPort,
///   token: token,
/// );
/// // cancelar: await runner.cancel();
/// ```
library;

export 'src/snakemake_runner.dart';
export 'src/workflow_events.dart';
export 'src/workflow_server.dart';
export 'src/workflow_state.dart';
