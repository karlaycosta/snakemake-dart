/// "App" headless mínimo: sobe o servidor embarcado e imprime cada evento.
///
/// Rode-o e aponte o plugin (ou um run real do Snakemake) para o endereço
/// impresso:
///
///   SNAKEMAKE_LOGGER_DART_TOKEN=dev dart run example/snakemake_bridge_example.dart
///   snakemake --cores 2 --logger dart \
///       --logger-dart-address ws://127.0.0.1:PORTA
library;

import 'dart:convert';
import 'dart:io';

import 'package:snakemake_bridge/snakemake_bridge.dart';

Future<void> main() async {
  final token = Platform.environment['SNAKEMAKE_LOGGER_DART_TOKEN'];
  final server = WorkflowServer(token: token);
  await server.start();
  stdout.writeln(
    'LISTENING port=${server.boundPort} address=${server.address}',
  );

  final state = WorkflowRunState();
  server.events.listen((event) {
    stdout.writeln(event);
    state.apply(event);
    final detail = switch (event) {
      RuleGraphEvent(:final rulegraph) => 'rulegraph=${jsonEncode(rulegraph)}',
      UnknownEvent(:final type, :final payload) =>
        'type=$type payload=${jsonEncode(payload)}',
      JobInfoEvent(:final jobId, :final ruleName, :final wildcards) =>
        'job=$jobId rule=$ruleName wildcards=${jsonEncode(wildcards)}',
      JobStartedEvent(:final jobIds) => 'jobs=$jobIds',
      JobFinishedEvent(:final jobId) => 'job=$jobId',
      ProgressEvent(:final done, :final total) => '$done/$total',
      LogLineEvent(:final message) =>
        message.length > 80 ? '${message.substring(0, 80)}…' : message,
      WorkflowErrorEvent(:final message) => 'error=$message',
      _ => '',
    };
    stdout.writeln('EVENT ${event.runtimeType} seq=${event.seq} $detail');
    if (event is ByeEvent) {
      stdout.writeln(
        'DONE status=${state.status.name} '
        'jobs=${state.jobs.length} progress=${state.progress}',
      );
      exit(0);
    }
  });
}
