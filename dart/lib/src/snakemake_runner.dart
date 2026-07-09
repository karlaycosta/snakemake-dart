/// Lança e é dono do processo filho do Snakemake conectado a um [WorkflowServer].
library;

import 'dart:async';
import 'dart:convert';
import 'dart:io';

/// É dono do processo Snakemake de um run.
///
/// O app ser dono do processo é o que faz o cancelamento funcionar: logger
/// plugins são passivos, então "cancelar" é simplesmente um SIGTERM no filho.
class SnakemakeRunner {
  SnakemakeRunner._(this.process);

  final Process process;

  final _stdout = StreamController<String>.broadcast();
  final _stderr = StreamController<String>.broadcast();

  /// Saída bruta do console, útil ao lado do feed estruturado de eventos.
  Stream<String> get stdout => _stdout.stream;
  Stream<String> get stderr => _stderr.stream;

  Future<int> get exitCode => process.exitCode;

  static Future<SnakemakeRunner> start({
    required String workflowDir,
    required int serverPort,
    required String token,
    String executable = 'snakemake',
    int cores = 4,
    List<String> extraArgs = const [],
  }) async {
    final process = await Process.start(
      executable,
      [
        '--cores',
        '$cores',
        '--logger',
        'dart',
        '--logger-dart-address',
        'ws://127.0.0.1:$serverPort',
        ...extraArgs,
      ],
      workingDirectory: workflowDir,
      environment: {
        // Token via env var para nunca aparecer na saída do `ps`.
        'SNAKEMAKE_LOGGER_DART_TOKEN': token,
      },
    );
    final runner = SnakemakeRunner._(process);
    process.stdout
        .transform(utf8.decoder)
        .transform(const LineSplitter())
        .listen(runner._stdout.add);
    process.stderr
        .transform(utf8.decoder)
        .transform(const LineSplitter())
        .listen(runner._stderr.add);
    unawaited(
      process.exitCode.whenComplete(() {
        runner._stdout.close();
        runner._stderr.close();
      }),
    );
    return runner;
  }

  /// Cancelamento gracioso; cai para SIGKILL depois de [killAfter].
  Future<int> cancel({Duration killAfter = const Duration(seconds: 15)}) {
    process.kill(ProcessSignal.sigterm);
    final timer = Timer(killAfter, () => process.kill(ProcessSignal.sigkill));
    return process.exitCode.whenComplete(timer.cancel);
  }
}
