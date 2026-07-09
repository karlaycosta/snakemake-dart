import 'dart:convert';
import 'dart:io';

import 'package:snakemake_bridge/snakemake_bridge.dart';
import 'package:test/test.dart';
import 'package:web_socket_channel/io.dart';

Map<String, dynamic> envelope(
  int seq,
  String type, [
  Map<String, dynamic> payload = const {},
  String runId = 'run-test',
]) =>
    {
      'v': 1,
      'seq': seq,
      'ts': '2026-07-03T12:00:00+00:00',
      'run_id': runId,
      'type': type,
      'payload': payload,
    };

void main() {
  test('rejects connections without the token', () async {
    final server = WorkflowServer(token: 's3cret');
    await server.start();
    addTearDown(server.close);

    final request = await HttpClient().getUrl(
      Uri.parse('http://127.0.0.1:${server.boundPort}/'),
    );
    final response = await request.close();
    expect(response.statusCode, 401);
  });

  test('decodes events, dedupes by seq and folds state', () async {
    final server = WorkflowServer(token: 's3cret');
    await server.start();
    addTearDown(server.close);

    final state = WorkflowRunState();
    final seen = <WorkflowEvent>[];
    server.events.listen((event) {
      seen.add(event);
      state.apply(event);
    });

    final channel = IOWebSocketChannel.connect(
      Uri.parse(server.address),
      headers: {'Authorization': 'Bearer s3cret'},
    );
    addTearDown(() => channel.sink.close());

    void send(Map<String, dynamic> json) => channel.sink.add(jsonEncode(json));

    send(envelope(0, 'hello', {'pid': 1, 'schema': 1}));
    send(envelope(1, 'workflow_started', {'snakefile': 'Snakefile'}));
    // Ordem real do Snakemake: JOB_STARTED chega antes de JOB_INFO.
    send(
      envelope(2, 'job_started', {
        'job_ids': [1],
      }),
    );
    send(
      envelope(2, 'job_started', {
        'job_ids': [1],
      }),
    ); // duplicado (replay)
    send(envelope(3, 'job_info', {'job_id': 1, 'rule_name': 'align'}));
    send(envelope(4, 'job_finished', {'job_id': 1, 'rule_name': 'align'}));
    send(envelope(5, 'progress', {'done': 1, 'total': 1}));
    send(envelope(6, 'bye'));

    await Future<void>.delayed(const Duration(milliseconds: 300));

    expect(
      seen.whereType<JobStartedEvent>().length,
      1,
      reason: 'duplicate seq must be dropped',
    );
    expect(state.runId, 'run-test');
    expect(state.status, RunStatus.finished);
    expect(state.jobs[1]!.status, JobStatus.finished);
    expect(
      state.jobs[1]!.ruleName,
      'align',
      reason: 'rule name from JOB_INFO must fill the job created by '
          'the earlier JOB_STARTED',
    );
    expect(state.progress, 1.0);
  });

  test('a second run on the same server is not dropped by seq dedup', () async {
    final server = WorkflowServer(token: 's3cret');
    await server.start();
    addTearDown(server.close);

    final state = WorkflowRunState();
    final seen = <WorkflowEvent>[];
    server.events.listen((event) {
      seen.add(event);
      state.apply(event);
    });

    final channel = IOWebSocketChannel.connect(
      Uri.parse(server.address),
      headers: {'Authorization': 'Bearer s3cret'},
    );
    addTearDown(() => channel.sink.close());

    void send(Map<String, dynamic> json) => channel.sink.add(jsonEncode(json));

    // O run A sobe até um seq alto.
    send(envelope(0, 'hello', {'pid': 1, 'schema': 1}, 'run-A'));
    send(envelope(1, 'workflow_started', {'snakefile': 'A'}, 'run-A'));
    send(envelope(50, 'progress', {'done': 5, 'total': 5}, 'run-A'));
    send(envelope(51, 'bye', const {}, 'run-A'));

    // O run B reinicia o seq em 0/1 com um novo run_id — NÃO pode ser descartado no dedup.
    send(envelope(0, 'hello', {'pid': 2, 'schema': 1}, 'run-B'));
    send(envelope(1, 'workflow_started', {'snakefile': 'B'}, 'run-B'));
    send(
      envelope(
          2,
          'job_started',
          {
            'job_ids': [1],
          },
          'run-B'),
    );
    send(envelope(3, 'job_info', {'job_id': 1, 'rule_name': 'map'}, 'run-B'));
    send(
      envelope(4, 'job_finished', {'job_id': 1, 'rule_name': 'map'}, 'run-B'),
    );
    send(envelope(5, 'progress', {'done': 1, 'total': 1}, 'run-B'));
    send(envelope(6, 'bye', const {}, 'run-B'));

    await Future<void>.delayed(const Duration(milliseconds: 300));

    // Os eventos de seq baixo do run B sobreviveram à troca.
    expect(
      seen.whereType<WorkflowStartedEvent>().length,
      2,
      reason: "run B's workflow_started (seq 1) must not be dropped",
    );
    expect(state.runId, 'run-B');
    expect(state.snakefile, 'B', reason: 'state reflects run B, not run A');
    expect(
        state.jobs.keys,
        [
          1,
        ],
        reason: 'run A jobs were cleared on the new run');
    expect(state.jobs[1]!.ruleName, 'map');
    expect(state.status, RunStatus.finished);
    expect(state.progress, 1.0);
  });

  test('hostile frames (binary, malformed) do not crash the server', () async {
    final server = WorkflowServer(token: 's3cret');
    await server.start();
    addTearDown(server.close);

    final seen = <WorkflowEvent>[];
    server.events.listen(seen.add);

    final channel = IOWebSocketChannel.connect(
      Uri.parse(server.address),
      headers: {'Authorization': 'Bearer s3cret'},
    );
    addTearDown(() => channel.sink.close());

    // Frame binário — fora do protocolo (só frames de texto JSON).
    channel.sink.add([1, 2, 3]);
    // `type` não-string: quebraria o cast do fromJson fora de um try.
    channel.sink.add(
      jsonEncode({'v': 1, 'seq': 1, 'run_id': 'x', 'type': 123}),
    );
    // Payload com tipo errado: job_ids deveria ser lista de números.
    channel.sink.add(
      jsonEncode(
        envelope(2, 'job_started', {
          'job_ids': ['not-a-number'],
        }),
      ),
    );
    // Evento válido na sequência: o servidor deve continuar vivo e entregá-lo.
    channel.sink.add(
      jsonEncode(envelope(3, 'progress', {'done': 1, 'total': 2})),
    );

    await Future<void>.delayed(const Duration(milliseconds: 300));

    expect(
      seen.whereType<ProgressEvent>().length,
      1,
      reason: 'o evento válido após frames hostis deve continuar fluindo',
    );
  });

  test('run_info stats land in statsByRule and retention is capped', () {
    final state = WorkflowRunState();

    state.apply(
      WorkflowEvent.fromJson(
        envelope(1, 'run_info', {
          'stats': {'align': 3, 'sort': 2, 'total': 5},
          'total': 5,
        }),
      ),
    );
    expect(
        state.statsByRule,
        {
          'align': 3,
          'sort': 2,
        },
        reason: 'a chave agregada `total` não é uma regra');
    expect(state.total, 5);

    for (var i = 0; i < WorkflowRunState.maxLogLines + 10; i++) {
      state.apply(
        WorkflowEvent.fromJson(
          envelope(2 + i, 'log', {'level': 'info', 'message': 'linha $i'}),
        ),
      );
    }
    expect(state.logLines.length, WorkflowRunState.maxLogLines);
    expect(
      state.logLines.first.message,
      'linha 10',
      reason: 'as linhas mais antigas são descartadas',
    );
  });

  test('unknown event types are forward-compatible', () {
    final event = WorkflowEvent.fromJson(
      envelope(9, 'gpu_metrics', {'utilisation': 0.5}),
    );
    expect(event, isA<UnknownEvent>());
    expect((event as UnknownEvent).type, 'gpu_metrics');
    expect(WorkflowRunState().apply(event), isFalse);
  });
}
