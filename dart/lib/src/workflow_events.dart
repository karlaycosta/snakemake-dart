/// Modelos tipados do protocolo plugin -> app (schema v1).
///
/// Sealed classes puras, sem codegen: `WorkflowEvent.fromJson` faz switch no
/// `type` do envelope e tipos desconhecidos caem em [UnknownEvent], então
/// versões mais novas do plugin nunca quebram apps antigos.
library;

/// Envelope comum a toda mensagem vinda do plugin.
sealed class WorkflowEvent {
  const WorkflowEvent({
    required this.v,
    required this.seq,
    required this.runId,
    this.ts,
  });

  /// Versão do schema do envelope.
  final int v;

  /// Número de sequência monotônico por run. A entrega é at-least-once:
  /// deduplique ignorando eventos com `seq <= lastSeenSeq` (exceto 0).
  final int seq;

  /// Id único do run do Snakemake que produziu este evento.
  final String runId;

  /// Timestamp do evento (UTC), se o plugin o informou.
  final DateTime? ts;

  factory WorkflowEvent.fromJson(Map<String, dynamic> json) {
    final v = (json['v'] as num?)?.toInt() ?? 1;
    final seq = (json['seq'] as num?)?.toInt() ?? 0;
    final runId = json['run_id'] as String? ?? '';
    final ts = DateTime.tryParse(json['ts'] as String? ?? '');
    final p = (json['payload'] as Map?)?.cast<String, dynamic>() ?? const {};

    List<String> strings(dynamic value) => switch (value) {
      List list => list.map((e) => e.toString()).toList(),
      null => const [],
      _ => [value.toString()],
    };
    int? asInt(dynamic value) => (value as num?)?.toInt();

    return switch (json['type'] as String?) {
      'hello' => HelloEvent(
        v: v,
        seq: seq,
        runId: runId,
        ts: ts,
        pid: asInt(p['pid']),
        schema: asInt(p['schema']) ?? 1,
      ),
      'workflow_started' => WorkflowStartedEvent(
        v: v,
        seq: seq,
        runId: runId,
        ts: ts,
        snakefile: p['snakefile'] as String?,
        workdir: p['workdir'] as String?,
      ),
      'rulegraph' => RuleGraphEvent(
        v: v,
        seq: seq,
        runId: runId,
        ts: ts,
        rulegraph: p['rulegraph'],
      ),
      'run_info' => RunInfoEvent(
        v: v,
        seq: seq,
        runId: runId,
        ts: ts,
        stats: (p['stats'] as Map?)?.cast<String, dynamic>() ?? const {},
        total: asInt(p['total']),
      ),
      'job_info' => JobInfoEvent(
        v: v,
        seq: seq,
        runId: runId,
        ts: ts,
        jobId: asInt(p['job_id']) ?? 0,
        ruleName: p['rule_name'] as String? ?? '',
        ruleMsg: p['rule_msg'] as String?,
        input: strings(p['input']),
        output: strings(p['output']),
        log: strings(p['log']),
        wildcards:
            (p['wildcards'] as Map?)?.cast<String, dynamic>() ?? const {},
        isCheckpoint: p['is_checkpoint'] as bool? ?? false,
        shellcmd: p['shellcmd'] as String?,
        threads: asInt(p['threads']),
        reason: p['reason']?.toString(),
        resources:
            (p['resources'] as Map?)?.cast<String, dynamic>() ?? const {},
      ),
      'job_started' => JobStartedEvent(
        v: v,
        seq: seq,
        runId: runId,
        ts: ts,
        jobIds:
            (p['job_ids'] as List?)?.map((e) => (e as num).toInt()).toList() ??
            const [],
      ),
      'job_finished' => JobFinishedEvent(
        v: v,
        seq: seq,
        runId: runId,
        ts: ts,
        jobId: asInt(p['job_id']) ?? 0,
        ruleName: p['rule_name'] as String? ?? '',
      ),
      'job_error' => JobErrorEvent(
        v: v,
        seq: seq,
        runId: runId,
        ts: ts,
        jobId: asInt(p['job_id']) ?? 0,
        ruleName: p['rule_name'] as String? ?? '',
      ),
      'shellcmd' => ShellCmdEvent(
        v: v,
        seq: seq,
        runId: runId,
        ts: ts,
        jobId: asInt(p['job_id']),
        shellcmd: p['shellcmd'] as String? ?? '',
      ),
      'progress' => ProgressEvent(
        v: v,
        seq: seq,
        runId: runId,
        ts: ts,
        done: asInt(p['done']) ?? 0,
        total: asInt(p['total']) ?? 0,
      ),
      'resources_info' => ResourcesInfoEvent(
        v: v,
        seq: seq,
        runId: runId,
        ts: ts,
        payload: p,
      ),
      'error' => WorkflowErrorEvent(
        v: v,
        seq: seq,
        runId: runId,
        ts: ts,
        message: p['message']?.toString() ?? '',
        rule: p['rule'] as String?,
        location: p['location']?.toString(),
        traceback: p['traceback']?.toString(),
      ),
      'log' => LogLineEvent(
        v: v,
        seq: seq,
        runId: runId,
        ts: ts,
        level: p['level'] as String? ?? 'info',
        message: p['message'] as String? ?? '',
      ),
      'pong' => PongEvent(v: v, seq: seq, runId: runId, ts: ts),
      'bye' => ByeEvent(v: v, seq: seq, runId: runId, ts: ts),
      _ => UnknownEvent(
        v: v,
        seq: seq,
        runId: runId,
        ts: ts,
        type: json['type'] as String? ?? '',
        payload: p,
      ),
    };
  }
}

final class HelloEvent extends WorkflowEvent {
  const HelloEvent({
    required super.v,
    required super.seq,
    required super.runId,
    super.ts,
    required this.schema,
    this.pid,
  });
  final int schema;
  final int? pid;
}

final class WorkflowStartedEvent extends WorkflowEvent {
  const WorkflowStartedEvent({
    required super.v,
    required super.seq,
    required super.runId,
    super.ts,
    this.snakefile,
    this.workdir,
  });
  final String? snakefile;
  final String? workdir;
}

final class RuleGraphEvent extends WorkflowEvent {
  const RuleGraphEvent({
    required super.v,
    required super.seq,
    required super.runId,
    super.ts,
    this.rulegraph,
  });

  /// Rule graph bruto como emitido pelo Snakemake; o formato depende da versão
  /// do Snakemake, então fica dinâmico e é interpretado na camada de renderização.
  final dynamic rulegraph;
}

final class RunInfoEvent extends WorkflowEvent {
  const RunInfoEvent({
    required super.v,
    required super.seq,
    required super.runId,
    super.ts,
    required this.stats,
    this.total,
  });
  final Map<String, dynamic> stats;
  final int? total;
}

final class JobInfoEvent extends WorkflowEvent {
  const JobInfoEvent({
    required super.v,
    required super.seq,
    required super.runId,
    super.ts,
    required this.jobId,
    required this.ruleName,
    this.ruleMsg,
    this.input = const [],
    this.output = const [],
    this.log = const [],
    this.wildcards = const {},
    this.isCheckpoint = false,
    this.shellcmd,
    this.threads,
    this.reason,
    this.resources = const {},
  });
  final int jobId;
  final String ruleName;
  final String? ruleMsg;
  final List<String> input;
  final List<String> output;
  final List<String> log;
  final Map<String, dynamic> wildcards;
  final bool isCheckpoint;
  final String? shellcmd;
  final int? threads;
  final String? reason;
  final Map<String, dynamic> resources;
}

final class JobStartedEvent extends WorkflowEvent {
  const JobStartedEvent({
    required super.v,
    required super.seq,
    required super.runId,
    super.ts,
    required this.jobIds,
  });
  final List<int> jobIds;
}

final class JobFinishedEvent extends WorkflowEvent {
  const JobFinishedEvent({
    required super.v,
    required super.seq,
    required super.runId,
    super.ts,
    required this.jobId,
    required this.ruleName,
  });
  final int jobId;
  final String ruleName;
}

final class JobErrorEvent extends WorkflowEvent {
  const JobErrorEvent({
    required super.v,
    required super.seq,
    required super.runId,
    super.ts,
    required this.jobId,
    required this.ruleName,
  });
  final int jobId;
  final String ruleName;
}

final class ShellCmdEvent extends WorkflowEvent {
  const ShellCmdEvent({
    required super.v,
    required super.seq,
    required super.runId,
    super.ts,
    required this.shellcmd,
    this.jobId,
  });
  final String shellcmd;
  final int? jobId;
}

final class ProgressEvent extends WorkflowEvent {
  const ProgressEvent({
    required super.v,
    required super.seq,
    required super.runId,
    super.ts,
    required this.done,
    required this.total,
  });
  final int done;
  final int total;
}

final class ResourcesInfoEvent extends WorkflowEvent {
  const ResourcesInfoEvent({
    required super.v,
    required super.seq,
    required super.runId,
    super.ts,
    required this.payload,
  });
  final Map<String, dynamic> payload;
}

final class WorkflowErrorEvent extends WorkflowEvent {
  const WorkflowErrorEvent({
    required super.v,
    required super.seq,
    required super.runId,
    super.ts,
    required this.message,
    this.rule,
    this.location,
    this.traceback,
  });
  final String message;
  final String? rule;
  final String? location;
  final String? traceback;
}

final class LogLineEvent extends WorkflowEvent {
  const LogLineEvent({
    required super.v,
    required super.seq,
    required super.runId,
    super.ts,
    required this.level,
    required this.message,
  });
  final String level;
  final String message;
}

final class PongEvent extends WorkflowEvent {
  const PongEvent({
    required super.v,
    required super.seq,
    required super.runId,
    super.ts,
  });
}

final class ByeEvent extends WorkflowEvent {
  const ByeEvent({
    required super.v,
    required super.seq,
    required super.runId,
    super.ts,
  });
}

/// Forward-compat: tipos de evento que esta versão do app ainda não conhece.
final class UnknownEvent extends WorkflowEvent {
  const UnknownEvent({
    required super.v,
    required super.seq,
    required super.runId,
    super.ts,
    required this.type,
    required this.payload,
  });
  final String type;
  final Map<String, dynamic> payload;
}
