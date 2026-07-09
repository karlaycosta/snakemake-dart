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

  /// Decodifica o envelope JSON do protocolo no evento tipado correspondente.
  ///
  /// Tipos que esta versão não conhece viram [UnknownEvent]; campos ausentes
  /// ou com formato inesperado assumem defaults seguros em vez de lançar.
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
          jobIds: (p['job_ids'] as List?)
                  ?.map((e) => (e as num).toInt())
                  .toList() ??
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

/// Primeiro frame de cada conexão do plugin; dispara o pedido de `replay`.
final class HelloEvent extends WorkflowEvent {
  const HelloEvent({
    required super.v,
    required super.seq,
    required super.runId,
    super.ts,
    required this.schema,
    this.pid,
  });

  /// Versão do schema de eventos que o plugin fala.
  final int schema;

  /// PID do processo Snakemake, se informado.
  final int? pid;
}

/// O workflow começou a executar.
final class WorkflowStartedEvent extends WorkflowEvent {
  const WorkflowStartedEvent({
    required super.v,
    required super.seq,
    required super.runId,
    super.ts,
    this.snakefile,
    this.workdir,
  });

  /// Caminho do Snakefile do run.
  final String? snakefile;

  /// Diretório de trabalho do run.
  final String? workdir;
}

/// O DAG de regras do workflow está disponível.
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

/// Estatística inicial do run: quantos jobs cada regra vai executar.
final class RunInfoEvent extends WorkflowEvent {
  const RunInfoEvent({
    required super.v,
    required super.seq,
    required super.runId,
    super.ts,
    required this.stats,
    this.total,
  });

  /// Contagem de jobs por regra (`{regra: contagem}`); pode incluir a chave
  /// agregada `total`.
  final Map<String, dynamic> stats;

  /// Total de jobs do run, quando informado.
  final int? total;
}

/// Detalhes de um job agendado.
///
/// Em runs reais pode chegar *depois* de [JobStartedEvent]; o reducer cria o
/// job no primeiro evento e completa os detalhes quando este chega.
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

  /// Id do job dentro do run.
  final int jobId;

  /// Nome da regra que o job executa.
  final String ruleName;

  /// `message` declarado na regra, se houver.
  final String? ruleMsg;

  /// Arquivos de entrada do job.
  final List<String> input;

  /// Arquivos de saída do job.
  final List<String> output;

  /// Arquivos de log declarados na regra.
  final List<String> log;

  /// Wildcards resolvidos para este job.
  final Map<String, dynamic> wildcards;

  /// Se a regra é um checkpoint do Snakemake.
  final bool isCheckpoint;

  /// Comando shell do job, quando a regra usa `shell:`.
  final String? shellcmd;

  /// Threads alocadas ao job.
  final int? threads;

  /// Motivo pelo qual o job foi agendado (ex.: output ausente).
  final String? reason;

  /// Recursos alocados ao job (`{recurso: valor}`).
  final Map<String, dynamic> resources;
}

/// Um lote de jobs começou a executar.
final class JobStartedEvent extends WorkflowEvent {
  const JobStartedEvent({
    required super.v,
    required super.seq,
    required super.runId,
    super.ts,
    required this.jobIds,
  });

  /// Ids dos jobs iniciados neste lote.
  final List<int> jobIds;
}

/// Um job terminou com sucesso.
final class JobFinishedEvent extends WorkflowEvent {
  const JobFinishedEvent({
    required super.v,
    required super.seq,
    required super.runId,
    super.ts,
    required this.jobId,
    required this.ruleName,
  });

  /// Id do job concluído.
  final int jobId;

  /// Nome da regra do job (pode vir vazio; complete via [JobInfoEvent]).
  final String ruleName;
}

/// Um job falhou.
final class JobErrorEvent extends WorkflowEvent {
  const JobErrorEvent({
    required super.v,
    required super.seq,
    required super.runId,
    super.ts,
    required this.jobId,
    required this.ruleName,
  });

  /// Id do job que falhou.
  final int jobId;

  /// Nome da regra do job (pode vir vazio; complete via [JobInfoEvent]).
  final String ruleName;
}

/// Comando shell executado por um job.
final class ShellCmdEvent extends WorkflowEvent {
  const ShellCmdEvent({
    required super.v,
    required super.seq,
    required super.runId,
    super.ts,
    required this.shellcmd,
    this.jobId,
  });

  /// Linha de comando executada.
  final String shellcmd;

  /// Job ao qual o comando pertence, quando informado.
  final int? jobId;
}

/// Progresso global do run.
final class ProgressEvent extends WorkflowEvent {
  const ProgressEvent({
    required super.v,
    required super.seq,
    required super.runId,
    super.ts,
    required this.done,
    required this.total,
  });

  /// Jobs concluídos até aqui.
  final int done;

  /// Total de jobs do run.
  final int total;
}

/// Recursos disponíveis para o run (nós, cores, recursos declarados).
final class ResourcesInfoEvent extends WorkflowEvent {
  const ResourcesInfoEvent({
    required super.v,
    required super.seq,
    required super.runId,
    super.ts,
    required this.payload,
  });

  /// Payload bruto (`nodes`, `cores`, `provided_resources`, …).
  final Map<String, dynamic> payload;
}

/// Erro de workflow (falha de regra, exceção do Snakemake, etc.).
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

  /// Descrição do erro.
  final String message;

  /// Regra associada ao erro, quando aplicável.
  final String? rule;

  /// Localização (arquivo/linha) do erro, quando aplicável.
  final String? location;

  /// Traceback Python do erro, quando disponível.
  final String? traceback;
}

/// Linha de log comum do Snakemake (registros sem `LogEvent` estruturado).
final class LogLineEvent extends WorkflowEvent {
  const LogLineEvent({
    required super.v,
    required super.seq,
    required super.runId,
    super.ts,
    required this.level,
    required this.message,
  });

  /// Nível do log (`info`, `warning`, `error`, …).
  final String level;

  /// Texto da linha de log.
  final String message;
}

/// Resposta do plugin ao comando `ping`.
final class PongEvent extends WorkflowEvent {
  const PongEvent({
    required super.v,
    required super.seq,
    required super.runId,
    super.ts,
  });
}

/// Encerramento limpo do run; último frame após o flush da fila do plugin.
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

  /// Valor bruto do campo `type` do envelope.
  final String type;

  /// Payload bruto do evento, sem interpretação.
  final Map<String, dynamic> payload;
}
