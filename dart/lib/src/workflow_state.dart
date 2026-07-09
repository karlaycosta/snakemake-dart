/// Reducer que consolida o stream de eventos num estado consultável do run.
///
/// A integração é agnóstica de gerenciamento de estado: envolva
/// [WorkflowRunState] num ChangeNotifier, num Notifier do Riverpod ou num
/// Bloc e chame [apply] a cada evento.
library;

import 'workflow_events.dart';

enum RunStatus { idle, running, failed, finished }

enum JobStatus { scheduled, running, finished, failed }

class JobState {
  JobState({required this.jobId, required this.ruleName, this.info});

  final int jobId;

  /// Mutável: em runs reais JOB_STARTED (sem nome de regra) chega antes de
  /// JOB_INFO, então o nome é preenchido por um evento posterior.
  String ruleName;
  JobInfoEvent? info;
  JobStatus status = JobStatus.scheduled;
  DateTime? startedAt;
  DateTime? endedAt;
  String? shellcmd;
}

class WorkflowRunState {
  /// Limites de retenção: em runs longos e verbosos, logs/erros ilimitados
  /// só fariam a memória do app crescer; os mais antigos são descartados.
  static const int maxLogLines = 5000;
  static const int maxErrors = 1000;

  String? runId;
  String? snakefile;
  RunStatus status = RunStatus.idle;
  int done = 0;
  int total = 0;
  dynamic rulegraph;

  /// Contagem de jobs por regra, vinda do evento `run_info`.
  Map<String, int> statsByRule = {};
  final Map<int, JobState> jobs = {};
  final List<LogLineEvent> logLines = [];
  final List<WorkflowErrorEvent> errors = [];

  double? get progress => total > 0 ? done / total : null;

  Iterable<JobState> byStatus(JobStatus s) =>
      jobs.values.where((j) => j.status == s);

  JobState _job(int jobId, [String ruleName = '']) {
    final job = jobs.putIfAbsent(
      jobId,
      () => JobState(jobId: jobId, ruleName: ruleName),
    );
    if (job.ruleName.isEmpty && ruleName.isNotEmpty) job.ruleName = ruleName;
    return job;
  }

  /// Consolida um evento no estado. Retorna `true` quando algo mudou
  /// (útil para limitar as notificações de UI).
  bool apply(WorkflowEvent event) {
    switch (event) {
      case HelloEvent():
        if (event.runId != runId) {
          // Novo run: descarta o estado herdado de um run anterior quando
          // esta instância é reutilizada (os seq reiniciam por run_id).
          runId = event.runId;
          snakefile = null;
          status = RunStatus.idle;
          done = 0;
          total = 0;
          rulegraph = null;
          statsByRule = {};
          jobs.clear();
          logLines.clear();
          errors.clear();
        }
        return true;
      case WorkflowStartedEvent():
        status = RunStatus.running;
        snakefile = event.snakefile;
        return true;
      case RuleGraphEvent():
        rulegraph = event.rulegraph;
        return true;
      case RunInfoEvent():
        // `stats` traz {regra: contagem}; a chave `total` (quando presente)
        // é o agregado, não uma regra — fica de fora do mapa.
        statsByRule = {
          for (final MapEntry(:key, :value) in event.stats.entries)
            if (value is num && key != 'total') key: value.toInt(),
        };
        if (event.total != null) total = event.total!;
        return true;
      case JobInfoEvent():
        final job = _job(event.jobId, event.ruleName);
        job.info = event;
        job.shellcmd = event.shellcmd ?? job.shellcmd;
        return true;
      case JobStartedEvent():
        for (final id in event.jobIds) {
          _job(id)
            ..status = JobStatus.running
            ..startedAt = event.ts;
        }
        return event.jobIds.isNotEmpty;
      case JobFinishedEvent():
        _job(event.jobId, event.ruleName)
          ..status = JobStatus.finished
          ..endedAt = event.ts;
        return true;
      case JobErrorEvent():
        _job(event.jobId, event.ruleName)
          ..status = JobStatus.failed
          ..endedAt = event.ts;
        return true;
      case ShellCmdEvent():
        if (event.jobId != null) _job(event.jobId!).shellcmd = event.shellcmd;
        return event.jobId != null;
      case ProgressEvent():
        done = event.done;
        total = event.total;
        return true;
      case WorkflowErrorEvent():
        errors.add(event);
        if (errors.length > maxErrors) {
          errors.removeRange(0, errors.length - maxErrors);
        }
        status = RunStatus.failed;
        return true;
      case LogLineEvent():
        logLines.add(event);
        if (logLines.length > maxLogLines) {
          logLines.removeRange(0, logLines.length - maxLogLines);
        }
        return true;
      case ByeEvent():
        if (status == RunStatus.running) status = RunStatus.finished;
        return true;
      case ResourcesInfoEvent() || PongEvent() || UnknownEvent():
        return false;
    }
  }
}
