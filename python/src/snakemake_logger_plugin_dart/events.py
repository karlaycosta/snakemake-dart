"""Traduz LogRecords do Snakemake em mensagens (type, payload) do protocolo.

Todos os atributos do record são acessados defensivamente com ``getattr``,
para que mudanças entre versões do Snakemake degradem graciosamente em vez
de derrubar o workflow.
"""

from __future__ import annotations

import logging
from logging import LogRecord
from typing import Any, Dict, Optional, Tuple

from snakemake_interface_logger_plugins.common import LogEvent

Message = Tuple[str, Dict[str, Any]]


def _to_jsonable(value: Any) -> Any:
    """Conversão best-effort para primitivos serializáveis em JSON."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, dict):
        return {str(k): _to_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_to_jsonable(v) for v in value]
    # Namedlist / IOFile / Wildcards do Snakemake se comportam como sequências;
    # cai para str() para o app ainda ver alguma coisa.
    try:
        return [_to_jsonable(v) for v in list(value)]  # type: ignore[arg-type]
    except TypeError:
        return str(value)


def _resources_to_dict(resources: Any) -> Dict[str, Any]:
    if resources is None:
        return {}
    names = getattr(resources, "_names", None)
    if names is None:
        return _to_jsonable(resources) if isinstance(resources, dict) else {}
    return {
        name: _to_jsonable(value)
        for name, value in zip(names, resources)
        if name not in {"_cores", "_nodes"}
    }


def _int_or_none(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _record_job_id(record: LogRecord) -> Optional[int]:
    """O Snakemake usa ora ``jobid``, ora ``job_id``; aceita ambos."""
    value = getattr(record, "jobid", None)
    if value is None:
        value = getattr(record, "job_id", None)
    return _int_or_none(value)


class EventTranslator:
    """Tradutor com estado: lembra jobid -> regra para que eventos terminais
    (finished/error) levem o nome da regra de volta ao app."""

    def __init__(self) -> None:
        self._job_rules: Dict[int, str] = {}

    # ------------------------------------------------------------------ #
    # tradutores por evento
    # ------------------------------------------------------------------ #

    def _on_workflow_started(self, record: LogRecord) -> Message:
        return (
            "workflow_started",
            {
                "snakefile": _to_jsonable(getattr(record, "snakefile", None)),
                "workdir": _to_jsonable(getattr(record, "workdir", None)),
            },
        )

    def _on_rulegraph(self, record: LogRecord) -> Message:
        return (
            "rulegraph",
            {"rulegraph": _to_jsonable(getattr(record, "rulegraph", None))},
        )

    def _on_run_info(self, record: LogRecord) -> Optional[Message]:
        stats = getattr(record, "stats", None) or {}
        return (
            "run_info",
            {
                "stats": _to_jsonable(stats),
                "total": _int_or_none(stats.get("total")) if isinstance(stats, dict) else None,
            },
        )

    def _on_job_info(self, record: LogRecord) -> Message:
        raw_job_id = _record_job_id(record)
        job_id = raw_job_id if raw_job_id is not None else 0
        rule_name = getattr(record, "rule_name", "") or ""
        if raw_job_id is not None:
            # Só memoriza a regra com um id real — jobs sem id colidiriam em 0.
            self._job_rules[job_id] = rule_name
        return (
            "job_info",
            {
                "job_id": job_id,
                "rule_name": rule_name,
                "rule_msg": getattr(record, "rule_msg", None),
                "input": _to_jsonable(getattr(record, "input", []) or []),
                "output": _to_jsonable(getattr(record, "output", []) or []),
                "log": _to_jsonable(getattr(record, "log", []) or []),
                "wildcards": _to_jsonable(getattr(record, "wildcards", {}) or {}),
                "is_checkpoint": bool(getattr(record, "is_checkpoint", False)),
                "shellcmd": getattr(record, "shellcmd", None),
                "threads": _to_jsonable(getattr(record, "threads", None)),
                "priority": _to_jsonable(getattr(record, "priority", None)),
                "reason": _to_jsonable(getattr(record, "reason", None)),
                "resources": _resources_to_dict(getattr(record, "resources", None)),
            },
        )

    def _on_job_started(self, record: LogRecord) -> Optional[Message]:
        jobs = getattr(record, "jobs", None)
        if jobs is None:
            return None
        if isinstance(jobs, int):
            jobs = [jobs]
        job_ids = [j for j in (_int_or_none(j) for j in jobs) if j is not None]
        return ("job_started", {"job_ids": job_ids})

    def _on_job_finished(self, record: LogRecord) -> Optional[Message]:
        job_id = _record_job_id(record)
        if job_id is None:
            return None
        return (
            "job_finished",
            {"job_id": job_id, "rule_name": self._job_rules.get(job_id, "")},
        )

    def _on_job_error(self, record: LogRecord) -> Message:
        job_id = _record_job_id(record) or 0
        return (
            "job_error",
            {"job_id": job_id, "rule_name": self._job_rules.get(job_id, "")},
        )

    def _on_group_info(self, record: LogRecord) -> Message:
        return (
            "group_info",
            {
                "group_id": _to_jsonable(getattr(record, "group_id", None)),
                "jobs": _to_jsonable(getattr(record, "jobs", None)),
            },
        )

    def _on_group_error(self, record: LogRecord) -> Message:
        return (
            "group_error",
            {
                "group_id": _to_jsonable(getattr(record, "group_id", None)),
                "message": record.getMessage(),
            },
        )

    def _on_shellcmd(self, record: LogRecord) -> Optional[Message]:
        shellcmd = getattr(record, "shellcmd", None)
        if not shellcmd:
            return None
        return (
            "shellcmd",
            {
                "job_id": _record_job_id(record),
                "shellcmd": str(shellcmd),
            },
        )

    def _on_progress(self, record: LogRecord) -> Message:
        return (
            "progress",
            {
                "done": _int_or_none(getattr(record, "done", 0)) or 0,
                "total": _int_or_none(getattr(record, "total", 0)) or 0,
            },
        )

    def _on_resources_info(self, record: LogRecord) -> Message:
        return (
            "resources_info",
            {
                "nodes": _to_jsonable(getattr(record, "nodes", None)),
                "cores": _to_jsonable(getattr(record, "cores", None)),
                "provided_resources": _to_jsonable(
                    getattr(record, "provided_resources", None)
                ),
            },
        )

    def _on_error(self, record: LogRecord) -> Message:
        return (
            "error",
            {
                "message": _to_jsonable(
                    getattr(record, "exception", None) or record.getMessage()
                ),
                "rule": _to_jsonable(getattr(record, "rule", None)),
                "location": _to_jsonable(getattr(record, "location", None)),
                "traceback": _to_jsonable(getattr(record, "traceback", None)),
            },
        )

    # ------------------------------------------------------------------ #
    # despacho
    # ------------------------------------------------------------------ #

    # Montado com getattr para o plugin tolerar membros de LogEvent surgindo
    # ou sumindo entre versões do snakemake-interface-logger-plugins.
    _DISPATCH_NAMES = {
        "WORKFLOW_STARTED": "_on_workflow_started",
        "RULEGRAPH": "_on_rulegraph",
        "RUN_INFO": "_on_run_info",
        "JOB_INFO": "_on_job_info",
        "JOB_STARTED": "_on_job_started",
        "JOB_FINISHED": "_on_job_finished",
        "JOB_ERROR": "_on_job_error",
        "GROUP_INFO": "_on_group_info",
        "GROUP_ERROR": "_on_group_error",
        "SHELLCMD": "_on_shellcmd",
        "PROGRESS": "_on_progress",
        "RESOURCES_INFO": "_on_resources_info",
        "ERROR": "_on_error",
    }

    _DISPATCH = {
        getattr(LogEvent, name): method
        for name, method in _DISPATCH_NAMES.items()
        if hasattr(LogEvent, name)
    }

    def translate(self, record: LogRecord) -> Optional[Message]:
        event = getattr(record, "event", None)
        if event is None:
            # Linha de log comum: encaminha INFO+ para o app poder exibir um console.
            if record.levelno < logging.INFO:
                return None
            message = record.getMessage()
            if not message:
                return None
            return ("log", {"level": record.levelname.lower(), "message": message})
        method_name = self._DISPATCH.get(event)
        if method_name is None:
            return None
        return getattr(self, method_name)(record)
