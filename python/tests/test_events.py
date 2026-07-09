import json
import logging

from snakemake_interface_logger_plugins.common import LogEvent

from snakemake_logger_plugin_dart.events import EventTranslator


def make_record(**extra):
    record = logging.makeLogRecord({"msg": extra.pop("msg", ""), "levelno": logging.INFO})
    record.levelname = "INFO"
    for key, value in extra.items():
        setattr(record, key, value)
    return record


def test_job_info_payload_is_json_serialisable():
    translator = EventTranslator()
    record = make_record(
        event=LogEvent.JOB_INFO,
        jobid=3,
        rule_name="align",
        input=("a.fq", "b.fq"),
        output=["out.bam"],
        wildcards={"sample": "s1"},
        threads=4,
    )
    type_, payload = translator.translate(record)
    assert type_ == "job_info"
    assert payload["job_id"] == 3
    assert payload["rule_name"] == "align"
    assert payload["input"] == ["a.fq", "b.fq"]
    assert payload["wildcards"] == {"sample": "s1"}
    json.dumps(payload)  # não deve lançar exceção


def test_job_finished_remembers_rule_name():
    translator = EventTranslator()
    translator.translate(make_record(event=LogEvent.JOB_INFO, jobid=7, rule_name="sort"))
    type_, payload = translator.translate(
        make_record(event=LogEvent.JOB_FINISHED, job_id=7)
    )
    assert type_ == "job_finished"
    assert payload == {"job_id": 7, "rule_name": "sort"}


def test_job_started_normalises_single_int():
    translator = EventTranslator()
    type_, payload = translator.translate(
        make_record(event=LogEvent.JOB_STARTED, jobs=5)
    )
    assert type_ == "job_started"
    assert payload == {"job_ids": [5]}


def test_progress():
    translator = EventTranslator()
    type_, payload = translator.translate(
        make_record(event=LogEvent.PROGRESS, done=2, total=10)
    )
    assert (type_, payload) == ("progress", {"done": 2, "total": 10})


def test_plain_info_log_becomes_log_event():
    translator = EventTranslator()
    type_, payload = translator.translate(make_record(msg="hello world"))
    assert type_ == "log"
    assert payload == {"level": "info", "message": "hello world"}


def test_plain_debug_log_is_dropped():
    translator = EventTranslator()
    record = make_record(msg="noise")
    record.levelno = logging.DEBUG
    assert translator.translate(record) is None
