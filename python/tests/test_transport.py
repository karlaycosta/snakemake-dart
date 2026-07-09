import json
import queue
import threading
import time

import pytest
from websockets.sync.server import serve

from snakemake_logger_plugin_dart.transport import WsTransport


class FakeApp:
    """Substituto mínimo do servidor WebSocket embarcado no app Dart."""

    def __init__(self, expected_token=None):
        self.received = []
        self.lock = threading.Lock()
        self.expected_token = expected_token
        self.rejected = threading.Event()
        self.connected = threading.Event()
        self._outgoing = []
        self._server = serve(self._handler, "127.0.0.1", 0)
        self.port = self._server.socket.getsockname()[1]
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    @property
    def address(self):
        return f"ws://127.0.0.1:{self.port}"

    def queue_command(self, command):
        self._outgoing.append(json.dumps(command))

    def _handler(self, ws):
        if self.expected_token is not None:
            auth = ws.request.headers.get("Authorization", "")
            if auth != f"Bearer {self.expected_token}":
                self.rejected.set()
                ws.close(code=4401, reason="bad token")
                return
        self.connected.set()
        for command in self._outgoing:
            ws.send(command)
        try:
            for raw in ws:
                with self.lock:
                    self.received.append(json.loads(raw))
        except Exception:
            pass

    def events(self, type_=None):
        with self.lock:
            events = list(self.received)
        if type_ is not None:
            events = [e for e in events if e["type"] == type_]
        return events

    def wait_for(self, predicate, timeout=5.0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return True
            time.sleep(0.02)
        return False

    def stop(self):
        self._server.shutdown()


@pytest.fixture
def app():
    server = FakeApp()
    yield server
    server.stop()


def test_events_are_delivered_with_envelope(app):
    transport = WsTransport(app.address, run_id="run-1")
    transport.send("progress", {"done": 1, "total": 3})
    assert app.wait_for(lambda: app.events("progress"))
    transport.close()

    hello = app.events("hello")[0]
    assert hello["seq"] == 0
    assert hello["run_id"] == "run-1"

    event = app.events("progress")[0]
    assert event["v"] == 1
    assert event["seq"] >= 1
    assert event["payload"] == {"done": 1, "total": 3}
    assert app.events("bye"), "close() should flush the bye event"


def test_replay_command_resends_buffered_events(app):
    app.queue_command({"type": "command", "cmd": "replay", "since_seq": 0})
    transport = WsTransport(app.address, run_id="run-2")
    transport.send("progress", {"done": 1, "total": 2})
    # replay + entrega ao vivo => at-least-once: espere >= 1 e deduplique por seq
    assert app.wait_for(lambda: len(app.events("progress")) >= 1)
    transport.close()
    seqs = {e["seq"] for e in app.events("progress")}
    assert len(seqs) == 1


def test_token_is_sent_and_bad_token_rejected():
    server = FakeApp(expected_token="s3cret")
    try:
        good = WsTransport(server.address, run_id="run-3", token="s3cret")
        good.send("progress", {"done": 0, "total": 1})
        assert server.wait_for(lambda: server.events("progress"))
        good.close()

        bad = WsTransport(server.address, run_id="run-4", token="wrong")
        assert server.wait_for(lambda: server.rejected.is_set())
        bad._stop.set()
    finally:
        server.stop()


def test_close_flushes_event_taken_from_queue_but_not_yet_sent(app, monkeypatch):
    """Regressão: close() precisa esperar também o evento já retirado da fila
    (_pending), não só a fila esvaziar — senão o `bye` se perde quando o
    processo encerra logo depois de close() retornar."""

    def slow_drain(self, ws):
        if self._pending is None:
            try:
                self._pending = self._queue.get(timeout=0.2)
            except queue.Empty:
                return False
            # Amplia a janela da race: se _stop for setado aqui dentro, o
            # worker desiste sem enviar — como um daemon thread morto na
            # saída do processo.
            deadline = time.monotonic() + 0.3
            while time.monotonic() < deadline:
                if self._stop.is_set():
                    return False
                time.sleep(0.01)
        ws.send(json.dumps(self._pending, default=str))
        self._pending = None
        return True

    monkeypatch.setattr(WsTransport, "_drain_one", slow_drain)
    transport = WsTransport(app.address, run_id="run-6")
    assert app.wait_for(lambda: app.events("hello"))
    transport.close()
    assert app.wait_for(lambda: app.events("bye")), (
        "close() deve entregar o bye mesmo quando o evento já saiu da fila "
        "mas ainda não foi enviado"
    )


def test_close_is_idempotent(app):
    transport = WsTransport(app.address, run_id="run-7")
    assert app.wait_for(lambda: app.events("hello"))
    transport.close()
    transport.close()  # segundo close: no-op, sem novo bye na fila
    assert app.wait_for(lambda: app.events("bye"))
    assert len(app.events("bye")) == 1


def test_send_never_blocks_when_server_is_down():
    transport = WsTransport("ws://127.0.0.1:1", run_id="run-5")  # ninguém escutando
    start = time.monotonic()
    for i in range(100):
        transport.send("progress", {"done": i, "total": 100})
    elapsed = time.monotonic() - start
    assert elapsed < 0.5, "send() must not block on connection failures"
    transport.close()
