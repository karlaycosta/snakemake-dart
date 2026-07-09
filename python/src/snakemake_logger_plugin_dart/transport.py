"""Transporte WebSocket assíncrono e resiliente usado pelo logger plugin Dart.

Objetivos de design (ver ../../ARCHITECTURE.md):
- ``send()`` nunca bloqueia a thread do Snakemake: eventos entram numa fila
  em memória drenada por uma thread de trabalho dedicada.
- A thread de trabalho reconecta com backoff exponencial e mantém um buffer
  de replay para o app se atualizar após (re)conectar.
- A entrega é at-least-once; consumidores deduplicam pelo ``seq``.
"""

from __future__ import annotations

import json
import logging
import os
import queue
import threading
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from websockets.sync.client import connect

_LOG = logging.getLogger(__name__)

SCHEMA_VERSION = 1


class WsTransport:
    """Cliente WebSocket rodando em thread própria.

    Os métodos públicos (``send``, ``close``) são chamados a partir da thread
    de logging do Snakemake; tudo que envolve rede acontece na thread de
    trabalho.
    """

    def __init__(
        self,
        address: str,
        run_id: str,
        token: Optional[str] = None,
        flush_timeout: float = 5.0,
        queue_size: int = 10_000,
        buffer_size: int = 100_000,
        max_backoff: float = 10.0,
    ):
        self.address = address
        self.run_id = run_id
        self._token = token
        self._flush_timeout = flush_timeout
        self._max_backoff = max_backoff

        self._queue: queue.Queue[Dict[str, Any]] = queue.Queue(maxsize=queue_size)
        self._buffer: deque[Dict[str, Any]] = deque(maxlen=buffer_size)
        self._pending: Optional[Dict[str, Any]] = None  # retirado da fila, ainda não enviado
        self._seq = 0
        self._seq_lock = threading.Lock()
        self._stop = threading.Event()
        self._ever_connected = False
        self._closed = False

        self._worker = threading.Thread(
            target=self._run, name="dart-logger-ws", daemon=True
        )
        self._worker.start()

    # ------------------------------------------------------------------ #
    # lado produtor (thread do Snakemake)
    # ------------------------------------------------------------------ #

    def send(self, type_: str, payload: Dict[str, Any]) -> None:
        event = self._make_event(type_, payload)
        self._buffer.append(event)
        try:
            self._queue.put_nowait(event)
        except queue.Full:
            _LOG.warning(
                "dart-logger: event buffer full, dropping %r (seq=%s)",
                type_,
                event["seq"],
            )

    def close(self) -> None:
        """Descarrega os eventos pendentes (limitado por flush_timeout) e para."""
        if self._closed:
            return
        self._closed = True
        self.send("bye", {})
        if self._ever_connected:
            deadline = time.monotonic() + self._flush_timeout
            # Espera também o evento já retirado da fila (_pending): a fila
            # vazia não significa que o último evento (o próprio bye) já foi
            # enviado — sem isso, ele se perde se o processo encerrar logo
            # depois de close() retornar.
            while (
                (not self._queue.empty() or self._pending is not None)
                and time.monotonic() < deadline
                and self._worker.is_alive()
            ):
                time.sleep(0.05)
        self._stop.set()
        self._worker.join(timeout=2.0)

    # ------------------------------------------------------------------ #
    # lado da thread de trabalho
    # ------------------------------------------------------------------ #

    def _make_event(self, type_: str, payload: Dict[str, Any], seq: Optional[int] = None) -> Dict[str, Any]:
        if seq is None:
            with self._seq_lock:
                self._seq += 1
                seq = self._seq
        return {
            "v": SCHEMA_VERSION,
            "seq": seq,
            "ts": datetime.now(timezone.utc).isoformat(),
            "run_id": self.run_id,
            "type": type_,
            "payload": payload,
        }

    def _hello(self) -> Dict[str, Any]:
        return self._make_event(
            "hello",
            {"pid": os.getpid(), "schema": SCHEMA_VERSION, "run_id": self.run_id},
            seq=0,
        )

    def _run(self) -> None:
        backoff = 0.5
        headers = {"Authorization": f"Bearer {self._token}"} if self._token else None
        while not self._stop.is_set():
            try:
                with connect(
                    self.address, additional_headers=headers, open_timeout=5.0
                ) as ws:
                    self._ever_connected = True
                    backoff = 0.5
                    ws.send(json.dumps(self._hello(), default=str))
                    while not self._stop.is_set():
                        self._handle_commands(ws)
                        if not self._drain_one(ws):
                            continue
            except Exception as exc:
                if self._stop.is_set():
                    break
                _LOG.debug("dart-logger: connection lost/failed: %s", exc)
                if self._stop.wait(backoff):
                    break
                backoff = min(backoff * 2, self._max_backoff)

    def _drain_one(self, ws: Any) -> bool:
        """Envia um evento do pendente/da fila. Retorna False quando ocioso."""
        if self._pending is None:
            try:
                self._pending = self._queue.get(timeout=0.2)
            except queue.Empty:
                return False
        ws.send(json.dumps(self._pending, default=str))
        self._pending = None
        return True

    def _handle_commands(self, ws: Any) -> None:
        while True:
            try:
                raw = ws.recv(timeout=0)
            except TimeoutError:
                return
            try:
                msg = json.loads(raw)
            except (ValueError, TypeError):
                continue
            if not isinstance(msg, dict) or msg.get("type") != "command":
                continue
            cmd = msg.get("cmd")
            if cmd == "replay":
                try:
                    since = int(msg.get("since_seq", 0))
                except (ValueError, TypeError):
                    since = 0
                for event in list(self._buffer):
                    if event["seq"] > since:
                        ws.send(json.dumps(event, default=str))
            elif cmd == "ping":
                ws.send(json.dumps(self._make_event("pong", {}), default=str))
