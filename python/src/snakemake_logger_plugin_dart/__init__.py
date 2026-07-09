"""Logger plugin do Snakemake que transmite os eventos do workflow para um
app desktop Dart via conexão WebSocket (ver ../../ARCHITECTURE.md).

Nota: sem ``from __future__ import annotations`` aqui, de propósito — o
registro de plugins do Snakemake lê as anotações do dataclass de settings em
tempo de execução para montar a CLI, e anotações stringificadas quebram o
argparse.
"""

import uuid
from dataclasses import dataclass, field
from logging import Handler, LogRecord
from typing import Optional

from snakemake_interface_logger_plugins.base import LogHandlerBase
from snakemake_interface_logger_plugins.settings import LogHandlerSettingsBase

from snakemake_logger_plugin_dart.events import EventTranslator
from snakemake_logger_plugin_dart.transport import WsTransport


@dataclass
class LogHandlerSettings(LogHandlerSettingsBase):
    """Configurações expostas via ``--logger-dart-*`` na CLI do Snakemake."""

    address: Optional[str] = field(
        default=None,
        metadata={
            "help": (
                "WebSocket URL of the Dart app's embedded server, "
                "e.g. ws://127.0.0.1:8765. Required."
            ),
            "env_var": True,
            "required": True,
        },
    )
    token: Optional[str] = field(
        default=None,
        metadata={
            "help": (
                "Bearer token expected by the app's WebSocket server. "
                "Prefer passing it via the SNAKEMAKE_LOGGER_DART_TOKEN "
                "env var so it does not show up in `ps` output."
            ),
            "env_var": True,
            "required": False,
        },
    )
    flush_timeout: Optional[float] = field(
        default=5.0,
        metadata={
            "help": (
                "Max seconds to wait on shutdown for queued events to be "
                "delivered to the app."
            ),
            "env_var": False,
            "required": False,
        },
    )


class DartBridgeHandler(Handler):
    """logging.Handler que alimenta o transporte WS com os eventos traduzidos."""

    def __init__(
        self,
        address: str,
        token: Optional[str] = None,
        flush_timeout: float = 5.0,
    ):
        super().__init__()
        self.run_id = str(uuid.uuid4())
        self.translator = EventTranslator()
        self.transport = WsTransport(
            address=address,
            run_id=self.run_id,
            token=token,
            flush_timeout=flush_timeout,
        )

    def emit(self, record: LogRecord) -> None:
        try:
            message = self.translator.translate(record)
            if message is None:
                return
            self.transport.send(*message)
        except Exception:
            self.handleError(record)

    def close(self) -> None:
        try:
            self.transport.close()
        finally:
            super().close()


class LogHandler(LogHandlerBase, DartBridgeHandler):
    """Ponto de entrada do logger plugin no Snakemake."""

    def __post_init__(self) -> None:
        # ``self.settings`` e ``self.common_settings`` são populados pelo
        # runtime de plugins do Snakemake antes de __post_init__ ser chamado.
        settings: LogHandlerSettings = self.settings  # type: ignore[assignment]
        if not settings.address:
            raise ValueError(
                "snakemake-logger-plugin-dart requires --logger-dart-address "
                "(or the SNAKEMAKE_LOGGER_DART_ADDRESS env var) to be set."
            )
        DartBridgeHandler.__init__(
            self,
            address=settings.address,
            token=settings.token,
            flush_timeout=(
                settings.flush_timeout if settings.flush_timeout is not None else 5.0
            ),
        )

    def emit(self, record: LogRecord) -> None:
        DartBridgeHandler.emit(self, record)

    @property
    def writes_to_stream(self) -> bool:
        return False

    @property
    def writes_to_file(self) -> bool:
        return False

    @property
    def has_filter(self) -> bool:
        return False

    @property
    def has_formatter(self) -> bool:
        return False

    @property
    def needs_rulegraph(self) -> bool:
        # Pede o rule graph ao Snakemake para o app poder desenhar o DAG.
        return True


__all__ = ["LogHandler", "LogHandlerSettings", "DartBridgeHandler"]
