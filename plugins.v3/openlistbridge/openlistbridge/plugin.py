"""MoviePilot V3 plugin facade.

The facade deliberately has no direct dependency on a particular MoviePilot
SDK import path.  A tiny host-specific bootstrap can instantiate it, provide a
``MoviePilotGateway`` built from that SDK, and mount ``wsgi_app`` below the
MoviePilot plugin API prefix.
"""

from typing import Callable, Optional

from .core import MoviePilotGateway
from .runtime import BridgeRuntime


class OpenListBridgePlugin:
    plugin_name = "OpenList Bridge"
    plugin_desc = "OpenList PT 下载控制面桥接（不整理、不上传）"
    plugin_version = "1.0.0"

    def __init__(self, *, event_sender: Optional[Callable[[dict, str], None]] = None) -> None:
        self._config: dict = {}
        self._gateway: Optional[MoviePilotGateway] = None
        self._event_sender = event_sender
        self.runtime: Optional[BridgeRuntime] = None
        self.last_error = "MoviePilot gateway has not been bound"

    def init_plugin(self, config: Optional[dict] = None) -> None:
        self._config = dict(config or {})
        self._build_runtime()

    def bind_gateway(self, gateway: MoviePilotGateway) -> None:
        self._gateway = gateway
        self._build_runtime()

    def get_state(self) -> bool:
        return self.runtime is not None

    @property
    def wsgi_app(self):
        return self.runtime.wsgi_app if self.runtime is not None else None

    def flush_outbox(self) -> int:
        return self.runtime.flush_outbox() if self.runtime is not None else 0

    def _build_runtime(self) -> None:
        self.runtime = None
        if self._gateway is None:
            self.last_error = "MoviePilot gateway has not been bound"
            return
        try:
            self.runtime = BridgeRuntime.from_config(
                self._config,
                gateway=self._gateway,
                event_sender=self._event_sender,
            )
            self.last_error = ""
        except ValueError as exc:
            self.last_error = str(exc)
