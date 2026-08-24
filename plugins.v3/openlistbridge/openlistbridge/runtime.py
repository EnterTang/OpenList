"""Runtime assembly used by the MoviePilot V3 plugin wrapper."""

from pathlib import Path
from typing import Callable, Optional

from .core import BridgeCore, MoviePilotGateway
from .http import BridgeWSGIApp
from .transport import CoordinatorEventSender


class BridgeRuntime:
    def __init__(self, core: BridgeCore) -> None:
        self.core = core
        self.wsgi_app = BridgeWSGIApp(core)

    @classmethod
    def from_config(
        cls,
        config: dict,
        *,
        gateway: MoviePilotGateway,
        event_sender: Optional[Callable[[dict, str], None]] = None,
    ) -> "BridgeRuntime":
        instance_id = str(config.get("instance_id", "")).strip()
        hmac_key = str(config.get("hmac_key", "")).encode()
        coordinator_url = str(config.get("coordinator_url", "")).strip()
        state_directory_raw = str(config.get("state_directory", "")).strip()
        if not instance_id or len(hmac_key) < 16 or not coordinator_url or not state_directory_raw:
            raise ValueError("instance_id, hmac_key, coordinator_url and state_directory are required")
        state_directory = Path(state_directory_raw)
        if event_sender is None:
            event_sender = CoordinatorEventSender(
                coordinator_url=coordinator_url,
                instance_id=instance_id,
                hmac_key=hmac_key,
                timeout_seconds=int(config.get("timeout_seconds", 15) or 15),
            )
        core = BridgeCore(
            instance_id=instance_id,
            hmac_key=hmac_key,
            database_path=state_directory / "openlistbridge.sqlite3",
            gateway=gateway,
            coordinator_event_sender=event_sender,
            retry_backoff_seconds=int(config.get("retry_backoff_seconds", 10) or 10),
            creation_recovery_timeout_seconds=int(config.get("creation_recovery_timeout_seconds", 600) or 600),
        )
        return cls(core)

    def flush_outbox(self) -> int:
        return self.core.flush_outbox()

    def tick(self) -> int:
        """Poll exact torrent bindings, then deliver all pending callbacks."""
        self.core.recover_pending_intents()
        self.core.poll_torrent_states()
        return self.core.flush_outbox()
