import tempfile
import unittest
from pathlib import Path

from openlistbridge.core import CreatedTorrent, MoviePilotGateway
from openlistbridge.runtime import BridgeRuntime


class Gateway(MoviePilotGateway):
    def search(self, request: dict) -> list[dict]:
        return []

    def create_download(self, intent: dict) -> CreatedTorrent:
        return CreatedTorrent("qb-a", "d" * 40, "/downloads/example")


class BridgeRuntimeTest(unittest.TestCase):
    def test_builds_plugin_runtime_from_secure_configuration(self) -> None:
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        runtime = BridgeRuntime.from_config(
            {
                "instance_id": "mp-main",
                "hmac_key": "0123456789abcdef0123456789abcdef",
                "coordinator_url": "https://openlist.example",
                "state_directory": temp.name,
            },
            gateway=Gateway(),
            event_sender=lambda *_: None,
        )

        self.assertEqual("mp-main", runtime.core.instance_id)
        self.assertEqual(Path(temp.name) / "openlistbridge.sqlite3", Path(runtime.core.database_path))
        self.assertIsNotNone(runtime.wsgi_app)


if __name__ == "__main__":
    unittest.main()
