import tempfile
import unittest

from openlistbridge.core import CreatedTorrent, MoviePilotGateway
from openlistbridge.plugin import OpenListBridgePlugin


class Gateway(MoviePilotGateway):
    def search(self, request: dict) -> list[dict]:
        return []

    def create_download(self, intent: dict) -> CreatedTorrent:
        return CreatedTorrent("qb-a", "e" * 40, "/downloads/example")


class OpenListBridgePluginTest(unittest.TestCase):
    def test_becomes_healthy_only_after_gateway_and_secure_config_are_present(self) -> None:
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        plugin = OpenListBridgePlugin(event_sender=lambda *_: None)
        plugin.init_plugin({"instance_id": "mp-main", "hmac_key": "0123456789abcdef0123456789abcdef", "coordinator_url": "https://openlist.example", "state_directory": temp.name})
        self.assertFalse(plugin.get_state())

        plugin.bind_gateway(Gateway())

        self.assertTrue(plugin.get_state())


if __name__ == "__main__":
    unittest.main()
