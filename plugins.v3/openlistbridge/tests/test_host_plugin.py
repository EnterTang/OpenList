import importlib.util
import asyncio
import json
import sys
import tempfile
import types
import unittest
from pathlib import Path


class FakePluginBase:
    pass


def load_host_plugin_module():
    app_module = types.ModuleType("app")
    plugins_module = types.ModuleType("app.plugins")
    plugins_module._PluginBase = FakePluginBase
    app_module.plugins = plugins_module
    sys.modules["app"] = app_module
    sys.modules["app.plugins"] = plugins_module

    plugin_root = Path(__file__).parents[1]
    spec = importlib.util.spec_from_file_location(
        "openlistbridge_host",
        plugin_root / "__init__.py",
        submodule_search_locations=[str(plugin_root)],
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class MoviePilotV3HostPluginTest(unittest.TestCase):
    def test_exposes_real_v3_plugin_lifecycle_and_hmac_routes(self) -> None:
        module = load_host_plugin_module()

        self.assertTrue(issubclass(module.OpenListBridge, FakePluginBase))
        plugin = module.OpenListBridge()
        routes = plugin.get_api()

        self.assertEqual(
            ["/search", "/intent", "/intent/{request_id}", "/intent/{request_id}/cancel", "/control"],
            [route["path"] for route in routes],
        )
        self.assertTrue(all("auth" not in route for route in routes))
        for method in ("init_plugin", "get_state", "get_api", "get_form", "get_page", "stop_service"):
            self.assertTrue(callable(getattr(plugin, method)))

    def test_disabled_configuration_has_no_import_time_or_runtime_side_effects(self) -> None:
        module = load_host_plugin_module()
        plugin = module.OpenListBridge()

        plugin.init_plugin({"enabled": False})

        self.assertFalse(plugin.get_state())
        self.assertIsNone(plugin.runtime)
        plugin.stop_service()
        self.assertFalse(plugin.get_state())

    def test_host_route_rejects_oversized_body_before_bridge_core(self) -> None:
        module = load_host_plugin_module()
        plugin = module.OpenListBridge()

        class Core:
            def handle(self, *_args):
                raise AssertionError("oversized body reached bridge core")

        class Runtime:
            core = Core()

        class Request:
            method = "POST"
            headers = {"content-length": str((4 << 20) + 1)}

            async def body(self):
                return b"x" * ((4 << 20) + 1)

        plugin.runtime = Runtime()
        response = asyncio.run(plugin._handle_request(Request(), module.SEARCH_PATH))

        self.assertEqual(413, response.status_code)
        self.assertEqual("request body is too large", json.loads(response.body)["error"])


if __name__ == "__main__":
    unittest.main()
