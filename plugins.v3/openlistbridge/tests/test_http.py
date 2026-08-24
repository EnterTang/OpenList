import io
import json
import tempfile
import unittest
from pathlib import Path

from openlistbridge.core import BridgeCore, CreatedTorrent, MoviePilotGateway, SEARCH_PATH
from openlistbridge.http import BridgeWSGIApp

from test_core import signed_headers


class Gateway(MoviePilotGateway):
    def search(self, request: dict) -> list[dict]:
        return [{"resource_ref": "opaque-1", "title": request["query"], "site_cookie": "must-not-leak"}]

    def create_download(self, intent: dict) -> CreatedTorrent:
        return CreatedTorrent("qb-a", "b" * 40, "/downloads/example")


class BridgeWSGIAppTest(unittest.TestCase):
    def test_search_exposes_only_sanitized_results(self) -> None:
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        key = b"0123456789abcdef0123456789abcdef"
        core = BridgeCore(instance_id="mp-main", hmac_key=key, database_path=Path(temp.name) / "bridge.db", gateway=Gateway(), coordinator_event_sender=lambda *_: None, now=lambda: 1_700_000_000)
        app = BridgeWSGIApp(core)
        body = json.dumps({"request_id": "search-1", "query": "Example", "media_source": "tmdb", "media_id": "123"}).encode()
        headers = signed_headers(key, "mp-main", "POST", SEARCH_PATH, body, 1_700_000_000, "nonce-http")
        environ = {
            "REQUEST_METHOD": "POST",
            "PATH_INFO": SEARCH_PATH,
            "CONTENT_LENGTH": str(len(body)),
            "wsgi.input": io.BytesIO(body),
            **{"HTTP_" + name.upper().replace("-", "_"): value for name, value in headers.items()},
        }
        status = []

        response = b"".join(app(environ, lambda line, values: status.append((line, values))))

        self.assertEqual("200 OK", status[0][0])
        self.assertEqual({"results": [{"resource_ref": "opaque-1", "title": "Example"}]}, json.loads(response))


if __name__ == "__main__":
    unittest.main()
