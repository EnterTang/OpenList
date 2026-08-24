import unittest

from types import SimpleNamespace

from openlistbridge.core import CreatedTorrent
from openlistbridge.moviepilot import MoviePilotGatewayAdapter, MoviePilotV3Gateway


class MoviePilotGatewayAdapterTest(unittest.TestCase):
    def test_delegates_download_creation_to_explicit_binding_provider(self) -> None:
        received = []
        adapter = MoviePilotGatewayAdapter(
            search_resources=lambda request: [{"resource_ref": "opaque-1", "title": request["query"]}],
            create_download_and_bind=lambda intent: received.append(intent) or {
                "downloader": "qb-hk", "torrent_hash": "c" * 40, "content_path": "/downloads/Example", "size": 9,
            },
        )

        created = adapter.create_download({"request_id": "request-1"})

        self.assertEqual([{"request_id": "request-1"}], received)
        self.assertEqual(CreatedTorrent("qb-hk", "c" * 40, "/downloads/Example", 9), created)


class FakeSearchChain:
    def __init__(self, contexts):
        self.contexts = contexts
        self.calls = []

    def search_by_id(self, **kwargs):
        self.calls.append(kwargs)
        return self.contexts


class FakeDownloadChain:
    def __init__(self, torrent):
        self.torrent = torrent
        self.download_calls = []
        self.list_calls = []
        self.stop_calls = []
        self.start_calls = []

    def download_single(self, context, **kwargs):
        self.download_calls.append((context, kwargs))
        return self.torrent.hash, ""

    def list_torrents(self, **kwargs):
        self.list_calls.append(kwargs)
        return [self.torrent]

    def stop_torrents(self, **kwargs):
        self.stop_calls.append(kwargs)
        return True

    def start_torrents(self, **kwargs):
        self.start_calls.append(kwargs)
        return True


class MoviePilotV3GatewayTest(unittest.TestCase):
    def setUp(self) -> None:
        torrent_info = SimpleNamespace(
            title="Example S01E01-E03",
            site_name="PT",
            site_cookie="must-not-leak",
            enclosure="https://secret.example/download?id=1",
            size=123,
            seeders=9,
            leechers=2,
            page_url="https://pt.example/details/1",
        )
        self.context = SimpleNamespace(torrent_info=torrent_info)
        self.bound = SimpleNamespace(
            downloader="qb-hk",
            hash="a" * 40,
            content_path="/downloads/Example S01",
            path="/downloads/Example S01",
            size=123,
            state="downloading",
            progress=0.25,
            ratio=0.4,
            seeding_time=0,
            left_time="1h",
        )
        self.search_chain = FakeSearchChain([self.context])
        self.download_chain = FakeDownloadChain(self.bound)
        self.gateway = MoviePilotV3Gateway(
            search_chain=self.search_chain,
            download_chain=self.download_chain,
            hmac_key=b"0123456789abcdef0123456789abcdef",
            save_path="/downloads",
            media_type_resolver=lambda value: value,
        )

    def test_search_returns_opaque_resource_and_exact_download_binding(self) -> None:
        request = {
            "request_id": "search-1",
            "query": "Example",
            "media_source": "tmdb",
            "media_id": "123",
            "media_type": "tv",
            "season": 1,
        }

        results = self.gateway.search(request)
        created = self.gateway.create_download({
            "request_id": "request-1",
            "media": request,
            "torrent": {
                "resource_ref": results[0]["resource_ref"],
                "selected_fingerprint": results[0]["selected_fingerprint"],
                "title": results[0]["title"],
            },
            "downloader_policy": {"mode": "moviepilot_select"},
        })

        self.assertTrue(results[0]["resource_ref"].startswith("olb:v1:"))
        self.assertNotIn("enclosure", results[0])
        self.assertNotIn("site_cookie", results[0])
        self.assertEqual(CreatedTorrent("qb-hk", "a" * 40, "/downloads/Example S01", 123), created)
        _, kwargs = self.download_chain.download_calls[0]
        self.assertEqual(self.gateway._request_label("request-1"), kwargs["label"])
        self.assertEqual("/downloads", kwargs["save_path"])
        self.assertTrue(kwargs["return_detail"])
        self.assertEqual({"hashs": ["a" * 40], "include_all_tags": True}, self.download_chain.list_calls[0])

    def test_recovers_created_torrent_by_request_specific_label_after_restart(self) -> None:
        request_id = "request-recovery"
        self.bound.tags = self.gateway._request_label(request_id)

        recovered = self.gateway.recover_download({"request_id": request_id})

        self.assertEqual(CreatedTorrent("qb-hk", "a" * 40, "/downloads/Example S01", 123), recovered)
        self.assertEqual({"include_all_tags": True}, self.download_chain.list_calls[0])

    def test_reads_state_for_the_exact_bound_downloader_and_hash(self) -> None:
        created = CreatedTorrent("qb-hk", "a" * 40, "/downloads/Example S01", 123)

        state = self.gateway.get_torrent_state(created)

        self.assertEqual("downloading", state.state)
        self.assertEqual(0.25, state.progress)
        self.assertEqual(0.4, state.ratio)
        self.assertEqual(
            {"hashs": ["a" * 40], "downloader": "qb-hk", "include_all_tags": True},
            self.download_chain.list_calls[0],
        )

    def test_controls_only_the_exact_bound_downloader_and_hash(self) -> None:
        created = CreatedTorrent("qb-hk", "a" * 40, "/downloads/Example S01", 123)

        self.gateway.control_torrent(created, "pause")
        self.gateway.control_torrent(created, "resume")

        expected = {"hashs": ["a" * 40], "downloader": "qb-hk"}
        self.assertEqual([expected], self.download_chain.stop_calls)
        self.assertEqual([expected], self.download_chain.start_calls)


if __name__ == "__main__":
    unittest.main()
