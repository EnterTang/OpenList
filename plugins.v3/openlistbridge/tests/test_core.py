import hashlib
import hmac
import json
import tempfile
import unittest
from pathlib import Path

from openlistbridge.core import BridgeCore, CONTROL_PATH, CreatedTorrent, INTENT_PATH, MoviePilotGateway, SEARCH_PATH, TorrentState


def signed_headers(key: bytes, instance_id: str, method: str, path: str, body: bytes, timestamp: int, nonce: str) -> dict[str, str]:
    body_hash = hashlib.sha256(body).hexdigest()
    canonical = "\n".join(("v1", instance_id, method, path, str(timestamp), nonce, body_hash)).encode()
    return {
        "X-OpenList-Bridge-Version": "v1",
        "X-OpenList-Bridge-Instance": instance_id,
        "X-OpenList-Bridge-Timestamp": str(timestamp),
        "X-OpenList-Bridge-Nonce": nonce,
        "X-OpenList-Bridge-Signature": hmac.new(key, canonical, hashlib.sha256).hexdigest(),
    }


class FakeGateway(MoviePilotGateway):
    def __init__(self) -> None:
        self.created: list[dict] = []
        self.controls: list[tuple[CreatedTorrent, str]] = []

    def search(self, request: dict) -> list[dict]:
        return [{"resource_ref": "torrent:example", "title": request["query"], "seeders": 3}]

    def create_download(self, intent: dict) -> CreatedTorrent:
        self.created.append(intent)
        return CreatedTorrent(downloader="qb-hk", torrent_hash="a" * 40, content_path="/downloads/Show", size=123)

    def control_torrent(self, torrent: CreatedTorrent, action: str) -> None:
        self.controls.append((torrent, action))


class BridgeCoreTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.key = b"0123456789abcdef0123456789abcdef"
        self.gateway = FakeGateway()
        self.callbacks: list[tuple[dict, str]] = []
        self.core = BridgeCore(
            instance_id="mp-main",
            hmac_key=self.key,
            database_path=Path(self.temp.name) / "bridge.sqlite3",
            gateway=self.gateway,
            coordinator_event_sender=lambda event, event_id: self.callbacks.append((event, event_id)),
            now=lambda: 1_700_000_000,
        )

    def test_submit_intent_binds_exact_qb_hash_and_emits_durable_events(self) -> None:
        body = json.dumps({
            "request_id": "request-1",
            "media": {"media_source": "tmdb", "media_id": "123", "media_type": "tv", "season": 1},
            "torrent": {"resource_ref": "torrent:example", "title": "Example S01"},
            "downloader_policy": {"mode": "moviepilot_select"},
        }).encode()
        headers = signed_headers(self.key, "mp-main", "POST", INTENT_PATH, body, 1_700_000_000, "nonce-1")

        result = self.core.handle("POST", INTENT_PATH, headers, body)

        self.assertEqual(202, result.status)
        self.assertEqual(["intent.accepted", "torrent.bound"], [event[0]["type"] for event in self.callbacks])
        bound = self.callbacks[1][0]["torrent"]
        self.assertEqual("qb-hk", bound["downloader"])
        self.assertEqual("a" * 40, bound["torrent_hash"])
        self.assertEqual("/downloads/Show", bound["content_path"])

    def test_request_id_cannot_be_reused_with_a_different_intent(self) -> None:
        first = {
            "request_id": "request-collision",
            "media": {"media_source": "tmdb", "media_id": "123"},
            "torrent": {"resource_ref": "torrent:first"},
            "downloader_policy": {"mode": "moviepilot_select"},
        }
        first_body = json.dumps(first).encode()
        first_headers = signed_headers(
            self.key, "mp-main", "POST", INTENT_PATH, first_body,
            1_700_000_000, "nonce-collision-first",
        )
        self.assertEqual(202, self.core.handle("POST", INTENT_PATH, first_headers, first_body).status)

        changed = dict(first)
        changed["torrent"] = {"resource_ref": "torrent:other"}
        changed_body = json.dumps(changed).encode()
        changed_headers = signed_headers(
            self.key, "mp-main", "POST", INTENT_PATH, changed_body,
            1_700_000_000, "nonce-collision-other",
        )
        response = self.core.handle("POST", INTENT_PATH, changed_headers, changed_body)

        self.assertEqual(409, response.status)
        self.assertIn("different intent", response.payload["error"])
        self.assertEqual(1, len(self.gateway.created))

    def test_persisted_torrent_binding_is_immutable(self) -> None:
        intent = {
            "request_id": "request-immutable-binding",
            "media": {"media_source": "tmdb", "media_id": "123"},
            "torrent": {"resource_ref": "torrent:example"},
            "downloader_policy": {"mode": "moviepilot_select"},
        }
        body = json.dumps(intent).encode()
        headers = signed_headers(
            self.key, "mp-main", "POST", INTENT_PATH, body,
            1_700_000_000, "nonce-immutable-binding",
        )
        self.assertEqual(202, self.core.handle("POST", INTENT_PATH, headers, body).status)

        with self.assertRaisesRegex(ValueError, "immutable torrent binding"):
            self.core._persist_created_torrent(
                intent, CreatedTorrent("qb-other", "b" * 40, "/downloads/Other", 456)
            )
        with self.core._connect() as conn:
            row = conn.execute(
                "SELECT downloader, torrent_hash, content_path FROM bindings WHERE request_id = ?",
                (intent["request_id"],),
            ).fetchone()
            bound_events = conn.execute(
                "SELECT COUNT(*) FROM outbox WHERE request_id = ? AND payload_json LIKE '%torrent.bound%'",
                (intent["request_id"],),
            ).fetchone()[0]
        self.assertEqual(("qb-hk", "a" * 40, "/downloads/Show"), tuple(row))
        self.assertEqual(1, bound_events)

    def test_replayed_nonce_is_rejected_before_gateway_runs(self) -> None:
        body = json.dumps({"request_id": "search-1", "query": "Example", "media_source": "tmdb", "media_id": "123"}).encode()
        headers = signed_headers(self.key, "mp-main", "POST", SEARCH_PATH, body, 1_700_000_000, "nonce-replayed")

        self.assertEqual(200, self.core.handle("POST", SEARCH_PATH, headers, body).status)
        result = self.core.handle("POST", SEARCH_PATH, headers, body)

        self.assertEqual(401, result.status)
        self.assertIn("already been used", result.payload["error"])

    def test_rejects_forbidden_secret_and_local_path_fields(self) -> None:
        body = json.dumps({
            "request_id": "search-forbidden", "query": "Example", "media_source": "tmdb", "media_id": "123",
            "nested": {"local_path": "/downloads", "site_cookie": "secret"},
        }).encode()
        headers = signed_headers(self.key, "mp-main", "POST", SEARCH_PATH, body, 1_700_000_000, "nonce-forbidden")

        response = self.core.handle("POST", SEARCH_PATH, headers, body)

        self.assertEqual(400, response.status)
        self.assertIn("forbidden", response.payload["error"])

    def test_rejects_pt_enclosure_instead_of_opaque_resource_reference(self) -> None:
        body = json.dumps({
            "request_id": "request-enclosure",
            "media": {"media_source": "tmdb", "media_id": "123"},
            "torrent": {"enclosure": "https://pt.example/download?passkey=secret"},
            "downloader_policy": {"mode": "moviepilot_select"},
        }).encode()
        headers = signed_headers(self.key, "mp-main", "POST", INTENT_PATH, body, 1_700_000_000, "nonce-enclosure")

        response = self.core.handle("POST", INTENT_PATH, headers, body)

        self.assertEqual(400, response.status)
        self.assertIn("forbidden", response.payload["error"])

    def test_rejects_direct_download_url_disguised_as_resource_reference(self) -> None:
        body = json.dumps({
            "request_id": "request-direct-url",
            "media": {"media_source": "tmdb", "media_id": "123"},
            "torrent": {"resource_ref": "https://pt.example/download?id=1&passkey=secret"},
            "downloader_policy": {"mode": "moviepilot_select"},
        }).encode()
        headers = signed_headers(
            self.key, "mp-main", "POST", INTENT_PATH, body,
            1_700_000_000, "nonce-direct-url",
        )

        response = self.core.handle("POST", INTENT_PATH, headers, body)

        self.assertEqual(400, response.status)
        self.assertIn("opaque", response.payload["error"])

    def test_polls_exact_binding_and_emits_deduplicated_state_changes(self) -> None:
        self.gateway.state = TorrentState("downloading", 0.2, ratio=0.1)
        self.gateway.get_torrent_state = lambda torrent: self.gateway.state
        body = json.dumps({
            "request_id": "request-state",
            "media": {"media_source": "tmdb", "media_id": "123", "media_type": "tv", "season": 1},
            "torrent": {"resource_ref": "torrent:example", "title": "Example S01"},
            "downloader_policy": {"mode": "moviepilot_select"},
        }).encode()
        headers = signed_headers(self.key, "mp-main", "POST", INTENT_PATH, body, 1_700_000_000, "nonce-state")

        self.assertEqual(202, self.core.handle("POST", INTENT_PATH, headers, body).status)
        self.assertEqual(
            ["intent.accepted", "torrent.bound", "torrent.state_changed"],
            [event[0]["type"] for event in self.callbacks],
        )

        self.core.poll_torrent_states()
        self.core.flush_outbox()
        self.assertEqual(3, len(self.callbacks))

        self.gateway.state = TorrentState("stalledUP", 1, ratio=1.5, seeding_seconds=3600, hnr_passed=True)
        self.core.poll_torrent_states()
        self.core.flush_outbox()

        self.assertEqual("torrent.state_changed", self.callbacks[-1][0]["type"])
        self.assertEqual("stalledUP", self.callbacks[-1][0]["state"]["state"])
        self.assertEqual(3600, self.callbacks[-1][0]["state"]["seeding_seconds"])

    def test_signed_control_pauses_only_the_exact_persisted_binding(self) -> None:
        intent_body = json.dumps({
            "request_id": "request-control",
            "media": {"media_source": "tmdb", "media_id": "123"},
            "torrent": {"resource_ref": "torrent:example"},
            "downloader_policy": {"mode": "moviepilot_select"},
        }).encode()
        intent_headers = signed_headers(self.key, "mp-main", "POST", INTENT_PATH, intent_body, 1_700_000_000, "nonce-control-intent")
        self.assertEqual(202, self.core.handle("POST", INTENT_PATH, intent_headers, intent_body).status)

        body = json.dumps({
            "request_id": "request-control", "downloader": "qb-hk",
            "torrent_hash": "a" * 40, "action": "pause", "reason": "worker_offline",
        }).encode()
        headers = signed_headers(self.key, "mp-main", "POST", CONTROL_PATH, body, 1_700_000_000, "nonce-control")
        response = self.core.handle("POST", CONTROL_PATH, headers, body)

        self.assertEqual(200, response.status)
        self.assertEqual([(CreatedTorrent("qb-hk", "a" * 40, "/downloads/Show", 123), "pause")], self.gateway.controls)

    def test_outbox_retries_with_backoff_without_reordering_events(self) -> None:
        clock = [1_700_000_000]
        attempts: list[str] = []

        def sender(event: dict, _event_id: str) -> None:
            attempts.append(event["type"])
            if len(attempts) == 1:
                raise RuntimeError("temporary failure")

        core = BridgeCore(
            instance_id="mp-main", hmac_key=self.key,
            database_path=Path(self.temp.name) / "backoff.sqlite3", gateway=self.gateway,
            coordinator_event_sender=sender, now=lambda: clock[0], retry_backoff_seconds=10,
        )
        body = json.dumps({
            "request_id": "request-backoff",
            "media": {"media_source": "tmdb", "media_id": "123"},
            "torrent": {"resource_ref": "torrent:example"},
            "downloader_policy": {"mode": "moviepilot_select"},
        }).encode()
        headers = signed_headers(self.key, "mp-main", "POST", INTENT_PATH, body, clock[0], "nonce-backoff")

        self.assertEqual(202, core.handle("POST", INTENT_PATH, headers, body).status)
        self.assertEqual(["intent.accepted"], attempts)
        self.assertEqual(0, core.flush_outbox())
        self.assertEqual(["intent.accepted"], attempts)

        clock[0] += 10
        self.assertEqual(2, core.flush_outbox())
        self.assertEqual(["intent.accepted", "intent.accepted", "torrent.bound"], attempts)
        self.assertEqual({"pending": 0, "acknowledged": 2}, {
            key: core.status_summary()[key] for key in ("pending", "acknowledged")
        })

    def test_restart_recovers_qb_created_before_binding_was_persisted(self) -> None:
        class CrashAfterCreateGateway(FakeGateway):
            def __init__(self) -> None:
                super().__init__()
                self.created_torrent = None
                self.create_calls = 0

            def create_download(self, intent: dict) -> CreatedTorrent:
                self.create_calls += 1
                self.created_torrent = CreatedTorrent("qb-hk", "d" * 40, "/downloads/Recovered", 456)
                raise SystemExit("simulated process termination after qB accepted the torrent")

            def recover_download(self, intent: dict):
                return self.created_torrent

        gateway = CrashAfterCreateGateway()
        callbacks = []
        database_path = Path(self.temp.name) / "restart-recovery.sqlite3"
        core = BridgeCore(
            instance_id="mp-main", hmac_key=self.key, database_path=database_path,
            gateway=gateway, coordinator_event_sender=lambda event, event_id: callbacks.append((event, event_id)),
            now=lambda: 1_700_000_000,
        )
        body = json.dumps({
            "request_id": "request-restart-recovery",
            "media": {"media_source": "tmdb", "media_id": "123"},
            "torrent": {"resource_ref": "torrent:example"},
            "downloader_policy": {"mode": "moviepilot_select"},
        }).encode()
        first_headers = signed_headers(self.key, "mp-main", "POST", INTENT_PATH, body, 1_700_000_000, "nonce-restart-1")
        with self.assertRaises(SystemExit):
            core.handle("POST", INTENT_PATH, first_headers, body)

        restarted = BridgeCore(
            instance_id="mp-main", hmac_key=self.key, database_path=database_path,
            gateway=gateway, coordinator_event_sender=lambda event, event_id: callbacks.append((event, event_id)),
            now=lambda: 1_700_000_000,
        )
        retry_headers = signed_headers(self.key, "mp-main", "POST", INTENT_PATH, body, 1_700_000_000, "nonce-restart-2")
        response = restarted.handle("POST", INTENT_PATH, retry_headers, body)

        self.assertEqual(202, response.status)
        self.assertEqual("bound", response.payload["status"])
        self.assertEqual(1, gateway.create_calls)
        self.assertEqual(["intent.accepted", "torrent.bound"], [event[0]["type"] for event in callbacks])
        self.assertEqual("d" * 40, callbacks[-1][0]["torrent"]["torrent_hash"])

    def test_tick_recovers_qb_binding_that_was_temporarily_invisible(self) -> None:
        class DelayedRecoveryGateway(FakeGateway):
            def __init__(self) -> None:
                super().__init__()
                self.recovered = None

            def create_download(self, intent: dict) -> CreatedTorrent:
                raise RuntimeError("qB accepted the request but binding lookup timed out")

            def recover_download(self, intent: dict):
                return self.recovered

        gateway = DelayedRecoveryGateway()
        callbacks = []
        core = BridgeCore(
            instance_id="mp-main", hmac_key=self.key,
            database_path=Path(self.temp.name) / "delayed-recovery.sqlite3",
            gateway=gateway, coordinator_event_sender=lambda event, event_id: callbacks.append((event, event_id)),
            now=lambda: 1_700_000_000,
        )
        body = json.dumps({
            "request_id": "request-delayed-recovery",
            "media": {"media_source": "tmdb", "media_id": "123"},
            "torrent": {"resource_ref": "torrent:example"},
            "downloader_policy": {"mode": "moviepilot_select"},
        }).encode()
        headers = signed_headers(self.key, "mp-main", "POST", INTENT_PATH, body, 1_700_000_000, "nonce-delayed-recovery")

        response = core.handle("POST", INTENT_PATH, headers, body)

        self.assertEqual("creating", response.payload["status"])
        self.assertEqual(["intent.accepted"], [event[0]["type"] for event in callbacks])
        gateway.recovered = CreatedTorrent("qb-hk", "e" * 40, "/downloads/Recovered", 789)
        self.assertEqual(1, core.recover_pending_intents())
        core.flush_outbox()
        self.assertEqual(["intent.accepted", "torrent.bound"], [event[0]["type"] for event in callbacks])

    def test_unresolved_creation_emits_failure_only_after_recovery_timeout(self) -> None:
        class UnresolvedGateway(FakeGateway):
            def create_download(self, intent: dict) -> CreatedTorrent:
                raise RuntimeError("uncertain create result")

        clock = [1_700_000_000]
        callbacks = []
        core = BridgeCore(
            instance_id="mp-main", hmac_key=self.key,
            database_path=Path(self.temp.name) / "recovery-timeout.sqlite3",
            gateway=UnresolvedGateway(), coordinator_event_sender=lambda event, event_id: callbacks.append((event, event_id)),
            now=lambda: clock[0], creation_recovery_timeout_seconds=60,
        )
        body = json.dumps({
            "request_id": "request-recovery-timeout",
            "media": {"media_source": "tmdb", "media_id": "123"},
            "torrent": {"resource_ref": "torrent:example"},
            "downloader_policy": {"mode": "moviepilot_select"},
        }).encode()
        headers = signed_headers(self.key, "mp-main", "POST", INTENT_PATH, body, clock[0], "nonce-recovery-timeout")
        self.assertEqual(202, core.handle("POST", INTENT_PATH, headers, body).status)

        clock[0] += 59
        self.assertEqual(0, core.recover_pending_intents())
        clock[0] += 1
        self.assertEqual(0, core.recover_pending_intents())
        core.flush_outbox()

        self.assertEqual(["intent.accepted", "torrent.failed"], [event[0]["type"] for event in callbacks])
        self.assertEqual("failed", core._intent_status("request-recovery-timeout"))


if __name__ == "__main__":
    unittest.main()
