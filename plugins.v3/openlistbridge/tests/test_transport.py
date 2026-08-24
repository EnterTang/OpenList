import json
import unittest

from openlistbridge.transport import CoordinatorEventSender


class CapturingOpener:
    def __init__(self) -> None:
        self.request = None

    def __call__(self, request, timeout):
        self.request = request
        self.timeout = timeout

        class Response:
            status = 204

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, traceback):
                return False

        return Response()


class CoordinatorEventSenderTest(unittest.TestCase):
    def test_sends_exact_event_body_with_bridge_signature(self) -> None:
        opener = CapturingOpener()
        sender = CoordinatorEventSender(
            coordinator_url="https://openlist.example",
            instance_id="mp-main",
            hmac_key=b"0123456789abcdef0123456789abcdef",
            opener=opener,
            now=lambda: 1_700_000_000,
            nonce_factory=lambda: "nonce-1",
            timeout_seconds=7,
        )
        event = {"event_id": "event-1", "request_id": "request-1", "type": "intent.accepted"}

        sender(event, "event-1")

        self.assertEqual("https://openlist.example/api/v1/cluster/moviepilot/events", opener.request.full_url)
        self.assertEqual(json.dumps(event, separators=(",", ":")).encode(), opener.request.data)
        self.assertEqual("mp-main", opener.request.headers["X-openlist-bridge-instance"])
        self.assertTrue(opener.request.headers["X-openlist-bridge-signature"])
        self.assertEqual(7, opener.timeout)

    def test_rejects_ambiguous_or_credential_bearing_coordinator_urls(self) -> None:
        for coordinator_url in (
            "http://openlist.example",
            "https://user:password@openlist.example",
            "https://openlist.example?token=secret",
            "https://openlist.example#fragment",
            "https://",
        ):
            with self.subTest(coordinator_url=coordinator_url):
                with self.assertRaises(ValueError):
                    CoordinatorEventSender(
                        coordinator_url=coordinator_url,
                        instance_id="mp-main",
                        hmac_key=b"0123456789abcdef0123456789abcdef",
                    )


if __name__ == "__main__":
    unittest.main()
