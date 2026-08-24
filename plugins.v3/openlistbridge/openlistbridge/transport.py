"""Signed callback transport from the Bridge plugin to OpenList."""

import hashlib
import hmac
import json
import time
import uuid
from typing import Callable, Optional
from urllib import request
from urllib.parse import urlsplit

from .core import (
    HEADER_INSTANCE,
    HEADER_NONCE,
    HEADER_SIGNATURE,
    HEADER_TIMESTAMP,
    HEADER_VERSION,
    SIGNATURE_VERSION,
)


class CoordinatorEventSender:
    """Posts durable outbox events using the exact V1 bridge signature."""

    def __init__(
        self,
        *,
        coordinator_url: str,
        instance_id: str,
        hmac_key: bytes,
        opener: Callable = request.urlopen,
        now: Optional[Callable[[], int]] = None,
        nonce_factory: Optional[Callable[[], str]] = None,
        timeout_seconds: int = 15,
    ) -> None:
        coordinator_url = coordinator_url.strip()
        try:
            parsed_url = urlsplit(coordinator_url)
            port = parsed_url.port
        except ValueError as exc:
            raise ValueError("Coordinator callback URL is invalid") from exc
        if (
            parsed_url.scheme.lower() != "https"
            or not parsed_url.hostname
            or parsed_url.username is not None
            or parsed_url.password is not None
            or parsed_url.query
            or parsed_url.fragment
            or port is not None and not 0 < port <= 65535
        ):
            raise ValueError("Coordinator callback URL must be an unambiguous HTTPS URL without credentials")
        self.coordinator_url = coordinator_url.rstrip("/")
        self.instance_id = instance_id
        self.hmac_key = hmac_key
        self.opener = opener
        self.now = now or (lambda: int(time.time()))
        self.nonce_factory = nonce_factory or (lambda: str(uuid.uuid4()))
        self.timeout_seconds = max(1, min(int(timeout_seconds), 120))

    def __call__(self, event: dict, event_id: str) -> None:
        if event.get("event_id") != event_id:
            raise ValueError("outbox event id does not match the callback payload")
        path = "/api/v1/cluster/moviepilot/events"
        body = json.dumps(event, separators=(",", ":")).encode()
        timestamp = self.now()
        nonce = self.nonce_factory()
        canonical = "\n".join((
            SIGNATURE_VERSION,
            self.instance_id,
            "POST",
            path,
            str(timestamp),
            nonce,
            hashlib.sha256(body).hexdigest(),
        )).encode()
        signature = hmac.new(self.hmac_key, canonical, hashlib.sha256).hexdigest()
        headers = {
            "Content-Type": "application/json",
            "X-OpenList-Request-ID": event.get("request_id", ""),
            HEADER_VERSION: SIGNATURE_VERSION,
            HEADER_INSTANCE: self.instance_id,
            HEADER_TIMESTAMP: str(timestamp),
            HEADER_NONCE: nonce,
            HEADER_SIGNATURE: signature,
        }
        outbound = request.Request(self.coordinator_url + path, data=body, headers=headers, method="POST")
        with self.opener(outbound, timeout=self.timeout_seconds) as response:
            status = getattr(response, "status", None)
            if status is None:
                status = response.getcode()
            if status < 200 or status >= 300:
                raise RuntimeError("OpenList Coordinator returned HTTP %s" % status)
