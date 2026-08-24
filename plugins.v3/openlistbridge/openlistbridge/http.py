"""WSGI adapter that a MoviePilot plugin route can mount without extra deps."""

import json
from typing import Callable, Iterable

from .core import BridgeCore


class BridgeWSGIApp:
    """Expose the Bridge core as a small, framework-neutral HTTP application."""

    max_body_bytes = 4 << 20

    def __init__(self, core: BridgeCore) -> None:
        self.core = core

    def __call__(self, environ: dict, start_response: Callable) -> Iterable[bytes]:
        try:
            content_length = int(environ.get("CONTENT_LENGTH") or 0)
        except ValueError:
            return self._respond(start_response, 400, {"error": "invalid content length"})
        if content_length < 0 or content_length > self.max_body_bytes:
            return self._respond(start_response, 413, {"error": "request body is too large"})
        body = environ["wsgi.input"].read(content_length)
        if len(body) != content_length:
            return self._respond(start_response, 400, {"error": "request body is incomplete"})
        path = environ.get("PATH_INFO", "")
        if environ.get("QUERY_STRING"):
            path += "?" + environ["QUERY_STRING"]
        headers = {
            key[5:].replace("_", "-"): value
            for key, value in environ.items()
            if key.startswith("HTTP_")
        }
        result = self.core.handle(environ.get("REQUEST_METHOD", ""), path, headers, body)
        return self._respond(start_response, result.status, result.payload)

    @staticmethod
    def _respond(start_response: Callable, status: int, payload: dict) -> Iterable[bytes]:
        body = json.dumps(payload, separators=(",", ":")).encode()
        labels = {200: "OK", 202: "Accepted", 400: "Bad Request", 401: "Unauthorized", 404: "Not Found", 409: "Conflict", 413: "Payload Too Large", 502: "Bad Gateway"}
        start_response("%d %s" % (status, labels.get(status, "Error")), [("Content-Type", "application/json"), ("Content-Length", str(len(body)))])
        return [body]
