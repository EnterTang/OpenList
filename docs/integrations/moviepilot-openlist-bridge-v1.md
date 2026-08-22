# MoviePilot OpenList Bridge V1

## Purpose

The Bridge plugin is the control-plane adapter between OpenList and MoviePilot. It performs resource search, lets MoviePilot select the downloader, and reports the resolved qBittorrent downloader and hash. It never uploads files or receives qBittorrent credentials.

## Intent

Coordinator sends a signed `DownloadIntentRequest` to the plugin. The request must include `request_id`, complete `media_source` plus `media_id`, and `downloader_policy.mode = moviepilot_select`. The torrent may be represented by an opaque `resource_ref`; tracker cookies stay inside MoviePilot.

## Events

The plugin sends a signed `BridgeEvent` to Coordinator. Every event has a stable `event_id` and `request_id`. Retries keep the same event ID and body, so Coordinator can consume them idempotently.

The `torrent.bound` event is the authoritative control-plane binding. Its `downloader`, `torrent_hash`, and absolute non-root `content_path` are required. Worker selection uses the `(bridge_instance_id, downloader)` route and never falls back to a different Worker.

## Signature

The signature input joins the following values with LF: protocol version, instance ID, HTTP method, request path, Unix timestamp, nonce, and SHA256 of the raw request body. The receiver accepts a five-minute timestamp window and rejects a reused `(instance_id, nonce)` pair.

## Compatibility

Unknown JSON fields must be ignored for forward compatibility. The plugin must not expose site cookies, qB passwords, qB URLs, or host filesystem paths in search results, events, logs, or UI responses.
