# MoviePilot OpenList Bridge V1

## Purpose

The Bridge plugin is the control-plane adapter between OpenList and MoviePilot. It performs resource search, lets MoviePilot select the downloader, and reports the resolved qBittorrent downloader and hash. It never uploads files or receives qBittorrent credentials.

## Intent

Coordinator sends a signed `DownloadIntentRequest` to the plugin. The request must include `request_id`, complete `media_source` plus `media_id`, `downloader_policy.mode = moviepilot_select`, and the opaque `resource_ref` returned by Bridge search. Download enclosures, passkeys, tracker cookies, and direct torrent URLs are forbidden in this contract and stay inside MoviePilot.

## Events

The plugin sends a signed `BridgeEvent` to Coordinator. Every event has a stable `event_id` and `request_id`. Retries keep the same event ID and body, so Coordinator can consume them idempotently.

The `torrent.bound` event is the authoritative control-plane binding. Its `downloader`, `torrent_hash`, and absolute non-root `content_path` are required. Worker selection uses the `(bridge_instance_id, downloader)` route and never falls back to a different Worker.

## Signature

The signature input joins the following values with LF: protocol version, instance ID, HTTP method, request path, Unix timestamp, nonce, and SHA256 of the raw request body. The receiver accepts a five-minute timestamp window and rejects a reused `(instance_id, nonce)` pair.

The HTTP headers are `X-OpenList-Bridge-Version`,
`X-OpenList-Bridge-Instance`, `X-OpenList-Bridge-Timestamp`,
`X-OpenList-Bridge-Nonce`, and `X-OpenList-Bridge-Signature`. The signature is
the lowercase hexadecimal HMAC-SHA256 of that canonical string. Bridge
callbacks must use HTTPS; a trusted reverse proxy may forward
`X-Forwarded-Proto: https`.

## V1 endpoints

Coordinator posts the selected-resource intent to the configured Bridge base
URL at `/api/v1/plugin/OpenListBridge/intent`. The Bridge posts events to Coordinator at
`/api/v1/cluster/moviepilot/events`. Both sides sign the exact raw JSON body;
`request_id` is also sent as `X-OpenList-Request-ID` for idempotent retries.

Subscription resource search uses the Bridge base URL at
`/api/v1/plugin/OpenListBridge/search`. Intent status and pre-download
cancellation use `/api/v1/plugin/OpenListBridge/intent/{request_id}` and
`/api/v1/plugin/OpenListBridge/intent/{request_id}/cancel`. Exact pause/resume
control uses `/api/v1/plugin/OpenListBridge/control`; the request must match the
persisted request ID, downloader, and torrent hash. Search results contain only an opaque
`resource_ref`, title, site label, size, and seed/leech metadata. Binding a
result stores that opaque reference in `BoundTorrent`; OpenList does not ask
MoviePilot to create a subscription.

The Bridge persists intents, exact torrent bindings, replay nonces, and an
ordered callback outbox in SQLite. Reusing a `request_id` with a different
payload or changing an established downloader/hash/path binding is rejected.
Callback retries use bounded exponential backoff and never allow
`torrent.bound` to overtake `intent.accepted`.

Bridge secrets are stored through the existing encrypted `ClusterSecret`
store with kind `moviepilot_bridge_hmac`. API responses expose only the
configured flag and secret fingerprint, never the key.

## Compatibility

Unknown JSON fields must be ignored for forward compatibility. The plugin must not expose site cookies, qB passwords, qB URLs, download enclosures, or arbitrary host filesystem paths in search results, logs, or UI responses. The sole path exception is the absolute qB `content_path` in `torrent.bound`; it is required to bind the selected qB task and is never returned to an OpenList end user.
