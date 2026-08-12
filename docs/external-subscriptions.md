# External Subscription API

OpenList exposes an optional machine-to-machine subscription API for callers
such as `etflix_media_server`. The API is disabled by default and is separate
from the administrator subscription API.

## Configuration

The JSON configuration block is:

```json
{
  "external_subscription": {
    "enabled": true,
    "api_token": "replace-with-a-long-random-token",
    "allow_unauthenticated": false,
    "run_on_create": true
  }
}
```

The equivalent environment variables use the normal OpenList prefix:

```text
OPENLIST_EXTERNAL_SUBSCRIPTION_ENABLED=true
OPENLIST_EXTERNAL_SUBSCRIPTION_API_TOKEN=replace-with-a-long-random-token
OPENLIST_EXTERNAL_SUBSCRIPTION_ALLOW_UNAUTHENTICATED=false
OPENLIST_EXTERNAL_SUBSCRIPTION_RUN_ON_CREATE=true
```

When `api_token` is configured, callers must send either
`Authorization: Bearer <token>` or
`X-OpenList-Subscription-Token: <token>`. For a trusted private reverse proxy,
`allow_unauthenticated` may be set to `true` explicitly. An enabled API with
neither a token nor this opt-in is rejected as a configuration error.

## Endpoints

The same endpoints are available under both `/api/subscriptions` and
`/api/v1/subscriptions`.

### Create

```http
POST /api/subscriptions
Content-Type: application/json
Idempotency-Key: etflix-task-123
```

```json
{
  "name": "Series name",
  "media_type": "tv",
  "tmdb_id": 123456,
  "source_type": "telegram",
  "source_config": {},
  "seasons_selected": [1, 2],
  "episode_start": 1,
  "episode_end": 10
}
```

`media_type` must be `tv` or `movie`, and `tmdb_id` must be positive. A TV
request without `seasons_selected` defaults to season 1. The optional
`source_config._request` field is ignored so request metadata cannot leak into
the stored source configuration.

The response is a flat compatibility projection. Its important fields are
`id`, `last_status`, `last_message`, `progress_json`, `seasons_json`, and
`completed`. `subscription_id` is the same stable external ID used by the
other external endpoints; `internal_subscription_id` is provided for
diagnostics only.

Requests with the same idempotency key and normalized body are replayed. When
no idempotency key is provided, the normalized request fingerprint is used.
The lookup key `media_type + tmdb_id` is unique, so a different request for
the same media returns `409 Conflict` instead of creating a second internal
subscription.

`POST /api/subscriptions/manual` is also accepted for compatibility with the
OpenList ETF target client. It accepts `share_url`, `access_code`,
`share_type`, and `season_start`; when `name` is omitted, a stable `TMDB <id>`
name is generated and the share is stored as a manual source.

### Poll status

```http
GET /api/subscriptions?id=123
```

The path form `GET /api/subscriptions/123` is also supported. Status values
are `pending`, `running`, `completed`, and `failed`. The create endpoint only
accepts and schedules work; it does not wait for the media transfer to finish.

### Lookup

```http
GET /api/subscriptions/lookup?media_type=tv&tmdb_id=123456
```

A successful lookup returns `{"exists":true,...}` or
`{"exists":false}`. A miss is not an error.

### Trigger a check

```http
POST /api/subscriptions/123/check
POST /api/subscriptions/123/update
```

Both forms queue another run and return the same status projection. The
`/update` form is used by the public ETF target client; `/check` is retained
for the versionless compatibility path.
