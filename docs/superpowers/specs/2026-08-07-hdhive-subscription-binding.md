# HDHive Subscription Binding

HDHive is available as the automatic subscription source value `hdhive`.
Unlike the legacy single-source `telegram` and `pansou` paths, an HDHive
subscription is a federated entry point that uses the following order:

1. Process the subscription's bound cloud share.
2. Resolve HDHive resources that are already shared or free for the user.
3. Re-run configured Telegram and PanSou searches.
4. Resolve a paid HDHive resource only when both regular sources produced no
   usable candidate and the unlock-point value is known and within the limit.

Telegram and PanSou are still retried on every HDHive-source run when they are
configured. Free HDHive resources may also be retried on every run. Unknown
unlock-point metadata is treated as unsafe and is never unlocked automatically.
The free HDHive phase runs before those regular-source retries; paid HDHive
unlock remains the final fallback and is skipped when a bound, free, Telegram,
or PanSou candidate is already available.

## Binding API

After a user explicitly unlocks an HDHive resource through the existing
`POST /admin/subscription/resource/unlock` endpoint, the returned cloud share
can be attached to a subscription with:

```http
POST /admin/subscription/resource/bind
Content-Type: application/json
```

```json
{
  "subscription_id": 123,
  "source_type": "hdhive",
  "resource_url": "https://hdhive.com/resource/115/<slug>",
  "share_url": "https://115.com/s/<share>",
  "access_code": "abcd",
  "provider": "pan115",
  "requires_unlock": true,
  "unlock_points": 3
}
```

This endpoint only validates and persists the resolved cloud share; it never
calls HDHive unlock. `POST /admin/subscription/resource/unbind` clears the
binding. A successful automatic paid unlock also persists the returned share,
so the same HDHive resource is reused instead of being charged again on the
next scheduled run.

The stored binding is consumed by the existing provider-specific share
inspection and transfer pipeline. Therefore the configured 123/115/GuangYaPan
source credentials and the mobile delivery target continue to control the
actual transfer destination.

## Cluster behavior

Cluster runs dispatch bound, free, and eligible paid shares as inspect
observations. A configured regular-source dispatch is considered a candidate
for the paid-unlock guard. A regular-source failure is not treated as proof of
absence, so the coordinator does not spend HDHive points while source
availability is uncertain.
