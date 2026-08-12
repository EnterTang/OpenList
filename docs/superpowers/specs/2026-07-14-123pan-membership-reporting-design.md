# 123Pan Membership Reporting Design

## Context

The 123Pan driver already calls `GET https://yun.123pan.com/b/api/user/info`
during initialization and when collecting storage details. The request uses the
driver's existing bearer access token, `App-Version: 3`, `platform: web`, and the
existing dynamic URL signature. The response also contains membership fields,
but the current response type only retains account and capacity data.

The same bearer-token request was verified against the current Web endpoint,
`GET https://api.123278.com/api/user/info`, without a `LoginUuid` header. Both
endpoints returned the same membership and capacity values. This design keeps
the existing driver endpoint to minimize authentication and compatibility risk.

## Goals

- Retrieve the current 123Pan membership status, tier, and expiration date from
  the existing user-info response.
- Expose the membership snapshot in the storage-details API.
- Include the membership snapshot in cluster provider-account inventory.
- Reuse the current driver login parameters, access-token refresh, request
  signing, and storage-details cache.
- Preserve manually configured membership-tier overrides.

## Non-goals

- Do not switch the driver to the `api.123278.com` endpoint.
- Do not add `LoginUuid`, cookies, or other Web-only credentials.
- Do not persist automatically detected membership data into storage addition
  configuration.
- Do not infer a precise expiration timestamp from a date-only API field.
- Do not change account selection or upload-limit policies beyond supplying the
  already supported runtime membership tier.

## Data Model

Add a reusable optional membership structure to storage details:

```go
type MembershipDetails struct {
	Tier       string `json:"tier,omitempty"`
	Status     string `json:"status,omitempty"`
	ExpireDate string `json:"expire_date,omitempty"`
}
```

`StorageDetails` gains an optional `Membership *MembershipDetails` field. Its
JSON representation remains backward compatible:

```json
{
  "total_space": 38311108280320,
  "used_space": 25831930334543,
  "free_space": 12479177945777,
  "membership": {
    "tier": "vip",
    "status": "active",
    "expire_date": "2040-01-31"
  }
}
```

When membership is unavailable, the `membership` field is omitted. Existing
disk-usage keys and their numeric semantics remain unchanged.

Extend `ProviderAccountInventory` with optional string fields:

```go
MembershipStatus     string `json:"membership_status,omitempty"`
MembershipExpireDate string `json:"membership_expire_date,omitempty"`
```

The existing `membership_tier` and `membership_weight` fields remain unchanged.

## 123Pan Mapping

Extend the existing 123Pan user-info response model with:

- `Vip bool`
- `VipLevel int`
- `VipExpire string`

Map the response conservatively:

| API state | Tier | Status | Expiration |
| --- | --- | --- | --- |
| `Vip == false` | `ordinary` | `inactive` | Preserve non-empty `VipExpire` |
| `Vip == true && VipLevel == 2` | `svip` | `active` | Preserve `VipExpire` |
| `Vip == true` with any other level | `vip` | `active` | Preserve `VipExpire` |

The driver does not compare `VipExpire` with the local clock. The upstream
`Vip` flag is authoritative for current status, avoiding timezone assumptions
around the date-only expiration value.

## Driver Runtime State

The driver stores the latest membership snapshot in memory behind a read/write
mutex. The snapshot is refreshed from the same user-info response in two paths:

1. Driver initialization, which already requires a successful user-info query.
2. `GetDetails`, which already refreshes user and capacity information and is
   cached by OpenList's storage-details cache.

The driver exposes:

- `ClusterMembershipTier() string` for existing subscription and cluster tier
  consumers.
- A membership-details reporter returning a copy of the full runtime snapshot
  for cluster inventory hydration.

An explicit configured tier other than `unknown` remains authoritative for the
tier. Runtime status and expiration still come from the user-info response.

## Storage-details Flow

`GetDetails` performs its existing user-info request, updates the runtime
snapshot, and returns both disk usage and membership details. The generic
storage-details JSON encoding must explicitly retain the existing disk-usage
keys because `DiskUsage` currently defines custom JSON marshaling.

The existing 30-minute storage-details cache applies to membership data as part
of the same object. Explicit storage-detail refreshes continue to invalidate and
repopulate the complete snapshot.

## Cluster Inventory Flow

During worker inventory hydration:

1. Read configured membership metadata as today.
2. If the driver reports runtime membership details, copy status and expiration
   into the provider account.
3. Use the runtime tier only when the configured tier is empty or `unknown`.
4. Recalculate membership weight from the selected tier.
5. Retain the existing tier-only reporter fallback for other drivers.

The new protocol fields are optional, so coordinators and persisted inventory
created by older workers remain decodable.

## Error Handling

- User-info request failures keep the existing behavior: initialization or
  storage-detail retrieval returns the request error.
- Authentication expiry continues through the driver's existing single retry
  and login refresh path.
- Missing membership fields produce an `ordinary`/`inactive` snapshot according
  to Go zero values only after a successful response; request failures never
  masquerade as non-member accounts.
- Cluster inventory does not make an additional remote membership request.

## Compatibility

- No new driver settings or credentials are required.
- Existing 123Pan storage configurations remain valid.
- Storage-detail clients continue receiving the existing disk-usage fields.
- The new storage-detail and cluster fields are additive and optional.
- Other storage drivers are unchanged unless they later opt into the reusable
  membership structure.

## Test Strategy

Follow a red-green test sequence covering:

1. 123Pan user-info JSON decoding retains membership fields.
2. Membership normalization handles ordinary, VIP, and SVIP responses.
3. `GetDetails` returns capacity and membership from one user-info request.
4. Storage-details JSON remains unchanged when membership is absent and includes
   the optional membership object when present.
5. Runtime membership tier honors an explicit configured override.
6. Worker inventory adds status and expiration, uses runtime tier for `unknown`,
   and preserves an explicit configured tier.
7. Existing 123Pan driver and cluster worker tests continue to pass.

## Implementation Scope

Expected production files:

- `internal/model/storage.go`
- `drivers/123/types.go`
- `drivers/123/driver.go`
- `internal/cluster/protocol/payloads.go`
- `internal/cluster/worker/provider_inventory.go`

Focused tests will be added next to the affected model, driver, and worker code.
No dependency or database migration is required.
