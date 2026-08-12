# Cloudpan Share Subscription Verification

Date: 2026-07-07

## Provider Matrix

| Provider | Example URL | Credential required for save | Covered behavior |
| --- | --- | --- | --- |
| Quark | `https://pan.quark.cn/s/bc18e4ea5fb8` | `cookie` | URL detection, share token request, detail listing, save request, task polling, subscription runtime save gating |
| Aliyun Drive | `https://www.alipan.com/s/odeXVKsEKxr` | Web `refresh_token`, or `access_token` with target `drive_id` derived from the configured `temp_transfer_root` AliyundriveOpen mount | URL detection, access token refresh, share token request, share listing, batch copy, subscription runtime save gating |
| 123Pan | `https://www.123pan.com/s/7Tx1jv-pVu7v?pwd=xoxo#` | `access_token` | URL detection, `pwd` extraction, share listing, file save through upload request, subscription runtime save gating |
| 115Pan | `https://115cdn.com/s/swssal13zrk?password=t58d` | `cookie` | URL detection, `password` extraction, share snapshot listing, receive/save request, subscription runtime save gating |

## Automated Verification

Passed:

```bash
go test ./internal/subscription -run 'TestTrySaveShareLinkToTemp|TestRunManualShareProviderSavesTempRoot|TestParseShareURL|TestDetectShareProvider|TestRowLinksForTelegramPanSources'
go test ./internal/subscription
go test ./server/handles ./internal/subscription
```

Full repository run:

```bash
go test ./...
```

Result: failed on existing repository/environment issues outside this change:

- `drivers/189`, `drivers/chaoxing`, `drivers/google_drive`, `drivers/google_photo`, `drivers/lanzou`, `internal/offline_download/115`, `internal/offline_download/115_open`, and `internal/offline_download/pikpak`: existing vet/build failures from non-constant format strings.
- `internal/fuse`: local environment is missing `fuse.h`.
- `internal/net`: `TestNewOSSClientUsesEnvironmentHTTPSProxy` expected `*http.Transport`, got `*net.safeTransport`.
- `pkg/aria2/rpc`: local aria2 RPC service on `localhost:6800` was not running.

## Manual Smoke Checklist

For each provider:

1. Configure global subscription Telegram provider credentials and `temp_transfer_root`. For Aliyun Drive, use the Web refresh token; if only an access token is configured, `temp_transfer_root` must resolve to an initialized AliyundriveOpen mount so the target drive ID can be derived from that storage.
2. Configure either provider-specific Telegram channels or a manual subscription link.
3. Run preview/check with transfer disabled.
4. Confirm matching media files are saved into the temp root and appear as pending subscription items.
5. Run check with transfer enabled.
6. Confirm target files are copied/renamed through existing subscription transfer logic.
7. If `delete_source_after` is enabled, confirm the temp source file is removed only after successful transfer.

## Known Limits

- Provider APIs are private and may change; request-shape tests cover the current expected shape.
- 123Pan directory save is not implemented in the minimal provider; file save is covered.
- The repo does not include editable frontend source, so this change exposes backend JSON fields and API behavior only.
