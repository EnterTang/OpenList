# MoviePilot-qB PT 搬运 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (- [ ]) syntax for tracking.

**Goal:** 为 OpenList 订阅增加 MoviePilot V3 搜索与下载调度、qB 本地文件搬运、139 上传与 ETF、以及可配置的 PT 保种清理。

**Architecture:** MoviePilot Bridge 是独立 MoviePilot V3 插件，负责资源搜索、下载器选择、request_id 到 downloader 到 torrent_hash 的可靠绑定和签名回调。Coordinator 保存 intent、torrent 和文件子任务，按 downloader 严格绑定唯一 Worker；Worker 使用本机 qB WebUI，以 hash 读取并复制文件到 staging，再复用既有 139 上传 manifest 和 ETF materializer。

**Tech Stack:** Go、Gin、GORM、Redis Streams、qBittorrent WebUI API 5.0、MoviePilot V3 Plugin SDK、Python 3.12、pytest、HTTPS、HMAC-SHA256。

## Global Constraints

- MoviePilot V3 插件位于外部插件仓 plugins.v3/openlistbridge/，使用 _PluginBase 和稳定 app.sdk。
- 每个 Bridge 请求使用 HTTPS、实例独立 HMAC-SHA256、5 分钟时钟窗口和持久化 nonce 防重放。
- qB 原文件不可移动、改名、改扩展名、写入 AntiHash；只允许处理和删除 staging 副本。
- Worker 最大上传并发是 2，单文件 staging 上限是 150 GiB；容量控制使用低/高水位滞回。
- qB 任务只能派发到绑定 Worker；离线时等待，不使用 PreferredWorkerNodeID 回退。
- 一个 torrent 是保种父任务，一个待搬运文件是子任务；只有全部必需子任务和 ETF 成功后才可清理 torrent。
- Coordinator 写 ETF；Worker 只回传既有 UploadETFManifest。
- MoviePilot 不创建订阅、不执行整理；qB share limits 仅为保护上限，不代表最短保种规则。

---

## File Structure

| 路径 | 责任 |
|---|---|
| internal/model/moviepilot_transfer.go | Bridge 实例、intent、torrent binding、文件子任务、保种策略、事件 inbox/outbox |
| internal/db/moviepilot_transfer.go | 幂等 upsert、状态迁移、保种候选查询 |
| internal/moviepilotbridge/contract.go | V1 请求、事件、签名头、状态常量 |
| internal/moviepilotbridge/signing.go | HMAC 规范化、签名、验证和 nonce |
| internal/moviepilotbridge/client.go | Coordinator 主动访问 Bridge 的 HTTPS 客户端 |
| internal/moviepilotbridge/service.go | intent 投递、事件消费、严格 Worker 解析 |
| server/handles/moviepilot_bridge.go | Bridge 管理、资源绑定、回调 handler |
| server/middlewares/moviepilot_bridge.go | 回调 HMAC 认证与大小限制 |
| internal/subscription/moviepilot.go | MoviePilot 搜索、绑定、订阅 intent 创建 |
| internal/cluster/protocol/control.go | qB、路径映射和 staging Worker 配置 |
| internal/cluster/protocol/payloads.go | TorrentTaskContext 与 inventory 字段 |
| pkg/qbittorrent/client.go | hash 原生 qB 查询、文件、暂停、恢复、删除 |
| internal/cluster/worker/qb.go | qB 文件发现、路径验证、容量观测 |
| internal/cluster/worker/qb_staging.go | 原子 staging copy 和清理 |
| internal/cluster/worker/qb_watchdog.go | Worker lease 失联保护 |
| internal/cluster/coordinator/torrent_transfer.go | torrent 父子任务与严格派发 |
| internal/cluster/coordinator/torrent_retention.go | 保种评估与 qB 删除 |
| docs/operations/moviepilot-qb-pt-transfer.md | 部署、轮换、容量、恢复与上线检查 |
| <MoviePilot-Plugins>/plugins.v3/openlistbridge/ | MoviePilot V3 Bridge 源码 |
| <MoviePilot-Plugins>/tests/v3/openlistbridge/test_plugin.py | Bridge 插件测试 |
| <MoviePilot-Plugins>/package.v3.json | 插件索引与版本 |

## Task 1: 冻结 V1 Bridge 契约

**Files:**
- Create: internal/moviepilotbridge/contract.go
- Create: internal/moviepilotbridge/contract_test.go
- Create: docs/integrations/moviepilot-openlist-bridge-v1.md
- Create: docs/integrations/testdata/moviepilot-bridge/torrent-bound.json
- Create: docs/integrations/testdata/moviepilot-bridge/torrent-failed.json

**Interfaces:**
- Define DownloadIntentRequest, BridgeEvent, TorrentBoundPayload, CanonicalSignatureInput.
- Event type values: intent.accepted, torrent.bound, torrent.state_changed, torrent.failed, bridge.health_changed.
- IDs are UUID strings; torrent_hash is lower-case 40- or 64-hex.

- [ ] **Step 1: Write the failing contract test.**

~~~go
func TestTorrentBoundPayloadRequiresResolvedDownloaderAndHash(t *testing.T) {
    event := BridgeEvent{
        EventID: "2b7d5ff9-6c18-4f53-b7b7-3522e6f58ad7",
        Type: EventTorrentBound,
        RequestID: "9e63fcd0-dbc0-4cb5-bac4-1b3d727239c5",
        Torrent: &TorrentBoundPayload{
            Downloader: "qb-hk", TorrentHash: strings.Repeat("a", 40), ContentPath: "/downloads/a",
        },
    }
    require.NoError(t, event.Validate())
    event.Torrent.Downloader = ""
    require.EqualError(t, event.Validate(), "torrent.bound downloader is required")
}
~~~

- [ ] **Step 2: Run go test ./internal/moviepilotbridge -run TestTorrentBoundPayloadRequiresResolvedDownloaderAndHash -count=1. Expected: compile failure because the package does not exist.**

- [ ] **Step 3: Implement V1 validation.** Require media_source plus media_id whenever media is present. Reject fields named site_cookie, qb_password, qb_url, and local_path. Only torrent.bound carries content_path.

- [ ] **Step 4: Add fixed canonical fixtures.** The bound fixture uses downloader qb-hk, content_path /downloads/Show, media_source tmdb, media_id 123 and a 40-character hash.

- [ ] **Step 5: Verify and commit.**

~~~sh
go test ./internal/moviepilotbridge -count=1
git diff --check
git add internal/moviepilotbridge docs/integrations
git commit -m "feat(moviepilot): define bridge v1 contract"
~~~

## Task 2: 建立持久化模型与状态约束

**Files:**
- Create: internal/model/moviepilot_transfer.go
- Create: internal/db/moviepilot_transfer.go
- Create: internal/db/moviepilot_transfer_test.go
- Modify: internal/model/subscription.go
- Modify: internal/db/db.go
- Modify: internal/model/cluster_job.go

**Interfaces:**
- Add MoviePilotBridgeInstance, MoviePilotDownloadIntent, MoviePilotTorrentBinding, MoviePilotDeliveryFile, TorrentRetentionPolicy.
- Add CreateIntentTx, BindTorrentTx, ListRetentionCandidates.
- Add Subscription.BoundTorrent with bridge ID, opaque resource_ref, complete media identity, selected torrent fingerprint.

- [ ] **Step 1: Write the failing binding test.**

~~~go
func TestBindTorrentTxRejectsASecondWorkerForTheSameHash(t *testing.T) {
    intent := seedIntent(t, database, "request-1")
    hash := strings.Repeat("b", 40)
    require.NoError(t, BindTorrentTx(ctx, database, intent, "mp-main", "qb-hk", "worker-a", "qb-a", hash))
    err := BindTorrentTx(ctx, database, intent, "mp-main", "qb-hk", "worker-b", "qb-b", hash)
    require.ErrorContains(t, err, "torrent hash is already bound to worker-a")
}
~~~

- [ ] **Step 2: Run go test ./internal/db -run TestBindTorrentTxRejectsASecondWorkerForTheSameHash -count=1. Expected: missing models and repository symbols.**

- [ ] **Step 3: Implement model indexes and migration registration.** Register all models in db.Init AutoMigrate. Use unique indexes for bridge_instance_id plus torrent_hash, request_id, torrent_binding_id plus relative_path, and bridge_instance_id plus nonce.

- [ ] **Step 4: Implement transaction semantics.** Lock the intent; equal repeated events are no-ops; a second hash for request_id fails; a hash bound to another Worker fails; retention policy is snapshotted in the binding transaction.

- [ ] **Step 5: Add torrent.observe and torrent.transfer job types plus qb_observing, qb_copying, retention_check, qb_deleting stages. Retain existing media.transfer and ETF stages.**

- [ ] **Step 6: Verify and commit.**

~~~sh
go test ./internal/model ./internal/db -count=1
git diff --check
git add internal/model internal/db
git commit -m "feat(moviepilot): persist torrent bindings and delivery files"
~~~

## Task 3: 实现 HMAC、Bridge 管理和可靠事件投递

**Files:**
- Create: internal/moviepilotbridge/signing.go
- Create: internal/moviepilotbridge/signing_test.go
- Create: internal/moviepilotbridge/client.go
- Create: internal/moviepilotbridge/service.go
- Create: server/handles/moviepilot_bridge.go
- Create: server/middlewares/moviepilot_bridge.go
- Modify: server/router.go
- Modify: internal/cluster/control.go

**Interfaces:**
- Produce SignRequest, VerifyRequest, SubmitIntent, ConsumeBridgeEvent.
- Add admin routes below /api/admin/moviepilot_bridge and callback POST /api/v1/cluster/moviepilot/events.
- Reuse ClusterSecret with kind moviepilot_bridge_hmac; API reads expose only configured state and fingerprint.

- [ ] **Step 1: Write the failing replay test.**

~~~go
func TestVerifyRequestRejectsReplayedNonce(t *testing.T) {
    body := []byte("{\"event_id\":\"e-1\"}")
    headers := signedHeaders(t, key, "mp-main", body, now)
    require.NoError(t, verifier.Verify(ctx, headers, http.MethodPost, path, body))
    require.EqualError(t, verifier.Verify(ctx, headers, http.MethodPost, path, body), "bridge nonce has already been used")
}
~~~

- [ ] **Step 2: Run go test ./internal/moviepilotbridge -run TestVerifyRequestRejectsReplayedNonce -count=1. Expected: verifier is undefined.**

- [ ] **Step 3: Implement signing.** Canonical input is version, instance ID, method, path, Unix timestamp, nonce, and SHA256 raw-body hash separated by LF. Enforce 300 seconds, use constant-time comparison, and persist nonce before event consumption in one transaction.

- [ ] **Step 4: Implement instance CRUD and outbound client.** Require HTTPS Bridge URLs, persist an intent outbox before POST, sign the exact raw body, and set request_id as idempotency key.

- [ ] **Step 5: Implement inbox consumption.** event_id is unique. A torrent.bound resolves one healthy bridge_instance plus downloader route before BindTorrentTx. Zero or multiple routes store waiting_worker and return retryable acknowledgement.

- [ ] **Step 6: Add handler tests for invalid signature, expired timestamp, duplicate event, unknown instance, and successful bound callback.**

- [ ] **Step 7: Verify and commit.**

~~~sh
go test ./internal/moviepilotbridge ./server/handles ./server/middlewares -count=1
git add internal/moviepilotbridge internal/cluster/control.go server
git commit -m "feat(moviepilot): add signed bridge control plane"
~~~

## Task 4: 接入订阅搜索、资源绑定与 intent 创建

**Files:**
- Create: internal/subscription/moviepilot.go
- Create: internal/subscription/moviepilot_test.go
- Modify: internal/model/subscription.go
- Modify: internal/subscription/resource_search.go
- Modify: internal/subscription/binding.go
- Modify: server/handles/subscription.go
- Modify: server/router.go

**Interfaces:**
- Add source type moviepilot, SearchMoviePilotResources, BindMoviePilotResource.
- Add ExternalRef and BridgeInstanceID to SubscriptionResourceSearchResult.
- Add POST /admin/subscription/resource/bind_moviepilot.

- [ ] **Step 1: Write the failing projection test.**

~~~go
func TestSearchMoviePilotResourcesDoesNotExposeSiteCookie(t *testing.T) {
    projected := projectMoviePilotResult(bridgeSearchResult{
        ResourceRef: "r-1", Title: "Show S01E01", SiteCookie: "private",
    })
    require.Equal(t, "r-1", projected.ExternalRef)
    require.NotContains(t, mustJSON(t, projected), "private")
}
~~~

- [ ] **Step 2: Run go test ./internal/subscription -run TestSearchMoviePilotResourcesDoesNotExposeSiteCookie -count=1. Expected: projection helper is undefined.**

- [ ] **Step 3: Implement MoviePilot resource search.** Extend SearchResources normalization and source capability logic. Call Bridge search with media_source plus media_id and project only title, size, site label, seed/leech metadata and opaque resource_ref.

- [ ] **Step 4: Implement explicit torrent binding.** Validate Bridge and resource_ref, write BoundTorrent, and ensure torrent unbind does not clear BoundShare.

- [ ] **Step 5: Implement coordinator intent creation.** Its idempotency key is subscription_id plus resource_ref plus selected fingerprint; it sends media_source plus media_id and moviepilot_select to Bridge.

- [ ] **Step 6: Test duplicate binding, Bridge unavailable, rerun idempotency and current share-search behavior.**

- [ ] **Step 7: Verify and commit.**

~~~sh
go test ./internal/subscription ./server/handles -count=1
git add internal/subscription internal/model/subscription.go server/handles/subscription.go server/router.go
git commit -m "feat(subscription): add MoviePilot PT resource binding"
~~~

## Task 5: 扩展 Worker 配置、inventory 与严格 qB affinity

**Files:**
- Modify: internal/cluster/protocol/control.go
- Modify: internal/cluster/protocol/payloads.go
- Modify: internal/cluster/worker/control.go
- Modify: internal/cluster/worker/inventory.go
- Modify: internal/cluster/runtime_inventory_support.go
- Modify: internal/cluster/subscription_dispatcher.go
- Create: internal/cluster/worker/qb_config.go
- Create: internal/cluster/worker/qb_config_test.go

**Interfaces:**
- Add QBClientConfig, QBPathMapping, MoviePilotRoute, StagingConfig to WorkerDesiredConfig.
- Add TorrentTaskContext with BindingID, WorkerNodeID, QBClientID, TorrentHash, RelativePath to TaskContext.
- Produce ResolveTorrentWorker.

- [ ] **Step 1: Write the failing configuration test.**

~~~go
func TestWorkerDesiredConfigRejectsRouteWithoutPathMapping(t *testing.T) {
    cfg := protocol.WorkerDesiredConfig{
        QBClients: []protocol.QBClientConfig{{ID: "qb-a", WebUIURL: "http://127.0.0.1:8080"}},
        MoviePilotRoutes: []protocol.MoviePilotRoute{{BridgeInstanceID: "mp-main", Downloader: "qb-a", QBClientID: "qb-a"}},
    }
    require.EqualError(t, cfg.Validate(), "qB client \"qb-a\" requires at least one path mapping")
}
~~~

- [ ] **Step 2: Run go test ./internal/cluster/protocol ./internal/cluster/worker -run TestWorkerDesiredConfigRejectsRouteWithoutPathMapping -count=1. Expected: qB config types do not exist.**

- [ ] **Step 3: Implement config validation.** Require unique IDs, loopback HTTP or HTTPS WebUI URL, local secret reference, absolute non-root Worker paths, longest-prefix mappings, upload concurrency from 1 through 2, and maximum file size at most 150 GiB.

- [ ] **Step 4: Extend inventory without secrets.** Report bridge/downloader/qB-client tuple, root labels, free and active staging bytes, upload slots, qB health, and qb.copy. Exclude URL, credentials and local filesystem paths.

- [ ] **Step 5: Enforce strict selection.** Any TaskContext with Torrent only targets Torrent.WorkerNodeID. A disconnected bound Worker leaves waiting_worker. Existing share-source fallback remains unchanged.

- [ ] **Step 6: Test duplicate route rejection, alias collision, offline binding, and normal share fallback.**

- [ ] **Step 7: Verify and commit.**

~~~sh
go test ./internal/cluster/protocol ./internal/cluster/worker ./internal/cluster -count=1
git add internal/cluster/protocol internal/cluster/worker internal/cluster/runtime_inventory_support.go internal/cluster/subscription_dispatcher.go
git commit -m "feat(cluster): route qB torrents to bound workers"
~~~

## Task 6: 实现 hash 原生 qB 与 staging copy

**Files:**
- Modify: pkg/qbittorrent/client.go
- Modify: pkg/qbittorrent/client_test.go
- Create: internal/cluster/worker/qb.go
- Create: internal/cluster/worker/qb_test.go
- Create: internal/cluster/worker/qb_staging.go
- Create: internal/cluster/worker/qb_staging_test.go
- Modify: internal/cluster/worker/service.go
- Modify: internal/cluster/worker/service_test.go

**Interfaces:**
- Add GetTorrentByHash, GetFilesByHash, StartByHash, StopByHash, DeleteByHash.
- Produce DiscoverTorrentFiles and CopyQBFileToStaging.
- Refactor executeMediaTransfer through prepareMediaTransferSource.

- [ ] **Step 1: Write the failing qB fixture test.**

~~~go
func TestGetFilesByHashUsesTheQBTorrentHash(t *testing.T) {
    hash := strings.Repeat("c", 40)
    client, server := newQBFakeServer(t, fixtureTorrentInfo(hash))
    defer server.Close()
    files, err := client.GetFilesByHash(context.Background(), hash)
    require.NoError(t, err)
    require.Equal(t, "Season 01/E01.mkv", files[0].Name)
}
~~~

- [ ] **Step 2: Run go test ./pkg/qbittorrent -run TestGetFilesByHashUsesTheQBTorrentHash -count=1. Expected: method does not exist.**

- [ ] **Step 3: Implement hash methods.** Use qB torrents/info with hashes, files with hash, start, stop and delete. Preserve legacy tag-based calls for ordinary offline-download code.

- [ ] **Step 4: Write the failing staging boundary test.**

~~~go
func TestCopyQBFileToStagingRejectsPathEscape(t *testing.T) {
    _, err := CopyQBFileToStaging(ctx, QBSource{WorkerPath: "/mnt/downloads/../../etc/passwd"}, admission)
    require.EqualError(t, err, "qB source path escapes declared download root")
}
~~~

- [ ] **Step 5: Implement discovery and copy.** Match content_path by longest mapping prefix; require qB file progress 1, an allowed media extension, a regular file, and a source within mapped Worker root. Copy with io.Copy into a unique staging directory, fsync, then atomic rename. Cleanup only this staging directory.

- [ ] **Step 6: Integrate source selection.** Keep prepareMediaTransferShareSave for shares. Select prepareMediaTransferQBStaging for TaskContext.Torrent. Keep present 139 upload-plugin and UploadETFManifest flow.

- [ ] **Step 7: Test two-file concurrency, 150 GiB rejection, source inode/content unchanged, stage cleanup, and share regression.**

- [ ] **Step 8: Verify and commit.**

~~~sh
go test ./pkg/qbittorrent ./internal/cluster/worker -count=1
git add pkg/qbittorrent internal/cluster/worker
git commit -m "feat(worker): stage qB files without modifying sources"
~~~

## Task 7: 编排 torrent 父任务、文件子任务、进度与 ETF

**Files:**
- Create: internal/cluster/coordinator/torrent_transfer.go
- Create: internal/cluster/coordinator/torrent_transfer_test.go
- Modify: internal/cluster/coordinator/service.go
- Modify: internal/cluster/coordinator/materializer.go
- Modify: internal/subscription/progress.go
- Modify: internal/db/subscription.go
- Modify: server/handles/subscription.go

**Interfaces:**
- Produce ObserveTorrent, CreateDeliveryFiles, DispatchDeliveryFile, ReconcileTorrent.
- Each delivery file maps to one media.transfer job and one current manifest.

- [ ] **Step 1: Write the failing multi-file parent/child test.**

~~~go
func TestCreateDeliveryFilesKeepsTorrentUntilEveryRequiredFileIsMaterialized(t *testing.T) {
    parent := seedTorrentJob(t, db)
    files := []QBFile{{Name: "S01E01.mkv", Size: 1}, {Name: "S01E02.mkv", Size: 1}}
    require.NoError(t, service.CreateDeliveryFiles(ctx, parent, files))
    markDeliverySucceeded(t, db, parent.ID, "S01E01.mkv")
    require.False(t, service.CanEvaluateRetention(ctx, parent.ID))
}
~~~

- [ ] **Step 2: Run go test ./internal/cluster/coordinator -run TestCreateDeliveryFilesKeepsTorrentUntilEveryRequiredFileIsMaterialized -count=1. Expected: coordinator symbols are undefined.**

- [ ] **Step 3: Implement observation and child creation.** The bound Worker observes qB by hash; after completion list files once per list revision and create idempotent delivery rows and media.transfer jobs. Map recognized season/episode to SubscriptionItem. Save unmatched extras as skipped with a reason.

- [ ] **Step 4: Preserve ETF lifecycle.** A file is materialized only after ProcessPendingManifests consumes its existing manifest. No Worker writes ETF.

- [ ] **Step 5: Extend existing status views.** Add read-only torrent_status, download_progress, upload_progress, seed_elapsed, retention_status. Keep transferred tied to ETF materialization.

- [ ] **Step 6: Test two episodes, skipped NFO, failed upload, duplicate manifest, and non-PT board output.**

- [ ] **Step 7: Verify and commit.**

~~~sh
go test ./internal/cluster/coordinator ./internal/subscription ./server/handles -count=1
git add internal/cluster/coordinator internal/subscription internal/db/subscription.go server/handles/subscription.go
git commit -m "feat(subscription): track torrent delivery children"
~~~

## Task 8: 实现保种、容量水位和 Worker 离线保护

**Files:**
- Create: internal/cluster/coordinator/torrent_retention.go
- Create: internal/cluster/coordinator/torrent_retention_test.go
- Create: internal/cluster/worker/qb_watchdog.go
- Create: internal/cluster/worker/qb_watchdog_test.go
- Modify: internal/bootstrap/run.go
- Modify: internal/conf/config.go
- Create: docs/operations/moviepilot-qb-pt-transfer.md

**Interfaces:**
- Produce RetentionSatisfied, RunTorrentRetention, WorkerLeaseWatchdog.Run.
- Policy fields: min_seed_seconds, min_ratio, site_rule_id, manual_hold_until, permanent.

- [ ] **Step 1: Write the failing retention test.**

~~~go
func TestRetentionSatisfiedRequiresAllEnabledMinimums(t *testing.T) {
    policy := TorrentRetentionPolicy{MinSeedSeconds: 3600, MinRatio: 1.0}
    info := QBTorrentInfo{SeedingTimeSeconds: 3600, Ratio: 0.8}
    require.False(t, RetentionSatisfied(policy, info, allFilesMaterialized, now))
    info.Ratio = 1.0
    require.True(t, RetentionSatisfied(policy, info, allFilesMaterialized, now))
}
~~~

- [ ] **Step 2: Run go test ./internal/cluster/coordinator -run TestRetentionSatisfiedRequiresAllEnabledMinimums -count=1. Expected: evaluator is undefined.**

- [ ] **Step 3: Implement conservative policy evaluation.** Permanent and active manual hold deny deletion. Disabled thresholds do not constrain deletion. An unknown configured H&R rule gives manual_review. Only all required files with consumed manifests may be evaluated.

- [ ] **Step 4: Implement qB deletion.** Persist DELETING, StopByHash, DeleteByHash(hash, true), then set DELETED only after qB no longer returns the hash. An already-absent hash on retry is successful.

- [ ] **Step 5: Implement watermarks and watchdog.** At low water pause only incomplete OpenList-managed torrents and resume at high water. Persist watchdog-paused hashes in ClusterWorkerObservedState. On lease loss pause only incomplete hashes; on recovery resume only that stored set. Never pause completed seeders for temporary Coordinator loss.

- [ ] **Step 6: Test H&R manual review, permanent seed, manual extension, hysteresis, lease loss, and seeder immunity.**

- [ ] **Step 7: Verify and commit.**

~~~sh
go test ./internal/cluster/coordinator ./internal/cluster/worker ./internal/conf -count=1
git add internal/cluster/coordinator internal/cluster/worker internal/bootstrap/run.go internal/conf docs/operations
git commit -m "feat(pt): enforce retention and worker lease safeguards"
~~~

## Task 9: 实现 MoviePilot V3 Bridge 插件（外部仓库）

**Files:**
- Create: <MoviePilot-Plugins>/plugins.v3/openlistbridge/__init__.py
- Create: <MoviePilot-Plugins>/plugins.v3/openlistbridge/bridge_client.py
- Create: <MoviePilot-Plugins>/plugins.v3/openlistbridge/models.py
- Create: <MoviePilot-Plugins>/plugins.v3/openlistbridge/outbox.py
- Create: <MoviePilot-Plugins>/plugins.v3/openlistbridge/README.md
- Create: <MoviePilot-Plugins>/tests/v3/openlistbridge/test_plugin.py
- Modify: <MoviePilot-Plugins>/package.v3.json

**Interfaces:**
- Expose search, intent create/status/cancel with _PluginBase.get_api().
- Persist request_id, downloader, torrent_hash and signed V1 callbacks.

- [ ] **Step 1: Write the failing plugin contract test.**

~~~python
def test_bound_event_uses_moviepilot_resolved_downloader(plugin, fake_download_service):
    fake_download_service.add.return_value = {"success": True}
    plugin.accept_intent(intent("request-1"))
    event = plugin.outbox_events()[0]
    assert event["type"] == "torrent.bound"
    assert event["torrent"]["downloader"] == "qb-hk"
    assert len(event["torrent"]["torrent_hash"]) in (40, 64)
~~~

- [ ] **Step 2: Run ../MoviePilot/.venv/bin/python -m pytest tests/v3/openlistbridge -q. Expected: import failure before plugin exists.**

- [ ] **Step 3: Implement the skeleton.** Implement init_plugin, get_state, get_api, get_form, get_page, stop_service. Use app.sdk only, start no thread during import, and create no independent Python environment.

- [ ] **Step 4: Implement search and intent APIs.** Search returns an opaque resource_ref. Intent creation uses MoviePilot download services to select a downloader and create the torrent; it queries task state until downloader and hash exist, then emits torrent.bound.

- [ ] **Step 5: Implement signed plugin outbox.** Persist event ID, raw body, retry count, next attempt and acknowledgement in plugin data storage. A retry keeps event ID and body but uses a new nonce and timestamp.

- [ ] **Step 6: Implement configuration and status page.** Fields are enabled, Coordinator HTTPS URL, instance ID, write-only HMAC key, timeout, retry backoff, health and redacted outbox summary. Do not show qB URL/password, site cookie, tracker URL or key.

- [ ] **Step 7: Verify and commit externally.**

~~~sh
../MoviePilot/.venv/bin/python -m compileall plugins.v3/openlistbridge
../MoviePilot/.venv/bin/python -m pytest tests/v3/openlistbridge -q
../MoviePilot/.venv/bin/python .github/scripts/check_plugin_versions.py package.json package.v2.json package.v3.json
git diff --check
git add plugins.v3/openlistbridge tests/v3/openlistbridge package.v3.json
git commit -m "feat(openlistbridge): relay MoviePilot downloads to OpenList"
~~~

- [ ] **Step 8: Load a real MoviePilot V3 host.** Verify install, enable, reload, disable cleanup, one-time API registration, search, torrent binding and signed retry. Record tested MoviePilot version in README.

## Task 10: 端到端测试、迁移与上线运行手册

**Files:**
- Create: internal/integration/moviepilot_qb_transfer_test.go
- Modify: docs/operations/moviepilot-qb-pt-transfer.md
- Modify: docs/cluster.md
- Modify: docs/external-subscriptions.md

**Interfaces:**
- Consumes Tasks 1 through 9.
- Produces repeatable local integration coverage and production rollout checklist.

- [ ] **Step 1: Write the failing end-to-end test.**

~~~go
func TestMoviePilotTorrentWithTwoEpisodesTransfersThenSeeds(t *testing.T) {
    env := newMoviePilotQBFakeEnvironment(t)
    env.Bind("mp-main", "qb-hk", "worker-hk")
    env.AcceptIntent("request-1", torrentWith("S01E01.mkv", "S01E02.mkv"))
    env.CompleteTorrent()
    env.MaterializeAllFiles()
    require.Equal(t, TorrentStateSeeding, env.TorrentState())
    require.False(t, env.QBDeleted())
}
~~~

- [ ] **Step 2: Run go test ./internal/integration -run TestMoviePilotTorrentWithTwoEpisodesTransfersThenSeeds -count=1. Expected: harness failure before complete wiring.**

- [ ] **Step 3: Build fake HTTPS Bridge, qB WebUI and 139 target.** Assert HMAC, exact Worker selection, unchanged qB source, two manifests, Coordinator-only ETF materialization, and delayed deletion after policy success.

- [ ] **Step 4: Add failure coverage.** Duplicate callback, Bridge outage after qB creation, Worker offline, staging exhaustion, partial file failure, unknown H&R, expired signature and qB already deleted during retry.

- [ ] **Step 5: Write operations runbook.** Include certificates, secret rotation, Worker path mappings, minimum disk calculation 2 times 150 GiB plus safety reserve, ETF root prerequisite, health checks, recovery commands and sidecar watchdog deployment.

- [ ] **Step 6: Run release verification.**

~~~sh
go test ./internal/moviepilotbridge ./internal/subscription ./internal/cluster/... ./pkg/qbittorrent ./internal/integration -count=1
go test ./...
git diff --check
~~~

- [ ] **Step 7: Stage a real acceptance test.** Use MoviePilot V3, a non-production two-file PT source, one qB Worker and one 139 target. Verify binding, event delivery, source copy, final ETF, seed policy and deletion only after test policy fulfillment.

- [ ] **Step 8: Commit integration coverage and docs.**

~~~sh
git add internal/integration docs
git commit -m "test(pt): cover MoviePilot qB transfer workflow"
~~~

## Dependency Order and Review Gates

1. Task 1 is the contract gate; no Bridge or Coordinator implementation starts before fixture tests pass.
2. Tasks 2 and 3 establish durable control-plane state. Task 9 may start after Task 1 but cannot integrate before Task 3.
3. Tasks 5 and 6 establish Worker safety and must pass before any live qB access.
4. Task 7 preserves existing media.transfer manifest behavior before Task 8 enables cleanup.
5. Task 8 begins in observe-only mode and cannot delete qB data until Task 10 passes.
6. Task 10 is the production-release gate. Any signature, strict-affinity, source-immutability, ETF, or retention failure blocks rollout.

## Plan Self-Review

- Spec coverage: Bridge plugin, search/binding, download notifications, multi-file transfer, 139 upload, AntiHash/ISO, Coordinator ETF, retention, capacity, Worker loss and strict routing map to Tasks 1 through 10.
- Compatibility: legacy share subscriptions and tag-based offline qB downloads remain unchanged; Task 6 adds hash methods rather than replacing tag methods.
- Security: the plan uses per-instance HMAC, replay protection, write-only secrets and redacted configuration/inventory.
- Scope: this plan changes OpenList and the named MoviePilot V3 plugin repository only; qB itself and the 139 driver protocol are not modified.
