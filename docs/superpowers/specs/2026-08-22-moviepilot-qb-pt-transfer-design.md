# MoviePilot、qBittorrent 与集群 PT 搬运设计

## 目标

在不改变 qBittorrent 原始下载文件、不启用 MoviePilot 整理的前提下，让 OpenList 订阅框架能够：

- 通过 MoviePilot 搜索资源并由其选择下载器；
- 在多个独立网络环境的 MoviePilot、qBittorrent、OpenList Worker 之间可靠关联任务；
- 将一个种子内的多个媒体文件分别搬运到移动 139 网盘；
- 在 Worker 端复制到 staging 后执行现有 AntiHash、ISO Rename 和上传；
- 保持现有集群上传 manifest、Coordinator ETF 写入路径和通知语义；
- 按可配置的保种政策延后清理 qB 的文件和种子。

MoviePilot 只负责检索、下载器选择和控制面消息中转；OpenList 负责订阅、任务编排、上传、保种策略、进度和 ETF；qBittorrent 负责下载和保种。

## 非目标

- 不使用 MoviePilot 订阅或自动整理。
- 不修改 qB 下载目录内的文件名、内容或扩展名。
- 不在 desired config、inventory、任务 payload 或日志中保存/暴露 qB 明文凭据，也不让 MoviePilot Bridge 上传网盘文件；凭据仅以 Coordinator 加密 Secret 保存并为目标 Worker 公钥封装。
- 不把 qB 来源任务调度给没有本地源文件的其它 Worker。
- 不将 qB 的最大分享限制当作 PT 最短保种规则的唯一来源。

## 外部契约依据

- [MoviePilot V3 插件开发说明](https://github.com/jxxghp/MoviePilot-Plugins/blob/main/docs/Plugin_Development.md) 定义了 `_PluginBase`、`get_api()` 和插件生命周期；插件只使用 V3 host 提供的 SDK/chain，不启动独立服务。
- MoviePilot V3 `DownloadChain.download_single(..., return_detail=True)` 返回本次添加操作的 hash；Bridge 用该精确 hash 查询 `list_torrents`，获得 MoviePilot 实际选择的 downloader 和 `content_path`。若添加结果不确定，使用 request 专属 qB label 恢复，禁止用下载列表前后差集猜测。
- [qBittorrent WebUI API](https://github.com/qbittorrent/qBittorrent/wiki/WebUI-API-%28qBittorrent-5.0%29) 支持按 hash 查询 torrent、列出文件、暂停/恢复和删除，并返回 `content_path`、`ratio`、`seeding_time` 和状态。控制客户端优先使用 qB 5 的 `start/stop`，404 时兼容 qB 4 的 `resume/pause`。

因此，正式链路由插件建立 `request_id → downloader → torrent_hash → content_path` 显式绑定，并由 Coordinator 再绑定唯一 Worker。

## 方案选型

采用 **MoviePilot Bridge 插件 + Coordinator 严格路由 + Worker 本地 qB 处理**。

```text
OpenList Coordinator
  ├─ subscription / policy / state / ETF / progress
  ├─ signed HTTPS → MoviePilot Bridge plugin
  └─ cluster connection ↔ OpenList Worker ↔ local qB WebUI

MoviePilot Bridge plugin
  ├─ MoviePilot internal search / downloader selection / add download
  └─ signed HTTPS event → Coordinator

OpenList Worker
  ├─ qB state and file inspection by torrent hash
  ├─ copy qB source → staging
  └─ existing 139 upload plugin → manifest → Coordinator
```

该方案优于直接调用公开下载 API 或下载列表快照匹配：前者不保证返回关联 ID，后者在并发下载或外部创建任务时可能错配。列表快照仅作为 Bridge 故障时的人工修复辅助，不作为自动主路径。

## 安全与通信

Coordinator 可以主动通过 HTTPS 访问每个 MoviePilot Bridge 插件。每个 MoviePilot 实例具有独立的 HMAC 密钥。

### 请求签名

两端都对请求签名，使用以下稳定的规范化输入：

```text
version + "\n" + instance_id + "\n" + HTTP method + "\n" + request path + "\n" + unix_timestamp + "\n" + nonce + "\n" + SHA256(raw request body)
```

- 签名算法：`HMAC-SHA256`，编码为小写十六进制。
- 必需头：`X-OpenList-Bridge-Version`、`X-OpenList-Bridge-Instance`、`X-OpenList-Bridge-Timestamp`、`X-OpenList-Bridge-Nonce`、`X-OpenList-Bridge-Signature`。
- 默认允许 300 秒时钟偏差；接收方为 `(instance_id, nonce)` 持久化去重至少 10 分钟。
- 所有变更请求必须包含 UUID `request_id` 或 `event_id`；接收方以该键幂等。
- 密钥仅存于 Coordinator 密钥存储和对应 MoviePilot 插件配置；日志、UI、任务 payload 均不得回显。

### Bridge API

Coordinator 调用插件：

```text
POST /api/v1/plugin/OpenListBridge/search
POST /api/v1/plugin/OpenListBridge/intent
GET  /api/v1/plugin/OpenListBridge/intent/{request_id}
POST /api/v1/plugin/OpenListBridge/intent/{request_id}/cancel
POST /api/v1/plugin/OpenListBridge/control
```

插件向 Coordinator 回调：

```text
POST /api/v1/cluster/moviepilot/events
```

创建意图的最小字段：

```json
{
  "request_id": "uuid",
  "subscription_id": "string",
  "subscription_item_id": "string",
  "media": {"media_source": "tmdb", "media_id": "123", "media_type": "movie|tv", "season": 0},
  "torrent": {"resource_ref": "opaque-ref-from-search", "title": "string", "site": "string", "size": 0},
  "downloader_policy": {"mode": "moviepilot_select", "allowed": ["optional aliases"]}
}
```

`downloader_policy.mode` 固定为 `moviepilot_select`；Coordinator 不替代 MoviePilot 的选机规则。`resource_ref` 必须来自 Bridge 搜索，严禁传输 enclosure、passkey、站点 Cookie 或直链。插件必须最终回传实际 downloader，而不是候选或配置名。

事件：

| 事件 | 必需字段 | 含义 |
|---|---|---|
| `intent.accepted` | `event_id`, `request_id` | 插件已接收且开始处理 |
| `torrent.bound` | 上述字段、`downloader`, `torrent_hash`, `content_path` | MoviePilot/qB 已建立的不可变关联 |
| `torrent.state_changed` | `request_id`, `state`, `progress` | 快速状态提示，Worker 的精确 hash 观察为最终事实来源 |
| `torrent.failed` | `request_id`, `code`, `message` | 创建或调度失败 |
| `bridge.health_changed` | `instance_id`, `health` | 插件健康变化 |

插件应保存 outbox 并重试未确认事件；Coordinator 为每个事件先持久化 inbox，再驱动状态机。相同 `request_id` 的不同 payload，以及已经建立后发生变化的 downloader/hash/path 绑定，均作为冲突拒绝。

## 数据模型

### DownloadIntent

订阅扫描通过选源后创建的下载意图。唯一键为 `request_id`，保存订阅、媒体、种子指纹、创建时间、Bridge 实例和终态错误。

### TorrentBinding

在收到 `torrent.bound` 后建立，字段至少包括：

```text
request_id, moviepilot_instance_id, downloader_alias, worker_node_id,
qb_client_id, torrent_hash, moviepilot_content_path, observed_qb_content_path,
download_root_mapping, retention_policy_snapshot, bound_at
```

唯一性：`(moviepilot_instance_id, torrent_hash)`；`request_id` 至多绑定一个 torrent。绑定创建后不可换 Worker；如配置错误或 Worker 永久退役，必须由显式迁移流程处理，不能自动回退。

### TorrentJob 与 DeliveryFileJob

一个 `TorrentJob` 对应一个 qB hash，管理下载、保种和最终删除。Worker 发现 qB 文件清单后，为每个可搬运文件创建一个 `DeliveryFileJob`。

`DeliveryFileJob` 持有相对路径、解析出的季/集或电影身份、源大小、staging 状态、上传进度、最终 manifest 及失败原因。字幕、NFO、样片、特典由显式文件策略决定；默认只搬运媒体扩展名白名单中的已完成文件。

## Worker 配置与路由

每个 Worker 配置 qB 连接、加密凭据引用和路径映射。Coordinator 只保存并下发由该 Worker 公钥加密的 Secret envelope，Worker 在内存中解密；凭据缺失时 qB 路由不健康，不允许匿名降级：

```yaml
qb_clients:
  - id: qb-hk-local
    webui_url: http://qbittorrent:8080
    secret_ref: local-secret://qb-hk
    path_mappings:
      - qb_path: /downloads
        worker_path: /mnt/downloads

moviepilot_routes:
  - bridge_instance_id: mp-main
    downloader: qb-hk
    qb_client_id: qb-hk-local

staging:
  root: /mnt/staging/openlist
  staging_max_file_size_gb: 150
  max_upload_concurrency: 2
  staging_safety_reserve_gb: 80
  staging_pause_download_watermark_gb: 380
  staging_resume_download_watermark_gb: 430
  download_disk_pause_watermark_gb: 20
  download_disk_resume_watermark_gb: 40
  extension_whitelist: [.mkv, .mp4, .iso]
  antihash_enabled: true
  iso_rename_enabled: true
```

`path_mappings` 是必需能力：qB 可能运行在容器内，`content_path` 的容器路径必须映射为 Worker 可访问的宿主机路径。`webui_url` 可以是 Worker 可达的容器 DNS 或私网地址；HTTP 仅限可信隔离网络，跨主机应使用 HTTPS 和防火墙。Coordinator 的 inventory 只保存别名、Worker ID、容量槽位和健康状态；不回传 WebUI URL、凭据或本地路径。

Worker 在注册与心跳中上报 `(moviepilot_instance, downloader, qb_client_id)`、staging 可用容量、上传槽位和本机 qB 健康度。Coordinator 收到 `torrent.bound` 后用该路由精确选择 Worker；没有唯一可用路由时，将任务置为 `waiting_worker` 并告警。

## 状态机

```text
INTENT_CREATED
→ MP_ACCEPTED
→ TORRENT_BOUND
→ DOWNLOADING
→ DOWNLOAD_COMPLETED
→ FILES_DISCOVERED
→ TRANSFERRING_FILES
→ ETF_MATERIALIZED
→ SEEDING
→ RETENTION_SATISFIED
→ DELETING
→ DELETED
```

- `DOWNLOADING`、`DOWNLOAD_COMPLETED` 的权威来源是绑定 Worker 对 qB hash 的查询。MoviePilot 事件只用于降低发现延迟。
- 当 qB `progress == 1` 且文件清单满足完成条件时进入 `FILES_DISCOVERED`；多文件 torrent 生成多个 `DeliveryFileJob`。
- 每个文件独立经历 `queued → staging → uploading → uploaded/failed`，并向现有订阅任务框架回传进度。
- 所有必需 `DeliveryFileJob` 成功且 manifest 已由 Coordinator 正常消费后，父 `TorrentJob` 才进入 `SEEDING`。
- Bridge 离线不影响已绑定 torrent 的 Worker 观察、上传、保种和清理；只阻止新 Intent 发起。

## staging、上传与 ETF

1. Worker 通过 qB hash 获取 torrent 信息和文件列表，检查 `content_path` 在已声明路径映射内。
2. 对单个符合规则的文件做只读复制到 staging；禁止移动、重命名或经本地插件改写 qB 原件。
3. 在上传 139 的 staging 副本上强制启用已有 AntiHash 与 ISO Rename。qB 来源不能通过兼容配置字段关闭这两个转换；扩展名白名单由 Worker 配置，最终名称、大小、SHA256 必须以上传后 manifest 为准。
4. 上传成功后，Worker 回传既有集群上传 manifest；Coordinator 沿用当前 ETF 根目录、写入路径、通知和幂等逻辑。
5. 上传 manifest 先进入 Worker 持久结果队列；Coordinator 接收并完成既有 ETF materialization 后确认，Worker 再通过清理队列删除 staging 副本。qB 原文件在保种条件满足前保持不变。

staging admission 按下式计算：

```text
free_bytes - active_staging_bytes - candidate_size - safety_reserve >= 0
```

容量配置统一使用 GB（1 GB = 1024^3 bytes）。单文件超过 150 GB 直接拒绝并标记 `staging_file_too_large`。并发为 2 时，运维容量必须能承受两个最大文件加安全余量。低水位暂停未完成的受管下载，高水位后才恢复，避免频繁抖动。

## 保种与清理

保种政策在 `TorrentBinding` 创建时快照，避免订阅配置后改导致历史 torrent 的删除条件漂移。默认政策：固定最短时间、最小分享率、站点 H&R 规则均可独立关闭；永久保种和人工延长优先级最高。

自动删除条件：

```text
所有必需 DeliveryFileJob 已成功
AND ETF 已按既有集群流程确认完成
AND 非永久保种
AND 非人工延长
AND min_seed_time（若启用）满足
AND min_ratio（若启用）满足
AND site H&R rule（若启用）满足
```

MoviePilot 的 `hit_and_run` 只作为“可能存在站点规则”的提示。具体阈值由 OpenList 站点规则配置决定；未知规则默认不自动删除并进入人工处理。

删除采用绑定 Worker 的 qB hash 操作，先记录 `DELETING` 意图，再调用停止/删除，且必须明确 `deleteFiles=true` 才清除本地数据。qB 的 ratio/seeding time limit 可设为辅助保护，不得替代上述最低条件。

### Worker 离线保护

- Worker 与 Coordinator 连接断开但进程仍存活时，本机保护逻辑按精确 hash 暂停未完成且由 OpenList 管理的 torrent，并持久化暂停原因；完成保种中的 torrent 不因短暂断线暂停。
- Worker 进程或整机离线时，Coordinator 通过对应 MoviePilot Bridge 的签名 HTTPS `/control`，使用持久化 request/downloader/hash 精确暂停；恢复时只恢复 `paused_for_worker_offline` 的任务。
- 容量暂停与离线暂停分别记账。离线原因解除但仍低于高水位时，不恢复下载。
- 如果 MoviePilot 同时无法访问 qB，则任何远程控制面都无法暂停该 qB；生产部署应让 MoviePilot 到 qB 的控制路径独立于 Worker 主机，或增加同机 supervisor 作为额外保护。

## 与现有代码的边界

- 扩展 Worker desired config 和 inventory，增加 qB 客户端、路径映射、MoviePilot downloader 路由和 staging 能力。
- 扩展 `pkg/qbittorrent` 为按 hash 的原生查询、文件列表、停止、启动和删除接口；现有按 `openlist-{id}` tag 的路径保留给旧离线下载，不复用为 PT 主路径。
- 新增 qB 到 staging 的来源处理器；不能复用当前以分享保存为前提的媒体传输准备流程。
- qB 来源必须使用严格 Worker affinity，不能采用当前 preferred Worker 不可用时的负载回退。
- 继续复用现有 139 上传插件、cluster manifest 和 Coordinator ETF materializer；不新增 Worker 侧 ETF 文件写入。

## 错误处理

| 场景 | 处理 |
|---|---|
| Bridge 接收成功但未 bound | 在配置恢复窗口内按 request 专属 qB label 对账；超时发送 `download_binding_timeout` |
| Bridge 重复回调 | event_id 幂等消费 |
| downloader 无 Worker 路由 | `waiting_worker`，不创建跨节点搬运任务 |
| qB hash 不存在或路径越界 | 标记绑定异常，停止自动清理并告警 |
| staging 空间不足 | 暂停未完成受管下载，等待高水位恢复 |
| 单个文件超 150 GB | 失败且不触碰 qB 原文件 |
| 上传完成但 Coordinator 未确认 manifest | outbox 重试；不删除 staging 和 qB 数据 |
| 文件子任务部分失败 | 父 torrent 保持保种/待处理，不自动删除 |
| H&R 规则未知 | 不自动删除，进入人工处理 |
| 同一 request_id payload 变化 | 返回冲突，不覆盖 intent、outbox 或 torrent binding |

## 验收与测试

- Bridge：签名校验、nonce 防重放、事件幂等、`request_id → hash` 绑定、重试恢复。
- 路由：多个 MoviePilot downloader 与多个 Worker；Worker 不可用时只等待，不回退。
- qB：按 hash 查询、容器路径映射、单文件和多文件 torrent、暂停/恢复/删除。
- 多集：一个种子多个剧集文件分别产生搬运进度与 ETF manifest；任一必需子任务失败时不删除 torrent。
- 上传：验证 qB 原文件未变；staging 上传后 AntiHash/ISO Rename 的最终 manifest 用于 ETF。
- 容量：两个并发上传、150 GB 上限、低/高水位滞回与恢复。
- 保种：时间、分享率、H&R、人工延长、永久保种及 Worker 离线行为。
- 回归：现有分享来源订阅、普通 qB 离线下载、139 ETF 与集群上传路径保持可用。

## 分期实施

1. **控制面与路由**：Bridge 插件契约、签名、DownloadIntent/TorrentBinding、Worker qB 路由与严格 affinity。
2. **qB 本地来源**：按 hash 的 qB client、路径映射、文件发现、staging copy、单文件端到端上传。
3. **多文件与可观测性**：TorrentJob/DeliveryFileJob、订阅框架进度、状态和通知。
4. **保种与容量治理**：策略快照、H&R 配置、watchdog、容量水位、删除编排。
5. **上线保障**：迁移、权限审计、指标、告警、故障演练和回归测试。
