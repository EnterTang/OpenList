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
- 不把 qB 凭据交给 Coordinator，也不让 MoviePilot Bridge 上传网盘文件。
- 不把 qB 来源任务调度给没有本地源文件的其它 Worker。
- 不将 qB 的最大分享限制当作 PT 最短保种规则的唯一来源。

## 外部契约依据

- [MoviePilot OpenAPI](https://api.movie-pilot.org/openapi.json) 的下载任务模型包含 `downloader`、`hash`、`content_path`、`season_episode`、`media`、进度和状态；`download/add` 的响应为通用响应，**不承诺**返回可直接关联的下载 ID。
- [qBittorrent WebUI API 5.0](https://github.com/qbittorrent/qBittorrent/wiki/WebUI-API-%28qBittorrent-5.0%29) 支持按 hash 查询 torrent、列出文件、暂停/恢复和删除，并返回 `content_path`、`ratio`、`seeding_time` 和状态。

因此，正式链路不能依赖 MoviePilot 添加下载后的响应猜测任务身份；必须由插件形成请求与 qB hash 的显式绑定。

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
- 必需头：`X-OpenList-Signature-Version`、`X-OpenList-Instance`、`X-OpenList-Timestamp`、`X-OpenList-Nonce`、`X-OpenList-Signature`。
- 默认允许 300 秒时钟偏差；接收方为 `(instance_id, nonce)` 持久化去重至少 10 分钟。
- 所有变更请求必须包含 UUID `request_id` 或 `event_id`；接收方以该键幂等。
- 密钥仅存于 Coordinator 密钥存储和对应 MoviePilot 插件配置；日志、UI、任务 payload 均不得回显。

### Bridge API

Coordinator 调用插件：

```text
POST /api/v1/plugin/openlist-bridge/intents
GET  /api/v1/plugin/openlist-bridge/intents/{request_id}
POST /api/v1/plugin/openlist-bridge/intents/{request_id}/cancel
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
  "media": {"tmdb_id": 0, "type": "movie|tv", "season": 0},
  "torrent": {"title": "string", "enclosure": "url", "site": "string", "size": 0},
  "downloader_policy": {"mode": "moviepilot_select", "allowed": ["optional aliases"]}
}
```

`downloader_policy.mode` 固定为 `moviepilot_select`；Coordinator 不替代 MoviePilot 的选机规则。插件必须最终回传实际 downloader，而不是候选或配置名。

事件：

| 事件 | 必需字段 | 含义 |
|---|---|---|
| `intent.accepted` | `event_id`, `request_id` | 插件已接收且开始处理 |
| `torrent.bound` | 上述字段、`downloader`, `torrent_hash`, `content_path` | MoviePilot/qB 已建立的不可变关联 |
| `torrent.state_changed` | `torrent_hash`, `state`, `progress` | 快速状态提示，非最终事实来源 |
| `torrent.failed` | `request_id`, `code`, `message` | 创建或调度失败 |
| `bridge.health_changed` | `instance_id`, `health` | 插件健康变化 |

插件应保存 outbox 并重试未确认事件；Coordinator 为每个事件先持久化 inbox，再驱动状态机。

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

每个 Worker 本地配置 qB 连接和路径映射，凭据只能通过 Worker 本地 secret 引用解析：

```yaml
qb_clients:
  - id: qb-hk-local
    webui_url: http://127.0.0.1:8080
    credential_ref: local-secret://qb-hk
    path_mappings:
      - qb_prefix: /downloads
        worker_prefix: /mnt/downloads

moviepilot_routes:
  - moviepilot_instance: mp-main
    downloader: qb-hk
    qb_client_id: qb-hk-local

staging:
  root: /mnt/staging/openlist
  max_file_size: 150GiB
  max_upload_concurrency: 2
  safety_reserve: 80GiB
  pause_download_low_watermark: 380GiB
  resume_download_high_watermark: 430GiB
```

`path_mappings` 是必需能力：qB 可能运行在容器内，`content_path` 的容器路径必须映射为 Worker 可访问的宿主机路径。Coordinator 只保存别名、Worker ID 和能力状态；不保存 WebUI 密钥或可访问路径。

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
3. 在上传 139 的流上启用已有 AntiHash 与 ISO Rename。扩展名白名单由 Worker 配置；最终名称、大小、SHA256 必须以上传后 manifest 为准。
4. 上传成功后，Worker 回传既有集群上传 manifest；Coordinator 沿用当前 ETF 根目录、写入路径、通知和幂等逻辑。
5. 只有上传终态确认后清理 staging 副本。上传或 ETF materialization 可重试时保留副本至重试策略终止。

staging admission 按下式计算：

```text
free_bytes - active_staging_bytes - candidate_size - safety_reserve >= 0
```

单文件超过 150 GiB 直接拒绝并标记 `staging_file_too_large`。并发为 2 时，运维容量必须能承受两个最大文件加安全余量。低水位暂停未完成的受管下载，高水位后才恢复，避免频繁抖动。

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

Coordinator 不能在 Worker 主机断网时远程暂停其 qB。为满足可执行性：

- Worker 本机 lease watchdog 在失去 Coordinator 租约时，仅暂停未完成且由 OpenList 管理的 torrent。
- 已完成的保种 torrent 默认继续运行，避免短暂控制面故障违反 H&R。
- 恢复租约后，仅恢复由 watchdog 自身暂停的未完成 torrent。
- 若要求 Worker 主进程崩溃后也暂停，watchdog 必须是独立 sidecar/supervisor；该要求不由 Coordinator 单独保证。

## 与现有代码的边界

- 扩展 Worker desired config 和 inventory，增加 qB 客户端、路径映射、MoviePilot downloader 路由和 staging 能力。
- 扩展 `pkg/qbittorrent` 为按 hash 的原生查询、文件列表、停止、启动和删除接口；现有按 `openlist-{id}` tag 的路径保留给旧离线下载，不复用为 PT 主路径。
- 新增 qB 到 staging 的来源处理器；不能复用当前以分享保存为前提的媒体传输准备流程。
- qB 来源必须使用严格 Worker affinity，不能采用当前 preferred Worker 不可用时的负载回退。
- 继续复用现有 139 上传插件、cluster manifest 和 Coordinator ETF materializer；不新增 Worker 侧 ETF 文件写入。

## 错误处理

| 场景 | 处理 |
|---|---|
| Bridge 接收成功但未 bound | 按 request_id 查询和重试；超时后进入人工核对 |
| Bridge 重复回调 | event_id 幂等消费 |
| downloader 无 Worker 路由 | `waiting_worker`，不创建跨节点搬运任务 |
| qB hash 不存在或路径越界 | 标记绑定异常，停止自动清理并告警 |
| staging 空间不足 | 暂停未完成受管下载，等待高水位恢复 |
| 单个文件超 150 GiB | 失败且不触碰 qB 原文件 |
| 上传完成但 Coordinator 未确认 manifest | outbox 重试；不删除 staging 和 qB 数据 |
| 文件子任务部分失败 | 父 torrent 保持保种/待处理，不自动删除 |
| H&R 规则未知 | 不自动删除，进入人工处理 |

## 验收与测试

- Bridge：签名校验、nonce 防重放、事件幂等、`request_id → hash` 绑定、重试恢复。
- 路由：多个 MoviePilot downloader 与多个 Worker；Worker 不可用时只等待，不回退。
- qB：按 hash 查询、容器路径映射、单文件和多文件 torrent、暂停/恢复/删除。
- 多集：一个种子多个剧集文件分别产生搬运进度与 ETF manifest；任一必需子任务失败时不删除 torrent。
- 上传：验证 qB 原文件未变；staging 上传后 AntiHash/ISO Rename 的最终 manifest 用于 ETF。
- 容量：两个并发上传、150 GiB 上限、低/高水位滞回与恢复。
- 保种：时间、分享率、H&R、人工延长、永久保种及 Worker 离线行为。
- 回归：现有分享来源订阅、普通 qB 离线下载、139 ETF 与集群上传路径保持可用。

## 分期实施

1. **控制面与路由**：Bridge 插件契约、签名、DownloadIntent/TorrentBinding、Worker qB 路由与严格 affinity。
2. **qB 本地来源**：按 hash 的 qB client、路径映射、文件发现、staging copy、单文件端到端上传。
3. **多文件与可观测性**：TorrentJob/DeliveryFileJob、订阅框架进度、状态和通知。
4. **保种与容量治理**：策略快照、H&R 配置、watchdog、容量水位、删除编排。
5. **上线保障**：迁移、权限审计、指标、告警、故障演练和回归测试。
