# MoviePilot / qBittorrent PT 搬运运行说明

该集成采用混合模式：OpenList Coordinator 负责订阅、torrent 绑定、交付状态、ETF 写入和通知；MoviePilot Bridge 插件仅提供搜索、下载器选择和消息中转；qBittorrent 仅下载及保种；与 qB 同机的 OpenList Worker 复制文件到本地 staging 后上传移动网盘。

## 部署边界

- 每个 MoviePilot 实例部署一个 V3 `OpenListBridge` 插件。Coordinator 通过 HTTPS 主动调用 `/api/v1/plugin/OpenListBridge/*`；每个实例使用独立 HMAC 密钥，并校验时间戳和持久化 nonce 防重放。插件回调使用持久化 outbox，按事件顺序和指数退避重试。
- Coordinator 的管理面保存 Worker desired config，因此会持久化 qB WebUI 地址、路径映射和 `secret_ref`；这些内容不会进入公开 inventory、订阅进度或任务事件。qB 用户名密码只以加密 `ClusterSecret` 保存，每次下发再使用目标 Worker 的 X25519 公钥封装，Worker 解密后只保留在运行内存中。
- 每条 `moviepilot_instance + downloader + qb_client_id` 路由必须唯一指向一个可用 Worker。Bridge 回传 MoviePilot 选择的 downloader 和 qB torrent hash，Coordinator 以路由选择 Worker；没有唯一可用 Worker 时保持 `waiting_worker`，不猜测 qB 归属。
- 一个 torrent 可以包含多集。Coordinator 将每个识别的媒体文件建立为独立 delivery，单个 delivery 可携带 `relative_path`，Worker 只复制该文件。
- `request_id`、torrent binding 和 delivery 均为强幂等。相同 ID 的不同 intent payload、已建立绑定的 downloader/hash/path 变化、终态 delivery 的伪重投都会被拒绝；失败 delivery 通过现有任务重试或人工重试入口处理。

## Worker 配置

下列字段属于 `WorkerDesiredConfig` 的 `qb_clients`、`moviepilot_routes` 与 `staging`。字节数使用整数；150 GiB 为单文件硬上限。

```yaml
qb_clients:
  - id: qb-hk-local
    webui_url: https://qb-worker.example.internal
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
  max_file_bytes: 161061273600       # 150 GiB
  max_upload_concurrency: 2
  safety_reserve_bytes: 85899345920  # 80 GiB
  pause_download_low_watermark_bytes: 408021893120
  resume_download_high_watermark_bytes: 461708984320
  extension_whitelist: [.mkv, .mp4, .iso]
  antihash_enabled: true
  iso_rename_enabled: true
```

`pause_download_low_watermark_bytes` 和 `resume_download_high_watermark_bytes` 必须同时设置，且高水位不得低于低水位。低水位以下会暂停未完成的受管 torrent；仅在高水位及以上恢复。`safety_reserve_bytes` 同时参与每次复制的准入计算：`free - reserved - file_size - safety_reserve >= 0`。

`webui_url` 可使用 Worker 可达的容器 DNS 或私网地址，例如 `http://qbittorrent:8080`，不限于 loopback。HTTP 只应部署在 Worker 与 qB 的可信隔离网络中；跨主机链路应使用 HTTPS，并用防火墙限制为 Worker 可访问。`secret_ref` 必填且必须能解密出用户名和密码，不会降级为匿名 qB 会话。Worker 同时兼容 qB 5 的 `start/stop` 与 qB 4 的 `resume/pause` 控制端点。

## 文件、ETF 与保种

1. Worker 使用 qB torrent hash 查询文件，先将源路径按 `path_mappings` 映射为本机路径。
2. 原始 qB 文件始终只读。Worker 复制到 staging 后才执行 AntiHash、ISO Rename 和上传。对 qB 来源，这两个转换由代码强制开启；desired config 中的同名兼容字段不能将其关闭，扩展名白名单仍以 Worker 配置为准。
3. Worker 回传上传 manifest；Coordinator 沿用现有 ETF 写入路径、通知和幂等逻辑。只有所有必需 delivery 均 materialized 且 manifest 完整，才允许进入保种删除评估。
4. staging 副本在上传成功、Coordinator 确认 manifest 后按既有清理队列删除；qB 原件只在保种策略满足时删除。

保种策略在 torrent 绑定时快照，支持最短保种时间、最低分享率、站点 H&R 规则、人工延长和永久保种。人工延长/永久保种优先于自动删除。

为避免误删，时间、分享率、H&R、人工延长和永久保种全部未配置时，不会立即删除，而是持续保种；至少配置一项可自动判定的删除门槛后才进入自动清理流程。

`site_rule_id` 表示启用了站点 H&R 判定。只有 Bridge 或站点适配器提供可信 `hnr_passed=true` 时才允许自动删除；标准 qB 状态无法给出结论时，Coordinator 将任务标为 `manual_review`。这是安全失败策略，禁止将“未知”视为通过。人工延长或永久保种必须在任务进入 `deleting` 前更新；每次重新满足删除条件都会生成新的删除幂等键。

## Worker 离线与容量保护

- WebSocket 断开时，Worker 查询并暂停已记录的未完成受管 torrent，并持久化 `paused_by_disconnect`。完成并保种的 torrent 不会因断线被暂停。
- 如果 Worker 进程或整机离线，无法执行本地断线钩子，Coordinator 会通过该 MoviePilot 实例的签名 HTTPS `/control` 接口，按持久化的 downloader/hash 精确暂停。Worker 恢复在线且路由健康后再精确恢复；该状态持久化在 torrent binding 上，可重复对账。
- 重连后只恢复由断线保护暂停的任务；若该任务同时因磁盘容量暂停，则先清除断线原因，仍等待高水位恢复。
- 容量暂停使用独立的 `paused_by_capacity` 状态。Worker 每分钟巡检 staging 空间，也会在配置应用、重连和 staging 准入失败时立即巡检。
- qB 返回 torrent 不存在时，Worker 移除本地受管记录，不会对已删除 hash 重试恢复。

## 上线检查

1. 验证 Bridge 的 HTTPS 证书、HMAC 密钥、时钟偏差和 nonce 存储。
2. 验证每一个 qB 容器路径均有准确的 Worker 路径映射，并确认 Worker 对下载目录只读、对 staging 可读写。
3. 使用单文件和多集 torrent 分别做端到端演练，确认每个 delivery 的进度、manifest、ETF 和订阅通知。
4. 将 staging 空间压到低水位以下，确认 qB 受管下载暂停；释放空间到高水位以上，确认仅容量暂停的未完成下载恢复。
5. 断开 Worker 与 Coordinator 的连接，确认未完成受管 torrent 暂停；恢复连接并重新下发加密 qB 凭据，确认只恢复断线保护暂停的任务。
6. 关闭 Worker 进程或整机，确认 Coordinator 能通过 MoviePilot Bridge 精确暂停其绑定 qB；恢复 Worker、session 和健康 inventory 后确认只恢复 `paused_for_worker_offline` 的任务。
7. 配置未知的 `site_rule_id`，确认达到时间/分享率后进入 `manual_review` 而不是删种；再分别验收人工延长和永久保种。
8. 模拟 qB 添加成功但短时不可见，确认 Bridge 在 `creation_recovery_timeout_seconds` 内按 request label 恢复同一 hash，且不会创建第二个 qB 任务。
