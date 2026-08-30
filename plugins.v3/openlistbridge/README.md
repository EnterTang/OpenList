# OpenList Bridge（MoviePilot V3 插件）

OpenList Bridge 是 MoviePilot V3 控制面插件。它只负责资源搜索、调用 MoviePilot 选择下载器并创建 qBittorrent 下载、把精确的 `downloader + torrent_hash` 绑定及状态回传给 OpenList。插件不创建 MoviePilot 订阅、不执行媒体整理、不上传网盘，也不读取或修改 qB 下载文件。

## 安装与入口

如果通过 `PLUGIN_LOCAL_REPO_PATHS` 使用本地插件仓库，路径必须指向包含
`package.v3.json` 的仓库根目录（本仓库中的 `plugins.v3`），不能指向
`plugins.v3/openlistbridge` 子目录。安装时由 MoviePilot 将
`plugins.v3/openlistbridge` 同步到插件目录，并确保能导入其中的
`__init__.py` 和 `OpenListBridge(_PluginBase)`。如果只手工安装单个插件目录，
则不应把该目录同时配置为本地插件仓库路径。

插件通过 MoviePilot V3 的 `get_api()` 注册以下入口，实际前缀由 MoviePilot 固定为 `/api/v1/plugin/OpenListBridge`：

- `POST /api/v1/plugin/OpenListBridge/search`
- `POST /api/v1/plugin/OpenListBridge/intent`
- `GET /api/v1/plugin/OpenListBridge/intent/{request_id}`
- `POST /api/v1/plugin/OpenListBridge/intent/{request_id}/cancel`
- `POST /api/v1/plugin/OpenListBridge/control`

`control` 只接受已持久化绑定的精确下载器和 torrent hash，用于 Worker 整机离线时暂停下载，以及恢复在线后的恢复下载。插件向 Coordinator 的回调固定为：

```text
POST https://<openlist>/api/v1/cluster/moviepilot/events
```

两端均对 HTTP 方法、完整 API path、时间戳、nonce 和原始请求 body 的 SHA-256 使用 HMAC-SHA256 签名。时间戳允许偏差为 5 分钟，nonce 在 SQLite 中持久化并防重放。每个 MoviePilot 实例必须使用不同密钥。

## 配置

插件配置页提供以下字段：

```yaml
enabled: true
instance_id: mp-main
hmac_key: change-me-to-an-instance-specific-secret
coordinator_url: https://openlist.example.internal
state_directory: /config/plugins/openlistbridge
save_path: /downloads          # 可选；传给 MoviePilot 下载链
timeout_seconds: 15            # 1～120 秒
retry_backoff_seconds: 10      # 1～3600 秒
creation_recovery_timeout_seconds: 600  # 60～86400 秒
```

- `instance_id` 必须与 OpenList Bridge 实例 ID、Worker 的 `moviepilot_routes.bridge_instance_id` 完全一致。
- `coordinator_url` 必须是无 userinfo、query 和 fragment 的 HTTPS URL。
- `state_directory` 必须位于持久卷。`openlistbridge.sqlite3` 保存 nonce、intent、torrent 绑定和回调 outbox；容器重启后不能丢失。
- `hmac_key` 至少 16 字节，只能保存在 MoviePilot 插件配置和 OpenList 加密 Secret 中，不得写入日志或 payload。
- MoviePilot 的自动整理功能无需开启，本插件也不会主动触发整理。
- `creation_recovery_timeout_seconds` 是 qB 已接受请求但 MoviePilot 暂时未查到绑定时的恢复窗口。窗口内插件按 request 专属 qB label 对账，不会重复添加下载；超时才发送 `download_binding_timeout`。

## 一致性与失败恢复

- 搜索结果只暴露 HMAC 生成的 opaque `resource_ref`、标题、站点显示名、大小和做种人数等安全投影；站点 Cookie、下载链接、qB 凭据、qB WebUI 地址和本机路径不会返回。
- 下载由 OpenList 提交 intent 发起。插件使用 MoviePilot `SearchChain.search_by_id` 和 `DownloadChain.download_single(..., return_detail=True)`，再按该次调用返回的 hash 精确查询 `list_torrents`，不会用创建前后列表差集猜测归属。
- `request_id` 幂等且绑定不可变；相同 ID 的不同 payload，或已绑定后改变 downloader、hash、路径或大小，都会被拒绝。`intent.accepted` 必须先于 `torrent.bound`；回调写入 SQLite outbox，按创建顺序、指数退避重试，成功后标记 `acknowledged`。
- 插件每分钟轮询已绑定 torrent，并对状态变化发送 `torrent.state_changed`。标准 qB 状态不一定包含站点 H&R 结论；启用 `site_rule_id` 但没有可信 `hnr_passed` 时，Coordinator 会进入人工复核，绝不自动删种。
- intent 取消只在下载尚未创建时有效；下载已经绑定后，应通过 OpenList 的保种策略和精确控制链路管理。

## 验证

```sh
cd plugins.v3/openlistbridge
PYTHONPATH=. python3 -m unittest discover -s tests -v
python3 -m compileall -q .
```

单元测试覆盖 V3 插件生命周期、签名与 nonce、secret 字段拦截、下载器/hash 精确绑定、状态轮询、暂停/恢复、outbox 顺序和退避。正式上线仍需在目标 MoviePilot 版本中完成一次真实搜索、下载器选择、qB 创建及 HTTPS 双向回调验收。
