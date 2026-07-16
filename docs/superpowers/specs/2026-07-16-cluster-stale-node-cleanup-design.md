# Cluster Stale Node Cleanup Design

## 状态

已与用户确认的设计文档。

适用仓库：

- 后端：`/Volumes/extend Disk/Github/OpenList`

## 背景

当前 cluster 节点在线状态由 coordinator 持久化到数据库。

现状问题：

- `OnConnect` 会将节点写为 `online`，并创建 `connected` session。
- `OnDisconnect` 才会将 session 改为 `disconnected`，并将节点写为 `offline`。
- 如果 coordinator 在异常退出、进程重启、lease fencing 或数据库故障期间未能完整执行 `OnDisconnect`，数据库中会残留：
  - `cluster_nodes.status = online`
  - `cluster_node_sessions.status = connected`
- `ListNodes()` 当前直接返回数据库中的节点状态，没有根据 `last_heartbeat_at` 或当前活动 session 做二次修正。

这会带来两个直接问题：

- 已销毁或永久离线的 worker 仍长期显示为在线，形成“幽灵在线”节点。
- 节点列表会长期累积 stale offline 节点，影响运维判断。

用户已明确要求：

1. 立即清理远端已确认的脏节点记录。
2. 在代码中补上 heartbeat 超时和启动重整逻辑，避免脏状态再次出现。
3. 默认隐藏 stale offline 节点，同时提供手动清理接口，用于永久离线节点从列表中移除。

## 目标

### 核心目标

- 在远端实例上安全清理已确认的脏节点记录。
- 在 coordinator / hybrid 启动时重整遗留 session 和节点状态。
- 通过 heartbeat 超时机制自动将失活节点转为 `offline`。
- 节点列表默认隐藏 stale offline 节点。
- 提供显式清理接口，允许管理员移除永久离线节点。

### 非目标

- 不支持把离线节点纳入任务调度。
- 不支持对离线节点做自动唤醒、远程拉起或外部开机控制。
- 不修改现有 cluster job offer / outbox 的在线发送语义。
- 不在第一版中引入复杂的 node archival 表或软删除审计模型。

## 现状分析

### 当前状态写入路径

节点状态的 authoritative 写入点为：

- `OnConnect`
  - 写 `cluster_nodes.status = online`
  - 写 `cluster_nodes.last_session_id`
  - 写 `cluster_nodes.last_heartbeat_at`
  - 创建 `cluster_node_sessions.status = connected`
- `handleHeartbeat`
  - 刷新 `cluster_nodes.status = online`
  - 刷新 `last_heartbeat_at`
- `OnDisconnect`
  - 将对应 session 写为 `disconnected`
  - 仅在 `id + last_session_id` 匹配时将节点写回 `offline`

### 当前显示与调度语义

- 节点列表展示来自 `ListNodes()`，当前直接读取数据库中的 `cluster_nodes`。
- 实际 cluster 任务调度主要依赖 transport hub 的 `ConnectedNodes()` / `Session()` 内存态，而不是单纯依赖 `cluster_nodes.status`。

这意味着：

- “幽灵在线”主要影响管理界面与运维判断。
- 调度层通常不会把任务派给真正不在线的节点，但显示层会误导用户。

## 设计选项

### 方案 A：仅展示层修正

仅在 `ListNodes()` 中根据 `last_heartbeat_at` 将超时节点显示为 `offline`，并隐藏 stale offline 节点。

优点：

- 改动最小。

缺点：

- 数据库中的脏 session / 脏 status 不会被修复。
- 进程重启后问题仍会重复积累。
- 无法为后续管理动作提供一致的节点状态基础。

### 方案 B：启动重整 + 心跳超时 + stale cleanup 接口

在状态写入、状态修正和管理入口三层同时治理。

优点：

- 既修复根因，也修复历史遗留。
- 自动治理与人工治理兼具。
- 改动集中在 coordinator service 和 cluster admin handle，边界清晰。

缺点：

- 需要补充多组状态机测试。

### 方案 C：定时硬删除所有离线旧节点

通过后台任务自动删除离线超过阈值的所有节点与 session。

优点：

- 列表最干净。

缺点：

- 过于激进，容易误删临时离线节点。
- 不利于保留最近会话和 inventory 调试线索。

## 设计结论

采用 **方案 B**。

具体由四部分组成：

1. 远端一次性 SQL 清理已确认脏记录。
2. 启动重整：修正上次异常退出遗留的 `connected` / `online` 状态。
3. 心跳超时 sweep：将失活节点自动降为 `offline`。
4. 节点列表默认隐藏 stale offline 节点，并提供手动清理接口。

## 详细设计

### 1. 远端一次性 SQL 清理

对已确认脏节点 `oplist-etf-139cloudPC` 执行一次性修正：

- 将该节点改为 `offline`
- 将其所有仍为 `connected` 且未写 `disconnected_at` 的 session 改为 `disconnected`
- 记录 `disconnect_error = 'manual stale session cleanup'`

约束：

- 仅修改已确认脏节点，不做全表批量强制清理。
- 操作前备份目标数据库文件。
- 操作后复查节点列表与 session 表。

### 2. 启动重整

在 coordinator service 初始化完成、对外提供 cluster 管理前，执行一次启动重整。

#### 重整目标

处理上一次 coordinator 进程未能完整执行 `OnDisconnect` 的遗留状态：

- 所有 `cluster_node_sessions.status = connected` 的记录改为 `disconnected`
- 所有处于 `online` 且不应视为在线的节点改为 `offline`

#### 重整规则

启动时 coordinator 内存中尚无活动 transport session，因此可将以下状态视为遗留：

- `cluster_node_sessions.status = connected`
- `cluster_nodes.status = online`

例外：

- `disabled`
- `revoked`
- `draining`

这些节点不应被统一覆写为普通 `offline`，保留其管理态。

#### 写入结果

- stale session：
  - `status = disconnected`
  - `disconnected_at = now`
  - `disconnect_error = 'startup reconciliation'`
- stale online node：
  - `status = offline`
  - 保留 `last_heartbeat_at`
  - 不清空 `last_session_id`，用于后续排查

### 3. 心跳超时 sweep

增加一个 coordinator 侧定时 sweep，用于把失活节点自动标记为离线。

#### 超时阈值

从现有 cluster heartbeat 配置推导：

- `heartbeat_interval_seconds <= 0` 时，默认按 `15s`
- 失活阈值为：`max(3 * heartbeat_interval, 60s)`

示例：

- `heartbeat = 15s` 时，超时阈值为 `60s`
- `heartbeat = 30s` 时，超时阈值为 `90s`

#### sweep 规则

仅对满足以下条件的节点生效：

- `status = online`
- `last_heartbeat_at IS NOT NULL`
- `last_heartbeat_at < now - timeout`
- 节点不处于 `disabled` / `revoked`

执行动作：

- 节点改为 `offline`
- 对应 `last_session_id` 的 session 若仍是 `connected`，改为 `disconnected`
  - `disconnected_at = now`
  - `disconnect_error = 'heartbeat timeout'`

### 4. ListNodes 有效状态修正

`ListNodes()` 不再盲信数据库中的 `status` 字段。

在返回节点摘要前，服务层会基于以下规则计算“有效可见状态”：

- `disabled` / `revoked` / `draining` 优先保留原值
- `online` 但心跳已超时：对外视为 `offline`
- 其余状态保持原样

这样即使 sweep 尚未来得及运行，列表也不会继续把明显失活节点显示为在线。

### 5. stale offline 节点定义

节点满足以下条件时，被视为 stale offline：

- `status = offline`
- 且 `reference_time < now - stale_threshold`

`reference_time` 取值顺序：

1. `last_heartbeat_at`
2. `updated_at`
3. `created_at`

`stale_threshold` 第一版固定为 `7 天`。

### 6. 节点列表默认隐藏 stale offline 节点

`ListNodes()` 默认过滤 stale offline 节点。

行为：

- 默认请求不返回 stale offline 节点
- 管理端可通过显式参数查看完整列表，例如 `include_stale=true`

第一版只要求后端支持该参数与默认隐藏行为；前端是否追加“显示陈旧离线节点”开关，可作为后续独立 UI 优化，不阻塞本次修复。

### 7. 手动清理接口

新增管理员接口，用于删除永久离线节点。

#### 接口能力

- 删除指定节点 ID
- 仅允许删除 stale offline 节点

#### 删除范围

删除节点时同时删除：

- `cluster_nodes` 中该节点记录
- `cluster_node_sessions` 中该节点 session 记录
- `cluster_node_inventories` 中该节点 inventory 记录
- 该节点相关的 desired config / observed config / storage apply state 等纯节点配置状态

不删除：

- 历史 job / attempt / outbox / upload manifest

原因：

- job 历史是调度审计数据，不应因节点移除而整体抹掉。
- 若历史 job 仍引用该 node id，允许以“节点已清理”的方式保留历史字符串引用。

#### 删除约束

以下节点不得被删除：

- 当前有效在线节点
- `disabled` / `revoked` 节点
- 非 stale 的普通离线节点

返回错误信息要明确说明原因，例如：

- `cluster node is not stale offline`
- `connected cluster node cannot be removed`

## 数据流

### 启动时

1. runtime 初始化 coordinator service
2. coordinator service 执行 startup reconciliation
3. 启动 heartbeat timeout sweep 定时器
4. 对外提供节点列表和管理接口

### 正常在线节点

1. worker connect
2. node -> `online`
3. session -> `connected`
4. heartbeat 持续刷新 `last_heartbeat_at`
5. disconnect 时正常回写 `offline` / `disconnected`

### 异常退出节点

1. worker 或 coordinator 异常终止
2. `OnDisconnect` 可能未执行
3. 数据库残留 `online` / `connected`
4. 下次启动时由 startup reconciliation 兜底
5. 若运行中断心跳，则由 heartbeat timeout sweep 兜底

### 永久离线节点

1. 节点长期保持 `offline`
2. 超过 `7 天` 成为 stale offline
3. 默认从列表隐藏
4. 管理员可显式查看并手动清理

## 错误处理

- 启动重整失败：
  - 记录 error
  - 阻止 coordinator service 进入对外服务态，避免继续暴露不一致状态
- heartbeat sweep 失败：
  - 记录 warning / error
  - 保留下个周期重试
- 手动清理接口删除失败：
  - 返回明确错误，不做部分成功后静默吞错
- 删除事务内任一步失败：
  - 整体回滚

## 验证

### 自动化测试

新增或更新以下测试：

- `startup reconciliation`：
  - 遗留 `connected` session 被改为 `disconnected`
  - 遗留 `online` 节点被改为 `offline`
  - `disabled` / `revoked` / `draining` 节点不被误改
- `heartbeat timeout sweep`：
  - 超时节点与 session 被正确降级
  - 未超时节点保持在线
- `ListNodes`：
  - 默认隐藏 stale offline 节点
  - `include_stale=true` 时返回全部节点
  - 超时在线节点在列表中对外显示为 `offline`
- `manual cleanup`：
  - stale offline 节点可删除
  - 在线节点、非 stale 离线节点、disabled/revoked 节点删除被拒绝
  - 删除仅影响节点相关表，不影响历史 job 记录

### 手工验证

- 在远端备份数据库后执行一次性 SQL 清理，确认 `oplist-etf-139cloudPC` 从默认列表消失。
- 重启 coordinator / hybrid 实例，确认启动后不会残留历史 `connected` session。
- 模拟 worker 失联超过超时阈值，确认节点自动转为 `offline`。
- 创建一个超过 7 天的离线测试节点，确认默认列表隐藏，且手动清理接口可以删除。

## 回滚

- 远端 SQL 清理前保留数据库备份。
- 若代码回滚，恢复到先前镜像或二进制版本。
- 若需要恢复某个被手动清理的节点，只能通过数据库备份恢复，不提供应用级反删除。
