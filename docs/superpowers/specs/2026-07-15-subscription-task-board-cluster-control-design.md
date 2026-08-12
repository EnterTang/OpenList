# 订阅任务看板与集群控制设计

## 目标

将订阅执行视图改为围绕实际内容变更和异常处置，而不是围绕每次转存；为订阅卡片提供按集的当前来源与执行节点详情；将现有集群页面统一收敛到首页的单一“集群控制”入口。

## 已确认的体验决策

- 首页侧边栏只增加一个“集群控制”父入口。
- 集群控制页面顶部使用横向 Tab，不在侧边栏嵌套子菜单。
- `hybrid` 和 `coordinator` 显示“总览、Worker 节点、任务看板、集群配置”。
- `worker` 和 `standalone` 仅显示“任务看板、集群配置”。
- 任务看板的“新增文件”统计当前筛选范围内所有变更记录的 `AddedCount` 总和。
- 成功执行仅在 `AddedCount > 0` 或 `ChangedCount > 0` 时进入最近执行记录；纯转存成功不显示。
- 异常执行不进入主列表，而是在任务看板右下角以收起的数量药丸显示。点击药丸后展开异常摘要和清除按钮。
- 订阅卡片的详情弹窗按季和集显示当前来源。每集仅保留最后一次成功创建转存任务时选择的来源。
- 每集来源显示订阅来源简称 `TG` 或 `PS`、来源盘 `123`、`115`、`Quark` 或 `Ali`、文件名直链、执行 Worker 和更新时间。
- standalone 的执行节点显示“本机”。

## 方案选择

来源详情采用独立的“每集当前来源快照”表，不从 `SubscriptionItem.UpdatedAt` 推导。

`SubscriptionItem` 以 `subscription_id + source_key` 唯一，换源、重试、转存状态推进都会影响该表。用它推导“最后选择来源”会在同一集存在多个分享时失真。将其改为按集唯一会破坏现有按来源去重和任务生命周期。

新增 `SubscriptionEpisodeSource` 作为投影表，每个 `(subscription_id, season, episode)` 仅保存一个当前快照。电影使用 `(season=0, episode=0)`。快照中的来源信息只在任务成功创建时覆盖，转存完成、失败或重试不改变来源选择时间。

## 后端设计

### 每集来源快照

新增模型 `SubscriptionEpisodeSource`，由数据库自动迁移，字段包括：

- `subscription_id`、`season`、`episode`：唯一集槽位。
- `subscription_item_id`：创建该快照的订阅项。
- `source_type`：订阅来源类型，前端映射为 `TG`、`PS`；手动来源保留“手动”。
- `source_provider`：实际网盘提供方，前端映射为 `123`、`115`、`Quark` 或 `Ali`。
- `share_url`、`file_name`：文件名直接链接到该分享。
- `cluster_job_id`：集群任务 ID；standalone 为空。
- `selected_at`：成功创建任务时刻。

更新时机：

1. standalone：文件转存任务成功入队后，以选中的 `SubscriptionItem` upsert 快照。
2. cluster：`DispatchSubscriptionMedia` 成功返回 job ID 后，以已选择的 `SubscriptionItem` 和 job ID upsert 快照。
3. 未成功创建任务不写快照，因此详情中不会把仅发现、未转存的分享误展示为当前来源。

详情接口为 `GET /admin/subscription/episode_sources?subscription_id=<id>`，按季、集排序。接口读取快照、对应的 `SubscriptionItem` 状态和集群任务：

- 有 `cluster_job_id` 时优先展示 `ClusterJob.AssignedNodeID` 对应的节点名称；当前未指派时回退到最新 `ClusterJobAttempt.NodeID`，两者均为空时显示“未指派”。
- 无 `cluster_job_id` 时返回 `worker_name = "本机"`。
- 无历史快照的旧订阅项不做不可靠回填，前端显示“暂未创建转存任务”。

### 任务看板运行记录

扩展订阅运行查询的视图参数和筛选：

- `view=changes`：仅返回成功且新增或变更数量大于零的运行记录。
- `view=failures`：仅返回失败状态或携带错误的运行记录。

两个视图均支持 `subscription_id`、状态、来源、关键词和分页参数。运行查询关联订阅表，在响应中附带订阅名称与来源类型，避免前端用不完整的订阅分页结果自行拼接。

新增 `GET /admin/subscription/board`。该接口接收与运行记录相同的筛选条件，并在数据库中使用完全相同的筛选范围返回订阅数、变更运行数、新增文件总数、变更文件总数和异常执行数量。任务看板主列表使用 `view=changes`，异常药丸使用 `view=failures`；三者共享同一筛选条件。

异常清除继续复用现有失败运行清理接口，只删除失败或带错误的记录。

## 前端设计

### 任务看板

修改 `OpenList-Frontend/src/pages/manage/subscription/TransferTasks.tsx`：

- 删除订阅列表及其内嵌的逐集来源区域。
- 保留统计卡、筛选和最近执行记录；新增订阅下拉筛选。
- 将“已转存文件”卡替换为“新增文件”，值为当前筛选范围的新增总数；保留“变更文件”卡。
- 最近执行表只渲染 `view=changes` 数据，行内显示后端返回的订阅名称。
- 使用 Hope UI 的受控浮层/抽屉实现固定在右下角的异常药丸：收起时只有异常数量，展开后显示异常摘要和清除按钮。

### 订阅详情

修改 `OpenList-Frontend/src/pages/home/SubscriptionManagement.tsx`：

- 每张订阅卡片增加“查看详情”按钮。
- 点击时请求 `episode_sources`，用 Hope UI Modal 展示按季切换的紧凑表格。
- 表格列为集数、状态、当前来源、执行节点、已更新时间。
- 当前来源单元格显示 `TG/PS`、来源盘和文件名；文件名使用 `share_url` 作为新标签页链接。

扩展 `src/types/subscription.ts` 和 `src/utils/api.ts`，加入来源详情响应、运行记录视图参数和请求函数。新增中文、繁中和英文翻译键。

### 集群控制

扩展 `src/pages/home/HomeAppSidebar.tsx`、`Layout.tsx`：

- 新增 `cluster_control` 首页页面键和侧边栏父入口。
- 新建首页嵌入式集群容器组件，加载 `clusterGetConfig()` 的 `active_role`，计算可见 Tab 和默认 Tab。
- 复用现有 `pages/manage/cluster/Overview.tsx`、`Nodes.tsx`、`Jobs.tsx`、`Settings.tsx` 的数据组件；必要时为其添加嵌入模式以避免管理端标题和首页标题重复。
- 集群控制中的“任务看板”挂载重构后的 `TransferTasks`。

## 数据流

```text
发现并选择分享文件
  -> standalone 入队转存任务 / cluster 创建媒体任务
  -> 成功创建任务后覆盖 SubscriptionEpisodeSource
  -> cluster 任务指派时由 ClusterJob / ClusterJobAttempt 提供 Worker
  -> 订阅详情接口返回每集当前来源投影

订阅运行完成
  -> view=changes 供主表与新增/变更统计使用
  -> view=failures 供右下角异常药丸使用
```

## 验证

- Go 单元测试：来源快照在 standalone 和 cluster 成功建任务时写入，纯发现和失败建任务不写入；换源覆盖同一集；Worker 名称按当前指派和最近 attempt 回退。
- Go 单元测试：`changes` 排除纯转存成功，`failures` 仅返回失败或错误记录，订阅筛选对两个视图有效，异常清理只删除失败视图数据。
- Go handler 测试：来源详情接口、运行视图参数和错误参数校验。
- 前端验证：`pnpm lint`、`pnpm build`；以浏览器验证四种角色的 Tab 集合、任务看板筛选与异常药丸、订阅详情文件名直链和 Worker 显示。
- 前端构建产物需同步到后端仓库的 `public/dist` 后再运行相关 Go 构建验证。

## 非目标

- 不回填旧订阅的来源快照；旧任务会在下一次成功创建转存任务后开始显示当前来源。
- 不改变现有分享来源优选规则、网盘转存逻辑或集群调度算法。
