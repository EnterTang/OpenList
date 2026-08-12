# 订阅 Observation 增量检查与早闭合转存设计

## 目标

在现有集群订阅路径上，让分享元数据检查（`share.inspect`）与媒体转存（`media.transfer`）解耦等待：

- **尽快转存**：不必等本批所有 inspect 终态；槽位一旦满足闭合条件立即派发转存。
- **干净任务面**：一次扫描对应一个 observation 父任务；子 inspect 默认折叠，避免「大量已租约」刷屏。
- **Worker 公平**：按任务类型分槽限流，inspect 不被 media/batch 饿死。

本设计与 [订阅源按集预筛选](2026-07-17-subscription-source-prefilter-design.md) 叠加：选源规则（来源优先级 > 大小 > 稳定键）保持不变，改变的是 **何时允许派发**。

## 非目标

- Coordinator 本地完成 inspect、绕过 worker（曾评估方案 C）。
- 单个集群 job 内嵌多分享 listing（曾评估方案 B）。
- 改派已处于 `transferring` / `transferred` 的槽位。
- 运维侧多实例抢 coordinator lease、139 panic、cleanup 死循环等环境问题（另案处理）。

## 方案选型

采用 **父 Observation + 子 Cluster Inspect**：

| 项 | 选择 |
|----|------|
| 闸门 | 优先级闭合 + 体积门槛早闭合 |
| 任务面 | 1 父任务 + N 去重子 `share.inspect` |
| 并发 | 分类型槽位池（inspect / media / batch） |
| 体积门槛 | 可配置，默认剧集 1 GiB、电影 20 GiB；`0` 表示关闭 |

## 数据流

1. 订阅扫描收集分享链接，按分享身份去重：`provider + shareID + parentID + passcode`。
2. 创建 **observation 父任务**（UI 主入口），`ObservationExpected = 去重后子任务数`。
3. 为每个去重分享派发 **子 `share.inspect`**（`parent_job_id` 指向父任务）。
4. 每个子任务终态（成功 manifest 或失败空 manifest）触发增量消费：
   - 合并已到达候选；
   - 对每个季集/电影槽位尝试闭合；
   - 已闭合且仍为 pending 的条目立即派发 `media.transfer`。
5. 全部子任务终态后更新父任务为 `succeeded` 或 `partial_failed`，并对仍未闭合的槽位做最终选择或跳过。

```text
Scan → Parent Observation
         ├─ Child inspect #1 ──► manifest ─┐
         ├─ Child inspect #2 ──► fail/empty ┼─► incremental close slots → media.transfer
         └─ Child inspect #N ──► manifest ─┘
```

## 槽位闭合规则

对每个 `(subscription_id, season, episode)`（电影用既有 movie 槽位；无法识别集号的剧集文件维持透传、不参与闭合）：

1. 若槽位已有 `transferring` / `transferred` → 保留最早接受者；新候选 `skipped`，不派发。
2. 否则在已到达候选中按既有规则选出当前胜出源（来源优先级 > 大小 > 稳定键）。
3. **体积门槛闭合**（配置 > 0 时）：若胜出文件大小 ≥ 对应门槛 → **立即派发**。
   - 默认：剧集 `episode_min_bytes = 1 GiB`，电影 `movie_min_bytes = 20 GiB`。
   - 配置为 `0` 时跳过本条。
4. **优先级闭合**：统计仍未终态的子 inspect 来源集合；若其中没有任何来源能在优先级上击败当前胜出源 → **立即派发**。
5. 否则继续等待更多子 inspect。

### 体积门槛的取舍

同来源先到 1.2G、后到 8G 时，若已门槛闭合并进入转存，后到更大文件只记 `skipped`。这是用「更快落库」换「可能不是最大版本」；将门槛设为 `0` 可关闭该行为，同级来源须等齐后再比大小。

### 失败与空候选

- 子 inspect 失败须写入 **空 objects 的 sealed manifest**（或等价终端记录），计入 observation 进度。
- 失败不阻塞其它已闭合槽位。
- 某槽位若最终没有任何可用媒体候选 → 不派发转存，记录跳过/失败原因。

## 任务模型与 UI

- 新增父任务类型 **`share.inspect.observation`**（与现有 `share.batch` 媒体父任务区分），字段要求：
  - 绑定唯一 `observation_key`；
  - `expected_items` = 去重后子 inspect 数；
  - 子任务 `type = share.inspect` 且 `parent_job_id` 指向父任务。
- 管理端任务列表 **默认展示父任务**，进度形如 `inspect 12/13 · transferred 3`；子 inspect 折叠可展开。
- 子任务仍为完整 cluster job（可单独租约、重试、失败），但不再作为默认刷屏主体。
- 增量消费取代「等齐全部 manifest 才 Apply」；`incomplete` 仅表示「尚有子任务未终态」，**不再阻止**对已闭合槽位的派发。

## Worker 并发

在 worker 接单路径按 `job_type` 维护独立槽位（接单前拒绝超额 offer，任务回 `queued`）：

| 类型 | 默认上限 |
|------|----------|
| `share.inspect` | 4 |
| `media.transfer` | 3 |
| `share.batch` | 2 |

其它约定：

- `share.inspect` **不受** media cleanup backlog 阻塞。
- Coordinator redispatch 优先 `share.inspect`。
- 子 inspect lease 默认 30 分钟（避免 1 分钟 lease 风暴）。

上限应可配置；缺省使用上表。

## 配置

- **体积门槛**写入订阅全局配置（与现有 telegram/subscription config 同层）：
  - `episode_early_close_min_bytes` 默认 `1073741824`（1 GiB）；`0` 关闭
  - `movie_early_close_min_bytes` 默认 `21474836480`（20 GiB）；`0` 关闭
- **分型槽位**写入 cluster/worker 配置：
  - `inspect_slots` 默认 `4`
  - `media_slots` 默认 `3`
  - `batch_slots` 默认 `2`

## 与现有 P0–P2 修复的关系

本地已实现且应保留的部分：

- inspect lease 延长、失败空 manifest、cleanup 不挡 inspect、media 预留槽、redispatch 优先 inspect、prefilter 选源。

本设计在此之上进一步要求：

- **增量闭合派发**（替换「等齐整批才 Apply」）；
- **体积门槛**；
- **父/子任务与 UI 折叠**；
- **分类型硬槽位**（强化现有「预留 2 槽」为独立池）。

## 错误处理摘要

| 场景 | 行为 |
|------|------|
| 子 inspect 失败 | 空 manifest，父进度 +1，不挡已闭合槽位 |
| 迟到更优/更大且槽位已接受 | `skipped` |
| 迟到更优且槽位仍 pending 未派发 | 可替换胜出源 |
| Worker 某类型槽满 | 拒收 offer → queued → 优先 redispatch |
| 全部子终态后仍有未闭合槽位 | 用已有候选最终选择；无候选则跳过 |

## 测试计划

- 优先级闭合：已有 123 候选，剩余仅 Quark → 立即派发。
- 体积门槛：同来源先到 ≥1 GiB 剧集 → 立即派发；后到更大 → `skipped`。
- 门槛 `0`：同来源两版本须等齐再选更大。
- 失败子 inspect：12 成功 + 1 失效 → 已闭合槽位仍派发。
- 去重：同 observation 两消息同分享 → 仅 1 子 inspect。
- 父任务进度与 `partial_failed`。
- media 槽满时仍可接受 inspect。

## 验证

- 相关 Go 测试覆盖上述场景。
- 格式化与静态检查。
- 远端部署后：新订阅应出现 1 个父检查任务；高优先级/达门槛剧集在整批 inspect 完成前进入 transferring。
