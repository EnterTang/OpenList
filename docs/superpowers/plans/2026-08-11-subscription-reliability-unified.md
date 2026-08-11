# Subscription Reliability and Direct Share Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan.

**Goal:** 在不重复已有可靠性修复的前提下，把订阅任务、115/115-sy、123 和光鸭驱动、Worker 能力检查和任务补偿统一成可确认、可分类、可恢复的执行链路，确保“扫描成功”不会再被误报为“文件转存成功”，且所有可恢复错误都能被自动或人工补偿。

**Architecture:** 保留现有 share.inspect -> share.batch -> media.transfer 任务模型和 internal/115sy 独立客户端；新增 Provider 无关的请求结果/重试、直链下载、秒传预检、远端结果确认、按账号健康状态、订阅条目补偿元数据和可审计运行摘要。订阅任务新增 direct-download-first 能力矩阵：115、123、光鸭等 Provider 只有在实现并通过对应能力测试后，才由 Worker 优先获取分享临时直链并下载到本地/目标存储；直链路径确认不可用且错误满足回退条件时，才创建 share-save/transfer 补偿任务。

**Tech Stack:** Go, GORM, PostgreSQL/SQLite, net/http, golang.org/x/time/rate, OpenList cluster coordinator/worker protocol, httptest.

## Global Constraints

- 不修改远端环境、生产数据库或任务状态；本计划只定义代码、迁移、测试和灰度步骤。
- 保留 drivers/115 的现有行为，先在 internal/115sy 和 drivers/115_share 完成统一能力，再按 feature flag 迁移订阅路径。
- 不复制随机 X-Forwarded-For、Client-IP、无退避死循环等所谓“防风控”做法；使用真实客户端 profile、按 115 账号/Provider 的全局限流、有限退避和明确错误分类。
- 不对非幂等的 115 转存 POST 请求做盲目自动重放；“请求已接受但结果未知”必须先探测目标状态，再决定是否补偿。
- 订阅的 direct-download-first 只在完成 HTTP contract test、目标存储写入测试和受控真实账号验证后才可成为默认行为；实验阶段必须保留关闭开关。
- 直链 URL 只在 Worker 内存中短暂存在，不进入 coordinator payload、数据库、日志、错误摘要、幂等键或重试消息；重试时重新请求直链。
- 直链下载成功与 share-save 转存成功都是“已交付”，但必须持久化 delivery_mode，不能把两者混写成无法审计的 transferred。
- 直链下载失败回退到转存只允许发生一次逻辑切换；回退前必须检查目标存储的 operation key、大小/hash 和已有 manifest，避免重复写入或重复转存。
- 123 与光鸭不能直接套用 115 的接口语义：每个 Provider 必须独立实现分页、秒传、分享转存、直链、token 刷新和业务错误映射，并通过能力声明进入调度。
- “秒传成功”必须同时由 Provider 的明确复用结果和目标文件 ID/状态证明；仅请求成功、空 upload key 或返回 task ID 不能作为成功依据。
- 所有 schema 变更使用向后兼容的增量字段和索引；不得删除历史任务、条目或错误记录。
- 认证 Cookie、refresh token、分享提取码、签名和下载 URL 不进入日志、错误摘要、幂等键或指标标签。
- 115 的错误分类和重试策略必须有单元测试；无法通过真实账号验证的接口行为，使用可替换 HTTP client 和明确的集成验证清单，不凭报告推断为已验证。

## Current Baseline

基线检查已执行：

    go test ./internal/115sy ./internal/subscription ./internal/cluster/... -count=1

当前分支已具备、后续任务不得重复实现的能力：

- internal/subscription/execution_status.go 已按条目最终状态聚合订阅状态；
- internal/subscription/reconcile.go 已修复条目与集群任务的双向残留，并处理失联租约；
- internal/cluster/coordinator/service.go 已有有限重试、过期租约回收和失败项重试入口；
- internal/subscription/share_115.go 已有进程级 115 限流、Retry-After、429/5xx/网络错误重试以及 401/403/405 不盲重试；
- internal/cluster/subscription_dispatcher.go、internal/cluster/worker/service.go 已实现 share-save singleflight、批次上下文复用和独立文件传输；
- 多集文件命名和批次 ID 的前置修复已经进入当前分支。

因此本计划是“统一硬化后续计划”，主要解决以下仍未闭合的问题：

1. 115 /share/receive 返回成功并不等于目标文件已经可见，当前 WaitSaveComplete 对 115 是 no-op。
2. _115sy 与 drivers/115_share 的请求、错误、限流、临时直链逻辑仍未完全统一；直接分享下载仍依赖旧 SDK。
3. Worker 只依据在线和静态能力判断，无法稳定识别 115 refresh token 失效、能力过期或需重新授权。
4. 失败条目没有任务记录时，现有重试入口只报告 Unmatched，不会根据已持久化的源信息重建任务。
5. 运行记录缺少发现、派发、成功、跳过、阻塞、结果未知、可重试失败等独立统计。
6. S03E23E24 类文件需要从“避免 Episode 0”进一步变成可审计的多集/待确认结果。

## Phase 0 — Freeze Contracts and Add Feature Gates

### Task 0.1: Define rollout configuration

**Files:**

- internal/conf/config.go
- internal/conf/ 中现有订阅、集群和 115 配置定义文件
- docs/superpowers/specs/2026-08-06-115-sy-design.md

**Changes:**

- 增加安全默认值：subscription.result_confirmation_enabled=false、subscription.provider_health_required=false、subscription.direct_download_first_enabled=false、subscription.direct_share_link_enabled=false、subscription.max_reconcile_attempts=3。完成各 Provider 的能力验证和灰度后，将 direct_download_first_enabled 的目标默认值改为 true；不支持直链的 Provider 自动保持 transfer 路径。
- 增加 115 请求参数：max_attempts=3、base_delay=1s、max_delay=30s、account_interval=1s，以及确认超时和轮询间隔。
- 配置校验拒绝负数、过小的限流间隔、超过上限的重试次数和不合理的确认超时。
- 明确 direct_share_link 是底层能力开关；订阅使用 direct_download_first_enabled 控制“先直链下载、失败后转存”，不能仅打开底层直链能力就改变订阅语义。
- 在 115-sy 设计文档补充 profile fallback、结果确认、健康探测和“不做随机 IP 伪造”的约束。

**Tests:**

- 配置默认值、边界值和非法值校验测试。
- 旧配置反序列化测试，证明缺少新字段时行为保持兼容。

## Phase 1 — Unify 115 Response Metadata, Error Classification and Retry

### Task 1.1: Upgrade _115sy response metadata

**Files:**

- internal/115sy/errors.go
- internal/115sy/request.go
- internal/115sy/endpoints.go
- internal/115sy/request_test.go
- internal/115sy/errors_test.go

**Changes:**

- 新增不可变 ResponseMeta：StatusCode、ContentType、BodyKind、BodyLength、RetryAfter、Endpoint、Profile。
- HTTPError 携带 ResponseMeta；JSON 解码失败改为明确的 protocol/parse 类错误，不再伪装成 NetworkError。
- BodyKind 至少区分 json、html、empty、text、binary；只保存有限响应摘要，禁止保存原始 Cookie、URL 查询参数和完整 HTML。
- 将 401/403、签名/凭证失效、分享无效、配额不足、重复接收、405、429、5xx、超时、断连映射到稳定错误码和 RetryDisposition：
  - retry_after：429、网络超时、连接重置、408、5xx；
  - fallback_profile：当前 profile 返回 405 且 operation 配置了 fallback；
  - reauthorize：401/403、refresh token 无效、签名失效；
  - blocked：没有兼容 Worker、Provider 被限流或健康探测过期；
  - terminal：分享失效、取消、参数错误、内容解析歧义；
  - result_unknown：非幂等请求返回无法确认的响应或连接在服务端可能已接受后断开。
- profile fallback 限定为 operation policy 允许的有限次数；fallback 失败后不得继续轮换 profile 形成循环。

### Task 1.2: Add one bounded retry policy

**Files:**

- internal/115sy/request.go
- internal/115sy/limiter.go
- internal/115sy/client.go
- internal/115sy/request_test.go

**Changes:**

- 定义 RequestPolicy，明确 Idempotent、MaxAttempts、RetryOnStatus、FallbackProfiles 和操作名。
- 每次尝试都先经过同一账号 limiter；分页请求额外经过 serialized page gate/cooldown；禁止每个任务单独创建 limiter。
- 使用有上限的指数退避和小幅 jitter，优先尊重合法 Retry-After；上下文取消立即终止等待。
- GET/HEAD/列表/直链解析可使用有限自动重试；share/receive、删除、移动等非幂等操作默认不自动重放。
- 将 internal/subscription/share_115.go 的请求重试逻辑提取为可复用策略或适配到同一错误分类，避免两个路径定义不同的 429/HTML/405 语义。
- 保持每秒最多约 1 次的初始保守策略，但让 limiter 按 provider + account fingerprint 共享，而不是按任务或 Worker 独立限流。

**Tests:**

- 429 + Retry-After、429 无 header、408、5xx、超时、连接重置的次数和延迟测试。
- 401、403、405、分享失效、业务拒绝不重试测试。
- receive 请求在连接中断后必须返回 result_unknown，不能自动重复 POST。
- 多 goroutine 共享同一账号 limiter 的时序测试；不同账号互不阻塞测试。
- HTML 405、2xx HTML、JSON 错误和空 body 的响应元数据测试。

## Phase 2 — Add 115 Share Snapshot/Receive Confirmation

### Task 2.1: Extend the share-save contract without breaking providers

**Files:**

- internal/subscription/share_provider.go
- internal/subscription/share_save.go
- internal/subscription/share_115.go
- internal/subscription/share_save_test.go

**Changes:**

- 保留现有 ShareProvider 接口；新增可选 ShareSaveConfirmer，避免一次性破坏 123/189/光鸭等 Provider：

      type ShareSaveConfirmation struct {
          OperationKey string
          RequestedIDs []string
          ConfirmedIDs []string
          UnknownIDs   []string
      }

      type ShareSaveConfirmer interface {
          ConfirmSavedItems(ctx context.Context, request ShareSaveRequest) (ShareSaveConfirmation, error)
      }

- 将 SaveShareItems 的返回值扩展为可选 durable operation reference；旧 Provider 继续使用兼容适配器。
- 对每个 save operation 保存不含提取码的 OperationKey、源文件标识、目标目录绑定和 provider account fingerprint。
- 115 share/receive 返回接受后，不直接写入 transferred；在结果确认开启时进入 confirming/unknown 流程。

### Task 2.2: Implement bounded remote confirmation for 115

**Files:**

- internal/subscription/share_115.go
- internal/subscription/share_save.go
- internal/cluster/worker/service.go
- internal/cluster/protocol/payloads.go
- internal/cluster/worker/service_test.go
- internal/subscription/share_115_test.go

**Changes:**

- 实现 115 的 WaitSaveComplete 或等价 confirmer：在目标目录轮询已保存条目，使用稳定的文件 ID、名称、大小、hash/source marker 组合确认；不得仅凭 HTTP 200 或 state=true 确认。
- 确认超时返回 share_save_result_unknown，保留 operation key 和 checkpoint，让后续 reconcile 先探测、再决定是否补偿。
- Worker 重启或租约回收后，先检查目标目录和 durable manifest；已存在的文件标记为成功，未确认的文件进入 retry_wait/unknown，不重新扫整份分享树。
- 一个批次内保持独立结果：已确认成功的文件不随失败文件重复接收；失败/未知文件保留独立 retry set。
- 只有确认目标中不存在且上次请求确定未被服务端接受时，才允许重新 receive；连接在 POST 后断开时不得直接判定未执行。

**Tests:**

- receive accepted、目标立即可见；
- receive accepted、目标延迟可见；
- receive accepted、超时不可见并生成 result_unknown；
- POST 后网络中断，reconcile 发现目标已存在时不重复 receive；
- 批次 100 个文件中 5 个成功、3 个失败、2 个未知时，重试只包含 5 个未完成文件。

## Phase 3 — Direct Download First with Share-Save Fallback

### Task 3.1: Add direct share URL to internal/115sy

**Files:**

- internal/115sy/share.go
- internal/115sy/endpoints.go
- internal/115sy/files.go
- internal/115sy/share_test.go
- internal/115sy/files_test.go

**Changes:**

- 增加明确命名的 ShareDownloadURL 能力，返回临时 URL、需要透传的请求头、可选过期时间和响应元数据。
- 仅使用已在本项目适配并可通过 HTTP contract test 验证的分享下载 endpoint；不要把未经本项目实测的 proapi.115.com 作为默认 endpoint。
- 直链请求沿用同一账号/provider limiter、profile fallback、有限重试和 typed errors；URL 不写入日志或持久化错误文本。
- 若上游未返回过期时间，不猜测 Expiration；调用方必须按 401/403/过期错误重新获取 URL。
- 保留个人文件 DownloadURL，清楚区分 personal-file-download 与 share-download 两种 operation profile。
- 直链解析接口只返回短生命周期的进程内结果；不得把完整 URL 传入 coordinator 或写入 ClusterJob/SubscriptionItem。

### Task 3.2: Migrate drivers/115_share through an adapter

**Files:**

- drivers/115_share/driver.go
- drivers/115_share/utils.go
- drivers/115_share/meta.go
- drivers/115_share/driver_test.go
- drivers/115_share/utils_test.go

**Changes:**

- 用 internal/115sy 的 direct-share adapter 替换 drivers/115_share 对 github.com/SheltonZhu/115driver 的新请求依赖；保留现有 Cookie/二维码登录配置兼容层。
- List 和 Link 使用同一客户端、同一限流器、同一响应错误分类；不要让列表走新 client、直链走旧 SDK。
- Link 直链失效时重新解析一次 URL；仍失败则返回明确的 link_expired、reauthorize 或 rate_limited，不循环刷新。
- 115_share 的直接播放/下载使用该 adapter；订阅的 direct-download-first 路径复用同一 client，但由 Worker 负责获取和刷新 URL。
- drivers/115 暂不切换到新 client，避免已有个人盘挂载引入非必要行为变化；稳定后再单独评估迁移。

**Tests:**

- 分享列表分页和 direct link 成功/失败测试；
- 405 profile fallback、429 backoff、HTML 响应、临时 URL 过期刷新测试；
- 提取码不出现在幂等键、日志和错误中测试；
- 旧配置登录流程回归测试。

### Task 3.3: Make subscription direct-download-first and fallback to transfer

**Files:**

- internal/model/cluster_job.go
- internal/model/subscription.go
- internal/cluster/protocol/payloads.go
- internal/cluster/subscription_dispatcher.go
- internal/cluster/worker/service.go
- internal/cluster/coordinator/service.go
- internal/subscription/service.go
- internal/subscription/share_save.go
- internal/subscription/execution_status.go
- internal/cluster/worker/service_test.go
- internal/cluster/coordinator/service_test.go
- internal/subscription/service_test.go

**Changes:**

- 增加独立的 media.direct_download job type 或等价的 SourceMode=direct_share；不要把直链下载伪装成 share-save。任务上下文只携带 share URL 的脱敏标识、file ID、目标绑定和 operation key，不携带临时直链。
- Worker 执行顺序固定为：解析分享 -> 获取临时直链 -> 流式下载/Range 下载 -> 写入本地临时文件或目标存储 -> 校验大小/hash -> durable manifest -> 更新条目为 delivered。
- 目标存储优先使用现有 upload/stream 写入能力；本地目标使用受控临时目录、空间上限、断点清理和文件原子 rename，禁止把临时文件误当最终文件。
- 直链过期时允许在同一个 direct-download job 内重新获取一次 URL；下载中断、429、408、5xx、网络超时按统一策略有限重试。达到上限后才评估 transfer fallback。
- 仅以下来源侧错误允许回退到 share-save/transfer：直链 endpoint 不支持或 405 fallback 失败、直链下载明确返回不可用/过期、受控重试后仍为源网关错误、源直链能力暂不可用但分享本身和账号仍可用。
- 以下错误不应立即回退到转存：refresh token/Cookie/签名失效、分享失效/取消、参数或内容解析错误、目标存储权限/空间/写入错误；这些错误分别进入 reauthorize、terminal 或 target_failed。
- 429/限流先进入 cooldown 和 retry_wait，不得因为直链失败立即同时发起 receive POST；冷却后仍失败才创建一次 transfer fallback。
- fallback 必须建立同一 logical task lineage，记录 fallback_reason、direct_attempts、transfer_job_id 和 delivery_mode；转存路径使用 Phase 2 的结果确认，不把 receive accepted 直接算作完成。
- direct download 成功后不得再创建 transfer job；direct download 部分写入失败时，先依据 operation key/hash 清理或续传，再决定是否 fallback，避免目标端出现重复文件。
- 在 SubscriptionItem/运行摘要中增加 delivery_mode 和可区分的结果：direct_download_succeeded、transfer_fallback_succeeded、direct_download_retryable、direct_download_blocked、target_failed。
- direct_download_first_enabled 开启后，只有明确支持 direct share download 且目标 Worker/存储满足下载与写入能力时才走直链；不支持的 Provider 继续走原有 transfer 路径。

**Required tests before changing the default:**

- mock 115 share endpoint 返回直链，mock target storage 收到完整文件，且没有调用 share receive；
- 直链返回 405/不可用、URL 过期、下载超时、5xx/429 时，分别验证有限重试和恰好一次 transfer fallback；
- 直链获取凭证失效、分享失效、目标存储无空间/无权限时，不错误触发 transfer fallback；
- 下载中途断开、目标已写入部分内容、Worker lease 过期和进程重启后，按 operation key/hash 恢复或清理，不产生重复文件；
- 直链成功、转存回退成功、两条路径都失败时，订阅状态和运行统计分别准确显示；
- direct URL 不出现在 job payload、数据库、日志、指标标签和错误文本；
- 未实现或未通过 direct share download contract test 的 Provider 保持原有 transfer 行为；
- 至少一次受控真实 115 分享验证：小文件直链下载、目标写入、URL 过期刷新和 transfer fallback；未通过前不修改生产默认值。

## Phase 4 — Worker 115 Account Health and Capability Gating

### Task 4.1: Add provider-account health to inventory

**Files:**

- internal/cluster/protocol/payloads.go
- internal/cluster/worker/inventory.go
- internal/cluster/worker/provider_inventory.go
- internal/cluster/provider_inventory.go
- internal/cluster/worker/inventory_test.go

**Changes:**

- 在现有 provider inventory 中增加按 account fingerprint 聚合的健康字段：CredentialState、HealthState、CheckedAt、NextProbeAt、LastErrorCode、LastError、SupportedOperations。
- 健康状态至少包括 ready、degraded、reauthorization_required、rate_limited、offline、unknown。
- Worker inventory refresh 对 115/115-sy 使用安全轻量探测：只验证 profile、凭证和必要的分享读取能力，不执行 receive、删除、移动或大文件下载。
- refresh token 无效必须把 share.save 从可用能力中移除，并携带 reauthorization_required；不能仅因为 Worker 在线就继续接收任务。
- 健康探测有 TTL 和冷却时间，失败时不能每个任务都立即触发一次探测；探测本身也必须走账号 limiter。
- 同一 115 账号在多个 Worker 上共享限流/健康语义；不同账号可以独立执行。

### Task 4.2: Make coordinator scheduling health-aware

**Files:**

- internal/cluster/coordinator/service.go
- internal/cluster/coordinator/selector.go
- internal/cluster/coordinator/service_test.go
- internal/model/cluster_job.go

**Changes:**

- 选择 Worker 时同时检查 required capabilities、account fingerprint、health state 和 freshness；stale/reauthorization Worker 不得获得 115 share-save job。
- 增加明确的 blocked job 状态，或在兼容迁移阶段使用 retry_wait + LastErrorCode=worker_unavailable；新代码必须统一选择一个路径并更新状态聚合，禁止长期保持无原因的 queued。
- 无兼容 Worker 时记录阻塞原因、首次阻塞时间、下一次探测时间和可恢复条件；能力恢复后自动 requeue，不需要重新扫描订阅。
- no compatible worker 不计为源文件失败；只有超过配置的阻塞时长并经人工策略确认，才允许进入 dead-letter/人工处理报表。
- 在 lease、worker disconnect、inventory refresh 后都触发一次受控 re-evaluation，防止任务永远等待旧能力快照。

**Tests:**

- 在线但 refresh token 失效的 Worker 不被选择；
- 无兼容 Worker 的任务进入 blocked/retry_wait，并在能力恢复后自动重入队；
- 115 Worker 和非 115 Worker capability 隔离；
- 健康状态过期不会被静态 work 状态覆盖；
- 多 Worker 同账号不产生超过账号限流的并发。

## Phase 5 — Durable Item Retry and Missing-Job Compensation

### Task 5.1: Add item-level compensation metadata

**Files:**

- internal/model/subscription.go
- internal/db/migration.go
- internal/db/subscription.go
- internal/subscription/service.go
- internal/subscription/reconcile.go
- internal/subscription/reconcile_test.go

**Changes:**

- 在 SubscriptionItem 增加向后兼容字段：LastErrorCode、RetryCount、RetryAt、BlockedReason、OperationKey、StateVersion；为常用查询增加组合索引。
- 保留 LastError 供人读，使用 LastErrorCode 驱动策略；禁止通过错误文本匹配决定是否重试。
- 状态转换统一通过带版本检查的事务函数，避免并发 reconcile、retry、worker result 互相覆盖。
- 记录 retryable failure、result unknown、blocked、terminal failure、skipped duplicate、unmatched source 的原因和时间。

### Task 5.2: Rebuild tasks for failed items without job records

**Files:**

- internal/cluster/coordinator/service.go
- internal/cluster/subscription_dispatcher.go
- internal/cluster/coordinator/recovery.go
- internal/cluster/coordinator/service_test.go

**Changes:**

- 改造 RetryFailedSubscriptionItems：当条目没有 ClusterJobID 但仍有完整 SourceURL、FileID、目标绑定、文件 identity 和 workflow version 时，使用与首次派发相同的 dispatcher 构造新的逻辑任务。
- 新任务保存 RetryOfJobID/LogicalTaskKey/OperationKey lineage；每次 retry 使用新的 job/attempt ID，但同一文件的逻辑幂等键保持稳定。
- 重建前先执行 idempotency lookup 和目标状态 probe，避免历史脱链记录被全部重复转存。
- 源信息不完整时标记 unmatched_source，加入人工补偿报告，不伪造 queued 任务、不重新扫描整个订阅。
- 只重试失败/未知/阻塞文件；已 transferred 和明确 skipped 的文件不得被重建。
- 自动补偿增加每条目最大次数、冷却时间和全局并发上限；达到上限进入人工队列。

### Task 5.3: Schedule reconciliation and repair safely

**Files:**

- internal/cluster/runtime.go
- internal/subscription/scheduler.go
- internal/repair/subscription_status.go
- cmd/repair_subscription_status.go
- internal/repair/subscription_status_test.go

**Changes:**

- 将条目无任务、任务无条目、状态与任务不一致、超时 transferring/notifying/running 纳入周期性、幂等 reconcile；每次只处理有限批量，避免全表锁。
- repair 命令支持 --dry-run、按订阅 ID、按错误码、按时间范围过滤，并输出可重试/阻塞/终态/缺少源信息数量。
- transferred 条目只在目标确认或 durable manifest 证明成功时修复为成功；不能用“任务 completed”单字段覆盖远端事实。
- notifying 只在通知任务完成或超过租约并明确可重放时回收；通知失败不得把文件重新标成 transfer failed。
- 维护删除订阅/条目后的孤儿任务策略：取消后保留审计记录，不再重新调度。

**Tests:**

- 21 个无任务 pending 条目可以重建；
- 110 个无任务 transferring 条目被安全回收；
- 任务成功/条目失败、任务失败/条目 transferring、任务存在但条目删除等冲突都有确定性结果；
- dry-run 不写库，apply 只写预期行；
- 并发执行两次 reconcile 不产生重复任务。

## Phase 6 — Separate Run Stages and Report Truthful Counts

### Task 6.1: Extend subscription run projection

**Files:**

- internal/model/subscription.go
- internal/db/migration.go
- internal/subscription/service.go
- internal/subscription/execution_status.go
- internal/subscription/execution_status_test.go

**Changes:**

- 在 SubscriptionRun 增加 additive 统计：DiscoveredCount、DispatchedCount、SucceededCount、SkippedCount、RetryableCount、BlockedCount、UnknownCount、FailedCount，以及 DiscoverStatus、DispatchStatus、TransferStatus、CompletionState。
- 保留旧 TransferredCount/QueuedCount 字段，定义兼容映射和 API 文档，避免旧客户端反序列化失败。
- CompletionState 至少区分 scanning、dispatching、transferring、blocked、completed、completed_with_skips、failed、partial_failed、unknown。
- 订阅整体只有在所有需要执行的条目已确认 transferred 或明确 skipped，且没有 retryable/blocked/unknown 条目时才显示成功。
- 发现成功只更新 discovery stage；派发成功只更新 dispatch stage；两者都不能单独把 transfer stage 写成成功。
- 将重复跳过、来源失效、解析歧义、实际失败、未派发、结果未知分别计数。

### Task 6.2: Expose the projection through existing APIs

**Files:**

- server/handles/subscription.go
- server/handles/subscription_test.go
- internal/subscription/api_types.go（如当前 API 类型集中在该文件）

**Changes:**

- 在现有订阅运行和条目 API 中增加字段，保持旧字段和旧状态值可读。
- 返回 last_error_code、retry_at、blocked_reason、result_unknown 等机器字段，让 UI 不必解析错误文本。
- 增加按订阅/条目执行 retry、reconcile、probe 的权限校验和审计记录；请求必须幂等，不能绕过最大重试次数。
- 若前端不在本仓库，仅完成后端契约、OpenAPI/接口文档和兼容测试，不假设已完成 UI 展示。

**Tests:**

- 旧 API 响应字段兼容测试；
- 新统计与条目状态的一致性测试；
- 无权限、重复 retry、达到最大次数和 dry-run 请求测试。

## Phase 7 — Make Multi-Episode and Identity Outcomes Auditable

### Task 7.1: Preserve episode ranges and ambiguous parses

**Files:**

- internal/media/release/release.go
- internal/media/release/ 相关测试文件
- internal/model/subscription.go
- internal/subscription/share_inspect.go
- internal/subscription/share_save.go
- internal/subscription/share_save_test.go

**Changes:**

- 在现有多集解析修复基础上，显式保存 EpisodeStart、EpisodeEnd 或 episode list，以及 EpisodeParseStatus。
- 对 S03E23E24 生成两个可审计的 episode identity；无法确认的文件进入 parse_ambiguous/人工确认，不再以 Episode 0 参与普通单集转存。
- identity key 同时包含订阅、季/集范围、文件 hash/size 或稳定 source ID；避免多集文件与单集文件发生幂等冲突。
- 运行摘要将 duplicate、not_found、parse_ambiguous 和 transfer_failed 分开统计。

**Tests:**

- S03E23E24、S03E23-24、多种分隔符、电影/特别篇和无集数文件测试；
- 多集文件重试不重复创建单集任务测试；
- 解析歧义不会进入 Episode 0 转存路径测试。

## Phase 8 — Observability, Alerts and Operational Reports

### Task 8.1: Add structured execution events and metrics

**Files:**

- internal/115sy/request.go
- internal/subscription/share_115.go
- internal/cluster/coordinator/service.go
- internal/cluster/worker/service.go
- internal/cluster/worker/inventory.go
- internal/repair/subscription_status.go
- existing metrics/logging packages under internal/

**Changes:**

- 记录 provider、account_fingerprint、operation、endpoint、profile、http_status、content_type、error_code、retry_disposition、attempt、delay_ms、worker_id、job_id、item_id。
- 账号 fingerprint 使用不可逆短标识；日志中不出现 Cookie、token、提取码、完整分享 URL、下载 URL 或原始 HTML。
- 增加 115 请求成功率和延迟、rate limit、reauthorization、405 fallback、result unknown、blocked 时长、retry success rate、dead-letter、unmatched source、订阅 completion state 等指标。
- 告警：账号进入 reauthorization、唯一可用 Worker 数为 0、result unknown 持续增长、blocked 超过阈值、同一错误码连续失败、重试队列长期不下降。

### Task 8.2: Add operator-facing repair report

**Files:**

- internal/repair/subscription_status.go
- cmd/repair_subscription_status.go
- docs/operations/subscription-reliability.md

**Changes:**

- 报告按订阅和错误分类列出成功、跳过、可重试、阻塞、结果未知、终态失败、无任务、源信息缺失。
- 提供“只探测不写库”“只重建缺失任务”“只重试明确可重试错误”“确认未知结果”四种显式动作；默认只读。
- 对 115 账号问题给出 reauthorization_required、rate_limited、method_not_allowed、source_invalid 等原因，而不是统一显示 JSON decode error。

## Phase 9 — 123 and GuangYa Provider Hardening

这一阶段不能把 123、光鸭当成“115 换一个 endpoint”。两者当前实现的完成度、鉴权方式、分享任务语义和下载防盗链约束不同，必须先建立各自的 HTTP contract test 和能力声明，再进入 direct-download-first 灰度。参考项目中可复用的是“转存后验证落盘、账号熔断、链接失效替换、可恢复任务”这些原则；不能把参考项目中的未验证 endpoint、随机请求头或合成成功状态直接复制到本项目。可作为边界参考的资料包括 [Mediary Scout](https://github.com/fancydirty/mediary-scout)、[Cloud Auto Save X](https://github.com/OzoO0/cloud-auto-save-x)、[urlDB](https://github.com/ctwj/urldb) 和 [OpenList 123 驱动文档](https://doc.oplist.org/guide/drivers/123)。

### Task 9.1: 修复 123 分享分页、目录和转存结果确认

**Files:**

- internal/subscription/share_123.go
- internal/subscription/share_save.go
- internal/subscription/share_123_test.go
- internal/subscription/share_save_test.go
- drivers/123_share/driver.go
- drivers/123_share/util.go
- drivers/123_share/types.go

**Changes:**

- 将 `ListShareChildren` 从固定第一页改为按 123 返回的 page/next/total 语义分页；每页使用 Provider limiter，按稳定 provider file ID 去重，防止分页重叠或重试造成重复条目。
- 明确处理分享根目录、子目录和嵌套目录；目录不能被当前接口支持时，必须生成逐条的 unsupported/blocked 结果，不能把整个批次伪装成成功。
- 修复 `SaveShareItems` 的逐文件结果模型：保存请求、服务端复用结果、目标 FileID、目标可见性和最终错误必须分别记录；禁止返回 synthetic task ID 作为完成证明。
- 替换 `WaitSaveComplete` 的 no-op 实现。若 123 没有可轮询的转存任务，则通过目标目录 probe、文件 ID/名称/大小/hash 或稳定 source marker 确认落盘；无法确认时返回 `result_unknown`，而不是成功。
- 复用 123 的 server-side reuse/upload request 时，非幂等请求断连必须先 probe 再决定补偿，不能盲目重放导致重复文件。
- 为 123 share client 增加 public-share/no-credential 与 authenticated-share 两种明确 profile；修复 `Pan123Share.Init` 的 TODO 行为，不能在未验证登录/刷新能力时把客户端宣称为 ready。
- 123 分享直链解析应复用 `/share/download/info` 的参数解码和 redirect 解析逻辑，但必须把直链限定为短生命周期结果，并在请求中保留已验证的 Referer/必要 header；不伪造来源 IP，不记录完整 URL。

**Tests:**

- 多页分享、分页重叠、重复 file ID、嵌套目录和分享失效测试；
- 公开分享无账号获取直链、需要提取码、鉴权失效和 anti-leech 拒绝测试；
- 每文件 reuse 成功/失败/结果未知，目标已存在时不重复提交测试；
- upload request 在服务端可能已接受但客户端断连时，不重复 POST 的 probe 测试；
- 直链 URL 不出现在日志、幂等键、任务 payload 和持久化错误摘要中。

### Task 9.2: 修复 123 秒传真实性和下载恢复

**Files:**

- drivers/123/driver.go
- drivers/123/download_reader.go
- drivers/123_open/driver.go
- drivers/123/*_test.go
- internal/subscription/share_123.go
- internal/subscription/share_provider.go

**Changes:**

- 删除 `Reuse || Key == ""` 即视为成功的宽松判断；只有明确 `Reuse=true` 且返回有效 FileID、或普通上传完成并通过最终文件 probe，才能写入 `instant_upload_succeeded`。
- 统一 123 与 123 Open 的 SHA1/MD5/etag 预检语义，明确 fingerprint 缺失、哈希不匹配、空间不足、账号无权限和服务端复用拒绝的错误码；不能把空 key、空 task ID 或 HTTP 200 当作秒传成功。
- 复用已有 RangeReader 的短读、EOF、URL 刷新和有界重试能力，但将 123 账号 limiter、Referer、URL 过期错误和 retry-after 纳入同一 RequestPolicy。
- 秒传成功后再执行目标文件存在性和大小/hash 校验；校验失败进入 `result_unknown` 或 `instant_upload_failed`，按幂等 key 补偿，不重复上传完整文件。
- 建立账号 fingerprint + size + hash 的短期预检缓存；缓存不得保存分享提取码、临时 URL 或跨账号复用结果。

**Tests:**

- SHA1 复用成功、MD5/etag fallback、无复用转普通上传、hash 不匹配、空 key/空 FileID、最终 probe 失败测试；
- 断点下载短读、EOF、401/403 URL 刷新、429/5xx/网络断开和达到最大重试次数测试；
- Worker 重启或任务 lease 过期后，秒传不会重复产生文件或错误标成成功。

### Task 9.3: 为光鸭建立分享、直链和转存的独立契约

**Files:**

- drivers/guangyapan/driver.go
- drivers/guangyapan/share.go
- drivers/guangyapan/offline.go
- drivers/guangyapan/types.go
- drivers/guangyapan/share_test.go
- internal/subscription/share_guangyapan.go
- internal/subscription/share_save.go

**Changes:**

- 为光鸭 `postAPI` 和 `postShareAPI` 引入统一 ResponseMeta/typed error：区分 401/403、429、408、5xx、网络错误、分享失效、配额不足、重复任务和业务拒绝；错误摘要只保留脱敏后的有限内容，不返回完整原始响应。
- 将当前按 endpoint 的 500ms limiter 升级为 Provider/account 级 limiter，endpoint gate 作为额外约束；所有分享列表、分享转存、离线任务、直链解析、任务轮询和上传接口必须经过统一策略。
- 401/403 只允许一次受控 token refresh；刷新仍失败时将账号标记为 `reauthorization_required`，不得在每个任务中重复刷新。429/408/5xx/网络错误只对幂等 GET、列表和状态轮询有限退避；非幂等 restore/create 在结果未知时先 probe，不盲目重放。
- `RestoreShare` 返回空 task ID 时不得直接视为同步完成；改为目标目录 probe，确认文件可见后才成功，否则返回 `result_unknown` 或明确业务失败。
- `ListShareFiles` 保留分页，但需校验 page total/next 语义、去重、排序稳定性和 access token 短 TTL 缓存；提取码只能在内存和请求范围存在。
- `WaitShareRestoreTask` 使用有界指数退避和 jitter，保存 task lineage、最后状态、轮询截止时间；share task status 不可用时的 personal task fallback 必须有明确证据，不能把任意 200 当完成。
- 增加光鸭分享直链能力时，必须先通过真实 endpoint contract test 验证“分享 access token + file ID -> 临时下载 URL”的完整链路；未验证或不支持时能力声明保持 false，订阅继续使用 transfer 路径。临时 URL 只能在 Worker 内存中使用，并处理 Referer、过期刷新和断点下载。

**Tests:**

- 分享多页列表、access token 过期、401 refresh 一次后成功/失败、429 Retry-After、5xx/网络断开测试；
- restore 返回有效 task ID、空 task ID、服务端已落盘但客户端断连、任务失败和超时测试；
- 直链 endpoint 成功、403 anti-leech、URL 过期刷新、Range 下载、目标写入失败和 URL 泄漏测试；
- 不支持直链时自动回到 transfer，且不会影响 123/115 已验证能力。

### Task 9.4: 修复光鸭秒传、离线下载和普通上传的结果语义

**Files:**

- drivers/guangyapan/driver.go
- drivers/guangyapan/offline.go
- drivers/guangyapan/types.go
- drivers/guangyapan/*_test.go
- internal/cluster/worker/service.go
- internal/subscription/share_guangyapan.go

**Changes:**

- `getUploadToken` 增加 Provider 支持的 fingerprint/GCID 预检；只有服务端明确返回可复用状态、有效 task/file ID 并且最终 probe 通过，才记录 `instant_upload_succeeded`。如果当前 endpoint 不支持 GCID，必须显式记录 `instant_upload_unsupported`，不能伪造秒传。
- 保留光鸭现有 code 156 快速路径，但校验 task ID、任务终态、目标 FileID 和文件大小/hash；空 task ID、未完成 task 或仅返回 code 156 均不得算成功。
- 普通 OSS 分片上传增加每片有限重试、上传 checkpoint、重启恢复和最终合并/文件存在性确认；非幂等 complete 请求断连后先 probe 再补偿。
- 离线下载统一为 resolve -> create -> poll -> confirm 状态机；保存 provider task ID 与 logical operation key，重复执行先查询已有任务，避免同一资源重复创建离线任务。
- 离线任务的 401/403、429、5xx、网络错误和业务拒绝使用与分享转存一致的错误分类；达到最大次数后进入 blocked/reauthorization/terminal，而不是长期 running。
- 若光鸭分享直链测试未通过，订阅仍可以使用“分享转存 + 结果确认”，但不允许把该路径误报为 direct download。

**Tests:**

- code 156 + 有效 task/file ID + 最终 probe 成功；code 156 但空 task、任务失败、probe 不可见；
- GCID/指纹可复用、不可复用后普通上传、分片重试与 checkpoint 恢复测试；
- 离线任务重复提交、服务端已创建但客户端断连、状态轮询超时和账号失效测试。

### Task 9.5: Register 123/GuangYa capabilities and account health

**Files:**

- internal/cluster/protocol/payloads.go
- internal/cluster/worker/provider_inventory.go
- internal/cluster/worker/inventory.go
- internal/cluster/coordinator/selector.go
- internal/cluster/coordinator/service.go
- internal/cluster/worker/inventory_test.go
- internal/cluster/coordinator/service_test.go

**Changes:**

- 将 Provider 能力拆成 `share.inspect`、`share.save`、`share.download`、`instant_upload`、`offline_download`、`result_probe` 和 `range_download`，不再用一个 `supports_123` 或 `supports_guangyapan` 布尔值覆盖所有能力。
- 123 和光鸭分别上报 account fingerprint、credential state、health state、capability version、checked_at、next_probe_at 和 last error code；能力必须有 TTL，过期后只能走已允许的降级路径。
- Coordinator 选择任务时同时校验 Provider、账号、能力组合和健康状态：例如光鸭只有 share.save 没有 share.download 时，仍可接收 transfer，但不能接 direct-download job；123 直链和秒传同理。
- 任何 capability contract test 或受控真实账号验证失败，只关闭对应能力，不下线整个 Provider；验证恢复后再自动 re-enable。
- 将 123/光鸭的限流和健康 probe 按账号隔离，避免同一账号在多个 Worker 上突破速率限制；不同账号仍可并行。

**Acceptance for Phase 9:**

- 123 不再固定只读第一页，目录/文件和重复条目均有确定性结果；分享转存只有目标可见或明确可重试/未知时才结束。
- 123 秒传不会因空 key 或 HTTP 200 被误标成功；直链下载能在 URL 过期时有界刷新，失败后最多创建一次 transfer fallback。
- 光鸭的 share list、restore、share direct link、instant preflight、offline task 和 multipart upload 均有独立状态和错误码；空 task ID 不代表成功。
- 123/光鸭的 401/403、429、5xx、网络和业务错误按策略处理，账号失效会触发健康门禁而不是任务风暴。
- 未实现或未通过验证的能力保持 disabled，并自动选择可验证的 transfer fallback；能力升级不会改变已完成任务的 delivery_mode。

## Phase 10 — Rollout, Migration and Acceptance

### Task 10.1: Add additive migrations and backfill

**Files:**

- internal/db/migration.go
- internal/db/subscription.go
- migration tests/fixtures under internal/db/
- docs/operations/subscription-reliability.md

**Changes:**

- 先新增字段和索引，再部署代码，再小批量 backfill；backfill 依据现有 job/item/manifest 事实，不凭旧 SubscriptionRun.Status 猜测成功。
- 将历史 405 HTML、401/refresh token、429、无兼容 Worker、source invalid 映射到新错误码；无法判断的记录标成 legacy_unknown，进入人工报表。
- 对现有无任务 pending 条目生成可重建候选；对 transferring 无任务条目只生成修复报告，默认不直接写 transferred。
- 迁移脚本支持断点、批量大小和 dry-run；失败可重复执行。

### Task 10.2: Execute staged rollout

**Order:**

1. _115sy response metadata/error tests and shadow logging；
2. 115-sy direct share URL and provider health probe；
3. drivers/115_share adapter with old driver fallback；
4. 123 share pagination/save confirmation/direct-link contract tests；
5. GuangYa share retry/save confirmation/instant-upload/direct-link contract tests；
6. subscription direct-download-first in shadow mode, without changing delivery；
7. subscription result confirmation and transfer fallback in shadow mode；
8. enable direct-download-first for one validated subscription/provider；
9. enable health gating for all validated direct-download and share-save workers；
10. enable missing-job compensation and automatic retry only for classified retryable errors；
11. remove old provider request paths only after an observation window and rollback evidence。

**Rollback:**

- 关闭 feature flag 即停止 direct-download-first、新确认、健康门禁和新 direct-share adapter；
- 保留新错误码、attempt、operation key 和 repair report 数据，不删除；
- 旧任务继续由兼容 adapter 读取；
- 不把新状态强行转换为旧的 success，回滚只改变执行路径，不篡改历史事实。

### Acceptance Criteria

- 任何订阅 run 的 success 都能由条目状态、目标确认或 durable manifest 证明；仅扫描/派发成功不能产生 transfer success。
- 115 429/408/5xx/网络错误按账号 limiter 和有限退避重试；401/refresh token/签名、405、分享失效、业务拒绝不会盲重试。
- 非幂等 receive 在结果未知时不会自动重复 POST；reconcile 能在目标已存在时收敛为成功。
- refresh token 失效 Worker 不接收 115 share-save 任务；无兼容 Worker 的任务可见、可恢复且不会无限 queued。
- 无任务但源信息完整的 failed/pending 条目可重建一次幂等任务；源信息缺失的条目可在报告中定位，不被静默遗漏。
- 重试只处理未完成文件，已确认成功或明确跳过的文件不重复转存。
- 订阅运行摘要能分别展示发现、派发、成功、跳过、可重试、阻塞、未知和终态失败。
- 订阅默认路径在通过 Phase 3 验证后为 direct-download-first：直链下载成功即完成交付；只有符合回退条件时才转存；两种结果通过 delivery_mode 区分。
- 直接分享下载 URL 可有限刷新，但临时 URL 不进入持久化任务或当作永久资源。
- 直链下载失败回退只能创建一次 transfer lineage；转存回退仍必须通过目标结果确认。
- 123 分享目录分页、分享转存、分享直链和秒传结果均有独立的可观测状态；不再用 synthetic task ID 或空 WaitSaveComplete 表示完成。
- 光鸭分享直链、分享转存任务、秒传预检和普通分片上传均有独立结果；401/403、429、5xx、网络错误和业务失败不会混为一个错误。
- 123/光鸭未实现 direct share download 或未通过真实受控验证时，订阅自动选择 transfer，不影响已支持 Provider。
- S03E23E24 等多集内容不会再以 Episode 0 进入普通单集任务。
- 全部新增行为有单元测试、HTTP contract test、迁移测试和至少一次真实账号的受控集成验证；未完成真实集成验证时不得宣称生产可用。

## Verification Commands

实现每个阶段后执行对应的最小测试；合并前执行：

    go test ./internal/115sy ./drivers/115_share ./drivers/123/... ./drivers/123_share/... ./drivers/123_open/... ./drivers/guangyapan/... ./internal/subscription ./internal/offline_download/... ./internal/cluster/... ./internal/repair/... -count=1
    go vet ./internal/115sy ./drivers/115_share ./drivers/123/... ./drivers/123_share/... ./drivers/123_open/... ./drivers/guangyapan/... ./internal/subscription ./internal/offline_download/... ./internal/cluster/... ./internal/repair/...
    git diff --check

如果仓库已有更窄的 CI lint/typecheck 命令，按 CI 配置补充执行；涉及数据库迁移时，必须在 SQLite 和 PostgreSQL 测试矩阵各执行一次，并验证旧 schema 数据可读。

## Delivery Order

推荐按以下四批交付，先验证各 Provider 的可确认能力和直链下载可行性，再把 validated Provider 的 direct-download-first 设为订阅默认路径：

1. **可靠性底座：** Phase 0、Phase 1、Phase 4、Phase 5。先保证错误可分类、Worker 可用性可信、缺失任务可补偿。
2. **123/光鸭 Provider 闭环：** Phase 9。分别完成分页、分享转存确认、直链 contract、秒传真实性、离线任务和账号健康门禁；未验证能力保持关闭。
3. **直链优先交付闭环：** Phase 2、Phase 3、Phase 6、Phase 8。先验证直链下载到本地/目标存储，再保证失败后只回退一次转存并准确统计结果。
4. **内容增强与生产迁移：** Phase 7、Phase 10。完善多集识别、执行真实账号验证和逐 Provider 将 direct-download-first 改为默认。

每批完成后都必须通过对应 acceptance criteria 和回滚演练，再进入下一批；不得先大规模重试历史失败数据。
