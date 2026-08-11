# 115 分享转存批处理与订阅任务可靠性改造实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在不改变每个文件独立下载、移动网盘上传、进度上报和失败重试语义的前提下，使同一 115 分享在同一个目标工作节点上的一批文件只进行一次目录树遍历和一次批量转存，并补齐 115 响应诊断、任务调度和远端验收能力。

**Architecture:** 保留一个文件一个 `media.transfer` 子任务；由调度端把同一分享、同一目标 worker 和同一 staging 目标绑定为一个 share-save batch，并把批次内的源文件清单附加到每个子任务。worker 使用已有的 `findExistingStagedSource` 做幂等复用，再用现有 `pkg/singleflight` 合并并发子任务的转存阶段。115 驱动走一次 `/webapi/share/receive`，其他分享驱动继续沿用现有按 provider 分组的行为。批处理失败只影响本批次的转存阶段，不合并文件级传输状态。

**Tech Stack:** Go、现有 cluster protocol、`internal/subscription` 分享保存逻辑、`internal/cluster` 调度与 worker、现有 `pkg/singleflight`、`httptest`、Go package tests；不新增第三方依赖。

---

## 一、已确认的问题与边界

### 1. 远端失败不是单一故障

在 `192.168.1.195:25044` 的持久化任务记录中，最近失败任务同时包含以下类别：

| 类别 | 已观测现象 | 处理方向 |
| --- | --- | --- |
| 工作节点不可用 | `no compatible cluster worker is connected` 占比最高 | 调度前做能力与在线状态预检；节点离线时延迟或明确失败，不创建无法执行的任务 |
| 凭据或签名失效 | `密钥错误或签名无效` | 在订阅/分享任务进入批量阶段前校验凭据；错误归类为配置问题，避免无意义重试 |
| Telegram 外部服务超时 | `telegram request timed out` | 对可重试网络错误采用有限次数、指数退避和总超时预算；保持通知幂等 |
| 115 转存响应非 JSON | `decode 115 response: invalid character '<' looking for beginning of value` | 保留错误传播，同时补充 HTTP 状态、Content-Type、响应长度和 HTML/JSON 类型判断；在受控测试中确认实际上游响应 |
| 数据库锁竞争 | `database is locked (5) (SQLITE_BUSY)` | 对短暂锁竞争增加有上限的退避重试，并减少批量任务产生的重复写入 |
| 集群链路不可用 | `cluster transport is not connected`、`cluster coordinator is disabled` | 调度前检查 coordinator/transport；失败时记录可恢复状态，不把基础设施故障伪装成业务失败 |
| CloudDrive2 115 授权失效 | `refresh_token 无效` | 独立处理 CD2 授权续期和重新授权；不能把该节点当作可用 115 worker |
| 状态统计不一致 | 订阅 `last_status` 为成功，但文件投递阶段大量失败 | 分离“订阅发现成功”和“文件投递成功”；汇总状态必须包含文件阶段失败 |

其中，代表性订阅“超感警探”有 149 个文件在 `saving_share` 阶段失败，3 个文件成功进入移动网盘上传。由此可以确认这批故障发生在 115 分享转存阶段，不是移动网盘上传阶段。当前证据尚未包含 115 原始 HTTP 状态码和响应体，因此不能武断认定是 115 认证、反向代理还是上游接口版本问题；必须通过新增诊断信息和受控复现确认。

### 2. 当前调用链的性能放大点

- `internal/cluster/subscription_dispatcher.go` 为每个文件创建独立的 `media.transfer`，这是正确的文件级语义，应保留。
- `internal/cluster/worker/service.go` 当前每个 `media.transfer` 都可能调用 `subscription.SaveClusterShareSelection`。
- `internal/subscription/share_save.go` 每次调用都会收集分享目录树；`internal/subscription/share_115.go` 随后调用 115 的 `/webapi/share/receive`。
- 因此，目录树遍历和 115 转存请求被按文件重复放大。批量父任务目前只是 `share.batch` 的聚合关系，并没有承担实际的分享转存工作。

### 3. 最小实现边界

- 不把多个文件塞入 `media.transfer.SourceObjects`。协议当前明确要求每个 `media.transfer` 恰好一个源对象，下载、上传、manifest、进度和失败重试都依赖该语义。
- 只批处理“分享发现与转存准备”阶段；每个文件仍独立执行下载、移动网盘上传和结果上报。
- 只在同一分享、同一目标 worker、同一 staging 目标/账号绑定下合并。不同分享、不同 worker 或不同临时目录不得共用转存结果。
- 115 驱动使用一次批量 `/webapi/share/receive`；非 115 provider 默认不改变现有分组规则。
- 不在本计划内修改远端生产配置、重启容器、替换授权、执行真实分享转存；这些属于部署后的受控验收步骤。
- 不新增依赖；复用仓库已有的 `pkg/singleflight`。

## 二、实现任务

### 阶段 0：建立基线与测试夹具

- [ ] 在修改代码前确认当前工作区状态，保留已有的无关未跟踪计划文件，不覆盖用户改动。
- [ ] 运行并记录基线测试：

  ```bash
  go test ./internal/subscription ./internal/cluster/... -count=1
  go vet ./internal/subscription ./internal/cluster/...
  git diff --check
  ```

- [ ] 如果执行全量 `go test ./...`，单独记录仓库已有的 macOS `fuse.h` 或无关 vet 问题，不把基线问题归因于本改造；本计划的阻断门槛以受影响 package 的测试为准。
- [ ] 复用现有 `internal/subscription/share_115_test.go`、`internal/subscription/cluster_share_test.go`、`internal/cluster/subscription_dispatcher_test.go` 和 `internal/cluster/worker/service_test.go` 的测试夹具，不引入新的测试依赖。

### 阶段 1：先锁定分享保存批处理行为

**涉及文件：**

- `internal/subscription/share_save.go`
- `internal/subscription/cluster_share.go`
- `internal/subscription/share_115.go`
- `internal/subscription/share_115_test.go`
- 新增或扩展 `internal/subscription/share_save_test.go`

- [ ] 先写失败测试 `TestSaveShareToTempBatch_Pan115CollectsTreeOnceAndReceivesOnce`：构造包含至少 3 个文件的分享树，记录目录树请求次数、`/webapi/share/receive` 请求次数和逗号分隔的 `file_id`，断言一次批量转存包含全部选中文件。
- [ ] 先写失败测试 `TestSaveShareToTempBatch_Pan115ReusesExistingStagedFiles`：全部目标文件已存在时，断言不再次调用 115 转存接口。
- [ ] 先写失败测试 `TestSaveShareToTempBatch_Pan115RetriesAfterFailedBatch`：第一次转存返回可观察失败，第二次调用允许重新尝试，不能因为内存中的 singleflight 结果永久污染后续任务。
- [ ] 增加面向集群的批量入口，例如 `SaveClusterShareSelectionBatch`；保留现有单文件 `SaveClusterShareSelection`，由它包装一个单元素批次，避免无关调用方改动。
- [ ] 批量入口一次收集目录树，按传入的 `SourceObject.SourceFileID` 过滤目标文件，并以 `Flatten: true` 维持当前集群转存后的临时目录布局。
- [ ] 对 115 provider 在批量路径中绕过按 `parentID` 分组，调用一次 `SaveShareItems` 传入全部文件 ID；这是因为 115 的实现本身忽略 `parentID`，目标是一个分享/一个目标目录的一次转存。
- [ ] 对非 115 provider 保留现有 `shareSaveGroup{parentID,dstDirID}` 分组和调用行为；如果复用批量收集结果，只允许减少目录树重复遍历，不改变 provider 的请求边界。
- [ ] 保持空选中文件、重复文件 ID、分享目录不存在和目标临时目录错误的原有错误语义；批处理入口不得静默成功。
- [ ] 运行阶段测试：

  ```bash
  go test ./internal/subscription -run 'Test(SaveShareToTempBatch|SaveClusterShareSelection|Pan115)' -count=1
  ```

### 阶段 2：增加协议层的批次上下文，不破坏单文件语义

**涉及文件：**

- `internal/cluster/protocol/payloads.go`
- `internal/cluster/protocol/validation.go`
- `internal/cluster/protocol/validation_test.go`（如不存在则新增）
- 与任务上下文 hash/签名相关的现有协议文件及测试

- [ ] 先写 `TestTaskContextValidate_AllowsShareSaveBatchWithOnePrimarySource`：任务保留一个 `SourceObjects` 主源对象，同时携带多个 `ShareSaveObjects`，验证通过。
- [ ] 先写 `TestTaskContextValidate_StillRejectsMultiplePrimarySourceObjects`：确认原有“一个 `media.transfer` 只能有一个主源对象”的校验仍然失败。
- [ ] 为 `TaskContext` 增加可选的批次字段，建议采用 `ShareSaveKey string` 与 `ShareSaveObjects []SourceObject`，并使用 `omitempty`，使未启用批处理的旧任务序列化结果不变。
- [ ] 明确字段约束：批次字段只用于 staging/share-save；每个 `media.transfer` 的 `SourceObjects` 仍只包含当前文件；`ShareSaveObjects` 中的 ID 必须唯一且包含当前主源对象。
- [ ] 确保任务上下文 hash、签名或 sealed manifest 使用完整的新增字段，避免同一个任务 ID 下批次成员变化后仍被当成同一上下文。
- [ ] 不把 share passcode、cookie、access token 放进日志字段或新 key 的明文；批次 key 只传不可逆 fingerprint，实际凭据继续使用现有受保护的 `Share`/storage 配置。
- [ ] 运行协议测试：

  ```bash
  go test ./internal/cluster/protocol -count=1
  ```

### 阶段 3：调度端按“分享转存批次”附加上下文

**涉及文件：**

- `internal/cluster/subscription_dispatcher.go`
- `internal/cluster/subscription_dispatcher_test.go`
- 必要时 `internal/subscription/cluster_dispatch.go` 及其测试

- [ ] 先写 `TestDispatchSubscriptionMedia_AttachesSameShareSaveBatchToSiblingTasks`：同一分享的多个文件在同一 worker/target 下仍生成多个 `media.transfer`，但这些任务具有相同 `ShareSaveKey` 和相同批次文件清单；每个任务的主 `SourceObjects` 仍只有当前文件。
- [ ] 先写 `TestDispatchSubscriptionMedia_SeparatesShareSaveBatchByShareAndTarget`：不同分享、不同目标 worker、不同 staging root 分别形成独立批次，不能跨 worker 合并。
- [ ] 在 `DispatchSubscriptionMedia` 选择目标之后分组，批次 key 至少包含 provider、规范化分享标识/URL、passcode fingerprint、选定 worker 和 staging target；key 的构造必须稳定，不能包含每文件的 target path。
- [ ] 将同一批次的全部源对象附加到每个子任务，同时保留当前任务的单一主源对象、单文件目标路径和原有 capability 要求。
- [ ] 保持现有 chunk 上限 100 和 `share.batch` 父聚合任务语义；不得把父聚合任务误当作 worker 端实际转存任务，也不得改变任务排序和 per-file observation。
- [ ] 增加 dispatch 侧日志或测试可观测字段，记录 provider、批次 fingerprint、文件数和目标 worker，不记录明文分享密码或 token。
- [ ] 运行调度测试：

  ```bash
  go test ./internal/cluster -run 'TestDispatchSubscriptionMedia' -count=1
  ```

### 阶段 4：worker 端 singleflight 合并实际转存调用

**涉及文件：**

- `internal/cluster/worker/service.go`
- `internal/cluster/worker/service_test.go`
- 必要时 `internal/cluster/runtime.go` 的任务处理测试

- [ ] 先写 `TestMediaTransfer_BatchShareSaveUsesSingleflight`：并发提交同一 `ShareSaveKey` 的多个 `media.transfer`，fake share saver 记录调用次数，断言 share-save 只调用一次；之后每个文件都能通过 `findExistingStagedSource` 找到自己的已转存文件并继续独立传输。
- [ ] 先写 `TestMediaTransfer_BatchShareSaveFailureFansOutAndAllowsRetry`：第一批调用失败时所有等待者收到一致失败；清除失败条件后重新提交，必须能够再次调用，不得永久缓存失败结果。
- [ ] 先写 `TestMediaTransfer_BatchShareSaveSkipsWhenAllFilesAlreadyStaged`：worker 重启或重复投递后，已有临时文件时不重新调用 115 接口。
- [ ] 在 `Service` 中增加只作用于 share-save 阶段的 singleflight；key 必须包含调度端批次 fingerprint 以及 worker 本地 staging root/账号绑定，避免不同目标目录或账号误合并。
- [ ] 将当前流程改为：先检查当前文件已有 staging 结果；若批次字段存在且仍有文件缺失，则 singleflight 执行一次批量保存；singleflight 返回后每个子任务再次按自己的源文件查找结果；找不到时返回明确的文件级错误。
- [ ] 保证 singleflight 只合并“准备阶段”的网络调用，不合并下载流、上传流、hash、manifest 或结果上报；这些仍由各 `media.transfer` 独立执行。
- [ ] singleflight 只存活于 worker 进程内；worker 重启后的正确性依赖磁盘/网盘已有 staging 文件检查，不依赖恢复内存状态。
- [ ] 运行 worker 测试：

  ```bash
  go test ./internal/cluster/worker -count=1
  ```

### 阶段 5：补齐 115 响应诊断和状态可靠性

**涉及文件：**

- `internal/subscription/share_115.go`
- `internal/subscription/share_115_test.go`
- 订阅运行状态聚合实现及其现有测试（通过 `rg` 定位 `last_status`、delivery/episode source 汇总逻辑后，仅修改相关文件）

- [ ] 先写 `TestDecodePan115JSON_ReportsHTTPMetadataForHTMLResponse`：模拟 2xx/4xx 的 HTML 响应，断言错误包含 HTTP status、Content-Type 和响应类型/长度信息，而不是只有 `invalid character '<'`。
- [ ] `decodePan115JSON` 在 JSON 解析失败时附带 HTTP status、Content-Type、响应长度和首个非空字符类型；不得输出完整响应体，也不得输出 cookie、passcode、access token。响应体若用于调试，只允许经过严格截断和脱敏后记录。
- [ ] 保持真实业务错误可区分：HTTP 鉴权错误、HTML 网关页、JSON 业务错误和本地 JSON 解码错误分别带有稳定错误上下文，供重试策略判断。
- [ ] 不因为“HTML 响应”盲目无限重试；只有已定义的瞬时网络/网关错误进入有限退避，鉴权/签名错误直接进入配置或重新授权提示。
- [ ] 修复或补充订阅最终状态聚合测试：发现/抓取成功但 `saving_share` 或移动网盘投递失败时，订阅运行结果不能仍报告为整体成功；同时保留可查看的阶段失败数量与主要错误。
- [ ] 运行诊断和状态测试：

  ```bash
  go test ./internal/subscription ./internal/cluster/... -run 'Test(DecodePan115JSON|.*Status|.*Observation|.*Delivery)' -count=1
  ```

### 阶段 6：集群健康、凭据和外部服务失败的配套处理

这些问题与 115 批量转存是不同故障面，必须分别处理，不能用批量转存掩盖：

- [ ] 在选择 worker 后、创建大量 `media.transfer` 前做 capability/online preflight；对于没有 `share.save`、`mobile.upload` 或 `result.report` 的节点，返回可操作的调度错误。
- [ ] 对 `cluster coordinator is disabled` 和 `cluster transport is not connected` 增加明确健康状态与短暂重试边界；在基础设施未恢复前不重复创建同一批次的海量文件任务。
- [ ] 对 `密钥错误或签名无效` 增加错误分类和订阅级汇总，避免把永久凭据错误当作瞬时网络错误重试。
- [ ] 对 Telegram 发送超时采用有限重试和退避，通知任务必须幂等；不能阻塞或回滚已经完成的分享转存/移动网盘上传。
- [ ] 对 SQLite `SQLITE_BUSY` 仅增加有上限的短退避，并检查相关事务是否持锁过久；不得通过无限重试掩盖数据库写入问题。
- [ ] 对 CloudDrive2 115 节点的 `refresh_token 无效` 单独标记节点不可用并要求重新授权；不要把 CD2 节点的授权状态混入普通 115 节点的批次 key。
- [ ] 为上述类别各补至少一个错误分类或调度测试；不在本阶段改变无关 provider 的业务协议。

## 三、验收测试计划

### A. 单元与组件测试

- [ ] 115 三文件批次：目录树请求一次，`/webapi/share/receive` 一次，`file_id` 包含三个目标文件。
- [ ] 115 十个文件批次：仍然一次目录树遍历、一次转存请求；每个文件独立完成后续下载/上传。
- [ ] 两个不同分享：两次独立目录树遍历和两次独立转存，不共享 staging。
- [ ] 同一分享分到两个 worker：每个 worker 各执行一次，不跨节点共享临时结果。
- [ ] 部分文件已 staging：只对缺失集合执行必要转存，所有子任务都能复用结果；若 115 接口语义只能安全重放整批，则必须验证重复转存的幂等性后再启用部分批次优化。
- [ ] 首次批量转存失败：所有相关文件进入一致的 `saving_share` 失败状态；重试后可再次调用。
- [ ] 115 HTML/网关响应：错误包含 status、Content-Type 和类型信息，日志没有敏感凭据。
- [ ] 非 115 provider：原有 parent/destination 分组、请求次数和结果保持不变。
- [ ] 协议校验：多文件仍是多个 `media.transfer`，主 `SourceObjects` 多于一个仍被拒绝。

### B. 集群集成测试

- [ ] 启动一个带 `pan115`、`yidong139`、`share.save`、`mobile.upload`、`result.report` capability 的 fake worker。
- [ ] 从同一分享创建 N 个文件任务，验证 worker 收到 N 个独立 `media.transfer`，但只产生一个批次 share-save 调用。
- [ ] 验证任一单文件下载或移动网盘上传失败时，不会重新触发整个批次的 115 转存；只重试该文件，除非 staging 文件确实不存在。
- [ ] 验证 worker 进程重启后，已有 staging 文件可被重新发现；没有 staging 文件时允许重新建立批量转存。
- [ ] 验证 `DispatchMediaBatch` 的 parent batch 仍能正确聚合每个文件的结果和失败原因。

### C. 远端受控验收

仅在本地代码测试通过、构建产物明确、并获得部署授权后执行；使用低文件数的测试分享，避免再次触发海量订阅任务：

- [ ] 在 `192.168.1.195:25044` 先确认目标 worker 在线、capability 完整、115 storage 凭据有效；不使用当前 `refresh_token 无效` 的 CD2 节点作为验收 worker。
- [ ] 执行一次 3～5 个文件的 115 分享转存，采集 OpenList 持久化任务日志和 worker 原始 stdout/stderr。
- [ ] 验收指标：同一 `share_save_key` 仅一次目录树遍历、仅一次 `/webapi/share/receive`；N 个文件分别产生下载、移动网盘上传和结果上报记录。
- [ ] 若仍出现 `invalid character '<'`，以新增的 HTTP status、Content-Type、响应长度/类型信息判断真实原因，并据此决定是修复 endpoint/鉴权/代理，还是调整重试分类；不能仅凭旧错误文本结论化。
- [ ] 分别制造/观察一次 worker 离线、凭据失效和 Telegram 超时，确认错误分类、重试上限和订阅最终状态正确。
- [ ] 验收完成后检查没有残留重复任务、没有明文 token/cookie/passcode 日志，并记录可回滚版本。

## 四、验证命令与完成门槛

实现阶段结束前按顺序执行：

```bash
go test ./internal/subscription ./internal/cluster/protocol ./internal/cluster/worker ./internal/cluster -count=1
go vet ./internal/subscription ./internal/cluster/...
git diff --check
git status --short
```

完成门槛：

- [ ] 受影响 package 的测试全部通过。
- [ ] 115 批次调用次数和每文件独立传输断言通过。
- [ ] 失败批次可重试，worker 重启后可复用已有 staging。
- [ ] 旧任务（无批次字段）仍按原路径运行，非 115 provider 无行为回归。
- [ ] `media.transfer` 单源对象协议约束仍通过测试。
- [ ] 115 错误日志具备足够的上游诊断信息且不泄露敏感字段。
- [ ] 订阅整体状态不再把文件投递阶段的大量失败报告为成功。
- [ ] 远端受控验收完成，或明确记录未能执行的环境原因；不得把本地单测结果表述为远端有效性证明。

## 五、风险、回滚与未覆盖项

- 115 `/webapi/share/receive` 对重复文件 ID 或部分已转存文件的幂等语义必须通过受控测试确认；在确认前，不对“部分 staging”做激进的增量合并。
- 115 返回 HTML 的根因尚未由原始 HTTP 响应确认，批量化只会降低重复请求和失败放大，不保证单次接口本身一定成功。
- 按 worker 分组会导致同一分享在多个 worker 上各转存一次，这是正确的隔离行为；若要跨 worker 去重，需要共享 staging/分布式锁，超出本次最小实现。
- singleflight 是进程内机制，无法覆盖多个 worker 实例或 worker 重启瞬间；磁盘/网盘 staging 检查是必要的幂等后备。
- 若部署后发现某个 provider 的 share-save 语义不支持批量，立即通过关闭批次字段或回退到旧单文件入口恢复，不修改文件级 `media.transfer` 协议。
- 本计划不包含 CD2 115 授权协议的重新逆向实现；CD2 `refresh_token 无效` 仍作为独立授权/节点健康问题处理。

