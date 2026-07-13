# 统一订阅存储语义与集群能力调度设计

## 状态

已与用户确认的设计文档。

适用仓库：

- 后端：`/Volumes/extend Disk/Github/OpenList`
- 前端：`/Volumes/extend Disk/Github/OpenList-Frontend`

## 背景

当前订阅与集群链路仍然大量依赖 OpenList 挂载路径语义，例如 `/123/转存至移动`、`/139_60t/剧集`、`cluster.etf_root_path=/139_60t`。这会带来以下问题：

- 用户必须理解 OpenList 内部挂载路径，配置心智负担大。
- `standalone`、`hybrid`、`worker` 三种模式对路径的解释不一致。
- 当同一 provider 存在多个账号或多个挂载时，路径语义无法表达按账号能力池调度。
- 控制器目前无法充分展示 worker 侧真实挂载/provider 能力，导致调度依据不透明。

用户已明确要求：

1. 无论 `standalone`、`hybrid`、`worker` 模式，统一目标都是先将资源转存到 `123` 或 `115` 网盘内指定目录，再下载并上传到 `139` 网盘内指定目录。
2. 用户配置不再填写 OpenList 绝对路径，只填写盘类型与盘内目录名。
3. 多同类 provider 存储不报歧义，而应按账号能力池调度。
4. 控制器需要展示 worker 的实际挂载/provider 能力，并据此进行任务分发。
5. 对 `123`、`115` 第一版调度先只考虑会员权重，不引入真实测速。
6. 对 `139` 调度必须考虑会员等级决定的单文件上传大小上限。

## 目标

### 核心目标

- 将订阅配置语义从“OpenList 路径”切换为“provider + 盘内目录”。
- 在三种模式下统一运行时解析流程。
- 将 cluster 调度从“节点/挂载路径匹配”升级为“provider 账号能力池调度”。
- 在控制器前端展示 worker 的 provider 能力、账号能力、空间和会员等级信息。
- 将后端与前端一起改造，保证用户只接触新语义。

### 非目标

- 第一版不做 `123` / `115` 真实下载测速。
- 第一版不做跨 worker 的多阶段流水线拆分调度。
- 第一版不保留长期双轨配置；旧路径语义只允许一次性迁移，不继续作为前端主入口。

## 已确认业务规则

### 目标存储流程

订阅任务统一分为两个目标：

1. 临时转存目标
   - provider：`pan123` 或 `pan115`
   - folder：例如 `转存至移动`
2. 最终上传目标
   - provider：`yidong139`
   - folder：例如 `剧集/港台剧`

运行顺序统一为：

1. 在源临时 provider 账号内定位或创建盘内目录。
2. 将分享内容临时转存到该目录。
3. 从该 provider 下载。
4. 上传到 `yidong139` provider 指定目录。
5. 向控制器上报结果；控制器只关心结果，不关心具体由哪个账号执行。

### 139 会员等级与单文件上传上限

按照用户提供的 `etflix_302_worker` 规则执行：

- 普通会员：`5G`
- 白银会员：`8G`
- 黄金会员：`20G`
- 钻石会员：`500G`

`yidong139` 调度时必须先满足：

- `file_size <= max_single_upload_bytes`

该规则为硬约束，不是排序权重。

### 123 / 115 第一版调度规则

第一版不引入真实测速，仅将会员等级转换为调度权重：

- 会员等级越高，调度权重越高。
- 排序时仍会综合剩余空间和当前负载。

## 用户配置模型

## 新配置语义

用户不再输入：

- `/123/转存至移动`
- `/139_60t/剧集`
- 任何 OpenList 挂载路径

用户只输入以下结构：

### 临时转存目标

- `provider`: `pan123` 或 `pan115`
- `folder`: 盘内目录名，例如 `转存至移动`

### 最终上传目标

- `provider`: `yidong139`
- `folder`: 盘内目录名，例如 `港台剧/热播`

### 示例

```json
{
  "temp_target": {
    "provider": "pan123",
    "folder": "转存至移动"
  },
  "delivery_target": {
    "provider": "yidong139",
    "folder": "港台剧"
  }
}
```

## 运行时解析模型

用户配置不直接产生执行路径。运行时必须先通过统一解析器得到真实执行目标。

### 统一解析输入

- 目标 `provider`
- 盘内 `folder`
- 任务类型
- 文件大小
- 节点本地可用 provider 账号池

### 统一解析输出

- 选中的具体 storage / account
- account alias / fingerprint
- 实际 mount path
- 盘内目录
- 最终可执行 OpenList 路径

### 统一原则

- 用户层只见 provider 与 folder。
- 路径仅作为运行时内部产物存在。
- `standalone`、`hybrid`、`worker` 全部复用同一解析逻辑。

## 三种模式的统一行为

### standalone

- 本机直接读取可用 provider 账号池。
- 根据任务类型、文件大小和权重选择账号。
- 在盘内确保目录存在后执行转存/下载/上传。

### hybrid

- coordinator 根据 provider 能力选择最优执行目标。
- 若本机 hybrid worker 命中，同样走本地统一解析器。
- 不再根据用户提供的绝对路径做目录推导。

### worker

- worker 接收到的是 provider + folder + 文件大小等任务语义。
- worker 本地解析到具体账号与目录。
- worker 不依赖控制器下发 OpenList 绝对路径。

## Cluster Inventory 模型

当前 inventory 只表达挂载信息，不足以支撑能力调度。本次要升级为“节点 + provider 账号能力池”模型。

### 节点级字段

控制器节点列表保留并继续展示：

- `node_id`
- 在线状态
- 角色
- 版本
- 最近心跳
- 当前任务数
- 节点级负载摘要

### provider 账号级字段

每个 worker 节点下新增 provider account inventory 列表。每项至少包含：

- `provider`
- `storage_id`
- `mount_path`
- `account_alias`
- `account_fingerprint`
- `status`
- `total_bytes`
- `free_bytes`
- `membership_tier`
- `membership_weight`
- `max_single_upload_bytes`
- `supports_share_save`
- `supports_download`
- `supports_upload`
- `supports_etf`
- `active_jobs`

### provider 特定能力

#### pan123

- `membership_tier`
- `membership_weight`
- `supports_share_save`
- `supports_download`

#### pan115

- `membership_tier`
- `membership_weight`
- `supports_share_save`
- `supports_download`

#### yidong139

- `membership_tier`
- `membership_weight`
- `max_single_upload_bytes`
- `supports_upload`
- `supports_etf`

## 调度规则

## 调度输入

控制器调度器应基于以下信息决策：

- 源任务所需 provider
- 目标任务所需 provider
- 任务类型能力需求
- 文件大小
- worker/provider 账号池 inventory

## 硬约束筛选

调度时先过滤掉不满足硬约束的候选账号：

- provider 不匹配
- 账号状态异常或未登录
- 不支持所需能力
- 空间不足
- 对 `yidong139`，`max_single_upload_bytes < file_size`

### 任务类型约束

#### 123 源订阅

只允许调度到：

- 拥有 `pan123` provider 能力
- 且支持 `share.save` 与 `download`

#### 115 源订阅

只允许调度到：

- 拥有 `pan115` provider 能力
- 且支持 `share.save` 与 `download`

#### 139 上传

只允许调度到：

- 拥有 `yidong139` provider 能力
- 且支持 `upload`
- 且满足单文件上传大小上限

## 排序规则

通过硬约束筛选后，再按以下顺序排序：

1. 会员权重高优先
2. 剩余空间大优先
3. 当前任务数少优先
4. 节点负载低优先
5. 稳定 ID 兜底

### 说明

- 对 `123` / `115`，会员权重是软排序项。
- 对 `139`，会员等级一方面决定排序权重，另一方面通过 `max_single_upload_bytes` 决定是否有资格参与候选。

## 后端改造范围

## A. 订阅配置模型改造

目标是把现有路径字段语义替换为 provider-target 语义。

### 涉及模块

- `internal/subscription/config.go`
- `internal/subscription/naming.go`
- `internal/subscription/service.go`
- 相关 `model.Subscription` 和序列化结构

### 改造要求

- 移除用户层面对 `TargetRoot` 的依赖。
- 移除用户层面对 `TempTransferRoot` 的依赖。
- 新增明确的临时目标和最终目标配置结构。
- 命名逻辑不再直接拼接用户输入路径，而是消费解析后的运行时目标。

## B. 存储解析与目录确保层

新增统一的 resolver 层，负责：

- 读取本机 provider 账号池
- 根据 provider、任务类型和文件大小筛选候选账号
- 根据会员权重、空间和负载排序
- 在目标账号内定位或创建盘内目录
- 生成实际执行路径

该层必须是 standalone / hybrid / worker 共享的底层能力，不能散落在各业务文件中重复实现。

## C. Cluster Inventory 与调度改造

### 涉及模块

- `internal/cluster/worker/inventory.go`
- `internal/cluster/runtime.go`
- `internal/cluster/subscription_dispatcher.go`
- `internal/cluster/protocol/*`

### 改造要求

- inventory 上报 provider 账号能力池，而不仅是 mount 列表。
- coordinator 保存并展示 provider account inventory。
- 调度 eligibility 判断从 mount-path 导向改为 provider capability 导向。
- 调度排序器按会员权重、空间和负载排序。

## D. Provider 能力采集

各 provider 必须能够暴露自身能力元数据。

### 第一版要求

- `pan123`：会员等级、权重
- `pan115`：会员等级、权重
- `yidong139`：会员等级、单文件上传上限、权重

其中 `yidong139` 必须准确映射到以下上限：

- 普通：`5G`
- 白银：`8G`
- 黄金：`20G`
- 钻石：`500G`

## 前端改造范围

前端改造仓库：`/Volumes/extend Disk/Github/OpenList-Frontend`

## A. 订阅管理页

### 目标页面与类型

- `src/pages/manage/subscription/`
- `src/pages/home/SubscriptionManagement.tsx`
- `src/types/subscription.ts`
- `src/lang/*/subscription.json`

### 改造要求

- 路径输入改为 provider 选择 + 盘内目录输入。
- provider 使用固定枚举选择，不允许手输路径。
- folder 允许多级目录名。
- 明确禁止用户填写 `/123/...`、`/139_60t/...` 等绝对路径。
- 文案需明确说明“填写盘内目录名，不是 OpenList 挂载路径”。

## B. 集群管理页

### 目标页面与类型

- `src/pages/manage/cluster/`
- `src/types/cluster.ts`
- `src/lang/*/cluster.json`

### 改造要求

- 节点详情增加 provider 账号池展示。
- 按 provider 分组展示账号列表。
- 展示会员等级、剩余空间、总空间、能力标签、当前任务数。
- 对 `yidong139` 明确展示单文件上传上限。
- 对异常状态高亮展示，例如空间不足、登录失效、能力缺失。

## 前端展示结构建议

- 节点列表保留概览信息。
- 点击节点进入详情抽屉或详情页。
- 详情页内按 provider 分组展示账号池。
- 每个账号卡片展示能力摘要与异常状态。

## 数据迁移策略

用户已确认完全切换到新语义，因此不保留长期双轨。

### 迁移原则

- 后端优先接受并持久化新结构。
- 前端只暴露新结构，不再允许填写旧路径语义。
- 旧路径字段仅允许作为一次性迁移来源，不能继续作为主输入。

### 自动迁移规则

对于可明确识别 provider 的旧值，允许自动迁移：

- `/123/转存至移动` -> `provider=pan123, folder=转存至移动`
- `/139_60t/剧集` -> `provider=yidong139, folder=剧集`

对于无法可靠识别 provider 或目录的旧值：

- 标记为需要人工确认
- 不做猜测性迁移

## 测试策略

## 后端单元测试

### 订阅与解析层

- provider + folder 正确解析到运行时目标
- 目录不存在时自动创建
- 多同类 provider 账号按权重选中正确账号
- `yidong139` 文件超过会员上限时被拒绝

### Cluster 调度

- `pan123` 任务不会派发给无 `pan123` 能力的 worker
- `pan115` 任务不会派发给无 `pan115` 能力的 worker
- `yidong139` 任务会按单文件上限筛选候选账号
- 同类账号排序符合会员权重、空间、负载规则

### Provider 能力

- `yidong139` 会员等级正确映射为上传上限
- `pan123` / `pan115` 会员等级正确映射为权重

## 前端测试

### 订阅页

- provider 必填
- folder 按规则校验
- 禁止旧路径语义输入
- 保存请求结构符合新 API

### 集群页

- 正确渲染 provider 账号池
- 正确显示会员等级、空间、上传上限
- 异常状态正确展示

## 集成测试

至少覆盖以下链路：

- `standalone + pan123 -> yidong139`
- `hybrid + pan123 -> yidong139`
- `worker cluster + pan115 -> yidong139`
- `yidong139` 超出会员单文件上限
- 多个 `pan123` 账号按权重选中更优账号
- 目标目录不存在时自动创建

## 实施顺序

建议实施顺序如下：

1. 后端定义新配置模型与 resolver
2. 后端完成 cluster inventory / scheduler 改造
3. 后端暴露新 API 与 inventory 输出
4. 前端改 subscription 表单
5. 前端改 cluster 展示
6. 前后端联调与集成测试
7. 移除旧路径主入口

## 风险与防线

### 风险

- provider 能力采集不完整会导致调度信息失真。
- 前端若未同步切换语义，会继续把用户输入当路径提交。
- 自动迁移过度猜测会制造错误配置。

### 防线

- `yidong139` 单文件上限作为硬约束，防止错误派单。
- 旧路径只做保守迁移，无法识别时显式提示人工确认。
- 所有执行链路统一走 resolver，避免路径拼接逻辑分散。

## 最终结论

本次改造本质上是一次统一的“存储目标语义升级”：

- 从“用户配置 OpenList 路径”切换为“用户配置 provider + 盘内目录”
- 从“按挂载路径理解任务”切换为“按 provider 能力池调度任务”
- 从“控制器只看节点”升级为“控制器可见节点内 provider 账号能力池”

该设计适用于 `standalone`、`hybrid`、`worker` 全部模式，并要求后端与前端同步推进。
