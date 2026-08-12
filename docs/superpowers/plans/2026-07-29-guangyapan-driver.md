# GuangYaPan（光鸭网盘）Driver Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 openlist-etf 中移植并适配 Alist 光鸭驱动，并按外部《光鸭转存与 API 接入技术规范》补齐**分享链接解析与转存**，把光鸭做成与 123 / 115 同级的临时转存资源池：分享/离线入库 → Link 下载 → 上传 139。**不生成、不消费 `.etf`。**

**Architecture:**  
1) **个人盘驱动**以 Alist `drivers/guangyapan` 为基线（鉴权、List/Link、CRUD、OSS Put、离线 Tool）。  
2) **分享转存**按本对话提供的 Python/规范实现：`get_share_summary` → `get_share_access_token` → `get_share_page_files_list` → `restore_share` → `get_task_status`；原语放在 `drivers/guangyapan/share.go`，订阅层用 `ShareSaver` 对标 `share_123.go` / `share_115.go`。  
3) 实现时必须处理 **Alist 与规范的 Header/client_id/根 ID 差异**（见下节冲突表），以实测为准择优，并写回归测试锁定解析与错误码。

**Tech Stack:** Go（`github.com/OpenListTeam/OpenList/v4`）、resty、aliyun-oss-go-sdk、`golang.org/x/time/rate`、go-cache、singleflight

## Product Role（已确认）

```text
光鸭分享链接（含提取码）
        ↓ ParseURL + get_share_access_token + list + restore_share
   GuangYaPan 个人盘临时目录（资源池，类似 123/115）
        ↓ Link（signedURL 直链）
   上传到 139Yun → 仅 139 做 ETF
```

- 光鸭：中转盘；含挂载、离线入库、**分享转存**、Link、临时目录清理。
- `code==156` / `check_can_flash_upload` 仅作上传去重，不做 `.etf`。
- 139：唯一 ETF 落点。

## 已确认决策

1. **根路径字段：** `root_path`（与 Alist 一致）。
2. **Writer：** 优先 Result 版（返回 `model.Obj`）；拿不到 id 时再退回 `error` 版。
3. **离线下载：** 包含（Phase 2）。
4. **分享转存：** **纳入本驱动适配**（不再另开远期计划）。驱动内提供转存原语；订阅侧同步接 `ShareProviderPanGuangya`（名称待定，建议 `guangyapan`）。
5. **不做 ETF。**

### Alist vs 规范：能力对照

| 能力 | Alist 光鸭 | 外部规范 / Python | 本计划 |
|------|------------|-------------------|--------|
| 挂载 / Link / CRUD / OSS Put | 有 | 部分路径不同 | Phase 1：Alist 基线 + 规范路径兜底 |
| URL/BT 离线 | 有 | 未覆盖 | Phase 2：移植 Alist |
| 分享解析 / token / 列表 / restore | **无** | **有完整流程** | Phase 3：按规范实现 |
| ETF | 无 | 无（仅 gcid 秒传检查） | 不做 |

---

## Global Constraints

- 模块导入一律 `github.com/OpenListTeam/OpenList/v4/...`。
- 驱动名 `GuangYaPan`；`NoOverwriteUpload = true`、`CheckStatus = true`。
- 根路径 JSON 字段 `root_path`。
- Writer 优先 Result 接口。
- 必须含离线下载 + **分享转存原语 + 订阅 ShareSaver**。
- 不做光鸭 ETF。
- 分享 API 以用户提供的规范为准；与 Alist Header/路径冲突时先按规范实现分享流，个人盘读写优先保持 Alist 已验证路径，必要时双路径兼容。
- Commit 用 Conventional Commits；禁止提交真实 token / 手机号。

---

## 规范接入：分享转存 API（纳入驱动）

### 流程

1. 解析：`https://www.guangyapan.com/s/{shareId}` + `code`/`pwd`/「提取码」文本  
2. `POST /userres/v1/get_share_summary`（可选，用于标题/校验）  
3. `POST /userres/v1/get_share_access_token` → **share_access_token**（≠ 用户 Bearer）  
4. `POST /userres/v1/get_share_page_files_list`（`parentId` 规范为 `"0"`；递归子目录）  
5. `POST /userres/v1/restore_share`（必须用户 `Authorization` + `fileIds` + 目标 `parentId`）  
6. `POST /userres/v1/get_task_status` 等待完成  

### 与 Alist 的冲突点（实现时实测裁决）

| 项 | Alist | 规范 / Python | 建议 |
|----|-------|---------------|------|
| Account host | `account.guangyapan.com` | `auth` 或 `account` | 登录先试 Alist host；刷新可兼容两者 |
| `client_id` | `aMe-8VSlkrbQXpUR` | `"301"` | **Addition 可配**；默认跟 Alist 登录，分享流若 401 再试规范值 |
| API `dt` | `"4"` | `"web"` | 个人盘跟 Alist；分享请求可跟规范 |
| `x-client-id` / protocol | ClientID / `301` | `301` / `0.0.1` | 分享请求对齐规范 Header；个人盘对齐 Alist |
| 根 `parentId` | `""` | `"0"` | **分享列表/转存跟规范用 `"0"`**；个人盘 List 跟 Alist 实测 |
| 业务 path 前缀 | 混用 `/userres` 与 `/nd.bizuserres.s` | 多为 `/userres/v1/...` | 分享一律 `/userres`；个人盘保留 Alist |
| 任务 status | int（2 成功） | 字符串 `"success"` 或 progress | 解析层同时兼容 int / string |
| 业务 `code` | 常看 `msg` | `0`/`"0"`/`200` | 统一 `isOK(code)` helper |

### 驱动内 API 表面（`drivers/guangyapan`）

```text
ParseShareURL(raw) (shareID, code, error)
GetShareSummary(ctx, shareID, code)
GetShareAccessToken(ctx, shareID, code) string
ListShareFiles(ctx, shareToken, parentID) []ShareFileItem   // 分页
RestoreShare(ctx, shareToken, fileIDs, parentID) taskID
WaitShareRestoreTask(ctx, taskID) error
```

订阅层 `internal/subscription/share_guangyapan.go` 实现 `ShareSaver`：`ParseURL` / `ListShareChildren` / `SaveShareItems` / `WaitSaveComplete` / `EnsureDir`，内部调用上述驱动方法或等价 HTTP（凭据来自已挂载 GuangYaPan storage 的 token，对标 123/115 的 TempTransfer 绑定）。

---

## Alist 实现全面分析（对接注意事项）

### 1. 源码结构

| 文件 | 职责 |
|------|------|
| [`meta.go`](https://github.com/AlistGo/alist/blob/main/drivers/guangyapan/meta.go) | `Addition`、`driver.Config`、注册 |
| [`types.go`](https://github.com/AlistGo/alist/blob/main/drivers/guangyapan/types.go) | 鉴权/列表/下载/任务/上传/离线类型 |
| [`driver.go`](https://github.com/AlistGo/alist/blob/main/drivers/guangyapan/driver.go) | Init、CRUD、Link、Put、Token、限流、OSS |
| [`offline.go`](https://github.com/AlistGo/alist/blob/main/drivers/guangyapan/offline.go) | 离线任务 |
| （本仓库新增）`share.go` | 规范分享转存原语 |
| Alist offline Tool | `internal/offline_download/guangyapan` |
上游近期相关提交（移植时建议对照 diff）：

- `feat: add GuangYaPan offline download (#9505)`
- `fix(guangyapan): resolve offline root folder lookup (#9516)`
- `fix(guangyapan): rate-limit API requests... (#9553)`

### 2. 双域名与客户端

- **Account：** `https://account.guangyapan.com`
  - 登录、验证码、refresh、`/v1/user/me`
  - 固定一批设备伪装 Header：`X-Device-*`、`X-Client-Id`、`X-SDK-Version=9.0.2`、`X-Protocol-Version=301` 等
  - `X-Device-Sign = "wdi10." + deviceID + "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"`
  - 可选 `X-Captcha-Token`
- **API：** `https://api.guangyapan.com`
  - Header：`Did`（device id）、`Dt=4`、`Authorization: Bearer <access_token>`
- **默认 ClientID：** `aMe-8VSlkrbQXpUR`
- **DeviceID：** 32 位小写 hex；非法则自动生成；需持久化到 storage addition，避免每次 Init 换设备指纹

### 3. 鉴权优先级与短信两段流（极易踩坑）

`Init` 优先级：

1. `access_token` → `GET /v1/user/me` 校验
2. 失败则 `refresh_token` → `POST /v1/auth/token`（`grant_type=refresh_token`）
3. 再失败则短信：
   - **阶段 A：** 填 `phone_number`，`send_code=true` → 发短信、写入 `verification_id`、`send_code` 复位为 false；**Init 必须成功返回**（仅更新 status 文案）
   - **阶段 B：** 填 `verify_code` → `verification/verify` → `signin` → 保存 token，清空 `verify_code` / `verification_id`

注意事项：

- 手机号规范化为 E.164 展示形态：`+86 1xxxxxxxxxx`（中间有空格）；captcha meta 里另有去 `+86` 的纯数字形式。
- captcha：`POST /v1/shield/captcha/init`，`action=POST:/v1/auth/verification`；遇 `captcha_invalid` / expired 应强制刷新后重试。
- 登录成功后必须 `op.MustSaveDriverStorage(d)`，否则刷新后的 token 丢失。
- `setTempStatus` 用 `time.AfterFunc(200ms)` 绕过 Init 结束后被改成 WORK 的行为——移植时保留或改为更稳妥的 status 机制，但不要删掉“仅发短信不报错”的产品语义。
- `CheckStatus: true`：未登录完成时 storage 状态文案应对用户可见。

### 4. 根目录解析

- Addition 字段在 Alist 是自定义 `root_path`（完整路径，如 `/电影/华语`），**不是** OpenList 常见的 `root_folder_path` / `root_folder_id`。
- 空路径 → 根 `parentId=""`。
- 非空 → 按 `/` 分段，在每层 `get_file_list` 里找 `resType==2` 且同名文件夹。
- `GetRooter` 返回解析后的 folder id。
- **离线下载特例：** 若目标目录就是驱动根，创建任务时要把 `parentId` 置为空字符串（见 `#9516`）。

OpenList 适配建议：

- 方案 A（推荐兼容上游）：继续用 `root_path` 字段名，文档标明含义。
- 方案 B：改嵌 `driver.RootPath`（`root_folder_path`），并在代码里读 `GetRootPath()`。  
  **选定后全仓一致**；若要与 Alist 配置互导，优先方案 A。

### 5. 业务 API 清单

| 能力 | Method / Path | 要点 |
|------|---------------|------|
| 列表 | `POST /userres/v1/file/get_file_list` | body: `parentId,page,pageSize,orderBy,sortType,fileTypes:[]`；分页直到不足一页或达到 total |
| 列表（路径解析） | `POST /nd.bizuserres.s/v1/file/get_file_list` | **与 List 路径前缀不同**；移植时原样保留，实测后再统一 |
| 下载 | `POST /nd.bizuserres.s/v1/get_res_download_url` | 优先 `signedURL`，否则 `downloadUrl` |
| 建目录 | `POST /nd.bizuserres.s/v1/file/create_dir` | 成功判 `msg ~= success` |
| 重命名 | `.../file/rename` | |
| 删除 | `.../file/delete_file` | `fileIds:[]`；可能返回 `taskId`，需轮询 |
| 移动 | `.../file/move_file` | 同上异步任务 |
| 复制 | `.../file/copy_file` | 同上；批量时依赖限流 |
| 任务状态 | `POST /nd.bizuserres.s/v1/get_task_status` | `status==2` 成功；`-1/3` 失败；最多约 30×300ms |
| 上传凭证 | `POST /nd.bizuserres.s/v1/get_res_center_token` | `capacity:2,name,parentId,res.fileSize` |
| 上传完成 | `POST /nd.bizuserres.s/v1/file/get_info_by_task_id` | 等到 `fileId`；部分 code 视为处理中 |
| 离线解析 | `POST /cloudcollection/v1/resolve_res` | 支持 BT 子文件索引 |
| 离线创建 | `POST /cloudcollection/v1/create_task` | `url,parentId,newName[,fileIndexes]` |
| 离线列表 | `POST /cloudcollection/v1/list_task` | |
| 离线删除 | `POST /cloudcollection/v2/delete_task` | 注意 **v2** |

文件夹判定：`resType == 2`。时间字段为 Unix 秒：`ctime` / `utime`。

### 6. `postAPI` 行为

- 每个 path 独立 `rate.Limiter`，间隔 **500ms**（防批量 copy 打爆）。
- HTTP 401/403 → refresh → 重试一次。
- 主要检查 HTTP status；业务层再看 `msg` / 特定 `code`。
- **注意：** List 成功路径几乎不校验 body `code`；写操作普遍校验 `msg==success`。

### 7. 上传 / OSS

1. `get_res_center_token` 取 STS：`accessKeyID/secret/sessionToken`（可能在顶层或 `creds` 嵌套）。
2. `code == 156` → 视为秒传 / 已存在，跳过 OSS，直接 `waitUploadTaskInfo`。
3. 否则阿里云 OSS multipart：`InitiateMultipartUpload(..., oss.Sequential())`，分片大小按文件体积阶梯（1/2/4/8 MiB）。
4. Endpoint 规范化：去掉 `bucket.` 前缀，补 `https://`。
5. 0 字节文件走空 `PutObject`。
6. Alist `Put` 返回 `error`；资源池场景跟 Alist 实现 `Put`（`error`）即可，无需为 ETF 上 `PutResult`。

当前上传请求 **未携带文件哈希**。`code==156` 只当作上传去重短路保留；**不要**据此实现 ETF。

### 8. 离线下载 Tool

- Tool 名：`GuangYaPan`
- 设置键：`guangyapan_temp_dir`（Alist `conf.GuangYaPanTempDir`）
- 目标 storage 必须是 `*guangyapan.GuangYaPan`
- 若下载目标本身就是光鸭盘 → `tempDir = DstDirPath`；否则落到配置的 temp 根下
- Status：`0 queued / 1 running / 2 completed / 3 failed / 4 canceled / 5 partially completed`
- 任务缓存 + singleflight，10s TTL

OpenList 还需同步改：

- `internal/conf/const.go`
- `internal/offline_download/all.go`
- `internal/offline_download/tool/add.go` 的 `switch`
- `server/handles/offline_download.go`（`SetGuangYaPan` + 路由）

### 9. 与 OpenList / openlist-etf 的接口差异

| 点 | Alist guangyapan | OpenList 现状 | 移植策略 |
|----|------------------|---------------|----------|
| 包路径 | `alist-org/alist/v3` | `OpenListTeam/OpenList/v4` | 全量替换 |
| `Config.OnlyLocal` | 有 | 已删除 | 不要加回 |
| Writer | `error` | 支持 Result 版 | **优先 Result** |
| ETF | 无 | 仅 139 | 光鸭永不实现 |
| 分享转存 | 无 | pan123/pan115 ShareSaver | **本计划 Phase 3 按规范实现** |
| 离线 Tool | 有 | 同模式 | Phase 2 |

### 10. 与 123 / 115 资源池对齐

| 能力 | Phase 1 | Phase 2 | Phase 3 |
|------|---------|---------|---------|
| 挂载 / Link / CRUD / Put | 必需 | — | — |
| URL/BT 离线入库 | — | 必需 | — |
| 分享解析 + restore | 驱动原语 | — | ShareSaver 接线 |
| `.etf` | 不做 | 不做 | 不做 |

### 11. 其他风险清单

1. 个人盘 vs 分享 API 的 Header / `parentId` 差异（见文首冲突表）。
2. 任务 status 兼容 int 与 string。
3. 提取码错误 / 分享失效 / 空间不足需可读错误。
4. 设备指纹、captcha、token 刷新竞态。
5. 大目录转存轮询超时。
6. AGPL 来源保留。

---

## 文件规划

### 新建

- `drivers/guangyapan/{meta,types,driver,offline,share}.go`
- `drivers/guangyapan/{auth,upload_helpers,share}_test.go`
- `internal/offline_download/guangyapan/{guangyapan,util}.go`
- `internal/subscription/share_guangyapan.go`
- `internal/subscription/share_guangyapan_test.go`

### 修改

- `drivers/all.go`
- `internal/conf/const.go`、`offline_download/all.go`、`tool/add.go`、`server/handles/offline_download.go` + 路由
- `internal/subscription/share_provider.go`（`ShareProviderGuangYaPan = "guangyapan"`）
- `ParseShareURL`、storage binding、telegram pan config、`validateSubscriptionConfigTargets`、`share_runtime` 等 pan123/pan115 对等位

### 明确不建

- 光鸭 ETF

---

## Phase 0 — 对齐

- [ ] 记录 Alist commit SHA
- [ ] 锁定冲突表裁决：个人盘跟 Alist，分享流跟规范
- [ ] 验收口径含分享转存；无 ETF

---

## Phase 1 — 核心驱动

### Task 1: meta + types + all.go 注册

- [ ] 拷贝 Alist Addition/types，OpenList v4 import；`root_path`；Result 友好类型预留

### Task 2: 鉴权 helpers 单测

- [ ] phone / device_id 规范化单测 → 实现

### Task 3: Init / token / postAPI / 限流

- [ ] 双 client；SMS 两段；401 refresh；500ms/path limiter

### Task 4: GetRoot / List / Link / CRUD（Result 版）

- [ ] 分页 List；Link（`signedURL`/`signedUrl`/`downloadUrl`）；写操作返回 Obj

### Task 5: OSS Put

- [ ] token / multipart / code 156 / waitUploadTaskInfo；LimitedUploadStream

---

## Phase 2 — 离线下载

### Task 6–7

- [ ] 移植 `offline.go`
- [ ] Tool + conf + add.go + Set API
- [ ] 冒烟：离线入库 → Link

---

## Phase 3 — 分享转存（规范）+ 订阅

### Task 8: `drivers/guangyapan/share.go`

- [ ] 单测 ParseShareURL（`?code=`、提取码文本）
- [ ] `GetShareSummary` / `GetShareAccessToken` / `ListShareFiles`（分页，根 `"0"`）
- [ ] `RestoreShare` + `WaitShareRestoreTask`（兼容 int/string status）
- [ ] 分享 Header 对齐规范；用户 401 刷新
- [ ] 错误：提取码 / 失效 / 空间不足

### Task 9: `internal/subscription/share_guangyapan.go`

- [ ] `ShareProviderGuangYaPan`；接入 `ParseShareURL`
- [ ] 实现 `ShareSaver`（EnsureDir / ListShareChildren / SaveShareItems / WaitSaveComplete）
- [ ] TempTransferTarget 配置与 binding 对标 pan123/pan115
- [ ] mock HTTP 单测

### Task 10: E2E 冒烟

- [ ] 有/无提取码分享 → 转存临时目录 → List/Link →（可选）上 139

---

## 建议提交切片

1. `feat(guangyapan): add storage driver with auth list link and mutations`
2. `feat(guangyapan): add OSS upload and offline download tool`
3. `feat(guangyapan): add share restore primitives`
4. `feat(subscription): wire guangyapan share provider for temp transfer`

---

## 验收清单

### Phase 1–2

- [ ] 挂载、登录、List、**直链 Link**、CRUD、Put、离线 Tool；无 ETF

### Phase 3

- [ ] 分享 URL + 提取码解析
- [ ] restore 到临时目录并等任务完成
- [ ] 订阅可选 `guangyapan` 作为 TempTransfer 源
- [ ] 错误提取码 / 失效分享可读

---

## 开放决策

已全部确认。冲突默认：**个人盘 = Alist，分享流 = 规范**。
