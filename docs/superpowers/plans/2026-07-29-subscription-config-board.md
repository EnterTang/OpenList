# Subscription Config Board Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Localize GuangYaPan storage help to Chinese and rebuild the subscription config tab into a three-region card board with status dots, a draggable transfer-priority bar, and modal editors for source disks (including 光鸭).

**Architecture:** Keep save/load on existing `/admin/subscription/config`. Add a small PanSou reachability API so the browser does not CORS-probe arbitrary hosts. Rebuild the config tab UI by extracting board/card components from `SubscriptionManagement.tsx`, binding `telegram.transfer_priority` and `telegram.guangyapan`.

**Tech Stack:** Go/Gin (backend), SolidJS + Hope UI (frontend), existing subscription config JSON schema.

## Global Constraints

- Spec: `docs/superpowers/specs/2026-07-29-subscription-config-board-design.md`
- Priority providers (exact order default): `pan123`, `pan115`, `guangyapan`, `quark`, `aliyun_drive`
- Source edit: modal; Telegram/PanSou: inline card forms; status dots top-right
- Persist priority only via existing config save (`telegram.transfer_priority`)
- All user-facing strings via i18n; GuangYaPan driver `help`/`Alert` in zh-CN
- Do not invent save APIs; do not remove existing config fields

## File Map

| Path | Responsibility |
|------|----------------|
| `drivers/guangyapan/meta.go` | Chinese help/Alert |
| `internal/subscription/pansou_status.go` | Probe PanSou base URL |
| `internal/subscription/pansou_status_test.go` | Probe unit tests |
| `server/handles/subscription.go` | HTTP handler for PanSou status |
| `server/router.go` | Register route |
| `OpenList-Frontend/src/types/subscription.ts` | Add `guangyapan`, `transfer_priority`, status resp types |
| `OpenList-Frontend/src/utils/api.ts` | `probePanSouStatus` |
| `OpenList-Frontend/src/pages/home/subscription/config/*` | New board/card components |
| `OpenList-Frontend/src/pages/home/SubscriptionManagement.tsx` | Wire config tab to board |
| `OpenList-Frontend/src/lang*/subscription.json` (+ overrides) | Locales |

---

### Task 1: GuangYaPan Chinese help

**Files:**
- Modify: `drivers/guangyapan/meta.go`

**Interfaces:**
- Consumes: existing `Addition` struct tags / `config.Alert`
- Produces: Chinese `help` and `Alert` only (JSON keys unchanged)

- [ ] **Step 1: Replace English help/Alert with Chinese**

In `drivers/guangyapan/meta.go`, set approximately:

```go
RootPath       string `json:"root_path" help:"光鸭网盘完整路径，作为挂载根目录"`
PhoneNumber    string `json:"phone_number" type:"text" help:"短信登录手机号，例如 +86 13800000000"`
CaptchaToken   string `json:"captcha_token" type:"text" help:"调用 /v1/auth/verification 所需的验证码 token"`
SendCode       bool   `json:"send_code" type:"bool" help:"设为 true 并保存以发送短信验证码；发送后会自动重置为 false"`
VerifyCode     string `json:"verify_code" type:"text" help:"短信验证码；填写后保存以完成登录"`
VerificationID string `json:"verification_id" type:"text" help:"发送短信后自动生成，请勿手动修改"`
AccessToken    string `json:"access_token" type:"text" help:"Bearer access token（若已提供 refresh_token 则可留空）"`
RefreshToken   string `json:"refresh_token" type:"text" help:"用于自动登录/刷新的 refresh token"`
DeviceID       string `json:"device_id" help:"可选自定义设备 ID（32 位十六进制）；为空时自动生成"`
OrderBy        int    `json:"order_by" type:"number" options:"0,1,2,3,4" default:"3" help:"文件列表排序字段"`
SortType       int    `json:"sort_type" type:"number" options:"0,1" default:"1" help:"文件列表排序方向"`

Alert: "info|两步短信登录：(1) 填写手机号（必要时填写 captcha_token），将 send_code 设为 true 并保存；(2) 填写 verify_code 并保存以完成登录，系统会自动保存 access_token/refresh_token。",
```

Write the file as UTF-8. Run:

```bash
python3 -c "open('drivers/guangyapan/meta.go','rb').read().decode('utf-8'); print('ok')"
```

Expected: `ok`

- [ ] **Step 2: Commit (only if user asked to commit)**

Skip unless explicitly requested.

---

### Task 2: PanSou status probe (backend)

**Files:**
- Create: `internal/subscription/pansou_status.go`
- Create: `internal/subscription/pansou_status_test.go`
- Modify: `server/handles/subscription.go`
- Modify: `server/router.go` (after telegram routes ~264)

**Interfaces:**
- Consumes: `model.SubscriptionPanSouSourceConfig.BaseURL`
- Produces:
  - `ProbePanSouStatus(ctx, baseURL string) (ok bool, message string, latencyMs int64)`
  - HTTP `POST /api/admin/subscription/pansou/status` body `{ "base_url": "..." }` → `{ code:200, data:{ ok, message, latency_ms } }`

- [ ] **Step 1: Write failing unit test**

```go
package subscription

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestProbePanSouStatusEmptyURL(t *testing.T) {
	ok, msg, _ := ProbePanSouStatus(context.Background(), "  ")
	if ok {
		t.Fatal("empty url should not be ok")
	}
	if msg == "" {
		t.Fatal("expected message")
	}
}

func TestProbePanSouStatusHealthy(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ok, msg, latency := ProbePanSouStatus(ctx, srv.URL)
	if !ok {
		t.Fatalf("expected ok, msg=%s", msg)
	}
	if latency < 0 {
		t.Fatalf("latency=%d", latency)
	}
}
```

- [ ] **Step 2: Run test (expect fail)**

```bash
go test ./internal/subscription/ -run TestProbePanSouStatus -count=1
```

Expected: FAIL undefined `ProbePanSouStatus`

- [ ] **Step 3: Implement probe**

```go
// pansou_status.go
func ProbePanSouStatus(ctx context.Context, baseURL string) (ok bool, message string, latencyMs int64) {
	baseURL = strings.TrimSpace(baseURL)
	if baseURL == "" {
		return false, "base_url is empty", 0
	}
	u, err := url.Parse(baseURL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return false, "invalid base_url", 0
	}
	start := time.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(baseURL, "/")+"/", nil)
	if err != nil {
		return false, err.Error(), 0
	}
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	latencyMs = time.Since(start).Milliseconds()
	if err != nil {
		return false, err.Error(), latencyMs
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 500 {
		// 2xx–4xx means host reachable; 5xx = unhealthy
		if resp.StatusCode >= 500 {
			return false, fmt.Sprintf("status=%d", resp.StatusCode), latencyMs
		}
		return true, fmt.Sprintf("status=%d", resp.StatusCode), latencyMs
	}
	return false, fmt.Sprintf("status=%d", resp.StatusCode), latencyMs
}
```

Adjust acceptance: treat any completed HTTP response with status < 500 as reachable (PanSou may 404 on `/`).

- [ ] **Step 4: Handler + route**

```go
// handles
type panSouStatusReq struct {
	BaseURL string `json:"base_url"`
}

func PanSouSubscriptionStatus(c *gin.Context) {
	var req panSouStatusReq
	_ = c.ShouldBindJSON(&req)
	ok, msg, latency := subscription.ProbePanSouStatus(c.Request.Context(), req.BaseURL)
	common.SuccessResp(c, gin.H{"ok": ok, "message": msg, "latency_ms": latency})
}

// router.go
subscription.POST("/pansou/status", handles.PanSouSubscriptionStatus)
```

- [ ] **Step 5: Re-run tests**

```bash
go test ./internal/subscription/ -run TestProbePanSouStatus -count=1
```

Expected: PASS

---

### Task 3: Frontend types + API + locales

**Files:**
- Modify: `OpenList-Frontend/src/types/subscription.ts`
- Modify: `OpenList-Frontend/src/utils/api.ts`
- Modify: `OpenList-Frontend/src/lang/en/subscription.json`
- Modify: `OpenList-Frontend/src/lang-overrides/zh-CN/subscription.json` (and zh-TW if present)
- Run: `node scripts/i18n.mjs` in frontend

**Interfaces:**
- Produces:
  - `SubscriptionTelegramSourceConfig.guangyapan`
  - `SubscriptionTelegramSourceConfig.transfer_priority?: string[]`
  - `probePanSouStatus(baseUrl: string): Promise<Resp<{ok:boolean; message:string; latency_ms:number}>>`

- [ ] **Step 1: Extend types**

```ts
export type TelegramPanKey =
  | "quark"
  | "aliyun_drive"
  | "pan123"
  | "pan115"
  | "guangyapan"

// on SubscriptionTelegramSourceConfig:
guangyapan: SubscriptionTelegramPanConfig
transfer_priority?: string[]
```

- [ ] **Step 2: API helper**

```ts
export const probePanSouStatus = (base_url: string) =>
  r.post("/admin/subscription/pansou/status", { base_url })
```

- [ ] **Step 3: Locales (en + zh-CN override)**

Add keys (examples):

```json
"config_region_services": "服务",
"config_region_delivery": "上传盘",
"config_region_sources": "来源盘",
"config_priority": "转存优先级",
"config_priority_hint": "拖动调整来源盘转存优先级",
"status_checking": "检测中",
"status_ok": "可用",
"status_fail": "不可用",
"status_unconfigured": "未配置",
"pansou_refresh_status": "刷新状态",
"source_channels_count": "{{count}} 个频道",
"source_no_channels": "未配置频道",
"source_edit": "编辑",
"telegram_pan_names": { "guangyapan": "光鸭网盘" },
"priority_short": {
  "pan123": "123",
  "guangyapan": "光鸭",
  "pan115": "115",
  "quark": "夸克",
  "aliyun_drive": "阿里"
}
```

- [ ] **Step 4: Merge i18n**

```bash
cd "../OpenList-Frontend" && node ./scripts/i18n.mjs
```

---

### Task 4: Shared UI primitives (status dot, priority bar, normalize)

**Files:**
- Create: `OpenList-Frontend/src/pages/home/subscription/config/transferPriority.ts`
- Create: `OpenList-Frontend/src/pages/home/subscription/config/transferPriority.test.mjs`
- Create: `OpenList-Frontend/src/pages/home/subscription/config/ServiceStatusDot.tsx`
- Create: `OpenList-Frontend/src/pages/home/subscription/config/SourcePriorityBar.tsx`

**Interfaces:**
- Produces:
  - `DEFAULT_TRANSFER_PRIORITY: TelegramPanKey[]`
  - `normalizeTransferPriority(values?: string[]): TelegramPanKey[]`
  - `ServiceStatusDot({ state: "checking"|"ok"|"fail"|"unconfigured", label: string })`
  - `SourcePriorityBar({ order, onChange })`

- [ ] **Step 1: Failing normalize test**

```js
import test from "node:test"
import assert from "node:assert/strict"
import { normalizeTransferPriority } from "./transferPriority.ts"

test("normalize fills missing guangyapan", () => {
  const got = normalizeTransferPriority(["quark"])
  assert.equal(got[0], "quark")
  assert.ok(got.includes("guangyapan"))
  assert.equal(got.length, 5)
})
```

(If TS import in node:test is awkward, use a `.mjs` that duplicates the pure function or add a small build-less `.ts`-free `.mjs` module.)

Prefer implementing `transferPriority.mjs` pure JS for node:test, imported by TS via re-export, matching existing `SubscriptionManagement.test.mjs` pattern.

- [ ] **Step 2: Implement normalize (mirror backend)**

```ts
export const DEFAULT_TRANSFER_PRIORITY = [
  "pan123",
  "pan115",
  "guangyapan",
  "quark",
  "aliyun_drive",
] as const

export function normalizeTransferPriorityName(value: string): string {
  switch (value.trim().toLowerCase()) {
    case "quark":
      return "quark"
    case "aliyun":
    case "aliyun_drive":
    case "ali":
    case "alipan":
      return "aliyun_drive"
    case "123":
    case "pan123":
      return "pan123"
    case "115":
    case "pan115":
      return "pan115"
    case "guangya":
    case "guangyapan":
      return "guangyapan"
    default:
      return ""
  }
}

export function normalizeTransferPriority(values?: string[]): string[] {
  // same algorithm as internal/subscription/config.go normalizeTransferPriority
}
```

- [ ] **Step 3: StatusDot + PriorityBar**

`ServiceStatusDot`: absolute top-right `8px` circle; colors: gray/green/red; `title={label}`.

`SourcePriorityBar`: horizontal flex; each block `draggable`; on drop reorder array and call `onChange(next)`; show `t("subscription.priority_short."+key)` and `→` between items.

- [ ] **Step 4: Run test**

```bash
cd OpenList-Frontend && pnpm test
```

Expected: new normalize test PASS (existing tests still PASS)

---

### Task 5: Config board cards + modal

**Files:**
- Create: `OpenList-Frontend/src/pages/home/subscription/config/SubscriptionConfigBoard.tsx`
- Create: `OpenList-Frontend/src/pages/home/subscription/config/SourcePanEditModal.tsx`
- Modify: `OpenList-Frontend/src/pages/home/SubscriptionManagement.tsx` (config tab Match only)

**Interfaces:**
- Consumes: `config`, `setConfig`/`update*` helpers, telegram auth handlers, `secretStatus`, storage provider options
- Produces: rendered three regions; does not own save HTTP (parent keeps 刷新/保存 buttons)

- [ ] **Step 1: Extract/reuse `TelegramPanConfigFields`**

Move or import existing pan field editor into modal body. Extend `TelegramPanKey` union + `telegramPanItems` to include `{ key: "guangyapan" }`.

Ensure `emptyTelegramConfig` / `fillTelegramConfig` initialize `guangyapan` and `transfer_priority: normalizeTransferPriority(...)`.

- [ ] **Step 2: Build `SubscriptionConfigBoard` regions**

```tsx
// Region 1: SimpleGrid 1–2 cols — Telegram card (existing fields + status dot), PanSou card (+ probe button/dot)
// Region 2: Delivery card — StorageTargetFields for default_target
// Region 3: SourcePriorityBar + SimpleGrid of source summary cards
```

Telegram status: map `telegramAuth()?.authorized` → ok/fail; loading → checking.

PanSou: on mount + when clicking refresh, call `probePanSouStatus(config().pansou.base_url)`; empty URL → unconfigured.

Source card click opens `SourcePanEditModal` with that `panKey`.

- [ ] **Step 3: Replace config tab body**

In `SubscriptionManagement.tsx` `<Match when={tab() === "config"}>`, render `<SubscriptionConfigBoard ...props />` instead of stacked `ConfigSection`s. Keep bottom refresh/save buttons in parent.

- [ ] **Step 4: Typecheck / build**

```bash
cd OpenList-Frontend && pnpm lint && pnpm build
```

Expected: PASS (UTF-8 clean)

---

### Task 6: End-to-end verification checklist

**Files:** none (manual + automated)

- [ ] **Step 1: Backend compile/tests**

```bash
go test ./internal/subscription/ ./drivers/guangyapan/ -count=1
go build -o /dev/null .
```

- [ ] **Step 2: Manual UI checklist**

1. Storage edit GuangYaPan → gray hints Chinese.  
2. 订阅配置 → three regions visible.  
3. Telegram/PanSou dots update after refresh.  
4. Drag priority → save → reload preserves order.  
5. Open 光鸭 card modal → edit channels → save → summary updates.  
6. Confirm no `openlist-redir` override when validating remote image later.

---

## Spec Coverage Check

| Spec requirement | Task |
|------------------|------|
| GuangYaPan Chinese help | Task 1 |
| PanSou active probe via backend | Task 2 |
| Types/API/i18n for guangyapan + priority | Task 3 |
| Status dots + priority drag | Task 4 |
| Three-region board + source modal | Task 5 |
| Acceptance verification | Task 6 |

## Placeholder Scan

No TBD/FIXME left in tasks.
