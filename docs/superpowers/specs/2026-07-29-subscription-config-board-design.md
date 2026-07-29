# Subscription Config Board & GuangYaPan Help Localization Design

## Goal

1. Localize GuangYaPan storage-driver gray help / alert text to Chinese.
2. Redesign the subscription **订阅配置** tab from a long vertical form into a three-region card board, with a draggable source-disk priority bar and modal editors for source disks.

## Decisions (confirmed)

| Topic | Choice |
|-------|--------|
| Source-disk detail editing | Click card → modal dialog |
| Telegram / PanSou presentation | Full form stays **inside** the card (no modal) |
| Telegram / PanSou availability | Colored top-right status dots with **active probing** for PanSou |
| Layout approach | Reorganize inside existing config tab; extract subcomponents (Approach 1) |
| Priority persistence | `telegram.transfer_priority` (backend already supports `guangyapan`) |

## Scope

In scope:

- Chinese `help` / `Alert` strings on GuangYaPan driver metadata.
- Subscription config tab UI: three regions, cards, priority bar, source-disk modal.
- Frontend wiring for `guangyapan` as a Telegram pan source (backend already has the field).
- Frontend binding for `telegram.transfer_priority` (drag reorder → save with config).
- Lightweight PanSou reachability probe (new small backend endpoint preferred; frontend-only probe only if CORS/proxy blocks browser fetch).
- Locale strings for new UI labels.

Out of scope:

- Changing transfer algorithm semantics beyond reordering `transfer_priority`.
- Redesigning 我的订阅 / 添加订阅 tabs.
- Redesigning storage-driver form layout (only help-text language for GuangYaPan).
- Cluster / worker UI changes.

## Current Behavior

- GuangYaPan `drivers/guangyapan/meta.go` exposes English `help` and `Alert`; the storage edit page renders those as gray hints.
- Subscription config renders Telegram settings, then each pan source as stacked `ConfigSection`s, then PanSou — one long column.
- Frontend `telegramPanItems` lists only `quark | aliyun_drive | pan123 | pan115` (no `guangyapan`).
- Backend `SubscriptionTelegramSourceConfig` already has `GuangYaPan` and `TransferPriority`; defaults include `guangyapan`.
- Frontend types / config form do not yet expose a visual priority editor.

## Design

### A. GuangYaPan help localization

Update `drivers/guangyapan/meta.go`:

- Translate all user-visible `help` strings and `Alert` to zh-CN.
- Keep JSON field names and English identifiers unchanged.
- Do not change runtime auth behavior.

### B. Config board layout

Replace the current vertical config stack with three regions (top → bottom):

1. **服务（Services）**  
   Responsive grid of two large cards: Telegram, PanSou.

2. **上传盘（Delivery）**  
   One card for default upload target (139 / `default_target` + related common fields currently under「通用」).

3. **来源盘（Sources）**  
   - Priority bar at the top of the region.  
   - Credit-card-sized source cards in a responsive grid.

Keep the existing bottom **刷新 / 保存** actions. All edits (including drag reorder and modal edits) mutate the in-memory `config` signal; only **保存** persists via the existing save API.

### C. Service cards (Telegram / PanSou)

Shared chrome:

- Title + optional subtitle.
- **Top-right status dot** with tooltip/title text.
- Full existing fields and actions remain inside the card.

Status colors:

| State | Color |
|-------|-------|
| Checking / loading | Gray |
| Available / healthy | Green |
| Unavailable / failed | Red |
| Not configured (PanSou empty `base_url`) | Gray (treat as unknown / not ready) |

**Telegram probe:** reuse existing auth status API (`telegramAuth.authorized`). Green when authorized; red when unauthorized or status request failed; gray while loading.

**PanSou probe:**

- On entering the config tab, after config load, and on an explicit「刷新状态」control on the PanSou card.
- Prefer a backend endpoint such as `POST /api/admin/subscription/pansou/status` (or GET with base URL from saved/draft config) that server-side requests the configured PanSou `base_url` (with short timeout) and returns `{ ok, message, latency_ms? }`.
- Do **not** rely on browser direct fetch to arbitrary PanSou hosts (CORS / mixed-content risk).
- Probe uses the **current form** `base_url` (draft), so users can test before save.
- If `base_url` is empty: gray + message「未配置」.

### D. Delivery card (139)

Inline form (not modal) showing the current「通用」defaults:

- Default network-disk / storage target provider.
- Default download folder / storage target path.
- Existing helper text about relative paths.

### E. Priority bar

- Horizontal strip of small blocks for each source provider in `transfer_priority` order.
- Display labels: `123` / `光鸭` / `115` / `夸克` / `阿里` (localized).
- Connectors `→` between blocks (visual only).
- Drag-and-drop reorder (HTML5 DnD or existing Hope/sortable pattern if present).
- On drop: rewrite `config.telegram.transfer_priority` to the new order.
- Always include the five providers (`pan123`, `guangyapan`, `pan115`, `quark`, `aliyun_drive`); if backend returns a partial/legacy list, normalize via the same rules as backend `normalizeTransferPriority` (frontend mirror or round-trip after load).

### F. Source-disk cards + modal

Cards (summary only):

- Provider display name.
- Channel count (or「未配置频道」).
- Temp-transfer target summary (provider + folder).
- Blacklist / delete-source-after on/off badge.
- Token/cookie configured badge when applicable (without revealing secrets).

Click card → modal with the **existing** pan editor fields (channels, temp transfer target, delete-source-after, aliyun tokens, etc.). Confirm/close writes back into `config.telegram[panKey]`.

Add `guangyapan` to `telegramPanItems` and locales (`subscription.telegram_pan_names.guangyapan` = 「光鸭」/「光鸭网盘」).

### G. Component structure (frontend)

Prefer extracting from `SubscriptionManagement.tsx` rather than growing it further:

- `SubscriptionConfigBoard` — regions + save/refresh wiring.
- `ServiceStatusDot` — shared status indicator.
- `TelegramConfigCard` / `PanSouConfigCard` / `DeliveryConfigCard`.
- `SourcePriorityBar`.
- `SourcePanCard` + `SourcePanEditModal` (reuse current pan form body).

### H. Backend (minimal)

- GuangYaPan Chinese help (A).
- Optional but recommended: PanSou status probe handler under existing admin subscription routes, reusing HTTP client timeouts consistent with other subscription outbound calls.
- No change to save/load config schema beyond ensuring `guangyapan` continues to round-trip (already on backend).

## Interaction Summary

1. Open 订阅配置 → load config → Telegram status refresh + PanSou probe.
2. Edit Telegram/PanSou/Delivery fields on-card.
3. Drag priority blocks to reorder transfer preference.
4. Click a source card → modal edit → close → summary updates.
5. Click 保存 → existing `SaveSubscriptionConfig` payload (includes `transfer_priority` and `guangyapan`).

## Testing

- GuangYaPan storage form shows Chinese help/alert.
- Config tab renders three regions; no full vertical pan stack.
- Priority drag updates order and survives save/reload.
- Source modal edits persist on save.
- `guangyapan` appears in priority bar and source cards.
- Telegram dot: authorized vs unauthorized vs loading.
- PanSou dot: empty URL / healthy / unhealthy / loading.
- Typecheck + production frontend build; relevant Go tests for normalize priority / new status handler if added.

## Acceptance Criteria

- GuangYaPan storage gray hints are Chinese.
- Subscription config is organized into Services / Delivery / Sources regions with cards.
- Telegram & PanSou cards keep full forms and show status dots with probing behavior as specified.
- Source disks are card summaries editable via modal.
- Priority bar drag order maps to `telegram.transfer_priority` and includes 光鸭.
- Existing save/refresh flow still works; no schema break for older configs after normalize.

## Risks / Mitigations

- **CORS if probing from browser:** use backend proxy status endpoint.
- **Large TSX file:** extract board/card components before adding drag + modal.
- **Legacy priority lists without guangyapan:** normalize on load to match backend defaults.
