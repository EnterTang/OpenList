# Subscription Detail Error Display Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep subscription detail rows visually balanced by truncating long recent errors to two lines while allowing users to copy the complete error text.

**Architecture:** Keep the backend and stored error data unchanged. Add a small frontend presentation helper inside the existing subscription detail modal that selects the displayed error, renders a bounded two-line summary, and invokes the existing `useUtil().copy` clipboard path from a compact action button.

**Tech Stack:** SolidJS, TypeScript, Hope UI, `copy-to-clipboard` through `useUtil`, existing i18n and notification utilities.

## Global Constraints

- Keep the detail table error column bounded by the existing `maxW`.
- Limit visible error text to two lines with ellipsis.
- Copy the complete original error string, not the truncated summary.
- Do not add inline expansion.
- Preserve existing error color and `-` empty state.
- Do not modify backend error content or persistence.
- Keep the modal open after copying.

---

### Task 1: Add a reusable compact error-cell renderer

**Files:**
- Modify: `/Volumes/extend Disk/Github/OpenList-Frontend/src/pages/home/SubscriptionManagement.tsx`
- Verify: `/Volumes/extend Disk/Github/OpenList-Frontend/package.json` scripts and the frontend typecheck/build commands

**Interfaces:**
- Add a local `SubscriptionErrorCell` component or equivalent renderer.
- Inputs: `error?: string`, `onCopy: (value: string) => boolean`.
- Output: compact error text and a copy button only when `error` is non-empty.

- [ ] **Step 1: Inspect the existing copy utility and component-test availability**

Use `useUtil().copy` from `src/hooks/useUtil.ts` and the existing `Button`, `Text`, `HStack`, and `Show` Hope UI components. Do not introduce a new clipboard dependency.

- [ ] **Step 2: Confirm the available frontend verification surface**

The frontend package exposes typecheck and production-build scripts but no component-test script. Use the typecheck/build checks and the manual visual verification in Task 3 for this presentational-only change. Keep the error selection and copy behavior in a small local renderer so it remains directly reviewable.

- [ ] **Step 3: Implement the compact renderer**

Use a bounded row layout similar to:

```tsx
<HStack alignItems="center" spacing="$1" gap="$1" minH="3.5rem">
  <Text
    color="$danger11"
    fontSize="$xs"
    flex="1"
    css={{
      wordBreak: "break-word",
      display: "-webkit-box",
      WebkitBoxOrient: "vertical",
      WebkitLineClamp: 2,
      overflow: "hidden",
    }}
  >
    {error || "-"}
  </Text>
  <Show when={error}>
    <Button
      size="xs"
      variant="ghost"
      aria-label={t("subscription.detail_copy_error")}
      title={t("subscription.detail_copy_error")}
      onClick={(event) => {
        event.stopPropagation()
        props.onCopy(error)
      }}
    >
      <AiOutlineCopy />
    </Button>
  </Show>
</HStack>
```

The actual implementation must use the component's existing `useT()` scope and `useUtil().copy`, preserve the red error color only for non-empty errors, and keep the existing `maxW="18rem"` cell boundary.

- [ ] **Step 4: Verify the focused frontend checks**

Run:

```bash
pnpm exec prettier --write src/pages/home/SubscriptionManagement.tsx
pnpm exec tsc -p tsconfig.json --noEmit
```

Expected: PASS.

- [ ] **Step 5: Commit the compact error renderer**

```bash
git add src/pages/home/SubscriptionManagement.tsx
git commit -m "fix(subscription): compact detail error display"
```

---

### Task 2: Wire the renderer into the detail table and localize the copy action

**Files:**
- Modify: `/Volumes/extend Disk/Github/OpenList-Frontend/src/pages/home/SubscriptionManagement.tsx`
- Modify: `/Volumes/extend Disk/Github/OpenList-Frontend/src/lang/en/subscription.json`
- Modify: `/Volumes/extend Disk/Github/OpenList-Frontend/src/lang/zh-CN/subscription.json`
- Modify: `/Volumes/extend Disk/Github/OpenList-Frontend/src/lang/zh-TW/subscription.json`

**Interfaces:**
- Select error in this order: `current_stage_error || job_last_error || item_last_error`.
- Copy callback uses `useUtil().copy(fullError)`.
- New translation key: `subscription.detail_copy_error`.

- [ ] **Step 1: Add the localized copy label**

Add `subscription.detail_copy_error` to the three subscription locale files with these values:

```json
// src/lang/en/subscription.json
"detail_copy_error": "Copy error details"

// src/lang/zh-CN/subscription.json
"detail_copy_error": "复制错误详情"

// src/lang/zh-TW/subscription.json
"detail_copy_error": "複製錯誤詳情"
```

Keep locale fallback behavior intact and do not alter existing keys.

- [ ] **Step 2: Replace the full-error cell**

Replace the current full-width error `Text` in the recent-error column with `SubscriptionErrorCell`. Keep the source selection order unchanged and pass the complete selected string to the copy callback.

- [ ] **Step 3: Stabilize row sizing**

Keep the existing `maxW="18rem"` on the error cell and set the inner error layout to a stable minimum height with vertical centering. Do not set a fixed table height that would break responsive horizontal scrolling; constrain only the row content.

- [ ] **Step 4: Verify formatting and type safety**

Run:

```bash
pnpm exec prettier --write src/pages/home/SubscriptionManagement.tsx src/lang/en/subscription.json src/lang/zh-CN/subscription.json src/lang/zh-TW/subscription.json
pnpm exec tsc -p tsconfig.json --noEmit
```

Expected: PASS.

- [ ] **Step 5: Commit the table integration**

```bash
git add src/pages/home/SubscriptionManagement.tsx src/lang/en/subscription.json src/lang/zh-CN/subscription.json src/lang/zh-TW/subscription.json
git commit -m "feat(subscription): add copy action for detail errors"
```

---

### Task 3: Visual and regression verification

**Files:**
- Test: frontend build/typecheck and manual subscription-detail verification
- Modify: no production files unless verification identifies a concrete defect

- [ ] **Step 1: Build the frontend**

Run:

```bash
pnpm exec tsc -p tsconfig.json --noEmit
pnpm run build
```

Expected: PASS. Existing bundle-size warnings are acceptable if there are no type or build errors.

- [ ] **Step 2: Verify a long signed-URL error**

Open a subscription detail modal containing a long 139 signed URL error. Confirm the row remains aligned with neighboring rows, the error summary occupies no more than two lines, and the copy control is visible.

- [ ] **Step 3: Verify copying the complete error**

Click the copy control and paste into a text field. Confirm the pasted value contains the complete signed URL/error message, including its final error suffix, rather than the truncated display text. Confirm the existing copy-success notification appears and the modal remains open.

- [ ] **Step 4: Verify short and empty errors**

Confirm short errors retain the red styling and compact height. Confirm rows with no error display `-` and no copy control.

- [ ] **Step 5: Review the final diff**

Run:

```bash
git diff --check
git status --short
git log -2 --oneline
```

Ensure no backend files or generated build artifacts are included in the frontend change.
