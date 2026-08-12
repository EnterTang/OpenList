# Subscription Detail Error Display Design

## Goal

Keep subscription detail rows visually balanced when a recent error contains a long URL, stack trace, or transport message, while preserving access to the complete error for troubleshooting.

## Scope

- Give detail table rows a stable visual height.
- Render the recent-error column as a compact two-line summary with ellipsis.
- Add a copy action that copies the complete error text.
- Reuse the frontend's existing clipboard utility and copy-success/error notifications.
- Preserve the existing error color and `-` empty state.

Out of scope:

- Inline expansion of the full error.
- Changing backend error content or persistence.
- Truncating the error before it reaches the frontend.

## Current Behavior

The subscription detail modal renders the selected source rows in a table. The recent-error cell currently renders the full `current_stage_error`, `job_last_error`, or `item_last_error` text with word breaking. Long signed URLs therefore increase the row height and make the surrounding card/table spacing inconsistent.

## Design

Add a small presentational error cell component or local renderer with this behavior:

1. Select the first non-empty value from `current_stage_error`, `job_last_error`, and `item_last_error`.
2. When no value exists, render `-` with the existing neutral styling.
3. When a value exists, render a fixed-height compact text block limited to two lines using CSS line clamping and word breaking.
4. Render a small copy icon button beside the summary.
5. Copy the original unmodified error string, not the visual ellipsis summary.
6. Use the existing `useUtil().copy` helper so success and failure notifications remain consistent with the rest of the frontend.
7. Set an accessible label/title for the copy button using a localized subscription detail string.

The error cell width remains bounded by the current `maxW` so the table does not expand beyond its horizontal scroll container. The row content should use a stable minimum height and vertical alignment so long and short error cells do not create visually irregular cards.

## Interaction

- Clicking the copy button copies the complete error and does not expand the row.
- Copying does not trigger row selection, navigation, or modal close.
- The copy button is available for any non-empty error, including non-failed stage errors.
- The detail modal remains open after copying.

## Testing

- Verify empty errors render `-` and no copy button.
- Verify long errors render within the fixed two-line height.
- Verify the copy handler receives the complete original error.
- Verify short errors retain the same error color and compact layout.
- Run Prettier, TypeScript checking, and production build.
- Manually inspect a signed URL error and a short error in the detail modal.

## Acceptance Criteria

- A long recent error does not make one subscription row substantially taller than neighboring rows.
- The visible error summary is limited to two lines with an ellipsis.
- Clicking copy copies the entire error string and shows the existing copy notification.
- Empty error cells remain compact and display `-`.
- The change is frontend-only and does not alter stored error data.
