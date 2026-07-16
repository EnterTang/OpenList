# Subscription Execution Role Routing Design

## Problem

The automatic subscription scheduler selects local or cluster execution from
the configured cluster role, but the manual subscription check endpoint always
calls the local execution path. On a Hybrid or Coordinator deployment, a
manual rerun therefore creates local transfer tasks instead of cluster jobs.
Those items have no cluster job ID and are displayed as running on the local
machine.

## Decision

Introduce one shared subscription execution entry point that accepts the
current cluster role and routes execution as follows:

- An empty role or `standalone` calls the existing local `Run` path and
  preserves the request's `transfer` flag.
- `hybrid`, `coordinator`, `worker`, and any other non-standalone role call the
  existing `RunCluster` path.

Both the automatic scheduler and the manual check handler will call this shared
entry point. The existing explicit `Run` and `RunCluster` functions remain
available for callers that intentionally require one execution mode.

## Transfer Semantics

The request-level `transfer` flag applies only to standalone local execution.
Cluster execution does not translate that flag into local work. It uses the
subscription's persisted `transfer_enabled` setting and the registered cluster
dispatcher to determine whether media jobs are created.

This matches automatic execution and prevents manual cluster reruns from
bypassing worker eligibility, capability matching, leases, and assignment.

## Error Handling

The shared entry point returns the selected execution path's result and error
without transforming them. Existing HTTP status handling and scheduler error
logging remain unchanged.

## Compatibility

- Standalone manual checks preserve their current `transfer` behavior.
- Automatic standalone checks remain local.
- Automatic cluster checks continue using `RunCluster`.
- Manual checks on cluster roles change intentionally from local execution to
  cluster execution.
- No database schema, API payload, or dependency changes are required.

## Testing

Add regression coverage proving that:

- Empty and `standalone` roles select local execution.
- `hybrid`, `coordinator`, and `worker` roles select cluster execution.
- The scheduler and manual handler use the same shared entry point.
- A manual Hybrid execution does not enter the local transfer path and instead
  dispatches through the cluster path.

Run focused subscription and handler tests, then the repository's standard Go
test, formatting, vet, and static-analysis checks supported by the project.
