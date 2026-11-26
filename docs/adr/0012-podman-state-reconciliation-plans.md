# ADR 0012: Podman state reconciliation plans

## Status

Accepted for the local preview prototype.

Retired: the preview code was removed from the tree; see [`docs/history/RETIRED_FEATURE_ISLANDS.md`](../history/RETIRED_FEATURE_ISLANDS.md).

## Context

ADR 0011 blocks operations that do not fit the observed Podman state. Some mismatches represent an already satisfied goal, not an error. Starting an already running preview and stopping an already inactive preview should avoid subprocess calls while remaining visible to callers and journals.

The execution layer needs a deterministic state decision before requesting authorization.

## Decision

SmolRunner plans an existing-container operation with one of three dispositions:

- `execute`: the reviewed command is appropriate for the observed state;
- `already_satisfied`: the requested state already holds and no subprocess should run;
- `blocked`: the state is missing, unsafe, transitional, unsupported, or incompatible with the command form.

The initial policy is:

| Operation | Execute | Already satisfied | Blocked |
| --- | --- | --- | --- |
| Start | configured, created, initialized, stopped, exited | running | paused, stopping, removing, dead, missing, unknown |
| Stop | running | configured, created, initialized, stopped, exited | paused, stopping, removing, dead, missing, unknown |
| Unforced remove | configured, created, initialized, stopped, exited | none while a container observation exists | running, paused, stopping, removing, dead, missing, unknown |

Create and inspect do not use existing-container mutation planning.

The plan records the operation, disposition, observed state when present, and one public reason. Plan fields are private and exposed through read-only accessors. The authorization function recomputes the plan and accepts only `execute`; callers handle `already_satisfied` as an explicit no-op.

## Consequences

- Reconciliation avoids redundant start and stop subprocesses.
- No-op outcomes remain explainable and serializable.
- A blocked state cannot be converted into a command by directly calling the public authorization gate.
- Remove remains conservative because a present observation always names a container that still needs removal.
- Future orchestration can journal executed, satisfied, and blocked outcomes separately.

## Deferred work

- Planning absence after a failed inspect or explicit `podman container exists` check.
- State freshness and immediate reinspection.
- Executing and journaling the planned outcome.
- Pause, unpause, and force-removal policy.
- Rootless Podman integration tests.
