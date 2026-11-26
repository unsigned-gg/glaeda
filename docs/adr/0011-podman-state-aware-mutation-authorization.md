# ADR 0011: Podman state-aware mutation authorization

## Status

Accepted for the local preview prototype.

Retired: the preview code was removed from the tree; see [`docs/history/RETIRED_FEATURE_ISLANDS.md`](../history/RETIRED_FEATURE_ISLANDS.md).

## Context

ADR 0010 binds exact Podman inspection evidence to a successful reviewed command. Exact ownership alone does not make every container operation sensible. A managed container may already be running, paused, stopping, removing, or dead.

The current command planner uses ordinary `podman start`, `podman stop`, and unforced `podman rm`. Authorization needs a conservative state policy that matches those command forms before execution wiring begins.

## Decision

The public existing-container mutation gate checks observed Podman state before applying the exact ownership gate.

Start is allowed from:

- `configured`;
- `created`;
- `initialized`;
- `stopped`;
- `exited`.

Stop is allowed only from `running`.

Unforced remove is allowed from:

- `configured`;
- `created`;
- `initialized`;
- `stopped`;
- `exited`.

Missing state, `paused`, `removing`, `stopping`, `dead`, and unrecognized states fail closed for every mutation. Start against `running`, stop against an inactive state, and remove against a running or paused container also fail closed. Create and inspect do not enter this existing-container mutation gate.

The ownership-only receipt helper becomes crate-visible. The public function performs the state check and then invokes the exact receipt and ownership authorization from ADRs 0008 and 0010. Approved commands continue targeting the observed full container ID.

## Consequences

- Exact ownership cannot authorize a nonsensical operation.
- Paused containers require a future explicit pause policy; they are never silently treated as stopped.
- Dead or transitional containers require reconciliation or an explicitly reviewed force policy.
- Running start requests and inactive stop requests become reconciliation decisions instead of subprocess calls.
- The current remove command remains unforced and therefore excludes running, paused, and unusable states.

## Deferred work

- Planning explicit no-op outcomes for already satisfied state.
- Pause and unpause support.
- Reviewed force-removal policy for dead or unusable containers.
- Reinspection or freshness policy immediately before mutation.
- Actual execution wiring and integration tests on a rootless Podman host.
