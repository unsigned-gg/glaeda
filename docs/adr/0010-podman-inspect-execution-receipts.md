# ADR 0010: Podman inspect execution receipts

## Status

Accepted for the local preview prototype.

Retired: the preview code was removed from the tree; see [`docs/history/RETIRED_FEATURE_ISLANDS.md`](../history/RETIRED_FEATURE_ISLANDS.md).

## Context

ADR 0008 decodes Podman inspect output and authorizes existing-container mutations from exact ownership evidence. Its first API accepted a decoded observation directly. That left the caller responsible for proving the bytes came from the reviewed inspect command and a successful process execution.

A caller could accidentally pair output from another command, another environment, or a failed execution with the ownership decoder. The inspect result needs a typed binding to the command and public execution evidence that produced it.

## Decision: bind command evidence before decoding

SmolRunner creates a `PreviewInspectExecutionReceipt` only when all of the following hold:

- the planned command is the read-only Podman inspect operation;
- the command targets the planned preview container name;
- the execution record's displayed argv exactly matches the reviewed command;
- the execution record's environment-key set exactly matches the reviewed command;
- the process completed successfully with status zero;
- stdout contains no Unicode replacement character that could indicate lossy byte conversion;
- stdout decodes as one bounded Podman container observation.

Bounded stderr may be retained as a diagnostic. It never contributes ownership evidence.

## Decision: public authorization requires a receipt

The public existing-container authorization function accepts a `PreviewInspectExecutionReceipt`, not a raw observation. The lower-level observation authorization function becomes crate-visible for the receipt layer and its unit tests.

Authorization still recomputes exact ownership from the planned preview specification and the receipt's observation. A receipt cannot authorize another generation, artifact, installation, lease, or container name. The resulting start, stop, or remove command targets the observed full container ID.

## Security consequences

- Arbitrary decoded bytes cannot enter the public mutation-authorization path directly.
- Failed or command-mismatched executions fail closed before ownership classification.
- Lossy UTF-8 conversion cannot silently alter machine-readable ownership evidence.
- Stderr remains diagnostic data and carries no authority.
- Replaying a receipt targets the same full container ID; deletion or replacement causes the command to fail safely.

## Deferred work

- Making execution records unforgeable outside the trusted executor.
- Attaching monotonic timestamps or freshness policy to receipts.
- State-specific start, stop, and remove authorization.
- Durable inspect receipts and container records.
- Wall-clock execution timeouts and descendant termination.
