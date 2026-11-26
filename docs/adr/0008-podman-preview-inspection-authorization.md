# ADR 0008: Podman preview inspection and mutation authorization

## Status

Accepted for the local preview prototype.

Retired: the preview code was removed from the tree; see [`docs/history/RETIRED_FEATURE_ISLANDS.md`](../history/RETIRED_FEATURE_ISLANDS.md).

## Context

ADR 0007 produces reviewed Podman command vectors and marks start, stop, and remove as requiring exact ownership evidence. A container name remains a locator and can be reused after deletion. The executor therefore needs a fresh, bounded observation before it can authorize a mutation against an existing container.

Podman container inspection returns a JSON array containing the full container ID, name, image digest, state, and nested configuration labels. The decoder needs only those fields and must tolerate unrelated current and future Podman fields.

## Decision: bounded inspect decoding

SmolRunner decodes at most one MiB of `podman container inspect` output and requires exactly one array element. The decoder revalidates:

- the full 64-character lowercase container ID;
- the bounded container name;
- the optional canonical SHA-256 image digest;
- the optional bounded lowercase state value;
- bounded label count, keys, and values.

Malformed JSON, zero or multiple results, unsafe identifiers, malformed digests, excessive labels, and oversized output fail closed. Unknown Podman fields are ignored because SmolRunner does not authorize from them.

## Decision: specialized preview ownership assessment

An observed preview container is managed only when all of the following match the planned generation:

- the exact derived container name;
- the exact immutable image digest;
- every expected SmolRunner ownership label.

Assessment classes are:

- `managed`: all required evidence matches;
- `foreign`: installation or lease ownership labels name another owner;
- `conflicting`: the locator, generation, artifact evidence, or image digest differs;
- `unknown`: required evidence is missing or uses an unsupported marker version.

Extra unrelated image or container labels do not authorize or invalidate ownership. A name match alone never authorizes a mutation.

## Decision: authorize by immutable container ID

Start, stop, and remove commands pass through one existing-container authorization gate. The gate recomputes ownership from the planned container specification and fresh inspection result. When managed, it clones the reviewed command and replaces the final name locator with the observed full container ID.

Create and inspect do not pass through this gate. Read-only inspection remains available to gather evidence. An unmanaged, foreign, conflicting, or unknown observation cannot produce an authorized mutation command.

## Security consequences

- Name reuse between inspection and mutation is reduced by targeting the immutable container ID.
- Callers cannot supply an assessment produced for another observation because authorization recomputes it internally.
- Missing labels and missing image digests deny mutation.
- Repository code still receives neither the Podman socket nor the command constructor.
- The decoder bounds the accepted document, though process-output capture limits remain an executor concern.

## Deferred work

- Running the inspect command and binding its execution receipt to the decoded observation.
- Bounded subprocess stdout and stderr capture.
- State-specific start, stop, and remove policy.
- Durable container records and last-observed container IDs.
- Port allocation, readiness checks, routing, expiry, and crash recovery.
