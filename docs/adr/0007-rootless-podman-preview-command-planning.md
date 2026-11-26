# ADR 0007: Rootless Podman preview command planning

## Status

Accepted for the local preview prototype.

Retired: the preview code was removed from the tree; see [`docs/history/RETIRED_FEATURE_ISLANDS.md`](../history/RETIRED_FEATURE_ISLANDS.md).

## Context

Preview-slot planning decides whether a verified artifact creates, renews, or replaces a live preview. The next layer needs an exact local container specification without granting repository code a Podman socket or allowing raw image, limit, name, label, or command strings to reach an executor.

The local prototype uses rootless Podman through the existing dedicated runner-user boundary. This ADR covers command planning only. No command is executed by this slice.

## Decision: validated runtime inputs

A local preview container requires:

- an OCI artifact identity;
- a fully qualified lowercase registry/repository reference pinned to the artifact's exact SHA-256 digest;
- an explicitly allocated unprivileged host port;
- bounded memory, CPU, and PID limits;
- one installation ID, preview slot, and checked generation.

Mutable image tags, short names, schemes, option-shaped values, digest mismatches, unlimited resource values, privileged host ports, generation zero, and overflowing generations fail closed.

CPU is represented in thousandths of one CPU and rendered canonically for Podman's `--cpus` option. Memory is represented in MiB. The initial bounds are deliberately conservative and can become policy later.

## Decision: container identity

The container name is a bounded deterministic locator derived from the slot, generation, installation identity, and a non-cryptographic hash. The name never proves ownership.

Full ownership evidence is carried in fixed Podman labels:

- schema version;
- installation ID;
- lease ID;
- preview generation;
- artifact repository;
- artifact commit;
- artifact digest.

Any future start, stop, removal, adoption, or reconciliation path must inspect and match those labels before acting on a pre-existing container. The command planner marks cleanup operations as requiring matching labels but does not implement that observation path.

## Decision: reviewed command vectors

Commands use absolute `/usr/sbin/runuser` and `/usr/bin/podman` paths, an empty child environment supplied by the existing process executor, and the same allowlisted runner-user variables already used by host preparation.

The create vector includes:

- `--pull never`;
- `--cap-drop all`;
- `--security-opt no-new-privileges`;
- a read-only root filesystem;
- bounded memory, CPU, and PID settings;
- a private network namespace;
- a TCP port bound only to `127.0.0.1`;
- a bounded writable `/tmp` tmpfs;
- the fixed ownership labels;
- an option terminator before the digest-pinned image reference.

The planner emits separate create, start, inspect, stop, and remove commands. Stop and remove use Podman's idempotent `--ignore` behavior. Removal remains irreversible. Stopping or restarting can only compensate for process effects; it does not restore external side effects produced by a running application.

## Security consequences

- Repository code receives no Podman socket, command constructor, host credentials, or arbitrary argument channel.
- Image resolution cannot silently move to another digest.
- Port publication is loopback-only.
- Host networking, privileged mode, added capabilities, writable root filesystems, unlimited PIDs, and implicit pulls are absent from the plan.
- Names remain locators; labels plus installation state are required evidence.
- Cleanup commands are inert data until an executor proves ownership and authorizes irreversible removal.

## Deferred work

- Podman inspection decoding and exact-label comparison.
- Execution receipts containing immutable container IDs.
- Durable preview-container state.
- Port allocation and conflict recovery.
- Image acquisition and registry authentication.
- Health and readiness checks.
- Route planning and reverse-proxy ownership.
- Expiry supervision and crash recovery.
- Measured VPS startup, memory, CPU, and disk results.
