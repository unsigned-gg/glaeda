---
status: active
created: 2026-10-15
tags:
  - runtime
  - isolation
  - security
  - workload-identity
  - receipts
priority: high
---

# 0001: Trust-Tiered Hot Runtime, Workload Identity, and Receipt Verification

## Overview

`glaeda` provides the sandboxed execution fabric and hot-runtime pool for agent and scientific workloads across the estate. This specification records how `glaeda` enforces trust-tier isolation, workload identity tokens, and cryptographic execution receipts, aligning with the Cortex runtime architecture (`cerebral-work/cortex`).

## Invariants

1. **Trust-Tiered Warm Pool Partitioning (`src/hot_runtime_pool.rs`)**
   - Warm runtime instances are partitioned by `TrustTier` and compatibility hash (`HotRuntimePoolSpec::validate`).
   - Cross-tier reuse of warm sandboxes is forbidden.
   - Eviction and lease replenishment follow deterministic lifecycle state transitions (`WarmInstanceLifecycleState`).

2. **Linux Sandboxing & Egress Enforcement (`src/linux_isolation.rs`, `src/egress_proxy.rs`)**
   - Unprivileged execution uses Linux namespaces, Bubblewrap (`bwrap`), Landlock LSM, seccomp-bpf filters, and cgroup v2 resource limits (`CPUQuota`, `MemoryMax`, `TasksMax`).
   - Direct ambient network access is blocked inside the sandbox; outbound connections traverse the host-enforced egress proxy (`src/egress_proxy.rs`).

3. **Short-Lived Workload Identity (`src/workload_identity.rs`)**
   - Each admitted execution receives a short-lived, scope-bound `WorkloadIdentityToken` signed with an HMAC key (`WorkloadIdentitySigningKey`) and bound to the workspace, tenant, trust tier, and expiry timestamp.

4. **Tamper-Evident Receipt Chains (`src/receipt_verification.rs`, `src/artifact_attestation.rs`)**
   - Every task execution emits a canonical SHA-256 receipt chain (`Sha256Digest`) covering the policy digest, input artifacts, resource usage, and exit status.

## Verification

- `cargo test -j 2 --locked --test threat_model_enforcement_bindings`
- `cargo test -j 2 --locked --test module_inventory`
