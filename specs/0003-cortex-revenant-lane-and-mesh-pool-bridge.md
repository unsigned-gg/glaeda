# LeanSpec 0003 — Cortex + Revenant Lane & Mesh-Pool Execution Bridge

- **Status**: Active
- **Owner**: `ctodie`
- **Repository**: `platform/runtime/glaeda`
- **Created**: 2026-10-15

## 1. Summary

Defines the architectural bridge ([`src/cortex_revenant_bridge.rs`](../src/cortex_revenant_bridge.rs)) connecting **Cortex** (`cortex-edge` delegated SPIFFE lane SVIDs and `cortex-worker` governed dispatch), **Revenant** (`godseat` harness identity, `wayfinder` 4-rank Cobham $M/G/1$ priority queueing, and `mesh-pool` `PoolDelivery` envelopes), and **Glaeda** ([`WorkerPoolKernelSolver`](../src/worker_pool_kernel_solver.rs)).

## 2. Three-Way Contract

1. **Cortex Delegated Lane Identity (`cortex-edge::lane_svid` ↔ `glaeda::cortex_revenant_bridge::CortexLaneSpiffeId`)**:
   - Validates DNS-1123 lowercase `host` and `role` labels (`[a-z0-9-]`, 1..=63 chars, no leading or trailing hyphen) matching `cortex-edge::lane_svid::valid_role`.
   - Constructs canonical lane SPIFFE IDs `spiffe://unsigned.gg/host/<host>/lane/<role>` and SPIRE delegated attestation selectors `cortex:pane-role:<role>`.
   - Enforces zero-grace SVID freshness (`issued_at <= observed_at < expires_at`) via `CortexDelegatedLaneSvid::verify_fresh`.
2. **Revenant Priority & Mesh-Pool Delivery (`revenant::wayfinder` & `mesh-pool` ↔ `glaeda::cortex_revenant_bridge`)**:
   - Maps `wayfinder::queueing::Priority` (`Permission = 0`, `Input = 1`, `Ready = 2`, `Idle = 3`) onto `PersonalWorkerPriority` (`Interactive`, `Normal`, `Background`) with base scores (`300`, `200`, `100`, `0`) and Kleinrock linear aging (`aged_score`).
   - Converts `MeshPoolDeliveryEnvelope` (`stream_seq`, `delivery_token`, `delivery_attempt`, `lease_ms`, `from_peer`, `from_session`, `subject`, `body`, `traceparent`, `correlation`, `family_key`, `wayfinder_priority`, `limits`, `submitted_at`) into [`KernelSolverTask`](../src/worker_pool_kernel_solver.rs) items.
   - Emits a content-addressed `CortexRevenantBridgeReceipt<O>` containing `lane_spiffe_id`, `lane_selector`, `godseat_principal`, `svid_digest`, `WorkerPoolBatchReport<O>`, and `bridge_receipt_digest` (`Sha256Digest`).
3. **Godseat Harness Identity (`estate::harness::identity` ↔ `GodseatLanePrincipal`)**:
   - Binds lane commits and receipts to principal `harness-glaeda@agents.cerebral.work` in namespace `git` and refuses key directories inside `.ssh`.

## 3. Verification

- `cargo test -j 2 --locked --test cortex_revenant_bridge_contract`
- `make smoke`
