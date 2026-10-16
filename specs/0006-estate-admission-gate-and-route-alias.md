# LeanSpec 0006 — Estate Admission Gate Enforcement and `/v1/estate/admit` Route Alias

- **Status**: Active
- **Owner**: `ctodie`
- **Repository**: `platform/runtime/glaeda`
- **Created**: 2026-10-16

## 1. Summary

Hardens the `glaeda` (`aegis`) estate HTTP control-plane admission surface ([`src/estate_http_admission.rs`](../src/estate_http_admission.rs)) with two invariants:
1. **Canonical Route Alias (`POST /v1/estate/admit`)**: Routes `POST /v1/estate/admit` identically to `POST /v1/admission/evaluate` (and rejects `GET /v1/estate/admit` with `405 Method Not Allowed`), matching `cerebral-work/revenant` (`docs/specs/ecosystem-integration-spec.md` §1.5).
2. **Cumulative Batch Admission Gate (`PoolAdmissionDecision`)**: Evaluates [`WorkerPoolFlowMetrics`](../src/worker_pool_kernel_solver.rs) across all envelopes in the incoming batch before dispatching work. Only batches whose cumulative evaluation remains [`PoolAdmissionDecision::Admit`](../src/worker_pool_kernel_solver.rs) execute with `"admitted:..."` (`admitted: true`); batches hitting `HoldBufferFull`, `HoldMemoryBound`, `HoldCpuCapacity`, `HoldUtilizationCliff`, or `RejectUnstableQueue` return `admitted: false` with zero worker execution units (`actual_duration_ms = 0`) and a deterministic hold/reject summary tag bound into `response_digest`.

## 2. Invariants

- `POST /v1/estate/admit` and `POST /v1/admission/evaluate` produce byte-identical [`EstateAdmissionResponse`](../src/estate_http_admission.rs) payloads and SHA-256 digests for identical inputs.
- Multi-envelope batches accumulate `queue_depth`, `in_flight_bytes`, and `in_flight_cpu_millis` across envelopes during admission evaluation so a batch cannot bypass the DBR buffer cap or `MemoryBound` ceiling by splitting demand across envelopes.
- `compute_admission_response_digest` binds `admitted` alongside `flow_metrics` and `bridge_receipt_digest`.

## 3. Verification

- `cargo test -j 2 --locked --lib estate_http_admission`
- `cargo test -j 2 --locked --test estate_http_admission_contract`
- `make smoke && make fmt`
