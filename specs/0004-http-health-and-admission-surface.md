# LeanSpec 0004 — Bounded HTTP/1.1 `/v1/health` and Admission Surface

- **Status**: Active
- **Owner**: `ctodie`
- **Repository**: `platform/runtime/glaeda`
- **Created**: 2026-10-15

## 1. Summary

Defines the bounded, zero-`unsafe`, std-only HTTP/1.1 control-plane surface ([`src/estate_http_admission.rs`](../src/estate_http_admission.rs)) for `glaeda` (`estate/w4G:p1`, callsign `aegis`). This surface allows **Revenant** (`godseat status`, `wayfinder`, `mesh-pool`) and **Cortex** (`cortex-worker`) to probe lane health and evaluate batch admission over loopback HTTP/1.1 while preserving `cargo --locked` and zero external HTTP crate dependencies.

## 2. Invariants & Security Controls

1. **Loopback-Only Bind Guard (`validate_loopback_bind_addr`)**:
   - Control-plane listeners must bind strictly to loopback (`127.0.0.1` or `::1`).
   - Wildcard (`0.0.0.0`, `::`) and private/LAN (`10.0.0.0/8`, `192.168.0.0/16`, etc.) bind addresses are rejected fail-closed (`non_loopback_bind_forbidden`).
2. **Bounded Request Parsing & Schema Privacy**:
   - Hard ceiling `MAX_ESTATE_HTTP_REQUEST_BYTES = 65_536` bytes (`413 Payload Too Large` on overflow) and `MAX_ESTATE_ADMISSION_BATCH_ENVELOPES = 64`.
   - Wire payloads enforce `#[serde(deny_unknown_fields)]` and map schema errors to static codes (`invalid_json_payload`) so attacker-controlled field names or raw bodies are never echoed.
3. **Endpoints**:
   - `GET /v1/health` (aliases `GET /health`, `GET /healthz`): Returns a content-addressed `EstateHealthReport` (`200 OK` when SVID is fresh and admission is `Admit`; `503 Service Unavailable` when SVID is expired or pool is in `DbrHold` / `MemoryBackpressure`).
   - `POST /v1/admission/evaluate`: Verifies Cortex delegated lane SVID freshness at `observed_at` with zero grace (`403 Forbidden` on expiry), evaluates `WorkerPoolFlowMetrics`, solves the `MeshPoolDeliveryEnvelope` batch via [`CortexRevenantBridge`](../src/cortex_revenant_bridge.rs), and returns a content-addressed `EstateAdmissionResponse`.

## 3. Verification

- `cargo test -j 2 --locked --lib estate_http_admission`
- `cargo test -j 2 --locked --test estate_http_admission_contract`
- `make smoke`
