# Spec 0007 — Estate Prometheus `/metrics` & `/v1/metrics` Telemetry Surface

- **Status**: Active
- **Lane**: `glaeda` (`aegis`)
- **Surface**: `src/estate_http_admission.rs`, `tests/estate_http_admission_contract.rs`

## 1. Objective

Expose a deterministic, zero-cardinality-explosion Prometheus text-format (`0.0.4`) scrape surface on `GET /metrics` and `GET /v1/metrics` so Revenant (`:9100`) and estate scrapers can ingest Little's Law (`L = \lambda W`), Kingman `G/G/m` queue-wait, Cobham priority-wait, and Cortex delegated SVID freshness gauges alongside the existing JSON `/v1/health` surface.

## 2. Invariants

1. **Content-Type Fidelity**:
   - `EstateHttpExchangeResponse` carries `pub content_type: &'static str`.
   - JSON endpoints return `Content-Type: application/json`.
   - `GET /metrics` and `GET /v1/metrics` return `200 OK` with `Content-Type: text/plain; version=0.0.4; charset=utf-8`.
   - `POST /metrics` and `POST /v1/metrics` return `405 Method Not Allowed`.
2. **Bounded Label Cardinality & Zero Secret Leakage**:
   - Exported metric series use only compile-time constant labels (`service="glaeda",callsign="aegis"`) and the closed `EstateHealthStatus` vocabulary (`status="healthy"|"degraded"|"unavailable"`).
   - Dynamic strings (SPIFFE IDs, Principals, paths, digests) never appear in Prometheus labels.
3. **Exported Gauges**:
   - `glaeda_estate_svid_fresh` (`0` or `1`)
   - `glaeda_estate_svid_expires_at_ms`
   - `glaeda_estate_health_status` (one-hot across `healthy`, `degraded`, `unavailable`)
   - `glaeda_worker_pool_total_wip`
   - `glaeda_worker_pool_utilization_bps`
   - `glaeda_worker_pool_effective_service_time_ms`
   - `glaeda_worker_pool_littles_law_cycle_time_ms`
   - `glaeda_worker_pool_kingman_queue_wait_ms`
   - `glaeda_worker_pool_cobham_priority_wait_ms`
   - `glaeda_worker_pool_admitting` (`0` or `1`)
