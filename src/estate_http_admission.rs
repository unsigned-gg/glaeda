//! Bounded, zero-`unsafe`, std-only HTTP/1.1 `/v1/health` and `/v1/admission/evaluate`
//! control-plane surface for the `glaeda` (`aegis`) estate lane.
//!
//! See `specs/0004-http-health-and-admission-surface.md` for the full architectural contract.

use std::fmt;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::artifact::Sha256Digest;
use crate::cortex_revenant_bridge::{
    CortexRevenantBridge, CortexRevenantBridgeReceipt, MeshPoolDeliveryEnvelope, WayfinderPriority,
};
use crate::execution_admission::{EpochMillis, ExecutionResourceLimits};
use crate::personal_worker_queue::PersonalWorkerPriority;
use crate::worker_pool_kernel_solver::{PoolAdmissionDecision, WorkerPoolFlowMetrics};

pub const ESTATE_HTTP_ADMISSION_SCHEMA_VERSION: u8 = 1;
pub const ESTATE_SERVICE_NAME: &str = "glaeda";
pub const ESTATE_LANE_CALLSIGN: &str = "aegis";
pub const CONTENT_TYPE_JSON: &str = "application/json";
pub const CONTENT_TYPE_PROMETHEUS_TEXT: &str = "text/plain; version=0.0.4; charset=utf-8";
pub const MAX_ESTATE_HTTP_REQUEST_BYTES: usize = 64 * 1024;
pub const MAX_ESTATE_ADMISSION_BATCH_ENVELOPES: usize = 64;
pub const DEFAULT_HTTP_IO_TIMEOUT_MILLIS: u64 = 5_000;

/// Structured, path-free, secret-free error returned by the estate HTTP control-plane surface.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EstateHttpAdmissionError {
    pub field: &'static str,
    pub code: &'static str,
    pub message: &'static str,
}

impl EstateHttpAdmissionError {
    #[must_use]
    pub const fn new(field: &'static str, code: &'static str, message: &'static str) -> Self {
        Self {
            field,
            code,
            message,
        }
    }
}

impl fmt::Display for EstateHttpAdmissionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({}): {}", self.field, self.code, self.message)
    }
}

impl std::error::Error for EstateHttpAdmissionError {}

/// Verify that a control-plane socket address binds strictly to loopback (`127.0.0.0/8` or `::1`).
///
/// Wildcard (`0.0.0.0`, `::`) and private/LAN addresses are rejected fail-closed so the control
/// plane never exposes an external network listener.
///
/// # Errors
///
/// Returns `non_loopback_bind_forbidden` if `addr.ip()` is not a loopback address.
pub fn validate_loopback_bind_addr(
    addr: SocketAddr,
) -> Result<SocketAddr, EstateHttpAdmissionError> {
    if !addr.ip().is_loopback() {
        return Err(EstateHttpAdmissionError::new(
            "bind_addr",
            "non_loopback_bind_forbidden",
            "estate HTTP control plane must bind strictly to a loopback address (127.0.0.1 or ::1)",
        ));
    }
    Ok(addr)
}

/// Overall health classification for the `glaeda` (`aegis`) runtime lane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EstateHealthStatus {
    /// SVID is fresh and worker-pool solver is admitting work below the Kingman cliff.
    Healthy,
    /// SVID is fresh, but worker-pool solver is holding new work due to DBR or memory backpressure.
    Degraded,
    /// Delegated lane SVID is expired or not yet valid.
    Unavailable,
}

/// Content-addressed health and capacity report returned by `GET /v1/health` (and `/healthz`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EstateHealthReport {
    pub schema_version: u8,
    pub service: &'static str,
    pub callsign: &'static str,
    pub status: EstateHealthStatus,
    pub lane_spiffe_id: String,
    pub lane_selector: String,
    pub godseat_principal: String,
    pub svid_fresh: bool,
    pub svid_expires_at: EpochMillis,
    pub flow_metrics: WorkerPoolFlowMetrics,
    pub report_digest: Sha256Digest,
}

/// Build a deterministic, content-addressed [`EstateHealthReport`] for the current bridge and pool
/// state without side effects.
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn build_estate_health_report(
    bridge: &CortexRevenantBridge,
    observed_at: EpochMillis,
    active_workers: usize,
    queue_depth: usize,
    in_flight_bytes: u64,
    in_flight_cpu_millis: u32,
    arrival_rate_milli_per_sec: u64,
    context_switch_rate_bps: u32,
) -> EstateHealthReport {
    let svid_fresh = bridge.lane_svid().verify_fresh(observed_at).is_ok();
    let probe_limits = ExecutionResourceLimits::new(1_000, 64 * 1024 * 1024, 16)
        .unwrap_or_else(|_| unreachable!());
    let flow_metrics = WorkerPoolFlowMetrics::evaluate(
        bridge.solver().config(),
        active_workers,
        queue_depth,
        in_flight_bytes,
        in_flight_cpu_millis,
        arrival_rate_milli_per_sec,
        context_switch_rate_bps,
        probe_limits,
        PersonalWorkerPriority::Normal,
    );

    let status = if !svid_fresh {
        EstateHealthStatus::Unavailable
    } else if flow_metrics.admission == PoolAdmissionDecision::Admit {
        EstateHealthStatus::Healthy
    } else {
        EstateHealthStatus::Degraded
    };

    let lane_spiffe_id = bridge.lane_svid().identity.spiffe_id.clone();
    let lane_selector = bridge.lane_svid().identity.selector.clone();
    let godseat_principal = bridge.godseat_principal().principal.clone();
    let svid_expires_at = bridge.lane_svid().expires_at;

    let report_digest = compute_health_report_digest(
        status,
        &lane_spiffe_id,
        &godseat_principal,
        svid_fresh,
        svid_expires_at,
        &flow_metrics,
    );

    EstateHealthReport {
        schema_version: ESTATE_HTTP_ADMISSION_SCHEMA_VERSION,
        service: ESTATE_SERVICE_NAME,
        callsign: ESTATE_LANE_CALLSIGN,
        status,
        lane_spiffe_id,
        lane_selector,
        godseat_principal,
        svid_fresh,
        svid_expires_at,
        flow_metrics,
        report_digest,
    }
}

/// Wire representation of a single Revenant `mesh-pool` delivery envelope inside an HTTP
/// `POST /v1/admission/evaluate` request body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EstateAdmissionWireEnvelope {
    pub stream_seq: u64,
    pub delivery_token: String,
    pub delivery_attempt: u32,
    pub lease_ms: u64,
    pub from_peer: String,
    pub from_session: Option<String>,
    pub subject: String,
    pub body: String,
    pub traceparent: Option<String>,
    pub correlation: Option<String>,
    pub family_key: String,
    pub wayfinder_priority: String,
    pub cpu_millis: u32,
    pub memory_bytes: u64,
    pub max_pids: u32,
    pub submitted_at_ms: u64,
}

impl EstateAdmissionWireEnvelope {
    /// Validate and convert this wire envelope into a domain [`MeshPoolDeliveryEnvelope`].
    ///
    /// # Errors
    ///
    /// Returns a static error receipt if any field violates domain bounds.
    pub fn into_domain(self) -> Result<MeshPoolDeliveryEnvelope, EstateHttpAdmissionError> {
        let wayfinder_priority = match self.wayfinder_priority.as_str() {
            "permission" => WayfinderPriority::Permission,
            "input" => WayfinderPriority::Input,
            "ready" => WayfinderPriority::Ready,
            "idle" => WayfinderPriority::Idle,
            _ => {
                return Err(EstateHttpAdmissionError::new(
                    "envelope.wayfinder_priority",
                    "invalid_wayfinder_priority",
                    "wayfinder_priority must be one of: permission, input, ready, idle",
                ));
            }
        };
        let limits = ExecutionResourceLimits::new(
            self.cpu_millis,
            self.memory_bytes,
            self.max_pids,
        )
        .map_err(|_| {
            EstateHttpAdmissionError::new(
                "envelope.limits",
                "invalid_resource_limits",
                "envelope resource limits must be positive and within personal-worker bounds",
            )
        })?;
        let submitted_at = EpochMillis::new(self.submitted_at_ms).map_err(|_| {
            EstateHttpAdmissionError::new(
                "envelope.submitted_at_ms",
                "invalid_submitted_at_ms",
                "submitted_at_ms must be a positive bounded epoch millisecond timestamp",
            )
        })?;
        let envelope = MeshPoolDeliveryEnvelope {
            stream_seq: self.stream_seq,
            delivery_token: self.delivery_token,
            delivery_attempt: self.delivery_attempt,
            lease_ms: self.lease_ms,
            from_peer: self.from_peer,
            from_session: self.from_session,
            subject: self.subject,
            body: self.body,
            traceparent: self.traceparent,
            correlation: self.correlation,
            family_key: self.family_key,
            wayfinder_priority,
            limits,
            submitted_at,
        };
        envelope
            .validate()
            .map_err(|err| EstateHttpAdmissionError::new(err.field, err.code, err.message))?;
        Ok(envelope)
    }
}

/// Wire request body for `POST /v1/admission/evaluate`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EstateAdmissionWireRequest {
    pub observed_at_ms: u64,
    pub active_workers: usize,
    pub queue_depth: usize,
    pub in_flight_bytes: u64,
    pub in_flight_cpu_millis: u32,
    pub arrival_rate_milli_per_sec: u64,
    pub context_switch_rate_bps: u32,
    pub envelopes: Vec<EstateAdmissionWireEnvelope>,
}

/// Content-addressed response returned by `POST /v1/admission/evaluate` (and `POST /v1/estate/admit`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EstateAdmissionResponse {
    pub schema_version: u8,
    pub admitted: bool,
    pub flow_metrics: WorkerPoolFlowMetrics,
    pub bridge_receipt: CortexRevenantBridgeReceipt<String>,
    pub response_digest: Sha256Digest,
}

/// Render a deterministic, fixed-cardinality Prometheus text-format (`0.0.4`) exposition payload
/// from an [`EstateHealthReport`].
#[must_use]
pub fn render_estate_prometheus_metrics(report: &EstateHealthReport) -> String {
    let svid_fresh_val: u8 = u8::from(report.svid_fresh);
    let healthy_val: u8 = u8::from(report.status == EstateHealthStatus::Healthy);
    let degraded_val: u8 = u8::from(report.status == EstateHealthStatus::Degraded);
    let unavailable_val: u8 = u8::from(report.status == EstateHealthStatus::Unavailable);
    let admitting_val: u8 = u8::from(report.flow_metrics.admission == PoolAdmissionDecision::Admit);

    format!(
        "# HELP glaeda_estate_svid_fresh Whether the Cortex delegated lane SVID is fresh (1=fresh, 0=expired).\n\
# TYPE glaeda_estate_svid_fresh gauge\n\
glaeda_estate_svid_fresh{{service=\"{svc}\",callsign=\"{cs}\"}} {svid_fresh_val}\n\
# HELP glaeda_estate_svid_expires_at_ms Epoch milliseconds when the Cortex delegated lane SVID expires.\n\
# TYPE glaeda_estate_svid_expires_at_ms gauge\n\
glaeda_estate_svid_expires_at_ms{{service=\"{svc}\",callsign=\"{cs}\"}} {expires_at}\n\
# HELP glaeda_estate_health_status One-hot health classification for the estate runtime lane.\n\
# TYPE glaeda_estate_health_status gauge\n\
glaeda_estate_health_status{{service=\"{svc}\",callsign=\"{cs}\",status=\"healthy\"}} {healthy_val}\n\
glaeda_estate_health_status{{service=\"{svc}\",callsign=\"{cs}\",status=\"degraded\"}} {degraded_val}\n\
glaeda_estate_health_status{{service=\"{svc}\",callsign=\"{cs}\",status=\"unavailable\"}} {unavailable_val}\n\
# HELP glaeda_worker_pool_total_wip Total work-in-progress L = active_workers + queue_depth.\n\
# TYPE glaeda_worker_pool_total_wip gauge\n\
glaeda_worker_pool_total_wip{{service=\"{svc}\",callsign=\"{cs}\"}} {total_wip}\n\
# HELP glaeda_worker_pool_utilization_bps Worker-pool utilization rho in basis points.\n\
# TYPE glaeda_worker_pool_utilization_bps gauge\n\
glaeda_worker_pool_utilization_bps{{service=\"{svc}\",callsign=\"{cs}\"}} {util_bps}\n\
# HELP glaeda_worker_pool_effective_service_time_ms Sweller context-switch adjusted service time in milliseconds.\n\
# TYPE glaeda_worker_pool_effective_service_time_ms gauge\n\
glaeda_worker_pool_effective_service_time_ms{{service=\"{svc}\",callsign=\"{cs}\"}} {svc_time_ms}\n\
# HELP glaeda_worker_pool_littles_law_cycle_time_ms Little's Law cycle time W = L / lambda in milliseconds.\n\
# TYPE glaeda_worker_pool_littles_law_cycle_time_ms gauge\n\
glaeda_worker_pool_littles_law_cycle_time_ms{{service=\"{svc}\",callsign=\"{cs}\"}} {cycle_time_ms}\n\
# HELP glaeda_worker_pool_kingman_queue_wait_ms Kingman G/G/m expected queue wait time in milliseconds.\n\
# TYPE glaeda_worker_pool_kingman_queue_wait_ms gauge\n\
glaeda_worker_pool_kingman_queue_wait_ms{{service=\"{svc}\",callsign=\"{cs}\"}} {kingman_ms}\n\
# HELP glaeda_worker_pool_cobham_priority_wait_ms Cobham non-preemptive priority queue wait time in milliseconds.\n\
# TYPE glaeda_worker_pool_cobham_priority_wait_ms gauge\n\
glaeda_worker_pool_cobham_priority_wait_ms{{service=\"{svc}\",callsign=\"{cs}\"}} {cobham_ms}\n\
# HELP glaeda_worker_pool_admitting Whether the DBR and memory-bound gate is currently admitting work (1=admit, 0=hold/reject).\n\
# TYPE glaeda_worker_pool_admitting gauge\n\
glaeda_worker_pool_admitting{{service=\"{svc}\",callsign=\"{cs}\"}} {admitting_val}\n",
        svc = ESTATE_SERVICE_NAME,
        cs = ESTATE_LANE_CALLSIGN,
        expires_at = report.svid_expires_at.get(),
        total_wip = report.flow_metrics.total_wip,
        util_bps = report.flow_metrics.utilization_bps,
        svc_time_ms = report.flow_metrics.effective_service_time_ms,
        cycle_time_ms = report.flow_metrics.littles_law_cycle_time_ms,
        kingman_ms = report.flow_metrics.kingman_queue_wait_ms,
        cobham_ms = report.flow_metrics.cobham_priority_wait_ms,
    )
}

/// Framed HTTP/1.1 response ready to serialize to a transport stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EstateHttpExchangeResponse {
    pub status_code: u16,
    pub reason_phrase: &'static str,
    pub content_type: &'static str,
    pub body_json: String,
}

impl EstateHttpExchangeResponse {
    #[must_use]
    pub fn to_http_bytes(&self) -> Vec<u8> {
        let header = format!(
            "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            self.status_code,
            self.reason_phrase,
            self.content_type,
            self.body_json.len()
        );
        let mut out = Vec::with_capacity(header.len() + self.body_json.len());
        out.extend_from_slice(header.as_bytes());
        out.extend_from_slice(self.body_json.as_bytes());
        out
    }
}

/// Process a single raw HTTP/1.1 request byte slice against [`CortexRevenantBridge`] without
/// mutating external state.
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn handle_http_request_bytes(
    bridge: &CortexRevenantBridge,
    request_bytes: &[u8],
    default_observed_at: EpochMillis,
    active_workers: usize,
    queue_depth: usize,
    in_flight_bytes: u64,
    in_flight_cpu_millis: u32,
    arrival_rate_milli_per_sec: u64,
    context_switch_rate_bps: u32,
) -> EstateHttpExchangeResponse {
    if request_bytes.len() > MAX_ESTATE_HTTP_REQUEST_BYTES {
        return error_response(
            413,
            "Payload Too Large",
            "request",
            "request_exceeds_max_bytes",
            "HTTP request exceeds the 65536-byte control-plane limit",
        );
    }

    let Some(header_end) = find_header_end(request_bytes) else {
        return error_response(
            400,
            "Bad Request",
            "request.headers",
            "malformed_http_headers",
            "HTTP request must terminate headers with CRLFCRLF",
        );
    };

    let Ok(header_text) = std::str::from_utf8(&request_bytes[..header_end]) else {
        return error_response(
            400,
            "Bad Request",
            "request.headers",
            "non_utf8_http_headers",
            "HTTP request headers must be valid ASCII/UTF-8",
        );
    };

    let body_bytes = &request_bytes[header_end + 4..];
    let mut lines = header_text.split("\r\n");
    let Some(request_line) = lines.next() else {
        return error_response(
            400,
            "Bad Request",
            "request.line",
            "empty_request_line",
            "HTTP request line is missing",
        );
    };

    let parts: Vec<&str> = request_line.split_whitespace().collect();
    if parts.len() != 3 || (parts[2] != "HTTP/1.1" && parts[2] != "HTTP/1.0") {
        return error_response(
            400,
            "Bad Request",
            "request.line",
            "invalid_request_line",
            "HTTP request line must be '<METHOD> <PATH> HTTP/1.1'",
        );
    }

    let method = parts[0];
    let raw_path = parts[1];
    let path = raw_path.split('?').next().unwrap_or(raw_path);

    let mut declared_content_length: Option<usize> = None;
    for line in lines {
        if line.is_empty() {
            continue;
        }
        if let Some((name, val)) = line.split_once(':')
            && name.trim().eq_ignore_ascii_case("content-length")
        {
            let Ok(parsed_len) = val.trim().parse::<usize>() else {
                return error_response(
                    400,
                    "Bad Request",
                    "request.content_length",
                    "invalid_content_length",
                    "Content-Length header must be a non-negative integer",
                );
            };
            declared_content_length = Some(parsed_len);
        }
    }

    if let Some(expected_len) = declared_content_length {
        if expected_len > MAX_ESTATE_HTTP_REQUEST_BYTES || body_bytes.len() < expected_len {
            return error_response(
                400,
                "Bad Request",
                "request.content_length",
                "content_length_mismatch",
                "HTTP request body length does not match declared Content-Length",
            );
        }
    }

    let effective_body = match declared_content_length {
        Some(len) => &body_bytes[..len],
        None => body_bytes,
    };

    match (method, path) {
        ("GET", "/v1/health" | "/health" | "/healthz") => {
            let report = build_estate_health_report(
                bridge,
                default_observed_at,
                active_workers,
                queue_depth,
                in_flight_bytes,
                in_flight_cpu_millis,
                arrival_rate_milli_per_sec,
                context_switch_rate_bps,
            );
            let (status_code, reason_phrase) = match report.status {
                EstateHealthStatus::Healthy => (200, "OK"),
                EstateHealthStatus::Degraded | EstateHealthStatus::Unavailable => {
                    (503, "Service Unavailable")
                }
            };
            let body_json = serde_json::to_string(&report).unwrap_or_else(|_| "{}".to_owned());
            EstateHttpExchangeResponse {
                status_code,
                reason_phrase,
                content_type: CONTENT_TYPE_JSON,
                body_json,
            }
        }
        ("GET", "/metrics" | "/v1/metrics") => {
            let report = build_estate_health_report(
                bridge,
                default_observed_at,
                active_workers,
                queue_depth,
                in_flight_bytes,
                in_flight_cpu_millis,
                arrival_rate_milli_per_sec,
                context_switch_rate_bps,
            );
            let body_text = render_estate_prometheus_metrics(&report);
            EstateHttpExchangeResponse {
                status_code: 200,
                reason_phrase: "OK",
                content_type: CONTENT_TYPE_PROMETHEUS_TEXT,
                body_json: body_text,
            }
        }
        ("POST", "/v1/health" | "/health" | "/healthz" | "/metrics" | "/v1/metrics")
        | ("GET", "/v1/admission/evaluate" | "/v1/estate/admit") => error_response(
            405,
            "Method Not Allowed",
            "request.method",
            "method_not_allowed",
            "HTTP method is not allowed for this control-plane endpoint",
        ),
        ("POST", "/v1/admission/evaluate" | "/v1/estate/admit") => {
            handle_admission_evaluate(bridge, effective_body)
        }
        _ => error_response(
            404,
            "Not Found",
            "request.path",
            "endpoint_not_found",
            "requested control-plane path does not exist",
        ),
    }
}

fn handle_admission_evaluate(
    bridge: &CortexRevenantBridge,
    body_bytes: &[u8],
) -> EstateHttpExchangeResponse {
    // Never echo serde's error string so attacker-controlled field names cannot leak into receipts.
    let Ok(wire_req) = serde_json::from_slice::<EstateAdmissionWireRequest>(body_bytes) else {
        return error_response(
            400,
            "Bad Request",
            "request.body",
            "invalid_json_payload",
            "request body failed strict schema validation",
        );
    };

    if wire_req.envelopes.is_empty() {
        return error_response(
            400,
            "Bad Request",
            "request.envelopes",
            "empty_admission_batch",
            "envelopes batch must contain at least one delivery envelope",
        );
    }
    if wire_req.envelopes.len() > MAX_ESTATE_ADMISSION_BATCH_ENVELOPES {
        return error_response(
            413,
            "Payload Too Large",
            "request.envelopes",
            "admission_batch_too_large",
            "envelopes batch exceeds MAX_ESTATE_ADMISSION_BATCH_ENVELOPES (64)",
        );
    }

    let Ok(observed_at) = EpochMillis::new(wire_req.observed_at_ms) else {
        return error_response(
            400,
            "Bad Request",
            "request.observed_at_ms",
            "invalid_observed_at_ms",
            "observed_at_ms must be a positive bounded epoch millisecond timestamp",
        );
    };

    if let Err(svid_err) = bridge.lane_svid().verify_fresh(observed_at) {
        return error_response(
            403,
            "Forbidden",
            svid_err.field,
            svid_err.code,
            svid_err.message,
        );
    }

    let mut domain_envelopes = Vec::with_capacity(wire_req.envelopes.len());
    for wire_env in wire_req.envelopes {
        match wire_env.into_domain() {
            Ok(env) => domain_envelopes.push(env),
            Err(err) => {
                return error_response(400, "Bad Request", err.field, err.code, err.message);
            }
        }
    }

    let mut running_queue_depth = wire_req.queue_depth;
    let mut running_in_flight_bytes = wire_req.in_flight_bytes;
    let mut running_in_flight_cpu_millis = wire_req.in_flight_cpu_millis;
    let mut gating_metrics: Option<WorkerPoolFlowMetrics> = None;

    for env in &domain_envelopes {
        let metrics = match bridge.evaluate_envelope_admission(
            env,
            observed_at,
            wire_req.active_workers,
            running_queue_depth,
            running_in_flight_bytes,
            running_in_flight_cpu_millis,
            wire_req.arrival_rate_milli_per_sec,
            wire_req.context_switch_rate_bps,
        ) {
            Ok(m) => m,
            Err(err) => {
                return error_response(400, "Bad Request", err.field, err.code, err.message);
            }
        };
        let is_admit = metrics.admission == PoolAdmissionDecision::Admit;
        gating_metrics = Some(metrics);
        if !is_admit {
            break;
        }
        running_queue_depth = running_queue_depth.saturating_add(1);
        running_in_flight_bytes = running_in_flight_bytes.saturating_add(env.limits.memory_bytes);
        running_in_flight_cpu_millis =
            running_in_flight_cpu_millis.saturating_add(env.limits.cpu_millis);
    }

    let flow_metrics = gating_metrics.unwrap_or_else(|| unreachable!());
    let admitted = flow_metrics.admission == PoolAdmissionDecision::Admit;
    let admission_decision = flow_metrics.admission;

    let bridge_receipt = match bridge.dispatch_batch(
        domain_envelopes,
        observed_at,
        wire_req.arrival_rate_milli_per_sec,
        |task| {
            let (prefix, duration_ms) = match admission_decision {
                PoolAdmissionDecision::Admit => ("admitted", 50),
                PoolAdmissionDecision::HoldBufferFull => ("held_buffer_full", 0),
                PoolAdmissionDecision::HoldMemoryBound => ("held_memory_bound", 0),
                PoolAdmissionDecision::HoldCpuCapacity => ("held_cpu_capacity", 0),
                PoolAdmissionDecision::HoldUtilizationCliff => ("held_utilization_cliff", 0),
                PoolAdmissionDecision::RejectUnstableQueue => ("rejected_unstable_queue", 0),
            };
            let summary = format!(
                "{prefix}:{}:seq={}:prio={:?}",
                task.family_key, task.payload.stream_seq, task.payload.wayfinder_priority
            );
            (summary, duration_ms)
        },
    ) {
        Ok(receipt) => receipt,
        Err(err) => {
            return error_response(400, "Bad Request", err.field, err.code, err.message);
        }
    };

    let response_digest = compute_admission_response_digest(
        admitted,
        &flow_metrics,
        &bridge_receipt.bridge_receipt_digest,
    );

    let response = EstateAdmissionResponse {
        schema_version: ESTATE_HTTP_ADMISSION_SCHEMA_VERSION,
        admitted,
        flow_metrics,
        bridge_receipt,
        response_digest,
    };

    let body_json = serde_json::to_string(&response).unwrap_or_else(|_| "{}".to_owned());
    EstateHttpExchangeResponse {
        status_code: 200,
        reason_phrase: "OK",
        content_type: CONTENT_TYPE_JSON,
        body_json,
    }
}

/// Serve up to `max_requests` HTTP/1.1 requests over a loopback-bound [`TcpListener`].
///
/// # Errors
///
/// Returns an error if the listener is not bound to loopback (`127.0.0.1` or `::1`) or if
/// socket accept/read/write fails.
#[allow(clippy::too_many_arguments)]
pub fn serve_bounded_loopback_requests(
    listener: &TcpListener,
    bridge: &CortexRevenantBridge,
    max_requests: usize,
    default_observed_at: EpochMillis,
    active_workers: usize,
    queue_depth: usize,
    in_flight_bytes: u64,
    in_flight_cpu_millis: u32,
    arrival_rate_milli_per_sec: u64,
    context_switch_rate_bps: u32,
) -> Result<usize, EstateHttpAdmissionError> {
    let local_addr = listener.local_addr().map_err(|_| {
        EstateHttpAdmissionError::new(
            "listener",
            "listener_local_addr_failed",
            "failed to query TcpListener local socket address",
        )
    })?;
    validate_loopback_bind_addr(local_addr)?;

    let timeout = Some(Duration::from_millis(DEFAULT_HTTP_IO_TIMEOUT_MILLIS));
    let mut served = 0usize;

    while served < max_requests {
        let (mut stream, _) = listener.accept().map_err(|_| {
            EstateHttpAdmissionError::new(
                "listener",
                "accept_failed",
                "failed to accept loopback HTTP connection",
            )
        })?;
        let _ = stream.set_read_timeout(timeout);
        let _ = stream.set_write_timeout(timeout);

        let request_bytes = read_bounded_http_request(&mut stream)?;
        let response = handle_http_request_bytes(
            bridge,
            &request_bytes,
            default_observed_at,
            active_workers,
            queue_depth,
            in_flight_bytes,
            in_flight_cpu_millis,
            arrival_rate_milli_per_sec,
            context_switch_rate_bps,
        );
        let wire_bytes = response.to_http_bytes();
        stream.write_all(&wire_bytes).map_err(|_| {
            EstateHttpAdmissionError::new(
                "stream",
                "write_failed",
                "failed to write HTTP response to loopback client",
            )
        })?;
        let _ = stream.flush();
        served += 1;
    }

    Ok(served)
}

fn read_bounded_http_request<R: Read>(reader: &mut R) -> Result<Vec<u8>, EstateHttpAdmissionError> {
    let mut buf = Vec::with_capacity(4096);
    let mut chunk = [0u8; 4096];
    let mut expected_total: Option<usize> = None;

    loop {
        if buf.len() > MAX_ESTATE_HTTP_REQUEST_BYTES {
            return Ok(buf);
        }
        if let Some(total) = expected_total
            && buf.len() >= total
        {
            break;
        }
        let n = reader.read(&mut chunk).map_err(|_| {
            EstateHttpAdmissionError::new(
                "stream",
                "read_failed",
                "failed to read HTTP request from loopback client",
            )
        })?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);

        if expected_total.is_none()
            && let Some(header_end) = find_header_end(&buf)
        {
            let content_len = parse_content_length(&buf[..header_end]).unwrap_or(0);
            expected_total = Some(header_end + 4 + content_len);
        }
    }

    Ok(buf)
}

fn find_header_end(bytes: &[u8]) -> Option<usize> {
    bytes.windows(4).position(|w| w == b"\r\n\r\n")
}

fn parse_content_length(header_bytes: &[u8]) -> Option<usize> {
    let text = std::str::from_utf8(header_bytes).ok()?;
    for line in text.split("\r\n").skip(1) {
        if let Some((name, val)) = line.split_once(':')
            && name.trim().eq_ignore_ascii_case("content-length")
        {
            return val.trim().parse::<usize>().ok();
        }
    }
    None
}

fn error_response(
    status_code: u16,
    reason_phrase: &'static str,
    field: &'static str,
    code: &'static str,
    message: &'static str,
) -> EstateHttpExchangeResponse {
    let err = EstateHttpAdmissionError::new(field, code, message);
    let body_json = serde_json::to_string(&err).unwrap_or_else(|_| {
        "{\"field\":\"request\",\"code\":\"internal_serialization_error\",\"message\":\"error\"}"
            .to_owned()
    });
    EstateHttpExchangeResponse {
        status_code,
        reason_phrase,
        content_type: CONTENT_TYPE_JSON,
        body_json,
    }
}

fn compute_health_report_digest(
    status: EstateHealthStatus,
    lane_spiffe_id: &str,
    godseat_principal: &str,
    svid_fresh: bool,
    svid_expires_at: EpochMillis,
    flow_metrics: &WorkerPoolFlowMetrics,
) -> Sha256Digest {
    let mut hasher = Sha256::new();
    hasher.update(b"glaeda.estate_health_report.v1:");
    let status_tag = match status {
        EstateHealthStatus::Healthy => "healthy",
        EstateHealthStatus::Degraded => "degraded",
        EstateHealthStatus::Unavailable => "unavailable",
    };
    hasher.update(status_tag.as_bytes());
    hasher.update(b":");
    hasher.update(lane_spiffe_id.as_bytes());
    hasher.update(b":");
    hasher.update(godseat_principal.as_bytes());
    hasher.update(b":");
    hasher.update(if svid_fresh { b"1" } else { b"0" });
    hasher.update(b":");
    hasher.update(svid_expires_at.get().to_le_bytes());
    hasher.update(b":");
    hasher.update(flow_metrics.utilization_bps.to_le_bytes());
    hasher.update(b":");
    hasher.update(flow_metrics.total_wip.to_le_bytes());
    let hex = format!("sha256:{:x}", hasher.finalize());
    Sha256Digest::parse(&hex).unwrap_or_else(|_| unreachable!())
}

fn compute_admission_response_digest(
    admitted: bool,
    flow_metrics: &WorkerPoolFlowMetrics,
    bridge_receipt_digest: &Sha256Digest,
) -> Sha256Digest {
    let mut hasher = Sha256::new();
    hasher.update(b"glaeda.estate_admission_response.v1:");
    hasher.update(if admitted { b"1:" } else { b"0:" });
    hasher.update(flow_metrics.utilization_bps.to_le_bytes());
    hasher.update(b":");
    hasher.update(flow_metrics.kingman_queue_wait_ms.to_le_bytes());
    hasher.update(b":");
    hasher.update(bridge_receipt_digest.as_str().as_bytes());
    let hex = format!("sha256:{:x}", hasher.finalize());
    Sha256Digest::parse(&hex).unwrap_or_else(|_| unreachable!())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cortex_revenant_bridge::{
        CortexDelegatedLaneSvid, CortexLaneSpiffeId, GodseatLanePrincipal,
    };
    use crate::worker_pool_kernel_solver::{WorkerPoolConfig, WorkerPoolKernelSolver};

    fn sample_bridge() -> CortexRevenantBridge {
        let spiffe = CortexLaneSpiffeId::new("ceres", "session-agy-ceres-estate-w3n-p1")
            .expect("valid spiffe");
        let digest =
            Sha256Digest::parse(&format!("sha256:{}", "ab".repeat(32))).expect("valid digest");
        let svid = CortexDelegatedLaneSvid::new(
            spiffe,
            EpochMillis::new(1_000).expect("t0"),
            EpochMillis::new(60_000).expect("t1"),
            digest,
        )
        .expect("valid svid");
        let godseat = GodseatLanePrincipal::new(
            "glaeda",
            "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIGxhZWRhX2hhcm5lc3Nfa2V5X2V4YW1wbGU=",
        )
        .expect("valid godseat");
        let solver =
            WorkerPoolKernelSolver::new(WorkerPoolConfig::default_personal_worker(2)).expect("ok");
        CortexRevenantBridge::new(svid, godseat, solver)
    }

    fn sample_wire_envelope(stream_seq: u64, memory_bytes: u64) -> EstateAdmissionWireEnvelope {
        EstateAdmissionWireEnvelope {
            stream_seq,
            delivery_token: format!("tok-{stream_seq}"),
            delivery_attempt: 1,
            lease_ms: 30_000,
            from_peer: "session-agy-ceres-estate-w3z-p1".to_owned(),
            from_session: Some("estate".to_owned()),
            subject: "revenant.wayfinder.dispatch".to_owned(),
            body: "{\"ticket\":\"EST-426\"}".to_owned(),
            traceparent: None,
            correlation: None,
            family_key: "unsigned-gg/glaeda".to_owned(),
            wayfinder_priority: "permission".to_owned(),
            cpu_millis: 1_000,
            memory_bytes,
            max_pids: 16,
            submitted_at_ms: 4_000,
        }
    }

    #[test]
    fn loopback_bind_guard_accepts_localhost_and_rejects_wildcard() {
        let lo4: SocketAddr = "127.0.0.1:8080".parse().expect("valid addr");
        let lo6: SocketAddr = "[::1]:8080".parse().expect("valid addr");
        let wild4: SocketAddr = "0.0.0.0:8080".parse().expect("valid addr");
        let lan4: SocketAddr = "192.168.1.50:8080".parse().expect("valid addr");

        assert!(validate_loopback_bind_addr(lo4).is_ok());
        assert!(validate_loopback_bind_addr(lo6).is_ok());
        assert!(validate_loopback_bind_addr(wild4).is_err());
        assert!(validate_loopback_bind_addr(lan4).is_err());
    }

    #[test]
    fn health_endpoint_reports_healthy_and_unavailable_states() {
        let bridge = sample_bridge();
        let req = b"GET /v1/health HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n";

        let healthy = handle_http_request_bytes(
            &bridge,
            req,
            EpochMillis::new(5_000).expect("fresh"),
            0,
            0,
            0,
            0,
            500,
            0,
        );
        assert_eq!(healthy.status_code, 200);
        assert!(healthy.body_json.contains("\"status\":\"healthy\""));

        let expired = handle_http_request_bytes(
            &bridge,
            req,
            EpochMillis::new(60_000).expect("expired"),
            0,
            0,
            0,
            0,
            500,
            0,
        );
        assert_eq!(expired.status_code, 503);
        assert!(expired.body_json.contains("\"status\":\"unavailable\""));
    }

    #[test]
    fn admission_endpoint_redacts_unknown_fields_and_evaluates_batches() {
        let bridge = sample_bridge();
        let attacker_body = br#"{"unexpected_secret_field_xyz": 1}"#;
        let bad_req = format!(
            "POST /v1/admission/evaluate HTTP/1.1\r\nContent-Length: {}\r\n\r\n{}",
            attacker_body.len(),
            std::str::from_utf8(attacker_body).expect("utf8")
        );
        let resp = handle_http_request_bytes(
            &bridge,
            bad_req.as_bytes(),
            EpochMillis::new(5_000).expect("fresh"),
            0,
            0,
            0,
            0,
            500,
            0,
        );
        assert_eq!(resp.status_code, 400);
        assert!(!resp.body_json.contains("unexpected_secret_field_xyz"));
    }

    #[test]
    fn estate_admit_alias_and_admission_gate_enforce_cumulative_dbr_and_memory_bounds() {
        let bridge = sample_bridge();

        // 1. GET /v1/estate/admit must return 405 Method Not Allowed
        let get_alias = handle_http_request_bytes(
            &bridge,
            b"GET /v1/estate/admit HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n",
            EpochMillis::new(5_000).expect("fresh"),
            0,
            0,
            0,
            0,
            500,
            0,
        );
        assert_eq!(get_alias.status_code, 405);
        assert!(
            get_alias
                .body_json
                .contains("\"code\":\"method_not_allowed\"")
        );

        // 2. POST /v1/estate/admit and POST /v1/admission/evaluate produce identical responses
        let admit_req = EstateAdmissionWireRequest {
            observed_at_ms: 5_000,
            active_workers: 0,
            queue_depth: 0,
            in_flight_bytes: 0,
            in_flight_cpu_millis: 0,
            arrival_rate_milli_per_sec: 500,
            context_switch_rate_bps: 0,
            envelopes: vec![sample_wire_envelope(1, 64 * 1024 * 1024)],
        };
        let body = serde_json::to_string(&admit_req).expect("serialize");
        let eval_http = format!(
            "POST /v1/admission/evaluate HTTP/1.1\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        let alias_http = format!(
            "POST /v1/estate/admit HTTP/1.1\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        let eval_resp = handle_http_request_bytes(
            &bridge,
            eval_http.as_bytes(),
            EpochMillis::new(5_000).expect("fresh"),
            0,
            0,
            0,
            0,
            500,
            0,
        );
        let alias_resp = handle_http_request_bytes(
            &bridge,
            alias_http.as_bytes(),
            EpochMillis::new(5_000).expect("fresh"),
            0,
            0,
            0,
            0,
            500,
            0,
        );
        assert_eq!(eval_resp, alias_resp);
        assert!(eval_resp.body_json.contains("\"admitted\":true"));
        assert!(
            eval_resp
                .body_json
                .contains("\"admitted:unsigned-gg/glaeda:seq=1:prio=Permission\"")
        );

        // 3. Cumulative DBR buffer cap hold across multi-envelope batch:
        // default_personal_worker(2) has max_wip_cap = 8. With active_workers=2 and queue_depth=5,
        // envelope 1 sees total_wip=7 (Admit), envelope 2 sees cumulative total_wip=8 (HoldBufferFull).
        let cumulative_hold_req = EstateAdmissionWireRequest {
            observed_at_ms: 5_000,
            active_workers: 2,
            queue_depth: 5,
            in_flight_bytes: 0,
            in_flight_cpu_millis: 0,
            arrival_rate_milli_per_sec: 500,
            context_switch_rate_bps: 0,
            envelopes: vec![
                sample_wire_envelope(10, 64 * 1024 * 1024),
                sample_wire_envelope(11, 64 * 1024 * 1024),
            ],
        };
        let hold_body = serde_json::to_string(&cumulative_hold_req).expect("serialize");
        let hold_http = format!(
            "POST /v1/estate/admit HTTP/1.1\r\nContent-Length: {}\r\n\r\n{}",
            hold_body.len(),
            hold_body
        );
        let hold_resp = handle_http_request_bytes(
            &bridge,
            hold_http.as_bytes(),
            EpochMillis::new(5_000).expect("fresh"),
            0,
            0,
            0,
            0,
            500,
            0,
        );
        assert_eq!(hold_resp.status_code, 200);
        assert!(hold_resp.body_json.contains("\"admitted\":false"));
        assert!(
            hold_resp
                .body_json
                .contains("\"admission\":\"hold_buffer_full\"")
        );
        assert!(
            hold_resp
                .body_json
                .contains("\"held_buffer_full:unsigned-gg/glaeda:seq=10:prio=Permission\"")
        );
        assert!(hold_resp.body_json.contains("\"total_service_units\":0"));
    }

    #[test]
    fn metrics_endpoint_renders_prometheus_text_format_and_rejects_post() {
        let bridge = sample_bridge();

        let get_metrics = handle_http_request_bytes(
            &bridge,
            b"GET /metrics HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n",
            EpochMillis::new(5_000).expect("fresh"),
            1,
            2,
            0,
            0,
            500,
            0,
        );
        assert_eq!(get_metrics.status_code, 200);
        assert_eq!(get_metrics.content_type, CONTENT_TYPE_PROMETHEUS_TEXT);
        assert!(
            get_metrics
                .body_json
                .contains("glaeda_estate_svid_fresh{service=\"glaeda\",callsign=\"aegis\"} 1")
        );
        assert!(
            get_metrics
                .body_json
                .contains("glaeda_estate_health_status{service=\"glaeda\",callsign=\"aegis\",status=\"healthy\"} 1")
        );
        assert!(
            get_metrics
                .body_json
                .contains("glaeda_worker_pool_total_wip{service=\"glaeda\",callsign=\"aegis\"} 3")
        );
        assert!(
            get_metrics
                .body_json
                .contains("glaeda_worker_pool_admitting{service=\"glaeda\",callsign=\"aegis\"} 1")
        );

        let get_v1_metrics = handle_http_request_bytes(
            &bridge,
            b"GET /v1/metrics HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n",
            EpochMillis::new(5_000).expect("fresh"),
            1,
            2,
            0,
            0,
            500,
            0,
        );
        assert_eq!(get_metrics, get_v1_metrics);

        let post_metrics = handle_http_request_bytes(
            &bridge,
            b"POST /metrics HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n",
            EpochMillis::new(5_000).expect("fresh"),
            0,
            0,
            0,
            0,
            500,
            0,
        );
        assert_eq!(post_metrics.status_code, 405);
    }
}
