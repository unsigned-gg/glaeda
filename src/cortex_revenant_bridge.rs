//! Three-way architectural bridge connecting **Cortex** (`cortex-edge` delegated lane SVIDs and
//! `cortex-worker` governed dispatch), **Revenant** (`godseat` harness signing identities,
//! `wayfinder` 4-rank Cobham `M/G/1` priority queueing, and `mesh-pool` `PoolDelivery` envelopes),
//! and **Glaeda** (`WorkerPoolKernelSolver` Little's Law / Kingman `G/G/m` / `MemoryBound` engine).
//!
//! See `specs/0003-cortex-revenant-lane-and-mesh-pool-bridge.md` for the full architectural contract.

use std::fmt;

use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::artifact::Sha256Digest;
use crate::execution_admission::{EpochMillis, ExecutionRequestId, ExecutionResourceLimits};
use crate::personal_worker_queue::PersonalWorkerPriority;
use crate::worker_pool_kernel_solver::{
    KernelSolverTask, WorkerPoolBatchReport, WorkerPoolFlowMetrics, WorkerPoolKernelSolver,
};

pub const CORTEX_REVENANT_BRIDGE_SCHEMA_VERSION: u8 = 1;
pub const CORTEX_TRUST_DOMAIN: &str = "unsigned.gg";
pub const CORTEX_LANE_SELECTOR_TYPE: &str = "cortex";
pub const MAX_CORTEX_DNS_LABEL_LEN: usize = 63;

pub const GODSEAT_PRINCIPAL_DOMAIN: &str = "agents.cerebral.work";
pub const GODSEAT_GIT_NAMESPACE: &str = "git";
pub const MAX_GODSEAT_LANE_LEN: usize = 32;

/// Structured validation, identity, or admission error from the Cortex–Revenant–Glaeda bridge.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CortexRevenantBridgeError {
    pub field: &'static str,
    pub code: &'static str,
    pub message: &'static str,
}

impl CortexRevenantBridgeError {
    #[must_use]
    pub const fn new(field: &'static str, code: &'static str, message: &'static str) -> Self {
        Self {
            field,
            code,
            message,
        }
    }
}

impl fmt::Display for CortexRevenantBridgeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({}): {}", self.field, self.code, self.message)
    }
}

impl std::error::Error for CortexRevenantBridgeError {}

/// Validate a Cortex DNS-1123 label (`host` or `role`) per `cortex-edge/src/lane_svid.rs`:
/// 1..=63 characters, ASCII lowercase alphanumeric or `'-'`, never starting or ending with `'-'`.
#[must_use]
pub fn valid_cortex_dns_label(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.is_empty() || bytes.len() > MAX_CORTEX_DNS_LABEL_LEN {
        return false;
    }
    if bytes[0] == b'-' || bytes[bytes.len() - 1] == b'-' {
        return false;
    }
    bytes
        .iter()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-')
}

/// Cortex delegated lane SPIFFE identity (`spiffe://unsigned.gg/host/<host>/lane/<role>`)
/// and workload selector (`cortex:pane-role:<role>`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CortexLaneSpiffeId {
    pub host: String,
    pub role: String,
    pub spiffe_id: String,
    pub selector: String,
}

impl CortexLaneSpiffeId {
    /// Construct a validated Cortex lane SPIFFE identity and selector from `(host, role)`.
    ///
    /// # Errors
    ///
    /// Returns an error if `host` or `role` violates DNS-1123 lowercase label rules.
    pub fn new(host: &str, role: &str) -> Result<Self, CortexRevenantBridgeError> {
        if !valid_cortex_dns_label(host) {
            return Err(CortexRevenantBridgeError::new(
                "lane_svid.host",
                "invalid_cortex_host_label",
                "host must be 1..=63 lowercase ASCII alphanumeric or '-' characters and not start or end with '-'",
            ));
        }
        if !valid_cortex_dns_label(role) {
            return Err(CortexRevenantBridgeError::new(
                "lane_svid.role",
                "invalid_cortex_role_label",
                "role must be 1..=63 lowercase ASCII alphanumeric or '-' characters and not start or end with '-'",
            ));
        }
        let spiffe_id = format!("spiffe://{CORTEX_TRUST_DOMAIN}/host/{host}/lane/{role}");
        let selector = format!("{CORTEX_LANE_SELECTOR_TYPE}:pane-role:{role}");
        Ok(Self {
            host: host.to_owned(),
            role: role.to_owned(),
            spiffe_id,
            selector,
        })
    }

    /// Parse a canonical `spiffe://unsigned.gg/host/<host>/lane/<role>` URI fail-closed.
    ///
    /// # Errors
    ///
    /// Returns an error if the URI prefix, trust domain, segments, or labels are invalid.
    pub fn parse(uri: &str) -> Result<Self, CortexRevenantBridgeError> {
        let prefix = format!("spiffe://{CORTEX_TRUST_DOMAIN}/host/");
        let Some(rest) = uri.strip_prefix(&prefix) else {
            return Err(CortexRevenantBridgeError::new(
                "lane_svid.spiffe_id",
                "invalid_spiffe_uri_prefix",
                "SPIFFE ID must start with spiffe://unsigned.gg/host/",
            ));
        };
        let Some((host, role)) = rest.split_once("/lane/") else {
            return Err(CortexRevenantBridgeError::new(
                "lane_svid.spiffe_id",
                "missing_lane_segment",
                "SPIFFE ID must contain /host/<host>/lane/<role>",
            ));
        };
        if role.contains('/') {
            return Err(CortexRevenantBridgeError::new(
                "lane_svid.spiffe_id",
                "unexpected_trailing_segment",
                "SPIFFE ID must not contain trailing path segments after /lane/<role>",
            ));
        }
        Self::new(host, role)
    }
}

/// Delegated X.509/JWT lane SVID attestation with zero-grace expiry verification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CortexDelegatedLaneSvid {
    pub identity: CortexLaneSpiffeId,
    pub issued_at: EpochMillis,
    pub expires_at: EpochMillis,
    pub svid_digest: Sha256Digest,
}

impl CortexDelegatedLaneSvid {
    /// Construct a bounded delegated lane SVID record.
    ///
    /// # Errors
    ///
    /// Returns an error when `expires_at <= issued_at`.
    pub fn new(
        identity: CortexLaneSpiffeId,
        issued_at: EpochMillis,
        expires_at: EpochMillis,
        svid_digest: Sha256Digest,
    ) -> Result<Self, CortexRevenantBridgeError> {
        if expires_at.get() <= issued_at.get() {
            return Err(CortexRevenantBridgeError::new(
                "lane_svid.expires_at",
                "non_positive_svid_ttl",
                "lane SVID expires_at must be strictly greater than issued_at",
            ));
        }
        Ok(Self {
            identity,
            issued_at,
            expires_at,
            svid_digest,
        })
    }

    /// Zero-grace freshness gate (`issued_at <= observed_at < expires_at`).
    ///
    /// # Errors
    ///
    /// Returns an error if the SVID is not yet valid or has reached/passed `expires_at`.
    pub fn verify_fresh(&self, observed_at: EpochMillis) -> Result<(), CortexRevenantBridgeError> {
        if observed_at.get() < self.issued_at.get() {
            return Err(CortexRevenantBridgeError::new(
                "lane_svid.issued_at",
                "svid_not_yet_valid",
                "observed_at precedes lane SVID issued_at timestamp",
            ));
        }
        if observed_at.get() >= self.expires_at.get() {
            return Err(CortexRevenantBridgeError::new(
                "lane_svid.expires_at",
                "svid_expired_zero_grace",
                "lane SVID has expired (zero-grace policy)",
            ));
        }
        Ok(())
    }
}

/// Validate a Revenant Godseat lane name per `revenant/crates/estate/src/harness/identity.rs`:
/// 1..=32 characters, starts with an ASCII lowercase letter, contains only `[a-z0-9-]`.
#[must_use]
pub fn valid_godseat_lane(lane: &str) -> bool {
    let bytes = lane.as_bytes();
    if bytes.is_empty() || bytes.len() > MAX_GODSEAT_LANE_LEN {
        return false;
    }
    if !bytes[0].is_ascii_lowercase() {
        return false;
    }
    bytes
        .iter()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-')
}

/// Ensure the harness key directory is an absolute path isolated from `.ssh`.
///
/// # Errors
///
/// Returns an error if `key_dir` is empty, relative, or references `.ssh`.
pub fn validate_isolated_key_dir(key_dir: &str) -> Result<(), CortexRevenantBridgeError> {
    if !key_dir.starts_with('/') {
        return Err(CortexRevenantBridgeError::new(
            "godseat.key_dir",
            "relative_key_dir",
            "harness key directory must be an absolute path",
        ));
    }
    if key_dir.split('/').any(|seg| seg == ".ssh") {
        return Err(CortexRevenantBridgeError::new(
            "godseat.key_dir",
            "ssh_dir_forbidden",
            "harness key directory must never reside inside .ssh",
        ));
    }
    Ok(())
}

/// Revenant Godseat per-lane Git signing identity (`harness-<lane>@agents.cerebral.work`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GodseatLanePrincipal {
    pub lane: String,
    pub principal: String,
    pub public_key: String,
}

impl GodseatLanePrincipal {
    /// Construct a validated Godseat lane principal and Ed25519 public key binding.
    ///
    /// # Errors
    ///
    /// Returns an error if `lane` violates Godseat naming rules or `public_key` is not a valid
    /// single-line `ssh-ed25519` public key.
    pub fn new(lane: &str, public_key: &str) -> Result<Self, CortexRevenantBridgeError> {
        if !valid_godseat_lane(lane) {
            return Err(CortexRevenantBridgeError::new(
                "godseat.lane",
                "invalid_godseat_lane",
                "lane must be 1..=32 characters of [a-z0-9-] starting with a lowercase letter",
            ));
        }
        let trimmed = public_key.trim();
        let Some(rest) = trimmed.strip_prefix("ssh-ed25519 ") else {
            return Err(CortexRevenantBridgeError::new(
                "godseat.public_key",
                "invalid_ssh_ed25519_key",
                "public_key must begin with 'ssh-ed25519 '",
            ));
        };
        let key_body = rest.split_whitespace().next().unwrap_or_default();
        if key_body.len() < 16 || trimmed.contains('\n') || trimmed.contains('\r') {
            return Err(CortexRevenantBridgeError::new(
                "godseat.public_key",
                "malformed_ssh_ed25519_key",
                "public_key must be a single-line ssh-ed25519 key with non-empty base64 material",
            ));
        }
        let principal = format!("harness-{lane}@{GODSEAT_PRINCIPAL_DOMAIN}");
        Ok(Self {
            lane: lane.to_owned(),
            principal,
            public_key: trimmed.to_owned(),
        })
    }

    /// Format the Git `allowed_signers` line per `revenant/crates/estate/src/harness/identity.rs`.
    #[must_use]
    pub fn allowed_signers_entry(&self) -> String {
        format!(
            "{} namespaces=\"{}\" {}",
            self.principal, GODSEAT_GIT_NAMESPACE, self.public_key
        )
    }
}

/// Revenant Wayfinder 4-rank Cobham `M/G/1` non-preemptive priority class
/// (`revenant/crates/wayfinder/src/queueing.rs`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WayfinderPriority {
    /// Rank 0 (highest): operator permission gate (`base_score = 300`).
    Permission = 0,
    /// Rank 1: interactive input required (`base_score = 200`).
    Input = 1,
    /// Rank 2: autonomous ready work (`base_score = 100`).
    Ready = 2,
    /// Rank 3 (lowest): background / idle sweep (`base_score = 0`).
    Idle = 3,
}

impl WayfinderPriority {
    /// Wayfinder base priority score (`Permission = 300`, `Input = 200`, `Ready = 100`, `Idle = 0`).
    #[must_use]
    pub const fn base_score(self) -> u64 {
        match self {
            Self::Permission => 300,
            Self::Input => 200,
            Self::Ready => 100,
            Self::Idle => 0,
        }
    }

    /// Kleinrock linear age-promoted priority score matching `wayfinder::queueing::aged_priority`.
    #[must_use]
    pub const fn aged_score(self, age_secs: u64, aging_window_secs: u64) -> u64 {
        let window = if aging_window_secs == 0 {
            1
        } else {
            aging_window_secs
        };
        let bonus = age_secs.saturating_mul(100) / window;
        self.base_score().saturating_add(bonus)
    }

    /// Deterministic mapping from Revenant `WayfinderPriority` to Glaeda `PersonalWorkerPriority`.
    #[must_use]
    pub const fn to_personal_worker_priority(self) -> PersonalWorkerPriority {
        match self {
            Self::Permission | Self::Input => PersonalWorkerPriority::Interactive,
            Self::Ready => PersonalWorkerPriority::Normal,
            Self::Idle => PersonalWorkerPriority::Background,
        }
    }
}

/// Revenant `mesh-pool` delivery envelope (`PoolDelivery` in `revenant/crates/mesh-pool/src/lib.rs`)
/// enriched with Wayfinder priority and Glaeda resource limits.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MeshPoolDeliveryEnvelope {
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
    pub wayfinder_priority: WayfinderPriority,
    pub limits: ExecutionResourceLimits,
    pub submitted_at: EpochMillis,
}

impl MeshPoolDeliveryEnvelope {
    /// Validate envelope invariants fail-closed.
    ///
    /// # Errors
    ///
    /// Returns an error if `stream_seq == 0`, `delivery_attempt == 0`, `lease_ms == 0`, or required
    /// string fields (`delivery_token`, `from_peer`, `subject`, `family_key`) are empty.
    pub fn validate(&self) -> Result<(), CortexRevenantBridgeError> {
        if self.stream_seq == 0 {
            return Err(CortexRevenantBridgeError::new(
                "envelope.stream_seq",
                "zero_stream_seq",
                "stream_seq must be greater than zero",
            ));
        }
        if self.delivery_token.trim().is_empty() {
            return Err(CortexRevenantBridgeError::new(
                "envelope.delivery_token",
                "empty_delivery_token",
                "delivery_token must not be empty",
            ));
        }
        if self.delivery_attempt == 0 {
            return Err(CortexRevenantBridgeError::new(
                "envelope.delivery_attempt",
                "zero_delivery_attempt",
                "delivery_attempt must be >= 1",
            ));
        }
        if self.lease_ms == 0 {
            return Err(CortexRevenantBridgeError::new(
                "envelope.lease_ms",
                "zero_lease_ms",
                "lease_ms must be greater than zero",
            ));
        }
        if self.from_peer.trim().is_empty() {
            return Err(CortexRevenantBridgeError::new(
                "envelope.from_peer",
                "empty_from_peer",
                "from_peer must not be empty",
            ));
        }
        if self.subject.trim().is_empty() {
            return Err(CortexRevenantBridgeError::new(
                "envelope.subject",
                "empty_subject",
                "subject must not be empty",
            ));
        }
        if self.family_key.trim().is_empty() {
            return Err(CortexRevenantBridgeError::new(
                "envelope.family_key",
                "empty_family_key",
                "family_key must not be empty",
            ));
        }
        Ok(())
    }

    /// Canonical [`ExecutionRequestId`] derived from `(stream_seq, delivery_attempt)`.
    ///
    /// # Errors
    ///
    /// Returns an error if envelope validation fails.
    pub fn execution_request_id(&self) -> Result<ExecutionRequestId, CortexRevenantBridgeError> {
        self.validate()?;
        let raw_id = format!("mesh-seq-{}-att-{}", self.stream_seq, self.delivery_attempt);
        ExecutionRequestId::parse(&raw_id).map_err(|_| {
            CortexRevenantBridgeError::new(
                "envelope.request_id",
                "invalid_execution_request_id",
                "failed to construct valid ExecutionRequestId from stream_seq and delivery_attempt",
            )
        })
    }

    /// Convert this validated mesh delivery envelope into a [`KernelSolverTask`] for Glaeda's
    /// [`WorkerPoolKernelSolver`].
    ///
    /// # Errors
    ///
    /// Returns an error if the envelope fails validation.
    pub fn into_kernel_task(self) -> Result<KernelSolverTask<Self>, CortexRevenantBridgeError> {
        let request_id = self.execution_request_id()?;
        let family_key = self.family_key.clone();
        let priority = self.wayfinder_priority.to_personal_worker_priority();
        let limits = self.limits;
        let submitted_at = self.submitted_at;
        Ok(KernelSolverTask {
            request_id,
            family_key,
            priority,
            limits,
            submitted_at,
            payload: self,
        })
    }
}

/// Content-addressed receipt emitted when a batch of Revenant `mesh-pool` deliveries is solved by
/// a Cortex-attested Glaeda worker-pool lane.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CortexRevenantBridgeReceipt<O> {
    pub schema_version: u8,
    pub lane_spiffe_id: String,
    pub lane_selector: String,
    pub godseat_principal: String,
    pub svid_digest: Sha256Digest,
    pub batch_report: WorkerPoolBatchReport<O>,
    pub bridge_receipt_digest: Sha256Digest,
}

/// Three-way coordinator binding a Cortex delegated lane SVID, a Revenant Godseat signing
/// principal, and a Glaeda [`WorkerPoolKernelSolver`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CortexRevenantBridge {
    lane_svid: CortexDelegatedLaneSvid,
    godseat_principal: GodseatLanePrincipal,
    solver: WorkerPoolKernelSolver,
}

impl CortexRevenantBridge {
    #[must_use]
    pub const fn new(
        lane_svid: CortexDelegatedLaneSvid,
        godseat_principal: GodseatLanePrincipal,
        solver: WorkerPoolKernelSolver,
    ) -> Self {
        Self {
            lane_svid,
            godseat_principal,
            solver,
        }
    }

    #[must_use]
    pub const fn lane_svid(&self) -> &CortexDelegatedLaneSvid {
        &self.lane_svid
    }

    #[must_use]
    pub const fn godseat_principal(&self) -> &GodseatLanePrincipal {
        &self.godseat_principal
    }

    #[must_use]
    pub const fn solver(&self) -> &WorkerPoolKernelSolver {
        &self.solver
    }

    /// Evaluate Drum-Buffer-Rope + Little's Law + Kingman `G/G/m` + `MemoryBound` admission for an
    /// inbound Revenant `mesh-pool` envelope after verifying Cortex lane SVID freshness.
    ///
    /// # Errors
    ///
    /// Returns an error if the lane SVID is expired/not-yet-valid or if the envelope is malformed.
    #[allow(clippy::too_many_arguments)]
    pub fn evaluate_envelope_admission(
        &self,
        envelope: &MeshPoolDeliveryEnvelope,
        observed_at: EpochMillis,
        active_workers: usize,
        queue_depth: usize,
        in_flight_bytes: u64,
        in_flight_cpu_millis: u32,
        arrival_rate_milli_per_sec: u64,
        context_switch_rate_bps: u32,
    ) -> Result<WorkerPoolFlowMetrics, CortexRevenantBridgeError> {
        self.lane_svid.verify_fresh(observed_at)?;
        envelope.validate()?;
        Ok(WorkerPoolFlowMetrics::evaluate(
            self.solver.config(),
            active_workers,
            queue_depth,
            in_flight_bytes,
            in_flight_cpu_millis,
            arrival_rate_milli_per_sec,
            context_switch_rate_bps,
            envelope.limits,
            envelope.wayfinder_priority.to_personal_worker_priority(),
        ))
    }

    /// Execute a batch of Revenant `mesh-pool` deliveries on Glaeda's [`WorkerPoolKernelSolver`]
    /// under a fresh Cortex delegated lane SVID and emit a content-addressed
    /// [`CortexRevenantBridgeReceipt`].
    ///
    /// # Errors
    ///
    /// Returns an error if the lane SVID is expired, any envelope is invalid, or the solver batch
    /// rejects duplicate request IDs.
    pub fn dispatch_batch<O, F>(
        &self,
        envelopes: Vec<MeshPoolDeliveryEnvelope>,
        observed_at: EpochMillis,
        arrival_rate_milli_per_sec: u64,
        kernel_fn: F,
    ) -> Result<CortexRevenantBridgeReceipt<O>, CortexRevenantBridgeError>
    where
        O: Send + Serialize,
        F: Fn(&KernelSolverTask<MeshPoolDeliveryEnvelope>) -> (O, u64) + Send + Sync,
    {
        self.lane_svid.verify_fresh(observed_at)?;

        let mut tasks = Vec::with_capacity(envelopes.len());
        for env in envelopes {
            tasks.push(env.into_kernel_task()?);
        }

        let batch_report = self
            .solver
            .solve_batch(tasks, observed_at, arrival_rate_milli_per_sec, kernel_fn)
            .map_err(|err| CortexRevenantBridgeError::new(err.field, err.code, err.message))?;

        let bridge_receipt_digest = compute_bridge_receipt_digest(
            &self.lane_svid.identity.spiffe_id,
            &self.godseat_principal.principal,
            &self.lane_svid.svid_digest,
            &batch_report.batch_receipt_digest,
        );

        Ok(CortexRevenantBridgeReceipt {
            schema_version: CORTEX_REVENANT_BRIDGE_SCHEMA_VERSION,
            lane_spiffe_id: self.lane_svid.identity.spiffe_id.clone(),
            lane_selector: self.lane_svid.identity.selector.clone(),
            godseat_principal: self.godseat_principal.principal.clone(),
            svid_digest: self.lane_svid.svid_digest.clone(),
            batch_report,
            bridge_receipt_digest,
        })
    }
}

fn compute_bridge_receipt_digest(
    lane_spiffe_id: &str,
    godseat_principal: &str,
    svid_digest: &Sha256Digest,
    batch_digest: &Sha256Digest,
) -> Sha256Digest {
    let mut hasher = Sha256::new();
    hasher.update(b"glaeda:cortex-revenant-bridge:v1:");
    hasher.update(lane_spiffe_id.as_bytes());
    hasher.update(b":");
    hasher.update(godseat_principal.as_bytes());
    hasher.update(b":");
    hasher.update(svid_digest.as_str().as_bytes());
    hasher.update(b":");
    hasher.update(batch_digest.as_str().as_bytes());
    let hex = format!("sha256:{:x}", hasher.finalize());
    Sha256Digest::parse(&hex).unwrap_or_else(|_| {
        Sha256Digest::parse(
            "sha256:0000000000000000000000000000000000000000000000000000000000000000",
        )
        .unwrap_or_else(|_| unreachable!())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker_pool_kernel_solver::{PoolAdmissionDecision, WorkerPoolConfig};

    fn sample_digest(byte: &str) -> Sha256Digest {
        Sha256Digest::parse(&format!("sha256:{}", byte.repeat(32))).expect("valid digest")
    }

    #[test]
    fn cortex_spiffe_id_round_trips_and_rejects_invalid_labels() {
        let id =
            CortexLaneSpiffeId::new("ceres", "session-agy-ceres-estate-w30-p4").expect("valid");
        assert_eq!(
            id.spiffe_id,
            "spiffe://unsigned.gg/host/ceres/lane/session-agy-ceres-estate-w30-p4"
        );
        assert_eq!(
            id.selector,
            "cortex:pane-role:session-agy-ceres-estate-w30-p4"
        );

        let parsed = CortexLaneSpiffeId::parse(&id.spiffe_id).expect("parses");
        assert_eq!(parsed, id);

        assert!(CortexLaneSpiffeId::new("-ceres", "role").is_err());
        assert!(CortexLaneSpiffeId::new("ceres", "RoleUpper").is_err());
        assert!(CortexLaneSpiffeId::parse("spiffe://evil.gg/host/ceres/lane/role").is_err());
    }

    #[test]
    fn godseat_principal_formats_allowed_signers_and_blocks_ssh_dir() {
        let pubkey = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIGxhZWRhX2hhcm5lc3Nfa2V5X2V4YW1wbGU= harness-glaeda";
        let principal = GodseatLanePrincipal::new("glaeda", pubkey).expect("valid principal");
        assert_eq!(principal.principal, "harness-glaeda@agents.cerebral.work");
        assert_eq!(
            principal.allowed_signers_entry(),
            format!("harness-glaeda@agents.cerebral.work namespaces=\"git\" {pubkey}")
        );

        assert!(validate_isolated_key_dir("/home/ctodie/.harness/keys").is_ok());
        assert!(validate_isolated_key_dir("/home/ctodie/.ssh/keys").is_err());
        assert!(validate_isolated_key_dir("relative/keys").is_err());
    }

    #[test]
    fn bridge_dispatches_mesh_pool_envelopes_and_enforces_zero_grace_svid() {
        let spiffe = CortexLaneSpiffeId::new("ceres", "session-agy-ceres-estate-w3n-p1")
            .expect("valid spiffe");
        let svid = CortexDelegatedLaneSvid::new(
            spiffe,
            EpochMillis::new(1_000).expect("t0"),
            EpochMillis::new(10_000).expect("t1"),
            sample_digest("aa"),
        )
        .expect("valid svid");
        let godseat = GodseatLanePrincipal::new(
            "glaeda",
            "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIGxhZWRhX2hhcm5lc3Nfa2V5X2V4YW1wbGU=",
        )
        .expect("valid godseat");
        let solver =
            WorkerPoolKernelSolver::new(WorkerPoolConfig::default_personal_worker(2)).expect("ok");
        let bridge = CortexRevenantBridge::new(svid, godseat, solver);

        let env = MeshPoolDeliveryEnvelope {
            stream_seq: 42,
            delivery_token: "tok-42".to_owned(),
            delivery_attempt: 1,
            lease_ms: 30_000,
            from_peer: "session-agy-ceres-estate-w3z-p1".to_owned(),
            from_session: Some("estate".to_owned()),
            subject: "glaeda.kernel.solve".to_owned(),
            body: "{\"op\":\"verify\"}".to_owned(),
            traceparent: None,
            correlation: Some("corr-42".to_owned()),
            family_key: "cerebral-work/glaeda".to_owned(),
            wayfinder_priority: WayfinderPriority::Permission,
            limits: ExecutionResourceLimits::new(1_000, 64 * 1_024 * 1_024, 16).expect("limits"),
            submitted_at: EpochMillis::new(2_000).expect("submitted"),
        };

        let metrics = bridge
            .evaluate_envelope_admission(
                &env,
                EpochMillis::new(5_000).expect("obs"),
                0,
                0,
                0,
                0,
                500,
                0,
            )
            .expect("admission ok");
        assert_eq!(metrics.admission, PoolAdmissionDecision::Admit);

        // Zero-grace expiry at exact expires_at timestamp (10_000) must fail closed.
        assert!(
            bridge
                .dispatch_batch(
                    vec![env.clone()],
                    EpochMillis::new(10_000).expect("expired"),
                    500,
                    |task| (task.payload.stream_seq, 100),
                )
                .is_err()
        );

        let receipt = bridge
            .dispatch_batch(
                vec![env],
                EpochMillis::new(5_000).expect("fresh"),
                500,
                |task| (task.payload.stream_seq * 2, 100),
            )
            .expect("batch solved");
        assert_eq!(receipt.batch_report.tasks_solved, 1);
        assert_eq!(receipt.batch_report.solutions[0].output, 84);
    }
}
