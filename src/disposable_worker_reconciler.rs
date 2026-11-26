use std::fmt;

use serde::Serialize;

use crate::artifact::Sha256Digest;
use crate::disposable_attempt_catalog::DisposableAttemptCatalogAction;
use crate::disposable_attempt_state::DisposableAttemptState;
use crate::execution_admission::EpochMillis;
use crate::github_scale_set_protocol::{ScaleSetJobEvent, ScaleSetRunnerReference};
#[cfg(unix)]
use crate::lima_host_identity::LimaHostInstanceIdentity;

pub const DISPOSABLE_WORKER_RECONCILER_SCHEMA_VERSION: u8 = 2;
const MAX_IDENTIFIER_LEN: usize = 96;
const MAX_WORKERS: u16 = 64;
const MAX_CPU_MILLIS: u32 = 1_000_000;
const MAX_BYTES: u64 = 1_u64 << 50;

macro_rules! identifier {
    ($name:ident, $field:literal) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn parse(value: &str) -> Result<Self, DisposableWorkerReconcilerError> {
                validate_identifier($field, value)?;
                Ok(Self(value.to_owned()))
            }

            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }
    };
}

identifier!(DisposableAttemptId, "attempt_id");
identifier!(CapacityClaimId, "capacity_claim_id");
identifier!(DisposableVmId, "vm_id");

/// Equality-only identity of one observed disposable Lima instance.
///
/// This digest is durable drift evidence, not standalone deletion authority. The host-mutation
/// boundary must still retain and freshly confirm the matching Lima host-identity observation
/// immediately before a name-targeted command.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct DisposableVmIdentity(Sha256Digest);

impl DisposableVmIdentity {
    /// Derive the durable equality token from one descriptor-bound post-clone observation.
    #[cfg(unix)]
    #[must_use]
    pub(crate) fn from_host_identity(identity: &LimaHostInstanceIdentity) -> Self {
        Self(identity.ownership_digest().clone())
    }

    #[cfg(unix)]
    pub(crate) fn matches_host_identity(&self, identity: &LimaHostInstanceIdentity) -> bool {
        &self.0 == identity.ownership_digest()
            || identity
                .legacy_digest()
                .is_some_and(|digest| &self.0 == digest)
    }

    /// Parse one canonical identity digest.
    ///
    /// Parsing does not prove that a VM exists or that SmolRunner owns it. It exists only for the
    /// private durable codec and crate-internal tests; only the future sealed clone-completion
    /// transaction may create the first durable binding.
    pub(crate) fn parse(value: &str) -> Result<Self, DisposableWorkerReconcilerError> {
        Sha256Digest::parse(value).map(Self).map_err(|_| {
            DisposableWorkerReconcilerError::new(
                "vm_identity",
                "invalid_vm_identity",
                "disposable VM identity digest is invalid",
            )
        })
    }

    #[must_use]
    pub(crate) fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Debug for DisposableVmIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DisposableVmIdentity(<private>)")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct DisposableWorkerResources {
    cpu_millis: u32,
    memory_bytes: u64,
    disk_bytes: u64,
}

impl DisposableWorkerResources {
    pub fn new(
        cpu_millis: u32,
        memory_bytes: u64,
        disk_bytes: u64,
    ) -> Result<Self, DisposableWorkerReconcilerError> {
        if !(1..=MAX_CPU_MILLIS).contains(&cpu_millis) {
            return Err(DisposableWorkerReconcilerError::new(
                "resources.cpu_millis",
                "invalid_cpu_limit",
                "CPU must be within the bounded positive range",
            ));
        }
        if !(1..=MAX_BYTES).contains(&memory_bytes) {
            return Err(DisposableWorkerReconcilerError::new(
                "resources.memory_bytes",
                "invalid_memory_limit",
                "memory must be within the bounded positive range",
            ));
        }
        if !(1..=MAX_BYTES).contains(&disk_bytes) {
            return Err(DisposableWorkerReconcilerError::new(
                "resources.disk_bytes",
                "invalid_disk_limit",
                "disk must be within the bounded positive range",
            ));
        }
        Ok(Self {
            cpu_millis,
            memory_bytes,
            disk_bytes,
        })
    }

    #[must_use]
    pub const fn cpu_millis(self) -> u32 {
        self.cpu_millis
    }

    #[must_use]
    pub const fn memory_bytes(self) -> u64 {
        self.memory_bytes
    }

    #[must_use]
    pub const fn disk_bytes(self) -> u64 {
        self.disk_bytes
    }

    fn worker_capacity_within(self, request: Self) -> u64 {
        u64::from(self.cpu_millis / request.cpu_millis)
            .min(self.memory_bytes / request.memory_bytes)
            .min(self.disk_bytes / request.disk_bytes)
    }

    fn fits_within(self, limit: Self) -> bool {
        self.cpu_millis <= limit.cpu_millis
            && self.memory_bytes <= limit.memory_bytes
            && self.disk_bytes <= limit.disk_bytes
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct DisposableHostBudget {
    max_workers: u16,
    total: DisposableWorkerResources,
}

impl DisposableHostBudget {
    pub fn new(
        max_workers: u16,
        total: DisposableWorkerResources,
    ) -> Result<Self, DisposableWorkerReconcilerError> {
        if !(1..=MAX_WORKERS).contains(&max_workers) {
            return Err(DisposableWorkerReconcilerError::new(
                "budget.max_workers",
                "invalid_worker_limit",
                "worker limit must be within the bounded positive range",
            ));
        }
        Ok(Self { max_workers, total })
    }

    /// The deployed shape: exactly one worker of `request` per host.
    #[must_use]
    pub const fn single(request: DisposableWorkerResources) -> Self {
        Self {
            max_workers: 1,
            total: request,
        }
    }

    #[must_use]
    pub const fn max_workers(self) -> u16 {
        self.max_workers
    }

    #[must_use]
    pub const fn total(self) -> DisposableWorkerResources {
        self.total
    }

    /// Whether one more worker of `request` fits beside the reserved `usage`: under the worker
    /// ceiling and with the summed CPU, memory and disk reservations inside the host total.
    /// Overflow fails closed.
    #[must_use]
    pub fn admits_another(
        self,
        usage: DisposableHostUsage,
        request: DisposableWorkerResources,
    ) -> bool {
        if usage.workers >= self.max_workers {
            return false;
        }
        let used = usage.resources;
        let (Some(cpu_millis), Some(memory_bytes), Some(disk_bytes)) = (
            used.cpu_millis.checked_add(request.cpu_millis),
            used.memory_bytes.checked_add(request.memory_bytes),
            used.disk_bytes.checked_add(request.disk_bytes),
        ) else {
            return false;
        };
        DisposableWorkerResources {
            cpu_millis,
            memory_bytes,
            disk_bytes,
        }
        .fits_within(self.total)
    }
}

/// The largest per-host worker ceiling any layer accepts (the bridge's own bound is the same).
pub(crate) const MAX_HOST_WORKERS: u16 = MAX_WORKERS;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct DisposableHostUsage {
    workers: u16,
    resources: DisposableWorkerResources,
}

impl DisposableHostUsage {
    #[must_use]
    pub const fn zero() -> Self {
        Self {
            workers: 0,
            resources: DisposableWorkerResources {
                cpu_millis: 0,
                memory_bytes: 0,
                disk_bytes: 0,
            },
        }
    }

    pub fn new(
        workers: u16,
        resources: DisposableWorkerResources,
    ) -> Result<Self, DisposableWorkerReconcilerError> {
        if workers > MAX_WORKERS {
            return Err(DisposableWorkerReconcilerError::new(
                "usage.workers",
                "invalid_worker_usage",
                "observed worker usage exceeds the implementation bound",
            ));
        }
        Ok(Self { workers, resources })
    }

    #[must_use]
    pub const fn workers(self) -> u16 {
        self.workers
    }

    #[must_use]
    pub const fn resources(self) -> DisposableWorkerResources {
        self.resources
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ScaleSetDemand {
    assigned_jobs: u16,
    running_jobs: u16,
    observed_at: EpochMillis,
    expires_at: EpochMillis,
}

impl ScaleSetDemand {
    pub fn new(
        assigned_jobs: u16,
        running_jobs: u16,
        observed_at: EpochMillis,
        expires_at: EpochMillis,
    ) -> Result<Self, DisposableWorkerReconcilerError> {
        if assigned_jobs > MAX_WORKERS || running_jobs > assigned_jobs {
            return Err(DisposableWorkerReconcilerError::new(
                "demand",
                "invalid_scale_set_demand",
                "scale-set demand must be bounded and running jobs cannot exceed assigned jobs",
            ));
        }
        if expires_at < observed_at {
            return Err(DisposableWorkerReconcilerError::new(
                "demand.expires_at",
                "invalid_demand_window",
                "scale-set demand expiry cannot precede its observation",
            ));
        }
        Ok(Self {
            assigned_jobs,
            running_jobs,
            observed_at,
            expires_at,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct DisposableCapacityPlan {
    schema_version: u8,
    advertised_max_capacity: u16,
    desired_workers: u16,
    additional_workers: u16,
}

impl DisposableCapacityPlan {
    #[must_use]
    pub const fn schema_version(&self) -> u8 {
        self.schema_version
    }

    #[must_use]
    pub const fn advertised_max_capacity(&self) -> u16 {
        self.advertised_max_capacity
    }

    #[must_use]
    pub const fn desired_workers(&self) -> u16 {
        self.desired_workers
    }

    #[must_use]
    pub const fn additional_workers(&self) -> u16 {
        self.additional_workers
    }
}

pub fn plan_capacity(
    now: EpochMillis,
    demand: ScaleSetDemand,
    budget: DisposableHostBudget,
    usage: DisposableHostUsage,
    worker: DisposableWorkerResources,
) -> Result<DisposableCapacityPlan, DisposableWorkerReconcilerError> {
    if now < demand.observed_at || now > demand.expires_at {
        return Err(DisposableWorkerReconcilerError::new(
            "demand",
            "stale_scale_set_demand",
            "scale-set demand must be current at the capacity decision",
        ));
    }
    if usage.workers > budget.max_workers || !usage.resources.fits_within(budget.total) {
        return Err(DisposableWorkerReconcilerError::new(
            "usage",
            "host_budget_exceeded",
            "current worker usage already exceeds the configured host budget",
        ));
    }

    let resource_capacity = budget.total.worker_capacity_within(worker);
    let advertised_max_capacity = budget
        .max_workers
        .min(u16::try_from(resource_capacity).unwrap_or(u16::MAX));
    let desired_workers = demand.assigned_jobs.min(advertised_max_capacity);

    let remaining = DisposableWorkerResources {
        cpu_millis: budget.total.cpu_millis - usage.resources.cpu_millis,
        memory_bytes: budget.total.memory_bytes - usage.resources.memory_bytes,
        disk_bytes: budget.total.disk_bytes - usage.resources.disk_bytes,
    };
    let resource_slots =
        u16::try_from(remaining.worker_capacity_within(worker)).unwrap_or(u16::MAX);
    let worker_slots = budget.max_workers - usage.workers;
    let demand_slots = desired_workers.saturating_sub(usage.workers);
    let additional_workers = demand_slots.min(worker_slots).min(resource_slots);

    Ok(DisposableCapacityPlan {
        schema_version: DISPOSABLE_WORKER_RECONCILER_SCHEMA_VERSION,
        advertised_max_capacity,
        desired_workers,
        additional_workers,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DisposableAttemptPhase {
    Reserved,
    UnprovisionedReleasing,
    /// Legacy schema-v1 state that did not prove VM absence and cannot authorize mutation.
    Provisioning,
    CloneAuthorized,
    /// The clone command crossed its durable no-replay checkpoint; only observation may advance it.
    CloneStarted,
    Registering,
    Waiting,
    Assigned,
    Running,
    Terminal,
    Destroying,
    Deregistering,
    Releasing,
    Complete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum DisposableVmObservation {
    Unknown,
    Absent,
    /// The exact owned clone exists but is not running or controller-ready.
    Stopped,
    /// The exact owned clone is running and has passed the backend readiness check.
    Ready,
    Conflicting,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum ScaleSetRunnerObservation {
    Unknown,
    Absent,
    /// The exact service registration exists, but a bounded launch/recovery check proved that no
    /// usable listener is becoming ready from the one-time JIT configuration.
    RegistrationOnly {
        runner: ScaleSetRunnerReference,
    },
    IdleReady {
        runner: ScaleSetRunnerReference,
    },
    Conflicting,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DisposableWorkerReconcileInput<'a> {
    pub now: EpochMillis,
    pub attempt: &'a DisposableAttemptState,
    pub vm: DisposableVmObservation,
    /// Exact identity accompanying a stopped or ready observation. This is absent for all other
    /// observation states and is only persisted as a drift check for the later held-evidence
    /// execution boundary.
    pub vm_identity: Option<&'a DisposableVmIdentity>,
    pub runner: ScaleSetRunnerObservation,
    pub job_event: Option<ScaleSetJobEvent>,
    pub capacity_reserved: bool,
    pub cancellation_requested: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DisposableWorkerObservationTarget {
    Vm,
    Runner,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum DisposableWorkerAction {
    Persist {
        transition: DisposableAttemptCatalogAction,
    },
    Observe {
        target: DisposableWorkerObservationTarget,
    },
    /// Under one durable lock, persist `RecordCloneStarted` and then issue one clone command.
    CheckpointAndCloneVm,
    /// Remove a stopped destination left by an interrupted clone after the durable absence marker.
    DiscardIncompleteVm,
    /// Under one durable lock, persist the JIT no-replay checkpoint before requesting one secret.
    CheckpointAndGenerateJit,
    Wait,
    DestroyVm,
    DeleteRunner {
        runner: ScaleSetRunnerReference,
    },
    ReleaseCapacity,
    NoOp,
    Blocked {
        code: &'static str,
    },
}

pub fn reconcile_attempt(
    input: DisposableWorkerReconcileInput<'_>,
) -> Result<DisposableWorkerAction, DisposableWorkerReconcilerError> {
    let cleanup = input.cancellation_requested || input.now > input.attempt.not_after();

    if matches!(input.vm, DisposableVmObservation::Conflicting) {
        return Ok(DisposableWorkerAction::Blocked {
            code: "conflicting_vm_identity",
        });
    }
    validate_vm_observation(input.attempt, input.vm, input.vm_identity)?;
    if matches!(input.runner, ScaleSetRunnerObservation::Conflicting) {
        return Ok(DisposableWorkerAction::Blocked {
            code: "conflicting_runner_identity",
        });
    }
    validate_runner_observation(input.attempt, &input.runner)?;
    if input.attempt.phase() == DisposableAttemptPhase::CloneStarted
        && input.attempt.vm_identity().is_none()
    {
        return Ok(DisposableWorkerAction::Blocked {
            code: "unbound_vm_recovery_required",
        });
    }
    if let Some(event) = input.job_event.as_ref()
        && let Some(action) = plan_job_event(input.attempt, event)?
    {
        return Ok(action);
    }

    use DisposableAttemptPhase as Phase;
    use DisposableWorkerAction as Action;
    if !input.capacity_reserved
        && matches!(
            input.attempt.phase(),
            Phase::Reserved | Phase::CloneAuthorized | Phase::UnprovisionedReleasing
        )
    {
        return Ok(persist(
            DisposableAttemptCatalogAction::CompleteUnprovisioned,
        ));
    }
    if !input.capacity_reserved
        && matches!(
            input.attempt.phase(),
            Phase::CloneStarted
                | Phase::Registering
                | Phase::Waiting
                | Phase::Assigned
                | Phase::Running
                | Phase::Terminal
        )
    {
        return Ok(persist(DisposableAttemptCatalogAction::BeginCleanup));
    }
    Ok(match input.attempt.phase() {
        Phase::Reserved if cleanup => {
            persist(DisposableAttemptCatalogAction::BeginUnprovisionedRelease)
        }
        Phase::Reserved => match input.vm {
            DisposableVmObservation::Unknown => Action::Observe {
                target: DisposableWorkerObservationTarget::Vm,
            },
            DisposableVmObservation::Absent => {
                persist(DisposableAttemptCatalogAction::AuthorizeClone)
            }
            DisposableVmObservation::Stopped | DisposableVmObservation::Ready => Action::Blocked {
                code: "vm_exists_before_clone_authorization",
            },
            DisposableVmObservation::Conflicting => unreachable!(),
        },
        Phase::UnprovisionedReleasing if input.capacity_reserved => Action::ReleaseCapacity,
        Phase::UnprovisionedReleasing => unreachable!(),
        Phase::Provisioning => Action::Blocked {
            code: "legacy_provisioning_recovery_required",
        },
        Phase::CloneAuthorized if cleanup => {
            persist(DisposableAttemptCatalogAction::BeginUnprovisionedRelease)
        }
        Phase::CloneAuthorized => match input.vm {
            DisposableVmObservation::Unknown => Action::Observe {
                target: DisposableWorkerObservationTarget::Vm,
            },
            DisposableVmObservation::Absent => Action::CheckpointAndCloneVm,
            DisposableVmObservation::Stopped | DisposableVmObservation::Ready => Action::Blocked {
                code: "vm_changed_before_clone_start",
            },
            DisposableVmObservation::Conflicting => unreachable!(),
        },
        Phase::CloneStarted if cleanup => persist(DisposableAttemptCatalogAction::BeginCleanup),
        Phase::CloneStarted => match input.vm {
            DisposableVmObservation::Unknown => Action::Observe {
                target: DisposableWorkerObservationTarget::Vm,
            },
            DisposableVmObservation::Absent => {
                persist(DisposableAttemptCatalogAction::BeginCleanup)
            }
            DisposableVmObservation::Stopped => Action::DiscardIncompleteVm,
            DisposableVmObservation::Ready => {
                persist(DisposableAttemptCatalogAction::BeginRegistration)
            }
            DisposableVmObservation::Conflicting => unreachable!(),
        },
        Phase::Registering if cleanup => persist(DisposableAttemptCatalogAction::BeginCleanup),
        Phase::Assigned if !input.attempt.runner_start_started() && cleanup => {
            persist(DisposableAttemptCatalogAction::BeginCleanup)
        }
        Phase::Registering | Phase::Assigned
            if !input.attempt.runner_start_started()
                && matches!(input.vm, DisposableVmObservation::Unknown) =>
        {
            Action::Observe {
                target: DisposableWorkerObservationTarget::Vm,
            }
        }
        Phase::Registering | Phase::Assigned
            if !input.attempt.runner_start_started()
                && matches!(
                    input.vm,
                    DisposableVmObservation::Absent | DisposableVmObservation::Stopped
                ) =>
        {
            persist(DisposableAttemptCatalogAction::BeginCleanup)
        }
        Phase::Registering | Phase::Assigned if !input.attempt.runner_start_started() => {
            match &input.runner {
                ScaleSetRunnerObservation::Unknown => Action::Observe {
                    target: DisposableWorkerObservationTarget::Runner,
                },
                ScaleSetRunnerObservation::Absent if !input.attempt.jit_generation_started() => {
                    Action::CheckpointAndGenerateJit
                }
                ScaleSetRunnerObservation::Absent if input.attempt.runner_id().is_none() => {
                    Action::Blocked {
                        code: "jit_generation_outcome_unknown",
                    }
                }
                ScaleSetRunnerObservation::Absent => {
                    persist(DisposableAttemptCatalogAction::BeginCleanup)
                }
                ScaleSetRunnerObservation::RegistrationOnly { runner }
                | ScaleSetRunnerObservation::IdleReady { runner }
                    if input.attempt.jit_generation_started()
                        && input.attempt.runner_id().is_none() =>
                {
                    persist(validate_transition(
                        input.attempt,
                        DisposableAttemptCatalogAction::RecordRegistration(runner.clone()),
                    )?)
                }
                ScaleSetRunnerObservation::RegistrationOnly { .. }
                | ScaleSetRunnerObservation::IdleReady { .. }
                    if input.attempt.jit_generation_started() =>
                {
                    persist(DisposableAttemptCatalogAction::BeginCleanup)
                }
                ScaleSetRunnerObservation::RegistrationOnly { .. }
                | ScaleSetRunnerObservation::IdleReady { .. } => Action::Blocked {
                    code: "runner_exists_before_jit_checkpoint",
                },
                ScaleSetRunnerObservation::Conflicting => unreachable!(),
            }
        }
        Phase::Registering => match &input.runner {
            ScaleSetRunnerObservation::Unknown => Action::Observe {
                target: DisposableWorkerObservationTarget::Runner,
            },
            ScaleSetRunnerObservation::Absent
            | ScaleSetRunnerObservation::RegistrationOnly { .. } => {
                persist(DisposableAttemptCatalogAction::BeginCleanup)
            }
            ScaleSetRunnerObservation::IdleReady { runner } => persist(validate_transition(
                input.attempt,
                DisposableAttemptCatalogAction::RecordRunnerReady(runner.clone()),
            )?),
            ScaleSetRunnerObservation::Conflicting => unreachable!(),
        },
        Phase::Waiting | Phase::Assigned | Phase::Running if cleanup => {
            persist(DisposableAttemptCatalogAction::BeginCleanup)
        }
        Phase::Waiting | Phase::Assigned | Phase::Running
            if matches!(input.vm, DisposableVmObservation::Unknown) =>
        {
            Action::Observe {
                target: DisposableWorkerObservationTarget::Vm,
            }
        }
        Phase::Waiting | Phase::Assigned | Phase::Running
            if matches!(
                input.vm,
                DisposableVmObservation::Absent | DisposableVmObservation::Stopped
            ) =>
        {
            persist(DisposableAttemptCatalogAction::BeginCleanup)
        }
        Phase::Waiting | Phase::Assigned | Phase::Running => match &input.runner {
            ScaleSetRunnerObservation::Unknown => Action::Observe {
                target: DisposableWorkerObservationTarget::Runner,
            },
            ScaleSetRunnerObservation::Absent
            | ScaleSetRunnerObservation::RegistrationOnly { .. } => {
                persist(DisposableAttemptCatalogAction::BeginCleanup)
            }
            ScaleSetRunnerObservation::IdleReady { runner }
                if input.attempt.runner_id().is_none() =>
            {
                persist(validate_transition(
                    input.attempt,
                    DisposableAttemptCatalogAction::RecordRunnerReady(runner.clone()),
                )?)
            }
            ScaleSetRunnerObservation::IdleReady { .. } => Action::Wait,
            ScaleSetRunnerObservation::Conflicting => unreachable!(),
        },
        Phase::Terminal => persist(DisposableAttemptCatalogAction::BeginCleanup),
        Phase::Destroying
            if input.vm_identity.is_some() && input.attempt.vm_identity().is_none() =>
        {
            Action::Blocked {
                code: "unbound_vm_recovery_required",
            }
        }
        Phase::Destroying => match input.vm {
            DisposableVmObservation::Unknown => Action::Observe {
                target: DisposableWorkerObservationTarget::Vm,
            },
            DisposableVmObservation::Stopped | DisposableVmObservation::Ready => Action::DestroyVm,
            DisposableVmObservation::Absent => persist(
                DisposableAttemptCatalogAction::AdvanceCleanup(Phase::Deregistering),
            ),
            DisposableVmObservation::Conflicting => unreachable!(),
        },
        Phase::Deregistering
            if input.vm_identity.is_some() && input.attempt.vm_identity().is_none() =>
        {
            Action::Blocked {
                code: "unbound_vm_recovery_required",
            }
        }
        Phase::Deregistering if matches!(input.vm, DisposableVmObservation::Unknown) => {
            Action::Observe {
                target: DisposableWorkerObservationTarget::Vm,
            }
        }
        Phase::Deregistering
            if matches!(
                input.vm,
                DisposableVmObservation::Stopped | DisposableVmObservation::Ready
            ) =>
        {
            Action::DestroyVm
        }
        Phase::Deregistering => match &input.runner {
            ScaleSetRunnerObservation::Unknown => Action::Observe {
                target: DisposableWorkerObservationTarget::Runner,
            },
            ScaleSetRunnerObservation::Absent => persist(
                DisposableAttemptCatalogAction::AdvanceCleanup(Phase::Releasing),
            ),
            ScaleSetRunnerObservation::RegistrationOnly { runner }
            | ScaleSetRunnerObservation::IdleReady { runner }
                if input.attempt.runner_id().is_none()
                    && input.attempt.jit_generation_started() =>
            {
                persist(validate_transition(
                    input.attempt,
                    DisposableAttemptCatalogAction::RecordRegistration(runner.clone()),
                )?)
            }
            ScaleSetRunnerObservation::RegistrationOnly { .. }
            | ScaleSetRunnerObservation::IdleReady { .. }
                if input.attempt.runner_id().is_none() =>
            {
                Action::Blocked {
                    code: "runner_exists_before_jit_checkpoint",
                }
            }
            ScaleSetRunnerObservation::RegistrationOnly { runner }
            | ScaleSetRunnerObservation::IdleReady { runner } => Action::DeleteRunner {
                runner: runner.clone(),
            },
            ScaleSetRunnerObservation::Conflicting => unreachable!(),
        },
        Phase::Releasing
            if input.vm_identity.is_some() && input.attempt.vm_identity().is_none() =>
        {
            Action::Blocked {
                code: "unbound_vm_recovery_required",
            }
        }
        Phase::Releasing if matches!(input.vm, DisposableVmObservation::Unknown) => {
            Action::Observe {
                target: DisposableWorkerObservationTarget::Vm,
            }
        }
        Phase::Releasing
            if matches!(
                input.vm,
                DisposableVmObservation::Stopped | DisposableVmObservation::Ready
            ) =>
        {
            Action::DestroyVm
        }
        Phase::Releasing if matches!(input.runner, ScaleSetRunnerObservation::Unknown) => {
            Action::Observe {
                target: DisposableWorkerObservationTarget::Runner,
            }
        }
        Phase::Releasing => match &input.runner {
            ScaleSetRunnerObservation::RegistrationOnly { runner }
            | ScaleSetRunnerObservation::IdleReady { runner }
                if input.attempt.runner_id().is_none() =>
            {
                persist(validate_transition(
                    input.attempt,
                    DisposableAttemptCatalogAction::RecordRegistration(runner.clone()),
                )?)
            }
            ScaleSetRunnerObservation::RegistrationOnly { runner }
            | ScaleSetRunnerObservation::IdleReady { runner } => Action::DeleteRunner {
                runner: runner.clone(),
            },
            ScaleSetRunnerObservation::Absent if input.capacity_reserved => Action::ReleaseCapacity,
            ScaleSetRunnerObservation::Absent => persist(
                DisposableAttemptCatalogAction::AdvanceCleanup(Phase::Complete),
            ),
            ScaleSetRunnerObservation::Unknown | ScaleSetRunnerObservation::Conflicting => {
                unreachable!()
            }
        },
        Phase::Complete if input.capacity_reserved => Action::Blocked {
            code: "completed_attempt_retains_capacity",
        },
        Phase::Complete
            if matches!(input.vm, DisposableVmObservation::Unknown)
                || matches!(input.runner, ScaleSetRunnerObservation::Unknown) =>
        {
            Action::Blocked {
                code: "completed_attempt_external_state_unknown",
            }
        }
        Phase::Complete
            if !matches!(input.vm, DisposableVmObservation::Absent)
                || !matches!(input.runner, ScaleSetRunnerObservation::Absent) =>
        {
            Action::Blocked {
                code: "completed_attempt_retains_external_state",
            }
        }
        Phase::Complete => Action::NoOp,
    })
}

fn persist(transition: DisposableAttemptCatalogAction) -> DisposableWorkerAction {
    DisposableWorkerAction::Persist { transition }
}

fn validate_transition(
    attempt: &DisposableAttemptState,
    transition: DisposableAttemptCatalogAction,
) -> Result<DisposableAttemptCatalogAction, DisposableWorkerReconcilerError> {
    let result = match &transition {
        DisposableAttemptCatalogAction::BeginProvisioning => attempt.begin_provisioning(),
        DisposableAttemptCatalogAction::AuthorizeClone => attempt.authorize_clone(),
        DisposableAttemptCatalogAction::RecordCloneStarted => attempt.record_clone_started(),
        DisposableAttemptCatalogAction::BeginUnprovisionedRelease => {
            attempt.begin_unprovisioned_release()
        }
        DisposableAttemptCatalogAction::CompleteUnprovisioned => attempt.complete_unprovisioned(),
        DisposableAttemptCatalogAction::BeginRegistration => attempt.begin_registration(),
        DisposableAttemptCatalogAction::RecordJitGenerationStarted => {
            attempt.record_jit_generation_started()
        }
        DisposableAttemptCatalogAction::RecordRegistration(runner) => {
            attempt.record_registration(runner)
        }
        DisposableAttemptCatalogAction::RecordRunnerStartStarted => {
            attempt.record_runner_start_started()
        }
        DisposableAttemptCatalogAction::RecordRunnerReady(runner) => {
            attempt.record_runner_ready(runner)
        }
        DisposableAttemptCatalogAction::RecordAssigned(job_id) => {
            attempt.record_assigned(job_id.clone())
        }
        DisposableAttemptCatalogAction::RecordRunning { runner, job_id } => {
            attempt.record_running(runner, job_id.clone())
        }
        DisposableAttemptCatalogAction::RecordTerminal {
            runner,
            job_id,
            result,
        } => attempt.record_terminal(runner.as_ref(), job_id.clone(), result.clone()),
        DisposableAttemptCatalogAction::BeginCleanup => attempt.begin_cleanup(),
        DisposableAttemptCatalogAction::AdvanceCleanup(phase) => attempt.advance_cleanup(*phase),
    };
    result.map_err(|error| {
        let code = if error.code() != "identity_drift" {
            "invalid_attempt_transition"
        } else {
            "github_job_identity_drift"
        };
        DisposableWorkerReconcilerError::new(
            "attempt",
            code,
            "observation cannot advance the durable disposable attempt",
        )
    })?;
    Ok(transition)
}

fn validate_vm_observation(
    attempt: &DisposableAttemptState,
    observation: DisposableVmObservation,
    observed_identity: Option<&DisposableVmIdentity>,
) -> Result<(), DisposableWorkerReconcilerError> {
    let identity_required = matches!(
        observation,
        DisposableVmObservation::Stopped | DisposableVmObservation::Ready
    );
    if identity_required != observed_identity.is_some() {
        return Err(DisposableWorkerReconcilerError::new(
            "vm_identity",
            "invalid_vm_observation",
            "VM identity evidence does not match the observation state",
        ));
    }
    if let (Some(current), Some(observed)) = (attempt.vm_identity(), observed_identity)
        && current != observed
    {
        return Err(DisposableWorkerReconcilerError::new(
            "vm_identity",
            "vm_identity_drift",
            "observed VM identity differs from the durable disposable VM identity",
        ));
    }
    Ok(())
}

fn validate_runner_observation(
    attempt: &DisposableAttemptState,
    observation: &ScaleSetRunnerObservation,
) -> Result<(), DisposableWorkerReconcilerError> {
    let runner = match observation {
        ScaleSetRunnerObservation::RegistrationOnly { runner }
        | ScaleSetRunnerObservation::IdleReady { runner } => runner,
        ScaleSetRunnerObservation::Unknown
        | ScaleSetRunnerObservation::Absent
        | ScaleSetRunnerObservation::Conflicting => return Ok(()),
    };
    if runner.name != *attempt.runner_name()
        || attempt.runner_id().is_some_and(|id| id != runner.id)
    {
        return Err(DisposableWorkerReconcilerError::new(
            "runner",
            "runner_identity_drift",
            "runner observation differs from the durable runner identity",
        ));
    }
    Ok(())
}

fn plan_job_event(
    attempt: &DisposableAttemptState,
    event: &ScaleSetJobEvent,
) -> Result<Option<DisposableWorkerAction>, DisposableWorkerReconcilerError> {
    let transition = match event {
        ScaleSetJobEvent::Started { runner, job_id } => {
            DisposableAttemptCatalogAction::RecordRunning {
                runner: runner.clone(),
                job_id: job_id.clone(),
            }
        }
        ScaleSetJobEvent::Completed {
            runner,
            job_id,
            result,
        } => DisposableAttemptCatalogAction::RecordTerminal {
            runner: runner.clone(),
            job_id: job_id.clone(),
            result: result.clone(),
        },
    };
    let before = attempt.revision();
    let transition = validate_transition(attempt, transition)?;
    let after = match &transition {
        DisposableAttemptCatalogAction::RecordRunning { runner, job_id } => {
            attempt.record_running(runner, job_id.clone())
        }
        DisposableAttemptCatalogAction::RecordTerminal {
            runner,
            job_id,
            result,
        } => attempt.record_terminal(runner.as_ref(), job_id.clone(), result.clone()),
        _ => unreachable!(),
    }
    .map_err(|_| {
        DisposableWorkerReconcilerError::new(
            "job_event",
            "invalid_job_event",
            "job event cannot advance the durable attempt",
        )
    })?;
    Ok((after.revision() != before).then(|| persist(transition)))
}

fn validate_identifier(
    field: &'static str,
    value: &str,
) -> Result<(), DisposableWorkerReconcilerError> {
    if value.is_empty()
        || value.len() > MAX_IDENTIFIER_LEN
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        || value.starts_with('-')
        || value.ends_with('-')
    {
        return Err(DisposableWorkerReconcilerError::new(
            field,
            "invalid_identifier",
            "identifier must be bounded lowercase ASCII with interior hyphens only",
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DisposableWorkerReconcilerError {
    field: &'static str,
    code: &'static str,
    message: &'static str,
}

impl DisposableWorkerReconcilerError {
    const fn new(field: &'static str, code: &'static str, message: &'static str) -> Self {
        Self {
            field,
            code,
            message,
        }
    }

    #[must_use]
    pub const fn code(&self) -> &'static str {
        self.code
    }
}

impl fmt::Display for DisposableWorkerReconcilerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.field, self.message)
    }
}

impl std::error::Error for DisposableWorkerReconcilerError {}
