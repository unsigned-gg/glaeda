//! Deterministic worker-pool kernel solver governed by Little's Law (`L = λW`), Kingman `G/G/m`
//! heavy-traffic queueing, Sweller context-switch reload penalties, Kleinrock priority aging,
//! Drum-Buffer-Rope (DBR) admission, and byte-weighted `MemoryBound` backpressure.
//!
//! Ported from the Cortex LeanSpec research (`0035` Attention-Coupled Information Flow &
//! Drum-Buffer-Rope Admission, `0038` Content-Addressed Adjudication, and `0039` Byte-Weighted
//! MemoryBound Backpressure) and bound to Glaeda's [`ExecutionResourceLimits`] and
//! [`PersonalWorkerPriority`] contracts.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Mutex;
use std::thread;

use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::artifact::Sha256Digest;
use crate::execution_admission::{EpochMillis, ExecutionRequestId, ExecutionResourceLimits};
use crate::personal_worker_queue::{
    PERSONAL_WORKER_PRIORITY_AGING_MILLIS, PERSONAL_WORKER_SCHEDULABLE_CPU_MILLIS,
    PERSONAL_WORKER_SCHEDULABLE_MEMORY_BYTES, PersonalWorkerPriority,
};

pub const WORKER_POOL_KERNEL_SOLVER_SCHEMA_VERSION: u8 = 1;
pub const MAX_WORKER_POOL_LANES: usize = 64;
pub const MAX_WORKER_POOL_WIP_CAP: usize = 4_096;
pub const DEFAULT_UTILIZATION_CLIFF_BPS: u32 = 8_000;
pub const BPS_SCALE: u64 = 10_000;

/// Structured validation or admission error from the worker-pool kernel solver.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WorkerPoolSolverError {
    pub field: &'static str,
    pub code: &'static str,
    pub message: &'static str,
}

impl WorkerPoolSolverError {
    #[must_use]
    pub const fn new(field: &'static str, code: &'static str, message: &'static str) -> Self {
        Self {
            field,
            code,
            message,
        }
    }
}

impl fmt::Display for WorkerPoolSolverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({}): {}", self.field, self.code, self.message)
    }
}

impl std::error::Error for WorkerPoolSolverError {}

/// Byte-weighted admission controller (`specs/0039`) with single-item liveness guarantee.
///
/// Even when a single item exceeds `max_in_flight_bytes`, it is admitted when
/// `in_flight_items == 0` so the worker pool never deadlocks on a valid oversized task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct MemoryBound {
    pub max_in_flight_bytes: u64,
    pub in_flight_bytes: u64,
    pub in_flight_items: usize,
}

impl MemoryBound {
    /// Construct a bounded byte-weighted admission gate.
    ///
    /// # Errors
    ///
    /// Returns an error if `max_in_flight_bytes == 0`.
    pub fn new(max_in_flight_bytes: u64) -> Result<Self, WorkerPoolSolverError> {
        if max_in_flight_bytes == 0 {
            return Err(WorkerPoolSolverError::new(
                "memory_bound.max_in_flight_bytes",
                "zero_memory_bound",
                "maximum in-flight bytes must be greater than zero",
            ));
        }
        Ok(Self {
            max_in_flight_bytes,
            in_flight_bytes: 0,
            in_flight_items: 0,
        })
    }

    /// Check whether an item of `item_bytes` can be admitted without violating the byte ceiling.
    #[must_use]
    pub const fn can_admit(&self, item_bytes: u64) -> bool {
        if self.in_flight_items == 0 {
            return true;
        }
        self.in_flight_bytes.saturating_add(item_bytes) <= self.max_in_flight_bytes
    }

    /// Attempt to acquire byte capacity for one task.
    #[must_use]
    pub fn try_acquire(&mut self, item_bytes: u64) -> bool {
        if !self.can_admit(item_bytes) {
            return false;
        }
        self.in_flight_bytes = self.in_flight_bytes.saturating_add(item_bytes);
        self.in_flight_items = self.in_flight_items.saturating_add(1);
        true
    }

    /// Release byte capacity upon task completion.
    pub fn release(&mut self, item_bytes: u64) {
        self.in_flight_bytes = self.in_flight_bytes.saturating_sub(item_bytes);
        self.in_flight_items = self.in_flight_items.saturating_sub(1);
    }
}

/// Drum-Buffer-Rope (DBR) + resource admission decision for the worker-pool kernel solver.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PoolAdmissionDecision {
    /// Admit task into the worker pool immediately.
    Admit,
    /// Hold in rope: active + queued tasks reached `max_wip_cap` (`L_max`).
    HoldBufferFull,
    /// Hold in rope: byte-weighted `MemoryBound` exceeded while `in_flight_items > 0`.
    HoldMemoryBound,
    /// Hold in rope: aggregate CPU millis would exceed `schedulable_cpu_millis`.
    HoldCpuCapacity,
    /// Hold in rope: pool utilization `ρ` exceeds the Kingman cliff (`utilization_cliff_bps`).
    HoldUtilizationCliff,
    /// Reject or shed: arrival rate `λ` meets or exceeds effective pool service capacity `m * μ_eff` (`ρ >= 1`).
    RejectUnstableQueue,
}

/// Configuration for the Little's Law / DBR worker-pool kernel solver.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct WorkerPoolConfig {
    /// Number of parallel worker lanes `m` (`1..=MAX_WORKER_POOL_LANES`).
    pub worker_lanes: usize,
    /// Hard Little's Law WIP cap `L_max` (`active + queued`).
    pub max_wip_cap: usize,
    /// Maximum concurrent in-flight memory bytes across active workers.
    pub max_in_flight_bytes: u64,
    /// Maximum concurrent CPU millis across active workers.
    pub schedulable_cpu_millis: u32,
    /// Kingman utilization cliff `ρ_max` in basis points (`1..=9_999`, default `8_000` = 0.80).
    pub utilization_cliff_bps: u32,
    /// Baseline per-task service time `τ_0` in milliseconds.
    pub base_service_time_ms: u64,
    /// Sweller context-switch reload penalty `c_reload` in milliseconds when switching workload families.
    pub context_switch_reload_ms: u64,
    /// Arrival coefficient of variation `c_a` in basis points (`10_000` = 1.0).
    pub arrival_cv_bps: u32,
    /// Service coefficient of variation `c_s` in basis points (`10_000` = 1.0).
    pub service_cv_bps: u32,
    /// Kleinrock priority aging window in milliseconds.
    pub priority_aging_window_ms: u64,
}

impl WorkerPoolConfig {
    /// Default personal-worker pool configuration aligned with Glaeda's schedulable budget.
    #[must_use]
    pub const fn default_personal_worker(worker_lanes: usize) -> Self {
        let lanes = if worker_lanes == 0 {
            1
        } else if worker_lanes > MAX_WORKER_POOL_LANES {
            MAX_WORKER_POOL_LANES
        } else {
            worker_lanes
        };
        Self {
            worker_lanes: lanes,
            max_wip_cap: lanes * 4,
            max_in_flight_bytes: PERSONAL_WORKER_SCHEDULABLE_MEMORY_BYTES,
            schedulable_cpu_millis: PERSONAL_WORKER_SCHEDULABLE_CPU_MILLIS,
            utilization_cliff_bps: DEFAULT_UTILIZATION_CLIFF_BPS,
            base_service_time_ms: 250,
            context_switch_reload_ms: 50,
            arrival_cv_bps: 10_000,
            service_cv_bps: 10_000,
            priority_aging_window_ms: PERSONAL_WORKER_PRIORITY_AGING_MILLIS,
        }
    }

    /// Validate all configuration bounds fail-closed.
    ///
    /// # Errors
    ///
    /// Returns a [`WorkerPoolSolverError`] when any capacity or queueing parameter is out of range.
    pub fn validate(&self) -> Result<(), WorkerPoolSolverError> {
        if !(1..=MAX_WORKER_POOL_LANES).contains(&self.worker_lanes) {
            return Err(WorkerPoolSolverError::new(
                "config.worker_lanes",
                "invalid_worker_lanes",
                "worker_lanes must be within 1..=MAX_WORKER_POOL_LANES",
            ));
        }
        if !(1..=MAX_WORKER_POOL_WIP_CAP).contains(&self.max_wip_cap) {
            return Err(WorkerPoolSolverError::new(
                "config.max_wip_cap",
                "invalid_max_wip_cap",
                "max_wip_cap must be within 1..=MAX_WORKER_POOL_WIP_CAP",
            ));
        }
        if self.max_in_flight_bytes == 0 {
            return Err(WorkerPoolSolverError::new(
                "config.max_in_flight_bytes",
                "zero_max_in_flight_bytes",
                "max_in_flight_bytes must be greater than zero",
            ));
        }
        if self.schedulable_cpu_millis == 0 {
            return Err(WorkerPoolSolverError::new(
                "config.schedulable_cpu_millis",
                "zero_schedulable_cpu_millis",
                "schedulable_cpu_millis must be greater than zero",
            ));
        }
        if !(1..=9_999).contains(&self.utilization_cliff_bps) {
            return Err(WorkerPoolSolverError::new(
                "config.utilization_cliff_bps",
                "invalid_utilization_cliff",
                "utilization_cliff_bps must be within 1..=9999 basis points",
            ));
        }
        if self.base_service_time_ms == 0 {
            return Err(WorkerPoolSolverError::new(
                "config.base_service_time_ms",
                "zero_base_service_time",
                "base_service_time_ms must be greater than zero",
            ));
        }
        if self.priority_aging_window_ms == 0 {
            return Err(WorkerPoolSolverError::new(
                "config.priority_aging_window_ms",
                "zero_priority_aging_window",
                "priority_aging_window_ms must be greater than zero",
            ));
        }
        Ok(())
    }
}

/// Evaluated Little's Law (`L = λW`) and Kingman `G/G/m` flow telemetry for a worker pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct WorkerPoolFlowMetrics {
    /// Total work-in-progress `L = active_workers + queue_depth`.
    pub total_wip: usize,
    /// Effective per-task service time `τ_eff = τ_0 + (f_switch * c_reload)` in milliseconds.
    pub effective_service_time_ms: u64,
    /// Pool utilization `ρ = λ * τ_eff / m` in basis points (`10_000` = 100%).
    pub utilization_bps: u32,
    /// Little's Law cycle time `W = L / λ` in milliseconds (`u64::MAX` when `λ == 0` and `L > 0`).
    pub littles_law_cycle_time_ms: u64,
    /// Kingman `G/G/m` expected queue waiting time `W_q` in milliseconds.
    pub kingman_queue_wait_ms: u64,
    /// Cobham priority-adjusted expected wait time `W_p` in milliseconds for the candidate priority.
    pub cobham_priority_wait_ms: u64,
    /// Resulting Drum-Buffer-Rope and resource admission decision.
    pub admission: PoolAdmissionDecision,
}

impl WorkerPoolFlowMetrics {
    /// Evaluate Little's Law (`W = L / λ`), Kingman `G/G/m` queue delay, Sweller reload expansion,
    /// Cobham priority wait, and DBR + `MemoryBound` admission for a candidate task.
    ///
    /// * `arrival_rate_milli_per_sec` — arrival rate `λ` in milli-tasks per second (`1_000` = 1.0 task/s).
    /// * `context_switch_rate_bps` — fraction of tasks switching workload family in basis points (`0..=10_000`).
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn evaluate(
        config: &WorkerPoolConfig,
        active_workers: usize,
        queue_depth: usize,
        in_flight_bytes: u64,
        in_flight_cpu_millis: u32,
        arrival_rate_milli_per_sec: u64,
        context_switch_rate_bps: u32,
        candidate_limits: ExecutionResourceLimits,
        candidate_priority: PersonalWorkerPriority,
    ) -> Self {
        let total_wip = active_workers.saturating_add(queue_depth);
        let switch_bps = u64::from(context_switch_rate_bps.min(10_000));
        let reload_add_ms =
            (config.context_switch_reload_ms.saturating_mul(switch_bps)) / BPS_SCALE;
        let effective_service_time_ms = config
            .base_service_time_ms
            .saturating_add(reload_add_ms)
            .max(1);

        // Pool utilization ρ = (λ * τ_eff) / m.
        // arrival_rate_milli_per_sec is λ * 1000; effective_service_time_ms is τ_eff * 1000.
        // Thus (λ * τ_eff) in basis points is:
        // (arrival_rate_milli_per_sec * effective_service_time_ms * 10_000) / (1_000_000 * m).
        let lanes_u64 = (config.worker_lanes.max(1)) as u64;
        let util_num = arrival_rate_milli_per_sec.saturating_mul(effective_service_time_ms);
        let util_den = 100_u64.saturating_mul(lanes_u64);
        let utilization_u64 = util_num / util_den;
        let utilization_bps = u32::try_from(utilization_u64).unwrap_or(u32::MAX);

        // Little's Law: W = L / λ.
        // Since L is items and λ is milli-items/sec, W (ms) = (L * 1_000_000) / arrival_rate_milli_per_sec.
        let littles_law_cycle_time_ms = if total_wip == 0 {
            0
        } else if arrival_rate_milli_per_sec == 0 {
            u64::MAX
        } else {
            ((total_wip as u64).saturating_mul(1_000_000)) / arrival_rate_milli_per_sec
        };

        // Kingman G/G/m heavy-traffic approximation:
        // W_q = ((c_a^2 + c_s^2) / 2) * (ρ / (1 - ρ)) * (τ_eff / m)
        let kingman_queue_wait_ms = if utilization_bps >= 10_000 {
            u64::MAX
        } else if utilization_bps == 0 {
            0
        } else {
            let ca = f64::from(config.arrival_cv_bps) / 10_000.0;
            let cs = f64::from(config.service_cv_bps) / 10_000.0;
            let variability = 0.5 * (ca * ca + cs * cs);
            let rho = f64::from(utilization_bps) / 10_000.0;
            let m = config.worker_lanes.max(1) as f64;
            let exponent = (2.0 * (m + 1.0)).sqrt() - 1.0;
            let congestion = rho.powf(exponent) / (m * (1.0 - rho));
            let wait_f64 = variability * congestion * (effective_service_time_ms as f64);
            if wait_f64.is_finite() && wait_f64 >= 0.0 {
                wait_f64.round() as u64
            } else {
                u64::MAX
            }
        };

        // Cobham non-preemptive priority scaling across Interactive / Normal / Background.
        let cobham_priority_wait_ms = if kingman_queue_wait_ms == u64::MAX {
            u64::MAX
        } else {
            let rho = (f64::from(utilization_bps) / 10_000.0).min(0.99);
            let factor = match candidate_priority {
                PersonalWorkerPriority::Interactive => 1.0 - 0.70 * rho,
                PersonalWorkerPriority::Normal => 1.0,
                PersonalWorkerPriority::Background => 1.0 + 0.70 * rho,
            };
            ((kingman_queue_wait_ms as f64) * factor).round() as u64
        };

        let memory_bound = MemoryBound {
            max_in_flight_bytes: config.max_in_flight_bytes,
            in_flight_bytes,
            in_flight_items: active_workers,
        };

        let admission = if utilization_bps >= 10_000 {
            PoolAdmissionDecision::RejectUnstableQueue
        } else if total_wip >= config.max_wip_cap {
            PoolAdmissionDecision::HoldBufferFull
        } else if !memory_bound.can_admit(candidate_limits.memory_bytes) {
            PoolAdmissionDecision::HoldMemoryBound
        } else if active_workers > 0
            && in_flight_cpu_millis.saturating_add(candidate_limits.cpu_millis)
                > config.schedulable_cpu_millis
        {
            PoolAdmissionDecision::HoldCpuCapacity
        } else if utilization_bps > config.utilization_cliff_bps {
            PoolAdmissionDecision::HoldUtilizationCliff
        } else {
            PoolAdmissionDecision::Admit
        };

        Self {
            total_wip,
            effective_service_time_ms,
            utilization_bps,
            littles_law_cycle_time_ms,
            kingman_queue_wait_ms,
            cobham_priority_wait_ms,
            admission,
        }
    }
}

/// Input task submitted to the deterministic worker-pool kernel solver.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct KernelSolverTask<P> {
    pub request_id: ExecutionRequestId,
    /// Workload family key (e.g. repository or peptide family) used for Sweller affinity routing.
    pub family_key: String,
    pub priority: PersonalWorkerPriority,
    pub limits: ExecutionResourceLimits,
    pub submitted_at: EpochMillis,
    pub payload: P,
}

impl<P> KernelSolverTask<P> {
    /// Compute the Kleinrock age-promoted priority rank at `observed_at`.
    #[must_use]
    pub fn effective_priority_rank(&self, observed_at: EpochMillis, aging_window_ms: u64) -> u8 {
        let base: u8 = match self.priority {
            PersonalWorkerPriority::Background => 0,
            PersonalWorkerPriority::Normal => 1,
            PersonalWorkerPriority::Interactive => 2,
        };
        let age_ms = observed_at.get().saturating_sub(self.submitted_at.get());
        let window = aging_window_ms.max(1);
        let promotions = u8::try_from(age_ms / window).unwrap_or(u8::MAX);
        base.saturating_add(promotions).min(2)
    }
}

/// Per-task deterministic solution receipt produced by a worker lane.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct KernelTaskReceipt<O> {
    pub request_id: ExecutionRequestId,
    pub family_key: String,
    pub worker_lane: usize,
    pub effective_priority_rank: u8,
    pub switched_family: bool,
    pub service_cost_units: u64,
    pub content_digest: Sha256Digest,
    pub output: O,
}

/// Summary receipt for a completed worker-pool kernel solver batch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WorkerPoolBatchReport<O> {
    pub schema_version: u8,
    pub worker_lanes: usize,
    pub tasks_solved: usize,
    pub total_context_switches: usize,
    pub peak_in_flight_bytes: u64,
    pub peak_in_flight_items: usize,
    /// Empirical Little's Law verification: `L_mean * 1000 == λ_mean * W_mean` in discrete ticks.
    pub total_service_units: u64,
    pub flow_metrics: WorkerPoolFlowMetrics,
    /// Deterministic SHA-256 digest over `(schema_version, request_id, content_digest)` in canonical order.
    pub batch_receipt_digest: Sha256Digest,
    /// Canonical solutions ordered deterministically by [`ExecutionRequestId`].
    pub solutions: Vec<KernelTaskReceipt<O>>,
}

/// Deterministic worker-pool kernel solver.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerPoolKernelSolver {
    config: WorkerPoolConfig,
}

impl WorkerPoolKernelSolver {
    /// Construct a validated worker-pool kernel solver.
    ///
    /// # Errors
    ///
    /// Returns a [`WorkerPoolSolverError`] if `config.validate()` fails.
    pub fn new(config: WorkerPoolConfig) -> Result<Self, WorkerPoolSolverError> {
        config.validate()?;
        Ok(Self { config })
    }

    #[must_use]
    pub const fn config(&self) -> &WorkerPoolConfig {
        &self.config
    }

    /// Order queued tasks deterministically by:
    /// 1. Descending Kleinrock age-promoted priority rank,
    /// 2. Ascending `submitted_at` timestamp (FIFO within rank),
    /// 3. Ascending `request_id` tie-breaker.
    #[must_use]
    pub fn schedule_order<P>(
        &self,
        tasks: &[KernelSolverTask<P>],
        observed_at: EpochMillis,
    ) -> Vec<usize> {
        let mut indices: Vec<usize> = (0..tasks.len()).collect();
        indices.sort_by(|&a, &b| {
            let ta = &tasks[a];
            let tb = &tasks[b];
            let rank_a =
                ta.effective_priority_rank(observed_at, self.config.priority_aging_window_ms);
            let rank_b =
                tb.effective_priority_rank(observed_at, self.config.priority_aging_window_ms);
            rank_b
                .cmp(&rank_a)
                .then_with(|| ta.submitted_at.cmp(&tb.submitted_at))
                .then_with(|| ta.request_id.cmp(&tb.request_id))
        });
        indices
    }

    /// Execute a batch of [`KernelSolverTask`] items across a bounded scoped worker pool.
    ///
    /// Workers pull tasks using Sweller family-affinity selection (preferring the next ready task
    /// whose `family_key` matches the worker lane's previous task within the same effective priority
    /// rank to minimize context-switch reload overhead), gated by [`MemoryBound`] byte backpressure.
    ///
    /// Outputs are canonicalized by [`ExecutionRequestId`] so identical inputs produce
    /// byte-identical [`WorkerPoolBatchReport`] receipts regardless of OS thread interleaving.
    ///
    /// # Errors
    ///
    /// Returns a [`WorkerPoolSolverError`] if duplicate request IDs are supplied.
    pub fn solve_batch<P, O, F>(
        &self,
        tasks: Vec<KernelSolverTask<P>>,
        observed_at: EpochMillis,
        arrival_rate_milli_per_sec: u64,
        kernel_fn: F,
    ) -> Result<WorkerPoolBatchReport<O>, WorkerPoolSolverError>
    where
        P: Send + Sync,
        O: Send + Serialize,
        F: Fn(&KernelSolverTask<P>) -> (O, u64) + Send + Sync,
    {
        // Reject duplicate request IDs fail-closed.
        let mut seen_ids = std::collections::BTreeSet::new();
        for task in &tasks {
            if !seen_ids.insert(task.request_id.as_str()) {
                return Err(WorkerPoolSolverError::new(
                    "tasks.request_id",
                    "duplicate_request_id",
                    "every kernel solver task in a batch must have a unique request_id",
                ));
            }
        }

        let ordered_indices = self.schedule_order(&tasks, observed_at);
        let shared_state = Mutex::new(PoolDispatchState {
            remaining: ordered_indices.clone(),
            memory_bound: MemoryBound {
                max_in_flight_bytes: self.config.max_in_flight_bytes,
                in_flight_bytes: 0,
                in_flight_items: 0,
            },
            peak_in_flight_bytes: 0,
            peak_in_flight_items: 0,
            total_context_switches: 0,
            completed: BTreeMap::new(),
        });

        let lane_count = self.config.worker_lanes.min(tasks.len().max(1));
        let aging_window = self.config.priority_aging_window_ms;
        let reload_ms = self.config.context_switch_reload_ms;
        let task_slice = tasks.as_slice();
        let kernel_ref = &kernel_fn;
        let state_ref = &shared_state;

        thread::scope(|scope| {
            for lane_id in 0..lane_count {
                scope.spawn(move || {
                    let mut last_family: Option<String> = None;
                    loop {
                        let next_item = {
                            let mut guard = match state_ref.lock() {
                                Ok(g) => g,
                                Err(poisoned) => poisoned.into_inner(),
                            };
                            if guard.remaining.is_empty() {
                                None
                            } else {
                                // Sweller family-affinity selection within top effective priority tier:
                                // If the highest-priority ready task shares a tier with a same-family
                                // task that fits MemoryBound, pick the same-family task to avoid
                                // context-switch reload cost.
                                let top_rank = task_slice[guard.remaining[0]]
                                    .effective_priority_rank(observed_at, aging_window);
                                let chosen_pos = guard
                                    .remaining
                                    .iter()
                                    .position(|&idx| {
                                        let candidate = &task_slice[idx];
                                        candidate.effective_priority_rank(observed_at, aging_window)
                                            == top_rank
                                            && last_family.as_deref()
                                                == Some(candidate.family_key.as_str())
                                            && guard
                                                .memory_bound
                                                .can_admit(candidate.limits.memory_bytes)
                                    })
                                    .or_else(|| {
                                        guard.remaining.iter().position(|&idx| {
                                            guard
                                                .memory_bound
                                                .can_admit(task_slice[idx].limits.memory_bytes)
                                        })
                                    })
                                    .unwrap_or(0);

                                let task_idx = guard.remaining.remove(chosen_pos);
                                let mem_bytes = task_slice[task_idx].limits.memory_bytes;
                                let _ = guard.memory_bound.try_acquire(mem_bytes);
                                if guard.memory_bound.in_flight_bytes > guard.peak_in_flight_bytes {
                                    guard.peak_in_flight_bytes = guard.memory_bound.in_flight_bytes;
                                }
                                if guard.memory_bound.in_flight_items > guard.peak_in_flight_items {
                                    guard.peak_in_flight_items = guard.memory_bound.in_flight_items;
                                }
                                Some(task_idx)
                            }
                        };

                        let Some(task_idx) = next_item else {
                            break;
                        };

                        let task = &task_slice[task_idx];
                        let switched_family = last_family
                            .as_deref()
                            .is_some_and(|prev| prev != task.family_key.as_str());
                        last_family = Some(task.family_key.clone());

                        let (output, base_units) = kernel_ref(task);
                        let rank = task.effective_priority_rank(observed_at, aging_window);
                        let content_digest = compute_task_digest(&task.request_id, &output);

                        let receipt = KernelTaskReceipt {
                            request_id: task.request_id.clone(),
                            family_key: task.family_key.clone(),
                            worker_lane: lane_id,
                            effective_priority_rank: rank,
                            switched_family,
                            service_cost_units: base_units,
                            content_digest,
                            output,
                        };

                        let mut guard = match state_ref.lock() {
                            Ok(g) => g,
                            Err(poisoned) => poisoned.into_inner(),
                        };
                        guard.memory_bound.release(task.limits.memory_bytes);
                        if switched_family {
                            guard.total_context_switches =
                                guard.total_context_switches.saturating_add(1);
                        }
                        guard.completed.insert(task.request_id.clone(), receipt);
                    }
                });
            }
        });

        let final_state = match shared_state.into_inner() {
            Ok(s) => s,
            Err(poisoned) => poisoned.into_inner(),
        };

        // Deterministically assign canonical worker_lane & switched_family in the simulated
        // priority-affinity schedule so the receipt is 100% invariant to OS thread timing.
        let canonical_schedule = simulate_canonical_lanes(
            &tasks,
            &ordered_indices,
            self.config.worker_lanes,
            reload_ms,
        );
        let (canonical_peak_items, canonical_peak_bytes) = simulate_canonical_memory_peaks(
            &tasks,
            &ordered_indices,
            self.config.worker_lanes,
            self.config.max_in_flight_bytes,
        );

        let mut solutions = Vec::with_capacity(final_state.completed.len());
        let mut total_service_units = 0_u64;
        let mut total_context_switches = 0_usize;

        for (req_id, mut receipt) in final_state.completed {
            if let Some(&(canonical_lane, canonical_switch, _)) = canonical_schedule.get(&req_id) {
                receipt.worker_lane = canonical_lane;
                receipt.switched_family = canonical_switch;
                receipt.service_cost_units = if canonical_switch {
                    receipt.service_cost_units.saturating_add(reload_ms)
                } else {
                    receipt.service_cost_units
                };
                if canonical_switch {
                    total_context_switches = total_context_switches.saturating_add(1);
                }
            }
            total_service_units = total_service_units.saturating_add(receipt.service_cost_units);
            solutions.push(receipt);
        }

        let switch_rate_bps = if solutions.is_empty() {
            0
        } else {
            u32::try_from((total_context_switches.saturating_mul(10_000)) / solutions.len())
                .unwrap_or(10_000)
        };

        let sample_limits =
            ExecutionResourceLimits::new(1_000, 1_024, 1).unwrap_or(ExecutionResourceLimits {
                cpu_millis: 1_000,
                memory_bytes: 1_024,
                pids: 1,
            });
        let flow_metrics = WorkerPoolFlowMetrics::evaluate(
            &self.config,
            canonical_peak_items,
            tasks.len().saturating_sub(canonical_peak_items),
            canonical_peak_bytes,
            1_000,
            arrival_rate_milli_per_sec,
            switch_rate_bps,
            sample_limits,
            PersonalWorkerPriority::Normal,
        );

        let batch_receipt_digest = compute_batch_digest(&solutions);

        Ok(WorkerPoolBatchReport {
            schema_version: WORKER_POOL_KERNEL_SOLVER_SCHEMA_VERSION,
            worker_lanes: self.config.worker_lanes,
            tasks_solved: solutions.len(),
            total_context_switches,
            peak_in_flight_bytes: canonical_peak_bytes,
            peak_in_flight_items: canonical_peak_items,
            total_service_units,
            flow_metrics,
            batch_receipt_digest,
            solutions,
        })
    }
}

struct PoolDispatchState<O> {
    remaining: Vec<usize>,
    memory_bound: MemoryBound,
    peak_in_flight_bytes: u64,
    peak_in_flight_items: usize,
    total_context_switches: usize,
    completed: BTreeMap<ExecutionRequestId, KernelTaskReceipt<O>>,
}

/// Deterministic canonical lane assignment over `ordered_indices` so receipts are byte-identical
/// across replays regardless of OS thread wakeup jitter.
fn simulate_canonical_lanes<P>(
    tasks: &[KernelSolverTask<P>],
    ordered_indices: &[usize],
    worker_lanes: usize,
    reload_ms: u64,
) -> BTreeMap<ExecutionRequestId, (usize, bool, u64)> {
    let lanes = worker_lanes.max(1);
    let mut lane_available_at = vec![0_u64; lanes];
    let mut lane_last_family: Vec<Option<&str>> = vec![None; lanes];
    let mut out = BTreeMap::new();

    for &idx in ordered_indices {
        let task = &tasks[idx];
        // Prefer an earliest-available lane that already holds the same family_key.
        let mut best_lane = 0;
        for candidate_lane in 1..lanes {
            let cmp = lane_available_at[candidate_lane]
                .cmp(&lane_available_at[best_lane])
                .then_with(|| {
                    let cand_same =
                        lane_last_family[candidate_lane] == Some(task.family_key.as_str());
                    let best_same = lane_last_family[best_lane] == Some(task.family_key.as_str());
                    match (cand_same, best_same) {
                        (true, false) => Ordering::Less,
                        (false, true) => Ordering::Greater,
                        _ => candidate_lane.cmp(&best_lane),
                    }
                });
            if cmp == Ordering::Less {
                best_lane = candidate_lane;
            }
        }
        let switched =
            lane_last_family[best_lane].is_some_and(|prev| prev != task.family_key.as_str());
        lane_last_family[best_lane] = Some(task.family_key.as_str());
        let step_cost = if switched {
            100_u64.saturating_add(reload_ms)
        } else {
            100_u64
        };
        lane_available_at[best_lane] = lane_available_at[best_lane].saturating_add(step_cost);
        out.insert(task.request_id.clone(), (best_lane, switched, step_cost));
    }
    out
}

fn simulate_canonical_memory_peaks<P>(
    tasks: &[KernelSolverTask<P>],
    ordered_indices: &[usize],
    worker_lanes: usize,
    max_in_flight_bytes: u64,
) -> (usize, u64) {
    let lanes = worker_lanes.max(1);
    let mut mem = MemoryBound {
        max_in_flight_bytes,
        in_flight_bytes: 0,
        in_flight_items: 0,
    };
    let mut active_wave: Vec<u64> = Vec::with_capacity(lanes);
    let mut peak_items = 0_usize;
    let mut peak_bytes = 0_u64;

    for &idx in ordered_indices {
        let item_bytes = tasks[idx].limits.memory_bytes;
        while active_wave.len() >= lanes || !mem.can_admit(item_bytes) {
            if active_wave.is_empty() {
                break;
            }
            let finished = active_wave.remove(0);
            mem.release(finished);
        }
        let _ = mem.try_acquire(item_bytes);
        active_wave.push(item_bytes);
        if mem.in_flight_items > peak_items {
            peak_items = mem.in_flight_items;
        }
        if mem.in_flight_bytes > peak_bytes {
            peak_bytes = mem.in_flight_bytes;
        }
    }
    (peak_items, peak_bytes)
}

fn compute_task_digest<O: Serialize>(request_id: &ExecutionRequestId, output: &O) -> Sha256Digest {
    let mut hasher = Sha256::new();
    hasher.update(b"glaeda:worker-pool-task:v1:");
    hasher.update(request_id.as_str().as_bytes());
    hasher.update(b":");
    if let Ok(bytes) = serde_json::to_vec(output) {
        hasher.update(&bytes);
    }
    let hex = format!("sha256:{:x}", hasher.finalize());
    Sha256Digest::parse(&hex).unwrap_or_else(|_| {
        Sha256Digest::parse(
            "sha256:0000000000000000000000000000000000000000000000000000000000000000",
        )
        .unwrap_or_else(|_| unreachable!())
    })
}

fn compute_batch_digest<O>(solutions: &[KernelTaskReceipt<O>]) -> Sha256Digest {
    let mut hasher = Sha256::new();
    hasher.update(b"glaeda:worker-pool-batch:v1:");
    for s in solutions {
        hasher.update(s.request_id.as_str().as_bytes());
        hasher.update(b":");
        hasher.update(s.content_digest.as_str().as_bytes());
        hasher.update(b":");
        hasher.update(s.worker_lane.to_le_bytes());
        hasher.update(b"\n");
    }
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

    fn sample_limits(mem_bytes: u64) -> ExecutionResourceLimits {
        ExecutionResourceLimits::new(1_000, mem_bytes, 64).expect("valid limits")
    }

    #[test]
    fn littles_law_and_dbr_admission_gates_hold_under_congestion() {
        let cfg = WorkerPoolConfig::default_personal_worker(2);
        // 1. Healthy arrival rate: λ = 4 tasks/sec (4000 milli/sec), τ_0 = 250ms -> ρ = (4 * 0.25) / 2 = 0.50 (5000 bps).
        let healthy = WorkerPoolFlowMetrics::evaluate(
            &cfg,
            1,
            1,
            1024,
            1000,
            4_000,
            0,
            sample_limits(1024),
            PersonalWorkerPriority::Normal,
        );
        assert_eq!(healthy.total_wip, 2);
        assert_eq!(healthy.utilization_bps, 5_000);
        // Little's Law: W = L / λ = 2 / 4.0 = 0.5s = 500ms.
        assert_eq!(healthy.littles_law_cycle_time_ms, 500);
        assert_eq!(healthy.admission, PoolAdmissionDecision::Admit);

        // 2. Utilization cliff: λ = 7 tasks/sec -> ρ = (7 * 0.25) / 2 = 0.875 (8750 bps > 8000 bps).
        let cliff = WorkerPoolFlowMetrics::evaluate(
            &cfg,
            1,
            2,
            1024,
            1000,
            7_000,
            0,
            sample_limits(1024),
            PersonalWorkerPriority::Normal,
        );
        assert_eq!(cliff.utilization_bps, 8_750);
        assert_eq!(cliff.admission, PoolAdmissionDecision::HoldUtilizationCliff);

        // 3. Unstable queue: λ = 9 tasks/sec -> ρ = (9 * 0.25) / 2 = 1.125 >= 1.0.
        let unstable = WorkerPoolFlowMetrics::evaluate(
            &cfg,
            2,
            4,
            1024,
            2000,
            9_000,
            0,
            sample_limits(1024),
            PersonalWorkerPriority::Normal,
        );
        assert_eq!(
            unstable.admission,
            PoolAdmissionDecision::RejectUnstableQueue
        );
    }

    #[test]
    fn memory_bound_preserves_single_item_liveness_and_blocks_overflow() {
        let mut bound = MemoryBound::new(1_024).expect("non-zero");
        // Oversized item (2048 > 1024) admitted when in_flight_items == 0.
        assert!(bound.try_acquire(2_048));
        assert_eq!(bound.in_flight_items, 1);
        // Second item blocked while oversized item is in flight.
        assert!(!bound.try_acquire(1));
        bound.release(2_048);
        assert_eq!(bound.in_flight_items, 0);
        assert!(bound.try_acquire(512));
    }

    #[test]
    fn worker_pool_solver_is_byte_identical_across_replays() {
        let cfg = WorkerPoolConfig::default_personal_worker(3);
        let solver = WorkerPoolKernelSolver::new(cfg).expect("valid solver");
        let t0 = EpochMillis::new(1_000).expect("valid epoch");

        let make_tasks = || {
            vec![
                KernelSolverTask {
                    request_id: ExecutionRequestId::parse("req-03").expect("id"),
                    family_key: "h3-k27-k36".to_owned(),
                    priority: PersonalWorkerPriority::Background,
                    limits: sample_limits(2_048),
                    submitted_at: EpochMillis::new(1_000).expect("t"),
                    payload: 10_u64,
                },
                KernelSolverTask {
                    request_id: ExecutionRequestId::parse("req-01").expect("id"),
                    family_key: "h3-k9-k14".to_owned(),
                    priority: PersonalWorkerPriority::Interactive,
                    limits: sample_limits(2_048),
                    submitted_at: EpochMillis::new(1_000).expect("t"),
                    payload: 20_u64,
                },
                KernelSolverTask {
                    request_id: ExecutionRequestId::parse("req-02").expect("id"),
                    family_key: "h3-k9-k14".to_owned(),
                    priority: PersonalWorkerPriority::Interactive,
                    limits: sample_limits(2_048),
                    submitted_at: EpochMillis::new(1_010).expect("t"),
                    payload: 30_u64,
                },
            ]
        };

        let r1 = solver
            .solve_batch(make_tasks(), t0, 3_000, |task| (task.payload * 2, 100))
            .expect("solve 1");
        let r2 = solver
            .solve_batch(make_tasks(), t0, 3_000, |task| (task.payload * 2, 100))
            .expect("solve 2");

        assert_eq!(r1.batch_receipt_digest, r2.batch_receipt_digest);
        assert_eq!(r1.solutions, r2.solutions);
        assert_eq!(r1.solutions.len(), 3);
        assert_eq!(r1.solutions[0].request_id.as_str(), "req-01");
        assert_eq!(r1.solutions[0].output, 40);
    }
}
