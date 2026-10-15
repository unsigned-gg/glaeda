//! Contract tests for the Little's Law (`L = λW`), Kingman `G/G/m`, Drum-Buffer-Rope (DBR),
//! and `MemoryBound` worker-pool kernel solver (`src/worker_pool_kernel_solver.rs`).

use glaeda::execution_admission::{EpochMillis, ExecutionRequestId, ExecutionResourceLimits};
use glaeda::personal_worker_queue::PersonalWorkerPriority;
use glaeda::worker_pool_kernel_solver::{
    KernelSolverTask, MemoryBound, PoolAdmissionDecision, WorkerPoolConfig, WorkerPoolFlowMetrics,
    WorkerPoolKernelSolver,
};

fn limits(cpu_millis: u32, memory_bytes: u64) -> ExecutionResourceLimits {
    ExecutionResourceLimits::new(cpu_millis, memory_bytes, 64).expect("valid resource limits")
}

#[test]
fn littles_law_identity_and_kingman_utilization_cliff_are_enforced() {
    let config = WorkerPoolConfig::default_personal_worker(2);

    // L = 4 items (2 active, 2 queued), λ = 2 tasks/sec (2000 milli/sec), τ_0 = 250ms.
    // Pool utilization ρ = (λ * τ_0) / m = (2 * 0.25) / 2 = 0.25 (2500 bps).
    // Little's Law cycle time W = L / λ = 4 / 2 = 2.0s (2000 ms).
    let flow = WorkerPoolFlowMetrics::evaluate(
        &config,
        2,
        2,
        1_024 * 1_024,
        2_000,
        2_000,
        0,
        limits(1_000, 1_024 * 1_024),
        PersonalWorkerPriority::Normal,
    );
    assert_eq!(flow.total_wip, 4);
    assert_eq!(flow.utilization_bps, 2_500);
    assert_eq!(flow.littles_law_cycle_time_ms, 2_000);
    assert_eq!(flow.admission, PoolAdmissionDecision::Admit);

    // WIP buffer cap reached: total_wip (8) >= max_wip_cap (8).
    let buffer_full = WorkerPoolFlowMetrics::evaluate(
        &config,
        2,
        6,
        1_024 * 1_024,
        2_000,
        2_000,
        0,
        limits(1_000, 1_024 * 1_024),
        PersonalWorkerPriority::Normal,
    );
    assert_eq!(buffer_full.admission, PoolAdmissionDecision::HoldBufferFull);

    // Sweller context-switch reload penalty pushes utilization across the 80% cliff.
    let cliff = WorkerPoolFlowMetrics::evaluate(
        &config,
        1,
        1,
        1_024 * 1_024,
        1_000,
        5_600,
        10_000, // 100% context switches -> τ_eff = 250 + 50 = 300ms -> ρ = (5.6 * 0.3) / 2 = 0.84 (8400 bps)
        limits(1_000, 1_024 * 1_024),
        PersonalWorkerPriority::Normal,
    );
    assert_eq!(cliff.effective_service_time_ms, 300);
    assert_eq!(cliff.utilization_bps, 8_400);
    assert_eq!(cliff.admission, PoolAdmissionDecision::HoldUtilizationCliff);
}

#[test]
fn memory_bound_enforces_byte_backpressure_with_single_item_liveness() {
    let mut gate = MemoryBound::new(4_096).expect("positive ceiling");
    // Single item larger than ceiling is admitted when pool is empty (in_flight_items == 0).
    assert!(gate.try_acquire(8_192));
    assert!(!gate.try_acquire(1));
    gate.release(8_192);

    assert!(gate.try_acquire(2_048));
    assert!(gate.try_acquire(2_048));
    assert!(!gate.try_acquire(1));
    gate.release(2_048);
    assert!(gate.try_acquire(1_024));
}

#[test]
fn worker_pool_kernel_solver_respects_kleinrock_aging_and_replay_determinism() {
    let mut cfg = WorkerPoolConfig::default_personal_worker(2);
    cfg.priority_aging_window_ms = 1_000;
    let solver = WorkerPoolKernelSolver::new(cfg).expect("valid solver");
    let observed_at = EpochMillis::new(5_000).expect("observed_at");

    let tasks = vec![
        // Submitted at t=1000 as Background -> aged 4000ms (4 windows) -> promoted to rank 2 (Interactive)
        KernelSolverTask {
            request_id: ExecutionRequestId::parse("aged-bg-task").expect("id"),
            family_key: "family-alpha".to_owned(),
            priority: PersonalWorkerPriority::Background,
            limits: limits(1_000, 1_024),
            submitted_at: EpochMillis::new(1_000).expect("t"),
            payload: 7_u64,
        },
        // Submitted at t=4500 as Normal -> aged 500ms (0 windows) -> stays rank 1 (Normal)
        KernelSolverTask {
            request_id: ExecutionRequestId::parse("fresh-normal-task").expect("id"),
            family_key: "family-beta".to_owned(),
            priority: PersonalWorkerPriority::Normal,
            limits: limits(1_000, 1_024),
            submitted_at: EpochMillis::new(4_500).expect("t"),
            payload: 11_u64,
        },
    ];

    let order = solver.schedule_order(&tasks, observed_at);
    // Aged background task (rank 2) must be scheduled ahead of fresh normal task (rank 1).
    assert_eq!(order, vec![0, 1]);

    let report_a = solver
        .solve_batch(tasks.clone(), observed_at, 2_000, |t| {
            (t.payload * t.payload, 100)
        })
        .expect("batch a");
    let report_b = solver
        .solve_batch(tasks, observed_at, 2_000, |t| (t.payload * t.payload, 100))
        .expect("batch b");

    assert_eq!(report_a.batch_receipt_digest, report_b.batch_receipt_digest);
    assert_eq!(report_a.solutions.len(), 2);
    assert_eq!(report_a.solutions[0].request_id.as_str(), "aged-bg-task");
    assert_eq!(report_a.solutions[0].output, 49);
    assert_eq!(
        report_a.solutions[1].request_id.as_str(),
        "fresh-normal-task"
    );
    assert_eq!(report_a.solutions[1].output, 121);
}
