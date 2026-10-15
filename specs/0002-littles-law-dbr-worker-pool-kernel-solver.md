---
status: active
created: 2026-10-15
tags:
  - queueing-theory
  - littles-law
  - kingman
  - drum-buffer-rope
  - memory-bound
  - worker-pool
priority: high
---

# 0002: Little's Law, Drum-Buffer-Rope, and Byte-Weighted Worker-Pool Kernel Solver

## Overview

Derived from the Cortex queueing and concurrency architecture (`specs/0035-cognitive-scheduler`, `specs/0037-Priority-Message-Classification`, `specs/0038-task-admission-and-wip-caps`, `specs/0039-byte-weighted-channel-bounds`, and Reverie observation `#37028`), this specification defines the deterministic worker-pool kernel solver (`src/worker_pool_kernel_solver.rs`) and its integration with `src/personal_worker_queue.rs` and `src/execution_admission.rs`.

## Mathematical Model

### 1. Little's Law ($L = \lambda W$)

For a stationary worker pool of $m$ parallel lanes with arrival rate $\lambda$ (tasks/sec), mean cycle time $W$ (sec), and average work-in-progress $L$:

$$L = \lambda W \implies W = \frac{L}{\lambda}$$

Unbounded $L$ at bounded service capacity drives cycle time $W$ linearly to infinity. The pool enforces an explicit Drum-Buffer-Rope (DBR) buffer cap $L_{\max}$ (`max_wip`).

### 2. Sweller Context-Switch Reload Penalty

When a worker lane switches between distinct task families (`family_key`), cognitive/state reload incurs an additive penalty $\Delta_{\text{reload}}$ (`context_switch_reload_ms`). For context-switch ratio $p_{\text{cs}} \in [0, 1]$:

$$\tau_{\text{eff}} = \tau_0 + p_{\text{cs}} \cdot \Delta_{\text{reload}}$$

### 3. Utilization and Kingman's $G/G/m$ Heavy-Traffic Approximation

Pool utilization across $m$ worker lanes is:

$$\rho = \frac{\lambda \cdot \tau_{\text{eff}}}{m}$$

Queue wait time $W_q$ is bounded by Kingman's heavy-traffic formula using arrival coefficient of variation $c_a$ and service coefficient of variation $c_s$:

$$W_q \approx \left(\frac{c_a^2 + c_s^2}{2}\right) \left(\frac{\rho^{\sqrt{2(m+1)} - 1}}{m(1 - \rho)}\right) \tau_{\text{eff}}$$

Because $\frac{\rho}{1 - \rho}$ diverges nonlinearly above $\rho = 0.80$ ($4\times$ penalty at $80\%$, $9\times$ at $90\%$), admission holds non-critical dispatches at `utilization_cliff_bps = 8000`.

### 4. Cobham Non-Preemptive Priority & Kleinrock Aging

Priority classes $p \in \{\text{Critical}, \text{High}, \text{Normal}, \text{Background}\}$ experience priority-conditioned wait times under Cobham's conservation law, while Kleinrock linear aging elevates effective priority after `priority_aging_window_ms` to prevent starvation of lower classes.

### 5. Byte-Weighted `MemoryBound` Admission (Cortex `specs/0039`)

Item count alone cannot bound physical RAM when task payloads vary in size. `MemoryBound` tracks both `in_flight_bytes` and `in_flight_items`:
- Admits if `in_flight_bytes + item_bytes <= max_bytes`, OR
- Admits a single oversized item when `in_flight_items == 0` (liveness guarantee preventing permanent deadlock).

## Implementation Surface

- **`src/worker_pool_kernel_solver.rs`**:
  - `MemoryBound`, `PoolAdmissionDecision`, `TaskPriorityClass`, `WorkerPoolConfig`, `WorkerPoolFlowMetrics`
  - `KernelSolverTask<P>`, `SolvedKernelItem<R>`, `WorkerLaneTelemetry`, `KernelBatchSolveReport<R>`
  - `WorkerPoolKernelSolver::solve_batch` (deterministic family-affinity + Kleinrock aging + `std::thread::scope` execution + SHA-256 lane/batch receipts)
- **`src/personal_worker_queue.rs`**:
  - `PersonalWorkerQueue::evaluate_queue_flow_metrics` bridges live queue depth and running leases into `WorkerPoolFlowMetrics::evaluate`.
- **`tests/worker_pool_kernel_solver_contract.rs`**:
  - Contract tests covering Little's Law identity, Kingman cliff gating, byte-weighted backpressure, Sweller family affinity, and deterministic SHA-256 receipts.
