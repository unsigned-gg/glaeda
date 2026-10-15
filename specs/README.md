# Glaeda Specifications (LeanSpec)

This directory tracks architectural specifications for `glaeda` in the estate platform runtime tier, grounded in the Cortex multi-agent queueing, memory-bound backpressure, and sandboxed hot-runtime research.

## Index

| Spec | Status | Title | Primary Modules |
|------|--------|-------|-----------------|
| [0001](./0001-trust-tiered-hot-runtime-and-workload-identity.md) | Active | Trust-Tiered Hot Runtime, Workload Identity, and Receipt Verification | `src/hot_runtime_pool.rs`, `src/workload_identity.rs`, `src/linux_isolation.rs`, `src/receipt_verification.rs` |
| [0002](./0002-littles-law-dbr-worker-pool-kernel-solver.md) | Active | Little's Law, Drum-Buffer-Rope, and Byte-Weighted Worker-Pool Kernel Solver | `src/worker_pool_kernel_solver.rs`, `src/personal_worker_queue.rs`, `src/execution_admission.rs` |
| [0003](./0003-cortex-revenant-lane-and-mesh-pool-bridge.md) | Active | Cortex + Revenant Lane & Mesh-Pool Execution Bridge | `src/cortex_revenant_bridge.rs`, `src/worker_pool_kernel_solver.rs`, `src/personal_worker_queue.rs` |
