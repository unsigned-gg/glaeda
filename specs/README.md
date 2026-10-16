# Glaeda Specifications (LeanSpec)

This directory tracks architectural specifications for `glaeda` in the estate platform runtime tier, grounded in the Cortex multi-agent queueing, memory-bound backpressure, and sandboxed hot-runtime research.

## Index

| Spec | Status | Title | Primary Modules |
|------|--------|-------|-----------------|
| [0001](./0001-trust-tiered-hot-runtime-and-workload-identity.md) | Active | Trust-Tiered Hot Runtime, Workload Identity, and Receipt Verification | `src/hot_runtime_pool.rs`, `src/workload_identity.rs`, `src/linux_isolation.rs`, `src/receipt_verification.rs` |
| [0002](./0002-littles-law-dbr-worker-pool-kernel-solver.md) | Active | Little's Law, Drum-Buffer-Rope, and Byte-Weighted Worker-Pool Kernel Solver | `src/worker_pool_kernel_solver.rs`, `src/personal_worker_queue.rs`, `src/execution_admission.rs` |
| [0003](./0003-cortex-revenant-lane-and-mesh-pool-bridge.md) | Active | Cortex + Revenant Lane & Mesh-Pool Execution Bridge | `src/cortex_revenant_bridge.rs`, `src/worker_pool_kernel_solver.rs`, `src/personal_worker_queue.rs` |
| [0004](./0004-http-health-and-admission-surface.md) | Active | Bounded HTTP/1.1 `/v1/health` and Admission Surface | `src/estate_http_admission.rs`, `src/cortex_revenant_bridge.rs` |
| [0005](./0005-estate-serve-cli-subcommand.md) | Active | `glaeda estate-serve` CLI Subcommand | `src/main.rs`, `src/estate_http_admission.rs`, `src/cortex_revenant_bridge.rs` |
| [0006](./0006-estate-admission-gate-and-route-alias.md) | Completed | Estate Admission Gate and `/v1/admission/evaluate` Route Alias | `src/estate_http_admission.rs`, `src/execution_admission.rs`, `src/personal_worker_queue.rs` |
| [0007](./0007-estate-prometheus-metrics-surface.md) | Completed | Estate Prometheus `/metrics` Scrape Surface | `src/estate_http_admission.rs`, `src/worker_pool_kernel_solver.rs` |
| [0008](./0008-estate-durable-execution-receipt-composition.md) | Completed | Estate Durable Execution Receipt Composition | `src/estate_http_admission.rs`, `src/execution_receipt.rs`, `src/execution_receipt_store.rs` |
| [0009](./0009-estate-diagnostic-kernel-composition.md) | Completed | Estate Diagnostic Kernel Composition (`rust_memory_diagnostic` + `hot_fleet_latency_summary`) | `src/estate_http_admission.rs`, `src/rust_memory_diagnostic.rs`, `src/hot_fleet_latency_summary.rs` |
| [0010](./0010-estate-failure-diagnostic-and-remediation-composition.md) | Completed | Estate Failure Diagnostic and Operator Remediation Composition (`failure_diagnostic` + `operator_remediation`) | `src/estate_http_admission.rs`, `src/failure_diagnostic.rs`, `src/operator_remediation.rs` |
| [0011](./0011-estate-rust-tooling-and-config.md) | Completed | Estate Rust Tooling and Supply-Chain Configuration (`.cargo/config.toml`, `clippy.toml`, `deny.toml`) | `.cargo/config.toml`, `clippy.toml`, `deny.toml` |
| [0012](./0012-estate-xtask-and-gitbutler-stack-workflow.md) | Completed | Estate `xtask` Automation, `xwork` Pipeline, and GitButler Stacked-PR Workflow | `xtask/src/main.rs`, `xtask/src/contract.rs`, `xtask/src/stack.rs`, `Makefile` |
