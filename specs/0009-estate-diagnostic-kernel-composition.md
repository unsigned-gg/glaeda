---
status: completed
created: 2026-10-16
priority: high
tags:
  - estate
  - diagnostics
  - performance
  - module-inventory
---

# Spec 0009: Estate Diagnostic Kernel Composition (`rust_memory_diagnostic` + `hot_fleet_latency_summary`)

## Overview

`src/rust_memory_diagnostic.rs` (`classify_rust_memory`) and `src/hot_fleet_latency_summary.rs` (`compare_hot_fleet_abba_with_latency`) are currently exported with `Reach::None` in `docs/MODULE_INVENTORY.md` (contributing 2 of the 34 unreferenced exports under `UNREFERENCED_EXPORT_CEILING`).

This spec composes both advisory diagnostic kernels into the `glaeda` (`aegis`) estate control plane (`src/estate_http_admission.rs`), promoting both modules from `Reach::None` to `Reach::Binary` and lowering `UNREFERENCED_EXPORT_CEILING` in `tests/module_inventory.rs` from `34` to `32`.

## Requirements

1. **Advisory Diagnostic Wrappers in `src/estate_http_admission.rs`**:
   - Export `evaluate_estate_rust_memory_diagnostic`:
     ```rust
     pub fn evaluate_estate_rust_memory_diagnostic(
         input: RustMemoryDiagnosticInput,
     ) -> Result<RustMemoryDiagnosticReport, EstateHttpAdmissionError>
     ```
     Calls `crate::rust_memory_diagnostic::classify_rust_memory(input)` and maps validation errors to `EstateHttpAdmissionError::new("memory_diagnostic", "rust_memory_diagnostic_invalid", "Rust memory diagnostic input failed validation")`.
   - Export `evaluate_estate_hot_fleet_latency`:
     ```rust
     pub fn evaluate_estate_hot_fleet_latency(
         windows: [&HotFleetWindowReceiptV1; 4],
     ) -> Result<HotFleetLatencyComparisonReportV1, EstateHttpAdmissionError>
     ```
     Calls `crate::hot_fleet_latency_summary::compare_hot_fleet_abba_with_latency(windows)` and maps comparability refusals to `EstateHttpAdmissionError::new("hot_fleet_latency", "hot_fleet_comparison_refused", "hot-fleet A-B-B-A windows failed comparability validation")`.
   - Preserve the universal invariant that diagnostic outputs carry advisory authority only and perform no side effects.

2. **Module Inventory Ratchet (`34` -> `32`)**:
   - Lower `UNREFERENCED_EXPORT_CEILING` in `tests/module_inventory.rs` from `34` to `32`.
   - Regenerate `docs/MODULE_INVENTORY.md` via `GLAEDA_WRITE_MODULE_INVENTORY=1 cargo test -j 2 --locked --test module_inventory`.

## Verification

- Unit tests in `src/estate_http_admission.rs` covering `evaluate_estate_rust_memory_diagnostic` and `evaluate_estate_hot_fleet_latency`.
- Negative test control: temporarily return `Err(...)` unconditionally from `evaluate_estate_rust_memory_diagnostic` and observe unit test failure before restoring.
- Verify `cargo test -j 2 --locked --test module_inventory` passes with `UNREFERENCED_EXPORT_CEILING = 32`.
- `make smoke` and `make fmt` with `-j 2`.
