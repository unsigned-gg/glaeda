---
status: completed
created: 2026-10-16
priority: high
tags:
  - estate
  - diagnostics
  - remediation
  - module-inventory
---

# Spec 0010: Estate Failure Diagnostic and Operator Remediation Composition (`failure_diagnostic` + `operator_remediation`)

## Overview

`src/failure_diagnostic.rs` (`diagnose_failure`) and `src/operator_remediation.rs` (`OperatorRemediationCandidate`) are currently exported with `Reach::None` in `docs/MODULE_INVENTORY.md` (contributing 2 of the 32 unreferenced exports under `UNREFERENCED_EXPORT_CEILING`).

This spec composes both advisory diagnostic and remediation kernels into the `glaeda` (`aegis`) estate control plane (`src/estate_http_admission.rs`), promoting both modules from `Reach::None` to `Reach::Binary` and lowering `UNREFERENCED_EXPORT_CEILING` in `tests/module_inventory.rs` from `32` to `30`.

## Requirements

1. **Advisory Failure Diagnostic and Remediation Wrappers in `src/estate_http_admission.rs`**:
   - Export `evaluate_estate_failure_diagnostic`:
     ```rust
     pub fn evaluate_estate_failure_diagnostic(
         evidence: &FailureEvidence,
         probe: Option<ProbeObservation>,
     ) -> Result<FailureDiagnosis, EstateHttpAdmissionError>
     ```
     Calls `crate::failure_diagnostic::diagnose_failure(evidence, probe)` and maps validation errors to `EstateHttpAdmissionError::new("failure_diagnostic", "failure_diagnostic_invalid", "failure diagnostic evidence or probe observation failed validation")`.
   - Export `evaluate_estate_operator_remediation`:
     ```rust
     #[allow(clippy::too_many_arguments)]
     pub fn evaluate_estate_operator_remediation(
         source_error: OperatorErrorCode,
         action_id: impl Into<String>,
         summary: impl Into<String>,
         confidence: RemediationConfidence,
         safety: RemediationSafety,
         applicability: RemediationApplicability,
         repair_budget_units: u16,
         checkpoint_required: bool,
         fresh_verification_required: bool,
         circuit_breaker_required: bool,
         required_evidence: impl IntoIterator<Item = impl Into<String>>,
     ) -> Result<OperatorRemediationCandidate, EstateHttpAdmissionError>
     ```
     Calls `crate::operator_remediation::OperatorRemediationCandidate::new(...)` and maps validation refusals to `EstateHttpAdmissionError::new("operator_remediation", "operator_remediation_invalid", "operator remediation candidate failed policy or safety validation")`.
   - Preserve the universal invariant that diagnostic and remediation candidate outputs carry advisory authority only (`authorizes_mutation() == false`) and perform no side effects.

2. **Module Inventory Ratchet (`32` -> `30`)**:
   - Lower `UNREFERENCED_EXPORT_CEILING` in `tests/module_inventory.rs` from `32` to `30`.
   - Regenerate `docs/MODULE_INVENTORY.md` via `GLAEDA_WRITE_MODULE_INVENTORY=1 cargo test -j 2 --locked --test module_inventory`.

## Verification

- Unit tests in `src/estate_http_admission.rs` covering `evaluate_estate_failure_diagnostic` and `evaluate_estate_operator_remediation` (both positive and negative cases).
- Negative test control: temporarily break `evaluate_estate_failure_diagnostic` to return `Err(...)` unconditionally and observe unit test failure before restoring.
- Verify `cargo test -j 2 --locked --test module_inventory` passes with `UNREFERENCED_EXPORT_CEILING = 30`.
- `make smoke` and `make fmt` with `-j 2`.
