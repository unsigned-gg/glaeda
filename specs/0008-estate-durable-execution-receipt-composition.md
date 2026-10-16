---
status: completed
created: 2026-10-16
priority: high
tags:
  - estate
  - receipts
  - persistence
  - module-inventory
---

# Spec 0008: Durable Execution Receipt Store Composition

## Overview

`src/execution_receipt_store.rs` provides atomic publication (`publish_execution_receipt`) and canonical readback (`read_execution_receipt`) for external execution receipts (`ExecutionReceipt` v1), but is currently reached only by `tests/` (`Reach::Tests` in `docs/MODULE_INVENTORY.md`).

This spec composes `execution_receipt_store` into the `glaeda` (`aegis`) estate control plane (`src/estate_http_admission.rs`) via `publish_and_verify_estate_execution_receipt`, promoting `execution_receipt_store` from `Reach::Tests` to `Reach::Binary` in `docs/MODULE_INVENTORY.md`.

## Requirements

1. **Durable Receipt Publication + Readback Verification**:
   - Export `publish_and_verify_estate_execution_receipt` in `src/estate_http_admission.rs`:
     ```rust
     pub fn publish_and_verify_estate_execution_receipt<S: StateStore>(
         store: &mut S,
         installation_id: &InstallationId,
         receipt: &ExecutionReceipt,
     ) -> Result<(ExecutionReceiptPublication, ExecutionReceipt), EstateHttpAdmissionError>
     ```
   - Call `crate::execution_receipt_store::publish_execution_receipt(store, installation_id, receipt)` and immediately verify round-trip readback via `crate::execution_receipt_store::read_execution_receipt(store, installation_id, receipt.execution_id())`.
   - Enforce that first publication returns `ExecutionReceiptPublicationDisposition::Created`, exact replay returns `Duplicate`, and conflicting content under the same `JournalId` fails closed with `receipt_store_conflict`.

2. **Module Inventory Promotion**:
   - Regenerate `docs/MODULE_INVENTORY.md` so `execution_receipt_store` advances from `reach: tests` (1 consumer file) to `reach: binary` (2 consumer files).

## Verification

- Unit test in `src/estate_http_admission.rs` covering `Created`, `Duplicate` replay, and `Conflict` on divergent receipt semantics under the same `JournalId`.
- Negative test control: temporarily invert the readback equality assertion in `publish_and_verify_estate_execution_receipt` and observe unit test failure before restoring.
- Regenerate and verify `docs/MODULE_INVENTORY.md` via `GLAEDA_WRITE_MODULE_INVENTORY=1 cargo test -j 2 --locked --test module_inventory`.
- `make smoke` and `make fmt` with `-j 2`.
