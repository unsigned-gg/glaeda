# LeanSpec 0005 — `glaeda estate-serve` CLI Subcommand

- **Status**: Active
- **Owner**: `ctodie`
- **Repository**: `platform/runtime/glaeda`
- **Created**: 2026-10-15

## 1. Summary

Wires the `glaeda estate-serve` CLI subcommand in [`src/main.rs`](../src/main.rs) so `glaeda` (`estate/w4G:p1`, callsign `aegis`) can serve its bounded HTTP/1.1 `/v1/health` and `/v1/admission/evaluate` control plane ([`src/estate_http_admission.rs`](../src/estate_http_admission.rs)) directly from the compiled binary.

## 2. CLI Contract & Invariants

1. **Command Syntax**:
   - `glaeda [--output human|json] estate-serve --bind <ADDR> --max-requests <N> --machine <MACHINE> --session <SESSION> --svid-digest <SHA256> --svid-issued-at <MS> --svid-expires-at <MS> --godseat-lane <LANE> --godseat-public-key <PUBKEY> --observed-at <MS> [pool flags...]`
2. **Fail-Closed Preconditions**:
   - Rejects non-loopback `--bind` addresses via [`validate_loopback_bind_addr`](../src/estate_http_admission.rs) before opening a socket (`ExitCode::from(2)` with structured `RuntimeErrorReport` on `--output json`).
   - Validates SPIFFE ID (`spiffe://unsigned.gg/host/<machine>/lane/<session>`), delegated SVID window (`issued_at < expires_at`), Godseat lane principal (`lane.<name>@godseat.unsigned.gg`), and `WorkerPoolConfig` bounds before binding.
3. **Bounded Execution & Receipt**:
   - Serves exactly `--max-requests` HTTP/1.1 exchanges via [`serve_bounded_loopback_requests`](../src/estate_http_admission.rs) and emits a typed `EstateServeReceipt` (`schema_version = 1`, `bound_addr`, `served_requests`, `lane_spiffe_id`, `godseat_principal`).

## 3. Verification

- `cargo test -j 2 --locked --bin glaeda estate_serve`
- `make smoke`
