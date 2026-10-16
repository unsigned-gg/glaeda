# Spec 0011: Estate Rust Tooling and Supply-Chain Configuration

- **Status:** Completed
- **Primary Files:** `.cargo/config.toml`, `clippy.toml`, `deny.toml`, `Makefile`
- **Related Specs:** [0001](./0001-trust-tiered-hot-runtime-and-workload-identity.md), [0012](./0012-estate-xtask-and-gitbutler-stack-workflow.md)

## Context & Problem

Every Rust repository in the estate platform runtime tier (`revenant`, `vilicus`, `saga`, `reverie`, `rina`) enforces uniform host-throttling (`jobs = 2` on `ceres`), Clippy thresholds, and `cargo-deny` supply-chain and license checks. `glaeda` pinned `rust-toolchain.toml` and `rustfmt.toml`, but lacked `.cargo/config.toml`, `clippy.toml`, and `deny.toml`.

## Architecture & Guarantees

1. **Cargo Host Throttling & Aliases (`.cargo/config.toml`)**:
   - Caps default parallel `rustc` jobs to `jobs = 2` (`[build]`) to prevent memory exhaustion on shared estate hosts.
   - Enables sparse `crates.io` index protocol and CLI git fetching.
   - Declares `cargo xtask` and `cargo xwork` aliases for workspace automation.
2. **Clippy Configuration (`clippy.toml`)**:
   - Standardizes `too-many-arguments-threshold = 8`, `type-complexity-threshold = 300`, `allow-unwrap-in-tests = true`, and `allow-expect-in-tests = true`.
3. **Supply-Chain & License Gate (`deny.toml`)**:
   - Enforces `yanked = "deny"`, zero ignored advisories (`ignore = []`), permissive SPDX license allowlist, `wildcards = "deny"`, and `unknown-registry = "deny"` / `unknown-git = "deny"` across Linux (`x86_64`/`aarch64`) and macOS (`aarch64`) targets.
