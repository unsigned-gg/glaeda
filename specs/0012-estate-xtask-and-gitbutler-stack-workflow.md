# Spec 0012: Estate `xtask` Automation, `xwork` Pipeline, and GitButler Stacked-PR Workflow

- **Status:** Completed
- **Primary Files:** `xtask/Cargo.toml`, `xtask/src/main.rs`, `xtask/src/contract.rs`, `xtask/src/stack.rs`, `Cargo.toml`, `Makefile`
- **Related Specs:** [0001](./0001-trust-tiered-hot-runtime-and-workload-identity.md), [0011](./0011-estate-rust-tooling-and-config.md)

## Context & Problem

Estate repositories use a zero-dependency `xtask` automation crate (`cargo xtask` / `cargo xwork`) implementing the Estate `xtask` contract v1 (`tools`, `toolchain-check`, `fmt`, `fmt-check`, `clippy`, `test`, `doc-test`, `deny`, `typos`, `machete`, `ci`, `install`), a deterministic FNV-1a worktree-fingerprinted verification pipeline (`xwork`), and an in-process stacked-PR + GitButler (`but`) helper (`cargo xtask stack`).

At the same time, `glaeda` scripts (`./scripts/verify`, `./scripts/macbook-runner-bootstrap.sh`) invoke `cargo run --locked --quiet -- ...` at the repository root without `-p glaeda` and require `Cargo.lock` to remain minimal and `--locked`.

## Architecture & Guarantees

1. **Workspace Layout Preserving Default Root Binary**:
   - Root `Cargo.toml` defines `[workspace]` with `resolver = "2"`, `members = [".", "xtask"]`, and `default-members = ["."]` alongside `default-run = "glaeda"`.
   - `cargo run --locked --quiet -- ...` continues to execute `glaeda` directly while `cargo xtask` / `cargo xwork` target `-p xtask`.
2. **Zero-External-Dependency `xtask` (`#![forbid(unsafe_code)]`)**:
   - `xtask` depends only on Rust `std`, adding zero external crates to `Cargo.lock`.
   - `xtask/src/contract.rs` implements the Estate `cargo xtask` contract v1 (`CI_STEPS`, `TOOLS`, version parsing, and step reporting).
   - `xtask/src/main.rs` implements `xwork` (64-bit FNV-1a worktree state fingerprinting cached at `target/.xwork-state`), `spec-check` (validating `specs/NNNN-*.md`, `specs/README.md`, relative Markdown links, and `.agents/persona.md`), `toolchain-check`, and `branch-hygiene`.
3. **Stacked-PR & GitButler (`but`) Workflow (`xtask/src/stack.rs`)**:
   - Resolves `.git/HEAD`, `refs/heads/*`, `packed-refs`, and linked worktree `gitdir:` pointers in-process without spawning `git`.
   - Tracks stacked branch parents in `.git/stack/<branch>.base` with automatic trunk detection (`unsigned` on `unsigned-gg/glaeda`, fallback `main`).
   - Integrates with GitButler (`but`) CLI (`stack status`, `stack new`, `stack but`) and GitHub CLI (`stack pr`, `stack retarget`, `stack land`).
