# Glaeda Makefile — enforces host ceres -j 2 throttling rule for estate targets
JOBS ?= 2
CARGO ?= cargo

.DEFAULT_GOAL := help

.PHONY: help all build release check xwork xtask-check hygiene toolchain-check branch-hygiene stack but-status test test-solver fmt fmt-check clippy deny machete audit typos tools ci install module-inventory threat-model-check spec-check smoke work work-cmux work-cmux-setup work-tmux vm vm-create vm-bootstrap vm-check vm-up vm-tmux vm-status vm-sync vm-doctor vm-observe vm-stop quarry-runner-install quarry-runner-status quarry-runner-route quarry-runner-unroute quarry-runner-pause quarry-runner-resume quarry-runner-remove quarry-runner-autoidle-enable quarry-runner-autoidle-disable quarry-runner-autoidle-status

##@ General

help: ## Show this help table
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-26s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

all: check test ## Run check and test

##@ Build & Estate Verification (xtask / xwork)

build: ## Build glaeda binary (locked, -j 2)
	$(CARGO) build -j $(JOBS) --locked

release: ## Build release glaeda binary (locked, -j 2)
	$(CARGO) build -j $(JOBS) --locked --release

check: ## Check all targets in root package (locked, -j 2)
	$(CARGO) check -j $(JOBS) --locked --all-targets

xwork: ## Run deterministic FNV-1a worktree-fingerprinted xtask verification pipeline
	$(CARGO) xwork

xtask-check: ## Run full xtask check pipeline (hygiene + fmt-check + deny + clippy)
	$(CARGO) xtask check

hygiene: ## Run static hygiene gates (spec-check, toolchain-check, branch-hygiene)
	$(CARGO) xtask hygiene

toolchain-check: ## Verify rust-toolchain.toml, clippy.toml, deny.toml, and .cargo/config.toml
	$(CARGO) xtask toolchain-check

branch-hygiene: ## Report local branches already merged into origin/unsigned
	$(CARGO) xtask branch-hygiene

##@ Stacked PRs & GitButler (but)

stack: ## Show stacked-PR status, parent base-ref, and GitButler workspace status
	$(CARGO) xtask stack status

but-status: ## Run GitButler CLI status (`but status`) via xtask stack
	$(CARGO) xtask stack but status

##@ Linting, Formatting & Supply Chain

fmt: ## Check Rust formatting across workspace
	$(CARGO) fmt --all -- --check

fmt-check: ## Check Rust formatting via xtask
	$(CARGO) xtask fmt-check

clippy: ## Run Clippy across all targets (-D warnings, -j 2)
	$(CARGO) clippy -j $(JOBS) --locked --all-targets -- -D warnings

deny: ## Run cargo-deny supply-chain and license checks
	$(CARGO) deny check

machete: ## Check for unused Cargo dependencies with cargo-machete
	$(CARGO) xtask machete

audit: ## Run cargo-audit against RustSec advisory database
	$(CARGO) audit

typos: ## Spell-check the repository with typos-cli
	$(CARGO) xtask typos

tools: ## Install pinned estate gate tools (cargo-deny, typos-cli, cargo-machete, cargo-nextest)
	$(CARGO) xtask tools

ci: ## Run the estate xtask contract v1 CI gate sequence
	$(CARGO) xtask ci

install: ## Run estate xtask ci and build locked release binaries
	$(CARGO) xtask install

##@ Testing & Contracts

test: ## Run Cargo test suite (locked, -j 2)
	$(CARGO) test -j $(JOBS) --locked

test-solver: ## Run focused estate kernel solver, bridge, HTTP admission, and xtask tests
	$(CARGO) test -j $(JOBS) --locked --lib worker_pool_kernel_solver
	$(CARGO) test -j $(JOBS) --locked --lib cortex_revenant_bridge
	$(CARGO) test -j $(JOBS) --locked --lib estate_http_admission
	$(CARGO) test -j $(JOBS) --locked --bin glaeda estate_serve
	$(CARGO) test -j $(JOBS) --locked --test worker_pool_kernel_solver_contract
	$(CARGO) test -j $(JOBS) --locked --test cortex_revenant_bridge_contract
	$(CARGO) test -j $(JOBS) --locked --test estate_http_admission_contract
	$(CARGO) test -j $(JOBS) --locked -p xtask

module-inventory: ## Verify or regenerate docs/MODULE_INVENTORY.md
	GLAEDA_WRITE_MODULE_INVENTORY=1 $(CARGO) test -j $(JOBS) --locked --test module_inventory

threat-model-check: ## Verify threat-model enforcement bindings
	$(CARGO) test -j $(JOBS) --locked --test threat_model_enforcement_bindings

spec-check: ## Verify LeanSpec files, index, relative links, and toolchain configs
	@test -f specs/README.md
	@test -f specs/0001-trust-tiered-hot-runtime-and-workload-identity.md
	@test -f specs/0002-littles-law-dbr-worker-pool-kernel-solver.md
	@test -f specs/0003-cortex-revenant-lane-and-mesh-pool-bridge.md
	@test -f specs/0004-http-health-and-admission-surface.md
	@test -f specs/0005-estate-serve-cli-subcommand.md
	@test -f specs/0006-estate-admission-gate-and-route-alias.md
	@test -f specs/0007-estate-prometheus-metrics-surface.md
	@test -f specs/0008-estate-durable-execution-receipt-composition.md
	@test -f specs/0009-estate-diagnostic-kernel-composition.md
	@test -f specs/0010-estate-failure-diagnostic-and-remediation-composition.md
	@test -f specs/0011-estate-rust-tooling-and-config.md
	@test -f specs/0012-estate-xtask-and-gitbutler-stack-workflow.md
	@test -f .cargo/config.toml
	@test -f clippy.toml
	@test -f deny.toml
	@test -f .agents/persona.md
	@$(CARGO) xtask spec-check
	@echo "specs verified"

smoke: test-solver threat-model-check spec-check ## Run fast estate smoke verification suite

##@ Workspace & VM Operations

work:
	@bash scripts/macbook-workspace.sh tmux

work-cmux:
	@bash scripts/macbook-workspace.sh cmux

work-cmux-setup:
	@bash scripts/macbook-workspace.sh setup-cmux

work-tmux:
	@bash scripts/macbook-workspace.sh tmux

vm:
	@bash scripts/macbook-runner-vm.sh shell

vm-create:
	@bash scripts/macbook-runner-bootstrap.sh create

vm-bootstrap:
	@bash scripts/macbook-runner-bootstrap.sh bootstrap

vm-check:
	@bash scripts/macbook-runner-bootstrap.sh check

vm-up:
	@bash scripts/macbook-runner-vm.sh up

vm-tmux:
	@bash scripts/macbook-runner-vm.sh tmux

vm-status:
	@bash scripts/macbook-runner-vm.sh status

vm-sync:
	@bash scripts/macbook-runner-vm.sh sync

vm-doctor:
	@status=0; bash scripts/macbook-runner-vm.sh doctor || status=$$?; \
	  bash scripts/macbook-workspace.sh notify-doctor "$$status" || true; \
	  exit "$$status"

vm-observe:
	@bash scripts/macbook-runner-vm.sh observe

vm-stop:
	@bash scripts/macbook-runner-vm.sh stop

quarry-runner-install:
	@bash scripts/quarry-trusted-runner.sh install

quarry-runner-status:
	@bash scripts/quarry-trusted-runner.sh status

quarry-runner-route:
	@bash scripts/quarry-trusted-runner.sh route

quarry-runner-unroute:
	@bash scripts/quarry-trusted-runner.sh unroute

quarry-runner-pause:
	@bash scripts/quarry-trusted-runner.sh pause

quarry-runner-resume:
	@bash scripts/quarry-trusted-runner.sh resume

quarry-runner-remove:
	@bash scripts/quarry-trusted-runner.sh remove

quarry-runner-autoidle-enable:
	@bash scripts/quarry-trusted-runner.sh autoidle-enable

quarry-runner-autoidle-disable:
	@bash scripts/quarry-trusted-runner.sh autoidle-disable

quarry-runner-autoidle-status:
	@bash scripts/quarry-trusted-runner.sh autoidle-status
