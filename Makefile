# Glaeda Makefile — enforces host ceres -j 2 throttling rule for estate targets
JOBS ?= 2
CARGO ?= cargo

.PHONY: all build release check test test-solver fmt clippy module-inventory threat-model-check spec-check smoke work work-cmux work-cmux-setup work-tmux vm vm-create vm-bootstrap vm-check vm-up vm-tmux vm-status vm-sync vm-doctor vm-observe vm-stop quarry-runner-install quarry-runner-status quarry-runner-route quarry-runner-unroute quarry-runner-pause quarry-runner-resume quarry-runner-remove quarry-runner-autoidle-enable quarry-runner-autoidle-disable quarry-runner-autoidle-status

all: check test

build:
	$(CARGO) build -j $(JOBS) --locked

release:
	$(CARGO) build -j $(JOBS) --locked --release

check:
	$(CARGO) check -j $(JOBS) --locked --all-targets

fmt:
	$(CARGO) fmt --all -- --check

clippy:
	$(CARGO) clippy -j $(JOBS) --locked --all-targets -- -D warnings

test:
	$(CARGO) test -j $(JOBS) --locked

test-solver:
	$(CARGO) test -j $(JOBS) --locked --lib worker_pool_kernel_solver
	$(CARGO) test -j $(JOBS) --locked --lib cortex_revenant_bridge
	$(CARGO) test -j $(JOBS) --locked --test worker_pool_kernel_solver_contract
	$(CARGO) test -j $(JOBS) --locked --test cortex_revenant_bridge_contract

module-inventory:
	GLAEDA_WRITE_MODULE_INVENTORY=1 $(CARGO) test -j $(JOBS) --locked --test module_inventory

threat-model-check:
	$(CARGO) test -j $(JOBS) --locked --test threat_model_enforcement_bindings

spec-check:
	@test -f specs/README.md
	@test -f specs/0001-trust-tiered-hot-runtime-and-workload-identity.md
	@test -f specs/0002-littles-law-dbr-worker-pool-kernel-solver.md
	@test -f specs/0003-cortex-revenant-lane-and-mesh-pool-bridge.md
	@test -f .agents/persona.md
	@echo "specs verified"

smoke: test-solver threat-model-check spec-check

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
