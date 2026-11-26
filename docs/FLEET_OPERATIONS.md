# CMUX fleet operations

This is the operator front door for the CMUX Mac fleet. Use the `fleet` wrapper
from cmuxterm-hq for host operations, and use the Glaeda commands for typed
observation, reconciliation, disk receipts, and seed decisions. Plans are
read-only unless a command ends with `--apply` or `--yes`.

## Which source owns which fact

| Fact | Source of truth | Read surface |
| --- | --- | --- |
| Host roles, hardware, labels, availability, and routing | cmuxterm-hq `build-fleet/mini-fleet.json` | `glaeda-mini-fleet` manifest and `fleet runner status` |
| What a host actually reports | the host probe | `glaeda-mini-fleet observe` and `glaeda-fleet-status` |
| Current worker, queue, and controller state | the controller status endpoint | `build-fleet/status.py --json` or its saved JSON |
| Installed Glaeda generation | the host's OTA receipt | `glaeda-fleet-status` tooling source and `fleet up` |
| Disk pressure and reclaim receipts | the host's `glaeda-disk` state | `glaeda-disk --json` and `glaeda-fleet-status` disk source |
| Seed usefulness and transfer cost | job records and seed receipts | `glaeda-seed-prefetch`, seed logs, and build receipts |
| Dashboard presentation | ci-dash's deployed data document | the dashboard version and role metadata in its snapshot |

If two surfaces disagree, refresh the observation first. Do not repair a host
from a stale dashboard row or from a copied manifest that is not the current
cmuxterm-hq `main` manifest.

## One bounded status pass

For a human-readable report with host checks, disk, recent job contention, and
installed-tooling age:

```bash
python3 scripts/glaeda-fleet-status --output text
```

For a replayable document and page:

```bash
python3 scripts/glaeda-fleet-status collect --sources-out fleet-sources.json
python3 scripts/glaeda-fleet-status build fleet-sources.json --out fleet-status.json --html fleet-status.html
```

The collector is read-only apart from the normal `glaeda-disk` size snapshot.
It does not query GitHub for runner or queue state unless
`--github-runners` or `--queue-repo` is passed. The default manifest copy may
perform the one-hour cached comparison with cmuxterm-hq `main`; pass an explicit
`--manifest` or use the saved sources bundle when investigating a known
incident instead of repeatedly fetching provider state.

## Host and runner checks

Run the cmuxterm-hq operator wrapper for the live fleet view:

```bash
fleet --no-fetch up all
fleet --no-fetch runner status HOST...
fleet --no-fetch xcode-pin-audit HOST...
```

`runner status` compares registered labels with the labels Glaeda main would
register from the manifest. It is the read-only prerequisite to:

```bash
fleet runner relabel HOST --apply --wait
```

The relabel command waits for current jobs and re-registers the existing runner
in place. It does not change workflow routing variables.

AWS and other routed hosts must be reached through the manifest route. Do not
replace that route with direct SSH, a copied key, or a guessed login user.

## Cleanup and disk policy

Always inspect both layers. The cmuxterm-hq sweep covers older broad categories;
Glaeda owns the current per-family safety checks and receipts.

```bash
fleet --no-fetch cruft-sweep --all
fleet --no-fetch disk-policy --all
ssh HOST '~/.local/bin/glaeda-disk --pressure --idle --json'
```

With `--json`, measurement or eviction lock contention emits a JSON report with
`status: deferred`, `complete: false`, and exit code 3. Retry later; this is not
a completed scan or evidence of zero reclaimable bytes. Text mode retains its
successful skip behavior for existing maintenance hooks.

The first two commands plan by default. Apply only the host-specific reclaim
that the fresh receipt identifies as rebuildable:

```bash
ssh HOST '~/.local/bin/glaeda-disk --pressure --idle --apply'
```

The LaunchAgent's background `--refresh` pass is wall-clock bounded by its
`--refresh-budget-s` value (normally ten minutes). If the process-evidence
probe or a candidate walk overruns, the owned refresh exits cleanly, closes its
measurement and refresh locks, and leaves measurements committed before the
timeout in the atomic snapshot for the next pass. A foreground report or apply is never interrupted
by this background-only deadline.

On fleet minis, simulator devices are judged one at a time on every pressure pass and idle sweep
(family `simulator-device-data`). glaeda-disk runs `xcrun simctl erase` on a device, never
`delete`, when all of these hold:
- simctl lists it as shut down and available;
- it holds at least 512 MiB of data;
- its `lastBootedAt` is at least 12 h old;
- no running command names its UDID or its name;
- no capture lease under `~/.cache/cmux-capture-host` names its UDID, and every lease file can be
  read (an unreadable lease keeps every device);
- no process has a file open in it.

The device's name, type and runtime stay, so a job or lane that boots it by name or UDID still finds it. Each
device is judged again right before its erase, on fresh simctl, `ps` and open-file evidence and a fresh
reservation check, while glaeda-disk holds `capacity/simulator.token`, the token the runner hook gives every
simulator-class job for its whole run. So no such job is admitted between that check and the erase, and none is
erased under one that is running. simctl itself refuses to erase a device that is booting or booted. No new erase starts after five minutes,
after the idle sweep's busy check names a reason, or, on a pressure pass, once every filesystem is back above
its target. A reserved host keeps every device. If the size after an erase can't be read, the receipt counts
0 bytes. Receipts carry `family: simulator-device-data`.

Never remove active workspaces, current seeds, checkouts, user projects, or
unclassified space by hand. A low free-space number is a reason to refresh the
receipt, not permission to delete a directory.

Idle repair is queued on each installed host by `glaeda-idle-repair`. Its
LaunchAgent runs every minute, records a durable `queued` receipt while a job,
worker, reservation, or host lock is active, and runs the normal disk and
mini-health repair paths on the first idle tick. Inspect the queue with:

```bash
ssh HOST '~/.local/bin/glaeda-idle-repair --status'
```

The queue never kills a worker or clears a live lock. HQ's `fleet health
refresh` includes the queue receipt so the operator view can distinguish queued,
retrying, and idle repair state.

## Probe and dashboard rollout

The probe and dashboard have separate rollout steps:

```bash
fleet --no-fetch push-probe
fleet --no-fetch deploy ci-dash --dry-run
fleet --no-fetch autodeploy status DASHBOARD_HOST
```

After a dashboard or probe change lands, verify the deployed version in the
snapshot before interpreting new role, disk, or health rows. A role row carries
owner, purpose, group, capabilities, availability, and protected/excluded
state. It must not carry credentials, private paths, or SSH details.

## Seeds and shared state

Use seed telemetry before changing retention or cadence:

```bash
glaeda-seed-prefetch
fleet --no-fetch seed-archive --plan HOST...
```

Keep the trusted archive and per-root seed receipts separate from PR runner
state. A seed is useful when it reduces measured admission work for the same
toolchain and source lineage. Its presence alone is not evidence that it should
be retained.

## Linux VM base on a mini

`glaeda-linux-arm64` jobs run in one fresh Lima clone each (cmuxterm-hq#13261). Step one on a
std mini is the stopped clone source:

```bash
python3 scripts/glaeda-linux-vm-base            # plan, no side effects
python3 scripts/glaeda-linux-vm-base --apply    # idempotent; second run says unchanged
python3 scripts/glaeda-linux-vm-base --apply --clone-smoke   # one 4 vCPU / 8 GiB clone, verified, deleted
```

It installs the pinned Lima 2.2.0 release (size and SHA-256 checked) under
`~/.glaeda/linux-vm/lima-2.2.0`, uses `~/.glaeda/linux-vm/lima-home` as `LIMA_HOME`, and
creates `glaeda-linux-arm64-base` from the exact bytes of
`examples/lima/glaeda-prepared-template.yaml`. Readiness checks the guest's architecture, host
mounts, root-owned marker, runner integrity, non-sudo workload account and masked apt timers, and
the base is left stopped. An instance without the installer's `base.json` receipt, built from
another template, or broken is reported (exit 2), never replaced. A clone is
`limactl --tty=false clone glaeda-linux-arm64-base NAME --cpus 4 --memory 8 --mount-none --start`
with that `LIMA_HOME`; 3 to 4 fit on a 48 GB mini.

Step two is the installed disposable-worker service that clones that base per job
(`docs/DISPOSABLE_INSTALLED_SERVICE_ACCEPTANCE.md`), staged by `scripts/glaeda-linux-vm-serve`:

```bash
python3 scripts/glaeda-linux-vm-serve --apply      # operator: pinned Go 1.26.3, Rust 1.97.1, build + stage
fleet-sudo HOST "$PWD/scripts/glaeda-linux-vm-serve" install-root --from ~/.glaeda/linux-vm \
  --glaeda-sha256 X --bridge-sha256 Y --apply      # root: exact staged digests to /opt/smolrunner/bin, 0555
# once ~/.config/glaeda/scaleset-token.json is pushed:
~/.glaeda/linux-vm/serve/stage/scaleset-enroll --token-file ~/.config/glaeda/scaleset-token.json \
  --workflow ORG/REPO/.github/workflows/F.yml@refs/heads/main --apply > ~/.glaeda/linux-vm/serve/scaleset-enroll.json
python3 scripts/glaeda-linux-vm-serve finish --scale-set ~/.glaeda/linux-vm/serve/scaleset-enroll.json   # prints plan_identity
python3 scripts/glaeda-linux-vm-serve finish --scale-set ... --apply --approve-plan sha256:...
```

The operator run prints the exact `install-root` line with both digests. `finish` writes the
canonical schema-2 enrollment (`installation_token_file` credential, base VM as the clone source,
4 vCPU / 8 GiB / 20 GiB) to `~/.glaeda/linux-vm/serve/enrollment.json` at mode 0600, re-plans,
runs `service apply` only for the approved identity, and requires `service status` to read
`loaded_exact`. Paths under `/opt/smolrunner` and the `io.smolrunner.disposable-worker` label are
the code's existing contracts. A file it did not install is reported, never replaced.

## Linux VM rollout on every mini

`glaeda-linux-vm-rollout` brings a mini's whole tier up in one order: base, stage
(`glaeda-linux-vm-serve`), install-root, token, enroll, finish. Run it on the mini as the
operator, from the glaeda checkout to install; it plans by default and stops at the first step with
work left, printing `stopped_at` and the exact `next` commands:

```bash
W=manaflow-ai/cmux/.github/workflows/FILE.yml@refs/heads/main   # the runner group's list, repeated exactly
python3 scripts/glaeda-linux-vm-rollout --host HOST --workflow "$W"            # plan
python3 scripts/glaeda-linux-vm-rollout --host HOST --workflow "$W" --apply    # stops at install_root
fleet-sudo HOST .../glaeda-linux-vm-serve install-root ... --apply              # printed; the controller runs it
python3 scripts/glaeda-linux-vm-rollout --host HOST --workflow "$W" --apply    # enrolls, stops at approve
python3 scripts/glaeda-linux-vm-rollout --host HOST --workflow "$W" --apply --approve-plan ID   # serving
```

Each mini enrolls its own scale set, `glaeda-linux-arm64-HOST`, in the shared runner group, all
carrying the one job label `glaeda-linux-arm64`. A scale set's message session belongs to one
listener, and every listener sizes itself from the whole scale set's assigned-job count, so minis
cannot share one. The token step waits for the org-runners token push to reach the host. Each mini
advertises a maximum capacity of one clone today: the bridge target refuses any other value, and
the fleet host lock does not count VM clones yet, so a higher cap waits for that accounting.

## New minis: plugged in to taking jobs

One command from the operator Mac (cosmic-gray), after manifest-sync so the manifest is main's:

```bash
scripts/glaeda-mini-fleet manifest-sync --yes
scripts/glaeda-mini-bringup cmux25 cmux26 ... cmux34 --registry-out /tmp/new-minis        # dry run
scripts/glaeda-mini-bringup cmux25 cmux26 ... cmux34 --registry-out /tmp/new-minis --yes
```

To rehearse on a mini already in the fleet, run `scripts/glaeda-mini-bringup-dryrun [SHA|TREE] [MANIFEST]`
there: it treats that mini as new (a copy of the manifest without it) and prints the admit entry, the
runner labels and every bringup step, changing nothing.

At the screen first, per mini (no tool can do these): create the `cmux` admin user, log in to
Tailscale with a `tag:mac-mini-cluster` key (Lawrence owns the tag), name it `cmuxNN`
(`machines/host-naming.md`: the pool continues after cmux24), turn on Remote Login and Remote
Management, and add the fleet key. `reach` in the dry run lists every mini still missing one.

What it runs, each step a no-op once done, so a rerun continues:

| Step | Command | What it sets |
| --- | --- | --- |
| admit | `glaeda-mini-fleet admit HOST... --yes` | hardware class from the probe (a new chip/memory becomes `<chip>-<memory>`, such as `m5pro-64`), node ids, `std` class, dedicated, `ci-runner`, sudo mode, and a host Xcode override per complete Xcode installed beside the declared ones, so the runner carries `xcode-<v>` and `glaeda-std-xcode-<v>` for each |
| seed | `onboard SEED --acceptance node`, then `upgrade SEED --candidate-run RUN` | a new class's one accept-local and its class receipt (RUN defaults to the newest green `fleet-candidate.yml` on main) |
| onboard | `onboard REST --from SEED` | preflight, fix, sudo-plan, enroll on the receipt; Xcode and caches copy from the seed over the LAN |
| runners | `glaeda-cmux-runner-fleet --apply --hosts ALL` | one runner per host with its labels |
| check | `glaeda-mini-fleet check HOST...` | drift, the disk floor (`disk.min_free_gib`), runner state |

Disk: no extra APFS volume. Everything lives on the data volume (`/System/Volumes/Data`), the fleet
root at `/Users/Shared/cmux-build-fleet`, budgets in `config/disk-registry.toml`; admit notes a host
under the fleet floor.

Then the registry: copy `/tmp/new-minis/*.json` into leo-workspace `machines/fleet/machines/`, add
the names to `machines/fleet/role_holders/ci-runner.json`, land that PR, and run
`reach enroll cmuxNN --apply` per host (tailscale-self-heal). `--lane-host` adds a `lanes` budget
and `lane_tools` so the minis also host agent lanes. Finally, land the operator manifest's diff in
cmuxterm-hq `build-fleet/mini-fleet.json`.

## Runbook map

- [`FLEET_STATUS.md`](FLEET_STATUS.md) defines the typed status document and finding schema.
- [`HOST_RECONCILIATION.md`](HOST_RECONCILIATION.md) defines desired, observed, and planned host state.
- [`CMUX_FLEET_EXECUTION_ROLES.md`](CMUX_FLEET_EXECUTION_ROLES.md) defines workload roles and capabilities.
- [`CMUX_MINI_RUNNER.md`](CMUX_MINI_RUNNER.md) defines runner admission, hooks, labels, and seed behavior.
- cmuxterm-hq `skills/infra/fleet-ops/SKILL.md` maps incidents to one operator command.
- cmuxterm-hq `skills/infra/build-fleet/SKILL.md` defines build routing and fleet-only native validation.

The old `macfleet` skill and direct SSH build allocation path are retired. Keep
the compatibility files for callers, but do not use them for new operations.

## Headless fleet services are LaunchDaemons

A service the fleet depends on from a host with no one logged in (the fleet-cas store and its gc, the seed
archive, on cmux-lawrence) is a system LaunchDaemon: `/Library/LaunchDaemons/LABEL.plist`, `UserName` the service
user, `launchctl bootstrap system`. A LaunchAgent stops when the login session ends, which is what took the store
down at the 2026-11-20 reboot. `glaeda-fleet-cas store|uninstall` and `glaeda-seed-archive install|uninstall` do it
through `sudo -n` (or print the root commands for `fleet-sudo HOST ...`), boot out and remove an old gui/UID agent
first, and adopt a daemon with the same label that was installed by hand. CI-mini roles (runner, node, writer,
relay, prewarm) stay agents: they need the GUI session. `scripts/test-glaeda-headless-services.py` fails when a
store/server role is written as an agent or a new agent writer is added without a reason.
