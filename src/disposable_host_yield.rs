//! Host yield: a fleet Mac's own work comes before a disposable Linux worker's next job.
//!
//! Leo, 2026-11-19: "we should have all the minis potentially capable of hte \"linux\" jobs" and
//! "like autoscaling based on demands". Every fleet Mac may run the Linux worker, nightly hosts too;
//! the fleet controller sizes the slots from queue depth, and this reading is the host's veto.
//!
//! While any reason holds, the coordinator advertises zero Scale Set capacity, so the worker takes
//! no new job and starts no new clone. A clone that already has its job keeps it and finishes; this
//! boundary never stops, kills, or resizes anything. The facts are the ones the cmux runner hook
//! already keeps on the Mac (`scripts/glaeda-cmux-runner-hook`), read without taking any of them:
//!
//! - a nightly, promote or DEV job runs: a `Runner.Worker` whose runner install's
//!   `glaeda-hooks/job-started.sh` (the hook's `runner_scope`) names the nightly slot, the nightly
//!   signing runner or a dedicated builder (`--nightly-runner`, `--trusted-next-signing`,
//!   `--dedicated-admit`), a nightly promotion lease's runner (`--promotion-lease`), the DEV compile
//!   runner (`--dev-compile-runner`, the freshness slot: DEV compiles and the Release compile that
//!   gates the promotion) or a capture lease's runner (`--capture-lease`); or whose path names
//!   `nightly`;
//! - a macOS runner job waits for capacity: a fresh `capacity/admit.want-<pid>` marker (`ADMIT_WANT`)
//!   whose pid lives and that was rewritten within `ADMIT_FRESH_S` (180 s). Its `tier` (the hook's
//!   `job_tier`, FEED_FIRST) names a nightly or promote job (`nightly`) or a DEV feed build (`dev`);
//!   a nightly job freeing disk leaves one too (`NIGHTLY_DISK_WANT`). A job GitHub queued that no
//!   runner on this Mac has taken yet is not visible here; queue depth is the fleet controller's;
//! - the disk ledger is low: a `capacity/disk-low-<pid>` marker of a live pid (`DISK_LOW`: a job
//!   admitted under the runner disk floor still runs);
//! - free disk is under the hook's floor: the largest `--min-free-gib` of the runner installs
//!   (`actions-runner-glaeda*`), raised on a Mac with a dedicated builder to that builder's floor plus
//!   one job's growth (`NIGHTLY_DISK_RESERVE`: `builder_reserve_gib` + `DISK_JOB_PEAK_GIB`), read on
//!   the fleet directory's volume as the gate reads it;
//! - macOS memory pressure is above normal (`kern.memorystatus_vm_pressure_level` > 1);
//! - another build holds the fleet host lock exclusively (`host.lock`, probed with one non-blocking
//!   shared flock as the hook's `host_held` does), or a build or drain waits for it
//!   (`host.lock.waiters`, the hook's `registered_lock_waiters`: a fleet lease's drain, a lend or
//!   measure drain, a reserved build), or the host is reserved (`reservation.json`,
//!   `glaeda-reservation/v1`, active while now < until; a fleet lease writes it for its whole term).
//!
//! A missing fleet directory, ledger, lock, waiter directory, runner home or reservation yields
//! nothing from that source, so a Mac without the runner hook still runs workers. Any other read
//! failure yields: an unknown Mac is treated as a busy one.

use std::cell::Cell;
use std::fs;
use std::io::{self, Read as _};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rustix::fs::{FlockOperation, Mode, OFlags};
use rustix::process::Pid;
use serde::Serialize;

use crate::process::{CommandSpec, TimedCommandExecutor};

pub(crate) const DEFAULT_FLEET_DIR: &str = "/Users/Shared/cmux-build-fleet";
const FLEET_DIR_ENV: &str = "GLAEDA_FLEET_DIR";
/// The home whose `actions-runner-glaeda*` installs carry the runner floors (default `HOME`).
const RUNNER_HOME_ENV: &str = "GLAEDA_RUNNER_HOME";
const CAPACITY_DIR: &str = "capacity";
const ADMIT_WANT: &str = "admit.want-";
const ADMIT_FRESH_SECONDS: u64 = 180;
const DISK_LOW: &str = "disk-low-";
const HOST_LOCK: &str = "host.lock";
const HOST_LOCK_WAITERS: &str = "host.lock.waiters";
/// A polling waiter (`-refresh-` in its name) rewrites its marker within this (the hook's 30 s).
const LOCK_WAITER_FRESH_SECONDS: u64 = 30;
const LOCK_WAITER_DRAIN_KINDS: [&str; 3] = [
    "-refresh-lend_drain-",
    "-refresh-measure_drain-",
    "-refresh-capture_lease_drain-",
];
const MAX_LOCK_WAITER_ENTRIES: usize = 1_024;
const RESERVATION: &str = "reservation.json";
const RESERVATION_SCHEMA: &str = "glaeda-reservation/v1";
const RUNNER_HOOK_SCRIPT: &str = "glaeda-hooks/job-started.sh";
const RUNNER_INSTALL_PREFIX: &str = "actions-runner-glaeda";
const NIGHTLY_RUNNER_FLAGS: [&str; 3] = [
    "--nightly-runner",
    "--trusted-next-signing",
    "--dedicated-admit",
];
const PROMOTE_RUNNER_FLAGS: [&str; 1] = ["--promotion-lease"];
const DEV_RUNNER_FLAGS: [&str; 1] = ["--dev-compile-runner"];
const LEASE_RUNNER_FLAGS: [&str; 1] = ["--capture-lease"];
const DEDICATED_FLAG: &str = "--dedicated-admit";
const MIN_FREE_FLAG: &str = "--min-free-gib";
/// The hook's `DISK_JOB_PEAK_GIB`: one cmux job's measured disk growth, added to a dedicated
/// builder's floor for every other runner (`NIGHTLY_DISK_RESERVE`).
const DISK_JOB_PEAK_GIB: f64 = 20.0;
const MAX_FLOOR_GIB: f64 = 10_000.0;
const TIER_NIGHTLY: &str = "nightly";
const TIER_DEV: &str = "dev";
const RUNNER_WORKER: &str = "Runner.Worker";
const MAX_SMALL_FILE_BYTES: u64 = 64 * 1024;
const MAX_LEDGER_ENTRIES: usize = 4_096;
const MAX_RUNNER_INSTALLS: usize = 256;
const MAX_PROCESS_ROWS: usize = 16_384;
const MAX_COUNT: u16 = 999;
const SYSCTL_PROGRAM: &str = "/usr/sbin/sysctl";
const PS_PROGRAM: &str = "/bin/ps";
const COMMAND_TIMEOUT: Duration = Duration::from_secs(10);
/// One observation answers for this long: two `ps`/`sysctl` runs per idle second would be the
/// worker's own load on the Mac it yields to.
const OBSERVATION_REUSE: Duration = Duration::from_secs(5);

/// Why the worker takes no new job right now. Nightly jobs come first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "reason", rename_all = "snake_case")]
pub(crate) enum HostYieldReason {
    NightlyJobRunning { jobs: u16 },
    PromoteJobRunning { jobs: u16 },
    DevJobRunning { jobs: u16 },
    LeaseJobRunning { jobs: u16 },
    NightlyJobWaiting { jobs: u16 },
    DevJobWaiting { jobs: u16 },
    RunnerJobWaiting { jobs: u16 },
    DiskLow { jobs: u16 },
    DiskUnderFloor { free_gib: u32, floor_gib: u32 },
    MemoryPressure { level: u8 },
    HostLockHeld,
    HostLockWanted { waiters: u16, drains: u16 },
    HostReserved,
    Unobservable { source: HostYieldSource },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum HostYieldSource {
    Processes,
    CapacityLedger,
    Disk,
    MemoryPressure,
    HostLock,
    HostLockWaiters,
    Reservation,
}

/// The `Runner.Worker`s of the Mac's own priority runners, by class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct HostFirstJobs {
    pub(crate) nightly: u16,
    pub(crate) promote: u16,
    pub(crate) dev: u16,
    pub(crate) lease: u16,
}

/// The capacity ledger: fresh admission waiters (all, and by tier) and live disk-low markers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct CapacityLedger {
    pub(crate) waiters: u16,
    pub(crate) nightly_waiters: u16,
    pub(crate) dev_waiters: u16,
    pub(crate) disk_low: u16,
}

/// Free disk on the fleet volume against the hook's floor, in MiB. A floor of 0 holds nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct DiskReading {
    pub(crate) free_mib: u64,
    pub(crate) floor_mib: u64,
}

/// Live registered waiters on `host.lock`, and how many of them are drains (a fleet lease's, a
/// lend or a measure drain).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct LockWaiters {
    pub(crate) waiters: u16,
    pub(crate) drains: u16,
}

/// One reading of the Mac. `None` is a source that could not be read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct HostYieldFacts {
    pub(crate) host_first_jobs: Option<HostFirstJobs>,
    pub(crate) ledger: Option<CapacityLedger>,
    pub(crate) disk: Option<DiskReading>,
    pub(crate) memory_pressure_level: Option<u8>,
    pub(crate) host_lock_held: Option<bool>,
    pub(crate) host_lock_waiters: Option<LockWaiters>,
    pub(crate) reservation_active: Option<bool>,
}

impl HostYieldFacts {
    #[cfg(test)]
    pub(crate) const fn quiet() -> Self {
        Self {
            host_first_jobs: Some(HostFirstJobs {
                nightly: 0,
                promote: 0,
                dev: 0,
                lease: 0,
            }),
            ledger: Some(CapacityLedger {
                waiters: 0,
                nightly_waiters: 0,
                dev_waiters: 0,
                disk_low: 0,
            }),
            disk: Some(DiskReading {
                free_mib: 0,
                floor_mib: 0,
            }),
            memory_pressure_level: Some(1),
            host_lock_held: Some(false),
            host_lock_waiters: Some(LockWaiters {
                waiters: 0,
                drains: 0,
            }),
            reservation_active: Some(false),
        }
    }
}

fn held(count: u16) -> Option<u16> {
    (count > 0).then_some(count)
}

/// The reason the Mac's own work holds new workers off, or `None`. Pure.
///
/// A held reason outranks an unreadable source, so the receipt names the real cause when there is
/// one; an unreadable source still yields.
pub(crate) fn host_yield_reason(facts: &HostYieldFacts) -> Option<HostYieldReason> {
    if let Some(jobs) = facts.host_first_jobs {
        if let Some(jobs) = held(jobs.nightly) {
            return Some(HostYieldReason::NightlyJobRunning { jobs });
        }
        if let Some(jobs) = held(jobs.promote) {
            return Some(HostYieldReason::PromoteJobRunning { jobs });
        }
        if let Some(jobs) = held(jobs.dev) {
            return Some(HostYieldReason::DevJobRunning { jobs });
        }
        if let Some(jobs) = held(jobs.lease) {
            return Some(HostYieldReason::LeaseJobRunning { jobs });
        }
    }
    if let Some(ledger) = facts.ledger {
        if let Some(jobs) = held(ledger.nightly_waiters) {
            return Some(HostYieldReason::NightlyJobWaiting { jobs });
        }
        if let Some(jobs) = held(ledger.dev_waiters) {
            return Some(HostYieldReason::DevJobWaiting { jobs });
        }
        if let Some(jobs) = held(ledger.waiters) {
            return Some(HostYieldReason::RunnerJobWaiting { jobs });
        }
        if let Some(jobs) = held(ledger.disk_low) {
            return Some(HostYieldReason::DiskLow { jobs });
        }
    }
    if let Some(disk) = facts.disk
        && disk.free_mib < disk.floor_mib
    {
        return Some(HostYieldReason::DiskUnderFloor {
            free_gib: u32::try_from(disk.free_mib / 1024).unwrap_or(u32::MAX),
            floor_gib: u32::try_from(disk.floor_mib.div_ceil(1024)).unwrap_or(u32::MAX),
        });
    }
    if let Some(level @ 2..) = facts.memory_pressure_level {
        return Some(HostYieldReason::MemoryPressure { level });
    }
    if facts.host_lock_held == Some(true) {
        return Some(HostYieldReason::HostLockHeld);
    }
    if let Some(waiters) = facts.host_lock_waiters
        && waiters.waiters > 0
    {
        return Some(HostYieldReason::HostLockWanted {
            waiters: waiters.waiters,
            drains: waiters.drains,
        });
    }
    if facts.reservation_active == Some(true) {
        return Some(HostYieldReason::HostReserved);
    }
    let unreadable = [
        (facts.host_first_jobs.is_none(), HostYieldSource::Processes),
        (facts.ledger.is_none(), HostYieldSource::CapacityLedger),
        (facts.disk.is_none(), HostYieldSource::Disk),
        (
            facts.memory_pressure_level.is_none(),
            HostYieldSource::MemoryPressure,
        ),
        (facts.host_lock_held.is_none(), HostYieldSource::HostLock),
        (
            facts.host_lock_waiters.is_none(),
            HostYieldSource::HostLockWaiters,
        ),
        (
            facts.reservation_active.is_none(),
            HostYieldSource::Reservation,
        ),
    ];
    unreadable
        .into_iter()
        .find(|(missing, _)| *missing)
        .map(|(_, source)| HostYieldReason::Unobservable { source })
}

pub(crate) trait DisposableHostYieldSource {
    /// Why no new worker may start now, or `None`. Never mutates host state.
    fn host_yield(&self) -> Option<HostYieldReason>;
}

/// No host work to yield to (native Linux workers and tests).
pub(crate) struct NoHostYield;

impl DisposableHostYieldSource for NoHostYield {
    fn host_yield(&self) -> Option<HostYieldReason> {
        None
    }
}

/// Read-only observation of a fleet Mac's runner-hook ledger, processes and memory pressure.
pub(crate) struct MacHostYield<E> {
    fleet_dir: PathBuf,
    runner_home: Option<PathBuf>,
    executor: E,
    last: Cell<Option<(Instant, Option<HostYieldReason>)>>,
}

impl<E: TimedCommandExecutor> MacHostYield<E> {
    /// A reading with no runner home: no runner floor is known, so free disk holds nothing.
    pub(crate) fn new(fleet_dir: PathBuf, executor: E) -> Self {
        Self {
            fleet_dir,
            runner_home: None,
            executor,
            last: Cell::new(None),
        }
    }

    /// The home whose `actions-runner-glaeda*` installs carry the runner disk floors.
    pub(crate) fn with_runner_home(mut self, runner_home: PathBuf) -> Self {
        self.runner_home = Some(runner_home);
        self
    }

    /// The fleet directory the runner hook uses (`GLAEDA_FLEET_DIR`, else the shared default) and
    /// the runner installs' home (`GLAEDA_RUNNER_HOME`, else `HOME`).
    pub(crate) fn from_environment(executor: E) -> Self {
        let fleet_dir = std::env::var_os(FLEET_DIR_ENV)
            .filter(|value| !value.is_empty())
            .map_or_else(|| PathBuf::from(DEFAULT_FLEET_DIR), PathBuf::from);
        let runner_home = std::env::var_os(RUNNER_HOME_ENV)
            .filter(|value| !value.is_empty())
            .or_else(|| std::env::var_os("HOME").filter(|value| !value.is_empty()))
            .map(PathBuf::from);
        let source = Self::new(fleet_dir, executor);
        match runner_home {
            Some(home) => source.with_runner_home(home),
            None => source,
        }
    }

    pub(crate) fn observe(&self, now_unix_seconds: u64) -> HostYieldFacts {
        let host_lock = self.fleet_dir.join(HOST_LOCK);
        HostYieldFacts {
            host_first_jobs: self.observe_host_first_jobs(),
            ledger: observe_capacity_ledger(&self.fleet_dir.join(CAPACITY_DIR), now_unix_seconds),
            disk: self.observe_disk(),
            memory_pressure_level: self.observe_memory_pressure(),
            host_lock_held: observe_host_lock(&host_lock),
            host_lock_waiters: observe_lock_waiters(
                &self.fleet_dir.join(HOST_LOCK_WAITERS),
                now_unix_seconds,
            ),
            reservation_active: observe_reservation(
                &self.fleet_dir.join(RESERVATION),
                now_unix_seconds,
            ),
        }
    }

    fn observe_host_first_jobs(&self) -> Option<HostFirstJobs> {
        let spec = CommandSpec::new(PS_PROGRAM)
            .argument("-axo")
            .argument("comm=");
        let record = self
            .executor
            .execute_with_timeout(&spec, COMMAND_TIMEOUT)
            .ok()?;
        if !record.success {
            return None;
        }
        count_host_first_workers(&record.stdout, |runner| {
            match read_small(&runner.join(RUNNER_HOOK_SCRIPT)) {
                Ok(text) => Ok(Some(text)),
                Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
                Err(_) => Err(()),
            }
        })
    }

    /// Free disk against the runner floor. No runner home, or no floor there: nothing to hold.
    fn observe_disk(&self) -> Option<DiskReading> {
        let Some(home) = &self.runner_home else {
            return Some(DiskReading::default());
        };
        let floor_gib = runner_disk_floor_gib(home)?;
        if floor_gib <= 0.0 {
            return Some(DiskReading::default());
        }
        // The gate reads free disk where host.lock lives, else in the home (`free_gib`).
        let volume = if self.fleet_dir.is_dir() {
            self.fleet_dir.as_path()
        } else {
            home.as_path()
        };
        let stat = rustix::fs::statvfs(volume).ok()?;
        let free_bytes = stat.f_bavail.saturating_mul(stat.f_frsize);
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let floor_mib = (floor_gib * 1024.0).ceil() as u64;
        Some(DiskReading {
            free_mib: free_bytes / (1024 * 1024),
            floor_mib,
        })
    }

    fn observe_memory_pressure(&self) -> Option<u8> {
        let spec = CommandSpec::new(SYSCTL_PROGRAM)
            .argument("-n")
            .argument("kern.memorystatus_vm_pressure_level");
        let record = self
            .executor
            .execute_with_timeout(&spec, COMMAND_TIMEOUT)
            .ok()?;
        if !record.success {
            return None;
        }
        record.stdout.trim().parse().ok()
    }
}

impl<E: TimedCommandExecutor> DisposableHostYieldSource for MacHostYield<E> {
    fn host_yield(&self) -> Option<HostYieldReason> {
        if let Some((at, reason)) = self.last.get()
            && at.elapsed() < OBSERVATION_REUSE
        {
            return reason;
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_secs());
        let reason = host_yield_reason(&self.observe(now));
        self.last.set(Some((Instant::now(), reason)));
        reason
    }
}

fn has_flag(text: &str, flags: &[&str]) -> bool {
    text.split_whitespace().any(|word| flags.contains(&word))
}

/// Running jobs of the Mac's own priority runners in a `ps -axo comm=` listing. `hook_script` reads
/// one runner install's job-started hook (`Ok(None)` when it has none, as the hook's `runner_scope`
/// reads it; `Err` when it cannot be read, which makes the whole listing unreadable, so the host
/// yields). An install is `actions-runner*`, or a lease runner's `runner` tree. Pure apart from
/// that reader.
pub(crate) fn count_host_first_workers(
    listing: &str,
    hook_script: impl Fn(&Path) -> Result<Option<String>, ()>,
) -> Option<HostFirstJobs> {
    let mut jobs = HostFirstJobs::default();
    let bump = |count: &mut u16| *count = count.saturating_add(1).min(MAX_COUNT);
    for (row, line) in listing.lines().enumerate() {
        if row >= MAX_PROCESS_ROWS {
            return None;
        }
        let command = line.trim();
        let Some(bin) = command
            .strip_suffix(RUNNER_WORKER)
            .and_then(|rest| rest.strip_suffix('/'))
        else {
            continue;
        };
        let bin = Path::new(bin);
        let runner = bin
            .file_name()
            .and_then(|name| name.to_str())
            .filter(|name| *name == "bin" || name.starts_with("bin."))
            .and_then(|_| bin.parent())
            .filter(|runner| {
                runner
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("actions-runner") || name == "runner")
            });
        let hook = match runner {
            Some(runner) => hook_script(runner).ok()?.unwrap_or_default(),
            None => String::new(),
        };
        if command.contains("nightly") || has_flag(&hook, &NIGHTLY_RUNNER_FLAGS) {
            bump(&mut jobs.nightly);
        } else if has_flag(&hook, &PROMOTE_RUNNER_FLAGS) {
            bump(&mut jobs.promote);
        } else if has_flag(&hook, &DEV_RUNNER_FLAGS) {
            bump(&mut jobs.dev);
        } else if has_flag(&hook, &LEASE_RUNNER_FLAGS) {
            bump(&mut jobs.lease);
        }
    }
    Some(jobs)
}

/// The last `--min-free-gib N` (or `=N`) in a job-started hook, as the hook's `min_free_gib` reads
/// it: 0 without one or when it is not a finite positive number.
pub(crate) fn hook_min_free_gib(text: &str) -> f64 {
    let mut value = None;
    let mut words = text.split_whitespace();
    while let Some(word) = words.next() {
        if word == MIN_FREE_FLAG {
            value = words.next();
        } else if let Some(rest) = word
            .strip_prefix(MIN_FREE_FLAG)
            .and_then(|rest| rest.strip_prefix('='))
        {
            value = Some(rest);
        }
    }
    value
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|value| value.is_finite() && *value > 0.0)
        .map_or(0.0, |value| value.min(MAX_FLOOR_GIB))
}

/// The hook's disk floor from its runner installs' job-started hooks (name, text): the largest
/// `--min-free-gib`, raised to a dedicated builder's floor plus one job's growth
/// (`NIGHTLY_DISK_RESERVE`). Pure.
pub(crate) fn disk_floor_gib<'a>(hooks: impl IntoIterator<Item = &'a str>) -> f64 {
    let mut floor = 0.0_f64;
    let mut builder = 0.0_f64;
    for text in hooks {
        let own = hook_min_free_gib(text);
        floor = floor.max(own);
        if text
            .split_whitespace()
            .any(|word| word == DEDICATED_FLAG || word.starts_with("--dedicated-admit="))
        {
            builder = builder.max(own);
        }
    }
    if builder > 0.0 {
        floor = floor.max(builder + DISK_JOB_PEAK_GIB);
    }
    floor
}

/// `disk_floor_gib` of the `actions-runner-glaeda*` installs under `home`. A missing home or hook
/// counts as no floor; an unreadable one is `None`.
fn runner_disk_floor_gib(home: &Path) -> Option<f64> {
    let entries = match fs::read_dir(home) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Some(0.0),
        Err(_) => return None,
    };
    let mut hooks = Vec::new();
    for entry in entries {
        let entry = entry.ok()?;
        let name = entry.file_name();
        if !name
            .to_str()
            .is_some_and(|name| name.starts_with(RUNNER_INSTALL_PREFIX))
        {
            continue;
        }
        if hooks.len() >= MAX_RUNNER_INSTALLS {
            return None;
        }
        match read_small(&entry.path().join(RUNNER_HOOK_SCRIPT)) {
            Ok(text) => hooks.push(text),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(_) => return None,
        }
    }
    Some(disk_floor_gib(hooks.iter().map(String::as_str)))
}

/// The capacity ledger; empty when the Mac has none, `None` when it cannot be read.
fn observe_capacity_ledger(capacity: &Path, now_unix_seconds: u64) -> Option<CapacityLedger> {
    let entries = match fs::read_dir(capacity) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Some(CapacityLedger::default());
        }
        Err(_) => return None,
    };
    let mut ledger = CapacityLedger::default();
    let bump = |count: &mut u16| *count = count.saturating_add(1).min(MAX_COUNT);
    for (index, entry) in entries.enumerate() {
        if index >= MAX_LEDGER_ENTRIES {
            return None;
        }
        let Ok(entry) = entry else {
            return None;
        };
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if let Some(pid) = name.strip_prefix(ADMIT_WANT).and_then(ledger_pid) {
            // The hook rewrites its marker every poll; one older than ADMIT_FRESH_S, or one whose
            // waiter is gone, is a waiter that was killed (the hook removes it; we only skip it).
            let fresh = entry
                .metadata()
                .and_then(|metadata| metadata.modified())
                .ok()
                .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
                .is_some_and(|modified| {
                    now_unix_seconds.saturating_sub(modified.as_secs()) < ADMIT_FRESH_SECONDS
                });
            if fresh && pid_alive(pid) {
                bump(&mut ledger.waiters);
                // The marker is the waiting job's own (untrusted) JSON; one that cannot be read,
                // or names no tier, still counts as a waiter.
                match read_small(&entry.path())
                    .ok()
                    .as_deref()
                    .and_then(waiter_tier)
                {
                    Some(TIER_NIGHTLY) => bump(&mut ledger.nightly_waiters),
                    Some(TIER_DEV) => bump(&mut ledger.dev_waiters),
                    _ => {}
                }
            }
        } else if let Some(pid) = name.strip_prefix(DISK_LOW).and_then(ledger_pid)
            && pid_alive(pid)
        {
            bump(&mut ledger.disk_low);
        }
    }
    Some(ledger)
}

/// A waiter marker's tier (`nightly` or `dev`), as the hook's `admission_waiters` reads it. Pure.
pub(crate) fn waiter_tier(text: &str) -> Option<&'static str> {
    let document: serde_json::Value = serde_json::from_str(text).ok()?;
    match document.get("tier")?.as_str()? {
        TIER_NIGHTLY => Some(TIER_NIGHTLY),
        TIER_DEV => Some(TIER_DEV),
        _ => None,
    }
}

/// A marker's pid: digits only (a `.tmp` being written, `disk-low.lock` and the like are not markers).
fn ledger_pid(suffix: &str) -> Option<i32> {
    if suffix.is_empty() || !suffix.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    suffix.parse().ok().filter(|pid| *pid > 1)
}

fn pid_alive(pid: i32) -> bool {
    let Some(pid) = Pid::from_raw(pid) else {
        return false;
    };
    match rustix::process::test_kill_process(pid) {
        Ok(()) => true,
        Err(error) => error != rustix::io::Errno::SRCH,
    }
}

/// Whether another build holds the fleet host lock exclusively. Runner jobs hold it shared, so one
/// non-blocking shared probe tells the two apart; closing the descriptor drops the probe's lock.
fn observe_host_lock(path: &Path) -> Option<bool> {
    let flags = OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK;
    let fd = match rustix::fs::open(path, flags, Mode::empty()) {
        Ok(fd) => fd,
        Err(rustix::io::Errno::NOENT) => return Some(false),
        Err(_) => return None,
    };
    match rustix::fs::flock(&fd, FlockOperation::NonBlockingLockShared) {
        Ok(()) => Some(false),
        Err(rustix::io::Errno::WOULDBLOCK) => Some(true),
        Err(_) => None,
    }
}

/// A `host.lock.waiters` entry name's pid, when it is a waiter marker (`<pid>-<word>`). Pure.
pub(crate) fn lock_waiter_pid(name: &str) -> Option<i32> {
    let (pid, rest) = name.split_once('-')?;
    if rest.is_empty()
        || !rest
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
    {
        return None;
    }
    ledger_pid(pid)
}

/// Live registered waiters on the host lock, as the hook's `registered_lock_waiters` reads them: a
/// regular `<pid>-<word>` file whose pid lives; one with `-refresh-` in its name only while
/// rewritten within 30 s. A missing directory holds none; a symlinked or unreadable one is `None`.
fn observe_lock_waiters(directory: &Path, now_unix_seconds: u64) -> Option<LockWaiters> {
    match fs::symlink_metadata(directory) {
        Ok(metadata) if metadata.is_dir() => {}
        Ok(_) => return None,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Some(LockWaiters::default());
        }
        Err(_) => return None,
    }
    let entries = fs::read_dir(directory).ok()?;
    let mut found = LockWaiters::default();
    for (index, entry) in entries.enumerate() {
        if index >= MAX_LOCK_WAITER_ENTRIES {
            break; // the hook bounds this shared directory the same way
        }
        let entry = entry.ok()?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let Some(pid) = lock_waiter_pid(name) else {
            continue;
        };
        let Ok(metadata) = entry.metadata() else {
            continue; // gone between the listing and the look
        };
        if !metadata.file_type().is_file() {
            continue;
        }
        if name.contains("-refresh-") {
            let fresh = metadata
                .modified()
                .ok()
                .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
                .is_some_and(|modified| {
                    now_unix_seconds.saturating_sub(modified.as_secs()) < LOCK_WAITER_FRESH_SECONDS
                });
            if !fresh {
                continue;
            }
        }
        if !pid_alive(pid) {
            continue;
        }
        found.waiters = found.waiters.saturating_add(1).min(MAX_COUNT);
        if LOCK_WAITER_DRAIN_KINDS
            .iter()
            .any(|kind| name.contains(kind))
            || name.ends_with("-drain")
        {
            found.drains = found.drains.saturating_add(1).min(MAX_COUNT);
        }
    }
    Some(found)
}

/// Whether a `glaeda-reservation/v1` marker holds the host now; an invalid one counts as unreadable
/// (the hook refuses on it too).
fn observe_reservation(path: &Path, now_unix_seconds: u64) -> Option<bool> {
    match read_small(path) {
        Ok(text) => reservation_active(&text, now_unix_seconds),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Some(false),
        Err(_) => None,
    }
}

/// `Some(now < until)` for a valid marker, `None` for an invalid one. Pure.
pub(crate) fn reservation_active(text: &str, now_unix_seconds: u64) -> Option<bool> {
    let document: serde_json::Value = serde_json::from_str(text).ok()?;
    let object = document.as_object()?;
    if object.get("schema")?.as_str()? != RESERVATION_SCHEMA
        || !object.get("owner")?.is_string()
        || !object.get("purpose")?.is_string()
    {
        return None;
    }
    let since = object.get("since")?;
    let until = object.get("until")?;
    if !(since.is_i64() || since.is_u64()) || !(until.is_i64() || until.is_u64()) {
        return None;
    }
    let now = i128::from(now_unix_seconds);
    let until = until
        .as_i64()
        .map(i128::from)
        .or_else(|| until.as_u64().map(i128::from))?;
    Some(now < until)
}

fn read_small(path: &Path) -> io::Result<String> {
    let mut text = String::new();
    fs::File::open(path)?
        .take(MAX_SMALL_FILE_BYTES + 1)
        .read_to_string(&mut text)?;
    if text.len() as u64 > MAX_SMALL_FILE_BYTES {
        return Err(io::Error::other("file exceeds the bound"));
    }
    Ok(text)
}

#[cfg(test)]
mod tests;
