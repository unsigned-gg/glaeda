use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::symlink;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use super::*;
use crate::process::{CommandExecutor, ExecutionRecord};

static NEXT_ROOT: AtomicU64 = AtomicU64::new(1);

struct TempRoot(PathBuf);

impl TempRoot {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "glaeda-host-yield-{}-{}",
            std::process::id(),
            NEXT_ROOT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn capacity(&self) -> PathBuf {
        let path = self.0.join(CAPACITY_DIR);
        fs::create_dir_all(&path).unwrap();
        path
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// Answers `ps` and `sysctl` from fixtures; a command without one fails.
struct FakeExecutor(BTreeMap<&'static str, (bool, &'static str)>);

impl FakeExecutor {
    fn quiet() -> Self {
        Self(BTreeMap::from([
            (PS_PROGRAM, (true, "/sbin/launchd\n/usr/bin/login\n")),
            (SYSCTL_PROGRAM, (true, "1\n")),
        ]))
    }

    fn with(mut self, program: &'static str, success: bool, stdout: &'static str) -> Self {
        self.0.insert(program, (success, stdout));
        self
    }

    fn without(mut self, program: &'static str) -> Self {
        self.0.remove(program);
        self
    }
}

impl CommandExecutor for FakeExecutor {
    fn execute(&self, spec: &CommandSpec) -> io::Result<ExecutionRecord> {
        let program = spec.program.to_str().unwrap();
        let (success, stdout) = self
            .0
            .get(program)
            .copied()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no fixture"))?;
        Ok(ExecutionRecord {
            argv: spec.displayed_argv(),
            environment_keys: Vec::new(),
            status: Some(i32::from(!success)),
            success,
            stdout: stdout.to_owned(),
            stderr: String::new(),
        })
    }
}

impl TimedCommandExecutor for FakeExecutor {
    fn execute_with_timeout(&self, spec: &CommandSpec, _: Duration) -> io::Result<ExecutionRecord> {
        self.execute(spec)
    }
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn own_pid() -> u32 {
    std::process::id()
}

/// A pid no live process has (pids are below 2^22 on Linux and 99999 on macOS).
const DEAD_PID: u32 = 2_000_000_000;

#[test]
fn quiet_host_yields_nothing() {
    assert_eq!(host_yield_reason(&HostYieldFacts::quiet()), None);
}

fn jobs(nightly: u16, promote: u16, dev: u16, lease: u16) -> Option<HostFirstJobs> {
    Some(HostFirstJobs {
        nightly,
        promote,
        dev,
        lease,
    })
}

fn ledger(waiters: u16, nightly: u16, dev: u16, disk_low: u16) -> Option<CapacityLedger> {
    Some(CapacityLedger {
        waiters,
        nightly_waiters: nightly,
        dev_waiters: dev,
        disk_low,
    })
}

#[test]
fn each_held_fact_yields_and_nightly_comes_first() {
    let quiet = HostYieldFacts::quiet();
    let cases = [
        (
            HostYieldFacts {
                host_first_jobs: jobs(1, 0, 0, 0),
                ..quiet
            },
            HostYieldReason::NightlyJobRunning { jobs: 1 },
        ),
        (
            HostYieldFacts {
                host_first_jobs: jobs(0, 1, 0, 0),
                ..quiet
            },
            HostYieldReason::PromoteJobRunning { jobs: 1 },
        ),
        (
            HostYieldFacts {
                host_first_jobs: jobs(0, 0, 2, 0),
                ..quiet
            },
            HostYieldReason::DevJobRunning { jobs: 2 },
        ),
        (
            HostYieldFacts {
                host_first_jobs: jobs(0, 0, 0, 1),
                ..quiet
            },
            HostYieldReason::LeaseJobRunning { jobs: 1 },
        ),
        (
            HostYieldFacts {
                ledger: ledger(2, 1, 0, 0),
                ..quiet
            },
            HostYieldReason::NightlyJobWaiting { jobs: 1 },
        ),
        (
            HostYieldFacts {
                ledger: ledger(2, 0, 1, 0),
                ..quiet
            },
            HostYieldReason::DevJobWaiting { jobs: 1 },
        ),
        (
            HostYieldFacts {
                ledger: ledger(2, 0, 0, 0),
                ..quiet
            },
            HostYieldReason::RunnerJobWaiting { jobs: 2 },
        ),
        (
            HostYieldFacts {
                ledger: ledger(0, 0, 0, 1),
                ..quiet
            },
            HostYieldReason::DiskLow { jobs: 1 },
        ),
        (
            HostYieldFacts {
                disk: Some(DiskReading {
                    free_mib: 40 * 1024 + 512,
                    floor_mib: 50 * 1024,
                }),
                ..quiet
            },
            HostYieldReason::DiskUnderFloor {
                free_gib: 40,
                floor_gib: 50,
            },
        ),
        (
            HostYieldFacts {
                memory_pressure_level: Some(2),
                ..quiet
            },
            HostYieldReason::MemoryPressure { level: 2 },
        ),
        (
            HostYieldFacts {
                memory_pressure_level: Some(4),
                ..quiet
            },
            HostYieldReason::MemoryPressure { level: 4 },
        ),
        (
            HostYieldFacts {
                host_lock_held: Some(true),
                ..quiet
            },
            HostYieldReason::HostLockHeld,
        ),
        (
            HostYieldFacts {
                host_lock_waiters: Some(LockWaiters {
                    waiters: 2,
                    drains: 1,
                }),
                ..quiet
            },
            HostYieldReason::HostLockWanted {
                waiters: 2,
                drains: 1,
            },
        ),
        (
            HostYieldFacts {
                reservation_active: Some(true),
                ..quiet
            },
            HostYieldReason::HostReserved,
        ),
    ];
    for (facts, reason) in cases {
        assert_eq!(host_yield_reason(&facts), Some(reason), "{facts:?}");
    }
    let everything = HostYieldFacts {
        host_first_jobs: jobs(1, 1, 1, 1),
        ledger: ledger(3, 1, 1, 1),
        disk: Some(DiskReading {
            free_mib: 1,
            floor_mib: 2,
        }),
        memory_pressure_level: Some(4),
        host_lock_held: Some(true),
        host_lock_waiters: Some(LockWaiters {
            waiters: 1,
            drains: 1,
        }),
        reservation_active: Some(true),
    };
    assert_eq!(
        host_yield_reason(&everything),
        Some(HostYieldReason::NightlyJobRunning { jobs: 1 })
    );
    // Free disk at the floor, or with no floor, holds nothing.
    for disk in [
        DiskReading {
            free_mib: 50 * 1024,
            floor_mib: 50 * 1024,
        },
        DiskReading {
            free_mib: 0,
            floor_mib: 0,
        },
    ] {
        assert_eq!(
            host_yield_reason(&HostYieldFacts {
                disk: Some(disk),
                ..quiet
            }),
            None
        );
    }
}

#[test]
fn an_unreadable_source_yields_but_a_held_reason_is_named_first() {
    let quiet = HostYieldFacts::quiet();
    assert_eq!(
        host_yield_reason(&HostYieldFacts {
            memory_pressure_level: None,
            ..quiet
        }),
        Some(HostYieldReason::Unobservable {
            source: HostYieldSource::MemoryPressure
        })
    );
    assert_eq!(
        host_yield_reason(&HostYieldFacts {
            disk: None,
            ..quiet
        }),
        Some(HostYieldReason::Unobservable {
            source: HostYieldSource::Disk
        })
    );
    assert_eq!(
        host_yield_reason(&HostYieldFacts {
            host_lock_waiters: None,
            ..quiet
        }),
        Some(HostYieldReason::Unobservable {
            source: HostYieldSource::HostLockWaiters
        })
    );
    assert_eq!(
        host_yield_reason(&HostYieldFacts::default()),
        Some(HostYieldReason::Unobservable {
            source: HostYieldSource::Processes
        })
    );
    assert_eq!(
        host_yield_reason(&HostYieldFacts {
            host_first_jobs: None,
            host_lock_held: Some(true),
            ..quiet
        }),
        Some(HostYieldReason::HostLockHeld)
    );
}

#[test]
fn reasons_serialize_as_bounded_tagged_receipts() {
    assert_eq!(
        serde_json::to_string(&HostYieldReason::NightlyJobRunning { jobs: 1 }).unwrap(),
        r#"{"reason":"nightly_job_running","jobs":1}"#
    );
    assert_eq!(
        serde_json::to_string(&HostYieldReason::DiskUnderFloor {
            free_gib: 12,
            floor_gib: 50
        })
        .unwrap(),
        r#"{"reason":"disk_under_floor","free_gib":12,"floor_gib":50}"#
    );
    assert_eq!(
        serde_json::to_string(&HostYieldReason::HostLockWanted {
            waiters: 1,
            drains: 1
        })
        .unwrap(),
        r#"{"reason":"host_lock_wanted","waiters":1,"drains":1}"#
    );
    assert_eq!(
        serde_json::to_string(&HostYieldReason::Unobservable {
            source: HostYieldSource::CapacityLedger
        })
        .unwrap(),
        r#"{"reason":"unobservable","source":"capacity_ledger"}"#
    );
}

#[test]
fn host_first_workers_are_counted_by_their_runner_class() {
    let listing = concat!(
        "/sbin/launchd\n",
        "/Users/cmux/actions-runner-glaeda-1/bin/Runner.Listener\n",
        "/Users/cmux/actions-runner-glaeda-2/bin/Runner.Worker\n",
        "/Users/cmux/actions-runner-glaeda-4/bin.2.330.0/Runner.Worker\n",
        "/Users/cmux/actions-runner-glaeda-5/bin/Runner.Worker\n",
        "/Users/cmux/actions-runner-glaeda-6/bin/Runner.Worker\n",
        "/Users/cmux/actions-runner-glaeda-7/bin/Runner.Worker\n",
        "/Users/cmux/actions-runner-nightly/bin/Runner.Worker\n",
        "/Users/cmux/.local/state/glaeda/promotion/p1/runner/bin/Runner.Worker\n",
        "/Users/cmux/.local/state/glaeda/capture/c1/runner/bin/Runner.Worker\n",
        "/tmp/not-a-runner/bin/Runner.Worker\n",
    );
    let hooks = |runner: &Path| {
        let name = runner.file_name()?.to_str()?;
        let parent = runner.parent()?.file_name()?.to_str()?;
        match (parent, name) {
            (_, "actions-runner-glaeda-2") => {
                Some("exec hook --instance 2 --canonical-roots 2\n".to_owned())
            }
            (_, "actions-runner-glaeda-4") => {
                Some("exec hook --instance 4 --nightly-runner\n".to_owned())
            }
            (_, "actions-runner-glaeda-5") => {
                Some("exec hook --trusted-next-signing --x\n".to_owned())
            }
            (_, "actions-runner-glaeda-6") => {
                Some("exec hook --instance 6 --dev-compile-runner\n".to_owned())
            }
            (_, "actions-runner-glaeda-7") => {
                Some("exec hook --dedicated-admit /x/dedicated-admit.sh\n".to_owned())
            }
            ("p1", "runner") => Some("exec hook job-started --promotion-lease /x/p1\n".to_owned()),
            ("c1", "runner") => Some("exec hook job-started --capture-lease /x/c1\n".to_owned()),
            _ => None,
        }
    };
    assert_eq!(
        count_host_first_workers(listing, |runner| Ok(hooks(runner))),
        jobs(4, 1, 1, 1)
    );
    assert_eq!(
        count_host_first_workers("/x/actions-runner-glaeda-2/bin/Runner.Worker\n", |_| Ok(
            None
        )),
        jobs(0, 0, 0, 0)
    );
    // A hook that exists but cannot be read is no reading: the host yields rather than miss a nightly.
    assert_eq!(
        count_host_first_workers(
            "/x/actions-runner-glaeda-2/bin/Runner.Worker\n",
            |_| Err(())
        ),
        None
    );
    // A flag word only counts whole: --nightly-runner-x is not the nightly slot.
    assert_eq!(
        count_host_first_workers("/x/actions-runner-glaeda-2/bin/Runner.Worker\n", |_| {
            Ok(Some("--nightly-runner-x --dev-compile-runner-y".to_owned()))
        }),
        jobs(0, 0, 0, 0)
    );
}

#[test]
fn runner_floor_is_the_largest_min_free_and_a_builders_floor_plus_one_job() {
    assert!((hook_min_free_gib("exec hook --min-free-gib 30 --x") - 30.0).abs() < f64::EPSILON);
    assert!((hook_min_free_gib("exec hook --min-free-gib=45.5") - 45.5).abs() < f64::EPSILON);
    assert!(
        (hook_min_free_gib("--min-free-gib 10 --min-free-gib 25") - 25.0).abs() < f64::EPSILON,
        "the last one wins, as in the hook"
    );
    assert!(hook_min_free_gib("exec hook --instance 2").abs() < f64::EPSILON);
    assert!(hook_min_free_gib("--min-free-gib nan").abs() < f64::EPSILON);
    assert!(hook_min_free_gib("--min-free-gib -5").abs() < f64::EPSILON);

    let pool = "exec hook --instance 1 --min-free-gib 30";
    let builder = "exec hook --dedicated-admit /x/admit.sh --min-free-gib 40";
    assert!((disk_floor_gib([pool]) - 30.0).abs() < f64::EPSILON);
    assert!(
        (disk_floor_gib([pool, builder]) - (40.0 + DISK_JOB_PEAK_GIB)).abs() < f64::EPSILON,
        "NIGHTLY_DISK_RESERVE: the builder's floor plus one job"
    );
    assert!(disk_floor_gib(std::iter::empty()).abs() < f64::EPSILON);
}

#[test]
fn disk_reads_the_runner_installs_floor_and_free_space() {
    let root = TempRoot::new();
    let home = root.0.join("home");
    let fleet = root.0.join("fleet");
    fs::create_dir_all(&fleet).unwrap();

    // No runner home: no floor, nothing held.
    let source =
        MacHostYield::new(fleet.clone(), FakeExecutor::quiet()).with_runner_home(home.clone());
    assert_eq!(source.observe(now()).disk, Some(DiskReading::default()));

    // A floor no volume can meet holds; the reading names it.
    let hooks = home.join("actions-runner-glaeda-1/glaeda-hooks");
    fs::create_dir_all(&hooks).unwrap();
    fs::write(
        hooks.join("job-started.sh"),
        "exec hook job-started --min-free-gib 9999\n",
    )
    .unwrap();
    fs::create_dir_all(home.join("actions-runner-glaeda-2")).unwrap();
    fs::create_dir_all(home.join("unrelated")).unwrap();
    let source =
        MacHostYield::new(fleet.clone(), FakeExecutor::quiet()).with_runner_home(home.clone());
    let disk = source.observe(now()).disk.unwrap();
    assert_eq!(disk.floor_mib, 9999 * 1024);
    assert!(disk.free_mib < disk.floor_mib);
    assert!(matches!(
        source.host_yield(),
        Some(HostYieldReason::DiskUnderFloor {
            floor_gib: 9999,
            ..
        })
    ));

    // A floor the volume meets holds nothing.
    fs::write(
        hooks.join("job-started.sh"),
        "exec hook job-started --min-free-gib 0.001\n",
    )
    .unwrap();
    let source = MacHostYield::new(fleet, FakeExecutor::quiet()).with_runner_home(home.clone());
    assert_eq!(source.host_yield(), None);

    // An unreadable hook yields as unobservable.
    fs::remove_file(hooks.join("job-started.sh")).unwrap();
    fs::create_dir(hooks.join("job-started.sh")).unwrap();
    assert_eq!(runner_disk_floor_gib(&home), None);
}

#[test]
fn waiter_tier_reads_nightly_and_dev_only() {
    assert_eq!(
        waiter_tier(r#"{"since":1,"wants":["units"],"tier":"nightly"}"#),
        Some(TIER_NIGHTLY)
    );
    assert_eq!(waiter_tier(r#"{"since":1,"tier":"dev"}"#), Some(TIER_DEV));
    assert_eq!(waiter_tier(r#"{"since":1,"tier":"stack"}"#), None);
    assert_eq!(waiter_tier(r#"{"since":1,"tier":7}"#), None);
    assert_eq!(waiter_tier("{}"), None);
    assert_eq!(waiter_tier("not json"), None);
}

#[test]
fn lock_waiter_names_follow_the_hooks_pattern() {
    assert_eq!(lock_waiter_pid("4242-refresh-lend_drain-17"), Some(4242));
    assert_eq!(lock_waiter_pid("4242-with-host-lock"), Some(4242));
    assert_eq!(lock_waiter_pid("4242-"), None);
    assert_eq!(lock_waiter_pid("x-refresh"), None);
    assert_eq!(lock_waiter_pid("1-init"), None);
    assert_eq!(lock_waiter_pid("4242-a b"), None);
}

#[test]
fn lock_waiters_count_live_fresh_markers_and_their_drains() {
    let root = TempRoot::new();
    let directory = root.0.join(HOST_LOCK_WAITERS);
    assert_eq!(
        observe_lock_waiters(&directory, now()),
        Some(LockWaiters::default())
    );
    fs::create_dir(&directory).unwrap();
    let mine = own_pid();
    fs::write(directory.join(format!("{mine}-refresh-lend_drain-1")), "").unwrap();
    fs::write(directory.join(format!("{mine}-with-host-lock")), "").unwrap();
    fs::write(directory.join(format!("{DEAD_PID}-with-host-lock")), "").unwrap();
    fs::write(directory.join("README"), "").unwrap();
    fs::create_dir(directory.join(format!("{mine}-a-directory"))).unwrap();
    assert_eq!(
        observe_lock_waiters(&directory, now()),
        Some(LockWaiters {
            waiters: 2,
            drains: 1
        })
    );
    // A polling waiter that stopped refreshing is gone; a blocking one stands while its pid lives.
    assert_eq!(
        observe_lock_waiters(&directory, now() + LOCK_WAITER_FRESH_SECONDS + 5),
        Some(LockWaiters {
            waiters: 1,
            drains: 0
        })
    );
    let source = MacHostYield::new(root.0.clone(), FakeExecutor::quiet());
    assert_eq!(
        source.host_yield(),
        Some(HostYieldReason::HostLockWanted {
            waiters: 2,
            drains: 1
        })
    );

    let alias = root.0.join("alias.waiters");
    symlink(&directory, &alias).unwrap();
    assert_eq!(
        observe_lock_waiters(&alias, now()),
        None,
        "a symlinked waiter directory is not read"
    );
}

#[test]
fn reservation_is_active_only_while_valid_and_before_until() {
    let marker = |until: &str| {
        format!(
            r#"{{"schema":"glaeda-reservation/v1","owner":"o","purpose":"p","since":1,"until":{until}}}"#
        )
    };
    assert_eq!(reservation_active(&marker("200"), 100), Some(true));
    assert_eq!(reservation_active(&marker("100"), 100), Some(false));
    assert_eq!(reservation_active(&marker("true"), 100), None);
    assert_eq!(reservation_active(&marker("1.5"), 100), None);
    assert_eq!(reservation_active("{}", 100), None);
    assert_eq!(reservation_active("not json", 100), None);
    assert_eq!(
        reservation_active(
            r#"{"schema":"other/v1","owner":"o","purpose":"p","since":1,"until":200}"#,
            100
        ),
        None
    );
}

#[test]
fn a_mac_without_the_runner_hook_yields_nothing() {
    let root = TempRoot::new();
    let missing = root.0.join("no-fleet-dir");
    let source = MacHostYield::new(missing, FakeExecutor::quiet());
    assert_eq!(source.observe(now()), HostYieldFacts::quiet());
    assert_eq!(source.host_yield(), None);
}

#[test]
fn ledger_counts_fresh_live_waiters_and_live_disk_low_markers() {
    let root = TempRoot::new();
    let capacity = root.capacity();
    let mine = own_pid();
    fs::write(capacity.join(format!("{ADMIT_WANT}{mine}")), "{}").unwrap();
    fs::write(capacity.join(format!("{ADMIT_WANT}{DEAD_PID}")), "{}").unwrap();
    fs::write(capacity.join(format!("{ADMIT_WANT}{mine}.tmp")), "{}").unwrap();
    fs::write(capacity.join(format!("{DISK_LOW}{mine}")), "id\n").unwrap();
    fs::write(capacity.join(format!("{DISK_LOW}{DEAD_PID}")), "id\n").unwrap();
    fs::write(capacity.join("disk-low.lock"), "").unwrap();
    fs::write(capacity.join("units.token"), "").unwrap();

    assert_eq!(
        observe_capacity_ledger(&capacity, now()),
        ledger(1, 0, 0, 1)
    );
    // A waiter that has not rewritten its marker for ADMIT_FRESH_S is gone.
    assert_eq!(
        observe_capacity_ledger(&capacity, now() + ADMIT_FRESH_SECONDS + 5),
        ledger(0, 0, 0, 1)
    );

    let source = MacHostYield::new(root.0.clone(), FakeExecutor::quiet());
    assert_eq!(
        source.host_yield(),
        Some(HostYieldReason::RunnerJobWaiting { jobs: 1 })
    );
}

#[test]
fn ledger_names_queued_nightly_promote_and_dev_jobs_by_tier() {
    let root = TempRoot::new();
    let capacity = root.capacity();
    // Two live pids: this test and its parent.
    let mine = own_pid();
    let parent = std::os::unix::process::parent_id();
    fs::write(
        capacity.join(format!("{ADMIT_WANT}{mine}")),
        r#"{"since":1,"wants":["units"],"tier":"dev"}"#,
    )
    .unwrap();
    fs::write(capacity.join(format!("{ADMIT_WANT}{parent}")), "not json").unwrap();
    assert_eq!(
        observe_capacity_ledger(&capacity, now()),
        ledger(2, 0, 1, 0)
    );
    assert_eq!(
        MacHostYield::new(root.0.clone(), FakeExecutor::quiet()).host_yield(),
        Some(HostYieldReason::DevJobWaiting { jobs: 1 })
    );

    // A promote job ranks nightly (the hook's job_tier), and a nightly job freeing disk too.
    fs::write(
        capacity.join(format!("{ADMIT_WANT}{parent}")),
        r#"{"since":1,"wants":["disk","units"],"tier":"nightly"}"#,
    )
    .unwrap();
    assert_eq!(
        observe_capacity_ledger(&capacity, now()),
        ledger(2, 1, 1, 0)
    );
    assert_eq!(
        MacHostYield::new(root.0.clone(), FakeExecutor::quiet()).host_yield(),
        Some(HostYieldReason::NightlyJobWaiting { jobs: 1 })
    );
}

#[test]
fn an_unreadable_ledger_yields() {
    let root = TempRoot::new();
    fs::write(root.0.join(CAPACITY_DIR), "not a directory").unwrap();
    let source = MacHostYield::new(root.0.clone(), FakeExecutor::quiet());
    assert_eq!(
        source.host_yield(),
        Some(HostYieldReason::Unobservable {
            source: HostYieldSource::CapacityLedger
        })
    );
}

#[test]
fn host_lock_counts_only_an_exclusive_holder() {
    let root = TempRoot::new();
    let path = root.0.join(HOST_LOCK);
    assert_eq!(observe_host_lock(&path), Some(false));
    fs::write(&path, "").unwrap();
    assert_eq!(observe_host_lock(&path), Some(false));

    let shared = fs::File::open(&path).unwrap();
    rustix::fs::flock(&shared, FlockOperation::LockShared).unwrap();
    assert_eq!(
        observe_host_lock(&path),
        Some(false),
        "runner jobs hold it shared"
    );
    drop(shared);

    let exclusive = fs::File::open(&path).unwrap();
    rustix::fs::flock(&exclusive, FlockOperation::LockExclusive).unwrap();
    assert_eq!(observe_host_lock(&path), Some(true));
    let source = MacHostYield::new(root.0.clone(), FakeExecutor::quiet());
    assert_eq!(source.host_yield(), Some(HostYieldReason::HostLockHeld));
    drop(exclusive);
    assert_eq!(observe_host_lock(&path), Some(false));

    let alias = root.0.join("alias.lock");
    symlink(&path, &alias).unwrap();
    assert_eq!(
        observe_host_lock(&alias),
        None,
        "a symlinked lock is not read"
    );
}

#[test]
fn reservation_file_missing_active_and_invalid() {
    let root = TempRoot::new();
    let path = root.0.join(RESERVATION);
    assert_eq!(observe_reservation(&path, 100), Some(false));
    fs::write(
        &path,
        r#"{"schema":"glaeda-reservation/v1","owner":"o","purpose":"p","since":1,"until":200}"#,
    )
    .unwrap();
    assert_eq!(observe_reservation(&path, 100), Some(true));
    assert_eq!(observe_reservation(&path, 300), Some(false));
    fs::write(&path, "garbage").unwrap();
    assert_eq!(observe_reservation(&path, 100), None);
}

#[test]
fn commands_feed_nightly_and_memory_pressure() {
    let root = TempRoot::new();
    let nightly = FakeExecutor::quiet().with(
        PS_PROGRAM,
        true,
        "/Users/cmux/actions-runner-nightly-1/bin/Runner.Worker\n",
    );
    assert_eq!(
        MacHostYield::new(root.0.clone(), nightly).host_yield(),
        Some(HostYieldReason::NightlyJobRunning { jobs: 1 })
    );

    let pressure = FakeExecutor::quiet().with(SYSCTL_PROGRAM, true, "4\n");
    assert_eq!(
        MacHostYield::new(root.0.clone(), pressure).host_yield(),
        Some(HostYieldReason::MemoryPressure { level: 4 })
    );

    let no_ps = FakeExecutor::quiet().without(PS_PROGRAM);
    assert_eq!(
        MacHostYield::new(root.0.clone(), no_ps).host_yield(),
        Some(HostYieldReason::Unobservable {
            source: HostYieldSource::Processes
        })
    );

    let failed_sysctl = FakeExecutor::quiet().with(SYSCTL_PROGRAM, false, "");
    assert_eq!(
        MacHostYield::new(root.0.clone(), failed_sysctl).host_yield(),
        Some(HostYieldReason::Unobservable {
            source: HostYieldSource::MemoryPressure
        })
    );
}

#[test]
fn one_observation_answers_for_the_reuse_window() {
    let root = TempRoot::new();
    let source = MacHostYield::new(root.0.clone(), FakeExecutor::quiet());
    assert_eq!(source.host_yield(), None);
    let capacity = root.capacity();
    fs::write(capacity.join(format!("{ADMIT_WANT}{}", own_pid())), "{}").unwrap();
    assert_eq!(source.host_yield(), None, "reused within OBSERVATION_REUSE");
    source.last.set(None);
    assert_eq!(
        source.host_yield(),
        Some(HostYieldReason::RunnerJobWaiting { jobs: 1 })
    );
}
