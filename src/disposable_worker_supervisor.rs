//! Bounded retry and pacing loop for the disposable worker coordinator.
//!
//! This module owns no GitHub, credential, process, VM, filesystem, or clock authority. The
//! production disposable-worker service supplies the session-bound coordinator driver; stop/wait
//! remains an injected control boundary so process termination can be composed separately. Session
//! failures always discard the session before retry; durable reconciliation, rather than an
//! in-memory retry, decides which external action may run next.

use std::fmt;
use std::time::Duration;

use crate::disposable_host_yield::HostYieldReason;
use crate::disposable_worker_coordinator::DisposableWorkerCoordinatorDisposition;

const IDLE_DELAY: Duration = Duration::from_secs(1);
const PROGRESS_BURST_LIMIT: u8 = 32;
const PROGRESS_YIELD_DELAY: Duration = Duration::from_millis(10);
const RECOVERY_RECONNECT_DELAY: Duration = Duration::from_secs(1);
const FAILURE_DELAYS: [Duration; 4] = [
    Duration::from_secs(1),
    Duration::from_secs(5),
    Duration::from_secs(30),
    Duration::from_secs(60),
];
const CIRCUIT_FAILURES: u8 = 5;
const CIRCUIT_DELAY: Duration = Duration::from_secs(5 * 60);

pub(crate) trait DisposableWorkerSupervisorDriver {
    type Session;

    fn connect(&mut self) -> Result<Self::Session, DisposableWorkerSupervisorError>;

    fn supervise_once(
        &mut self,
        session: &mut Self::Session,
    ) -> Result<DisposableWorkerCoordinatorDisposition, DisposableWorkerSupervisorError>;
}

pub(crate) trait DisposableWorkerSupervisorControl {
    fn stop_requested(&self) -> bool;

    /// Wait up to the supplied delay, returning `true` when stop was requested during the wait.
    fn wait_or_stop(&mut self, duration: Duration)
    -> Result<bool, DisposableWorkerSupervisorError>;

    /// Record that the worker started or stopped yielding to the host's own work. Called once per
    /// change, never per idle poll: one bounded JSON line on stderr (the LaunchAgent's log).
    fn report_host_yield(&mut self, reason: Option<HostYieldReason>) {
        let line = match reason {
            Some(reason) => {
                serde_json::json!({"event": "disposable_host_yield", "state": "yielding", "why": reason})
            }
            None => serde_json::json!({"event": "disposable_host_yield", "state": "cleared"}),
        };
        eprintln!("{line}");
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DisposableWorkerSupervisorDisposition {
    Stopped,
}

/// Run until the injected stop source requests termination.
///
/// Every failed or recovery-required session is dropped before any wait or reconnect. Successful
/// progress may continue immediately, but a fixed burst limit yields to stop handling and avoids
/// a tight persistence loop. Idle polls always use the fixed idle cadence.
pub(crate) fn supervise_disposable_worker<D, C>(
    driver: &mut D,
    control: &mut C,
) -> Result<DisposableWorkerSupervisorDisposition, DisposableWorkerSupervisorError>
where
    D: DisposableWorkerSupervisorDriver,
    C: DisposableWorkerSupervisorControl,
{
    let mut consecutive_failures = 0_u8;
    let mut immediate_progress = 0_u8;
    let mut yielding: Option<HostYieldReason> = None;

    loop {
        if control.stop_requested() {
            return Ok(DisposableWorkerSupervisorDisposition::Stopped);
        }
        let mut session = match driver.connect() {
            Ok(session) => session,
            Err(_) => {
                consecutive_failures = consecutive_failures.saturating_add(1);
                if wait_for_failure(control, consecutive_failures)? {
                    return Ok(DisposableWorkerSupervisorDisposition::Stopped);
                }
                continue;
            }
        };

        loop {
            if control.stop_requested() {
                return Ok(DisposableWorkerSupervisorDisposition::Stopped);
            }
            let step = driver.supervise_once(&mut session);
            if let Ok(disposition) = &step {
                let now = match disposition {
                    DisposableWorkerCoordinatorDisposition::HostYielding(reason) => Some(*reason),
                    _ => None,
                };
                if now != yielding {
                    control.report_host_yield(now);
                    yielding = now;
                }
            }
            match step {
                Ok(DisposableWorkerCoordinatorDisposition::DeliveryRecoveryRequired) => {
                    drop(session);
                    immediate_progress = 0;
                    if control.wait_or_stop(RECOVERY_RECONNECT_DELAY)? {
                        return Ok(DisposableWorkerSupervisorDisposition::Stopped);
                    }
                    break;
                }
                Ok(
                    DisposableWorkerCoordinatorDisposition::Idle
                    | DisposableWorkerCoordinatorDisposition::HostStorageUnavailable
                    | DisposableWorkerCoordinatorDisposition::HostYielding(_),
                ) => {
                    consecutive_failures = 0;
                    immediate_progress = 0;
                    if control.wait_or_stop(IDLE_DELAY)? {
                        return Ok(DisposableWorkerSupervisorDisposition::Stopped);
                    }
                }
                Ok(_) => {
                    consecutive_failures = 0;
                    immediate_progress = immediate_progress.saturating_add(1);
                    if immediate_progress >= PROGRESS_BURST_LIMIT {
                        immediate_progress = 0;
                        if control.wait_or_stop(PROGRESS_YIELD_DELAY)? {
                            return Ok(DisposableWorkerSupervisorDisposition::Stopped);
                        }
                    }
                }
                Err(_) => {
                    drop(session);
                    immediate_progress = 0;
                    consecutive_failures = consecutive_failures.saturating_add(1);
                    if wait_for_failure(control, consecutive_failures)? {
                        return Ok(DisposableWorkerSupervisorDisposition::Stopped);
                    }
                    break;
                }
            }
        }
    }
}

fn wait_for_failure(
    control: &mut impl DisposableWorkerSupervisorControl,
    consecutive_failures: u8,
) -> Result<bool, DisposableWorkerSupervisorError> {
    let delay = if consecutive_failures >= CIRCUIT_FAILURES {
        CIRCUIT_DELAY
    } else {
        FAILURE_DELAYS[usize::from(consecutive_failures.saturating_sub(1))]
    };
    control.wait_or_stop(delay)
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct DisposableWorkerSupervisorError {
    code: &'static str,
}

impl DisposableWorkerSupervisorError {
    pub(crate) const fn new(code: &'static str) -> Self {
        Self { code }
    }

    pub(crate) const fn code(self) -> &'static str {
        self.code
    }
}

impl fmt::Debug for DisposableWorkerSupervisorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DisposableWorkerSupervisorError")
            .field("code", &self.code)
            .finish()
    }
}

impl fmt::Display for DisposableWorkerSupervisorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("the disposable worker supervisor could not continue")
    }
}

impl std::error::Error for DisposableWorkerSupervisorError {}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::*;
    use crate::disposable_worker_coordinator::DisposableWorkerCoordinatorDisposition;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct Session(u8);

    struct FakeDriver {
        connect_results: VecDeque<Result<Session, DisposableWorkerSupervisorError>>,
        steps: VecDeque<
            Result<DisposableWorkerCoordinatorDisposition, DisposableWorkerSupervisorError>,
        >,
        connections: usize,
    }

    impl DisposableWorkerSupervisorDriver for FakeDriver {
        type Session = Session;

        fn connect(&mut self) -> Result<Self::Session, DisposableWorkerSupervisorError> {
            self.connections += 1;
            self.connect_results.pop_front().unwrap_or(Ok(Session(1)))
        }

        fn supervise_once(
            &mut self,
            _: &mut Self::Session,
        ) -> Result<DisposableWorkerCoordinatorDisposition, DisposableWorkerSupervisorError>
        {
            self.steps
                .pop_front()
                .unwrap_or(Ok(DisposableWorkerCoordinatorDisposition::Idle))
        }
    }

    #[derive(Default)]
    struct FakeControl {
        waits: Vec<Duration>,
        stop_after_waits: usize,
        yields: Vec<Option<HostYieldReason>>,
    }

    impl DisposableWorkerSupervisorControl for FakeControl {
        fn stop_requested(&self) -> bool {
            self.waits.len() >= self.stop_after_waits
        }

        fn wait_or_stop(
            &mut self,
            duration: Duration,
        ) -> Result<bool, DisposableWorkerSupervisorError> {
            self.waits.push(duration);
            Ok(self.stop_requested())
        }

        fn report_host_yield(&mut self, reason: Option<HostYieldReason>) {
            self.yields.push(reason);
        }
    }

    fn driver(
        steps: impl IntoIterator<
            Item = Result<DisposableWorkerCoordinatorDisposition, DisposableWorkerSupervisorError>,
        >,
    ) -> FakeDriver {
        FakeDriver {
            connect_results: VecDeque::new(),
            steps: steps.into_iter().collect(),
            connections: 0,
        }
    }

    #[test]
    fn idle_uses_fixed_cadence_and_observes_stop_before_another_step() {
        let mut driver = driver([Ok(DisposableWorkerCoordinatorDisposition::Idle)]);
        let mut control = FakeControl {
            waits: Vec::new(),
            stop_after_waits: 1,
            yields: Vec::new(),
        };
        assert_eq!(
            supervise_disposable_worker(&mut driver, &mut control).unwrap(),
            DisposableWorkerSupervisorDisposition::Stopped
        );
        assert_eq!(control.waits, [IDLE_DELAY]);
        assert_eq!(driver.connections, 1);
    }

    #[test]
    fn unavailable_host_storage_uses_idle_cadence_without_opening_the_failure_circuit() {
        let mut driver = driver([Ok(
            DisposableWorkerCoordinatorDisposition::HostStorageUnavailable,
        )]);
        let mut control = FakeControl {
            waits: Vec::new(),
            stop_after_waits: 1,
            yields: Vec::new(),
        };
        assert_eq!(
            supervise_disposable_worker(&mut driver, &mut control).unwrap(),
            DisposableWorkerSupervisorDisposition::Stopped
        );
        assert_eq!(control.waits, [IDLE_DELAY]);
        assert_eq!(driver.connections, 1);
    }

    #[test]
    fn host_yield_idles_and_reports_each_change_once() {
        let nightly = HostYieldReason::NightlyJobRunning { jobs: 1 };
        let waiting = HostYieldReason::RunnerJobWaiting { jobs: 2 };
        let mut driver = driver([
            Ok(DisposableWorkerCoordinatorDisposition::HostYielding(
                nightly,
            )),
            Ok(DisposableWorkerCoordinatorDisposition::HostYielding(
                nightly,
            )),
            Ok(DisposableWorkerCoordinatorDisposition::HostYielding(
                waiting,
            )),
            Ok(DisposableWorkerCoordinatorDisposition::Idle),
            Ok(DisposableWorkerCoordinatorDisposition::Idle),
        ]);
        let mut control = FakeControl {
            stop_after_waits: 5,
            yields: Vec::new(),
            ..FakeControl::default()
        };
        assert_eq!(
            supervise_disposable_worker(&mut driver, &mut control).unwrap(),
            DisposableWorkerSupervisorDisposition::Stopped
        );
        assert_eq!(control.waits, [IDLE_DELAY; 5]);
        assert_eq!(control.yields, [Some(nightly), Some(waiting), None]);
        assert_eq!(driver.connections, 1);
    }

    #[test]
    fn progress_is_bounded_before_yielding_to_stop() {
        let steps = (0..PROGRESS_BURST_LIMIT)
            .map(|_| Ok(DisposableWorkerCoordinatorDisposition::DeliverySettled { acquired: 0 }));
        let mut driver = driver(steps);
        let mut control = FakeControl {
            waits: Vec::new(),
            stop_after_waits: 1,
            yields: Vec::new(),
        };
        assert_eq!(
            supervise_disposable_worker(&mut driver, &mut control).unwrap(),
            DisposableWorkerSupervisorDisposition::Stopped
        );
        assert_eq!(control.waits, [PROGRESS_YIELD_DELAY]);
        assert_eq!(driver.connections, 1);
    }

    #[test]
    fn recovery_required_discards_the_session_and_reconnects_after_fixed_delay() {
        let mut driver = driver([
            Ok(DisposableWorkerCoordinatorDisposition::DeliveryRecoveryRequired),
            Ok(DisposableWorkerCoordinatorDisposition::Idle),
        ]);
        let mut control = FakeControl {
            waits: Vec::new(),
            stop_after_waits: 2,
            yields: Vec::new(),
        };
        assert_eq!(
            supervise_disposable_worker(&mut driver, &mut control).unwrap(),
            DisposableWorkerSupervisorDisposition::Stopped
        );
        assert_eq!(control.waits, [RECOVERY_RECONNECT_DELAY, IDLE_DELAY]);
        assert_eq!(driver.connections, 2);
    }

    #[test]
    fn repeated_failures_open_the_fixed_circuit_and_success_resets_it() {
        let mut driver = driver([
            Err(DisposableWorkerSupervisorError::new("fake_failure")),
            Err(DisposableWorkerSupervisorError::new("fake_failure")),
            Err(DisposableWorkerSupervisorError::new("fake_failure")),
            Err(DisposableWorkerSupervisorError::new("fake_failure")),
            Err(DisposableWorkerSupervisorError::new("fake_failure")),
            Ok(DisposableWorkerCoordinatorDisposition::Idle),
            Err(DisposableWorkerSupervisorError::new("fake_failure")),
        ]);
        let mut control = FakeControl {
            waits: Vec::new(),
            stop_after_waits: 7,
            yields: Vec::new(),
        };
        assert_eq!(
            supervise_disposable_worker(&mut driver, &mut control).unwrap(),
            DisposableWorkerSupervisorDisposition::Stopped
        );
        assert_eq!(
            control.waits,
            [
                FAILURE_DELAYS[0],
                FAILURE_DELAYS[1],
                FAILURE_DELAYS[2],
                FAILURE_DELAYS[3],
                CIRCUIT_DELAY,
                IDLE_DELAY,
                FAILURE_DELAYS[0],
            ]
        );
        assert_eq!(driver.connections, 6);
    }
}
