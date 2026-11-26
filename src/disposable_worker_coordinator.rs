//! One-step production composition of disposable Scale Set delivery and worker lifecycle.
//!
//! The coordinator owns no new external semantics. It gives durable delivery recovery priority,
//! advertises only the free slots under the per-host worker ceiling (zero while one attempt is
//! active on the default one-worker host, and zero while the host yields), and dispatches exactly
//! one already-reviewed canonical-lock transaction per call. A live zero-capacity Scale Set poll is
//! repeated inside every pre-clone checkpoint boundary; a message observed there is durably
//! reconciled through the existing paired delivery transaction before any retry.

#![allow(dead_code)] // The launchd service entry point is the next operator-facing slice.

use std::cell::RefCell;
use std::fmt;
use std::path::{Path, PathBuf};

use crate::disposable_attempt_catalog::{
    DisposableAttemptCatalog, DisposableAttemptCatalogAction, DisposableAttemptCatalogDocument,
    DisposableAttemptReservation,
};
use crate::disposable_clone_runtime::{
    CloneRuntimeClock, DisposableCleanupRunnerSource, DisposableCleanupTransactionOutcome,
    DisposableCloneAdmissionObservation, DisposableCloneAdmissionSource, DisposableCloneRuntime,
    DisposableCloneRuntimeError, DisposableCloneTransactionOutcome, admission_seal,
};
use crate::disposable_host_storage::{DisposableHostStorageSource, HOST_STORAGE_UNAVAILABLE_CODE};
use crate::disposable_host_yield::{DisposableHostYieldSource, HostYieldReason, NoHostYield};
use crate::disposable_runner_runtime::{
    DisposableRunnerRegistrationSource, DisposableRunnerRuntime, DisposableRunnerRuntimeError,
};
use crate::disposable_template_runtime::DisposableTemplateRuntimeDisposition;
use crate::disposable_worker_reconciler::DisposableAttemptPhase;
use crate::execution_admission::EpochMillis;
use crate::github_scale_set_bridge::{
    ScaleSetBridgeClient, ScaleSetBridgeError, ScaleSetJitReceipt, ScaleSetRunnerLookup,
};
use crate::github_scale_set_delivery::ScaleSetDelivery;
use crate::github_scale_set_delivery_consumer::ScaleSetDeliveryConsumerPolicy;
use crate::github_scale_set_delivery_controller::{
    DeliveryBridge, ScaleSetDeliveryControllerDisposition, consume_with_bridge_capacity,
    consume_with_bridge_capacity_at_revision, reconcile_and_ack_delivery,
};
use crate::github_scale_set_protocol::{ScaleSetRunnerName, ScaleSetRunnerReference};
use crate::owned_linux_jit_runtime::OwnedLinuxJitRuntime;
use crate::process::TimedCommandExecutor;
use crate::unix_personal_worker_store::UnixPersonalWorkerStore;
use crate::unix_personal_worker_store::disposable_runner_transaction::DisposableRunnerTransactionOutcome;

const LIVE_ADMISSION_MILLIS: u64 = 30_000;

pub(crate) trait DisposableWorkerServiceBridge: DeliveryBridge {
    fn observe_runner(
        &mut self,
        runner_name: &ScaleSetRunnerName,
    ) -> Result<ScaleSetRunnerLookup, ScaleSetBridgeError>;

    fn generate_jit(
        &mut self,
        runner_name: &ScaleSetRunnerName,
    ) -> Result<ScaleSetJitReceipt, ScaleSetBridgeError>;

    fn remove_runner(
        &mut self,
        runner: &ScaleSetRunnerReference,
    ) -> Result<(), ScaleSetBridgeError>;
}

impl DisposableWorkerServiceBridge for ScaleSetBridgeClient {
    fn observe_runner(
        &mut self,
        runner_name: &ScaleSetRunnerName,
    ) -> Result<ScaleSetRunnerLookup, ScaleSetBridgeError> {
        ScaleSetBridgeClient::observe_runner(self, runner_name)
    }

    fn generate_jit(
        &mut self,
        runner_name: &ScaleSetRunnerName,
    ) -> Result<ScaleSetJitReceipt, ScaleSetBridgeError> {
        ScaleSetBridgeClient::generate_jit(self, runner_name)
    }

    fn remove_runner(
        &mut self,
        runner: &ScaleSetRunnerReference,
    ) -> Result<(), ScaleSetBridgeError> {
        ScaleSetBridgeClient::remove_runner(self, runner)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DisposableWorkerCoordinatorDisposition {
    Idle,
    HostStorageUnavailable,
    /// The Mac's own work holds new workers off; zero capacity was advertised.
    HostYielding(HostYieldReason),
    TemplateAdvanced(DisposableTemplateRuntimeDisposition),
    DeliverySettled {
        acquired: usize,
    },
    DeliveryRecoveryRequired,
    PrecloneCheckpointed {
        attempt_id: String,
        phase: DisposableAttemptPhase,
    },
    CloneCompleted {
        attempt_id: String,
    },
    RegistrationCheckpointed {
        attempt_id: String,
        phase: DisposableAttemptPhase,
    },
    RunnerRegistrationRecovered {
        attempt_id: String,
    },
    RunnerCommandCompleted {
        attempt_id: String,
    },
    CleanupCheckpointed {
        attempt_id: String,
        phase: DisposableAttemptPhase,
    },
    VmDestroyed {
        attempt_id: String,
    },
    RunnerDeleted {
        attempt_id: String,
    },
    CapacityReleased {
        attempt_id: String,
    },
    AttemptRetired {
        attempt_id: String,
    },
}

pub(crate) struct DisposableWorkerCoordinator {
    state_root: PathBuf,
    policy: ScaleSetDeliveryConsumerPolicy,
    host_storage: Box<dyn DisposableHostStorageSource>,
    host_yield: Box<dyn DisposableHostYieldSource>,
}

impl DisposableWorkerCoordinator {
    pub(crate) fn new(
        state_root: impl Into<PathBuf>,
        policy: ScaleSetDeliveryConsumerPolicy,
        host_storage: Box<dyn DisposableHostStorageSource>,
    ) -> Self {
        Self {
            state_root: state_root.into(),
            policy,
            host_storage,
            host_yield: Box::new(NoHostYield),
        }
    }

    /// Advertise zero capacity while the host's own work holds new workers off. A worker that
    /// already has its job keeps it.
    pub(crate) fn with_host_yield(
        mut self,
        host_yield: Box<dyn DisposableHostYieldSource>,
    ) -> Self {
        self.host_yield = host_yield;
        self
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(
        state_root: impl Into<PathBuf>,
        policy: ScaleSetDeliveryConsumerPolicy,
    ) -> Self {
        Self::new(state_root, policy, Box::new(AlwaysAvailableHostStorage))
    }

    pub(crate) fn supervise_once(
        &self,
        bridge: &mut ScaleSetBridgeClient,
        clone_runtime: &DisposableCloneRuntime,
        runner_runtime: &DisposableRunnerRuntime,
        executor: &impl TimedCommandExecutor,
        clock: &impl CloneRuntimeClock,
    ) -> Result<DisposableWorkerCoordinatorDisposition, DisposableWorkerCoordinatorError> {
        self.supervise_with_bridge(bridge, clone_runtime, runner_runtime, executor, clock)
    }

    pub(crate) fn supervise_with_bridge<B: DisposableWorkerServiceBridge>(
        &self,
        bridge: &mut B,
        clone_runtime: &DisposableCloneRuntime,
        runner_runtime: &DisposableRunnerRuntime,
        executor: &impl TimedCommandExecutor,
        clock: &impl CloneRuntimeClock,
    ) -> Result<DisposableWorkerCoordinatorDisposition, DisposableWorkerCoordinatorError> {
        self.supervise_backend(
            bridge,
            PhysicalBackend::Lima(clone_runtime),
            runner_runtime,
            executor,
            clock,
        )
    }

    pub(crate) fn supervise_owned_linux_with_bridge<B: DisposableWorkerServiceBridge>(
        &self,
        bridge: &mut B,
        runtime: &OwnedLinuxJitRuntime,
        runner_runtime: &DisposableRunnerRuntime,
        executor: &impl TimedCommandExecutor,
        clock: &impl CloneRuntimeClock,
    ) -> Result<DisposableWorkerCoordinatorDisposition, DisposableWorkerCoordinatorError> {
        self.supervise_backend(
            bridge,
            PhysicalBackend::OwnedLinux(runtime),
            runner_runtime,
            executor,
            clock,
        )
    }

    fn supervise_backend<B: DisposableWorkerServiceBridge>(
        &self,
        bridge: &mut B,
        backend: PhysicalBackend<'_>,
        runner_runtime: &DisposableRunnerRuntime,
        executor: &impl TimedCommandExecutor,
        clock: &impl CloneRuntimeClock,
    ) -> Result<DisposableWorkerCoordinatorDisposition, DisposableWorkerCoordinatorError> {
        if has_delivery_recovery(&self.state_root)? {
            let observed_at = clock
                .epoch_millis()
                .map_err(|_| coordinator_error("disposable_clock_unavailable"))?;
            return consume_with_bridge_capacity(
                &self.state_root,
                &self.policy,
                bridge,
                observed_at,
                0,
            )
            .map_err(|error| coordinator_error(error.code()))
            .and_then(map_delivery_disposition);
        }
        let catalog = load_catalog(&self.state_root)?;
        if catalog.active().len() > usize::from(self.policy.host_budget().max_workers()) {
            return Err(coordinator_error("disposable_capacity_invariant_violated"));
        }

        if catalog.active().is_empty()
            && let PhysicalBackend::Lima(clone_runtime) = backend
        {
            let template = clone_runtime
                .reconcile_template_once(executor, clock)
                .map_err(map_clone_error)?;
            if template != DisposableTemplateRuntimeDisposition::Satisfied {
                return Ok(DisposableWorkerCoordinatorDisposition::TemplateAdvanced(
                    template,
                ));
            }
        }

        let selected_at = clock
            .epoch_millis()
            .map_err(|_| coordinator_error("disposable_clock_unavailable"))?;
        let operation_owns_live_poll = select_attempt(&catalog, selected_at)?
            .map(operation_for)
            .transpose()?
            .is_some_and(|operation| {
                matches!(
                    operation,
                    CoordinatorOperation::ExecuteClone
                        | CoordinatorOperation::CheckpointRegistration
                )
            });
        if !operation_owns_live_poll {
            // The host is asked only while a slot is free: on a one-worker host that is exactly
            // while no attempt is active. While it yields, no new worker is admitted; attempts
            // that already hold a job keep it.
            let has_free_slot =
                catalog.active().len() < usize::from(self.policy.host_budget().max_workers());
            let host_yield = if has_free_slot {
                self.host_yield.host_yield()
            } else {
                None
            };
            let available_capacity = advertised_capacity(
                &catalog,
                &self.policy,
                self.host_storage.as_ref(),
                host_yield.is_some(),
            );
            let observed_at = clock
                .epoch_millis()
                .map_err(|_| coordinator_error("disposable_clock_unavailable"))?;
            let delivery = if available_capacity > 0 {
                consume_with_bridge_capacity_at_revision(
                    &self.state_root,
                    &self.policy,
                    bridge,
                    observed_at,
                    available_capacity,
                    catalog.revision(),
                )
            } else {
                consume_with_bridge_capacity(
                    &self.state_root,
                    &self.policy,
                    bridge,
                    observed_at,
                    available_capacity,
                )
            };
            match delivery.map_err(|error| coordinator_error(error.code()))? {
                ScaleSetDeliveryControllerDisposition::Settled { acquired } => {
                    return Ok(DisposableWorkerCoordinatorDisposition::DeliverySettled {
                        acquired,
                    });
                }
                ScaleSetDeliveryControllerDisposition::RecoveryRequired => {
                    return Ok(DisposableWorkerCoordinatorDisposition::DeliveryRecoveryRequired);
                }
                ScaleSetDeliveryControllerDisposition::Idle => {}
            }

            if let Some(reason) = host_yield
                && catalog.active().is_empty()
            {
                return Ok(DisposableWorkerCoordinatorDisposition::HostYielding(reason));
            }
            if catalog.active().is_empty() && available_capacity == 0 {
                return Ok(DisposableWorkerCoordinatorDisposition::HostStorageUnavailable);
            }
        }

        let catalog = load_catalog(&self.state_root)?;
        let now = clock
            .epoch_millis()
            .map_err(|_| coordinator_error("disposable_clock_unavailable"))?;
        let Some(reservation) = select_attempt(&catalog, now)? else {
            return Ok(DisposableWorkerCoordinatorDisposition::Idle);
        };
        let attempt_id = reservation.attempt().attempt_id().clone();
        if requires_post_start_deadline_cleanup(reservation, now) {
            let mut durable = DisposableAttemptCatalog::new(open_catalog(&self.state_root)?);
            let (next, _) = durable
                .transition(
                    catalog.revision(),
                    &attempt_id,
                    reservation.attempt().revision(),
                    DisposableAttemptCatalogAction::BeginCleanup,
                )
                .map_err(|_| coordinator_error("disposable_deadline_cleanup_checkpoint_failed"))?;
            let phase = next
                .find_active(&attempt_id)
                .ok_or_else(|| coordinator_error("disposable_attempt_missing_after_deadline"))?
                .attempt()
                .phase();
            return Ok(
                DisposableWorkerCoordinatorDisposition::CleanupCheckpointed {
                    attempt_id: attempt_id.as_str().to_owned(),
                    phase,
                },
            );
        }
        match operation_for(reservation)? {
            CoordinatorOperation::AuthorizeClone => {
                let mut store = open_catalog(&self.state_root)?;
                let outcome = match backend {
                    PhysicalBackend::Lima(clone_runtime) => store
                        .authorize_disposable_clone_transaction(
                            clone_runtime,
                            &attempt_id,
                            executor,
                            clock,
                        ),
                    PhysicalBackend::OwnedLinux(runtime) => store
                        .authorize_owned_linux_task_transaction(
                            runtime,
                            &attempt_id,
                            executor,
                            clock,
                        ),
                }
                .map_err(map_clone_error)?;
                map_clone_outcome(outcome)
            }
            CoordinatorOperation::ExecuteClone => {
                self.with_live_admission(bridge, clock, |admission| {
                    let mut store = open_catalog(&self.state_root)?;
                    match backend {
                        PhysicalBackend::Lima(clone_runtime) => store
                            .execute_disposable_clone_transaction(
                                clone_runtime,
                                &attempt_id,
                                admission,
                                executor,
                                clock,
                            ),
                        PhysicalBackend::OwnedLinux(runtime) => store
                            .execute_owned_linux_task_transaction(
                                runtime,
                                &attempt_id,
                                admission,
                                executor,
                                clock,
                            ),
                    }
                    .map_err(map_clone_error)
                })
            }
            CoordinatorOperation::CheckpointRegistration => {
                self.with_live_admission(bridge, clock, |admission| {
                    let mut store = open_catalog(&self.state_root)?;
                    match backend {
                        PhysicalBackend::Lima(clone_runtime) => store
                            .checkpoint_disposable_registration_transaction(
                                clone_runtime,
                                &attempt_id,
                                admission,
                                executor,
                                clock,
                            ),
                        PhysicalBackend::OwnedLinux(runtime) => store
                            .checkpoint_owned_linux_registration_transaction(
                                runtime,
                                &attempt_id,
                                admission,
                                executor,
                                clock,
                            ),
                    }
                    .map_err(map_clone_error)
                })
            }
            CoordinatorOperation::RunRunner => {
                let mut source = LiveRunnerSource { bridge };
                let mut store = open_catalog(&self.state_root)?;
                let outcome = match backend {
                    PhysicalBackend::Lima(clone_runtime) => store
                        .execute_disposable_runner_transaction(
                            runner_runtime,
                            clone_runtime,
                            &attempt_id,
                            &mut source,
                            executor,
                            clock,
                        ),
                    PhysicalBackend::OwnedLinux(runtime) => store
                        .execute_disposable_runner_transaction(
                            runner_runtime,
                            runtime,
                            &attempt_id,
                            &mut source,
                            executor,
                            clock,
                        ),
                }
                .map_err(map_runner_error)?;
                match outcome {
                    DisposableRunnerTransactionOutcome::RegistrationRecovered { attempt_id } => Ok(
                        DisposableWorkerCoordinatorDisposition::RunnerRegistrationRecovered {
                            attempt_id,
                        },
                    ),
                    DisposableRunnerTransactionOutcome::CommandCompleted(receipt) => Ok(
                        DisposableWorkerCoordinatorDisposition::RunnerCommandCompleted {
                            attempt_id: receipt.attempt_id().as_str().to_owned(),
                        },
                    ),
                }
            }
            CoordinatorOperation::Cleanup => {
                let mut source = LiveRunnerSource { bridge };
                let mut store = open_catalog(&self.state_root)?;
                let outcome = match backend {
                    PhysicalBackend::Lima(clone_runtime) => store
                        .execute_disposable_cleanup_transaction(
                            clone_runtime,
                            &attempt_id,
                            &mut source,
                            executor,
                            clock,
                        ),
                    PhysicalBackend::OwnedLinux(runtime) => store
                        .execute_owned_linux_cleanup_transaction(
                            runtime,
                            &attempt_id,
                            &mut source,
                            executor,
                        ),
                }
                .map_err(map_clone_error)?;
                map_cleanup_outcome(outcome)
            }
            CoordinatorOperation::Wait => Ok(DisposableWorkerCoordinatorDisposition::Idle),
        }
    }

    fn with_live_admission<B, C, F>(
        &self,
        bridge: &mut B,
        clock: &C,
        transaction: F,
    ) -> Result<DisposableWorkerCoordinatorDisposition, DisposableWorkerCoordinatorError>
    where
        B: DisposableWorkerServiceBridge,
        C: CloneRuntimeClock,
        F: FnOnce(
            &LiveScaleSetAdmission<'_, B, C>,
        )
            -> Result<DisposableCloneTransactionOutcome, DisposableWorkerCoordinatorError>,
    {
        let admission = LiveScaleSetAdmission::new(bridge, clock, self.host_storage.as_ref());
        let result = transaction(&admission);
        let pending = admission.take_pending();
        drop(admission);
        if let Some((delivery, observed_at)) = pending {
            return reconcile_and_ack_delivery(
                &self.state_root,
                &self.policy,
                bridge,
                delivery,
                observed_at,
            )
            .map_err(|error| coordinator_error(error.code()))
            .and_then(map_delivery_disposition);
        }
        match result {
            Err(error) if error.code() == HOST_STORAGE_UNAVAILABLE_CODE => {
                Ok(DisposableWorkerCoordinatorDisposition::HostStorageUnavailable)
            }
            result => map_clone_outcome(result?),
        }
    }
}

#[derive(Clone, Copy)]
enum PhysicalBackend<'a> {
    Lima(&'a DisposableCloneRuntime),
    OwnedLinux(&'a OwnedLinuxJitRuntime),
}

/// New workers this host can take now: zero while it yields, else the smallest of the slots left
/// under the worker ceiling, the workers the host budget still fits beside the reserved ones, and
/// the workers host storage has room for beside every occupied clone. On a one-worker host this
/// is one exactly when the catalog is empty and storage admits a worker, as before.
fn advertised_capacity(
    catalog: &DisposableAttemptCatalogDocument,
    policy: &ScaleSetDeliveryConsumerPolicy,
    host_storage: &dyn DisposableHostStorageSource,
    host_yields: bool,
) -> u16 {
    if host_yields {
        return 0;
    }
    let budget = policy.host_budget();
    let Ok(occupied) = u16::try_from(catalog.active().len()) else {
        return 0;
    };
    let slots = budget.max_workers().saturating_sub(occupied);
    if slots == 0 {
        return 0;
    }
    let Ok(mut usage) = catalog.host_usage() else {
        return 0;
    };
    let mut fits = 0_u16;
    while fits < slots && budget.admits_another(usage, policy.resources()) {
        fits += 1;
        let Some(next) = add_worker(usage, policy.resources()) else {
            break;
        };
        usage = next;
    }
    if fits == 0 {
        return 0;
    }
    match host_storage.new_worker_room(occupied) {
        Ok(room) => fits.min(room),
        Err(error) => {
            debug_assert_eq!(error.code(), HOST_STORAGE_UNAVAILABLE_CODE);
            0
        }
    }
}

fn add_worker(
    usage: crate::disposable_worker_reconciler::DisposableHostUsage,
    request: crate::disposable_worker_reconciler::DisposableWorkerResources,
) -> Option<crate::disposable_worker_reconciler::DisposableHostUsage> {
    let used = usage.resources();
    let resources = crate::disposable_worker_reconciler::DisposableWorkerResources::new(
        used.cpu_millis().checked_add(request.cpu_millis())?,
        used.memory_bytes().checked_add(request.memory_bytes())?,
        used.disk_bytes().checked_add(request.disk_bytes())?,
    )
    .ok()?;
    crate::disposable_worker_reconciler::DisposableHostUsage::new(
        usage.workers().checked_add(1)?,
        resources,
    )
    .ok()
}

/// The attempt this step advances: the first, in catalog order, with work to do now (a post-start
/// deadline cleanup or any operation but waiting), else the first attempt. With one attempt this
/// is always that attempt, as before.
fn select_attempt(
    catalog: &DisposableAttemptCatalogDocument,
    now: EpochMillis,
) -> Result<Option<&DisposableAttemptReservation>, DisposableWorkerCoordinatorError> {
    for reservation in catalog.active() {
        if requires_post_start_deadline_cleanup(reservation, now)
            || operation_for(reservation)? != CoordinatorOperation::Wait
        {
            return Ok(Some(reservation));
        }
    }
    Ok(catalog.active().first())
}

fn host_storage_room_for_one(host_storage: &dyn DisposableHostStorageSource, others: u16) -> bool {
    match host_storage.new_worker_room(others) {
        Ok(room) => room >= 1,
        Err(error) => {
            debug_assert_eq!(error.code(), HOST_STORAGE_UNAVAILABLE_CODE);
            false
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CoordinatorOperation {
    AuthorizeClone,
    ExecuteClone,
    CheckpointRegistration,
    RunRunner,
    Cleanup,
    Wait,
}

fn requires_post_start_deadline_cleanup(
    reservation: &DisposableAttemptReservation,
    now: EpochMillis,
) -> bool {
    let attempt = reservation.attempt();
    attempt.runner_start_started()
        && now > attempt.not_after()
        && matches!(
            attempt.phase(),
            DisposableAttemptPhase::Registering
                | DisposableAttemptPhase::Waiting
                | DisposableAttemptPhase::Assigned
                | DisposableAttemptPhase::Running
        )
}

fn operation_for(
    reservation: &DisposableAttemptReservation,
) -> Result<CoordinatorOperation, DisposableWorkerCoordinatorError> {
    let attempt = reservation.attempt();
    match attempt.phase() {
        DisposableAttemptPhase::Reserved => Ok(CoordinatorOperation::AuthorizeClone),
        DisposableAttemptPhase::CloneAuthorized => Ok(CoordinatorOperation::ExecuteClone),
        DisposableAttemptPhase::CloneStarted => Ok(CoordinatorOperation::CheckpointRegistration),
        DisposableAttemptPhase::Registering | DisposableAttemptPhase::Assigned
            if !attempt.runner_start_started() && attempt.runner_id().is_some() =>
        {
            Ok(CoordinatorOperation::Cleanup)
        }
        DisposableAttemptPhase::Registering | DisposableAttemptPhase::Assigned
            if !attempt.runner_start_started() =>
        {
            Ok(CoordinatorOperation::RunRunner)
        }
        DisposableAttemptPhase::Terminal
        | DisposableAttemptPhase::Destroying
        | DisposableAttemptPhase::Deregistering
        | DisposableAttemptPhase::Releasing
        | DisposableAttemptPhase::UnprovisionedReleasing
        | DisposableAttemptPhase::Complete => Ok(CoordinatorOperation::Cleanup),
        DisposableAttemptPhase::Registering
        | DisposableAttemptPhase::Waiting
        | DisposableAttemptPhase::Assigned
        | DisposableAttemptPhase::Running => Ok(CoordinatorOperation::Wait),
        DisposableAttemptPhase::Provisioning => Err(coordinator_error(
            "disposable_legacy_provisioning_recovery_required",
        )),
    }
}

struct LiveScaleSetAdmission<'a, B, C> {
    bridge: RefCell<&'a mut B>,
    clock: &'a C,
    host_storage: &'a dyn DisposableHostStorageSource,
    pending: RefCell<Option<(ScaleSetDelivery, EpochMillis)>>,
}

impl<'a, B, C> LiveScaleSetAdmission<'a, B, C> {
    fn new(
        bridge: &'a mut B,
        clock: &'a C,
        host_storage: &'a dyn DisposableHostStorageSource,
    ) -> Self {
        Self {
            bridge: RefCell::new(bridge),
            clock,
            host_storage,
            pending: RefCell::new(None),
        }
    }

    fn take_pending(&self) -> Option<(ScaleSetDelivery, EpochMillis)> {
        self.pending.borrow_mut().take()
    }
}

impl<B, C> admission_seal::Sealed for LiveScaleSetAdmission<'_, B, C> {}

impl<B: DeliveryBridge, C: CloneRuntimeClock> DisposableCloneAdmissionSource
    for LiveScaleSetAdmission<'_, B, C>
{
    fn observe(
        &self,
        catalog: &DisposableAttemptCatalogDocument,
        reservation: &DisposableAttemptReservation,
    ) -> Result<DisposableCloneAdmissionObservation, DisposableCloneRuntimeError> {
        if self.pending.borrow().is_some() {
            return Err(DisposableCloneRuntimeError::recovery(
                "clone_scale_set_message_pending",
            ));
        }
        let poll =
            self.bridge.borrow_mut().poll(0).map_err(|_| {
                DisposableCloneRuntimeError::observation("clone_scale_set_poll_failed")
            })?;
        let observed_at = self
            .clock
            .epoch_millis()
            .map_err(|_| DisposableCloneRuntimeError::observation("clone_clock_unavailable"))?;
        let delivery = match ScaleSetDelivery::from_bridge_poll(&poll) {
            Ok(delivery) => delivery,
            Err(_) => {
                self.bridge.borrow_mut().poison();
                return Err(DisposableCloneRuntimeError::recovery(
                    "clone_scale_set_message_invalid",
                ));
            }
        };
        if let Some(delivery) = delivery {
            self.pending.replace(Some((delivery, observed_at)));
            return Err(DisposableCloneRuntimeError::recovery(
                "clone_scale_set_message_pending",
            ));
        }
        // This clone is admitted beside every other active attempt on the host; on a one-worker
        // host there are none and this is the plain storage check.
        let others = u16::try_from(catalog.active().len().saturating_sub(1)).unwrap_or(u16::MAX);
        if requires_host_storage(reservation)
            && !host_storage_room_for_one(self.host_storage, others)
        {
            return Err(DisposableCloneRuntimeError::observation(
                HOST_STORAGE_UNAVAILABLE_CODE,
            ));
        }
        let expires_at = observed_at
            .get()
            .checked_add(LIVE_ADMISSION_MILLIS)
            .and_then(|value| EpochMillis::new(value).ok())
            .ok_or_else(|| DisposableCloneRuntimeError::observation("clone_clock_unavailable"))?;
        let capacity_reserved = catalog
            .find_active(reservation.attempt().attempt_id())
            .is_some_and(|current| current == reservation)
            && reservation.attempt().phase() != DisposableAttemptPhase::Complete
            && catalog.host_usage().is_ok_and(|usage| usage.workers() >= 1);
        Ok(DisposableCloneAdmissionObservation::new(
            catalog,
            reservation,
            observed_at,
            expires_at,
            capacity_reserved,
            false,
        ))
    }
}

fn requires_host_storage(reservation: &DisposableAttemptReservation) -> bool {
    let attempt = reservation.attempt();
    matches!(
        attempt.phase(),
        DisposableAttemptPhase::Reserved | DisposableAttemptPhase::CloneAuthorized
    ) || (attempt.phase() == DisposableAttemptPhase::CloneStarted
        && attempt.vm_identity().is_none())
}

#[cfg(test)]
struct AlwaysAvailableHostStorage;

#[cfg(test)]
impl DisposableHostStorageSource for AlwaysAvailableHostStorage {
    fn admits_new_worker(
        &self,
    ) -> Result<bool, crate::disposable_host_storage::DisposableHostStorageError> {
        Ok(true)
    }
}

struct LiveRunnerSource<'a, B> {
    bridge: &'a mut B,
}

impl<B: DisposableWorkerServiceBridge> DisposableRunnerRegistrationSource
    for LiveRunnerSource<'_, B>
{
    fn observe_runner(
        &mut self,
        runner_name: &ScaleSetRunnerName,
    ) -> Result<ScaleSetRunnerLookup, DisposableRunnerRuntimeError> {
        self.bridge
            .observe_runner(runner_name)
            .map_err(|_| DisposableRunnerRuntimeError::bridge("runner_observation_failed"))
    }

    fn generate_jit(
        &mut self,
        runner_name: &ScaleSetRunnerName,
    ) -> Result<ScaleSetJitReceipt, DisposableRunnerRuntimeError> {
        self.bridge
            .generate_jit(runner_name)
            .map_err(|_| DisposableRunnerRuntimeError::bridge("runner_jit_generation_failed"))
    }
}

impl<B: DisposableWorkerServiceBridge> DisposableCleanupRunnerSource for LiveRunnerSource<'_, B> {
    fn observe_runner(
        &mut self,
        runner_name: &ScaleSetRunnerName,
    ) -> Result<ScaleSetRunnerLookup, DisposableCloneRuntimeError> {
        self.bridge.observe_runner(runner_name).map_err(|_| {
            DisposableCloneRuntimeError::observation("cleanup_runner_observation_failed")
        })
    }

    fn remove_runner(
        &mut self,
        runner: &ScaleSetRunnerReference,
    ) -> Result<(), DisposableCloneRuntimeError> {
        self.bridge
            .remove_runner(runner)
            .map_err(|_| DisposableCloneRuntimeError::recovery("cleanup_runner_delete_failed"))
    }
}

fn load_catalog(
    root: &Path,
) -> Result<DisposableAttemptCatalogDocument, DisposableWorkerCoordinatorError> {
    let store = open_catalog(root)?;
    DisposableAttemptCatalog::new(store)
        .load()
        .map_err(|_| coordinator_error("disposable_catalog_unavailable"))
}

fn has_delivery_recovery(root: &Path) -> Result<bool, DisposableWorkerCoordinatorError> {
    let store = UnixPersonalWorkerStore::open_or_create_scale_set_delivery_controller(root)
        .map_err(|_| coordinator_error("disposable_delivery_recovery_unavailable"))?;
    store
        .load_scale_set_delivery_recovery()
        .map(|current| current.is_some())
        .map_err(|_| coordinator_error("disposable_delivery_recovery_unavailable"))
}

fn open_catalog(root: &Path) -> Result<UnixPersonalWorkerStore, DisposableWorkerCoordinatorError> {
    UnixPersonalWorkerStore::open_or_create_disposable_catalog(root)
        .map_err(|_| coordinator_error("disposable_catalog_unavailable"))
}

fn map_clone_outcome(
    outcome: DisposableCloneTransactionOutcome,
) -> Result<DisposableWorkerCoordinatorDisposition, DisposableWorkerCoordinatorError> {
    Ok(match outcome {
        DisposableCloneTransactionOutcome::PrecloneCheckpointed { attempt_id, phase } => {
            DisposableWorkerCoordinatorDisposition::PrecloneCheckpointed { attempt_id, phase }
        }
        DisposableCloneTransactionOutcome::RegistrationCheckpointed { attempt_id, phase } => {
            DisposableWorkerCoordinatorDisposition::RegistrationCheckpointed { attempt_id, phase }
        }
        DisposableCloneTransactionOutcome::Completed(receipt) => {
            DisposableWorkerCoordinatorDisposition::CloneCompleted {
                attempt_id: receipt.attempt_id().to_owned(),
            }
        }
    })
}

fn map_cleanup_outcome(
    outcome: DisposableCleanupTransactionOutcome,
) -> Result<DisposableWorkerCoordinatorDisposition, DisposableWorkerCoordinatorError> {
    Ok(match outcome {
        DisposableCleanupTransactionOutcome::CleanupCheckpointed { attempt_id, phase } => {
            DisposableWorkerCoordinatorDisposition::CleanupCheckpointed { attempt_id, phase }
        }
        DisposableCleanupTransactionOutcome::VmDestroyed { attempt_id } => {
            DisposableWorkerCoordinatorDisposition::VmDestroyed { attempt_id }
        }
        DisposableCleanupTransactionOutcome::RunnerDeleted { attempt_id } => {
            DisposableWorkerCoordinatorDisposition::RunnerDeleted { attempt_id }
        }
        DisposableCleanupTransactionOutcome::CapacityReleased { attempt_id } => {
            DisposableWorkerCoordinatorDisposition::CapacityReleased { attempt_id }
        }
        DisposableCleanupTransactionOutcome::AttemptRetired { attempt_id } => {
            DisposableWorkerCoordinatorDisposition::AttemptRetired { attempt_id }
        }
    })
}

fn map_delivery_disposition(
    disposition: ScaleSetDeliveryControllerDisposition,
) -> Result<DisposableWorkerCoordinatorDisposition, DisposableWorkerCoordinatorError> {
    Ok(match disposition {
        ScaleSetDeliveryControllerDisposition::Idle => DisposableWorkerCoordinatorDisposition::Idle,
        ScaleSetDeliveryControllerDisposition::Settled { acquired } => {
            DisposableWorkerCoordinatorDisposition::DeliverySettled { acquired }
        }
        ScaleSetDeliveryControllerDisposition::RecoveryRequired => {
            DisposableWorkerCoordinatorDisposition::DeliveryRecoveryRequired
        }
    })
}

fn map_clone_error(error: DisposableCloneRuntimeError) -> DisposableWorkerCoordinatorError {
    coordinator_error(error.code())
}

fn map_runner_error(error: DisposableRunnerRuntimeError) -> DisposableWorkerCoordinatorError {
    coordinator_error(error.code())
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct DisposableWorkerCoordinatorError {
    code: &'static str,
}

impl DisposableWorkerCoordinatorError {
    pub(crate) const fn code(self) -> &'static str {
        self.code
    }
}

impl fmt::Debug for DisposableWorkerCoordinatorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DisposableWorkerCoordinatorError")
            .field("code", &self.code)
            .finish()
    }
}

impl fmt::Display for DisposableWorkerCoordinatorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code)
    }
}

impl std::error::Error for DisposableWorkerCoordinatorError {}

const fn coordinator_error(code: &'static str) -> DisposableWorkerCoordinatorError {
    DisposableWorkerCoordinatorError { code }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::io;

    use super::*;
    use crate::disposable_prepared_template::current_disposable_prepared_template;
    use crate::disposable_worker_reconciler::{
        CapacityClaimId, DisposableAttemptId, DisposableVmId, DisposableWorkerResources,
    };
    use crate::github_scale_set_bridge::{
        ScaleSetBridgeEvent, ScaleSetBridgeJobEvidence, ScaleSetBridgePoll, ScaleSetStatistics,
    };
    use crate::github_scale_set_protocol::{
        ScaleSetJobId, ScaleSetRunnerId, ScaleSetRunnerRequestId,
    };
    use crate::lima_observation::LimaObservationClock;

    const GIB: u64 = 1 << 30;

    struct FixedClock;

    struct FixedHostStorage(
        Result<bool, crate::disposable_host_storage::DisposableHostStorageError>,
    );

    impl DisposableHostStorageSource for FixedHostStorage {
        fn admits_new_worker(
            &self,
        ) -> Result<bool, crate::disposable_host_storage::DisposableHostStorageError> {
            self.0
        }
    }

    impl LimaObservationClock for FixedClock {
        fn unix_seconds(&self) -> io::Result<u64> {
            Ok(1_900_000_000)
        }
    }

    impl CloneRuntimeClock for FixedClock {
        fn epoch_millis(&self) -> io::Result<EpochMillis> {
            EpochMillis::new(1_900_000_000_000).map_err(io::Error::other)
        }
    }

    struct FakeDeliveryBridge {
        polls: VecDeque<ScaleSetBridgePoll>,
        capacities: Vec<u16>,
        poisoned: bool,
    }

    impl DeliveryBridge for FakeDeliveryBridge {
        fn resume_after(&mut self, _: u32) -> Result<(), ScaleSetBridgeError> {
            Ok(())
        }

        fn poll(
            &mut self,
            available_capacity: u16,
        ) -> Result<ScaleSetBridgePoll, ScaleSetBridgeError> {
            self.capacities.push(available_capacity);
            self.polls
                .pop_front()
                .ok_or_else(|| ScaleSetBridgeError::new("missing_fake_poll"))
        }

        fn ack(&mut self, _: u32) -> Result<Vec<u64>, ScaleSetBridgeError> {
            Ok(Vec::new())
        }

        fn acquire(
            &mut self,
            _: &[ScaleSetRunnerRequestId],
        ) -> Result<Vec<ScaleSetRunnerRequestId>, ScaleSetBridgeError> {
            Ok(Vec::new())
        }

        fn poison(&mut self) {
            self.poisoned = true;
        }
    }

    fn reservation() -> DisposableAttemptReservation {
        DisposableAttemptReservation::new(
            crate::disposable_attempt_state::DisposableAttemptState::reserved(
                DisposableAttemptId::parse("attempt-1").unwrap(),
                CapacityClaimId::parse("claim-1").unwrap(),
                DisposableVmId::parse("smol-1").unwrap(),
                ScaleSetRunnerName::parse("smol-1").unwrap(),
                ScaleSetRunnerRequestId::new(41).unwrap(),
                EpochMillis::new(1_900_000_600_000).unwrap(),
            ),
            DisposableWorkerResources::new(4_000, 8 * GIB, 80 * GIB).unwrap(),
            current_disposable_prepared_template()
                .unwrap()
                .identity()
                .unwrap(),
        )
        .unwrap()
    }

    fn catalog() -> DisposableAttemptCatalogDocument {
        DisposableAttemptCatalogDocument::empty()
            .reserve(reservation())
            .unwrap()
    }
    fn worker_resources() -> DisposableWorkerResources {
        DisposableWorkerResources::new(4_000, 8 * GIB, 80 * GIB).unwrap()
    }

    fn single_policy() -> ScaleSetDeliveryConsumerPolicy {
        ScaleSetDeliveryConsumerPolicy::new_owned_linux(
            7,
            "repo",
            "owner",
            &["self-hosted".to_owned()],
            worker_resources(),
            current_disposable_prepared_template()
                .unwrap()
                .identity()
                .unwrap(),
        )
        .unwrap()
    }

    /// A host ceiling of `max_workers` whose memory total fits `memory_fits` workers.
    fn budget_policy(max_workers: u16, memory_fits: u64) -> ScaleSetDeliveryConsumerPolicy {
        let total = DisposableWorkerResources::new(
            64_000,
            memory_fits * 8 * GIB,
            u64::from(max_workers) * 80 * GIB,
        )
        .unwrap();
        single_policy()
            .with_host_budget(
                crate::disposable_worker_reconciler::DisposableHostBudget::new(max_workers, total)
                    .unwrap(),
            )
            .unwrap()
    }

    /// Storage with room for `self.0` new workers whatever is occupied.
    struct RoomHostStorage(u16);

    impl DisposableHostStorageSource for RoomHostStorage {
        fn admits_new_worker(
            &self,
        ) -> Result<bool, crate::disposable_host_storage::DisposableHostStorageError> {
            Ok(self.0 > 0)
        }

        fn new_worker_room(
            &self,
            _: u16,
        ) -> Result<u16, crate::disposable_host_storage::DisposableHostStorageError> {
            Ok(self.0)
        }
    }

    /// Storage that holds `self.0` clones in total, occupied ones included.
    struct OccupiedRoomHostStorage(u16);

    impl DisposableHostStorageSource for OccupiedRoomHostStorage {
        fn admits_new_worker(
            &self,
        ) -> Result<bool, crate::disposable_host_storage::DisposableHostStorageError> {
            Ok(self.0 > 0)
        }

        fn new_worker_room(
            &self,
            occupied: u16,
        ) -> Result<u16, crate::disposable_host_storage::DisposableHostStorageError> {
            Ok(self.0.saturating_sub(occupied))
        }
    }

    fn second_reservation() -> DisposableAttemptReservation {
        DisposableAttemptReservation::new(
            crate::disposable_attempt_state::DisposableAttemptState::reserved(
                DisposableAttemptId::parse("attempt-2").unwrap(),
                CapacityClaimId::parse("claim-2").unwrap(),
                DisposableVmId::parse("smol-2").unwrap(),
                ScaleSetRunnerName::parse("smol-2").unwrap(),
                ScaleSetRunnerRequestId::new(42).unwrap(),
                EpochMillis::new(1_900_000_600_000).unwrap(),
            ),
            worker_resources(),
            current_disposable_prepared_template()
                .unwrap()
                .identity()
                .unwrap(),
        )
        .unwrap()
    }

    fn two_reserved_catalog() -> DisposableAttemptCatalogDocument {
        catalog().reserve(second_reservation()).unwrap()
    }

    fn idle_bridge() -> FakeDeliveryBridge {
        FakeDeliveryBridge {
            polls: VecDeque::from([ScaleSetBridgePoll::Idle {
                statistics: statistics(),
            }]),
            capacities: Vec::new(),
            poisoned: false,
        }
    }

    fn runner_registered_catalog() -> DisposableAttemptCatalogDocument {
        register_first(catalog())
    }

    /// Drive the first active attempt to a registered runner, leaving any others as they are.
    fn register_first(
        mut catalog: DisposableAttemptCatalogDocument,
    ) -> DisposableAttemptCatalogDocument {
        let attempt_id = catalog.active()[0].attempt().attempt_id().clone();
        for action in [
            crate::disposable_attempt_catalog::DisposableAttemptCatalogAction::AuthorizeClone,
            crate::disposable_attempt_catalog::DisposableAttemptCatalogAction::RecordCloneStarted,
        ] {
            let revision = catalog.active()[0].attempt().revision();
            catalog = catalog
                .replace_attempt(&attempt_id, revision, action)
                .unwrap();
        }
        let revision = catalog.active()[0].attempt().revision();
        catalog = catalog
            .bind_vm_identity_after_clone(
                &attempt_id,
                revision,
                crate::disposable_worker_reconciler::DisposableVmIdentity::parse(&format!(
                    "sha256:{}",
                    "55".repeat(32)
                ))
                .unwrap(),
            )
            .unwrap();
        for action in [
            crate::disposable_attempt_catalog::DisposableAttemptCatalogAction::BeginRegistration,
            crate::disposable_attempt_catalog::DisposableAttemptCatalogAction::RecordJitGenerationStarted,
        ] {
            let revision = catalog.active()[0].attempt().revision();
            catalog = catalog
                .replace_attempt(&attempt_id, revision, action)
                .unwrap();
        }
        let runner = ScaleSetRunnerReference::new(
            ScaleSetRunnerId::new(77).unwrap(),
            catalog.active()[0].attempt().runner_name().clone(),
        );
        let revision = catalog.active()[0].attempt().revision();
        catalog
            .replace_attempt(
                &attempt_id,
                revision,
                crate::disposable_attempt_catalog::DisposableAttemptCatalogAction::RecordRegistration(
                    runner,
                ),
            )
            .unwrap()
    }

    fn runner_registered_reservation() -> DisposableAttemptReservation {
        runner_registered_catalog().active()[0].clone()
    }

    fn runner_started_reservation() -> DisposableAttemptReservation {
        let catalog = runner_registered_catalog();
        let reservation = &catalog.active()[0];
        catalog
            .replace_attempt(
                reservation.attempt().attempt_id(),
                reservation.attempt().revision(),
                crate::disposable_attempt_catalog::DisposableAttemptCatalogAction::RecordRunnerStartStarted,
            )
            .unwrap()
            .active()[0]
            .clone()
    }

    fn unavailable_storage_error() -> crate::disposable_host_storage::DisposableHostStorageError {
        crate::disposable_host_storage::DisposableHostStorage::new(
            PathBuf::from("/unused"),
            u64::MAX,
        )
        .err()
        .unwrap()
    }

    fn statistics() -> ScaleSetStatistics {
        ScaleSetStatistics {
            available_jobs: 0,
            acquired_jobs: 1,
            assigned_jobs: 0,
            running_jobs: 0,
            registered_runners: 0,
            busy_runners: 0,
            idle_runners: 0,
        }
    }

    #[test]
    fn lost_jit_after_runner_registration_routes_directly_to_cleanup() {
        let reservation = runner_registered_reservation();
        assert!(reservation.attempt().runner_id().is_some());
        assert!(!reservation.attempt().runner_start_started());
        assert_eq!(
            operation_for(&reservation).unwrap(),
            CoordinatorOperation::Cleanup
        );
    }

    #[test]
    fn runner_exit_before_terminal_waits_until_deadline_then_forces_cleanup() {
        let reservation = runner_started_reservation();
        assert_eq!(
            operation_for(&reservation).unwrap(),
            CoordinatorOperation::Wait
        );
        let not_after = reservation.attempt().not_after();
        assert!(!requires_post_start_deadline_cleanup(
            &reservation,
            not_after
        ));
        assert!(requires_post_start_deadline_cleanup(
            &reservation,
            EpochMillis::new(not_after.get() + 1).unwrap()
        ));
    }

    #[test]
    fn live_idle_admission_is_exactly_zero_capacity_and_catalog_bound() {
        let catalog = catalog();
        let reservation = catalog.active().first().unwrap();
        let mut bridge = FakeDeliveryBridge {
            polls: VecDeque::from([ScaleSetBridgePoll::Idle {
                statistics: statistics(),
            }]),
            capacities: Vec::new(),
            poisoned: false,
        };
        let admission =
            LiveScaleSetAdmission::new(&mut bridge, &FixedClock, &AlwaysAvailableHostStorage);
        let observation = admission.observe(&catalog, reservation).unwrap();
        observation
            .validate_for(
                &catalog,
                reservation,
                EpochMillis::new(1_900_000_000_000).unwrap(),
            )
            .unwrap();
        assert!(admission.take_pending().is_none());
        drop(admission);
        assert_eq!(bridge.capacities, [0]);
    }

    #[test]
    fn host_storage_refusal_and_unknown_state_advertise_zero_capacity() {
        // The deployed one-worker host: exactly the old rule.
        let policy = single_policy();
        let empty = DisposableAttemptCatalogDocument::empty();
        assert_eq!(
            advertised_capacity(&empty, &policy, &FixedHostStorage(Ok(true)), false),
            1
        );
        assert_eq!(
            advertised_capacity(&empty, &policy, &FixedHostStorage(Ok(false)), false),
            0
        );
        assert_eq!(
            advertised_capacity(
                &empty,
                &policy,
                &FixedHostStorage(Err(unavailable_storage_error())),
                false
            ),
            0
        );
        assert_eq!(
            advertised_capacity(&catalog(), &policy, &FixedHostStorage(Ok(true)), false),
            0
        );
        // Even a storage source with room for many keeps a one-worker host at one.
        assert_eq!(
            advertised_capacity(&empty, &policy, &RoomHostStorage(9), false),
            1
        );
        assert_eq!(
            advertised_capacity(&catalog(), &policy, &RoomHostStorage(9), false),
            0
        );
    }

    #[test]
    fn three_worker_host_advertises_free_slots_and_admission_refuses_the_third() {
        let policy = budget_policy(3, 3);
        let empty = DisposableAttemptCatalogDocument::empty();
        let one = catalog();
        let two = two_reserved_catalog();
        assert_eq!(two.active().len(), 2);

        // Ceiling 3, plenty of storage: 3, then 2 beside one, then 1 beside two.
        assert_eq!(
            advertised_capacity(&empty, &policy, &RoomHostStorage(9), false),
            3
        );
        assert_eq!(
            advertised_capacity(&one, &policy, &RoomHostStorage(9), false),
            2
        );
        assert_eq!(
            advertised_capacity(&two, &policy, &RoomHostStorage(9), false),
            1
        );
        // Storage room counts every occupied clone: no room beside two refuses the third.
        assert_eq!(
            advertised_capacity(&two, &policy, &OccupiedRoomHostStorage(2), false),
            0
        );
        assert_eq!(
            advertised_capacity(&one, &policy, &OccupiedRoomHostStorage(2), false),
            1
        );
        // The host memory budget fits two workers of this size: the third is refused even
        // though the ceiling is three.
        let memory_bound = budget_policy(3, 2);
        assert_eq!(
            advertised_capacity(&empty, &memory_bound, &RoomHostStorage(9), false),
            2
        );
        assert_eq!(
            advertised_capacity(&two, &memory_bound, &RoomHostStorage(9), false),
            0
        );
        // A source that only knows the single-worker rule never admits a second clone.
        assert_eq!(
            advertised_capacity(&one, &policy, &FixedHostStorage(Ok(true)), false),
            0
        );
    }

    #[test]
    fn host_yield_advertises_zero_new_capacity_on_any_ceiling() {
        let empty = DisposableAttemptCatalogDocument::empty();
        for policy in [single_policy(), budget_policy(3, 3)] {
            assert_eq!(
                advertised_capacity(&empty, &policy, &RoomHostStorage(9), true),
                0
            );
            assert_eq!(
                advertised_capacity(&catalog(), &policy, &RoomHostStorage(9), true),
                0
            );
        }
    }

    #[test]
    fn live_admission_counts_other_clones_against_host_storage() {
        let two = two_reserved_catalog();
        let second = &two.active()[1];
        // Room for exactly one clone beside one other: admitted.
        let mut bridge = idle_bridge();
        let admission =
            LiveScaleSetAdmission::new(&mut bridge, &FixedClock, &OccupiedRoomHostStorage(2));
        let observation = admission.observe(&two, second).unwrap();
        assert!(observation.capacity_reserved());
        drop(admission);
        // No room beside one other: refused.
        let mut bridge = idle_bridge();
        let admission =
            LiveScaleSetAdmission::new(&mut bridge, &FixedClock, &OccupiedRoomHostStorage(1));
        let error = match admission.observe(&two, second) {
            Ok(_) => panic!("a clone without storage room beside the others was admitted"),
            Err(error) => error,
        };
        assert_eq!(error.code(), HOST_STORAGE_UNAVAILABLE_CODE);
    }

    #[test]
    fn select_attempt_advances_the_first_attempt_with_work() {
        let now = EpochMillis::new(1_900_000_000_000).unwrap();
        let single = catalog();
        assert_eq!(
            select_attempt(&single, now).unwrap().unwrap(),
            &single.active()[0]
        );
        // A waiting first attempt does not block a reserved second one.
        let registered = register_first(two_reserved_catalog());
        let first = &registered.active()[0];
        let waiting_then_reserved = registered
            .replace_attempt(
                first.attempt().attempt_id(),
                first.attempt().revision(),
                crate::disposable_attempt_catalog::DisposableAttemptCatalogAction::RecordRunnerStartStarted,
            )
            .unwrap();
        assert_eq!(
            waiting_then_reserved.active()[0]
                .attempt()
                .attempt_id()
                .as_str(),
            "attempt-1"
        );
        assert_eq!(
            operation_for(&waiting_then_reserved.active()[0]).unwrap(),
            CoordinatorOperation::Wait
        );
        assert_eq!(
            select_attempt(&waiting_then_reserved, now)
                .unwrap()
                .unwrap()
                .attempt()
                .attempt_id()
                .as_str(),
            "attempt-2"
        );
        assert!(
            select_attempt(&DisposableAttemptCatalogDocument::empty(), now)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn live_preclone_admission_polls_then_refuses_low_host_storage() {
        let catalog = catalog();
        let reservation = catalog.active().first().unwrap();
        let mut bridge = FakeDeliveryBridge {
            polls: VecDeque::from([ScaleSetBridgePoll::Idle {
                statistics: statistics(),
            }]),
            capacities: Vec::new(),
            poisoned: false,
        };
        let admission =
            LiveScaleSetAdmission::new(&mut bridge, &FixedClock, &FixedHostStorage(Ok(false)));
        let error = match admission.observe(&catalog, reservation) {
            Ok(_) => panic!("low host storage unexpectedly admitted the clone"),
            Err(error) => error,
        };
        assert_eq!(error.code(), HOST_STORAGE_UNAVAILABLE_CODE);
        assert!(admission.take_pending().is_none());
        drop(admission);
        assert_eq!(bridge.capacities, [0]);
    }

    #[test]
    fn storage_gate_ends_after_exact_vm_binding_and_never_applies_to_cleanup() {
        let catalog = catalog();
        let reserved = catalog.active().first().unwrap();
        assert!(requires_host_storage(reserved));
        let authorized = catalog
            .replace_attempt(
                reserved.attempt().attempt_id(),
                reserved.attempt().revision(),
                crate::disposable_attempt_catalog::DisposableAttemptCatalogAction::AuthorizeClone,
            )
            .unwrap();
        let authorized_reservation = authorized.active().first().unwrap();
        assert!(requires_host_storage(authorized_reservation));
        let started = authorized
            .checkpoint_clone_started(
                authorized_reservation.attempt().attempt_id(),
                authorized_reservation.attempt().revision(),
            )
            .unwrap();
        let started_reservation = started.active().first().unwrap();
        assert!(requires_host_storage(started_reservation));
        let bound = started
            .bind_vm_identity_after_clone(
                started_reservation.attempt().attempt_id(),
                started_reservation.attempt().revision(),
                crate::disposable_worker_reconciler::DisposableVmIdentity::parse(&format!(
                    "sha256:{}",
                    "44".repeat(32)
                ))
                .unwrap(),
            )
            .unwrap();
        assert!(!requires_host_storage(bound.active().first().unwrap()));
        let destroying = bound
            .replace_attempt(
                bound.active()[0].attempt().attempt_id(),
                bound.active()[0].attempt().revision(),
                crate::disposable_attempt_catalog::DisposableAttemptCatalogAction::BeginCleanup,
            )
            .unwrap();
        assert!(!requires_host_storage(destroying.active().first().unwrap()));
    }

    #[test]
    fn live_message_is_retained_for_durable_delivery_before_clone_retry() {
        let catalog = catalog();
        let reservation = catalog.active().first().unwrap();
        let event = ScaleSetBridgeEvent::Assigned(ScaleSetBridgeJobEvidence {
            runner_request_id: 41,
            repository: "repo".to_owned(),
            owner: "owner".to_owned(),
            job_id: ScaleSetJobId::parse("job-1").unwrap(),
            workflow_run_id: 7,
            request_labels: vec!["self-hosted".to_owned()],
        });
        let mut bridge = FakeDeliveryBridge {
            polls: VecDeque::from([ScaleSetBridgePoll::Message {
                message_id: 9,
                statistics: statistics(),
                events: vec![event],
            }]),
            capacities: Vec::new(),
            poisoned: false,
        };
        let admission =
            LiveScaleSetAdmission::new(&mut bridge, &FixedClock, &AlwaysAvailableHostStorage);
        let error = match admission.observe(&catalog, reservation) {
            Ok(_) => panic!("a live message must block clone admission"),
            Err(error) => error,
        };
        assert_eq!(error.code(), "clone_scale_set_message_pending");
        let (delivery, observed_at) = admission.take_pending().unwrap();
        assert_eq!(delivery.message_id(), 9);
        assert_eq!(observed_at.get(), 1_900_000_000_000);
        drop(admission);
        assert_eq!(bridge.capacities, [0]);
    }

    #[test]
    fn phase_dispatch_separates_clone_wait_and_unprovisioned_cleanup() {
        let catalog = catalog();
        let reserved = catalog.active().first().unwrap();
        assert_eq!(
            operation_for(reserved).unwrap(),
            CoordinatorOperation::AuthorizeClone
        );
        let authorized_catalog = catalog
            .replace_attempt(
                reserved.attempt().attempt_id(),
                reserved.attempt().revision(),
                crate::disposable_attempt_catalog::DisposableAttemptCatalogAction::AuthorizeClone,
            )
            .unwrap();
        let authorized = authorized_catalog.active().first().unwrap();
        assert_eq!(
            operation_for(authorized).unwrap(),
            CoordinatorOperation::ExecuteClone
        );
        let releasing_catalog = catalog
            .replace_attempt(
                reserved.attempt().attempt_id(),
                reserved.attempt().revision(),
                crate::disposable_attempt_catalog::DisposableAttemptCatalogAction::BeginUnprovisionedRelease,
            )
            .unwrap();
        let releasing = releasing_catalog.active().first().unwrap();
        assert_eq!(
            operation_for(releasing).unwrap(),
            CoordinatorOperation::Cleanup
        );
    }
}
