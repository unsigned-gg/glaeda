#![allow(dead_code)]

use std::collections::BTreeSet;
use std::fmt;
use std::path::Path;

use crate::disposable_attempt_catalog::{
    DisposableAttemptCatalog, DisposableAttemptCatalogRevision,
};
use crate::execution_admission::EpochMillis;
use crate::github_scale_set_bridge::{
    ScaleSetBridgeClient, ScaleSetBridgeError, ScaleSetBridgePoll,
};
use crate::github_scale_set_delivery::ScaleSetDelivery;
use crate::github_scale_set_delivery_consumer::ScaleSetDeliveryConsumerPolicy;
use crate::github_scale_set_delivery_state::{
    ScaleSetDeliveryRecoveryPhase, ScaleSetDeliveryRecoveryState,
};
use crate::github_scale_set_protocol::ScaleSetRunnerRequestId;
use crate::personal_worker_store::PersonalWorkerStoreError;
use crate::unix_personal_worker_store::UnixPersonalWorkerStore;
use crate::unix_personal_worker_store::scale_set_delivery_recovery::ScaleSetExternalTransaction;

/// Narrow bridge surface consumed by the durable controller.
///
/// The production implementation delegates protocol semantics to the pinned official-client
/// bridge. The trait remains private so callers cannot substitute external mutation authority.
pub(crate) trait DeliveryBridge {
    fn resume_after(&mut self, last_acked_message_id: u32) -> Result<(), ScaleSetBridgeError>;
    fn poll(&mut self, available_capacity: u16) -> Result<ScaleSetBridgePoll, ScaleSetBridgeError>;
    fn ack(&mut self, message_id: u32) -> Result<Vec<u64>, ScaleSetBridgeError>;
    fn acquire(
        &mut self,
        request_ids: &[ScaleSetRunnerRequestId],
    ) -> Result<Vec<ScaleSetRunnerRequestId>, ScaleSetBridgeError>;
    fn poison(&mut self);
}

impl DeliveryBridge for ScaleSetBridgeClient {
    fn resume_after(&mut self, last_acked_message_id: u32) -> Result<(), ScaleSetBridgeError> {
        self.resume_after(last_acked_message_id)
    }

    fn poll(&mut self, available_capacity: u16) -> Result<ScaleSetBridgePoll, ScaleSetBridgeError> {
        self.poll(available_capacity)
    }

    fn ack(&mut self, message_id: u32) -> Result<Vec<u64>, ScaleSetBridgeError> {
        self.ack(message_id)
    }

    fn acquire(
        &mut self,
        request_ids: &[ScaleSetRunnerRequestId],
    ) -> Result<Vec<ScaleSetRunnerRequestId>, ScaleSetBridgeError> {
        self.acquire(request_ids)
    }

    fn poison(&mut self) {
        self.poison();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScaleSetDeliveryControllerDisposition {
    Idle,
    Settled { acquired: usize },
    RecoveryRequired,
}

/// Advance at most one exact Scale Set delivery across its external acknowledgement boundary.
///
/// A new message is reconciled and published before acknowledgement begins. A durable
/// `AcknowledgementStarted` with Available work is never replayed through `ack`; a fresh bridge may
/// only invoke standalone acquisition for the exact retained available request IDs. A
/// lifecycle-only message has no acquisition side effect, so a fresh zero-capacity session either
/// re-acknowledges its exact redelivery or confirms its absence before settlement. Empty acquisition
/// replay restores the original cursor and admits only zero-capacity lifecycle evidence; exact
/// assignment or runnerless cancellation then replaces the original fence before its own
/// acknowledgement.
pub(crate) fn consume_scale_set_delivery_once(
    root_path: &Path,
    policy: &ScaleSetDeliveryConsumerPolicy,
    bridge: &mut ScaleSetBridgeClient,
    observed_at: EpochMillis,
) -> Result<ScaleSetDeliveryControllerDisposition, ScaleSetDeliveryControllerError> {
    consume_with_bridge(root_path, policy, bridge, observed_at)
}

fn consume_with_bridge<B: DeliveryBridge>(
    root_path: &Path,
    policy: &ScaleSetDeliveryConsumerPolicy,
    bridge: &mut B,
    observed_at: EpochMillis,
) -> Result<ScaleSetDeliveryControllerDisposition, ScaleSetDeliveryControllerError> {
    consume_with_bridge_capacity(root_path, policy, bridge, observed_at, 1)
}

pub(crate) fn consume_with_bridge_capacity<B: DeliveryBridge>(
    root_path: &Path,
    policy: &ScaleSetDeliveryConsumerPolicy,
    bridge: &mut B,
    observed_at: EpochMillis,
    available_capacity: u16,
) -> Result<ScaleSetDeliveryControllerDisposition, ScaleSetDeliveryControllerError> {
    consume_with_bridge_capacity_inner(
        root_path,
        policy,
        bridge,
        observed_at,
        available_capacity,
        None,
    )
}

pub(crate) fn consume_with_bridge_capacity_at_revision<B: DeliveryBridge>(
    root_path: &Path,
    policy: &ScaleSetDeliveryConsumerPolicy,
    bridge: &mut B,
    observed_at: EpochMillis,
    available_capacity: u16,
    expected_catalog_revision: DisposableAttemptCatalogRevision,
) -> Result<ScaleSetDeliveryControllerDisposition, ScaleSetDeliveryControllerError> {
    consume_with_bridge_capacity_inner(
        root_path,
        policy,
        bridge,
        observed_at,
        available_capacity,
        Some(expected_catalog_revision),
    )
}

fn consume_with_bridge_capacity_inner<B: DeliveryBridge>(
    root_path: &Path,
    policy: &ScaleSetDeliveryConsumerPolicy,
    bridge: &mut B,
    observed_at: EpochMillis,
    available_capacity: u16,
    expected_catalog_revision: Option<DisposableAttemptCatalogRevision>,
) -> Result<ScaleSetDeliveryControllerDisposition, ScaleSetDeliveryControllerError> {
    if available_capacity > policy.host_budget().max_workers() {
        return Err(controller_error("scale_set_capacity_invalid"));
    }
    let mut recovery_store =
        UnixPersonalWorkerStore::open_or_create_scale_set_delivery_controller(root_path)
            .map_err(map_store_error)?;
    if let Some(current) = recovery_store
        .load_scale_set_delivery_recovery()
        .map_err(map_store_error)?
    {
        return resume_delivery(
            root_path,
            policy,
            bridge,
            observed_at,
            &mut recovery_store,
            current,
        );
    }
    drop(recovery_store);

    let poll = bridge.poll(available_capacity).map_err(map_bridge_error)?;
    let delivery = match ScaleSetDelivery::from_bridge_poll(&poll) {
        Ok(Some(delivery)) => delivery,
        Ok(None) => return Ok(ScaleSetDeliveryControllerDisposition::Idle),
        Err(_) => {
            bridge.poison();
            return Err(controller_error("scale_set_delivery_invalid"));
        }
    };
    reconcile_and_ack_delivery_inner(
        root_path,
        policy,
        bridge,
        delivery,
        observed_at,
        expected_catalog_revision,
    )
}

pub(crate) fn reconcile_and_ack_delivery<B: DeliveryBridge>(
    root_path: &Path,
    policy: &ScaleSetDeliveryConsumerPolicy,
    bridge: &mut B,
    delivery: ScaleSetDelivery,
    observed_at: EpochMillis,
) -> Result<ScaleSetDeliveryControllerDisposition, ScaleSetDeliveryControllerError> {
    reconcile_and_ack_delivery_inner(root_path, policy, bridge, delivery, observed_at, None)
}

fn reconcile_and_ack_delivery_inner<B: DeliveryBridge>(
    root_path: &Path,
    policy: &ScaleSetDeliveryConsumerPolicy,
    bridge: &mut B,
    delivery: ScaleSetDelivery,
    observed_at: EpochMillis,
    expected_catalog_revision: Option<DisposableAttemptCatalogRevision>,
) -> Result<ScaleSetDeliveryControllerDisposition, ScaleSetDeliveryControllerError> {
    let publication = (|| {
        let expected_catalog_revision = match expected_catalog_revision {
            Some(revision) => revision,
            None => {
                let catalog_store =
                    UnixPersonalWorkerStore::open_or_create_disposable_catalog(root_path)
                        .map_err(|_| controller_error("scale_set_catalog_unavailable"))?;
                DisposableAttemptCatalog::new(catalog_store)
                    .load()
                    .map_err(|_| controller_error("scale_set_catalog_unavailable"))?
                    .revision()
            }
        };
        let mut paired =
            UnixPersonalWorkerStore::open_or_create_scale_set_reconcile_transaction(root_path)
                .map_err(map_store_error)?;
        let result = paired
            .publish_scale_set_reconciled_delivery(
                expected_catalog_revision,
                policy,
                &delivery,
                observed_at,
            )
            .map_err(map_store_error)?;
        Ok::<_, ScaleSetDeliveryControllerError>((paired, result.1))
    })();
    let (paired, recovery) = match publication {
        Ok(value) => value,
        Err(error) => {
            bridge.poison();
            return Err(error);
        }
    };
    drop(paired);

    let mut recovery_store =
        UnixPersonalWorkerStore::open_or_create_scale_set_delivery_controller(root_path)
            .map_err(map_store_error)?;
    acknowledge_delivery(&mut recovery_store, bridge, recovery)
}

fn resume_delivery<B: DeliveryBridge>(
    root_path: &Path,
    policy: &ScaleSetDeliveryConsumerPolicy,
    bridge: &mut B,
    observed_at: EpochMillis,
    recovery_store: &mut UnixPersonalWorkerStore,
    current: ScaleSetDeliveryRecoveryState,
) -> Result<ScaleSetDeliveryControllerDisposition, ScaleSetDeliveryControllerError> {
    match current.phase() {
        ScaleSetDeliveryRecoveryPhase::Reconciled => {
            // The replay must admit exactly as many Available events as the retained delivery
            // holds: zero or one on a one-worker host, up to the host ceiling otherwise.
            let recovery_capacity = u16::try_from(
                current
                    .delivery()
                    .available_request_ids()
                    .map_err(|_| controller_error("scale_set_delivery_invalid"))?
                    .len(),
            )
            .map_err(|_| controller_error("scale_set_delivery_invalid"))?;
            let poll = bridge.poll(recovery_capacity).map_err(map_bridge_error)?;
            let observed = match ScaleSetDelivery::from_bridge_poll(&poll) {
                Ok(Some(delivery)) => delivery,
                Ok(None) => {
                    bridge.poison();
                    return Err(controller_error("scale_set_delivery_recovery_required"));
                }
                Err(_) => {
                    bridge.poison();
                    return Err(controller_error("scale_set_delivery_invalid"));
                }
            };
            if &observed != current.delivery() {
                bridge.poison();
                return Err(controller_error("scale_set_delivery_recovery_conflict"));
            }
            let mut paired =
                UnixPersonalWorkerStore::open_or_create_scale_set_reconcile_transaction(root_path)
                    .map_err(|error| {
                        bridge.poison();
                        map_store_error(error)
                    })?;
            if let Err(error) = paired.publish_scale_set_reconciled_delivery(
                current.prior_catalog_revision(),
                policy,
                &observed,
                observed_at,
            ) {
                bridge.poison();
                return Err(map_store_error(error));
            }
            drop(paired);
            acknowledge_delivery(recovery_store, bridge, current)
        }
        ScaleSetDeliveryRecoveryPhase::AcknowledgementStarted => {
            recover_acquisition(root_path, policy, recovery_store, bridge, current)
        }
        ScaleSetDeliveryRecoveryPhase::AcquisitionRecoveryObserved { acquired }
            if acquired.is_empty() =>
        {
            observe_lifecycle_resolution(root_path, policy, recovery_store, bridge, current)
        }
        ScaleSetDeliveryRecoveryPhase::AcquisitionRecoveryObserved { acquired } => {
            Ok(settle_delivery(recovery_store, &current, acquired.len())?)
        }
        ScaleSetDeliveryRecoveryPhase::Acknowledged { acquired } => {
            Ok(settle_delivery(recovery_store, &current, acquired.len())?)
        }
        ScaleSetDeliveryRecoveryPhase::LifecycleReconciled { .. }
        | ScaleSetDeliveryRecoveryPhase::LifecycleAcknowledgementStarted { .. } => {
            recover_lifecycle_ack(recovery_store, bridge, current)
        }
        ScaleSetDeliveryRecoveryPhase::LifecycleAcknowledged { resolution } => Ok(settle_delivery(
            recovery_store,
            &current,
            resolution.acquired().len(),
        )?),
        ScaleSetDeliveryRecoveryPhase::SettlementPrepared { acquired, .. } => {
            Ok(settle_delivery(recovery_store, &current, acquired.len())?)
        }
    }
}

fn acknowledge_delivery<B: DeliveryBridge>(
    store: &mut UnixPersonalWorkerStore,
    bridge: &mut B,
    current: ScaleSetDeliveryRecoveryState,
) -> Result<ScaleSetDeliveryControllerDisposition, ScaleSetDeliveryControllerError> {
    let available = current
        .delivery()
        .available_request_ids()
        .map_err(|_| controller_error("scale_set_delivery_invalid"))?
        .into_iter()
        .collect::<BTreeSet<_>>();
    let transaction = store
        .acknowledge_scale_set_delivery_locked(&current, |message_id| {
            let acquired = bridge
                .ack(message_id)
                .map_err(map_bridge_error)?
                .into_iter()
                .map(|value| {
                    ScaleSetRunnerRequestId::new(value)
                        .map_err(|_| controller_error("scale_set_ack_response_invalid"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            if acquired
                .iter()
                .any(|request_id| !available.contains(request_id))
            {
                bridge.poison();
                return Err(controller_error("scale_set_ack_response_invalid"));
            }
            Ok(acquired)
        })
        .map_err(map_store_error)?;
    let acknowledged = match transaction {
        ScaleSetExternalTransaction::Completed(state) => state,
        ScaleSetExternalTransaction::ExternalFailed(error) => {
            // The Go bridge deliberately retains its session-local pending message when either
            // deletion or acquisition fails. That session cannot perform the standalone
            // acquisition recovery required by the durable Started checkpoint, so it must not
            // remain authoritative after an ambiguous acknowledgement outcome.
            bridge.poison();
            return Err(error);
        }
    };
    let ScaleSetDeliveryRecoveryPhase::Acknowledged { acquired } = acknowledged.phase() else {
        return Err(controller_error("scale_set_delivery_recovery_conflict"));
    };
    settle_delivery(store, &acknowledged, acquired.len())
}

fn recover_acquisition<B: DeliveryBridge>(
    root_path: &Path,
    policy: &ScaleSetDeliveryConsumerPolicy,
    store: &mut UnixPersonalWorkerStore,
    bridge: &mut B,
    current: ScaleSetDeliveryRecoveryState,
) -> Result<ScaleSetDeliveryControllerDisposition, ScaleSetDeliveryControllerError> {
    if current
        .delivery()
        .available_request_ids()
        .map_err(|_| controller_error("scale_set_delivery_invalid"))?
        .is_empty()
    {
        return recover_acknowledgement_without_acquisition(store, bridge, current);
    }
    let transaction = store
        .recover_scale_set_acquisition_locked(&current, |available| {
            bridge.acquire(available).map_err(map_bridge_error)
        })
        .map_err(map_store_error)?;
    let successor = match transaction {
        ScaleSetExternalTransaction::Completed(state) => state,
        ScaleSetExternalTransaction::ExternalFailed(error) => return Err(error),
    };
    let ScaleSetDeliveryRecoveryPhase::AcquisitionRecoveryObserved { acquired } = successor.phase()
    else {
        return Err(controller_error("scale_set_delivery_recovery_conflict"));
    };
    if acquired.is_empty() {
        observe_lifecycle_resolution(root_path, policy, store, bridge, successor)
    } else {
        settle_delivery(store, &successor, acquired.len())
    }
}

fn recover_acknowledgement_without_acquisition<B: DeliveryBridge>(
    store: &mut UnixPersonalWorkerStore,
    bridge: &mut B,
    current: ScaleSetDeliveryRecoveryState,
) -> Result<ScaleSetDeliveryControllerDisposition, ScaleSetDeliveryControllerError> {
    let poll = bridge.poll(0).map_err(map_bridge_error)?;
    let observed = match ScaleSetDelivery::from_bridge_poll(&poll) {
        Ok(value) => value,
        Err(_) => {
            bridge.poison();
            return Err(controller_error("scale_set_delivery_invalid"));
        }
    };
    if observed.as_ref() == Some(current.delivery()) {
        let transaction = store
            .retry_scale_set_delivery_acknowledgement_locked(&current, |message_id| {
                let acquired = bridge.ack(message_id).map_err(map_bridge_error)?;
                if !acquired.is_empty() {
                    bridge.poison();
                    return Err(controller_error("scale_set_ack_response_invalid"));
                }
                Ok(Vec::new())
            })
            .map_err(map_store_error)?;
        let acknowledged = match transaction {
            ScaleSetExternalTransaction::Completed(state) => state,
            ScaleSetExternalTransaction::ExternalFailed(error) => {
                bridge.poison();
                return Err(error);
            }
        };
        return settle_delivery(store, &acknowledged, 0);
    }

    let original_absent = observed.as_ref().is_none_or(|delivery| {
        delivery.message_id() > current.delivery().message_id()
            && delivery
                .available_request_ids()
                .is_ok_and(|available| available.is_empty())
    });
    bridge.poison();
    if !original_absent {
        return Err(controller_error("scale_set_delivery_recovery_conflict"));
    }
    let acknowledged = store
        .confirm_scale_set_delivery_acknowledged_locked(&current)
        .map_err(map_store_error)?;
    settle_delivery(store, &acknowledged, 0)
}

fn observe_lifecycle_resolution<B: DeliveryBridge>(
    root_path: &Path,
    policy: &ScaleSetDeliveryConsumerPolicy,
    store: &mut UnixPersonalWorkerStore,
    bridge: &mut B,
    current: ScaleSetDeliveryRecoveryState,
) -> Result<ScaleSetDeliveryControllerDisposition, ScaleSetDeliveryControllerError> {
    bridge
        .resume_after(current.delivery().message_id())
        .map_err(map_bridge_error)?;
    let poll = bridge.poll(0).map_err(map_bridge_error)?;
    let delivery = match ScaleSetDelivery::from_bridge_poll(&poll) {
        Ok(Some(delivery)) => delivery,
        Ok(None) => {
            // The next attempt requires a fresh bridge because resume is intentionally one-shot.
            bridge.poison();
            return Ok(ScaleSetDeliveryControllerDisposition::RecoveryRequired);
        }
        Err(_) => {
            bridge.poison();
            return Err(controller_error("scale_set_delivery_invalid"));
        }
    };
    let mut paired =
        UnixPersonalWorkerStore::open_or_create_scale_set_reconcile_transaction(root_path)
            .map_err(|error| {
                bridge.poison();
                map_store_error(error)
            })?;
    let resolved = paired
        .publish_scale_set_lifecycle_resolution(&current, policy, &delivery)
        .map_err(|error| {
            bridge.poison();
            map_store_error(error)
        })?;
    drop(paired);
    acknowledge_lifecycle(store, bridge, resolved)
}

fn recover_lifecycle_ack<B: DeliveryBridge>(
    store: &mut UnixPersonalWorkerStore,
    bridge: &mut B,
    current: ScaleSetDeliveryRecoveryState,
) -> Result<ScaleSetDeliveryControllerDisposition, ScaleSetDeliveryControllerError> {
    let resolution_delivery = current
        .lifecycle_resolution()
        .ok_or_else(|| controller_error("scale_set_delivery_recovery_conflict"))?
        .delivery()
        .clone();
    let acknowledgement_started = matches!(
        current.phase(),
        ScaleSetDeliveryRecoveryPhase::LifecycleAcknowledgementStarted { .. }
    );
    bridge
        .resume_after(current.delivery().message_id())
        .map_err(map_bridge_error)?;
    let poll = bridge.poll(0).map_err(map_bridge_error)?;
    let redelivered = match ScaleSetDelivery::from_bridge_poll(&poll) {
        Ok(Some(delivery)) => delivery,
        Ok(None) if acknowledgement_started => {
            bridge.poison();
            return confirm_lifecycle_ack(store, current);
        }
        Ok(None) => {
            bridge.poison();
            return Ok(ScaleSetDeliveryControllerDisposition::RecoveryRequired);
        }
        Err(_) => {
            bridge.poison();
            return Err(controller_error("scale_set_delivery_invalid"));
        }
    };
    if redelivered == resolution_delivery {
        return acknowledge_lifecycle(store, bridge, current);
    }
    if acknowledgement_started
        && redelivered.message_id() > resolution_delivery.message_id()
        && redelivered
            .available_request_ids()
            .map_err(|_| controller_error("scale_set_delivery_invalid"))?
            .is_empty()
    {
        // The later message remains unacknowledged. Terminating this bridge makes it redeliver to
        // the next fresh controller session after the prior acknowledgement is settled.
        bridge.poison();
        return confirm_lifecycle_ack(store, current);
    }
    bridge.poison();
    Err(controller_error("scale_set_delivery_recovery_conflict"))
}

fn confirm_lifecycle_ack(
    store: &mut UnixPersonalWorkerStore,
    current: ScaleSetDeliveryRecoveryState,
) -> Result<ScaleSetDeliveryControllerDisposition, ScaleSetDeliveryControllerError> {
    let acknowledged = store
        .confirm_scale_set_lifecycle_acknowledged_locked(&current)
        .map_err(map_store_error)?;
    let acquired = acknowledged
        .lifecycle_resolution()
        .ok_or_else(|| controller_error("scale_set_delivery_recovery_conflict"))?
        .acquired()
        .len();
    settle_delivery(store, &acknowledged, acquired)
}

fn acknowledge_lifecycle<B: DeliveryBridge>(
    store: &mut UnixPersonalWorkerStore,
    bridge: &mut B,
    current: ScaleSetDeliveryRecoveryState,
) -> Result<ScaleSetDeliveryControllerDisposition, ScaleSetDeliveryControllerError> {
    let transaction = store
        .acknowledge_scale_set_lifecycle_locked(&current, |message_id| {
            bridge
                .ack(message_id)
                .map_err(map_bridge_error)
                .and_then(|acquired| {
                    if acquired.is_empty() {
                        Ok(Vec::new())
                    } else {
                        Err(controller_error("scale_set_ack_response_invalid"))
                    }
                })
        })
        .map_err(map_store_error)?;
    let acknowledged = match transaction {
        ScaleSetExternalTransaction::Completed(state) => state,
        ScaleSetExternalTransaction::ExternalFailed(error) => {
            bridge.poison();
            return Err(error);
        }
    };
    let acquired = acknowledged
        .lifecycle_resolution()
        .ok_or_else(|| controller_error("scale_set_delivery_recovery_conflict"))?
        .acquired()
        .len();
    settle_delivery(store, &acknowledged, acquired)
}

fn settle_delivery(
    store: &mut UnixPersonalWorkerStore,
    current: &ScaleSetDeliveryRecoveryState,
    expected_acquired: usize,
) -> Result<ScaleSetDeliveryControllerDisposition, ScaleSetDeliveryControllerError> {
    let settled = store
        .settle_scale_set_delivery_locked(current)
        .map_err(map_store_error)?;
    if settled.acquired() != expected_acquired {
        return Err(controller_error("scale_set_delivery_recovery_conflict"));
    }
    Ok(ScaleSetDeliveryControllerDisposition::Settled {
        acquired: settled.acquired(),
    })
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct ScaleSetDeliveryControllerError {
    code: &'static str,
}

impl ScaleSetDeliveryControllerError {
    pub(crate) const fn code(self) -> &'static str {
        self.code
    }
}

impl fmt::Debug for ScaleSetDeliveryControllerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ScaleSetDeliveryControllerError")
            .field("code", &self.code)
            .finish()
    }
}

impl fmt::Display for ScaleSetDeliveryControllerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code)
    }
}

impl std::error::Error for ScaleSetDeliveryControllerError {}

const fn controller_error(code: &'static str) -> ScaleSetDeliveryControllerError {
    ScaleSetDeliveryControllerError { code }
}

fn map_bridge_error(_: ScaleSetBridgeError) -> ScaleSetDeliveryControllerError {
    controller_error("scale_set_bridge_failed")
}

fn map_store_error(_: PersonalWorkerStoreError) -> ScaleSetDeliveryControllerError {
    controller_error("scale_set_delivery_store_failed")
}

#[cfg(test)]
mod tests;
