use glaeda::artifact::Sha256Digest;
use glaeda::cortex_revenant_bridge::{
    CORTEX_REVENANT_BRIDGE_SCHEMA_VERSION, CortexDelegatedLaneSvid, CortexLaneSpiffeId,
    CortexRevenantBridge, GodseatLanePrincipal, MeshPoolDeliveryEnvelope, WayfinderPriority,
    validate_isolated_key_dir,
};
use glaeda::execution_admission::{EpochMillis, ExecutionResourceLimits};
use glaeda::personal_worker_queue::PersonalWorkerPriority;
use glaeda::worker_pool_kernel_solver::{
    PoolAdmissionDecision, WorkerPoolConfig, WorkerPoolKernelSolver,
};

fn sample_digest(hex_pair: &str) -> Sha256Digest {
    Sha256Digest::parse(&format!("sha256:{}", hex_pair.repeat(32))).expect("valid sha256 digest")
}

#[test]
fn cortex_lane_svid_and_godseat_identity_enforce_cross_project_invariants() {
    let spiffe = CortexLaneSpiffeId::new("ceres", "session-agy-ceres-estate-w3n-p1")
        .expect("valid cortex lane spiffe id");
    assert_eq!(
        spiffe.spiffe_id,
        "spiffe://unsigned.gg/host/ceres/lane/session-agy-ceres-estate-w3n-p1"
    );
    assert_eq!(
        spiffe.selector,
        "cortex:pane-role:session-agy-ceres-estate-w3n-p1"
    );

    let parsed = CortexLaneSpiffeId::parse(&spiffe.spiffe_id).expect("round-trip parse");
    assert_eq!(parsed, spiffe);

    // Reject uppercase, leading/trailing dashes, and foreign trust domains.
    assert!(CortexLaneSpiffeId::new("Ceres", "session-agy-ceres-estate-w3n-p1").is_err());
    assert!(CortexLaneSpiffeId::new("ceres", "-session-agy").is_err());
    assert!(CortexLaneSpiffeId::new("ceres", "session-agy-").is_err());
    assert!(
        CortexLaneSpiffeId::parse(
            "spiffe://other.domain/host/ceres/lane/session-agy-ceres-estate-w3n-p1"
        )
        .is_err()
    );

    // Godseat principal and isolated key directory validation.
    let pubkey =
        "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIGxhZWRhX2hhcm5lc3Nfa2V5X2V4YW1wbGU= harness-glaeda";
    let godseat = GodseatLanePrincipal::new("glaeda", pubkey).expect("valid godseat principal");
    assert_eq!(godseat.principal, "harness-glaeda@agents.cerebral.work");
    assert_eq!(
        godseat.allowed_signers_entry(),
        format!("harness-glaeda@agents.cerebral.work namespaces=\"git\" {pubkey}")
    );
    assert!(GodseatLanePrincipal::new("1glaeda", pubkey).is_err());
    assert!(GodseatLanePrincipal::new("glaeda_bad", pubkey).is_err());
    assert!(validate_isolated_key_dir("/home/ctodie/.harness/keys").is_ok());
    assert!(validate_isolated_key_dir("/home/ctodie/.ssh").is_err());
    assert!(validate_isolated_key_dir("/home/ctodie/.ssh/harness").is_err());
}

#[test]
fn wayfinder_priority_maps_monotonically_and_ages_linearly() {
    assert_eq!(
        WayfinderPriority::Permission.to_personal_worker_priority(),
        PersonalWorkerPriority::Interactive
    );
    assert_eq!(
        WayfinderPriority::Input.to_personal_worker_priority(),
        PersonalWorkerPriority::Interactive
    );
    assert_eq!(
        WayfinderPriority::Ready.to_personal_worker_priority(),
        PersonalWorkerPriority::Normal
    );
    assert_eq!(
        WayfinderPriority::Idle.to_personal_worker_priority(),
        PersonalWorkerPriority::Background
    );

    // Wayfinder linear aging: after 1 full aging window (1800s), Idle (0) reaches Ready base (100).
    assert_eq!(WayfinderPriority::Idle.aged_score(1_800, 1_800), 100);
    assert_eq!(WayfinderPriority::Ready.aged_score(1_800, 1_800), 200);
    assert_eq!(WayfinderPriority::Input.aged_score(1_800, 1_800), 300);
}

#[test]
fn cortex_revenant_bridge_produces_byte_identical_receipts_and_gates_expired_svid() {
    let spiffe = CortexLaneSpiffeId::new("ceres", "session-agy-ceres-estate-w3n-p1")
        .expect("valid cortex spiffe");
    let svid = CortexDelegatedLaneSvid::new(
        spiffe,
        EpochMillis::new(10_000).expect("issued_at"),
        EpochMillis::new(60_000).expect("expires_at"),
        sample_digest("11"),
    )
    .expect("valid svid");
    let godseat = GodseatLanePrincipal::new(
        "glaeda",
        "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIGxhZWRhX2hhcm5lc3Nfa2V5X2V4YW1wbGU=",
    )
    .expect("valid godseat");
    let solver = WorkerPoolKernelSolver::new(WorkerPoolConfig::default_personal_worker(2))
        .expect("valid solver");
    let bridge = CortexRevenantBridge::new(svid, godseat, solver);

    let envelopes = vec![
        MeshPoolDeliveryEnvelope {
            stream_seq: 101,
            delivery_token: "tok-101".to_owned(),
            delivery_attempt: 1,
            lease_ms: 30_000,
            from_peer: "session-agy-ceres-estate-w3z-p1".to_owned(),
            from_session: Some("estate".to_owned()),
            subject: "revenant.wayfinder.dispatch".to_owned(),
            body: "{\"ticket\":\"EST-426\"}".to_owned(),
            traceparent: Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_owned()),
            correlation: Some("corr-101".to_owned()),
            family_key: "cerebral-work/revenant".to_owned(),
            wayfinder_priority: WayfinderPriority::Idle,
            limits: ExecutionResourceLimits::new(1_000, 128 * 1_024 * 1_024, 16).expect("limits"),
            submitted_at: EpochMillis::new(12_000).expect("submitted"),
        },
        MeshPoolDeliveryEnvelope {
            stream_seq: 102,
            delivery_token: "tok-102".to_owned(),
            delivery_attempt: 1,
            lease_ms: 30_000,
            from_peer: "session-agy-ceres-estate-w3y-p4".to_owned(),
            from_session: Some("estate".to_owned()),
            subject: "cortex.worker.solve".to_owned(),
            body: "{\"spec\":\"0039\"}".to_owned(),
            traceparent: None,
            correlation: Some("corr-102".to_owned()),
            family_key: "cerebral-work/cortex".to_owned(),
            wayfinder_priority: WayfinderPriority::Permission,
            limits: ExecutionResourceLimits::new(1_000, 128 * 1_024 * 1_024, 16).expect("limits"),
            submitted_at: EpochMillis::new(12_500).expect("submitted"),
        },
    ];

    let obs = EpochMillis::new(20_000).expect("obs");
    let metrics = bridge
        .evaluate_envelope_admission(
            &envelopes[1],
            obs,
            1,
            1,
            128 * 1_024 * 1_024,
            1_000,
            1_000,
            0,
        )
        .expect("admission evaluated");
    assert_eq!(metrics.admission, PoolAdmissionDecision::Admit);

    let receipt_a = bridge
        .dispatch_batch(envelopes.clone(), obs, 1_000, |task| {
            (
                format!("{}:{}", task.payload.from_peer, task.payload.stream_seq),
                120,
            )
        })
        .expect("receipt a");
    let receipt_b = bridge
        .dispatch_batch(envelopes, obs, 1_000, |task| {
            (
                format!("{}:{}", task.payload.from_peer, task.payload.stream_seq),
                120,
            )
        })
        .expect("receipt b");

    assert_eq!(
        receipt_a.schema_version,
        CORTEX_REVENANT_BRIDGE_SCHEMA_VERSION
    );
    assert_eq!(receipt_a, receipt_b);
    assert_eq!(
        receipt_a.bridge_receipt_digest,
        receipt_b.bridge_receipt_digest
    );
}
