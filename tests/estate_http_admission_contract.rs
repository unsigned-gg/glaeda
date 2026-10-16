use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::thread;

use glaeda::artifact::Sha256Digest;
use glaeda::cortex_revenant_bridge::{
    CortexDelegatedLaneSvid, CortexLaneSpiffeId, CortexRevenantBridge, GodseatLanePrincipal,
};
use glaeda::estate_http_admission::{
    ESTATE_HTTP_ADMISSION_SCHEMA_VERSION, EstateAdmissionWireEnvelope, EstateAdmissionWireRequest,
    EstateHealthStatus, MAX_ESTATE_HTTP_REQUEST_BYTES, handle_http_request_bytes,
    serve_bounded_loopback_requests, validate_loopback_bind_addr,
};
use glaeda::execution_admission::EpochMillis;
use glaeda::worker_pool_kernel_solver::{WorkerPoolConfig, WorkerPoolKernelSolver};

fn build_test_bridge() -> CortexRevenantBridge {
    let spiffe = CortexLaneSpiffeId::new("ceres", "session-agy-ceres-estate-w3n-p1")
        .expect("valid cortex lane spiffe id");
    let svid_digest =
        Sha256Digest::parse(&format!("sha256:{}", "cd".repeat(32))).expect("valid sha256 digest");
    let svid = CortexDelegatedLaneSvid::new(
        spiffe,
        EpochMillis::new(10_000).expect("issued_at"),
        EpochMillis::new(90_000).expect("expires_at"),
        svid_digest,
    )
    .expect("valid svid");
    let godseat = GodseatLanePrincipal::new(
        "glaeda",
        "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIGxhZWRhX2hhcm5lc3Nfa2V5X2V4YW1wbGU=",
    )
    .expect("valid godseat");
    let solver = WorkerPoolKernelSolver::new(WorkerPoolConfig::default_personal_worker(2))
        .expect("valid solver");
    CortexRevenantBridge::new(svid, godseat, solver)
}

#[test]
fn loopback_bind_guard_rejects_wildcard_and_lan_listeners() {
    let lo_v4: SocketAddr = "127.0.0.1:18437".parse().expect("lo4");
    let lo_v6: SocketAddr = "[::1]:18437".parse().expect("lo6");
    let wild_v4: SocketAddr = "0.0.0.0:18437".parse().expect("wild4");
    let wild_v6: SocketAddr = "[::]:18437".parse().expect("wild6");
    let lan_v4: SocketAddr = "10.0.0.15:18437".parse().expect("lan4");

    assert!(validate_loopback_bind_addr(lo_v4).is_ok());
    assert!(validate_loopback_bind_addr(lo_v6).is_ok());
    assert_eq!(
        validate_loopback_bind_addr(wild_v4)
            .expect_err("wildcard v4 must be rejected")
            .code,
        "non_loopback_bind_forbidden"
    );
    assert_eq!(
        validate_loopback_bind_addr(wild_v6)
            .expect_err("wildcard v6 must be rejected")
            .code,
        "non_loopback_bind_forbidden"
    );
    assert_eq!(
        validate_loopback_bind_addr(lan_v4)
            .expect_err("LAN address must be rejected")
            .code,
        "non_loopback_bind_forbidden"
    );
}

#[test]
fn health_and_admission_endpoints_enforce_svid_dbr_and_size_bounds() {
    let bridge = build_test_bridge();

    // 1. Direct report via CortexRevenantBridge::build_health_report
    let direct_report =
        bridge.build_health_report(EpochMillis::new(20_000).expect("fresh"), 0, 0, 0, 0, 500, 0);
    assert_eq!(
        direct_report.schema_version,
        ESTATE_HTTP_ADMISSION_SCHEMA_VERSION
    );
    assert_eq!(direct_report.status, EstateHealthStatus::Healthy);

    // 2. Degraded when pool is saturated (DBR hold)
    let degraded_resp = handle_http_request_bytes(
        &bridge,
        b"GET /healthz HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n",
        EpochMillis::new(20_000).expect("fresh"),
        2,
        10,
        0,
        0,
        2_000,
        0,
    );
    assert_eq!(degraded_resp.status_code, 503);
    assert!(degraded_resp.body_json.contains("\"status\":\"degraded\""));

    // 3. Payload too large (> 64 KiB) returns 413
    let oversized = vec![b'A'; MAX_ESTATE_HTTP_REQUEST_BYTES + 1];
    let oversized_resp = handle_http_request_bytes(
        &bridge,
        &oversized,
        EpochMillis::new(20_000).expect("fresh"),
        0,
        0,
        0,
        0,
        500,
        0,
    );
    assert_eq!(oversized_resp.status_code, 413);
    assert!(
        oversized_resp
            .body_json
            .contains("\"code\":\"request_exceeds_max_bytes\"")
    );

    // 4. Expired SVID on POST /v1/admission/evaluate returns 403 Forbidden
    let wire_req = EstateAdmissionWireRequest {
        observed_at_ms: 90_000, // exact expires_at -> zero-grace rejection
        active_workers: 0,
        queue_depth: 0,
        in_flight_bytes: 0,
        in_flight_cpu_millis: 0,
        arrival_rate_milli_per_sec: 500,
        context_switch_rate_bps: 0,
        envelopes: vec![EstateAdmissionWireEnvelope {
            stream_seq: 1,
            delivery_token: "tok-1".to_owned(),
            delivery_attempt: 1,
            lease_ms: 30_000,
            from_peer: "session-agy-ceres-estate-w3z-p1".to_owned(),
            from_session: Some("estate".to_owned()),
            subject: "revenant.wayfinder.dispatch".to_owned(),
            body: "{\"ticket\":\"EST-426\"}".to_owned(),
            traceparent: None,
            correlation: Some("corr-1".to_owned()),
            family_key: "unsigned-gg/glaeda".to_owned(),
            wayfinder_priority: "permission".to_owned(),
            cpu_millis: 1_000,
            memory_bytes: 64 * 1024 * 1024,
            max_pids: 16,
            submitted_at_ms: 15_000,
        }],
    };
    let body_json = serde_json::to_string(&wire_req).expect("serialize wire req");
    let raw_http = format!(
        "POST /v1/admission/evaluate HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: {}\r\n\r\n{}",
        body_json.len(),
        body_json
    );
    let forbidden = handle_http_request_bytes(
        &bridge,
        raw_http.as_bytes(),
        EpochMillis::new(20_000).expect("default"),
        0,
        0,
        0,
        0,
        500,
        0,
    );
    assert_eq!(forbidden.status_code, 403);
    assert!(
        forbidden
            .body_json
            .contains("\"code\":\"svid_expired_zero_grace\"")
    );
}

#[test]
fn loopback_tcp_listener_serves_health_and_admission_deterministically() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral loopback");
    let addr = listener.local_addr().expect("local addr");
    let bridge = build_test_bridge();

    let server_handle = thread::spawn(move || {
        serve_bounded_loopback_requests(
            &listener,
            &bridge,
            3,
            EpochMillis::new(25_000).expect("obs"),
            0,
            0,
            0,
            0,
            500,
            0,
        )
        .expect("serve 3 requests")
    });

    // Request 1: GET /v1/health
    {
        let mut client = TcpStream::connect(addr).expect("connect 1");
        client
            .write_all(b"GET /v1/health HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
            .expect("write 1");
        let mut resp = String::new();
        client.read_to_string(&mut resp).expect("read 1");
        assert!(resp.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(resp.contains("Content-Type: application/json\r\n"));
        assert!(resp.contains("\"callsign\":\"aegis\""));
        assert!(resp.contains("\"status\":\"healthy\""));
    }

    // Request 2: POST /v1/estate/admit
    {
        let wire_req = EstateAdmissionWireRequest {
            observed_at_ms: 25_000,
            active_workers: 0,
            queue_depth: 0,
            in_flight_bytes: 0,
            in_flight_cpu_millis: 0,
            arrival_rate_milli_per_sec: 500,
            context_switch_rate_bps: 0,
            envelopes: vec![
                EstateAdmissionWireEnvelope {
                    stream_seq: 10,
                    delivery_token: "tok-10".to_owned(),
                    delivery_attempt: 1,
                    lease_ms: 30_000,
                    from_peer: "session-agy-ceres-estate-w3z-p1".to_owned(),
                    from_session: Some("estate".to_owned()),
                    subject: "revenant.wayfinder.dispatch".to_owned(),
                    body: "{\"op\":\"idle\"}".to_owned(),
                    traceparent: None,
                    correlation: None,
                    family_key: "unsigned-gg/glaeda".to_owned(),
                    wayfinder_priority: "idle".to_owned(),
                    cpu_millis: 1_000,
                    memory_bytes: 64 * 1024 * 1024,
                    max_pids: 16,
                    submitted_at_ms: 20_000,
                },
                EstateAdmissionWireEnvelope {
                    stream_seq: 11,
                    delivery_token: "tok-11".to_owned(),
                    delivery_attempt: 1,
                    lease_ms: 30_000,
                    from_peer: "session-agy-ceres-estate-w30-p4".to_owned(),
                    from_session: Some("estate".to_owned()),
                    subject: "cortex.worker.dispatch".to_owned(),
                    body: "{\"op\":\"permission\"}".to_owned(),
                    traceparent: None,
                    correlation: None,
                    family_key: "unsigned-gg/glaeda".to_owned(),
                    wayfinder_priority: "permission".to_owned(),
                    cpu_millis: 1_000,
                    memory_bytes: 64 * 1024 * 1024,
                    max_pids: 16,
                    submitted_at_ms: 21_000,
                },
            ],
        };
        let body = serde_json::to_string(&wire_req).expect("json");
        let req = format!(
            "POST /v1/estate/admit HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        let mut client = TcpStream::connect(addr).expect("connect 2");
        client.write_all(req.as_bytes()).expect("write 2");
        let mut resp = String::new();
        client.read_to_string(&mut resp).expect("read 2");
        assert!(resp.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(resp.contains("\"admitted\":true"));
        assert!(resp.contains("\"tasks_solved\":2"));
        assert!(resp.contains("\"bridge_receipt_digest\":\"sha256:"));
    }

    // Request 3: GET /metrics
    {
        let mut client = TcpStream::connect(addr).expect("connect 3");
        client
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n")
            .expect("write 3");
        let mut resp = String::new();
        client.read_to_string(&mut resp).expect("read 3");
        assert!(resp.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(resp.contains("Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n"));
        assert!(
            resp.contains("glaeda_worker_pool_admitting{service=\"glaeda\",callsign=\"aegis\"} 1")
        );
    }

    let served = server_handle.join().expect("server thread join");
    assert_eq!(served, 3);
}
