use super::*;
use hyper::body::to_bytes;
use tempfile::tempdir;

const TEST_TLS_CERT_PEM: &str = r#"-----BEGIN CERTIFICATE-----
MIIDCTCCAfGgAwIBAgIUVaFPJmszsQZiWnM3obSar9/CN9owDQYJKoZIhvcNAQEL
BQAwFDESMBAGA1UEAwwJMTI3LjAuMC4xMB4XDTI2MDQzMDA3MDI0NVoXDTI2MDUw
MTA3MDI0NVowFDESMBAGA1UEAwwJMTI3LjAuMC4xMIIBIjANBgkqhkiG9w0BAQEF
AAOCAQ8AMIIBCgKCAQEAwUchidUdWHWopktZaqbHy7KFnYgSZFcXlt7Tj9GTw7Sw
dPMDsUiyayHCZfE8U8IPzs/7975OiM/HHippkSX+vHEkwQ8h6EJ1XQuGhFfZp11U
ShrHq6WgMFFkvorqZRy4R6CiqtFefPEKfSTqsdRHA4lW+NWpFvPiYj5Sk9hdpJPd
wLucp4LFscvEeghqbn1mSPDehJolNVX5y4iDRvAaPF5++sgEb1tQbr7VyDgdSsT0
/XuCrfNDPBvOGOvp86aH4A5SaJkoewJnWwkxB2Z6YKt+BCjGOsVBw8yHyEUvTCXk
3cUhjHBtwdRf47cLEN9o4VFQY3nc33S+ofsua9GREwIDAQABo1MwUTAdBgNVHQ4E
FgQUv8RtHGhrSGx6VYTVmT9va1bLSqQwHwYDVR0jBBgwFoAUv8RtHGhrSGx6VYTV
mT9va1bLSqQwDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEAqHk1
BEeux5284FHhG98JopveEQrA3f9w2Ywvzx9cyxgsEDpTj7DWR5EFBVsxrJJdplM1
aWgaly13BsGgG6U2imZuydYKO5y8ekX+qC5jh/aBfgnUa/rQ8NdzXeIik7+gEkUe
CPFzGNGvitrcgnqnAH4S5vy6OCj2Ay33pKW8UQgnxcCryxfJ+GxaOnOcYf0jwfyc
6vjiIuddYbxEP/sVFIKj1rNdLtvtiB15ep/A0KMlSRfpllOvG2f24YufXVhB8qMQ
ZC/YtnhyWUay1bIcQbrPIPVLsjgjM+Fu91VOKbVB5fb1BuDhdDsuWVaTcZenrxpP
H7nbZnmhvkgzl+bBog==
-----END CERTIFICATE-----
"#;

const TEST_TLS_KEY_PEM: &str = r#"-----BEGIN PRIVATE KEY-----
MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDBRyGJ1R1Ydaim
S1lqpsfLsoWdiBJkVxeW3tOP0ZPDtLB08wOxSLJrIcJl8TxTwg/Oz/v3vk6Iz8ce
KmmRJf68cSTBDyHoQnVdC4aEV9mnXVRKGserpaAwUWS+iuplHLhHoKKq0V588Qp9
JOqx1EcDiVb41akW8+JiPlKT2F2kk93Au5yngsWxy8R6CGpufWZI8N6EmiU1VfnL
iING8Bo8Xn76yARvW1BuvtXIOB1KxPT9e4Kt80M8G84Y6+nzpofgDlJomSh7Amdb
CTEHZnpgq34EKMY6xUHDzIfIRS9MJeTdxSGMcG3B1F/jtwsQ32jhUVBjedzfdL6h
+y5r0ZETAgMBAAECggEAVD7C+acw8VvntQRm5zvnHnykDPRAwAfOOm7J3IhHViiu
OWurklzTmCrQ50ptNz0BUu4JMAV9idi3PAjUlvXuwQi4MoZ8CxbcvT/G1GzObEsb
8GkX21OILUdtGDjIzmXkVSRJgxdbji4qmj27JuQWSA5XIINQ/rYzWQs9R0AqIQ+n
/fLcOA7tyQRN+ndYJ61k0WjjjXMWrhnmKQMEOXYK0B6cSPoU8CYR/jj0ewrg5XJU
6iZauHf/SnvjlUORw0r2tWgM9Svuafujv8HqY1E9PzQh1pa+LCGne/Aj6MYnZtGZ
b5R5lhy1HB+/0r/LUFKobHZKadxC5LKqSODXzGGs4QKBgQDz5vZtmy6PTVA3ZSo4
6IPH3BwnhrR3NAHUBD12ZzKoSikoM2ICXrIP3KRfHVuxo9xz69c1JTch2uX6rsXb
4/AGpkZt+HpD9Sfbfw6kdXUWGTvm6c2ENuQbnAI4zWYwrBY7kKMa/ZNghNWpbhdF
AULfvSHpTeOvsp+SMCWfrWSx+wKBgQDK3VjoME8R4g2eMQOFt+VTOq8RKVHh1Dps
1i1N5VRpV2i4xRVDjh2ucEX+Qgz5vu64aX2wJC2bChwfgdCtMhDI86zzP+qq5l7p
v1uoJKY725ipToIlFDWSXNjPVtXjUbWBG0C8YsBYa2QWhkKtzz+vjyCymCJ5YHlX
2/dtQIAJyQKBgCY+PL2K644ErWNCNZCexKr91FxOPtXCDddUot6B5+uDVVi8Vc3R
U1IxYoSXcd00uEhk3mWy5CYm0JCx/swvvV8Ni1WK9IDbW9iK35zh3e4NHttiJZtp
j/LUT3Tgn/lZwlKspyaARC+KJIZggL2NKRMz8LFIST8vXt3pNr0GzxcpAoGAA+Z1
iyFCo+lgsaXnl26Nrif2rbHJrTnTVbxYaqL6GHxhuwuu+PmGgJAQCG9kqHiPRmRg
0j4f0ldDayenx2yq/fIRZSvZaye6s2vGa1kpCQWTzc2Amw3kacf3MyVMP26WusC3
YefUIt8NsZErPwQ5CTsLOePK5eKA8rt76lHPJGECgYBpWfniRtq9peKu4CHN4S3W
e/URtTFvdJsCvolY9WnMS7qtOhuTmKWuBPzkbEra2uYWMmB5FQq7NXvFNLfK6Oh2
VvfZab2Q10gdVv02GoNc1NiveAzxhCCQgLud9A6Sss0/9ZJdTh6RMPs+BPWTfMax
dB0HtnAja5pmq6N9Ck79+g==
-----END PRIVATE KEY-----
"#;
use std::fs;
use std::io::Write;

async fn response_body_string(response: Response<Body>) -> String {
    let bytes = to_bytes(response.into_body()).await.unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
}

#[tokio::test]
async fn test_exec_url_generation() {
    let server = StreamingServer::for_test("http://127.0.0.1:12345");
    let req = ExecRequest {
        container_id: "abc".to_string(),
        cmd: vec!["sh".to_string()],
        stdin: true,
        stdout: true,
        stderr: false,
        tty: true,
    };

    let response = server
        .get_exec(
            &req,
            ExecStreamOptions {
                runtime_path: PathBuf::from("/bin/false"),
                runtime_config_path: PathBuf::new(),
                exec_cpu_affinity: None,
                exec_io_socket_path: None,
                exec_resize_socket_path: None,
                websocket_enabled: true,
            },
        )
        .await
        .unwrap();
    assert!(response.url.contains("/exec/"));
    assert!(response.url.starts_with("http://127.0.0.1:12345"));
}

#[test]
fn test_validate_exec_request_rejects_empty_command() {
    let req = ExecRequest {
        container_id: "abc".to_string(),
        cmd: Vec::new(),
        stdin: false,
        stdout: true,
        stderr: true,
        tty: false,
    };

    let err = StreamingServer::validate_exec_request(&req).unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[test]
fn test_expected_exec_roles_omit_stderr_for_tty() {
    let req = ExecRequest {
        container_id: "abc".to_string(),
        cmd: vec!["sh".to_string()],
        stdin: true,
        stdout: true,
        stderr: false,
        tty: true,
    };

    assert_eq!(
        expected_exec_roles(&req),
        vec![
            ExecStreamRole::Error,
            ExecStreamRole::Stdin,
            ExecStreamRole::Stdout
        ]
    );
}

#[tokio::test]
async fn test_attach_url_generation() {
    let server = StreamingServer::for_test("http://127.0.0.1:12345");
    let req = AttachRequest {
        container_id: "abc".to_string(),
        stdin: false,
        stdout: true,
        stderr: true,
        tty: false,
    };

    let response = server.get_attach(&req, None, None, true).await.unwrap();
    assert!(response.url.contains("/attach/"));
}

#[tokio::test]
async fn test_exec_transport_explicitly_rejects_non_spdy_requests() {
    let server = StreamingServer::for_test("http://127.0.0.1:12345");
    let token = server
        .insert_request(StreamingRequest::Exec(ExecRequestContext {
            req: ExecRequest {
                container_id: "abc".to_string(),
                cmd: vec!["sh".to_string()],
                stdin: true,
                stdout: true,
                stderr: false,
                tty: true,
            },
            runtime_path: PathBuf::from("/bin/false"),
            runtime_config_path: PathBuf::new(),
            exec_cpu_affinity: None,
            exec_io_socket_path: None,
            exec_resize_socket_path: None,
            websocket_enabled: true,
        }))
        .await;
    let request = Request::builder()
        .method(Method::GET)
        .uri(format!("/exec/{}", token))
        .body(Body::empty())
        .unwrap();

    let response = server
        .handle_request_for_test(PathBuf::from("/bin/false"), request)
        .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(response_body_string(response)
        .await
        .contains("SPDY or websocket upgrade headers"));
}

#[tokio::test]
async fn test_exec_transport_accepts_websocket_upgrade_requests() {
    let server = StreamingServer::for_test("http://127.0.0.1:12345");
    let token = server
        .insert_request(StreamingRequest::Exec(ExecRequestContext {
            req: ExecRequest {
                container_id: "abc".to_string(),
                cmd: vec!["sh".to_string()],
                stdin: true,
                stdout: true,
                stderr: false,
                tty: true,
            },
            runtime_path: PathBuf::from("/bin/false"),
            runtime_config_path: PathBuf::new(),
            exec_cpu_affinity: None,
            exec_io_socket_path: None,
            exec_resize_socket_path: None,
            websocket_enabled: true,
        }))
        .await;
    let request = Request::builder()
        .method(Method::GET)
        .uri(format!("/exec/{}", token))
        .header(CONNECTION, "Upgrade")
        .header(UPGRADE, "websocket")
        .header(SEC_WEBSOCKET_VERSION, "13")
        .header(SEC_WEBSOCKET_KEY, "dGhlIHNhbXBsZSBub25jZQ==")
        .header(SEC_WEBSOCKET_PROTOCOL, "v4.channel.k8s.io")
        .body(Body::empty())
        .unwrap();

    let response = server
        .handle_request_for_test(PathBuf::from("/bin/false"), request)
        .await;
    assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
    assert_eq!(
        response
            .headers()
            .get(UPGRADE)
            .and_then(|value| value.to_str().ok()),
        Some("websocket")
    );
    assert_eq!(
        response
            .headers()
            .get(SEC_WEBSOCKET_PROTOCOL)
            .and_then(|value| value.to_str().ok()),
        Some("v4.channel.k8s.io")
    );
}

#[tokio::test]
async fn test_exec_transport_rejects_websocket_when_disabled_for_token() {
    let server = StreamingServer::for_test("http://127.0.0.1:12345");
    let token = server
        .insert_request(StreamingRequest::Exec(ExecRequestContext {
            req: ExecRequest {
                container_id: "abc".to_string(),
                cmd: vec!["sh".to_string()],
                stdin: true,
                stdout: true,
                stderr: false,
                tty: true,
            },
            runtime_path: PathBuf::from("/bin/false"),
            runtime_config_path: PathBuf::new(),
            exec_cpu_affinity: None,
            exec_io_socket_path: None,
            exec_resize_socket_path: None,
            websocket_enabled: false,
        }))
        .await;
    let request = Request::builder()
        .method(Method::GET)
        .uri(format!("/exec/{}", token))
        .header(CONNECTION, "Upgrade")
        .header(UPGRADE, "websocket")
        .header(SEC_WEBSOCKET_VERSION, "13")
        .header(SEC_WEBSOCKET_KEY, "dGhlIHNhbXBsZSBub25jZQ==")
        .header(SEC_WEBSOCKET_PROTOCOL, "v4.channel.k8s.io")
        .body(Body::empty())
        .unwrap();

    let response = server
        .handle_request_for_test(PathBuf::from("/bin/false"), request)
        .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(response_body_string(response)
        .await
        .contains("websocket streaming is disabled"));
}

#[test]
fn test_negotiate_remotecommand_websocket_protocol_prefers_highest_supported_version() {
    let request = Request::builder()
        .header(
            SEC_WEBSOCKET_PROTOCOL,
            "channel.k8s.io, v3.channel.k8s.io, v5.channel.k8s.io",
        )
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        negotiate_remotecommand_websocket_protocol(&request),
        Some("v5.channel.k8s.io")
    );

    let request = Request::builder()
        .header(SEC_WEBSOCKET_PROTOCOL, "v2.channel.k8s.io, channel.k8s.io")
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        negotiate_remotecommand_websocket_protocol(&request),
        Some("v2.channel.k8s.io")
    );
}

#[tokio::test]
async fn test_attach_transport_explicitly_rejects_non_spdy_requests() {
    let server = StreamingServer::for_test("http://127.0.0.1:12345");
    let token = server
        .insert_request(StreamingRequest::Attach(AttachRequestContext {
            req: AttachRequest {
                container_id: "abc".to_string(),
                stdin: false,
                stdout: true,
                stderr: true,
                tty: false,
            },
            attach_io_socket_path: None,
            attach_resize_socket_path: None,
            websocket_enabled: true,
        }))
        .await;
    let request = Request::builder()
        .method(Method::GET)
        .uri(format!("/attach/{}", token))
        .body(Body::empty())
        .unwrap();

    let response = server
        .handle_request_for_test(PathBuf::from("/bin/false"), request)
        .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(response_body_string(response)
        .await
        .contains("SPDY or websocket upgrade headers"));
}

#[tokio::test]
async fn test_attach_log_url_generation() {
    let server = StreamingServer::for_test("http://127.0.0.1:12345");
    let req = AttachRequest {
        container_id: "abc".to_string(),
        stdin: false,
        stdout: true,
        stderr: true,
        tty: false,
    };

    let response = server
        .get_attach_log(&req, PathBuf::from("/var/log/pods/abc.log"), true)
        .await
        .unwrap();
    assert!(response.url.contains("/attach/"));
    assert!(response.url.starts_with("http://127.0.0.1:12345"));
}

#[tokio::test]
async fn test_attach_transport_accepts_websocket_upgrade_requests() {
    let server = StreamingServer::for_test("http://127.0.0.1:12345");
    let token = server
        .insert_request(StreamingRequest::Attach(AttachRequestContext {
            req: AttachRequest {
                container_id: "abc".to_string(),
                stdin: false,
                stdout: true,
                stderr: true,
                tty: false,
            },
            attach_io_socket_path: None,
            attach_resize_socket_path: None,
            websocket_enabled: true,
        }))
        .await;
    let request = Request::builder()
        .method(Method::GET)
        .uri(format!("/attach/{}", token))
        .header(CONNECTION, "Upgrade")
        .header(UPGRADE, "websocket")
        .header(SEC_WEBSOCKET_VERSION, "13")
        .header(SEC_WEBSOCKET_KEY, "dGhlIHNhbXBsZSBub25jZQ==")
        .header(SEC_WEBSOCKET_PROTOCOL, "v4.channel.k8s.io")
        .body(Body::empty())
        .unwrap();

    let response = server
        .handle_request_for_test(PathBuf::from("/bin/false"), request)
        .await;
    assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
    assert_eq!(
        response
            .headers()
            .get(UPGRADE)
            .and_then(|value| value.to_str().ok()),
        Some("websocket")
    );
    assert_eq!(
        response
            .headers()
            .get(SEC_WEBSOCKET_PROTOCOL)
            .and_then(|value| value.to_str().ok()),
        Some("v4.channel.k8s.io")
    );
}

#[tokio::test]
async fn test_attach_transport_rejects_websocket_when_disabled_for_token() {
    let server = StreamingServer::for_test("http://127.0.0.1:12345");
    let token = server
        .insert_request(StreamingRequest::Attach(AttachRequestContext {
            req: AttachRequest {
                container_id: "abc".to_string(),
                stdin: false,
                stdout: true,
                stderr: true,
                tty: false,
            },
            attach_io_socket_path: None,
            attach_resize_socket_path: None,
            websocket_enabled: false,
        }))
        .await;
    let request = Request::builder()
        .method(Method::GET)
        .uri(format!("/attach/{}", token))
        .header(CONNECTION, "Upgrade")
        .header(UPGRADE, "websocket")
        .header(SEC_WEBSOCKET_VERSION, "13")
        .header(SEC_WEBSOCKET_KEY, "dGhlIHNhbXBsZSBub25jZQ==")
        .header(SEC_WEBSOCKET_PROTOCOL, "v4.channel.k8s.io")
        .body(Body::empty())
        .unwrap();

    let response = server
        .handle_request_for_test(PathBuf::from("/bin/false"), request)
        .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(response_body_string(response)
        .await
        .contains("websocket streaming is disabled"));
}

#[tokio::test]
async fn test_attach_log_transport_accepts_websocket_upgrade_requests() {
    let server = StreamingServer::for_test("http://127.0.0.1:12345");
    let token = server
        .insert_request(StreamingRequest::AttachLog(AttachLogRequestContext {
            req: AttachRequest {
                container_id: "abc".to_string(),
                stdin: false,
                stdout: true,
                stderr: true,
                tty: false,
            },
            log_path: PathBuf::from("/var/log/pods/abc.log"),
            websocket_enabled: true,
        }))
        .await;
    let request = Request::builder()
        .method(Method::GET)
        .uri(format!("/attach/{}", token))
        .header(CONNECTION, "Upgrade")
        .header(UPGRADE, "websocket")
        .header(SEC_WEBSOCKET_VERSION, "13")
        .header(SEC_WEBSOCKET_KEY, "dGhlIHNhbXBsZSBub25jZQ==")
        .header(SEC_WEBSOCKET_PROTOCOL, "v4.channel.k8s.io")
        .body(Body::empty())
        .unwrap();

    let response = server
        .handle_request_for_test(PathBuf::from("/bin/false"), request)
        .await;
    assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
    assert_eq!(
        response
            .headers()
            .get(UPGRADE)
            .and_then(|value| value.to_str().ok()),
        Some("websocket")
    );
    assert_eq!(
        response
            .headers()
            .get(SEC_WEBSOCKET_PROTOCOL)
            .and_then(|value| value.to_str().ok()),
        Some("v4.channel.k8s.io")
    );
}

#[tokio::test]
async fn test_portforward_url_generation() {
    let server = StreamingServer::for_test("http://127.0.0.1:12345");
    let req = PortForwardRequest {
        pod_sandbox_id: "pod-1".to_string(),
        port: vec![8080, 9090],
    };

    let response = server
        .get_port_forward(&req, PathBuf::from("/proc/self/ns/net"), true)
        .await
        .unwrap();
    assert!(response.url.contains("/portforward/"));
    assert!(response.url.starts_with("http://127.0.0.1:12345"));
}

#[tokio::test]
async fn test_portforward_transport_explicitly_rejects_non_spdy_requests() {
    let server = StreamingServer::for_test("http://127.0.0.1:12345");
    let token = server
        .insert_request(StreamingRequest::PortForward(PortForwardRequestContext {
            req: PortForwardRequest {
                pod_sandbox_id: "pod-1".to_string(),
                port: vec![8080],
            },
            netns_path: PathBuf::from("/proc/thread-self/ns/net"),
            websocket_enabled: true,
        }))
        .await;
    let request = Request::builder()
        .method(Method::POST)
        .uri(format!("/portforward/{}", token))
        .body(Body::empty())
        .unwrap();

    let response = server
        .handle_request_for_test(PathBuf::from("/bin/false"), request)
        .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(response_body_string(response)
        .await
        .contains("SPDY or websocket upgrade headers"));
}

#[tokio::test]
async fn test_portforward_transport_accepts_websocket_upgrade_requests() {
    let server = StreamingServer::for_test("http://127.0.0.1:12345");
    let token = server
        .insert_request(StreamingRequest::PortForward(PortForwardRequestContext {
            req: PortForwardRequest {
                pod_sandbox_id: "pod-1".to_string(),
                port: vec![8080],
            },
            netns_path: PathBuf::from("/proc/thread-self/ns/net"),
            websocket_enabled: true,
        }))
        .await;
    let request = Request::builder()
        .method(Method::GET)
        .uri(format!("/portforward/{}", token))
        .header(CONNECTION, "Upgrade")
        .header(UPGRADE, "websocket")
        .header(SEC_WEBSOCKET_VERSION, "13")
        .header(SEC_WEBSOCKET_KEY, "dGhlIHNhbXBsZSBub25jZQ==")
        .header(SEC_WEBSOCKET_PROTOCOL, PORT_FORWARD_WS_PROTOCOL_V4_BINARY)
        .body(Body::empty())
        .unwrap();

    let response = server
        .handle_request_for_test(PathBuf::from("/bin/false"), request)
        .await;
    assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
    assert_eq!(
        response
            .headers()
            .get(UPGRADE)
            .and_then(|value| value.to_str().ok()),
        Some("websocket")
    );
    assert_eq!(
        response
            .headers()
            .get(SEC_WEBSOCKET_PROTOCOL)
            .and_then(|value| value.to_str().ok()),
        Some(PORT_FORWARD_WS_PROTOCOL_V4_BINARY)
    );
}

#[tokio::test]
async fn test_portforward_transport_rejects_websocket_when_disabled_for_token() {
    let server = StreamingServer::for_test("http://127.0.0.1:12345");
    let token = server
        .insert_request(StreamingRequest::PortForward(PortForwardRequestContext {
            req: PortForwardRequest {
                pod_sandbox_id: "pod-1".to_string(),
                port: vec![8080],
            },
            netns_path: PathBuf::from("/proc/thread-self/ns/net"),
            websocket_enabled: false,
        }))
        .await;
    let request = Request::builder()
        .method(Method::GET)
        .uri(format!("/portforward/{}", token))
        .header(CONNECTION, "Upgrade")
        .header(UPGRADE, "websocket")
        .header(SEC_WEBSOCKET_VERSION, "13")
        .header(SEC_WEBSOCKET_KEY, "dGhlIHNhbXBsZSBub25jZQ==")
        .header(SEC_WEBSOCKET_PROTOCOL, PORT_FORWARD_WS_PROTOCOL_V4_BINARY)
        .body(Body::empty())
        .unwrap();

    let response = server
        .handle_request_for_test(PathBuf::from("/bin/false"), request)
        .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(response_body_string(response)
        .await
        .contains("websocket streaming is disabled"));
}

#[tokio::test]
async fn test_portforward_transport_requires_supported_protocol_version() {
    let server = StreamingServer::for_test("http://127.0.0.1:12345");
    let token = server
        .insert_request(StreamingRequest::PortForward(PortForwardRequestContext {
            req: PortForwardRequest {
                pod_sandbox_id: "pod-1".to_string(),
                port: vec![8080],
            },
            netns_path: PathBuf::from("/proc/thread-self/ns/net"),
            websocket_enabled: true,
        }))
        .await;
    let request = Request::builder()
        .method(Method::POST)
        .uri(format!("/portforward/{}", token))
        .header(CONNECTION, "Upgrade")
        .header(UPGRADE, spdy::SPDY_31)
        .body(Body::empty())
        .unwrap();

    let response = server
        .handle_request_for_test(PathBuf::from("/bin/false"), request)
        .await;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert!(response_body_string(response)
        .await
        .contains("no supported port-forward protocol was requested"));
}

#[tokio::test]
async fn test_portforward_route_rejects_token_kind_mismatch() {
    let server = StreamingServer::for_test("http://127.0.0.1:12345");
    let token = server
        .insert_request(StreamingRequest::Exec(ExecRequestContext {
            req: ExecRequest {
                container_id: "abc".to_string(),
                cmd: vec!["sh".to_string()],
                stdin: true,
                stdout: true,
                stderr: false,
                tty: true,
            },
            runtime_path: PathBuf::from("/bin/false"),
            runtime_config_path: PathBuf::new(),
            exec_cpu_affinity: None,
            exec_io_socket_path: None,
            exec_resize_socket_path: None,
            websocket_enabled: true,
        }))
        .await;
    let request = Request::builder()
        .method(Method::POST)
        .uri(format!("/portforward/{}", token))
        .body(Body::empty())
        .unwrap();

    let response = server
        .handle_request_for_test(PathBuf::from("/bin/false"), request)
        .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(response_body_string(response)
        .await
        .contains("streaming token kind mismatch"));
}

#[tokio::test]
async fn test_expired_streaming_token_is_rejected() {
    let server = StreamingServer::for_test_with_config(
        "http://127.0.0.1:12345",
        StreamingConfig {
            request_token_ttl: Duration::from_secs(5),
            ..StreamingConfig::default()
        },
    );
    let token = server
        .insert_request_for_test(
            StreamingRequest::Exec(ExecRequestContext {
                req: ExecRequest {
                    container_id: "abc".to_string(),
                    cmd: vec!["sh".to_string()],
                    stdin: false,
                    stdout: true,
                    stderr: true,
                    tty: false,
                },
                runtime_path: PathBuf::from("/bin/false"),
                runtime_config_path: PathBuf::new(),
                exec_cpu_affinity: None,
                exec_io_socket_path: None,
                exec_resize_socket_path: None,
                websocket_enabled: true,
            }),
            Duration::from_secs(6),
        )
        .await;
    let request = Request::builder()
        .method(Method::POST)
        .uri(format!("/exec/{}", token))
        .body(Body::empty())
        .unwrap();

    let response = server
        .handle_request_for_test(PathBuf::from("/bin/false"), request)
        .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert!(response_body_string(response)
        .await
        .contains("streaming token not found"));
}

#[test]
fn test_validate_portforward_request_rejects_invalid_values() {
    let missing_pod = PortForwardRequest {
        pod_sandbox_id: String::new(),
        port: vec![8080],
    };
    let err = StreamingServer::validate_port_forward_request(&missing_pod).unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    let missing_ports = PortForwardRequest {
        pod_sandbox_id: "pod-1".to_string(),
        port: Vec::new(),
    };
    StreamingServer::validate_port_forward_request(&missing_ports).unwrap();

    let invalid_port = PortForwardRequest {
        pod_sandbox_id: "pod-1".to_string(),
        port: vec![0],
    };
    let err = StreamingServer::validate_port_forward_request(&invalid_port).unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[test]
fn test_portforward_request_id_falls_back_from_stream_id() {
    assert_eq!(
        portforward_request_id(5, "error", None).unwrap(),
        "5".to_string()
    );
    assert_eq!(
        portforward_request_id(7, "data", None).unwrap(),
        "5".to_string()
    );
    assert!(portforward_request_id(1, "data", None).is_err());
    assert_eq!(
        portforward_request_id(11, "data", Some("42")).unwrap(),
        "42".to_string()
    );
}

#[test]
fn test_register_portforward_stream_rejects_duplicate_pending_data_stream() {
    let now = Instant::now();
    let mut pending_pairs = HashMap::new();
    let active_request_ids = HashSet::new();

    assert!(matches!(
        register_portforward_stream(
            &mut pending_pairs,
            &active_request_ids,
            now,
            1,
            PortForwardStreamRole::Data,
            "1".to_string(),
            8080,
        ),
        PortForwardStreamRegistration::Pending
    ));

    match register_portforward_stream(
        &mut pending_pairs,
        &active_request_ids,
        now,
        3,
        PortForwardStreamRole::Data,
        "1".to_string(),
        8080,
    ) {
        PortForwardStreamRegistration::RejectPendingPair { pair, message } => {
            assert_eq!(pair.request_id, "1");
            assert_eq!(pair.data_stream, Some(1));
            assert!(pair.error_stream.is_none());
            assert!(message.contains("duplicate"));
        }
        other => panic!("unexpected registration result: {:?}", other),
    }
    assert!(pending_pairs.is_empty());
}

#[test]
fn test_register_portforward_stream_rejects_conflicting_port_for_same_request() {
    let now = Instant::now();
    let mut pending_pairs = HashMap::new();
    let active_request_ids = HashSet::new();

    assert!(matches!(
        register_portforward_stream(
            &mut pending_pairs,
            &active_request_ids,
            now,
            1,
            PortForwardStreamRole::Data,
            "7".to_string(),
            8080,
        ),
        PortForwardStreamRegistration::Pending
    ));

    match register_portforward_stream(
        &mut pending_pairs,
        &active_request_ids,
        now,
        3,
        PortForwardStreamRole::Error,
        "7".to_string(),
        9090,
    ) {
        PortForwardStreamRegistration::RejectPendingPair { pair, message } => {
            assert_eq!(pair.request_id, "7");
            assert_eq!(pair.port, 8080);
            assert!(message.contains("conflicting ports"));
        }
        other => panic!("unexpected registration result: {:?}", other),
    }
    assert!(pending_pairs.is_empty());
}

#[test]
fn test_register_portforward_stream_rejects_reuse_of_active_request_id() {
    let now = Instant::now();
    let mut pending_pairs = HashMap::new();
    let active_request_ids = HashSet::from(["5".to_string()]);

    match register_portforward_stream(
        &mut pending_pairs,
        &active_request_ids,
        now,
        9,
        PortForwardStreamRole::Data,
        "5".to_string(),
        8080,
    ) {
        PortForwardStreamRegistration::RejectCurrent(message) => {
            assert!(message.contains("already has an active stream pair"));
        }
        other => panic!("unexpected registration result: {:?}", other),
    }
    assert!(pending_pairs.is_empty());
}

#[test]
fn test_take_expired_portforward_pairs_returns_only_timed_out_pairs() {
    let now = Instant::now();
    let creation_timeout = Duration::from_secs(15);
    let mut pending_pairs = HashMap::from([
        (
            "expired".to_string(),
            PortForwardPair {
                request_id: "expired".to_string(),
                port: 8080,
                data_stream: Some(1),
                error_stream: None,
                created_at: now - creation_timeout - Duration::from_secs(1),
            },
        ),
        (
            "fresh".to_string(),
            PortForwardPair {
                request_id: "fresh".to_string(),
                port: 9090,
                data_stream: Some(3),
                error_stream: None,
                created_at: now,
            },
        ),
    ]);

    let expired = take_expired_portforward_pairs(&mut pending_pairs, now, creation_timeout);
    assert_eq!(expired.len(), 1);
    assert_eq!(expired[0].request_id, "expired");
    assert!(pending_pairs.contains_key("fresh"));
    assert!(!pending_pairs.contains_key("expired"));
}

#[test]
fn test_next_portforward_pair_timeout_uses_earliest_pending_deadline() {
    let now = Instant::now();
    let creation_timeout = Duration::from_secs(20);
    let pending_pairs = HashMap::from([
        (
            "first".to_string(),
            PortForwardPair {
                request_id: "first".to_string(),
                port: 8080,
                data_stream: Some(1),
                error_stream: None,
                created_at: now - Duration::from_secs(10),
            },
        ),
        (
            "second".to_string(),
            PortForwardPair {
                request_id: "second".to_string(),
                port: 9090,
                data_stream: Some(3),
                error_stream: None,
                created_at: now - Duration::from_secs(2),
            },
        ),
    ]);

    let timeout = next_portforward_pair_timeout(&pending_pairs, now, creation_timeout).unwrap();
    assert_eq!(timeout, creation_timeout - Duration::from_secs(10));
}

#[tokio::test]
async fn test_start_with_config_uses_configured_base_url_and_port() {
    let server = StreamingServer::start_with_config(
        StreamingConfig {
            address: "127.0.0.1".to_string(),
            port: 0,
            ..StreamingConfig::default()
        },
        PathBuf::from("/bin/false"),
    )
    .await
    .unwrap();

    assert!(server.base_url().starts_with("http://127.0.0.1:"));
}

#[tokio::test]
async fn test_start_with_config_serves_https_when_tls_is_enabled() {
    let dir = tempdir().unwrap();
    let cert_path = dir.path().join("tls.crt");
    let key_path = dir.path().join("tls.key");
    std::fs::write(&cert_path, TEST_TLS_CERT_PEM).unwrap();
    std::fs::write(&key_path, TEST_TLS_KEY_PEM).unwrap();

    let server = StreamingServer::start_with_config(
        StreamingConfig {
            address: "127.0.0.1".to_string(),
            port: 0,
            enable_tls: true,
            tls_cert_file: cert_path.display().to_string(),
            tls_key_file: key_path.display().to_string(),
            ..StreamingConfig::default()
        },
        PathBuf::from("/bin/false"),
    )
    .await
    .unwrap();

    assert!(server.base_url().starts_with("https://127.0.0.1:"));

    let response = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap()
        .get(format!("{}/missing", server.base_url()))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);
}

#[test]
fn test_load_streaming_tls_config_accepts_explicit_cipher_suites() {
    let dir = tempdir().unwrap();
    let cert_path = dir.path().join("tls.crt");
    let key_path = dir.path().join("tls.key");
    std::fs::write(&cert_path, TEST_TLS_CERT_PEM).unwrap();
    std::fs::write(&key_path, TEST_TLS_KEY_PEM).unwrap();

    let config = StreamingConfig {
        enable_tls: true,
        tls_cert_file: cert_path.display().to_string(),
        tls_key_file: key_path.display().to_string(),
        tls_cipher_suites: vec![
            "TLS13_AES_256_GCM_SHA384".to_string(),
            "TLS13_AES_128_GCM_SHA256".to_string(),
        ],
        ..StreamingConfig::default()
    };

    load_streaming_tls_config(&config).expect("cipher suite list should be accepted");
}

#[test]
fn test_load_streaming_tls_config_rejects_unknown_cipher_suite() {
    let dir = tempdir().unwrap();
    let cert_path = dir.path().join("tls.crt");
    let key_path = dir.path().join("tls.key");
    std::fs::write(&cert_path, TEST_TLS_CERT_PEM).unwrap();
    std::fs::write(&key_path, TEST_TLS_KEY_PEM).unwrap();

    let config = StreamingConfig {
        enable_tls: true,
        tls_cert_file: cert_path.display().to_string(),
        tls_key_file: key_path.display().to_string(),
        tls_cipher_suites: vec!["TLS_FAKE_SUITE".to_string()],
        ..StreamingConfig::default()
    };

    let err =
        load_streaming_tls_config(&config).expect_err("unknown cipher suites should be rejected");
    assert!(err
        .to_string()
        .contains("unsupported streaming TLS cipher suite"));
}

#[test]
fn test_load_streaming_tls_config_rejects_tls12_suites_when_tls13_only() {
    let dir = tempdir().unwrap();
    let cert_path = dir.path().join("tls.crt");
    let key_path = dir.path().join("tls.key");
    std::fs::write(&cert_path, TEST_TLS_CERT_PEM).unwrap();
    std::fs::write(&key_path, TEST_TLS_KEY_PEM).unwrap();

    let config = StreamingConfig {
        enable_tls: true,
        tls_cert_file: cert_path.display().to_string(),
        tls_key_file: key_path.display().to_string(),
        tls_min_version: "VersionTLS13".to_string(),
        tls_cipher_suites: vec!["TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256".to_string()],
        ..StreamingConfig::default()
    };

    let err = load_streaming_tls_config(&config)
        .expect_err("TLS1.2-only cipher suites should be rejected when TLS1.3 is required");
    assert!(err
        .to_string()
        .contains("no usable cipher suites configured"));
}

#[test]
fn test_stream_idle_helpers_use_latest_activity() {
    let now = Instant::now();
    let activity = Arc::new(std::sync::Mutex::new(now - Duration::from_secs(5)));

    assert_eq!(
        stream_idle_remaining(&activity, now, Duration::from_secs(10)),
        Some(Duration::from_secs(5))
    );
    assert!(!stream_is_idle(&activity, now, Duration::from_secs(10)));
    assert_eq!(
        next_stream_idle_timeout(&activity, now, Duration::from_secs(10)),
        Duration::from_secs(5)
    );

    mark_stream_activity(&activity, now);
    assert_eq!(
        stream_idle_remaining(&activity, now, Duration::from_secs(10)),
        Some(Duration::from_secs(10))
    );
}

#[test]
fn test_next_portforward_wait_timeout_prefers_earlier_pair_timeout_than_idle_timeout() {
    let now = Instant::now();
    let activity = Arc::new(std::sync::Mutex::new(now));
    let pending_pairs = HashMap::from([(
        "first".to_string(),
        PortForwardPair {
            request_id: "first".to_string(),
            port: 8080,
            data_stream: Some(1),
            error_stream: None,
            created_at: now - Duration::from_secs(29),
        },
    )]);

    assert_eq!(
        next_portforward_wait_timeout(
            &pending_pairs,
            &activity,
            now,
            Duration::from_secs(60),
            Duration::from_secs(30),
        ),
        Duration::from_secs(1)
    );
}

#[tokio::test]
async fn test_write_portforward_websocket_channel_frame_roundtrips_v4_binary() {
    let (writer_stream, mut reader_stream) = tokio::io::duplex(128);
    let writer = Arc::new(Mutex::new(writer_stream));
    let activity = new_stream_activity(Instant::now() - Duration::from_secs(5));

    write_portforward_websocket_channel_frame(
        &writer,
        &activity,
        PortForwardWebsocketProtocol::V4Binary,
        2,
        b"hello",
    )
    .await
    .unwrap();

    let frame = read_websocket_frame(&mut reader_stream)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(frame.opcode, 0x2);
    let decoded =
        decode_portforward_websocket_frame(PortForwardWebsocketProtocol::V4Binary, &frame)
            .unwrap()
            .unwrap();
    assert_eq!(decoded.0, 2);
    assert_eq!(decoded.1, b"hello");
    assert!(!stream_is_idle(
        &activity,
        Instant::now(),
        Duration::from_secs(1)
    ));
}

#[tokio::test]
async fn test_write_portforward_websocket_channel_frame_roundtrips_v4_base64() {
    let (writer_stream, mut reader_stream) = tokio::io::duplex(128);
    let writer = Arc::new(Mutex::new(writer_stream));
    let activity = new_stream_activity(Instant::now() - Duration::from_secs(5));

    write_portforward_websocket_channel_frame(
        &writer,
        &activity,
        PortForwardWebsocketProtocol::V4Base64,
        3,
        b"hello",
    )
    .await
    .unwrap();

    let frame = read_websocket_frame(&mut reader_stream)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(frame.opcode, 0x1);
    let decoded =
        decode_portforward_websocket_frame(PortForwardWebsocketProtocol::V4Base64, &frame)
            .unwrap()
            .unwrap();
    assert_eq!(decoded.0, 3);
    assert_eq!(decoded.1, b"hello");
    assert!(!stream_is_idle(
        &activity,
        Instant::now(),
        Duration::from_secs(1)
    ));
}

#[test]
fn test_negotiate_portforward_protocol_accepts_v1() {
    let request = Request::builder()
        .header("X-Stream-Protocol-Version", PORT_FORWARD_PROTOCOL_V1)
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        negotiate_portforward_protocol(&request),
        Some(PORT_FORWARD_PROTOCOL_V1)
    );
}

#[test]
fn test_negotiate_portforward_websocket_protocol_prefers_v4() {
    let request = Request::builder()
        .header(
            SEC_WEBSOCKET_PROTOCOL,
            format!(
                "{}, {}",
                PORT_FORWARD_PROTOCOL_V1, PORT_FORWARD_WS_PROTOCOL_V4_BINARY
            ),
        )
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        negotiate_portforward_websocket_protocol(&request),
        Some(PORT_FORWARD_WS_PROTOCOL_V4_BINARY)
    );

    let request = Request::builder()
        .header(SEC_WEBSOCKET_PROTOCOL, PORT_FORWARD_WS_PROTOCOL_V4_BASE64)
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        negotiate_portforward_websocket_protocol(&request),
        Some(PORT_FORWARD_WS_PROTOCOL_V4_BASE64)
    );
}

#[test]
fn test_exec_exit_error_message_is_stable() {
    assert_eq!(
        exec_exit_error_message(17),
        "command terminated with non-zero exit code: 17"
    );
}

#[test]
fn test_parse_terminal_size_accepts_kubernetes_payload() {
    assert_eq!(
        parse_terminal_size(br#"{"Width":120,"Height":40}"#).unwrap(),
        (120, 40)
    );
    assert_eq!(
        parse_terminal_size(br#"{"width":80,"height":24}"#).unwrap(),
        (80, 24)
    );
}

#[test]
fn test_parse_cri_text_log_record_full_appends_newline() {
    let record =
        parse_cri_text_log_record(b"2024-01-02T03:04:05.000000000Z stdout F hello world").unwrap();
    assert_eq!(record.stream, "stdout");
    assert_eq!(record.payload, b"hello world\n");
}

#[test]
fn test_parse_cri_text_log_record_partial_preserves_open_line() {
    let record =
        parse_cri_text_log_record(b"2024-01-02T03:04:05.000000000Z stderr P partial line").unwrap();
    assert_eq!(record.stream, "stderr");
    assert_eq!(record.payload, b"partial line");
}

#[test]
fn test_parse_cri_text_log_record_accepts_future_full_tags() {
    let record = parse_cri_text_log_record(
        b"2024-01-02T03:04:05.000000000Z stdout X:meta future compatible",
    )
    .unwrap();
    assert_eq!(record.stream, "stdout");
    assert_eq!(record.payload, b"future compatible\n");
}

#[test]
fn test_parse_cri_text_log_record_uses_first_tag_for_partial_semantics() {
    let record =
        parse_cri_text_log_record(b"2024-01-02T03:04:05.000000000Z stderr P:meta partial line")
            .unwrap();
    assert_eq!(record.stream, "stderr");
    assert_eq!(record.payload, b"partial line");
}

#[test]
fn test_decode_portforward_websocket_frame_supports_base64_v4() {
    let payload = {
        let mut payload = vec![b'0'];
        payload.extend_from_slice(
            base64::engine::general_purpose::STANDARD
                .encode(b"hello")
                .as_bytes(),
        );
        payload
    };
    let frame = WebSocketFrame {
        opcode: 0x1,
        payload,
    };
    let decoded =
        decode_portforward_websocket_frame(PortForwardWebsocketProtocol::V4Base64, &frame)
            .unwrap()
            .unwrap();
    assert_eq!(decoded.0, 0);
    assert_eq!(decoded.1, b"hello");
}

#[tokio::test]
async fn test_read_attach_log_records_starts_from_tail_and_handles_truncate() {
    let dir = tempdir().unwrap();
    let log_path = dir.path().join("attach.log");
    fs::write(
        &log_path,
        b"2024-01-02T03:04:05.000000000Z stdout F old line\n",
    )
    .unwrap();

    let mut state = AttachLogFollowState::default();
    let initial = read_attach_log_records(&log_path, &mut state)
        .await
        .unwrap();
    assert!(
        initial.is_empty(),
        "initial attach fallback should start from the current end of file"
    );

    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&log_path)
        .unwrap();
    file.write_all(b"2024-01-02T03:04:06.000000000Z stdout F next line\n")
        .unwrap();
    let appended = read_attach_log_records(&log_path, &mut state)
        .await
        .unwrap();
    assert_eq!(appended.len(), 1);
    assert_eq!(appended[0].payload, b"next line\n");

    fs::write(
        &log_path,
        b"2024-01-02T03:04:07.000000000Z stdout F rotated line\n",
    )
    .unwrap();
    let rotated = read_attach_log_records(&log_path, &mut state)
        .await
        .unwrap();
    assert_eq!(rotated.len(), 1);
    assert_eq!(rotated[0].payload, b"rotated line\n");
}

#[test]
fn test_parse_terminal_size_rejects_zero_values() {
    let err = parse_terminal_size(br#"{"Width":0,"Height":24}"#).unwrap_err();
    assert!(err.to_string().contains("greater than zero"));
}

#[test]
fn test_shim_socket_path_honors_env_override() {
    std::env::set_var("CRIUS_ATTACH_SOCKET_DIR", "/tmp/crius-attach");
    let socket_path = shim_socket_path("abc123", "attach.sock");
    assert_eq!(
        socket_path,
        PathBuf::from("/tmp/crius-attach/abc123/attach.sock")
    );
    std::env::remove_var("CRIUS_ATTACH_SOCKET_DIR");
}

#[test]
fn test_with_netns_path_allows_current_namespace() {
    let result = with_netns_path(Path::new("/proc/thread-self/ns/net"), || Ok(42)).unwrap();
    assert_eq!(result, 42);
}
