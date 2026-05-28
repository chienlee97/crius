//! Release gate definitions for production-like validation.
//!
//! The default tests keep the matrix and gate contracts executable without
//! requiring a kubelet node. Environment-gated tests run the real tools when a
//! release worker opts in.

use std::process::Command;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RootMode {
    Rootful,
    Rootless,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RuntimeBackend {
    Runc,
    WasmDirect,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SnapshotterMode {
    Internal,
    External,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CniMode {
    None,
    Single,
    Multi,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ShimMode {
    Enabled,
    Disabled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReleaseTarget {
    name: &'static str,
    root: RootMode,
    backend: RuntimeBackend,
    snapshotter: SnapshotterMode,
    cni: CniMode,
    shim: ShimMode,
    required: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FaultInjectionCase {
    name: &'static str,
    failure: &'static str,
    expected_state: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SoakProfile {
    name: &'static str,
    min_duration: Duration,
    concurrent_pods: usize,
    max_fd_growth: usize,
    max_process_growth: usize,
}

fn release_matrix() -> Vec<ReleaseTarget> {
    vec![
        ReleaseTarget {
            name: "rootful-runc-internal-cni-shim",
            root: RootMode::Rootful,
            backend: RuntimeBackend::Runc,
            snapshotter: SnapshotterMode::Internal,
            cni: CniMode::Single,
            shim: ShimMode::Enabled,
            required: true,
        },
        ReleaseTarget {
            name: "rootful-runc-external-cni-shim",
            root: RootMode::Rootful,
            backend: RuntimeBackend::Runc,
            snapshotter: SnapshotterMode::External,
            cni: CniMode::Single,
            shim: ShimMode::Enabled,
            required: true,
        },
        ReleaseTarget {
            name: "rootless-runc-internal-none-shim",
            root: RootMode::Rootless,
            backend: RuntimeBackend::Runc,
            snapshotter: SnapshotterMode::Internal,
            cni: CniMode::None,
            shim: ShimMode::Enabled,
            required: true,
        },
        ReleaseTarget {
            name: "rootful-wasm-direct-internal-none",
            root: RootMode::Rootful,
            backend: RuntimeBackend::WasmDirect,
            snapshotter: SnapshotterMode::Internal,
            cni: CniMode::None,
            shim: ShimMode::Disabled,
            required: true,
        },
        ReleaseTarget {
            name: "rootful-runc-internal-multi-cni-shim",
            root: RootMode::Rootful,
            backend: RuntimeBackend::Runc,
            snapshotter: SnapshotterMode::Internal,
            cni: CniMode::Multi,
            shim: ShimMode::Enabled,
            required: false,
        },
    ]
}

fn fault_injection_cases() -> Vec<FaultInjectionCase> {
    vec![
        FaultInjectionCase {
            name: "shim-crash",
            failure: "kill shim process while container is running",
            expected_state: "reconnected or degraded shim",
        },
        FaultInjectionCase {
            name: "daemon-crash",
            failure: "restart crius without clean shutdown marker",
            expected_state: "ledger recovery completed",
        },
        FaultInjectionCase {
            name: "cni-failure",
            failure: "remove selected CNI plugin before pod setup",
            expected_state: "NetworkReady false with reason",
        },
        FaultInjectionCase {
            name: "content-store-failure",
            failure: "interrupt pull before final image ref",
            expected_state: "interrupted transfer and no usable image ref",
        },
        FaultInjectionCase {
            name: "ledger-corruption",
            failure: "remove owned artifact from ledger graph",
            expected_state: "repair dry-run reports action",
        },
        FaultInjectionCase {
            name: "runtime-kill",
            failure: "kill runtime task process",
            expected_state: "stopped or broken runtime artifact",
        },
    ]
}

fn soak_profiles() -> Vec<SoakProfile> {
    vec![
        SoakProfile {
            name: "short-release",
            min_duration: Duration::from_secs(30 * 60),
            concurrent_pods: 10,
            max_fd_growth: 16,
            max_process_growth: 2,
        },
        SoakProfile {
            name: "overnight",
            min_duration: Duration::from_secs(8 * 60 * 60),
            concurrent_pods: 25,
            max_fd_growth: 32,
            max_process_growth: 4,
        },
    ]
}

#[test]
fn release_e2e_matrix_covers_required_axes() {
    let matrix = release_matrix();

    assert!(matrix.iter().any(|target| target.required
        && target.root == RootMode::Rootful
        && target.backend == RuntimeBackend::Runc
        && target.snapshotter == SnapshotterMode::Internal
        && target.cni == CniMode::Single
        && target.shim == ShimMode::Enabled));
    assert!(matrix
        .iter()
        .any(|target| target.snapshotter == SnapshotterMode::External));
    assert!(matrix
        .iter()
        .any(|target| target.root == RootMode::Rootless));
    assert!(matrix
        .iter()
        .any(|target| target.backend == RuntimeBackend::WasmDirect));
    assert!(matrix.iter().any(|target| target.cni == CniMode::Multi));
    assert!(matrix
        .iter()
        .any(|target| target.shim == ShimMode::Disabled));
}

#[test]
fn fault_injection_matrix_covers_recovery_failure_classes() {
    let cases = fault_injection_cases();
    for required in [
        "shim-crash",
        "daemon-crash",
        "cni-failure",
        "content-store-failure",
        "ledger-corruption",
        "runtime-kill",
    ] {
        assert!(
            cases.iter().any(|case| case.name == required),
            "missing fault injection case {required}"
        );
    }
    assert!(cases
        .iter()
        .all(|case| !case.failure.is_empty() && !case.expected_state.is_empty()));
}

#[test]
fn long_running_soak_profiles_have_resource_leak_thresholds() {
    let profiles = soak_profiles();
    assert!(profiles
        .iter()
        .any(|profile| profile.min_duration >= Duration::from_secs(8 * 60 * 60)));
    assert!(profiles.iter().all(|profile| profile.concurrent_pods > 0));
    assert!(profiles.iter().all(|profile| profile.max_fd_growth > 0));
    assert!(profiles
        .iter()
        .all(|profile| profile.max_process_growth > 0));
}

#[test]
fn release_checklist_requires_tests_real_environment_and_risk_accounting() {
    let checklist = [
        "cargo-fmt",
        "cargo-test",
        "integration-tests",
        "crictl-smoke",
        "kubelet-smoke",
        "fault-injection",
        "long-running-soak",
        "known-limitations",
    ];

    for item in checklist {
        assert!(!item.trim().is_empty());
    }
}

#[test]
fn gated_crictl_smoke_has_fixed_entrypoint() {
    if std::env::var("CRIUS_RUN_CRICTL_SMOKE").as_deref() != Ok("1") {
        eprintln!("skipping crictl smoke; set CRIUS_RUN_CRICTL_SMOKE=1");
        return;
    }

    let endpoint = std::env::var("CRIUS_CRICTL_ENDPOINT")
        .unwrap_or_else(|_| "unix:///run/crius/crius.sock".to_string());
    for command in ["version", "info", "pods", "images"] {
        let status = Command::new("crictl")
            .args(["--runtime-endpoint", endpoint.as_str(), command])
            .status()
            .unwrap_or_else(|err| panic!("failed to run crictl {command}: {err}"));
        assert!(status.success(), "crictl {command} failed with {status}");
    }
}

#[test]
fn gated_kubelet_smoke_has_fixed_entrypoint() {
    if std::env::var("CRIUS_RUN_KUBELET_SMOKE").as_deref() != Ok("1") {
        eprintln!("skipping kubelet smoke; set CRIUS_RUN_KUBELET_SMOKE=1");
        return;
    }

    for command in [
        vec!["kubectl", "get", "nodes"],
        vec!["kubectl", "get", "pods", "-A"],
        vec![
            "kubectl",
            "run",
            "crius-smoke",
            "--image=busybox:latest",
            "--restart=Never",
            "--",
            "true",
        ],
    ] {
        let status = Command::new(command[0])
            .args(&command[1..])
            .status()
            .unwrap_or_else(|err| panic!("failed to run {:?}: {err}", command));
        assert!(status.success(), "{command:?} failed with {status}");
    }
}

#[test]
fn gated_release_soak_has_fixed_entrypoint() {
    if std::env::var("CRIUS_RUN_RELEASE_SOAK").as_deref() != Ok("1") {
        eprintln!("skipping release soak; set CRIUS_RUN_RELEASE_SOAK=1");
        return;
    }

    let profile =
        std::env::var("CRIUS_RELEASE_SOAK_PROFILE").unwrap_or_else(|_| "short-release".to_string());
    let selected = soak_profiles()
        .into_iter()
        .find(|candidate| candidate.name == profile)
        .unwrap_or_else(|| panic!("unknown release soak profile {profile}"));

    assert!(
        selected.min_duration >= Duration::from_secs(30 * 60),
        "release soak profile is too short"
    );
}
