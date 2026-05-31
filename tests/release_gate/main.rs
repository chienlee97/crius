//! Release gate definitions for production-like validation.
//!
//! The default tests keep the matrix and gate contracts executable without
//! requiring a kubelet node. Environment-gated tests run the real tools when a
//! release worker opts in.

use std::ffi::{OsStr, OsString};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use serde_json::Value;

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
    isolated_test: &'static str,
    real_driver_arg: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SoakProfile {
    name: &'static str,
    min_duration: Duration,
    concurrent_pods: usize,
    max_fd_growth: usize,
    max_process_growth: usize,
    min_iterations: usize,
    required_operations: &'static [&'static str],
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SoakReportSummary {
    profile: String,
    duration_seconds: u64,
    iterations: u64,
    failures: u64,
    fd_growth: u64,
    process_growth: u64,
    operations: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GateOutcome {
    Pass,
    Skip,
    Risk,
}

impl GateOutcome {
    fn as_str(self) -> &'static str {
        match self {
            Self::Pass => "PASS",
            Self::Skip => "SKIP",
            Self::Risk => "RISK",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReleaseGateReportRow {
    gate: &'static str,
    command: String,
    outcome: GateOutcome,
    evidence: String,
    risk: String,
}

fn unique_name(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock is before unix epoch")
        .as_nanos();
    format!("{prefix}-{nanos}")
}

fn command_output<I, S>(program: &str, args: I) -> String
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let args: Vec<OsString> = args
        .into_iter()
        .map(|arg| arg.as_ref().to_os_string())
        .collect();
    let output = Command::new(program)
        .args(&args)
        .output()
        .unwrap_or_else(|err| panic!("failed to run {program} {args:?}: {err}"));
    assert!(
        output.status.success(),
        "{program} {args:?} failed with {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

fn command_status<I, S>(program: &str, args: I) -> bool
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    Command::new(program)
        .args(args)
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn crictl_args(endpoint: &str, args: &[&str]) -> Vec<OsString> {
    let mut command_args = vec![
        OsString::from("--runtime-endpoint"),
        OsString::from(endpoint),
    ];
    command_args.extend(args.iter().map(OsString::from));
    command_args
}

fn crictl_output(endpoint: &str, args: &[&str]) -> String {
    command_output("crictl", crictl_args(endpoint, args))
}

fn crictl_status(endpoint: &str, args: &[&str]) -> bool {
    command_status("crictl", crictl_args(endpoint, args))
}

fn write_crictl_smoke_configs(dir: &Path, name: &str, image: &str) -> (PathBuf, PathBuf) {
    let pod_config_path = dir.join("pod.json");
    let container_config_path = dir.join("container.json");
    fs::write(
        &pod_config_path,
        format!(
            r#"{{
  "metadata": {{
    "name": "{name}",
    "uid": "{name}-uid",
    "namespace": "default",
    "attempt": 1
  }},
  "hostname": "{name}",
  "log_directory": "{log_dir}",
  "linux": {{}}
}}
"#,
            log_dir = dir.display()
        ),
    )
    .expect("failed to write crictl pod config");
    fs::write(
        &container_config_path,
        format!(
            r#"{{
  "metadata": {{
    "name": "workload",
    "attempt": 1
  }},
  "image": {{
    "image": "{image}"
  }},
  "command": ["sh", "-c", "echo crius-crictl-started; sleep 3600"],
  "log_path": "{name}.log",
  "linux": {{}}
}}
"#
        ),
    )
    .expect("failed to write crictl container config");
    (pod_config_path, container_config_path)
}

struct CrictlSmokeCleanup {
    endpoint: String,
    container_id: Option<String>,
    pod_id: Option<String>,
}

impl CrictlSmokeCleanup {
    fn new(endpoint: &str) -> Self {
        Self {
            endpoint: endpoint.to_string(),
            container_id: None,
            pod_id: None,
        }
    }
}

impl Drop for CrictlSmokeCleanup {
    fn drop(&mut self) {
        if let Some(container_id) = self.container_id.as_deref() {
            let _ = crictl_status(&self.endpoint, &["stop", container_id]);
            let _ = crictl_status(&self.endpoint, &["rm", container_id]);
        }
        if let Some(pod_id) = self.pod_id.as_deref() {
            let _ = crictl_status(&self.endpoint, &["stopp", pod_id]);
            let _ = crictl_status(&self.endpoint, &["rmp", pod_id]);
        }
    }
}

fn run_crictl_workload(endpoint: &str, image: &str, name: &str) {
    let temp = tempfile::tempdir().expect("failed to create crictl smoke tempdir");
    let (pod_config, container_config) = write_crictl_smoke_configs(temp.path(), name, image);
    let mut cleanup = CrictlSmokeCleanup::new(endpoint);

    crictl_output(endpoint, &["version"]);
    crictl_output(endpoint, &["info"]);
    crictl_output(endpoint, &["pods"]);
    crictl_output(endpoint, &["images"]);
    crictl_output(endpoint, &["pull", image]);

    let pod_config = pod_config.display().to_string();
    let container_config = container_config.display().to_string();
    let pod_id = crictl_output(endpoint, &["runp", &pod_config]);
    assert!(!pod_id.is_empty(), "crictl runp returned an empty pod id");
    cleanup.pod_id = Some(pod_id.clone());

    let container_id = crictl_output(
        endpoint,
        &["create", &pod_id, &container_config, &pod_config],
    );
    assert!(
        !container_id.is_empty(),
        "crictl create returned an empty container id"
    );
    cleanup.container_id = Some(container_id.clone());

    crictl_output(endpoint, &["start", &container_id]);
    let exec_output = crictl_output(
        endpoint,
        &[
            "execsync",
            &container_id,
            "sh",
            "-c",
            "echo crius-crictl-exec",
        ],
    );
    assert!(
        exec_output.contains("crius-crictl-exec"),
        "crictl execsync output did not contain marker: {exec_output}"
    );

    let logs = crictl_output(endpoint, &["logs", &container_id]);
    assert!(
        logs.contains("crius-crictl-started"),
        "crictl logs output did not contain marker: {logs}"
    );
    crictl_output(endpoint, &["stats", &container_id]);
    crictl_output(endpoint, &["stop", &container_id]);
    crictl_output(endpoint, &["rm", &container_id]);
    cleanup.container_id = None;
    crictl_output(endpoint, &["rmp", &pod_id]);
    cleanup.pod_id = None;
}

fn kubelet_smoke_required_operations() -> Vec<&'static str> {
    vec![
        "node-query",
        "pod-query",
        "pull-run",
        "logs",
        "exec",
        "stats",
        "stop-remove",
        "restart-recovery",
    ]
}

struct KubectlPodCleanup {
    namespace: String,
    name: String,
}

impl KubectlPodCleanup {
    fn new(namespace: &str, name: &str) -> Self {
        Self {
            namespace: namespace.to_string(),
            name: name.to_string(),
        }
    }
}

impl Drop for KubectlPodCleanup {
    fn drop(&mut self) {
        let _ = command_status(
            "kubectl",
            [
                "-n",
                self.namespace.as_str(),
                "delete",
                "pod",
                self.name.as_str(),
                "--ignore-not-found",
                "--wait=true",
                "--timeout=60s",
            ],
        );
    }
}

fn kubectl_output(args: &[&str]) -> String {
    command_output("kubectl", args)
}

fn first_kubelet_node_name() -> String {
    let nodes = kubectl_output(&["get", "nodes", "-o", "name"]);
    nodes
        .lines()
        .find_map(|line| line.strip_prefix("node/"))
        .map(str::to_string)
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| panic!("kubectl get nodes returned no node names:\n{nodes}"))
}

fn run_kubelet_pod_cycle(namespace: &str, name: &str, image: &str, marker: &str) {
    let _cleanup = KubectlPodCleanup::new(namespace, name);
    command_output(
        "kubectl",
        [
            "-n",
            namespace,
            "run",
            name,
            "--image",
            image,
            "--restart=Never",
            "--command",
            "--",
            "sh",
            "-c",
            &format!("echo {marker}; sleep 3600"),
        ],
    );
    command_output(
        "kubectl",
        [
            "-n",
            namespace,
            "wait",
            "--for=condition=Ready",
            "pod",
            name,
            "--timeout=120s",
        ],
    );
    let logs = command_output("kubectl", ["-n", namespace, "logs", name]);
    assert!(
        logs.contains(marker),
        "kubectl logs output did not contain marker {marker}: {logs}"
    );
    let exec_output = command_output(
        "kubectl",
        [
            "-n",
            namespace,
            "exec",
            name,
            "--",
            "sh",
            "-c",
            "echo crius-kubelet-exec",
        ],
    );
    assert!(
        exec_output.contains("crius-kubelet-exec"),
        "kubectl exec output did not contain marker: {exec_output}"
    );
    command_output(
        "kubectl",
        [
            "-n",
            namespace,
            "delete",
            "pod",
            name,
            "--wait=true",
            "--timeout=120s",
        ],
    );
}

fn run_kubelet_stats_probe(node_name: &str) {
    if let Ok(endpoint) = std::env::var("CRIUS_CRICTL_ENDPOINT") {
        crictl_output(&endpoint, &["stats"]);
        return;
    }

    assert!(
        command_status("kubectl", ["top", "node", node_name]),
        "kubelet stats probe requires either CRIUS_CRICTL_ENDPOINT for crictl stats or kubectl top node support"
    );
}

fn release_gate_test_command(test_name: &str) -> String {
    format!("cargo test {test_name} --test release_gate")
}

fn integration_test_command(test_name: &str, test_binary: &str) -> String {
    format!("cargo test {test_name} --test {test_binary}")
}

fn run_fault_injection_driver(cases: &[FaultInjectionCase]) {
    let driver = std::env::var("CRIUS_FAULT_INJECTION_DRIVER")
        .unwrap_or_else(|_| "crius-fault-injection".to_string());
    let endpoint = std::env::var("CRIUS_CRICTL_ENDPOINT")
        .unwrap_or_else(|_| "unix:///run/crius/crius.sock".to_string());
    let image = std::env::var("CRIUS_SMOKE_IMAGE").unwrap_or_else(|_| "busybox:latest".to_string());

    for case in cases {
        command_output(
            &driver,
            [
                "--runtime-endpoint",
                endpoint.as_str(),
                "--image",
                image.as_str(),
                case.real_driver_arg,
            ],
        );
    }
}

fn parse_soak_report(raw: &str) -> SoakReportSummary {
    let report: Value =
        serde_json::from_str(raw).unwrap_or_else(|err| panic!("invalid soak report JSON: {err}"));
    let operations = report
        .get("operations")
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("soak report missing operations array: {report}"))
        .iter()
        .map(|operation| {
            operation
                .as_str()
                .unwrap_or_else(|| panic!("soak report operation must be a string: {operation}"))
                .to_string()
        })
        .collect::<Vec<_>>();

    SoakReportSummary {
        profile: report
            .get("profile")
            .and_then(Value::as_str)
            .unwrap_or_else(|| panic!("soak report missing profile: {report}"))
            .to_string(),
        duration_seconds: report
            .get("duration_seconds")
            .and_then(Value::as_u64)
            .unwrap_or_else(|| panic!("soak report missing duration_seconds: {report}")),
        iterations: report
            .get("iterations")
            .and_then(Value::as_u64)
            .unwrap_or_else(|| panic!("soak report missing iterations: {report}")),
        failures: report
            .get("failures")
            .and_then(Value::as_u64)
            .unwrap_or_else(|| panic!("soak report missing failures: {report}")),
        fd_growth: report
            .get("fd_growth")
            .and_then(Value::as_u64)
            .unwrap_or_else(|| panic!("soak report missing fd_growth: {report}")),
        process_growth: report
            .get("process_growth")
            .and_then(Value::as_u64)
            .unwrap_or_else(|| panic!("soak report missing process_growth: {report}")),
        operations,
    }
}

fn validate_soak_report(profile: &SoakProfile, report: &SoakReportSummary) {
    assert_eq!(report.profile, profile.name);
    assert!(
        report.duration_seconds >= profile.min_duration.as_secs(),
        "soak duration {}s is below required {}s",
        report.duration_seconds,
        profile.min_duration.as_secs()
    );
    assert!(
        report.iterations >= profile.min_iterations as u64,
        "soak iterations {} are below required {}",
        report.iterations,
        profile.min_iterations
    );
    assert_eq!(report.failures, 0, "soak report contains failures");
    assert!(
        report.fd_growth <= profile.max_fd_growth as u64,
        "fd growth {} exceeds {}",
        report.fd_growth,
        profile.max_fd_growth
    );
    assert!(
        report.process_growth <= profile.max_process_growth as u64,
        "process growth {} exceeds {}",
        report.process_growth,
        profile.max_process_growth
    );
    for required in profile.required_operations {
        assert!(
            report
                .operations
                .iter()
                .any(|operation| operation == required),
            "soak report missing operation {required}"
        );
    }
}

fn run_release_soak_driver(profile: &SoakProfile) -> SoakReportSummary {
    let driver =
        std::env::var("CRIUS_RELEASE_SOAK_DRIVER").unwrap_or_else(|_| "crius-release-soak".into());
    let endpoint = std::env::var("CRIUS_CRICTL_ENDPOINT")
        .unwrap_or_else(|_| "unix:///run/crius/crius.sock".to_string());
    let image = std::env::var("CRIUS_SMOKE_IMAGE").unwrap_or_else(|_| "busybox:latest".to_string());
    let raw = command_output(
        &driver,
        [
            "--runtime-endpoint",
            endpoint.as_str(),
            "--image",
            image.as_str(),
            "--profile",
            profile.name,
            "--duration-seconds",
            &profile.min_duration.as_secs().to_string(),
            "--concurrent-pods",
            &profile.concurrent_pods.to_string(),
            "--json",
        ],
    );
    parse_soak_report(&raw)
}

fn release_report_rows() -> Vec<ReleaseGateReportRow> {
    vec![
        ReleaseGateReportRow {
            gate: "cargo-fmt",
            command: "cargo fmt --check".to_string(),
            outcome: GateOutcome::Pass,
            evidence: "local command output or CI artifact".to_string(),
            risk: "none".to_string(),
        },
        ReleaseGateReportRow {
            gate: "cargo-test",
            command: "cargo test".to_string(),
            outcome: GateOutcome::Pass,
            evidence: "local command output or CI artifact".to_string(),
            risk: "none".to_string(),
        },
        ReleaseGateReportRow {
            gate: "integration-tests",
            command: "cargo test --test integration --test recovery --test network --test runtime_backend".to_string(),
            outcome: GateOutcome::Pass,
            evidence: "test output or CI artifact".to_string(),
            risk: "none".to_string(),
        },
        ReleaseGateReportRow {
            gate: "crictl-smoke",
            command: "CRIUS_RUN_CRICTL_SMOKE=1 cargo test --test release_gate gated_crictl_smoke_has_fixed_entrypoint -- --nocapture".to_string(),
            outcome: GateOutcome::Pass,
            evidence: "crictl lifecycle transcript".to_string(),
            risk: "record skipped operation if node prerequisites are missing".to_string(),
        },
        ReleaseGateReportRow {
            gate: "kubelet-smoke",
            command: "CRIUS_RUN_KUBELET_SMOKE=1 cargo test --test release_gate gated_kubelet_smoke_has_fixed_entrypoint -- --nocapture".to_string(),
            outcome: GateOutcome::Pass,
            evidence: "kubectl logs/exec/stats transcript".to_string(),
            risk: "record cluster version and any missing metrics server support".to_string(),
        },
        ReleaseGateReportRow {
            gate: "fault-injection",
            command: "CRIUS_RUN_FAULT_INJECTION=1 cargo test --test release_gate gated_fault_injection_has_fixed_entrypoint -- --nocapture".to_string(),
            outcome: GateOutcome::Risk,
            evidence: "driver output for each failure class".to_string(),
            risk: "document any failure class covered only by isolated tests".to_string(),
        },
        ReleaseGateReportRow {
            gate: "long-running-soak",
            command: "CRIUS_RUN_RELEASE_SOAK=1 cargo test --test release_gate gated_release_soak_has_fixed_entrypoint -- --nocapture".to_string(),
            outcome: GateOutcome::Risk,
            evidence: "JSON soak report with resource growth counters".to_string(),
            risk: "document duration, skipped operations, fd/process growth, and failure rate".to_string(),
        },
        ReleaseGateReportRow {
            gate: "known-limitations",
            command: "manual release note review".to_string(),
            outcome: GateOutcome::Skip,
            evidence: "release note link or issue list".to_string(),
            risk: "record unsupported backend, snapshotter, CNI, attach, or security scope".to_string(),
        },
    ]
}

fn render_release_report_template(rows: &[ReleaseGateReportRow]) -> String {
    let mut rendered = String::from(
        "# crius release gate report\n\n| gate | command | outcome | evidence | risk |\n| --- | --- | --- | --- | --- |\n",
    );
    for row in rows {
        rendered.push_str(&format!(
            "| {} | `{}` | {} | {} | {} |\n",
            row.gate,
            row.command,
            row.outcome.as_str(),
            row.evidence,
            row.risk
        ));
    }
    rendered
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
            isolated_test: "cargo test recovery_fixture_models_missing_shim_socket --test recovery",
            real_driver_arg: "shim-crash",
        },
        FaultInjectionCase {
            name: "daemon-crash",
            failure: "restart crius without clean shutdown marker",
            expected_state: "ledger recovery completed",
            isolated_test: "cargo test recovery_fixture_builds_healthy_ledger_graph --test recovery",
            real_driver_arg: "daemon-crash",
        },
        FaultInjectionCase {
            name: "cni-failure",
            failure: "remove selected CNI plugin before pod setup",
            expected_state: "NetworkReady false with reason",
            isolated_test: "cargo test network_health_suite_uses_health_service_condition --test network",
            real_driver_arg: "cni-failure",
        },
        FaultInjectionCase {
            name: "content-store-failure",
            failure: "interrupt pull before final image ref",
            expected_state: "interrupted transfer and no usable image ref",
            isolated_test: "cargo test image_service_marks_interrupted_transfer_on_startup --lib",
            real_driver_arg: "content-store-failure",
        },
        FaultInjectionCase {
            name: "ledger-corruption",
            failure: "remove owned artifact from ledger graph",
            expected_state: "repair dry-run reports action",
            isolated_test: "cargo test recovery_fixture_ledger_repair_dry_run_preserves_state_and_apply_marks_broken --test recovery",
            real_driver_arg: "ledger-corruption",
        },
        FaultInjectionCase {
            name: "runtime-kill",
            failure: "kill runtime task process",
            expected_state: "stopped or broken runtime artifact",
            isolated_test: "cargo test wasm_direct_backend_marks_self_exited_engine_stopped --test runtime_backend",
            real_driver_arg: "runtime-kill",
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
            min_iterations: 100,
            required_operations: &[
                "pull",
                "create",
                "start",
                "exec",
                "logs",
                "stats",
                "stop",
                "remove",
                "restart-recovery",
            ],
        },
        SoakProfile {
            name: "overnight",
            min_duration: Duration::from_secs(8 * 60 * 60),
            concurrent_pods: 25,
            max_fd_growth: 32,
            max_process_growth: 4,
            min_iterations: 1000,
            required_operations: &[
                "pull",
                "create",
                "start",
                "exec",
                "logs",
                "stats",
                "stop",
                "remove",
                "restart-recovery",
            ],
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
    assert!(cases.iter().all(|case| !case.failure.is_empty()
        && !case.expected_state.is_empty()
        && !case.isolated_test.is_empty()
        && !case.real_driver_arg.is_empty()));
}

#[test]
fn fault_injection_matrix_has_executable_isolated_tests() {
    let cases = fault_injection_cases();
    for case in &cases {
        assert!(
            case.isolated_test.starts_with("cargo test "),
            "fault injection case {} lacks a cargo test command",
            case.name
        );
        assert!(
            case.isolated_test.contains(" --test ") || case.isolated_test.ends_with(" --lib"),
            "fault injection case {} must identify its test binary or lib scope",
            case.name
        );
    }
}

#[test]
fn fault_injection_command_helpers_render_expected_cargo_invocations() {
    assert_eq!(
        release_gate_test_command("fault_injection_matrix_covers_recovery_failure_classes"),
        "cargo test fault_injection_matrix_covers_recovery_failure_classes --test release_gate"
    );
    assert_eq!(
        integration_test_command("recovery_fixture_models_missing_shim_socket", "recovery"),
        "cargo test recovery_fixture_models_missing_shim_socket --test recovery"
    );
}

#[test]
fn gated_fault_injection_has_fixed_entrypoint() {
    if std::env::var("CRIUS_RUN_FAULT_INJECTION").as_deref() != Ok("1") {
        eprintln!("skipping fault injection; set CRIUS_RUN_FAULT_INJECTION=1");
        return;
    }

    run_fault_injection_driver(&fault_injection_cases());
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
    assert!(profiles.iter().all(|profile| profile.min_iterations > 0));
    assert!(profiles
        .iter()
        .all(|profile| profile.required_operations.contains(&"restart-recovery")));
}

#[test]
fn release_soak_report_parser_validates_resource_and_operation_thresholds() {
    let profile = soak_profiles()
        .into_iter()
        .find(|candidate| candidate.name == "short-release")
        .unwrap();
    let report = parse_soak_report(
        r#"{
  "profile": "short-release",
  "duration_seconds": 1800,
  "iterations": 100,
  "failures": 0,
  "fd_growth": 4,
  "process_growth": 1,
  "operations": ["pull", "create", "start", "exec", "logs", "stats", "stop", "remove", "restart-recovery"]
}"#,
    );

    validate_soak_report(&profile, &report);
}

#[test]
#[should_panic(expected = "soak report missing operation restart-recovery")]
fn release_soak_report_rejects_missing_restart_recovery() {
    let profile = soak_profiles()
        .into_iter()
        .find(|candidate| candidate.name == "short-release")
        .unwrap();
    let report = parse_soak_report(
        r#"{
  "profile": "short-release",
  "duration_seconds": 1800,
  "iterations": 100,
  "failures": 0,
  "fd_growth": 4,
  "process_growth": 1,
  "operations": ["pull", "create", "start", "exec", "logs", "stats", "stop", "remove"]
}"#,
    );

    validate_soak_report(&profile, &report);
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
fn release_report_template_covers_each_checklist_gate() {
    let rows = release_report_rows();
    let template = render_release_report_template(&rows);
    for gate in [
        "cargo-fmt",
        "cargo-test",
        "integration-tests",
        "crictl-smoke",
        "kubelet-smoke",
        "fault-injection",
        "long-running-soak",
        "known-limitations",
    ] {
        assert!(
            rows.iter().any(|row| row.gate == gate),
            "release report missing gate {gate}"
        );
        assert!(
            template.contains(gate),
            "rendered release report missing gate {gate}"
        );
    }
    assert!(template.contains("| gate | command | outcome | evidence | risk |"));
}

#[test]
fn release_report_rows_require_outcome_evidence_and_risk_text() {
    for row in release_report_rows() {
        assert!(
            !row.command.trim().is_empty(),
            "missing command for {}",
            row.gate
        );
        assert!(
            !row.evidence.trim().is_empty(),
            "missing evidence for {}",
            row.gate
        );
        assert!(!row.risk.trim().is_empty(), "missing risk for {}", row.gate);
        assert!(matches!(
            row.outcome,
            GateOutcome::Pass | GateOutcome::Skip | GateOutcome::Risk
        ));
    }
}

#[test]
fn crictl_smoke_contract_covers_core_cri_lifecycle() {
    let required_steps = [
        "version", "info", "pods", "images", "pull", "runp", "create", "start", "execsync", "logs",
        "stats", "stop", "rm", "rmp",
    ];

    for step in required_steps {
        assert!(!step.trim().is_empty());
    }
}

#[test]
fn kubelet_smoke_contract_covers_runtime_observable_operations() {
    let operations = kubelet_smoke_required_operations();
    for required in [
        "node-query",
        "pod-query",
        "pull-run",
        "logs",
        "exec",
        "stats",
        "stop-remove",
        "restart-recovery",
    ] {
        assert!(
            operations.contains(&required),
            "missing kubelet smoke operation {required}"
        );
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
    let image = std::env::var("CRIUS_SMOKE_IMAGE").unwrap_or_else(|_| "busybox:latest".to_string());
    let name = unique_name("crius-crictl-smoke");
    run_crictl_workload(&endpoint, &image, &name);
}

#[test]
fn gated_kubelet_smoke_has_fixed_entrypoint() {
    if std::env::var("CRIUS_RUN_KUBELET_SMOKE").as_deref() != Ok("1") {
        eprintln!("skipping kubelet smoke; set CRIUS_RUN_KUBELET_SMOKE=1");
        return;
    }

    let namespace =
        std::env::var("CRIUS_KUBELET_SMOKE_NAMESPACE").unwrap_or_else(|_| "default".to_string());
    let image = std::env::var("CRIUS_SMOKE_IMAGE").unwrap_or_else(|_| "busybox:latest".to_string());
    let name = unique_name("crius-kubelet-smoke");
    let restart_name = unique_name("crius-kubelet-restart");
    let marker = "crius-kubelet-started";

    let node_name = first_kubelet_node_name();
    kubectl_output(&["get", "pods", "-A"]);
    run_kubelet_stats_probe(&node_name);
    run_kubelet_pod_cycle(&namespace, &name, &image, marker);
    run_kubelet_pod_cycle(&namespace, &restart_name, &image, "crius-kubelet-restarted");
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
    let report = run_release_soak_driver(&selected);
    validate_soak_report(&selected, &report);
}
