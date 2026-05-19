use super::*;
use std::sync::{Mutex, OnceLock};
use tempfile::tempdir;

#[test]
fn runtime_handlers_always_include_default_runtime() {
    let config = RuntimeConfig {
        runtime_type: "runc".to_string(),
        handlers: vec!["kata".to_string(), "runc".to_string(), "".to_string()],
        ..Default::default()
    };

    assert_eq!(config.normalized_handlers(), vec!["kata", "runc"]);
}

#[test]
fn resolved_runtimes_merge_default_and_handler_specific_entries() {
    let config = RuntimeConfig {
        runtime_type: "runc".to_string(),
        runtime_path: "/usr/bin/runc".to_string(),
        root: "/run/crius".to_string(),
        monitor_env: vec!["PATH=/usr/bin".to_string()],
        handlers: vec!["kata".to_string(), "crun".to_string()],
        runtimes: HashMap::from([
            (
                "kata".to_string(),
                RuntimeHandlerConfig {
                    backend: "runc".to_string(),
                    backend_options: HashMap::new(),
                    runtime_path: "/usr/bin/kata-runtime".to_string(),
                    runtime_root: "/run/crius/kata".to_string(),
                    runtime_config_path: String::new(),
                    platform_runtime_paths: HashMap::new(),
                    monitor_path: "/usr/bin/kata-shim".to_string(),
                    monitor_cgroup: None,
                    monitor_env: Some(vec!["RUST_LOG=debug".to_string()]),
                    stream_websockets: None,
                    allowed_annotations: vec!["io.example.runtime/".to_string()],
                    default_annotations: HashMap::from([(
                        "io.example.runtime/default".to_string(),
                        "kata".to_string(),
                    )]),
                    privileged_without_host_devices: false,
                    privileged_without_host_devices_all_devices_allowed: false,
                    container_create_timeout: Some(15),
                    snapshotter: "internal-cached-rootfs".to_string(),
                    cni_conf_dir: String::new(),
                    cni_max_conf_num: None,
                    inherit_default_runtime: false,
                },
            ),
            (
                "crun".to_string(),
                RuntimeHandlerConfig {
                    inherit_default_runtime: true,
                    ..Default::default()
                },
            ),
        ]),
        ..Default::default()
    };

    let resolved = config.resolved_runtimes().unwrap();
    let expected_monitor_cgroup =
        resolve_monitor_cgroup("", config.effective_cgroup_driver()).unwrap();
    assert_eq!(
        resolved.get("runc"),
        Some(&ResolvedRuntimeHandlerConfig {
            backend: "runc".to_string(),
            backend_options: HashMap::new(),
            runtime_path: "/usr/bin/runc".to_string(),
            runtime_config_path: String::new(),
            runtime_root: "/run/crius".to_string(),
            platform_runtime_paths: HashMap::new(),
            monitor_path: "/usr/bin/crius-shim".to_string(),
            monitor_cgroup: expected_monitor_cgroup.clone(),
            monitor_env: vec!["PATH=/usr/bin".to_string()],
            stream_websockets: false,
            allowed_annotations: Vec::new(),
            default_annotations: HashMap::new(),
            privileged_without_host_devices: false,
            privileged_without_host_devices_all_devices_allowed: false,
            container_create_timeout: DEFAULT_CONTAINER_CREATE_TIMEOUT_SECS,
            snapshotter: "internal-overlay-untar".to_string(),
        })
    );
    assert_eq!(
        resolved.get("kata"),
        Some(&ResolvedRuntimeHandlerConfig {
            backend: "runc".to_string(),
            backend_options: HashMap::new(),
            runtime_path: "/usr/bin/kata-runtime".to_string(),
            runtime_config_path: String::new(),
            runtime_root: "/run/crius/kata".to_string(),
            platform_runtime_paths: HashMap::new(),
            monitor_path: "/usr/bin/kata-shim".to_string(),
            monitor_cgroup: expected_monitor_cgroup.clone(),
            monitor_env: vec!["RUST_LOG=debug".to_string()],
            stream_websockets: false,
            allowed_annotations: vec!["io.example.runtime/".to_string()],
            default_annotations: HashMap::from([(
                "io.example.runtime/default".to_string(),
                "kata".to_string(),
            )]),
            privileged_without_host_devices: false,
            privileged_without_host_devices_all_devices_allowed: false,
            container_create_timeout: MIN_CONTAINER_CREATE_TIMEOUT_SECS,
            snapshotter: "internal-cached-rootfs".to_string(),
        })
    );
    assert_eq!(
        resolved.get("crun"),
        Some(&ResolvedRuntimeHandlerConfig {
            backend: "runc".to_string(),
            backend_options: HashMap::new(),
            runtime_path: "/usr/bin/runc".to_string(),
            runtime_config_path: String::new(),
            runtime_root: "/run/crius".to_string(),
            platform_runtime_paths: HashMap::new(),
            monitor_path: "/usr/bin/crius-shim".to_string(),
            monitor_cgroup: expected_monitor_cgroup,
            monitor_env: vec!["PATH=/usr/bin".to_string()],
            stream_websockets: false,
            allowed_annotations: Vec::new(),
            default_annotations: HashMap::new(),
            privileged_without_host_devices: false,
            privileged_without_host_devices_all_devices_allowed: false,
            container_create_timeout: DEFAULT_CONTAINER_CREATE_TIMEOUT_SECS,
            snapshotter: "internal-overlay-untar".to_string(),
        })
    );
}

#[test]
fn validate_rejects_non_default_handler_without_runtime_table() {
    let mut config = Config::default();
    config.runtime.handlers = vec!["kata".to_string()];

    let err = config
        .validate()
        .expect_err("missing runtime table must fail");
    assert!(err
        .to_string()
        .contains("runtime handler kata requires [runtime.runtimes.kata]"));
}

#[test]
fn validate_rejects_empty_pause_command() {
    let mut config = Config::default();
    config.runtime.pause_command.clear();

    let err = config
        .validate()
        .expect_err("empty pause command must fail");
    assert!(err
        .to_string()
        .contains("runtime.pause_command must not be empty"));
}

#[test]
fn network_config_accepts_legacy_single_config_dir() {
    let config: Config = toml::from_str(
        r#"
            root = "/var/lib/crius"

            [network]
            plugin = "cni"
            config_dir = "/etc/cni/net.d"
            "#,
    )
    .expect("legacy config_dir should deserialize");

    assert_eq!(config.network.config_dirs, vec!["/etc/cni/net.d"]);
}

#[test]
fn network_config_accepts_default_network_name() {
    let config: Config = toml::from_str(
        r#"
            root = "/var/lib/crius"

            [network]
            plugin = "cni"
            default_network_name = "crio-bridge"
            "#,
    )
    .expect("default network name should deserialize");

    assert_eq!(
        config.network.default_network_name.as_deref(),
        Some("crio-bridge")
    );
    assert_eq!(
        config.network.cni_config().default_network_name(),
        Some("crio-bridge")
    );
}

#[test]
fn network_config_accepts_crio_default_network_alias() {
    let config: Config = toml::from_str(
        r#"
            root = "/var/lib/crius"

            [network]
            plugin = "cni"
            cni_default_network = "crio-bridge"
            "#,
    )
    .expect("CRI-O-style default network alias should deserialize");

    assert_eq!(
        config.network.default_network_name.as_deref(),
        Some("crio-bridge")
    );
}

#[test]
fn network_config_accepts_disable_hostport_mapping() {
    let config: Config = toml::from_str(
        r#"
            root = "/var/lib/crius"

            [network]
            plugin = "cni"
            disable_hostport_mapping = true
            "#,
    )
    .expect("disable_hostport_mapping should deserialize");

    assert!(config.network.disable_hostport_mapping);
    assert!(config.network.cni_config().disable_hostport_mapping());
}

#[test]
fn network_config_accepts_netns_mounts_under_state_dir() {
    let config: Config = toml::from_str(
        r#"
            root = "/var/lib/crius"

            [network]
            plugin = "cni"
            netns_mounts_under_state_dir = true
            "#,
    )
    .expect("netns_mounts_under_state_dir should deserialize");

    assert!(config.network.netns_mounts_under_state_dir);
    assert!(config.network.cni_config().netns_mounts_under_state_dir());
}

#[test]
fn network_config_accepts_teardown_timeout() {
    let config: Config = toml::from_str(
        r#"
            root = "/var/lib/crius"

            [network]
            plugin = "cni"
            teardown_timeout = "90s"
            "#,
    )
    .expect("teardown_timeout should deserialize");

    assert_eq!(config.network.teardown_timeout.as_secs(), 90);
    assert_eq!(config.network.cni_config().teardown_timeout().as_secs(), 90);
}

#[test]
fn runtime_handler_config_accepts_cni_conf_dir() {
    let config: Config = toml::from_str(
        r#"
            root = "/var/lib/crius"

            [runtime]
            runtime_type = "runc"
            runtime_path = "/usr/bin/runc"
            root = "/run/crius"
            handlers = ["runc", "kata"]

            [runtime.runtimes.kata]
            runtime_path = "/usr/bin/kata-runtime"
            runtime_root = "/run/crius/kata"
            cni_conf_dir = "/etc/cni/kata.d"
            "#,
    )
    .expect("handler cni_conf_dir should deserialize");

    assert_eq!(
        config.runtime.runtimes["kata"].cni_conf_dir,
        "/etc/cni/kata.d"
    );
}

#[test]
fn runtime_handler_config_accepts_cni_max_conf_num() {
    let config: Config = toml::from_str(
        r#"
            root = "/var/lib/crius"

            [runtime]
            runtime_type = "runc"
            runtime_path = "/usr/bin/runc"
            root = "/run/crius"
            handlers = ["runc", "kata"]

            [runtime.runtimes.kata]
            runtime_path = "/usr/bin/kata-runtime"
            runtime_root = "/run/crius/kata"
            cni_max_conf_num = 2
            "#,
    )
    .expect("handler cni_max_conf_num should deserialize");

    assert_eq!(config.runtime.runtimes["kata"].cni_max_conf_num, Some(2));
}

#[test]
fn runtime_handler_config_accepts_allowed_and_default_annotations() {
    let config: Config = toml::from_str(
        r#"
            root = "/var/lib/crius"

            [runtime]
            runtime_type = "runc"
            runtime_path = "/usr/bin/runc"
            root = "/run/crius"
            handlers = ["runc", "kata"]

            [runtime.runtimes.kata]
            runtime_path = "/usr/bin/kata-runtime"
            runtime_root = "/run/crius/kata"
            allowed_annotations = ["io.example.runtime/", "workload.example/"]

            [runtime.runtimes.kata.default_annotations]
            "io.example.runtime/default" = "kata"
            "#,
    )
    .expect("handler annotations should deserialize");

    assert_eq!(
        config.runtime.runtimes["kata"].allowed_annotations,
        vec![
            "io.example.runtime/".to_string(),
            "workload.example/".to_string()
        ]
    );
    assert_eq!(
        config.runtime.runtimes["kata"]
            .default_annotations
            .get("io.example.runtime/default")
            .map(String::as_str),
        Some("kata")
    );
}

#[test]
fn runtime_handler_config_accepts_monitor_path() {
    let config: Config = toml::from_str(
        r#"
            root = "/var/lib/crius"

            [runtime]
            runtime_type = "runc"
            runtime_path = "/usr/bin/runc"
            root = "/run/crius"
            handlers = ["runc", "kata"]

            [runtime.runtimes.kata]
            runtime_path = "/usr/bin/kata-runtime"
            runtime_root = "/run/crius/kata"
            monitor_path = "/usr/bin/kata-shim"
            "#,
    )
    .expect("handler monitor_path should deserialize");

    assert_eq!(
        config.runtime.runtimes["kata"].monitor_path,
        "/usr/bin/kata-shim"
    );
}

#[test]
fn runtime_handler_config_accepts_runtime_config_path() {
    let dir = tempdir().unwrap();
    let runtime_config_path = dir.path().join("kata-config.toml");
    fs::write(&runtime_config_path, "sandbox = true\n").unwrap();
    let config_path = dir.path().join("crius.toml");
    fs::write(
        &config_path,
        format!(
            r#"
                root = "/var/lib/crius"

                [runtime]
                runtime_type = "runc"
                runtime_path = "/usr/bin/runc"
                root = "/run/crius"
                handlers = ["runc", "kata"]

                [runtime.runtimes.kata]
                runtime_path = "/usr/bin/kata-runtime"
                runtime_root = "/run/crius/kata"
                runtime_config_path = "{}"
                "#,
            runtime_config_path.display()
        ),
    )
    .unwrap();

    let config = Config::load(&config_path).expect("handler runtime_config_path should load");
    assert_eq!(
        config.runtime.runtimes["kata"].runtime_config_path,
        runtime_config_path.display().to_string()
    );
}

#[test]
fn runtime_handler_config_accepts_backend_options() {
    let config: Config = toml::from_str(
        r#"
            root = "/var/lib/crius"

            [runtime]
            runtime_type = "runc"
            runtime_path = "/usr/bin/runc"
            root = "/run/crius"
            handlers = ["runc", "kata"]

            [runtime.runtimes.kata]
            backend = "kata"
            runtime_path = "/usr/bin/kata-runtime"
            runtime_root = "/run/crius/kata"

            [runtime.runtimes.kata.backend_options]
            sandbox_cgroup_only = "true"
            sandbox_type = "podsandbox"
            "#,
    )
    .expect("handler backend_options should deserialize");

    assert_eq!(
        config.runtime.runtimes["kata"]
            .backend_options
            .get("sandbox_type")
            .map(String::as_str),
        Some("podsandbox")
    );

    let resolved = config.runtime.resolved_runtimes().unwrap();
    assert_eq!(
        resolved["kata"]
            .backend_options
            .get("sandbox_cgroup_only")
            .map(String::as_str),
        Some("true")
    );
}

#[test]
fn validate_rejects_unknown_runc_backend_option() {
    let mut config = Config::default();
    config.runtime.handlers = vec!["kata".to_string()];
    config.runtime.runtimes.insert(
        "kata".to_string(),
        RuntimeHandlerConfig {
            backend: "runc".to_string(),
            inherit_default_runtime: true,
            backend_options: HashMap::from([(
                "sandbox_type".to_string(),
                "podsandbox".to_string(),
            )]),
            ..Default::default()
        },
    );

    let err = config
        .validate()
        .expect_err("unknown runc backend option must fail validation");
    assert!(err.to_string().contains(
        "runtime.runtimes.kata.backend_options: unsupported runc backend option sandbox_type"
    ));
}

#[test]
fn runtime_config_accepts_default_mounts_file_and_reject_list() {
    let dir = tempdir().unwrap();
    let mounts_path = dir.path().join("mounts.conf");
    fs::write(&mounts_path, "/etc/hosts:/run/hosts\n").unwrap();
    let config_path = dir.path().join("crius.toml");
    fs::write(
        &config_path,
        format!(
            r#"
                root = "/var/lib/crius"

                [runtime]
                runtime_type = "runc"
                runtime_path = "/usr/bin/runc"
                root = "/run/crius"
                handlers = ["runc"]
                default_mounts_file = "{}"
                absent_mount_sources_to_reject = ["/etc/hostname"]
                "#,
            mounts_path.display()
        ),
    )
    .unwrap();

    let config = Config::load(&config_path).expect("mount policy config should load");
    assert_eq!(
        config.runtime.default_mounts_file,
        mounts_path.display().to_string()
    );
    assert_eq!(
        config.runtime.absent_mount_sources_to_reject,
        vec!["/etc/hostname".to_string()]
    );
}

#[test]
fn runtime_config_accepts_base_spec_timezone_and_default_ulimits() {
    let dir = tempdir().unwrap();
    let base_spec = dir.path().join("base-spec.json");
    fs::write(
        &base_spec,
        r#"{"ociVersion":"1.0.2","process":{"args":["/bin/sh"],"cwd":"/"}}"#,
    )
    .unwrap();
    let config_path = dir.path().join("crius.toml");
    fs::write(
        &config_path,
        format!(
            r#"
                root = "/var/lib/crius"

                [runtime]
                runtime_type = "runc"
                runtime_path = "/usr/bin/runc"
                root = "/run/crius"
                base_runtime_spec = "{}"
                timezone = "UTC"
                default_ulimits = ["nofile=1024:2048"]
                add_inheritable_capabilities = true
                disable_proc_mount = true
                "#,
            base_spec.display()
        ),
    )
    .unwrap();

    let config = Config::load(&config_path).expect("base spec config should load");
    assert_eq!(
        config.runtime.base_runtime_spec,
        base_spec.display().to_string()
    );
    assert_eq!(config.runtime.timezone, "UTC");
    assert_eq!(
        config.runtime.default_ulimits,
        vec!["nofile=1024:2048".to_string()]
    );
    assert!(config.runtime.add_inheritable_capabilities);
    assert!(config.runtime.disable_proc_mount);
}

#[test]
fn nri_config_accepts_cdi_settings_from_file() {
    let config: Config = toml::from_str(
        r#"
            root = "/var/lib/crius"

            [nri]
            enable = true
            enable_cdi = false
            cdi_spec_dirs = ["/etc/cdi", "/var/run/cdi"]
            "#,
    )
    .expect("nri cdi settings should deserialize");

    assert!(config.nri.enable);
    assert!(!config.nri.enable_cdi);
    assert_eq!(config.nri.cdi_spec_dirs, vec!["/etc/cdi", "/var/run/cdi"]);
}

#[test]
fn resolved_runtimes_prefer_platform_specific_runtime_path() {
    let mut config = RuntimeConfig {
        runtime_type: "runc".to_string(),
        runtime_path: "/usr/bin/runc".to_string(),
        root: "/run/crius".to_string(),
        ..Default::default()
    };
    config
        .platform_runtime_paths
        .insert(current_platform_key(), "/usr/bin/runc-platform".to_string());

    let resolved = config.resolved_runtimes().unwrap();
    assert_eq!(
        resolved
            .get("runc")
            .map(|value| value.runtime_path.as_str()),
        Some("/usr/bin/runc-platform")
    );
}

#[test]
fn validate_rejects_monitor_cgroup_slice_for_cgroupfs() {
    let mut config = Config::default();
    config.runtime.cgroup_driver = Some(CgroupDriverConfig::Cgroupfs);
    config.runtime.monitor_cgroup = "system.slice".to_string();

    let err = config
        .validate()
        .expect_err("cgroupfs monitor slice must fail");
    assert!(err
        .to_string()
        .contains("monitor cgroup should be \"pod\" or empty for cgroupfs"));
}

#[test]
fn validate_rejects_relative_absent_mount_source_entry() {
    let mut config = Config::default();
    config.runtime.absent_mount_sources_to_reject = vec!["etc/hostname".to_string()];

    let err = config
        .validate()
        .expect_err("relative reject path must fail validation");
    assert!(err
        .to_string()
        .contains("runtime.absent_mount_sources_to_reject entry must be an absolute path"));
}

#[test]
fn validate_rejects_invalid_timezone() {
    let mut config = Config::default();
    config.runtime.timezone = "Invalid/Timezone".to_string();

    let err = config
        .validate()
        .expect_err("invalid timezone must fail validation");
    assert!(err.to_string().contains("invalid timezone"));
}

#[test]
fn parsed_default_ulimits_normalizes_rlimit_names() {
    let config = RuntimeConfig {
        default_ulimits: vec!["nofile=1024:2048".to_string()],
        ..Default::default()
    };

    let parsed = config.parsed_default_ulimits().unwrap();
    assert_eq!(parsed.len(), 1);
    assert_eq!(parsed[0].rtype, "RLIMIT_NOFILE");
    assert_eq!(parsed[0].soft, 1024);
    assert_eq!(parsed[0].hard, 2048);
}

#[test]
fn runtime_handler_config_accepts_container_create_timeout() {
    let config: Config = toml::from_str(
        r#"
            root = "/var/lib/crius"

            [runtime]
            runtime_type = "runc"
            runtime_path = "/usr/bin/runc"
            root = "/run/crius"
            handlers = ["runc", "kata"]

            [runtime.runtimes.kata]
            runtime_path = "/usr/bin/kata-runtime"
            runtime_root = "/run/crius/kata"
            container_create_timeout = 600
            "#,
    )
    .expect("handler container_create_timeout should deserialize");

    assert_eq!(
        config.runtime.runtimes["kata"].container_create_timeout,
        Some(600)
    );
}

#[test]
fn runtime_handler_config_accepts_privileged_device_flags() {
    let config: Config = toml::from_str(
        r#"
            root = "/var/lib/crius"

            [runtime]
            runtime_type = "runc"
            runtime_path = "/usr/bin/runc"
            root = "/run/crius"
            handlers = ["runc", "kata"]

            [runtime.runtimes.kata]
            runtime_path = "/usr/bin/kata-runtime"
            runtime_root = "/run/crius/kata"
            privileged_without_host_devices = true
            privileged_without_host_devices_all_devices_allowed = true
            "#,
    )
    .expect("handler device policy flags should deserialize");

    assert!(config.runtime.runtimes["kata"].privileged_without_host_devices);
    assert!(config.runtime.runtimes["kata"].privileged_without_host_devices_all_devices_allowed);
}

#[test]
fn runtime_config_accepts_device_policy_fields_from_file() {
    let config: Config = toml::from_str(
        r#"
            root = "/var/lib/crius"

            [runtime]
            runtime_type = "runc"
            runtime_path = "/usr/bin/runc"
            root = "/run/crius"
            allowed_devices = ["/dev/null", "/dev/zero"]
            additional_devices = ["/dev/null:/dev/custom-null:rw"]
            device_ownership_from_security_context = true
            "#,
    )
    .expect("runtime device policy fields should deserialize");

    assert_eq!(
        config.runtime.allowed_devices,
        vec!["/dev/null".to_string(), "/dev/zero".to_string()]
    );
    assert_eq!(
        config.runtime.additional_devices,
        vec!["/dev/null:/dev/custom-null:rw".to_string()]
    );
    assert!(config.runtime.device_ownership_from_security_context);
}

#[test]
fn network_config_accepts_max_conf_num() {
    let config: Config = toml::from_str(
        r#"
            root = "/var/lib/crius"

            [network]
            plugin = "cni"
            max_conf_num = 2
            "#,
    )
    .expect("max_conf_num should deserialize");

    assert_eq!(config.network.max_conf_num, 2);
    assert_eq!(config.network.cni_config().max_conf_num(), 2);
}

#[test]
fn network_config_accepts_conf_template() {
    let config: Config = toml::from_str(
        r#"
            root = "/var/lib/crius"

            [network]
            plugin = "cni"
            conf_template = "/etc/cni/template.conflist"
            "#,
    )
    .expect("conf_template should deserialize");

    assert_eq!(config.network.conf_template, "/etc/cni/template.conflist");
    assert_eq!(
        config
            .network
            .cni_config()
            .conf_template()
            .map(|path| path.to_string_lossy().to_string()),
        Some("/etc/cni/template.conflist".to_string())
    );
}

#[test]
fn validate_accepts_included_pod_metrics_subset() {
    let mut config = Config::default();
    config.api.included_pod_metrics = vec!["cpu".to_string(), "memory".to_string()];

    config
        .validate()
        .expect("valid included_pod_metrics subset should pass");
}

#[test]
fn validate_rejects_invalid_included_pod_metrics_entry() {
    let mut config = Config::default();
    config.api.included_pod_metrics = vec!["invalid".to_string()];

    let err = config
        .validate()
        .expect_err("invalid included_pod_metrics entry must fail");
    assert!(err.to_string().contains("invalid pod metric"));
}

#[test]
fn validate_rejects_all_mixed_with_other_included_pod_metrics() {
    let mut config = Config::default();
    config.api.included_pod_metrics = vec!["all".to_string(), "cpu".to_string()];

    let err = config
        .validate()
        .expect_err("'all' mixed with others must fail");
    assert!(err
        .to_string()
        .contains("'all' should be the only value in api.included_pod_metrics"));
}

#[test]
fn network_config_accepts_ip_pref() {
    let config: Config = toml::from_str(
        r#"
            root = "/var/lib/crius"

            [network]
            plugin = "cni"
            ip_pref = "ipv6"
            "#,
    )
    .expect("ip_pref should deserialize");

    assert_eq!(config.network.ip_pref, MainIpPreference::Ipv6);
    assert_eq!(
        config.network.cni_config().ip_pref(),
        MainIpPreference::Ipv6
    );
}

#[test]
fn validate_rejects_invalid_listen_address() {
    let mut config = Config::default();
    config.api.listen = "not-a-listen-address".to_string();

    let err = config.validate().expect_err("invalid listen must fail");
    assert!(err.to_string().contains("invalid api.listen"));
}

#[test]
fn validate_rejects_zero_max_container_log_line_size() {
    let mut config = Config::default();
    config.logging.max_container_log_line_size = 0;

    let err = config
        .validate()
        .expect_err("zero log line size must fail validation");
    assert!(err
        .to_string()
        .contains("logging.max_container_log_line_size must be greater than zero"));
}

#[test]
fn logging_config_accepts_max_container_log_line_size_from_file() {
    let config: Config = toml::from_str(
        r#"
            root = "/var/lib/crius"

            [logging]
            level = "info"
            dir = "/var/log/crius"
            max_container_log_line_size = 8192
            "#,
    )
    .expect("logging config should deserialize");

    assert_eq!(config.logging.max_container_log_line_size, 8192);
}

#[test]
fn runtime_config_accepts_unprivileged_network_defaults_from_file() {
    let raw = r#"
            root = "/var/lib/crius"

            [runtime]
            runtime_type = "runc"
            runtime_path = "/usr/bin/runc"
            root = "/run/crius"
            handlers = ["runc"]
            pause_image = "registry.k8s.io/pause:3.9"
            pause_command = "/pause"
            shim_path = "/usr/bin/crius-shim"
            shim_dir = "/var/run/crius/shims"
            monitor_env = ["PATH=/usr/bin", "RUST_LOG=debug"]
            attach_socket_dir = "/var/run/crius/attach"
            container_exits_dir = "/var/run/crius/exits"
            clean_shutdown_file = "/var/lib/crius/clean.shutdown"
            container_stop_timeout = 5
            version_file = "/run/crius/version"
            version_file_persist = "/var/lib/crius/version"
            criu_path = "/usr/sbin/criu"
            criu_image_path = "/var/lib/crius/checkpoint-images"
            criu_work_path = "/var/lib/crius/checkpoint-work"
            enable_criu_support = false
            internal_wipe = false
            internal_repair = false
            bind_mount_prefix = "/host"
            disable_cgroup = true
            tolerate_missing_hugetlb_controller = false
            separate_pull_cgroup = ""
            uid_mappings = "0:100000:65536"
            gid_mappings = "0:200000:65536"
            minimum_mappable_uid = 100000
            minimum_mappable_gid = 200000
            io_uid = 1000
            io_gid = 2000
            pids_limit = 2048
            infra_ctr_cpuset = "1"
            shared_cpuset = "2-3"
            exec_cpu_affinity = "first"
            read_only = true
            no_pivot = true
            default_env = ["HTTP_PROXY=http://proxy.internal", "LANG=C.UTF-8"]
            default_capabilities = ["CHOWN", "NET_BIND_SERVICE"]
            default_sysctls = ["net.ipv4.ip_forward = 1", "kernel.shm_rmid_forced=1"]
            log_to_journald = true
            no_sync_log = true
            restrict_oom_score_adj = true
            enable_unprivileged_ports = true
            enable_unprivileged_icmp = true

            [security]
            seccomp_profile = "/etc/crius/seccomp-default.json"
            privileged_seccomp_profile = "localhost/privileged.json"
            unset_seccomp_profile = "unconfined"
            apparmor_default_profile = "crius-default"
            disable_apparmor = true
            enable_selinux = true
            selinux_category_range = 32
            hostnetwork_disable_selinux = false
            "#;
    let dir = tempdir().unwrap();
    let path = dir.path().join("crius.toml");
    fs::write(&path, raw).unwrap();
    let config = Config::load(&path).expect("runtime config should deserialize");

    assert_eq!(
        config.runtime.monitor_env,
        vec!["PATH=/usr/bin", "RUST_LOG=debug"]
    );
    assert_eq!(config.runtime.attach_socket_dir, "/var/run/crius/attach");
    assert_eq!(config.runtime.container_exits_dir, "/run/crius/exits");
    assert_eq!(
        config.runtime.clean_shutdown_file,
        "/var/lib/crius/clean.shutdown"
    );
    assert_eq!(config.runtime.version_file, "/run/crius/version");
    assert_eq!(
        config.runtime.version_file_persist,
        "/var/lib/crius/version"
    );
    assert_eq!(config.runtime.criu_path, "/usr/sbin/criu");
    assert_eq!(
        config.runtime.criu_image_path,
        "/var/lib/crius/checkpoint-images"
    );
    assert_eq!(
        config.runtime.criu_work_path,
        "/var/lib/crius/checkpoint-work"
    );
    assert!(!config.runtime.enable_criu_support);
    assert!(!config.runtime.internal_wipe);
    assert!(!config.runtime.internal_repair);
    assert_eq!(config.runtime.bind_mount_prefix, "/host");
    assert!(config.runtime.disable_cgroup);
    assert!(!config.runtime.tolerate_missing_hugetlb_controller);
    assert_eq!(config.runtime.separate_pull_cgroup, "");
    assert_eq!(config.runtime.uid_mappings, "0:100000:65536");
    assert_eq!(config.runtime.gid_mappings, "0:200000:65536");
    assert_eq!(config.runtime.minimum_mappable_uid, 100000);
    assert_eq!(config.runtime.minimum_mappable_gid, 200000);
    assert_eq!(config.runtime.io_uid, 1000);
    assert_eq!(config.runtime.io_gid, 2000);
    assert_eq!(config.runtime.pids_limit, 2048);
    assert_eq!(config.runtime.infra_ctr_cpuset, "1");
    assert_eq!(config.runtime.shared_cpuset, "2-3");
    assert_eq!(config.runtime.exec_cpu_affinity, "first");
    assert!(config.runtime.read_only);
    assert!(config.runtime.no_pivot);
    assert_eq!(
        config.runtime.default_env,
        vec![
            "HTTP_PROXY=http://proxy.internal".to_string(),
            "LANG=C.UTF-8".to_string()
        ]
    );
    assert_eq!(
        config.runtime.default_capabilities,
        vec!["CHOWN".to_string(), "NET_BIND_SERVICE".to_string()]
    );
    assert_eq!(
        config.runtime.default_sysctls,
        vec![
            "net.ipv4.ip_forward = 1".to_string(),
            "kernel.shm_rmid_forced=1".to_string()
        ]
    );
    assert_eq!(
        config.security.seccomp_profile,
        "/etc/crius/seccomp-default.json"
    );
    assert_eq!(
        config.security.privileged_seccomp_profile,
        "localhost/privileged.json"
    );
    assert_eq!(config.security.unset_seccomp_profile, "unconfined");
    assert_eq!(config.security.apparmor_default_profile, "crius-default");
    assert!(config.security.disable_apparmor);
    assert!(config.security.enable_selinux);
    assert_eq!(config.security.selinux_category_range, 32);
    assert!(!config.security.hostnetwork_disable_selinux);
    assert_eq!(
        config.runtime.container_stop_timeout,
        MIN_CONTAINER_STOP_TIMEOUT_SECS
    );
    assert!(config.runtime.log_to_journald);
    assert!(config.runtime.no_sync_log);
    assert!(config.runtime.restrict_oom_score_adj);
    assert!(config.runtime.enable_unprivileged_ports);
    assert!(config.runtime.enable_unprivileged_icmp);
}

#[test]
fn rootless_config_rewrites_default_paths_to_xdg_layout() {
    let mut config = Config::default();
    config.rootless.enabled = true;
    config.rootless.xdg_runtime_dir = "/run/user/1000".to_string();
    config.rootless.xdg_data_home = "/home/test/.local/share".to_string();

    config.normalize_derived_settings();

    assert_eq!(config.root, "/home/test/.local/share/crius");
    assert_eq!(config.runtime.root, "/run/user/1000/crius");
    assert_eq!(config.image.root, "/home/test/.local/share/crius/storage");
    assert_eq!(config.logging.dir, "/home/test/.local/share/crius/logs");
}

#[test]
fn validate_rejects_unsupported_image_driver() {
    let mut config = Config::default();
    config.image.driver = "btrfs".to_string();

    let err = config
        .validate()
        .expect_err("unsupported image driver must fail validation");
    assert!(err.to_string().contains("image.driver must be \"overlay\""));
}

#[test]
fn image_config_accepts_global_auth_file_from_file() {
    let config: Config = toml::from_str(
        r#"
            root = "/var/lib/crius"

            [image]
            driver = "overlay"
            root = "/var/lib/containers/storage"
            global_auth_file = "/var/lib/kubelet/config.json"
            "#,
    )
    .expect("image config should deserialize");

    assert_eq!(
        config.image.global_auth_file,
        "/var/lib/kubelet/config.json"
    );
}

#[test]
fn image_config_accepts_namespaced_auth_dir_from_file() {
    let config: Config = toml::from_str(
        r#"
            root = "/var/lib/crius"

            [image]
            driver = "overlay"
            root = "/var/lib/containers/storage"
            namespaced_auth_dir = "/var/lib/kubelet/credentialprovider"
            "#,
    )
    .expect("namespaced auth dir should deserialize");

    assert_eq!(
        config.image.namespaced_auth_dir,
        "/var/lib/kubelet/credentialprovider"
    );
}

#[test]
fn image_config_accepts_pull_policy_fields_from_file() {
    let config: Config = toml::from_str(
        r#"
            root = "/var/lib/crius"

            [image]
            driver = "overlay"
            root = "/var/lib/containers/storage"
            default_transport = "docker://"
            short_name_mode = "enforcing"
            pull_progress_timeout = "30s"
            max_concurrent_downloads = 5
            pull_retry_count = 2
            registry_config_dir = "/etc/containerd/certs.d"
            image_volumes = "ignore"
            pinned_images = ["busybox*", "*pause*"]
            "#,
    )
    .expect("image pull policy fields should deserialize");

    assert_eq!(config.image.default_transport, "docker://");
    assert_eq!(config.image.short_name_mode, "enforcing");
    assert_eq!(config.image.pull_progress_timeout.as_secs(), 30);
    assert_eq!(config.image.max_concurrent_downloads, 5);
    assert_eq!(config.image.pull_retry_count, 2);
    assert_eq!(config.image.registry_config_dir, "/etc/containerd/certs.d");
    assert_eq!(config.image.image_volumes, "ignore");
    assert_eq!(
        config.image.pinned_images,
        vec!["busybox*".to_string(), "*pause*".to_string()]
    );
}

#[test]
fn validate_rejects_relative_image_global_auth_file() {
    let mut config = Config::default();
    config.image.global_auth_file = "config.json".to_string();

    let err = config
        .validate()
        .expect_err("relative image global auth file must fail validation");
    assert!(err
        .to_string()
        .contains("image.global_auth_file must be an absolute path when set"));
}

#[test]
fn validate_rejects_relative_image_namespaced_auth_dir() {
    let mut config = Config::default();
    config.image.namespaced_auth_dir = "credentialprovider".to_string();

    let err = config
        .validate()
        .expect_err("relative namespaced auth dir must fail validation");
    assert!(err
        .to_string()
        .contains("image.namespaced_auth_dir must be an absolute path when set"));
}

#[test]
fn validate_rejects_invalid_image_short_name_mode() {
    let mut config = Config::default();
    config.image.short_name_mode = "permissive".to_string();

    let err = config
        .validate()
        .expect_err("invalid short name mode must fail validation");
    assert!(err
        .to_string()
        .contains("image.short_name_mode must be \"disabled\" or \"enforcing\""));
}

#[test]
fn validate_rejects_relative_image_registry_config_dir() {
    let mut config = Config::default();
    config.image.registry_config_dir = "certs.d".to_string();

    let err = config
        .validate()
        .expect_err("relative registry config dir must fail validation");
    assert!(err
        .to_string()
        .contains("image.registry_config_dir must be an absolute path when set"));
}

#[test]
fn validate_accepts_supported_image_storage_options() {
    let mut config = Config::default();
    config.image.storage_options = vec![
        "overlay.mount_program=/usr/bin/fuse-overlayfs".to_string(),
        "overlay.ignore_chown_errors=true".to_string(),
    ];

    config.validate().unwrap();
}

#[test]
fn validate_rejects_unsupported_image_storage_option() {
    let mut config = Config::default();
    config.image.storage_options = vec!["overlay.mountopt=nodev".to_string()];

    let err = config
        .validate()
        .expect_err("unsupported image storage option must fail validation");
    assert!(err
        .to_string()
        .contains("unsupported image.storage_options key overlay.mountopt"));
}

#[test]
fn validate_rejects_invalid_image_volumes_mode() {
    let mut config = Config::default();
    config.image.image_volumes = "tmpfs".to_string();

    let err = config
        .validate()
        .expect_err("invalid image volume mode must fail validation");
    assert!(err
        .to_string()
        .contains("image.image_volumes must be \"mkdir\", \"bind\", or \"ignore\""));
}

#[test]
fn validate_rejects_empty_pinned_image_pattern() {
    let mut config = Config::default();
    config.image.pinned_images = vec![String::new()];

    let err = config
        .validate()
        .expect_err("empty pinned image pattern must fail validation");
    assert!(err
        .to_string()
        .contains("image.pinned_images entries must not be empty"));
}

#[test]
fn validate_rejects_enabled_drop_infra_ctr() {
    let mut config = Config::default();
    config.runtime.drop_infra_ctr = true;

    let err = config
        .validate()
        .expect_err("drop infra ctr must fail validation");
    assert!(err
        .to_string()
        .contains("runtime.drop_infra_ctr=true is not supported by crius"));
}

#[test]
fn validate_rejects_relative_pinns_path() {
    let mut config = Config::default();
    config.runtime.pinns_path = "pinns".to_string();

    let err = config
        .validate()
        .expect_err("relative pinns path must fail validation");
    assert!(err
        .to_string()
        .contains("runtime.pinns_path must be an absolute path when set"));
}

#[test]
fn validate_rejects_relative_runtime_seccomp_profile() {
    let mut config = Config::default();
    config.security.seccomp_profile = "profiles/default.json".to_string();

    let err = config
        .validate()
        .expect_err("relative runtime seccomp profile must fail validation");
    assert!(err
        .to_string()
        .contains("security.seccomp_profile must be an absolute path when set"));
}

#[test]
fn validate_rejects_invalid_runtime_unset_seccomp_profile() {
    let mut config = Config::default();
    config.security.unset_seccomp_profile = "runtime/unknown".to_string();

    let err = config
        .validate()
        .expect_err("invalid unset seccomp profile selector must fail validation");
    assert!(err
        .to_string()
        .contains("security.unset_seccomp_profile must be empty"));
}

#[test]
fn validate_rejects_zero_runtime_pids_limit() {
    let mut config = Config::default();
    config.runtime.pids_limit = 0;

    let err = config
        .validate()
        .expect_err("zero pids limit must fail validation");
    assert!(err
        .to_string()
        .contains("runtime.pids_limit must be -1 or greater than zero"));
}

#[test]
fn validate_rejects_invalid_infra_ctr_cpuset() {
    let mut config = Config::default();
    config.runtime.infra_ctr_cpuset = "3-1".to_string();

    let err = config
        .validate()
        .expect_err("descending infra ctr cpuset must fail validation");
    assert!(err
        .to_string()
        .contains("runtime.infra_ctr_cpuset contains descending CPU range"));
}

#[test]
fn validate_rejects_invalid_shared_cpuset() {
    let mut config = Config::default();
    config.runtime.shared_cpuset = "cpu0".to_string();

    let err = config
        .validate()
        .expect_err("invalid shared cpuset must fail validation");
    assert!(err
        .to_string()
        .contains("runtime.shared_cpuset contains invalid CPU entry"));
}

#[test]
fn validate_rejects_relative_bind_mount_prefix() {
    let mut config = Config::default();
    config.runtime.bind_mount_prefix = "host".to_string();

    let err = config
        .validate()
        .expect_err("relative bind mount prefix must fail validation");
    assert!(err
        .to_string()
        .contains("runtime.bind_mount_prefix must be an absolute path when set"));
}

#[test]
fn validate_rejects_invalid_monitor_env_entry() {
    let mut config = Config::default();
    config.runtime.monitor_env = vec!["INVALID".to_string()];

    let err = config
        .validate()
        .expect_err("monitor env without equals must fail validation");
    assert!(err
        .to_string()
        .contains("runtime.monitor_env entries must be in KEY=value format"));
}

#[test]
fn validate_rejects_invalid_default_env_entry() {
    let mut config = Config::default();
    config.runtime.default_env = vec!["INVALID".to_string()];

    let err = config
        .validate()
        .expect_err("default env without equals must fail validation");
    assert!(err
        .to_string()
        .contains("runtime.default_env entries must be in KEY=value format"));
}

#[test]
fn validate_rejects_invalid_default_sysctl_entry() {
    let mut config = Config::default();
    config.runtime.default_sysctls = vec!["net.ipv4.ip_forward".to_string()];

    let err = config
        .validate()
        .expect_err("invalid default sysctl must fail validation");
    assert!(err
        .to_string()
        .contains("runtime.default_sysctls contains invalid entry"));
}

#[test]
fn validate_rejects_invalid_additional_device_entry() {
    let mut config = Config::default();
    config.runtime.additional_devices = vec!["/dev/null:relative:rw".to_string()];

    let err = config
        .validate()
        .expect_err("invalid additional device entry must fail validation");
    assert!(err
        .to_string()
        .contains("runtime.additional_devices contains invalid entry"));
}

#[test]
fn validate_rejects_empty_runtime_handler_allowed_annotation_entry() {
    let mut config = Config::default();
    config.runtime.handlers = vec!["kata".to_string()];
    config.runtime.runtimes.insert(
        "kata".to_string(),
        RuntimeHandlerConfig {
            runtime_path: "/usr/bin/kata-runtime".to_string(),
            runtime_root: "/run/crius/kata".to_string(),
            allowed_annotations: vec![String::new()],
            ..Default::default()
        },
    );

    let err = config
        .validate()
        .expect_err("empty runtime handler allowed annotation must fail validation");
    assert!(err
        .to_string()
        .contains("runtime.runtimes.kata.allowed_annotations entries must not be empty"));
}

#[test]
fn validate_rejects_empty_runtime_handler_default_annotation_value() {
    let mut config = Config::default();
    config.runtime.handlers = vec!["kata".to_string()];
    config.runtime.runtimes.insert(
        "kata".to_string(),
        RuntimeHandlerConfig {
            runtime_path: "/usr/bin/kata-runtime".to_string(),
            runtime_root: "/run/crius/kata".to_string(),
            default_annotations: HashMap::from([(
                "io.example.runtime/default".to_string(),
                String::new(),
            )]),
            ..Default::default()
        },
    );

    let err = config
        .validate()
        .expect_err("empty runtime handler default annotation value must fail validation");
    assert!(err.to_string().contains(
        "runtime.runtimes.kata.default_annotations.io.example.runtime/default must not be empty"
    ));
}

#[test]
fn validate_rejects_relative_criu_image_path() {
    let mut config = Config::default();
    config.runtime.criu_image_path = "checkpoint-images".to_string();

    let err = config
        .validate()
        .expect_err("relative criu image path must fail validation");
    assert!(err
        .to_string()
        .contains("runtime.criu_image_path must be an absolute path when set"));
}

#[test]
fn validate_rejects_relative_criu_work_path() {
    let mut config = Config::default();
    config.runtime.criu_work_path = "checkpoint-work".to_string();

    let err = config
        .validate()
        .expect_err("relative criu work path must fail validation");
    assert!(err
        .to_string()
        .contains("runtime.criu_work_path must be an absolute path when set"));
}

#[test]
fn validate_rejects_uid_mappings_without_gid_mappings() {
    let mut config = Config::default();
    config.runtime.uid_mappings = "0:100000:65536".to_string();

    let err = config
        .validate()
        .expect_err("uid mappings without gid mappings must fail validation");
    assert!(err
        .to_string()
        .contains("runtime.uid_mappings and runtime.gid_mappings must be configured together"));
}

#[test]
fn validate_rejects_invalid_runtime_id_mapping() {
    let mut config = Config::default();
    config.runtime.uid_mappings = "0:100000:0".to_string();
    config.runtime.gid_mappings = "0:200000:65536".to_string();

    let err = config
        .validate()
        .expect_err("zero-sized id mapping must fail validation");
    assert!(err.to_string().contains("size greater than zero"));
}

#[test]
fn validate_rejects_invalid_minimum_mappable_uid() {
    let mut config = Config::default();
    config.runtime.minimum_mappable_uid = -2;

    let err = config
        .validate()
        .expect_err("minimum mappable uid below -1 must fail validation");
    assert!(err
        .to_string()
        .contains("runtime.minimum_mappable_uid must be -1 or greater"));
}

#[test]
fn validate_rejects_invalid_minimum_mappable_gid() {
    let mut config = Config::default();
    config.runtime.minimum_mappable_gid = -2;

    let err = config
        .validate()
        .expect_err("minimum mappable gid below -1 must fail validation");
    assert!(err
        .to_string()
        .contains("runtime.minimum_mappable_gid must be -1 or greater"));
}

#[test]
fn runtime_workloads_parse_from_file() {
    let config: Config = toml::from_str(
        r#"
            root = "/var/lib/crius"

            [runtime]
            runtime_type = "runc"
            runtime_path = "/usr/bin/runc"
            root = "/run/crius"
            pause_image = "registry.k8s.io/pause:3.9"
            pause_command = "/pause"
            shim_path = "/usr/bin/crius-shim"
            shim_dir = "/var/run/crius/shims"
            attach_socket_dir = "/var/run/crius/attach"
            container_exits_dir = "/var/run/crius/exits"
            clean_shutdown_file = "/var/lib/crius/clean.shutdown"
            version_file = "/run/crius/version"
            version_file_persist = "/var/lib/crius/version"

            [runtime.workloads.management]
            activation_annotation = "target.workload.openshift.io/management"
            annotation_prefix = "resources.workload.openshift.io"
            allowed_annotations = ["workload.example/"]

            [runtime.workloads.management.resources]
            cpushares = 2048
            cpuquota = 50000
            cpuperiod = 100000
            cpuset = "0-1"
            cpulimit = 1000
            "#,
    )
    .expect("workload config should deserialize");

    let workload = config
        .runtime
        .workloads
        .get("management")
        .expect("workload should be present");
    assert_eq!(
        workload.activation_annotation,
        "target.workload.openshift.io/management"
    );
    assert_eq!(
        workload.annotation_prefix,
        "resources.workload.openshift.io"
    );
    assert_eq!(workload.allowed_annotations, vec!["workload.example/"]);
    assert_eq!(workload.resources.cpu_shares, 2048);
    assert_eq!(workload.resources.cpu_limit, 1000);
}

#[test]
fn validate_rejects_duplicate_workload_activation_annotation() {
    let mut config = Config::default();
    config.runtime.workloads.insert(
        "management".to_string(),
        RuntimeWorkloadConfig {
            activation_annotation: "target.workload.openshift.io/management".to_string(),
            annotation_prefix: "resources.workload.openshift.io".to_string(),
            ..Default::default()
        },
    );
    config.runtime.workloads.insert(
        "management-copy".to_string(),
        RuntimeWorkloadConfig {
            activation_annotation: "target.workload.openshift.io/management".to_string(),
            annotation_prefix: "resources.workload.openshift.io".to_string(),
            ..Default::default()
        },
    );

    let err = config
        .validate()
        .expect_err("duplicate workload activation annotation must fail");
    assert!(err.to_string().contains("duplicate activation_annotation"));
}

#[test]
fn validate_rejects_invalid_workload_cpu_period() {
    let mut config = Config::default();
    config.runtime.workloads.insert(
        "management".to_string(),
        RuntimeWorkloadConfig {
            activation_annotation: "target.workload.openshift.io/management".to_string(),
            annotation_prefix: "resources.workload.openshift.io".to_string(),
            resources: RuntimeWorkloadResources {
                cpu_period: 500,
                ..Default::default()
            },
            ..Default::default()
        },
    );

    let err = config
        .validate()
        .expect_err("invalid workload cpu period must fail");
    assert!(err.to_string().contains("cpuperiod"));
}

#[test]
fn validate_rejects_invalid_exec_cpu_affinity() {
    let mut config = Config::default();
    config.runtime.exec_cpu_affinity = "last".to_string();

    let err = config
        .validate()
        .expect_err("invalid exec cpu affinity must fail");
    assert!(err.to_string().contains("runtime.exec_cpu_affinity"));
}

#[test]
fn validate_accepts_supported_separate_pull_cgroup_modes() {
    let mut config = Config::default();
    config.runtime.cgroup_driver = Some(CgroupDriverConfig::Systemd);
    config.runtime.separate_pull_cgroup = "pod".to_string();
    config.validate().unwrap();

    config.runtime.separate_pull_cgroup = "kubepods.slice".to_string();
    config.validate().unwrap();
}

#[test]
fn validate_rejects_invalid_systemd_separate_pull_cgroup() {
    let mut config = Config::default();
    config.runtime.cgroup_driver = Some(CgroupDriverConfig::Systemd);
    config.runtime.separate_pull_cgroup = "kubepods/pod123".to_string();
    let err = config
        .validate()
        .expect_err("invalid systemd separate pull cgroup must fail");
    assert!(err
        .to_string()
        .contains("runtime.separate_pull_cgroup with systemd"));
}

#[test]
fn validate_rejects_tcp_listen_when_tcp_service_is_not_explicitly_enabled() {
    let mut config = Config::default();
    config.api.listen = "127.0.0.1:50051".to_string();

    let err = config
        .validate()
        .expect_err("tcp listen without explicit enablement must fail");
    assert!(err
        .to_string()
        .contains("api.listen 127.0.0.1:50051 requires api.allow_tcp_service = true"));
}

#[test]
fn validate_allows_tcp_listen_when_tcp_service_is_explicitly_enabled() {
    let mut config = Config::default();
    config.api.listen = "127.0.0.1:50051".to_string();
    config.api.allow_tcp_service = true;

    config
        .validate()
        .expect("tcp listen should be accepted when explicitly enabled");
}

#[test]
fn api_config_accepts_grpc_message_size_fields() {
    let config: Config = toml::from_str(
        r#"
            root = "/var/lib/crius"

            [api]
            listen = "unix:///run/crius/crius.sock"
            grpc_max_send_msg_size = 12345
            grpc_max_recv_msg_size = 23456
            "#,
    )
    .expect("grpc message sizes should deserialize");

    assert_eq!(config.api.grpc_max_send_msg_size, 12345);
    assert_eq!(config.api.grpc_max_recv_msg_size, 23456);
}

#[test]
fn api_config_accepts_listen_aliases() {
    let config: Config = toml::from_str(
            r#"
            root = "/var/lib/crius"

            [api]
            listen = "unix:///run/crius/crius.sock"
            listen_aliases = ["unix:///var/run/containerd/containerd.sock", "unix:///var/run/crio/crio.sock"]
            "#,
        )
        .expect("listen aliases should deserialize");

    assert_eq!(
        config.api.listen_aliases,
        vec![
            "unix:///var/run/containerd/containerd.sock".to_string(),
            "unix:///var/run/crio/crio.sock".to_string()
        ]
    );
}

#[test]
fn validate_rejects_non_unix_listen_aliases() {
    let mut config = Config::default();
    config.api.listen_aliases = vec!["127.0.0.1:12345".to_string()];

    let err = config
        .validate()
        .expect_err("non-unix aliases must fail validation");
    assert!(err
        .to_string()
        .contains("api.listen_aliases value 127.0.0.1:12345 must use unix://"));
}

#[test]
fn validate_rejects_zero_grpc_max_send_msg_size() {
    let mut config = Config::default();
    config.api.grpc_max_send_msg_size = 0;

    let err = config
        .validate()
        .expect_err("zero grpc max send msg size must fail");
    assert!(err
        .to_string()
        .contains("api.grpc_max_send_msg_size must be greater than zero"));
}

#[test]
fn validate_rejects_zero_grpc_max_recv_msg_size() {
    let mut config = Config::default();
    config.api.grpc_max_recv_msg_size = 0;

    let err = config
        .validate()
        .expect_err("zero grpc max recv msg size must fail");
    assert!(err
        .to_string()
        .contains("api.grpc_max_recv_msg_size must be greater than zero"));
}

#[test]
fn streaming_config_accepts_duration_strings() {
    let config: Config = toml::from_str(
        r#"
            root = "/var/lib/crius"

            [api]
            listen = "unix:///run/crius/crius.sock"
            exec_sync_io_drain_timeout = "250ms"

            [api.streaming]
            address = "127.0.0.1"
            port = 10010
            request_token_ttl = "45s"
            port_forward_stream_creation_timeout = "90s"
            port_forward_idle_timeout = "2h"
            "#,
    )
    .expect("streaming config should deserialize");

    assert_eq!(config.api.streaming.address, "127.0.0.1");
    assert_eq!(config.api.streaming.port, 10010);
    assert_eq!(config.api.exec_sync_io_drain_timeout.as_millis(), 250);
    assert_eq!(config.api.streaming.request_token_ttl.as_secs(), 45);
    assert_eq!(
        config
            .api
            .streaming
            .port_forward_stream_creation_timeout
            .as_secs(),
        90
    );
    assert_eq!(
        config.api.streaming.port_forward_idle_timeout.as_secs(),
        2 * 60 * 60
    );
}

#[test]
fn streaming_config_accepts_tls_fields() {
    let config: Config = toml::from_str(
        r#"
            root = "/var/lib/crius"

            [api]
            listen = "unix:///run/crius/crius.sock"

            [api.streaming]
            address = "127.0.0.1"
            port = 0
            enable_tls = true
            tls_cert_file = "/etc/crius/tls/tls.crt"
            tls_key_file = "/etc/crius/tls/tls.key"
            tls_ca_file = "/etc/crius/tls/ca.crt"
            tls_min_version = "VersionTLS13"
            tls_cipher_suites = ["TLS13_AES_256_GCM_SHA384", "TLS13_AES_128_GCM_SHA256"]
            request_token_ttl = "45s"
            port_forward_stream_creation_timeout = "10s"
            port_forward_idle_timeout = "1h"
            "#,
    )
    .expect("streaming TLS config should deserialize");

    assert!(config.api.streaming.enable_tls);
    assert_eq!(config.api.streaming.tls_min_version, "VersionTLS13");
    assert_eq!(config.api.streaming.tls_cert_file, "/etc/crius/tls/tls.crt");
    assert_eq!(
        config.api.streaming.tls_cipher_suites,
        vec![
            "TLS13_AES_256_GCM_SHA384".to_string(),
            "TLS13_AES_128_GCM_SHA256".to_string()
        ]
    );
}

#[test]
fn validate_rejects_streaming_tls_without_cert_or_key() {
    let mut config = Config::default();
    config.api.streaming.enable_tls = true;
    config.api.streaming.tls_cert_file = "/etc/crius/tls/tls.crt".to_string();

    let err = config
        .validate()
        .expect_err("missing TLS key must fail validation");
    assert!(err
        .to_string()
        .contains("api.streaming.tls_key_file must not be empty"));
}

#[test]
fn env_overrides_take_precedence() {
    let _lock = env_lock().lock().unwrap();
    let dir = tempdir().unwrap();
    let _guard = EnvGuard::set_many(&[
        ("CRIUS_LISTEN", "unix:///tmp/env.sock"),
        (
            "CRIUS_LISTEN_ALIASES",
            "unix:///var/run/containerd/containerd.sock,unix:///var/run/crio/crio.sock",
        ),
        ("CRIUS_ALLOW_TCP_SERVICE", "true"),
        ("CRIUS_ENABLE_POD_EVENTS", "false"),
        ("CRIUS_INCLUDED_POD_METRICS", "cpu,memory"),
        ("CRIUS_STATS_COLLECTION_PERIOD", "15"),
        ("CRIUS_POD_SANDBOX_METRICS_COLLECTION_PERIOD", "45"),
        ("CRIUS_EXEC_SYNC_IO_DRAIN_TIMEOUT", "125ms"),
        ("CRIUS_STREAM_ADDRESS", "127.0.0.2"),
        ("CRIUS_STREAM_PORT", "10020"),
        (
            "CRIUS_STREAM_TLS_CIPHER_SUITES",
            "TLS13_AES_256_GCM_SHA384,TLS13_AES_128_GCM_SHA256",
        ),
        ("CRIUS_STREAM_REQUEST_TOKEN_TTL", "40s"),
        ("CRIUS_STREAM_PORT_FORWARD_STREAM_CREATION_TIMEOUT", "70s"),
        ("CRIUS_STREAM_IDLE_TIMEOUT", "3h"),
        ("CRIUS_RUNTIME_HANDLERS", "kata,runsc"),
        ("CRIUS_PAUSE_COMMAND", "/custom-pause"),
        ("CRIUS_PINNS_PATH", "/usr/bin/pinns"),
        ("CRIUS_DROP_INFRA_CTR", "false"),
        ("CRIUS_MONITOR_ENV", "PATH=/custom/bin,RUST_LOG=trace"),
        ("CRIUS_CGROUP_DRIVER", "systemd"),
        (
            "CRIUS_CNI_CONFIG_DIRS",
            "/etc/cni/net.d:/etc/kubernetes/cni/net.d",
        ),
        ("CRIUS_CNI_PLUGIN_DIRS", "/opt/cni/bin:/usr/libexec/cni"),
        (
            "CRIUS_CNI_CACHE_DIR",
            dir.path().join("cache").to_str().unwrap(),
        ),
        (
            "CRIUS_CNI_CONF_TEMPLATE",
            dir.path().join("template.conflist").to_str().unwrap(),
        ),
        ("CRIUS_CNI_MAX_CONF_NUM", "3"),
        ("CRIUS_CNI_IP_PREF", "ipv4"),
        ("CRIUS_CNI_TEARDOWN_TIMEOUT", "75s"),
        ("CRIUS_CNI_DEFAULT_NETWORK", "cluster-bridge"),
        ("CRIUS_DISABLE_HOSTPORT_MAPPING", "true"),
        ("CRIUS_LOG_LEVEL", "debug"),
        (
            "CRIUS_LOG_FILE",
            dir.path().join("crius.log").to_str().unwrap(),
        ),
        ("CRIUS_MAX_CONTAINER_LOG_LINE_SIZE", "8192"),
        ("CRIUS_LOG_TO_JOURNALD", "true"),
        ("CRIUS_NO_SYNC_LOG", "true"),
        ("CRIUS_RESTRICT_OOM_SCORE_ADJ", "true"),
        ("CRIUS_ATTACH_SOCKET_DIR", "/run/crius-attach"),
        ("CRIUS_CONTAINER_EXITS_DIR", "/run/crius-exits"),
        (
            "CRIUS_CLEAN_SHUTDOWN_FILE",
            "/var/lib/crius/custom-clean.shutdown",
        ),
        ("CRIUS_VERSION_FILE", "/run/crius/custom-version"),
        (
            "CRIUS_VERSION_FILE_PERSIST",
            "/var/lib/crius/custom-version-persist",
        ),
        ("CRIUS_CRIU_PATH", "/custom/criu"),
        ("CRIUS_CRIU_IMAGE_PATH", "/custom/criu-images"),
        ("CRIUS_CRIU_WORK_PATH", "/custom/criu-work"),
        ("CRIUS_ENABLE_CRIU_SUPPORT", "false"),
        ("CRIUS_INTERNAL_WIPE", "false"),
        ("CRIUS_INTERNAL_REPAIR", "false"),
        (
            "CRIUS_IMAGE_NAMESPACED_AUTH_DIR",
            "/var/lib/kubelet/credentialprovider",
        ),
        ("CRIUS_IMAGE_DEFAULT_TRANSPORT", "docker://"),
        ("CRIUS_IMAGE_SHORT_NAME_MODE", "enforcing"),
        ("CRIUS_IMAGE_PULL_PROGRESS_TIMEOUT", "25s"),
        ("CRIUS_MAX_CONCURRENT_DOWNLOADS", "5"),
        ("CRIUS_IMAGE_PULL_RETRY_COUNT", "2"),
        ("CRIUS_IMAGE_REGISTRY_CONFIG_DIR", "/etc/containerd/certs.d"),
        ("CRIUS_IMAGE_STORAGE_OPTIONS", ""),
        ("CRIUS_IMAGE_VOLUMES", "bind"),
        ("CRIUS_PINNED_IMAGES", "busybox*,*pause*"),
        ("CRIUS_ALLOWED_DEVICES", "/dev/null,/dev/zero"),
        ("CRIUS_ADDITIONAL_DEVICES", "/dev/null:/dev/custom-null:rw"),
        ("CRIUS_DEVICE_OWNERSHIP_FROM_SECURITY_CONTEXT", "true"),
        ("CRIUS_BIND_MOUNT_PREFIX", "/host-prefix"),
        ("CRIUS_DISABLE_CGROUP", "true"),
        ("CRIUS_TOLERATE_MISSING_HUGETLB_CONTROLLER", "false"),
        ("CRIUS_SEPARATE_PULL_CGROUP", ""),
        ("CRIUS_UID_MAPPINGS", "0:300000:65536"),
        ("CRIUS_GID_MAPPINGS", "0:400000:65536"),
        ("CRIUS_MINIMUM_MAPPABLE_UID", "300000"),
        ("CRIUS_MINIMUM_MAPPABLE_GID", "400000"),
        ("CRIUS_IO_UID", "3000"),
        ("CRIUS_IO_GID", "4000"),
        ("CRIUS_PIDS_LIMIT", "4096"),
        ("CRIUS_INFRA_CTR_CPUSET", "1"),
        ("CRIUS_SHARED_CPUSET", "2-3"),
        ("CRIUS_EXEC_CPU_AFFINITY", "first"),
        ("CRIUS_READ_ONLY", "true"),
        ("CRIUS_NO_PIVOT", "true"),
        ("CRIUS_CONTAINER_STOP_TIMEOUT", "7"),
        ("CRIUS_ENABLE_UNPRIVILEGED_PORTS", "true"),
        ("CRIUS_ENABLE_UNPRIVILEGED_ICMP", "true"),
        ("CRIUS_PRIVILEGED_SECCOMP_PROFILE", "unconfined"),
        ("CRIUS_APPARMOR_DEFAULT_PROFILE", "custom-default"),
        ("CRIUS_DISABLE_APPARMOR", "true"),
        ("CRIUS_ENABLE_SELINUX", "true"),
        ("CRIUS_SELINUX_CATEGORY_RANGE", "64"),
        ("CRIUS_HOSTNETWORK_DISABLE_SELINUX", "false"),
        ("CRIUS_ENABLE_CDI", "false"),
        ("CRIUS_CDI_SPEC_DIRS", "/etc/cdi:/var/run/cdi:/opt/cdi"),
    ]);

    let mut config = Config::default();
    config.runtime.runtimes = HashMap::from([
        (
            "kata".to_string(),
            RuntimeHandlerConfig {
                inherit_default_runtime: true,
                cni_conf_dir: String::new(),
                ..Default::default()
            },
        ),
        (
            "runsc".to_string(),
            RuntimeHandlerConfig {
                inherit_default_runtime: true,
                cni_conf_dir: String::new(),
                ..Default::default()
            },
        ),
    ]);
    config.apply_env_overrides().unwrap();

    assert_eq!(config.api.listen, "unix:///tmp/env.sock");
    assert_eq!(
        config.api.listen_aliases,
        vec![
            "unix:///var/run/containerd/containerd.sock".to_string(),
            "unix:///var/run/crio/crio.sock".to_string()
        ]
    );
    assert!(config.api.allow_tcp_service);
    assert!(!config.api.enable_pod_events);
    assert_eq!(config.api.included_pod_metrics, vec!["cpu", "memory"]);
    assert_eq!(config.api.stats_collection_period, 15);
    assert_eq!(config.api.pod_sandbox_metrics_collection_period, 45);
    assert_eq!(config.api.exec_sync_io_drain_timeout.as_millis(), 125);
    assert_eq!(config.api.streaming.address, "127.0.0.2");
    assert_eq!(config.api.streaming.port, 10020);
    assert_eq!(
        config.api.streaming.tls_cipher_suites,
        vec![
            "TLS13_AES_256_GCM_SHA384".to_string(),
            "TLS13_AES_128_GCM_SHA256".to_string()
        ]
    );
    assert_eq!(config.api.streaming.request_token_ttl.as_secs(), 40);
    assert_eq!(
        config
            .api
            .streaming
            .port_forward_stream_creation_timeout
            .as_secs(),
        70
    );
    assert_eq!(
        config.api.streaming.port_forward_idle_timeout.as_secs(),
        3 * 60 * 60
    );
    assert_eq!(
        config.runtime.normalized_handlers(),
        vec!["kata", "runsc", "runc"]
    );
    assert_eq!(config.runtime.pause_command, "/custom-pause");
    assert_eq!(config.runtime.pinns_path, "/usr/bin/pinns");
    assert!(!config.runtime.drop_infra_ctr);
    assert_eq!(
        config.runtime.cgroup_driver,
        Some(CgroupDriverConfig::Systemd)
    );
    assert_eq!(
        config.runtime.monitor_env,
        vec!["PATH=/custom/bin", "RUST_LOG=trace"]
    );
    assert_eq!(config.runtime.attach_socket_dir, "/run/crius-attach");
    assert_eq!(config.runtime.container_exits_dir, "/run/crius-exits");
    assert_eq!(
        config.runtime.clean_shutdown_file,
        "/var/lib/crius/custom-clean.shutdown"
    );
    assert_eq!(config.runtime.version_file, "/run/crius/custom-version");
    assert_eq!(
        config.runtime.version_file_persist,
        "/var/lib/crius/custom-version-persist"
    );
    assert_eq!(config.runtime.criu_path, "/custom/criu");
    assert_eq!(config.runtime.criu_image_path, "/custom/criu-images");
    assert_eq!(config.runtime.criu_work_path, "/custom/criu-work");
    assert!(!config.runtime.enable_criu_support);
    assert_eq!(
        config.image.namespaced_auth_dir,
        "/var/lib/kubelet/credentialprovider"
    );
    assert_eq!(config.image.default_transport, "docker://");
    assert_eq!(config.image.short_name_mode, "enforcing");
    assert_eq!(config.image.pull_progress_timeout.as_secs(), 25);
    assert_eq!(config.image.max_concurrent_downloads, 5);
    assert_eq!(config.image.pull_retry_count, 2);
    assert_eq!(config.image.registry_config_dir, "/etc/containerd/certs.d");
    assert!(config.image.storage_options.is_empty());
    assert_eq!(config.image.image_volumes, "bind");
    assert_eq!(
        config.image.pinned_images,
        vec!["busybox*".to_string(), "*pause*".to_string()]
    );
    assert_eq!(
        config.runtime.allowed_devices,
        vec!["/dev/null".to_string(), "/dev/zero".to_string()]
    );
    assert_eq!(
        config.runtime.additional_devices,
        vec!["/dev/null:/dev/custom-null:rw".to_string()]
    );
    assert!(config.runtime.device_ownership_from_security_context);
    assert_eq!(config.security.privileged_seccomp_profile, "unconfined");
    assert_eq!(config.security.apparmor_default_profile, "custom-default");
    assert!(config.security.disable_apparmor);
    assert!(config.security.enable_selinux);
    assert_eq!(config.security.selinux_category_range, 64);
    assert!(!config.security.hostnetwork_disable_selinux);
    assert!(!config.nri.enable_cdi);
    assert_eq!(
        config.nri.cdi_spec_dirs,
        vec!["/etc/cdi", "/var/run/cdi", "/opt/cdi"]
    );
    assert!(!config.runtime.internal_wipe);
    assert!(!config.runtime.internal_repair);
    assert_eq!(config.runtime.bind_mount_prefix, "/host-prefix");
    assert!(config.runtime.disable_cgroup);
    assert!(!config.runtime.tolerate_missing_hugetlb_controller);
    assert_eq!(config.runtime.separate_pull_cgroup, "");
    assert_eq!(config.runtime.uid_mappings, "0:300000:65536");
    assert_eq!(config.runtime.gid_mappings, "0:400000:65536");
    assert_eq!(config.runtime.minimum_mappable_uid, 300000);
    assert_eq!(config.runtime.minimum_mappable_gid, 400000);
    assert_eq!(config.runtime.io_uid, 3000);
    assert_eq!(config.runtime.io_gid, 4000);
    assert_eq!(config.runtime.pids_limit, 4096);
    assert_eq!(config.runtime.infra_ctr_cpuset, "1");
    assert_eq!(config.runtime.shared_cpuset, "2-3");
    assert_eq!(config.runtime.exec_cpu_affinity, "first");
    assert!(config.runtime.read_only);
    assert!(config.runtime.no_pivot);
    assert!(config.runtime.no_sync_log);
    assert_eq!(
        config.runtime.container_stop_timeout,
        MIN_CONTAINER_STOP_TIMEOUT_SECS
    );
    assert!(config.runtime.log_to_journald);
    assert!(config.runtime.restrict_oom_score_adj);
    assert!(config.runtime.enable_unprivileged_ports);
    assert!(config.runtime.enable_unprivileged_icmp);
    assert_eq!(
        config.network.config_dirs,
        vec!["/etc/cni/net.d", "/etc/kubernetes/cni/net.d"]
    );
    assert_eq!(
        config.network.plugin_dirs,
        vec!["/opt/cni/bin", "/usr/libexec/cni"]
    );
    assert_eq!(
        config.network.conf_template,
        dir.path().join("template.conflist").to_string_lossy()
    );
    assert_eq!(
        config
            .network
            .cni_config()
            .conf_template()
            .map(|path| path.to_string_lossy().to_string()),
        Some(
            dir.path()
                .join("template.conflist")
                .to_string_lossy()
                .to_string()
        )
    );
    assert_eq!(config.network.max_conf_num, 3);
    assert_eq!(config.network.ip_pref, MainIpPreference::Ipv4);
    assert_eq!(config.network.teardown_timeout.as_secs(), 75);
    assert_eq!(config.network.cni_config().teardown_timeout().as_secs(), 75);
    assert_eq!(
        config.network.default_network_name.as_deref(),
        Some("cluster-bridge")
    );
    assert!(config.network.disable_hostport_mapping);
    assert_eq!(config.logging.level, "debug");
    assert!(config.logging.file.is_some());
    assert_eq!(config.logging.max_container_log_line_size, 8192);
}

#[test]
fn env_overrides_can_enable_tcp_service_for_tcp_listen() {
    let _lock = env_lock().lock().unwrap();
    let _guard = EnvGuard::set_many(&[
        ("CRIUS_LISTEN", "127.0.0.1:50051"),
        ("CRIUS_ALLOW_TCP_SERVICE", "true"),
    ]);

    let mut config = Config::default();
    config
        .apply_env_overrides()
        .expect("tcp listen should validate after explicit env enablement");

    assert_eq!(config.api.listen, "127.0.0.1:50051");
    assert!(config.api.allow_tcp_service);
}

#[test]
fn default_stateful_paths_follow_runtime_and_persistent_roots() {
    let mut config = Config {
        root: "/var/lib/custom-crius".to_string(),
        api: ApiConfig {
            listen: DEFAULT_CRI_SOCKET_URI.to_string(),
            ..Default::default()
        },
        runtime: RuntimeConfig {
            root: "/run/custom-crius".to_string(),
            shim_dir: DEFAULT_RUNTIME_SHIM_DIR.to_string(),
            attach_socket_dir: DEFAULT_RUNTIME_ATTACH_SOCKET_DIR.to_string(),
            container_exits_dir: DEFAULT_RUNTIME_CONTAINER_EXITS_DIR.to_string(),
            clean_shutdown_file: DEFAULT_RUNTIME_CLEAN_SHUTDOWN_FILE.to_string(),
            version_file: DEFAULT_RUNTIME_VERSION_FILE.to_string(),
            version_file_persist: DEFAULT_RUNTIME_VERSION_FILE_PERSIST.to_string(),
            ..Default::default()
        },
        nri: NriConfig {
            socket_path: DEFAULT_NRI_SOCKET_PATH.to_string(),
            ..Default::default()
        },
        ..Default::default()
    };

    config.normalize_runtime_settings();

    assert_eq!(config.api.listen, "unix:///run/custom-crius/crius.sock");
    assert_eq!(config.runtime.shim_dir, "/run/custom-crius/shims");
    assert_eq!(config.runtime.attach_socket_dir, "/run/custom-crius/shims");
    assert_eq!(
        config.runtime.container_exits_dir,
        "/run/custom-crius/exits"
    );
    assert_eq!(
        config.runtime.clean_shutdown_file,
        "/var/lib/custom-crius/clean.shutdown"
    );
    assert_eq!(config.runtime.version_file, "/run/custom-crius/version");
    assert_eq!(
        config.runtime.version_file_persist,
        "/var/lib/custom-crius/version"
    );
    assert_eq!(config.nri.socket_path, "/run/custom-crius/nri.sock");
}

#[test]
fn legacy_runtime_clean_shutdown_path_rewrites_to_persistent_root() {
    let mut config = Config {
        root: "/var/lib/custom-crius".to_string(),
        runtime: RuntimeConfig {
            root: "/run/custom-crius".to_string(),
            clean_shutdown_file: LEGACY_RUNTIME_CLEAN_SHUTDOWN_FILE.to_string(),
            ..Default::default()
        },
        ..Default::default()
    };

    config.normalize_runtime_settings();

    assert_eq!(
        config.runtime.clean_shutdown_file,
        "/var/lib/custom-crius/clean.shutdown"
    );
}

fn env_lock() -> &'static Mutex<()> {
    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    ENV_LOCK.get_or_init(|| Mutex::new(()))
}

struct EnvGuard {
    saved: Vec<(String, Option<String>)>,
}

impl EnvGuard {
    fn set_many(values: &[(&str, &str)]) -> Self {
        let saved = values
            .iter()
            .map(|(key, value)| {
                let previous = std::env::var(key).ok();
                std::env::set_var(key, value);
                ((*key).to_string(), previous)
            })
            .collect();
        Self { saved }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        for (key, previous) in self.saved.drain(..) {
            if let Some(previous) = previous {
                std::env::set_var(key, previous);
            } else {
                std::env::remove_var(key);
            }
        }
    }
}
