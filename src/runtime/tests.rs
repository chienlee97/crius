use super::*;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use tempfile::tempdir;

fn create_test_runtime() -> (RuncRuntime, tempfile::TempDir) {
    create_test_runtime_with_restrict(false)
}

fn create_test_runtime_with_restrict(restrict: bool) -> (RuncRuntime, tempfile::TempDir) {
    let temp_dir = tempdir().unwrap();
    let mut runtime = RuncRuntime::new(PathBuf::from("runc"), temp_dir.path().join("containers"));
    runtime.set_restrict_oom_score_adj(restrict);
    (runtime, temp_dir)
}

#[test]
fn runtime_uses_rootless_xdg_runtime_dir_when_enabled() {
    let (mut runtime, _dir) = create_test_runtime();
    runtime.set_rootless(crate::rootless::EffectiveRootlessConfig {
        enabled: true,
        current_uid: 1000,
        current_gid: 1000,
        in_user_namespace: true,
        xdg_runtime_dir: PathBuf::from("/run/user/1000"),
        xdg_data_home: PathBuf::from("/home/test/.local/share"),
        storage_root: PathBuf::from("/home/test/.local/share/crius/storage"),
        runtime_root: PathBuf::from("/run/user/1000/crius"),
        netns_dir: PathBuf::from("/run/user/1000/crius/netns"),
        use_fuse_overlayfs: true,
        network_mode: crate::rootless::NetworkMode::None,
        slirp4netns_path: PathBuf::from("slirp4netns"),
        pasta_path: PathBuf::from("pasta"),
        disable_cgroup: true,
        tolerate_missing_hugetlb_controller: true,
    });

    assert_eq!(
        runtime.effective_xdg_runtime_dir(),
        PathBuf::from("/run/user/1000")
    );
}

fn write_arg_capture_runtime_script(dir: &Path, args_path: &Path) -> PathBuf {
    let script_path = dir.join("fake-runtime-args.sh");
    fs::write(
        &script_path,
        format!(
            r#"#!/bin/sh
set -eu
cmd="${{1:-}}"
shift || true
printf '%s\n' "$@" > "{}"
case "$cmd" in
  run|restore)
    exit 0
    ;;
  delete)
    exit 0
    ;;
  *)
    exit 1
    ;;
esac
"#,
            args_path.display()
        ),
    )
    .unwrap();
    fs::set_permissions(&script_path, fs::Permissions::from_mode(0o755)).unwrap();
    script_path
}

fn write_checkpoint_restore_arg_capture_runtime_script(
    dir: &Path,
    checkpoint_args_path: &Path,
    restore_args_path: &Path,
) -> PathBuf {
    let script_path = dir.join("fake-runtime-checkpoint-restore-args.sh");
    fs::write(
        &script_path,
        format!(
            r#"#!/bin/sh
set -eu
cmd="${{1:-}}"
shift || true
case "$cmd" in
  checkpoint)
    printf '%s\n' "$@" > "{checkpoint_args}"
    exit 0
    ;;
  restore)
    printf '%s\n' "$@" > "{restore_args}"
    exit 0
    ;;
  *)
    exit 1
    ;;
esac
"#,
            checkpoint_args = checkpoint_args_path.display(),
            restore_args = restore_args_path.display()
        ),
    )
    .unwrap();
    fs::set_permissions(&script_path, fs::Permissions::from_mode(0o755)).unwrap();
    script_path
}

fn write_runtime_features_script(dir: &Path, stdout_payload: &str, exit_code: i32) -> PathBuf {
    let script_path = dir.join("fake-runtime-features.sh");
    fs::write(
        &script_path,
        format!(
            r#"#!/bin/sh
set -eu
if [ "${{1:-}}" = "features" ]; then
  cat <<'EOF'
{stdout_payload}
EOF
  exit {exit_code}
fi
exit 1
"#,
            stdout_payload = stdout_payload,
            exit_code = exit_code
        ),
    )
    .unwrap();
    fs::set_permissions(&script_path, fs::Permissions::from_mode(0o755)).unwrap();
    script_path
}

fn write_runtime_script_that_fails_state(dir: &Path) -> PathBuf {
    let script_path = dir.join("fake-runtime-state-fails.sh");
    fs::write(
        &script_path,
        r#"#!/bin/sh
set -eu
if [ "${1:-}" = "state" ]; then
  exit 42
fi
exit 0
"#,
    )
    .unwrap();
    fs::set_permissions(&script_path, fs::Permissions::from_mode(0o755)).unwrap();
    script_path
}

fn write_stop_runtime_script(dir: &Path, command_log_path: &Path, initial_state: &str) -> PathBuf {
    let script_path = dir.join("fake-runtime-stop.sh");
    let state_dir = dir.join("runtime-state");
    fs::create_dir_all(&state_dir).unwrap();
    fs::write(state_dir.join("container-1.state"), initial_state).unwrap();
    fs::write(state_dir.join("container-1.pid"), "4242").unwrap();
    fs::write(
            &script_path,
            format!(
                r#"#!/bin/sh
set -eu
STATE_DIR="{state_dir}"
LOG_PATH="{log_path}"
cmd="${{1:-}}"
shift || true
case "$cmd" in
  state)
    id="${{1:-}}"
    file="$STATE_DIR/$id.state"
    if [ ! -f "$file" ]; then
      exit 1
    fi
    status="$(cat "$file")"
    printf '{{"ociVersion":"1.0.2","id":"%s","status":"%s","pid":4242,"bundle":"%s","rootfs":"%s","created":"2024-01-01T00:00:00Z","owner":"root"}}\n' "$id" "$status" "$STATE_DIR/bundle" "$STATE_DIR/rootfs"
    ;;
  resume)
    id="${{1:-}}"
    printf 'resume\n' >> "$LOG_PATH"
    echo running > "$STATE_DIR/$id.state"
    ;;
  kill)
    id="${{1:-}}"
    sig="${{2:-}}"
    printf 'kill:%s\n' "$sig" >> "$LOG_PATH"
    if [ "$sig" = "KILL" ]; then
      echo stopped > "$STATE_DIR/$id.state"
    fi
    ;;
  *)
    exit 1
    ;;
esac
"#,
                state_dir = state_dir.display(),
                log_path = command_log_path.display(),
            ),
        )
        .unwrap();
    fs::set_permissions(&script_path, fs::Permissions::from_mode(0o755)).unwrap();
    script_path
}

fn create_test_config() -> ContainerConfig {
    ContainerConfig {
        name: "test".to_string(),
        image: "test:latest".to_string(),
        command: vec!["echo".to_string(), "hello".to_string()],
        args: vec![],
        env: vec![],
        working_dir: None,
        mounts: vec![],
        labels: vec![],
        annotations: vec![],
        privileged: false,
        user: None,
        run_as_group: None,
        supplemental_groups: vec![],
        hostname: None,
        tty: false,
        stdin: false,
        stdin_once: false,
        log_path: None,
        readonly_rootfs: false,
        seccomp_notifier: None,
        pids_limit: None,
        no_new_privileges: None,
        apparmor_profile: None,
        selinux_label: None,
        seccomp_profile: None,
        capabilities: None,
        cgroup_parent: None,
        sysctls: HashMap::new(),
        namespace_options: None,
        namespace_paths: NamespacePaths::default(),
        linux_resources: None,
        devices: vec![],
        masked_paths: vec![],
        readonly_paths: vec![],
        rootfs: PathBuf::from("/tmp/rootfs"),
    }
}

fn write_test_image_metadata(
    runtime: &RuncRuntime,
    image_id: &str,
    repo_tag: &str,
    env: Vec<&str>,
    entrypoint: Vec<&str>,
    cmd: Vec<&str>,
    working_dir: Option<&str>,
) {
    let image_dir = runtime.image_storage_root.join("images").join(image_id);
    fs::create_dir_all(&image_dir).unwrap();
    let metadata = crate::image::ImageMeta {
        id: image_id.to_string(),
        repo_tags: vec![repo_tag.to_string()],
        config_env: env.into_iter().map(str::to_string).collect(),
        config_entrypoint: entrypoint.into_iter().map(str::to_string).collect(),
        config_cmd: cmd.into_iter().map(str::to_string).collect(),
        config_working_dir: working_dir.map(str::to_string),
        ..Default::default()
    };
    fs::write(
        image_dir.join("metadata.json"),
        serde_json::to_vec(&metadata).unwrap(),
    )
    .unwrap();
}

fn test_mount_config(source: PathBuf, destination: &str) -> MountConfig {
    MountConfig {
        source,
        destination: PathBuf::from(destination),
        read_only: false,
        missing_source_policy: MissingMountSourcePolicy::Reject,
        selinux_relabel: false,
        propagation: MountPropagationMode::Private,
        recursive_read_only: false,
        uid_mappings: Vec::new(),
        gid_mappings: Vec::new(),
        requested_image: None,
        image_sub_path: None,
    }
}

#[test]
fn test_create_spec() {
    let (runtime, _temp) = create_test_runtime();
    let config = create_test_config();

    let spec = runtime.create_spec(&config, "test-id").unwrap();

    assert_eq!(spec.oci_version, "1.0.2");
    assert!(spec.process.is_some());
    assert!(spec.root.is_some());
    assert!(spec.linux.is_some());
}

#[test]
fn test_create_spec_merges_image_runtime_defaults() {
    let (runtime, _temp) = create_test_runtime();
    write_test_image_metadata(
        &runtime,
        "sha256:test-image",
        "test:latest",
        vec!["PATH=/usr/local/bin:/usr/bin", "FOO=image"],
        vec!["kube-apiserver"],
        vec!["--help"],
        Some("/workspace"),
    );

    let mut config = create_test_config();
    config.command = vec!["kube-apiserver".to_string()];
    config.args = vec!["--secure-port=6443".to_string()];
    config.env = vec![("FOO".to_string(), "override".to_string())];
    config.working_dir = None;

    let spec = runtime.create_spec(&config, "test-id").unwrap();
    let process = spec.process.expect("process config should exist");
    let env = process.env.expect("process env should exist");

    assert_eq!(
        process.args,
        vec![
            "kube-apiserver".to_string(),
            "--secure-port=6443".to_string()
        ]
    );
    assert!(env
        .iter()
        .any(|entry| entry == "PATH=/usr/local/bin:/usr/bin"));
    assert!(env.iter().any(|entry| entry == "FOO=override"));
    assert_eq!(process.cwd, "/workspace");
}

#[test]
fn test_create_spec_encodes_labels_annotation_for_nri() {
    let (runtime, _temp) = create_test_runtime();
    let mut config = create_test_config();
    config.labels = vec![("app".to_string(), "demo".to_string())];

    let spec = runtime.create_spec(&config, "test-id").unwrap();
    let annotations = spec.annotations.unwrap();
    let labels: std::collections::HashMap<String, String> =
        serde_json::from_str(annotations.get(CRIO_LABELS_ANNOTATION).unwrap()).unwrap();

    assert_eq!(labels.get("app").map(String::as_str), Some("demo"));
}

#[test]
fn test_create_spec_uses_configured_default_ulimits() {
    let (mut runtime, _temp) = create_test_runtime();
    runtime.set_default_ulimits(vec![Rlimit {
        rtype: "RLIMIT_NOFILE".to_string(),
        soft: 1024,
        hard: 2048,
    }]);

    let spec = runtime
        .create_spec(&create_test_config(), "test-id")
        .unwrap();
    let rlimits = spec.process.unwrap().rlimits.unwrap();

    assert_eq!(rlimits.len(), 1);
    assert_eq!(rlimits[0].rtype, "RLIMIT_NOFILE");
    assert_eq!(rlimits[0].soft, 1024);
    assert_eq!(rlimits[0].hard, 2048);
}

#[test]
fn test_create_spec_adds_timezone_mount_and_env() {
    let (mut runtime, _temp) = create_test_runtime();
    runtime.set_timezone("UTC".to_string());

    let spec = runtime
        .create_spec(&create_test_config(), "test-id")
        .unwrap();
    let mounts = spec.mounts.unwrap();
    let process = spec.process.unwrap();

    assert!(mounts
        .iter()
        .any(|mount| mount.destination == "/etc/localtime"
            && mount.mount_type.as_deref() == Some("bind")));
    assert!(process
        .env
        .unwrap_or_default()
        .iter()
        .any(|entry| entry == "TZ=UTC"));
}

#[test]
fn test_create_spec_applies_default_proc_protection_when_proc_mount_disabled() {
    let (mut runtime, _temp) = create_test_runtime();
    runtime.set_disable_proc_mount(true);

    let spec = runtime
        .create_spec(&create_test_config(), "test-id")
        .unwrap();
    let linux = spec.linux.unwrap();

    assert_eq!(linux.masked_paths, Some(Spec::default_masked_paths()));
    assert_eq!(linux.readonly_paths, Some(Spec::default_readonly_paths()));
}

#[test]
fn test_create_spec_rejects_custom_proc_paths_when_proc_mount_disabled() {
    let (mut runtime, _temp) = create_test_runtime();
    runtime.set_disable_proc_mount(true);
    let mut config = create_test_config();
    config.masked_paths = vec!["/proc".to_string()];

    let err = runtime
        .create_spec(&config, "test-id")
        .expect_err("custom proc paths must fail when proc mount is disabled");
    assert!(err.to_string().contains("ProcMount support is disabled"));
}

#[test]
fn test_create_spec_honors_base_runtime_spec() {
    let (mut runtime, _temp) = create_test_runtime();
    runtime.set_base_runtime_spec(Some(Spec {
        oci_version: "1.0.2".to_string(),
        process: Some(Process {
            terminal: Some(false),
            user: None,
            args: vec!["/bin/sh".to_string()],
            env: None,
            cwd: "/".to_string(),
            capabilities: None,
            rlimits: Some(vec![Rlimit {
                rtype: "RLIMIT_NPROC".to_string(),
                soft: 64,
                hard: 128,
            }]),
            oom_score_adj: None,
            scheduler: None,
            no_new_privileges: None,
            apparmor_profile: None,
            selinux_label: None,
            io_priority: None,
        }),
        root: None,
        hostname: None,
        mounts: None,
        hooks: Some(crate::oci::spec::Hooks {
            prestart: Some(vec![crate::oci::spec::Hook {
                path: "/bin/true".to_string(),
                args: None,
                env: None,
                timeout: None,
            }]),
            create_runtime: None,
            create_container: None,
            start_container: None,
            poststart: None,
            poststop: None,
        }),
        linux: None,
        annotations: Some(HashMap::from([(
            "example.com/base".to_string(),
            "true".to_string(),
        )])),
    }));

    let spec = runtime
        .create_spec(&create_test_config(), "test-id")
        .unwrap();

    assert!(spec.hooks.is_some());
    assert_eq!(
        spec.annotations
            .as_ref()
            .and_then(|annotations| annotations.get("example.com/base"))
            .map(String::as_str),
        Some("true")
    );
    assert_eq!(
        spec.process
            .as_ref()
            .and_then(|process| process.rlimits.as_ref())
            .map(Vec::len),
        Some(1)
    );
}

#[test]
fn test_create_spec_merges_configured_hooks_dirs() {
    let (mut runtime, temp_dir) = create_test_runtime();
    let hooks_a = temp_dir.path().join("hooks-a");
    let hooks_b = temp_dir.path().join("hooks-b");
    fs::create_dir_all(&hooks_a).unwrap();
    fs::create_dir_all(&hooks_b).unwrap();
    fs::write(
        hooks_a.join("00-prestart.json"),
        serde_json::to_vec(&crate::oci::spec::Hooks {
            prestart: Some(vec![crate::oci::spec::Hook {
                path: "/usr/bin/pre-a".to_string(),
                args: None,
                env: None,
                timeout: None,
            }]),
            create_runtime: None,
            create_container: None,
            start_container: None,
            poststart: None,
            poststop: None,
        })
        .unwrap(),
    )
    .unwrap();
    fs::write(
        hooks_b.join("01-poststop.json"),
        serde_json::to_vec(&crate::oci::spec::Spec {
            oci_version: "1.0.2".to_string(),
            process: None,
            root: None,
            hostname: None,
            mounts: None,
            hooks: Some(crate::oci::spec::Hooks {
                prestart: None,
                create_runtime: None,
                create_container: None,
                start_container: None,
                poststart: None,
                poststop: Some(vec![crate::oci::spec::Hook {
                    path: "/usr/bin/post-b".to_string(),
                    args: None,
                    env: None,
                    timeout: None,
                }]),
            }),
            linux: None,
            annotations: None,
        })
        .unwrap(),
    )
    .unwrap();
    runtime.set_hooks_dirs(vec![hooks_a, hooks_b]);

    let spec = runtime
        .create_spec(&create_test_config(), "test-id")
        .unwrap();
    let hooks = spec
        .hooks
        .expect("configured hooks should be merged into spec");
    assert_eq!(hooks.prestart.unwrap()[0].path, "/usr/bin/pre-a");
    assert_eq!(hooks.poststop.unwrap()[0].path, "/usr/bin/post-b");
}

#[test]
fn test_create_spec_leaves_inheritable_caps_empty_by_default() {
    let (runtime, _temp) = create_test_runtime();
    let spec = runtime
        .create_spec(&create_test_config(), "test-id")
        .unwrap();
    let inheritable = spec
        .process
        .unwrap()
        .capabilities
        .unwrap()
        .inheritable
        .unwrap();

    assert!(inheritable.is_empty());
}

#[test]
fn test_create_spec_can_add_inheritable_caps() {
    let (mut runtime, _temp) = create_test_runtime();
    runtime.set_add_inheritable_capabilities(true);

    let spec = runtime
        .create_spec(&create_test_config(), "test-id")
        .unwrap();
    let inheritable = spec
        .process
        .unwrap()
        .capabilities
        .unwrap()
        .inheritable
        .unwrap();

    assert!(inheritable.iter().any(|cap| cap == "CAP_CHOWN"));
}

#[test]
fn test_bundle_path() {
    let (runtime, _temp) = create_test_runtime();
    let path = runtime.bundle_path("test-container");
    assert!(path.to_string_lossy().contains("test-container"));
}

#[test]
fn test_resolve_image_dir_uses_runtime_configured_storage_root() {
    let temp_dir = tempdir().unwrap();
    let storage_root = temp_dir
        .path()
        .join("storage")
        .join("images")
        .join("sha256:test-image");
    fs::create_dir_all(&storage_root).unwrap();
    fs::write(
        storage_root.join("metadata.json"),
        serde_json::json!({
            "id": "sha256:test-image",
            "repo_tags": ["busybox:latest"],
            "size": 123,
        })
        .to_string(),
    )
    .unwrap();

    let runtime = RuncRuntime::new(PathBuf::from("runc"), temp_dir.path().join("containers"));
    let resolved = runtime.resolve_image_dir("busybox:latest").unwrap();
    assert_eq!(resolved, storage_root);
}

#[test]
fn test_spec_with_custom_mounts() {
    let (runtime, temp_dir) = create_test_runtime();
    let host_path = temp_dir.path().join("host-path");
    fs::write(&host_path, "data").unwrap();
    let mut config = create_test_config();
    config.mounts = vec![MountConfig {
        source: host_path,
        destination: PathBuf::from("/container/path"),
        read_only: true,
        missing_source_policy: MissingMountSourcePolicy::Reject,
        selinux_relabel: false,
        propagation: MountPropagationMode::Private,
        recursive_read_only: false,
        uid_mappings: Vec::new(),
        gid_mappings: Vec::new(),
        requested_image: None,
        image_sub_path: None,
    }];

    let spec = runtime.create_spec(&config, "test-id").unwrap();
    let mounts = spec.mounts.unwrap();

    // Should have default mounts + 1 custom mount
    assert!(mounts.len() > 1);
    assert!(mounts.iter().any(|m| m.destination == "/container/path"));
}

#[test]
fn test_spec_applies_bind_mount_prefix_to_custom_mount_sources() {
    let (mut runtime, temp_dir) = create_test_runtime();
    let prefix = temp_dir.path().join("host");
    let prefixed_source = prefix.join("var/lib/data");
    fs::create_dir_all(prefixed_source.parent().unwrap()).unwrap();
    fs::write(&prefixed_source, "data").unwrap();
    runtime.set_bind_mount_prefix(prefix);
    let mut config = create_test_config();
    config.mounts = vec![MountConfig {
        source: PathBuf::from("/var/lib/data"),
        destination: PathBuf::from("/container/path"),
        read_only: true,
        missing_source_policy: MissingMountSourcePolicy::Reject,
        selinux_relabel: false,
        propagation: MountPropagationMode::Private,
        recursive_read_only: false,
        uid_mappings: Vec::new(),
        gid_mappings: Vec::new(),
        requested_image: None,
        image_sub_path: None,
    }];

    let spec = runtime.create_spec(&config, "test-id").unwrap();
    let mounts = spec.mounts.unwrap();

    assert!(mounts.iter().any(|mount| {
        mount.destination == "/container/path"
            && mount.source.as_deref() == Some(prefixed_source.to_string_lossy().as_ref())
    }));
}

#[test]
fn test_spec_excludes_hugepages_mount_by_default() {
    let (runtime, _temp) = create_test_runtime();
    let config = create_test_config();

    let spec = runtime.create_spec(&config, "test-id").unwrap();
    let mounts = spec.mounts.unwrap();

    assert!(!mounts.iter().any(|m| m.destination == "/dev/hugepages"));
}

#[test]
fn test_spec_with_user() {
    let (runtime, _temp) = create_test_runtime();
    let mut config = create_test_config();
    config.user = Some("1000".to_string());

    let spec = runtime.create_spec(&config, "test-id").unwrap();
    let process = spec.process.unwrap();
    let user = process.user.unwrap();

    assert_eq!(user.uid, 1000);
    assert_eq!(user.gid, 1000);
}

#[test]
fn test_spec_with_supplemental_groups_without_explicit_user() {
    let (runtime, _temp) = create_test_runtime();
    let mut config = create_test_config();
    config.run_as_group = Some(3000);
    config.supplemental_groups = vec![2000, 2001];

    let spec = runtime.create_spec(&config, "test-id").unwrap();
    let process = spec.process.unwrap();
    let user = process.user.unwrap();

    assert_eq!(user.uid, 0);
    assert_eq!(user.gid, 3000);
    assert_eq!(user.additional_gids, Some(vec![2000, 2001]));
}

#[test]
fn test_spec_with_username() {
    let (runtime, _temp) = create_test_runtime();
    let mut config = create_test_config();
    config.user = Some("nobody".to_string());

    let spec = runtime.create_spec(&config, "test-id").unwrap();
    let process = spec.process.unwrap();
    let user = process.user.unwrap();

    assert_eq!(user.username.as_deref(), Some("nobody"));
}

#[test]
fn test_spec_privileged() {
    let (runtime, _temp) = create_test_runtime();
    let mut config = create_test_config();
    config.privileged = true;

    let spec = runtime.create_spec(&config, "test-id").unwrap();
    let linux = spec.linux.unwrap();

    let devices = linux.devices.unwrap();
    assert!(devices.iter().any(|device| device.path == "/dev/null"));
    let device_rules = linux.resources.unwrap().devices.unwrap();
    assert_eq!(device_rules.len(), 1);
    assert!(device_rules[0].allow);
    assert_eq!(device_rules[0].access.as_deref(), Some("rwm"));
}

#[test]
fn test_spec_privileged_without_host_devices_skips_host_devices() {
    let (mut runtime, _temp) = create_test_runtime();
    runtime.set_privileged_without_host_devices(true);
    let mut config = create_test_config();
    config.privileged = true;

    let spec = runtime.create_spec(&config, "test-id").unwrap();
    let linux = spec.linux.unwrap();

    assert!(linux.devices.unwrap().is_empty());
    assert!(linux.resources.unwrap().devices.unwrap().is_empty());
}

#[test]
fn test_spec_privileged_without_host_devices_can_keep_allow_all_rule() {
    let (mut runtime, _temp) = create_test_runtime();
    runtime.set_privileged_without_host_devices(true);
    runtime.set_privileged_without_host_devices_all_devices_allowed(true);
    let mut config = create_test_config();
    config.privileged = true;

    let spec = runtime.create_spec(&config, "test-id").unwrap();
    let linux = spec.linux.unwrap();

    assert!(linux.devices.unwrap().is_empty());
    let device_rules = linux.resources.unwrap().devices.unwrap();
    assert_eq!(device_rules.len(), 1);
    assert!(device_rules[0].allow);
    assert_eq!(device_rules[0].access.as_deref(), Some("rwm"));
}

#[test]
fn test_host_device_skip_rules_match_containerd_style_filters() {
    assert!(RuncRuntime::should_skip_host_device_dir("pts"));
    assert!(RuncRuntime::should_skip_host_device_dir("shm"));
    assert!(RuncRuntime::should_skip_host_device_dir("fd"));
    assert!(RuncRuntime::should_skip_host_device_dir("mqueue"));
    assert!(RuncRuntime::should_skip_host_device_dir(".udev"));
    assert!(!RuncRuntime::should_skip_host_device_dir("mapper"));

    assert!(RuncRuntime::should_skip_host_device_file("console"));
    assert!(!RuncRuntime::should_skip_host_device_file("null"));
}

#[test]
fn test_privileged_spec_uses_full_capability_baseline() {
    let (mut runtime, _temp) = create_test_runtime();
    runtime.set_default_capabilities(vec!["CAP_CHOWN".to_string()]);

    let mut config = create_test_config();
    config.privileged = true;
    config.image.clear();

    let spec = runtime.create_spec(&config, "test-id").unwrap();
    let capabilities = spec
        .process
        .and_then(|process| process.capabilities)
        .expect("privileged container should include capabilities");
    let bounding = capabilities.bounding.unwrap_or_default();

    assert!(bounding.contains(&"CAP_CHOWN".to_string()));
    assert!(bounding.contains(&"CAP_NET_ADMIN".to_string()));
    assert!(bounding.contains(&"CAP_SYS_ADMIN".to_string()));
    assert!(!bounding.is_empty());
}

#[test]
fn test_spec_with_runtime_options() {
    let (mut runtime, _temp) = create_test_runtime();
    runtime.set_default_env(vec![
        (
            "HTTP_PROXY".to_string(),
            "http://proxy.internal".to_string(),
        ),
        ("LANG".to_string(), "C".to_string()),
    ]);
    runtime.set_default_capabilities(vec!["NET_BIND_SERVICE".to_string(), "CHOWN".to_string()]);
    runtime.set_default_sysctls(HashMap::from([(
        "kernel.shm_rmid_forced".to_string(),
        "1".to_string(),
    )]));
    let mut config = create_test_config();
    config.tty = true;
    config.readonly_rootfs = true;
    config.cgroup_parent = Some("kubepods.slice/pod123".to_string());
    config.env = vec![
        ("LANG".to_string(), "C.UTF-8".to_string()),
        ("FOO".to_string(), "bar".to_string()),
    ];
    config
        .sysctls
        .insert("net.ipv4.ip_forward".to_string(), "1".to_string());
    config.namespace_paths.network = Some(PathBuf::from("/var/run/netns/test-pod"));
    config.linux_resources = Some(LinuxContainerResources {
        cpu_period: 100000,
        cpu_quota: 200000,
        cpu_shares: 2048,
        memory_limit_in_bytes: 536870912,
        oom_score_adj: 0,
        cpuset_cpus: "0-1".to_string(),
        cpuset_mems: "0".to_string(),
        hugepage_limits: vec![],
        unified: HashMap::new(),
        memory_swap_limit_in_bytes: 1073741824,
    });

    let spec = runtime.create_spec(&config, "test-id").unwrap();
    let process = spec.process.unwrap();
    let root = spec.root.unwrap();
    let linux = spec.linux.unwrap();

    assert_eq!(process.terminal, Some(true));
    assert_eq!(root.readonly, Some(true));
    assert!(process
        .env
        .as_ref()
        .unwrap()
        .iter()
        .any(|entry| entry == "HTTP_PROXY=http://proxy.internal"));
    assert!(process
        .env
        .as_ref()
        .unwrap()
        .iter()
        .any(|entry| entry == "LANG=C.UTF-8"));
    assert!(process
        .env
        .as_ref()
        .unwrap()
        .iter()
        .any(|entry| entry == "FOO=bar"));
    assert_eq!(
        process
            .capabilities
            .as_ref()
            .and_then(|caps| caps.bounding.as_ref())
            .cloned()
            .unwrap_or_default(),
        vec!["CAP_NET_BIND_SERVICE".to_string(), "CAP_CHOWN".to_string()]
    );
    assert_eq!(linux.cgroups_path.as_deref(), Some("kubepods.slice/pod123"));
    assert_eq!(
        linux
            .sysctl
            .as_ref()
            .and_then(|sysctls| sysctls.get("kernel.shm_rmid_forced"))
            .map(String::as_str),
        Some("1")
    );
    assert_eq!(
        linux
            .sysctl
            .as_ref()
            .and_then(|sysctls| sysctls.get("net.ipv4.ip_forward"))
            .map(String::as_str),
        Some("1")
    );
    assert_eq!(
        linux
            .namespaces
            .as_ref()
            .and_then(|namespaces| namespaces.iter().find(|ns| ns.ns_type == "network"))
            .and_then(|namespace| namespace.path.as_deref()),
        Some("/var/run/netns/test-pod")
    );
    assert_eq!(
        linux
            .resources
            .as_ref()
            .and_then(|resources| resources.cpu.as_ref())
            .and_then(|cpu| cpu.quota),
        Some(200000)
    );
    assert_eq!(
        linux
            .resources
            .as_ref()
            .and_then(|resources| resources.memory.as_ref())
            .and_then(|memory| memory.limit),
        Some(536870912)
    );
}

#[test]
fn test_spec_omits_cgroup_configuration_when_disabled() {
    let (mut runtime, _temp) = create_test_runtime();
    runtime.set_disable_cgroup(true);
    let mut config = create_test_config();
    config.cgroup_parent = Some("kubepods.slice/pod123".to_string());
    config.pids_limit = Some(256);
    config.linux_resources = Some(LinuxContainerResources {
        cpu_shares: 2048,
        memory_limit_in_bytes: 536870912,
        ..Default::default()
    });

    let spec = runtime.create_spec(&config, "test-id").unwrap();
    let linux = spec.linux.unwrap();

    assert!(linux.cgroups_path.is_none());
    assert!(linux.resources.is_none());
}

#[test]
fn test_oci_cgroups_path_keeps_non_systemd_parent_unchanged() {
    assert_eq!(
        RuncRuntime::oci_cgroups_path(Some("kubepods/pod123"), "container-1").as_deref(),
        Some("kubepods/pod123")
    );
}

#[test]
fn test_oci_cgroups_path_derives_systemd_scope_from_slice_parent() {
    assert_eq!(
        RuncRuntime::oci_cgroups_path(
            Some("/kubepods.slice/kubepods-burstable.slice/pod123.slice"),
            "container-1",
        )
        .as_deref(),
        Some("pod123.slice:crius:container-1")
    );
}

#[test]
fn test_oci_cgroups_path_uses_systemd_slice_basename() {
    assert_eq!(
        RuncRuntime::oci_cgroups_path(
            Some("/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-podabc.slice",),
            "container-1",
        )
        .as_deref(),
        Some("kubepods-burstable-podabc.slice:crius:container-1")
    );
}

#[test]
fn test_create_spec_sets_process_oom_score_adj_from_linux_resources() {
    let (runtime, _temp) = create_test_runtime();
    let mut config = create_test_config();
    config.linux_resources = Some(LinuxContainerResources {
        oom_score_adj: 321,
        ..Default::default()
    });

    let spec = runtime.create_spec(&config, "test-id").unwrap();

    assert_eq!(
        spec.process
            .as_ref()
            .and_then(|process| process.oom_score_adj),
        Some(321)
    );
}

#[test]
fn test_create_spec_restricts_oom_score_adj_to_daemon_floor() {
    let current = RuncRuntime::daemon_oom_score_adj().unwrap();
    let (runtime, _temp) = create_test_runtime_with_restrict(true);
    let mut config = create_test_config();
    config.linux_resources = Some(LinuxContainerResources {
        oom_score_adj: current - 1,
        ..Default::default()
    });

    let spec = runtime.create_spec(&config, "test-id").unwrap();

    assert_eq!(
        spec.process
            .as_ref()
            .and_then(|process| process.oom_score_adj),
        Some(current as i32)
    );
}

#[test]
fn test_enforce_oom_score_adj_policy_clamps_existing_spec_after_adjustment() {
    let current = RuncRuntime::daemon_oom_score_adj().unwrap();
    let (runtime, _temp) = create_test_runtime_with_restrict(true);
    let mut spec = Spec::new("1.0.2");
    spec.process = Some(Process {
        terminal: Some(false),
        user: None,
        args: vec!["sleep".to_string(), "1".to_string()],
        env: None,
        cwd: "/".to_string(),
        capabilities: None,
        rlimits: None,
        oom_score_adj: Some((current - 1) as i32),
        scheduler: None,
        no_new_privileges: None,
        apparmor_profile: None,
        selinux_label: None,
        io_priority: None,
    });

    runtime.enforce_oom_score_adj_policy(&mut spec).unwrap();

    assert_eq!(
        spec.process
            .as_ref()
            .and_then(|process| process.oom_score_adj),
        Some(current as i32)
    );
}

#[test]
fn test_readonly_rootfs_keeps_default_tmpfs_and_custom_rw_mounts() {
    let (runtime, temp_dir) = create_test_runtime();
    let host_path = temp_dir.path().join("host-rw");
    fs::write(&host_path, "data").unwrap();
    let mut config = create_test_config();
    config.readonly_rootfs = true;
    config.mounts = vec![MountConfig {
        source: host_path,
        destination: PathBuf::from("/var/lib/app"),
        read_only: false,
        missing_source_policy: MissingMountSourcePolicy::Reject,
        selinux_relabel: false,
        propagation: MountPropagationMode::Private,
        recursive_read_only: false,
        uid_mappings: Vec::new(),
        gid_mappings: Vec::new(),
        requested_image: None,
        image_sub_path: None,
    }];

    let spec = runtime.create_spec(&config, "test-id").unwrap();
    let root = spec.root.unwrap();
    let mounts = spec.mounts.unwrap();

    assert_eq!(root.readonly, Some(true));
    assert!(mounts.iter().any(|mount| {
        mount.destination == "/proc" && mount.mount_type.as_deref() == Some("proc")
    }));
    assert!(mounts.iter().any(|mount| {
        mount.destination == "/dev"
            && mount.mount_type.as_deref() == Some("tmpfs")
            && mount
                .options
                .as_ref()
                .map(|options| options.iter().any(|option| option == "mode=755"))
                .unwrap_or(false)
    }));
    assert!(mounts.iter().any(|mount| {
        mount.destination == "/dev/shm"
            && mount.mount_type.as_deref() == Some("tmpfs")
            && mount
                .options
                .as_ref()
                .map(|options| options.iter().any(|option| option == "mode=1777"))
                .unwrap_or(false)
    }));
    assert!(mounts.iter().any(|mount| {
        mount.destination == "/var/lib/app"
            && mount.mount_type.as_deref() == Some("bind")
            && mount
                .options
                .as_ref()
                .map(|options| options.iter().any(|option| option == "rw"))
                .unwrap_or(false)
    }));
}

#[test]
fn test_spec_uses_host_pid_namespace_when_requested() {
    let (runtime, _temp) = create_test_runtime();
    let mut config = create_test_config();
    config.namespace_options = Some(NamespaceOption {
        network: NamespaceMode::Pod as i32,
        pid: NamespaceMode::Node as i32,
        ipc: NamespaceMode::Pod as i32,
        target_id: String::new(),
        userns_options: None,
    });

    let spec = runtime.create_spec(&config, "test-id").unwrap();
    let linux = spec.linux.unwrap();
    let pid_namespace = linux
        .namespaces
        .unwrap()
        .into_iter()
        .find(|namespace| namespace.ns_type == "pid")
        .unwrap();

    assert_eq!(pid_namespace.path.as_deref(), Some("/proc/1/ns/pid"));
}

#[test]
fn test_spec_uses_host_ipc_namespace_and_host_ipc_mounts_when_requested() {
    let (runtime, _temp) = create_test_runtime();
    let mut config = create_test_config();
    config.namespace_options = Some(NamespaceOption {
        network: NamespaceMode::Pod as i32,
        pid: NamespaceMode::Pod as i32,
        ipc: NamespaceMode::Node as i32,
        target_id: String::new(),
        userns_options: None,
    });

    let spec = runtime.create_spec(&config, "test-id").unwrap();
    let linux = spec.linux.clone().unwrap();
    let ipc_namespace = linux
        .namespaces
        .unwrap()
        .into_iter()
        .find(|namespace| namespace.ns_type == "ipc")
        .unwrap();
    let mounts = spec.mounts.unwrap();

    assert_eq!(ipc_namespace.path.as_deref(), Some("/proc/1/ns/ipc"));
    assert!(mounts.iter().any(|mount| {
        mount.destination == "/dev/shm"
            && mount.source.as_deref() == Some("/dev/shm")
            && mount.mount_type.as_deref() == Some("bind")
    }));
    assert!(mounts.iter().any(|mount| {
        mount.destination == "/dev/mqueue"
            && mount.source.as_deref() == Some("/dev/mqueue")
            && mount.mount_type.as_deref() == Some("bind")
    }));
}

#[test]
fn test_spec_applies_bind_mount_prefix_to_host_ipc_mounts() {
    let (mut runtime, temp_dir) = create_test_runtime();
    let prefix = temp_dir.path().join("host");
    fs::create_dir_all(prefix.join("dev")).unwrap();
    fs::create_dir_all(prefix.join("dev/shm")).unwrap();
    fs::create_dir_all(prefix.join("dev/mqueue")).unwrap();
    runtime.set_bind_mount_prefix(prefix.clone());
    let mut config = create_test_config();
    config.namespace_options = Some(NamespaceOption {
        network: NamespaceMode::Pod as i32,
        pid: NamespaceMode::Pod as i32,
        ipc: NamespaceMode::Node as i32,
        target_id: String::new(),
        userns_options: None,
    });

    let spec = runtime.create_spec(&config, "test-id").unwrap();
    let mounts = spec.mounts.unwrap();

    assert!(mounts.iter().any(|mount| {
        mount.destination == "/dev/shm"
            && mount.source.as_deref() == Some(prefix.join("dev/shm").to_string_lossy().as_ref())
    }));
    assert!(mounts.iter().any(|mount| {
        mount.destination == "/dev/mqueue"
            && mount.source.as_deref() == Some(prefix.join("dev/mqueue").to_string_lossy().as_ref())
    }));
}

#[test]
fn test_spec_adds_user_namespace_and_mappings_when_userns_pod_mode_requested() {
    let (runtime, _temp) = create_test_runtime();
    let mut config = create_test_config();
    config.namespace_options = Some(NamespaceOption {
        network: NamespaceMode::Pod as i32,
        pid: NamespaceMode::Pod as i32,
        ipc: NamespaceMode::Pod as i32,
        target_id: String::new(),
        userns_options: Some(crate::proto::runtime::v1::UserNamespace {
            mode: NamespaceMode::Pod as i32,
            uids: vec![crate::proto::runtime::v1::IdMapping {
                host_id: 100000,
                container_id: 0,
                length: 65536,
            }],
            gids: vec![crate::proto::runtime::v1::IdMapping {
                host_id: 200000,
                container_id: 0,
                length: 65536,
            }],
        }),
    });

    let spec = runtime.create_spec(&config, "test-id").unwrap();
    let linux = spec.linux.unwrap();

    assert!(linux
        .namespaces
        .as_ref()
        .unwrap()
        .iter()
        .any(|namespace| namespace.ns_type == "user"));
    assert_eq!(linux.uid_mappings.as_ref().unwrap()[0].host_id, 100000);
    assert_eq!(linux.gid_mappings.as_ref().unwrap()[0].host_id, 200000);
}

#[test]
fn test_spec_rejects_userns_node_mode_with_mappings() {
    let (runtime, _temp) = create_test_runtime();
    let mut config = create_test_config();
    config.namespace_options = Some(NamespaceOption {
        network: NamespaceMode::Pod as i32,
        pid: NamespaceMode::Pod as i32,
        ipc: NamespaceMode::Pod as i32,
        target_id: String::new(),
        userns_options: Some(crate::proto::runtime::v1::UserNamespace {
            mode: NamespaceMode::Node as i32,
            uids: vec![crate::proto::runtime::v1::IdMapping {
                host_id: 100000,
                container_id: 0,
                length: 65536,
            }],
            gids: vec![],
        }),
    });

    let err = runtime.create_spec(&config, "test-id").unwrap_err();
    assert!(err.to_string().contains("mode NODE"));
}

#[test]
fn test_spec_with_device_mappings() {
    let (runtime, _temp) = create_test_runtime();
    let mut config = create_test_config();
    config.devices = vec![DeviceMapping {
        source: PathBuf::from("/dev/null"),
        destination: PathBuf::from("/dev/custom-null"),
        permissions: "rw".to_string(),
    }];

    let spec = runtime.create_spec(&config, "test-id").unwrap();
    let linux = spec.linux.unwrap();
    let devices = linux.devices.unwrap();
    let cgroup_rules = linux.resources.unwrap().devices.unwrap();

    assert!(devices
        .iter()
        .any(|device| device.path == "/dev/custom-null"));
    assert!(cgroup_rules.iter().any(|rule| {
        rule.access.as_deref() == Some("rw") && rule.device_type.as_deref() == Some("c")
    }));
}

#[test]
fn test_spec_injects_additional_devices() {
    let (mut runtime, _temp) = create_test_runtime();
    runtime.set_additional_devices(vec![DeviceMapping {
        source: PathBuf::from("/dev/zero"),
        destination: PathBuf::from("/dev/test-zero"),
        permissions: "r".to_string(),
    }]);
    let config = create_test_config();

    let spec = runtime.create_spec(&config, "test-id").unwrap();
    let linux = spec.linux.unwrap();
    let devices = linux.devices.unwrap();
    let rules = linux.resources.unwrap().devices.unwrap();

    assert!(devices.iter().any(|device| device.path == "/dev/test-zero"));
    assert!(rules.iter().any(|rule| {
        rule.device_type.as_deref() == Some("c")
            && rule.major == Some(1)
            && rule.minor == Some(5)
            && rule.access.as_deref() == Some("r")
    }));
}

#[test]
fn test_spec_rejects_unlisted_requested_device() {
    let (mut runtime, _temp) = create_test_runtime();
    runtime.set_allowed_devices(vec![PathBuf::from("/dev/zero")]);
    let mut config = create_test_config();
    config.devices = vec![DeviceMapping {
        source: PathBuf::from("/dev/null"),
        destination: PathBuf::from("/dev/null"),
        permissions: "rwm".to_string(),
    }];

    let err = runtime
        .create_spec(&config, "test-id")
        .expect_err("unlisted devices must be rejected");
    assert!(err
        .to_string()
        .contains("device /dev/null is not allowed by runtime.allowed_devices"));
}

#[test]
fn test_spec_device_ownership_follows_security_context() {
    let (mut runtime, _temp) = create_test_runtime();
    runtime.set_device_ownership_from_security_context(true);
    let mut config = create_test_config();
    config.user = Some("1234".to_string());
    config.run_as_group = Some(2345);
    config.devices = vec![DeviceMapping {
        source: PathBuf::from("/dev/null"),
        destination: PathBuf::from("/dev/custom-null"),
        permissions: "rwm".to_string(),
    }];

    let spec = runtime.create_spec(&config, "test-id").unwrap();
    let linux = spec.linux.unwrap();
    let device = linux
        .devices
        .unwrap()
        .into_iter()
        .find(|device| device.path == "/dev/custom-null")
        .expect("device should be present");

    assert_eq!(device.uid, Some(1234));
    assert_eq!(device.gid, Some(2345));
}

#[test]
fn test_spec_with_selinux_and_localhost_seccomp() {
    let (runtime, temp) = create_test_runtime();
    let mut config = create_test_config();
    config.selinux_label = Some("system_u:system_r:container_t:s0".to_string());

    let seccomp_profile_path = temp.path().join("seccomp.json");
    let seccomp_profile = crate::oci::spec::Seccomp {
        default_action: "SCMP_ACT_ALLOW".to_string(),
        default_errno_ret: None,
        architectures: None,
        flags: None,
        listener_path: None,
        listener_metadata: None,
        syscalls: None,
    };
    std::fs::write(
        &seccomp_profile_path,
        serde_json::to_vec(&seccomp_profile).unwrap(),
    )
    .unwrap();
    config.seccomp_profile = Some(SeccompProfile::Localhost(seccomp_profile_path));

    let spec = runtime.create_spec(&config, "test-id").unwrap();
    let process = spec.process.unwrap();
    let linux = spec.linux.unwrap();

    assert_eq!(
        process.selinux_label.as_deref(),
        Some("system_u:system_r:container_t:s0")
    );
    assert_eq!(
        linux.mount_label.as_deref(),
        Some("system_u:system_r:container_t:s0")
    );
    assert!(linux.seccomp.is_some());
}

#[test]
fn test_spec_injects_seccomp_notifier_listener() {
    let (runtime, _temp) = create_test_runtime();
    let mut config = create_test_config();
    config.seccomp_profile = Some(SeccompProfile::RuntimeDefault);
    config.seccomp_notifier = Some(SeccompNotifierConfig {
        listener_path: PathBuf::from("/run/crius/seccomp-notify/container-1"),
        listener_metadata: "container=container-1".to_string(),
        mode: SeccompNotifierMode::Stop,
    });

    let spec = runtime.create_spec(&config, "container-1").unwrap();
    let seccomp = spec
        .linux
        .and_then(|linux| linux.seccomp)
        .expect("seccomp config should be present");

    assert_eq!(
        seccomp.listener_path.as_deref(),
        Some("/run/crius/seccomp-notify/container-1")
    );
    assert_eq!(
        seccomp.listener_metadata.as_deref(),
        Some("container=container-1")
    );
    assert!(seccomp
        .syscalls
        .unwrap_or_default()
        .iter()
        .any(|syscall| syscall.action == "SCMP_ACT_NOTIFY"));
}

#[test]
fn test_prepare_rootfs_from_image_uses_content_store_blob_paths() {
    let (runtime, temp_dir) = create_test_runtime();
    let storage_root = temp_dir.path().join("storage");
    let image_dir = storage_root.join("images").join("sha256:blob-image");
    std::fs::create_dir_all(&image_dir).unwrap();

    let layer_source = temp_dir.path().join("blob-layer-src");
    std::fs::create_dir_all(layer_source.join("usr/bin")).unwrap();
    std::fs::write(layer_source.join("usr/bin/hello"), "blob world").unwrap();
    let tar_path = temp_dir.path().join("blob-layer.tar");
    let status = Command::new("tar")
        .arg("-cf")
        .arg(&tar_path)
        .arg("-C")
        .arg(&layer_source)
        .arg(".")
        .status()
        .unwrap();
    assert!(status.success());
    let tar_bytes = std::fs::read(&tar_path).unwrap();
    let digest = crate::image::content_store::FsContentStore::compute_digest(&tar_bytes);
    let blob_path = storage_root
        .join(crate::image::content_store::FsContentStore::relative_blob_path_for_digest(&digest));
    std::fs::create_dir_all(blob_path.parent().unwrap()).unwrap();
    std::fs::write(&blob_path, &tar_bytes).unwrap();

    std::fs::write(
        image_dir.join("metadata.json"),
        serde_json::json!({
            "id": "sha256:blob-image",
            "repo_tags": ["blob:latest"],
            "stored_layers": [{
                "digest": digest,
                "path": blob_path.strip_prefix(&storage_root).unwrap().display().to_string(),
                "media_type": "application/vnd.oci.image.layer.v1.tar",
                "source_media_type": "application/vnd.oci.image.layer.v1.tar",
                "encrypted": false
            }]
        })
        .to_string(),
    )
    .unwrap();

    let rootfs = temp_dir.path().join("rootfs-blob");
    runtime
        .prepare_rootfs_from_image("blob:latest", &rootfs, "container-blob")
        .unwrap();
    assert_eq!(
        std::fs::read_to_string(rootfs.join("usr/bin/hello")).unwrap(),
        "blob world"
    );
}

#[test]
fn test_spec_with_runtime_default_seccomp_uses_builtin_profile() {
    let (runtime, _) = create_test_runtime();
    let mut config = create_test_config();
    config.seccomp_profile = Some(SeccompProfile::RuntimeDefault);

    let spec = runtime.create_spec(&config, "test-id").unwrap();
    let linux = spec.linux.unwrap();
    let seccomp = linux
        .seccomp
        .expect("runtime/default should produce seccomp");

    assert_eq!(seccomp.default_action, "SCMP_ACT_ALLOW");
    assert!(seccomp
        .syscalls
        .unwrap_or_default()
        .iter()
        .any(|syscall| syscall.names.iter().any(|name| name == "unshare")));
}

#[test]
fn test_cri_to_limits() {
    let resources = LinuxContainerResources {
        cpu_period: 100000,
        cpu_quota: 200000,
        cpu_shares: 1024,
        memory_limit_in_bytes: 1073741824, // 1GB
        oom_score_adj: 0,
        cpuset_cpus: "0-3".to_string(),
        cpuset_mems: "0".to_string(),
        hugepage_limits: vec![],
        unified: HashMap::new(),
        memory_swap_limit_in_bytes: 2147483648, // 2GB
    };

    let limits = RuncRuntime::cri_to_limits(&resources);

    // 验证 CPU 限制
    let cpu = limits.cpu.as_ref().unwrap();
    assert_eq!(cpu.shares, Some(1024));
    assert_eq!(cpu.quota, Some(200000));
    assert_eq!(cpu.period, Some(100000));
    assert_eq!(cpu.cpus.as_deref(), Some("0-3"));
    assert_eq!(cpu.mems.as_deref(), Some("0"));

    // 验证内存限制
    let memory = limits.memory.as_ref().unwrap();
    assert_eq!(memory.limit, Some(1073741824));
    assert_eq!(memory.swap, Some(2147483648));
}

#[test]
fn test_cri_to_limits_zero_values() {
    // 测试零值被过滤（不设置）
    let resources = LinuxContainerResources {
        cpu_period: 0,
        cpu_quota: 0,
        cpu_shares: 0,
        memory_limit_in_bytes: 0,
        oom_score_adj: 0,
        cpuset_cpus: "".to_string(),
        cpuset_mems: "".to_string(),
        hugepage_limits: vec![],
        unified: HashMap::new(),
        memory_swap_limit_in_bytes: 0,
    };

    let limits = RuncRuntime::cri_to_limits(&resources);

    // 零值应该被过滤为 None
    let cpu = limits.cpu.as_ref().unwrap();
    assert_eq!(cpu.shares, None);
    assert_eq!(cpu.quota, None);
    assert_eq!(cpu.period, None);
    assert_eq!(cpu.cpus, None);
    assert_eq!(cpu.mems, None);

    let memory = limits.memory.as_ref().unwrap();
    assert_eq!(memory.limit, None);
    assert_eq!(memory.swap, None);
}

#[test]
fn test_create_spec_applies_container_pids_limit() {
    let (runtime, _temp) = create_test_runtime();
    let mut config = create_test_config();
    config.pids_limit = Some(256);

    let spec = runtime.create_spec(&config, "test-id").unwrap();
    let linux = spec.linux.unwrap();
    let resources = linux.resources.unwrap();

    assert_eq!(resources.pids.as_ref().map(|pids| pids.limit), Some(256));
}

#[test]
fn test_checkpoint_container_passes_configured_criu_path() {
    let temp_dir = tempdir().unwrap();
    let checkpoint_args_path = temp_dir.path().join("checkpoint.args");
    let restore_args_path = temp_dir.path().join("restore.args");
    let runtime_path = write_checkpoint_restore_arg_capture_runtime_script(
        temp_dir.path(),
        &checkpoint_args_path,
        &restore_args_path,
    );
    let root = temp_dir.path().join("containers");
    let mut runtime = RuncRuntime::new(runtime_path, root);
    runtime.set_criu_path(PathBuf::from("/usr/sbin/criu"));

    let image_path = temp_dir.path().join("checkpoint");
    let work_path = temp_dir.path().join("checkpoint-work");
    runtime
        .checkpoint_container("container-1", &image_path, &work_path)
        .unwrap();

    let args = fs::read_to_string(&checkpoint_args_path).unwrap();
    assert!(args.lines().any(|line| line == "--image-path"));
    assert!(args
        .lines()
        .any(|line| line == image_path.to_string_lossy()));
    assert!(args.lines().any(|line| line == "--work-path"));
    assert!(args.lines().any(|line| line == work_path.to_string_lossy()));
    assert!(args.lines().any(|line| line == "--criu"));
    assert!(args.lines().any(|line| line == "/usr/sbin/criu"));
}

#[test]
fn test_stop_container_resumes_paused_container_before_term() {
    let temp_dir = tempdir().unwrap();
    let command_log_path = temp_dir.path().join("stop.log");
    let runtime_path = write_stop_runtime_script(temp_dir.path(), &command_log_path, "paused");
    let runtime = RuncRuntime::new(runtime_path, temp_dir.path().join("containers"));

    runtime.stop_container("container-1", Some(1)).unwrap();

    let log = fs::read_to_string(&command_log_path).unwrap();
    let lines = log.lines().collect::<Vec<_>>();
    assert_eq!(lines, vec!["resume", "kill:TERM", "kill:KILL"]);
}

#[test]
fn test_stop_container_retries_with_sigkill_after_graceful_timeout() {
    let temp_dir = tempdir().unwrap();
    let command_log_path = temp_dir.path().join("stop.log");
    let runtime_path = write_stop_runtime_script(temp_dir.path(), &command_log_path, "running");
    let runtime = RuncRuntime::new(runtime_path, temp_dir.path().join("containers"));

    runtime.stop_container("container-1", Some(1)).unwrap();

    let log = fs::read_to_string(&command_log_path).unwrap();
    let lines = log.lines().collect::<Vec<_>>();
    assert_eq!(lines.first().copied(), Some("kill:TERM"));
    assert!(lines.iter().any(|line| *line == "kill:KILL"));
    let final_state = fs::read_to_string(
        temp_dir
            .path()
            .join("runtime-state")
            .join("container-1.state"),
    )
    .unwrap();
    assert_eq!(final_state.trim(), "stopped");
}

#[test]
fn test_exec_in_container_uses_first_cpu_affinity_when_configured() {
    let temp_dir = tempdir().unwrap();
    let runtime_path = temp_dir.path().join("fake-runtime.sh");
    let affinity_path = temp_dir.path().join("affinity.txt");
    fs::write(
        &runtime_path,
        format!(
            r#"#!/bin/sh
set -eu
cmd="${{1:-}}"
shift || true
case "$cmd" in
  exec)
    while [ "$#" -gt 0 ]; do
      case "$1" in
        -t|-i)
          shift
          ;;
        *)
          break
          ;;
      esac
    done
    shift || true
    awk '/Cpus_allowed_list/ {{print $2}}' /proc/self/status > "{}"
    ;;
  *)
    exit 1
    ;;
esac
"#,
            affinity_path.display()
        ),
    )
    .unwrap();
    fs::set_permissions(&runtime_path, fs::Permissions::from_mode(0o755)).unwrap();

    let root = temp_dir.path().join("containers");
    let mut runtime = RuncRuntime::new(runtime_path, root.clone());
    runtime.set_exec_cpu_affinity("first".to_string());
    let bundle = root.join("container-1");
    fs::create_dir_all(&bundle).unwrap();
    fs::write(
        bundle.join("config.json"),
        serde_json::json!({
            "ociVersion": "1.0.2",
            "linux": {
                "resources": {
                    "cpu": {
                        "cpus": "0-3"
                    }
                }
            }
        })
        .to_string(),
    )
    .unwrap();

    let exit_code = runtime
        .exec_in_container("container-1", &["true".to_string()], false)
        .unwrap();
    assert_eq!(exit_code, 0);
    assert_eq!(fs::read_to_string(&affinity_path).unwrap().trim(), "0");
}

#[test]
fn test_start_container_direct_run_uses_configured_no_pivot_policy() {
    let temp_dir = tempdir().unwrap();
    let args_path = temp_dir.path().join("run.args");
    let runtime_path = write_arg_capture_runtime_script(temp_dir.path(), &args_path);
    let root = temp_dir.path().join("containers");
    let mut runtime = RuncRuntime::new(runtime_path, root.clone());
    runtime.set_no_pivot(true);
    fs::create_dir_all(root.join("container-1")).unwrap();

    runtime.start_container("container-1").unwrap();

    let args = fs::read_to_string(&args_path).unwrap();
    assert!(args.lines().any(|line| line == "--no-pivot"));
}

#[test]
fn test_start_container_direct_run_omits_no_pivot_by_default() {
    let temp_dir = tempdir().unwrap();
    let args_path = temp_dir.path().join("run.args");
    let runtime_path = write_arg_capture_runtime_script(temp_dir.path(), &args_path);
    let root = temp_dir.path().join("containers");
    let runtime = RuncRuntime::new(runtime_path, root.clone());
    fs::create_dir_all(root.join("container-1")).unwrap();

    runtime.start_container("container-1").unwrap();

    let args = fs::read_to_string(&args_path).unwrap();
    assert!(!args.lines().any(|line| line == "--no-pivot"));
}

#[test]
fn test_start_container_direct_run_uses_configured_no_new_keyring_policy() {
    let temp_dir = tempdir().unwrap();
    let args_path = temp_dir.path().join("run.args");
    let runtime_path = write_arg_capture_runtime_script(temp_dir.path(), &args_path);
    let root = temp_dir.path().join("containers");
    let mut runtime = RuncRuntime::new(runtime_path, root.clone());
    runtime.set_no_new_keyring(true);
    fs::create_dir_all(root.join("container-1")).unwrap();

    runtime.start_container("container-1").unwrap();

    let args = fs::read_to_string(&args_path).unwrap();
    assert!(args.lines().any(|line| line == "--no-new-keyring"));
}

#[test]
fn test_start_container_direct_run_includes_runtime_config_path() {
    let temp_dir = tempdir().unwrap();
    let args_path = temp_dir.path().join("run.args");
    let runtime_path = temp_dir.path().join("fake-runtime-config.sh");
    fs::write(
        &runtime_path,
        format!(
            r#"#!/bin/sh
set -eu
printf '%s\n' "$@" > "{}"
if [ "${{1:-}}" = "--config" ]; then
  shift 2
fi
cmd="${{1:-}}"
shift || true
case "$cmd" in
  run|restore)
    exit 0
    ;;
  delete)
    exit 0
    ;;
  *)
    exit 1
    ;;
esac
"#,
            args_path.display()
        ),
    )
    .unwrap();
    fs::set_permissions(&runtime_path, fs::Permissions::from_mode(0o755)).unwrap();
    let root = temp_dir.path().join("containers");
    let mut runtime = RuncRuntime::new(runtime_path, root.clone());
    runtime.set_runtime_config_path(PathBuf::from("/etc/kata/config.toml"));
    fs::create_dir_all(root.join("container-1")).unwrap();

    runtime.start_container("container-1").unwrap();

    let args = fs::read_to_string(&args_path).unwrap();
    assert!(args.lines().any(|line| line == "--config"));
    assert!(args.lines().any(|line| line == "/etc/kata/config.toml"));
}

#[test]
fn test_remove_container_cleans_persistent_rootfs_directory() {
    let temp_dir = tempdir().unwrap();
    let runtime_root = temp_dir.path().join("runtime-root");
    let persistent_root = temp_dir.path().join("persistent-root");
    fs::create_dir_all(&runtime_root).unwrap();
    fs::create_dir_all(&persistent_root).unwrap();

    let args_path = temp_dir.path().join("runtime-args.txt");
    let runtime_path = write_arg_capture_runtime_script(temp_dir.path(), &args_path);
    let runtime = RuncRuntime::new(runtime_path, runtime_root);

    let container_id = "container-1";
    let bundle_path = runtime.bundle_path(container_id);
    fs::create_dir_all(&bundle_path).unwrap();

    let container_root = persistent_root.join("containers").join(container_id);
    let rootfs = container_root.join("rootfs");
    fs::create_dir_all(&rootfs).unwrap();
    fs::write(rootfs.join("marker"), "data").unwrap();

    let spec = Spec {
        oci_version: "1.0.2".to_string(),
        process: None,
        root: Some(Root {
            path: rootfs.display().to_string(),
            readonly: Some(false),
        }),
        hostname: None,
        mounts: None,
        hooks: None,
        linux: None,
        annotations: None,
    };
    runtime
        .save_spec_for_bundle(&spec, runtime.config_path(container_id))
        .unwrap();

    runtime.remove_container(container_id).unwrap();

    assert!(!container_root.exists());
    assert!(!bundle_path.exists());
}

#[test]
fn test_probe_runtime_features_reports_idmap_and_rro_support() {
    let temp_dir = tempdir().unwrap();
    let runtime_path = write_runtime_features_script(
        temp_dir.path(),
        r#"{
  "ociVersionMin": "1.0.0",
  "ociVersionMax": "1.2.0",
  "mountOptions": ["ro", "rro"],
  "linux": {
    "mountExtensions": {
      "idmap": {
        "enabled": true
      }
    }
  }
}"#,
        0,
    );
    let runtime = RuncRuntime::new(runtime_path, temp_dir.path().join("containers"));

    let features = runtime.probe_runtime_features();
    assert!(features.available);
    assert!(features.idmap_mounts);
    assert!(features.recursive_read_only_mounts);
    assert_eq!(features.mount_options, vec!["ro", "rro"]);
    assert_eq!(features.oci_version_min.as_deref(), Some("1.0.0"));
    assert_eq!(features.oci_version_max.as_deref(), Some("1.2.0"));
    assert!(features.error.is_none());
}

#[test]
fn test_probe_runtime_features_reports_invalid_document() {
    let temp_dir = tempdir().unwrap();
    let runtime_path = write_runtime_features_script(temp_dir.path(), "{}", 0);
    let runtime = RuncRuntime::new(runtime_path, temp_dir.path().join("containers"));

    let features = runtime.probe_runtime_features();
    assert!(!features.available);
    assert!(features.error.is_some());
}

#[test]
fn test_validate_mount_requests_rejects_recursive_read_only_without_runtime_support() {
    let temp_dir = tempdir().unwrap();
    let runtime_path = write_runtime_features_script(temp_dir.path(), "{}", 0);
    let runtime = RuncRuntime::new(runtime_path, temp_dir.path().join("containers"));
    let source = temp_dir.path().join("source");
    fs::create_dir_all(&source).unwrap();

    let mut config = create_test_config();
    let mut mount = test_mount_config(source, "/data");
    mount.read_only = true;
    mount.recursive_read_only = true;
    config.mounts = vec![mount];

    let err = runtime.validate_mount_requests(&config).unwrap_err();
    assert!(matches!(
        err,
        MountSemanticsError::RecursiveReadOnlyUnsupported { .. }
    ));
}

#[test]
fn test_validate_mount_requests_rejects_idmapped_mount_without_runtime_support() {
    let temp_dir = tempdir().unwrap();
    let runtime_path = write_runtime_features_script(temp_dir.path(), "{}", 0);
    let runtime = RuncRuntime::new(runtime_path, temp_dir.path().join("containers"));
    let source = temp_dir.path().join("source");
    fs::create_dir_all(&source).unwrap();

    let mut config = create_test_config();
    let mut mount = test_mount_config(source, "/data");
    mount.uid_mappings = vec![crate::oci::spec::IdMapping {
        container_id: 0,
        host_id: 1000,
        size: 1,
    }];
    config.mounts = vec![mount];

    let err = runtime.validate_mount_requests(&config).unwrap_err();
    assert!(matches!(
        err,
        MountSemanticsError::IdmapMountUnsupported { .. }
    ));
}

#[test]
fn test_validate_mount_propagation_from_mountinfo_rejects_private_source_for_bidirectional() {
    let temp_dir = tempdir().unwrap();
    let source = temp_dir.path().join("source");
    fs::create_dir_all(&source).unwrap();
    let mountinfo = format!("1 0 0:1 / {} rw - ext4 /dev/root rw", source.display());

    let err = RuncRuntime::validate_mount_propagation_from_mountinfo(
        &source,
        Path::new("/data"),
        MountPropagationMode::Bidirectional,
        &mountinfo,
    )
    .unwrap_err();
    assert!(matches!(
        err,
        MountSemanticsError::BidirectionalPropagationRequiresShared { .. }
    ));
}

#[test]
fn test_validate_mount_propagation_from_mountinfo_accepts_slave_source_for_host_to_container() {
    let temp_dir = tempdir().unwrap();
    let source = temp_dir.path().join("source");
    fs::create_dir_all(&source).unwrap();
    let mountinfo = format!(
        "1 0 0:1 / {} rw master:1 - ext4 /dev/root rw",
        source.display()
    );

    RuncRuntime::validate_mount_propagation_from_mountinfo(
        &source,
        Path::new("/data"),
        MountPropagationMode::HostToContainer,
        &mountinfo,
    )
    .unwrap();
}

#[test]
fn test_build_mounts_includes_default_mounts_file_entries() {
    let (mut runtime, temp_dir) = create_test_runtime();
    let mounts_path = temp_dir.path().join("mounts.conf");
    let source_dir = temp_dir.path().join("secrets");
    fs::create_dir_all(&source_dir).unwrap();
    fs::write(
        &mounts_path,
        format!("{}:/run/secrets\n", source_dir.display()),
    )
    .unwrap();
    runtime.set_default_mounts_file(mounts_path);

    let mut config = create_test_config();
    config.rootfs = temp_dir.path().join("rootfs");

    let mounts = runtime.build_mounts("test-id", &config, false).unwrap();
    assert!(mounts.iter().any(|mount| {
        mount.destination == "/run/secrets"
            && mount.source.as_deref() == Some(source_dir.to_string_lossy().as_ref())
    }));
}

#[test]
fn test_build_mounts_rejects_configured_absent_mount_sources() {
    let (mut runtime, temp_dir) = create_test_runtime();
    let missing_source = temp_dir.path().join("missing-hostname");
    runtime.set_absent_mount_sources_to_reject(vec![missing_source.clone()]);

    let mut config = create_test_config();
    config.rootfs = temp_dir.path().join("rootfs");
    config.mounts = vec![MountConfig {
        source: missing_source,
        destination: PathBuf::from("/host-etc-hostname"),
        read_only: true,
        missing_source_policy: MissingMountSourcePolicy::Reject,
        selinux_relabel: false,
        propagation: MountPropagationMode::Private,
        recursive_read_only: false,
        uid_mappings: Vec::new(),
        gid_mappings: Vec::new(),
        requested_image: None,
        image_sub_path: None,
    }];

    let err = runtime.build_mounts("test-id", &config, false).unwrap_err();
    assert!(format!("{err}").contains("does not exist"));
}

#[test]
fn test_build_mounts_adds_bind_mounts_for_image_defined_volumes() {
    let (mut runtime, temp_dir) = create_test_runtime();
    runtime.set_image_volumes_mode(ImageVolumesMode::Bind);
    let image_dir = temp_dir
        .path()
        .join("storage")
        .join("images")
        .join("sha256:test-image");
    fs::create_dir_all(&image_dir).unwrap();
    fs::write(
        image_dir.join("metadata.json"),
        serde_json::json!({
            "id": "sha256:test-image",
            "repo_tags": ["busybox:latest"],
            "declared_volumes": ["/cache", "/var/lib/data"]
        })
        .to_string(),
    )
    .unwrap();

    let mut config = create_test_config();
    config.image = "busybox:latest".to_string();
    let mounts = runtime.build_mounts("container-1", &config, false).unwrap();

    assert!(mounts.iter().any(|mount| mount.destination == "/cache"));
    assert!(mounts
        .iter()
        .any(|mount| mount.destination == "/var/lib/data"));
    assert!(runtime
        .image_volume_state_dir("container-1")
        .join("volume-0")
        .exists());
    assert!(runtime
        .image_volume_state_dir("container-1")
        .join("volume-1")
        .exists());
}

#[test]
fn test_build_mounts_skips_bind_image_volume_when_explicit_mount_overrides_destination() {
    let (mut runtime, temp_dir) = create_test_runtime();
    runtime.set_image_volumes_mode(ImageVolumesMode::Bind);
    let image_dir = temp_dir
        .path()
        .join("storage")
        .join("images")
        .join("sha256:test-image");
    fs::create_dir_all(&image_dir).unwrap();
    fs::write(
        image_dir.join("metadata.json"),
        serde_json::json!({
            "id": "sha256:test-image",
            "repo_tags": ["busybox:latest"],
            "declared_volumes": ["/var/lib/data"]
        })
        .to_string(),
    )
    .unwrap();

    let explicit_source = temp_dir.path().join("explicit");
    fs::create_dir_all(&explicit_source).unwrap();
    let mut config = create_test_config();
    config.image = "busybox:latest".to_string();
    config.mounts = vec![MountConfig {
        source: explicit_source.clone(),
        destination: PathBuf::from("/var/lib/data"),
        read_only: false,
        missing_source_policy: MissingMountSourcePolicy::Reject,
        selinux_relabel: false,
        propagation: MountPropagationMode::Private,
        recursive_read_only: false,
        uid_mappings: Vec::new(),
        gid_mappings: Vec::new(),
        requested_image: None,
        image_sub_path: None,
    }];

    let mounts = runtime.build_mounts("container-1", &config, false).unwrap();
    let matching: Vec<_> = mounts
        .iter()
        .filter(|mount| mount.destination == "/var/lib/data")
        .collect();
    assert_eq!(matching.len(), 1);
    assert_eq!(
        matching[0].source.as_deref(),
        Some(explicit_source.to_string_lossy().as_ref())
    );
}

#[test]
fn test_desired_rootfs_propagation_tracks_mount_propagation_requirements() {
    let temp_dir = tempdir().unwrap();
    let source = temp_dir.path().join("source");
    let mut mount = test_mount_config(source, "/data");
    mount.propagation = MountPropagationMode::HostToContainer;

    assert_eq!(
        RuncRuntime::desired_rootfs_propagation(&[mount]),
        Some("rslave")
    );
}

#[test]
fn test_write_bundle_serializes_mount_id_mappings() {
    let temp_dir = tempdir().unwrap();
    let runtime_path = write_runtime_features_script(
        temp_dir.path(),
        r#"{
  "ociVersionMin": "1.0.0",
  "ociVersionMax": "1.2.0",
  "mountOptions": ["ro", "rro"],
  "linux": {
    "mountExtensions": {
      "idmap": {
        "enabled": true
      }
    }
  }
}"#,
        0,
    );
    let runtime = RuncRuntime::new(runtime_path, temp_dir.path().join("containers"));
    let source = temp_dir.path().join("source");
    fs::create_dir_all(&source).unwrap();
    let rootfs = temp_dir.path().join("rootfs");
    fs::create_dir_all(&rootfs).unwrap();

    let mut config = create_test_config();
    config.rootfs = rootfs.clone();
    let mut mount = test_mount_config(source, "/data");
    mount.uid_mappings = vec![crate::oci::spec::IdMapping {
        container_id: 0,
        host_id: 1000,
        size: 1,
    }];
    mount.gid_mappings = vec![crate::oci::spec::IdMapping {
        container_id: 0,
        host_id: 2000,
        size: 1,
    }];
    config.mounts = vec![mount];

    let spec = runtime.create_spec(&config, "test-id").unwrap();
    runtime.write_bundle("test-id", &rootfs, &spec).unwrap();

    let raw = fs::read_to_string(runtime.config_path("test-id")).unwrap();
    let saved: serde_json::Value = serde_json::from_str(&raw).unwrap();
    let mount = saved["mounts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["destination"] == "/data")
        .unwrap();
    assert_eq!(mount["uidMappings"].as_array().unwrap().len(), 1);
    assert_eq!(mount["gidMappings"].as_array().unwrap().len(), 1);
    assert!(!mount["options"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(serde_json::Value::as_str)
        .any(
            |option| option.starts_with(INTERNAL_UID_MAPPINGS_MOUNT_OPTION_PREFIX)
                || option.starts_with(INTERNAL_GID_MAPPINGS_MOUNT_OPTION_PREFIX)
        ));
}

#[test]
fn test_write_bundle_persists_runtime_artifacts_when_ledger_enabled() {
    let temp_dir = tempdir().unwrap();
    let runtime_path = write_runtime_features_script(
        temp_dir.path(),
        r#"{
  "ociVersionMin": "1.0.0",
  "ociVersionMax": "1.2.0",
  "mountOptions": ["ro"],
  "linux": { "mountExtensions": { "idmap": { "enabled": true } } }
}"#,
        0,
    );
    let mut runtime = RuncRuntime::new(runtime_path, temp_dir.path().join("containers"));
    let db_path = temp_dir.path().join("crius.db");
    runtime.set_state_db_path(db_path.clone());
    let rootfs = temp_dir.path().join("rootfs");
    fs::create_dir_all(&rootfs).unwrap();
    let mut spec = Spec::new("1.0.2");
    spec.root = Some(Root {
        path: rootfs.display().to_string(),
        readonly: Some(false),
    });

    runtime.write_bundle("ledger-test", &rootfs, &spec).unwrap();

    let storage = crate::storage::StorageManager::new(db_path).unwrap();
    let artifacts = storage.list_runtime_artifacts().unwrap();
    assert!(artifacts.iter().any(|artifact| {
        artifact.owner_kind == "container"
            && artifact.owner_id == "ledger-test"
            && artifact.artifact_kind == "bundle"
    }));
    assert!(artifacts.iter().any(|artifact| {
        artifact.owner_kind == "container"
            && artifact.owner_id == "ledger-test"
            && artifact.artifact_kind == "rootfs"
    }));
}

#[test]
fn test_prepare_rootfs_creates_image_defined_volume_directories_for_mkdir() {
    let (runtime, temp_dir) = create_test_runtime();
    let image_dir = temp_dir
        .path()
        .join("storage")
        .join("images")
        .join("sha256:test-image");
    fs::create_dir_all(&image_dir).unwrap();
    fs::write(
        image_dir.join("metadata.json"),
        serde_json::json!({
            "id": "sha256:test-image",
            "repo_tags": ["busybox:latest"],
            "declared_volumes": ["/cache", "/var/lib/data"]
        })
        .to_string(),
    )
    .unwrap();

    let rootfs = temp_dir.path().join("rootfs");
    runtime
        .ensure_image_volume_directories("busybox:latest", &rootfs)
        .unwrap();

    assert!(rootfs.join("cache").is_dir());
    assert!(rootfs.join("var/lib/data").is_dir());
}

#[test]
fn test_restore_container_uses_configured_no_pivot_policy() {
    let temp_dir = tempdir().unwrap();
    let checkpoint_args_path = temp_dir.path().join("checkpoint.args");
    let args_path = temp_dir.path().join("restore.args");
    let runtime_path = write_checkpoint_restore_arg_capture_runtime_script(
        temp_dir.path(),
        &checkpoint_args_path,
        &args_path,
    );
    let root = temp_dir.path().join("containers");
    let mut runtime = RuncRuntime::new(runtime_path, root.clone());
    runtime.set_no_pivot(true);
    runtime.set_criu_path(PathBuf::from("/usr/sbin/criu"));
    fs::create_dir_all(root.join("container-1")).unwrap();
    let image_path = temp_dir.path().join("checkpoint");
    let work_path = temp_dir.path().join("checkpoint-work");
    fs::create_dir_all(&image_path).unwrap();

    runtime
        .restore_container_from_checkpoint("container-1", &image_path, &work_path)
        .unwrap();

    let args = fs::read_to_string(&args_path).unwrap();
    assert!(args.lines().any(|line| line == "--image-path"));
    assert!(args
        .lines()
        .any(|line| line == image_path.to_string_lossy()));
    assert!(args.lines().any(|line| line == "--work-path"));
    assert!(args.lines().any(|line| line == work_path.to_string_lossy()));
    assert!(args.lines().any(|line| line == "--no-pivot"));
    assert!(args.lines().any(|line| line == "--criu"));
    assert!(args.lines().any(|line| line == "/usr/sbin/criu"));
}

#[test]
fn test_restore_container_replaces_rootfs_from_snapshot() {
    let temp_dir = tempdir().unwrap();
    let checkpoint_args_path = temp_dir.path().join("checkpoint.args");
    let restore_args_path = temp_dir.path().join("restore.args");
    let runtime_path = write_checkpoint_restore_arg_capture_runtime_script(
        temp_dir.path(),
        &checkpoint_args_path,
        &restore_args_path,
    );
    let root = temp_dir.path().join("containers");
    let runtime = RuncRuntime::new(runtime_path, root.clone());
    let bundle = root.join("container-1");
    let rootfs = temp_dir.path().join("rootfs-live");
    fs::create_dir_all(&rootfs).unwrap();
    fs::write(rootfs.join("stale.txt"), "stale").unwrap();
    fs::create_dir_all(&bundle).unwrap();
    fs::write(
        bundle.join("config.json"),
        serde_json::json!({
            "ociVersion": "1.0.2",
            "root": { "path": rootfs.display().to_string() }
        })
        .to_string(),
    )
    .unwrap();

    let snapshot_src = temp_dir.path().join("rootfs-snapshot");
    fs::create_dir_all(&snapshot_src).unwrap();
    fs::write(snapshot_src.join("restored.txt"), "restored").unwrap();
    let image_path = temp_dir.path().join("checkpoint");
    let work_path = temp_dir.path().join("checkpoint-work");
    fs::create_dir_all(&image_path).unwrap();
    let tar_status = Command::new("tar")
        .args([
            "-cf",
            image_path.join("rootfs.tar").to_str().unwrap(),
            "-C",
            snapshot_src.to_str().unwrap(),
            ".",
        ])
        .status()
        .unwrap();
    assert!(tar_status.success());

    runtime
        .restore_container_from_checkpoint("container-1", &image_path, &work_path)
        .unwrap();

    assert!(!rootfs.join("stale.txt").exists());
    assert_eq!(
        fs::read_to_string(rootfs.join("restored.txt")).unwrap(),
        "restored"
    );
}

#[test]
fn test_restore_container_keeps_existing_rootfs_when_snapshot_absent() {
    let temp_dir = tempdir().unwrap();
    let checkpoint_args_path = temp_dir.path().join("checkpoint.args");
    let restore_args_path = temp_dir.path().join("restore.args");
    let runtime_path = write_checkpoint_restore_arg_capture_runtime_script(
        temp_dir.path(),
        &checkpoint_args_path,
        &restore_args_path,
    );
    let root = temp_dir.path().join("containers");
    let runtime = RuncRuntime::new(runtime_path, root.clone());
    let bundle = root.join("container-1");
    let rootfs = temp_dir.path().join("rootfs-live");
    fs::create_dir_all(&rootfs).unwrap();
    fs::write(rootfs.join("keep.txt"), "keep").unwrap();
    fs::create_dir_all(&bundle).unwrap();
    fs::write(
        bundle.join("config.json"),
        serde_json::json!({
            "ociVersion": "1.0.2",
            "root": { "path": rootfs.display().to_string() }
        })
        .to_string(),
    )
    .unwrap();

    let image_path = temp_dir.path().join("checkpoint");
    let work_path = temp_dir.path().join("checkpoint-work");
    fs::create_dir_all(&image_path).unwrap();

    runtime
        .restore_container_from_checkpoint("container-1", &image_path, &work_path)
        .unwrap();

    assert_eq!(fs::read_to_string(rootfs.join("keep.txt")).unwrap(), "keep");
}

#[test]
fn test_reopen_container_log_reports_missing_socket() {
    let temp_dir = tempdir().unwrap();
    let shim_dir = temp_dir.path().join("shims");
    let container_id = "container-1";
    let container_shim_dir = shim_dir.join(container_id);
    fs::create_dir_all(&container_shim_dir).unwrap();

    let runtime = RuncRuntime::with_shim(
        PathBuf::from("runc"),
        temp_dir.path().join("containers"),
        ShimConfig {
            work_dir: shim_dir,
            attach_socket_dir: temp_dir.path().join("attach"),
            container_exits_dir: temp_dir.path().join("exits"),
            ..Default::default()
        },
    );

    let err = runtime.reopen_container_log(container_id).unwrap_err();
    assert!(matches!(
        err.downcast_ref::<LogReopenError>(),
        Some(LogReopenError::MissingSocket { .. })
    ));
}

#[test]
fn test_shim_container_status_does_not_fallback_to_runc_state() {
    let temp_dir = tempdir().unwrap();
    let command_log_path = temp_dir.path().join("runtime.commands");
    let runtime_path = write_stop_runtime_script(temp_dir.path(), &command_log_path, "running");
    let runtime = RuncRuntime::with_shim(
        runtime_path,
        temp_dir.path().join("containers"),
        ShimConfig {
            work_dir: temp_dir.path().join("shims"),
            attach_socket_dir: temp_dir.path().join("attach"),
            container_exits_dir: temp_dir.path().join("exits"),
            ..Default::default()
        },
    );

    let err = runtime.container_status("container-1").unwrap_err();
    assert!(
        err.to_string().contains("failed to connect to shim RPC"),
        "shim-enabled status must fail through shim instead of falling back to runc: {err}"
    );
    assert!(
        !command_log_path.exists(),
        "runc state should not be queried when shim owns task lifecycle"
    );
}

#[test]
fn test_shim_start_container_does_not_query_runc_state() {
    struct StartTaskHandler;

    impl crate::shim_rpc::server::ShimRpcHandler for StartTaskHandler {
        fn handle_request(
            &self,
            request: crate::shim_rpc::ShimRpcRequest,
        ) -> anyhow::Result<crate::shim_rpc::ShimRpcResponse> {
            match request {
                crate::shim_rpc::ShimRpcRequest::Ping => {
                    Ok(crate::shim_rpc::ShimRpcResponse::Empty)
                }
                crate::shim_rpc::ShimRpcRequest::StartTask(_) => {
                    Ok(crate::shim_rpc::ShimRpcResponse::Empty)
                }
                other => anyhow::bail!("unexpected request: {:?}", other),
            }
        }
    }

    let temp_dir = tempdir().unwrap();
    let container_id = "container-1";
    let shim_dir = temp_dir.path().join("shims");
    let runtime = RuncRuntime::with_shim(
        write_runtime_script_that_fails_state(temp_dir.path()),
        temp_dir.path().join("containers"),
        ShimConfig {
            work_dir: shim_dir.clone(),
            attach_socket_dir: temp_dir.path().join("attach"),
            container_exits_dir: temp_dir.path().join("exits"),
            ..Default::default()
        },
    );
    let socket_path = crate::shim_rpc::default_task_socket_path(&shim_dir, container_id);
    let running = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
    let running_for_thread = running.clone();
    let socket_for_thread = socket_path.clone();
    let handle = std::thread::spawn(move || {
        crate::shim_rpc::server::serve(
            &socket_for_thread,
            running_for_thread,
            std::sync::Arc::new(StartTaskHandler),
        )
        .unwrap();
    });

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while !socket_path.exists() {
        if std::time::Instant::now() >= deadline {
            panic!("shim RPC socket was not created before deadline");
        }
        std::thread::sleep(std::time::Duration::from_millis(25));
    }

    runtime.start_container(container_id).unwrap();

    running.store(false, std::sync::atomic::Ordering::Relaxed);
    let _ = std::os::unix::net::UnixStream::connect(&socket_path);
    handle.join().unwrap();
}

#[test]
fn test_shim_target_namespace_path_uses_shim_pid() {
    struct PidHandler;

    impl crate::shim_rpc::server::ShimRpcHandler for PidHandler {
        fn handle_request(
            &self,
            request: crate::shim_rpc::ShimRpcRequest,
        ) -> anyhow::Result<crate::shim_rpc::ShimRpcResponse> {
            match request {
                crate::shim_rpc::ShimRpcRequest::ContainerPid(_) => {
                    Ok(crate::shim_rpc::ShimRpcResponse::ContainerPid(Some(4242)))
                }
                other => anyhow::bail!("unexpected request: {:?}", other),
            }
        }
    }

    let temp_dir = tempdir().unwrap();
    let container_id = "container-1";
    let shim_dir = temp_dir.path().join("shims");
    let runtime = RuncRuntime::with_shim(
        write_runtime_script_that_fails_state(temp_dir.path()),
        temp_dir.path().join("containers"),
        ShimConfig {
            work_dir: shim_dir.clone(),
            attach_socket_dir: temp_dir.path().join("attach"),
            container_exits_dir: temp_dir.path().join("exits"),
            ..Default::default()
        },
    );
    let socket_path = crate::shim_rpc::default_task_socket_path(&shim_dir, container_id);
    let running = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
    let running_for_thread = running.clone();
    let socket_for_thread = socket_path.clone();
    let handle = std::thread::spawn(move || {
        crate::shim_rpc::server::serve(
            &socket_for_thread,
            running_for_thread,
            std::sync::Arc::new(PidHandler),
        )
        .unwrap();
    });

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while !socket_path.exists() {
        if std::time::Instant::now() >= deadline {
            panic!("shim RPC socket was not created before deadline");
        }
        std::thread::sleep(std::time::Duration::from_millis(25));
    }

    assert_eq!(
        runtime
            .target_namespace_path(container_id, "pid")
            .as_deref(),
        Some("/proc/4242/ns/pid")
    );

    running.store(false, std::sync::atomic::Ordering::Relaxed);
    let _ = std::os::unix::net::UnixStream::connect(&socket_path);
    handle.join().unwrap();
}
