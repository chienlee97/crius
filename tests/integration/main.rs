//! 集成测试
//!
//! 本文件统一承载两类测试：
//! 1. 系统级集成测试：直接依赖 `runc`、rootfs、root 权限。
//! 2. 跨模块集成测试：验证 `storage` / 持久化恢复 / pod-container 关联流程。

use crius::storage::{ContainerRecord, PodSandboxRecord, StorageManager};

fn temp_dir() -> tempfile::TempDir {
    tempfile::tempdir().unwrap()
}

#[path = "../common/mod.rs"]
mod common;

mod rootless_lifecycle {
    use std::collections::HashMap;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::time::Duration;

    use crius::config::ResolvedRuntimeHandlerConfig;
    use crius::image::{metadata_store::FilesystemImageMetadataStore, CriusImage};
    use crius::network::CniConfig;
    use crius::proto::runtime::v1::{
        runtime_service_server::RuntimeService, ContainerConfig, ContainerMetadata,
        CreateContainerRequest, ImageSpec, LinuxPodSandboxConfig, LinuxSandboxSecurityContext,
        NamespaceMode, NamespaceOption, PodSandboxConfig, PodSandboxMetadata,
        RemovePodSandboxRequest, RunPodSandboxRequest, StartContainerRequest, StopContainerRequest,
    };
    use crius::rootless::{EffectiveRootlessConfig, NetworkMode, RootlessConfig};
    use crius::runtime::ShimConfig;
    use crius::server::{RuntimeConfig, RuntimeServiceImpl};
    use tonic::Request;

    use crate::common::{FakeRuntimeBackend, FakeRuntimeCall};

    fn runtime_handler(runtime_root: &Path, runtime_path: &Path) -> ResolvedRuntimeHandlerConfig {
        ResolvedRuntimeHandlerConfig {
            backend: "fake".to_string(),
            backend_options: HashMap::new(),
            runtime_path: runtime_path.display().to_string(),
            runtime_config_path: String::new(),
            runtime_root: runtime_root.display().to_string(),
            platform_runtime_paths: HashMap::new(),
            monitor_path: "/missing/crius-shim".to_string(),
            monitor_cgroup: String::new(),
            monitor_env: Vec::new(),
            stream_websockets: false,
            allowed_annotations: Vec::new(),
            default_annotations: HashMap::new(),
            privileged_without_host_devices: false,
            privileged_without_host_devices_all_devices_allowed: false,
            container_create_timeout: 30,
            snapshotter: "internal-overlay-untar".to_string(),
        }
    }

    fn rootless_runtime_config(
        temp: &Path,
        rootless: EffectiveRootlessConfig,
        namespace_helper_path: Option<PathBuf>,
    ) -> RuntimeConfig {
        let root_dir = temp.join("state");
        let runtime_root = rootless.runtime_root.join("containers");
        let runtime_path = temp.join("missing-runc");
        let image_root = rootless.storage_root.clone();
        let attach_socket_dir = rootless.runtime_root.join("attach");
        let exits_dir = rootless.runtime_root.join("exits");
        let shim_work_dir = rootless.runtime_root.join("shims");
        let mut cni_config = CniConfig::default();
        cni_config.set_rootless_config(Some(rootless.clone()));
        cni_config.set_netns_mount_dir(rootless.netns_dir.clone());
        cni_config.set_namespace_helper_path(namespace_helper_path);

        RuntimeConfig {
            root_dir: root_dir.clone(),
            runtime: "runc".to_string(),
            runtime_handlers: vec!["runc".to_string()],
            runtime_configs: HashMap::from([(
                "runc".to_string(),
                runtime_handler(&runtime_root, &runtime_path),
            )]),
            runtime_root: runtime_root.clone(),
            runtime_path: runtime_path.clone(),
            image_root: image_root.clone(),
            attach_socket_dir: attach_socket_dir.clone(),
            container_exits_dir: exits_dir.clone(),
            clean_shutdown_file: root_dir.join("clean.shutdown"),
            version_file: rootless.runtime_root.join("version"),
            version_file_persist: root_dir.join("version"),
            disable_cgroup: rootless.disable_cgroup,
            tolerate_missing_hugetlb_controller: rootless.tolerate_missing_hugetlb_controller,
            cni_config,
            cgroup_driver: Some(crius::proto::runtime::v1::CgroupDriver::Cgroupfs),
            rootless: rootless.clone(),
            shim: ShimConfig {
                shim_path: PathBuf::from("/missing/crius-shim"),
                runtime_config_path: PathBuf::new(),
                monitor_cgroup: String::new(),
                work_dir: shim_work_dir,
                attach_socket_dir,
                container_exits_dir: exits_dir,
                io_uid: 0,
                io_gid: 0,
                monitor_env: Vec::new(),
                debug: false,
                log_to_journald: false,
                no_sync_log: false,
                no_pivot: false,
                no_new_keyring: false,
                systemd_cgroup: false,
                runtime_path,
                max_container_log_line_size: 4096,
                state_db_path: root_dir.join("crius.db"),
            },
            ..RuntimeConfig::default()
        }
    }

    fn pod_request(name: &str, network_mode: NamespaceMode) -> RunPodSandboxRequest {
        RunPodSandboxRequest {
            config: Some(PodSandboxConfig {
                metadata: Some(PodSandboxMetadata {
                    name: name.to_string(),
                    uid: format!("{name}-uid"),
                    namespace: "default".to_string(),
                    attempt: 1,
                }),
                hostname: name.to_string(),
                linux: Some(LinuxPodSandboxConfig {
                    security_context: Some(LinuxSandboxSecurityContext {
                        namespace_options: Some(NamespaceOption {
                            network: network_mode as i32,
                            pid: NamespaceMode::Pod as i32,
                            ipc: NamespaceMode::Pod as i32,
                            target_id: String::new(),
                            userns_options: None,
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            runtime_handler: String::new(),
        }
    }

    fn container_request(pod_id: &str, sandbox_config: PodSandboxConfig) -> CreateContainerRequest {
        CreateContainerRequest {
            pod_sandbox_id: pod_id.to_string(),
            config: Some(ContainerConfig {
                metadata: Some(ContainerMetadata {
                    name: "workload".to_string(),
                    attempt: 1,
                }),
                image: Some(ImageSpec {
                    image: "busybox:latest".to_string(),
                    user_specified_image: "busybox:latest".to_string(),
                    ..Default::default()
                }),
                command: vec!["sleep".to_string()],
                args: vec!["1".to_string()],
                ..Default::default()
            }),
            sandbox_config: Some(sandbox_config),
        }
    }

    fn seed_pause_image(storage_root: &Path, db_path: &Path) {
        FilesystemImageMetadataStore::new(storage_root, Vec::new(), Some(db_path.to_path_buf()))
            .save(&CriusImage {
                id: "sha256:rootless-pause".to_string(),
                repo_tags: vec!["registry.k8s.io/pause:3.9".to_string()],
                size: 64,
                pinned: true,
                ..Default::default()
            })
            .unwrap();
    }

    fn make_executable(path: &Path) {
        let mut perms = fs::metadata(path).unwrap().permissions();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            perms.set_mode(0o755);
        }
        fs::set_permissions(path, perms).unwrap();
    }

    fn write_namespace_helper(path: &Path, args_path: &Path) {
        fs::write(
            path,
            format!(
                r#"#!/bin/sh
set -eu
dir=""
name=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    -d)
      dir="$2"
      shift 2
      ;;
    -f)
      name="$2"
      shift 2
      ;;
    --net)
      shift
      ;;
    *)
      shift
      ;;
  esac
done
printf '%s\n%s\n' "$dir" "$name" > "{}"
mkdir -p "$dir/netns"
: > "$dir/netns/$name"
"#,
                args_path.display()
            ),
        )
        .unwrap();
        make_executable(path);
    }

    fn write_rootless_network_helper(path: &Path, args_path: &Path, signal_path: &Path) {
        fs::write(
            path,
            format!(
                r#"#!/bin/sh
set -eu
printf '%s\n' "$@" > "{}"
trap 'printf terminated > "{}"; exit 0' TERM INT
while true; do sleep 1; done
"#,
                args_path.display(),
                signal_path.display()
            ),
        )
        .unwrap();
        make_executable(path);
    }

    async fn wait_for_file(path: &Path) {
        for _ in 0..50 {
            if path.exists() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("timed out waiting for {}", path.display());
    }

    #[tokio::test]
    async fn rootless_minimal_lifecycle_uses_xdg_paths() {
        let temp = tempfile::tempdir().unwrap();
        let xdg_runtime_dir = temp.path().join("xdg-runtime");
        let xdg_data_home = temp.path().join("xdg-data");
        let rootless = EffectiveRootlessConfig::resolve(&RootlessConfig {
            enabled: true,
            xdg_runtime_dir: xdg_runtime_dir.display().to_string(),
            xdg_data_home: xdg_data_home.display().to_string(),
            network_mode: NetworkMode::None,
            ..Default::default()
        })
        .unwrap();
        let runtime_root = rootless.runtime_root.join("containers");
        let fake_backend = Arc::new(FakeRuntimeBackend::new(&runtime_root));
        seed_pause_image(&rootless.storage_root, &temp.path().join("state/crius.db"));
        let service = RuntimeServiceImpl::new_with_runtime_backends(
            rootless_runtime_config(temp.path(), rootless.clone(), None),
            HashMap::from([(
                "runc".to_string(),
                fake_backend.clone() as Arc<dyn crius::runtime::RuntimeBackend>,
            )]),
        );

        let sandbox_config = pod_request("rootless-life", NamespaceMode::Node)
            .config
            .unwrap();
        let pod_id = RuntimeService::run_pod_sandbox(
            &service,
            Request::new(RunPodSandboxRequest {
                config: Some(sandbox_config.clone()),
                runtime_handler: String::new(),
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .pod_sandbox_id;
        assert!(!pod_id.is_empty());

        let container_id = RuntimeService::create_container(
            &service,
            Request::new(container_request(&pod_id, sandbox_config)),
        )
        .await
        .unwrap()
        .into_inner()
        .container_id;
        RuntimeService::start_container(
            &service,
            Request::new(StartContainerRequest {
                container_id: container_id.clone(),
            }),
        )
        .await
        .unwrap();
        RuntimeService::stop_container(
            &service,
            Request::new(StopContainerRequest {
                container_id: container_id.clone(),
                timeout: 0,
            }),
        )
        .await
        .unwrap();

        let image_usage = service.image_service().metrics_provider().snapshot().await;
        assert!(image_usage.image_fs_bytes_used > 0);
        assert!(rootless.storage_root.starts_with(&xdg_data_home));
        assert!(rootless.runtime_root.starts_with(&xdg_runtime_dir));
        assert!(rootless.netns_dir.starts_with(&xdg_runtime_dir));
        assert!(!rootless.storage_root.starts_with("/var/lib"));
        assert!(!rootless.runtime_root.starts_with("/run/crius"));
        assert!(!rootless.netns_dir.starts_with("/var/run/netns"));

        let calls = fake_backend.calls();
        assert!(calls.contains(&FakeRuntimeCall::CreateContainer {
            container_id: format!("pause-{pod_id}")
        }));
        assert!(calls.contains(&FakeRuntimeCall::PrepareRootfs {
            container_id: container_id.clone()
        }));
        assert!(calls.contains(&FakeRuntimeCall::BuildSpec {
            container_id: container_id.clone()
        }));
        assert!(calls.iter().any(|call| matches!(
            call,
            FakeRuntimeCall::WriteBundle {
                container_id: id,
                rootfs
            } if id == &container_id && rootfs.starts_with(temp.path().join("state/containers"))
        )));
        assert!(calls.contains(&FakeRuntimeCall::StartContainer {
            container_id: container_id.clone()
        }));
        assert!(calls.contains(&FakeRuntimeCall::StopContainer {
            container_id,
            timeout: Some(30)
        }));
    }

    #[tokio::test]
    async fn rootless_slirp4netns_lifecycle_starts_and_stops_helper_under_xdg() {
        let temp = tempfile::tempdir().unwrap();
        let xdg_runtime_dir = temp.path().join("xdg-runtime");
        let xdg_data_home = temp.path().join("xdg-data");
        let helper_dir = temp.path().join("helpers");
        fs::create_dir_all(&helper_dir).unwrap();
        let namespace_helper = helper_dir.join("pinns");
        let namespace_args = temp.path().join("namespace.args");
        let slirp_helper = helper_dir.join("slirp4netns");
        let slirp_args = temp.path().join("slirp.args");
        let slirp_signal = temp.path().join("slirp.signal");
        write_namespace_helper(&namespace_helper, &namespace_args);
        write_rootless_network_helper(&slirp_helper, &slirp_args, &slirp_signal);

        let rootless = EffectiveRootlessConfig::resolve(&RootlessConfig {
            enabled: true,
            xdg_runtime_dir: xdg_runtime_dir.display().to_string(),
            xdg_data_home: xdg_data_home.display().to_string(),
            network_mode: NetworkMode::Slirp4netns,
            slirp4netns_path: slirp_helper.display().to_string(),
            ..Default::default()
        })
        .unwrap();
        let runtime_root = rootless.runtime_root.join("containers");
        let fake_backend = Arc::new(FakeRuntimeBackend::new(&runtime_root));
        seed_pause_image(&rootless.storage_root, &temp.path().join("state/crius.db"));
        let service = RuntimeServiceImpl::new_with_runtime_backends(
            rootless_runtime_config(
                temp.path(),
                rootless.clone(),
                Some(namespace_helper.clone()),
            ),
            HashMap::from([(
                "runc".to_string(),
                fake_backend.clone() as Arc<dyn crius::runtime::RuntimeBackend>,
            )]),
        );

        let sandbox_config = pod_request("rootless-slirp", NamespaceMode::Pod)
            .config
            .unwrap();
        let pod_id = RuntimeService::run_pod_sandbox(
            &service,
            Request::new(RunPodSandboxRequest {
                config: Some(sandbox_config.clone()),
                runtime_handler: String::new(),
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .pod_sandbox_id;
        assert!(!pod_id.is_empty());
        wait_for_file(&slirp_args).await;

        let container_id = RuntimeService::create_container(
            &service,
            Request::new(container_request(&pod_id, sandbox_config)),
        )
        .await
        .unwrap()
        .into_inner()
        .container_id;
        RuntimeService::start_container(
            &service,
            Request::new(StartContainerRequest {
                container_id: container_id.clone(),
            }),
        )
        .await
        .unwrap();
        RuntimeService::stop_container(
            &service,
            Request::new(StopContainerRequest {
                container_id: container_id.clone(),
                timeout: 0,
            }),
        )
        .await
        .unwrap();
        RuntimeService::remove_pod_sandbox(
            &service,
            Request::new(RemovePodSandboxRequest {
                pod_sandbox_id: pod_id.clone(),
            }),
        )
        .await
        .unwrap();
        wait_for_file(&slirp_signal).await;

        let netns_name = "crius-default-rootless-slirp";
        assert_eq!(
            fs::read_to_string(namespace_args).unwrap(),
            format!("{}\n{}\n", rootless.runtime_root.display(), netns_name)
        );
        let slirp_args = fs::read_to_string(slirp_args).unwrap();
        let netns_path = rootless.netns_dir.join(netns_name);
        assert!(slirp_args.contains("--netns-type=path"));
        assert!(slirp_args.contains("--disable-host-loopback"));
        assert!(slirp_args.contains(&netns_path.display().to_string()));
        assert!(netns_path.starts_with(&xdg_runtime_dir));
        assert!(rootless.storage_root.starts_with(&xdg_data_home));
        assert!(!rootless.netns_dir.starts_with("/var/run/netns"));

        let calls = fake_backend.calls();
        assert!(calls.contains(&FakeRuntimeCall::CreateContainer {
            container_id: format!("pause-{pod_id}")
        }));
        assert!(calls.contains(&FakeRuntimeCall::StartContainer { container_id }));
    }

    #[tokio::test]
    async fn rootless_pasta_lifecycle_starts_and_stops_helper_under_xdg() {
        let temp = tempfile::tempdir().unwrap();
        let xdg_runtime_dir = temp.path().join("xdg-runtime");
        let xdg_data_home = temp.path().join("xdg-data");
        let helper_dir = temp.path().join("helpers");
        fs::create_dir_all(&helper_dir).unwrap();
        let namespace_helper = helper_dir.join("pinns");
        let namespace_args = temp.path().join("namespace.args");
        let pasta_helper = helper_dir.join("pasta");
        let pasta_args = temp.path().join("pasta.args");
        let pasta_signal = temp.path().join("pasta.signal");
        write_namespace_helper(&namespace_helper, &namespace_args);
        write_rootless_network_helper(&pasta_helper, &pasta_args, &pasta_signal);

        let rootless = EffectiveRootlessConfig::resolve(&RootlessConfig {
            enabled: true,
            xdg_runtime_dir: xdg_runtime_dir.display().to_string(),
            xdg_data_home: xdg_data_home.display().to_string(),
            network_mode: NetworkMode::Pasta,
            pasta_path: pasta_helper.display().to_string(),
            ..Default::default()
        })
        .unwrap();
        let runtime_root = rootless.runtime_root.join("containers");
        let fake_backend = Arc::new(FakeRuntimeBackend::new(&runtime_root));
        seed_pause_image(&rootless.storage_root, &temp.path().join("state/crius.db"));
        let service = RuntimeServiceImpl::new_with_runtime_backends(
            rootless_runtime_config(
                temp.path(),
                rootless.clone(),
                Some(namespace_helper.clone()),
            ),
            HashMap::from([(
                "runc".to_string(),
                fake_backend.clone() as Arc<dyn crius::runtime::RuntimeBackend>,
            )]),
        );

        let sandbox_config = pod_request("rootless-pasta", NamespaceMode::Pod)
            .config
            .unwrap();
        let pod_id = RuntimeService::run_pod_sandbox(
            &service,
            Request::new(RunPodSandboxRequest {
                config: Some(sandbox_config.clone()),
                runtime_handler: String::new(),
            }),
        )
        .await
        .unwrap()
        .into_inner()
        .pod_sandbox_id;
        assert!(!pod_id.is_empty());
        wait_for_file(&pasta_args).await;

        let container_id = RuntimeService::create_container(
            &service,
            Request::new(container_request(&pod_id, sandbox_config)),
        )
        .await
        .unwrap()
        .into_inner()
        .container_id;
        RuntimeService::start_container(
            &service,
            Request::new(StartContainerRequest {
                container_id: container_id.clone(),
            }),
        )
        .await
        .unwrap();
        RuntimeService::remove_pod_sandbox(
            &service,
            Request::new(RemovePodSandboxRequest {
                pod_sandbox_id: pod_id.clone(),
            }),
        )
        .await
        .unwrap();
        wait_for_file(&pasta_signal).await;

        let netns_name = "crius-default-rootless-pasta";
        assert_eq!(
            fs::read_to_string(namespace_args).unwrap(),
            format!("{}\n{}\n", rootless.runtime_root.display(), netns_name)
        );
        let pasta_args = fs::read_to_string(pasta_args).unwrap();
        let netns_path = rootless.netns_dir.join(netns_name);
        assert!(pasta_args.contains("--netns"));
        assert!(pasta_args.contains("--config-net"));
        assert!(pasta_args.contains("--quiet"));
        assert!(pasta_args.contains(&netns_path.display().to_string()));
        assert!(netns_path.starts_with(&xdg_runtime_dir));
        assert!(rootless.storage_root.starts_with(&xdg_data_home));
        assert!(!rootless.netns_dir.starts_with("/var/run/netns"));

        let calls = fake_backend.calls();
        assert!(calls.contains(&FakeRuntimeCall::CreateContainer {
            container_id: format!("pause-{pod_id}")
        }));
        assert!(calls.contains(&FakeRuntimeCall::StartContainer { container_id }));
    }
}

mod reload_watcher {
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::time::Duration;

    use crius::config::ResolvedRuntimeHandlerConfig;
    use crius::network::CniConfig;
    use crius::runtime::ShimConfig;
    use crius::server::{RuntimeConfig, RuntimeReloadWatcherStatus, RuntimeServiceImpl};

    fn runtime_handler(runtime_root: &Path, runtime_path: &Path) -> ResolvedRuntimeHandlerConfig {
        ResolvedRuntimeHandlerConfig {
            backend: "runc".to_string(),
            backend_options: HashMap::new(),
            runtime_path: runtime_path.display().to_string(),
            runtime_config_path: String::new(),
            runtime_root: runtime_root.display().to_string(),
            platform_runtime_paths: HashMap::new(),
            monitor_path: "/missing/crius-shim".to_string(),
            monitor_cgroup: String::new(),
            monitor_env: Vec::new(),
            stream_websockets: false,
            allowed_annotations: Vec::new(),
            default_annotations: HashMap::new(),
            privileged_without_host_devices: false,
            privileged_without_host_devices_all_devices_allowed: false,
            container_create_timeout: 30,
            snapshotter: "internal-overlay-untar".to_string(),
        }
    }

    fn runtime_config(temp: &Path, cni_config: CniConfig) -> RuntimeConfig {
        let root_dir = temp.join("state");
        let runtime_root = temp.join("runtime");
        let runtime_path = temp.join("missing-runc");
        RuntimeConfig {
            root_dir: root_dir.clone(),
            runtime: "runc".to_string(),
            runtime_handlers: vec!["runc".to_string()],
            runtime_configs: HashMap::from([(
                "runc".to_string(),
                runtime_handler(&runtime_root, &runtime_path),
            )]),
            runtime_root: runtime_root.clone(),
            runtime_path: runtime_path.clone(),
            image_root: temp.join("images"),
            attach_socket_dir: temp.join("attach"),
            container_exits_dir: temp.join("exits"),
            clean_shutdown_file: root_dir.join("clean.shutdown"),
            version_file: temp.join("version"),
            version_file_persist: root_dir.join("version"),
            cni_config,
            shim: ShimConfig {
                shim_path: PathBuf::from("/missing/crius-shim"),
                runtime_config_path: PathBuf::new(),
                monitor_cgroup: String::new(),
                work_dir: temp.join("shims"),
                attach_socket_dir: temp.join("attach"),
                container_exits_dir: temp.join("exits"),
                io_uid: 0,
                io_gid: 0,
                monitor_env: Vec::new(),
                debug: false,
                log_to_journald: false,
                no_sync_log: false,
                no_pivot: false,
                no_new_keyring: false,
                systemd_cgroup: false,
                runtime_path,
                max_container_log_line_size: 4096,
                state_db_path: root_dir.join("crius.db"),
            },
            ..RuntimeConfig::default()
        }
    }

    async fn wait_until<F>(mut predicate: F)
    where
        F: FnMut() -> bool,
    {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if predicate() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("condition was not reached before timeout");
    }

    #[tokio::test]
    async fn cni_directory_watcher_records_reload_status() {
        let temp = tempfile::tempdir().unwrap();
        let cni_dir = temp.path().join("cni");
        let plugin_dir = temp.path().join("bin");
        tokio::fs::create_dir_all(&cni_dir).await.unwrap();
        tokio::fs::create_dir_all(&plugin_dir).await.unwrap();

        let mut cni_config = CniConfig::default();
        cni_config.set_config_dirs(vec![cni_dir.clone()]);
        cni_config.set_plugin_dirs(vec![plugin_dir.clone()]);
        cni_config.set_max_conf_num(1);
        let service = RuntimeServiceImpl::new(runtime_config(temp.path(), cni_config));

        wait_until(|| {
            service.current_reload_state().watcher_status == RuntimeReloadWatcherStatus::Running
        })
        .await;

        tokio::fs::write(
            cni_dir.join("10-reload.conf"),
            r#"{"cniVersion":"1.0.0","name":"reload-net","type":"bridge"}"#,
        )
        .await
        .unwrap();

        wait_until(|| {
            let state = service.current_reload_state();
            state.last_cni_watch_at_unix_millis.is_some()
                && state
                    .last_cni_watch_error
                    .as_deref()
                    .is_some_and(|message| message.contains("missing or non-executable"))
        })
        .await;

        let state = service.current_reload_state();
        assert_eq!(state.cni_watch_dirs, vec![cni_dir.display().to_string()]);
        assert_eq!(state.watcher_status, RuntimeReloadWatcherStatus::Backoff);
    }
}

mod cross_module {
    use std::collections::HashMap;

    use super::{temp_dir, ContainerRecord, PodSandboxRecord, StorageManager};
    use crius::runtime::ContainerStatus;
    use crius::storage::persistence::{PersistenceConfig, PersistenceManager};

    #[test]
    fn test_pod_with_containers_integration() {
        let temp_dir = temp_dir();
        let db_path = temp_dir.path().join("pod_integration.db");

        let mut storage = StorageManager::new(&db_path).unwrap();

        let pod_id = format!("pod-{}", uuid::Uuid::new_v4());
        let pod = PodSandboxRecord {
            id: pod_id.clone(),
            state: "ready".to_string(),
            name: "test-app".to_string(),
            namespace: "production".to_string(),
            uid: format!("uid-{}", uuid::Uuid::new_v4()),
            created_at: chrono::Utc::now().timestamp(),
            netns_path: format!("/var/run/netns/{}", pod_id),
            labels: r#"{"app": "test-app", "tier": "frontend"}"#.to_string(),
            annotations: "{}".to_string(),
            pause_container_id: Some(format!("pause-{}", uuid::Uuid::new_v4())),
            ip: Some("10.88.0.10".to_string()),
        };

        storage.save_pod_sandbox(&pod).unwrap();

        let container1_id = format!("container-{}", uuid::Uuid::new_v4());
        let container2_id = format!("container-{}", uuid::Uuid::new_v4());

        let container1 = ContainerRecord {
            id: container1_id.clone(),
            pod_id: pod_id.clone(),
            state: "running".to_string(),
            image: "nginx:latest".to_string(),
            command: "nginx".to_string(),
            created_at: chrono::Utc::now().timestamp(),
            labels: "{}".to_string(),
            annotations: "{}".to_string(),
            exit_code: None,
            exit_time: None,
            runtime_handler: None,
            runtime_backend: None,
            snapshot_key: None,
        };

        let container2 = ContainerRecord {
            id: container2_id.clone(),
            pod_id: pod_id.clone(),
            state: "running".to_string(),
            image: "redis:latest".to_string(),
            command: "redis-server".to_string(),
            created_at: chrono::Utc::now().timestamp(),
            labels: "{}".to_string(),
            annotations: "{}".to_string(),
            exit_code: None,
            exit_time: None,
            runtime_handler: None,
            runtime_backend: None,
            snapshot_key: None,
        };

        storage.save_container(&container1).unwrap();
        storage.save_container(&container2).unwrap();

        let containers = storage.list_containers_by_pod(&pod_id).unwrap();
        assert_eq!(containers.len(), 2);

        storage.delete_pod_sandbox(&pod_id).unwrap();

        assert!(storage.get_pod_sandbox(&pod_id).unwrap().is_none());
        assert!(storage.get_container(&container1_id).unwrap().is_none());
        assert!(storage.get_container(&container2_id).unwrap().is_none());
    }

    #[test]
    fn test_storage_reopen_preserves_pod_container_relationship() {
        let temp_dir = temp_dir();
        let db_path = temp_dir.path().join("recovery.db");

        {
            let mut storage = StorageManager::new(&db_path).unwrap();

            let pod_id = format!("pod-{}", uuid::Uuid::new_v4());
            let pod = PodSandboxRecord {
                id: pod_id.clone(),
                state: "ready".to_string(),
                name: "recover-test".to_string(),
                namespace: "default".to_string(),
                uid: format!("uid-{}", uuid::Uuid::new_v4()),
                created_at: chrono::Utc::now().timestamp(),
                netns_path: "/var/run/netns/test".to_string(),
                labels: "{}".to_string(),
                annotations: "{}".to_string(),
                pause_container_id: None,
                ip: Some("10.88.0.5".to_string()),
            };
            storage.save_pod_sandbox(&pod).unwrap();

            let container_id = format!("container-{}", uuid::Uuid::new_v4());
            let container = ContainerRecord {
                id: container_id.clone(),
                pod_id: pod_id.clone(),
                state: "running".to_string(),
                image: "alpine:latest".to_string(),
                command: "sh".to_string(),
                created_at: chrono::Utc::now().timestamp(),
                labels: "{}".to_string(),
                annotations: "{}".to_string(),
                exit_code: None,
                exit_time: None,
                runtime_handler: None,
                runtime_backend: None,
                snapshot_key: None,
            };
            storage.save_container(&container).unwrap();

            storage.close().unwrap();
        }

        {
            let storage = StorageManager::new(&db_path).unwrap();

            let pods = storage.list_pod_sandboxes().unwrap();
            assert_eq!(pods.len(), 1);
            assert_eq!(pods[0].state, "ready");
            assert_eq!(pods[0].ip, Some("10.88.0.5".to_string()));

            let containers = storage.list_containers().unwrap();
            assert_eq!(containers.len(), 1);
            assert_eq!(containers[0].state, "running");
            assert_eq!(containers[0].image, "alpine:latest");

            let pod_containers = storage.list_containers_by_pod(&pods[0].id).unwrap();
            assert_eq!(pod_containers.len(), 1);
            assert_eq!(pod_containers[0].id, containers[0].id);
        }
    }

    #[test]
    fn test_persistence_manager_roundtrip_integration() {
        let temp_dir = temp_dir();
        let db_path = temp_dir.path().join("persistence-roundtrip.db");
        let config = PersistenceConfig {
            db_path: db_path.clone(),
            enable_recovery: true,
            auto_save_interval: 30,
        };

        let mut labels = HashMap::new();
        labels.insert("app".to_string(), "demo".to_string());
        let mut annotations = HashMap::new();
        annotations.insert("owner".to_string(), "integration".to_string());

        let pod_id = format!("pod-{}", uuid::Uuid::new_v4());
        let container_id = format!("container-{}", uuid::Uuid::new_v4());

        {
            let mut manager = PersistenceManager::new(config.clone()).unwrap();
            manager
                .save_pod_sandbox(
                    &pod_id,
                    "ready",
                    "demo-pod",
                    "default",
                    "uid-123",
                    "/var/run/netns/demo",
                    &labels,
                    &annotations,
                    Some("pause-1"),
                    Some("10.88.0.20"),
                )
                .unwrap();
            manager
                .save_container(
                    &container_id,
                    &pod_id,
                    ContainerStatus::Running,
                    "demo:latest",
                    &["sh".to_string(), "-c".to_string(), "sleep 1".to_string()],
                    &labels,
                    &annotations,
                )
                .unwrap();
            manager
                .update_container_state(&container_id, ContainerStatus::Stopped(17))
                .unwrap();
            manager.close().unwrap();
        }

        let manager = PersistenceManager::new(config).unwrap();
        let recovered_containers = manager.recover_containers().unwrap();
        assert_eq!(recovered_containers.len(), 1);
        assert_eq!(recovered_containers[0].0, container_id);
        assert_eq!(recovered_containers[0].1, ContainerStatus::Stopped(17));

        let recovered_container_labels: HashMap<String, String> =
            serde_json::from_str(&recovered_containers[0].2.labels).unwrap();
        let recovered_container_annotations: HashMap<String, String> =
            serde_json::from_str(&recovered_containers[0].2.annotations).unwrap();
        assert_eq!(
            recovered_container_labels.get("app"),
            Some(&"demo".to_string())
        );
        assert_eq!(
            recovered_container_annotations.get("owner"),
            Some(&"integration".to_string())
        );

        let recovered_pods = manager.recover_pods().unwrap();
        assert_eq!(recovered_pods.len(), 1);
        assert_eq!(recovered_pods[0].id, pod_id);
        assert_eq!(
            recovered_pods[0].pause_container_id.as_deref(),
            Some("pause-1")
        );
        assert_eq!(recovered_pods[0].ip.as_deref(), Some("10.88.0.20"));
    }
}
