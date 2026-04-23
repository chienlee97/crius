use super::*;
use crate::nri::{apply_container_adjustment, NriStopContainerResult, NriUpdateContainerResult};
use crate::storage::ContainerRecord;
use crate::storage::PodSandboxRecord;
use std::collections::HashMap;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixListener;
use std::path::Path;
use std::path::PathBuf;
use std::sync::{mpsc, Mutex as StdMutex, OnceLock};
use std::time::Duration;
use tempfile::tempdir;
use tempfile::TempDir;
use tokio::time::timeout;
use tokio_stream::StreamExt;

fn test_runtime_config(root_dir: PathBuf) -> RuntimeConfig {
    RuntimeConfig {
        root_dir,
        runtime: "runc".to_string(),
        runtime_handlers: vec!["runc".to_string(), "kata".to_string()],
        runtime_root: PathBuf::from("/tmp/crius-test-runtime-root"),
        log_dir: PathBuf::from("/tmp/crius-test-logs"),
        runtime_path: PathBuf::from("/definitely/missing/runc"),
        pause_image: "registry.k8s.io/pause:3.9".to_string(),
        cni_config: crate::network::CniConfig::default(),
    }
}

fn test_service() -> RuntimeServiceImpl {
    RuntimeServiceImpl::new(test_runtime_config(tempdir().unwrap().keep()))
}

fn test_service_with_env_cni() -> RuntimeServiceImpl {
    let mut config = test_runtime_config(tempdir().unwrap().keep());
    config.cni_config = crate::network::CniConfig::from_env();
    RuntimeServiceImpl::new(config)
}

#[test]
fn cgroup_support_flags_detect_v1_and_v2_layouts() {
    let dir = tempdir().unwrap();
    let root = dir.path();

    fs::create_dir_all(root.join("memory")).unwrap();
    fs::create_dir_all(root.join("cpu")).unwrap();
    fs::create_dir_all(root.join("hugetlb")).unwrap();
    fs::write(root.join("memory").join("memory.memsw.limit_in_bytes"), "0").unwrap();
    fs::write(root.join("memory").join("memory.kmem.limit_in_bytes"), "0").unwrap();
    fs::write(
        root.join("memory").join("memory.kmem.tcp.limit_in_bytes"),
        "0",
    )
    .unwrap();
    fs::write(root.join("memory").join("memory.swappiness"), "60").unwrap();
    fs::write(root.join("memory").join("memory.oom_control"), "0").unwrap();
    fs::write(root.join("memory").join("memory.use_hierarchy"), "1").unwrap();
    fs::write(root.join("cpu").join("cpu.rt_runtime_us"), "0").unwrap();
    fs::write(root.join("cpu").join("cpu.rt_period_us"), "0").unwrap();
    assert_eq!(
        RuntimeServiceImpl::cgroup_support_flags_for_root(root),
        CgroupResourceSupport {
            swap: true,
            hugetlb: true,
            memory_kernel: true,
            memory_kernel_tcp: true,
            memory_swappiness: true,
            memory_disable_oom_killer: true,
            memory_use_hierarchy: true,
            cpu_realtime: true,
            blockio: true,
            rdt: true,
        }
    );

    fs::remove_dir_all(root.join("memory")).unwrap();
    fs::remove_dir_all(root.join("cpu")).unwrap();
    fs::remove_dir_all(root.join("hugetlb")).unwrap();
    fs::write(root.join("cgroup.controllers"), "cpu memory").unwrap();
    fs::write(root.join("memory.swap.max"), "max").unwrap();
    fs::write(root.join("hugetlb.2MB.max"), "max").unwrap();
    assert_eq!(
        RuntimeServiceImpl::cgroup_support_flags_for_root(root),
        CgroupResourceSupport {
            swap: true,
            hugetlb: true,
            memory_kernel: false,
            memory_kernel_tcp: false,
            memory_swappiness: false,
            memory_disable_oom_killer: false,
            memory_use_hierarchy: false,
            cpu_realtime: false,
            blockio: true,
            rdt: true,
        }
    );
}

#[test]
fn stored_linux_resources_to_nri_omits_cleared_swap_and_hugepages() {
    let mut resources = StoredLinuxResources {
        memory_swap_limit_in_bytes: 8192,
        hugepage_limits: vec![StoredHugepageLimit {
            page_size: "2MB".to_string(),
            limit: 1,
        }],
        ..Default::default()
    };
    RuntimeServiceImpl::sanitize_stored_runtime_resources_with_flags(
        &mut resources,
        CgroupResourceSupport {
            swap: false,
            hugetlb: false,
            memory_kernel: false,
            memory_kernel_tcp: false,
            memory_swappiness: false,
            memory_disable_oom_killer: false,
            memory_use_hierarchy: false,
            cpu_realtime: false,
            blockio: true,
            rdt: true,
        },
    );

    let nri = resources.to_nri();
    assert!(nri
        .memory
        .as_ref()
        .and_then(|memory| memory.swap.as_ref())
        .is_none());
    assert!(nri.hugepage_limits.is_empty());
}

#[test]
fn sanitize_spec_runtime_resources_clears_unsupported_extended_resources() {
    let mut spec = crate::oci::spec::Spec {
        oci_version: "1.0.2".to_string(),
        process: None,
        root: None,
        hostname: None,
        mounts: None,
        hooks: None,
        linux: Some(crate::oci::spec::Linux {
            namespaces: None,
            uid_mappings: None,
            gid_mappings: None,
            devices: None,
            net_devices: None,
            cgroups_path: None,
            resources: Some(crate::oci::spec::LinuxResources {
                memory: Some(crate::oci::spec::LinuxMemory {
                    limit: None,
                    swap: Some(8192),
                    kernel: Some(4096),
                    kernel_tcp: Some(2048),
                    reservation: None,
                    swappiness: Some(60),
                    disable_oom_killer: Some(true),
                    use_hierarchy: Some(true),
                }),
                network: None,
                pids: None,
                cpu: Some(crate::oci::spec::LinuxCpu {
                    shares: None,
                    quota: None,
                    period: None,
                    realtime_runtime: Some(1000),
                    realtime_period: Some(2000),
                    cpus: None,
                    mems: None,
                }),
                block_io: None,
                hugepage_limits: Some(vec![crate::oci::spec::LinuxHugepageLimit {
                    page_size: "2MB".to_string(),
                    limit: 1,
                }]),
                devices: None,
                intel_rdt: None,
                unified: None,
            }),
            rootfs_propagation: None,
            seccomp: None,
            sysctl: None,
            mount_label: None,
            intel_rdt: None,
        }),
        annotations: None,
    };

    RuntimeServiceImpl::sanitize_spec_runtime_resources_with_flags(
        &mut spec,
        CgroupResourceSupport {
            swap: false,
            hugetlb: false,
            memory_kernel: false,
            memory_kernel_tcp: false,
            memory_swappiness: false,
            memory_disable_oom_killer: false,
            memory_use_hierarchy: false,
            cpu_realtime: false,
            blockio: false,
            rdt: false,
        },
    );

    let resources = spec
        .linux
        .as_ref()
        .and_then(|linux| linux.resources.as_ref())
        .unwrap();
    let memory = resources.memory.as_ref().unwrap();
    assert!(memory.swap.is_none());
    assert!(memory.kernel.is_none());
    assert!(memory.kernel_tcp.is_none());
    assert!(memory.swappiness.is_none());
    assert!(memory.disable_oom_killer.is_none());
    assert!(memory.use_hierarchy.is_none());
    let cpu = resources.cpu.as_ref().unwrap();
    assert!(cpu.realtime_runtime.is_none());
    assert!(cpu.realtime_period.is_none());
    assert!(resources.hugepage_limits.is_none());
}

#[derive(Default)]
struct FakeNri {
    calls: tokio::sync::Mutex<Vec<&'static str>>,
    post_start_events: tokio::sync::Mutex<Vec<NriContainerEvent>>,
    post_update_events: tokio::sync::Mutex<Vec<NriContainerEvent>>,
    stop_events: tokio::sync::Mutex<Vec<NriContainerEvent>>,
    stop_pod_events: tokio::sync::Mutex<Vec<NriPodEvent>>,
    fail_run_pod_sandbox: bool,
    fail_post_update_pod_sandbox: bool,
    fail_post_create_container: bool,
    fail_post_start_container: bool,
    fail_post_update_container: bool,
    fail_stop_container: bool,
    fail_remove_container: bool,
    create_result: tokio::sync::Mutex<Option<NriCreateContainerResult>>,
    update_result: tokio::sync::Mutex<Option<NriUpdateContainerResult>>,
    stop_result: tokio::sync::Mutex<Option<NriStopContainerResult>>,
}

#[async_trait::async_trait]
impl NriApi for FakeNri {
    async fn start(&self) -> crate::nri::Result<()> {
        self.calls.lock().await.push("start");
        Ok(())
    }

    async fn shutdown(&self) -> crate::nri::Result<()> {
        self.calls.lock().await.push("shutdown");
        Ok(())
    }

    async fn synchronize(&self) -> crate::nri::Result<()> {
        self.calls.lock().await.push("synchronize");
        Ok(())
    }

    async fn run_pod_sandbox(&self, _event: NriPodEvent) -> crate::nri::Result<()> {
        self.calls.lock().await.push("run_pod");
        if self.fail_run_pod_sandbox {
            return Err(crate::nri::NriError::Plugin(
                "forced pod run failure".to_string(),
            ));
        }
        Ok(())
    }

    async fn stop_pod_sandbox(&self, _event: NriPodEvent) -> crate::nri::Result<()> {
        self.calls.lock().await.push("stop_pod");
        self.stop_pod_events.lock().await.push(_event);
        Ok(())
    }

    async fn remove_pod_sandbox(&self, _event: NriPodEvent) -> crate::nri::Result<()> {
        self.calls.lock().await.push("remove_pod");
        Ok(())
    }

    async fn update_pod_sandbox(&self, _event: NriPodEvent) -> crate::nri::Result<()> {
        self.calls.lock().await.push("update_pod");
        Ok(())
    }

    async fn post_update_pod_sandbox(&self, _event: NriPodEvent) -> crate::nri::Result<()> {
        self.calls.lock().await.push("post_update_pod");
        if self.fail_post_update_pod_sandbox {
            return Err(crate::nri::NriError::Plugin(
                "forced post update pod failure".to_string(),
            ));
        }
        Ok(())
    }

    async fn create_container(
        &self,
        _event: NriContainerEvent,
    ) -> crate::nri::Result<NriCreateContainerResult> {
        self.calls.lock().await.push("create");
        Ok(self.create_result.lock().await.clone().unwrap_or_default())
    }

    async fn post_create_container(&self, _event: NriContainerEvent) -> crate::nri::Result<()> {
        self.calls.lock().await.push("post_create");
        if self.fail_post_create_container {
            return Err(crate::nri::NriError::Plugin(
                "forced post create failure".to_string(),
            ));
        }
        Ok(())
    }

    async fn start_container(&self, _event: NriContainerEvent) -> crate::nri::Result<()> {
        self.calls.lock().await.push("start_container");
        Ok(())
    }

    async fn post_start_container(&self, event: NriContainerEvent) -> crate::nri::Result<()> {
        self.calls.lock().await.push("post_start_container");
        self.post_start_events.lock().await.push(event);
        if self.fail_post_start_container {
            return Err(crate::nri::NriError::Plugin(
                "forced post start failure".to_string(),
            ));
        }
        Ok(())
    }

    async fn update_container(
        &self,
        event: NriContainerEvent,
    ) -> crate::nri::Result<NriUpdateContainerResult> {
        self.calls.lock().await.push("update_container");
        Ok(self
            .update_result
            .lock()
            .await
            .clone()
            .unwrap_or(NriUpdateContainerResult {
                linux_resources: event.linux_resources,
                ..Default::default()
            }))
    }

    async fn post_update_container(&self, event: NriContainerEvent) -> crate::nri::Result<()> {
        self.calls.lock().await.push("post_update_container");
        self.post_update_events.lock().await.push(event);
        if self.fail_post_update_container {
            return Err(crate::nri::NriError::Plugin(
                "forced post update failure".to_string(),
            ));
        }
        Ok(())
    }

    async fn stop_container(
        &self,
        event: NriContainerEvent,
    ) -> crate::nri::Result<NriStopContainerResult> {
        self.calls.lock().await.push("stop_container");
        self.stop_events.lock().await.push(event);
        if self.fail_stop_container {
            return Err(crate::nri::NriError::Plugin(
                "forced stop failure".to_string(),
            ));
        }
        Ok(self.stop_result.lock().await.clone().unwrap_or_default())
    }

    async fn remove_container(&self, _event: NriContainerEvent) -> crate::nri::Result<()> {
        self.calls.lock().await.push("remove_container");
        if self.fail_remove_container {
            return Err(crate::nri::NriError::Plugin(
                "forced remove failure".to_string(),
            ));
        }
        Ok(())
    }
}

fn env_lock() -> &'static StdMutex<()> {
    static ENV_LOCK: OnceLock<StdMutex<()>> = OnceLock::new();
    ENV_LOCK.get_or_init(|| StdMutex::new(()))
}

fn write_fake_runtime_script(dir: &Path) -> PathBuf {
    let state_dir = dir.join("runtime-state");
    fs::create_dir_all(&state_dir).unwrap();

    let script_path = dir.join("fake-runc.sh");
    let script = format!(
        r#"#!/bin/bash
set -eu
STATE_DIR="{state_dir}"
cmd="${{1:-}}"
if [ "$#" -gt 0 ]; then
  shift
fi
case "$cmd" in
  state)
    id="${{1:-}}"
    file="$STATE_DIR/$id.state"
    if [ ! -f "$file" ]; then
      exit 1
    fi
    status="$(cat "$file")"
    pid=0
    if [ -f "$STATE_DIR/$id.pid" ]; then
      pid="$(cat "$STATE_DIR/$id.pid")"
    fi
    printf '{{"ociVersion":"1.0.2","id":"%s","status":"%s","pid":%s,"bundle":"%s","rootfs":"%s","created":"2024-01-01T00:00:00Z","owner":"root"}}\n' "$id" "$status" "$pid" "$STATE_DIR/bundle" "$STATE_DIR/rootfs"
    ;;
  kill)
    id="${{1:-}}"
    echo stopped > "$STATE_DIR/$id.state"
    ;;
  delete)
    id="${{1:-}}"
    if [ -f "$STATE_DIR/$id.state" ]; then
      rm -f "$STATE_DIR/$id.state" "$STATE_DIR/$id.pid"
      exit 0
    fi
    echo "container does not exist" >&2
    exit 1
    ;;
  start)
    id="${{1:-}}"
    echo running > "$STATE_DIR/$id.state"
    echo $$ > "$STATE_DIR/$id.pid"
    ;;
  pause)
    id="${{1:-}}"
    if [ -n "$id" ]; then
      echo paused > "$STATE_DIR/$id.state"
    fi
    ;;
  resume)
    id="${{1:-}}"
    if [ -n "$id" ]; then
      echo running > "$STATE_DIR/$id.state"
    fi
    ;;
  update)
    resource_file=""
    id=""
    while [ "$#" -gt 0 ]; do
      case "$1" in
        --resources)
          resource_file="${{2:-}}"
          shift 2
          ;;
        *)
          id="$1"
          shift
          ;;
      esac
    done
    if [ -n "$resource_file" ] && [ -n "$id" ]; then
      cp "$resource_file" "$STATE_DIR/$id.update.json"
    fi
    ;;
  checkpoint)
    image_path=""
    id=""
    while [ "$#" -gt 0 ]; do
      case "$1" in
        --file-locks)
          shift
          ;;
        --image-path|--work-path)
          if [ "$1" = "--image-path" ]; then
            image_path="${{2:-}}"
          fi
          shift 2
          ;;
        --leave-running)
          shift
          ;;
        *)
          id="$1"
          shift
          ;;
      esac
    done
    if [ -n "$image_path" ] && [ -n "$id" ]; then
      mkdir -p "$image_path"
      printf '{{"id":"%s","checkpointed":true}}\n' "$id" > "$image_path/checkpoint.json"
    fi
    ;;
  restore)
    id=""
    while [ "$#" -gt 0 ]; do
      case "$1" in
        -d|--image-path|--work-path|--bundle|--no-pivot)
          if [ "$1" = "-d" ] || [ "$1" = "--no-pivot" ]; then
            shift
          else
            shift 2
          fi
          ;;
        *)
          id="$1"
          shift
          ;;
      esac
    done
    if [ -n "$id" ]; then
      echo running > "$STATE_DIR/$id.state"
      echo $$ > "$STATE_DIR/$id.pid"
    fi
    ;;
esac
exit 0
"#,
        state_dir = state_dir.display()
    );

    fs::write(&script_path, script).unwrap();
    let mut perms = fs::metadata(&script_path).unwrap().permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&script_path, perms).unwrap();
    script_path
}

fn ensure_test_shim_binary() {
    static ONCE: std::sync::Once = std::sync::Once::new();

    ONCE.call_once(|| {
        let shim_path = PathBuf::from("/root/crius/target/debug/crius-shim");
        if let Some(parent) = shim_path.parent() {
            fs::create_dir_all(parent).unwrap();
        }

        fs::write(
            &shim_path,
            r#"#!/bin/sh
set -eu
exit_code_file=""
log_file=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --exit-code-file)
      exit_code_file="${2:-}"
      shift 2
      ;;
    --log)
      log_file="${2:-}"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done
if [ -n "$exit_code_file" ]; then
  shim_dir="$(dirname "$exit_code_file")"
  mkdir -p "$shim_dir"
  : > "$shim_dir/attach.sock"
  : > "$shim_dir/resize.sock"
fi
if [ -n "$log_file" ]; then
  mkdir -p "$(dirname "$log_file")"
  : > "$log_file"
fi
trap 'exit 0' TERM INT
while :; do
  sleep 1
done
"#,
        )
        .unwrap();
        let mut perms = fs::metadata(&shim_path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&shim_path, perms).unwrap();
    });
}

fn test_service_with_fake_runtime() -> (TempDir, RuntimeServiceImpl) {
    let dir = tempdir().unwrap();
    let runtime_path = write_fake_runtime_script(dir.path());
    let shim_work_dir = dir.path().join("shims");
    let config = RuntimeConfig {
        root_dir: dir.path().join("root"),
        runtime: "runc".to_string(),
        runtime_handlers: vec!["runc".to_string()],
        runtime_root: dir.path().join("runtime-root"),
        log_dir: dir.path().join("logs"),
        runtime_path,
        pause_image: "registry.k8s.io/pause:3.9".to_string(),
        cni_config: crate::network::CniConfig::default(),
    };
    let nri_config = NriConfig {
        blockio_config_path: write_blockio_config(&dir).display().to_string(),
        ..Default::default()
    };
    let service = RuntimeServiceImpl::new_with_shim_work_dir(config, nri_config, shim_work_dir);
    (dir, service)
}

fn fake_runtime_state_path(dir: &TempDir, id: &str) -> PathBuf {
    dir.path()
        .join("runtime-state")
        .join(format!("{}.state", id))
}

fn fake_runtime_update_path(dir: &TempDir, id: &str) -> PathBuf {
    dir.path()
        .join("runtime-state")
        .join(format!("{}.update.json", id))
}

fn write_blockio_config(dir: &TempDir) -> PathBuf {
    let path = dir.path().join("blockio.json");
    fs::write(
        &path,
        serde_json::json!({
            "classes": {
                "gold": {
                    "weight": 500
                }
            }
        })
        .to_string(),
    )
    .unwrap();
    path
}

fn test_service_with_fake_runtime_and_nri(
    fake_nri: Arc<dyn NriApi>,
) -> (TempDir, RuntimeServiceImpl) {
    ensure_test_shim_binary();
    let dir = tempdir().unwrap();
    let runtime_path = write_fake_runtime_script(dir.path());
    let shim_work_dir = dir.path().join("shims");
    let config = RuntimeConfig {
        root_dir: dir.path().join("root"),
        runtime: "runc".to_string(),
        runtime_handlers: vec!["runc".to_string()],
        runtime_root: dir.path().join("runtime-root"),
        log_dir: dir.path().join("logs"),
        runtime_path,
        pause_image: "registry.k8s.io/pause:3.9".to_string(),
        cni_config: crate::network::CniConfig::default(),
    };
    let nri_config = NriConfig {
        enable: true,
        blockio_config_path: write_blockio_config(&dir).display().to_string(),
        ..Default::default()
    };
    let service = RuntimeServiceImpl::new_with_shim_work_dir_and_nri(
        config,
        nri_config,
        shim_work_dir,
        Some(fake_nri),
    );
    (dir, service)
}

#[tokio::test]
async fn initialize_nri_starts_then_synchronizes_once() {
    let fake_nri = Arc::new(FakeNri::default());
    let nri_config = NriConfig {
        enable: true,
        ..Default::default()
    };
    let service = RuntimeServiceImpl::new_with_nri_api(
        test_runtime_config(tempdir().unwrap().keep()),
        nri_config,
        fake_nri.clone(),
    );

    service
        .initialize_nri()
        .await
        .expect("initialize_nri should succeed");

    assert_eq!(
        fake_nri.calls.lock().await.clone(),
        vec!["start", "synchronize"]
    );
}

#[tokio::test]
async fn initialize_nri_skips_disabled_configuration() {
    let fake_nri = Arc::new(FakeNri::default());
    let service = RuntimeServiceImpl::new_with_nri_api(
        test_runtime_config(tempdir().unwrap().keep()),
        NriConfig::default(),
        fake_nri.clone(),
    );

    service
        .initialize_nri()
        .await
        .expect("disabled NRI should be a no-op");

    assert!(fake_nri.calls.lock().await.is_empty());
}

#[tokio::test]
async fn rollback_failed_pod_sandbox_run_undoes_nri_and_local_state() {
    let fake_nri = Arc::new(FakeNri {
        fail_run_pod_sandbox: true,
        ..Default::default()
    });
    let nri_config = NriConfig {
        enable: true,
        ..Default::default()
    };
    let root_dir = tempdir().unwrap().keep();
    let service = RuntimeServiceImpl::new_with_nri_api(
        test_runtime_config(root_dir.clone()),
        nri_config,
        fake_nri.clone(),
    );

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            pause_container_id: Some("pause-rollback".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    service.pod_sandboxes.lock().await.insert(
        "pod-rollback".to_string(),
        test_pod("pod-rollback", annotations.clone()),
    );
    service
        .persistence
        .lock()
        .await
        .save_pod_sandbox(
            "pod-rollback",
            "ready",
            "pod",
            "default",
            "uid",
            "/tmp/netns-rollback",
            &HashMap::new(),
            &annotations,
            Some("pause-rollback"),
            None,
        )
        .unwrap();

    service
        .rollback_failed_pod_sandbox_run("pod-rollback")
        .await;

    assert_eq!(
        fake_nri.calls.lock().await.clone(),
        vec!["stop_pod", "remove_pod"]
    );
    assert!(!service
        .pod_sandboxes
        .lock()
        .await
        .contains_key("pod-rollback"));
    let persistence = service.persistence.lock().await;
    assert!(persistence
        .storage()
        .get_pod_sandbox("pod-rollback")
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn rollback_failed_container_create_undoes_nri_and_local_state() {
    let fake_nri = Arc::new(FakeNri {
        fail_post_create_container: true,
        ..Default::default()
    });
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());
    let container_id = "ctr-create-undo".to_string();
    let pod_id = "pod-create-undo".to_string();
    let sidecar_id = "ctr-create-undo-sidecar".to_string();

    service
        .pod_sandboxes
        .lock()
        .await
        .insert(pod_id.clone(), test_pod(&pod_id, HashMap::new()));
    service.containers.lock().await.insert(
        container_id.clone(),
        test_container(&container_id, &pod_id, HashMap::new()),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            &container_id,
            &pod_id,
            crate::runtime::ContainerStatus::Created,
            "busybox:latest",
            &["/bin/sh".to_string()],
            &HashMap::new(),
            &HashMap::new(),
        )
        .unwrap();
    service.containers.lock().await.insert(
        sidecar_id.clone(),
        test_container(&sidecar_id, &pod_id, HashMap::new()),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            &sidecar_id,
            &pod_id,
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &["sleep".to_string()],
            &HashMap::new(),
            &HashMap::new(),
        )
        .unwrap();
    set_fake_runtime_state(&dir, &sidecar_id, "running");

    let mut update = crate::nri_proto::api::ContainerUpdate::new();
    update.container_id = sidecar_id.clone();
    let mut linux = crate::nri_proto::api::LinuxContainerUpdate::new();
    let mut resources = crate::nri_proto::api::LinuxResources::new();
    let mut cpu = crate::nri_proto::api::LinuxCPU::new();
    let mut shares = crate::nri_proto::api::OptionalUInt64::new();
    shares.value = 333;
    cpu.shares = protobuf::MessageField::some(shares);
    resources.cpu = protobuf::MessageField::some(cpu);
    linux.resources = protobuf::MessageField::some(resources);
    update.linux = protobuf::MessageField::some(linux);
    *fake_nri.stop_result.lock().await = Some(NriStopContainerResult {
        updates: vec![update],
    });

    let event = service
        .nri_container_event(&pod_id, &container_id, &HashMap::new())
        .await;
    fake_nri.create_container(event.clone()).await.unwrap();
    let _ = fake_nri.post_create_container(event.clone()).await;

    service
        .rollback_failed_container_create(&container_id, event)
        .await;

    assert_eq!(
        fake_nri.calls.lock().await.clone(),
        vec![
            "create",
            "post_create",
            "stop_container",
            "remove_container"
        ]
    );
    let containers = service.containers.lock().await;
    assert!(!containers.contains_key(&container_id));
    assert!(containers.contains_key(&sidecar_id));
    drop(containers);
    assert!(service
        .persistence
        .lock()
        .await
        .storage()
        .list_containers()
        .unwrap()
        .iter()
        .all(|container| container.id != container_id));
    let update_payload: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(fake_runtime_update_path(&dir, &sidecar_id)).unwrap(),
    )
    .unwrap();
    assert_eq!(update_payload["cpu"]["shares"], 333);
}

#[tokio::test]
async fn update_pod_sandbox_resources_updates_pause_container_and_notifies_nri() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());
    let pod_id = "pod-update".to_string();
    let pause_id = "pause-pod-update".to_string();
    let netns_path = "/var/run/netns/pod-update".to_string();

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            pause_container_id: Some(pause_id.clone()),
            netns_path: Some(netns_path.clone()),
            ..Default::default()
        },
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        pod_id.clone(),
        crate::proto::runtime::v1::PodSandbox {
            id: pod_id.clone(),
            metadata: Some(PodSandboxMetadata {
                name: "pod-update".to_string(),
                namespace: "default".to_string(),
                uid: "uid-update".to_string(),
                attempt: 0,
            }),
            state: PodSandboxState::SandboxReady as i32,
            annotations: annotations.clone(),
            runtime_handler: "runc".to_string(),
            ..Default::default()
        },
    );
    {
        let mut pod_manager = service.pod_manager.lock().await;
        pod_manager.restore_pod_sandbox(crate::pod::PodSandbox {
            id: pod_id.clone(),
            config: PodSandboxConfig {
                name: "pod-update".to_string(),
                namespace: "default".to_string(),
                uid: "uid-update".to_string(),
                hostname: "pod-update".to_string(),
                log_directory: None,
                runtime_handler: "runc".to_string(),
                labels: Vec::new(),
                annotations: Vec::new(),
                dns_config: None,
                port_mappings: Vec::new(),
                network_config: None,
                cgroup_parent: None,
                sysctls: HashMap::new(),
                namespace_options: None,
                privileged: false,
                run_as_user: None,
                run_as_group: None,
                supplemental_groups: Vec::new(),
                readonly_rootfs: false,
                no_new_privileges: None,
                apparmor_profile: None,
                selinux_label: None,
                seccomp_profile: None,
                linux_resources: None,
            },
            netns_path: PathBuf::from(&netns_path),
            pause_container_id: pause_id.clone(),
            state: crate::pod::PodSandboxState::Ready,
            created_at: 0,
            ip: String::new(),
            network_status: None,
        });
    }
    service
        .persistence
        .lock()
        .await
        .save_pod_sandbox(
            &pod_id,
            "ready",
            "pod-update",
            "default",
            "uid-update",
            &netns_path,
            &HashMap::new(),
            &annotations,
            Some(&pause_id),
            None,
        )
        .unwrap();

    let overhead = crate::proto::runtime::v1::LinuxContainerResources {
        cpu_shares: 128,
        ..Default::default()
    };
    let resources = crate::proto::runtime::v1::LinuxContainerResources {
        cpu_shares: 512,
        memory_limit_in_bytes: 4096,
        ..Default::default()
    };

    RuntimeService::update_pod_sandbox_resources(
        &service,
        Request::new(UpdatePodSandboxResourcesRequest {
            pod_sandbox_id: pod_id.clone(),
            overhead: Some(overhead.clone()),
            resources: Some(resources.clone()),
        }),
    )
    .await
    .unwrap();

    let update_payload: serde_json::Value =
        serde_json::from_slice(&fs::read(fake_runtime_update_path(&dir, &pause_id)).unwrap())
            .unwrap();
    assert_eq!(update_payload["cpu"]["shares"], 512);
    assert_eq!(update_payload["memory"]["limit"], 4096);
    assert_eq!(
        fake_nri.calls.lock().await.clone(),
        vec!["update_pod", "post_update_pod"]
    );

    let updated_pod = service
        .pod_sandboxes
        .lock()
        .await
        .get(&pod_id)
        .cloned()
        .unwrap();
    let state = RuntimeServiceImpl::read_internal_state::<StoredPodState>(
        &updated_pod.annotations,
        INTERNAL_POD_STATE_KEY,
    )
    .unwrap();
    assert_eq!(state.overhead_linux_resources.unwrap().cpu_shares, 128);
    assert_eq!(state.linux_resources.unwrap().cpu_shares, 512);
    let pod = service.nri_pod_event(&pod_id).await.pod.unwrap();
    let pod_linux = pod.linux.unwrap();
    assert_eq!(
        pod_linux
            .pod_overhead
            .as_ref()
            .and_then(|resources| resources.cpu.as_ref())
            .and_then(|cpu| cpu.shares.as_ref())
            .map(|shares| shares.value),
        Some(128)
    );
    assert_eq!(
        pod_linux
            .pod_resources
            .as_ref()
            .and_then(|resources| resources.cpu.as_ref())
            .and_then(|cpu| cpu.shares.as_ref())
            .map(|shares| shares.value),
        Some(512)
    );

    let persisted = service
        .persistence
        .lock()
        .await
        .storage()
        .get_pod_sandbox(&pod_id)
        .unwrap()
        .unwrap();
    let persisted_annotations: HashMap<String, String> =
        serde_json::from_str(&persisted.annotations).unwrap();
    let persisted_state = RuntimeServiceImpl::read_internal_state::<StoredPodState>(
        &persisted_annotations,
        INTERNAL_POD_STATE_KEY,
    )
    .unwrap();
    assert_eq!(
        persisted_state.overhead_linux_resources.unwrap().cpu_shares,
        128
    );
    assert_eq!(
        persisted_state
            .linux_resources
            .unwrap()
            .memory_limit_in_bytes,
        4096
    );
}

#[tokio::test]
async fn update_pod_sandbox_resources_succeeds_when_nri_post_update_fails() {
    let fake_nri = Arc::new(FakeNri {
        fail_post_update_pod_sandbox: true,
        ..Default::default()
    });
    let (_dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());
    let pod_id = "pod-update-post-fail".to_string();
    let pause_id = "pause-pod-update-post-fail".to_string();
    let netns_path = "/var/run/netns/pod-update-post-fail".to_string();

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            pause_container_id: Some(pause_id.clone()),
            netns_path: Some(netns_path.clone()),
            ..Default::default()
        },
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        pod_id.clone(),
        crate::proto::runtime::v1::PodSandbox {
            id: pod_id.clone(),
            metadata: Some(PodSandboxMetadata {
                name: "pod-update-post-fail".to_string(),
                namespace: "default".to_string(),
                uid: "uid-update-post-fail".to_string(),
                attempt: 0,
            }),
            state: PodSandboxState::SandboxReady as i32,
            annotations,
            runtime_handler: "runc".to_string(),
            ..Default::default()
        },
    );
    {
        let mut pod_manager = service.pod_manager.lock().await;
        pod_manager.restore_pod_sandbox(crate::pod::PodSandbox {
            id: pod_id.clone(),
            config: PodSandboxConfig {
                name: "pod-update-post-fail".to_string(),
                namespace: "default".to_string(),
                uid: "uid-update-post-fail".to_string(),
                hostname: "pod-update-post-fail".to_string(),
                log_directory: None,
                runtime_handler: "runc".to_string(),
                labels: Vec::new(),
                annotations: Vec::new(),
                dns_config: None,
                port_mappings: Vec::new(),
                network_config: None,
                cgroup_parent: None,
                sysctls: HashMap::new(),
                namespace_options: None,
                privileged: false,
                run_as_user: None,
                run_as_group: None,
                supplemental_groups: Vec::new(),
                readonly_rootfs: false,
                no_new_privileges: None,
                apparmor_profile: None,
                selinux_label: None,
                seccomp_profile: None,
                linux_resources: None,
            },
            netns_path: PathBuf::from(&netns_path),
            pause_container_id: pause_id,
            state: crate::pod::PodSandboxState::Ready,
            created_at: 0,
            ip: String::new(),
            network_status: None,
        });
    }

    RuntimeService::update_pod_sandbox_resources(
        &service,
        Request::new(UpdatePodSandboxResourcesRequest {
            pod_sandbox_id: pod_id,
            overhead: None,
            resources: Some(crate::proto::runtime::v1::LinuxContainerResources {
                cpu_shares: 256,
                ..Default::default()
            }),
        }),
    )
    .await
    .expect("post-update pod failure should not fail sandbox resource update");

    assert_eq!(
        fake_nri.calls.lock().await.clone(),
        vec!["update_pod", "post_update_pod"]
    );
}

#[tokio::test]
async fn nri_pod_event_uses_pause_spec_resources_for_linux_resources() {
    let (dir, service) = test_service_with_fake_runtime();
    let pod_id = "pod-nri-linux-resources".to_string();
    let pause_id = "pause-nri-linux-resources".to_string();

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            pause_container_id: Some(pause_id.clone()),
            ..Default::default()
        },
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        pod_id.clone(),
        crate::proto::runtime::v1::PodSandbox {
            id: pod_id.clone(),
            annotations,
            ..Default::default()
        },
    );

    write_test_bundle_config_with_linux(
        &dir,
        &pause_id,
        &HashMap::new(),
        serde_json::json!({
            "resources": {
                "memory": {
                    "limit": 8192
                },
                "cpu": {
                    "shares": 1024
                }
            }
        }),
    );

    let pod = service.nri_pod_event(&pod_id).await.pod.unwrap();
    let linux = pod.linux.unwrap();
    let resources = linux
        .resources
        .into_option()
        .expect("pause OCI resources should be exposed");
    assert_eq!(
        resources
            .memory
            .as_ref()
            .and_then(|memory| memory.limit.as_ref())
            .map(|limit| limit.value),
        Some(8192)
    );
    assert_eq!(
        resources
            .cpu
            .as_ref()
            .and_then(|cpu| cpu.shares.as_ref())
            .map(|shares| shares.value),
        Some(1024)
    );
}

#[tokio::test]
async fn nri_create_result_applies_adjustments_and_side_effects() {
    let (dir, service) = test_service_with_fake_runtime();
    let mut adjustment = crate::nri_proto::api::ContainerAdjustment::new();
    adjustment
        .annotations
        .insert("plugin.annotation".to_string(), "set".to_string());
    adjustment.env.push(crate::nri_proto::api::KeyValue {
        key: "PLUGIN_ENV".to_string(),
        value: "enabled".to_string(),
        ..Default::default()
    });
    adjustment.args = vec![String::new(), "/bin/echo".to_string(), "plugin".to_string()];

    let mut update = crate::nri_proto::api::ContainerUpdate::new();
    update.container_id = "container-update".to_string();
    let mut linux_update = crate::nri_proto::api::LinuxContainerUpdate::new();
    let mut resources = crate::nri_proto::api::LinuxResources::new();
    let mut cpu = crate::nri_proto::api::LinuxCPU::new();
    let mut shares = crate::nri_proto::api::OptionalUInt64::new();
    shares.value = 2048;
    cpu.shares = protobuf::MessageField::some(shares);
    resources.cpu = protobuf::MessageField::some(cpu);
    linux_update.resources = protobuf::MessageField::some(resources);
    update.linux = protobuf::MessageField::some(linux_update);

    let create_result = NriCreateContainerResult {
        adjustment,
        updates: vec![update],
        evictions: vec![crate::nri_proto::api::ContainerEviction {
            container_id: "container-evict".to_string(),
            reason: "plugin request".to_string(),
            ..Default::default()
        }],
    };

    service.pod_sandboxes.lock().await.insert(
        "pod-create".to_string(),
        test_pod("pod-create", HashMap::new()),
    );

    let mut update_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut update_annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();
    let update_container =
        test_container("container-update", "pod-create", update_annotations.clone());
    let evict_container = test_container("container-evict", "pod-create", HashMap::new());
    service
        .containers
        .lock()
        .await
        .insert("container-update".to_string(), update_container.clone());
    service
        .containers
        .lock()
        .await
        .insert("container-evict".to_string(), evict_container.clone());
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-update",
            "pod-create",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &["sleep".to_string(), "10".to_string()],
            &HashMap::new(),
            &update_annotations,
        )
        .unwrap();
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-evict",
            "pod-create",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &["sleep".to_string(), "10".to_string()],
            &HashMap::new(),
            &HashMap::new(),
        )
        .unwrap();
    set_fake_runtime_state(&dir, "container-update", "running");
    set_fake_runtime_state(&dir, "container-evict", "running");

    let mut stored_annotations = HashMap::new();
    stored_annotations.insert("existing".to_string(), "value".to_string());
    let container_config = ContainerConfig {
        name: "created".to_string(),
        image: "busybox:latest".to_string(),
        command: vec!["sleep".to_string()],
        args: vec!["10".to_string()],
        env: Vec::new(),
        working_dir: None,
        mounts: Vec::new(),
        labels: Vec::new(),
        annotations: stored_annotations
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
        privileged: false,
        user: None,
        run_as_group: None,
        supplemental_groups: Vec::new(),
        hostname: None,
        tty: false,
        stdin: false,
        stdin_once: false,
        log_path: None,
        readonly_rootfs: false,
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
        devices: Vec::new(),
        rootfs: dir
            .path()
            .join("root")
            .join("containers")
            .join("created")
            .join("rootfs"),
    };
    let mut spec = service
        .runtime
        .build_spec("created", &container_config)
        .unwrap();
    let mut nri_event = NriContainerEvent::default();

    service
        .process_nri_create_side_effects(&create_result)
        .await
        .unwrap();
    apply_container_adjustment(&mut spec, &create_result.adjustment).unwrap();
    RuntimeServiceImpl::apply_adjusted_annotations(
        &mut stored_annotations,
        &create_result.adjustment,
    );
    RuntimeServiceImpl::refresh_nri_event_container_from_spec(
        &mut nri_event,
        &spec,
        &stored_annotations,
    );

    assert_eq!(
        stored_annotations.get("plugin.annotation"),
        Some(&"set".to_string())
    );
    assert_eq!(
        spec.annotations.as_ref().unwrap().get("plugin.annotation"),
        Some(&"set".to_string())
    );
    assert_eq!(
        spec.process.as_ref().unwrap().env.as_ref().unwrap(),
        &vec!["PLUGIN_ENV=enabled".to_string()]
    );
    assert_eq!(
        spec.process.as_ref().unwrap().args,
        vec!["/bin/echo".to_string(), "plugin".to_string()]
    );
    assert_eq!(
        nri_event.container.annotations.get("plugin.annotation"),
        Some(&"set".to_string())
    );

    let updated = service
        .containers
        .lock()
        .await
        .get("container-update")
        .cloned()
        .unwrap();
    let updated_state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
        &updated.annotations,
        INTERNAL_CONTAINER_STATE_KEY,
    )
    .unwrap();
    assert_eq!(
        updated_state
            .linux_resources
            .as_ref()
            .map(|resources| resources.cpu_shares),
        Some(2048)
    );
    let evicted = service
        .containers
        .lock()
        .await
        .get("container-evict")
        .cloned()
        .unwrap();
    assert_eq!(evicted.state, ContainerState::ContainerExited as i32);
}

#[test]
fn enrich_container_annotations_adds_upstream_runtime_metadata() {
    let mut annotations = HashMap::new();
    annotations.insert("existing".to_string(), "value".to_string());
    let pod_state = StoredPodState {
        runtime_handler: "runc".to_string(),
        ..Default::default()
    };
    let log_path = PathBuf::from("/var/log/pods/workload.log");

    RuntimeServiceImpl::enrich_container_annotations(
        super::annotations::ContainerAnnotationContext {
            annotations: &mut annotations,
            container_id: "container-1",
            pod_sandbox_id: "pod-1",
            metadata_name: Some("workload"),
            requested_image: Some("docker.io/library/busybox:latest"),
            resolved_image_name: Some("busybox:latest"),
            log_path: Some(&log_path),
            pod_state: Some(&pod_state),
            default_runtime: "fallback-runtime",
        },
    );

    assert_eq!(
        annotations.get("existing").map(String::as_str),
        Some("value")
    );
    assert_eq!(
        annotations
            .get(CRIO_CONTAINER_TYPE_ANNOTATION)
            .map(String::as_str),
        Some(CONTAINER_TYPE_CONTAINER)
    );
    assert_eq!(
        annotations
            .get(CONTAINERD_CONTAINER_TYPE_ANNOTATION)
            .map(String::as_str),
        Some(CONTAINER_TYPE_CONTAINER)
    );
    assert_eq!(
        annotations
            .get(CRIO_RUNTIME_HANDLER_ANNOTATION)
            .map(String::as_str),
        Some("runc")
    );
    assert_eq!(
        annotations
            .get(CONTAINERD_RUNTIME_HANDLER_ANNOTATION)
            .map(String::as_str),
        Some("runc")
    );
    assert_eq!(
        annotations
            .get(CRIO_LOG_PATH_ANNOTATION)
            .map(String::as_str),
        Some("/var/log/pods/workload.log")
    );
    assert_eq!(
        annotations
            .get(CRIO_CONTAINER_NAME_ANNOTATION)
            .map(String::as_str),
        Some("workload")
    );
    assert_eq!(
        annotations
            .get(CONTAINERD_CONTAINER_NAME_ANNOTATION)
            .map(String::as_str),
        Some("workload")
    );
    assert_eq!(
        annotations
            .get(CRIO_USER_REQUESTED_IMAGE_ANNOTATION)
            .map(String::as_str),
        Some("docker.io/library/busybox:latest")
    );
    assert_eq!(
        annotations
            .get(CRIO_IMAGE_NAME_ANNOTATION)
            .map(String::as_str),
        Some("busybox:latest")
    );
    assert_eq!(
        annotations
            .get(CONTAINERD_IMAGE_NAME_ANNOTATION)
            .map(String::as_str),
        Some("busybox:latest")
    );
}

#[test]
fn nri_allowed_annotation_prefixes_include_existing_annotations() {
    let (_dir, service) = test_service_with_fake_runtime();
    let annotations = HashMap::from([
        ("existing.annotation".to_string(), "value".to_string()),
        (
            INTERNAL_CONTAINER_STATE_KEY.to_string(),
            "{\"hidden\":true}".to_string(),
        ),
    ]);

    let allowed = service.nri_allowed_annotation_prefixes(&annotations, &annotations, "runc");
    assert!(allowed.iter().any(|item| item == "existing.annotation"));
    assert!(!allowed
        .iter()
        .any(|item| item == INTERNAL_CONTAINER_STATE_KEY));
}

#[test]
fn filter_nri_annotation_adjustments_drops_unlisted_keys() {
    let (_dir, service) = test_service_with_fake_runtime();
    let existing = HashMap::from([("existing.annotation".to_string(), "value".to_string())]);
    let adjustments = HashMap::from([
        ("existing.annotation".to_string(), "updated".to_string()),
        ("disallowed.annotation".to_string(), "value".to_string()),
    ]);

    let filtered =
        service.filter_nri_annotation_adjustments(&existing, &adjustments, &existing, "runc");
    assert_eq!(
        filtered,
        HashMap::from([("existing.annotation".to_string(), "updated".to_string())])
    );
}

#[test]
fn nri_allowed_annotation_prefixes_include_runtime_and_workload_overrides() {
    let (_dir, mut service) = test_service_with_fake_runtime();
    service
        .nri_config
        .runtime_allowed_annotation_prefixes
        .insert(
            "kata".to_string(),
            vec!["io.kubernetes.cri-o.RuntimeHandler.kata".to_string()],
        );
    service.nri_config.workload_allowed_annotation_prefixes = vec![NriAnnotationWorkloadConfig {
        activation_annotation: "workload.example/class".to_string(),
        activation_value: "latency".to_string(),
        allowed_annotation_prefixes: vec!["workload.example/".to_string()],
    }];

    let activation_annotations =
        HashMap::from([("workload.example/class".to_string(), "latency".to_string())]);
    let existing_annotations = HashMap::new();

    let allowed = service.nri_allowed_annotation_prefixes(
        &activation_annotations,
        &existing_annotations,
        "kata",
    );

    assert!(allowed
        .iter()
        .any(|item| item == "io.kubernetes.cri-o.RuntimeHandler.kata"));
    assert!(allowed.iter().any(|item| item == "workload.example/"));
}

#[test]
fn sanitize_nri_linux_resources_with_flags_clears_unsupported_fields() {
    let mut resources = crate::nri_proto::api::LinuxResources::new();
    let mut memory = crate::nri_proto::api::LinuxMemory::new();
    let mut swap = crate::nri_proto::api::OptionalInt64::new();
    swap.value = 4096;
    memory.swap = protobuf::MessageField::some(swap);
    let mut kernel = crate::nri_proto::api::OptionalInt64::new();
    kernel.value = 1024;
    memory.kernel = protobuf::MessageField::some(kernel);
    resources.memory = protobuf::MessageField::some(memory);
    let mut cpu = crate::nri_proto::api::LinuxCPU::new();
    let mut runtime = crate::nri_proto::api::OptionalInt64::new();
    runtime.value = 1000;
    cpu.realtime_runtime = protobuf::MessageField::some(runtime);
    resources.cpu = protobuf::MessageField::some(cpu);
    let mut blockio_class = crate::nri_proto::api::OptionalString::new();
    blockio_class.value = "gold".to_string();
    resources.blockio_class = protobuf::MessageField::some(blockio_class);
    let mut rdt_class = crate::nri_proto::api::OptionalString::new();
    rdt_class.value = "silver".to_string();
    resources.rdt_class = protobuf::MessageField::some(rdt_class);
    resources
        .hugepage_limits
        .push(crate::nri_proto::api::HugepageLimit {
            page_size: "2MB".to_string(),
            limit: 1,
            ..Default::default()
        });

    RuntimeServiceImpl::sanitize_nri_linux_resources_with_flags(
        &mut resources,
        CgroupResourceSupport {
            swap: false,
            hugetlb: false,
            memory_kernel: false,
            memory_kernel_tcp: false,
            memory_swappiness: false,
            memory_disable_oom_killer: false,
            memory_use_hierarchy: false,
            cpu_realtime: false,
            blockio: false,
            rdt: false,
        },
    );

    let memory = resources.memory.as_ref().unwrap();
    assert!(memory.swap.is_none());
    assert!(memory.kernel.is_none());
    assert!(resources.cpu.as_ref().unwrap().realtime_runtime.is_none());
    assert!(resources.hugepage_limits.is_empty());
    assert!(resources.blockio_class.is_none());
    assert!(resources.rdt_class.is_none());
}

#[test]
fn sanitize_nri_adjustment_for_nri_config_clears_blockio_without_config() {
    let mut adjustment = crate::nri_proto::api::ContainerAdjustment::new();
    let mut linux = crate::nri_proto::api::LinuxContainerAdjustment::new();
    let mut resources = crate::nri_proto::api::LinuxResources::new();
    let mut blockio_class = crate::nri_proto::api::OptionalString::new();
    blockio_class.value = "gold".to_string();
    resources.blockio_class = protobuf::MessageField::some(blockio_class);
    let mut rdt_class = crate::nri_proto::api::OptionalString::new();
    rdt_class.value = "silver".to_string();
    resources.rdt_class = protobuf::MessageField::some(rdt_class);
    linux.resources = protobuf::MessageField::some(resources);
    let mut rdt = crate::nri_proto::api::LinuxRdt::new();
    let mut clos_id = crate::nri_proto::api::OptionalString::new();
    clos_id.value = "silver".to_string();
    rdt.clos_id = protobuf::MessageField::some(clos_id);
    linux.rdt = protobuf::MessageField::some(rdt);
    adjustment.linux = protobuf::MessageField::some(linux);

    RuntimeServiceImpl::sanitize_nri_adjustment_for_nri_config(
        &mut adjustment,
        &NriConfig::default(),
    );

    let linux = adjustment.linux.as_ref().unwrap();
    let resources = linux.resources.as_ref().unwrap();
    assert!(resources.blockio_class.is_none());
    if !Path::new("/sys/fs/resctrl").exists() {
        assert!(resources.rdt_class.is_none());
        assert!(linux.rdt.is_none());
    }
}

fn set_fake_runtime_state(dir: &TempDir, id: &str, state: &str) {
    let state_path = fake_runtime_state_path(dir, id);
    fs::create_dir_all(state_path.parent().unwrap()).unwrap();
    fs::write(&state_path, state).unwrap();
    let pid_path = dir.path().join("runtime-state").join(format!("{}.pid", id));
    if state == "running" {
        fs::write(pid_path, format!("{}", std::process::id())).unwrap();
    } else {
        let _ = fs::remove_file(pid_path);
    }
}

fn write_test_bundle_config(
    dir: &TempDir,
    container_id: &str,
    annotations: &HashMap<String, String>,
) {
    let bundle_dir = dir.path().join("runtime-root").join(container_id);
    fs::create_dir_all(&bundle_dir).unwrap();
    fs::write(
        bundle_dir.join("config.json"),
        serde_json::json!({
            "ociVersion": "1.0.2",
            "annotations": annotations,
            "root": {
                "path": "/tmp/rootfs"
            }
        })
        .to_string(),
    )
    .unwrap();
}

fn write_test_bundle_config_with_linux(
    dir: &TempDir,
    container_id: &str,
    annotations: &HashMap<String, String>,
    linux: serde_json::Value,
) {
    let bundle_dir = dir.path().join("runtime-root").join(container_id);
    fs::create_dir_all(&bundle_dir).unwrap();
    fs::write(
        bundle_dir.join("config.json"),
        serde_json::json!({
            "ociVersion": "1.0.2",
            "annotations": annotations,
            "root": {
                "path": "/tmp/rootfs"
            },
            "linux": linux,
        })
        .to_string(),
    )
    .unwrap();
}

fn test_container(
    id: &str,
    pod_sandbox_id: &str,
    annotations: HashMap<String, String>,
) -> Container {
    Container {
        id: id.to_string(),
        pod_sandbox_id: pod_sandbox_id.to_string(),
        state: ContainerState::ContainerCreated as i32,
        metadata: Some(ContainerMetadata {
            name: format!("{}-name", id),
            attempt: 1,
        }),
        image: Some(ImageSpec {
            image: "busybox:latest".to_string(),
            ..Default::default()
        }),
        image_ref: "busybox:latest".to_string(),
        annotations,
        created_at: RuntimeServiceImpl::now_nanos(),
        ..Default::default()
    }
}

fn test_pod(
    id: &str,
    annotations: HashMap<String, String>,
) -> crate::proto::runtime::v1::PodSandbox {
    crate::proto::runtime::v1::PodSandbox {
        id: id.to_string(),
        metadata: Some(PodSandboxMetadata {
            name: format!("{}-pod", id),
            uid: format!("{}-uid", id),
            namespace: "default".to_string(),
            attempt: 1,
        }),
        state: PodSandboxState::SandboxReady as i32,
        created_at: RuntimeServiceImpl::now_nanos(),
        labels: HashMap::new(),
        annotations,
        runtime_handler: "runc".to_string(),
    }
}

#[test]
fn pod_network_status_keeps_primary_and_additional_ips() {
    let state = StoredPodState {
        ip: Some("10.88.0.10".to_string()),
        additional_ips: vec![
            "10.88.0.11".to_string(),
            "10.88.0.10".to_string(),
            "".to_string(),
            "10.88.0.11".to_string(),
        ],
        ..Default::default()
    };

    let status = RuntimeServiceImpl::pod_network_status_from_state(Some(&state))
        .expect("expected network status");
    assert_eq!(status.ip, "10.88.0.10");
    assert_eq!(status.additional_ips.len(), 1);
    assert_eq!(status.additional_ips[0].ip, "10.88.0.11");
}

#[test]
fn pod_network_status_promotes_first_additional_ip_when_primary_missing() {
    let state = StoredPodState {
        additional_ips: vec!["fd00::10".to_string(), "10.88.0.11".to_string()],
        ..Default::default()
    };

    let status = RuntimeServiceImpl::pod_network_status_from_state(Some(&state))
        .expect("expected network status");
    assert_eq!(status.ip, "fd00::10");
    assert_eq!(status.additional_ips.len(), 1);
    assert_eq!(status.additional_ips[0].ip, "10.88.0.11");
}

#[test]
fn build_container_status_snapshot_uses_internal_timestamps_and_exit_code() {
    let mut annotations = HashMap::new();
    let stored = StoredContainerState {
        started_at: Some(5),
        finished_at: Some(8),
        exit_code: Some(42),
        mounts: vec![StoredMount {
            container_path: "/data".to_string(),
            host_path: "/host/data".to_string(),
            readonly: true,
            selinux_relabel: false,
            propagation: crate::proto::runtime::v1::MountPropagation::PropagationPrivate as i32,
        }],
        ..Default::default()
    };
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &stored,
    )
    .expect("store internal state");

    let container = Container {
        id: "container-1".to_string(),
        metadata: Some(ContainerMetadata {
            name: "c1".to_string(),
            attempt: 1,
        }),
        image: Some(ImageSpec {
            image: "busybox:latest".to_string(),
            ..Default::default()
        }),
        image_ref: "busybox:latest".to_string(),
        annotations,
        created_at: 1,
        ..Default::default()
    };

    let status = RuntimeServiceImpl::build_container_status_snapshot(
        &container,
        ContainerState::ContainerExited as i32,
    );

    assert_eq!(
        status.started_at,
        RuntimeServiceImpl::normalize_timestamp_nanos(5)
    );
    assert_eq!(
        status.finished_at,
        RuntimeServiceImpl::normalize_timestamp_nanos(8)
    );
    assert_eq!(status.exit_code, 42);
    assert_eq!(status.reason, "Error");
    assert_eq!(status.message, "container exited with code 42");
    assert_eq!(status.mounts.len(), 1);
    assert_eq!(status.mounts[0].container_path, "/data");
}

#[test]
fn selinux_label_from_proto_uses_defaults_for_missing_parts() {
    let label = RuntimeServiceImpl::selinux_label_from_proto(Some(
        &crate::proto::runtime::v1::SeLinuxOption {
            user: String::new(),
            role: String::new(),
            r#type: "spc_t".to_string(),
            level: String::new(),
        },
    ));

    assert_eq!(label.as_deref(), Some("system_u:system_r:spc_t:s0"));
}

#[test]
fn build_nri_container_from_proto_preserves_spec_cgroups_path() {
    let (_dir, service) = test_service_with_fake_runtime();
    let mut spec = crate::oci::spec::Spec::new("1.0.2");
    spec.linux = Some(crate::oci::spec::Linux {
        namespaces: None,
        uid_mappings: None,
        gid_mappings: None,
        devices: None,
        net_devices: None,
        cgroups_path: Some("/kubepods.slice/pod123/ctr.scope".to_string()),
        resources: None,
        rootfs_propagation: None,
        seccomp: None,
        sysctl: None,
        mount_label: None,
        intel_rdt: None,
    });
    service
        .runtime
        .write_bundle("container-cgroup-path", Path::new("/tmp"), &spec)
        .unwrap();

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            cgroup_parent: Some("kubepods.slice/pod123".to_string()),
            ..Default::default()
        },
    )
    .unwrap();

    let container = Container {
        id: "container-cgroup-path".to_string(),
        pod_sandbox_id: "pod-1".to_string(),
        annotations,
        ..Default::default()
    };

    let nri = RuntimeServiceImpl::build_nri_container_from_proto(&service.runtime, &container);
    let linux = nri.linux.unwrap();
    assert_eq!(linux.cgroups_path, "/kubepods.slice/pod123/ctr.scope");
}

#[test]
fn build_nri_container_from_proto_exposes_stored_seccomp_profile() {
    let (_dir, service) = test_service_with_fake_runtime();
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            seccomp_profile: Some(StoredSecurityProfile {
                profile_type: crate::proto::runtime::v1::security_profile::ProfileType::Localhost
                    as i32,
                localhost_ref: "/etc/seccomp/workload.json".to_string(),
            }),
            ..Default::default()
        },
    )
    .unwrap();

    let container = Container {
        id: "container-seccomp".to_string(),
        pod_sandbox_id: "pod-1".to_string(),
        annotations,
        ..Default::default()
    };

    let nri = RuntimeServiceImpl::build_nri_container_from_proto(&service.runtime, &container);
    let profile = nri
        .linux
        .as_ref()
        .and_then(|linux| linux.seccomp_profile.as_ref())
        .expect("seccomp profile should be present");
    assert_eq!(
        profile.profile_type.enum_value().unwrap(),
        crate::nri_proto::api::security_profile::ProfileType::LOCALHOST
    );
    assert_eq!(profile.localhost_ref, "/etc/seccomp/workload.json");
}

#[test]
fn build_nri_container_from_proto_exposes_paused_runtime_state() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "container-paused", "paused");

    let container = Container {
        id: "container-paused".to_string(),
        pod_sandbox_id: "pod-1".to_string(),
        state: ContainerState::ContainerRunning as i32,
        ..Default::default()
    };

    let nri = RuntimeServiceImpl::build_nri_container_from_proto(&service.runtime, &container);
    assert_eq!(
        nri.state.enum_value().unwrap(),
        crate::nri_proto::api::ContainerState::CONTAINER_PAUSED
    );
}

#[test]
fn build_nri_container_from_proto_merges_external_spec_annotations() {
    let (_dir, service) = test_service_with_fake_runtime();
    let mut spec = crate::oci::spec::Spec::new("1.0.2");
    let mut spec_annotations = HashMap::new();
    spec_annotations.insert("from-spec".to_string(), "visible".to_string());
    spec_annotations.insert(
        INTERNAL_CONTAINER_STATE_KEY.to_string(),
        "{\"hidden\":true}".to_string(),
    );
    spec.annotations = Some(spec_annotations);
    service
        .runtime
        .write_bundle("container-annotation-merge", Path::new("/tmp"), &spec)
        .unwrap();

    let mut annotations = HashMap::new();
    annotations.insert("from-cri".to_string(), "visible".to_string());
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();

    let container = Container {
        id: "container-annotation-merge".to_string(),
        pod_sandbox_id: "pod-1".to_string(),
        annotations,
        ..Default::default()
    };

    let nri = RuntimeServiceImpl::build_nri_container_from_proto(&service.runtime, &container);
    assert_eq!(
        nri.annotations.get("from-cri").map(String::as_str),
        Some("visible")
    );
    assert_eq!(
        nri.annotations.get("from-spec").map(String::as_str),
        Some("visible")
    );
    assert!(!nri.annotations.contains_key(INTERNAL_CONTAINER_STATE_KEY));
}

#[test]
fn build_nri_container_from_proto_merges_labels_from_spec_annotation() {
    let (_dir, service) = test_service_with_fake_runtime();
    let mut spec = crate::oci::spec::Spec::new("1.0.2");
    let mut spec_annotations = HashMap::new();
    spec_annotations.insert(
        CRIO_LABELS_ANNOTATION.to_string(),
        "{\"from-spec\":\"visible\"}".to_string(),
    );
    spec.annotations = Some(spec_annotations);
    service
        .runtime
        .write_bundle("container-label-merge", Path::new("/tmp"), &spec)
        .unwrap();

    let mut labels = HashMap::new();
    labels.insert("from-cri".to_string(), "visible".to_string());
    let container = Container {
        id: "container-label-merge".to_string(),
        pod_sandbox_id: "pod-1".to_string(),
        labels,
        ..Default::default()
    };

    let nri = RuntimeServiceImpl::build_nri_container_from_proto(&service.runtime, &container);
    assert_eq!(
        nri.labels.get("from-cri").map(String::as_str),
        Some("visible")
    );
    assert_eq!(
        nri.labels.get("from-spec").map(String::as_str),
        Some("visible")
    );
}

#[test]
fn build_nri_container_from_proto_falls_back_to_spec_identity_annotations() {
    let (_dir, service) = test_service_with_fake_runtime();
    let mut spec = crate::oci::spec::Spec::new("1.0.2");
    let mut spec_annotations = HashMap::new();
    spec_annotations.insert(
        CRIO_SANDBOX_ID_ANNOTATION.to_string(),
        "pod-from-spec".to_string(),
    );
    spec_annotations.insert(
        CONTAINERD_CONTAINER_NAME_ANNOTATION.to_string(),
        "container-from-spec".to_string(),
    );
    spec.annotations = Some(spec_annotations);
    service
        .runtime
        .write_bundle("container-identity-merge", Path::new("/tmp"), &spec)
        .unwrap();

    let container = Container {
        id: "container-identity-merge".to_string(),
        pod_sandbox_id: String::new(),
        ..Default::default()
    };

    let nri = RuntimeServiceImpl::build_nri_container_from_proto(&service.runtime, &container);
    assert_eq!(nri.pod_sandbox_id, "pod-from-spec");
    assert_eq!(nri.name, "container-from-spec");
}

#[test]
fn build_nri_pod_from_proto_merges_external_pause_spec_annotations() {
    let (_dir, service) = test_service_with_fake_runtime();
    let mut pause_spec = crate::oci::spec::Spec::new("1.0.2");
    let mut spec_annotations = HashMap::new();
    spec_annotations.insert("from-pause-spec".to_string(), "visible".to_string());
    spec_annotations.insert(
        INTERNAL_POD_STATE_KEY.to_string(),
        "{\"hidden\":true}".to_string(),
    );
    pause_spec.annotations = Some(spec_annotations);
    service
        .runtime
        .write_bundle("pause-annotation-merge", Path::new("/tmp"), &pause_spec)
        .unwrap();

    let mut annotations = HashMap::new();
    annotations.insert("from-cri".to_string(), "visible".to_string());
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            pause_container_id: Some("pause-annotation-merge".to_string()),
            ..Default::default()
        },
    )
    .unwrap();

    let pod = crate::proto::runtime::v1::PodSandbox {
        id: "pod-annotation-merge".to_string(),
        annotations,
        ..Default::default()
    };

    let nri = RuntimeServiceImpl::build_nri_pod_from_proto(&service.runtime, &pod);
    assert_eq!(
        nri.annotations.get("from-cri").map(String::as_str),
        Some("visible")
    );
    assert_eq!(
        nri.annotations.get("from-pause-spec").map(String::as_str),
        Some("visible")
    );
    assert!(!nri.annotations.contains_key(INTERNAL_POD_STATE_KEY));
}

#[test]
fn build_nri_pod_from_proto_merges_labels_from_pause_spec_annotation() {
    let (_dir, service) = test_service_with_fake_runtime();
    let mut pause_spec = crate::oci::spec::Spec::new("1.0.2");
    let mut spec_annotations = HashMap::new();
    spec_annotations.insert(
        CRIO_LABELS_ANNOTATION.to_string(),
        "{\"from-pause-spec\":\"visible\"}".to_string(),
    );
    pause_spec.annotations = Some(spec_annotations);
    service
        .runtime
        .write_bundle("pause-label-merge", Path::new("/tmp"), &pause_spec)
        .unwrap();

    let mut labels = HashMap::new();
    labels.insert("from-cri".to_string(), "visible".to_string());

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            pause_container_id: Some("pause-label-merge".to_string()),
            ..Default::default()
        },
    )
    .unwrap();

    let pod = crate::proto::runtime::v1::PodSandbox {
        id: "pod-label-merge".to_string(),
        labels,
        annotations,
        ..Default::default()
    };

    let nri = RuntimeServiceImpl::build_nri_pod_from_proto(&service.runtime, &pod);
    assert_eq!(
        nri.labels.get("from-cri").map(String::as_str),
        Some("visible")
    );
    assert_eq!(
        nri.labels.get("from-pause-spec").map(String::as_str),
        Some("visible")
    );
}

#[test]
fn build_nri_pod_from_proto_falls_back_to_pause_spec_identity_annotations() {
    let (_dir, service) = test_service_with_fake_runtime();
    let mut pause_spec = crate::oci::spec::Spec::new("1.0.2");
    let mut spec_annotations = HashMap::new();
    spec_annotations.insert(
        CONTAINERD_SANDBOX_UID_ANNOTATION.to_string(),
        "uid-from-spec".to_string(),
    );
    spec_annotations.insert(
        CONTAINERD_SANDBOX_NAMESPACE_ANNOTATION.to_string(),
        "ns-from-spec".to_string(),
    );
    spec_annotations.insert(
        CRIO_SANDBOX_NAME_ANNOTATION.to_string(),
        "pod-from-spec".to_string(),
    );
    pause_spec.annotations = Some(spec_annotations);
    service
        .runtime
        .write_bundle("pause-identity-merge", Path::new("/tmp"), &pause_spec)
        .unwrap();

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            pause_container_id: Some("pause-identity-merge".to_string()),
            ..Default::default()
        },
    )
    .unwrap();

    let pod = crate::proto::runtime::v1::PodSandbox {
        id: "pod-identity-merge".to_string(),
        annotations,
        ..Default::default()
    };

    let nri = RuntimeServiceImpl::build_nri_pod_from_proto(&service.runtime, &pod);
    assert_eq!(nri.name, "pod-from-spec");
    assert_eq!(nri.namespace, "ns-from-spec");
    assert_eq!(nri.uid, "uid-from-spec");
}

#[test]
fn seccomp_profile_from_proto_supports_localhost_and_unconfined() {
    let localhost_profile = RuntimeServiceImpl::seccomp_profile_from_proto(
        Some(&crate::proto::runtime::v1::SecurityProfile {
            profile_type: crate::proto::runtime::v1::security_profile::ProfileType::Localhost
                as i32,
            localhost_ref: "/tmp/seccomp/profile.json".to_string(),
        }),
        "",
    );
    assert!(matches!(
        localhost_profile,
        Some(SeccompProfile::Localhost(_))
    ));

    let unconfined_profile = RuntimeServiceImpl::seccomp_profile_from_proto(
        Some(&crate::proto::runtime::v1::SecurityProfile {
            profile_type: crate::proto::runtime::v1::security_profile::ProfileType::Unconfined
                as i32,
            localhost_ref: String::new(),
        }),
        "",
    );
    assert!(matches!(
        unconfined_profile,
        Some(SeccompProfile::Unconfined)
    ));
}

#[tokio::test]
async fn resolve_container_id_handles_zero_single_and_multiple_matches() {
    let service = test_service();
    service.containers.lock().await.insert(
        "abcdef123456".to_string(),
        test_container("abcdef123456", "pod-1", HashMap::new()),
    );
    service.containers.lock().await.insert(
        "abc999999999".to_string(),
        test_container("abc999999999", "pod-1", HashMap::new()),
    );

    assert_eq!(
        service.resolve_container_id("abcdef").await.unwrap(),
        "abcdef123456"
    );
    assert_eq!(
        service
            .resolve_container_id("missing")
            .await
            .unwrap_err()
            .code(),
        tonic::Code::NotFound
    );
    assert_eq!(
        service
            .resolve_container_id("abc")
            .await
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
}

#[tokio::test]
async fn resolve_pod_sandbox_id_handles_zero_single_and_multiple_matches() {
    let service = test_service();
    service.pod_sandboxes.lock().await.insert(
        "podabcdef123456".to_string(),
        test_pod("podabcdef123456", HashMap::new()),
    );
    service.pod_sandboxes.lock().await.insert(
        "podabc999999999".to_string(),
        test_pod("podabc999999999", HashMap::new()),
    );

    assert_eq!(
        service.resolve_pod_sandbox_id("podabcdef").await.unwrap(),
        "podabcdef123456"
    );
    assert_eq!(
        service
            .resolve_pod_sandbox_id("missing")
            .await
            .unwrap_err()
            .code(),
        tonic::Code::NotFound
    );
    assert_eq!(
        service
            .resolve_pod_sandbox_id("podabc")
            .await
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
}

#[tokio::test]
async fn stop_and_remove_container_are_idempotent_when_missing() {
    let service = test_service();

    let stop = RuntimeService::stop_container(
        &service,
        Request::new(StopContainerRequest {
            container_id: "missing".to_string(),
            timeout: 0,
        }),
    )
    .await;
    assert!(stop.is_ok());

    let remove = RuntimeService::remove_container(
        &service,
        Request::new(RemoveContainerRequest {
            container_id: "missing".to_string(),
        }),
    )
    .await;
    assert!(remove.is_ok());
}

#[tokio::test]
async fn stop_and_remove_pod_are_idempotent_when_missing() {
    let service = test_service();

    let stop = RuntimeService::stop_pod_sandbox(
        &service,
        Request::new(StopPodSandboxRequest {
            pod_sandbox_id: "missing".to_string(),
        }),
    )
    .await;
    assert!(stop.is_ok());

    let remove = RuntimeService::remove_pod_sandbox(
        &service,
        Request::new(RemovePodSandboxRequest {
            pod_sandbox_id: "missing".to_string(),
        }),
    )
    .await;
    assert!(remove.is_ok());
}

#[tokio::test]
async fn exec_validates_container_is_streamable() {
    let (dir, service) = test_service_with_fake_runtime();
    service
        .set_streaming_server(crate::streaming::StreamingServer::for_test(
            "http://127.0.0.1:12345",
        ))
        .await;

    let missing = RuntimeService::exec(
        &service,
        Request::new(ExecRequest {
            container_id: "missing".to_string(),
            cmd: vec!["sh".to_string()],
            stdin: false,
            stdout: true,
            stderr: true,
            tty: false,
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(missing.code(), tonic::Code::NotFound);

    service.containers.lock().await.insert(
        "container-stopped".to_string(),
        test_container("container-stopped", "pod-1", HashMap::new()),
    );
    set_fake_runtime_state(&dir, "container-stopped", "stopped");
    let stopped = RuntimeService::exec(
        &service,
        Request::new(ExecRequest {
            container_id: "container-stopped".to_string(),
            cmd: vec!["sh".to_string()],
            stdin: false,
            stdout: true,
            stderr: true,
            tty: false,
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(stopped.code(), tonic::Code::FailedPrecondition);

    service.containers.lock().await.insert(
        "container-created".to_string(),
        test_container("container-created", "pod-1", HashMap::new()),
    );
    set_fake_runtime_state(&dir, "container-created", "created");
    let response = RuntimeService::exec(
        &service,
        Request::new(ExecRequest {
            container_id: "container-created".to_string(),
            cmd: vec!["sh".to_string()],
            stdin: false,
            stdout: true,
            stderr: true,
            tty: false,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(response.url.contains("/exec/"));
}

#[tokio::test]
async fn exec_sync_validates_container_is_streamable() {
    let (dir, service) = test_service_with_fake_runtime();

    let missing = RuntimeService::exec_sync(
        &service,
        Request::new(ExecSyncRequest {
            container_id: "missing".to_string(),
            cmd: vec!["true".to_string()],
            timeout: 0,
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(missing.code(), tonic::Code::NotFound);

    service.containers.lock().await.insert(
        "container-stopped".to_string(),
        test_container("container-stopped", "pod-1", HashMap::new()),
    );
    set_fake_runtime_state(&dir, "container-stopped", "stopped");
    let stopped = RuntimeService::exec_sync(
        &service,
        Request::new(ExecSyncRequest {
            container_id: "container-stopped".to_string(),
            cmd: vec!["true".to_string()],
            timeout: 0,
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(stopped.code(), tonic::Code::FailedPrecondition);

    service.containers.lock().await.insert(
        "container-running".to_string(),
        test_container("container-running", "pod-1", HashMap::new()),
    );
    set_fake_runtime_state(&dir, "container-running", "running");
    let response = RuntimeService::exec_sync(
        &service,
        Request::new(ExecSyncRequest {
            container_id: "container-running".to_string(),
            cmd: vec!["true".to_string()],
            timeout: 0,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(response.exit_code, 0);
}

#[tokio::test]
async fn attach_validates_container_is_streamable() {
    let (dir, service) = test_service_with_fake_runtime();
    service
        .set_streaming_server(crate::streaming::StreamingServer::for_test(
            "http://127.0.0.1:12345",
        ))
        .await;

    let missing = RuntimeService::attach(
        &service,
        Request::new(AttachRequest {
            container_id: "missing".to_string(),
            stdin: false,
            stdout: true,
            stderr: true,
            tty: false,
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(missing.code(), tonic::Code::NotFound);

    service.containers.lock().await.insert(
        "container-stopped".to_string(),
        test_container("container-stopped", "pod-1", HashMap::new()),
    );
    set_fake_runtime_state(&dir, "container-stopped", "stopped");
    let stopped = RuntimeService::attach(
        &service,
        Request::new(AttachRequest {
            container_id: "container-stopped".to_string(),
            stdin: false,
            stdout: true,
            stderr: true,
            tty: false,
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(stopped.code(), tonic::Code::FailedPrecondition);

    service.containers.lock().await.insert(
        "container-running".to_string(),
        test_container("container-running", "pod-1", HashMap::new()),
    );
    set_fake_runtime_state(&dir, "container-running", "running");
    let attach_socket = dir
        .path()
        .join("shims")
        .join("container-running")
        .join("attach.sock");
    fs::create_dir_all(attach_socket.parent().unwrap()).unwrap();
    fs::write(&attach_socket, "").unwrap();
    let response = RuntimeService::attach(
        &service,
        Request::new(AttachRequest {
            container_id: "container-running".to_string(),
            stdin: false,
            stdout: true,
            stderr: true,
            tty: false,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(response.url.contains("/attach/"));
}

#[tokio::test]
async fn attach_uses_log_stream_fallback_when_recovered_socket_is_missing() {
    let (dir, service) = test_service_with_fake_runtime();
    service
        .set_streaming_server(crate::streaming::StreamingServer::for_test(
            "http://127.0.0.1:12345",
        ))
        .await;

    set_fake_runtime_state(&dir, "recover-container", "running");
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            log_path: Some(
                dir.path()
                    .join("logs")
                    .join("recover.log")
                    .display()
                    .to_string(),
            ),
            ..Default::default()
        },
    )
    .unwrap();
    service
        .persistence
        .lock()
        .await
        .save_container(
            "recover-container",
            "pod-1",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    service.containers.lock().await.insert(
        "recover-container".to_string(),
        test_container("recover-container", "pod-1", annotations),
    );

    service.recover_state().await.unwrap();

    let response = RuntimeService::attach(
        &service,
        Request::new(AttachRequest {
            container_id: "recover-container".to_string(),
            stdin: false,
            stdout: true,
            stderr: true,
            tty: false,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(response.url.contains("/attach/"));
}

#[tokio::test]
async fn attach_uses_log_stream_fallback_for_read_only_tty_when_socket_is_missing() {
    let (dir, service) = test_service_with_fake_runtime();
    service
        .set_streaming_server(crate::streaming::StreamingServer::for_test(
            "http://127.0.0.1:12345",
        ))
        .await;

    set_fake_runtime_state(&dir, "recover-container-tty", "running");
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            log_path: Some(
                dir.path()
                    .join("logs")
                    .join("recover-tty.log")
                    .display()
                    .to_string(),
            ),
            tty: true,
            ..Default::default()
        },
    )
    .unwrap();
    service.containers.lock().await.insert(
        "recover-container-tty".to_string(),
        test_container("recover-container-tty", "pod-1", annotations),
    );

    let response = RuntimeService::attach(
        &service,
        Request::new(AttachRequest {
            container_id: "recover-container-tty".to_string(),
            stdin: false,
            stdout: true,
            stderr: false,
            tty: true,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(response.url.contains("/attach/"));
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn attach_recreates_tty_shim_when_socket_is_missing() {
    let _guard = env_lock().lock().unwrap();
    let dir = tempdir().unwrap();
    let runtime_path = write_fake_runtime_script(dir.path());
    let shim_work_dir = dir.path().join("shims");
    let fake_shim_path = dir.path().join("fake-shim.sh");
    fs::write(
        &fake_shim_path,
        r#"#!/bin/sh
set -eu
exit_code_file=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --exit-code-file)
      exit_code_file="${2:-}"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done
shim_dir="$(dirname "$exit_code_file")"
mkdir -p "$shim_dir"
: > "$shim_dir/attach.sock"
: > "$shim_dir/resize.sock"
sleep 1
"#,
    )
    .unwrap();
    let mut perms = fs::metadata(&fake_shim_path).unwrap().permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&fake_shim_path, perms).unwrap();

    std::env::set_var("CRIUS_SHIM_PATH", &fake_shim_path);
    let service = RuntimeServiceImpl::new_with_shim_work_dir(
        RuntimeConfig {
            root_dir: dir.path().join("root"),
            runtime: "runc".to_string(),
            runtime_handlers: vec!["runc".to_string()],
            runtime_root: dir.path().join("runtime-root"),
            log_dir: dir.path().join("logs"),
            runtime_path,
            pause_image: "registry.k8s.io/pause:3.9".to_string(),
            cni_config: crate::network::CniConfig::default(),
        },
        NriConfig::default(),
        shim_work_dir.clone(),
    );
    std::env::remove_var("CRIUS_SHIM_PATH");
    service
        .set_streaming_server(crate::streaming::StreamingServer::for_test(
            "http://127.0.0.1:12345",
        ))
        .await;

    set_fake_runtime_state(&dir, "tty-restore", "running");
    let bundle_dir = dir.path().join("runtime-root").join("tty-restore");
    fs::create_dir_all(&bundle_dir).unwrap();
    fs::write(
        bundle_dir.join("config.json"),
        serde_json::json!({
            "ociVersion": "1.0.2",
            "process": {
                "terminal": true
            }
        })
        .to_string(),
    )
    .unwrap();

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            tty: true,
            ..Default::default()
        },
    )
    .unwrap();
    service.containers.lock().await.insert(
        "tty-restore".to_string(),
        Container {
            state: ContainerState::ContainerRunning as i32,
            ..test_container("tty-restore", "pod-1", annotations)
        },
    );

    let response = RuntimeService::attach(
        &service,
        Request::new(AttachRequest {
            container_id: "tty-restore".to_string(),
            stdin: true,
            stdout: true,
            stderr: false,
            tty: true,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(response.url.contains("/attach/"));
    assert!(shim_work_dir
        .join("tty-restore")
        .join("attach.sock")
        .exists());
}

#[tokio::test]
async fn attach_returns_error_when_socket_missing_and_interactive_recovery_is_requested() {
    let (dir, service) = test_service_with_fake_runtime();
    service
        .set_streaming_server(crate::streaming::StreamingServer::for_test(
            "http://127.0.0.1:12345",
        ))
        .await;

    set_fake_runtime_state(&dir, "recover-container", "running");
    service.containers.lock().await.insert(
        "recover-container".to_string(),
        test_container("recover-container", "pod-1", HashMap::new()),
    );

    let err = RuntimeService::attach(
        &service,
        Request::new(AttachRequest {
            container_id: "recover-container".to_string(),
            stdin: true,
            stdout: true,
            stderr: false,
            tty: true,
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("interactive recovery"));
}

#[tokio::test]
async fn container_status_verbose_returns_json_info() {
    let (dir, service) = test_service_with_fake_runtime();
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            log_path: Some("/var/log/pods/c1.log".to_string()),
            tty: true,
            privileged: true,
            mounts: vec![StoredMount {
                container_path: "/data".to_string(),
                host_path: "/host/data".to_string(),
                readonly: false,
                selinux_relabel: false,
                propagation: crate::proto::runtime::v1::MountPropagation::PropagationPrivate as i32,
            }],
            run_as_user: Some("1000".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    service.containers.lock().await.insert(
        "container-1".to_string(),
        test_container("container-1", "pod-1", annotations),
    );
    set_fake_runtime_state(&dir, "container-1", "running");
    let bundle_dir = dir.path().join("runtime-root").join("container-1");
    fs::create_dir_all(&bundle_dir).unwrap();
    fs::write(
        bundle_dir.join("config.json"),
        serde_json::json!({
            "ociVersion": "1.0.2",
            "process": {
                "terminal": true,
                "cwd": "/"
            }
        })
        .to_string(),
    )
    .unwrap();

    let response = RuntimeService::container_status(
        &service,
        Request::new(ContainerStatusRequest {
            container_id: "container-1".to_string(),
            verbose: true,
        }),
    )
    .await
    .unwrap()
    .into_inner();

    assert!(response.info.contains_key("info"));
    let info: serde_json::Value = serde_json::from_str(response.info.get("info").unwrap()).unwrap();
    assert_eq!(info["id"], "container-1");
    assert_eq!(info["sandboxID"], "pod-1");
    assert_eq!(info["privileged"], true);
    assert_eq!(info["user"], "1000");
    assert_eq!(info["runtimeSpec"]["process"]["cwd"], "/");
    assert_eq!(response.status.unwrap().mounts.len(), 1);
}

#[tokio::test]
async fn pod_sandbox_status_verbose_returns_json_info() {
    let (dir, service) = test_service_with_fake_runtime();
    let mut annotations = HashMap::new();
    annotations.insert(
        "io.kubernetes.cri.sandbox-image".to_string(),
        "registry.k8s.io/pause:3.10".to_string(),
    );
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            netns_path: Some("/var/run/netns/test-pod".to_string()),
            pause_container_id: Some("pause-1".to_string()),
            log_directory: Some("/var/log/pods/test-pod".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    service
        .pod_sandboxes
        .lock()
        .await
        .insert("pod-1".to_string(), test_pod("pod-1", annotations));
    set_fake_runtime_state(&dir, "pause-1", "running");
    let bundle_dir = dir.path().join("runtime-root").join("pause-1");
    fs::create_dir_all(&bundle_dir).unwrap();
    fs::write(
        bundle_dir.join("config.json"),
        serde_json::json!({
            "ociVersion": "1.0.2",
            "root": {
                "path": "rootfs"
            }
        })
        .to_string(),
    )
    .unwrap();

    let response = RuntimeService::pod_sandbox_status(
        &service,
        Request::new(PodSandboxStatusRequest {
            pod_sandbox_id: "pod-1".to_string(),
            verbose: true,
        }),
    )
    .await
    .unwrap()
    .into_inner();

    assert!(response.info.contains_key("info"));
    let info: serde_json::Value = serde_json::from_str(response.info.get("info").unwrap()).unwrap();
    assert_eq!(info["id"], "pod-1");
    assert_eq!(info["image"], "registry.k8s.io/pause:3.10");
    assert_eq!(info["pauseContainerId"], "pause-1");
    assert!(info["pid"].is_number());
    assert_eq!(info["runtimeSpec"]["root"]["path"], "rootfs");
}

#[tokio::test]
async fn status_verbose_returns_structured_config() {
    let service = test_service();
    let response = RuntimeService::status(&service, Request::new(StatusRequest { verbose: true }))
        .await
        .unwrap()
        .into_inner();

    assert!(response.info.contains_key("config"));
    let config: serde_json::Value =
        serde_json::from_str(response.info.get("config").unwrap()).unwrap();
    assert_eq!(config["runtimeName"], "crius");
    assert!(!config["runtimeHandlers"].as_array().unwrap().is_empty());
    assert_eq!(config["recovery"]["startupReconcile"], true);
    assert_eq!(config["recovery"]["eventReplayOnRecovery"], false);
    assert_eq!(config["runtimeFeatures"]["updateContainerResources"], true);
    assert_eq!(config["runtimeFeatures"]["containerStats"], true);
    assert_eq!(config["runtimeFeatures"]["podSandboxStats"], true);
    assert_eq!(config["runtimeFeatures"]["podSandboxMetrics"], true);
    assert_eq!(
        response.status.unwrap().conditions.len(),
        2,
        "expected runtime and network conditions"
    );
}

#[tokio::test]
async fn version_reports_runtime_binary_version_when_available() {
    let dir = tempdir().unwrap();
    let runtime_path = dir.path().join("fake-runtime-version.sh");
    fs::write(
        &runtime_path,
        r#"#!/bin/sh
if [ "$1" = "--version" ]; then
  echo "runc version 1.2.3"
  exit 0
fi
exit 0
"#,
    )
    .unwrap();
    let mut perms = fs::metadata(&runtime_path).unwrap().permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&runtime_path, perms).unwrap();

    let service = RuntimeServiceImpl::new(RuntimeConfig {
        runtime_path: runtime_path.clone(),
        ..test_runtime_config(dir.path().join("root"))
    });
    let response = RuntimeService::version(
        &service,
        Request::new(VersionRequest {
            version: "0.1.0".to_string(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(response.runtime_name, "runc");
    assert_eq!(response.runtime_version, "runc version 1.2.3");
}

#[test]
fn parse_cgroup_hint_from_procfs_prefers_relevant_controller_or_unified_line() {
    let v1 = "11:hugetlb:/\n10:memory:/kubepods.slice/pod123/container.scope\n9:cpuset:/\n";
    assert_eq!(
        RuntimeServiceImpl::parse_cgroup_hint_from_procfs(v1),
        Some(PathBuf::from("/kubepods.slice/pod123/container.scope"))
    );

    let v2 = "0::/user.slice/user-0.slice/session-1.scope\n";
    assert_eq!(
        RuntimeServiceImpl::parse_cgroup_hint_from_procfs(v2),
        Some(PathBuf::from("/user.slice/user-0.slice/session-1.scope"))
    );
}

#[tokio::test]
async fn status_reports_runtime_not_ready_when_binary_is_not_executable() {
    let dir = tempdir().unwrap();
    let runtime_path = dir.path().join("fake-runtime");
    fs::write(&runtime_path, "#!/bin/sh\nexit 0\n").unwrap();
    let mut perms = fs::metadata(&runtime_path).unwrap().permissions();
    perms.set_mode(0o644);
    fs::set_permissions(&runtime_path, perms).unwrap();

    let service = RuntimeServiceImpl::new(RuntimeConfig {
        runtime_path: runtime_path.clone(),
        ..test_runtime_config(dir.path().join("root"))
    });
    let response = RuntimeService::status(&service, Request::new(StatusRequest { verbose: false }))
        .await
        .unwrap()
        .into_inner();
    let runtime_condition = response
        .status
        .unwrap()
        .conditions
        .into_iter()
        .find(|condition| condition.r#type == "RuntimeReady")
        .unwrap();
    assert!(!runtime_condition.status);
    assert_eq!(runtime_condition.reason, "RuntimeBinaryNotExecutable");
}

#[tokio::test]
async fn runtime_config_reports_detected_cgroup_driver() {
    let service = test_service();
    let response = RuntimeService::runtime_config(&service, Request::new(RuntimeConfigRequest {}))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        response.linux.unwrap().cgroup_driver,
        service.cgroup_driver() as i32
    );
}

#[tokio::test]
async fn update_runtime_config_persists_network_config_and_exposes_it_via_status() {
    let root_dir = tempdir().unwrap().keep();
    let service = RuntimeServiceImpl::new(test_runtime_config(root_dir.clone()));

    RuntimeService::update_runtime_config(
        &service,
        Request::new(UpdateRuntimeConfigRequest {
            runtime_config: Some(crate::proto::runtime::v1::RuntimeConfig {
                network_config: Some(crate::proto::runtime::v1::NetworkConfig {
                    pod_cidr: "10.244.0.0/16".to_string(),
                }),
            }),
        }),
    )
    .await
    .unwrap();

    let status = RuntimeService::status(&service, Request::new(StatusRequest { verbose: true }))
        .await
        .unwrap()
        .into_inner();
    let config: serde_json::Value =
        serde_json::from_str(status.info.get("config").unwrap()).unwrap();
    assert_eq!(config["runtimeNetworkConfig"]["podCIDR"], "10.244.0.0/16");

    let reloaded = RuntimeServiceImpl::new(test_runtime_config(root_dir));
    let reloaded_status =
        RuntimeService::status(&reloaded, Request::new(StatusRequest { verbose: true }))
            .await
            .unwrap()
            .into_inner();
    let reloaded_config: serde_json::Value =
        serde_json::from_str(reloaded_status.info.get("config").unwrap()).unwrap();
    assert_eq!(
        reloaded_config["runtimeNetworkConfig"]["podCIDR"],
        "10.244.0.0/16"
    );
}

#[tokio::test]
async fn run_pod_sandbox_persists_runtime_pod_cidr_in_verbose_info() {
    let (dir, service) = test_service_with_fake_runtime();
    RuntimeService::update_runtime_config(
        &service,
        Request::new(UpdateRuntimeConfigRequest {
            runtime_config: Some(crate::proto::runtime::v1::RuntimeConfig {
                network_config: Some(crate::proto::runtime::v1::NetworkConfig {
                    pod_cidr: "10.88.0.0/16".to_string(),
                }),
            }),
        }),
    )
    .await
    .unwrap();

    let netns_path = dir.path().join("pod-runtime-cidr.netns");
    fs::write(&netns_path, "netns").unwrap();
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            runtime_handler: "runc".to_string(),
            runtime_pod_cidr: Some("10.88.0.0/16".to_string()),
            netns_path: Some(netns_path.display().to_string()),
            pause_container_id: Some("pause-runtime-cidr".to_string()),
            ..Default::default()
        },
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-runtime-cidr".to_string(),
        test_pod("pod-runtime-cidr", annotations),
    );

    let response = RuntimeService::pod_sandbox_status(
        &service,
        Request::new(PodSandboxStatusRequest {
            pod_sandbox_id: "pod-runtime-cidr".to_string(),
            verbose: true,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let info: serde_json::Value = serde_json::from_str(response.info.get("info").unwrap()).unwrap();
    assert_eq!(info["runtimePodCIDR"], "10.88.0.0/16");
}

#[test]
fn network_health_requires_declared_plugin_binary() {
    let _guard = env_lock().lock().unwrap();
    let dir = tempdir().unwrap();
    let config_dir = dir.path().join("cni-conf");
    let plugin_dir = dir.path().join("cni-bin");
    fs::create_dir_all(&config_dir).unwrap();
    fs::create_dir_all(&plugin_dir).unwrap();
    fs::write(
        config_dir.join("10-test.conflist"),
        r#"{"cniVersion":"0.4.0","name":"test","plugins":[{"type":"bridge"}]}"#,
    )
    .unwrap();

    std::env::set_var("CRIUS_CNI_CONFIG_DIRS", config_dir.display().to_string());
    std::env::set_var("CRIUS_CNI_PLUGIN_DIRS", plugin_dir.display().to_string());
    let service = test_service_with_env_cni();
    let (ready, reason, message) = service.network_health();
    std::env::remove_var("CRIUS_CNI_CONFIG_DIRS");
    std::env::remove_var("CRIUS_CNI_PLUGIN_DIRS");

    assert!(!ready);
    assert_eq!(reason, "CNIPluginMissing");
    assert!(message.contains("bridge"));
}

#[test]
fn network_health_requires_plugin_to_be_executable() {
    let _guard = env_lock().lock().unwrap();
    let dir = tempdir().unwrap();
    let config_dir = dir.path().join("cni-conf");
    let plugin_dir = dir.path().join("cni-bin");
    fs::create_dir_all(&config_dir).unwrap();
    fs::create_dir_all(&plugin_dir).unwrap();
    fs::write(
        config_dir.join("10-test.conflist"),
        r#"{"cniVersion":"0.4.0","name":"test","plugins":[{"type":"bridge"}]}"#,
    )
    .unwrap();
    let plugin_path = plugin_dir.join("bridge");
    fs::write(&plugin_path, "#!/bin/sh\nexit 0\n").unwrap();
    let mut perms = fs::metadata(&plugin_path).unwrap().permissions();
    perms.set_mode(0o644);
    fs::set_permissions(&plugin_path, perms).unwrap();

    std::env::set_var("CRIUS_CNI_CONFIG_DIRS", config_dir.display().to_string());
    std::env::set_var("CRIUS_CNI_PLUGIN_DIRS", plugin_dir.display().to_string());
    let service = test_service_with_env_cni();
    let (ready, reason, message) = service.network_health();
    std::env::remove_var("CRIUS_CNI_CONFIG_DIRS");
    std::env::remove_var("CRIUS_CNI_PLUGIN_DIRS");

    assert!(!ready);
    assert_eq!(reason, "CNIPluginMissing");
    assert!(message.contains("bridge"));
    assert!(message.contains("non-executable"));
}

#[tokio::test]
async fn checkpoint_container_writes_checkpoint_artifact() {
    let (dir, service) = test_service_with_fake_runtime();
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            log_path: Some(dir.path().join("logs").join("c1.log").display().to_string()),
            metadata_name: Some("checkpointed".to_string()),
            metadata_attempt: Some(2),
            started_at: Some(10),
            ..Default::default()
        },
    )
    .unwrap();

    service.containers.lock().await.insert(
        "container-running".to_string(),
        test_container("container-running", "pod-1", annotations),
    );
    set_fake_runtime_state(&dir, "container-running", "running");

    let bundle_dir = dir.path().join("runtime-root").join("container-running");
    fs::create_dir_all(&bundle_dir).unwrap();
    let rootfs_dir = dir.path().join("checkpoint-rootfs-json");
    fs::create_dir_all(&rootfs_dir).unwrap();
    fs::write(
        bundle_dir.join("config.json"),
        serde_json::json!({
            "ociVersion": "1.0.2",
            "root": {
                "path": rootfs_dir.display().to_string()
            },
        })
        .to_string(),
    )
    .unwrap();

    let artifact_path = dir.path().join("checkpoint.json");
    RuntimeService::checkpoint_container(
        &service,
        Request::new(CheckpointContainerRequest {
            container_id: "container-running".to_string(),
            location: artifact_path.display().to_string(),
            timeout: 30,
        }),
    )
    .await
    .unwrap();

    let artifact: serde_json::Value =
        serde_json::from_slice(&fs::read(&artifact_path).unwrap()).unwrap();
    assert_eq!(artifact["manifest"]["containerId"], "container-running");
    assert_eq!(artifact["manifest"]["runtimeState"], "running");
    assert_eq!(artifact["manifest"]["metadata"]["name"], "checkpointed");
    assert_eq!(artifact["manifest"]["metadata"]["attempt"], 2);
    assert_eq!(
        artifact["ociConfig"]["root"]["path"],
        rootfs_dir.display().to_string()
    );
    let checkpoint_image_path = PathBuf::from(
        artifact["manifest"]["checkpointImagePath"]
            .as_str()
            .expect("checkpoint image path should be recorded"),
    );
    assert!(checkpoint_image_path.join("checkpoint.json").exists());
    assert!(checkpoint_image_path.join("rootfs.tar").exists());
}

#[tokio::test]
async fn checkpoint_container_writes_tar_export_when_location_is_archive() {
    let (dir, service) = test_service_with_fake_runtime();
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            metadata_name: Some("checkpointed-tar".to_string()),
            metadata_attempt: Some(1),
            ..Default::default()
        },
    )
    .unwrap();

    service.containers.lock().await.insert(
        "container-running".to_string(),
        test_container("container-running", "pod-1", annotations),
    );
    set_fake_runtime_state(&dir, "container-running", "running");

    let bundle_dir = dir.path().join("runtime-root").join("container-running");
    fs::create_dir_all(&bundle_dir).unwrap();
    let rootfs_dir = dir.path().join("checkpoint-rootfs-tar");
    fs::create_dir_all(&rootfs_dir).unwrap();
    fs::write(
        bundle_dir.join("config.json"),
        serde_json::json!({
            "ociVersion": "1.0.2",
            "root": {
                "path": rootfs_dir.display().to_string()
            },
        })
        .to_string(),
    )
    .unwrap();

    let artifact_path = dir.path().join("checkpoint.tar");
    RuntimeService::checkpoint_container(
        &service,
        Request::new(CheckpointContainerRequest {
            container_id: "container-running".to_string(),
            location: artifact_path.display().to_string(),
            timeout: 30,
        }),
    )
    .await
    .unwrap();

    let tar_output = Command::new("tar")
        .args(["-tf", artifact_path.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(tar_output.status.success());
    let stdout = String::from_utf8_lossy(&tar_output.stdout);
    assert!(stdout.contains("manifest.json"));
    assert!(stdout.contains("config.json"));
    assert!(stdout.contains("checkpoint.json"));
    assert!(stdout.contains("rootfs.tar"));
}

#[tokio::test]
async fn start_container_restores_from_checkpoint_artifact_and_clears_pending_marker() {
    let (dir, service) = test_service_with_fake_runtime();

    let checkpoint_location = dir.path().join("restore-artifact.json");
    let checkpoint_image_path =
        RuntimeServiceImpl::checkpoint_runtime_image_path(&checkpoint_location);
    fs::create_dir_all(&checkpoint_image_path).unwrap();
    fs::write(checkpoint_image_path.join("checkpoint.json"), "{}").unwrap();
    fs::write(
        &checkpoint_location,
        serde_json::to_vec_pretty(&serde_json::json!({
            "manifest": {
                "imageRef": "busybox:latest",
                "checkpointImagePath": checkpoint_image_path.display().to_string(),
            },
            "ociConfig": {
                "ociVersion": "1.0.2",
                "root": {
                    "path": "/tmp/rootfs"
                }
            }
        }))
        .unwrap(),
    )
    .unwrap();

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CHECKPOINT_RESTORE_KEY,
        &StoredCheckpointRestore {
            checkpoint_location: checkpoint_location.display().to_string(),
            checkpoint_image_path: checkpoint_image_path.display().to_string(),
            oci_config: serde_json::json!({
                "ociVersion": "1.0.2",
                "root": {
                    "path": "/tmp/rootfs"
                }
            }),
            image_ref: "busybox:latest".to_string(),
        },
    )
    .unwrap();

    service.containers.lock().await.insert(
        "restore-container".to_string(),
        Container {
            state: ContainerState::ContainerCreated as i32,
            ..test_container("restore-container", "pod-1", annotations.clone())
        },
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "restore-container",
            "pod-1",
            crate::runtime::ContainerStatus::Created,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();

    let bundle_dir = dir.path().join("runtime-root").join("restore-container");
    fs::create_dir_all(&bundle_dir).unwrap();
    fs::write(
        bundle_dir.join("config.json"),
        serde_json::json!({
            "ociVersion": "1.0.2",
            "annotations": annotations,
            "root": {
                "path": "/tmp/rootfs"
            }
        })
        .to_string(),
    )
    .unwrap();

    RuntimeService::start_container(
        &service,
        Request::new(StartContainerRequest {
            container_id: "restore-container".to_string(),
        }),
    )
    .await
    .unwrap();

    let container = service
        .containers
        .lock()
        .await
        .get("restore-container")
        .cloned()
        .unwrap();
    assert_eq!(container.state, ContainerState::ContainerRunning as i32);
    let internal_state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
        &container.annotations,
        INTERNAL_CONTAINER_STATE_KEY,
    )
    .expect("started_at should be persisted after restore");
    assert!(internal_state.started_at.is_some());
    assert!(
        !container
            .annotations
            .contains_key(INTERNAL_CHECKPOINT_RESTORE_KEY),
        "restore marker should be cleared after successful restore"
    );

    let persisted = service
        .persistence
        .lock()
        .await
        .storage()
        .get_container("restore-container")
        .unwrap()
        .unwrap();
    let persisted_annotations: HashMap<String, String> =
        serde_json::from_str(&persisted.annotations).unwrap();
    assert!(
        !persisted_annotations.contains_key(INTERNAL_CHECKPOINT_RESTORE_KEY),
        "restore marker should be cleared from persistence after successful restore"
    );

    let bundle_config: serde_json::Value = serde_json::from_slice(
        &fs::read(
            dir.path()
                .join("runtime-root")
                .join("restore-container")
                .join("config.json"),
        )
        .unwrap(),
    )
    .unwrap();
    let bundle_annotations = bundle_config
        .get("annotations")
        .and_then(|value| value.as_object())
        .cloned()
        .unwrap_or_default();
    assert!(!bundle_annotations.contains_key(INTERNAL_CHECKPOINT_RESTORE_KEY));
    let bundle_container_state: StoredContainerState = serde_json::from_str(
        bundle_annotations
            .get(INTERNAL_CONTAINER_STATE_KEY)
            .and_then(|value| value.as_str())
            .expect("bundle should persist internal container state"),
    )
    .unwrap();
    assert!(bundle_container_state.started_at.is_some());

    let exit_code_path = dir
        .path()
        .join("shims")
        .join("restore-container")
        .join("exit_code");
    fs::create_dir_all(exit_code_path.parent().unwrap()).unwrap();
    fs::write(&exit_code_path, "0").unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
}

#[tokio::test]
async fn start_container_notifies_nri_before_and_after_runtime_start() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-start".to_string(),
        test_pod("pod-start", HashMap::new()),
    );
    service.containers.lock().await.insert(
        "container-start".to_string(),
        test_container("container-start", "pod-start", annotations.clone()),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-start",
            "pod-start",
            crate::runtime::ContainerStatus::Created,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    write_test_bundle_config(&dir, "container-start", &annotations);

    RuntimeService::start_container(
        &service,
        Request::new(StartContainerRequest {
            container_id: "container-start".to_string(),
        }),
    )
    .await
    .unwrap();

    assert_eq!(
        fake_nri.calls.lock().await.clone(),
        vec!["start_container", "post_start_container"]
    );
    let container = service
        .containers
        .lock()
        .await
        .get("container-start")
        .cloned()
        .unwrap();
    assert_eq!(container.state, ContainerState::ContainerRunning as i32);

    let post_start_event = fake_nri
        .post_start_events
        .lock()
        .await
        .last()
        .cloned()
        .expect("post start event should be recorded");
    assert_eq!(
        post_start_event.container.state.enum_value().unwrap(),
        crate::nri_proto::api::ContainerState::CONTAINER_RUNNING
    );
    assert!(post_start_event.container.started_at > 0);
}

#[tokio::test]
async fn start_container_succeeds_when_nri_post_start_fails() {
    let fake_nri = Arc::new(FakeNri {
        fail_post_start_container: true,
        ..Default::default()
    });
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-start-post-fail".to_string(),
        test_pod("pod-start-post-fail", HashMap::new()),
    );
    service.containers.lock().await.insert(
        "container-start-post-fail".to_string(),
        test_container(
            "container-start-post-fail",
            "pod-start-post-fail",
            annotations.clone(),
        ),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-start-post-fail",
            "pod-start-post-fail",
            crate::runtime::ContainerStatus::Created,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    write_test_bundle_config(&dir, "container-start-post-fail", &annotations);

    RuntimeService::start_container(
        &service,
        Request::new(StartContainerRequest {
            container_id: "container-start-post-fail".to_string(),
        }),
    )
    .await
    .expect("post-start failure should not fail start");

    let container = service
        .containers
        .lock()
        .await
        .get("container-start-post-fail")
        .cloned()
        .unwrap();
    assert_eq!(container.state, ContainerState::ContainerRunning as i32);
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn start_container_undoes_nri_when_runtime_start_fails() {
    let _guard = env_lock().lock().unwrap();
    std::env::set_var("CRIUS_SHIM_PATH", "/definitely/missing/crius-shim");
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());
    std::env::remove_var("CRIUS_SHIM_PATH");

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-start-fail".to_string(),
        test_pod("pod-start-fail", HashMap::new()),
    );
    service.containers.lock().await.insert(
        "container-start-fail".to_string(),
        test_container(
            "container-start-fail",
            "pod-start-fail",
            annotations.clone(),
        ),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-start-fail",
            "pod-start-fail",
            crate::runtime::ContainerStatus::Created,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    write_test_bundle_config(&dir, "container-start-fail", &annotations);

    let mut update = crate::nri_proto::api::ContainerUpdate::new();
    update.container_id = "container-sidecar".to_string();
    let mut linux = crate::nri_proto::api::LinuxContainerUpdate::new();
    let mut resources = crate::nri_proto::api::LinuxResources::new();
    let mut cpu = crate::nri_proto::api::LinuxCPU::new();
    let mut shares = crate::nri_proto::api::OptionalUInt64::new();
    shares.value = 256;
    cpu.shares = protobuf::MessageField::some(shares);
    resources.cpu = protobuf::MessageField::some(cpu);
    linux.resources = protobuf::MessageField::some(resources);
    update.linux = protobuf::MessageField::some(linux);
    *fake_nri.stop_result.lock().await = Some(NriStopContainerResult {
        updates: vec![update],
    });

    service.containers.lock().await.insert(
        "container-sidecar".to_string(),
        test_container("container-sidecar", "pod-start-fail", annotations.clone()),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-sidecar",
            "pod-start-fail",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    set_fake_runtime_state(&dir, "container-sidecar", "running");

    let err = RuntimeService::start_container(
        &service,
        Request::new(StartContainerRequest {
            container_id: "container-start-fail".to_string(),
        }),
    )
    .await
    .expect_err("runtime start failure should be surfaced");
    assert_eq!(err.code(), tonic::Code::Internal);
    assert!(err.message().contains("Failed to start container"));

    assert_eq!(
        fake_nri.calls.lock().await.clone(),
        vec!["start_container", "stop_container"]
    );
    let container = service
        .containers
        .lock()
        .await
        .get("container-start-fail")
        .cloned()
        .unwrap();
    assert_eq!(container.state, ContainerState::ContainerCreated as i32);
    let state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
        &container.annotations,
        INTERNAL_CONTAINER_STATE_KEY,
    )
    .unwrap();
    assert!(state.nri_stop_notified);

    let sidecar_update_payload: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(fake_runtime_update_path(&dir, "container-sidecar")).unwrap(),
    )
    .unwrap();
    assert_eq!(sidecar_update_payload["cpu"]["shares"], 256);
}

#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn remove_after_failed_start_does_not_repeat_nri_stop() {
    let _guard = env_lock().lock().unwrap();
    std::env::set_var("CRIUS_SHIM_PATH", "/definitely/missing/crius-shim");
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());
    std::env::remove_var("CRIUS_SHIM_PATH");

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-start-remove".to_string(),
        test_pod("pod-start-remove", HashMap::new()),
    );
    service.containers.lock().await.insert(
        "container-start-remove".to_string(),
        test_container(
            "container-start-remove",
            "pod-start-remove",
            annotations.clone(),
        ),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-start-remove",
            "pod-start-remove",
            crate::runtime::ContainerStatus::Created,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    write_test_bundle_config(&dir, "container-start-remove", &annotations);

    let err = RuntimeService::start_container(
        &service,
        Request::new(StartContainerRequest {
            container_id: "container-start-remove".to_string(),
        }),
    )
    .await
    .expect_err("runtime start failure should be surfaced");
    assert_eq!(err.code(), tonic::Code::Internal);

    RuntimeService::remove_container(
        &service,
        Request::new(RemoveContainerRequest {
            container_id: "container-start-remove".to_string(),
        }),
    )
    .await
    .unwrap();

    assert_eq!(
        fake_nri.calls.lock().await.clone(),
        vec!["start_container", "stop_container", "remove_container"]
    );
}

#[tokio::test]
async fn stop_container_notifies_nri_only_once_across_repeat_calls() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();

    service
        .pod_sandboxes
        .lock()
        .await
        .insert("pod-stop".to_string(), test_pod("pod-stop", HashMap::new()));
    service.containers.lock().await.insert(
        "container-stop".to_string(),
        test_container("container-stop", "pod-stop", annotations.clone()),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-stop",
            "pod-stop",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    set_fake_runtime_state(&dir, "container-stop", "running");

    RuntimeService::stop_container(
        &service,
        Request::new(StopContainerRequest {
            container_id: "container-stop".to_string(),
            timeout: 1,
        }),
    )
    .await
    .expect("first stop should succeed");
    RuntimeService::stop_container(
        &service,
        Request::new(StopContainerRequest {
            container_id: "container-stop".to_string(),
            timeout: 1,
        }),
    )
    .await
    .expect("repeated stop should succeed");

    assert_eq!(fake_nri.calls.lock().await.clone(), vec!["stop_container"]);
    let state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
        &service
            .containers
            .lock()
            .await
            .get("container-stop")
            .unwrap()
            .annotations,
        INTERNAL_CONTAINER_STATE_KEY,
    )
    .unwrap();
    assert!(state.nri_stop_notified);
}

#[tokio::test]
async fn stop_container_notifies_nri_with_post_stop_state() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-stop-state".to_string(),
        test_pod("pod-stop-state", HashMap::new()),
    );
    service.containers.lock().await.insert(
        "container-stop-state".to_string(),
        test_container(
            "container-stop-state",
            "pod-stop-state",
            annotations.clone(),
        ),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-stop-state",
            "pod-stop-state",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    set_fake_runtime_state(&dir, "container-stop-state", "running");

    RuntimeService::stop_container(
        &service,
        Request::new(StopContainerRequest {
            container_id: "container-stop-state".to_string(),
            timeout: 1,
        }),
    )
    .await
    .expect("stop should succeed");

    let stop_event = fake_nri
        .stop_events
        .lock()
        .await
        .last()
        .cloned()
        .expect("stop event should be recorded");
    assert_eq!(
        stop_event.container.state.enum_value().unwrap(),
        crate::nri_proto::api::ContainerState::CONTAINER_STOPPED
    );
}

#[tokio::test]
async fn stop_container_succeeds_when_nri_stop_fails() {
    let fake_nri = Arc::new(FakeNri {
        fail_stop_container: true,
        ..Default::default()
    });
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-stop-fail".to_string(),
        test_pod("pod-stop-fail", HashMap::new()),
    );
    service.containers.lock().await.insert(
        "container-stop-fail".to_string(),
        test_container("container-stop-fail", "pod-stop-fail", annotations.clone()),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-stop-fail",
            "pod-stop-fail",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    set_fake_runtime_state(&dir, "container-stop-fail", "running");

    RuntimeService::stop_container(
        &service,
        Request::new(StopContainerRequest {
            container_id: "container-stop-fail".to_string(),
            timeout: 1,
        }),
    )
    .await
    .expect("NRI stop failure should not fail container stop");
}

#[tokio::test]
async fn stop_container_skips_nri_for_already_exited_container() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-exited".to_string(),
        test_pod("pod-exited", HashMap::new()),
    );
    let mut container = test_container("container-exited", "pod-exited", annotations.clone());
    container.state = ContainerState::ContainerExited as i32;
    service
        .containers
        .lock()
        .await
        .insert("container-exited".to_string(), container);
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-exited",
            "pod-exited",
            crate::runtime::ContainerStatus::Stopped(17),
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    set_fake_runtime_state(&dir, "container-exited", "stopped");

    RuntimeService::stop_container(
        &service,
        Request::new(StopContainerRequest {
            container_id: "container-exited".to_string(),
            timeout: 1,
        }),
    )
    .await
    .expect("stop should succeed");

    assert!(fake_nri.calls.lock().await.is_empty());
}

#[tokio::test]
async fn remove_container_notifies_nri_only_once_across_repeat_calls() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-remove".to_string(),
        test_pod("pod-remove", HashMap::new()),
    );
    service.containers.lock().await.insert(
        "container-remove".to_string(),
        test_container("container-remove", "pod-remove", annotations.clone()),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-remove",
            "pod-remove",
            crate::runtime::ContainerStatus::Stopped(0),
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    set_fake_runtime_state(&dir, "container-remove", "stopped");

    RuntimeService::remove_container(
        &service,
        Request::new(RemoveContainerRequest {
            container_id: "container-remove".to_string(),
        }),
    )
    .await
    .expect("first remove should succeed");
    RuntimeService::remove_container(
        &service,
        Request::new(RemoveContainerRequest {
            container_id: "container-remove".to_string(),
        }),
    )
    .await
    .expect("repeated remove should succeed");

    assert_eq!(
        fake_nri.calls.lock().await.clone(),
        vec!["remove_container"]
    );
}

#[tokio::test]
async fn remove_container_succeeds_when_nri_remove_fails() {
    let fake_nri = Arc::new(FakeNri {
        fail_remove_container: true,
        ..Default::default()
    });
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-remove-fail".to_string(),
        test_pod("pod-remove-fail", HashMap::new()),
    );
    service.containers.lock().await.insert(
        "container-remove-fail".to_string(),
        test_container(
            "container-remove-fail",
            "pod-remove-fail",
            annotations.clone(),
        ),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-remove-fail",
            "pod-remove-fail",
            crate::runtime::ContainerStatus::Stopped(0),
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    set_fake_runtime_state(&dir, "container-remove-fail", "stopped");

    RuntimeService::remove_container(
        &service,
        Request::new(RemoveContainerRequest {
            container_id: "container-remove-fail".to_string(),
        }),
    )
    .await
    .expect("NRI remove failure should not fail container removal");
}

#[tokio::test]
async fn remove_container_notifies_nri_stop_before_remove_for_running_container() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-remove-running".to_string(),
        test_pod("pod-remove-running", HashMap::new()),
    );
    service.containers.lock().await.insert(
        "container-remove-running".to_string(),
        test_container(
            "container-remove-running",
            "pod-remove-running",
            annotations.clone(),
        ),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-remove-running",
            "pod-remove-running",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    set_fake_runtime_state(&dir, "container-remove-running", "running");

    RuntimeService::remove_container(
        &service,
        Request::new(RemoveContainerRequest {
            container_id: "container-remove-running".to_string(),
        }),
    )
    .await
    .expect("remove should succeed");

    assert_eq!(
        fake_nri.calls.lock().await.clone(),
        vec!["stop_container", "remove_container"]
    );
}

#[tokio::test]
async fn stop_pod_sandbox_stops_member_containers_before_pod_notification() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    service.pod_sandboxes.lock().await.insert(
        "pod-stop-order".to_string(),
        test_pod("pod-stop-order", HashMap::new()),
    );
    service.containers.lock().await.insert(
        "container-under-pod-stop".to_string(),
        test_container("container-under-pod-stop", "pod-stop-order", HashMap::new()),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-under-pod-stop",
            "pod-stop-order",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &HashMap::new(),
        )
        .unwrap();
    set_fake_runtime_state(&dir, "container-under-pod-stop", "running");

    RuntimeService::stop_pod_sandbox(
        &service,
        Request::new(StopPodSandboxRequest {
            pod_sandbox_id: "pod-stop-order".to_string(),
        }),
    )
    .await
    .expect("pod stop should succeed");

    assert_eq!(
        fake_nri.calls.lock().await.clone(),
        vec!["stop_container", "stop_pod"]
    );
    assert_eq!(fake_nri.stop_pod_events.lock().await.len(), 1);
}

#[tokio::test]
async fn remove_pod_sandbox_removes_member_containers_before_pod_notification() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    service.pod_sandboxes.lock().await.insert(
        "pod-remove-order".to_string(),
        test_pod("pod-remove-order", HashMap::new()),
    );
    service.containers.lock().await.insert(
        "container-under-pod-remove".to_string(),
        test_container(
            "container-under-pod-remove",
            "pod-remove-order",
            HashMap::new(),
        ),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-under-pod-remove",
            "pod-remove-order",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &HashMap::new(),
        )
        .unwrap();
    set_fake_runtime_state(&dir, "container-under-pod-remove", "running");

    RuntimeService::remove_pod_sandbox(
        &service,
        Request::new(RemovePodSandboxRequest {
            pod_sandbox_id: "pod-remove-order".to_string(),
        }),
    )
    .await
    .expect("pod remove should succeed");

    assert_eq!(
        fake_nri.calls.lock().await.clone(),
        vec!["stop_container", "remove_container", "remove_pod"]
    );
}

#[tokio::test]
async fn start_container_clears_nri_stop_notification_marker() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            nri_stop_notified: true,
            ..Default::default()
        },
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-restart".to_string(),
        test_pod("pod-restart", HashMap::new()),
    );
    service.containers.lock().await.insert(
        "container-restart".to_string(),
        test_container("container-restart", "pod-restart", annotations.clone()),
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-restart",
            "pod-restart",
            crate::runtime::ContainerStatus::Created,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();
    write_test_bundle_config(&dir, "container-restart", &annotations);

    RuntimeService::start_container(
        &service,
        Request::new(StartContainerRequest {
            container_id: "container-restart".to_string(),
        }),
    )
    .await
    .unwrap();
    RuntimeService::stop_container(
        &service,
        Request::new(StopContainerRequest {
            container_id: "container-restart".to_string(),
            timeout: 1,
        }),
    )
    .await
    .unwrap();

    assert_eq!(
        fake_nri.calls.lock().await.clone(),
        vec!["start_container", "post_start_container", "stop_container",]
    );
}

#[tokio::test]
async fn reopen_container_log_validates_running_state_and_log_path() {
    let (dir, service) = test_service_with_fake_runtime();

    let missing = RuntimeService::reopen_container_log(
        &service,
        Request::new(ReopenContainerLogRequest {
            container_id: "missing".to_string(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(missing.code(), tonic::Code::NotFound);

    let mut stopped_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut stopped_annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            log_path: Some(
                dir.path()
                    .join("logs")
                    .join("stopped.log")
                    .display()
                    .to_string(),
            ),
            ..Default::default()
        },
    )
    .unwrap();
    service.containers.lock().await.insert(
        "container-stopped".to_string(),
        test_container("container-stopped", "pod-1", stopped_annotations),
    );
    set_fake_runtime_state(&dir, "container-stopped", "stopped");

    let stopped = RuntimeService::reopen_container_log(
        &service,
        Request::new(ReopenContainerLogRequest {
            container_id: "container-stopped".to_string(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(stopped.code(), tonic::Code::FailedPrecondition);

    let log_path = dir.path().join("logs").join("running.log");
    let mut running_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut running_annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            log_path: Some(log_path.display().to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    service.containers.lock().await.insert(
        "container-running".to_string(),
        test_container("container-running", "pod-1", running_annotations),
    );
    set_fake_runtime_state(&dir, "container-running", "running");

    let running = RuntimeService::reopen_container_log(
        &service,
        Request::new(ReopenContainerLogRequest {
            container_id: "container-running".to_string(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(running.code(), tonic::Code::Internal);
    assert!(running.message().contains("reopen log socket"));
}

#[tokio::test]
#[ignore = "requires unix socket bind permissions in the current test environment"]
async fn reopen_container_log_notifies_shim_socket_when_available() {
    let (dir, service) = test_service_with_fake_runtime();

    let log_path = dir.path().join("logs").join("running.log");
    let mut running_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut running_annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            log_path: Some(log_path.display().to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    service.containers.lock().await.insert(
        "container-running".to_string(),
        test_container("container-running", "pod-1", running_annotations),
    );
    set_fake_runtime_state(&dir, "container-running", "running");

    let reopen_socket = dir
        .path()
        .join("shims")
        .join("container-running")
        .join("reopen.sock");
    fs::create_dir_all(reopen_socket.parent().unwrap()).unwrap();
    let listener = UnixListener::bind(&reopen_socket).unwrap();
    let log_path_for_server = log_path.clone();
    let (tx, rx) = mpsc::channel();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        fs::write(&log_path_for_server, "").unwrap();
        stream.write_all(b"OK\n").unwrap();
        tx.send(()).unwrap();
    });

    RuntimeService::reopen_container_log(
        &service,
        Request::new(ReopenContainerLogRequest {
            container_id: "container-running".to_string(),
        }),
    )
    .await
    .unwrap();
    rx.recv_timeout(Duration::from_secs(1)).unwrap();
    server.join().unwrap();
    assert!(log_path.exists(), "expected reopen to touch the log file");
}

#[tokio::test]
async fn update_container_resources_validates_state_and_persists_resources() {
    let (dir, service) = test_service_with_fake_runtime();

    let missing = RuntimeService::update_container_resources(
        &service,
        Request::new(UpdateContainerResourcesRequest {
            container_id: "missing".to_string(),
            linux: Some(crate::proto::runtime::v1::LinuxContainerResources {
                cpu_shares: 2,
                ..Default::default()
            }),
            windows: None,
            annotations: HashMap::new(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(missing.code(), tonic::Code::NotFound);

    service.containers.lock().await.insert(
        "container-stopped".to_string(),
        test_container("container-stopped", "pod-1", HashMap::new()),
    );
    set_fake_runtime_state(&dir, "container-stopped", "stopped");
    let stopped = RuntimeService::update_container_resources(
        &service,
        Request::new(UpdateContainerResourcesRequest {
            container_id: "container-stopped".to_string(),
            linux: Some(crate::proto::runtime::v1::LinuxContainerResources {
                cpu_shares: 2,
                ..Default::default()
            }),
            windows: None,
            annotations: HashMap::new(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(stopped.code(), tonic::Code::FailedPrecondition);

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();
    service.containers.lock().await.insert(
        "container-running".to_string(),
        test_container("container-running", "pod-1", annotations.clone()),
    );
    set_fake_runtime_state(&dir, "container-running", "running");
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-running",
            "pod-1",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();

    let resources = crate::proto::runtime::v1::LinuxContainerResources {
        cpu_shares: 256,
        cpu_period: 100_000,
        cpu_quota: 50_000,
        memory_limit_in_bytes: 128 * 1024 * 1024,
        cpuset_cpus: "0-1".to_string(),
        cpuset_mems: "0".to_string(),
        ..Default::default()
    };

    RuntimeService::update_container_resources(
        &service,
        Request::new(UpdateContainerResourcesRequest {
            container_id: "container-running".to_string(),
            linux: Some(resources.clone()),
            windows: None,
            annotations: HashMap::new(),
        }),
    )
    .await
    .unwrap();

    let status = RuntimeService::container_status(
        &service,
        Request::new(ContainerStatusRequest {
            container_id: "container-running".to_string(),
            verbose: false,
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .status
    .unwrap();
    let linux = status.resources.unwrap().linux.unwrap();
    assert_eq!(linux.cpu_shares, 256);
    assert_eq!(linux.memory_limit_in_bytes, 128 * 1024 * 1024);

    let persisted = service
        .persistence
        .lock()
        .await
        .storage()
        .get_container("container-running")
        .unwrap()
        .unwrap();
    let persisted_annotations: HashMap<String, String> =
        serde_json::from_str(&persisted.annotations).unwrap();
    let persisted_state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
        &persisted_annotations,
        INTERNAL_CONTAINER_STATE_KEY,
    )
    .unwrap();
    assert_eq!(
        persisted_state
            .linux_resources
            .unwrap()
            .memory_limit_in_bytes,
        128 * 1024 * 1024
    );

    let update_payload: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(fake_runtime_update_path(&dir, "container-running")).unwrap(),
    )
    .unwrap();
    assert_eq!(update_payload["cpu"]["shares"], 256);
    assert_eq!(update_payload["memory"]["limit"], 128 * 1024 * 1024);
}

#[tokio::test]
async fn update_container_resources_applies_nri_result_before_post_update() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    service
        .pod_sandboxes
        .lock()
        .await
        .insert("pod-1".to_string(), test_pod("pod-1", HashMap::new()));

    let mut target_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut target_annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();
    service.containers.lock().await.insert(
        "container-running".to_string(),
        test_container("container-running", "pod-1", target_annotations.clone()),
    );
    set_fake_runtime_state(&dir, "container-running", "running");
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-running",
            "pod-1",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &target_annotations,
        )
        .unwrap();

    let mut sidecar_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut sidecar_annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();
    service.containers.lock().await.insert(
        "container-sidecar".to_string(),
        test_container("container-sidecar", "pod-1", sidecar_annotations.clone()),
    );
    set_fake_runtime_state(&dir, "container-sidecar", "running");
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-sidecar",
            "pod-1",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &sidecar_annotations,
        )
        .unwrap();

    let mut target_resources = crate::nri_proto::api::LinuxResources::new();
    let mut target_cpu = crate::nri_proto::api::LinuxCPU::new();
    let mut shares = crate::nri_proto::api::OptionalUInt64::new();
    shares.value = 1024;
    target_cpu.shares = protobuf::MessageField::some(shares);
    target_resources.cpu = protobuf::MessageField::some(target_cpu);

    let mut sidecar_update = crate::nri_proto::api::ContainerUpdate::new();
    sidecar_update.container_id = "container-sidecar".to_string();
    let mut sidecar_linux = crate::nri_proto::api::LinuxContainerUpdate::new();
    let mut sidecar_resources = crate::nri_proto::api::LinuxResources::new();
    let mut sidecar_memory = crate::nri_proto::api::LinuxMemory::new();
    let mut sidecar_limit = crate::nri_proto::api::OptionalInt64::new();
    sidecar_limit.value = 64 * 1024 * 1024;
    sidecar_memory.limit = protobuf::MessageField::some(sidecar_limit);
    sidecar_resources.memory = protobuf::MessageField::some(sidecar_memory);
    sidecar_linux.resources = protobuf::MessageField::some(sidecar_resources);
    sidecar_update.linux = protobuf::MessageField::some(sidecar_linux);

    *fake_nri.update_result.lock().await = Some(NriUpdateContainerResult {
        linux_resources: Some(target_resources),
        updates: vec![sidecar_update],
        evictions: Vec::new(),
    });

    RuntimeService::update_container_resources(
        &service,
        Request::new(UpdateContainerResourcesRequest {
            container_id: "container-running".to_string(),
            linux: Some(crate::proto::runtime::v1::LinuxContainerResources {
                cpu_shares: 256,
                ..Default::default()
            }),
            windows: None,
            annotations: HashMap::new(),
        }),
    )
    .await
    .unwrap();

    let target_payload: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(fake_runtime_update_path(&dir, "container-running")).unwrap(),
    )
    .unwrap();
    assert_eq!(target_payload["cpu"]["shares"], 1024);

    let sidecar_payload: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(fake_runtime_update_path(&dir, "container-sidecar")).unwrap(),
    )
    .unwrap();
    assert_eq!(sidecar_payload["memory"]["limit"], 64 * 1024 * 1024);

    assert_eq!(
        fake_nri.calls.lock().await.clone(),
        vec!["update_container", "post_update_container"]
    );

    let post_update_event = fake_nri
        .post_update_events
        .lock()
        .await
        .last()
        .cloned()
        .expect("post update event should be recorded");
    let post_update_linux = post_update_event
        .container
        .linux
        .as_ref()
        .expect("post update container should include linux state");
    assert_eq!(
        post_update_linux
            .resources
            .as_ref()
            .and_then(|resources| resources.cpu.as_ref())
            .and_then(|cpu| cpu.shares.as_ref())
            .map(|value| value.value),
        Some(1024)
    );
}

#[tokio::test]
async fn update_container_resources_succeeds_when_nri_post_update_fails() {
    let fake_nri = Arc::new(FakeNri {
        fail_post_update_container: true,
        ..Default::default()
    });
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    service.pod_sandboxes.lock().await.insert(
        "pod-update-post-fail".to_string(),
        test_pod("pod-update-post-fail", HashMap::new()),
    );

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();
    service.containers.lock().await.insert(
        "container-update-post-fail".to_string(),
        test_container(
            "container-update-post-fail",
            "pod-update-post-fail",
            annotations.clone(),
        ),
    );
    set_fake_runtime_state(&dir, "container-update-post-fail", "running");
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-update-post-fail",
            "pod-update-post-fail",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();

    RuntimeService::update_container_resources(
        &service,
        Request::new(UpdateContainerResourcesRequest {
            container_id: "container-update-post-fail".to_string(),
            linux: Some(crate::proto::runtime::v1::LinuxContainerResources {
                cpu_shares: 512,
                ..Default::default()
            }),
            windows: None,
            annotations: HashMap::new(),
        }),
    )
    .await
    .expect("post-update failure should not fail resource update");

    let payload: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(fake_runtime_update_path(&dir, "container-update-post-fail")).unwrap(),
    )
    .unwrap();
    assert_eq!(payload["cpu"]["shares"], 512);
}

#[tokio::test]
async fn update_container_resources_applies_extended_nri_resources() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    service
        .pod_sandboxes
        .lock()
        .await
        .insert("pod-1".to_string(), test_pod("pod-1", HashMap::new()));

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();
    service.containers.lock().await.insert(
        "container-running".to_string(),
        test_container("container-running", "pod-1", annotations.clone()),
    );
    set_fake_runtime_state(&dir, "container-running", "running");
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-running",
            "pod-1",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();

    let mut extended = crate::nri_proto::api::LinuxResources::new();
    let mut memory = crate::nri_proto::api::LinuxMemory::new();
    let mut reservation = crate::nri_proto::api::OptionalInt64::new();
    reservation.value = 4096;
    memory.reservation = protobuf::MessageField::some(reservation);
    extended.memory = protobuf::MessageField::some(memory);
    extended.pids = Some(crate::nri_proto::api::LinuxPids {
        limit: 42,
        ..Default::default()
    })
    .into();
    extended
        .devices
        .push(crate::nri_proto::api::LinuxDeviceCgroup {
            allow: true,
            type_: "c".to_string(),
            access: "rwm".to_string(),
            major: {
                let mut value = crate::nri_proto::api::OptionalInt64::new();
                value.value = 10;
                protobuf::MessageField::some(value)
            },
            minor: {
                let mut value = crate::nri_proto::api::OptionalInt64::new();
                value.value = 200;
                protobuf::MessageField::some(value)
            },
            ..Default::default()
        });
    let mut blockio_class = crate::nri_proto::api::OptionalString::new();
    blockio_class.value = "gold".to_string();
    extended.blockio_class = protobuf::MessageField::some(blockio_class);
    *fake_nri.update_result.lock().await = Some(NriUpdateContainerResult {
        linux_resources: Some(extended),
        updates: Vec::new(),
        evictions: Vec::new(),
    });

    RuntimeService::update_container_resources(
        &service,
        Request::new(UpdateContainerResourcesRequest {
            container_id: "container-running".to_string(),
            linux: Some(crate::proto::runtime::v1::LinuxContainerResources {
                cpu_shares: 256,
                ..Default::default()
            }),
            windows: None,
            annotations: HashMap::new(),
        }),
    )
    .await
    .unwrap();

    let payload: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(fake_runtime_update_path(&dir, "container-running")).unwrap(),
    )
    .unwrap();
    assert_eq!(payload["cpu"]["shares"], 256);
    assert_eq!(payload["memory"]["reservation"], 4096);
    assert_eq!(payload["pids"]["limit"], 42);
    assert_eq!(payload["devices"][0]["type"], "c");
    assert_eq!(payload["devices"][0]["major"], 10);
    assert_eq!(payload["devices"][0]["minor"], 200);
    assert_eq!(payload["devices"][0]["access"], "rwm");
    assert_eq!(payload["blockIO"]["weight"], 500);
    assert_eq!(
        fake_nri.calls.lock().await.clone(),
        vec!["update_container", "post_update_container"]
    );

    let container = service
        .containers
        .lock()
        .await
        .get("container-running")
        .cloned()
        .unwrap();
    let state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
        &container.annotations,
        INTERNAL_CONTAINER_STATE_KEY,
    )
    .unwrap();
    let resources = state.linux_resources.unwrap();
    assert_eq!(resources.cpu_shares, 256);
    assert_eq!(resources.memory_reservation_in_bytes, Some(4096));
    assert_eq!(resources.pids_limit, Some(42));
    assert_eq!(resources.devices.len(), 1);
    assert_eq!(resources.devices[0].device_type.as_deref(), Some("c"));
    assert_eq!(resources.devices[0].major, Some(10));
    assert_eq!(resources.devices[0].minor, Some(200));
    assert_eq!(resources.blockio_class.as_deref(), Some("gold"));
}

#[tokio::test]
async fn stop_container_applies_nri_update_side_effects() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());

    let mut target_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut target_annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();
    let mut sidecar_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut sidecar_annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();

    service
        .pod_sandboxes
        .lock()
        .await
        .insert("pod-1".to_string(), test_pod("pod-1", HashMap::new()));
    service.containers.lock().await.insert(
        "container-stop".to_string(),
        test_container("container-stop", "pod-1", target_annotations.clone()),
    );
    service.containers.lock().await.insert(
        "container-sidecar".to_string(),
        test_container("container-sidecar", "pod-1", sidecar_annotations.clone()),
    );
    set_fake_runtime_state(&dir, "container-stop", "running");
    set_fake_runtime_state(&dir, "container-sidecar", "running");
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-stop",
            "pod-1",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &target_annotations,
        )
        .unwrap();
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-sidecar",
            "pod-1",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &sidecar_annotations,
        )
        .unwrap();

    let mut update = crate::nri_proto::api::ContainerUpdate::new();
    update.container_id = "container-sidecar".to_string();
    let mut linux = crate::nri_proto::api::LinuxContainerUpdate::new();
    let mut resources = crate::nri_proto::api::LinuxResources::new();
    let mut cpu = crate::nri_proto::api::LinuxCPU::new();
    let mut shares = crate::nri_proto::api::OptionalUInt64::new();
    shares.value = 2048;
    cpu.shares = protobuf::MessageField::some(shares);
    resources.cpu = protobuf::MessageField::some(cpu);
    linux.resources = protobuf::MessageField::some(resources);
    update.linux = protobuf::MessageField::some(linux);
    *fake_nri.stop_result.lock().await = Some(NriStopContainerResult {
        updates: vec![update],
    });

    RuntimeService::stop_container(
        &service,
        Request::new(StopContainerRequest {
            container_id: "container-stop".to_string(),
            timeout: 1,
        }),
    )
    .await
    .unwrap();

    assert_eq!(fake_nri.calls.lock().await.clone(), vec!["stop_container"]);
    let update_payload: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(fake_runtime_update_path(&dir, "container-sidecar")).unwrap(),
    )
    .unwrap();
    assert_eq!(update_payload["cpu"]["shares"], 2048);
    let sidecar = service
        .containers
        .lock()
        .await
        .get("container-sidecar")
        .cloned()
        .unwrap();
    let state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
        &sidecar.annotations,
        INTERNAL_CONTAINER_STATE_KEY,
    )
    .unwrap();
    assert_eq!(state.linux_resources.unwrap().cpu_shares, 2048);
}

#[tokio::test]
async fn nri_domain_apply_updates_updates_runtime_and_persistence() {
    let (dir, service) = test_service_with_fake_runtime();
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();
    service.containers.lock().await.insert(
        "container-running".to_string(),
        test_container("container-running", "pod-1", annotations.clone()),
    );
    service
        .pod_sandboxes
        .lock()
        .await
        .insert("pod-1".to_string(), test_pod("pod-1", annotations.clone()));
    set_fake_runtime_state(&dir, "container-running", "running");
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-running",
            "pod-1",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();

    let domain = NriRuntimeDomain {
        containers: service.containers.clone(),
        pod_sandboxes: service.pod_sandboxes.clone(),
        config: service.config.clone(),
        nri_config: service.nri_config.clone(),
        runtime: service.runtime.clone(),
        persistence: service.persistence.clone(),
        events: service.events.clone(),
    };

    let mut update = crate::nri_proto::api::ContainerUpdate::new();
    update.container_id = "container-running".to_string();
    let mut linux_update = crate::nri_proto::api::LinuxContainerUpdate::new();
    let mut resources = crate::nri_proto::api::LinuxResources::new();
    let mut cpu = crate::nri_proto::api::LinuxCPU::new();
    let mut shares = crate::nri_proto::api::OptionalUInt64::new();
    shares.value = 512;
    cpu.shares = protobuf::MessageField::some(shares);
    resources.cpu = protobuf::MessageField::some(cpu);
    linux_update.resources = protobuf::MessageField::some(resources);
    update.linux = protobuf::MessageField::some(linux_update);

    let failed = domain.apply_updates(&[update]).await.unwrap();
    assert!(failed.is_empty());

    let update_payload: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(fake_runtime_update_path(&dir, "container-running")).unwrap(),
    )
    .unwrap();
    assert_eq!(update_payload["cpu"]["shares"], 512);

    let persisted = service
        .persistence
        .lock()
        .await
        .storage()
        .get_container("container-running")
        .unwrap()
        .unwrap();
    let persisted_annotations: HashMap<String, String> =
        serde_json::from_str(&persisted.annotations).unwrap();
    let persisted_state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
        &persisted_annotations,
        INTERNAL_CONTAINER_STATE_KEY,
    )
    .unwrap();
    assert_eq!(persisted_state.linux_resources.unwrap().cpu_shares, 512);
}

#[tokio::test]
async fn nri_domain_snapshot_excludes_exited_containers() {
    let (_dir, service) = test_service_with_fake_runtime();
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();

    service
        .pod_sandboxes
        .lock()
        .await
        .insert("pod-1".to_string(), test_pod("pod-1", HashMap::new()));

    let mut running = test_container("container-running", "pod-1", annotations.clone());
    running.state = ContainerState::ContainerRunning as i32;
    let mut exited = test_container("container-exited", "pod-1", annotations);
    exited.state = ContainerState::ContainerExited as i32;

    let mut containers = service.containers.lock().await;
    containers.insert(running.id.clone(), running);
    containers.insert(exited.id.clone(), exited);
    drop(containers);

    let domain = NriRuntimeDomain {
        containers: service.containers.clone(),
        pod_sandboxes: service.pod_sandboxes.clone(),
        config: service.config.clone(),
        nri_config: service.nri_config.clone(),
        runtime: service.runtime.clone(),
        persistence: service.persistence.clone(),
        events: service.events.clone(),
    };

    let snapshot = domain.snapshot().await.unwrap();
    let ids = snapshot
        .containers
        .iter()
        .map(|container| container.id.as_str())
        .collect::<Vec<_>>();

    assert_eq!(ids, vec!["container-running"]);
}

#[tokio::test]
async fn nri_domain_snapshot_includes_paused_runtime_containers() {
    let (dir, service) = test_service_with_fake_runtime();
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();

    service
        .pod_sandboxes
        .lock()
        .await
        .insert("pod-1".to_string(), test_pod("pod-1", HashMap::new()));

    let mut paused = test_container("container-paused", "pod-1", annotations);
    paused.state = ContainerState::ContainerExited as i32;
    service
        .containers
        .lock()
        .await
        .insert(paused.id.clone(), paused);
    set_fake_runtime_state(&dir, "container-paused", "paused");

    let domain = NriRuntimeDomain {
        containers: service.containers.clone(),
        pod_sandboxes: service.pod_sandboxes.clone(),
        config: service.config.clone(),
        nri_config: service.nri_config.clone(),
        runtime: service.runtime.clone(),
        persistence: service.persistence.clone(),
        events: service.events.clone(),
    };

    let snapshot = domain.snapshot().await.unwrap();
    assert_eq!(snapshot.containers.len(), 1);
    assert_eq!(snapshot.containers[0].id, "container-paused");
    assert_eq!(
        snapshot.containers[0].state.enum_value().unwrap(),
        crate::nri_proto::api::ContainerState::CONTAINER_PAUSED
    );
}

#[tokio::test]
async fn nri_domain_apply_updates_ignores_failures_marked_ignore_failure() {
    let (_dir, service) = test_service_with_fake_runtime();
    let domain = NriRuntimeDomain {
        containers: service.containers.clone(),
        pod_sandboxes: service.pod_sandboxes.clone(),
        config: service.config.clone(),
        nri_config: service.nri_config.clone(),
        runtime: service.runtime.clone(),
        persistence: service.persistence.clone(),
        events: service.events.clone(),
    };

    let mut update = crate::nri_proto::api::ContainerUpdate::new();
    update.container_id = "missing".to_string();
    update.ignore_failure = true;

    let failed = domain.apply_updates(&[update.clone()]).await.unwrap();
    assert!(failed.is_empty());
}

#[tokio::test]
async fn nri_domain_apply_updates_ignores_missing_containers() {
    let (_dir, service) = test_service_with_fake_runtime();
    let domain = NriRuntimeDomain {
        containers: service.containers.clone(),
        pod_sandboxes: service.pod_sandboxes.clone(),
        config: service.config.clone(),
        nri_config: service.nri_config.clone(),
        runtime: service.runtime.clone(),
        persistence: service.persistence.clone(),
        events: service.events.clone(),
    };

    let mut update = crate::nri_proto::api::ContainerUpdate::new();
    update.container_id = "missing".to_string();
    let mut linux_update = crate::nri_proto::api::LinuxContainerUpdate::new();
    linux_update.resources =
        protobuf::MessageField::some(crate::nri_proto::api::LinuxResources::new());
    update.linux = protobuf::MessageField::some(linux_update);

    let failed = domain.apply_updates(&[update.clone()]).await.unwrap();
    assert!(failed.is_empty());
}

#[tokio::test]
async fn nri_domain_apply_updates_accepts_short_container_ids() {
    let (dir, service) = test_service_with_fake_runtime();
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();
    service.containers.lock().await.insert(
        "container-running-long".to_string(),
        test_container("container-running-long", "pod-1", annotations.clone()),
    );
    set_fake_runtime_state(&dir, "container-running-long", "running");
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-running-long",
            "pod-1",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();

    let domain = NriRuntimeDomain {
        containers: service.containers.clone(),
        pod_sandboxes: service.pod_sandboxes.clone(),
        config: service.config.clone(),
        nri_config: service.nri_config.clone(),
        runtime: service.runtime.clone(),
        persistence: service.persistence.clone(),
        events: service.events.clone(),
    };

    let mut update = crate::nri_proto::api::ContainerUpdate::new();
    update.container_id = "container-run".to_string();
    let mut linux_update = crate::nri_proto::api::LinuxContainerUpdate::new();
    let mut resources = crate::nri_proto::api::LinuxResources::new();
    let mut cpu = crate::nri_proto::api::LinuxCPU::new();
    let mut shares = crate::nri_proto::api::OptionalUInt64::new();
    shares.value = 768;
    cpu.shares = protobuf::MessageField::some(shares);
    resources.cpu = protobuf::MessageField::some(cpu);
    linux_update.resources = protobuf::MessageField::some(resources);
    update.linux = protobuf::MessageField::some(linux_update);

    let failed = domain.apply_updates(&[update]).await.unwrap();
    assert!(failed.is_empty());

    let update_payload: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(fake_runtime_update_path(&dir, "container-running-long")).unwrap(),
    )
    .unwrap();
    assert_eq!(update_payload["cpu"]["shares"], 768);
}

#[tokio::test]
async fn nri_domain_apply_updates_ignores_non_mutable_containers() {
    let (dir, service) = test_service_with_fake_runtime();
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();
    service.containers.lock().await.insert(
        "container-exited".to_string(),
        test_container("container-exited", "pod-1", annotations),
    );
    set_fake_runtime_state(&dir, "container-exited", "stopped");

    let domain = NriRuntimeDomain {
        containers: service.containers.clone(),
        pod_sandboxes: service.pod_sandboxes.clone(),
        config: service.config.clone(),
        nri_config: service.nri_config.clone(),
        runtime: service.runtime.clone(),
        persistence: service.persistence.clone(),
        events: service.events.clone(),
    };

    let mut update = crate::nri_proto::api::ContainerUpdate::new();
    update.container_id = "container-exited".to_string();
    let mut linux_update = crate::nri_proto::api::LinuxContainerUpdate::new();
    linux_update.resources =
        protobuf::MessageField::some(crate::nri_proto::api::LinuxResources::new());
    update.linux = protobuf::MessageField::some(linux_update);

    let failed = domain.apply_updates(&[update]).await.unwrap();
    assert!(failed.is_empty());
}

#[tokio::test]
async fn nri_domain_apply_updates_accepts_extended_resources() {
    let (dir, service) = test_service_with_fake_runtime();
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();
    service.containers.lock().await.insert(
        "container-running".to_string(),
        test_container("container-running", "pod-1", annotations.clone()),
    );
    set_fake_runtime_state(&dir, "container-running", "running");
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-running",
            "pod-1",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();

    let domain = NriRuntimeDomain {
        containers: service.containers.clone(),
        pod_sandboxes: service.pod_sandboxes.clone(),
        config: service.config.clone(),
        nri_config: service.nri_config.clone(),
        runtime: service.runtime.clone(),
        persistence: service.persistence.clone(),
        events: service.events.clone(),
    };

    let mut update = crate::nri_proto::api::ContainerUpdate::new();
    update.container_id = "container-running".to_string();
    let mut linux_update = crate::nri_proto::api::LinuxContainerUpdate::new();
    let mut resources = crate::nri_proto::api::LinuxResources::new();
    let mut cpu = crate::nri_proto::api::LinuxCPU::new();
    let mut shares = crate::nri_proto::api::OptionalUInt64::new();
    shares.value = 512;
    cpu.shares = protobuf::MessageField::some(shares);
    resources.cpu = protobuf::MessageField::some(cpu);
    resources.pids = Some(crate::nri_proto::api::LinuxPids {
        limit: 7,
        ..Default::default()
    })
    .into();
    resources
        .devices
        .push(crate::nri_proto::api::LinuxDeviceCgroup {
            allow: false,
            type_: "b".to_string(),
            access: "rw".to_string(),
            major: {
                let mut value = crate::nri_proto::api::OptionalInt64::new();
                value.value = 8;
                protobuf::MessageField::some(value)
            },
            minor: {
                let mut value = crate::nri_proto::api::OptionalInt64::new();
                value.value = 0;
                protobuf::MessageField::some(value)
            },
            ..Default::default()
        });
    let mut blockio_class = crate::nri_proto::api::OptionalString::new();
    blockio_class.value = "gold".to_string();
    resources.blockio_class = protobuf::MessageField::some(blockio_class);
    linux_update.resources = protobuf::MessageField::some(resources);
    update.linux = protobuf::MessageField::some(linux_update);

    let failed = domain.apply_updates(&[update]).await.unwrap();
    assert!(failed.is_empty());

    let payload: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(fake_runtime_update_path(&dir, "container-running")).unwrap(),
    )
    .unwrap();
    assert_eq!(payload["cpu"]["shares"], 512);
    assert_eq!(payload["pids"]["limit"], 7);
    assert_eq!(payload["devices"][0]["type"], "b");
    assert_eq!(payload["devices"][0]["allow"], false);
    assert_eq!(payload["devices"][0]["major"], 8);
    assert_eq!(payload["blockIO"]["weight"], 500);
}

#[tokio::test]
async fn nri_domain_evict_stops_container_and_persists_state() {
    let (dir, service) = test_service_with_fake_runtime();
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();
    let mut container = test_container("container-running", "pod-1", annotations.clone());
    container.state = ContainerState::ContainerRunning as i32;
    service
        .containers
        .lock()
        .await
        .insert("container-running".to_string(), container);
    service
        .pod_sandboxes
        .lock()
        .await
        .insert("pod-1".to_string(), test_pod("pod-1", HashMap::new()));
    set_fake_runtime_state(&dir, "container-running", "running");
    service
        .persistence
        .lock()
        .await
        .save_container(
            "container-running",
            "pod-1",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();

    let domain = NriRuntimeDomain {
        containers: service.containers.clone(),
        pod_sandboxes: service.pod_sandboxes.clone(),
        config: service.config.clone(),
        nri_config: service.nri_config.clone(),
        runtime: service.runtime.clone(),
        persistence: service.persistence.clone(),
        events: service.events.clone(),
    };

    domain.evict("container-running", "policy").await.unwrap();

    let containers = service.containers.lock().await;
    assert_eq!(
        containers.get("container-running").unwrap().state,
        ContainerState::ContainerExited as i32
    );
    drop(containers);

    let persisted = service
        .persistence
        .lock()
        .await
        .storage()
        .get_container("container-running")
        .unwrap()
        .unwrap();
    assert_eq!(persisted.state, "stopped");
    let persisted_annotations: HashMap<String, String> =
        serde_json::from_str(&persisted.annotations).unwrap();
    let persisted_state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
        &persisted_annotations,
        INTERNAL_CONTAINER_STATE_KEY,
    )
    .unwrap();
    assert!(persisted_state.nri_stop_notified);
}

#[tokio::test]
async fn inspect_and_info_endpoints_return_consistent_snapshots() {
    let (dir, service) = test_service_with_fake_runtime();
    let netns_path = dir.path().join("inspect.netns");
    fs::write(&netns_path, "netns").unwrap();
    let log_path = dir.path().join("logs").join("inspect.log");

    let mut pod_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut pod_annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            runtime_handler: "runc".to_string(),
            netns_path: Some(netns_path.display().to_string()),
            pause_container_id: Some("pause-inspect".to_string()),
            ip: Some("10.88.0.10".to_string()),
            additional_ips: vec!["fd00::10".to_string()],
            ..Default::default()
        },
    )
    .unwrap();
    service.pod_sandboxes.lock().await.insert(
        "pod-inspect".to_string(),
        test_pod("pod-inspect", pod_annotations),
    );

    let mut container_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut container_annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            log_path: Some(log_path.display().to_string()),
            run_as_user: Some("1000".to_string()),
            mounts: vec![StoredMount {
                container_path: "/data".to_string(),
                host_path: "/host/data".to_string(),
                readonly: false,
                selinux_relabel: false,
                propagation: crate::proto::runtime::v1::MountPropagation::PropagationPrivate as i32,
            }],
            ..Default::default()
        },
    )
    .unwrap();
    service.containers.lock().await.insert(
        "container-inspect".to_string(),
        test_container("container-inspect", "pod-inspect", container_annotations),
    );
    set_fake_runtime_state(&dir, "container-inspect", "running");

    let status = RuntimeService::status(&service, Request::new(StatusRequest { verbose: true }))
        .await
        .unwrap()
        .into_inner();
    let container = RuntimeService::container_status(
        &service,
        Request::new(ContainerStatusRequest {
            container_id: "container-inspect".to_string(),
            verbose: true,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let pod = RuntimeService::pod_sandbox_status(
        &service,
        Request::new(PodSandboxStatusRequest {
            pod_sandbox_id: "pod-inspect".to_string(),
            verbose: true,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let containers =
        RuntimeService::list_containers(&service, Request::new(ListContainersRequest::default()))
            .await
            .unwrap()
            .into_inner();
    let pods =
        RuntimeService::list_pod_sandbox(&service, Request::new(ListPodSandboxRequest::default()))
            .await
            .unwrap()
            .into_inner();

    let config: serde_json::Value =
        serde_json::from_str(status.info.get("config").unwrap()).unwrap();
    assert_eq!(config["runtimeFeatures"]["containerEvents"], true);
    assert_eq!(container.status.as_ref().unwrap().reason, "Running");
    assert_eq!(container.status.as_ref().unwrap().mounts.len(), 1);
    assert!(container.info.contains_key("info"));
    assert_eq!(
        pod.status.as_ref().unwrap().network.as_ref().unwrap().ip,
        "10.88.0.10"
    );
    assert_eq!(pod.containers_statuses.len(), 1);
    assert_eq!(containers.containers.len(), 1);
    assert_eq!(pods.items.len(), 1);
}

#[tokio::test]
async fn list_container_stats_applies_id_pod_and_label_filters() {
    let (dir, service) = test_service_with_fake_runtime();

    let mut container_a = test_container("container-alpha-123", "pod-alpha", HashMap::new());
    container_a
        .labels
        .insert("app".to_string(), "api".to_string());
    let mut container_b = test_container("container-bravo-456", "pod-bravo", HashMap::new());
    container_b
        .labels
        .insert("app".to_string(), "worker".to_string());

    {
        let mut pods = service.pod_sandboxes.lock().await;
        pods.insert(
            "pod-alpha".to_string(),
            test_pod("pod-alpha", HashMap::new()),
        );
        pods.insert(
            "pod-bravo".to_string(),
            test_pod("pod-bravo", HashMap::new()),
        );
    }
    {
        let mut containers = service.containers.lock().await;
        containers.insert(container_a.id.clone(), container_a);
        containers.insert(container_b.id.clone(), container_b);
    }
    set_fake_runtime_state(&dir, "container-alpha-123", "running");
    set_fake_runtime_state(&dir, "container-bravo-456", "running");

    let mut selector = HashMap::new();
    selector.insert("app".to_string(), "api".to_string());
    let response = RuntimeService::list_container_stats(
        &service,
        Request::new(ListContainerStatsRequest {
            filter: Some(crate::proto::runtime::v1::ContainerStatsFilter {
                id: "container-alpha".to_string(),
                pod_sandbox_id: "pod-alpha".to_string(),
                label_selector: selector,
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner();

    assert_eq!(response.stats.len(), 1);
    assert_eq!(
        response.stats[0]
            .attributes
            .as_ref()
            .expect("container stats should include attributes")
            .id,
        "container-alpha-123"
    );
}

#[tokio::test]
async fn list_containers_returns_empty_when_short_id_filter_is_ambiguous() {
    let service = test_service();

    {
        let mut containers = service.containers.lock().await;
        containers.insert(
            "container-ambiguous-a".to_string(),
            test_container("container-ambiguous-a", "pod-a", HashMap::new()),
        );
        containers.insert(
            "container-ambiguous-b".to_string(),
            test_container("container-ambiguous-b", "pod-b", HashMap::new()),
        );
    }

    let response = RuntimeService::list_containers(
        &service,
        Request::new(ListContainersRequest {
            filter: Some(crate::proto::runtime::v1::ContainerFilter {
                id: "container-ambiguous".to_string(),
                ..Default::default()
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner();

    assert!(
        response.containers.is_empty(),
        "ambiguous short container ids should behave like cri-o filters and return no entries"
    );
}

#[tokio::test]
async fn container_stats_resolves_short_id_and_list_container_stats_skips_exited() {
    let (dir, service) = test_service_with_fake_runtime();

    {
        let mut containers = service.containers.lock().await;
        containers.insert(
            "container-running-123".to_string(),
            test_container("container-running-123", "pod-1", HashMap::new()),
        );
        containers.insert(
            "container-stopped-456".to_string(),
            test_container("container-stopped-456", "pod-1", HashMap::new()),
        );
    }
    set_fake_runtime_state(&dir, "container-running-123", "running");
    set_fake_runtime_state(&dir, "container-stopped-456", "stopped");

    let stats = RuntimeService::container_stats(
        &service,
        Request::new(ContainerStatsRequest {
            container_id: "container-running".to_string(),
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .stats
    .expect("short container id should resolve to stats");
    assert_eq!(
        stats
            .attributes
            .as_ref()
            .expect("container stats should include attributes")
            .id,
        "container-running-123"
    );

    let list = RuntimeService::list_container_stats(
        &service,
        Request::new(ListContainerStatsRequest { filter: None }),
    )
    .await
    .unwrap()
    .into_inner();

    let ids = list
        .stats
        .iter()
        .filter_map(|stat| {
            stat.attributes
                .as_ref()
                .map(|attributes| attributes.id.clone())
        })
        .collect::<Vec<_>>();
    assert!(
        ids.iter().any(|id| id == "container-running-123"),
        "running containers should remain visible in list stats"
    );
    assert!(
        !ids.iter().any(|id| id == "container-stopped-456"),
        "exited containers should be filtered out from list stats by default"
    );
}

#[tokio::test]
async fn list_pod_sandbox_stats_applies_id_and_label_filters() {
    let service = test_service();

    let mut pod_alpha = test_pod("pod-alpha", HashMap::new());
    pod_alpha
        .labels
        .insert("tier".to_string(), "frontend".to_string());
    let pod_alpha_uid = pod_alpha
        .metadata
        .as_ref()
        .expect("test pod should include metadata")
        .uid
        .clone();

    let mut pod_bravo = test_pod("pod-bravo", HashMap::new());
    pod_bravo
        .labels
        .insert("tier".to_string(), "backend".to_string());
    let pod_bravo_uid = pod_bravo
        .metadata
        .as_ref()
        .expect("test pod should include metadata")
        .uid
        .clone();

    {
        let mut pods = service.pod_sandboxes.lock().await;
        pods.insert("pod-alpha".to_string(), pod_alpha);
        pods.insert("pod-bravo".to_string(), pod_bravo);
    }

    let mut container_alpha_annotations = HashMap::new();
    container_alpha_annotations.insert("io.kubernetes.pod.uid".to_string(), pod_alpha_uid);
    let mut container_bravo_annotations = HashMap::new();
    container_bravo_annotations.insert("io.kubernetes.pod.uid".to_string(), pod_bravo_uid);
    {
        let mut containers = service.containers.lock().await;
        containers.insert(
            "container-alpha".to_string(),
            test_container("container-alpha", "pod-alpha", container_alpha_annotations),
        );
        containers.insert(
            "container-bravo".to_string(),
            test_container("container-bravo", "pod-bravo", container_bravo_annotations),
        );
    }

    let mut selector = HashMap::new();
    selector.insert("tier".to_string(), "frontend".to_string());
    let response = RuntimeService::list_pod_sandbox_stats(
        &service,
        Request::new(ListPodSandboxStatsRequest {
            filter: Some(crate::proto::runtime::v1::PodSandboxStatsFilter {
                id: "pod-al".to_string(),
                label_selector: selector,
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner();

    assert_eq!(response.stats.len(), 1);
    assert_eq!(
        response.stats[0]
            .attributes
            .as_ref()
            .expect("pod stats should include attributes")
            .id,
        "pod-alpha"
    );
}

#[tokio::test]
async fn pod_sandbox_stats_resolves_short_id_and_hides_internal_annotations() {
    let (dir, service) = test_service_with_fake_runtime();

    let mut pod_annotations = HashMap::new();
    pod_annotations.insert("visible".to_string(), "true".to_string());
    RuntimeServiceImpl::insert_internal_state(
        &mut pod_annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            pause_container_id: Some("pause-pod-short-123".to_string()),
            ..Default::default()
        },
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-short-123".to_string(),
        test_pod("pod-short-123", pod_annotations),
    );
    service.containers.lock().await.insert(
        "container-pod-short-123".to_string(),
        test_container("container-pod-short-123", "pod-short-123", HashMap::new()),
    );
    set_fake_runtime_state(&dir, "container-pod-short-123", "running");
    set_fake_runtime_state(&dir, "pause-pod-short-123", "running");

    let response = RuntimeService::pod_sandbox_stats(
        &service,
        Request::new(PodSandboxStatsRequest {
            pod_sandbox_id: "pod-short".to_string(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let stats = response
        .stats
        .expect("short pod id should resolve to pod sandbox stats");
    let attributes = stats
        .attributes
        .expect("pod stats should include attributes");
    assert_eq!(attributes.id, "pod-short-123");
    assert_eq!(
        attributes.annotations.get("visible").map(String::as_str),
        Some("true")
    );
    assert!(
        !attributes.annotations.contains_key(INTERNAL_POD_STATE_KEY),
        "internal pod annotations should not leak through statsp responses"
    );
}

#[tokio::test]
async fn list_pod_sandbox_stats_returns_empty_when_short_id_filter_is_ambiguous() {
    let service = test_service();

    {
        let mut pods = service.pod_sandboxes.lock().await;
        pods.insert(
            "pod-ambiguous-a".to_string(),
            test_pod("pod-ambiguous-a", HashMap::new()),
        );
        pods.insert(
            "pod-ambiguous-b".to_string(),
            test_pod("pod-ambiguous-b", HashMap::new()),
        );
    }

    let response = RuntimeService::list_pod_sandbox_stats(
        &service,
        Request::new(ListPodSandboxStatsRequest {
            filter: Some(crate::proto::runtime::v1::PodSandboxStatsFilter {
                id: "pod-ambiguous".to_string(),
                ..Default::default()
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner();

    assert!(
        response.stats.is_empty(),
        "ambiguous short pod ids should return no stats entries"
    );
}

#[tokio::test]
async fn list_metric_descriptors_returns_non_empty_supported_names() {
    let service = test_service();
    let response = RuntimeService::list_metric_descriptors(
        &service,
        Request::new(ListMetricDescriptorsRequest {}),
    )
    .await
    .unwrap()
    .into_inner();

    assert!(
        !response.descriptors.is_empty(),
        "metric descriptors should not be empty"
    );
    let names: HashSet<String> = response
        .descriptors
        .into_iter()
        .map(|descriptor| descriptor.name)
        .collect();
    assert!(names.contains("container_cpu_usage_seconds_total"));
    assert!(names.contains("container_memory_working_set_bytes"));
    assert!(names.contains("container_memory_usage_bytes"));
    assert!(names.contains("container_spec_memory_limit_bytes"));
}

#[tokio::test]
async fn list_pod_sandbox_metrics_returns_pod_and_container_entries() {
    let service = test_service();

    let pod = test_pod("pod-metrics", HashMap::new());
    let pod_uid = pod
        .metadata
        .as_ref()
        .expect("test pod should include metadata")
        .uid
        .clone();
    service
        .pod_sandboxes
        .lock()
        .await
        .insert("pod-metrics".to_string(), pod);

    let mut container_annotations = HashMap::new();
    container_annotations.insert("io.kubernetes.pod.uid".to_string(), pod_uid);
    service.containers.lock().await.insert(
        "container-metrics".to_string(),
        test_container("container-metrics", "pod-metrics", container_annotations),
    );

    let response = RuntimeService::list_pod_sandbox_metrics(
        &service,
        Request::new(ListPodSandboxMetricsRequest {}),
    )
    .await
    .unwrap()
    .into_inner();

    assert_eq!(response.pod_metrics.len(), 1);
    let pod_metrics = &response.pod_metrics[0];
    assert_eq!(pod_metrics.pod_sandbox_id, "pod-metrics");
    assert!(
        !pod_metrics.metrics.is_empty(),
        "pod-level metrics should not be empty"
    );
    assert_eq!(pod_metrics.container_metrics.len(), 1);
    assert_eq!(
        pod_metrics.container_metrics[0].container_id,
        "container-metrics"
    );
    assert!(
        !pod_metrics.container_metrics[0].metrics.is_empty(),
        "container-level metrics should not be empty"
    );
}

#[tokio::test]
async fn get_container_events_streams_broadcast_events() {
    let service = test_service();
    let mut stream =
        RuntimeService::get_container_events(&service, Request::new(GetEventsRequest {}))
            .await
            .unwrap()
            .into_inner();

    service.publish_event(ContainerEventResponse {
        container_id: "container-1".to_string(),
        container_event_type: ContainerEventType::ContainerCreatedEvent as i32,
        created_at: 1,
        pod_sandbox_status: None,
        containers_statuses: Vec::new(),
    });

    let event = stream.next().await.unwrap().unwrap();
    assert_eq!(event.container_id, "container-1");
    assert_eq!(
        event.container_event_type,
        ContainerEventType::ContainerCreatedEvent as i32
    );
}

#[tokio::test]
async fn get_container_events_broadcasts_to_multiple_subscribers() {
    let service = test_service();
    let mut stream_a =
        RuntimeService::get_container_events(&service, Request::new(GetEventsRequest {}))
            .await
            .unwrap()
            .into_inner();
    let mut stream_b =
        RuntimeService::get_container_events(&service, Request::new(GetEventsRequest {}))
            .await
            .unwrap()
            .into_inner();

    service.publish_event(ContainerEventResponse {
        container_id: "shared".to_string(),
        container_event_type: ContainerEventType::ContainerStartedEvent as i32,
        created_at: 2,
        pod_sandbox_status: None,
        containers_statuses: Vec::new(),
    });

    let event_a = stream_a.next().await.unwrap().unwrap();
    let event_b = stream_b.next().await.unwrap().unwrap();
    assert_eq!(event_a.container_id, "shared");
    assert_eq!(event_b.container_id, "shared");
    assert_eq!(event_a.container_event_type, event_b.container_event_type);
}

#[tokio::test]
async fn get_container_events_preserves_publish_order() {
    let service = test_service();
    let mut stream =
        RuntimeService::get_container_events(&service, Request::new(GetEventsRequest {}))
            .await
            .unwrap()
            .into_inner();

    for (id, event_type) in [
        ("created", ContainerEventType::ContainerCreatedEvent),
        ("started", ContainerEventType::ContainerStartedEvent),
        ("stopped", ContainerEventType::ContainerStoppedEvent),
        ("deleted", ContainerEventType::ContainerDeletedEvent),
    ] {
        service.publish_event(ContainerEventResponse {
            container_id: id.to_string(),
            container_event_type: event_type as i32,
            created_at: RuntimeServiceImpl::now_nanos(),
            pod_sandbox_status: None,
            containers_statuses: Vec::new(),
        });
    }

    let mut received = Vec::new();
    for _ in 0..4 {
        let event = stream.next().await.unwrap().unwrap();
        received.push((event.container_id, event.container_event_type));
    }

    assert_eq!(
        received,
        vec![
            (
                "created".to_string(),
                ContainerEventType::ContainerCreatedEvent as i32
            ),
            (
                "started".to_string(),
                ContainerEventType::ContainerStartedEvent as i32
            ),
            (
                "stopped".to_string(),
                ContainerEventType::ContainerStoppedEvent as i32
            ),
            (
                "deleted".to_string(),
                ContainerEventType::ContainerDeletedEvent as i32
            ),
        ]
    );
}

#[tokio::test]
async fn pod_events_use_pause_container_id_when_available_and_preserve_order() {
    let service = test_service();
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            pause_container_id: Some("pause-event-1".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    let pod = test_pod("pod-event-1", annotations);

    let mut stream =
        RuntimeService::get_container_events(&service, Request::new(GetEventsRequest {}))
            .await
            .unwrap()
            .into_inner();

    service
        .emit_pod_event(ContainerEventType::ContainerCreatedEvent, &pod, Vec::new())
        .await;
    service
        .emit_pod_event(ContainerEventType::ContainerStartedEvent, &pod, Vec::new())
        .await;

    let created = stream.next().await.unwrap().unwrap();
    let started = stream.next().await.unwrap().unwrap();
    assert_eq!(created.container_id, "pause-event-1");
    assert_eq!(started.container_id, "pause-event-1");
    assert_eq!(
        created.container_event_type,
        ContainerEventType::ContainerCreatedEvent as i32
    );
    assert_eq!(
        started.container_event_type,
        ContainerEventType::ContainerStartedEvent as i32
    );
    assert_eq!(
        started
            .pod_sandbox_status
            .expect("pod status should be included")
            .id,
        "pod-event-1"
    );
}

#[tokio::test]
async fn publish_event_without_subscribers_does_not_panic() {
    let service = test_service();
    service.publish_event(ContainerEventResponse {
        container_id: "orphan".to_string(),
        container_event_type: ContainerEventType::ContainerCreatedEvent as i32,
        created_at: 1,
        pod_sandbox_status: None,
        containers_statuses: Vec::new(),
    });
}

#[tokio::test]
async fn get_container_events_reports_lagged_consumers() {
    let service = test_service();
    let mut stream =
        RuntimeService::get_container_events(&service, Request::new(GetEventsRequest {}))
            .await
            .unwrap()
            .into_inner();

    for idx in 0..600 {
        service.publish_event(ContainerEventResponse {
            container_id: format!("container-{}", idx),
            container_event_type: ContainerEventType::ContainerCreatedEvent as i32,
            created_at: idx,
            pod_sandbox_status: None,
            containers_statuses: Vec::new(),
        });
    }

    let mut saw_lagged = false;
    for _ in 0..260 {
        match timeout(Duration::from_millis(200), stream.next()).await {
            Ok(Some(Err(status))) if status.code() == tonic::Code::ResourceExhausted => {
                saw_lagged = true;
                break;
            }
            Ok(Some(_)) => continue,
            _ => break,
        }
    }

    assert!(saw_lagged, "expected lagged consumer error");
}

#[tokio::test]
async fn exit_monitor_publishes_async_stop_events() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState::default(),
    )
    .unwrap();
    service.containers.lock().await.insert(
        "async-stop".to_string(),
        Container {
            state: ContainerState::ContainerRunning as i32,
            ..test_container("async-stop", "pod-1", annotations)
        },
    );
    service
        .persistence
        .lock()
        .await
        .save_container(
            "async-stop",
            "pod-1",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &service
                .containers
                .lock()
                .await
                .get("async-stop")
                .unwrap()
                .annotations,
        )
        .unwrap();
    service.ensure_exit_monitor_registered("async-stop");

    let mut stream =
        RuntimeService::get_container_events(&service, Request::new(GetEventsRequest {}))
            .await
            .unwrap()
            .into_inner();

    let exit_code_path = dir
        .path()
        .join("shims")
        .join("async-stop")
        .join("exit_code");
    fs::create_dir_all(exit_code_path.parent().unwrap()).unwrap();
    fs::write(&exit_code_path, "17").unwrap();

    let event = timeout(Duration::from_secs(3), async {
        loop {
            if let Some(Ok(event)) = stream.next().await {
                if event.container_id == "async-stop"
                    && event.container_event_type
                        == ContainerEventType::ContainerStoppedEvent as i32
                {
                    return event;
                }
            }
        }
    })
    .await
    .expect("timed out waiting for async stop event");

    assert_eq!(event.container_id, "async-stop");
    let container = service
        .containers
        .lock()
        .await
        .get("async-stop")
        .cloned()
        .unwrap();
    assert_eq!(container.state, ContainerState::ContainerExited as i32);
    let state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
        &container.annotations,
        INTERNAL_CONTAINER_STATE_KEY,
    )
    .unwrap();
    assert_eq!(state.exit_code, Some(17));
    assert!(state.nri_stop_notified);
    assert_eq!(fake_nri.calls.lock().await.clone(), vec!["stop_container"]);

    let persisted = service
        .persistence
        .lock()
        .await
        .storage()
        .get_container("async-stop")
        .unwrap()
        .unwrap();
    let persisted_annotations: HashMap<String, String> =
        serde_json::from_str(&persisted.annotations).unwrap();
    let persisted_state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
        &persisted_annotations,
        INTERNAL_CONTAINER_STATE_KEY,
    )
    .unwrap();
    assert!(persisted_state.nri_stop_notified);
}

#[tokio::test]
async fn port_forward_validates_pod_state() {
    let service = test_service();
    service
        .pod_sandboxes
        .lock()
        .await
        .insert("pod-1".to_string(), test_pod("pod-1", HashMap::new()));

    let mut not_ready_pod = test_pod("pod-2", HashMap::new());
    not_ready_pod.state = PodSandboxState::SandboxNotready as i32;
    service
        .pod_sandboxes
        .lock()
        .await
        .insert("pod-2".to_string(), not_ready_pod);
    let not_ready = RuntimeService::port_forward(
        &service,
        Request::new(PortForwardRequest {
            pod_sandbox_id: "pod-2".to_string(),
            port: Vec::new(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(not_ready.code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn port_forward_requires_existing_netns_and_returns_stream_url() {
    let service = test_service();
    let mut missing_netns_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut missing_netns_annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            runtime_handler: "runc".to_string(),
            ..Default::default()
        },
    )
    .unwrap();
    service.pod_sandboxes.lock().await.insert(
        "pod-missing-netns".to_string(),
        test_pod("pod-missing-netns", missing_netns_annotations),
    );
    let missing_netns = RuntimeService::port_forward(
        &service,
        Request::new(PortForwardRequest {
            pod_sandbox_id: "pod-missing-netns".to_string(),
            port: vec![80],
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(missing_netns.code(), tonic::Code::FailedPrecondition);

    let netns_dir = tempdir().unwrap();
    let netns_path = netns_dir.path().join("netns");
    fs::write(&netns_path, "placeholder").unwrap();
    let mut ready_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut ready_annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            netns_path: Some(netns_path.display().to_string()),
            runtime_handler: "runc".to_string(),
            ..Default::default()
        },
    )
    .unwrap();
    service.pod_sandboxes.lock().await.insert(
        "pod-ready".to_string(),
        test_pod("pod-ready", ready_annotations),
    );
    service
        .set_streaming_server(crate::streaming::StreamingServer::for_test(
            "http://127.0.0.1:12345",
        ))
        .await;
    let response = RuntimeService::port_forward(
        &service,
        Request::new(PortForwardRequest {
            pod_sandbox_id: "pod-ready".to_string(),
            port: vec![80],
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(response.url.contains("/portforward/"));
}

#[tokio::test]
async fn port_forward_refreshes_stale_pause_container_state() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "pause-portforward", "stopped");

    let netns_dir = tempdir().unwrap();
    let netns_path = netns_dir.path().join("netns");
    fs::write(&netns_path, "placeholder").unwrap();
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            netns_path: Some(netns_path.display().to_string()),
            runtime_handler: "runc".to_string(),
            pause_container_id: Some("pause-portforward".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    service.pod_sandboxes.lock().await.insert(
        "pod-stale-ready".to_string(),
        crate::proto::runtime::v1::PodSandbox {
            state: PodSandboxState::SandboxReady as i32,
            ..test_pod("pod-stale-ready", annotations)
        },
    );
    service
        .set_streaming_server(crate::streaming::StreamingServer::for_test(
            "http://127.0.0.1:12345",
        ))
        .await;

    let err = RuntimeService::port_forward(
        &service,
        Request::new(PortForwardRequest {
            pod_sandbox_id: "pod-stale-ready".to_string(),
            port: vec![80],
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("not ready"));
}

#[tokio::test]
async fn stop_and_remove_existing_container_support_repeat_calls() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "container-1", "running");
    service.containers.lock().await.insert(
        "container-1".to_string(),
        test_container("container-1", "pod-1", HashMap::new()),
    );

    RuntimeService::stop_container(
        &service,
        Request::new(StopContainerRequest {
            container_id: "container-1".to_string(),
            timeout: 1,
        }),
    )
    .await
    .unwrap();
    RuntimeService::stop_container(
        &service,
        Request::new(StopContainerRequest {
            container_id: "container-1".to_string(),
            timeout: 1,
        }),
    )
    .await
    .unwrap();

    set_fake_runtime_state(&dir, "container-2", "running");
    service.containers.lock().await.insert(
        "container-2".to_string(),
        test_container("container-2", "pod-1", HashMap::new()),
    );
    RuntimeService::remove_container(
        &service,
        Request::new(RemoveContainerRequest {
            container_id: "container-2".to_string(),
        }),
    )
    .await
    .unwrap();
    RuntimeService::remove_container(
        &service,
        Request::new(RemoveContainerRequest {
            container_id: "container-2".to_string(),
        }),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn stop_and_remove_existing_pod_support_repeat_calls() {
    let (dir, service) = test_service_with_fake_runtime();

    let pod = test_pod("pod-1", HashMap::new());
    service
        .pod_sandboxes
        .lock()
        .await
        .insert("pod-1".to_string(), pod);
    RuntimeService::stop_pod_sandbox(
        &service,
        Request::new(StopPodSandboxRequest {
            pod_sandbox_id: "pod-1".to_string(),
        }),
    )
    .await
    .unwrap();
    RuntimeService::stop_pod_sandbox(
        &service,
        Request::new(StopPodSandboxRequest {
            pod_sandbox_id: "pod-1".to_string(),
        }),
    )
    .await
    .unwrap();

    set_fake_runtime_state(&dir, "container-under-pod", "running");
    service.containers.lock().await.insert(
        "container-under-pod".to_string(),
        test_container("container-under-pod", "pod-2", HashMap::new()),
    );
    service
        .pod_sandboxes
        .lock()
        .await
        .insert("pod-2".to_string(), test_pod("pod-2", HashMap::new()));
    RuntimeService::remove_pod_sandbox(
        &service,
        Request::new(RemovePodSandboxRequest {
            pod_sandbox_id: "pod-2".to_string(),
        }),
    )
    .await
    .unwrap();
    RuntimeService::remove_pod_sandbox(
        &service,
        Request::new(RemovePodSandboxRequest {
            pod_sandbox_id: "pod-2".to_string(),
        }),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn recover_state_reconciles_running_container_to_exited() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "recover-container", "stopped");

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            metadata_name: Some("recover".to_string()),
            ..Default::default()
        },
    )
    .unwrap();

    service
        .persistence
        .lock()
        .await
        .save_container(
            "recover-container",
            "pod-1",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();

    service.recover_state().await.unwrap();

    let container = service
        .containers
        .lock()
        .await
        .get("recover-container")
        .cloned()
        .unwrap();
    assert_eq!(container.state, ContainerState::ContainerExited as i32);
    let state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
        &container.annotations,
        INTERNAL_CONTAINER_STATE_KEY,
    )
    .unwrap();
    assert_eq!(state.exit_code, Some(0));
    assert!(state.finished_at.is_some());

    let persisted = service
        .persistence
        .lock()
        .await
        .storage()
        .get_container("recover-container")
        .unwrap()
        .unwrap();
    assert_eq!(persisted.state, "stopped");
}

#[tokio::test]
async fn recover_state_marks_ready_pod_notready_when_pause_is_stopped() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "pause-1", "stopped");
    let netns_path = dir.path().join("pause-netns");
    fs::write(&netns_path, "netns").unwrap();

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            runtime_handler: "runc".to_string(),
            netns_path: Some(netns_path.display().to_string()),
            pause_container_id: Some("pause-1".to_string()),
            ..Default::default()
        },
    )
    .unwrap();

    service
        .persistence
        .lock()
        .await
        .storage_mut()
        .save_pod_sandbox(&PodSandboxRecord {
            id: "pod-recover".to_string(),
            state: "ready".to_string(),
            name: "pod-recover".to_string(),
            namespace: "default".to_string(),
            uid: "uid-1".to_string(),
            created_at: RuntimeServiceImpl::now_nanos(),
            netns_path: netns_path.display().to_string(),
            labels: "{}".to_string(),
            annotations: serde_json::to_string(&annotations).unwrap(),
            pause_container_id: Some("pause-1".to_string()),
            ip: None,
        })
        .unwrap();

    service.recover_state().await.unwrap();

    let pod = service
        .pod_sandboxes
        .lock()
        .await
        .get("pod-recover")
        .cloned()
        .unwrap();
    assert_eq!(pod.state, PodSandboxState::SandboxNotready as i32);
    let persisted = service
        .persistence
        .lock()
        .await
        .storage()
        .get_pod_sandbox("pod-recover")
        .unwrap()
        .unwrap();
    assert_eq!(persisted.state, "notready");
}

#[tokio::test]
async fn recover_state_does_not_replay_historical_events() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "recover-container", "stopped");

    service
        .persistence
        .lock()
        .await
        .storage_mut()
        .save_container(&ContainerRecord {
            id: "recover-container".to_string(),
            pod_id: "pod-1".to_string(),
            state: "running".to_string(),
            image: "busybox:latest".to_string(),
            command: String::new(),
            created_at: RuntimeServiceImpl::now_nanos(),
            labels: "{}".to_string(),
            annotations: "{}".to_string(),
            exit_code: None,
            exit_time: None,
        })
        .unwrap();

    let mut stream =
        RuntimeService::get_container_events(&service, Request::new(GetEventsRequest {}))
            .await
            .unwrap()
            .into_inner();

    service.recover_state().await.unwrap();

    assert!(
        timeout(Duration::from_millis(100), stream.next())
            .await
            .is_err(),
        "recover_state should not replay historical events"
    );
}

#[tokio::test]
async fn recover_state_re_registers_exit_monitor_for_running_container() {
    let fake_nri = Arc::new(FakeNri::default());
    let (dir, service) = test_service_with_fake_runtime_and_nri(fake_nri.clone());
    set_fake_runtime_state(&dir, "recover-running", "running");

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            metadata_name: Some("recover-running".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    service
        .persistence
        .lock()
        .await
        .storage_mut()
        .save_container(&ContainerRecord {
            id: "recover-running".to_string(),
            pod_id: "pod-1".to_string(),
            state: "running".to_string(),
            image: "busybox:latest".to_string(),
            command: String::new(),
            created_at: RuntimeServiceImpl::now_nanos(),
            labels: "{}".to_string(),
            annotations: serde_json::to_string(&annotations).unwrap(),
            exit_code: None,
            exit_time: None,
        })
        .unwrap();

    let mut stream =
        RuntimeService::get_container_events(&service, Request::new(GetEventsRequest {}))
            .await
            .unwrap()
            .into_inner();

    service.recover_state().await.unwrap();

    let exit_code_path = dir
        .path()
        .join("shims")
        .join("recover-running")
        .join("exit_code");
    fs::create_dir_all(exit_code_path.parent().unwrap()).unwrap();
    fs::write(&exit_code_path, "23").unwrap();

    let event = timeout(Duration::from_secs(3), async {
        loop {
            if let Some(Ok(event)) = stream.next().await {
                if event.container_id == "recover-running"
                    && event.container_event_type
                        == ContainerEventType::ContainerStoppedEvent as i32
                {
                    return event;
                }
            }
        }
    })
    .await
    .expect("timed out waiting for recovered container stop event");

    assert_eq!(event.container_id, "recover-running");
    let container = service
        .containers
        .lock()
        .await
        .get("recover-running")
        .cloned()
        .unwrap();
    assert_eq!(container.state, ContainerState::ContainerExited as i32);
    let state = RuntimeServiceImpl::read_internal_state::<StoredContainerState>(
        &container.annotations,
        INTERNAL_CONTAINER_STATE_KEY,
    )
    .unwrap();
    assert_eq!(state.exit_code, Some(23));
    assert!(state.nri_stop_notified);
    assert_eq!(fake_nri.calls.lock().await.clone(), vec!["stop_container"]);
}

#[tokio::test]
async fn recover_state_supports_inspect_and_list_after_restart() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "recover-container", "running");
    set_fake_runtime_state(&dir, "pause-recover", "running");
    let netns_path = dir.path().join("recover.netns");
    fs::write(&netns_path, "netns").unwrap();

    let mut pod_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut pod_annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            runtime_handler: "runc".to_string(),
            netns_path: Some(netns_path.display().to_string()),
            pause_container_id: Some("pause-recover".to_string()),
            ip: Some("10.88.0.20".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    service
        .persistence
        .lock()
        .await
        .storage_mut()
        .save_pod_sandbox(&PodSandboxRecord {
            id: "pod-recover".to_string(),
            state: "ready".to_string(),
            name: "pod-recover".to_string(),
            namespace: "default".to_string(),
            uid: "uid-recover".to_string(),
            created_at: RuntimeServiceImpl::now_nanos(),
            netns_path: netns_path.display().to_string(),
            labels: "{}".to_string(),
            annotations: serde_json::to_string(&pod_annotations).unwrap(),
            pause_container_id: Some("pause-recover".to_string()),
            ip: Some("10.88.0.20".to_string()),
        })
        .unwrap();

    let mut container_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut container_annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            metadata_name: Some("recover-container".to_string()),
            log_path: Some(
                dir.path()
                    .join("logs")
                    .join("recover.log")
                    .display()
                    .to_string(),
            ),
            ..Default::default()
        },
    )
    .unwrap();
    service
        .persistence
        .lock()
        .await
        .save_container(
            "recover-container",
            "pod-recover",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &container_annotations,
        )
        .unwrap();

    service.recover_state().await.unwrap();

    let container_status = RuntimeService::container_status(
        &service,
        Request::new(ContainerStatusRequest {
            container_id: "recover-container".to_string(),
            verbose: true,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let pod_status = RuntimeService::pod_sandbox_status(
        &service,
        Request::new(PodSandboxStatusRequest {
            pod_sandbox_id: "pod-recover".to_string(),
            verbose: true,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let containers =
        RuntimeService::list_containers(&service, Request::new(ListContainersRequest::default()))
            .await
            .unwrap()
            .into_inner();
    let pods =
        RuntimeService::list_pod_sandbox(&service, Request::new(ListPodSandboxRequest::default()))
            .await
            .unwrap()
            .into_inner();

    assert_eq!(
        container_status.status.as_ref().unwrap().state,
        ContainerState::ContainerRunning as i32
    );
    assert_eq!(
        pod_status.status.as_ref().unwrap().state,
        PodSandboxState::SandboxReady as i32
    );
    assert_eq!(containers.containers.len(), 1);
    assert_eq!(containers.containers[0].pod_sandbox_id, "pod-recover");
    assert_eq!(pods.items.len(), 1);
    let info: serde_json::Value =
        serde_json::from_str(container_status.info.get("info").unwrap()).unwrap();
    assert_eq!(info["sandboxID"], "pod-recover");
}

#[tokio::test]
async fn recover_state_supports_stop_and_remove_after_restart() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "recover-container", "running");
    set_fake_runtime_state(&dir, "pause-recover", "running");
    let netns_path = dir.path().join("recover-remove.netns");
    fs::write(&netns_path, "netns").unwrap();

    let mut pod_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut pod_annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            runtime_handler: "runc".to_string(),
            netns_path: Some(netns_path.display().to_string()),
            pause_container_id: Some("pause-recover".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    service
        .persistence
        .lock()
        .await
        .storage_mut()
        .save_pod_sandbox(&PodSandboxRecord {
            id: "pod-recover".to_string(),
            state: "ready".to_string(),
            name: "pod-recover".to_string(),
            namespace: "default".to_string(),
            uid: "uid-recover".to_string(),
            created_at: RuntimeServiceImpl::now_nanos(),
            netns_path: netns_path.display().to_string(),
            labels: "{}".to_string(),
            annotations: serde_json::to_string(&pod_annotations).unwrap(),
            pause_container_id: Some("pause-recover".to_string()),
            ip: None,
        })
        .unwrap();
    service
        .persistence
        .lock()
        .await
        .save_container(
            "recover-container",
            "pod-recover",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &HashMap::new(),
        )
        .unwrap();

    service.recover_state().await.unwrap();

    RuntimeService::stop_container(
        &service,
        Request::new(StopContainerRequest {
            container_id: "recover-container".to_string(),
            timeout: 1,
        }),
    )
    .await
    .unwrap();
    RuntimeService::remove_container(
        &service,
        Request::new(RemoveContainerRequest {
            container_id: "recover-container".to_string(),
        }),
    )
    .await
    .unwrap();
    RuntimeService::stop_pod_sandbox(
        &service,
        Request::new(StopPodSandboxRequest {
            pod_sandbox_id: "pod-recover".to_string(),
        }),
    )
    .await
    .unwrap();
    RuntimeService::remove_pod_sandbox(
        &service,
        Request::new(RemovePodSandboxRequest {
            pod_sandbox_id: "pod-recover".to_string(),
        }),
    )
    .await
    .unwrap();

    assert!(service
        .persistence
        .lock()
        .await
        .storage()
        .get_container("recover-container")
        .unwrap()
        .is_none());
    assert!(service
        .persistence
        .lock()
        .await
        .storage()
        .get_pod_sandbox("pod-recover")
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn recover_state_marks_ready_pod_notready_when_netns_is_missing() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "pause-missing-netns", "running");

    let missing_netns_path = dir.path().join("missing.netns");
    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            runtime_handler: "runc".to_string(),
            netns_path: Some(missing_netns_path.display().to_string()),
            pause_container_id: Some("pause-missing-netns".to_string()),
            ..Default::default()
        },
    )
    .unwrap();

    service
        .persistence
        .lock()
        .await
        .storage_mut()
        .save_pod_sandbox(&PodSandboxRecord {
            id: "pod-missing-netns".to_string(),
            state: "ready".to_string(),
            name: "pod-missing-netns".to_string(),
            namespace: "default".to_string(),
            uid: "uid-missing-netns".to_string(),
            created_at: RuntimeServiceImpl::now_nanos(),
            netns_path: missing_netns_path.display().to_string(),
            labels: "{}".to_string(),
            annotations: serde_json::to_string(&annotations).unwrap(),
            pause_container_id: Some("pause-missing-netns".to_string()),
            ip: Some("10.88.0.21".to_string()),
        })
        .unwrap();

    service.recover_state().await.unwrap();

    let pod = service
        .pod_sandboxes
        .lock()
        .await
        .get("pod-missing-netns")
        .cloned()
        .unwrap();
    assert_eq!(pod.state, PodSandboxState::SandboxNotready as i32);

    let persisted = service
        .persistence
        .lock()
        .await
        .storage()
        .get_pod_sandbox("pod-missing-netns")
        .unwrap()
        .unwrap();
    assert_eq!(persisted.state, "notready");
}

#[tokio::test]
async fn remove_pod_sandbox_cascades_recovered_containers_after_restart() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "recover-container", "running");
    set_fake_runtime_state(&dir, "pause-recover", "running");
    let netns_path = dir.path().join("recover-cascade.netns");
    fs::write(&netns_path, "netns").unwrap();

    let mut pod_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut pod_annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            runtime_handler: "runc".to_string(),
            netns_path: Some(netns_path.display().to_string()),
            pause_container_id: Some("pause-recover".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    service
        .persistence
        .lock()
        .await
        .storage_mut()
        .save_pod_sandbox(&PodSandboxRecord {
            id: "pod-recover".to_string(),
            state: "ready".to_string(),
            name: "pod-recover".to_string(),
            namespace: "default".to_string(),
            uid: "uid-recover".to_string(),
            created_at: RuntimeServiceImpl::now_nanos(),
            netns_path: netns_path.display().to_string(),
            labels: "{}".to_string(),
            annotations: serde_json::to_string(&pod_annotations).unwrap(),
            pause_container_id: Some("pause-recover".to_string()),
            ip: None,
        })
        .unwrap();

    let mut container_annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut container_annotations,
        INTERNAL_CONTAINER_STATE_KEY,
        &StoredContainerState {
            metadata_name: Some("recover-container".to_string()),
            ..Default::default()
        },
    )
    .unwrap();
    service
        .persistence
        .lock()
        .await
        .save_container(
            "recover-container",
            "pod-recover",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &container_annotations,
        )
        .unwrap();

    service.recover_state().await.unwrap();

    RuntimeService::remove_pod_sandbox(
        &service,
        Request::new(RemovePodSandboxRequest {
            pod_sandbox_id: "pod-recover".to_string(),
        }),
    )
    .await
    .unwrap();

    assert!(service
        .containers
        .lock()
        .await
        .get("recover-container")
        .is_none());
    assert!(service
        .persistence
        .lock()
        .await
        .storage()
        .get_container("recover-container")
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn container_status_refreshes_stale_runtime_state_on_query() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "refresh-container", "stopped");

    service.containers.lock().await.insert(
        "refresh-container".to_string(),
        Container {
            state: ContainerState::ContainerRunning as i32,
            ..test_container("refresh-container", "pod-1", HashMap::new())
        },
    );

    let response = RuntimeService::container_status(
        &service,
        Request::new(ContainerStatusRequest {
            container_id: "refresh-container".to_string(),
            verbose: false,
        }),
    )
    .await
    .unwrap()
    .into_inner();

    assert_eq!(
        response.status.unwrap().state,
        ContainerState::ContainerExited as i32
    );
}

#[tokio::test]
async fn pod_queries_refresh_pause_container_state_on_query() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "pause-refresh", "stopped");

    let mut annotations = HashMap::new();
    RuntimeServiceImpl::insert_internal_state(
        &mut annotations,
        INTERNAL_POD_STATE_KEY,
        &StoredPodState {
            runtime_handler: "runc".to_string(),
            pause_container_id: Some("pause-refresh".to_string()),
            ..Default::default()
        },
    )
    .unwrap();

    service.pod_sandboxes.lock().await.insert(
        "pod-refresh".to_string(),
        crate::proto::runtime::v1::PodSandbox {
            state: PodSandboxState::SandboxReady as i32,
            ..test_pod("pod-refresh", annotations)
        },
    );

    let list =
        RuntimeService::list_pod_sandbox(&service, Request::new(ListPodSandboxRequest::default()))
            .await
            .unwrap()
            .into_inner();
    assert_eq!(list.items.len(), 1);
    assert_eq!(list.items[0].state, PodSandboxState::SandboxNotready as i32);

    let inspect = RuntimeService::pod_sandbox_status(
        &service,
        Request::new(PodSandboxStatusRequest {
            pod_sandbox_id: "pod-refresh".to_string(),
            verbose: false,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(
        inspect.status.unwrap().state,
        PodSandboxState::SandboxNotready as i32
    );
}

#[tokio::test]
async fn recover_state_ignores_stale_shim_metadata_artifacts() {
    let (dir, service) = test_service_with_fake_runtime();
    set_fake_runtime_state(&dir, "recover-container", "running");

    let stale_dir = dir.path().join("shims").join("orphan");
    fs::create_dir_all(&stale_dir).unwrap();
    fs::write(stale_dir.join("attach.sock"), "stale").unwrap();
    fs::write(
        stale_dir.join("shim.json"),
        serde_json::to_vec_pretty(&crate::runtime::ShimProcess {
            container_id: "orphan".to_string(),
            shim_pid: 999_999,
            exit_code_file: stale_dir.join("exit_code"),
            log_file: stale_dir.join("shim.log"),
            socket_path: stale_dir.join("attach.sock"),
            bundle_path: dir.path().join("bundles").join("orphan"),
        })
        .unwrap(),
    )
    .unwrap();

    service
        .persistence
        .lock()
        .await
        .save_container(
            "recover-container",
            "pod-1",
            crate::runtime::ContainerStatus::Running,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &HashMap::new(),
        )
        .unwrap();

    service.recover_state().await.unwrap();

    let containers =
        RuntimeService::list_containers(&service, Request::new(ListContainersRequest::default()))
            .await
            .unwrap()
            .into_inner();
    assert_eq!(containers.containers.len(), 1);
    assert!(
        !stale_dir.exists(),
        "recover_state should clean orphaned shim artifacts"
    );
}
