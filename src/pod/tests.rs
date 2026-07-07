use super::*;
use crate::network::NetworkError;
use crate::runtime::RuncRuntime;
use async_trait::async_trait;
use std::sync::{Arc, Mutex};
use tempfile::tempdir;

#[derive(Clone, Default)]
struct RecordingRuntime {
    created: Arc<Mutex<Vec<(String, ContainerConfig)>>>,
    removed: Arc<Mutex<Vec<String>>>,
    fail_start: Arc<Mutex<bool>>,
}

impl RecordingRuntime {
    fn take_created(&self) -> Vec<(String, ContainerConfig)> {
        self.created.lock().unwrap().clone()
    }

    fn take_removed(&self) -> Vec<String> {
        self.removed.lock().unwrap().clone()
    }

    fn set_fail_start(&self, value: bool) {
        *self.fail_start.lock().unwrap() = value;
    }
}

impl ContainerRuntime for RecordingRuntime {
    fn create_container(&self, container_id: &str, config: &ContainerConfig) -> Result<String> {
        self.created
            .lock()
            .unwrap()
            .push((container_id.to_string(), config.clone()));
        Ok(container_id.to_string())
    }

    fn start_container(&self, _container_id: &str) -> Result<()> {
        if *self.fail_start.lock().unwrap() {
            return Err(anyhow::anyhow!("start failed"));
        }
        Ok(())
    }

    fn stop_container(&self, _container_id: &str, _timeout: Option<u32>) -> Result<()> {
        Ok(())
    }

    fn remove_container(&self, container_id: &str) -> Result<()> {
        self.removed.lock().unwrap().push(container_id.to_string());
        Ok(())
    }

    fn container_status(&self, _container_id: &str) -> Result<ContainerStatus> {
        Ok(ContainerStatus::Running)
    }

    fn reopen_container_log(&self, _container_id: &str) -> Result<()> {
        Ok(())
    }

    fn exec_in_container(
        &self,
        _container_id: &str,
        _command: &[String],
        _tty: bool,
    ) -> Result<i32> {
        Ok(0)
    }

    fn update_container_resources(
        &self,
        _container_id: &str,
        _resources: &LinuxContainerResources,
    ) -> Result<()> {
        Ok(())
    }
}

#[derive(Clone, Default)]
struct RecordingNetworkManager {
    calls: Arc<Mutex<Vec<String>>>,
    fail_setup: Arc<Mutex<bool>>,
    fail_teardown: Arc<Mutex<bool>>,
}

impl RecordingNetworkManager {
    fn take_calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }

    fn set_fail_setup(&self, value: bool) {
        *self.fail_setup.lock().unwrap() = value;
    }

    fn set_fail_teardown(&self, value: bool) {
        *self.fail_teardown.lock().unwrap() = value;
    }
}

#[async_trait]
impl NetworkManager for RecordingNetworkManager {
    async fn init(&self) -> std::result::Result<(), NetworkError> {
        Ok(())
    }

    async fn create_network_namespace(
        &self,
        ns_path: &str,
    ) -> std::result::Result<(), NetworkError> {
        self.calls
            .lock()
            .unwrap()
            .push(format!("create_netns:{ns_path}"));
        Ok(())
    }

    async fn remove_network_namespace(
        &self,
        ns_path: &str,
    ) -> std::result::Result<(), NetworkError> {
        self.calls
            .lock()
            .unwrap()
            .push(format!("remove_netns:{ns_path}"));
        Ok(())
    }

    async fn setup_pod_network(
        &self,
        request: NetworkSetupRequest<'_>,
    ) -> std::result::Result<NetworkStatus, NetworkError> {
        self.calls
            .lock()
            .unwrap()
            .push(format!("setup_network:{}", request.pod_id));
        if *self.fail_setup.lock().unwrap() {
            return Err(NetworkError::Other("setup failed".to_string()));
        }
        Ok(NetworkStatus {
            name: "test".to_string(),
            ip: Some("10.88.0.2".parse().unwrap()),
            mac: None,
            interfaces: vec![],
            raw_result: None,
        })
    }

    async fn teardown_pod_network(
        &self,
        pod_id: &str,
        _netns: &str,
        _pod_namespace: &str,
        _pod_name: &str,
        _pod_uid: &str,
        _runtime_handler: &str,
    ) -> std::result::Result<(), NetworkError> {
        self.calls
            .lock()
            .unwrap()
            .push(format!("teardown_network:{pod_id}"));
        if *self.fail_teardown.lock().unwrap() {
            return Err(NetworkError::Other("teardown failed".to_string()));
        }
        Ok(())
    }
}

#[derive(Clone, Default)]
struct RecordingPortMapper {
    added: Arc<Mutex<Vec<HostPortMapping>>>,
    removed: Arc<Mutex<Vec<HostPortMapping>>>,
    fail_add_after: Arc<Mutex<Option<usize>>>,
    cleanup_calls: Arc<Mutex<usize>>,
}

impl RecordingPortMapper {
    fn take_added(&self) -> Vec<HostPortMapping> {
        self.added.lock().unwrap().clone()
    }

    fn take_removed(&self) -> Vec<HostPortMapping> {
        self.removed.lock().unwrap().clone()
    }

    fn fail_add_after(&self, count: usize) {
        *self.fail_add_after.lock().unwrap() = Some(count);
    }

    fn cleanup_calls(&self) -> usize {
        *self.cleanup_calls.lock().unwrap()
    }
}

impl PodPortMapper for RecordingPortMapper {
    fn add_port_mapping(&self, mapping: &HostPortMapping) -> Result<()> {
        let mut added = self.added.lock().unwrap();
        if self
            .fail_add_after
            .lock()
            .unwrap()
            .map(|limit| added.len() >= limit)
            .unwrap_or(false)
        {
            return Err(anyhow::anyhow!("port mapping add failed"));
        }
        added.push(mapping.clone());
        Ok(())
    }

    fn remove_port_mapping(&self, mapping: &HostPortMapping) -> Result<()> {
        self.removed.lock().unwrap().push(mapping.clone());
        Ok(())
    }

    fn cleanup_all(&self) -> Result<()> {
        *self.cleanup_calls.lock().unwrap() += 1;
        Ok(())
    }
}

fn test_pod_config() -> PodSandboxConfig {
    PodSandboxConfig {
        name: "test-pod".to_string(),
        namespace: "default".to_string(),
        uid: "uid-1".to_string(),
        hostname: "test-host".to_string(),
        log_directory: None,
        runtime_handler: "runc".to_string(),
        labels: vec![],
        annotations: vec![],
        dns_config: None,
        port_mappings: vec![],
        network_config: Some(NetworkConfig {
            network_namespace: "crius-default-test-pod".to_string(),
            pod_cidr: "10.88.0.0/16".to_string(),
        }),
        cgroup_parent: None,
        sysctls: HashMap::new(),
        namespace_options: None,
        privileged: false,
        run_as_user: None,
        run_as_group: None,
        supplemental_groups: vec![],
        readonly_rootfs: false,
        pids_limit: None,
        no_new_privileges: None,
        apparmor_profile: None,
        selinux_label: None,
        seccomp_profile: None,
        linux_resources: None,
    }
}

fn test_manager_options(root_dir: PathBuf) -> PodSandboxManagerOptions {
    PodSandboxManagerOptions {
        disable_hostport_mapping: false,
        root_dir,
        pause_image: "registry.k8s.io/pause:3.9".to_string(),
        pause_command: "/pause".to_string(),
        infra_ctr_cpuset: String::new(),
    }
}

#[tokio::test]
async fn create_resolv_conf_copies_host_config_when_dns_unspecified() {
    let temp_dir = tempdir().unwrap();
    let runtime = RuncRuntime::new(PathBuf::from("runc"), temp_dir.path().join("runtime"));
    let manager = PodSandboxManager::new(
        runtime,
        temp_dir.path().join("pods"),
        "registry.k8s.io/pause:3.9".to_string(),
        "/pause".to_string(),
        String::new(),
        CniConfig::default(),
    );
    tokio::fs::create_dir_all(temp_dir.path().join("pods").join("pod-1"))
        .await
        .unwrap();

    let resolv_path = manager.create_resolv_conf("pod-1", None).await.unwrap();
    let generated = tokio::fs::read_to_string(&resolv_path).await.unwrap();
    let host = tokio::fs::read_to_string("/etc/resolv.conf").await.unwrap();
    assert_eq!(generated, host);
}

#[tokio::test]
async fn create_resolv_conf_writes_explicit_dns_config() {
    let temp_dir = tempdir().unwrap();
    let runtime = RuncRuntime::new(PathBuf::from("runc"), temp_dir.path().join("runtime"));
    let manager = PodSandboxManager::new(
        runtime,
        temp_dir.path().join("pods"),
        "registry.k8s.io/pause:3.9".to_string(),
        "/pause".to_string(),
        String::new(),
        CniConfig::default(),
    );
    tokio::fs::create_dir_all(temp_dir.path().join("pods").join("pod-1"))
        .await
        .unwrap();

    let dns = DNSConfig {
        servers: vec!["1.1.1.1".to_string(), "8.8.8.8".to_string()],
        searches: vec!["example.com".to_string()],
        options: vec!["ndots:5".to_string()],
    };
    let resolv_path = manager
        .create_resolv_conf("pod-1", Some(&dns))
        .await
        .unwrap();
    let generated = tokio::fs::read_to_string(&resolv_path).await.unwrap();

    assert!(generated.contains("search example.com"));
    assert!(generated.contains("nameserver 1.1.1.1"));
    assert!(generated.contains("nameserver 8.8.8.8"));
    assert!(generated.contains("options ndots:5"));
}

#[tokio::test]
async fn create_pause_container_propagates_pod_metadata_to_runtime() {
    let temp_dir = tempdir().unwrap();
    let runtime = RecordingRuntime::default();
    let manager = PodSandboxManager::new(
        runtime.clone(),
        temp_dir.path().join("pods"),
        "registry.k8s.io/pause:3.9".to_string(),
        "/pause".to_string(),
        String::new(),
        CniConfig::default(),
    );
    tokio::fs::create_dir_all(temp_dir.path().join("pods").join("pod-1"))
        .await
        .unwrap();

    let pod_config = PodSandboxConfig {
        name: "test-pod".to_string(),
        namespace: "default".to_string(),
        uid: "uid-1".to_string(),
        hostname: "test-host".to_string(),
        log_directory: Some(temp_dir.path().join("logs")),
        runtime_handler: "runc".to_string(),
        labels: vec![("app".to_string(), "demo".to_string())],
        annotations: vec![
            ("team".to_string(), "runtime".to_string()),
            (
                "io.kubernetes.cri.sandbox-image".to_string(),
                "registry.example/pause:custom".to_string(),
            ),
        ],
        dns_config: None,
        port_mappings: vec![],
        network_config: None,
        cgroup_parent: Some("kubepods.slice".to_string()),
        sysctls: HashMap::from([("net.ipv4.ip_forward".to_string(), "1".to_string())]),
        namespace_options: None,
        privileged: false,
        run_as_user: Some("1000".to_string()),
        run_as_group: Some(1000),
        supplemental_groups: vec![2000],
        readonly_rootfs: true,
        pids_limit: Some(128),
        no_new_privileges: Some(true),
        apparmor_profile: Some("runtime/default".to_string()),
        selinux_label: Some("system_u:system_r:container_t:s0".to_string()),
        seccomp_profile: None,
        linux_resources: None,
    };

    let pause_id = manager
        .create_pause_container("pod-1", &pod_config, Path::new("/var/run/netns/pod-1"))
        .await
        .unwrap();

    assert_eq!(pause_id, "pause-pod-1");

    let created = runtime.take_created();
    assert_eq!(created.len(), 1);
    let (container_id, pause_config) = &created[0];
    assert_eq!(container_id, "pause-pod-1");
    assert_eq!(pause_config.image, "registry.example/pause:custom");
    assert_eq!(pause_config.command, vec!["/pause".to_string()]);
    assert_eq!(pause_config.hostname.as_deref(), Some("test-host"));
    assert_eq!(pause_config.supplemental_groups, vec![2000]);
    assert!(pause_config.readonly_rootfs);
    assert_eq!(pause_config.pids_limit, Some(128));
    assert_eq!(pause_config.labels, pod_config.labels);
    assert!(pause_config
        .annotations
        .starts_with(&pod_config.annotations));
    let annotations = pause_config
        .annotations
        .iter()
        .cloned()
        .collect::<HashMap<_, _>>();
    assert_eq!(
        annotations
            .get(CRIO_SANDBOX_ID_ANNOTATION)
            .map(String::as_str),
        Some("pod-1")
    );
    assert_eq!(
        annotations
            .get(CRIO_SANDBOX_NAME_ANNOTATION)
            .map(String::as_str),
        Some("test-pod")
    );
    assert_eq!(
        annotations
            .get(CRIO_POD_NAMESPACE_ANNOTATION)
            .map(String::as_str),
        Some("default")
    );
    assert_eq!(
        annotations
            .get(CONTAINERD_SANDBOX_UID_ANNOTATION)
            .map(String::as_str),
        Some("uid-1")
    );
    assert_eq!(
        annotations
            .get(CRIO_CONTAINER_NAME_ANNOTATION)
            .map(String::as_str),
        Some("pause-pod-1")
    );
    assert_eq!(
        annotations
            .get(CRIO_CONTAINER_TYPE_ANNOTATION)
            .map(String::as_str),
        Some(CONTAINER_TYPE_SANDBOX)
    );
    assert_eq!(
        annotations
            .get(CONTAINERD_CONTAINER_TYPE_ANNOTATION)
            .map(String::as_str),
        Some(CONTAINER_TYPE_SANDBOX)
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
            .get(CONTAINERD_SANDBOX_LOG_DIR_ANNOTATION)
            .map(String::as_str),
        Some(temp_dir.path().join("logs").to_string_lossy().as_ref())
    );
    assert_eq!(
        annotations
            .get(CRIO_LOG_PATH_ANNOTATION)
            .map(String::as_str),
        Some(
            temp_dir
                .path()
                .join("logs")
                .join("sandbox.log")
                .to_string_lossy()
                .as_ref()
        )
    );
    assert_eq!(
        pause_config.namespace_paths.network.as_deref(),
        Some(Path::new("/var/run/netns/pod-1"))
    );
}

#[tokio::test]
async fn create_pause_container_leaves_cgroup_parent_unset_by_default() {
    let temp_dir = tempdir().unwrap();
    let runtime = RecordingRuntime::default();
    let manager = PodSandboxManager::new(
        runtime.clone(),
        temp_dir.path().join("pods"),
        "registry.k8s.io/pause:3.9".to_string(),
        "/pause".to_string(),
        String::new(),
        CniConfig::default(),
    );
    tokio::fs::create_dir_all(temp_dir.path().join("pods").join("pod-1"))
        .await
        .unwrap();

    manager
        .create_pause_container(
            "pod-1",
            &test_pod_config(),
            Path::new("/var/run/netns/pod-1"),
        )
        .await
        .unwrap();

    let created = runtime.take_created();
    assert_eq!(created.len(), 1);
    assert_eq!(created[0].1.cgroup_parent, None);
}

#[tokio::test]
async fn create_pause_container_uses_configured_pause_command() {
    let temp_dir = tempdir().unwrap();
    let runtime = RecordingRuntime::default();
    let manager = PodSandboxManager::new(
        runtime.clone(),
        temp_dir.path().join("pods"),
        "registry.k8s.io/pause:3.9".to_string(),
        "/custom/pause".to_string(),
        String::new(),
        CniConfig::default(),
    );
    tokio::fs::create_dir_all(temp_dir.path().join("pods").join("pod-1"))
        .await
        .unwrap();

    manager
        .create_pause_container(
            "pod-1",
            &test_pod_config(),
            Path::new("/var/run/netns/pod-1"),
        )
        .await
        .unwrap();

    let created = runtime.take_created();
    assert_eq!(created.len(), 1);
    assert_eq!(created[0].1.command, vec!["/custom/pause".to_string()]);
    assert!(created[0].1.args.is_empty());
}

#[tokio::test]
async fn create_pause_container_applies_configured_infra_ctr_cpuset() {
    let temp_dir = tempdir().unwrap();
    let runtime = RecordingRuntime::default();
    let manager = PodSandboxManager::new(
        runtime.clone(),
        temp_dir.path().join("pods"),
        "registry.k8s.io/pause:3.9".to_string(),
        "/pause".to_string(),
        "1-2".to_string(),
        CniConfig::default(),
    );
    tokio::fs::create_dir_all(temp_dir.path().join("pods").join("pod-1"))
        .await
        .unwrap();

    manager
        .create_pause_container(
            "pod-1",
            &test_pod_config(),
            Path::new("/var/run/netns/pod-1"),
        )
        .await
        .unwrap();

    let created = runtime.take_created();
    let resources = created[0]
        .1
        .linux_resources
        .as_ref()
        .expect("pause container should carry linux resources");
    assert_eq!(resources.cpuset_cpus, "1-2");
}

#[tokio::test]
async fn create_pod_sandbox_rolls_back_workspace_and_netns_when_network_setup_fails() {
    let temp_dir = tempdir().unwrap();
    let runtime = RecordingRuntime::default();
    let network = RecordingNetworkManager::default();
    network.set_fail_setup(true);
    let mut manager = PodSandboxManager::with_network_manager(
        runtime,
        network.clone(),
        Box::new(RecordingPortMapper::default()),
        test_manager_options(temp_dir.path().join("pods")),
    );

    let err = manager
        .create_pod_sandbox(test_pod_config())
        .await
        .expect_err("network failure should rollback pod sandbox creation");
    assert!(err.to_string().contains("setup failed"));

    let calls = network.take_calls();
    assert!(calls.iter().any(|call| call.starts_with("create_netns:")));
    assert!(calls.iter().any(|call| call.starts_with("setup_network:")));
    assert!(calls
        .iter()
        .any(|call| call.starts_with("teardown_network:")));
    assert!(calls.iter().any(|call| call.starts_with("remove_netns:")));
    assert!(manager.pods.is_empty());
    assert!(std::fs::read_dir(temp_dir.path().join("pods"))
        .unwrap()
        .next()
        .is_none());
}

#[tokio::test]
async fn create_pod_sandbox_rolls_back_network_and_workspace_when_pause_start_fails() {
    let temp_dir = tempdir().unwrap();
    let runtime = RecordingRuntime::default();
    runtime.set_fail_start(true);
    let network = RecordingNetworkManager::default();
    let mut manager = PodSandboxManager::with_network_manager(
        runtime.clone(),
        network.clone(),
        Box::new(RecordingPortMapper::default()),
        test_manager_options(temp_dir.path().join("pods")),
    );

    let err = manager
        .create_pod_sandbox(test_pod_config())
        .await
        .expect_err("pause failure should rollback pod sandbox creation");
    assert!(err.to_string().contains("Failed to create pause container"));

    let calls = network.take_calls();
    assert!(calls.iter().any(|call| call.starts_with("setup_network:")));
    assert!(calls
        .iter()
        .any(|call| call.starts_with("teardown_network:")));
    assert!(calls.iter().any(|call| call.starts_with("remove_netns:")));
    let removed = runtime.take_removed();
    assert_eq!(removed.len(), 1);
    assert!(removed[0].starts_with("pause-"));
    assert!(manager.pods.is_empty());
    assert!(std::fs::read_dir(temp_dir.path().join("pods"))
        .unwrap()
        .next()
        .is_none());
}

#[tokio::test]
async fn create_pod_sandbox_continues_cleanup_when_network_teardown_fails() {
    let temp_dir = tempdir().unwrap();
    let runtime = RecordingRuntime::default();
    runtime.set_fail_start(true);
    let network = RecordingNetworkManager::default();
    network.set_fail_teardown(true);
    let mut manager = PodSandboxManager::with_network_manager(
        runtime,
        network.clone(),
        Box::new(RecordingPortMapper::default()),
        test_manager_options(temp_dir.path().join("pods")),
    );

    let err = manager
        .create_pod_sandbox(test_pod_config())
        .await
        .expect_err("pause failure should still cleanup when teardown fails");
    assert!(err.to_string().contains("Failed to create pause container"));

    let calls = network.take_calls();
    assert!(calls
        .iter()
        .any(|call| call.starts_with("teardown_network:")));
    assert!(calls.iter().any(|call| call.starts_with("remove_netns:")));
    assert!(manager.pods.is_empty());
    assert!(std::fs::read_dir(temp_dir.path().join("pods"))
        .unwrap()
        .next()
        .is_none());
}

#[tokio::test]
async fn create_pod_sandbox_host_network_skips_managed_netns_and_cni() {
    let temp_dir = tempdir().unwrap();
    let runtime = RecordingRuntime::default();
    let network = RecordingNetworkManager::default();
    let mut manager = PodSandboxManager::with_network_manager(
        runtime.clone(),
        network.clone(),
        Box::new(RecordingPortMapper::default()),
        test_manager_options(temp_dir.path().join("pods")),
    );

    let mut config = test_pod_config();
    config.namespace_options = Some(NamespaceOption {
        network: crate::proto::runtime::v1::NamespaceMode::Node as i32,
        pid: crate::proto::runtime::v1::NamespaceMode::Pod as i32,
        ipc: crate::proto::runtime::v1::NamespaceMode::Pod as i32,
        target_id: String::new(),
        userns_options: None,
    });

    let pod_id = manager
        .create_pod_sandbox(config)
        .await
        .expect("host network pod sandbox should be created");

    assert!(network.take_calls().is_empty());
    let pod = manager
        .get_pod_sandbox(&pod_id)
        .expect("pod sandbox should be stored");
    assert!(pod.netns_path.as_os_str().is_empty());
    assert!(pod.network_status.is_none());

    let created = runtime.take_created();
    assert_eq!(created.len(), 1);
    assert!(created[0].1.namespace_paths.network.is_none());
}

#[tokio::test]
async fn create_pause_container_propagates_host_pid_and_ipc_namespace_modes() {
    let temp_dir = tempdir().unwrap();
    let runtime = RecordingRuntime::default();
    let manager = PodSandboxManager::new(
        runtime.clone(),
        temp_dir.path().join("pods"),
        "registry.k8s.io/pause:3.9".to_string(),
        "/pause".to_string(),
        String::new(),
        CniConfig::default(),
    );
    tokio::fs::create_dir_all(temp_dir.path().join("pods").join("pod-1"))
        .await
        .unwrap();

    let mut pod_config = test_pod_config();
    pod_config.namespace_options = Some(NamespaceOption {
        network: crate::proto::runtime::v1::NamespaceMode::Pod as i32,
        pid: crate::proto::runtime::v1::NamespaceMode::Node as i32,
        ipc: crate::proto::runtime::v1::NamespaceMode::Node as i32,
        target_id: String::new(),
        userns_options: None,
    });

    manager
        .create_pause_container("pod-1", &pod_config, Path::new("/var/run/netns/pod-1"))
        .await
        .unwrap();

    let created = runtime.take_created();
    assert_eq!(created.len(), 1);
    let pause_config = &created[0].1;
    let namespace_options = pause_config
        .namespace_options
        .as_ref()
        .expect("pause config should include namespace options");
    assert_eq!(
        namespace_options.pid,
        crate::proto::runtime::v1::NamespaceMode::Node as i32
    );
    assert_eq!(
        namespace_options.ipc,
        crate::proto::runtime::v1::NamespaceMode::Node as i32
    );
}

#[tokio::test]
async fn create_pod_sandbox_applies_port_mappings_and_stop_removes_them() {
    let temp_dir = tempdir().unwrap();
    let runtime = RecordingRuntime::default();
    let network = RecordingNetworkManager::default();
    let port_mapper = RecordingPortMapper::default();
    let mut manager = PodSandboxManager::with_network_manager(
        runtime,
        network,
        Box::new(port_mapper.clone()),
        test_manager_options(temp_dir.path().join("pods")),
    );

    let mut config = test_pod_config();
    config.port_mappings = vec![PortMapping {
        protocol: "TCP".to_string(),
        container_port: 80,
        host_port: 8080,
        host_ip: "127.0.0.1".to_string(),
    }];

    let pod_id = manager.create_pod_sandbox(config).await.unwrap();
    let added = port_mapper.take_added();
    assert_eq!(added.len(), 1);
    assert_eq!(added[0].host_port, 8080);

    manager.stop_pod_sandbox(&pod_id).await.unwrap();
    let removed = port_mapper.take_removed();
    assert_eq!(removed.len(), 1);
    assert_eq!(removed[0].host_port, 8080);
}

#[tokio::test]
async fn create_pod_sandbox_accepts_sctp_port_mappings() {
    let temp_dir = tempdir().unwrap();
    let runtime = RecordingRuntime::default();
    let network = RecordingNetworkManager::default();
    let port_mapper = RecordingPortMapper::default();
    let mut manager = PodSandboxManager::with_network_manager(
        runtime,
        network,
        Box::new(port_mapper.clone()),
        test_manager_options(temp_dir.path().join("pods")),
    );

    let mut config = test_pod_config();
    config.port_mappings = vec![PortMapping {
        protocol: "SCTP".to_string(),
        container_port: 5000,
        host_port: 5000,
        host_ip: String::new(),
    }];

    let pod_id = manager
        .create_pod_sandbox(config)
        .await
        .expect("SCTP hostPort mapping should be accepted");
    let added = port_mapper.take_added();
    assert_eq!(added.len(), 1);
    assert_eq!(added[0].protocol, Protocol::Sctp);
    assert_eq!(added[0].host_port, 5000);

    manager
        .stop_pod_sandbox(&pod_id)
        .await
        .expect("SCTP hostPort cleanup should succeed");
    let removed = port_mapper.take_removed();
    assert_eq!(removed.len(), 1);
    assert_eq!(removed[0].protocol, Protocol::Sctp);
    assert_eq!(removed[0].host_port, 5000);
}

#[tokio::test]
async fn create_pod_sandbox_skips_port_mappings_when_hostport_mapping_is_disabled() {
    let temp_dir = tempdir().unwrap();
    let runtime = RecordingRuntime::default();
    let network = RecordingNetworkManager::default();
    let port_mapper = RecordingPortMapper::default();
    port_mapper.fail_add_after(0);
    let mut manager = PodSandboxManager::with_network_manager(
        runtime,
        network,
        Box::new(port_mapper.clone()),
        test_manager_options(temp_dir.path().join("pods")),
    );
    manager.disable_hostport_mapping = true;

    let mut config = test_pod_config();
    config.port_mappings = vec![PortMapping {
        protocol: "TCP".to_string(),
        container_port: 80,
        host_port: 8080,
        host_ip: String::new(),
    }];

    let pod_id = manager
        .create_pod_sandbox(config)
        .await
        .expect("hostPort-disabled sandbox creation should succeed");
    manager
        .stop_pod_sandbox(&pod_id)
        .await
        .expect("hostPort-disabled stop should succeed");

    assert!(port_mapper.take_added().is_empty());
    assert!(port_mapper.take_removed().is_empty());
}

#[tokio::test]
async fn create_pod_sandbox_rolls_back_applied_port_mappings_when_mapper_fails() {
    let temp_dir = tempdir().unwrap();
    let runtime = RecordingRuntime::default();
    let network = RecordingNetworkManager::default();
    let port_mapper = RecordingPortMapper::default();
    port_mapper.fail_add_after(1);
    let mut manager = PodSandboxManager::with_network_manager(
        runtime,
        network.clone(),
        Box::new(port_mapper.clone()),
        test_manager_options(temp_dir.path().join("pods")),
    );

    let mut config = test_pod_config();
    config.port_mappings = vec![
        PortMapping {
            protocol: "TCP".to_string(),
            container_port: 80,
            host_port: 8080,
            host_ip: String::new(),
        },
        PortMapping {
            protocol: "UDP".to_string(),
            container_port: 53,
            host_port: 5353,
            host_ip: String::new(),
        },
    ];

    let err = manager
        .create_pod_sandbox(config)
        .await
        .expect_err("port mapping failure should rollback pod sandbox creation");
    assert!(err
        .to_string()
        .contains("Failed to apply pod port mappings"));
    assert_eq!(port_mapper.take_added().len(), 1);
    let removed = port_mapper.take_removed();
    assert!(removed.iter().any(|mapping| mapping.host_port == 8080));
    let calls = network.take_calls();
    assert!(calls
        .iter()
        .any(|call| call.starts_with("teardown_network:")));
    assert!(calls.iter().any(|call| call.starts_with("remove_netns:")));
    assert!(manager.pods.is_empty());
}

#[tokio::test]
async fn restored_pod_sandbox_remove_cleans_up_port_mappings() {
    let temp_dir = tempdir().unwrap();
    let runtime = RecordingRuntime::default();
    let network = RecordingNetworkManager::default();
    let port_mapper = RecordingPortMapper::default();
    let mut manager = PodSandboxManager::with_network_manager(
        runtime,
        network,
        Box::new(port_mapper.clone()),
        test_manager_options(temp_dir.path().join("pods")),
    );

    manager.restore_pod_sandbox(PodSandbox {
        id: "pod-1".to_string(),
        config: PodSandboxConfig {
            port_mappings: vec![PortMapping {
                protocol: "TCP".to_string(),
                container_port: 80,
                host_port: 8080,
                host_ip: String::new(),
            }],
            ..test_pod_config()
        },
        netns_path: PathBuf::from("/var/run/netns/pod-1"),
        pause_container_id: "pause-pod-1".to_string(),
        state: PodSandboxState::Ready,
        created_at: 1,
        ip: "10.88.0.2".to_string(),
        network_status: Some(NetworkStatus {
            name: "test".to_string(),
            ip: Some("10.88.0.2".parse().unwrap()),
            mac: None,
            interfaces: vec![],
            raw_result: None,
        }),
    });

    manager.remove_pod_sandbox("pod-1").await.unwrap();
    let removed = port_mapper.take_removed();
    assert_eq!(removed.len(), 1);
    assert_eq!(removed[0].container_ip.to_string(), "10.88.0.2");
    assert_eq!(removed[0].host_port, 8080);
}

#[tokio::test]
async fn rebuild_port_mappings_skips_cleanup_when_hostport_mapping_is_disabled() {
    let temp_dir = tempdir().unwrap();
    let runtime = RecordingRuntime::default();
    let network = RecordingNetworkManager::default();
    let port_mapper = RecordingPortMapper::default();
    let mut manager = PodSandboxManager::with_network_manager(
        runtime,
        network,
        Box::new(port_mapper.clone()),
        test_manager_options(temp_dir.path().join("pods")),
    );
    manager.disable_hostport_mapping = true;

    manager.restore_pod_sandbox(PodSandbox {
        id: "pod-1".to_string(),
        config: PodSandboxConfig {
            port_mappings: vec![PortMapping {
                protocol: "TCP".to_string(),
                container_port: 80,
                host_port: 8080,
                host_ip: String::new(),
            }],
            ..test_pod_config()
        },
        netns_path: PathBuf::from("/var/run/netns/pod-1"),
        pause_container_id: "pause-pod-1".to_string(),
        state: PodSandboxState::Ready,
        created_at: 1,
        ip: "10.88.0.2".to_string(),
        network_status: Some(NetworkStatus {
            name: "test".to_string(),
            ip: Some("10.88.0.2".parse().unwrap()),
            mac: None,
            interfaces: vec![],
            raw_result: None,
        }),
    });

    manager.rebuild_port_mappings();

    assert_eq!(port_mapper.cleanup_calls(), 0);
    assert!(port_mapper.take_added().is_empty());
}

#[tokio::test]
async fn rebuild_port_mappings_cleans_existing_rules_before_reapplying_recovered_pods() {
    let temp_dir = tempdir().unwrap();
    let runtime = RecordingRuntime::default();
    let network = RecordingNetworkManager::default();
    let port_mapper = RecordingPortMapper::default();
    let mut manager = PodSandboxManager::with_network_manager(
        runtime,
        network,
        Box::new(port_mapper.clone()),
        test_manager_options(temp_dir.path().join("pods")),
    );

    manager.restore_pod_sandbox(PodSandbox {
        id: "pod-1".to_string(),
        config: PodSandboxConfig {
            port_mappings: vec![PortMapping {
                protocol: "TCP".to_string(),
                container_port: 80,
                host_port: 8080,
                host_ip: String::new(),
            }],
            ..test_pod_config()
        },
        netns_path: PathBuf::from("/var/run/netns/pod-1"),
        pause_container_id: "pause-pod-1".to_string(),
        state: PodSandboxState::Ready,
        created_at: 1,
        ip: "10.88.0.2".to_string(),
        network_status: Some(NetworkStatus {
            name: "test".to_string(),
            ip: Some("10.88.0.2".parse().unwrap()),
            mac: None,
            interfaces: vec![],
            raw_result: None,
        }),
    });

    manager.rebuild_port_mappings();

    assert_eq!(port_mapper.cleanup_calls(), 1);
    let added = port_mapper.take_added();
    assert_eq!(added.len(), 1);
    assert_eq!(added[0].host_port, 8080);
}
