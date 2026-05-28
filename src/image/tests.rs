use super::*;
use crate::image::content_store::{RemoteContentProviderKind, TransferState};
use crate::proto::runtime::v1::{
    ImageFilter, ImageFsInfoRequest, ImageSpec, PodSandboxConfig, PodSandboxMetadata,
};
use crate::storage::{ContainerRecord, StorageManager};
use chrono::Utc;
use std::os::unix::fs::PermissionsExt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tempfile::{tempdir, TempDir};

fn test_image_service_result_with_options(
    storage_path: &Path,
    storage_driver: &str,
    global_auth_file: Option<&Path>,
    namespaced_auth_dir: Option<&Path>,
    registry_config_dir: Option<&Path>,
    pinned_image_patterns: Vec<String>,
) -> Result<ImageServiceImpl, Error> {
    ImageServiceImpl::new_with_options(ImageServiceOptions {
        storage_path: storage_path.to_path_buf(),
        ledger_db_path: None,
        storage_driver: storage_driver.to_string(),
        storage_options: Vec::new(),
        global_auth_file: global_auth_file.map(Path::to_path_buf),
        namespaced_auth_dir: namespaced_auth_dir.map(Path::to_path_buf),
        default_transport: "docker://".to_string(),
        short_name_mode: "disabled".to_string(),
        pull_progress_timeout: std::time::Duration::ZERO,
        max_concurrent_downloads: 3,
        pull_retry_count: 0,
        registry_config_dir: registry_config_dir.map(Path::to_path_buf),
        decryption_keys_path: None,
        decryption_decoder_path: "ctd-decoder".to_string(),
        decryption_keyprovider_config: None,
        additional_artifact_stores: Vec::new(),
        pinned_image_patterns,
        signature_policy: None,
        signature_policy_dir: None,
        big_files_temporary_dir: None,
        separate_pull_cgroup: String::new(),
        cgroup_driver: crate::config::CgroupDriverConfig::Cgroupfs,
        rootless: crate::rootless::EffectiveRootlessConfig::disabled(),
        disable_cgroup: false,
        pull_cgroup_root: None,
    })
}

fn test_image_service_with_options(
    storage_path: &Path,
    storage_driver: &str,
    global_auth_file: Option<&Path>,
    namespaced_auth_dir: Option<&Path>,
    registry_config_dir: Option<&Path>,
    pinned_image_patterns: Vec<String>,
) -> ImageServiceImpl {
    test_image_service_result_with_options(
        storage_path,
        storage_driver,
        global_auth_file,
        namespaced_auth_dir,
        registry_config_dir,
        pinned_image_patterns,
    )
    .unwrap()
}

async fn test_image_service() -> ImageServiceImpl {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    std::mem::forget(dir);
    test_image_service_with_options(
        &path,
        "overlay",
        Option::<&Path>::None,
        Option::<&Path>::None,
        Option::<&Path>::None,
        Vec::new(),
    )
}

fn test_image_service_in_tempdir() -> (TempDir, ImageServiceImpl) {
    let dir = tempdir().unwrap();
    let service = test_image_service_with_options(
        dir.path(),
        "overlay",
        Option::<&Path>::None,
        Option::<&Path>::None,
        Option::<&Path>::None,
        Vec::new(),
    );
    (dir, service)
}

fn test_image_service_with_ledger_in_tempdir() -> (TempDir, ImageServiceImpl, PathBuf) {
    let dir = tempdir().unwrap();
    let ledger_db_path = dir.path().join("crius.db");
    let service = ImageServiceImpl::new_with_options(ImageServiceOptions {
        storage_path: dir.path().join("images"),
        ledger_db_path: Some(ledger_db_path.clone()),
        storage_driver: "overlay".to_string(),
        storage_options: Vec::new(),
        global_auth_file: None,
        namespaced_auth_dir: None,
        default_transport: "docker://".to_string(),
        short_name_mode: "disabled".to_string(),
        pull_progress_timeout: std::time::Duration::ZERO,
        max_concurrent_downloads: 3,
        pull_retry_count: 0,
        registry_config_dir: None,
        decryption_keys_path: None,
        decryption_decoder_path: "ctd-decoder".to_string(),
        decryption_keyprovider_config: None,
        additional_artifact_stores: Vec::new(),
        pinned_image_patterns: Vec::new(),
        signature_policy: None,
        signature_policy_dir: None,
        big_files_temporary_dir: None,
        separate_pull_cgroup: String::new(),
        cgroup_driver: crate::config::CgroupDriverConfig::Cgroupfs,
        rootless: crate::rootless::EffectiveRootlessConfig::disabled(),
        disable_cgroup: false,
        pull_cgroup_root: None,
    })
    .unwrap();
    (dir, service, ledger_db_path)
}

fn test_image_service_with_pull_cgroup(
    storage_path: &Path,
    cgroup_root: &Path,
    separate_pull_cgroup: &str,
    cgroup_driver: crate::config::CgroupDriverConfig,
    rootless: crate::rootless::EffectiveRootlessConfig,
    disable_cgroup: bool,
) -> ImageServiceImpl {
    ImageServiceImpl::new_with_options(ImageServiceOptions {
        storage_path: storage_path.to_path_buf(),
        ledger_db_path: None,
        storage_driver: "overlay".to_string(),
        storage_options: Vec::new(),
        global_auth_file: None,
        namespaced_auth_dir: None,
        default_transport: "docker://".to_string(),
        short_name_mode: "disabled".to_string(),
        pull_progress_timeout: std::time::Duration::ZERO,
        max_concurrent_downloads: 3,
        pull_retry_count: 0,
        registry_config_dir: None,
        decryption_keys_path: None,
        decryption_decoder_path: "ctd-decoder".to_string(),
        decryption_keyprovider_config: None,
        additional_artifact_stores: Vec::new(),
        pinned_image_patterns: Vec::new(),
        signature_policy: None,
        signature_policy_dir: None,
        big_files_temporary_dir: None,
        separate_pull_cgroup: separate_pull_cgroup.to_string(),
        cgroup_driver,
        rootless,
        disable_cgroup,
        pull_cgroup_root: Some(cgroup_root.to_path_buf()),
    })
    .unwrap()
}

#[test]
fn image_service_rejects_unsupported_storage_driver() {
    let dir = tempdir().unwrap();
    let err = match test_image_service_result_with_options(
        dir.path(),
        "btrfs",
        Option::<&Path>::None,
        Option::<&Path>::None,
        Option::<&Path>::None,
        Vec::new(),
    ) {
        Ok(_) => panic!("unsupported image driver must fail initialization"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("image.driver must be \"overlay\""));
}

#[test]
fn image_service_accepts_supported_overlay_storage_options() {
    let dir = tempdir().unwrap();
    let service = ImageServiceImpl::new_with_options(ImageServiceOptions {
        storage_path: dir.path().to_path_buf(),
        ledger_db_path: None,
        storage_driver: "overlay".to_string(),
        storage_options: vec![
            "overlay.mount_program=/usr/bin/fuse-overlayfs".to_string(),
            "overlay.ignore_chown_errors=true".to_string(),
        ],
        global_auth_file: None,
        namespaced_auth_dir: None,
        default_transport: "docker://".to_string(),
        short_name_mode: "disabled".to_string(),
        pull_progress_timeout: std::time::Duration::ZERO,
        max_concurrent_downloads: 3,
        pull_retry_count: 0,
        registry_config_dir: None,
        decryption_keys_path: None,
        decryption_decoder_path: "ctd-decoder".to_string(),
        decryption_keyprovider_config: None,
        additional_artifact_stores: Vec::new(),
        pinned_image_patterns: Vec::new(),
        signature_policy: None,
        signature_policy_dir: None,
        big_files_temporary_dir: None,
        separate_pull_cgroup: String::new(),
        cgroup_driver: crate::config::CgroupDriverConfig::Cgroupfs,
        rootless: crate::rootless::EffectiveRootlessConfig::disabled(),
        disable_cgroup: false,
        pull_cgroup_root: None,
    })
    .unwrap();

    assert_eq!(service.storage_options.len(), 2);
    assert_eq!(
        service.parsed_storage_options.mount_program.as_deref(),
        Some(Path::new("/usr/bin/fuse-overlayfs"))
    );
    assert!(service.parsed_storage_options.ignore_chown_errors);
}

#[tokio::test]
async fn pull_image_enters_configured_pull_cgroup_scope() {
    let dir = tempdir().unwrap();
    let cgroup_root = dir.path().join("cgroup");
    let service = test_image_service_with_pull_cgroup(
        dir.path().join("images").as_path(),
        &cgroup_root,
        "kubepods/pod123",
        crate::config::CgroupDriverConfig::Cgroupfs,
        crate::rootless::EffectiveRootlessConfig::disabled(),
        false,
    );
    let observed_scope = Arc::new(std::sync::Mutex::new(Vec::new()));
    service.set_test_pull_scope_observer(Arc::new({
        let observed_scope = Arc::clone(&observed_scope);
        move |stage, active| {
            observed_scope.lock().unwrap().push((stage, active));
        }
    }));
    service.set_test_pull_handler(Arc::new(|_| {
        Ok(TestPullResponse {
            image_id: "sha256:pull-cgroup".to_string(),
            size: 12,
            ..Default::default()
        })
    }));

    ImageService::pull_image(
        &service,
        Request::new(PullImageRequest {
            image: Some(ImageSpec {
                image: "registry.example.com/ns/image:latest".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    assert!(cgroup_root.join("kubepods/pod123/cgroup.procs").exists());
    let last_scope = service.last_pull_cgroup_scope().unwrap();
    assert!(last_scope.entered);
    assert!(!last_scope.active);
    assert!(last_scope.started_at_unix_millis <= last_scope.ended_at_unix_millis.unwrap());
    assert_eq!(last_scope.mode, PullCgroupMode::Path);
    assert!(last_scope
        .effective_path
        .unwrap()
        .ends_with("kubepods/pod123"));
    let observations = observed_scope.lock().unwrap();
    for stage in [
        "test-handler",
        "persist-record-start",
        "persist-metadata-saved",
    ] {
        assert!(
            observations
                .iter()
                .any(|(observed_stage, active)| observed_stage == &stage && *active),
            "{stage} should run inside active pull cgroup scope; observed {observations:?}"
        );
    }
}

#[tokio::test]
async fn pull_image_uses_pod_cgroup_parent_in_pod_mode() {
    let dir = tempdir().unwrap();
    let cgroup_root = dir.path().join("cgroup");
    let service = test_image_service_with_pull_cgroup(
        dir.path().join("images").as_path(),
        &cgroup_root,
        "pod",
        crate::config::CgroupDriverConfig::Cgroupfs,
        crate::rootless::EffectiveRootlessConfig::disabled(),
        false,
    );
    service.set_test_pull_handler(Arc::new(|_| {
        Ok(TestPullResponse {
            image_id: "sha256:pod-pull-cgroup".to_string(),
            size: 12,
            ..Default::default()
        })
    }));

    ImageService::pull_image(
        &service,
        Request::new(PullImageRequest {
            image: Some(ImageSpec {
                image: "registry.example.com/ns/pod-image:latest".to_string(),
                ..Default::default()
            }),
            sandbox_config: Some(PodSandboxConfig {
                linux: Some(crate::proto::runtime::v1::LinuxPodSandboxConfig {
                    cgroup_parent: "kubepods/podabc".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    assert!(cgroup_root.join("kubepods/podabc/cgroup.procs").exists());
    let last_scope = service.last_pull_cgroup_scope().unwrap();
    assert_eq!(last_scope.mode, PullCgroupMode::Pod);
    assert!(last_scope.entered);
}

#[test]
fn image_service_uses_global_auth_file_for_matching_registry() {
    let dir = tempdir().unwrap();
    let auth_file = dir.path().join("config.json");
    std::fs::write(
        &auth_file,
        r#"{
                "auths": {
                    "https://index.docker.io/v1/": {
                        "auth": "dXNlcjpwYXNz"
                    }
                }
            }"#,
    )
    .unwrap();
    let service = test_image_service_with_options(
        dir.path(),
        "overlay",
        Some(&auth_file),
        Option::<&Path>::None,
        Option::<&Path>::None,
        Vec::new(),
    );
    let reference: Reference = "docker.io/library/busybox:latest".parse().unwrap();

    let auth = service
        .registry_auth_from_global_auth_file(&reference)
        .unwrap()
        .expect("matching auth should be loaded");

    match auth {
        RegistryAuth::Basic(username, password) => {
            assert_eq!(username, "user");
            assert_eq!(password, "pass");
        }
        RegistryAuth::Anonymous => panic!("expected basic auth from auth file"),
    }
}

#[test]
fn image_service_uses_namespaced_auth_file_for_matching_registry() {
    let dir = tempdir().unwrap();
    let auth_dir = dir.path().join("credentialprovider");
    std::fs::create_dir_all(&auth_dir).unwrap();
    let reference: Reference = "docker.io/library/busybox:latest".parse().unwrap();
    let service = test_image_service_with_options(
        dir.path(),
        "overlay",
        Option::<&Path>::None,
        Some(&auth_dir),
        Option::<&Path>::None,
        Vec::new(),
    );
    let auth_path = service
        .namespaced_auth_file_path("default", &reference)
        .expect("expected namespaced auth path");
    std::fs::write(
        &auth_path,
        r#"{
                "auths": {
                    "https://index.docker.io/v1/": {
                        "auth": "dGVzdDpzZWNyZXQ="
                    }
                }
            }"#,
    )
    .unwrap();

    match service
        .registry_auth_from_namespaced_auth_dir(&reference, Some("default"))
        .unwrap()
        .expect("expected auth from namespaced auth dir")
    {
        RegistryAuth::Basic(username, password) => {
            assert_eq!(username, "test");
            assert_eq!(password, "secret");
        }
        RegistryAuth::Anonymous => panic!("expected basic auth from namespaced auth dir"),
    }
}

#[test]
fn namespaced_auth_dir_takes_precedence_over_global_auth_file() {
    let dir = tempdir().unwrap();
    let auth_dir = dir.path().join("credentialprovider");
    std::fs::create_dir_all(&auth_dir).unwrap();
    let global_auth_file = dir.path().join("global.json");
    std::fs::write(
        &global_auth_file,
        r#"{
                "auths": {
                    "https://index.docker.io/v1/": {
                        "auth": "Z2xvYmFsOnNlY3JldA=="
                    }
                }
            }"#,
    )
    .unwrap();
    let reference: Reference = "docker.io/library/busybox:latest".parse().unwrap();
    let service = test_image_service_with_options(
        dir.path(),
        "overlay",
        Some(&global_auth_file),
        Some(&auth_dir),
        Option::<&Path>::None,
        Vec::new(),
    );
    let auth_path = service
        .namespaced_auth_file_path("default", &reference)
        .expect("expected namespaced auth path");
    std::fs::write(
        &auth_path,
        r#"{
                "auths": {
                    "https://index.docker.io/v1/": {
                        "auth": "bmFtZXNwYWNlOnNlY3JldA=="
                    }
                }
            }"#,
    )
    .unwrap();

    let namespaced = service
        .registry_auth_from_namespaced_auth_dir(&reference, Some("default"))
        .unwrap()
        .expect("expected namespaced auth");
    let global = service
        .registry_auth_from_global_auth_file(&reference)
        .unwrap()
        .expect("expected global auth");

    match (namespaced, global) {
        (RegistryAuth::Basic(ns_user, _), RegistryAuth::Basic(global_user, _)) => {
            assert_eq!(ns_user, "namespace");
            assert_eq!(global_user, "global");
        }
        _ => panic!("expected basic auth from both sources"),
    }
}

#[tokio::test]
async fn ensure_image_exists_for_sandbox_falls_back_to_global_auth_file() {
    let dir = tempdir().unwrap();
    let global_auth_file = dir.path().join("global.json");
    std::fs::write(
        &global_auth_file,
        r#"{
                "auths": {
                    "registry.example": {
                        "auth": "Z2xvYmFsOnNlY3JldA=="
                    }
                }
            }"#,
    )
    .unwrap();
    let service = test_image_service_with_options(
        dir.path(),
        "overlay",
        Some(&global_auth_file),
        Option::<&Path>::None,
        Option::<&Path>::None,
        Vec::new(),
    );
    let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
    service.set_test_pull_handler(Arc::new({
        let observed = observed.clone();
        move |request| {
            observed.lock().unwrap().push(request);
            Ok(TestPullResponse {
                image_id: "sha256:global-auth".to_string(),
                size: 12,
                ..Default::default()
            })
        }
    }));

    let sandbox_config = PodSandboxConfig {
        metadata: Some(PodSandboxMetadata {
            name: "test-pod".to_string(),
            uid: "uid-1".to_string(),
            namespace: "default".to_string(),
            attempt: 0,
        }),
        ..Default::default()
    };
    let image = service
        .ensure_image_exists_for_sandbox("registry.example/repo:latest", &sandbox_config)
        .await
        .expect("missing sandbox image should be pulled through global auth");

    assert_eq!(image.id, "sha256:global-auth");
    let calls = observed.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(
        calls[0].auth,
        TestRegistryAuth::Basic {
            username: "global".to_string(),
            password: "secret".to_string(),
        }
    );
    assert_eq!(calls[0].pull_namespace.as_deref(), Some("default"));
}

#[tokio::test]
async fn collect_with_concurrency_limit_honors_max_parallelism() {
    let peak = Arc::new(AtomicUsize::new(0));
    let current = Arc::new(AtomicUsize::new(0));
    let results = ImageServiceImpl::collect_with_concurrency_limit(2, vec![0usize, 1, 2, 3], {
        let peak = peak.clone();
        let current = current.clone();
        move |idx| {
            let peak = peak.clone();
            let current = current.clone();
            async move {
                let active = current.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(active, Ordering::SeqCst);
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                current.fetch_sub(1, Ordering::SeqCst);
                Ok::<_, Status>((idx, vec![idx as u8], 1))
            }
        }
    })
    .await
    .unwrap();

    assert_eq!(results.len(), 4);
    assert!(peak.load(Ordering::SeqCst) <= 2);
}

#[tokio::test]
async fn execute_pull_with_retries_retries_retryable_failures() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let result = ImageServiceImpl::execute_pull_with_retries(2, {
        let attempts = attempts.clone();
        move || {
            let attempts = attempts.clone();
            async move {
                let current = attempts.fetch_add(1, Ordering::SeqCst);
                if current == 0 {
                    Err(Status::internal("temporary pull failure"))
                } else {
                    Ok("ok")
                }
            }
        }
    })
    .await
    .unwrap();

    assert_eq!(result, "ok");
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[test]
fn short_name_mode_enforcing_rejects_unqualified_pull_reference() {
    let dir = tempdir().unwrap();
    let service = ImageServiceImpl::new_with_options(ImageServiceOptions {
        storage_path: dir.path().to_path_buf(),
        ledger_db_path: None,
        storage_driver: "overlay".to_string(),
        storage_options: Vec::new(),
        global_auth_file: None,
        namespaced_auth_dir: None,
        default_transport: "docker://".to_string(),
        short_name_mode: "enforcing".to_string(),
        pull_progress_timeout: std::time::Duration::ZERO,
        max_concurrent_downloads: 3,
        pull_retry_count: 0,
        registry_config_dir: None,
        decryption_keys_path: None,
        decryption_decoder_path: "ctd-decoder".to_string(),
        decryption_keyprovider_config: None,
        additional_artifact_stores: Vec::new(),
        pinned_image_patterns: Vec::new(),
        signature_policy: None,
        signature_policy_dir: None,
        big_files_temporary_dir: None,
        separate_pull_cgroup: String::new(),
        cgroup_driver: crate::config::CgroupDriverConfig::Cgroupfs,
        rootless: crate::rootless::EffectiveRootlessConfig::disabled(),
        disable_cgroup: false,
        pull_cgroup_root: None,
    })
    .unwrap();

    let err = service
        .resolve_pull_reference("busybox")
        .expect_err("short name should be rejected");
    assert!(err
        .message()
        .contains("short image names are rejected when image.short_name_mode = enforcing"));
}

#[test]
fn short_name_mode_disabled_normalizes_short_name_with_default_transport() {
    let dir = tempdir().unwrap();
    let service = test_image_service_with_options(
        dir.path(),
        "overlay",
        Option::<&Path>::None,
        Option::<&Path>::None,
        Option::<&Path>::None,
        Vec::new(),
    );

    assert_eq!(
        service.resolve_pull_reference("busybox").unwrap(),
        "docker.io/library/busybox:latest"
    );
    assert_eq!(
        service.resolve_pull_reference("docker://busybox").unwrap(),
        "docker.io/library/busybox:latest"
    );
}

#[tokio::test]
async fn pull_progress_timeout_cancels_stalled_response_body() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    let dir = tempdir().unwrap();
    let service = ImageServiceImpl::new_with_options(ImageServiceOptions {
        storage_path: dir.path().to_path_buf(),
        ledger_db_path: None,
        storage_driver: "overlay".to_string(),
        storage_options: Vec::new(),
        global_auth_file: None,
        namespaced_auth_dir: None,
        default_transport: "docker://".to_string(),
        short_name_mode: "disabled".to_string(),
        pull_progress_timeout: std::time::Duration::from_millis(50),
        max_concurrent_downloads: 3,
        pull_retry_count: 0,
        registry_config_dir: None,
        decryption_keys_path: None,
        decryption_decoder_path: "ctd-decoder".to_string(),
        decryption_keyprovider_config: None,
        additional_artifact_stores: Vec::new(),
        pinned_image_patterns: Vec::new(),
        signature_policy: None,
        signature_policy_dir: None,
        big_files_temporary_dir: None,
        separate_pull_cgroup: String::new(),
        cgroup_driver: crate::config::CgroupDriverConfig::Cgroupfs,
        rootless: crate::rootless::EffectiveRootlessConfig::disabled(),
        disable_cgroup: false,
        pull_cgroup_root: None,
    })
    .unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 1024];
        let _ = socket.read(&mut buf).await.unwrap();
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        let _ = socket.write_all(b"body").await;
    });

    let response = reqwest::get(format!("http://{}/slow", addr)).await.unwrap();
    let err = service
        .read_response_bytes_with_progress_timeout(response, "test body")
        .await
        .expect_err("stalled response body should time out");
    assert_eq!(err.code(), tonic::Code::DeadlineExceeded);
}

async fn insert_image(service: &ImageServiceImpl, image: Image) {
    let mut images = service.images.lock().await;
    for tag in &image.repo_tags {
        images.insert(tag.clone(), image.clone());
    }
}

#[test]
fn registry_config_dir_loads_hosts_toml_endpoints() {
    let dir = tempdir().unwrap();
    let registry_dir = dir.path().join("certs.d").join("docker.io");
    std::fs::create_dir_all(&registry_dir).unwrap();
    std::fs::write(
        registry_dir.join("hosts.toml"),
        r#"
server = "https://docker.io"

[host."https://mirror.local"]
  capabilities = ["pull"]

[host."https://registry-1.docker.io"]
  capabilities = ["pull", "resolve"]
"#,
    )
    .unwrap();
    let registry_config_dir = dir.path().join("certs.d");
    let service = test_image_service_with_options(
        dir.path(),
        "overlay",
        Option::<&Path>::None,
        Option::<&Path>::None,
        Some(registry_config_dir.as_path()),
        Vec::new(),
    );
    let reference: Reference = "docker.io/library/busybox:latest".parse().unwrap();

    let resolve_endpoints = service.registry_endpoints_for(&reference, true).unwrap();
    let pull_endpoints = service.registry_endpoints_for(&reference, false).unwrap();

    assert!(resolve_endpoints
        .iter()
        .any(|endpoint| endpoint.base_url == "https://registry-1.docker.io"));
    assert!(resolve_endpoints
        .iter()
        .any(|endpoint| endpoint.base_url == "https://docker.io"));
    assert!(!resolve_endpoints
        .iter()
        .any(|endpoint| endpoint.base_url == "https://mirror.local"));
    assert!(pull_endpoints
        .iter()
        .any(|endpoint| endpoint.base_url == "https://mirror.local"));
}

#[test]
fn registry_config_dir_reload_reads_updated_hosts_toml() {
    let dir = tempdir().unwrap();
    let registry_dir = dir.path().join("certs.d").join("docker.io");
    std::fs::create_dir_all(&registry_dir).unwrap();
    let hosts_path = registry_dir.join("hosts.toml");
    std::fs::write(
        &hosts_path,
        r#"
[host."https://mirror-a.local"]
  capabilities = ["pull", "resolve"]
"#,
    )
    .unwrap();
    let registry_config_dir = dir.path().join("certs.d");
    let service = test_image_service_with_options(
        dir.path(),
        "overlay",
        Option::<&Path>::None,
        Option::<&Path>::None,
        Some(registry_config_dir.as_path()),
        Vec::new(),
    );
    let reference: Reference = "docker.io/library/busybox:latest".parse().unwrap();

    let first = service.registry_endpoints_for(&reference, false).unwrap();
    assert!(first
        .iter()
        .any(|endpoint| endpoint.base_url == "https://mirror-a.local"));

    std::fs::write(
        &hosts_path,
        r#"
[host."https://mirror-b.local"]
  capabilities = ["pull", "resolve"]
"#,
    )
    .unwrap();

    let second = service.registry_endpoints_for(&reference, false).unwrap();
    assert!(second
        .iter()
        .any(|endpoint| endpoint.base_url == "https://mirror-b.local"));
    assert!(!second
        .iter()
        .any(|endpoint| endpoint.base_url == "https://mirror-a.local"));
}

#[tokio::test]
async fn list_images_supports_filter_image_ref() {
    let service = test_image_service().await;
    insert_image(
        &service,
        Image {
            id: "sha256:1111111111111111".to_string(),
            repo_tags: vec!["busybox:latest".to_string()],
            ..Default::default()
        },
    )
    .await;
    insert_image(
        &service,
        Image {
            id: "sha256:2222222222222222".to_string(),
            repo_tags: vec!["pause:3.9".to_string()],
            ..Default::default()
        },
    )
    .await;

    let by_tag = ImageService::list_images(
        &service,
        Request::new(ListImagesRequest {
            filter: Some(ImageFilter {
                image: Some(ImageSpec {
                    image: "busybox:latest".to_string(),
                    ..Default::default()
                }),
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(by_tag.images.len(), 1);
    assert_eq!(by_tag.images[0].id, "sha256:1111111111111111");

    let by_id_prefix = ImageService::list_images(
        &service,
        Request::new(ListImagesRequest {
            filter: Some(ImageFilter {
                image: Some(ImageSpec {
                    image: "111111111111".to_string(),
                    ..Default::default()
                }),
            }),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(by_id_prefix.images.len(), 1);
    assert_eq!(by_id_prefix.images[0].repo_tags, vec!["busybox:latest"]);
}

#[tokio::test]
async fn image_status_returns_empty_response_when_missing() {
    let service = test_image_service().await;

    let response = ImageService::image_status(
        &service,
        Request::new(ImageStatusRequest {
            image: Some(ImageSpec {
                image: "missing:latest".to_string(),
                ..Default::default()
            }),
            verbose: false,
        }),
    )
    .await
    .unwrap()
    .into_inner();

    assert!(response.image.is_none());
    assert!(response.info.is_empty());
}

#[tokio::test]
async fn image_status_verbose_returns_structured_info_and_repo_digests() {
    let (dir, service) = test_image_service_in_tempdir();
    let image_dir = dir.path().join("images").join("sha256:img-id");
    std::fs::create_dir_all(&image_dir).unwrap();
    std::fs::write(image_dir.join("0.tar.gz"), b"layer").unwrap();

    insert_image(
        &service,
        Image {
            id: "sha256:img-id".to_string(),
            repo_tags: vec!["busybox:latest".to_string()],
            repo_digests: vec!["docker.io/library/busybox@sha256:img-id".to_string()],
            size: 5,
            ..Default::default()
        },
    )
    .await;

    let response = ImageService::image_status(
        &service,
        Request::new(ImageStatusRequest {
            image: Some(ImageSpec {
                image: "busybox:latest".to_string(),
                ..Default::default()
            }),
            verbose: true,
        }),
    )
    .await
    .unwrap()
    .into_inner();

    let image = response.image.expect("expected image status");
    assert_eq!(
        image.repo_digests,
        vec!["docker.io/library/busybox@sha256:img-id"]
    );
    assert_eq!(
        image
            .spec
            .expect("expected image spec")
            .user_specified_image,
        "busybox:latest"
    );
    assert!(!image.pinned);
    let info: serde_json::Value =
        serde_json::from_str(response.info.get("info").expect("missing verbose info")).unwrap();
    assert_eq!(info["id"], "sha256:img-id");
    assert_eq!(info["repoTags"][0], "busybox:latest");
    assert_eq!(
        info["repoDigests"][0],
        "docker.io/library/busybox@sha256:img-id"
    );
    assert_eq!(info["pinned"], false);
    assert_eq!(info["layers"][0], "0.tar.gz");
}

#[tokio::test]
async fn image_status_verbose_includes_selected_platform_metadata() {
    let (dir, service) = test_image_service_in_tempdir();
    let image_dir = dir.path().join("images").join("sha256:platform-id");
    std::fs::create_dir_all(&image_dir).unwrap();

    service
        .save_image_metadata(&CriusImage {
            id: "sha256:platform-id".to_string(),
            repo_tags: vec!["repo/platform:latest".to_string()],
            repo_digests: vec!["repo/platform@sha256:platform-id".to_string()],
            size: 7,
            pinned: false,
            pulled_at: 1,
            source_reference: None,
            os: Some("linux".to_string()),
            architecture: Some("amd64".to_string()),
            config_user: None,
            config_env: Vec::new(),
            config_entrypoint: Vec::new(),
            config_cmd: Vec::new(),
            config_working_dir: None,
            annotations: HashMap::new(),
            declared_volumes: Vec::new(),
            manifest_media_type: Some("application/vnd.oci.image.manifest.v1+json".to_string()),
            selected_manifest_digest: Some("sha256:child-manifest".to_string()),
            selected_platform: Some("linux/amd64".to_string()),
            stored_layers: Vec::new(),
            artifact_type: None,
            artifact_blobs: Vec::new(),
        })
        .await
        .unwrap();
    insert_image(
        &service,
        Image {
            id: "sha256:platform-id".to_string(),
            repo_tags: vec!["repo/platform:latest".to_string()],
            ..Default::default()
        },
    )
    .await;

    let response = ImageService::image_status(
        &service,
        Request::new(ImageStatusRequest {
            image: Some(ImageSpec {
                image: "repo/platform:latest".to_string(),
                ..Default::default()
            }),
            verbose: true,
        }),
    )
    .await
    .unwrap()
    .into_inner();

    let info: serde_json::Value =
        serde_json::from_str(response.info.get("info").expect("missing verbose info")).unwrap();
    assert_eq!(info["selectedManifestDigest"], "sha256:child-manifest");
    assert_eq!(info["selectedPlatform"], "linux/amd64");
}

#[tokio::test]
async fn remove_image_is_idempotent_when_missing() {
    let service = test_image_service().await;

    ImageService::remove_image(
        &service,
        Request::new(RemoveImageRequest {
            image: Some(ImageSpec {
                image: "missing:latest".to_string(),
                ..Default::default()
            }),
        }),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn list_images_deduplicates_same_id_across_multiple_tags() {
    let service = test_image_service().await;
    insert_image(
        &service,
        Image {
            id: "sha256:same-id".to_string(),
            repo_tags: vec!["repo/a:latest".to_string()],
            ..Default::default()
        },
    )
    .await;
    insert_image(
        &service,
        Image {
            id: "sha256:same-id".to_string(),
            repo_tags: vec!["repo/b:latest".to_string()],
            ..Default::default()
        },
    )
    .await;

    let response =
        ImageService::list_images(&service, Request::new(ListImagesRequest { filter: None }))
            .await
            .unwrap()
            .into_inner();
    assert_eq!(response.images.len(), 1);
    assert_eq!(response.images[0].id, "sha256:same-id");
    assert_eq!(
        response.images[0].repo_tags,
        vec!["repo/a:latest".to_string(), "repo/b:latest".to_string()]
    );
}

#[tokio::test]
async fn image_status_aggregates_tags_and_user_for_same_id() {
    let (dir, service) = test_image_service_in_tempdir();
    service
        .save_image_metadata(&CriusImage {
            id: "sha256:agg-id".to_string(),
            repo_tags: vec!["repo/a:latest".to_string(), "repo/b:latest".to_string()],
            repo_digests: vec!["repo/a@sha256:agg-id".to_string()],
            size: 42,
            pinned: false,
            pulled_at: 1,
            source_reference: None,
            os: Some("linux".to_string()),
            architecture: Some("amd64".to_string()),
            config_user: Some("1001".to_string()),
            config_env: Vec::new(),
            config_entrypoint: Vec::new(),
            config_cmd: Vec::new(),
            config_working_dir: None,
            annotations: HashMap::new(),
            declared_volumes: Vec::new(),
            manifest_media_type: None,
            selected_manifest_digest: None,
            selected_platform: None,
            stored_layers: Vec::new(),
            artifact_type: None,
            artifact_blobs: Vec::new(),
        })
        .await
        .unwrap();
    let image_dir = dir.path().join("images").join("sha256:agg-id");
    std::fs::create_dir_all(&image_dir).unwrap();

    insert_image(
        &service,
        Image {
            id: "sha256:agg-id".to_string(),
            repo_tags: vec!["repo/a:latest".to_string()],
            repo_digests: vec!["repo/a@sha256:agg-id".to_string()],
            ..Default::default()
        },
    )
    .await;
    insert_image(
        &service,
        Image {
            id: "sha256:agg-id".to_string(),
            repo_tags: vec!["repo/b:latest".to_string()],
            ..Default::default()
        },
    )
    .await;

    let response = ImageService::image_status(
        &service,
        Request::new(ImageStatusRequest {
            image: Some(ImageSpec {
                image: "repo/b:latest".to_string(),
                ..Default::default()
            }),
            verbose: false,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let image = response.image.expect("expected aggregated image status");
    assert_eq!(
        image.repo_tags,
        vec!["repo/a:latest".to_string(), "repo/b:latest".to_string()]
    );
    assert_eq!(image.repo_digests, vec!["repo/a@sha256:agg-id".to_string()]);
    let spec = image.spec.expect("expected image spec");
    assert_eq!(spec.image, "repo/b:latest");
    assert_eq!(spec.user_specified_image, "repo/b:latest");
    assert_eq!(
        image.uid.expect("expected uid from image metadata").value,
        1001
    );
    assert!(image.username.is_empty());
}

#[tokio::test]
async fn image_status_restores_spec_annotations_from_metadata() {
    let service = test_image_service().await;
    insert_image(
        &service,
        Image {
            id: "sha256:anno-id".to_string(),
            repo_tags: vec!["repo/anno:latest".to_string()],
            ..Default::default()
        },
    )
    .await;
    service
        .save_image_metadata(&CriusImage {
            id: "sha256:anno-id".to_string(),
            repo_tags: vec!["repo/anno:latest".to_string()],
            repo_digests: Vec::new(),
            size: 7,
            pinned: false,
            pulled_at: 0,
            source_reference: None,
            os: None,
            architecture: None,
            config_user: None,
            config_env: Vec::new(),
            config_entrypoint: Vec::new(),
            config_cmd: Vec::new(),
            config_working_dir: None,
            annotations: HashMap::from([(
                "org.opencontainers.image.title".to_string(),
                "anno".to_string(),
            )]),
            declared_volumes: vec!["/var/lib/data".to_string()],
            manifest_media_type: None,
            selected_manifest_digest: None,
            selected_platform: None,
            stored_layers: Vec::new(),
            artifact_type: None,
            artifact_blobs: Vec::new(),
        })
        .await
        .unwrap();

    let response = ImageService::image_status(
        &service,
        Request::new(ImageStatusRequest {
            image: Some(ImageSpec {
                image: "repo/anno:latest".to_string(),
                ..Default::default()
            }),
            verbose: false,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let image = response.image.expect("expected image");
    assert_eq!(
        image
            .spec
            .expect("expected image spec")
            .annotations
            .get("org.opencontainers.image.title")
            .map(String::as_str),
        Some("anno")
    );
}

#[test]
fn registry_auth_from_auth_config_decodes_auth_field() {
    let encoded =
        base64::engine::general_purpose::STANDARD.encode("demo-user:demo-password".as_bytes());
    let auth = AuthConfig {
        auth: encoded,
        ..Default::default()
    };
    match ImageServiceImpl::registry_auth_from_auth_config(auth).unwrap() {
        RegistryAuth::Basic(username, password) => {
            assert_eq!(username, "demo-user");
            assert_eq!(password, "demo-password");
        }
        RegistryAuth::Anonymous => panic!("expected basic auth from encoded auth field"),
    }
}

#[test]
fn provided_registry_bearer_token_prefers_registry_then_identity_token() {
    let auth = AuthConfig {
        registry_token: "registry-token".to_string(),
        identity_token: "identity-token".to_string(),
        ..Default::default()
    };
    assert_eq!(
        ImageServiceImpl::provided_registry_bearer_token(&auth),
        Some("registry-token".to_string())
    );

    let auth = AuthConfig {
        identity_token: "identity-token".to_string(),
        ..Default::default()
    };
    assert_eq!(
        ImageServiceImpl::provided_registry_bearer_token(&auth),
        Some("identity-token".to_string())
    );
}

#[test]
fn canonical_image_id_keeps_full_digest_without_truncation() {
    let digest = "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
    let id = ImageServiceImpl::canonical_image_id(digest, b"fallback");
    assert_eq!(id, digest);
}

#[test]
fn canonicalize_image_reference_expands_short_names() {
    assert_eq!(
        ImageServiceImpl::canonicalize_image_reference("busybox"),
        "docker.io/library/busybox:latest"
    );
    assert_eq!(
        ImageServiceImpl::canonicalize_image_reference("busybox:1.36"),
        "docker.io/library/busybox:1.36"
    );
    assert_eq!(
        ImageServiceImpl::canonicalize_image_reference("library/busybox"),
        "docker.io/library/busybox:latest"
    );
}

#[test]
fn pinned_pattern_matching_supports_exact_glob_and_keyword_modes() {
    assert!(ImageServiceImpl::pinned_pattern_matches(
        "registry.k8s.io/pause:3.9",
        "registry.k8s.io/pause:3.9"
    ));
    assert!(ImageServiceImpl::pinned_pattern_matches(
        "busybox*",
        "busybox:latest"
    ));
    assert!(ImageServiceImpl::pinned_pattern_matches(
        "*pause*",
        "registry.k8s.io/pause:3.9"
    ));
    assert!(!ImageServiceImpl::pinned_pattern_matches(
        "busybox*",
        "registry.k8s.io/pause:3.9"
    ));
}

#[tokio::test]
async fn image_status_matches_short_name_against_canonical_tag() {
    let service = test_image_service().await;
    insert_image(
        &service,
        Image {
            id: "sha256:busybox-id".to_string(),
            repo_tags: vec!["docker.io/library/busybox:latest".to_string()],
            ..Default::default()
        },
    )
    .await;

    let response = ImageService::image_status(
        &service,
        Request::new(ImageStatusRequest {
            image: Some(ImageSpec {
                image: "busybox".to_string(),
                ..Default::default()
            }),
            verbose: false,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(
        response.image.expect("expected image for short name").id,
        "sha256:busybox-id"
    );
}

#[tokio::test]
async fn load_local_images_marks_matching_pinned_images() {
    let dir = tempdir().unwrap();
    let image_dir = dir.path().join("images").join("sha256:pinned-id");
    std::fs::create_dir_all(&image_dir).unwrap();
    std::fs::write(
        image_dir.join("metadata.json"),
        serde_json::json!({
            "id": "sha256:pinned-id",
            "repo_tags": ["busybox:latest"],
            "repo_digests": [],
            "pinned": false
        })
        .to_string(),
    )
    .unwrap();
    let service = test_image_service_with_options(
        dir.path(),
        "overlay",
        Option::<&Path>::None,
        Option::<&Path>::None,
        Option::<&Path>::None,
        vec!["busybox*".to_string()],
    );

    service.load_local_images().await.unwrap();
    let response =
        ImageService::list_images(&service, Request::new(ListImagesRequest { filter: None }))
            .await
            .unwrap()
            .into_inner();
    assert_eq!(response.images.len(), 1);
    assert!(response.images[0].pinned);
}

#[tokio::test]
async fn image_fs_info_reports_real_usage() {
    let (dir, service) = test_image_service_in_tempdir();
    let image_dir = dir.path().join("images").join("sha256:test-id");
    std::fs::create_dir_all(&image_dir).unwrap();
    std::fs::write(image_dir.join("layer1.tar"), b"abcde").unwrap();
    std::fs::write(image_dir.join("layer2.tar"), b"123456789").unwrap();

    let response = ImageService::image_fs_info(&service, Request::new(ImageFsInfoRequest {}))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.image_filesystems.len(), 1);
    let usage = &response.image_filesystems[0];
    assert!(usage.timestamp > 0);
    assert_eq!(
        usage
            .fs_id
            .as_ref()
            .expect("expected filesystem identifier")
            .mountpoint,
        dir.path().display().to_string()
    );
    let used_bytes = usage
        .used_bytes
        .as_ref()
        .expect("expected used bytes")
        .value;
    assert!(
        used_bytes >= 14,
        "expected used bytes >= 14, got {}",
        used_bytes
    );
}

#[tokio::test]
async fn pull_persists_layers_into_content_store_and_metadata() {
    let (dir, service) = test_image_service_in_tempdir();
    service.set_test_pull_handler(Arc::new(|_| {
        Ok(TestPullResponse {
            image_id: "sha256:blob-backed".to_string(),
            size: TEST_EMPTY_LAYER_TAR_GZ.len() as u64,
            annotations: HashMap::new(),
            declared_volumes: Vec::new(),
        })
    }));

    ImageService::pull_image(
        &service,
        Request::new(PullImageRequest {
            image: Some(ImageSpec {
                image: "repo/blob-backed:latest".to_string(),
                user_specified_image: "repo/blob-backed:latest".to_string(),
                ..Default::default()
            }),
            auth: None,
            sandbox_config: None,
        }),
    )
    .await
    .unwrap();

    let meta = service
        .load_image_metadata("sha256:blob-backed")
        .expect("expected image metadata");
    assert_eq!(meta.stored_layers.len(), 1);
    assert!(
        !meta.stored_layers[0].digest.is_empty(),
        "expected stored layer digest to be recorded"
    );
    let blob_path = dir.path().join(&meta.stored_layers[0].path);
    assert!(
        blob_path.exists(),
        "expected blob-backed layer path {} to exist",
        blob_path.display()
    );
}

#[tokio::test]
async fn pull_image_records_successful_content_transfer() {
    let (_dir, service) = test_image_service_in_tempdir();
    service.set_test_pull_handler(Arc::new(|_| {
        Ok(TestPullResponse {
            image_id: "sha256:transfer-success".to_string(),
            size: TEST_EMPTY_LAYER_TAR_GZ.len() as u64,
            ..Default::default()
        })
    }));

    ImageService::pull_image(
        &service,
        Request::new(PullImageRequest {
            image: Some(ImageSpec {
                image: "repo/transfer-success:latest".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    let status = service.content_transfer_status();
    assert!(status.active.is_empty());
    assert_eq!(status.recent.len(), 1);
    let record = &status.recent[0];
    assert_eq!(record.source, "docker.io/repo/transfer-success:latest");
    assert_eq!(record.provider, RemoteContentProviderKind::Test);
    assert_eq!(record.state, TransferState::Succeeded);
    assert!(record.finished_at_unix_nanos.is_some());
    assert!(record.error.is_none());
}

#[tokio::test]
async fn pull_image_records_failed_content_transfer() {
    let (_dir, service) = test_image_service_in_tempdir();
    service.set_test_pull_handler(Arc::new(|_| Err(Status::internal("registry failed"))));

    let result = ImageService::pull_image(
        &service,
        Request::new(PullImageRequest {
            image: Some(ImageSpec {
                image: "repo/transfer-failure:latest".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        }),
    )
    .await;

    assert!(result.is_err());
    let status = service.content_transfer_status();
    assert!(status.active.is_empty());
    assert_eq!(status.recent.len(), 1);
    let record = &status.recent[0];
    assert_eq!(record.source, "docker.io/repo/transfer-failure:latest");
    assert_eq!(record.provider, RemoteContentProviderKind::Test);
    assert_eq!(record.state, TransferState::Failed);
    assert_eq!(record.error.as_deref(), Some("registry failed"));
}

#[tokio::test]
async fn pull_image_persists_content_transfer_result_to_ledger() {
    let (_dir, service, ledger_db_path) = test_image_service_with_ledger_in_tempdir();
    service.set_test_pull_handler(Arc::new(|_| {
        Ok(TestPullResponse {
            image_id: "sha256:transfer-ledger".to_string(),
            size: TEST_EMPTY_LAYER_TAR_GZ.len() as u64,
            ..Default::default()
        })
    }));

    ImageService::pull_image(
        &service,
        Request::new(PullImageRequest {
            image: Some(ImageSpec {
                image: "repo/transfer-ledger:latest".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    let storage = StorageManager::new(&ledger_db_path).unwrap();
    let transfers = storage.list_content_transfers().unwrap();
    assert_eq!(transfers.len(), 1);
    assert_eq!(transfers[0].source, "docker.io/repo/transfer-ledger:latest");
    assert_eq!(transfers[0].state, "succeeded");
    assert!(transfers[0].finished_at.is_some());
}

#[test]
fn image_service_marks_interrupted_transfer_on_startup() {
    let dir = tempdir().unwrap();
    let ledger_db_path = dir.path().join("crius.db");
    let mut storage = StorageManager::new(&ledger_db_path).unwrap();
    storage
        .save_content_transfer(&crate::storage::ContentTransferRecord {
            id: "startup-transfer".to_string(),
            source: "docker.io/repo/interrupted:latest".to_string(),
            provider: "registry".to_string(),
            state: "running".to_string(),
            current_stage: "downloading".to_string(),
            bytes_total: 20,
            bytes_completed: 8,
            started_at: 100,
            finished_at: None,
            error: None,
        })
        .unwrap();
    drop(storage);

    let service = ImageServiceImpl::new_with_options(ImageServiceOptions {
        storage_path: dir.path().join("images"),
        ledger_db_path: Some(ledger_db_path.clone()),
        storage_driver: "overlay".to_string(),
        storage_options: Vec::new(),
        global_auth_file: None,
        namespaced_auth_dir: None,
        default_transport: "docker://".to_string(),
        short_name_mode: "disabled".to_string(),
        pull_progress_timeout: std::time::Duration::ZERO,
        max_concurrent_downloads: 3,
        pull_retry_count: 0,
        registry_config_dir: None,
        decryption_keys_path: None,
        decryption_decoder_path: "ctd-decoder".to_string(),
        decryption_keyprovider_config: None,
        additional_artifact_stores: Vec::new(),
        pinned_image_patterns: Vec::new(),
        signature_policy: None,
        signature_policy_dir: None,
        big_files_temporary_dir: None,
        separate_pull_cgroup: String::new(),
        cgroup_driver: crate::config::CgroupDriverConfig::Cgroupfs,
        rootless: crate::rootless::EffectiveRootlessConfig::disabled(),
        disable_cgroup: false,
        pull_cgroup_root: None,
    })
    .unwrap();

    let status = service.content_transfer_status();
    assert!(status.active.is_empty());
    assert_eq!(status.recent.len(), 1);
    assert_eq!(status.recent[0].state, TransferState::Interrupted);

    let storage = StorageManager::new(&ledger_db_path).unwrap();
    let transfers = storage.list_content_transfers().unwrap();
    assert_eq!(transfers[0].state, "interrupted");
}

#[tokio::test]
async fn pull_image_does_not_materialize_container_rootfs() {
    let (dir, service) = test_image_service_in_tempdir();
    service.set_test_pull_handler(Arc::new(|_| {
        Ok(TestPullResponse {
            image_id: "sha256:pull-only".to_string(),
            size: TEST_EMPTY_LAYER_TAR_GZ.len() as u64,
            ..Default::default()
        })
    }));

    ImageService::pull_image(
        &service,
        Request::new(PullImageRequest {
            image: Some(ImageSpec {
                image: "repo/pull-only:latest".to_string(),
                user_specified_image: "repo/pull-only:latest".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    let meta = service
        .load_image_metadata("sha256:pull-only")
        .expect("expected pull metadata");
    assert_eq!(meta.stored_layers.len(), 1);
    assert!(dir.path().join(&meta.stored_layers[0].path).exists());
    assert!(dir
        .path()
        .join("images")
        .join("sha256:pull-only")
        .join("metadata.json")
        .exists());
    assert!(
        !dir.path().join("snapshots").exists(),
        "pull must not create snapshot/rootfs storage"
    );
    assert!(
        !dir.path()
            .join("images")
            .join("sha256:pull-only")
            .join("rootfs")
            .exists(),
        "pull metadata directory must not contain a rootfs"
    );
}

#[tokio::test]
async fn remove_image_reports_in_use_when_container_references_it() {
    let (dir, service) = test_image_service_in_tempdir();
    insert_image(
        &service,
        Image {
            id: "sha256:busybox-id".to_string(),
            repo_tags: vec!["busybox:latest".to_string()],
            ..Default::default()
        },
    )
    .await;

    let db_path = dir.path().join("crius.db");
    let mut storage = StorageManager::new(&db_path).unwrap();
    storage
        .save_container(&ContainerRecord {
            id: "container-1".to_string(),
            pod_id: "pod-1".to_string(),
            state: "running".to_string(),
            image: "busybox:latest".to_string(),
            command: "sleep 60".to_string(),
            created_at: Utc::now().timestamp(),
            labels: "{}".to_string(),
            annotations: "{}".to_string(),
            exit_code: None,
            exit_time: None,
            runtime_handler: None,
            runtime_backend: None,
            snapshot_key: None,
        })
        .unwrap();

    let err = ImageService::remove_image(
        &service,
        Request::new(RemoveImageRequest {
            image: Some(ImageSpec {
                image: "busybox:latest".to_string(),
                ..Default::default()
            }),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("in use"));
}

#[tokio::test]
async fn remove_image_untags_single_reference_when_other_tags_remain() {
    let (dir, service) = test_image_service_in_tempdir();
    let image_dir = dir.path().join("images").join("sha256:untag-id");
    std::fs::create_dir_all(&image_dir).unwrap();
    service
        .save_image_metadata(&CriusImage {
            id: "sha256:untag-id".to_string(),
            repo_tags: vec![
                "docker.io/library/busybox:latest".to_string(),
                "docker.io/library/busybox:debug".to_string(),
            ],
            repo_digests: vec!["docker.io/library/busybox@sha256:untag-id".to_string()],
            size: 10,
            pinned: false,
            pulled_at: 123,
            source_reference: Some("busybox".to_string()),
            os: Some("linux".to_string()),
            architecture: Some("amd64".to_string()),
            config_user: Some("1000".to_string()),
            config_env: Vec::new(),
            config_entrypoint: Vec::new(),
            config_cmd: Vec::new(),
            config_working_dir: None,
            annotations: HashMap::from([(
                "org.opencontainers.image.title".to_string(),
                "busybox".to_string(),
            )]),
            declared_volumes: vec!["/cache".to_string(), "/data".to_string()],
            manifest_media_type: Some("application/vnd.oci.image.manifest.v1+json".to_string()),
            selected_manifest_digest: None,
            selected_platform: None,
            stored_layers: Vec::new(),
            artifact_type: None,
            artifact_blobs: Vec::new(),
        })
        .await
        .unwrap();
    insert_image(
        &service,
        Image {
            id: "sha256:untag-id".to_string(),
            repo_tags: vec![
                "docker.io/library/busybox:latest".to_string(),
                "docker.io/library/busybox:debug".to_string(),
            ],
            repo_digests: vec!["docker.io/library/busybox@sha256:untag-id".to_string()],
            spec: Some(ImageSpec {
                image: "docker.io/library/busybox:latest".to_string(),
                user_specified_image: "docker.io/library/busybox:latest".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await;

    ImageService::remove_image(
        &service,
        Request::new(RemoveImageRequest {
            image: Some(ImageSpec {
                image: "docker.io/library/busybox:latest".to_string(),
                ..Default::default()
            }),
        }),
    )
    .await
    .unwrap();

    let remaining = ImageService::image_status(
        &service,
        Request::new(ImageStatusRequest {
            image: Some(ImageSpec {
                image: "docker.io/library/busybox:debug".to_string(),
                ..Default::default()
            }),
            verbose: true,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let image = remaining.image.expect("expected remaining tag");
    assert_eq!(image.repo_tags, vec!["docker.io/library/busybox:debug"]);
    let info: serde_json::Value =
        serde_json::from_str(remaining.info.get("info").expect("missing verbose info")).unwrap();
    assert_eq!(info["pulledAt"], 123);
    assert_eq!(info["sourceReference"], "busybox");
    assert_eq!(info["os"], "linux");
    assert_eq!(info["architecture"], "amd64");
    assert_eq!(info["configUser"], "1000");
    assert_eq!(
        info["manifestMediaType"],
        "application/vnd.oci.image.manifest.v1+json"
    );
    assert_eq!(
        info["annotations"]["org.opencontainers.image.title"],
        "busybox"
    );
    assert_eq!(
        info["declaredVolumes"],
        serde_json::json!(["/cache", "/data"])
    );
    assert!(
        image_dir.exists(),
        "image directory should remain after untag"
    );

    let removed = ImageService::image_status(
        &service,
        Request::new(ImageStatusRequest {
            image: Some(ImageSpec {
                image: "docker.io/library/busybox:latest".to_string(),
                ..Default::default()
            }),
            verbose: false,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert!(removed.image.is_none());
}

#[tokio::test]
async fn remove_image_by_id_deletes_all_tags_for_same_image() {
    let dir = tempdir().unwrap();
    let service = test_image_service_with_options(
        dir.path(),
        "overlay",
        Option::<&Path>::None,
        Option::<&Path>::None,
        Option::<&Path>::None,
        Vec::new(),
    );
    insert_image(
        &service,
        Image {
            id: "sha256:multi-tag-id".to_string(),
            repo_tags: vec![
                "docker.io/library/busybox:latest".to_string(),
                "docker.io/library/busybox:debug".to_string(),
            ],
            repo_digests: vec!["docker.io/library/busybox@sha256:multi-tag-id".to_string()],
            ..Default::default()
        },
    )
    .await;

    ImageService::remove_image(
        &service,
        Request::new(RemoveImageRequest {
            image: Some(ImageSpec {
                image: "sha256:multi-tag-id".to_string(),
                ..Default::default()
            }),
        }),
    )
    .await
    .expect("image id removal should delete the whole image");

    for reference in [
        "sha256:multi-tag-id",
        "docker.io/library/busybox:latest",
        "docker.io/library/busybox:debug",
    ] {
        let removed = ImageService::image_status(
            &service,
            Request::new(ImageStatusRequest {
                image: Some(ImageSpec {
                    image: reference.to_string(),
                    ..Default::default()
                }),
                verbose: false,
            }),
        )
        .await
        .expect("image status lookup should succeed")
        .into_inner();
        assert!(
            removed.image.is_none(),
            "reference {reference} should be removed with image id deletion"
        );
    }
}

#[tokio::test]
async fn remove_image_allows_pinned_images() {
    let dir = tempdir().unwrap();
    let service = test_image_service_with_options(
        dir.path(),
        "overlay",
        Option::<&Path>::None,
        Option::<&Path>::None,
        Option::<&Path>::None,
        vec!["busybox*".to_string()],
    );
    insert_image(
        &service,
        Image {
            id: "sha256:pinned-id".to_string(),
            repo_tags: vec!["busybox:latest".to_string()],
            pinned: true,
            ..Default::default()
        },
    )
    .await;

    let response = ImageService::remove_image(
        &service,
        Request::new(RemoveImageRequest {
            image: Some(ImageSpec {
                image: "busybox:latest".to_string(),
                ..Default::default()
            }),
        }),
    )
    .await
    .expect("pinned image removal should succeed");
    let _ = response.into_inner();

    let removed = ImageService::image_status(
        &service,
        Request::new(ImageStatusRequest {
            image: Some(ImageSpec {
                image: "busybox:latest".to_string(),
                ..Default::default()
            }),
            verbose: false,
        }),
    )
    .await
    .expect("image status lookup should succeed")
    .into_inner();
    assert!(removed.image.is_none());
}

#[tokio::test]
async fn load_local_images_includes_additional_artifact_stores() {
    let dir = tempdir().unwrap();
    let additional = dir.path().join("readonly-store");
    let artifact_dir = additional.join("artifacts").join("sha256:artifact-id");
    std::fs::create_dir_all(&artifact_dir).unwrap();
    std::fs::write(artifact_dir.join("0.tar.gz"), b"artifact").unwrap();
    std::fs::write(
        artifact_dir.join("metadata.json"),
        serde_json::to_vec(&CriusImage {
            id: "sha256:artifact-id".to_string(),
            repo_tags: vec!["registry.example.com/artifact:latest".to_string()],
            repo_digests: vec!["registry.example.com/artifact@sha256:artifact-id".to_string()],
            size: 8,
            pinned: false,
            pulled_at: 0,
            source_reference: None,
            os: None,
            architecture: None,
            config_user: None,
            config_env: Vec::new(),
            config_entrypoint: Vec::new(),
            config_cmd: Vec::new(),
            config_working_dir: None,
            annotations: HashMap::new(),
            declared_volumes: Vec::new(),
            manifest_media_type: Some("application/vnd.oci.image.manifest.v1+json".to_string()),
            selected_manifest_digest: None,
            selected_platform: None,
            stored_layers: Vec::new(),
            artifact_type: Some("application/vnd.example.artifact".to_string()),
            artifact_blobs: vec![ArtifactBlobMeta {
                digest: "sha256:blob".to_string(),
                media_type: "text/plain".to_string(),
                path: "artifact.txt".to_string(),
                size: 8,
                annotations: HashMap::from([(
                    "org.opencontainers.image.title".to_string(),
                    "artifact.txt".to_string(),
                )]),
            }],
        })
        .unwrap(),
    )
    .unwrap();

    let service = ImageServiceImpl::new_with_options(ImageServiceOptions {
        storage_path: dir.path().join("storage"),
        ledger_db_path: None,
        storage_driver: "overlay".to_string(),
        storage_options: Vec::new(),
        global_auth_file: None,
        namespaced_auth_dir: None,
        default_transport: "docker://".to_string(),
        short_name_mode: "disabled".to_string(),
        pull_progress_timeout: std::time::Duration::ZERO,
        max_concurrent_downloads: 3,
        pull_retry_count: 0,
        registry_config_dir: None,
        decryption_keys_path: None,
        decryption_decoder_path: "ctd-decoder".to_string(),
        decryption_keyprovider_config: None,
        additional_artifact_stores: vec![additional.clone()],
        pinned_image_patterns: Vec::new(),
        signature_policy: None,
        signature_policy_dir: None,
        big_files_temporary_dir: None,
        separate_pull_cgroup: String::new(),
        cgroup_driver: crate::config::CgroupDriverConfig::Cgroupfs,
        rootless: crate::rootless::EffectiveRootlessConfig::disabled(),
        disable_cgroup: false,
        pull_cgroup_root: None,
    })
    .unwrap();

    service.load_local_images().await.unwrap();
    let response = ImageService::image_status(
        &service,
        Request::new(ImageStatusRequest {
            image: Some(ImageSpec {
                image: "registry.example.com/artifact:latest".to_string(),
                user_specified_image: "registry.example.com/artifact:latest".to_string(),
                runtime_handler: String::new(),
                annotations: HashMap::new(),
            }),
            verbose: true,
        }),
    )
    .await
    .unwrap()
    .into_inner();

    let image = response
        .image
        .expect("artifact should be visible in image status");
    assert_eq!(image.id, "sha256:artifact-id");
    let info: serde_json::Value = serde_json::from_str(response.info.get("info").unwrap()).unwrap();
    assert_eq!(info["artifactType"], "application/vnd.example.artifact");
    assert_eq!(info["storagePath"], artifact_dir.display().to_string());
}

#[tokio::test]
async fn remove_image_rejects_artifact_from_additional_store() {
    let dir = tempdir().unwrap();
    let additional = dir.path().join("readonly-store");
    let artifact_dir = additional.join("artifacts").join("sha256:artifact-id");
    std::fs::create_dir_all(&artifact_dir).unwrap();
    std::fs::write(artifact_dir.join("0.tar.gz"), b"artifact").unwrap();
    std::fs::write(
        artifact_dir.join("metadata.json"),
        serde_json::to_vec(&CriusImage {
            id: "sha256:artifact-id".to_string(),
            repo_tags: vec!["registry.example.com/artifact:latest".to_string()],
            repo_digests: vec!["registry.example.com/artifact@sha256:artifact-id".to_string()],
            size: 8,
            pinned: false,
            pulled_at: 0,
            source_reference: None,
            os: None,
            architecture: None,
            config_user: None,
            config_env: Vec::new(),
            config_entrypoint: Vec::new(),
            config_cmd: Vec::new(),
            config_working_dir: None,
            annotations: HashMap::new(),
            declared_volumes: Vec::new(),
            manifest_media_type: Some("application/vnd.oci.image.manifest.v1+json".to_string()),
            selected_manifest_digest: None,
            selected_platform: None,
            stored_layers: Vec::new(),
            artifact_type: Some("application/vnd.example.artifact".to_string()),
            artifact_blobs: vec![ArtifactBlobMeta {
                digest: "sha256:blob".to_string(),
                media_type: "text/plain".to_string(),
                path: "artifact.txt".to_string(),
                size: 8,
                annotations: HashMap::new(),
            }],
        })
        .unwrap(),
    )
    .unwrap();

    let service = ImageServiceImpl::new_with_options(ImageServiceOptions {
        storage_path: dir.path().join("storage"),
        ledger_db_path: None,
        storage_driver: "overlay".to_string(),
        storage_options: Vec::new(),
        global_auth_file: None,
        namespaced_auth_dir: None,
        default_transport: "docker://".to_string(),
        short_name_mode: "disabled".to_string(),
        pull_progress_timeout: std::time::Duration::ZERO,
        max_concurrent_downloads: 3,
        pull_retry_count: 0,
        registry_config_dir: None,
        decryption_keys_path: None,
        decryption_decoder_path: "ctd-decoder".to_string(),
        decryption_keyprovider_config: None,
        additional_artifact_stores: vec![additional],
        pinned_image_patterns: Vec::new(),
        signature_policy: None,
        signature_policy_dir: None,
        big_files_temporary_dir: None,
        separate_pull_cgroup: String::new(),
        cgroup_driver: crate::config::CgroupDriverConfig::Cgroupfs,
        rootless: crate::rootless::EffectiveRootlessConfig::disabled(),
        disable_cgroup: false,
        pull_cgroup_root: None,
    })
    .unwrap();
    service.load_local_images().await.unwrap();

    let err = ImageService::remove_image(
        &service,
        Request::new(RemoveImageRequest {
            image: Some(ImageSpec {
                image: "registry.example.com/artifact:latest".to_string(),
                user_specified_image: String::new(),
                runtime_handler: String::new(),
                annotations: HashMap::new(),
            }),
        }),
    )
    .await
    .expect_err("removing read-only additional store artifact must fail");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err
        .message()
        .contains("read-only additional OCI artifact store"));
}

#[test]
fn decrypt_layer_bytes_uses_external_decoder() {
    let dir = tempdir().unwrap();
    let decoder = dir.path().join("fake-decoder.sh");
    let keys_dir = dir.path().join("keys");
    std::fs::create_dir_all(&keys_dir).unwrap();
    std::fs::write(keys_dir.join("test.pem"), b"key").unwrap();
    std::fs::write(
        &decoder,
        r#"#!/bin/sh
set -eu
cat
"#,
    )
    .unwrap();
    std::fs::set_permissions(&decoder, std::fs::Permissions::from_mode(0o755)).unwrap();

    let service = ImageServiceImpl::new_with_options(ImageServiceOptions {
        storage_path: dir.path().join("storage"),
        ledger_db_path: None,
        storage_driver: "overlay".to_string(),
        storage_options: Vec::new(),
        global_auth_file: None,
        namespaced_auth_dir: None,
        default_transport: "docker://".to_string(),
        short_name_mode: "disabled".to_string(),
        pull_progress_timeout: std::time::Duration::ZERO,
        max_concurrent_downloads: 3,
        pull_retry_count: 0,
        registry_config_dir: None,
        decryption_keys_path: Some(keys_dir),
        decryption_decoder_path: decoder.display().to_string(),
        decryption_keyprovider_config: None,
        additional_artifact_stores: Vec::new(),
        pinned_image_patterns: Vec::new(),
        signature_policy: None,
        signature_policy_dir: None,
        big_files_temporary_dir: None,
        separate_pull_cgroup: String::new(),
        cgroup_driver: crate::config::CgroupDriverConfig::Cgroupfs,
        rootless: crate::rootless::EffectiveRootlessConfig::disabled(),
        disable_cgroup: false,
        pull_cgroup_root: None,
    })
    .unwrap();

    let (bytes, media_type) = service
        .decrypt_layer_bytes(
            "application/vnd.oci.image.layer.v1.tar+gzip+encrypted",
            b"encrypted-layer",
        )
        .unwrap();
    assert_eq!(bytes, b"encrypted-layer");
    assert_eq!(media_type, "application/vnd.oci.image.layer.v1.tar+gzip");
}
