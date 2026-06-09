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
    assert_eq!(artifact["manifest"]["rootfsSnapshot"]["path"], "rootfs.tar");
    assert_eq!(artifact["manifest"]["rootfsSnapshot"]["format"], "tar");
    assert_eq!(
        artifact["manifest"]["rootfsSnapshot"]["restorePolicy"],
        "replace"
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
async fn checkpoint_container_uses_configured_criu_staging_paths() {
    let (dir, mut service) = test_service_with_fake_runtime();
    service.config.criu_image_path = dir.path().join("criu-images");
    service.config.criu_work_path = dir.path().join("criu-work");

    service.containers.lock().await.insert(
        "container-running".to_string(),
        test_container("container-running", "pod-1", HashMap::new()),
    );
    set_fake_runtime_state(&dir, "container-running", "running");

    let bundle_dir = dir.path().join("runtime-root").join("container-running");
    fs::create_dir_all(&bundle_dir).unwrap();
    let rootfs_dir = dir.path().join("checkpoint-rootfs-custom");
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
    let expected_image_path = service.checkpoint_runtime_image_path(&artifact_path);
    let expected_work_path = service.checkpoint_runtime_work_path(&artifact_path);
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

    assert!(expected_image_path.join("checkpoint.json").exists());
    assert!(expected_image_path.join("rootfs.tar").exists());
    assert!(expected_work_path.is_dir());
    assert!(
        expected_image_path.starts_with(dir.path().join("criu-images")),
        "checkpoint image staging path should honor runtime.criu_image_path"
    );
    assert!(
        expected_work_path.starts_with(dir.path().join("criu-work")),
        "checkpoint work staging path should honor runtime.criu_work_path"
    );
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
async fn checkpoint_container_fails_when_criu_support_is_disabled() {
    let (dir, mut service) = test_service_with_fake_runtime();
    service.config.enable_criu_support = false;

    service.containers.lock().await.insert(
        "container-running".to_string(),
        test_container("container-running", "pod-1", HashMap::new()),
    );
    set_fake_runtime_state(&dir, "container-running", "running");

    let err = RuntimeService::checkpoint_container(
        &service,
        Request::new(CheckpointContainerRequest {
            container_id: "container-running".to_string(),
            location: dir.path().join("checkpoint.json").display().to_string(),
            timeout: 30,
        }),
    )
    .await
    .unwrap_err();

    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("CRIU support is disabled"));
}

#[test]
fn validate_checkpoint_rootfs_snapshot_manifest_rejects_unsupported_format() {
    let dir = tempdir().unwrap();
    let checkpoint_image_path = dir.path().join("checkpoint");
    fs::create_dir_all(&checkpoint_image_path).unwrap();
    fs::write(checkpoint_image_path.join("rootfs.tar"), "placeholder").unwrap();

    let manifest = serde_json::json!({
        "rootfsSnapshot": {
            "path": "rootfs.tar",
            "format": "cpio",
            "restorePolicy": "replace",
        }
    });

    let err = RuntimeServiceImpl::validate_checkpoint_rootfs_snapshot_manifest(
        &manifest,
        &checkpoint_image_path,
    )
    .expect_err("unsupported rootfs snapshot format must be rejected");
    assert!(err
        .to_string()
        .contains("checkpoint rootfsSnapshot.format must be tar"));
}

#[test]
fn checkpoint_restore_from_artifact_requires_manifest_image_ref() {
    let (dir, service) = test_service_with_fake_runtime();
    let checkpoint_location = dir.path().join("restore-artifact.json");
    let checkpoint_image_path = service.checkpoint_runtime_image_path(&checkpoint_location);
    fs::create_dir_all(&checkpoint_image_path).unwrap();
    fs::write(checkpoint_image_path.join("rootfs.tar"), "placeholder").unwrap();
    fs::write(
        &checkpoint_location,
        serde_json::to_vec_pretty(&serde_json::json!({
            "manifest": {
                "rootfsSnapshot": {
                    "path": "rootfs.tar",
                    "format": "tar",
                    "restorePolicy": "replace",
                }
            },
            "ociConfig": {
                "ociVersion": "1.0.2",
                "root": { "path": "/tmp/rootfs" }
            }
        }))
        .unwrap(),
    )
    .unwrap();

    let err = service
        .checkpoint_restore_from_artifact(&checkpoint_location)
        .expect_err("restore artifact without imageRef must be rejected");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("manifest.imageRef"));
}

#[test]
fn checkpoint_restore_from_artifact_ignores_manifest_bundle_paths() {
    let (dir, service) = test_service_with_fake_runtime();
    let checkpoint_location = dir.path().join("restore-artifact.json");
    let checkpoint_image_path = service.checkpoint_runtime_image_path(&checkpoint_location);
    fs::create_dir_all(&checkpoint_image_path).unwrap();
    fs::write(checkpoint_image_path.join("rootfs.tar"), "placeholder").unwrap();
    fs::write(
        &checkpoint_location,
        serde_json::to_vec_pretty(&serde_json::json!({
            "manifest": {
                "imageRef": "busybox:latest",
                "bundlePath": "/definitely/stale/bundle",
                "configPath": "/definitely/stale/config.json",
                "rootfsSnapshot": {
                    "path": "rootfs.tar",
                    "format": "tar",
                    "restorePolicy": "replace",
                }
            },
            "ociConfig": {
                "ociVersion": "1.0.2",
                "root": { "path": "/tmp/rootfs" }
            }
        }))
        .unwrap(),
    )
    .unwrap();

    let restore = service
        .checkpoint_restore_from_artifact(&checkpoint_location)
        .expect("bundle/config export paths are informational only");
    assert_eq!(restore.image_ref, "busybox:latest");
    assert_eq!(
        restore.checkpoint_image_path,
        checkpoint_image_path.display().to_string()
    );
}

#[tokio::test]
async fn start_container_restores_from_checkpoint_artifact_and_clears_pending_marker() {
    let (dir, service) = test_service_with_fake_runtime();

    let checkpoint_location = dir.path().join("restore-artifact.json");
    let checkpoint_image_path = service.checkpoint_runtime_image_path(&checkpoint_location);
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
            Some("pod-1"),
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

    let exit_code_path = dir.path().join("exits").join("restore-container");
    fs::create_dir_all(exit_code_path.parent().unwrap()).unwrap();
    fs::write(&exit_code_path, "0").unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
}

#[tokio::test]
async fn start_container_rejects_checkpoint_restore_when_criu_support_is_disabled() {
    let (dir, mut service) = test_service_with_fake_runtime();
    service.config.enable_criu_support = false;

    let checkpoint_location = dir.path().join("restore-artifact.json");
    let checkpoint_image_path = service.checkpoint_runtime_image_path(&checkpoint_location);
    fs::create_dir_all(&checkpoint_image_path).unwrap();
    fs::write(checkpoint_image_path.join("checkpoint.json"), "{}").unwrap();

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
                "root": { "path": "/tmp/rootfs" }
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
            Some("pod-1"),
            crate::runtime::ContainerStatus::Created,
            "busybox:latest",
            &Vec::new(),
            &HashMap::new(),
            &annotations,
        )
        .unwrap();

    let err = RuntimeService::start_container(
        &service,
        Request::new(StartContainerRequest {
            container_id: "restore-container".to_string(),
        }),
    )
    .await
    .unwrap_err();

    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("CRIU support is disabled"));
    let container = service
        .containers
        .lock()
        .await
        .get("restore-container")
        .cloned()
        .unwrap();
    assert!(container
        .annotations
        .contains_key(INTERNAL_CHECKPOINT_RESTORE_KEY));
}

