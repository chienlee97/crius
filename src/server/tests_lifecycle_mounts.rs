#[test]
fn runtime_mounts_from_proto_resolves_oci_artifact_mounts() {
    let (dir, service) = test_service_with_fake_runtime();
    let artifact_dir = service
        .config
        .image_root
        .join("artifacts")
        .join("sha256:test-artifact");
    fs::create_dir_all(&artifact_dir).unwrap();
    fs::write(artifact_dir.join("0.tar.gz"), b"hello artifact").unwrap();
    fs::write(
        artifact_dir.join("metadata.json"),
        serde_json::to_vec(&crate::image::CriusImage {
            id: "sha256:test-artifact".to_string(),
            repo_tags: vec!["registry.example.com/artifact:latest".to_string()],
            repo_digests: vec!["registry.example.com/artifact@sha256:test-artifact".to_string()],
            size: 14,
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
            artifact_blobs: vec![crate::image::ArtifactBlobMeta {
                digest: "sha256:blob".to_string(),
                media_type: "text/plain".to_string(),
                path: "artifact.txt".to_string(),
                size: 14,
                annotations: HashMap::from([(
                    "org.opencontainers.image.title".to_string(),
                    "artifact.txt".to_string(),
                )]),
            }],
        })
        .unwrap(),
    )
    .unwrap();

    let mounts = service
        .runtime_mounts_from_proto(
            &[crate::proto::runtime::v1::Mount {
                container_path: "/root/artifact".to_string(),
                host_path: String::new(),
                readonly: false,
                selinux_relabel: false,
                propagation: crate::proto::runtime::v1::MountPropagation::PropagationPrivate as i32,
                uid_mappings: Vec::new(),
                gid_mappings: Vec::new(),
                recursive_read_only: false,
                image: Some(crate::proto::runtime::v1::ImageSpec {
                    image: "registry.example.com/artifact:latest".to_string(),
                    user_specified_image: "registry.example.com/artifact:latest".to_string(),
                    runtime_handler: String::new(),
                    annotations: HashMap::new(),
                }),
                image_sub_path: String::new(),
            }],
            true,
        )
        .unwrap();

    assert_eq!(mounts.len(), 1);
    assert_eq!(mounts[0].source, artifact_dir.join("0.tar.gz"));
    assert_eq!(
        mounts[0].destination,
        PathBuf::from("/root/artifact/artifact.txt")
    );
    assert!(mounts[0].read_only);
    assert_eq!(
        mounts[0].requested_image.as_deref(),
        Some("registry.example.com/artifact:latest")
    );

    drop(dir);
}

#[test]
fn runtime_mounts_from_proto_rejects_oci_artifact_file_target() {
    let (dir, service) = test_service_with_fake_runtime();
    let err = service
        .runtime_mounts_from_proto(
            &[crate::proto::runtime::v1::Mount {
                container_path: "/root/artifact.txt".to_string(),
                host_path: String::new(),
                readonly: false,
                selinux_relabel: false,
                propagation: crate::proto::runtime::v1::MountPropagation::PropagationPrivate as i32,
                uid_mappings: Vec::new(),
                gid_mappings: Vec::new(),
                recursive_read_only: false,
                image: Some(crate::proto::runtime::v1::ImageSpec {
                    image: "registry.example.com/artifact:latest".to_string(),
                    user_specified_image: "registry.example.com/artifact:latest".to_string(),
                    runtime_handler: String::new(),
                    annotations: HashMap::new(),
                }),
                image_sub_path: String::new(),
            }],
            true,
        )
        .unwrap_err();

    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("directory"));

    drop(dir);
}

#[test]
fn runtime_mounts_from_proto_explains_regular_image_mounts() {
    let (dir, service) = test_service_with_fake_runtime();
    let image_dir = service
        .config
        .image_root
        .join("images")
        .join("sha256:test-image");
    fs::create_dir_all(&image_dir).unwrap();
    fs::write(
        image_dir.join("metadata.json"),
        serde_json::to_vec(&crate::image::CriusImage {
            id: "sha256:test-image".to_string(),
            repo_tags: vec!["registry.example.com/app:latest".to_string()],
            repo_digests: vec!["registry.example.com/app@sha256:test-image".to_string()],
            size: 14,
            pinned: false,
            pulled_at: 0,
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
            selected_manifest_digest: None,
            selected_platform: None,
            stored_layers: Vec::new(),
            artifact_type: None,
            artifact_blobs: Vec::new(),
        })
        .unwrap(),
    )
    .unwrap();

    let err = service
        .runtime_mounts_from_proto(
            &[crate::proto::runtime::v1::Mount {
                container_path: "/root/image".to_string(),
                host_path: String::new(),
                readonly: false,
                selinux_relabel: false,
                propagation: crate::proto::runtime::v1::MountPropagation::PropagationPrivate as i32,
                uid_mappings: Vec::new(),
                gid_mappings: Vec::new(),
                recursive_read_only: false,
                image: Some(crate::proto::runtime::v1::ImageSpec {
                    image: "registry.example.com/app:latest".to_string(),
                    user_specified_image: "registry.example.com/app:latest".to_string(),
                    runtime_handler: String::new(),
                    annotations: HashMap::new(),
                }),
                image_sub_path: String::new(),
            }],
            true,
        )
        .unwrap_err();

    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("regular container image"));
    assert!(err.message().contains("OCI artifact"));

    drop(dir);
}

#[test]
fn runtime_mounts_from_proto_rejects_missing_host_path_for_bind_mount() {
    let (_dir, service) = test_service_with_fake_runtime();
    let err = service
        .runtime_mounts_from_proto(
            &[crate::proto::runtime::v1::Mount {
                container_path: "/data".to_string(),
                host_path: String::new(),
                readonly: false,
                selinux_relabel: false,
                propagation: crate::proto::runtime::v1::MountPropagation::PropagationPrivate as i32,
                uid_mappings: Vec::new(),
                gid_mappings: Vec::new(),
                recursive_read_only: false,
                image: None,
                image_sub_path: String::new(),
            }],
            true,
        )
        .unwrap_err();

    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(err.message().contains("host_path"));
}

#[test]
fn runtime_mounts_from_proto_preserves_mount_semantics_for_runtime() {
    let (_dir, service) = test_service_with_fake_runtime();
    let mounts = service
        .runtime_mounts_from_proto(
            &[crate::proto::runtime::v1::Mount {
                container_path: "/data".to_string(),
                host_path: "/host/data".to_string(),
                readonly: true,
                selinux_relabel: true,
                propagation: crate::proto::runtime::v1::MountPropagation::PropagationHostToContainer
                    as i32,
                uid_mappings: vec![crate::proto::runtime::v1::IdMapping {
                    host_id: 1000,
                    container_id: 0,
                    length: 1,
                }],
                gid_mappings: vec![crate::proto::runtime::v1::IdMapping {
                    host_id: 2000,
                    container_id: 0,
                    length: 1,
                }],
                recursive_read_only: false,
                image: None,
                image_sub_path: String::new(),
            }],
            true,
        )
        .unwrap();

    assert_eq!(mounts.len(), 1);
    assert_eq!(mounts[0].source, PathBuf::from("/host/data"));
    assert_eq!(mounts[0].destination, PathBuf::from("/data"));
    assert!(mounts[0].read_only);
    assert_eq!(
        mounts[0].missing_source_policy,
        crate::runtime::MissingMountSourcePolicy::CreateDirectory
    );
    assert!(mounts[0].selinux_relabel);
    assert_eq!(
        mounts[0].propagation,
        crate::runtime::MountPropagationMode::HostToContainer
    );
    assert_eq!(mounts[0].uid_mappings.len(), 1);
    assert_eq!(mounts[0].gid_mappings.len(), 1);
}

#[test]
fn runtime_mounts_from_proto_rejects_recursive_read_only_with_non_private_propagation() {
    let (_dir, service) = test_service_with_fake_runtime();
    let err = service
        .runtime_mounts_from_proto(
            &[crate::proto::runtime::v1::Mount {
                container_path: "/data".to_string(),
                host_path: "/host/data".to_string(),
                readonly: true,
                selinux_relabel: false,
                propagation: crate::proto::runtime::v1::MountPropagation::PropagationHostToContainer
                    as i32,
                uid_mappings: Vec::new(),
                gid_mappings: Vec::new(),
                recursive_read_only: true,
                image: None,
                image_sub_path: String::new(),
            }],
            true,
        )
        .unwrap_err();

    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(err.message().contains("propagation"));
}

#[test]
fn runtime_mounts_from_proto_keeps_restore_mount_sources_strict() {
    let (_dir, service) = test_service_with_fake_runtime();
    let mounts = service
        .runtime_mounts_from_proto(
            &[crate::proto::runtime::v1::Mount {
                container_path: "/data".to_string(),
                host_path: "/host/data".to_string(),
                readonly: false,
                selinux_relabel: false,
                propagation: crate::proto::runtime::v1::MountPropagation::PropagationPrivate as i32,
                uid_mappings: Vec::new(),
                gid_mappings: Vec::new(),
                recursive_read_only: false,
                image: None,
                image_sub_path: String::new(),
            }],
            false,
        )
        .unwrap();

    assert_eq!(
        mounts[0].missing_source_policy,
        crate::runtime::MissingMountSourcePolicy::Reject
    );
}

#[test]
fn runtime_mount_validation_reports_rro_unsupported_as_failed_precondition() {
    let (dir, service) = test_service_with_fake_runtime();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();
    let runtime = service.runtime.runtime_for_handler("runc").unwrap();
    let config = test_runtime_container_config_with_mounts(vec![MountConfig {
        source,
        destination: PathBuf::from("/data"),
        read_only: true,
        missing_source_policy: crate::runtime::MissingMountSourcePolicy::Reject,
        selinux_relabel: false,
        propagation: crate::runtime::MountPropagationMode::Private,
        recursive_read_only: true,
        uid_mappings: Vec::new(),
        gid_mappings: Vec::new(),
        requested_image: None,
        image_sub_path: None,
    }]);

    let err = runtime
        .runtime_context()
        .validate_mount_requests(&config)
        .unwrap_err();
    let status = err.to_status();
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert!(status.message().contains("recursive_read_only"));
}

#[test]
fn runtime_mount_validation_reports_selinux_relabel_without_label() {
    let (dir, service) = test_service_with_fake_runtime();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();
    let runtime = service.runtime.runtime_for_handler("runc").unwrap();
    let config = test_runtime_container_config_with_mounts(vec![MountConfig {
        source,
        destination: PathBuf::from("/data"),
        read_only: false,
        missing_source_policy: crate::runtime::MissingMountSourcePolicy::Reject,
        selinux_relabel: true,
        propagation: crate::runtime::MountPropagationMode::Private,
        recursive_read_only: false,
        uid_mappings: Vec::new(),
        gid_mappings: Vec::new(),
        requested_image: None,
        image_sub_path: None,
    }]);

    let err = runtime
        .runtime_context()
        .validate_mount_requests(&config)
        .unwrap_err();
    let status = err.to_status();
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert!(status.message().contains("SELinux"));
}

#[test]
fn runtime_mount_validation_reports_missing_source_as_failed_precondition() {
    let (dir, service) = test_service_with_fake_runtime();
    let source = dir.path().join("missing");
    let runtime = service.runtime.runtime_for_handler("runc").unwrap();
    let config = test_runtime_container_config_with_mounts(vec![MountConfig {
        source: source.clone(),
        destination: PathBuf::from("/data"),
        read_only: false,
        missing_source_policy: crate::runtime::MissingMountSourcePolicy::Reject,
        selinux_relabel: false,
        propagation: crate::runtime::MountPropagationMode::Private,
        recursive_read_only: false,
        uid_mappings: Vec::new(),
        gid_mappings: Vec::new(),
        requested_image: None,
        image_sub_path: None,
    }]);

    let err = runtime
        .runtime_context()
        .validate_mount_requests(&config)
        .unwrap_err();
    let status = err.to_status();
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert!(status.message().contains(source.to_string_lossy().as_ref()));
}
