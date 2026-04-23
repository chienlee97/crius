use super::*;
use serde_json::json;

impl RuntimeServiceImpl {
    pub(super) fn stored_mounts_to_proto(
        mounts: &[StoredMount],
    ) -> Vec<crate::proto::runtime::v1::Mount> {
        mounts
            .iter()
            .map(|mount| crate::proto::runtime::v1::Mount {
                container_path: mount.container_path.clone(),
                host_path: mount.host_path.clone(),
                readonly: mount.readonly,
                selinux_relabel: mount.selinux_relabel,
                propagation: mount.propagation,
                uid_mappings: Vec::new(),
                gid_mappings: Vec::new(),
            })
            .collect()
    }

    pub(super) fn build_container_status_snapshot(
        container: &Container,
        runtime_state: i32,
    ) -> CriContainerStatus {
        let container_state = Self::read_internal_state::<StoredContainerState>(
            &container.annotations,
            INTERNAL_CONTAINER_STATE_KEY,
        );
        let started_at = container_state
            .as_ref()
            .and_then(|state| state.started_at)
            .unwrap_or_default();
        let finished_at = container_state
            .as_ref()
            .and_then(|state| state.finished_at)
            .unwrap_or_default();
        let exit_code = container_state
            .as_ref()
            .and_then(|state| state.exit_code)
            .unwrap_or_default();
        let (reason, message) = Self::container_reason_message(runtime_state, exit_code);
        let mounts = Self::stored_mounts_to_proto(
            container_state
                .as_ref()
                .map(|state| state.mounts.as_slice())
                .unwrap_or(&[]),
        );

        CriContainerStatus {
            id: container.id.clone(),
            metadata: Some(container.metadata.clone().unwrap_or(ContainerMetadata {
                name: container.id.clone(),
                attempt: 1,
            })),
            state: runtime_state,
            created_at: Self::normalize_timestamp_nanos(container.created_at),
            started_at: Self::normalize_timestamp_nanos(started_at),
            finished_at: Self::normalize_timestamp_nanos(finished_at),
            exit_code,
            image: Some(container.image.clone().unwrap_or(ImageSpec {
                image: container.image_ref.clone(),
                ..Default::default()
            })),
            image_ref: container.image_ref.clone(),
            reason,
            message,
            labels: container.labels.clone(),
            annotations: Self::external_annotations(&container.annotations),
            mounts,
            log_path: container_state
                .as_ref()
                .and_then(|state| state.log_path.clone())
                .unwrap_or_default(),
            resources: container_state.and_then(|state| {
                state.linux_resources.map(|linux| ContainerResources {
                    linux: Some(linux.to_proto()),
                    windows: None,
                })
            }),
        }
    }

    pub(super) fn build_pod_sandbox_status_snapshot(
        &self,
        pod_sandbox: &crate::proto::runtime::v1::PodSandbox,
    ) -> PodSandboxStatus {
        Self::build_pod_sandbox_status_snapshot_with_config(&self.config, pod_sandbox)
    }

    pub(super) fn encode_info_payload(
        value: serde_json::Value,
    ) -> Result<HashMap<String, String>, Status> {
        let encoded = serde_json::to_string(&value)
            .map_err(|e| Status::internal(format!("Failed to encode info payload: {}", e)))?;
        Ok(HashMap::from([("info".to_string(), encoded)]))
    }

    pub(super) fn runtime_spec_snapshot(&self, container_id: &str) -> Option<serde_json::Value> {
        let config_path = self.checkpoint_config_path(container_id);
        std::fs::read(&config_path)
            .ok()
            .and_then(|raw| serde_json::from_slice::<serde_json::Value>(&raw).ok())
    }

    pub(super) async fn build_container_verbose_info(
        &self,
        container: &Container,
        runtime_state: i32,
    ) -> Result<HashMap<String, String>, Status> {
        let container_state = Self::read_internal_state::<StoredContainerState>(
            &container.annotations,
            INTERNAL_CONTAINER_STATE_KEY,
        );
        let runtime_spec = self.runtime_spec_snapshot(&container.id);
        let payload = json!({
            "id": container.id.clone(),
            "sandboxID": container.pod_sandbox_id.clone(),
            "podSandboxId": container.pod_sandbox_id.clone(),
            "runtimeState": Self::runtime_state_name(runtime_state),
            "pid": self.runtime_container_pid_checked(&container.id).await,
            "runtimeSpec": runtime_spec,
            "privileged": container_state.as_ref().map(|state| state.privileged).unwrap_or(false),
            "logPath": container_state.as_ref().and_then(|state| state.log_path.clone()),
            "tty": container_state.as_ref().map(|state| state.tty).unwrap_or(false),
            "stdin": container_state.as_ref().map(|state| state.stdin).unwrap_or(false),
            "stdinOnce": container_state.as_ref().map(|state| state.stdin_once).unwrap_or(false),
            "readonlyRootfs": container_state
                .as_ref()
                .map(|state| state.readonly_rootfs)
                .unwrap_or(false),
            "networkNamespacePath": container_state
                .as_ref()
                .and_then(|state| state.network_namespace_path.clone()),
            "cgroupParent": container_state.as_ref().and_then(|state| state.cgroup_parent.clone()),
            "user": container_state.as_ref().and_then(|state| state.run_as_user.clone()),
            "runAsGroup": container_state.as_ref().and_then(|state| state.run_as_group),
            "supplementalGroups": container_state
                .as_ref()
                .map(|state| state.supplemental_groups.clone())
                .unwrap_or_default(),
            "apparmorProfile": container_state
                .as_ref()
                .and_then(|state| state.apparmor_profile.clone()),
            "noNewPrivileges": container_state
                .as_ref()
                .and_then(|state| state.no_new_privileges),
            "mounts": container_state
                .as_ref()
                .map(|state| {
                    state.mounts.iter().map(|mount| {
                        json!({
                            "containerPath": mount.container_path.clone(),
                            "hostPath": mount.host_path.clone(),
                            "readonly": mount.readonly,
                            "selinuxRelabel": mount.selinux_relabel,
                            "propagation": mount.propagation,
                        })
                    }).collect::<Vec<_>>()
                })
                .unwrap_or_default(),
        });
        Self::encode_info_payload(payload)
    }

    pub(super) async fn build_pod_verbose_info(
        &self,
        pod_sandbox: &crate::proto::runtime::v1::PodSandbox,
    ) -> Result<HashMap<String, String>, Status> {
        let pod_state = Self::read_internal_state::<StoredPodState>(
            &pod_sandbox.annotations,
            INTERNAL_POD_STATE_KEY,
        );
        let pause_container_id = pod_state
            .as_ref()
            .and_then(|state| state.pause_container_id.clone());
        let pause_pid = if let Some(pause_container_id) = pause_container_id.as_deref() {
            self.runtime_container_pid_checked(pause_container_id).await
        } else {
            None
        };
        let runtime_spec = pause_container_id
            .as_deref()
            .and_then(|pause_container_id| self.runtime_spec_snapshot(pause_container_id));
        let pause_image = pod_sandbox
            .annotations
            .get("io.kubernetes.cri.sandbox-image")
            .filter(|image| !image.trim().is_empty())
            .cloned()
            .unwrap_or_else(|| self.config.pause_image.clone());
        let payload = json!({
            "id": pod_sandbox.id.clone(),
            "image": pause_image,
            "pid": pause_pid,
            "runtimeSpec": runtime_spec,
            "runtimeHandler": pod_sandbox.runtime_handler.clone(),
            "runtimePodCIDR": pod_state.as_ref().and_then(|state| state.runtime_pod_cidr.clone()),
            "netnsPath": pod_state.as_ref().and_then(|state| state.netns_path.clone()),
            "logDirectory": pod_state.as_ref().and_then(|state| state.log_directory.clone()),
            "pauseContainerId": pause_container_id,
            "ip": pod_state.as_ref().and_then(|state| state.ip.clone()),
            "additionalIPs": pod_state
                .as_ref()
                .map(|state| state.additional_ips.clone())
                .unwrap_or_default(),
            "cgroupParent": pod_state.as_ref().and_then(|state| state.cgroup_parent.clone()),
            "sysctls": pod_state
                .as_ref()
                .map(|state| state.sysctls.clone())
                .unwrap_or_default(),
            "privileged": pod_state.as_ref().map(|state| state.privileged).unwrap_or(false),
            "readonlyRootfs": pod_state
                .as_ref()
                .map(|state| state.readonly_rootfs)
                .unwrap_or(false),
            "selinuxLabel": pod_state.as_ref().and_then(|state| state.selinux_label.clone()),
            "seccompProfile": pod_state.as_ref().map(|state| json!({
                "profileType": state.seccomp_profile.as_ref().map(|profile| profile.profile_type),
                "localhostRef": state
                    .seccomp_profile
                    .as_ref()
                    .map(|profile| profile.localhost_ref.clone())
                    .unwrap_or_default(),
            })),
        });
        Self::encode_info_payload(payload)
    }

    pub(super) fn build_pod_sandbox_status_snapshot_with_config(
        config: &RuntimeConfig,
        pod_sandbox: &crate::proto::runtime::v1::PodSandbox,
    ) -> PodSandboxStatus {
        let pod_state = Self::read_internal_state::<StoredPodState>(
            &pod_sandbox.annotations,
            INTERNAL_POD_STATE_KEY,
        );

        PodSandboxStatus {
            id: pod_sandbox.id.clone(),
            metadata: Some(pod_sandbox.metadata.clone().unwrap_or(PodSandboxMetadata {
                name: pod_sandbox.id.clone(),
                uid: pod_sandbox.id.clone(),
                namespace: "default".to_string(),
                attempt: 1,
            })),
            state: pod_sandbox.state,
            created_at: Self::normalize_timestamp_nanos(pod_sandbox.created_at),
            network: Self::pod_network_status_from_state(pod_state.as_ref()),
            linux: Self::pod_linux_status_from_state(pod_state.as_ref()),
            labels: pod_sandbox.labels.clone(),
            annotations: Self::external_annotations(&pod_sandbox.annotations),
            runtime_handler: if pod_sandbox.runtime_handler.is_empty() {
                let restored = pod_state
                    .as_ref()
                    .map(|state| state.runtime_handler.clone())
                    .filter(|handler| !handler.is_empty())
                    .unwrap_or_else(|| config.runtime.clone());
                if config
                    .runtime_handlers
                    .iter()
                    .any(|handler| handler == &restored)
                {
                    restored
                } else {
                    config.runtime.clone()
                }
            } else {
                pod_sandbox.runtime_handler.clone()
            },
        }
    }
}
