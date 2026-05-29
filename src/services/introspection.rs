use std::path::Path;

use serde_json::{json, Value};

use crate::config::NriConfig;
use crate::image::content_store::ContentTransferStatus;
use crate::image::{PullCgroupEffectiveConfig, PullCgroupScopeRecord};
use crate::rootless::EffectiveRootlessConfig;
use crate::security::HostCapabilityProbe;
use crate::server::{RuntimeConfig, RuntimeReloadState, RuntimeReloadableConfig};
use crate::state::LedgerCheckReport;
use crate::storage::{ContentGcBlocker, ContentGcCandidate};

use super::health::RecoveryLedgerHealthSummary;

#[derive(Debug, Clone, Default)]
pub struct IntrospectionService;

pub struct RecoveryStatusInput<'a, T> {
    pub config: &'a RuntimeConfig,
    pub last_startup_clean_shutdown: Option<bool>,
    pub last_startup_detected_reboot: Option<bool>,
    pub last_startup_detected_upgrade: Option<bool>,
    pub last_startup_attempted_repair: Option<bool>,
    pub last_startup_repair_succeeded: Option<bool>,
    pub last_recovery_result: Option<&'a T>,
    pub ledger_summary: Option<&'a RecoveryLedgerHealthSummary>,
    pub ledger_summary_error: Option<&'a str>,
    pub ledger_check_report: Option<&'a LedgerCheckReport>,
    pub ledger_check_error: Option<&'a str>,
}

impl IntrospectionService {
    pub fn runtime_backend(&self, config: &RuntimeConfig) -> Value {
        json!({
            "defaultHandler": config.runtime,
            "runtimePath": config.runtime_path.display().to_string(),
            "runtimeRoot": config.runtime_root.display().to_string(),
            "handlers": config.runtime_handlers,
            "handlerBackends": config
                .runtime_configs
                .iter()
                .map(|(handler, handler_config)| {
                    (
                        handler.clone(),
                        json!({
                            "backend": handler_config.backend,
                            "backendOptions": handler_config.backend_options,
                            "runtimePath": handler_config.runtime_path,
                            "runtimeRoot": handler_config.runtime_root,
                            "snapshotter": handler_config.snapshotter,
                        }),
                    )
                })
                .collect::<serde_json::Map<String, Value>>(),
        })
    }

    pub fn snapshot_backend(&self, config: &RuntimeConfig) -> Value {
        let requested_default = config
            .runtime_configs
            .get(&config.runtime)
            .map(|runtime| runtime.snapshotter.as_str())
            .unwrap_or("internal-overlay-untar");
        let default_probe = crate::image::snapshotter::probe_configured_snapshotter(
            requested_default,
            &config.image_external_snapshotters,
        );
        json!({
            "contentStore": {
                "root": config.image_root.display().to_string(),
                "driver": config.image_driver,
                "storageOptions": config.image_storage_options,
                "effectiveOptions": effective_overlay_storage_options(&config.image_storage_options),
                "additionalArtifactStores": config
                    .image_additional_artifact_stores
                    .iter()
                    .map(|path| path.display().to_string())
                    .collect::<Vec<_>>(),
            },
            "snapshotter": {
                "default": default_probe.name,
                "resolved": default_probe.resolved_name(),
                "type": default_probe.snapshotter_type,
                "endpoint": default_probe.endpoint,
                "path": default_probe.path,
                "capabilities": default_probe.capabilities,
                "available": default_probe.available,
                "unavailableReason": default_probe.unavailable_reason,
                "configured": crate::image::snapshotter::probe_all_configured_snapshotters(
                    &config.image_external_snapshotters
                ),
                "runtimeSnapshotterOverrideSupported": true,
                "externalSnapshotterSupported": !config.image_external_snapshotters.is_empty(),
            },
        })
    }

    pub fn image_layout(&self, config: &RuntimeConfig) -> Value {
        json!({
            "mode": "single-store-root",
            "root": config.image_root.display().to_string(),
            "imageRecordPathPattern": config
                .image_root
                .join("images")
                .join("<imageID>")
                .display()
                .to_string(),
            "separateImageStoreSupported": false
        })
    }

    pub fn image_transfers(&self, status: &ContentTransferStatus) -> Value {
        json!({
            "providerModel": {
                "localBlobStore": true,
                "remoteProviders": [
                    "registry",
                    "test"
                ],
                "transferLifecycle": [
                    "running",
                    "succeeded",
                    "failed",
                    "interrupted"
                ]
            },
            "active": status.active,
            "recent": status.recent,
        })
    }

    pub fn content_gc(&self, candidates: &[ContentGcCandidate], error: Option<&str>) -> Value {
        let candidate_values = candidates
            .iter()
            .map(|candidate| {
                let blockers = candidate
                    .blockers
                    .iter()
                    .map(|blocker| match blocker {
                        ContentGcBlocker::ContentRef {
                            owner_kind,
                            owner_id,
                            ref_kind,
                        } => json!({
                            "reason": "referenced",
                            "ownerKind": owner_kind,
                            "ownerId": owner_id,
                            "refKind": ref_kind,
                        }),
                        ContentGcBlocker::ActiveTransfer {
                            transfer_id,
                            source,
                        } => json!({
                            "reason": "activeTransfer",
                            "transferId": transfer_id,
                            "source": source,
                        }),
                    })
                    .collect::<Vec<_>>();
                json!({
                    "digest": candidate.blob.digest,
                    "mediaType": candidate.blob.media_type,
                    "size": candidate.blob.size,
                    "relativePath": candidate.blob.relative_path,
                    "createdAt": candidate.blob.created_at,
                    "lastUsedAt": candidate.blob.last_used_at,
                    "blocked": !candidate.blockers.is_empty(),
                    "blockers": blockers,
                })
            })
            .collect::<Vec<_>>();
        let reclaimable_bytes: u64 = candidates
            .iter()
            .filter(|candidate| candidate.blockers.is_empty())
            .map(|candidate| candidate.blob.size)
            .sum();
        json!({
            "dryRunSupported": true,
            "deleteSupported": false,
            "candidateCount": candidates.len(),
            "reclaimableCount": candidates
                .iter()
                .filter(|candidate| candidate.blockers.is_empty())
                .count(),
            "blockedCount": candidates
                .iter()
                .filter(|candidate| !candidate.blockers.is_empty())
                .count(),
            "reclaimableBytes": reclaimable_bytes,
            "candidates": candidate_values,
            "recentResult": serde_json::Value::Null,
            "error": error,
        })
    }

    pub fn image_snapshot_model(&self, config: &RuntimeConfig) -> Value {
        let requested_default = config
            .runtime_configs
            .get(&config.runtime)
            .map(|runtime| runtime.snapshotter.as_str())
            .unwrap_or("internal-overlay-untar");
        let default_probe = crate::image::snapshotter::probe_configured_snapshotter(
            requested_default,
            &config.image_external_snapshotters,
        );
        json!({
            "snapshotter": default_probe.name,
            "resolvedSnapshotter": default_probe.resolved_name(),
            "snapshotterType": default_probe.snapshotter_type,
            "snapshotterCapabilities": default_probe.capabilities,
            "snapshotterAvailable": default_probe.available,
            "snapshotterUnavailableReason": default_probe.unavailable_reason,
            "storageDriver": config.image_driver,
            "storageOptions": config.image_storage_options,
            "externalSnapshotterSupported": !config.image_external_snapshotters.is_empty(),
            "runtimeSnapshotterOverrideSupported": true,
            "snapshotAnnotationPassthrough": false,
            "discardUnpackedLayers": false,
            "pullOptionPassthrough": false
        })
    }

    pub fn snapshot_stats_collection(&self, config: &RuntimeConfig) -> Value {
        json!({
            "strategy": "on-demand-rootfs-walk",
            "backgroundCollector": false,
            "containerStatsPeriodSeconds": config.stats_collection_period,
            "podSandboxMetricsPeriodSeconds": config.pod_sandbox_metrics_collection_period
        })
    }

    pub fn rootless(&self, rootless: &EffectiveRootlessConfig) -> Value {
        let network_helper_path = match rootless.network_mode {
            crate::rootless::NetworkMode::Slirp4netns => {
                Some(rootless.slirp4netns_path.display().to_string())
            }
            crate::rootless::NetworkMode::Pasta => Some(rootless.pasta_path.display().to_string()),
            crate::rootless::NetworkMode::None | crate::rootless::NetworkMode::Rootlesskit => None,
        };
        json!({
            "enabled": rootless.enabled,
            "currentUid": rootless.current_uid,
            "currentGid": rootless.current_gid,
            "inUserNamespace": rootless.in_user_namespace,
            "xdgRuntimeDir": rootless.xdg_runtime_dir.display().to_string(),
            "xdgDataHome": rootless.xdg_data_home.display().to_string(),
            "storageRoot": rootless.storage_root.display().to_string(),
            "runtimeRoot": rootless.runtime_root.display().to_string(),
            "netnsDir": rootless.netns_dir.display().to_string(),
            "networkMode": rootless.network_mode.as_str(),
            "networkModeSupported": rootless.network_mode.is_supported(),
            "networkModeReason": rootless.network_mode.support_reason(),
            "networkHelperPath": network_helper_path,
            "userNamespace": rootless.user_namespace_status(),
            "limitations": rootless.limitations(),
            "useFuseOverlayfs": rootless.use_fuse_overlayfs,
            "disableCgroup": rootless.disable_cgroup,
            "tolerateMissingHugetlbController": rootless.tolerate_missing_hugetlb_controller,
            "slirp4netnsPath": rootless.slirp4netns_path.display().to_string(),
            "pastaPath": rootless.pasta_path.display().to_string(),
        })
    }

    pub fn runtime_feature_flags(&self, config: &RuntimeConfig) -> Value {
        json!({
            "exec": true,
            "execSync": true,
            "attach": true,
            "portForward": true,
            "containerStats": true,
            "podSandboxStats": true,
            "podSandboxMetrics": true,
            "containerEvents": false,
            "podLifecycleEvents": false,
            "reopenContainerLog": true,
            "updateContainerResources": !config.disable_cgroup,
            "checkpointContainer": config.enable_criu_support,
        })
    }

    pub fn security_availability(
        &self,
        seccomp_notifier_dir: &Path,
        seccomp_notifier_active_containers: Vec<String>,
        nri_config: &NriConfig,
    ) -> Value {
        let security = crate::security::SecurityManager::new();
        let capabilities = security.host_capability_report(
            &nri_config.cdi_spec_dirs,
            Some(&nri_config.blockio_config_path),
        );
        let degraded_capabilities = capabilities.degraded_capability_names();
        let degraded_reasons = capabilities.degraded_reasons();
        json!({
            "selinuxAvailable": security.is_selinux_available(),
            "apparmorAvailable": security.is_apparmor_available(),
            "seccompAvailable": security.is_seccomp_available(),
            "seccompNotifierSupported": security.is_seccomp_available(),
            "seccompNotifierBaseDir": seccomp_notifier_dir.display().to_string(),
            "seccompNotifierActiveContainers": seccomp_notifier_active_containers,
            "hostCapabilities": {
                "seccomp": host_capability_value(&capabilities.seccomp),
                "apparmor": host_capability_value(&capabilities.apparmor),
                "selinux": host_capability_value(&capabilities.selinux),
                "cdi": host_capability_value(&capabilities.cdi),
                "blockio": host_capability_value(&capabilities.blockio),
                "rdt": host_capability_value(&capabilities.rdt),
                "devices": host_capability_value(&capabilities.devices),
                "cgroup": host_capability_value(&capabilities.cgroup),
            },
            "degradedCapabilities": degraded_capabilities,
            "degradedReasons": degraded_reasons,
        })
    }

    pub fn recovery<T: serde::Serialize>(
        &self,
        ledger_summary: Option<&RecoveryLedgerHealthSummary>,
        ledger_summary_error: Option<&str>,
        ledger_check_report: Option<&LedgerCheckReport>,
        ledger_check_error: Option<&str>,
        last_recovery_result: Option<&T>,
    ) -> Value {
        json!({
            "ledgerSummary": ledger_summary,
            "ledgerSummaryError": ledger_summary_error,
            "ledgerCheck": ledger_check_report,
            "ledgerCheckError": ledger_check_error,
            "unhealthyObjectCount": ledger_summary
                .map(RecoveryLedgerHealthSummary::unhealthy_object_count)
                .unwrap_or_default(),
            "lastRecoveryResult": last_recovery_result,
        })
    }

    pub fn recovery_status<T: serde::Serialize>(&self, input: RecoveryStatusInput<'_, T>) -> Value {
        json!({
            "enabled": true,
            "startupReconcile": true,
            "eventReplayOnRecovery": false,
            "lastStartupWasCleanShutdown": input.last_startup_clean_shutdown,
            "lastStartupDetectedReboot": input.last_startup_detected_reboot,
            "lastStartupDetectedUpgrade": input.last_startup_detected_upgrade,
            "lastStartupAttemptedRepair": input.last_startup_attempted_repair,
            "lastStartupRepairSucceeded": input.last_startup_repair_succeeded,
            "lastRecoveryResult": input.last_recovery_result,
            "ledgerSummary": input.ledger_summary,
            "ledgerSummaryError": input.ledger_summary_error,
            "ledgerCheck": input.ledger_check_report,
            "ledgerCheckError": input.ledger_check_error,
            "internalWipe": input.config.internal_wipe,
            "internalRepair": input.config.internal_repair,
            "policy": {
                "startupInputs": [
                    "cleanShutdownFile",
                    "versionFile",
                    "versionFilePersist",
                    "internalWipe",
                    "internalRepair",
                ],
                "wipeTriggers": [
                    "unclean-shutdown",
                    "reboot",
                    "upgrade",
                ],
                "wipeScope": [
                    "orphanRuntimeBundles",
                    "orphanShimArtifacts",
                    "orphanAttachArtifacts",
                    "orphanPodWorkspaces",
                ],
                "repairTriggers": [
                    "unclean-shutdown",
                ],
                "repairScope": "sqlite-persistence-and-ledger",
                "repairActions": [
                    "integrity-check",
                    "reindex",
                    "vacuum",
                    "ledger-check",
                    "mark-missing-artifact-broken",
                    "mark-unreachable-shim-dead",
                    "delete-dangling-shim-record",
                ],
            },
        })
    }

    pub fn pull_cgroup(
        &self,
        effective: &PullCgroupEffectiveConfig,
        last_scope: Option<&PullCgroupScopeRecord>,
    ) -> Value {
        json!({
            "requestedValue": effective.configured,
            "effectiveMode": effective.mode,
            "enabled": effective.enabled,
            "rootlessDegraded": effective.rootless_degraded,
            "disableCgroupDegraded": effective.disable_cgroup_degraded,
            "cgroupDriver": effective.cgroup_driver,
            "lastError": last_scope.and_then(|scope| scope.error.as_deref()),
            "effective": effective,
            "lastScope": last_scope,
        })
    }

    pub fn runtime_handler_configs(
        &self,
        config: &RuntimeConfig,
        runtime_detected_features: serde_json::Map<String, Value>,
    ) -> Value {
        config
            .runtime_configs
            .iter()
            .map(|(handler, handler_config)| {
                (
                    handler.clone(),
                    json!({
                        "backend": handler_config.backend,
                        "backendOptions": handler_config.backend_options,
                        "runtimePath": handler_config.runtime_path,
                        "runtimeConfigPath": handler_config.runtime_config_path,
                        "runtimeRoot": handler_config.runtime_root,
                        "platformRuntimePaths": handler_config.platform_runtime_paths,
                        "monitorPath": handler_config.monitor_path,
                        "monitorCgroup": handler_config.monitor_cgroup,
                        "monitorEnv": handler_config.monitor_env,
                        "streamWebsockets": handler_config.stream_websockets,
                        "runtimeDetectedFeatures": runtime_detected_features
                            .get(handler)
                            .cloned()
                            .unwrap_or_else(|| json!({
                                "available": false,
                                "error": "runtime feature probe was not collected",
                            })),
                        "allowedAnnotations": handler_config.allowed_annotations,
                        "defaultAnnotations": handler_config.default_annotations,
                        "privilegedWithoutHostDevices": handler_config.privileged_without_host_devices,
                        "privilegedWithoutHostDevicesAllDevicesAllowed": handler_config
                            .privileged_without_host_devices_all_devices_allowed,
                        "containerCreateTimeoutSeconds": handler_config.container_create_timeout,
                        "snapshotter": handler_config.snapshotter,
                        "cniConfDir": config
                            .cni_config
                            .handler_config_dirs(handler)
                            .and_then(|dirs| dirs.first())
                            .map(|dir| dir.to_string_lossy().to_string()),
                        "cniMaxConfNum": config
                            .cni_config
                            .handler_max_conf_num(handler)
                            .unwrap_or(config.cni_config.max_conf_num()),
                    }),
                )
            })
            .collect::<serde_json::Map<String, Value>>()
            .into()
    }

    pub fn workloads(&self, config: &RuntimeConfig) -> Value {
        config
            .workloads
            .iter()
            .map(|(name, workload)| {
                (
                    name.clone(),
                    json!({
                        "activationAnnotation": workload.activation_annotation,
                        "annotationPrefix": workload.annotation_prefix,
                        "allowedAnnotations": workload.allowed_annotations,
                        "resources": {
                            "cpuShares": workload.resources.cpu_shares,
                            "cpuQuota": workload.resources.cpu_quota,
                            "cpuPeriod": workload.resources.cpu_period,
                            "cpusetCpus": workload.resources.cpuset_cpus,
                            "cpuLimit": workload.resources.cpu_limit,
                        },
                    }),
                )
            })
            .collect::<serde_json::Map<String, Value>>()
            .into()
    }

    pub fn cgroup_support(
        &self,
        config: &RuntimeConfig,
        active_version: &str,
        resource_classes: crate::security::resource_classes::ResourceClassSupport,
    ) -> Value {
        json!({
            "activeVersion": active_version,
            "resourceUpdateStrategy": "runtime-update-resources",
            "disableCgroup": config.disable_cgroup,
            "tolerateMissingHugetlbController": config.tolerate_missing_hugetlb_controller,
            "resourceClasses": {
                "blockio": {
                    "supported": resource_classes.blockio_supported,
                    "configPath": resource_classes
                        .blockio_config_path
                        .as_ref()
                        .map(|path| path.display().to_string())
                        .unwrap_or_default(),
                    "softFailure": "drop-class-when-config-missing",
                },
                "rdt": {
                    "supported": resource_classes.rdt_supported,
                    "resctrlPath": resource_classes.rdt_resctrl_path.display().to_string(),
                    "softFailure": "drop-class-when-resctrl-missing",
                },
            },
            "drivers": {
                "systemd": {
                    "supported": true,
                    "monitorCgroup": {
                        "default": "system.slice",
                        "acceptedValues": ["", "pod", "*.slice"],
                    },
                    "resourceUpdatePath": "runtime update --resources",
                },
                "cgroupfs": {
                    "supported": true,
                    "monitorCgroup": {
                        "default": "",
                        "acceptedValues": ["", "pod"],
                    },
                    "resourceUpdatePath": "runtime update --resources",
                },
            },
            "versions": {
                "v1": {
                    "supported": true,
                    "hierarchyMode": "legacy",
                    "hugetlbBehavior": if config.tolerate_missing_hugetlb_controller {
                        "best-effort"
                    } else {
                        "required"
                    },
                },
                "v2": {
                    "supported": true,
                    "hierarchyMode": "unified",
                    "hugetlbBehavior": if config.tolerate_missing_hugetlb_controller {
                        "best-effort"
                    } else {
                        "required"
                    },
                },
            },
        })
    }

    pub fn reload(
        &self,
        config_path: Option<&Path>,
        reloadable_config: &RuntimeReloadableConfig,
        reload_state: &RuntimeReloadState,
    ) -> Value {
        json!({
            "strategy": "config-file-watch-and-cni-watch",
            "signalReload": false,
            "configFileWatch": reload_state.config_file_watch,
            "configFilePath": config_path.map(|path| path.display().to_string()),
            "watcherActive": reload_state.watcher_active,
            "watcherStatus": reload_state.watcher_status,
            "watcherBackoffCount": reload_state.watcher_backoff_count,
            "watcherNextRetryUnixMillis": reload_state.watcher_next_retry_unix_millis,
            "watcherLastError": reload_state.watcher_last_error,
            "cniWatchDirs": reload_state.cni_watch_dirs,
            "reloadableFields": [
                "runtime.pause_image",
                "image.pinned_images",
                "image.registry_config_dir",
                "image.global_auth_file",
                "image.namespaced_auth_dir",
                "image.signature_policy",
                "image.signature_policy_dir",
                "image.decryption_keys_path",
                "image.decryption_decoder_path",
                "image.decryption_keyprovider_config",
                "security.seccomp_profile",
                "security.apparmor_default_profile",
                "network.config_dirs",
                "network.conf_template",
                "network.max_conf_num",
                "network.default_network_name",
            ],
            "current": {
                "pauseImage": reloadable_config.pause_image,
                "pinnedImages": reloadable_config.pinned_images,
                "registryConfigDir": reloadable_config.registry_config_dir.display().to_string(),
                "globalAuthFile": reloadable_config.global_auth_file.display().to_string(),
                "namespacedAuthDir": reloadable_config.namespaced_auth_dir.display().to_string(),
                "signaturePolicy": reloadable_config.signature_policy.display().to_string(),
                "signaturePolicyDir": reloadable_config.signature_policy_dir.display().to_string(),
                "decryptionKeysPath": reloadable_config.decryption_keys_path.display().to_string(),
                "decryptionDecoderPath": reloadable_config.decryption_decoder_path,
                "decryptionKeyproviderConfig": reloadable_config
                    .decryption_keyprovider_config
                    .display()
                    .to_string(),
                "seccompProfile": reloadable_config.seccomp_profile.display().to_string(),
                "apparmorDefaultProfile": reloadable_config.apparmor_default_profile,
                "cniConfigDirs": reloadable_config
                    .cni_config_dirs
                    .iter()
                    .map(|dir| dir.display().to_string())
                    .collect::<Vec<_>>(),
                "cniConfTemplate": reloadable_config
                    .cni_conf_template
                    .as_ref()
                    .map(|path| path.display().to_string()),
                "cniMaxConfNum": reloadable_config.cni_max_conf_num,
                "cniDefaultNetworkName": reloadable_config.cni_default_network_name,
            },
            "lastReloadAtUnixMillis": reload_state.last_reload_at_unix_millis,
            "lastReloadSource": reload_state.last_reload_source,
            "lastReloadFields": reload_state.last_reload_fields,
            "lastReloadError": reload_state.last_reload_error,
            "lastCniWatchAtUnixMillis": reload_state.last_cni_watch_at_unix_millis,
            "lastCniWatchError": reload_state.last_cni_watch_error,
            "runtimeConfigApiOnly": [
                "UpdateRuntimeConfig.network_config.pod_cidr"
            ],
        })
    }

    pub fn feature_flags(
        &self,
        runtime_features: Value,
        security_availability: Value,
        nri_config: &NriConfig,
        resource_classes: crate::security::resource_classes::ResourceClassSupport,
    ) -> Value {
        json!({
            "runtime": runtime_features,
            "security": security_availability,
            "nri": {
                "enabled": nri_config.enable,
                "cdi": nri_config.enable_cdi,
            },
            "resourceClasses": {
                "blockio": {
                    "supported": resource_classes.blockio_supported,
                    "configPath": resource_classes
                        .blockio_config_path
                        .as_ref()
                        .map(|path| path.display().to_string()),
                },
                "rdt": {
                    "supported": resource_classes.rdt_supported,
                    "resctrlPath": resource_classes.rdt_resctrl_path.display().to_string(),
                },
            },
        })
    }
}

fn host_capability_value(probe: &HostCapabilityProbe) -> Value {
    json!({
        "available": probe.state == crate::security::HostCapabilityState::Available,
        "state": probe.state,
        "reason": probe.reason,
    })
}

fn effective_overlay_storage_options(options: &[String]) -> Value {
    let mut mount_program = None;
    let mut ignore_chown_errors = false;
    for option in options {
        let Some((key, value)) = option.split_once('=') else {
            continue;
        };
        match key.trim() {
            "overlay.mount_program" => mount_program = Some(value.trim().to_string()),
            "overlay.ignore_chown_errors" => {
                ignore_chown_errors = matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                );
            }
            _ => {}
        }
    }
    json!({
        "mountProgram": mount_program,
        "ignoreChownErrors": ignore_chown_errors,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;

    use super::*;
    use crate::config::CgroupDriverConfig;
    use crate::image::{PullCgroupEffectiveConfig, PullCgroupMode, PullCgroupScopeRecord};
    use crate::server::{RuntimeReloadState, RuntimeReloadableConfig};

    #[test]
    fn rootless_report_exposes_effective_paths() {
        let service = IntrospectionService;
        let rootless = EffectiveRootlessConfig {
            enabled: true,
            current_uid: 1000,
            current_gid: 1000,
            in_user_namespace: true,
            xdg_runtime_dir: PathBuf::from("/run/user/1000"),
            xdg_data_home: PathBuf::from("/home/user/.local/share"),
            storage_root: PathBuf::from("/home/user/.local/share/crius"),
            runtime_root: PathBuf::from("/run/user/1000/crius"),
            netns_dir: PathBuf::from("/run/user/1000/netns"),
            network_mode: crate::rootless::NetworkMode::Slirp4netns,
            use_fuse_overlayfs: true,
            disable_cgroup: true,
            tolerate_missing_hugetlb_controller: true,
            slirp4netns_path: PathBuf::from("/usr/bin/slirp4netns"),
            pasta_path: PathBuf::from("/usr/bin/pasta"),
        };

        let value = service.rootless(&rootless);
        assert_eq!(value["enabled"], true);
        assert_eq!(value["networkMode"], "slirp4netns");
        assert_eq!(value["networkModeSupported"], true);
        assert_eq!(value["networkModeReason"], "RootlessSlirp4netnsSupported");
        assert_eq!(value["networkHelperPath"], "/usr/bin/slirp4netns");
        assert_eq!(
            value["userNamespace"]["reason"],
            "RootlessUserNamespaceReady"
        );
        assert_eq!(value["userNamespace"]["ready"], true);
        assert_eq!(
            value["limitations"]["cgroup"]["reason"],
            "RootlessCgroupDisabled"
        );
        assert_eq!(value["limitations"]["devices"]["degraded"], true);
        assert_eq!(value["storageRoot"], "/home/user/.local/share/crius");
    }

    #[test]
    fn rootless_report_marks_rootlesskit_unsupported() {
        let service = IntrospectionService;
        let mut rootless = EffectiveRootlessConfig::disabled();
        rootless.enabled = true;
        rootless.network_mode = crate::rootless::NetworkMode::Rootlesskit;

        let value = service.rootless(&rootless);

        assert_eq!(value["networkMode"], "rootlesskit");
        assert_eq!(value["networkModeSupported"], false);
        assert_eq!(value["networkModeReason"], "RootlesskitUnsupported");
        assert!(value["networkHelperPath"].is_null());
    }

    #[test]
    fn reload_report_contains_watcher_and_last_error() {
        let service = IntrospectionService;
        let reloadable = RuntimeReloadableConfig {
            pause_image: "pause".to_string(),
            pinned_images: Vec::new(),
            registry_config_dir: PathBuf::from("/etc/containers/registries.conf.d"),
            global_auth_file: PathBuf::new(),
            namespaced_auth_dir: PathBuf::new(),
            signature_policy: PathBuf::new(),
            signature_policy_dir: PathBuf::new(),
            decryption_keys_path: PathBuf::new(),
            decryption_decoder_path: String::new(),
            decryption_keyprovider_config: PathBuf::new(),
            seccomp_profile: PathBuf::new(),
            apparmor_default_profile: "crius-default".to_string(),
            cni_config_dirs: Vec::new(),
            cni_conf_template: None,
            cni_max_conf_num: 1,
            cni_default_network_name: Some("podman".to_string()),
        };
        let state = RuntimeReloadState {
            watcher_active: true,
            last_reload_error: Some("failed".to_string()),
            ..Default::default()
        };

        let value = service.reload(
            Some(Path::new("/etc/crius/config.toml")),
            &reloadable,
            &state,
        );
        assert_eq!(value["watcherActive"], true);
        assert_eq!(value["watcherStatus"], "stopped");
        assert_eq!(value["watcherBackoffCount"], 0);
        assert_eq!(value["lastReloadError"], "failed");
        assert_eq!(value["configFilePath"], "/etc/crius/config.toml");
    }

    #[test]
    fn pull_cgroup_report_exposes_effective_config_and_last_error() {
        let service = IntrospectionService;
        let effective = PullCgroupEffectiveConfig {
            configured: "pod".to_string(),
            mode: PullCgroupMode::Pod,
            enabled: true,
            rootless_degraded: false,
            disable_cgroup_degraded: false,
            cgroup_driver: CgroupDriverConfig::Cgroupfs,
        };
        let last_scope = PullCgroupScopeRecord {
            configured: "pod".to_string(),
            mode: PullCgroupMode::Pod,
            effective_path: Some("/sys/fs/cgroup/kubepods/pod1".to_string()),
            entered: false,
            active: false,
            restored: false,
            error: Some("failed to create pull cgroup".to_string()),
            at_unix_millis: 42,
            started_at_unix_millis: 42,
            ended_at_unix_millis: Some(42),
        };

        let value = service.pull_cgroup(&effective, Some(&last_scope));

        assert_eq!(value["requestedValue"], "pod");
        assert_eq!(value["effectiveMode"], "pod");
        assert_eq!(value["enabled"], true);
        assert_eq!(value["cgroupDriver"], "cgroupfs");
        assert_eq!(value["lastError"], "failed to create pull cgroup");
        assert_eq!(value["effective"]["mode"], "pod");
        assert_eq!(value["lastScope"]["entered"], false);
    }

    #[test]
    fn runtime_handler_configs_include_runtime_probe_and_cni_overrides() {
        let service = IntrospectionService;
        let mut config = RuntimeConfig {
            config_path: None,
            ..Default::default()
        };
        config.runtime_configs.insert(
            "kata".to_string(),
            crate::config::ResolvedRuntimeHandlerConfig {
                backend: "vm".to_string(),
                backend_options: HashMap::from([(
                    "sandbox_type".to_string(),
                    "podsandbox".to_string(),
                )]),
                runtime_path: "/usr/bin/kata-runtime".to_string(),
                runtime_root: "/run/kata".to_string(),
                stream_websockets: true,
                snapshotter: "internal-cached-rootfs".to_string(),
                ..Default::default()
            },
        );
        config
            .cni_config
            .set_handler_config_dirs("kata", vec![PathBuf::from("/etc/cni/kata.d")]);
        config.cni_config.set_handler_max_conf_num("kata", 2);

        let value = service.runtime_handler_configs(
            &config,
            serde_json::Map::from_iter([(
                "kata".to_string(),
                json!({
                    "available": true,
                    "shimRpc": true,
                }),
            )]),
        );

        assert_eq!(value["kata"]["backend"], "vm");
        assert_eq!(
            value["kata"]["backendOptions"]["sandbox_type"],
            "podsandbox"
        );
        assert_eq!(value["kata"]["runtimeDetectedFeatures"]["available"], true);
        assert_eq!(value["kata"]["cniConfDir"], "/etc/cni/kata.d");
        assert_eq!(value["kata"]["cniMaxConfNum"], 2);
        assert_eq!(value["kata"]["streamWebsockets"], true);
        assert_eq!(value["kata"]["snapshotter"], "internal-cached-rootfs");
    }

    #[test]
    fn snapshot_backend_reports_external_snapshotter_probe() {
        let service = IntrospectionService;
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("stargz.sock");
        std::fs::write(&socket, "").unwrap();
        let mut config = RuntimeConfig {
            config_path: None,
            image_external_snapshotters: HashMap::from([(
                "stargz".to_string(),
                crate::config::ExternalSnapshotterConfig {
                    snapshotter_type: "proxy".to_string(),
                    endpoint: format!("unix://{}", socket.display()),
                    capabilities: vec!["mount-spec".to_string()],
                    ..Default::default()
                },
            )]),
            ..Default::default()
        };
        if let Some(default_runtime) = config.runtime_configs.get_mut("runc") {
            default_runtime.snapshotter = "stargz".to_string();
        }

        let value = service.snapshot_backend(&config);

        assert_eq!(value["snapshotter"]["default"], "stargz");
        assert_eq!(value["snapshotter"]["resolved"], "stargz");
        assert_eq!(value["snapshotter"]["type"], "proxy");
        assert_eq!(value["snapshotter"]["available"], true);
        assert_eq!(value["snapshotter"]["capabilities"][0], "mount-spec");
        assert_eq!(value["snapshotter"]["externalSnapshotterSupported"], true);
    }

    #[test]
    fn content_gc_reports_dry_run_candidates_and_blockers() {
        let service = IntrospectionService;
        let candidates = vec![
            ContentGcCandidate {
                blob: crate::storage::ContentBlobRecord {
                    digest: "sha256:free".to_string(),
                    media_type: "application/vnd.oci.image.layer.v1.tar+gzip".to_string(),
                    size: 12,
                    relative_path: "blobs/sha256/fr/ee".to_string(),
                    created_at: 1,
                    last_used_at: 2,
                },
                blockers: Vec::new(),
            },
            ContentGcCandidate {
                blob: crate::storage::ContentBlobRecord {
                    digest: "sha256:blocked".to_string(),
                    media_type: "application/vnd.oci.image.layer.v1.tar+gzip".to_string(),
                    size: 24,
                    relative_path: "blobs/sha256/bl/ocked".to_string(),
                    created_at: 3,
                    last_used_at: 4,
                },
                blockers: vec![ContentGcBlocker::ActiveTransfer {
                    transfer_id: "transfer-1".to_string(),
                    source: "pull:sha256:blocked".to_string(),
                }],
            },
        ];

        let value = service.content_gc(&candidates, None);

        assert_eq!(value["dryRunSupported"], true);
        assert_eq!(value["candidateCount"], 2);
        assert_eq!(value["reclaimableCount"], 1);
        assert_eq!(value["blockedCount"], 1);
        assert_eq!(value["reclaimableBytes"], 12);
        assert_eq!(
            value["candidates"][1]["blockers"][0]["reason"],
            "activeTransfer"
        );
    }

    #[test]
    fn service_reports_image_runtime_security_cgroup_and_recovery_sections() {
        #[derive(serde::Serialize)]
        struct RecoveryResult {
            completed: bool,
        }

        let service = IntrospectionService;
        let config = RuntimeConfig {
            image_root: PathBuf::from("/var/lib/crius/images"),
            image_storage_options: vec!["overlay.mount_program=/usr/bin/fuse-overlayfs".to_string()],
            disable_cgroup: true,
            enable_criu_support: false,
            internal_wipe: false,
            internal_repair: true,
            ..Default::default()
        };

        let image_layout = service.image_layout(&config);
        let snapshot_model = service.image_snapshot_model(&config);
        let stats = service.snapshot_stats_collection(&config);
        let runtime_features = service.runtime_feature_flags(&config);
        let security = service.security_availability(
            PathBuf::from("/run/crius/seccomp").as_path(),
            vec!["ctr1".to_string()],
            &NriConfig::default(),
        );
        let cgroup = service.cgroup_support(
            &config,
            "v2",
            crate::security::resource_classes::ResourceClassSupport {
                blockio_supported: true,
                blockio_config_path: Some(PathBuf::from("/etc/blockio.json")),
                rdt_supported: false,
                rdt_resctrl_path: PathBuf::from("/sys/fs/resctrl"),
            },
        );
        let ledger_summary = RecoveryLedgerHealthSummary {
            dead_shims: 1,
            ..Default::default()
        };
        let ledger_check_report = LedgerCheckReport {
            dry_run: true,
            checked_at_unix_millis: 1,
            issue_count: 2,
            repairable_issue_count: 1,
            action_count: 1,
            applied_action_count: 0,
            issues: Vec::new(),
            actions: Vec::new(),
        };
        let recovery_result = RecoveryResult { completed: true };
        let recovery = service.recovery_status(RecoveryStatusInput {
            config: &config,
            last_startup_clean_shutdown: Some(false),
            last_startup_detected_reboot: Some(true),
            last_startup_detected_upgrade: None,
            last_startup_attempted_repair: Some(true),
            last_startup_repair_succeeded: Some(false),
            last_recovery_result: Some(&recovery_result),
            ledger_summary: Some(&ledger_summary),
            ledger_summary_error: Some("ledger unavailable"),
            ledger_check_report: Some(&ledger_check_report),
            ledger_check_error: None,
        });

        assert_eq!(image_layout["root"], "/var/lib/crius/images");
        assert_eq!(
            image_layout["imageRecordPathPattern"],
            "/var/lib/crius/images/images/<imageID>"
        );
        assert_eq!(snapshot_model["storageDriver"], config.image_driver);
        assert_eq!(snapshot_model["externalSnapshotterSupported"], false);
        assert_eq!(stats["backgroundCollector"], false);
        assert_eq!(runtime_features["updateContainerResources"], false);
        assert_eq!(runtime_features["checkpointContainer"], false);
        assert_eq!(security["seccompNotifierBaseDir"], "/run/crius/seccomp");
        assert_eq!(security["seccompNotifierActiveContainers"], json!(["ctr1"]));
        assert!(security["hostCapabilities"]["seccomp"]["state"].is_string());
        assert!(security["degradedCapabilities"].is_array());
        assert_eq!(cgroup["activeVersion"], "v2");
        assert_eq!(cgroup["resourceClasses"]["blockio"]["supported"], true);
        assert_eq!(
            cgroup["resourceClasses"]["blockio"]["configPath"],
            "/etc/blockio.json"
        );
        assert_eq!(recovery["lastStartupWasCleanShutdown"], false);
        assert_eq!(recovery["lastStartupDetectedReboot"], true);
        assert_eq!(recovery["lastRecoveryResult"]["completed"], true);
        assert_eq!(recovery["ledgerSummary"]["deadShims"], 1);
        assert_eq!(recovery["ledgerSummaryError"], "ledger unavailable");
        assert_eq!(recovery["ledgerCheck"]["issueCount"], 2);
        assert_eq!(recovery["internalWipe"], false);
        assert_eq!(recovery["internalRepair"], true);
        assert_eq!(
            recovery["policy"]["repairScope"],
            "sqlite-persistence-and-ledger"
        );
    }
}
