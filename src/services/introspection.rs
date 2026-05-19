use std::path::Path;

use serde_json::{json, Value};

use crate::config::NriConfig;
use crate::rootless::EffectiveRootlessConfig;
use crate::server::{RuntimeConfig, RuntimeReloadState, RuntimeReloadableConfig};

#[derive(Debug, Clone, Default)]
pub struct IntrospectionService;

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
                "default": "internal-overlay-untar",
                "runtimeSnapshotterOverrideSupported": true,
                "externalSnapshotterSupported": false,
            },
        })
    }

    pub fn rootless(&self, rootless: &EffectiveRootlessConfig) -> Value {
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
            "useFuseOverlayfs": rootless.use_fuse_overlayfs,
            "disableCgroup": rootless.disable_cgroup,
            "tolerateMissingHugetlbController": rootless.tolerate_missing_hugetlb_controller,
            "slirp4netnsPath": rootless.slirp4netns_path.display().to_string(),
            "pastaPath": rootless.pasta_path.display().to_string(),
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
    use std::path::PathBuf;

    use super::*;
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
        assert_eq!(value["storageRoot"], "/home/user/.local/share/crius");
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
        assert_eq!(value["lastReloadError"], "failed");
        assert_eq!(value["configFilePath"], "/etc/crius/config.toml");
    }
}
