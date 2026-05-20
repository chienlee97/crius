use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, RwLock};

use anyhow::{Context, Result};
use serde::Serialize;

use crate::config::CgroupDriverConfig;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum PullCgroupMode {
    Disabled,
    Pod,
    Path,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PullCgroupTarget {
    Disabled,
    Pod { cgroup_parent: String },
    Path(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PullCgroupEffectiveConfig {
    pub configured: String,
    pub mode: PullCgroupMode,
    pub enabled: bool,
    pub rootless_degraded: bool,
    pub disable_cgroup_degraded: bool,
    pub cgroup_driver: CgroupDriverConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PullCgroupScopeRecord {
    pub configured: String,
    pub mode: PullCgroupMode,
    pub effective_path: Option<String>,
    pub entered: bool,
    pub restored: bool,
    pub error: Option<String>,
    pub at_unix_millis: i64,
}

#[derive(Debug, Clone)]
pub struct PullCgroupExecutor {
    configured: String,
    mode: PullCgroupMode,
    disabled_by_rootless: bool,
    disabled_by_disable_cgroup: bool,
    cgroup_driver: CgroupDriverConfig,
    cgroup_root: PathBuf,
    last_scope: Arc<RwLock<Option<PullCgroupScopeRecord>>>,
}

impl PullCgroupExecutor {
    pub fn new(
        configured: impl Into<String>,
        cgroup_driver: CgroupDriverConfig,
        rootless_enabled: bool,
        disable_cgroup: bool,
    ) -> Self {
        Self::new_with_root(
            configured,
            cgroup_driver,
            rootless_enabled,
            disable_cgroup,
            PathBuf::from("/sys/fs/cgroup"),
        )
    }

    pub fn new_with_root(
        configured: impl Into<String>,
        cgroup_driver: CgroupDriverConfig,
        rootless_enabled: bool,
        disable_cgroup: bool,
        cgroup_root: PathBuf,
    ) -> Self {
        let configured = configured.into();
        Self {
            mode: parse_pull_cgroup_mode(&configured),
            configured,
            disabled_by_rootless: rootless_enabled,
            disabled_by_disable_cgroup: disable_cgroup,
            cgroup_driver,
            cgroup_root,
            last_scope: Arc::new(RwLock::new(None)),
        }
    }

    pub fn effective_config(&self) -> PullCgroupEffectiveConfig {
        let enabled = self.mode != PullCgroupMode::Disabled
            && !self.disabled_by_rootless
            && !self.disabled_by_disable_cgroup;
        PullCgroupEffectiveConfig {
            configured: self.configured.clone(),
            mode: self.mode.clone(),
            enabled,
            rootless_degraded: self.mode != PullCgroupMode::Disabled && self.disabled_by_rootless,
            disable_cgroup_degraded: self.mode != PullCgroupMode::Disabled
                && self.disabled_by_disable_cgroup,
            cgroup_driver: self.cgroup_driver,
        }
    }

    pub fn last_scope(&self) -> Option<PullCgroupScopeRecord> {
        self.last_scope
            .read()
            .ok()
            .and_then(|record| record.clone())
    }

    pub fn target_for_pod(&self, pod_cgroup_parent: Option<&str>) -> Result<PullCgroupTarget> {
        if self.effective_config().enabled {
            match self.mode {
                PullCgroupMode::Disabled => Ok(PullCgroupTarget::Disabled),
                PullCgroupMode::Path => Ok(PullCgroupTarget::Path(self.configured.clone())),
                PullCgroupMode::Pod => {
                    let cgroup_parent = pod_cgroup_parent
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "runtime.separate_pull_cgroup=pod requires sandbox linux.cgroup_parent"
                            )
                        })?;
                    Ok(PullCgroupTarget::Pod {
                        cgroup_parent: cgroup_parent.to_string(),
                    })
                }
            }
        } else {
            Ok(PullCgroupTarget::Disabled)
        }
    }

    pub fn enter(&self, target: &PullCgroupTarget) -> Result<PullCgroupScopeGuard> {
        let mode = target.mode();
        if matches!(target, PullCgroupTarget::Disabled) {
            let record = PullCgroupScopeRecord {
                configured: self.configured.clone(),
                mode,
                effective_path: None,
                entered: false,
                restored: false,
                error: None,
                at_unix_millis: chrono::Utc::now().timestamp_millis(),
            };
            self.record_scope(record);
            return Ok(PullCgroupScopeGuard::inactive());
        }

        match self.enter_active(target) {
            Ok(guard) => Ok(guard),
            Err(error) => {
                self.record_scope(PullCgroupScopeRecord {
                    configured: self.configured.clone(),
                    mode,
                    effective_path: target.raw_path().map(ToOwned::to_owned),
                    entered: false,
                    restored: false,
                    error: Some(error.to_string()),
                    at_unix_millis: chrono::Utc::now().timestamp_millis(),
                });
                Err(error)
            }
        }
    }

    fn enter_active(&self, target: &PullCgroupTarget) -> Result<PullCgroupScopeGuard> {
        let relative = self.relative_cgroup_path(target)?;
        let procs_files = self.target_procs_files(&relative);
        for procs_file in &procs_files {
            if let Some(parent) = procs_file.parent() {
                std::fs::create_dir_all(parent).with_context(|| {
                    format!("failed to create pull cgroup {}", parent.display())
                })?;
            }
        }

        let previous = current_process_cgroup_relative_path();
        let pid = std::process::id().to_string();
        for procs_file in &procs_files {
            std::fs::write(procs_file, &pid).with_context(|| {
                format!(
                    "failed to move pull process {} into cgroup {}",
                    pid,
                    procs_file.display()
                )
            })?;
        }

        self.record_scope(PullCgroupScopeRecord {
            configured: self.configured.clone(),
            mode: target.mode(),
            effective_path: procs_files
                .first()
                .and_then(|path| path.parent())
                .map(|path| path.display().to_string()),
            entered: true,
            restored: false,
            error: None,
            at_unix_millis: chrono::Utc::now().timestamp_millis(),
        });

        Ok(PullCgroupScopeGuard {
            restore_procs_files: previous
                .as_ref()
                .map(|path| self.target_procs_files(path))
                .unwrap_or_default(),
            last_scope: self.last_scope.clone(),
            active: true,
        })
    }

    fn target_procs_files(&self, relative: &Path) -> Vec<PathBuf> {
        if self.cgroup_root.join("cgroup.controllers").exists() {
            return vec![self.cgroup_root.join(relative).join("cgroup.procs")];
        }

        let controller_files = ["cpu", "memory", "pids"]
            .into_iter()
            .filter_map(|controller| {
                let controller_root = self.cgroup_root.join(controller);
                controller_root
                    .exists()
                    .then(|| controller_root.join(relative).join("cgroup.procs"))
            })
            .collect::<Vec<_>>();
        if controller_files.is_empty() {
            vec![self.cgroup_root.join(relative).join("cgroup.procs")]
        } else {
            controller_files
        }
    }

    fn relative_cgroup_path(&self, target: &PullCgroupTarget) -> Result<PathBuf> {
        let raw = target.raw_path().unwrap_or_default();
        match self.cgroup_driver {
            CgroupDriverConfig::Systemd => {
                let trimmed = raw.trim();
                let systemd_path = if trimmed.ends_with(".slice") {
                    let basename = Path::new(trimmed)
                        .file_name()
                        .and_then(|name| name.to_str())
                        .unwrap_or(trimmed);
                    PathBuf::from(trimmed.trim_start_matches('/'))
                        .join(format!("crius-pull-{}.scope", std::process::id()))
                        .components()
                        .fold(PathBuf::new(), |mut acc, component| {
                            if matches!(component, Component::Normal(_)) {
                                acc.push(component.as_os_str());
                            }
                            if acc.as_os_str().is_empty() && !basename.is_empty() {
                                acc.push(basename);
                            }
                            acc
                        })
                } else {
                    sanitize_relative_cgroup_path(trimmed)
                };
                Ok(systemd_path)
            }
            CgroupDriverConfig::Cgroupfs => Ok(sanitize_relative_cgroup_path(raw)),
        }
    }

    fn record_scope(&self, record: PullCgroupScopeRecord) {
        if let Ok(mut last_scope) = self.last_scope.write() {
            *last_scope = Some(record);
        }
    }
}

impl PullCgroupTarget {
    fn mode(&self) -> PullCgroupMode {
        match self {
            PullCgroupTarget::Disabled => PullCgroupMode::Disabled,
            PullCgroupTarget::Pod { .. } => PullCgroupMode::Pod,
            PullCgroupTarget::Path(_) => PullCgroupMode::Path,
        }
    }

    fn raw_path(&self) -> Option<&str> {
        match self {
            PullCgroupTarget::Disabled => None,
            PullCgroupTarget::Pod { cgroup_parent } => Some(cgroup_parent),
            PullCgroupTarget::Path(path) => Some(path),
        }
    }
}

pub struct PullCgroupScopeGuard {
    restore_procs_files: Vec<PathBuf>,
    last_scope: Arc<RwLock<Option<PullCgroupScopeRecord>>>,
    active: bool,
}

impl PullCgroupScopeGuard {
    fn inactive() -> Self {
        Self {
            restore_procs_files: Vec::new(),
            last_scope: Arc::new(RwLock::new(None)),
            active: false,
        }
    }
}

impl Drop for PullCgroupScopeGuard {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let mut restored = false;
        let mut error = None;
        for procs_file in &self.restore_procs_files {
            match std::fs::write(procs_file, std::process::id().to_string()) {
                Ok(()) => restored = true,
                Err(err) => error = Some(format!("failed to restore pull cgroup: {err}")),
            }
        }
        if let Ok(mut last_scope) = self.last_scope.write() {
            if let Some(record) = last_scope.as_mut() {
                record.restored = restored;
                if error.is_some() {
                    record.error = error;
                }
            }
        }
    }
}

pub fn parse_pull_cgroup_mode(raw: &str) -> PullCgroupMode {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        PullCgroupMode::Disabled
    } else if trimmed == "pod" {
        PullCgroupMode::Pod
    } else {
        PullCgroupMode::Path
    }
}

pub fn validate_pull_cgroup_config(
    raw: &str,
    cgroup_driver: CgroupDriverConfig,
) -> std::result::Result<(), String> {
    match parse_pull_cgroup_mode(raw) {
        PullCgroupMode::Disabled | PullCgroupMode::Pod => Ok(()),
        PullCgroupMode::Path => {
            let trimmed = raw.trim();
            if has_parent_dir_component(trimmed) {
                return Err(
                    "runtime.separate_pull_cgroup must not contain parent directory components"
                        .to_string(),
                );
            }
            if cgroup_driver == CgroupDriverConfig::Systemd
                && !(trimmed.contains(".slice") || trimmed.ends_with(".scope"))
            {
                return Err(
                    "runtime.separate_pull_cgroup with systemd must be empty, \"pod\", a .slice, or a .scope"
                        .to_string(),
                );
            }
            Ok(())
        }
    }
}

fn has_parent_dir_component(raw: &str) -> bool {
    Path::new(raw)
        .components()
        .any(|component| matches!(component, Component::ParentDir))
}

fn current_process_cgroup_relative_path() -> Option<PathBuf> {
    let raw = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    raw.lines().find_map(|line| {
        let mut parts = line.splitn(3, ':');
        let _hierarchy = parts.next()?;
        let controllers = parts.next()?;
        let path = parts.next()?;
        if controllers.is_empty() || controllers.split(',').any(|value| value == "memory") {
            Some(PathBuf::from(path.trim_start_matches('/')))
        } else {
            None
        }
    })
}

fn sanitize_relative_cgroup_path(raw: &str) -> PathBuf {
    let mut path = PathBuf::new();
    for component in Path::new(raw.trim_start_matches('/')).components() {
        if let Component::Normal(value) = component {
            path.push(value);
        }
    }
    if path.as_os_str().is_empty() {
        path.push(format!("crius-pull-{}", std::process::id()));
    }
    path
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn parses_pull_cgroup_modes() {
        assert_eq!(parse_pull_cgroup_mode(""), PullCgroupMode::Disabled);
        assert_eq!(parse_pull_cgroup_mode("pod"), PullCgroupMode::Pod);
        assert_eq!(
            parse_pull_cgroup_mode("kubepods.slice"),
            PullCgroupMode::Path
        );
    }

    #[test]
    fn validates_systemd_pull_cgroup_values() {
        assert!(validate_pull_cgroup_config("", CgroupDriverConfig::Systemd).is_ok());
        assert!(validate_pull_cgroup_config("pod", CgroupDriverConfig::Systemd).is_ok());
        assert!(validate_pull_cgroup_config("kubepods.slice", CgroupDriverConfig::Systemd).is_ok());
        assert!(
            validate_pull_cgroup_config("crius-pull.scope", CgroupDriverConfig::Systemd).is_ok()
        );
        assert!(
            validate_pull_cgroup_config("kubepods/pod123", CgroupDriverConfig::Systemd).is_err()
        );
        assert!(
            validate_pull_cgroup_config("kubepods/pod123", CgroupDriverConfig::Cgroupfs).is_ok()
        );
    }

    #[test]
    fn validates_cgroupfs_pull_cgroup_path_components() {
        assert!(
            validate_pull_cgroup_config("kubepods/pod123", CgroupDriverConfig::Cgroupfs).is_ok()
        );
        assert!(
            validate_pull_cgroup_config("kubepods/foo..bar", CgroupDriverConfig::Cgroupfs).is_ok()
        );
        assert!(
            validate_pull_cgroup_config("kubepods/../pod123", CgroupDriverConfig::Cgroupfs)
                .is_err()
        );
        assert!(validate_pull_cgroup_config(
            "kubepods/../pod123.slice",
            CgroupDriverConfig::Systemd
        )
        .is_err());
    }

    #[test]
    fn pod_target_requires_pod_cgroup_parent() {
        let executor = PullCgroupExecutor::new_with_root(
            "pod",
            CgroupDriverConfig::Cgroupfs,
            false,
            false,
            PathBuf::from("/tmp/cgroup"),
        );

        assert!(executor.target_for_pod(None).is_err());
        assert_eq!(
            executor.target_for_pod(Some("/kubepods/pod123")).unwrap(),
            PullCgroupTarget::Pod {
                cgroup_parent: "/kubepods/pod123".to_string()
            }
        );
    }

    #[test]
    fn rootless_degrades_pull_cgroup_without_enabling_scope() {
        let executor = PullCgroupExecutor::new_with_root(
            "pod",
            CgroupDriverConfig::Systemd,
            true,
            false,
            PathBuf::from("/tmp/cgroup"),
        );

        let effective = executor.effective_config();
        assert!(!effective.enabled);
        assert!(effective.rootless_degraded);
        assert!(matches!(
            executor.target_for_pod(Some("pod.slice")).unwrap(),
            PullCgroupTarget::Disabled
        ));
    }

    #[test]
    fn records_failed_scope_entry() {
        let dir = tempdir().unwrap();
        let file_root = dir.path().join("not-a-dir");
        std::fs::write(&file_root, "block directory creation").unwrap();
        let executor = PullCgroupExecutor::new_with_root(
            "kubepods/pod123",
            CgroupDriverConfig::Cgroupfs,
            false,
            false,
            file_root,
        );

        let target = executor.target_for_pod(None).unwrap();
        let result = executor.enter(&target);

        assert!(result.is_err());
        let last = executor.last_scope().unwrap();
        assert!(!last.entered);
        assert!(last.error.unwrap().contains("failed to create pull cgroup"));
    }

    #[test]
    fn systemd_slice_scope_and_pod_targets_use_expected_paths() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("cgroup.controllers"), "cpu memory pids").unwrap();

        let slice_executor = PullCgroupExecutor::new_with_root(
            "kubepods.slice",
            CgroupDriverConfig::Systemd,
            false,
            false,
            dir.path().to_path_buf(),
        );
        let slice_target = slice_executor.target_for_pod(None).unwrap();
        let slice_guard = slice_executor.enter(&slice_target).unwrap();
        drop(slice_guard);
        let slice_scope = slice_executor.last_scope().unwrap();
        let slice_path = slice_scope.effective_path.unwrap();
        assert!(slice_path.contains("kubepods.slice"));
        assert!(slice_path.contains("crius-pull-"));
        assert!(slice_path.ends_with(".scope"));

        let scope_executor = PullCgroupExecutor::new_with_root(
            "crius-pull.scope",
            CgroupDriverConfig::Systemd,
            false,
            false,
            dir.path().to_path_buf(),
        );
        let scope_target = scope_executor.target_for_pod(None).unwrap();
        let scope_guard = scope_executor.enter(&scope_target).unwrap();
        drop(scope_guard);
        let scope_path = scope_executor.last_scope().unwrap().effective_path.unwrap();
        assert!(scope_path.ends_with("crius-pull.scope"));

        let pod_executor = PullCgroupExecutor::new_with_root(
            "pod",
            CgroupDriverConfig::Systemd,
            false,
            false,
            dir.path().to_path_buf(),
        );
        let pod_target = pod_executor
            .target_for_pod(Some("kubepods.slice/podabc.slice"))
            .unwrap();
        let pod_guard = pod_executor.enter(&pod_target).unwrap();
        drop(pod_guard);
        let pod_path = pod_executor.last_scope().unwrap().effective_path.unwrap();
        assert!(pod_path.contains("kubepods.slice"));
        assert!(pod_path.contains("podabc.slice"));
        assert!(pod_path.ends_with(".scope"));
    }

    #[test]
    fn cgroupfs_v1_layout_writes_controller_procs_files() {
        let dir = tempdir().unwrap();
        for controller in ["cpu", "memory", "pids"] {
            std::fs::create_dir_all(dir.path().join(controller)).unwrap();
        }
        let executor = PullCgroupExecutor::new_with_root(
            "kubepods/pod123",
            CgroupDriverConfig::Cgroupfs,
            false,
            false,
            dir.path().to_path_buf(),
        );

        let target = executor.target_for_pod(None).unwrap();
        let _guard = executor.enter(&target).unwrap();

        for controller in ["cpu", "memory", "pids"] {
            assert!(dir
                .path()
                .join(controller)
                .join("kubepods/pod123/cgroup.procs")
                .exists());
        }
    }
}
