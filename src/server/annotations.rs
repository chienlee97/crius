use super::*;

pub(super) struct ContainerAnnotationContext<'a> {
    pub(super) annotations: &'a mut HashMap<String, String>,
    pub(super) container_id: &'a str,
    pub(super) pod_sandbox_id: &'a str,
    pub(super) metadata_name: Option<&'a str>,
    pub(super) requested_image: Option<&'a str>,
    pub(super) resolved_image_name: Option<&'a str>,
    pub(super) log_path: Option<&'a Path>,
    pub(super) pod_state: Option<&'a StoredPodState>,
    pub(super) default_runtime: &'a str,
}

impl RuntimeServiceImpl {
    fn resolved_runtime_handler_name<'a>(&'a self, runtime_handler: &'a str) -> &'a str {
        let runtime_handler = runtime_handler.trim();
        if runtime_handler.is_empty() {
            self.config.runtime.as_str()
        } else {
            runtime_handler
        }
    }

    fn runtime_handler_allowed_annotations(&self, runtime_handler: &str) -> Vec<String> {
        self.config
            .runtime_configs
            .get(self.resolved_runtime_handler_name(runtime_handler))
            .map(|config| config.allowed_annotations.clone())
            .unwrap_or_default()
    }

    pub(super) fn apply_runtime_handler_default_annotations(
        &self,
        annotations: &mut HashMap<String, String>,
        runtime_handler: &str,
    ) {
        let Some(config) = self
            .config
            .runtime_configs
            .get(self.resolved_runtime_handler_name(runtime_handler))
        else {
            return;
        };

        for (key, value) in &config.default_annotations {
            annotations.entry(key.clone()).or_insert_with(|| value.clone());
        }
    }

    fn workload_profile_selected_name(
        &self,
        activation_annotations: &HashMap<String, String>,
    ) -> Result<Option<String>, Status> {
        let mut selected: Vec<String> = self
            .config
            .workloads
            .iter()
            .filter(|(_, workload)| {
                !workload.activation_annotation.trim().is_empty()
                    && activation_annotations.contains_key(workload.activation_annotation.trim())
            })
            .map(|(name, _)| name.clone())
            .collect();
        selected.sort();
        selected.dedup();

        if selected.len() > 1 {
            return Err(Status::invalid_argument(format!(
                "multiple workload profiles are activated simultaneously: {}",
                selected.join(", ")
            )));
        }

        Ok(selected.into_iter().next())
    }

    pub(super) fn selected_workload_profile(
        &self,
        activation_annotations: &HashMap<String, String>,
    ) -> Result<Option<&crate::config::RuntimeWorkloadConfig>, Status> {
        let Some(name) = self.workload_profile_selected_name(activation_annotations)? else {
            return Ok(None);
        };
        Ok(self.config.workloads.get(&name))
    }

    fn workload_profile_allowed_annotations(
        &self,
        activation_annotations: &HashMap<String, String>,
    ) -> Result<Vec<String>, Status> {
        Ok(self
            .selected_workload_profile(activation_annotations)?
            .map(|workload| workload.allowed_annotations.clone())
            .unwrap_or_default())
    }

    fn milli_cpu_to_quota(milli_cpu: i64, period: i64) -> i64 {
        if milli_cpu == 0 {
            return 0;
        }
        let period = if period == 0 { 100_000 } else { period };
        ((milli_cpu * period) / 1000).max(1_000)
    }

    pub(super) fn workload_resources_from_annotation(
        &self,
        activation_annotations: &HashMap<String, String>,
        container_name: &str,
    ) -> Result<Option<crate::config::RuntimeWorkloadResources>, Status> {
        let Some(workload) = self.selected_workload_profile(activation_annotations)? else {
            return Ok(None);
        };
        let mut resources = workload.resources.clone();
        let annotation_key = format!("{}/{}", workload.annotation_prefix.trim(), container_name);
        let Some(raw) = activation_annotations.get(&annotation_key) else {
            return Ok(Some(resources));
        };
        let override_resources: crate::config::RuntimeWorkloadResources = serde_json::from_str(raw)
            .map_err(|err| {
                Status::invalid_argument(format!(
                    "failed to parse workload annotation {annotation_key}: {err}"
                ))
            })?;
        override_resources
            .validate()
            .map_err(Status::invalid_argument)?;

        if override_resources.cpu_shares != 0 {
            resources.cpu_shares = override_resources.cpu_shares;
        }
        if override_resources.cpu_quota != 0 {
            resources.cpu_quota = override_resources.cpu_quota;
        }
        if override_resources.cpu_period != 0 {
            resources.cpu_period = override_resources.cpu_period;
        }
        if !override_resources.cpuset_cpus.trim().is_empty() {
            resources.cpuset_cpus = override_resources.cpuset_cpus;
        }
        if override_resources.cpu_limit != 0 {
            resources.cpu_limit = override_resources.cpu_limit;
        }
        Ok(Some(resources))
    }

    pub(super) fn apply_workload_resources(
        resources: &mut StoredLinuxResources,
        workload: &crate::config::RuntimeWorkloadResources,
    ) {
        if workload.cpu_shares > 0 {
            resources.cpu_shares = workload.cpu_shares;
        }
        if workload.cpu_period > 0 {
            resources.cpu_period = workload.cpu_period;
        }
        if workload.cpu_quota > 0 {
            resources.cpu_quota = workload.cpu_quota;
        }
        if !workload.cpuset_cpus.trim().is_empty() {
            resources.cpuset_cpus = workload.cpuset_cpus.trim().to_string();
        }
        if workload.cpu_limit > 0 {
            let period = if resources.cpu_period > 0 {
                resources.cpu_period
            } else if workload.cpu_period > 0 {
                workload.cpu_period
            } else {
                100_000
            };
            resources.cpu_period = period;
            resources.cpu_quota = Self::milli_cpu_to_quota(workload.cpu_limit, period);
        }
    }

    pub(super) fn default_allowed_annotation_prefixes() -> Vec<String> {
        vec![
            "io.kubernetes.cri-o.".to_string(),
            "io.kubernetes.cri.".to_string(),
            "io.containerd.cri.".to_string(),
            "io.kubernetes.container.".to_string(),
            "cpu-".to_string(),
            "irq-".to_string(),
            "cpu-quota.".to_string(),
            "cpu-load-balancing.crio.io".to_string(),
            "irq-load-balancing.crio.io".to_string(),
            "cpu-c-states.crio.io".to_string(),
            "cpu-freq-governor.crio.io".to_string(),
        ]
    }

    pub(super) fn workload_annotation_prefixes(
        activation_annotations: &HashMap<String, String>,
        workloads: &[NriAnnotationWorkloadConfig],
    ) -> Vec<String> {
        workloads
            .iter()
            .filter(|workload| !workload.activation_annotation.trim().is_empty())
            .filter(|workload| {
                activation_annotations
                    .get(workload.activation_annotation.trim())
                    .map(|value| {
                        workload.activation_value.trim().is_empty()
                            || value == workload.activation_value.trim()
                    })
                    .unwrap_or(false)
            })
            .flat_map(|workload| workload.allowed_annotation_prefixes.iter().cloned())
            .collect()
    }

    pub(super) fn nri_allowed_annotation_prefixes(
        &self,
        activation_annotations: &HashMap<String, String>,
        existing_annotations: &HashMap<String, String>,
        runtime_handler: &str,
    ) -> Result<Vec<String>, Status> {
        let mut allowed = Self::default_allowed_annotation_prefixes();
        allowed.extend(self.nri_config.allowed_annotation_prefixes.iter().cloned());
        allowed.extend(
            std::env::var(NRI_ALLOWED_ANNOTATION_PREFIXES_ENV)
                .ok()
                .into_iter()
                .flat_map(|raw| {
                    raw.split(',')
                        .map(str::trim)
                        .filter(|item| !item.is_empty())
                        .map(str::to_string)
                        .collect::<Vec<_>>()
                }),
        );
        allowed.extend(
            Self::external_annotations(existing_annotations)
                .into_keys()
                .collect::<Vec<_>>(),
        );
        if !runtime_handler.trim().is_empty() {
            allowed.push(format!(
                "{}.{}",
                CRIO_RUNTIME_HANDLER_ANNOTATION,
                runtime_handler.trim()
            ));
            allowed.push(format!(
                "{}.{}",
                CONTAINERD_RUNTIME_HANDLER_ANNOTATION,
                runtime_handler.trim()
            ));
            if let Some(runtime_allowed) = self
                .nri_config
                .runtime_allowed_annotation_prefixes
                .get(runtime_handler.trim())
            {
                allowed.extend(runtime_allowed.iter().cloned());
            }
        }
        allowed.extend(self.runtime_handler_allowed_annotations(runtime_handler));
        allowed.extend(self.workload_profile_allowed_annotations(activation_annotations)?);
        allowed.extend(Self::workload_annotation_prefixes(
            activation_annotations,
            &self.nri_config.workload_allowed_annotation_prefixes,
        ));
        allowed.sort();
        allowed.dedup();
        Ok(allowed)
    }

    pub(super) fn filter_nri_annotation_adjustments(
        &self,
        activation_annotations: &HashMap<String, String>,
        adjustments: &HashMap<String, String>,
        existing_annotations: &HashMap<String, String>,
        runtime_handler: &str,
    ) -> Result<HashMap<String, String>, Status> {
        let allowed = self.nri_allowed_annotation_prefixes(
            activation_annotations,
            existing_annotations,
            runtime_handler,
        )?;
        let disallowed = disallowed_annotation_adjustment_keys(adjustments, &allowed);
        if !disallowed.is_empty() {
            log::warn!(
                "Filtering disallowed NRI CreateContainer annotations: {}",
                disallowed.join(", ")
            );
        }
        Ok(filter_annotation_adjustments_by_allowlist(
            adjustments,
            &allowed,
        ))
    }

    pub(super) fn is_internal_annotation_key(key: &str) -> bool {
        key.starts_with(INTERNAL_ANNOTATION_PREFIX)
    }

    pub(super) fn external_annotations(
        annotations: &HashMap<String, String>,
    ) -> HashMap<String, String> {
        annotations
            .iter()
            .filter(|(key, _)| !Self::is_internal_annotation_key(key))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    }

    pub(super) fn merge_external_annotations(
        base: &HashMap<String, String>,
        spec_annotations: Option<&HashMap<String, String>>,
    ) -> HashMap<String, String> {
        let mut merged = Self::external_annotations(base);
        if let Some(spec_annotations) = spec_annotations {
            for (key, value) in Self::external_annotations(spec_annotations) {
                merged.insert(key, value);
            }
        }
        merged
    }

    pub(super) fn merge_labels(
        base: &HashMap<String, String>,
        spec_annotations: Option<&HashMap<String, String>>,
    ) -> HashMap<String, String> {
        let mut merged = base.clone();
        let Some(spec_annotations) = spec_annotations else {
            return merged;
        };
        let Some(raw) = spec_annotations.get(CRIO_LABELS_ANNOTATION) else {
            return merged;
        };
        if let Ok(extra) = serde_json::from_str::<HashMap<String, String>>(raw) {
            for (key, value) in extra {
                merged.insert(key, value);
            }
        }
        merged
    }

    pub(super) fn spec_annotation_value(
        spec_annotations: Option<&HashMap<String, String>>,
        keys: &[&str],
    ) -> Option<String> {
        let annotations = spec_annotations?;
        keys.iter().find_map(|key| {
            annotations
                .get(*key)
                .filter(|value| !value.is_empty())
                .cloned()
        })
    }

    pub(super) fn enrich_container_annotations(context: ContainerAnnotationContext<'_>) {
        context.annotations.insert(
            CRIO_CONTAINER_ID_ANNOTATION.to_string(),
            context.container_id.to_string(),
        );
        context.annotations.insert(
            CRIO_SANDBOX_ID_ANNOTATION.to_string(),
            context.pod_sandbox_id.to_string(),
        );
        context.annotations.insert(
            CRIO_CONTAINER_NAME_ANNOTATION.to_string(),
            context
                .metadata_name
                .unwrap_or(context.container_id)
                .to_string(),
        );
        context.annotations.insert(
            CRIO_CONTAINER_TYPE_ANNOTATION.to_string(),
            CONTAINER_TYPE_CONTAINER.to_string(),
        );
        context.annotations.insert(
            CONTAINERD_SANDBOX_ID_ANNOTATION.to_string(),
            context.pod_sandbox_id.to_string(),
        );
        context.annotations.insert(
            CONTAINERD_CONTAINER_TYPE_ANNOTATION.to_string(),
            CONTAINER_TYPE_CONTAINER.to_string(),
        );
        if let Some(name) = context.metadata_name {
            context.annotations.insert(
                CONTAINERD_CONTAINER_NAME_ANNOTATION.to_string(),
                name.to_string(),
            );
            context.annotations.insert(
                KUBERNETES_CONTAINER_NAME_ANNOTATION.to_string(),
                name.to_string(),
            );
        }
        if let Some(image) = context.requested_image.filter(|image| !image.is_empty()) {
            context.annotations.insert(
                CRIO_USER_REQUESTED_IMAGE_ANNOTATION.to_string(),
                image.to_string(),
            );
        }
        if let Some(image_name) = context
            .resolved_image_name
            .filter(|image| !image.is_empty())
        {
            context.annotations.insert(
                CRIO_IMAGE_NAME_ANNOTATION.to_string(),
                image_name.to_string(),
            );
            context.annotations.insert(
                CONTAINERD_IMAGE_NAME_ANNOTATION.to_string(),
                image_name.to_string(),
            );
        }
        let runtime_handler = context
            .pod_state
            .map(|state| state.runtime_handler.clone())
            .filter(|handler| !handler.is_empty())
            .unwrap_or_else(|| context.default_runtime.to_string());
        context.annotations.insert(
            CRIO_RUNTIME_HANDLER_ANNOTATION.to_string(),
            runtime_handler.clone(),
        );
        context.annotations.insert(
            CONTAINERD_RUNTIME_HANDLER_ANNOTATION.to_string(),
            runtime_handler,
        );
        if let Some(path) = context.log_path {
            context.annotations.insert(
                CRIO_LOG_PATH_ANNOTATION.to_string(),
                path.to_string_lossy().to_string(),
            );
        }
    }

    pub(super) fn insert_internal_state<T: Serialize>(
        annotations: &mut HashMap<String, String>,
        key: &str,
        state: &T,
    ) -> Result<(), Status> {
        let encoded = serde_json::to_string(state)
            .map_err(|e| Status::internal(format!("Failed to encode internal state: {}", e)))?;
        annotations.insert(key.to_string(), encoded);
        Ok(())
    }

    pub(super) fn read_internal_state<T: for<'de> Deserialize<'de>>(
        annotations: &HashMap<String, String>,
        key: &str,
    ) -> Option<T> {
        annotations
            .get(key)
            .and_then(|value| serde_json::from_str(value).ok())
    }
}
