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
    ) -> Vec<String> {
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
        allowed.extend(Self::workload_annotation_prefixes(
            activation_annotations,
            &self.nri_config.workload_allowed_annotation_prefixes,
        ));
        allowed.sort();
        allowed.dedup();
        allowed
    }

    pub(super) fn filter_nri_annotation_adjustments(
        &self,
        activation_annotations: &HashMap<String, String>,
        adjustments: &HashMap<String, String>,
        existing_annotations: &HashMap<String, String>,
        runtime_handler: &str,
    ) -> HashMap<String, String> {
        let allowed = self.nri_allowed_annotation_prefixes(
            activation_annotations,
            existing_annotations,
            runtime_handler,
        );
        let disallowed = disallowed_annotation_adjustment_keys(adjustments, &allowed);
        if !disallowed.is_empty() {
            log::warn!(
                "Filtering disallowed NRI CreateContainer annotations: {}",
                disallowed.join(", ")
            );
        }
        filter_annotation_adjustments_by_allowlist(adjustments, &allowed)
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
