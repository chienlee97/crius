use std::collections::HashSet;

use crate::config::NriDefaultValidatorConfig;
use crate::nri::{NriError, Result};
use crate::nri_proto::api as nri_api;

const REQUIRED_PLUGINS_ANNOTATION: &str = "required-plugins.noderesource.dev";

pub fn validate_default_adjustment(
    cfg: &NriDefaultValidatorConfig,
    req: &nri_api::ValidateContainerAdjustmentRequest,
) -> Result<()> {
    if !cfg.enable {
        return Ok(());
    }

    validate_oci_hooks(cfg, req)?;
    validate_seccomp(cfg, req)?;
    validate_namespaces(cfg, req)?;
    validate_required_plugins(cfg, req)?;

    Ok(())
}

fn validate_oci_hooks(
    cfg: &NriDefaultValidatorConfig,
    req: &nri_api::ValidateContainerAdjustmentRequest,
) -> Result<()> {
    if !cfg.reject_oci_hook_adjustment || req.adjust.is_none() {
        return Ok(());
    }

    let Some(owners) = simple_owner(req, req.container.as_ref(), nri_api::Field::OciHooks) else {
        return Ok(());
    };

    let offender = if owners.contains(',') {
        format!("plugins {owners:?}")
    } else {
        format!("plugin {owners:?}")
    };

    Err(NriError::Plugin(format!(
        "validation error: {offender} attempted restricted OCI hook injection"
    )))
}

fn validate_seccomp(
    cfg: &NriDefaultValidatorConfig,
    req: &nri_api::ValidateContainerAdjustmentRequest,
) -> Result<()> {
    if req.adjust.is_none() {
        return Ok(());
    }

    let Some(owner) = simple_owner(req, req.container.as_ref(), nri_api::Field::SeccompPolicy)
    else {
        return Ok(());
    };

    let profile_type = req
        .container
        .as_ref()
        .and_then(|container| container.linux.as_ref())
        .and_then(|linux| linux.seccomp_profile.as_ref())
        .map(|profile| profile.profile_type.enum_value_or_default())
        .unwrap_or(nri_api::security_profile::ProfileType::UNCONFINED);

    match profile_type {
        nri_api::security_profile::ProfileType::UNCONFINED
            if cfg.reject_unconfined_seccomp_adjustment =>
        {
            Err(NriError::Plugin(format!(
                "validation error: plugin {owner} attempted restricted unconfined seccomp policy adjustment"
            )))
        }
        nri_api::security_profile::ProfileType::RUNTIME_DEFAULT
            if cfg.reject_runtime_default_seccomp_adjustment =>
        {
            Err(NriError::Plugin(format!(
                "validation error: plugin {owner} attempted restricted runtime default seccomp policy adjustment"
            )))
        }
        nri_api::security_profile::ProfileType::LOCALHOST
            if cfg.reject_custom_seccomp_adjustment =>
        {
            Err(NriError::Plugin(format!(
                "validation error: plugin {owner} attempted restricted custom seccomp policy adjustment"
            )))
        }
        _ => Ok(()),
    }
}

fn validate_namespaces(
    cfg: &NriDefaultValidatorConfig,
    req: &nri_api::ValidateContainerAdjustmentRequest,
) -> Result<()> {
    if !cfg.reject_namespace_adjustment || req.adjust.is_none() {
        return Ok(());
    }

    let Some(field_owners) = req
        .owners
        .as_ref()
        .and_then(|owners| owners_for_container(owners, req.container.as_ref()))
    else {
        return Ok(());
    };

    let Some(namespace_owners) = field_owners
        .compound
        .get(&(nri_api::Field::Namespace as i32))
    else {
        return Ok(());
    };

    if namespace_owners.owners.is_empty() {
        return Ok(());
    }

    let mut offenders = namespace_owners
        .owners
        .iter()
        .map(|(namespace, plugin)| format!("{plugin:?} (namespace {namespace:?})"))
        .collect::<Vec<_>>();
    offenders.sort();

    Err(NriError::Plugin(format!(
        "validation error: attempted restricted namespace adjustment by plugin(s) {}",
        offenders.join(", ")
    )))
}

fn validate_required_plugins(
    cfg: &NriDefaultValidatorConfig,
    req: &nri_api::ValidateContainerAdjustmentRequest,
) -> Result<()> {
    let mut required = cfg.required_plugins.clone();
    let container_name = req
        .container
        .as_ref()
        .map(|container| container.name.as_str())
        .unwrap_or_default();

    if let Some(annotation) = cfg
        .tolerate_missing_plugins_annotation
        .split_whitespace()
        .next()
        .filter(|annotation| !annotation.is_empty())
    {
        if let Some(value) = effective_annotation(req.pod.as_ref(), annotation, container_name) {
            let tolerate = value.parse::<bool>().map_err(|err| {
                NriError::Plugin(format!(
                    "invalid {} annotation {:?}: {}",
                    annotation, value, err
                ))
            })?;
            if tolerate {
                return Ok(());
            }
        }
    }

    if let Some(value) = effective_annotation(
        req.pod.as_ref(),
        REQUIRED_PLUGINS_ANNOTATION,
        container_name,
    ) {
        let annotated = parse_required_plugins_annotation(&value).map_err(|err| {
            NriError::Plugin(format!(
                "invalid {} annotation {:?}: {}",
                REQUIRED_PLUGINS_ANNOTATION, value, err
            ))
        })?;
        required.extend(annotated);
    }

    if required.is_empty() {
        return Ok(());
    }

    let plugins = plugin_map(req);
    let mut missing = required
        .into_iter()
        .filter(|plugin| !plugins.contains(plugin))
        .collect::<Vec<_>>();
    missing.sort();
    missing.dedup();

    if missing.is_empty() {
        return Ok(());
    }

    let offender = if missing.len() == 1 {
        format!("required plugin {:?}", missing[0])
    } else {
        format!("required plugins {:?}", missing.join(","))
    };

    Err(NriError::Plugin(format!(
        "validation error: {offender} not present"
    )))
}

fn owners_for_container<'a>(
    owners: &'a nri_api::OwningPlugins,
    container: Option<&nri_api::Container>,
) -> Option<&'a nri_api::FieldOwners> {
    container.and_then(|container| owners.owners.get(&container.id))
}

fn simple_owner(
    req: &nri_api::ValidateContainerAdjustmentRequest,
    container: Option<&nri_api::Container>,
    field: nri_api::Field,
) -> Option<String> {
    req.owners
        .as_ref()
        .and_then(|owners| owners_for_container(owners, container))
        .and_then(|field_owners| field_owners.simple.get(&(field as i32)))
        .cloned()
}

fn effective_annotation(
    pod: Option<&nri_api::PodSandbox>,
    key: &str,
    container_name: &str,
) -> Option<String> {
    let pod = pod?;
    let annotations = &pod.annotations;
    for candidate in [
        format!("{key}/container.{container_name}"),
        format!("{key}/pod"),
        key.to_string(),
    ] {
        if let Some(value) = annotations.get(&candidate) {
            return Some(value.clone());
        }
    }
    None
}

fn plugin_map(req: &nri_api::ValidateContainerAdjustmentRequest) -> HashSet<String> {
    let mut plugins = HashSet::new();
    for plugin in &req.plugins {
        plugins.insert(plugin.name.clone());
        plugins.insert(format!("{}-{}", plugin.index, plugin.name));
    }
    plugins
}

fn parse_required_plugins_annotation(raw: &str) -> std::result::Result<Vec<String>, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }

    if trimmed.starts_with('[') {
        let parsed = serde_json::from_str::<Vec<String>>(trimmed)
            .map_err(|err| format!("failed to parse JSON list: {err}"))?;
        return Ok(parsed
            .into_iter()
            .map(|entry| entry.trim().to_string())
            .filter(|entry| !entry.is_empty())
            .collect());
    }

    let mut plugins = Vec::new();
    for line in trimmed.lines() {
        let entry = line.trim();
        if entry.is_empty() {
            continue;
        }
        let Some(entry) = entry.strip_prefix('-') else {
            return Err("expected YAML list entries prefixed by '-'".to_string());
        };
        let entry = entry.trim();
        if entry.is_empty() {
            return Err("required plugin entry must not be empty".to_string());
        }
        plugins.push(entry.to_string());
    }

    Ok(plugins)
}

#[cfg(test)]
mod tests {
    use protobuf::MessageField;

    use super::{validate_default_adjustment, REQUIRED_PLUGINS_ANNOTATION};
    use crate::config::NriDefaultValidatorConfig;
    use crate::nri_proto::api as nri_api;

    fn base_request() -> nri_api::ValidateContainerAdjustmentRequest {
        let mut req = nri_api::ValidateContainerAdjustmentRequest::new();

        let mut pod = nri_api::PodSandbox::new();
        pod.id = "pod-1".to_string();
        pod.name = "pod".to_string();
        pod.namespace = "default".to_string();
        req.pod = MessageField::some(pod);

        let mut container = nri_api::Container::new();
        container.id = "ctr-1".to_string();
        container.name = "main".to_string();
        req.container = MessageField::some(container);

        req.adjust = MessageField::some(nri_api::ContainerAdjustment::new());
        req.owners = MessageField::some(nri_api::OwningPlugins::new());
        req
    }

    fn set_simple_owner(
        req: &mut nri_api::ValidateContainerAdjustmentRequest,
        field: nri_api::Field,
        owner: &str,
    ) {
        let owners = req.owners.as_mut().unwrap();
        let entry = owners
            .owners
            .entry(req.container.as_ref().unwrap().id.clone())
            .or_default();
        entry.simple.insert(field as i32, owner.to_string());
    }

    #[test]
    fn rejects_oci_hook_adjustment_when_enabled() {
        let mut req = base_request();
        set_simple_owner(&mut req, nri_api::Field::OciHooks, "00-validator");

        let err = validate_default_adjustment(
            &NriDefaultValidatorConfig {
                enable: true,
                reject_oci_hook_adjustment: true,
                ..Default::default()
            },
            &req,
        )
        .unwrap_err();

        assert!(format!("{err}").contains("OCI hook injection"));
    }

    #[test]
    fn rejects_runtime_default_seccomp_adjustment_when_enabled() {
        let mut req = base_request();
        let mut linux = nri_api::LinuxContainer::new();
        let mut profile = nri_api::SecurityProfile::new();
        profile.profile_type = nri_api::security_profile::ProfileType::RUNTIME_DEFAULT.into();
        linux.seccomp_profile = MessageField::some(profile);
        req.container.as_mut().unwrap().linux = MessageField::some(linux);
        set_simple_owner(&mut req, nri_api::Field::SeccompPolicy, "02-policy");

        let err = validate_default_adjustment(
            &NriDefaultValidatorConfig {
                enable: true,
                reject_runtime_default_seccomp_adjustment: true,
                ..Default::default()
            },
            &req,
        )
        .unwrap_err();

        assert!(format!("{err}").contains("runtime default seccomp"));
    }

    #[test]
    fn rejects_namespace_adjustment_when_enabled() {
        let mut req = base_request();
        let owners = req.owners.as_mut().unwrap();
        let entry = owners
            .owners
            .entry(req.container.as_ref().unwrap().id.clone())
            .or_default();
        entry
            .compound
            .entry(nri_api::Field::Namespace as i32)
            .or_default()
            .owners
            .insert("network".to_string(), "03-topology".to_string());

        let err = validate_default_adjustment(
            &NriDefaultValidatorConfig {
                enable: true,
                reject_namespace_adjustment: true,
                ..Default::default()
            },
            &req,
        )
        .unwrap_err();

        assert!(format!("{err}").contains("namespace adjustment"));
    }

    #[test]
    fn rejects_missing_required_plugins() {
        let mut req = base_request();
        req.plugins.push(nri_api::PluginInstance {
            name: "cpu".to_string(),
            index: "01".to_string(),
            ..Default::default()
        });

        let err = validate_default_adjustment(
            &NriDefaultValidatorConfig {
                enable: true,
                required_plugins: vec!["memory".to_string()],
                ..Default::default()
            },
            &req,
        )
        .unwrap_err();

        assert!(format!("{err}").contains("required plugin"));
    }

    #[test]
    fn honors_required_plugins_annotation_and_toleration() {
        let mut req = base_request();
        req.plugins.push(nri_api::PluginInstance {
            name: "memory".to_string(),
            index: "02".to_string(),
            ..Default::default()
        });
        req.pod.as_mut().unwrap().annotations.insert(
            REQUIRED_PLUGINS_ANNOTATION.to_string(),
            "- memory\n".to_string(),
        );

        validate_default_adjustment(
            &NriDefaultValidatorConfig {
                enable: true,
                ..Default::default()
            },
            &req,
        )
        .expect("annotated required plugin should pass");

        req.pod
            .as_mut()
            .unwrap()
            .annotations
            .insert("example.com/tolerate".to_string(), "true".to_string());

        validate_default_adjustment(
            &NriDefaultValidatorConfig {
                enable: true,
                tolerate_missing_plugins_annotation: "example.com/tolerate".to_string(),
                required_plugins: vec!["missing".to_string()],
                ..Default::default()
            },
            &req,
        )
        .expect("toleration should bypass required plugin check");
    }
}
