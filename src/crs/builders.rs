#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap};

use crate::{
    crs::{
        args::{
            ContainerCreateArgs, ContainerCreateOptions, ContainerResourceArgs,
            ContainerSecurityArgs, ImageAuthArgs, PodCreateArgs, SandboxSecurityArgs,
        },
        parsers::{
            parse_auth_json, parse_byte_size, parse_device, parse_env_file, parse_hugepage,
            parse_id_mapping, parse_key_value, parse_mount, parse_port_mapping,
            parse_resource_spec, parse_security_profile, parse_selinux_option, parse_user,
            KeyValuePair, ParsedUser, ResourceFragment,
        },
    },
    proto::runtime::v1::{
        AuthConfig, Capability, CdiDevice, ContainerConfig, ContainerMetadata, DnsConfig,
        ImageSpec, Int64Value, KeyValue, LinuxContainerConfig, LinuxContainerResources,
        LinuxContainerSecurityContext, LinuxPodSandboxConfig, LinuxSandboxSecurityContext,
        NamespaceMode, NamespaceOption, PodSandboxConfig, PodSandboxMetadata, UserNamespace,
    },
};

pub(crate) fn build_pod_sandbox_config(args: &PodCreateArgs) -> Result<PodSandboxConfig, String> {
    let metadata = PodSandboxMetadata {
        name: required_or_default(args.name.as_deref(), "pod name")?,
        uid: required_or_default(args.uid.as_deref(), "pod uid")?,
        namespace: args.namespace.clone().unwrap_or_else(|| "default".into()),
        attempt: args.attempt.unwrap_or_default(),
    };

    let linux = LinuxPodSandboxConfig {
        cgroup_parent: args.cgroup_parent.clone().unwrap_or_default(),
        security_context: Some(build_sandbox_security_context(args, &args.security)?),
        sysctls: key_value_map("--sysctl", &args.sysctls)?,
        overhead: build_resources_from_specs(&args.overhead)?,
        resources: build_resources_from_specs(&args.pod_resources)?,
    };

    Ok(PodSandboxConfig {
        metadata: Some(metadata),
        hostname: args.hostname.clone().unwrap_or_default(),
        log_directory: args.log_dir.clone().unwrap_or_default(),
        dns_config: Some(DnsConfig {
            servers: args.dns_servers.clone(),
            searches: args.dns_searches.clone(),
            options: args.dns_options.clone(),
        }),
        port_mappings: args
            .publish
            .iter()
            .map(|mapping| parse_port_mapping(mapping))
            .collect::<Result<Vec<_>, _>>()?,
        labels: key_value_map("--label", &args.labels)?,
        annotations: key_value_map("--annotation", &args.annotations)?,
        linux: Some(linux),
        windows: None,
    })
}

pub(crate) fn build_container_config(
    args: &ContainerCreateArgs,
) -> Result<ContainerConfig, String> {
    build_container_config_from_parts(&args.image, &args.command, &args.options)
}

pub(crate) fn build_auth_config(args: &ImageAuthArgs) -> Result<Option<AuthConfig>, String> {
    let sources = [
        args.auth_json.is_some(),
        args.auth_file.is_some(),
        args.username.is_some()
            || args.password.is_some()
            || args.server.is_some()
            || args.identity_token.is_some()
            || args.registry_token.is_some(),
    ]
    .into_iter()
    .filter(|present| *present)
    .count();

    if sources > 1 {
        return Err(
            "auth options must use only one source: --auth-json, --auth-file, or username flags"
                .into(),
        );
    }

    if let Some(value) = &args.auth_json {
        return parse_auth_json("--auth-json", value).map(Some);
    }

    if let Some(path) = &args.auth_file {
        let content = std::fs::read_to_string(path)
            .map_err(|error| format!("failed to read auth file \"{path}\": {error}"))?;
        return parse_auth_json(path, &content).map(Some);
    }

    if args.username.is_none()
        && args.password.is_none()
        && args.server.is_none()
        && args.identity_token.is_none()
        && args.registry_token.is_none()
    {
        return Ok(None);
    }

    Ok(Some(AuthConfig {
        username: args.username.clone().unwrap_or_default(),
        password: args.password.clone().unwrap_or_default(),
        auth: String::new(),
        server_address: args.server.clone().unwrap_or_default(),
        identity_token: args.identity_token.clone().unwrap_or_default(),
        registry_token: args.registry_token.clone().unwrap_or_default(),
    }))
}

pub(crate) fn parse_local_sysctls(sysctls: &[String]) -> Result<Vec<String>, String> {
    key_value_map("--sysctl", sysctls).map(|values| {
        values
            .into_iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect()
    })
}

fn build_container_config_from_parts(
    image: &str,
    positional_command: &[String],
    options: &ContainerCreateOptions,
) -> Result<ContainerConfig, String> {
    if image.is_empty() {
        return Err("container image must not be empty".into());
    }

    let metadata = ContainerMetadata {
        name: options
            .name
            .clone()
            .unwrap_or_else(|| default_container_name(image)),
        attempt: options.attempt.unwrap_or_default(),
    };
    if metadata.name.is_empty() {
        return Err("container name must not be empty".into());
    }

    let (command, args) = container_command_and_args(options, positional_command);
    let linux = LinuxContainerConfig {
        resources: build_container_resources(&options.resources)?,
        security_context: Some(build_container_security_context(&options.security)?),
    };

    Ok(ContainerConfig {
        metadata: Some(metadata),
        image: Some(ImageSpec {
            image: image.to_string(),
            user_specified_image: image.to_string(),
            ..Default::default()
        }),
        command,
        args,
        working_dir: options.workdir.clone().unwrap_or_default(),
        envs: build_envs(options)?,
        mounts: options
            .mounts
            .iter()
            .map(|mount| parse_mount(mount))
            .collect::<Result<Vec<_>, _>>()?,
        devices: options
            .devices
            .iter()
            .map(|device| parse_device(device))
            .collect::<Result<Vec<_>, _>>()?,
        labels: key_value_map("--label", &options.labels)?,
        annotations: build_container_annotations(options)?,
        log_path: options.log_path.clone().unwrap_or_default(),
        stdin: options.stdin,
        stdin_once: options.stdin,
        tty: options.tty,
        linux: Some(linux),
        windows: None,
        cdi_devices: options
            .cdi_devices
            .iter()
            .cloned()
            .map(|name| CdiDevice { name })
            .collect(),
    })
}

fn default_container_name(image: &str) -> String {
    image
        .rsplit(['/', ':', '@'])
        .find(|part| !part.is_empty())
        .unwrap_or("container")
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' || ch == '.' {
                ch
            } else {
                '-'
            }
        })
        .collect()
}

fn container_command_and_args(
    options: &ContainerCreateOptions,
    positional_command: &[String],
) -> (Vec<String>, Vec<String>) {
    if !options.commands.is_empty() {
        let mut args = options.args.clone();
        args.extend_from_slice(positional_command);
        (options.commands.clone(), args)
    } else if !positional_command.is_empty() {
        (positional_command.to_vec(), options.args.clone())
    } else {
        (Vec::new(), options.args.clone())
    }
}

fn build_envs(options: &ContainerCreateOptions) -> Result<Vec<KeyValue>, String> {
    let mut envs = BTreeMap::new();
    for file in &options.env_files {
        for pair in parse_env_file(file)? {
            envs.insert(pair.key, pair.value);
        }
    }
    for pair in options
        .env
        .iter()
        .map(|value| parse_key_value("--env", value))
        .collect::<Result<Vec<_>, _>>()?
    {
        envs.insert(pair.key, pair.value);
    }

    Ok(envs
        .into_iter()
        .map(|(key, value)| KeyValue { key, value })
        .collect())
}

fn build_container_annotations(
    options: &ContainerCreateOptions,
) -> Result<HashMap<String, String>, String> {
    let mut annotations = key_value_map("--annotation", &options.annotations)?;
    if let Some(value) = &options.security.blockio_class {
        annotations.insert("crius.io/blockio-class".into(), value.clone());
    }
    if let Some(value) = &options.security.rdt_class {
        annotations.insert("crius.io/rdt-class".into(), value.clone());
    }
    Ok(annotations)
}

fn build_container_resources(
    args: &ContainerResourceArgs,
) -> Result<Option<LinuxContainerResources>, String> {
    let mut fragment = ResourceFragment {
        cpu_period: args.cpu_period,
        cpu_quota: args.cpu_quota,
        cpu_shares: args.cpu_shares,
        memory_limit_in_bytes: args
            .memory
            .as_deref()
            .map(|value| parse_byte_size_as_i64("--memory", value))
            .transpose()?,
        memory_swap_limit_in_bytes: args
            .memory_swap
            .as_deref()
            .map(|value| parse_byte_size_as_i64("--memory-swap", value))
            .transpose()?,
        oom_score_adj: args.oom_score_adj,
        cpuset_cpus: args.cpuset_cpus.clone(),
        cpuset_mems: args.cpuset_mems.clone(),
        hugepages: args
            .hugepages
            .iter()
            .map(|value| parse_hugepage(value))
            .collect::<Result<Vec<_>, _>>()?,
        unified: args
            .unified
            .iter()
            .map(|value| parse_key_value("--unified", value))
            .collect::<Result<Vec<_>, _>>()?,
    };

    dedupe_resource_keys(&mut fragment);
    for value in &args.resources {
        merge_resource_fragment(&mut fragment, parse_resource_spec(value)?);
    }
    validate_resource_fragment(&fragment)?;
    if is_empty_resource_fragment(&fragment) {
        return Ok(None);
    }

    Ok(Some(resources_from_fragment(fragment)))
}

fn build_container_security_context(
    args: &ContainerSecurityArgs,
) -> Result<LinuxContainerSecurityContext, String> {
    let (run_as_user, run_as_group_from_user, run_as_username) = match args.user.as_deref() {
        Some(user) => match parse_user(user)? {
            ParsedUser::Id { uid, gid } => (
                Some(Int64Value { value: uid }),
                gid.map(|value| Int64Value { value }),
                String::new(),
            ),
            ParsedUser::Name(name) => (None, None, name),
        },
        None => (None, None, String::new()),
    };

    if args.group.is_some() && run_as_user.is_none() && run_as_username.is_empty() {
        return Err("--group requires --user".into());
    }

    Ok(LinuxContainerSecurityContext {
        capabilities: if args.cap_add.is_empty()
            && args.cap_drop.is_empty()
            && args.ambient_cap_add.is_empty()
        {
            None
        } else {
            Some(Capability {
                add_capabilities: args.cap_add.clone(),
                drop_capabilities: args.cap_drop.clone(),
                add_ambient_capabilities: args.ambient_cap_add.clone(),
            })
        },
        privileged: args.privileged,
        namespace_options: Some(build_container_namespace_options(args)?),
        selinux_options: optional_profile(args.selinux.as_deref(), parse_selinux_option)?,
        run_as_user,
        run_as_group: args
            .group
            .as_deref()
            .map(|value| parse_non_negative_i64("--group", value))
            .transpose()?
            .map(|value| Int64Value { value })
            .or(run_as_group_from_user),
        run_as_username,
        readonly_rootfs: args.readonly_rootfs,
        supplemental_groups: args
            .supplemental_groups
            .iter()
            .map(|value| parse_non_negative_i64("--supplemental-group", value))
            .collect::<Result<Vec<_>, _>>()?,
        no_new_privs: args.no_new_privs,
        masked_paths: args.masked_paths.clone(),
        readonly_paths: args.readonly_paths.clone(),
        seccomp: optional_profile(args.seccomp.as_deref(), parse_security_profile)?,
        apparmor: optional_profile(args.apparmor.as_deref(), parse_security_profile)?,
        ..Default::default()
    })
}

fn build_container_namespace_options(
    args: &ContainerSecurityArgs,
) -> Result<NamespaceOption, String> {
    Ok(NamespaceOption {
        network: NamespaceMode::Pod as i32,
        pid: parse_namespace_mode("--pid", args.pid.as_deref(), NamespaceMode::Container)?,
        ipc: parse_namespace_mode("--ipc", args.ipc.as_deref(), NamespaceMode::Pod)?,
        target_id: String::new(),
        userns_options: None,
    })
}

fn build_sandbox_security_context(
    args: &PodCreateArgs,
    security: &SandboxSecurityArgs,
) -> Result<LinuxSandboxSecurityContext, String> {
    let (run_as_user, run_as_group_from_user, run_as_username) =
        match security.sandbox_user.as_deref() {
            Some(user) => match parse_user(user)? {
                ParsedUser::Id { uid, gid } => (
                    Some(Int64Value { value: uid }),
                    gid.map(|value| Int64Value { value }),
                    String::new(),
                ),
                ParsedUser::Name(name) => (None, None, name),
            },
            None => (None, None, String::new()),
        };

    if security.sandbox_group.is_some() && run_as_user.is_none() && run_as_username.is_empty() {
        return Err("--sandbox-group requires --sandbox-user".into());
    }

    Ok(LinuxSandboxSecurityContext {
        namespace_options: Some(build_pod_namespace_options(args)?),
        selinux_options: optional_profile(
            security.sandbox_selinux.as_deref(),
            parse_selinux_option,
        )?,
        run_as_user,
        run_as_group: security
            .sandbox_group
            .map(|group| Int64Value {
                value: i64::from(group),
            })
            .or(run_as_group_from_user),
        readonly_rootfs: security.sandbox_readonly_rootfs,
        supplemental_groups: security
            .sandbox_supplemental_groups
            .iter()
            .map(|group| i64::from(*group))
            .collect(),
        privileged: security.sandbox_privileged,
        seccomp: optional_profile(security.sandbox_seccomp.as_deref(), parse_security_profile)?,
        apparmor: optional_profile(security.sandbox_apparmor.as_deref(), parse_security_profile)?,
        ..Default::default()
    })
}

fn build_pod_namespace_options(args: &PodCreateArgs) -> Result<NamespaceOption, String> {
    Ok(NamespaceOption {
        network: if args.host_network {
            NamespaceMode::Node
        } else {
            NamespaceMode::Pod
        } as i32,
        pid: if args.host_pid {
            NamespaceMode::Node
        } else {
            NamespaceMode::Pod
        } as i32,
        ipc: if args.host_ipc {
            NamespaceMode::Node
        } else {
            NamespaceMode::Pod
        } as i32,
        target_id: String::new(),
        userns_options: build_user_namespace(args)?,
    })
}

fn build_user_namespace(args: &PodCreateArgs) -> Result<Option<UserNamespace>, String> {
    let mode = match args.userns.as_deref() {
        None => {
            if args.uid_maps.is_empty() && args.gid_maps.is_empty() {
                return Ok(None);
            }
            NamespaceMode::Pod
        }
        Some("pod") => NamespaceMode::Pod,
        Some("node") => NamespaceMode::Node,
        Some(value) => {
            return Err(format!("invalid userns \"{value}\": expected pod or node"));
        }
    };

    Ok(Some(UserNamespace {
        mode: mode as i32,
        uids: args
            .uid_maps
            .iter()
            .map(|mapping| parse_id_mapping(mapping))
            .collect::<Result<Vec<_>, _>>()?,
        gids: args
            .gid_maps
            .iter()
            .map(|mapping| parse_id_mapping(mapping))
            .collect::<Result<Vec<_>, _>>()?,
    }))
}

pub(crate) fn build_resources_from_specs(
    values: &[String],
) -> Result<Option<LinuxContainerResources>, String> {
    if values.is_empty() {
        return Ok(None);
    }

    let mut merged = ResourceFragment::default();
    for value in values {
        merge_resource_fragment(&mut merged, parse_resource_spec(value)?);
    }
    validate_resource_fragment(&merged)?;

    Ok(Some(resources_from_fragment(merged)))
}

fn validate_resource_fragment(fragment: &ResourceFragment) -> Result<(), String> {
    let fields = [
        ("cpu-period", fragment.cpu_period),
        ("cpu-quota", fragment.cpu_quota),
        ("cpu-shares", fragment.cpu_shares),
        ("memory", fragment.memory_limit_in_bytes),
        ("memory-swap", fragment.memory_swap_limit_in_bytes),
        ("oom-score-adj", fragment.oom_score_adj),
    ];

    for (name, value) in fields {
        if value.is_some_and(|value| value < 0) {
            return Err(format!("resource field {name} must be non-negative"));
        }
    }

    Ok(())
}

fn is_empty_resource_fragment(fragment: &ResourceFragment) -> bool {
    fragment.cpu_period.is_none()
        && fragment.cpu_quota.is_none()
        && fragment.cpu_shares.is_none()
        && fragment.memory_limit_in_bytes.is_none()
        && fragment.memory_swap_limit_in_bytes.is_none()
        && fragment.oom_score_adj.is_none()
        && fragment.cpuset_cpus.is_none()
        && fragment.cpuset_mems.is_none()
        && fragment.hugepages.is_empty()
        && fragment.unified.is_empty()
}

fn merge_resource_fragment(target: &mut ResourceFragment, fragment: ResourceFragment) {
    if fragment.cpu_period.is_some() {
        target.cpu_period = fragment.cpu_period;
    }
    if fragment.cpu_quota.is_some() {
        target.cpu_quota = fragment.cpu_quota;
    }
    if fragment.cpu_shares.is_some() {
        target.cpu_shares = fragment.cpu_shares;
    }
    if fragment.memory_limit_in_bytes.is_some() {
        target.memory_limit_in_bytes = fragment.memory_limit_in_bytes;
    }
    if fragment.memory_swap_limit_in_bytes.is_some() {
        target.memory_swap_limit_in_bytes = fragment.memory_swap_limit_in_bytes;
    }
    if fragment.oom_score_adj.is_some() {
        target.oom_score_adj = fragment.oom_score_adj;
    }
    if fragment.cpuset_cpus.is_some() {
        target.cpuset_cpus = fragment.cpuset_cpus;
    }
    if fragment.cpuset_mems.is_some() {
        target.cpuset_mems = fragment.cpuset_mems;
    }
    merge_keyed(&mut target.hugepages, fragment.hugepages, |item| {
        item.page_size.as_str()
    });
    merge_keyed(&mut target.unified, fragment.unified, |item| {
        item.key.as_str()
    });
}

fn dedupe_resource_keys(fragment: &mut ResourceFragment) {
    let hugepages = std::mem::take(&mut fragment.hugepages);
    merge_keyed(&mut fragment.hugepages, hugepages, |item| {
        item.page_size.as_str()
    });
    let unified = std::mem::take(&mut fragment.unified);
    merge_keyed(&mut fragment.unified, unified, |item| item.key.as_str());
}

fn resources_from_fragment(fragment: ResourceFragment) -> LinuxContainerResources {
    LinuxContainerResources {
        cpu_period: fragment.cpu_period.unwrap_or_default(),
        cpu_quota: fragment.cpu_quota.unwrap_or_default(),
        cpu_shares: fragment.cpu_shares.unwrap_or_default(),
        memory_limit_in_bytes: fragment.memory_limit_in_bytes.unwrap_or_default(),
        oom_score_adj: fragment.oom_score_adj.unwrap_or_default(),
        cpuset_cpus: fragment.cpuset_cpus.unwrap_or_default(),
        cpuset_mems: fragment.cpuset_mems.unwrap_or_default(),
        hugepage_limits: fragment.hugepages,
        unified: fragment
            .unified
            .into_iter()
            .map(|pair| (pair.key, pair.value))
            .collect(),
        memory_swap_limit_in_bytes: fragment.memory_swap_limit_in_bytes.unwrap_or_default(),
    }
}

fn key_value_map(flag: &str, values: &[String]) -> Result<HashMap<String, String>, String> {
    values
        .iter()
        .map(|value| parse_key_value(flag, value))
        .collect::<Result<Vec<_>, _>>()
        .map(key_value_pairs_to_map)
}

fn key_value_pairs_to_map(pairs: Vec<KeyValuePair>) -> HashMap<String, String> {
    pairs
        .into_iter()
        .map(|pair| (pair.key, pair.value))
        .collect()
}

fn required_or_default(value: Option<&str>, name: &str) -> Result<String, String> {
    match value {
        Some(value) if !value.is_empty() => Ok(value.to_string()),
        Some(_) => Err(format!("{name} must not be empty")),
        None => Ok(String::new()),
    }
}

fn optional_profile<T>(
    value: Option<&str>,
    parse: impl FnOnce(&str) -> Result<T, String>,
) -> Result<Option<T>, String> {
    value.map(parse).transpose()
}

fn merge_keyed<T>(target: &mut Vec<T>, values: Vec<T>, key: impl Fn(&T) -> &str) {
    for value in values {
        if let Some(index) = target.iter().position(|item| key(item) == key(&value)) {
            target[index] = value;
        } else {
            target.push(value);
        }
    }
}

fn parse_namespace_mode(
    flag: &str,
    value: Option<&str>,
    default: NamespaceMode,
) -> Result<i32, String> {
    let mode = match value {
        None => default,
        Some("pod") => NamespaceMode::Pod,
        Some("container") => NamespaceMode::Container,
        Some("node") => NamespaceMode::Node,
        Some(value) => {
            return Err(format!(
                "invalid {flag} \"{value}\": expected pod, container, or node"
            ));
        }
    };
    Ok(mode as i32)
}

fn parse_non_negative_i64(flag: &str, value: &str) -> Result<i64, String> {
    let parsed = value
        .parse::<i64>()
        .map_err(|_| format!("invalid {flag} \"{value}\": expected non-negative integer"))?;
    if parsed < 0 {
        return Err(format!(
            "invalid {flag} \"{value}\": expected non-negative integer"
        ));
    }
    Ok(parsed)
}

fn parse_byte_size_as_i64(flag: &str, value: &str) -> Result<i64, String> {
    let bytes = parse_byte_size(value)?;
    i64::try_from(bytes).map_err(|_| format!("invalid {flag} \"{value}\": value is out of range"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::runtime::v1::{security_profile::ProfileType, MountPropagation, Protocol};
    use tempfile::NamedTempFile;

    #[test]
    fn builds_pod_basic_fields_and_maps() {
        let args = PodCreateArgs {
            name: Some("demo".into()),
            uid: Some("uid-1".into()),
            namespace: Some("ns".into()),
            attempt: Some(3),
            hostname: Some("host".into()),
            log_dir: Some("/var/log/pods/demo".into()),
            labels: vec!["app=old".into(), "app=new".into()],
            annotations: vec!["trace=a".into(), "trace=b".into()],
            ..Default::default()
        };

        let config = build_pod_sandbox_config(&args).unwrap();
        let metadata = config.metadata.unwrap();

        assert_eq!(metadata.name, "demo");
        assert_eq!(metadata.uid, "uid-1");
        assert_eq!(metadata.namespace, "ns");
        assert_eq!(metadata.attempt, 3);
        assert_eq!(config.hostname, "host");
        assert_eq!(config.log_directory, "/var/log/pods/demo");
        assert_eq!(config.labels["app"], "new");
        assert_eq!(config.annotations["trace"], "b");
    }

    #[test]
    fn builds_pod_network_and_runtime_fields() {
        let args = PodCreateArgs {
            dns_servers: vec!["1.1.1.1".into()],
            dns_searches: vec!["svc.cluster.local".into()],
            dns_options: vec!["ndots:5".into()],
            publish: vec!["127.0.0.1:8080:80".into(), "8443:443/udp".into()],
            ..Default::default()
        };

        let config = build_pod_sandbox_config(&args).unwrap();
        let dns = config.dns_config.unwrap();

        assert_eq!(dns.servers, ["1.1.1.1"]);
        assert_eq!(dns.searches, ["svc.cluster.local"]);
        assert_eq!(dns.options, ["ndots:5"]);
        assert_eq!(config.port_mappings[0].host_ip, "127.0.0.1");
        assert_eq!(config.port_mappings[0].host_port, 8080);
        assert_eq!(config.port_mappings[0].container_port, 80);
        assert_eq!(config.port_mappings[0].protocol, Protocol::Tcp as i32);
        assert_eq!(config.port_mappings[1].protocol, Protocol::Udp as i32);
    }

    #[test]
    fn builds_pod_namespace_and_userns_options() {
        let args = PodCreateArgs {
            host_network: true,
            host_pid: true,
            host_ipc: true,
            userns: Some("pod".into()),
            uid_maps: vec!["1000:0:1".into()],
            gid_maps: vec!["1000:0:1".into()],
            ..Default::default()
        };

        let config = build_pod_sandbox_config(&args).unwrap();
        let namespace = config
            .linux
            .unwrap()
            .security_context
            .unwrap()
            .namespace_options
            .unwrap();
        let userns = namespace.userns_options.unwrap();

        assert_eq!(namespace.network, NamespaceMode::Node as i32);
        assert_eq!(namespace.pid, NamespaceMode::Node as i32);
        assert_eq!(namespace.ipc, NamespaceMode::Node as i32);
        assert_eq!(userns.mode, NamespaceMode::Pod as i32);
        assert_eq!(userns.uids[0].host_id, 1000);
        assert_eq!(userns.uids[0].container_id, 0);
        assert_eq!(userns.gids[0].length, 1);
    }

    #[test]
    fn builds_sandbox_security_context() {
        let args = PodCreateArgs {
            security: SandboxSecurityArgs {
                sandbox_user: Some("1000".into()),
                sandbox_group: Some(1001),
                sandbox_supplemental_groups: vec![44, 55],
                sandbox_readonly_rootfs: true,
                sandbox_privileged: true,
                sandbox_seccomp: Some("runtime/default".into()),
                sandbox_apparmor: Some("localhost:profile".into()),
                sandbox_selinux: Some("user:role:type:level".into()),
            },
            ..Default::default()
        };

        let config = build_pod_sandbox_config(&args).unwrap();
        let security = config.linux.unwrap().security_context.unwrap();

        assert_eq!(security.run_as_user.unwrap().value, 1000);
        assert_eq!(security.run_as_group.unwrap().value, 1001);
        assert_eq!(security.supplemental_groups, [44, 55]);
        assert!(security.readonly_rootfs);
        assert!(security.privileged);
        assert_eq!(
            security.seccomp.unwrap().profile_type,
            ProfileType::RuntimeDefault as i32
        );
        assert_eq!(security.apparmor.unwrap().localhost_ref, "profile");
        assert_eq!(security.selinux_options.unwrap().r#type, "type");
    }

    #[test]
    fn rejects_sandbox_group_without_user() {
        let args = PodCreateArgs {
            security: SandboxSecurityArgs {
                sandbox_group: Some(1001),
                ..Default::default()
            },
            ..Default::default()
        };

        let error = build_pod_sandbox_config(&args).unwrap_err();
        assert!(error.contains("--sandbox-group requires --sandbox-user"));
    }

    #[test]
    fn builds_pod_resources_and_sysctls() {
        let args = PodCreateArgs {
            sysctls: vec![
                "net.ipv4.ip_forward=0".into(),
                "net.ipv4.ip_forward=1".into(),
            ],
            overhead: vec!["cpu=100,memory=64MiB,unified=memory.max=10".into()],
            pod_resources: vec![
                "hugepage=2MiB=1MiB,unified=io.weight=100".into(),
                "hugepage=2MiB=2MiB,unified=io.weight=200".into(),
            ],
            ..Default::default()
        };

        let config = build_pod_sandbox_config(&args).unwrap();
        let linux = config.linux.unwrap();
        let overhead = linux.overhead.unwrap();
        let resources = linux.resources.unwrap();

        assert_eq!(linux.sysctls["net.ipv4.ip_forward"], "1");
        assert_eq!(overhead.cpu_quota, 100);
        assert_eq!(overhead.memory_limit_in_bytes, 64 * 1024 * 1024);
        assert_eq!(overhead.unified["memory.max"], "10");
        assert_eq!(resources.hugepage_limits.len(), 1);
        assert_eq!(resources.hugepage_limits[0].limit, 2 * 1024 * 1024);
        assert_eq!(resources.unified["io.weight"], "200");
    }

    #[test]
    fn rejects_negative_resource_values() {
        let args = PodCreateArgs {
            overhead: vec!["cpu=-1".into()],
            ..Default::default()
        };

        let error = build_pod_sandbox_config(&args).unwrap_err();
        assert!(error.contains("must be non-negative") || error.contains("expected"));
    }

    #[test]
    fn builds_container_basic_fields() {
        let args = ContainerCreateArgs {
            pod: "pod1".into(),
            image: "registry.example.com/library/busybox:latest".into(),
            command: vec!["echo".into(), "ok".into()],
            options: ContainerCreateOptions {
                name: Some("ctr".into()),
                attempt: Some(2),
                commands: vec!["/bin/sh".into()],
                args: vec!["-c".into()],
                workdir: Some("/work".into()),
                stdin: true,
                tty: true,
                ..Default::default()
            },
        };

        let config = build_container_config(&args).unwrap();
        let metadata = config.metadata.unwrap();
        let image = config.image.unwrap();

        assert_eq!(metadata.name, "ctr");
        assert_eq!(metadata.attempt, 2);
        assert_eq!(image.image, "registry.example.com/library/busybox:latest");
        assert_eq!(image.user_specified_image, image.image);
        assert_eq!(config.command, ["/bin/sh"]);
        assert_eq!(config.args, ["-c", "echo", "ok"]);
        assert_eq!(config.working_dir, "/work");
        assert!(config.stdin);
        assert!(config.stdin_once);
        assert!(config.tty);
    }

    #[test]
    fn builds_container_default_name_from_image() {
        let args = ContainerCreateArgs {
            pod: "pod1".into(),
            image: "registry.example.com/ns/app@sha256:abcd".into(),
            command: Vec::new(),
            options: ContainerCreateOptions::default(),
        };

        let config = build_container_config(&args).unwrap();

        assert_eq!(config.metadata.unwrap().name, "abcd");
    }

    #[test]
    fn builds_container_env_labels_and_annotations() {
        let mut file = NamedTempFile::new().unwrap();
        std::io::Write::write_all(&mut file, b"A=file\nB=file\n# ignored\n").unwrap();

        let args = ContainerCreateArgs {
            pod: "pod1".into(),
            image: "busybox".into(),
            command: Vec::new(),
            options: ContainerCreateOptions {
                env_files: vec![file.path().display().to_string()],
                env: vec!["B=flag".into(), "C=flag".into()],
                labels: vec!["app=old".into(), "app=new".into()],
                annotations: vec!["anno=old".into(), "anno=new".into()],
                ..Default::default()
            },
        };

        let config = build_container_config(&args).unwrap();
        let envs: BTreeMap<_, _> = config
            .envs
            .into_iter()
            .map(|env| (env.key, env.value))
            .collect();

        assert_eq!(envs["A"], "file");
        assert_eq!(envs["B"], "flag");
        assert_eq!(envs["C"], "flag");
        assert_eq!(config.labels["app"], "new");
        assert_eq!(config.annotations["anno"], "new");
    }

    #[test]
    fn builds_container_mounts_devices_cdi_and_log_path() {
        let args = ContainerCreateArgs {
            pod: "pod1".into(),
            image: "busybox".into(),
            command: Vec::new(),
            options: ContainerCreateOptions {
                mounts: vec![
                    "type=bind,src=/host,dst=/ctr,ro,recursive-ro".into(),
                    "type=image,image=alpine,dst=/image,subpath=bin".into(),
                ],
                devices: vec!["/dev/fuse:/dev/fuse:rwm".into()],
                cdi_devices: vec!["vendor.com/device=name".into()],
                log_path: Some("ctr/0.log".into()),
                ..Default::default()
            },
        };

        let config = build_container_config(&args).unwrap();

        assert_eq!(config.mounts[0].host_path, "/host");
        assert_eq!(config.mounts[0].container_path, "/ctr");
        assert!(config.mounts[0].readonly);
        assert!(config.mounts[0].recursive_read_only);
        assert_eq!(
            config.mounts[0].propagation,
            MountPropagation::PropagationPrivate as i32
        );
        assert_eq!(config.mounts[1].image.as_ref().unwrap().image, "alpine");
        assert_eq!(config.mounts[1].image_sub_path, "bin");
        assert_eq!(config.devices[0].permissions, "rwm");
        assert_eq!(config.cdi_devices[0].name, "vendor.com/device=name");
        assert_eq!(config.log_path, "ctr/0.log");
    }

    #[test]
    fn builds_container_resources() {
        let args = ContainerCreateArgs {
            pod: "pod1".into(),
            image: "busybox".into(),
            command: Vec::new(),
            options: ContainerCreateOptions {
                resources: ContainerResourceArgs {
                    resources: vec!["cpu=3000,unified=memory.max=33554432".into()],
                    cpu_period: Some(1000),
                    cpu_quota: Some(2000),
                    cpu_shares: Some(512),
                    memory: Some("64MiB".into()),
                    memory_swap: Some("128MiB".into()),
                    oom_score_adj: Some(10),
                    cpuset_cpus: Some("0-1".into()),
                    cpuset_mems: Some("0".into()),
                    hugepages: vec!["2MiB=1MiB".into()],
                    unified: vec!["memory.max=67108864".into()],
                },
                ..Default::default()
            },
        };

        let config = build_container_config(&args).unwrap();
        let resources = config.linux.unwrap().resources.unwrap();

        assert_eq!(resources.cpu_period, 1000);
        assert_eq!(resources.cpu_quota, 3000);
        assert_eq!(resources.cpu_shares, 512);
        assert_eq!(resources.memory_limit_in_bytes, 64 * 1024 * 1024);
        assert_eq!(resources.memory_swap_limit_in_bytes, 128 * 1024 * 1024);
        assert_eq!(resources.oom_score_adj, 10);
        assert_eq!(resources.cpuset_cpus, "0-1");
        assert_eq!(resources.cpuset_mems, "0");
        assert_eq!(resources.hugepage_limits[0].page_size, "2MiB");
        assert_eq!(resources.unified["memory.max"], "33554432");
    }

    #[test]
    fn builds_container_security_context() {
        let args = ContainerCreateArgs {
            pod: "pod1".into(),
            image: "busybox".into(),
            command: Vec::new(),
            options: ContainerCreateOptions {
                security: ContainerSecurityArgs {
                    privileged: true,
                    cap_add: vec!["NET_ADMIN".into()],
                    cap_drop: vec!["MKNOD".into()],
                    ambient_cap_add: vec!["CHOWN".into()],
                    user: Some("1000:1001".into()),
                    group: Some("1002".into()),
                    supplemental_groups: vec!["44".into(), "55".into()],
                    readonly_rootfs: true,
                    no_new_privs: true,
                    masked_paths: vec!["/proc/acpi".into()],
                    readonly_paths: vec!["/proc/sys".into()],
                    seccomp: Some("runtime/default".into()),
                    apparmor: Some("localhost:profile".into()),
                    selinux: Some("user:role:type:level".into()),
                    pid: Some("node".into()),
                    ipc: Some("container".into()),
                    blockio_class: Some("latency".into()),
                    rdt_class: Some("gold".into()),
                },
                ..Default::default()
            },
        };

        let config = build_container_config(&args).unwrap();
        let annotations = config.annotations;
        let security = config.linux.unwrap().security_context.unwrap();
        let capabilities = security.capabilities.unwrap();
        let namespaces = security.namespace_options.unwrap();

        assert!(security.privileged);
        assert_eq!(capabilities.add_capabilities, ["NET_ADMIN"]);
        assert_eq!(capabilities.drop_capabilities, ["MKNOD"]);
        assert_eq!(capabilities.add_ambient_capabilities, ["CHOWN"]);
        assert_eq!(security.run_as_user.unwrap().value, 1000);
        assert_eq!(security.run_as_group.unwrap().value, 1002);
        assert_eq!(security.supplemental_groups, [44, 55]);
        assert!(security.readonly_rootfs);
        assert!(security.no_new_privs);
        assert_eq!(security.masked_paths, ["/proc/acpi"]);
        assert_eq!(security.readonly_paths, ["/proc/sys"]);
        assert_eq!(
            security.seccomp.unwrap().profile_type,
            ProfileType::RuntimeDefault as i32
        );
        assert_eq!(security.apparmor.unwrap().localhost_ref, "profile");
        assert_eq!(security.selinux_options.unwrap().level, "level");
        assert_eq!(namespaces.pid, NamespaceMode::Node as i32);
        assert_eq!(namespaces.ipc, NamespaceMode::Container as i32);
        assert_eq!(annotations["crius.io/blockio-class"], "latency");
        assert_eq!(annotations["crius.io/rdt-class"], "gold");
    }

    #[test]
    fn rejects_container_group_without_user() {
        let args = ContainerCreateArgs {
            pod: "pod1".into(),
            image: "busybox".into(),
            command: Vec::new(),
            options: ContainerCreateOptions {
                security: ContainerSecurityArgs {
                    group: Some("1000".into()),
                    ..Default::default()
                },
                ..Default::default()
            },
        };

        let error = build_container_config(&args).unwrap_err();
        assert!(error.contains("--group requires --user"));
    }

    #[test]
    fn builds_auth_config_from_inline_json() {
        let args = ImageAuthArgs {
            auth_json: Some(
                r#"{"username":"alice","password":"secret","serverAddress":"registry.example"}"#
                    .into(),
            ),
            ..Default::default()
        };

        let auth = build_auth_config(&args).unwrap().unwrap();

        assert_eq!(auth.username, "alice");
        assert_eq!(auth.password, "secret");
        assert_eq!(auth.server_address, "registry.example");
    }

    #[test]
    fn builds_auth_config_from_file() {
        let mut file = NamedTempFile::new().unwrap();
        std::io::Write::write_all(
            &mut file,
            br#"{"auths":{"registry.example":{"auth":"dXNlcjpzZWNyZXQ="}}}"#,
        )
        .unwrap();
        let args = ImageAuthArgs {
            auth_file: Some(file.path().display().to_string()),
            ..Default::default()
        };

        let auth = build_auth_config(&args).unwrap().unwrap();

        assert_eq!(auth.server_address, "registry.example");
        assert_eq!(auth.username, "user");
        assert_eq!(auth.password, "secret");
    }

    #[test]
    fn builds_auth_config_from_discrete_flags() {
        let args = ImageAuthArgs {
            username: Some("alice".into()),
            password: Some("secret".into()),
            server: Some("registry.example".into()),
            identity_token: Some("identity".into()),
            registry_token: Some("registry-token".into()),
            ..Default::default()
        };

        let auth = build_auth_config(&args).unwrap().unwrap();

        assert_eq!(auth.username, "alice");
        assert_eq!(auth.password, "secret");
        assert_eq!(auth.server_address, "registry.example");
        assert_eq!(auth.identity_token, "identity");
        assert_eq!(auth.registry_token, "registry-token");
    }

    #[test]
    fn rejects_auth_source_conflicts() {
        let args = ImageAuthArgs {
            auth_json: Some(r#"{"username":"alice"}"#.into()),
            username: Some("bob".into()),
            ..Default::default()
        };

        let error = build_auth_config(&args).unwrap_err();

        assert!(error.contains("only one source"));
        assert!(!error.contains("alice"));
        assert!(!error.contains("bob"));
    }

    #[test]
    fn auth_file_read_error_does_not_include_secret_flags() {
        let args = ImageAuthArgs {
            auth_file: Some("/does/not/exist".into()),
            ..Default::default()
        };

        let error = build_auth_config(&args).unwrap_err();

        assert!(error.contains("failed to read auth file"));
        assert!(!error.contains("secret"));
        assert!(!error.contains("token"));
    }
}
