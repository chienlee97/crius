use std::collections::HashMap;

use crate::{
    crs::{
        args::{PodCreateArgs, SandboxSecurityArgs},
        parsers::{
            parse_id_mapping, parse_key_value, parse_port_mapping, parse_resource_spec,
            parse_security_profile, parse_selinux_option, parse_user, KeyValuePair, ParsedUser,
            ResourceFragment,
        },
    },
    proto::runtime::v1::{
        DnsConfig, Int64Value, LinuxContainerResources, LinuxPodSandboxConfig,
        LinuxSandboxSecurityContext, NamespaceMode, NamespaceOption, PodSandboxConfig,
        PodSandboxMetadata, UserNamespace,
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

fn build_sandbox_security_context(
    args: &PodCreateArgs,
    security: &SandboxSecurityArgs,
) -> Result<LinuxSandboxSecurityContext, String> {
    let (run_as_user, run_as_username) = match security.sandbox_user.as_deref() {
        Some(user) => match parse_user(user)? {
            ParsedUser::Id { uid, gid: _ } => (Some(Int64Value { value: uid }), String::new()),
            ParsedUser::Name(name) => (None, name),
        },
        None => (None, String::new()),
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
        run_as_group: security.sandbox_group.map(|group| Int64Value {
            value: i64::from(group),
        }),
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

fn build_resources_from_specs(
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::runtime::v1::{security_profile::ProfileType, Protocol};

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
}
