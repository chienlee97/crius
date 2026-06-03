use std::{fs, net::IpAddr, path::Path, time::Duration};

use base64::Engine;

use crate::proto::runtime::v1::{
    AuthConfig, Device, HugepageLimit, IdMapping, ImageSpec, Mount, MountPropagation, PortMapping,
    Protocol, SeLinuxOption, SecurityProfile,
};

pub(crate) const DEFAULT_ENDPOINT: &str = "unix:///run/crius/crius.sock";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Endpoint {
    Unix(String),
    Tcp(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct KeyValuePair {
    pub(crate) key: String,
    pub(crate) value: String,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct ResourceFragment {
    pub(crate) cpu_period: Option<i64>,
    pub(crate) cpu_quota: Option<i64>,
    pub(crate) cpu_shares: Option<i64>,
    pub(crate) memory_limit_in_bytes: Option<i64>,
    pub(crate) memory_swap_limit_in_bytes: Option<i64>,
    pub(crate) oom_score_adj: Option<i64>,
    pub(crate) cpuset_cpus: Option<String>,
    pub(crate) cpuset_mems: Option<String>,
    pub(crate) hugepages: Vec<HugepageLimit>,
    pub(crate) unified: Vec<KeyValuePair>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ParsedUser {
    Id { uid: i64, gid: Option<i64> },
    Name(String),
}

impl std::fmt::Display for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unix(path) => write!(f, "unix://{path}"),
            Self::Tcp(uri) => f.write_str(uri),
        }
    }
}

pub(crate) fn parse_endpoint(value: &str) -> Result<Endpoint, String> {
    let value = value.trim();
    if value.is_empty() {
        return Err("invalid endpoint \"\": expected unix path, unix://, http://, or https://".into());
    }

    if let Some(path) = value.strip_prefix("unix://") {
        if path.is_empty() {
            return Err(format!(
                "invalid endpoint \"{value}\": unix endpoint requires a socket path"
            ));
        }
        return Ok(Endpoint::Unix(path.to_string()));
    }

    if value.starts_with('/') {
        return Ok(Endpoint::Unix(value.to_string()));
    }

    if value.starts_with("http://") || value.starts_with("https://") {
        return Ok(Endpoint::Tcp(value.to_string()));
    }

    Err(format!(
        "invalid endpoint \"{value}\": expected unix path, unix://, http://, or https://"
    ))
}

pub(crate) fn parse_duration(value: &str) -> Result<Duration, String> {
    let trimmed = value.trim();
    let split_at = trimmed
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(trimmed.len());
    let (digits, unit) = trimmed.split_at(split_at);

    if digits.is_empty() || unit.is_empty() {
        return Err(format!(
            "invalid duration \"{value}\": expected an integer followed by ms, s, m, or h"
        ));
    }

    let amount = digits
        .parse::<u64>()
        .map_err(|_| format!("invalid duration \"{value}\": value is out of range"))?;

    match unit {
        "ms" => Ok(Duration::from_millis(amount)),
        "s" => Ok(Duration::from_secs(amount)),
        "m" => amount
            .checked_mul(60)
            .map(Duration::from_secs)
            .ok_or_else(|| format!("invalid duration \"{value}\": value is out of range")),
        "h" => amount
            .checked_mul(60 * 60)
            .map(Duration::from_secs)
            .ok_or_else(|| format!("invalid duration \"{value}\": value is out of range")),
        _ => Err(format!(
            "invalid duration \"{value}\": expected an integer followed by ms, s, m, or h"
        )),
    }
}

#[allow(dead_code)]
pub(crate) fn parse_byte_size(value: &str) -> Result<u64, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(format!(
            "invalid byte size \"{value}\": expected an integer optionally followed by B, KiB, MiB, GiB, or TiB"
        ));
    }

    let split_at = trimmed
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(trimmed.len());
    let (digits, unit) = trimmed.split_at(split_at);

    if digits.is_empty() {
        return Err(format!(
            "invalid byte size \"{value}\": expected an integer optionally followed by B, KiB, MiB, GiB, or TiB"
        ));
    }

    let amount = digits
        .parse::<u64>()
        .map_err(|_| format!("invalid byte size \"{value}\": value is out of range"))?;

    match unit {
        "" | "B" => Ok(amount),
        "KiB" => amount
            .checked_mul(1024)
            .ok_or_else(|| format!("invalid byte size \"{value}\": value is out of range")),
        "MiB" => amount
            .checked_mul(1024 * 1024)
            .ok_or_else(|| format!("invalid byte size \"{value}\": value is out of range")),
        "GiB" => amount
            .checked_mul(1024 * 1024 * 1024)
            .ok_or_else(|| format!("invalid byte size \"{value}\": value is out of range")),
        "TiB" => amount
            .checked_mul(1024 * 1024 * 1024 * 1024)
            .ok_or_else(|| format!("invalid byte size \"{value}\": value is out of range")),
        _ => Err(format!(
            "invalid byte size \"{value}\": expected an integer optionally followed by B, KiB, MiB, GiB, or TiB"
        )),
    }
}

#[allow(dead_code)]
pub(crate) fn parse_key_value(flag: &str, value: &str) -> Result<KeyValuePair, String> {
    let Some((key, parsed_value)) = value.split_once('=') else {
        return Err(format!(
            "invalid {flag} value \"{value}\": expected KEY=VALUE"
        ));
    };

    if key.is_empty() {
        return Err(format!(
            "invalid {flag} value \"{value}\": key must not be empty"
        ));
    }

    Ok(KeyValuePair {
        key: key.to_string(),
        value: parsed_value.to_string(),
    })
}

#[allow(dead_code)]
pub(crate) fn parse_env_file(path: impl AsRef<Path>) -> Result<Vec<KeyValuePair>, String> {
    let path = path.as_ref();
    let source = path.display().to_string();
    let content = fs::read_to_string(path)
        .map_err(|error| format!("failed to read env file \"{source}\": {error}"))?;

    content
        .lines()
        .enumerate()
        .filter_map(|(index, line)| {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                None
            } else {
                Some((index + 1, line))
            }
        })
        .map(|(line_number, line)| {
            parse_key_value("env file", line).map_err(|error| {
                format!("invalid env file \"{source}\" line {line_number}: {error}")
            })
        })
        .collect()
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct AuthConfigJson {
    #[serde(default)]
    username: String,
    #[serde(default)]
    password: String,
    #[serde(default)]
    auth: String,
    #[serde(default, alias = "server")]
    server_address: String,
    #[serde(default)]
    identity_token: String,
    #[serde(default)]
    registry_token: String,
}

#[derive(serde::Deserialize)]
struct DockerAuthFile {
    auths: std::collections::BTreeMap<String, DockerAuthEntry>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct DockerAuthEntry {
    #[serde(default)]
    username: String,
    #[serde(default)]
    password: String,
    #[serde(default)]
    auth: String,
    #[serde(default)]
    identity_token: String,
    #[serde(default)]
    registry_token: String,
}

impl From<AuthConfigJson> for AuthConfig {
    fn from(value: AuthConfigJson) -> Self {
        Self {
            username: value.username,
            password: value.password,
            auth: value.auth,
            server_address: value.server_address,
            identity_token: value.identity_token,
            registry_token: value.registry_token,
        }
    }
}

#[allow(dead_code)]
pub(crate) fn parse_auth_json(source: &str, value: &str) -> Result<AuthConfig, String> {
    if let Ok(config) = serde_json::from_str::<AuthConfigJson>(value) {
        return Ok(config.into());
    }

    let docker = serde_json::from_str::<DockerAuthFile>(value)
        .map_err(|error| format!("invalid auth JSON from {source}: {error}"))?;

    let Some((server, entry)) = docker.auths.into_iter().next() else {
        return Err(format!("invalid auth JSON from {source}: auths must not be empty"));
    };

    let (username, password) = if !entry.auth.is_empty()
        && (entry.username.is_empty() || entry.password.is_empty())
    {
        decode_docker_auth(source, &entry.auth)?
    } else {
        (entry.username, entry.password)
    };

    Ok(AuthConfig {
        username,
        password,
        auth: entry.auth,
        server_address: server,
        identity_token: entry.identity_token,
        registry_token: entry.registry_token,
    })
}

fn decode_docker_auth(source: &str, auth: &str) -> Result<(String, String), String> {
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(auth)
        .map_err(|error| format!("invalid auth JSON from {source}: invalid docker auth: {error}"))?;
    let decoded = String::from_utf8(decoded)
        .map_err(|error| format!("invalid auth JSON from {source}: invalid docker auth: {error}"))?;
    let Some((username, password)) = decoded.split_once(':') else {
        return Err(format!(
            "invalid auth JSON from {source}: docker auth must decode to username:password"
        ));
    };

    Ok((username.to_string(), password.to_string()))
}

#[allow(dead_code)]
pub(crate) fn parse_cidr_list(value: &str) -> Result<Vec<String>, String> {
    value
        .split(',')
        .map(|segment| {
            let cidr = segment.trim();
            if cidr.is_empty() {
                return Err(format!(
                    "invalid CIDR list \"{value}\": entries must not be empty"
                ));
            }
            validate_cidr(value, cidr)?;
            Ok(cidr.to_string())
        })
        .collect()
}

fn validate_cidr(source: &str, cidr: &str) -> Result<(), String> {
    let Some((addr, prefix)) = cidr.split_once('/') else {
        return Err(format!(
            "invalid CIDR list \"{source}\": expected address/prefix"
        ));
    };
    let addr = addr
        .parse::<IpAddr>()
        .map_err(|error| format!("invalid CIDR list \"{source}\": {error}"))?;
    let prefix = prefix
        .parse::<u8>()
        .map_err(|error| format!("invalid CIDR list \"{source}\": {error}"))?;
    let max_prefix = match addr {
        IpAddr::V4(_) => 32,
        IpAddr::V6(_) => 128,
    };
    if prefix > max_prefix {
        return Err(format!(
            "invalid CIDR list \"{source}\": prefix {prefix} exceeds {max_prefix}"
        ));
    }

    Ok(())
}

#[allow(dead_code)]
pub(crate) fn parse_port_mapping(value: &str) -> Result<PortMapping, String> {
    parse_port_mapping_with_host_ip(value, None)
}

#[allow(dead_code)]
pub(crate) fn parse_port_mapping_with_host_ip(
    value: &str,
    default_host_ip: Option<&str>,
) -> Result<PortMapping, String> {
    let (mapping, protocol) = value.split_once('/').unwrap_or((value, "tcp"));
    let protocol = parse_protocol(value, protocol)?;
    let (host_ip, ports) = split_mapping_host_ip(value, mapping, default_host_ip)?;
    let (host_port, container_port) = ports.split_once(':').ok_or_else(|| {
        format!("invalid port mapping \"{value}\": expected HOST:CONTAINER[/PROTO]")
    })?;

    Ok(PortMapping {
        protocol,
        container_port: parse_port(value, container_port)?,
        host_port: parse_port(value, host_port)?,
        host_ip,
    })
}

fn parse_protocol(source: &str, protocol: &str) -> Result<i32, String> {
    match protocol.to_ascii_lowercase().as_str() {
        "tcp" => Ok(Protocol::Tcp as i32),
        "udp" => Ok(Protocol::Udp as i32),
        "sctp" => Ok(Protocol::Sctp as i32),
        _ => Err(format!(
            "invalid port mapping \"{source}\": protocol must be tcp, udp, or sctp"
        )),
    }
}

fn split_mapping_host_ip<'a>(
    source: &str,
    mapping: &'a str,
    default_host_ip: Option<&str>,
) -> Result<(String, &'a str), String> {
    if let Some(rest) = mapping.strip_prefix('[') {
        let Some((host_ip, ports)) = rest.split_once("]:") else {
            return Err(format!(
                "invalid port mapping \"{source}\": bracket IPv6 host IP must be followed by :HOST:CONTAINER"
            ));
        };
        host_ip
            .parse::<std::net::Ipv6Addr>()
            .map_err(|error| format!("invalid port mapping \"{source}\": {error}"))?;
        return Ok((host_ip.to_string(), ports));
    }

    let colon_count = mapping.matches(':').count();
    match colon_count {
        1 => Ok((default_host_ip.unwrap_or_default().to_string(), mapping)),
        2 => {
            let Some((host_ip, ports)) = mapping.split_once(':') else {
                unreachable!("colon_count checked above")
            };
            host_ip
                .parse::<IpAddr>()
                .map_err(|error| format!("invalid port mapping \"{source}\": {error}"))?;
            Ok((host_ip.to_string(), ports))
        }
        count if count > 2 => Err(format!(
            "invalid port mapping \"{source}\": IPv6 host IP must be enclosed in brackets"
        )),
        _ => Err(format!(
            "invalid port mapping \"{source}\": expected HOST:CONTAINER[/PROTO]"
        )),
    }
}

fn parse_port(source: &str, value: &str) -> Result<i32, String> {
    let port = value
        .parse::<u16>()
        .map_err(|_| format!("invalid port mapping \"{source}\": port must be 1-65535"))?;
    if port == 0 {
        return Err(format!(
            "invalid port mapping \"{source}\": port must be 1-65535"
        ));
    }
    Ok(i32::from(port))
}

#[allow(dead_code)]
pub(crate) fn parse_mount(value: &str) -> Result<Mount, String> {
    let mut mount_type: Option<String> = None;
    let mut source: Option<String> = None;
    let mut destination: Option<String> = None;
    let mut image: Option<String> = None;
    let mut image_sub_path = String::new();
    let mut readonly = false;
    let mut selinux_relabel = false;
    let mut recursive_read_only = false;
    let mut propagation = MountPropagation::PropagationPrivate as i32;
    let mut uid_mappings = Vec::new();
    let mut gid_mappings = Vec::new();

    for part in value.split(',') {
        if part.is_empty() {
            return Err(format!("invalid mount \"{value}\": entries must not be empty"));
        }
        let Some((key, raw_value)) = part.split_once('=') else {
            match part {
                "ro" | "readonly" => readonly = true,
                "rw" => readonly = false,
                "z" | "Z" => selinux_relabel = true,
                "recursive-ro" | "recursive_read_only" => recursive_read_only = true,
                _ => {
                    return Err(format!(
                        "invalid mount \"{value}\": option \"{part}\" must be KEY=VALUE or a supported flag"
                    ));
                }
            }
            continue;
        };

        match key {
            "type" => mount_type = Some(raw_value.to_string()),
            "src" | "source" => source = Some(raw_value.to_string()),
            "dst" | "target" | "destination" => destination = Some(raw_value.to_string()),
            "image" => image = Some(raw_value.to_string()),
            "subpath" | "image-subpath" | "image_sub_path" => {
                image_sub_path = raw_value.to_string()
            }
            "options" | "option" => {
                for option in raw_value.split(':') {
                    apply_mount_option(value, option, &mut readonly, &mut selinux_relabel)?;
                }
            }
            "readonly" | "ro" => readonly = parse_bool("mount", value, raw_value)?,
            "recursive-ro" | "recursive_read_only" => {
                recursive_read_only = parse_bool("mount", value, raw_value)?
            }
            "propagation" => propagation = parse_mount_propagation(value, raw_value)?,
            "uidmap" | "uid-map" => uid_mappings.push(parse_mount_id_mapping(value, raw_value)?),
            "gidmap" | "gid-map" => gid_mappings.push(parse_mount_id_mapping(value, raw_value)?),
            _ => {
                return Err(format!(
                    "invalid mount \"{value}\": unsupported key \"{key}\""
                ));
            }
        }
    }

    let mount_type = mount_type.ok_or_else(|| {
        format!("invalid mount \"{value}\": type must be bind or image")
    })?;
    let container_path = destination.ok_or_else(|| {
        format!("invalid mount \"{value}\": dst/target is required")
    })?;

    if recursive_read_only
        && (!readonly || propagation != MountPropagation::PropagationPrivate as i32)
    {
        return Err(format!(
            "invalid mount \"{value}\": recursive-ro requires readonly=true and private propagation"
        ));
    }

    match mount_type.as_str() {
        "bind" => {
            let host_path = source.ok_or_else(|| {
                format!("invalid mount \"{value}\": bind mount requires src/source")
            })?;
            if image.is_some() {
                return Err(format!(
                    "invalid mount \"{value}\": bind mount must not include image"
                ));
            }
            Ok(Mount {
                container_path,
                host_path,
                readonly,
                selinux_relabel,
                propagation,
                uid_mappings,
                gid_mappings,
                recursive_read_only,
                image: None,
                image_sub_path,
            })
        }
        "image" => {
            if source.is_some() {
                return Err(format!(
                    "invalid mount \"{value}\": image mount must not include src/source"
                ));
            }
            let image = image.ok_or_else(|| {
                format!("invalid mount \"{value}\": image mount requires image")
            })?;
            Ok(Mount {
                container_path,
                host_path: String::new(),
                readonly: true,
                selinux_relabel,
                propagation,
                uid_mappings,
                gid_mappings,
                recursive_read_only,
                image: Some(ImageSpec {
                    image,
                    annotations: Default::default(),
                    user_specified_image: String::new(),
                    runtime_handler: String::new(),
                }),
                image_sub_path,
            })
        }
        _ => Err(format!(
            "invalid mount \"{value}\": type must be bind or image"
        )),
    }
}

fn apply_mount_option(
    source: &str,
    option: &str,
    readonly: &mut bool,
    selinux_relabel: &mut bool,
) -> Result<(), String> {
    match option {
        "ro" | "readonly" => {
            *readonly = true;
            Ok(())
        }
        "rw" => {
            *readonly = false;
            Ok(())
        }
        "z" | "Z" => {
            *selinux_relabel = true;
            Ok(())
        }
        "" => Err(format!("invalid mount \"{source}\": empty mount option")),
        _ => Err(format!(
            "invalid mount \"{source}\": unsupported mount option \"{option}\""
        )),
    }
}

fn parse_bool(kind: &str, source: &str, value: &str) -> Result<bool, String> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(format!(
            "invalid {kind} \"{source}\": expected boolean true or false"
        )),
    }
}

fn parse_mount_propagation(source: &str, value: &str) -> Result<i32, String> {
    match value {
        "private" | "rprivate" => Ok(MountPropagation::PropagationPrivate as i32),
        "host-to-container" | "rslave" => {
            Ok(MountPropagation::PropagationHostToContainer as i32)
        }
        "bidirectional" | "rshared" => Ok(MountPropagation::PropagationBidirectional as i32),
        _ => Err(format!(
            "invalid mount \"{source}\": propagation must be private, host-to-container, or bidirectional"
        )),
    }
}

fn parse_mount_id_mapping(source: &str, value: &str) -> Result<IdMapping, String> {
    let parts: Vec<_> = value.split(':').collect();
    if parts.len() != 3 {
        return Err(format!(
            "invalid mount \"{source}\": ID mapping must be HOST:CONTAINER:LENGTH"
        ));
    }
    let host_id = parse_u32_field("mount", source, parts[0])?;
    let container_id = parse_u32_field("mount", source, parts[1])?;
    let length = parse_u32_field("mount", source, parts[2])?;
    if length == 0 {
        return Err(format!(
            "invalid mount \"{source}\": ID mapping length must be greater than 0"
        ));
    }

    Ok(IdMapping {
        host_id,
        container_id,
        length,
    })
}

fn parse_u32_field(kind: &str, source: &str, value: &str) -> Result<u32, String> {
    value
        .parse::<u32>()
        .map_err(|_| format!("invalid {kind} \"{source}\": expected non-negative integer"))
}

#[allow(dead_code)]
pub(crate) fn parse_device(value: &str) -> Result<Device, String> {
    let parts: Vec<_> = value.split(':').collect();
    if parts.is_empty() || parts.len() > 3 || parts[0].is_empty() {
        return Err(format!(
            "invalid device \"{value}\": expected HOST[:CONTAINER[:PERMS]]"
        ));
    }

    let host_path = parts[0].to_string();
    let container_path = parts
        .get(1)
        .filter(|path| !path.is_empty())
        .copied()
        .unwrap_or(parts[0])
        .to_string();
    let permissions = parts.get(2).copied().unwrap_or("rwm");
    validate_device_permissions(value, permissions)?;

    Ok(Device {
        container_path,
        host_path,
        permissions: permissions.to_string(),
    })
}

fn validate_device_permissions(source: &str, permissions: &str) -> Result<(), String> {
    if permissions.is_empty() {
        return Err(format!(
            "invalid device \"{source}\": permissions must contain r, w, and/or m"
        ));
    }
    let mut seen = std::collections::BTreeSet::new();
    for permission in permissions.chars() {
        if !matches!(permission, 'r' | 'w' | 'm') || !seen.insert(permission) {
            return Err(format!(
                "invalid device \"{source}\": permissions must contain unique r, w, and/or m"
            ));
        }
    }
    Ok(())
}

#[allow(dead_code)]
pub(crate) fn parse_hugepage(value: &str) -> Result<HugepageLimit, String> {
    let Some((page_size, limit)) = value.split_once('=') else {
        return Err(format!(
            "invalid hugepage \"{value}\": expected SIZE=BYTES"
        ));
    };
    if page_size.is_empty() || limit.is_empty() {
        return Err(format!(
            "invalid hugepage \"{value}\": size and bytes must not be empty"
        ));
    }

    Ok(HugepageLimit {
        page_size: page_size.to_string(),
        limit: parse_byte_size(limit)?,
    })
}

#[allow(dead_code)]
pub(crate) fn parse_resource_spec(value: &str) -> Result<ResourceFragment, String> {
    let mut fragment = ResourceFragment::default();
    if value.is_empty() {
        return Err("invalid resource spec \"\": expected comma-separated KEY=VALUE".into());
    }

    for part in value.split(',') {
        let pair = parse_key_value("resource spec", part)?;
        match pair.key.as_str() {
            "cpu-period" | "cpu_period" => {
                fragment.cpu_period = Some(parse_i64_field("resource spec", value, &pair.value)?)
            }
            "cpu-quota" | "cpu_quota" | "cpu" => {
                fragment.cpu_quota = Some(parse_i64_field("resource spec", value, &pair.value)?)
            }
            "cpu-shares" | "cpu_shares" => {
                fragment.cpu_shares = Some(parse_i64_field("resource spec", value, &pair.value)?)
            }
            "memory" => {
                fragment.memory_limit_in_bytes =
                    Some(parse_byte_size_as_i64("resource spec", value, &pair.value)?)
            }
            "swap" | "memory-swap" | "memory_swap" => {
                fragment.memory_swap_limit_in_bytes =
                    Some(parse_byte_size_as_i64("resource spec", value, &pair.value)?)
            }
            "oom" | "oom-score-adj" | "oom_score_adj" => {
                fragment.oom_score_adj = Some(parse_i64_field("resource spec", value, &pair.value)?)
            }
            "cpuset" | "cpuset-cpus" | "cpuset_cpus" => fragment.cpuset_cpus = Some(pair.value),
            "cpuset-mems" | "cpuset_mems" => fragment.cpuset_mems = Some(pair.value),
            "hugepage" => fragment.hugepages.push(parse_hugepage(&pair.value)?),
            "unified" => fragment.unified.push(parse_key_value("resource unified", &pair.value)?),
            key => {
                return Err(format!(
                    "invalid resource spec \"{value}\": unsupported key \"{key}\""
                ));
            }
        }
    }

    Ok(fragment)
}

fn parse_i64_field(kind: &str, source: &str, value: &str) -> Result<i64, String> {
    value
        .parse::<i64>()
        .map_err(|_| format!("invalid {kind} \"{source}\": expected integer"))
}

fn parse_byte_size_as_i64(kind: &str, source: &str, value: &str) -> Result<i64, String> {
    let bytes = parse_byte_size(value)?;
    i64::try_from(bytes).map_err(|_| format!("invalid {kind} \"{source}\": value is out of range"))
}

#[allow(dead_code)]
pub(crate) fn parse_security_profile(value: &str) -> Result<SecurityProfile, String> {
    let profile_type = match value {
        "runtime/default" => crate::proto::runtime::v1::security_profile::ProfileType::RuntimeDefault,
        "unconfined" => crate::proto::runtime::v1::security_profile::ProfileType::Unconfined,
        _ => {
            let Some(localhost_ref) = value.strip_prefix("localhost:") else {
                return Err(format!(
                    "invalid security profile \"{value}\": expected runtime/default, unconfined, or localhost:VALUE"
                ));
            };
            if localhost_ref.is_empty() {
                return Err(format!(
                    "invalid security profile \"{value}\": localhost value must not be empty"
                ));
            }
            return Ok(SecurityProfile {
                profile_type:
                    crate::proto::runtime::v1::security_profile::ProfileType::Localhost as i32,
                localhost_ref: localhost_ref.to_string(),
            });
        }
    };

    Ok(SecurityProfile {
        profile_type: profile_type as i32,
        localhost_ref: String::new(),
    })
}

#[allow(dead_code)]
pub(crate) fn parse_selinux_option(value: &str) -> Result<SeLinuxOption, String> {
    let parts: Vec<_> = value.split(':').collect();
    if parts.len() != 4 {
        return Err(format!(
            "invalid SELinux option \"{value}\": expected user:role:type:level"
        ));
    }

    Ok(SeLinuxOption {
        user: parts[0].to_string(),
        role: parts[1].to_string(),
        r#type: parts[2].to_string(),
        level: parts[3].to_string(),
    })
}

#[allow(dead_code)]
pub(crate) fn parse_user(value: &str) -> Result<ParsedUser, String> {
    if value.is_empty() {
        return Err("invalid user \"\": expected UID, UID:GID, or username".into());
    }

    if let Some((uid, gid)) = value.split_once(':') {
        if uid.is_empty() || gid.is_empty() {
            return Err(format!(
                "invalid user \"{value}\": UID and GID must not be empty"
            ));
        }
        return Ok(ParsedUser::Id {
            uid: parse_user_id(value, uid)?,
            gid: Some(parse_user_id(value, gid)?),
        });
    }

    match value.parse::<i64>() {
        Ok(uid) if uid >= 0 => Ok(ParsedUser::Id { uid, gid: None }),
        Ok(_) => Err(format!("invalid user \"{value}\": UID must be non-negative")),
        Err(_) => Ok(ParsedUser::Name(value.to_string())),
    }
}

fn parse_user_id(source: &str, value: &str) -> Result<i64, String> {
    let id = value
        .parse::<i64>()
        .map_err(|_| format!("invalid user \"{source}\": UID and GID must be numeric"))?;
    if id < 0 {
        return Err(format!(
            "invalid user \"{source}\": UID and GID must be non-negative"
        ));
    }
    Ok(id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_supported_endpoints() {
        assert_eq!(
            parse_endpoint("unix:///run/crius/crius.sock").unwrap(),
            Endpoint::Unix("/run/crius/crius.sock".into())
        );
        assert_eq!(
            parse_endpoint("/run/crius/crius.sock").unwrap(),
            Endpoint::Unix("/run/crius/crius.sock".into())
        );
        assert_eq!(
            parse_endpoint("http://127.0.0.1:8080").unwrap(),
            Endpoint::Tcp("http://127.0.0.1:8080".into())
        );
        assert_eq!(
            parse_endpoint("https://127.0.0.1:8443").unwrap(),
            Endpoint::Tcp("https://127.0.0.1:8443".into())
        );
    }

    #[test]
    fn rejects_invalid_endpoints() {
        for endpoint in ["", "tcp://127.0.0.1:8080", "unix://"] {
            let error = parse_endpoint(endpoint).expect_err("endpoint should be rejected");
            assert!(error.contains("invalid endpoint"));
            assert!(error.contains(endpoint));
        }
    }

    #[test]
    fn parses_durations_with_supported_units() {
        assert_eq!(parse_duration("500ms").unwrap(), Duration::from_millis(500));
        assert_eq!(parse_duration("5s").unwrap(), Duration::from_secs(5));
        assert_eq!(parse_duration("2m").unwrap(), Duration::from_secs(120));
        assert_eq!(parse_duration("1h").unwrap(), Duration::from_secs(3_600));
    }

    #[test]
    fn rejects_invalid_durations() {
        for input in ["", "10", "1.5s", "5d", "ms"] {
            let error = parse_duration(input).expect_err("duration should be rejected");
            assert!(error.contains(&format!("invalid duration \"{input}\"")));
        }

        let overflow = format!("{}h", u64::MAX);
        let error = parse_duration(&overflow).expect_err("duration overflow should be rejected");
        assert!(error.contains("value is out of range"));
    }

    #[test]
    fn parses_byte_sizes() {
        assert_eq!(parse_byte_size("0").unwrap(), 0);
        assert_eq!(parse_byte_size("512").unwrap(), 512);
        assert_eq!(parse_byte_size("512B").unwrap(), 512);
        assert_eq!(parse_byte_size("1KiB").unwrap(), 1024);
        assert_eq!(parse_byte_size("64MiB").unwrap(), 67_108_864);
        assert_eq!(parse_byte_size("1GiB").unwrap(), 1_073_741_824);
        assert_eq!(parse_byte_size("1TiB").unwrap(), 1_099_511_627_776);
    }

    #[test]
    fn rejects_invalid_byte_sizes() {
        for input in ["", "KiB", "abc", "64Mi", "64 MB", "-1"] {
            let error = parse_byte_size(input).expect_err(&format!("{input} should be rejected"));
            assert!(error.contains(&format!("invalid byte size \"{input}\"")), "{error}");
        }

        let overflow = format!("{}TiB", u64::MAX);
        let error = parse_byte_size(&overflow).expect_err("byte size overflow should be rejected");
        assert!(error.contains("value is out of range"));
    }

    #[test]
    fn parses_key_value_pairs() {
        assert_eq!(
            parse_key_value("--env", "Name=").unwrap(),
            KeyValuePair {
                key: "Name".into(),
                value: "".into()
            }
        );
        assert_eq!(
            parse_key_value("--label", "App=Demo").unwrap(),
            KeyValuePair {
                key: "App".into(),
                value: "Demo".into()
            }
        );
    }

    #[test]
    fn rejects_invalid_key_value_pairs() {
        for input in ["", "missing-equals", "=x"] {
            let error = parse_key_value("--env", input).expect_err("key/value should be rejected");
            assert!(error.contains("--env"), "{error}");
            assert!(error.contains(input), "{error}");
        }
    }

    #[test]
    fn parses_env_files() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("env.list");
        std::fs::write(&path, "# comment\n\nFOO=bar\nEMPTY=\nName=Value\n").unwrap();

        let entries = parse_env_file(&path).unwrap();

        assert_eq!(
            entries,
            vec![
                KeyValuePair {
                    key: "FOO".into(),
                    value: "bar".into(),
                },
                KeyValuePair {
                    key: "EMPTY".into(),
                    value: "".into(),
                },
                KeyValuePair {
                    key: "Name".into(),
                    value: "Value".into(),
                },
            ]
        );
    }

    #[test]
    fn rejects_invalid_env_files() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("env.list");
        std::fs::write(&path, "=secret\n").unwrap();

        let error = parse_env_file(&path).expect_err("invalid env file should be rejected");
        assert!(error.contains("env file"), "{error}");
        assert!(error.contains("=secret"), "{error}");

        let missing = dir.path().join("missing.env");
        let error = parse_env_file(&missing).expect_err("missing env file should be rejected");
        assert!(error.contains("failed to read env file"), "{error}");
    }

    #[test]
    fn parses_cri_auth_json() {
        let auth = parse_auth_json(
            "inline",
            r#"{"username":"u","password":"p","serverAddress":"registry.example","identityToken":"id","registryToken":"rt"}"#,
        )
        .unwrap();

        assert_eq!(auth.username, "u");
        assert_eq!(auth.password, "p");
        assert_eq!(auth.server_address, "registry.example");
        assert_eq!(auth.identity_token, "id");
        assert_eq!(auth.registry_token, "rt");
    }

    #[test]
    fn parses_docker_auth_json() {
        let auth = parse_auth_json(
            "config.json",
            r#"{"auths":{"registry.example":{"auth":"dXNlcjpzZWNyZXQ="}}}"#,
        )
        .unwrap();

        assert_eq!(auth.server_address, "registry.example");
        assert_eq!(auth.username, "user");
        assert_eq!(auth.password, "secret");
        assert_eq!(auth.auth, "dXNlcjpzZWNyZXQ=");
    }

    #[test]
    fn rejects_invalid_auth_json_without_leaking_secret() {
        let error = parse_auth_json("inline", r#"{"auths":{"r":{"auth":"not a secret token"}}}"#)
            .expect_err("invalid auth JSON should be rejected");

        assert!(error.contains("inline"), "{error}");
        assert!(!error.contains("not a secret token"), "{error}");
    }

    #[test]
    fn parses_cidr_lists() {
        assert_eq!(
            parse_cidr_list("10.244.0.0/16,fd00::/64").unwrap(),
            vec!["10.244.0.0/16".to_string(), "fd00::/64".to_string()]
        );
    }

    #[test]
    fn rejects_invalid_cidr_lists() {
        for input in ["", "10.244.0.0", "10.0.0.0/8,,fd00::/64", "not-cidr"] {
            let error = parse_cidr_list(input).expect_err("CIDR list should be rejected");
            assert!(error.contains(&format!("invalid CIDR list \"{input}\"")), "{error}");
        }
    }

    #[test]
    fn parses_port_mappings() {
        let mapping = parse_port_mapping("8080:80/UDP").unwrap();
        assert_eq!(mapping.host_port, 8080);
        assert_eq!(mapping.container_port, 80);
        assert_eq!(mapping.protocol, Protocol::Udp as i32);
        assert_eq!(mapping.host_ip, "");

        let mapping = parse_port_mapping("[fd00::1]:8443:443").unwrap();
        assert_eq!(mapping.host_ip, "fd00::1");
        assert_eq!(mapping.host_port, 8443);
        assert_eq!(mapping.container_port, 443);
        assert_eq!(mapping.protocol, Protocol::Tcp as i32);

        let mapping = parse_port_mapping_with_host_ip("8080:80", Some("127.0.0.1")).unwrap();
        assert_eq!(mapping.host_ip, "127.0.0.1");
    }

    #[test]
    fn rejects_invalid_port_mappings() {
        for input in ["", "80", "0:80", "8080:0", "8080:80/http", "fd00::1:8080:80"] {
            let error = parse_port_mapping(input).expect_err("port mapping should be rejected");
            assert!(error.contains(&format!("invalid port mapping \"{input}\"")), "{error}");
        }
    }

    #[test]
    fn parses_mounts() {
        let mount = parse_mount(
            "type=bind,src=/host,dst=/container,readonly=true,uidmap=0:1000:1,gidmap=0:1000:1",
        )
        .unwrap();
        assert_eq!(mount.host_path, "/host");
        assert_eq!(mount.container_path, "/container");
        assert!(mount.readonly);
        assert_eq!(mount.uid_mappings[0].host_id, 0);
        assert_eq!(mount.gid_mappings[0].container_id, 1000);

        let mount =
            parse_mount("type=image,image=busybox,dst=/rootfs,subpath=bin,src=/bad")
                .expect_err("image mount with source should fail");
        assert!(mount.contains("image mount must not include src/source"), "{mount}");

        let mount = parse_mount("type=image,image=busybox,dst=/rootfs,subpath=bin").unwrap();
        assert_eq!(mount.image.unwrap().image, "busybox");
        assert_eq!(mount.image_sub_path, "bin");
        assert!(mount.readonly);
    }

    #[test]
    fn rejects_invalid_mounts() {
        for input in [
            "type=bind,dst=/container",
            "type=image,image=busybox,dst=/rootfs,src=/host",
            "type=bind,src=/host,dst=/container,recursive-ro",
            "type=tmpfs,dst=/x",
        ] {
            let error = parse_mount(input).expect_err("mount should be rejected");
            assert!(error.contains(&format!("invalid mount \"{input}\"")), "{error}");
        }
    }

    #[test]
    fn parses_devices() {
        assert_eq!(parse_device("/dev/null").unwrap().container_path, "/dev/null");
        assert_eq!(parse_device("/dev/null").unwrap().permissions, "rwm");

        let device = parse_device("/dev/fuse:/dev/fuse:rwm").unwrap();
        assert_eq!(device.host_path, "/dev/fuse");
        assert_eq!(device.container_path, "/dev/fuse");
        assert_eq!(device.permissions, "rwm");
    }

    #[test]
    fn rejects_invalid_devices() {
        for input in ["", ":/dev/null", "/dev/fuse:/dev/fuse:rx", "/dev/null:/x:rr"] {
            let error = parse_device(input).expect_err("device should be rejected");
            assert!(error.contains(&format!("invalid device \"{input}\"")), "{error}");
        }
    }

    #[test]
    fn parses_hugepages() {
        let hugepage = parse_hugepage("2Mi=64MiB").unwrap();
        assert_eq!(hugepage.page_size, "2Mi");
        assert_eq!(hugepage.limit, 67_108_864);
    }

    #[test]
    fn rejects_invalid_hugepages() {
        for input in ["", "2Mi", "=64MiB", "2Mi="] {
            let error = parse_hugepage(input).expect_err("hugepage should be rejected");
            assert!(error.contains(&format!("invalid hugepage \"{input}\"")), "{error}");
        }
    }

    #[test]
    fn parses_resource_specs() {
        let fragment = parse_resource_spec(
            "cpu=50000,memory=64MiB,swap=128MiB,oom=-10,cpuset=0-1,hugepage=2Mi=64MiB,unified=memory.max=67108864",
        )
        .unwrap();

        assert_eq!(fragment.cpu_quota, Some(50_000));
        assert_eq!(fragment.memory_limit_in_bytes, Some(67_108_864));
        assert_eq!(fragment.memory_swap_limit_in_bytes, Some(134_217_728));
        assert_eq!(fragment.oom_score_adj, Some(-10));
        assert_eq!(fragment.cpuset_cpus.as_deref(), Some("0-1"));
        assert_eq!(fragment.hugepages[0].limit, 67_108_864);
        assert_eq!(fragment.unified[0].key, "memory.max");
    }

    #[test]
    fn rejects_invalid_resource_specs() {
        for input in ["", "unknown=1", "memory=64MB", "unified=missing-value"] {
            let error = parse_resource_spec(input).expect_err("resource spec should be rejected");
            assert!(
                error.contains("resource spec") || error.contains("byte size"),
                "{error}"
            );
            assert!(error.contains(input) || input == "unified=missing-value", "{error}");
        }
    }

    #[test]
    fn parses_security_profiles() {
        let profile = parse_security_profile("runtime/default").unwrap();
        assert_eq!(
            profile.profile_type,
            crate::proto::runtime::v1::security_profile::ProfileType::RuntimeDefault as i32
        );

        let profile = parse_security_profile("localhost:profiles/default.json").unwrap();
        assert_eq!(
            profile.profile_type,
            crate::proto::runtime::v1::security_profile::ProfileType::Localhost as i32
        );
        assert_eq!(profile.localhost_ref, "profiles/default.json");
    }

    #[test]
    fn rejects_invalid_security_profiles() {
        for input in ["", "default", "localhost:"] {
            let error = parse_security_profile(input).expect_err("security profile should fail");
            assert!(
                error.contains(&format!("invalid security profile \"{input}\"")),
                "{error}"
            );
        }
    }

    #[test]
    fn parses_selinux_options() {
        let option = parse_selinux_option("user:role:type:level").unwrap();
        assert_eq!(option.user, "user");
        assert_eq!(option.role, "role");
        assert_eq!(option.r#type, "type");
        assert_eq!(option.level, "level");

        let option = parse_selinux_option("::type:").unwrap();
        assert_eq!(option.user, "");
        assert_eq!(option.role, "");
        assert_eq!(option.r#type, "type");
        assert_eq!(option.level, "");
    }

    #[test]
    fn rejects_invalid_selinux_options() {
        for input in ["user:role:type", "user:role:type:level:extra"] {
            let error = parse_selinux_option(input).expect_err("SELinux option should fail");
            assert!(
                error.contains(&format!("invalid SELinux option \"{input}\"")),
                "{error}"
            );
        }
    }

    #[test]
    fn parses_users() {
        assert_eq!(parse_user("1000").unwrap(), ParsedUser::Id { uid: 1000, gid: None });
        assert_eq!(
            parse_user("1000:1000").unwrap(),
            ParsedUser::Id {
                uid: 1000,
                gid: Some(1000)
            }
        );
        assert_eq!(parse_user("nobody").unwrap(), ParsedUser::Name("nobody".into()));
    }

    #[test]
    fn rejects_invalid_users() {
        for input in ["", "1000:", "-1", "1000:group"] {
            let error = parse_user(input).expect_err("user should fail");
            assert!(error.contains(&format!("invalid user \"{input}\"")), "{error}");
        }
    }
}
