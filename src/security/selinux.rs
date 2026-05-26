use sha2::{Digest, Sha256};

use crate::proto::runtime::v1::SeLinuxOption;

#[derive(Debug, Clone, Copy)]
pub struct SelinuxPolicy {
    pub enabled: bool,
    pub available: bool,
    pub category_range: u32,
    pub hostnetwork_disable: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct SelinuxResolverConfig {
    pub enabled: bool,
    pub category_range: u32,
    pub hostnetwork_disable: bool,
}

pub fn generated_level(seed: &str, category_range: u32) -> String {
    let effective_range = category_range.max(1);
    if effective_range == 1 {
        return "s0:c0,c0".to_string();
    }

    let digest = Sha256::digest(seed.as_bytes());
    let first = u32::from_le_bytes([digest[0], digest[1], digest[2], digest[3]]) % effective_range;
    let mut second =
        u32::from_le_bytes([digest[4], digest[5], digest[6], digest[7]]) % effective_range;
    if second == first {
        second = (second + 1) % effective_range;
    }
    let (low, high) = if first <= second {
        (first, second)
    } else {
        (second, first)
    };
    format!("s0:c{low},c{high}")
}

pub fn label_from_options(
    options: Option<&SeLinuxOption>,
    auto_level_seed: Option<&str>,
    category_range: u32,
) -> Option<String> {
    let options = options?;
    if options.user.is_empty()
        && options.role.is_empty()
        && options.r#type.is_empty()
        && options.level.is_empty()
    {
        return None;
    }

    let user = if options.user.is_empty() {
        "system_u"
    } else {
        options.user.as_str()
    };
    let role = if options.role.is_empty() {
        "system_r"
    } else {
        options.role.as_str()
    };
    let selinux_type = if options.r#type.is_empty() {
        "container_t"
    } else {
        options.r#type.as_str()
    };
    let level = if options.level.is_empty() {
        auto_level_seed
            .map(|seed| generated_level(seed, category_range))
            .unwrap_or_else(|| "s0".to_string())
    } else {
        options.level.clone()
    };
    Some(format!("{user}:{role}:{selinux_type}:{level}"))
}

pub fn effective_label(
    options: Option<&SeLinuxOption>,
    host_network: bool,
    auto_level_seed: Option<&str>,
    policy: SelinuxPolicy,
) -> Option<String> {
    if !policy.enabled || !policy.available {
        return None;
    }
    if host_network && policy.hostnetwork_disable {
        return None;
    }
    label_from_options(options, auto_level_seed, policy.category_range)
}

pub fn effective_label_from_config(
    options: Option<&SeLinuxOption>,
    host_network: bool,
    auto_level_seed: Option<&str>,
    config: SelinuxResolverConfig,
    available: bool,
) -> Option<String> {
    effective_label(
        options,
        host_network,
        auto_level_seed,
        SelinuxPolicy {
            enabled: config.enabled,
            available,
            category_range: config.category_range,
            hostnetwork_disable: config.hostnetwork_disable,
        },
    )
}

pub fn is_available() -> bool {
    std::path::Path::new("/sys/fs/selinux").exists() || std::path::Path::new("/selinux").exists()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn label_from_options_uses_defaults_for_missing_parts() {
        let options = SeLinuxOption {
            user: String::new(),
            role: String::new(),
            r#type: String::new(),
            level: "s0:c1,c2".to_string(),
        };

        assert_eq!(
            label_from_options(Some(&options), None, 1024),
            Some("system_u:system_r:container_t:s0:c1,c2".to_string())
        );
    }

    #[test]
    fn generated_level_stays_inside_range() {
        let level = generated_level("pod/container", 4);
        assert!(level.starts_with("s0:c"));
        for category in level.trim_start_matches("s0:").split(',') {
            let value = category.trim_start_matches('c').parse::<u32>().unwrap();
            assert!(value < 4);
        }
    }

    #[test]
    fn effective_label_respects_host_network_disable() {
        let options = SeLinuxOption {
            level: "s0:c1,c2".to_string(),
            ..Default::default()
        };
        let policy = SelinuxPolicy {
            enabled: true,
            available: true,
            category_range: 1024,
            hostnetwork_disable: true,
        };

        assert_eq!(effective_label(Some(&options), true, None, policy), None);
    }

    #[test]
    fn effective_label_from_config_combines_config_and_availability() {
        let options = SeLinuxOption {
            r#type: "spc_t".to_string(),
            ..Default::default()
        };
        let config = SelinuxResolverConfig {
            enabled: true,
            category_range: 8,
            hostnetwork_disable: false,
        };

        assert!(
            effective_label_from_config(Some(&options), false, Some("pod"), config, false)
                .is_none()
        );

        let label = effective_label_from_config(Some(&options), false, Some("pod"), config, true)
            .expect("available selinux should produce a label");
        assert!(label.starts_with("system_u:system_r:spc_t:s0:c"));
    }
}
