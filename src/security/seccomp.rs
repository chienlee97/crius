use std::path::{Path, PathBuf};

use crate::proto::runtime::v1::{security_profile::ProfileType, SecurityProfile};
use crate::runtime::SeccompProfile;

pub fn profile_from_proto(
    profile: Option<&SecurityProfile>,
    deprecated_profile: &str,
) -> Option<SeccompProfile> {
    if let Some(profile) = profile {
        return match profile.profile_type {
            x if x == ProfileType::RuntimeDefault as i32 => Some(SeccompProfile::RuntimeDefault),
            x if x == ProfileType::Unconfined as i32 => Some(SeccompProfile::Unconfined),
            x if x == ProfileType::Localhost as i32 => {
                if profile.localhost_ref.is_empty() {
                    None
                } else {
                    Some(SeccompProfile::Localhost(PathBuf::from(
                        profile.localhost_ref.clone(),
                    )))
                }
            }
            _ => None,
        };
    }

    if deprecated_profile.is_empty() {
        None
    } else {
        Some(SeccompProfile::Localhost(PathBuf::from(
            deprecated_profile.to_string(),
        )))
    }
}

pub fn profile_from_selector(selector: &str) -> Option<SeccompProfile> {
    let selector = selector.trim();
    if selector.is_empty() {
        return None;
    }
    if selector.eq_ignore_ascii_case("runtime/default")
        || selector.eq_ignore_ascii_case("docker/default")
    {
        return Some(SeccompProfile::RuntimeDefault);
    }
    if selector.eq_ignore_ascii_case("unconfined") {
        return Some(SeccompProfile::Unconfined);
    }
    if let Some(localhost_ref) = selector.strip_prefix("localhost/") {
        return (!localhost_ref.trim().is_empty())
            .then(|| SeccompProfile::Localhost(PathBuf::from(localhost_ref)));
    }
    Some(SeccompProfile::Localhost(PathBuf::from(selector)))
}

pub fn expand_runtime_default_profile(
    profile: Option<SeccompProfile>,
    runtime_default_path: &Path,
) -> Option<SeccompProfile> {
    match profile {
        Some(SeccompProfile::RuntimeDefault) => {
            if runtime_default_path.as_os_str().is_empty() {
                Some(SeccompProfile::RuntimeDefault)
            } else {
                Some(SeccompProfile::Localhost(
                    runtime_default_path.to_path_buf(),
                ))
            }
        }
        other => other,
    }
}

pub fn effective_profile(
    profile: Option<&SecurityProfile>,
    deprecated_profile: &str,
    privileged: bool,
    privileged_selector: &str,
    unset_selector: &str,
    runtime_default_path: &Path,
) -> Option<SeccompProfile> {
    let selected = match profile_from_proto(profile, deprecated_profile) {
        Some(SeccompProfile::RuntimeDefault) if privileged => {
            profile_from_selector(privileged_selector)
        }
        Some(explicit) => Some(explicit),
        None if privileged => profile_from_selector(privileged_selector),
        None => profile_from_selector(unset_selector),
    };
    expand_runtime_default_profile(selected, runtime_default_path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profile_from_proto_supports_localhost_and_unconfined() {
        let localhost_profile = profile_from_proto(
            Some(&SecurityProfile {
                profile_type: ProfileType::Localhost as i32,
                localhost_ref: "/tmp/seccomp/profile.json".to_string(),
            }),
            "",
        );
        assert!(matches!(
            localhost_profile,
            Some(SeccompProfile::Localhost(_))
        ));

        let unconfined_profile = profile_from_proto(
            Some(&SecurityProfile {
                profile_type: ProfileType::Unconfined as i32,
                localhost_ref: String::new(),
            }),
            "",
        );
        assert!(matches!(
            unconfined_profile,
            Some(SeccompProfile::Unconfined)
        ));
    }

    #[test]
    fn effective_profile_uses_unset_and_privileged_fallbacks() {
        assert!(matches!(
            effective_profile(
                None,
                "",
                false,
                "unconfined",
                "runtime/default",
                Path::new("")
            ),
            Some(SeccompProfile::RuntimeDefault)
        ));
        assert!(matches!(
            effective_profile(
                None,
                "",
                true,
                "unconfined",
                "runtime/default",
                Path::new("")
            ),
            Some(SeccompProfile::Unconfined)
        ));
    }

    #[test]
    fn effective_profile_expands_runtime_default_path() {
        let path = Path::new("/etc/crius/seccomp.json");

        match effective_profile(None, "", false, "unconfined", "runtime/default", path) {
            Some(SeccompProfile::Localhost(actual)) => assert_eq!(actual, path),
            other => panic!("expected localhost seccomp profile, got {other:?}"),
        }
    }

    #[test]
    fn explicit_unconfined_is_not_overridden_by_unset_fallback() {
        let profile = SecurityProfile {
            profile_type: ProfileType::Unconfined as i32,
            localhost_ref: String::new(),
        };

        assert!(matches!(
            effective_profile(
                Some(&profile),
                "",
                false,
                "runtime/default",
                "runtime/default",
                Path::new("")
            ),
            Some(SeccompProfile::Unconfined)
        ));
    }
}
