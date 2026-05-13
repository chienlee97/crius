use thiserror::Error;

use crate::proto::runtime::v1::{security_profile::ProfileType, SecurityProfile};

#[derive(Debug, Error, PartialEq, Eq)]
pub enum AppArmorError {
    #[error("apparmor is not available or is disabled for this node")]
    Unavailable,
}

pub fn profile_name(profile: Option<&SecurityProfile>, deprecated_profile: &str) -> Option<String> {
    if let Some(profile) = profile {
        match profile.profile_type {
            x if x == ProfileType::RuntimeDefault as i32 => Some("runtime/default".to_string()),
            x if x == ProfileType::Unconfined as i32 => Some("unconfined".to_string()),
            x if x == ProfileType::Localhost as i32 => {
                (!profile.localhost_ref.is_empty()).then(|| profile.localhost_ref.clone())
            }
            _ => None,
        }
    } else if deprecated_profile.is_empty() {
        None
    } else {
        Some(deprecated_profile.to_string())
    }
}

pub fn effective_profile(
    requested: Option<String>,
    default_profile: &str,
    privileged: bool,
    disabled: bool,
    available: bool,
) -> Result<Option<String>, AppArmorError> {
    if disabled || !available {
        return match requested.as_deref() {
            Some(profile) if !profile.eq_ignore_ascii_case("unconfined") && !profile.is_empty() => {
                Err(AppArmorError::Unavailable)
            }
            _ => Ok(None),
        };
    }

    if privileged {
        return Ok(None);
    }

    match requested.as_deref() {
        None => Ok(Some(default_profile.to_string())),
        Some(profile) if profile.eq_ignore_ascii_case("runtime/default") => {
            Ok(Some(default_profile.to_string()))
        }
        Some(profile) if profile.eq_ignore_ascii_case("unconfined") => Ok(None),
        Some(profile) => Ok(Some(profile.to_string())),
    }
}

pub fn is_available() -> bool {
    std::path::Path::new("/sys/kernel/security/apparmor").exists()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn effective_profile_uses_default_for_unset_and_runtime_default() {
        assert_eq!(
            effective_profile(None, "crius-default", false, false, true).unwrap(),
            Some("crius-default".to_string())
        );
        assert_eq!(
            effective_profile(
                Some("runtime/default".to_string()),
                "crius-default",
                false,
                false,
                true,
            )
            .unwrap(),
            Some("crius-default".to_string())
        );
    }

    #[test]
    fn effective_profile_allows_unconfined_when_disabled() {
        assert_eq!(
            effective_profile(
                Some("unconfined".to_string()),
                "crius-default",
                false,
                true,
                false,
            )
            .unwrap(),
            None
        );
    }

    #[test]
    fn effective_profile_rejects_named_profile_when_unavailable() {
        let err = effective_profile(
            Some("localhost/custom".to_string()),
            "crius-default",
            false,
            true,
            false,
        )
        .unwrap_err();
        assert_eq!(err, AppArmorError::Unavailable);
    }
}
