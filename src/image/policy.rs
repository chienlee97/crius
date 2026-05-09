use std::collections::HashMap;
use std::path::Path;

use oci_distribution::Reference;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct SignaturePolicy {
    pub default: Vec<PolicyRequirement>,
    pub transports: HashMap<String, HashMap<String, Vec<PolicyRequirement>>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PolicyRequirement {
    #[serde(rename = "type")]
    pub kind: String,
}

impl Default for SignaturePolicy {
    fn default() -> Self {
        Self {
            default: vec![PolicyRequirement {
                kind: "insecureAcceptAnything".to_string(),
            }],
            transports: HashMap::new(),
        }
    }
}

pub fn load_signature_policy(path: &Path) -> anyhow::Result<SignaturePolicy> {
    let raw = std::fs::read_to_string(path)?;
    Ok(serde_json::from_str(&raw)?)
}

pub fn evaluate_signature_policy(
    policy: &SignaturePolicy,
    reference: &Reference,
) -> anyhow::Result<()> {
    let transport_rules = policy.transports.get("docker");
    let scoped_rules = transport_rules.and_then(|rules| {
        repository_scope_candidates(reference)
            .into_iter()
            .find_map(|scope| rules.get(&scope))
    });
    let requirements = scoped_rules.unwrap_or(&policy.default);
    if requirements.is_empty() {
        anyhow::bail!("signature policy has an empty requirement list")
    }

    for requirement in requirements {
        match requirement.kind.as_str() {
            "insecureAcceptAnything" => return Ok(()),
            "reject" => {
                return Err(anyhow::anyhow!(
                    "image {} is rejected by signature policy",
                    reference
                ));
            }
            _ => {}
        }
    }
    anyhow::bail!(
        "signature policy for {} only contains unsupported requirement types",
        reference
    )
}

fn repository_scope_candidates(reference: &Reference) -> Vec<String> {
    let mut scopes = Vec::new();
    let registry = reference.resolve_registry();
    let repository = reference.repository();
    let segments: Vec<&str> = repository.split('/').collect();
    let mut registries = vec![registry.to_string()];
    if matches!(registry, "registry-1.docker.io" | "index.docker.io") {
        registries.push("docker.io".to_string());
    }
    for registry in registries {
        for idx in (1..=segments.len()).rev() {
            scopes.push(format!("{}/{}", registry, segments[..idx].join("/")));
        }
        scopes.push(registry);
    }
    scopes
}

#[cfg(test)]
mod tests {
    use super::{evaluate_signature_policy, SignaturePolicy};

    #[test]
    fn policy_rejects_when_scope_requires_reject() {
        let policy: SignaturePolicy = serde_json::from_str(
            r#"{
                "default": [{"type":"insecureAcceptAnything"}],
                "transports": {
                    "docker": {
                        "docker.io/library/busybox": [{"type":"reject"}]
                    }
                }
            }"#,
        )
        .unwrap();
        let reference: oci_distribution::Reference =
            "docker.io/library/busybox:latest".parse().unwrap();
        assert!(evaluate_signature_policy(&policy, &reference).is_err());
    }
}
