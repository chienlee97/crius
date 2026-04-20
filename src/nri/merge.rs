use std::collections::HashMap;

use crate::nri::{NriError, Result};

pub fn merge_annotation_adjustments(
    plugins: &[(String, HashMap<String, String>)],
) -> Result<HashMap<String, String>> {
    let mut merged = HashMap::new();
    let mut owners = HashMap::new();

    for (plugin, annotations) in plugins {
        for (k, v) in annotations {
            if let Some(owner) = owners.get(k) {
                if owner != plugin {
                    return Err(NriError::Plugin(format!(
                        "annotation '{}' conflict between plugins '{}' and '{}'",
                        k, owner, plugin
                    )));
                }
            }
            owners.insert(k.clone(), plugin.clone());
            merged.insert(k.clone(), v.clone());
        }
    }

    Ok(merged)
}
