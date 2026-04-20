use std::collections::HashMap;

use crate::oci::spec::Spec;

pub fn apply_annotation_adjustments(spec: &mut Spec, adjustments: &HashMap<String, String>) {
    let annotations = spec.annotations.get_or_insert_with(HashMap::new);
    for (k, v) in adjustments {
        annotations.insert(k.clone(), v.clone());
    }
}
