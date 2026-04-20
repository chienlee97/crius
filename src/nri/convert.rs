use std::collections::HashMap;

const INTERNAL_ANNOTATION_PREFIX: &str = "io.crius.internal/";

pub fn external_annotations(
    annotations: &HashMap<String, String>,
) -> HashMap<String, String> {
    annotations
        .iter()
        .filter(|(k, _)| !k.starts_with(INTERNAL_ANNOTATION_PREFIX))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::external_annotations;
    use std::collections::HashMap;

    #[test]
    fn hides_internal_annotations() {
        let mut annotations = HashMap::new();
        annotations.insert("io.crius.internal/state".to_string(), "x".to_string());
        annotations.insert("k8s.io/name".to_string(), "nginx".to_string());

        let filtered = external_annotations(&annotations);
        assert!(!filtered.contains_key("io.crius.internal/state"));
        assert_eq!(filtered.get("k8s.io/name"), Some(&"nginx".to_string()));
    }
}
