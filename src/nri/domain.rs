use std::collections::HashMap;

#[derive(Debug, Clone, Default)]
pub struct RuntimeSnapshot {
    pub pods: Vec<String>,
    pub containers: Vec<String>,
    pub annotations: HashMap<String, HashMap<String, String>>,
}
