#[derive(Clone, Debug)]
pub(crate) struct CrsClient {
    #[allow(dead_code)]
    endpoint: String,
}

impl CrsClient {
    pub(crate) fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn endpoint(&self) -> &str {
        &self.endpoint
    }
}
