use serde_json::json;
use std::collections::BTreeMap;
use tokio::sync::mpsc;
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::{Context, Layer};

#[derive(Clone)]
pub struct TraceExportLayer {
    sender: mpsc::UnboundedSender<String>,
    sampling_rate_per_million: u32,
}

pub fn build_layer(
    config: &crate::config::TracingConfig,
) -> anyhow::Result<Option<TraceExportLayer>> {
    if !config.enable {
        return Ok(None);
    }
    let endpoint = config.endpoint.trim();
    if endpoint.is_empty() {
        anyhow::bail!("tracing.endpoint must not be empty when tracing.enable is true");
    }
    if !(endpoint.starts_with("http://") || endpoint.starts_with("https://")) {
        anyhow::bail!("tracing.endpoint must start with http:// or https://");
    }

    let (sender, mut receiver) = mpsc::unbounded_channel::<String>();
    let endpoint = endpoint.to_string();
    tokio::spawn(async move {
        let client = reqwest::Client::new();
        while let Some(payload) = receiver.recv().await {
            if let Err(err) = client
                .post(&endpoint)
                .header("content-type", "application/json")
                .body(payload)
                .send()
                .await
            {
                eprintln!("trace export failed: {err}");
            }
        }
    });

    Ok(Some(TraceExportLayer {
        sender,
        sampling_rate_per_million: config.sampling_rate_per_million,
    }))
}

impl<S> Layer<S> for TraceExportLayer
where
    S: Subscriber,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        if !should_sample(self.sampling_rate_per_million) {
            return;
        }

        let metadata = event.metadata();
        let mut visitor = JsonVisitor::default();
        event.record(&mut visitor);
        let payload = json!({
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "target": metadata.target(),
            "name": metadata.name(),
            "level": metadata.level().as_str(),
            "modulePath": metadata.module_path(),
            "file": metadata.file(),
            "line": metadata.line(),
            "fields": visitor.fields,
        });
        let _ = self.sender.send(payload.to_string());
    }
}

fn should_sample(sampling_rate_per_million: u32) -> bool {
    if sampling_rate_per_million == 0 {
        return false;
    }
    if sampling_rate_per_million >= 1_000_000 {
        return true;
    }
    let threshold = sampling_rate_per_million as u64;
    let sample = rand::random::<u64>() % 1_000_000;
    sample < threshold
}

#[derive(Default)]
struct JsonVisitor {
    fields: BTreeMap<String, serde_json::Value>,
}

impl Visit for JsonVisitor {
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.fields.insert(field.name().to_string(), json!(value));
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.fields.insert(field.name().to_string(), json!(value));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.fields.insert(field.name().to_string(), json!(value));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.fields.insert(field.name().to_string(), json!(value));
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.fields
            .insert(field.name().to_string(), json!(format!("{value:?}")));
    }
}

#[cfg(test)]
mod tests {
    use super::should_sample;

    #[test]
    fn sampler_bounds_are_stable() {
        assert!(!should_sample(0));
        assert!(should_sample(1_000_000));
    }
}
