use crate::crs::{
    args::{MetricsArgs, MetricsCommand, OutputArg},
    client::CrsClient,
    commands::{config::load_effective_config, status::render_and_print},
    context::CliContext,
    error::{CliError, CommandResult},
    format::{CommandOutput, MetricDescriptorView, MetricsScrapeView},
};
use crate::proto::runtime::v1::ListMetricDescriptorsRequest;

pub(crate) async fn handle(
    ctx: &CliContext,
    client: &CrsClient,
    args: MetricsArgs,
) -> Result<CommandResult, CliError> {
    match args.command {
        MetricsCommand::Descriptors => handle_descriptors(ctx, client).await,
        MetricsCommand::Scrape => handle_scrape(ctx, client).await,
    }
}

async fn handle_descriptors(
    ctx: &CliContext,
    client: &CrsClient,
) -> Result<CommandResult, CliError> {
    let mut runtime = client.runtime()?;
    let response = client
        .with_rpc_timeout(async {
            runtime
                .list_metric_descriptors(ListMetricDescriptorsRequest {})
                .await
                .map_err(|status| {
                    CliError::from_tonic_status(status)
                        .with_command("crs metrics descriptors")
                        .with_endpoint(client.endpoint())
                })
        })
        .await?
        .into_inner();
    let items = response
        .descriptors
        .into_iter()
        .map(|descriptor| MetricDescriptorView {
            name: descriptor.name,
            help: descriptor.help,
            metric_type: "unknown".to_string(),
            labels: descriptor.label_keys,
        })
        .collect();

    render_and_print(
        ctx,
        CommandOutput::new("MetricDescriptors", client.endpoint(), items),
    )
}

async fn handle_scrape(ctx: &CliContext, client: &CrsClient) -> Result<CommandResult, CliError> {
    let mut warnings = Vec::new();
    let (config, _) = load_effective_config(client, "crs metrics scrape", &mut warnings)
        .await
        .ok_or_else(|| {
            CliError::diagnostics_unavailable(client.endpoint()).with_command("crs metrics scrape")
        })?;
    let endpoint = metrics_endpoint(&config).ok_or_else(|| {
        CliError::from_tonic_status(tonic::Status::failed_precondition(
            "metrics endpoint is disabled; enable the daemon metrics endpoint before scraping",
        ))
        .with_command("crs metrics scrape")
        .with_endpoint(client.endpoint())
    })?;
    let url = metrics_url(&endpoint)?;
    let response = client
        .with_rpc_timeout(async {
            reqwest::get(&url).await.map_err(|source| {
                CliError::daemon_unavailable(
                    url.clone(),
                    format!("failed to scrape metrics endpoint: {source}"),
                )
                .with_command("crs metrics scrape")
            })
        })
        .await?;
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("text/plain")
        .to_string();
    let text = response.text().await.map_err(|source| {
        CliError::internal(format!("failed to read metrics response body: {source}"))
            .with_command("crs metrics scrape")
    })?;
    let bytes = text.len();
    let scraped_at = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);

    if matches!(ctx.output(), OutputArg::Text) {
        print!("{text}");
        return Ok(CommandResult::success());
    }

    render_and_print(
        ctx,
        CommandOutput::new(
            "MetricsScrape",
            client.endpoint(),
            vec![MetricsScrapeView {
                content_type: content_type.clone(),
                bytes,
                scraped_at: scraped_at.clone(),
            }],
        )
        .with_summary(serde_json::json!({
            "contentType": content_type,
            "bytes": bytes,
            "scrapedAt": scraped_at,
        }))
        .with_warnings(warnings),
    )
}

fn metrics_endpoint(config: &serde_json::Value) -> Option<String> {
    let metrics = config.get("metrics").unwrap_or(config);
    let enabled = metrics
        .get("enabled")
        .and_then(|value| value.as_bool())
        .unwrap_or(false);
    if !enabled {
        return None;
    }

    metrics
        .get("endpoint")
        .or_else(|| metrics.get("address"))
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn metrics_url(endpoint: &str) -> Result<String, CliError> {
    let endpoint = endpoint.trim();
    let base = if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else {
        format!("http://{endpoint}")
    };

    let url = if base.ends_with("/metrics") || base.contains("/metrics?") {
        base
    } else {
        format!("{}/metrics", base.trim_end_matches('/'))
    };

    reqwest::Url::parse(&url)
        .map(|url| url.to_string())
        .map_err(|source| {
            CliError::invalid_input(format!("invalid metrics endpoint {endpoint}: {source}"))
                .with_command("crs metrics scrape")
        })
}
