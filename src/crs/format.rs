#![allow(dead_code)]

use serde::Serialize;
use serde_json::{json, Value};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::crs::{args::OutputArg, context::CliContext, ids::truncate_field};

pub(crate) const API_VERSION: &str = "crius.io/crs/v1";

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct FormatOptions {
    output: OutputArg,
    quiet: bool,
    no_trunc: bool,
}

impl FormatOptions {
    pub(crate) fn from_context(ctx: &CliContext) -> Self {
        Self {
            output: ctx.output(),
            quiet: ctx.quiet(),
            no_trunc: ctx.no_trunc(),
        }
    }

    pub(crate) fn output(self) -> OutputArg {
        self.output
    }

    pub(crate) fn quiet(self) -> bool {
        self.quiet
    }

    pub(crate) fn no_trunc(self) -> bool {
        self.no_trunc
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CommandOutput<T>
where
    T: Serialize,
{
    pub kind: &'static str,
    pub api_version: &'static str,
    pub endpoint: String,
    pub items: Vec<T>,
    pub summary: Value,
    pub warnings: Vec<String>,
}

impl<T> CommandOutput<T>
where
    T: Serialize,
{
    pub(crate) fn new(kind: &'static str, endpoint: impl Into<String>, items: Vec<T>) -> Self {
        let count = items.len();
        Self {
            kind,
            api_version: API_VERSION,
            endpoint: endpoint.into(),
            items,
            summary: json!({ "count": count }),
            warnings: Vec::new(),
        }
    }

    pub(crate) fn with_summary(mut self, summary: Value) -> Self {
        self.summary = summary;
        self
    }

    pub(crate) fn with_warnings(mut self, warnings: Vec<String>) -> Self {
        self.warnings = warnings;
        self
    }
}

pub(crate) trait TableRow {
    fn headers() -> &'static [&'static str]
    where
        Self: Sized;
    fn cells(&self) -> Vec<String>;
    fn quiet_cell(&self) -> String {
        self.cells().into_iter().next().unwrap_or_default()
    }
}

pub(crate) fn print_envelope<T>(output: &CommandOutput<T>) -> Result<String, serde_json::Error>
where
    T: Serialize,
{
    serde_json::to_string_pretty(output)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RenderedOutput {
    pub stdout: String,
    pub stderr: String,
}

pub(crate) fn render_output<T>(
    output: &CommandOutput<T>,
    options: FormatOptions,
) -> Result<RenderedOutput, serde_json::Error>
where
    T: Serialize + TableRow,
{
    let stdout = match options.output() {
        OutputArg::Json => print_envelope(output)?,
        OutputArg::Table | OutputArg::Text if options.quiet() => {
            print_quiet(&output.items, options.no_trunc())
        }
        OutputArg::Table | OutputArg::Text => print_table(&output.items, options.no_trunc()),
    };

    let stderr = render_warnings(&output.warnings, options.output()).unwrap_or_default();

    Ok(RenderedOutput { stdout, stderr })
}

pub(crate) fn print_table<T>(items: &[T], no_trunc: bool) -> String
where
    T: TableRow,
{
    let headers = T::headers();
    let rows: Vec<Vec<String>> = items
        .iter()
        .map(|item| {
            item.cells()
                .into_iter()
                .map(|cell| truncate_field(&normalize_cell(&cell), no_trunc))
                .collect()
        })
        .collect();

    let widths: Vec<usize> = headers
        .iter()
        .enumerate()
        .map(|(index, header)| {
            rows.iter()
                .filter_map(|row| row.get(index))
                .map(|cell| cell.len())
                .max()
                .unwrap_or(0)
                .max(header.len())
        })
        .collect();

    let mut lines = Vec::with_capacity(rows.len() + 1);
    lines.push(format_row(
        &headers
            .iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>(),
        &widths,
    ));
    lines.extend(rows.iter().map(|row| format_row(row, &widths)));
    lines.join("\n")
}

pub(crate) fn print_quiet<T>(items: &[T], no_trunc: bool) -> String
where
    T: TableRow,
{
    items
        .iter()
        .map(|item| truncate_field(&normalize_cell(&item.quiet_cell()), no_trunc))
        .collect::<Vec<_>>()
        .join("\n")
}

pub(crate) fn render_warnings(warnings: &[String], output: OutputArg) -> Option<String> {
    match output {
        OutputArg::Json => None,
        OutputArg::Table | OutputArg::Text if warnings.is_empty() => None,
        OutputArg::Table | OutputArg::Text => Some(
            warnings
                .iter()
                .map(|warning| format!("warning: {warning}"))
                .collect::<Vec<_>>()
                .join("\n"),
        ),
    }
}

fn format_row(cells: &[String], widths: &[usize]) -> String {
    cells
        .iter()
        .enumerate()
        .map(|(index, cell)| format!("{cell:<width$}", width = widths[index]))
        .collect::<Vec<_>>()
        .join("  ")
        .trim_end()
        .to_string()
}

pub(crate) fn normalize_cell(value: &str) -> String {
    let value = value.replace(['\n', '\r'], " ");
    if value.is_empty() {
        "-".to_string()
    } else {
        value
    }
}

pub(crate) fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }

    if unit == 0 {
        format!("{bytes}B")
    } else {
        format!("{value:.1}{}", UNITS[unit])
    }
}

pub(crate) fn format_cpu_millis(nano_cores: u64) -> String {
    format!("{}m", nano_cores / 1_000_000)
}

pub(crate) fn format_bool(value: bool) -> &'static str {
    if value {
        "true"
    } else {
        "false"
    }
}

pub(crate) fn format_unix_nanos(unix_nanos: i64, now: SystemTime) -> String {
    let timestamp = if unix_nanos >= 0 {
        UNIX_EPOCH + Duration::from_nanos(unix_nanos as u64)
    } else {
        UNIX_EPOCH
    };

    let age = now.duration_since(timestamp).unwrap_or_default();
    if age < Duration::from_secs(24 * 60 * 60) {
        return format_relative_duration(age);
    }

    let seconds = unix_nanos.div_euclid(1_000_000_000).max(0) as u64;
    chrono::DateTime::<chrono::Local>::from(UNIX_EPOCH + Duration::from_secs(seconds))
        .format("%Y-%m-%d %H:%M:%S")
        .to_string()
}

fn format_relative_duration(duration: Duration) -> String {
    let seconds = duration.as_secs();
    match seconds {
        0..=59 => format!("{seconds}s ago"),
        60..=3_599 => format!("{}m ago", seconds / 60),
        _ => format!("{}h ago", seconds / 3_600),
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RuntimeVersionView {
    pub runtime_name: String,
    pub runtime_version: String,
    pub runtime_api_version: String,
}

impl TableRow for RuntimeVersionView {
    fn headers() -> &'static [&'static str] {
        &["RUNTIME", "VERSION", "API VERSION"]
    }

    fn cells(&self) -> Vec<String> {
        vec![
            self.runtime_name.clone(),
            self.runtime_version.clone(),
            self.runtime_api_version.clone(),
        ]
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RuntimeStatusView {
    pub runtime_ready: bool,
    pub network_ready: bool,
    pub conditions: Vec<ConditionView>,
    pub info_json: Value,
    pub info_raw: Value,
}

impl TableRow for RuntimeStatusView {
    fn headers() -> &'static [&'static str] {
        &["RUNTIME READY", "NETWORK READY", "CONDITIONS"]
    }

    fn cells(&self) -> Vec<String> {
        vec![
            format_bool(self.runtime_ready).to_string(),
            format_bool(self.network_ready).to_string(),
            self.conditions.len().to_string(),
        ]
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ConditionView {
    pub kind: String,
    pub status: bool,
    pub reason: String,
    pub message: String,
}

impl TableRow for ConditionView {
    fn headers() -> &'static [&'static str] {
        &["TYPE", "STATUS", "REASON", "MESSAGE"]
    }

    fn cells(&self) -> Vec<String> {
        vec![
            self.kind.clone(),
            format_bool(self.status).to_string(),
            self.reason.clone(),
            self.message.clone(),
        ]
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RuntimeConfigView {
    pub cgroup_driver: String,
}

impl TableRow for RuntimeConfigView {
    fn headers() -> &'static [&'static str] {
        &["CGROUP DRIVER"]
    }

    fn cells(&self) -> Vec<String> {
        vec![self.cgroup_driver.clone()]
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RuntimeConfigUpdateView {
    pub pod_cidrs: Vec<String>,
    pub updated: bool,
}

impl TableRow for RuntimeConfigUpdateView {
    fn headers() -> &'static [&'static str] {
        &["POD CIDRS", "UPDATED"]
    }

    fn cells(&self) -> Vec<String> {
        vec![
            self.pod_cidrs.join(","),
            format_bool(self.updated).to_string(),
        ]
    }

    fn quiet_cell(&self) -> String {
        self.pod_cidrs.join(",")
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct EffectiveConfigView {
    pub config: Value,
    pub redacted_fields: Vec<String>,
}

impl TableRow for EffectiveConfigView {
    fn headers() -> &'static [&'static str] {
        &["REDACTED FIELDS", "CONFIG KEYS"]
    }

    fn cells(&self) -> Vec<String> {
        vec![
            self.redacted_fields.join(","),
            value_object_keys(&self.config).join(","),
        ]
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ConfigReloadStatusView {
    pub watcher: String,
    pub last_reload: String,
    pub last_error: String,
    pub cni_watcher: String,
}

impl TableRow for ConfigReloadStatusView {
    fn headers() -> &'static [&'static str] {
        &["WATCHER", "LAST RELOAD", "LAST ERROR", "CNI WATCHER"]
    }

    fn cells(&self) -> Vec<String> {
        vec![
            self.watcher.clone(),
            self.last_reload.clone(),
            self.last_error.clone(),
            self.cni_watcher.clone(),
        ]
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RuntimeHandlerView {
    pub name: String,
    pub runtime_type: String,
    pub runtime_path: String,
    pub runtime_config_path: String,
    pub features: Vec<String>,
    pub warnings: Vec<String>,
}

impl TableRow for RuntimeHandlerView {
    fn headers() -> &'static [&'static str] {
        &["NAME", "TYPE", "PATH", "CONFIG", "FEATURES", "WARNINGS"]
    }

    fn cells(&self) -> Vec<String> {
        vec![
            self.name.clone(),
            self.runtime_type.clone(),
            self.runtime_path.clone(),
            self.runtime_config_path.clone(),
            self.features.join(","),
            self.warnings.join(","),
        ]
    }

    fn quiet_cell(&self) -> String {
        self.name.clone()
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ImageTransferView {
    pub image: String,
    pub status: String,
    pub updated: String,
    pub error: String,
}

impl TableRow for ImageTransferView {
    fn headers() -> &'static [&'static str] {
        &["IMAGE", "STATUS", "UPDATED", "ERROR"]
    }

    fn cells(&self) -> Vec<String> {
        vec![
            self.image.clone(),
            self.status.clone(),
            self.updated.clone(),
            self.error.clone(),
        ]
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ImageConfigView {
    pub snapshotter: String,
    pub policy: String,
    pub auth_configured: String,
    pub pinned_images: String,
    pub config: Value,
}

impl TableRow for ImageConfigView {
    fn headers() -> &'static [&'static str] {
        &["SNAPSHOTTER", "POLICY", "AUTH", "PINNED IMAGES"]
    }

    fn cells(&self) -> Vec<String> {
        vec![
            self.snapshotter.clone(),
            self.policy.clone(),
            self.auth_configured.clone(),
            self.pinned_images.clone(),
        ]
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RecoveryStatusView {
    pub status: String,
    pub last_startup: String,
    pub unhealthy_object_count: u64,
    pub ledger_summary: Value,
    pub warnings: Vec<String>,
}

impl TableRow for RecoveryStatusView {
    fn headers() -> &'static [&'static str] {
        &["STATUS", "LAST STARTUP", "UNHEALTHY", "WARNINGS"]
    }

    fn cells(&self) -> Vec<String> {
        vec![
            self.status.clone(),
            self.last_startup.clone(),
            self.unhealthy_object_count.to_string(),
            self.warnings.join(","),
        ]
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RecoveryActionView {
    pub object_type: String,
    pub object_id: String,
    pub action: String,
    pub reason: String,
    pub executed: bool,
    pub error: String,
}

impl TableRow for RecoveryActionView {
    fn headers() -> &'static [&'static str] {
        &["TYPE", "ID", "ACTION", "EXECUTED", "ERROR", "REASON"]
    }

    fn cells(&self) -> Vec<String> {
        vec![
            self.object_type.clone(),
            self.object_id.clone(),
            self.action.clone(),
            format_bool(self.executed).to_string(),
            self.error.clone(),
            self.reason.clone(),
        ]
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GcCandidateView {
    pub object_type: String,
    pub object_id: String,
    pub path: String,
    pub reason: String,
    pub size_bytes: u64,
    pub deleted: bool,
    pub error: String,
}

impl TableRow for GcCandidateView {
    fn headers() -> &'static [&'static str] {
        &["TYPE", "ID", "SIZE", "DELETED", "ERROR", "REASON", "PATH"]
    }

    fn cells(&self) -> Vec<String> {
        vec![
            self.object_type.clone(),
            self.object_id.clone(),
            format_bytes(self.size_bytes),
            format_bool(self.deleted).to_string(),
            self.error.clone(),
            self.reason.clone(),
            self.path.clone(),
        ]
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DebugView {
    pub check: String,
    pub status: String,
    pub message: String,
    pub details: Value,
}

impl TableRow for DebugView {
    fn headers() -> &'static [&'static str] {
        &["CHECK", "STATUS", "MESSAGE"]
    }

    fn cells(&self) -> Vec<String> {
        vec![
            self.check.clone(),
            self.status.clone(),
            self.message.clone(),
        ]
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ImageView {
    pub image: String,
    pub image_id: String,
    pub size_bytes: u64,
    pub user_spec: String,
    pub pinned: bool,
}

fn value_object_keys(value: &Value) -> Vec<String> {
    value
        .as_object()
        .map(|object| object.keys().cloned().collect())
        .unwrap_or_default()
}

impl TableRow for ImageView {
    fn headers() -> &'static [&'static str] {
        &["IMAGE", "IMAGE ID", "SIZE", "USER SPEC", "PINNED"]
    }

    fn cells(&self) -> Vec<String> {
        vec![
            self.image.clone(),
            self.image_id.clone(),
            format_bytes(self.size_bytes),
            self.user_spec.clone(),
            format_bool(self.pinned).to_string(),
        ]
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ImageOperationView {
    pub image: String,
    pub image_ref: String,
    pub action: String,
    pub success: bool,
}

impl TableRow for ImageOperationView {
    fn headers() -> &'static [&'static str] {
        &["IMAGE", "IMAGE REF", "ACTION", "SUCCESS"]
    }

    fn cells(&self) -> Vec<String> {
        vec![
            self.image.clone(),
            self.image_ref.clone(),
            self.action.clone(),
            format_bool(self.success).to_string(),
        ]
    }

    fn quiet_cell(&self) -> String {
        if self.image_ref.is_empty() {
            self.image.clone()
        } else {
            self.image_ref.clone()
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct InspectView {
    pub object_type: String,
    pub id: String,
    pub response: Value,
    pub info_json: Value,
    pub info_raw: Value,
}

impl TableRow for InspectView {
    fn headers() -> &'static [&'static str] {
        &["TYPE", "ID"]
    }

    fn cells(&self) -> Vec<String> {
        vec![self.object_type.clone(), self.id.clone()]
    }

    fn quiet_cell(&self) -> String {
        self.id.clone()
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FilesystemUsageView {
    pub kind: String,
    pub mountpoint: String,
    pub used_bytes: u64,
    pub inodes_used: u64,
    pub timestamp: i64,
}

impl TableRow for FilesystemUsageView {
    fn headers() -> &'static [&'static str] {
        &["KIND", "MOUNTPOINT", "USED", "INODES", "TIMESTAMP"]
    }

    fn cells(&self) -> Vec<String> {
        vec![
            self.kind.clone(),
            self.mountpoint.clone(),
            format_bytes(self.used_bytes),
            self.inodes_used.to_string(),
            self.timestamp.to_string(),
        ]
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PodView {
    pub pod_id: String,
    pub name: String,
    pub namespace: String,
    pub state: String,
    pub ip: String,
    pub created: String,
    pub attempt: u32,
}

impl TableRow for PodView {
    fn headers() -> &'static [&'static str] {
        &[
            "POD ID",
            "NAME",
            "NAMESPACE",
            "STATE",
            "IP",
            "CREATED",
            "ATTEMPT",
        ]
    }

    fn cells(&self) -> Vec<String> {
        vec![
            self.pod_id.clone(),
            self.name.clone(),
            self.namespace.clone(),
            self.state.clone(),
            self.ip.clone(),
            self.created.clone(),
            self.attempt.to_string(),
        ]
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PodOperationView {
    pub pod_id: String,
    pub name: String,
    pub namespace: String,
    pub action: String,
    pub success: bool,
}

impl TableRow for PodOperationView {
    fn headers() -> &'static [&'static str] {
        &["POD ID", "NAME", "NAMESPACE", "ACTION", "SUCCESS"]
    }

    fn cells(&self) -> Vec<String> {
        vec![
            self.pod_id.clone(),
            self.name.clone(),
            self.namespace.clone(),
            self.action.clone(),
            format_bool(self.success).to_string(),
        ]
    }

    fn quiet_cell(&self) -> String {
        self.pod_id.clone()
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ContainerView {
    pub container_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub pod: String,
    pub image: String,
    pub state: String,
    pub created: String,
    pub name: String,
    pub attempt: u32,
}

impl TableRow for ContainerView {
    fn headers() -> &'static [&'static str] {
        &[
            "CONTAINER ID",
            "POD",
            "IMAGE",
            "STATE",
            "CREATED",
            "NAME",
            "ATTEMPT",
        ]
    }

    fn cells(&self) -> Vec<String> {
        vec![
            self.container_id.clone(),
            self.pod.clone(),
            self.image.clone(),
            self.state.clone(),
            self.created.clone(),
            self.name.clone(),
            self.attempt.to_string(),
        ]
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ContainerOperationView {
    pub container_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub pod_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub image: String,
    pub action: String,
    pub success: bool,
}

impl TableRow for ContainerOperationView {
    fn headers() -> &'static [&'static str] {
        &["CONTAINER ID", "POD ID", "IMAGE", "ACTION", "SUCCESS"]
    }

    fn cells(&self) -> Vec<String> {
        vec![
            self.container_id.clone(),
            self.pod_id.clone(),
            self.image.clone(),
            self.action.clone(),
            format_bool(self.success).to_string(),
        ]
    }

    fn quiet_cell(&self) -> String {
        self.container_id.clone()
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ContainerEventView {
    pub time: String,
    pub event_type: String,
    pub container_id: String,
    pub pod_id: String,
    pub created_at_unix_nanos: i64,
}

impl TableRow for ContainerEventView {
    fn headers() -> &'static [&'static str] {
        &["TIME", "TYPE", "CONTAINER", "POD"]
    }

    fn cells(&self) -> Vec<String> {
        vec![
            self.time.clone(),
            self.event_type.clone(),
            self.container_id.clone(),
            self.pod_id.clone(),
        ]
    }

    fn quiet_cell(&self) -> String {
        self.container_id.clone()
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ResourceUsageView {
    pub id: String,
    pub name: String,
    pub cpu_nano_cores: u64,
    pub memory_bytes: u64,
    pub pids: u64,
}

impl TableRow for ResourceUsageView {
    fn headers() -> &'static [&'static str] {
        &["ID", "NAME", "CPU", "MEMORY", "PIDS"]
    }

    fn cells(&self) -> Vec<String> {
        vec![
            self.id.clone(),
            self.name.clone(),
            format_cpu_millis(self.cpu_nano_cores),
            format_bytes(self.memory_bytes),
            self.pids.to_string(),
        ]
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PodStatsView {
    pub id: String,
    pub name: String,
    pub namespace: String,
    pub cpu_nano_cores: u64,
    pub memory_bytes: u64,
    pub network_bytes: u64,
    pub pids: u64,
}

impl TableRow for PodStatsView {
    fn headers() -> &'static [&'static str] {
        &[
            "POD ID",
            "NAME",
            "NAMESPACE",
            "CPU",
            "MEMORY",
            "NETWORK",
            "PIDS",
        ]
    }

    fn cells(&self) -> Vec<String> {
        vec![
            self.id.clone(),
            self.name.clone(),
            self.namespace.clone(),
            format_cpu_millis(self.cpu_nano_cores),
            format_bytes(self.memory_bytes),
            format_bytes(self.network_bytes),
            self.pids.to_string(),
        ]
    }

    fn quiet_cell(&self) -> String {
        self.id.clone()
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PodMetricsView {
    pub pod_id: String,
    pub pod_metric_count: usize,
    pub container_metric_count: usize,
    pub total_metric_count: usize,
}

impl TableRow for PodMetricsView {
    fn headers() -> &'static [&'static str] {
        &["POD ID", "POD METRICS", "CONTAINER METRICS", "TOTAL"]
    }

    fn cells(&self) -> Vec<String> {
        vec![
            self.pod_id.clone(),
            self.pod_metric_count.to_string(),
            self.container_metric_count.to_string(),
            self.total_metric_count.to_string(),
        ]
    }

    fn quiet_cell(&self) -> String {
        self.pod_id.clone()
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MetricDescriptorView {
    pub name: String,
    pub help: String,
    pub metric_type: String,
    pub labels: Vec<String>,
}

impl TableRow for MetricDescriptorView {
    fn headers() -> &'static [&'static str] {
        &["NAME", "HELP", "TYPE", "LABELS"]
    }

    fn cells(&self) -> Vec<String> {
        vec![
            self.name.clone(),
            self.help.clone(),
            self.metric_type.clone(),
            self.labels.join(","),
        ]
    }

    fn quiet_cell(&self) -> String {
        self.name.clone()
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MetricsScrapeView {
    pub content_type: String,
    pub bytes: usize,
    pub scraped_at: String,
}

impl TableRow for MetricsScrapeView {
    fn headers() -> &'static [&'static str] {
        &["CONTENT TYPE", "BYTES", "SCRAPED AT"]
    }

    fn cells(&self) -> Vec<String> {
        vec![
            self.content_type.clone(),
            self.bytes.to_string(),
            self.scraped_at.clone(),
        ]
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct OperationView {
    pub target: String,
    pub status: String,
    pub message: String,
}

impl TableRow for OperationView {
    fn headers() -> &'static [&'static str] {
        &["TARGET", "STATUS", "MESSAGE"]
    }

    fn cells(&self) -> Vec<String> {
        vec![
            self.target.clone(),
            self.status.clone(),
            self.message.clone(),
        ]
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DoctorCheckView {
    pub check: String,
    pub status: String,
    pub message: String,
}

impl TableRow for DoctorCheckView {
    fn headers() -> &'static [&'static str] {
        &["CHECK", "STATUS", "MESSAGE"]
    }

    fn cells(&self) -> Vec<String> {
        vec![
            self.check.clone(),
            self.status.clone(),
            self.message.clone(),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Serialize)]
    struct TestView {
        id: String,
        name: String,
    }

    impl TableRow for TestView {
        fn headers() -> &'static [&'static str] {
            &["ID", "NAME"]
        }

        fn cells(&self) -> Vec<String> {
            vec![self.id.clone(), self.name.clone()]
        }
    }

    #[test]
    fn renders_table_with_stable_columns() {
        let rows = vec![
            TestView {
                id: "abc".into(),
                name: "first".into(),
            },
            TestView {
                id: "".into(),
                name: "two\nlines".into(),
            },
        ];

        let rendered = print_table(&rows, false);

        assert_eq!(rendered, "ID   NAME\nabc  first\n-    two lines");
    }

    #[test]
    fn renders_quiet_without_header_or_envelope() {
        let rows = vec![
            TestView {
                id: "abc".into(),
                name: "first".into(),
            },
            TestView {
                id: "def".into(),
                name: "second".into(),
            },
        ];

        assert_eq!(print_quiet(&rows, false), "abc\ndef");
    }

    #[test]
    fn renders_json_envelope() {
        let output = CommandOutput::new(
            "TestKind",
            "unix:///tmp/crius.sock",
            vec![TestView {
                id: "abc".into(),
                name: "first".into(),
            }],
        )
        .with_summary(json!({ "count": 1 }))
        .with_warnings(vec!["diagnostics unavailable".into()]);

        let rendered = print_envelope(&output).expect("json should render");
        let value: Value = serde_json::from_str(&rendered).expect("json should parse");

        assert_eq!(value["kind"], "TestKind");
        assert_eq!(value["apiVersion"], API_VERSION);
        assert_eq!(value["summary"]["count"], 1);
        assert_eq!(value["warnings"][0], "diagnostics unavailable");
    }

    #[test]
    fn renders_warnings_only_for_human_outputs() {
        let warnings = vec!["fallback used".to_string()];

        assert_eq!(
            render_warnings(&warnings, OutputArg::Table).as_deref(),
            Some("warning: fallback used")
        );
        assert_eq!(render_warnings(&warnings, OutputArg::Json), None);
    }

    #[test]
    fn renders_output_with_warnings_in_the_expected_stream() {
        let output = CommandOutput::new(
            "TestKind",
            "unix:///tmp/crius.sock",
            vec![TestView {
                id: "abc".into(),
                name: "first".into(),
            }],
        )
        .with_warnings(vec!["fallback used".into()]);

        let human = render_output(
            &output,
            FormatOptions {
                output: OutputArg::Table,
                quiet: false,
                no_trunc: false,
            },
        )
        .expect("table should render");

        assert_eq!(human.stdout, "ID   NAME\nabc  first");
        assert_eq!(human.stderr, "warning: fallback used");

        let json = render_output(
            &output,
            FormatOptions {
                output: OutputArg::Json,
                quiet: false,
                no_trunc: false,
            },
        )
        .expect("json should render");
        let value: Value = serde_json::from_str(&json.stdout).expect("json should parse");

        assert_eq!(json.stderr, "");
        assert_eq!(value["warnings"][0], "fallback used");
    }

    #[test]
    fn formats_numbers() {
        assert_eq!(format_bytes(0), "0B");
        assert_eq!(format_bytes(64 * 1024 * 1024), "64.0MiB");
        assert_eq!(format_cpu_millis(125_000_000), "125m");
        assert_eq!(format_bool(true), "true");
        assert_eq!(format_bool(false), "false");
    }

    #[test]
    fn formats_times() {
        let now = UNIX_EPOCH + Duration::from_secs(3_600);

        assert_eq!(format_unix_nanos(3_590_000_000_000, now), "10s ago");
        assert_eq!(format_unix_nanos(3_000_000_000_000, now), "10m ago");
        assert_eq!(format_unix_nanos(0, now), "1h ago");

        let old_now = UNIX_EPOCH + Duration::from_secs(3 * 24 * 60 * 60);
        assert!(format_unix_nanos(0, old_now).starts_with("1970-"));
    }

    #[test]
    fn basic_views_have_expected_headers() {
        assert_eq!(
            RuntimeVersionView::headers(),
            &["RUNTIME", "VERSION", "API VERSION"]
        );
        assert_eq!(
            ImageView::headers(),
            &["IMAGE", "IMAGE ID", "SIZE", "USER SPEC", "PINNED"]
        );
        assert_eq!(
            PodView::headers(),
            &[
                "POD ID",
                "NAME",
                "NAMESPACE",
                "STATE",
                "IP",
                "CREATED",
                "ATTEMPT"
            ]
        );
        assert_eq!(
            ContainerView::headers(),
            &[
                "CONTAINER ID",
                "POD",
                "IMAGE",
                "STATE",
                "CREATED",
                "NAME",
                "ATTEMPT"
            ]
        );
        assert_eq!(
            ResourceUsageView::headers(),
            &["ID", "NAME", "CPU", "MEMORY", "PIDS"]
        );
    }

    #[test]
    fn adapts_format_options_from_context() {
        use clap::Parser;

        let args = crate::crs::args::Args::try_parse_from([
            "crs",
            "--output",
            "json",
            "--quiet",
            "--no-trunc",
            "version",
        ])
        .expect("args should parse");
        let ctx = CliContext::from_args(&args).expect("context should build");

        let options = FormatOptions::from_context(&ctx);

        assert_eq!(options.output(), OutputArg::Json);
        assert!(options.quiet());
        assert!(options.no_trunc());
    }
}
