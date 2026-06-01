#![allow(dead_code)]

use serde::Serialize;
use serde_json::{json, Value};

use crate::crs::{args::OutputArg, ids::truncate_field};

pub(crate) const API_VERSION: &str = "crius.io/crs/v1";

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
}

pub(crate) fn print_envelope<T>(output: &CommandOutput<T>) -> Result<String, serde_json::Error>
where
    T: Serialize,
{
    serde_json::to_string_pretty(output)
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
        &headers.iter().map(|value| value.to_string()).collect::<Vec<_>>(),
        &widths,
    ));
    lines.extend(rows.iter().map(|row| format_row(row, &widths)));
    lines.join("\n")
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
}
