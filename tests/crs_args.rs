use clap::{error::ErrorKind, Parser};
use crius::crs::args::Args;
use std::time::Duration;

#[test]
fn parses_top_level_help() {
    let error = Args::try_parse_from(["crs", "--help"]).expect_err("help should short-circuit");

    assert_eq!(error.kind(), ErrorKind::DisplayHelp);
}

#[test]
fn parses_version_help() {
    let error =
        Args::try_parse_from(["crs", "version", "--help"]).expect_err("help should short-circuit");

    assert_eq!(error.kind(), ErrorKind::DisplayHelp);
}

#[test]
fn parses_minimal_version_command() {
    let args = Args::try_parse_from(["crs", "version"]).expect("version should parse");

    assert!(matches!(args.command, crius::crs::args::Command::Version(_)));
}

#[test]
fn parses_global_options_before_command() {
    let args = Args::try_parse_from([
        "crs",
        "--address",
        "unix:///tmp/crius.sock",
        "--connect-timeout",
        "500ms",
        "--timeout",
        "2m",
        "--output",
        "json",
        "--quiet",
        "--no-trunc",
        "version",
    ])
    .expect("global options should parse before the command");

    assert_eq!(args.address, "unix:///tmp/crius.sock");
    assert_eq!(args.connect_timeout, Duration::from_millis(500));
    assert_eq!(args.timeout, Duration::from_secs(120));
    assert_eq!(args.output, crius::crs::args::OutputArg::Json);
    assert!(args.quiet);
    assert!(args.no_trunc);
}

#[test]
fn parses_global_options_after_command() {
    let args = Args::try_parse_from(["crs", "version", "--timeout", "5s", "--debug"])
        .expect("global options should parse after the command");

    assert_eq!(args.timeout, Duration::from_secs(5));
    assert!(args.debug);
}

#[test]
fn rejects_invalid_output_value() {
    let error = Args::try_parse_from(["crs", "--output", "xml", "version"])
        .expect_err("invalid output value should fail parsing");

    assert_eq!(error.kind(), ErrorKind::InvalidValue);
}
