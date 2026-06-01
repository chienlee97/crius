use clap::{error::ErrorKind, Parser};
use crius::crs::args::Args;

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
