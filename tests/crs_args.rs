use clap::{error::ErrorKind, Parser};
use crius::crs::args::{Args, Command, ObjectType, StopObjectType};
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
    let args = Args::try_parse_from(["crs", "version", "--debug"])
        .expect("global options should parse after the command");

    assert!(args.debug);
}

#[test]
fn rejects_invalid_output_value() {
    let error = Args::try_parse_from(["crs", "--output", "xml", "version"])
        .expect_err("invalid output value should fail parsing");

    assert_eq!(error.kind(), ErrorKind::InvalidValue);
}

#[test]
fn parses_top_level_shortcut_commands() {
    let cases: &[(&[&str], fn(&Command) -> bool)] = &[
        (&["crs", "version"], |command| matches!(command, Command::Version(_))),
        (&["crs", "status"], |command| matches!(command, Command::Status(_))),
        (&["crs", "doctor"], |command| matches!(command, Command::Doctor(_))),
        (&["crs", "ps"], |command| matches!(command, Command::Ps(_))),
        (&["crs", "pods"], |command| matches!(command, Command::Pods(_))),
        (&["crs", "images"], |command| matches!(command, Command::Images(_))),
        (&["crs", "pull", "busybox"], |command| matches!(command, Command::Pull(_))),
        (&["crs", "inspect", "abc123"], |command| {
            matches!(command, Command::Inspect(_))
        }),
        (&["crs", "logs", "abc123"], |command| matches!(command, Command::Logs(_))),
        (&["crs", "exec", "abc123", "--", "echo"], |command| {
            matches!(command, Command::Exec(_))
        }),
        (&["crs", "stop", "abc123"], |command| matches!(command, Command::Stop(_))),
        (&["crs", "rm", "abc123"], |command| matches!(command, Command::Rm(_))),
    ];

    for (argv, assert_command) in cases {
        let args = Args::try_parse_from(*argv).unwrap_or_else(|error| {
            panic!("failed to parse {argv:?}: {error}");
        });

        assert!(assert_command(&args.command), "unexpected command for {argv:?}");
    }
}

#[test]
fn parses_top_level_command_groups() {
    let cases: &[(&[&str], fn(&Command) -> bool)] = &[
        (&["crs", "config", "show"], |command| matches!(command, Command::Config(_))),
        (&["crs", "runtime", "config"], |command| {
            matches!(command, Command::Runtime(_))
        }),
        (&["crs", "image", "list"], |command| matches!(command, Command::Image(_))),
        (&["crs", "pod", "list"], |command| matches!(command, Command::Pod(_))),
        (&["crs", "container", "list"], |command| {
            matches!(command, Command::Container(_))
        }),
        (&["crs", "run", "busybox"], |command| matches!(command, Command::Run(_))),
        (&["crs", "events"], |command| matches!(command, Command::Events(_))),
        (&["crs", "stats"], |command| matches!(command, Command::Stats(_))),
        (&["crs", "metrics", "descriptors"], |command| {
            matches!(command, Command::Metrics(_))
        }),
        (&["crs", "recovery", "status"], |command| {
            matches!(command, Command::Recovery(_))
        }),
        (&["crs", "gc", "candidates"], |command| matches!(command, Command::Gc(_))),
        (&["crs", "debug", "network"], |command| matches!(command, Command::Debug(_))),
        (&["crs", "completion", "bash"], |command| {
            matches!(command, Command::Completion(_))
        }),
    ];

    for (argv, assert_command) in cases {
        let args = Args::try_parse_from(*argv).unwrap_or_else(|error| {
            panic!("failed to parse {argv:?}: {error}");
        });

        assert!(assert_command(&args.command), "unexpected command for {argv:?}");
    }
}

#[test]
fn parses_base_and_multitype_shortcut_args() {
    let args =
        Args::try_parse_from(["crs", "inspect", "--type", "image", "busybox:latest"])
            .expect("inspect object type should parse");
    let Command::Inspect(inspect) = args.command else {
        panic!("expected inspect command");
    };
    assert_eq!(inspect.object_type, Some(ObjectType::Image));
    assert_eq!(inspect.target, "busybox:latest");

    let args = Args::try_parse_from(["crs", "stop", "--type", "pod", "--timeout", "10", "pod1"])
        .expect("stop object type and timeout should parse");
    let Command::Stop(stop) = args.command else {
        panic!("expected stop command");
    };
    assert_eq!(stop.object_type, Some(StopObjectType::Pod));
    assert_eq!(stop.timeout, Some(10));
    assert_eq!(stop.target, "pod1");

    let args =
        Args::try_parse_from(["crs", "rm", "--force", "--type", "container", "container1"])
            .expect("remove force should parse");
    let Command::Rm(remove) = args.command else {
        panic!("expected rm command");
    };
    assert!(remove.force);
    assert_eq!(remove.object_type, Some(ObjectType::Container));
    assert_eq!(remove.target, "container1");
}
