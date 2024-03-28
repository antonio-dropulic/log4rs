use std::{fmt::format, process::Command, sync::OnceLock};

use log::info;
use log4rs::{
    append::{
        console::ConsoleAppender,
        rolling_file::{
            policy::{
                compound::{
                    roll::fixed_window::{Compression, FixedWindowRoller},
                    trigger::{size::SizeTrigger, Trigger},
                    CompoundPolicy,
                },
                Policy,
            },
            RollingFileAppender,
        },
    },
    config::{
        runtime::{ConfigBuilder, RootBuilder},
        Appender, Root,
    },
    Config, Handle,
};

fn execute_test(env_key: &str, env_val: &str) {
    let mut child_proc = Command::new("cargo")
        .args(&["run", "--example", "compile_time_config"])
        .env(env_key, env_val)
        .spawn()
        .expect("Cargo command failed to start");

    let ecode = child_proc.wait().expect("failed to wait on child");

    assert!(ecode.success());
}

// Maintaining as a single test to avoid blocking calls to the package cache
#[test]
fn test_no_color() {
    let keys = vec!["NO_COLOR", "CLICOLOR_FORCE", "CLICOLOR"];

    for key in keys {
        execute_test(key, "1");
        execute_test(key, "0");
    }
}

static HANDLE: OnceLock<log4rs::Handle> = OnceLock::new();

// TODO: this should be an example
#[test]
fn rolling_log() {
    let roller = FixedWindowRoller::builder()
        .base(1)
        .build(Compression::None, 5)
        .unwrap();

    // every entry should be a new log
    let trigger = SizeTrigger::new(1);
    let roll_policy = CompoundPolicy::new(Box::new(trigger), Box::new(roller));

    let appender_pattern = format!("log-name-{}-{}.log", "{TIME}", "{}");

    let appender = RollingFileAppender::builder()
        .append(false)
        .base(0)
        .build(appender_pattern, Box::new(roll_policy))
        .unwrap();

    let cfg = Config::builder();
    let root = Root::builder()
        .appender("rolland")
        .build(log::LevelFilter::Trace);
    let config = cfg
        .appender(Appender::builder().build("rolland", Box::new(appender)))
        .build(root)
        .unwrap();

    let handle = log4rs::config::init_config_with_err_handler(
        config,
        Box::new(|e| {
            dbg!(e);
        }),
    )
    .unwrap();

    HANDLE.get_or_init(|| handle);

    // TODO: how do i get this to show up in the log files
    for i in 0..10 {
        info!("LOG NUMBER {i}")
    }
}
