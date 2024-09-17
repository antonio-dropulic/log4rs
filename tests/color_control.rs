use std::{process::Command, sync::OnceLock, thread, time::Duration};

use log::info;
use log4rs::{
    append::rolling_file::{
        policy::compound::{
            roll::fixed_window::FixedWindowRoller,
            trigger::{
                size::SizeTrigger,
                time::{TimeTrigger, TimeTriggerConfig, TimeTriggerInterval},
            },
            CompoundPolicy,
        },
        RollingFileAppender,
    },
    config::{Appender, Root},
    Config,
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
// TODO: test with their time control lib
// TODO: this seems to work.. whats up with my lib
#[test]
#[ignore = "manual"]
fn rolling_log_with_size_trigger() {
    let pattern = format!("test_logs/log-name-{}-{}.log", "{TIME}", "{}");

    let max_log_count = 5;
    let roller = FixedWindowRoller::builder()
        .base(1)
        .build(pattern.clone(), max_log_count)
        .unwrap();

    // every entry should be a new log
    let trigger = SizeTrigger::new(1);
    let roll_policy = CompoundPolicy::new(Box::new(trigger), Box::new(roller));

    let appender = RollingFileAppender::builder()
        .append(false)
        .base(0)
        .build(pattern, Box::new(roll_policy))
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
    for i in 0..5 {
        info!("LOG NUMBER {i}");
        // sleep to notice the differences in timestamps
        thread::sleep(Duration::from_secs(1));
    }
}

#[test]
#[ignore = "manual"]
fn rolling_log_with_time_trigger() {
    let appender_pattern = format!("test_logs/log-name-{}-{}.log", "{TIME}", "{}");

    let max_log_count = 5;
    let roller = FixedWindowRoller::builder()
        .base(1)
        .build(appender_pattern.clone(), max_log_count)
        .unwrap();

    // new log entry every sec
    let trigger = TimeTrigger::new(TimeTriggerConfig {
        interval: TimeTriggerInterval::Second(1),
        modulate: false,
        max_random_delay: 0,
    });

    let roll_policy = CompoundPolicy::new(Box::new(trigger), Box::new(roller));

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
    for i in 0..5 {
        info!("LOG NUMBER {i}");
        thread::sleep(Duration::from_secs(2));
    }
}
