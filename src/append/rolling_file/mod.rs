//! A rolling file appender.
//!
//! Logging directly to a file can be a dangerous proposition for long running
//! processes. You wouldn't want to start a server up and find out a couple
//! weeks later that the disk is filled with hundreds of gigabytes of logs! A
//! rolling file appender alleviates these issues by limiting the amount of log
//! data that's preserved.
//!
//! Like a normal file appender, a rolling file appender is configured with the
//! location of its log file and the encoder which formats log events written
//! to it. In addition, it holds a "policy" object which controls when a log
//! file is rolled over and how the old files are archived.
//!
//! For example, you may configure an appender to roll the log over once it
//! reaches 50 megabytes, and to preserve the last 10 log files.
//!
//! Requires the `rolling_file_appender` feature.

use anyhow::Context;
use derivative::Derivative;
use log::Record;
use parking_lot::Mutex;
use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufWriter, Write},
    path::{Path, PathBuf},
    str::FromStr,
};

#[cfg(feature = "config_parsing")]
use serde_value::Value;
#[cfg(feature = "config_parsing")]
use std::collections::BTreeMap;

use crate::{
    append::Append,
    encode::{self, pattern::PatternEncoder, Encode},
};

#[cfg(feature = "config_parsing")]
use crate::config::{Deserialize, Deserializers};
#[cfg(feature = "config_parsing")]
use crate::encode::EncoderConfig;

use super::env_util;

pub mod policy;

/// Pattern used to replace DateTime in the logfile path
pub const TIME_PATTERN: &str = "{TIME}";
/// Pattern used to replace rolling log count
pub const COUNT_PATTERN: &str = "{}";
// TODO: USER SHOULD INJECT DATE FORMAT OR ATLEAST SELECT IT
const DATE_FORMAT_STR: &str = "%Y-%m-%d-%H:%M:%S";

/// Configuration for the rolling file appender.
#[cfg(feature = "config_parsing")]
#[derive(Clone, Eq, PartialEq, Hash, Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RollingFileAppenderConfig {
    path: String,
    append: Option<bool>,
    encoder: Option<EncoderConfig>,
    policy: Policy,
}

#[cfg(feature = "config_parsing")]
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
struct Policy {
    kind: String,
    config: Value,
}

#[cfg(feature = "config_parsing")]
impl<'de> serde::Deserialize<'de> for Policy {
    fn deserialize<D>(d: D) -> Result<Policy, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let mut map = BTreeMap::<Value, Value>::deserialize(d)?;

        let kind = match map.remove(&Value::String("kind".to_owned())) {
            Some(kind) => kind.deserialize_into().map_err(|e| e.to_error())?,
            None => "compound".to_owned(),
        };

        Ok(Policy {
            kind,
            config: Value::Map(map),
        })
    }
}

// TODO: log file helper. no need to be visible
#[derive(Debug)]
struct LogWriter {
    file: BufWriter<File>,
    len: u64,
}

impl io::Write for LogWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.file.write(buf).map(|n| {
            self.len += n as u64;
            n
        })
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

impl encode::Write for LogWriter {}

impl LogWriter {
    const BUFFER_CAPACITY: usize = 1024;
}

/// Active log file. You can write to the log file with [Self::get_or_init_writer].
/// When rolling the file you must close the writer with [Self::close_writer].
/// When re-opening the writer the file path may change. For more information see [Self::new].
pub struct LogFile {
    /// Writer to the active log file
    writer: Option<LogWriter>,
    /// Active log file path. If [Self::writer] is closed, it
    /// is the path of the last active log file.
    path: PathBuf,
    created_on: String,

    // Reinitialization fields
    /// Pattern with [COUNT_PATTERN] in [Self::path]
    pattern: String,
    base_count: u32,
    append: bool,
}

impl LogFile {
    /// Create a new log file.
    ///
    /// `append` controls if the file is opened in append mode.
    /// `pattern` and `count` are used to create the log file path: [Self::path].
    /// If the `pattern` contains [TIME_PATTERN], [chrono::Utc::now] is used to inject
    /// the timestamp.
    fn new(append: bool, time_count_pattern: &str, base_count: u32) -> anyhow::Result<Self> {
        let created_on = chrono::Utc::now().format(DATE_FORMAT_STR).to_string();

        // TODO: better names for appender pattern / roller pattern
        // TODO: expand env vars?
        let count_pattern = time_count_pattern.replace(TIME_PATTERN, &created_on);

        let file_name = count_pattern.replace(COUNT_PATTERN, &base_count.to_string());

        let path = PathBuf::from_str(&file_name)
            .context(format!("Invalid log file path: {}", file_name))?;

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .context(format!("Cant create log file parent: {}", file_name))?;
        }

        let file = OpenOptions::new()
            .write(true)
            .append(append)
            .truncate(!append)
            .create(true)
            .open(&path)
            .context(format!("Can't open log file {}", file_name))?;

        let len = if append { file.metadata()?.len() } else { 0 };

        Ok(LogFile {
            writer: Some(LogWriter {
                file: BufWriter::with_capacity(LogWriter::BUFFER_CAPACITY, file),
                len,
            }),
            path,
            created_on,
            pattern: count_pattern,
            base_count,
            append,
        })
    }

    /// A policy must call this method when it wishes to roll the log. The
    /// appender's handle to the file will be closed, which is necessary to
    /// move or delete the file on Windows.
    ///
    /// If this method is called, the log file must no longer be present on
    /// disk when the policy returns.
    pub fn close_writer(&mut self) {
        self.writer = None;
    }

    /// Allows writing to the log file. Opens a new writer if the log was rolled
    /// and the writer was closed. A new file may be opened if [Self::pattern]
    /// evaluation leads to a new file name. This is most commonly the case
    /// if [Self::pattern] contains [TIME_PATTERN]
    fn get_or_init_writer(&mut self) -> anyhow::Result<&mut LogWriter> {
        match self.writer {
            Some(ref mut writer) => Ok(writer),
            None => {
                *self = Self::new(self.append, &self.pattern, self.base_count)?;
                // SAFETY: new must create a writer
                Ok(self.writer.as_mut().unwrap())
            }
        }
    }

    /// Returns the path to the log file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns an estimate of the log file's current size.
    ///
    /// This is calculated by taking the size of the log file when it is opened
    /// and adding the number of bytes written. It may be inaccurate if any
    /// writes have failed or if another process has modified the file
    /// concurrently.
    #[allow(clippy::len_without_is_empty)]
    #[deprecated(since = "0.9.1", note = "Please use the len_estimate function instead")]
    pub fn len(&self) -> u64 {
        self.writer.as_ref().map(|writer| writer.len).unwrap_or(0)
    }

    /// Returns an estimate of the log file's current size.
    ///
    /// This is calculated by taking the size of the log file when it is opened
    /// and adding the number of bytes written. It may be inaccurate if any
    /// writes have failed or if another process has modified the file
    /// concurrently.
    pub fn len_estimate(&self) -> u64 {
        self.writer.as_ref().map(|writer| writer.len).unwrap_or(0)
    }

    fn created_on(&self) -> &str {
        &self.created_on
    }
}

/// An appender which archives log files in a configurable strategy.
#[derive(Derivative)]
#[derivative(Debug)]
pub struct RollingFileAppender {
    #[derivative(Debug = "ignore")]
    log_file: Mutex<LogFile>,
    encoder: Box<dyn Encode>,
    policy: Box<dyn policy::Policy>,
}

impl Append for RollingFileAppender {
    fn append(&self, record: &Record) -> anyhow::Result<()> {
        // TODO(eas): Perhaps this is better as a concurrent queue?
        let mut log_file = self.log_file.lock();

        let is_pre_process = self.policy.is_pre_process();
        if is_pre_process {
            // TODO(eas): Idea: make this optionally return a future, and if so, we initialize a queue for
            // data that comes in while we are processing the file rotation.
            self.policy.process(&mut log_file)?;

            let log_writer = log_file.get_or_init_writer()?;

            self.encoder.encode(log_writer, record)?;
            log_writer.flush()?;
        } else {
            // Create a new log writter if the log was rolled
            let log_writer = log_file.get_or_init_writer()?;

            self.encoder.encode(log_writer, record)?;
            log_writer.flush()?;

            // roll after writing
            self.policy.process(&mut log_file)?;
        }

        Ok(())
    }

    fn flush(&self) {}
}

impl RollingFileAppender {
    /// Creates a new `RollingFileAppenderBuilder`.
    pub fn builder() -> RollingFileAppenderBuilder {
        RollingFileAppenderBuilder {
            append: true,
            encoder: None,
            base_count: 0,
        }
    }
}

/// A builder for the `RollingFileAppender`.
pub struct RollingFileAppenderBuilder {
    append: bool,
    encoder: Option<Box<dyn Encode>>,
    base_count: u32,
}

impl RollingFileAppenderBuilder {
    /// Determines if the appender will append to or truncate the log file.
    ///
    /// Defaults to `true`.
    pub fn append(mut self, append: bool) -> RollingFileAppenderBuilder {
        self.append = append;
        self
    }

    /// Sets the encoder used by the appender.
    ///
    /// Defaults to a `PatternEncoder` with the default pattern.
    pub fn encoder(mut self, encoder: Box<dyn Encode>) -> RollingFileAppenderBuilder {
        self.encoder = Some(encoder);
        self
    }

    /// Value used to replace [COUNT_PATTERN] in the file path.
    ///
    /// Defaults to 0.
    pub fn base(mut self, base_count: u32) -> RollingFileAppenderBuilder {
        self.base_count = base_count;
        self
    }

    /// Constructs a `RollingFileAppender`.
    /// The path argument can contain environment variables of the form $ENV{name_here},
    /// where 'name_here' will be the name of the environment variable that
    /// will be resolved. Note that if the variable fails to resolve,
    /// $ENV{name_here} will NOT be replaced in the path.
    pub fn build(
        self,
        appender_pattern: String,
        policy: Box<dyn policy::Policy>,
    ) -> anyhow::Result<RollingFileAppender> {
        let pattern = env_util::expand_env_vars(appender_pattern.clone()).to_string();

        let appender = RollingFileAppender {
            log_file: Mutex::new(LogFile::new(self.append, &pattern, self.base_count)?),
            encoder: self
                .encoder
                .unwrap_or_else(|| Box::<PatternEncoder>::default()),
            policy,
        };

        // open the file immediately
        appender.log_file.lock().get_or_init_writer()?;

        Ok(appender)
    }
}

/// A deserializer for the `RollingFileAppender`.
///
/// # Configuration
///
/// ```yaml
/// kind: rolling_file
///
/// # The path of the log file. Required.
/// # The path can contain environment variables of the form $ENV{name_here},
/// # where 'name_here' will be the name of the environment variable that
/// # will be resolved. Note that if the variable fails to resolve,
/// # $ENV{name_here} will NOT be replaced in the path.
/// path: log/foo.log
///
/// # Specifies if the appender should append to or truncate the log file if it
/// # already exists. Defaults to `true`.
/// append: true
///
/// # The encoder to use to format output. Defaults to `kind: pattern`.
/// encoder:
///   kind: pattern
///
/// # The policy which handles rotation of the log file. Required.
/// policy:
///   # Identifies which policy is to be used. If no kind is specified, it will
///   # default to "compound".
///   kind: compound
///
///   # The remainder of the configuration is passed along to the policy's
///   # deserializer, and will vary based on the kind of policy.
///   trigger:
///     kind: size
///     limit: 10 mb
///
///   roller:
///     kind: delete
/// ```
#[cfg(feature = "config_parsing")]
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug, Default)]
pub struct RollingFileAppenderDeserializer;

#[cfg(feature = "config_parsing")]
impl Deserialize for RollingFileAppenderDeserializer {
    type Trait = dyn Append;

    type Config = RollingFileAppenderConfig;

    fn deserialize(
        &self,
        config: RollingFileAppenderConfig,
        deserializers: &Deserializers,
    ) -> anyhow::Result<Box<dyn Append>> {
        let mut builder = RollingFileAppender::builder();
        if let Some(append) = config.append {
            builder = builder.append(append);
        }
        if let Some(encoder) = config.encoder {
            let encoder = deserializers.deserialize(&encoder.kind, encoder.config)?;
            builder = builder.encoder(encoder);
        }

        let policy = deserializers.deserialize(&config.policy.kind, config.policy.config)?;
        // TODO: Extend appender config
        let appender = builder.build(config.path, policy)?;
        Ok(Box::new(appender))
    }
}

// #[cfg(test)]
// mod test {
//     use std::{
//         fs::File,
//         io::{Read, Write},
//     };

//     use super::*;
//     use crate::append::rolling_file::policy::Policy;

//     #[test]
//     #[cfg(feature = "yaml_format")]
//     fn deserialize() {
//         use crate::config::{Deserializers, RawConfig};

//         let dir = tempfile::tempdir().unwrap();

//         let config = format!(
//             "
// appenders:
//     foo:
//         kind: rolling_file
//         path: {0}/foo.log
//         policy:
//             trigger:
//                 kind: time
//                 interval: 2 minutes
//             roller:
//                 kind: delete
//     bar:
//         kind: rolling_file
//         path: {0}/foo.log
//         policy:
//             kind: compound
//             trigger:
//                 kind: size
//                 limit: 5 mb
//             roller:
//                 kind: fixed_window
//                 pattern: '{0}/foo.log.{{}}'
//                 base: 1
//                 count: 5
// ",
//             dir.path().display()
//         );

//         let config = ::serde_yaml::from_str::<RawConfig>(&config).unwrap();
//         let errors = config.appenders_lossy(&Deserializers::new()).1;
//         println!("{:?}", errors);
//         assert!(errors.is_empty());
//     }

//     #[derive(Debug)]
//     struct NopPolicy;

//     impl Policy for NopPolicy {
//         fn process(&self, _: &mut LogFile) -> anyhow::Result<()> {
//             Ok(())
//         }
//         fn is_pre_process(&self) -> bool {
//             false
//         }
//     }

//     #[test]
//     fn append() {
//         let dir = tempfile::tempdir().unwrap();
//         let path = dir.path().join("append.log");
//         RollingFileAppender::builder()
//             .append(true)
//             .build(&path, Box::new(NopPolicy))
//             .unwrap();
//         assert!(path.exists());
//         File::create(&path).unwrap().write_all(b"hello").unwrap();

//         RollingFileAppender::builder()
//             .append(true)
//             .build(&path, Box::new(NopPolicy))
//             .unwrap();
//         let mut contents = vec![];
//         File::open(&path)
//             .unwrap()
//             .read_to_end(&mut contents)
//             .unwrap();
//         assert_eq!(contents, b"hello");
//     }

//     #[test]
//     fn truncate() {
//         let dir = tempfile::tempdir().unwrap();
//         let path = dir.path().join("truncate.log");
//         RollingFileAppender::builder()
//             .append(false)
//             .build(&path, Box::new(NopPolicy))
//             .unwrap();
//         assert!(path.exists());
//         File::create(&path).unwrap().write_all(b"hello").unwrap();

//         RollingFileAppender::builder()
//             .append(false)
//             .build(&path, Box::new(NopPolicy))
//             .unwrap();
//         let mut contents = vec![];
//         File::open(&path)
//             .unwrap()
//             .read_to_end(&mut contents)
//             .unwrap();
//         assert_eq!(contents, b"");
//     }
// }
