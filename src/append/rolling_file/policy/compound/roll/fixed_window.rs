//! The fixed-window roller.
//!
//! Requires the `fixed_window_roller` feature.

use anyhow::bail;
#[cfg(feature = "background_rotation")]
use parking_lot::{Condvar, Mutex};
#[cfg(feature = "background_rotation")]
use std::sync::Arc;
use std::{
    collections::VecDeque,
    fs, io,
    path::Path,
    sync::{Arc, Mutex},
};

use crate::append::{
    env_util::expand_env_vars,
    rolling_file::{policy::compound::roll::Roll, LogFile, COUNT_PATTERN, TIME_PATTERN},
};

#[cfg(feature = "config_parsing")]
use crate::config::{Deserialize, Deserializers};

/// Configuration for the fixed window roller.
#[cfg(feature = "config_parsing")]
#[derive(Clone, Eq, PartialEq, Hash, Debug, Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FixedWindowRollerConfig {
    pattern: String,
    base: Option<u32>,
    count: u32,
}

/// Kind of compression used for rolled logs
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum Compression {
    /// No compression
    None,
    #[cfg(feature = "gzip")]
    Gzip,
}

impl Compression {
    fn compress(&self, src: &Path, dst: &Path) -> io::Result<()> {
        match *self {
            Compression::None => move_file(src, dst),
            #[cfg(feature = "gzip")]
            Compression::Gzip => {
                #[cfg(feature = "flate2")]
                use flate2::write::GzEncoder;
                use std::fs::File;

                let mut i = File::open(src)?;

                let o = File::create(dst)?;
                let mut o = GzEncoder::new(o, flate2::Compression::default());

                io::copy(&mut i, &mut o)?;
                drop(o.finish()?);
                drop(i); // needs to happen before remove_file call on Windows

                fs::remove_file(src)
            }
        }
    }
}

/// A roller which maintains a fixed window of archived log files.
///
/// A `FixedWindowRoller` is configured with a filename pattern, a base index,
/// and a maximum file count. Each archived log file is associated with a numeric
/// index ordering it by age, starting at the base index. Archived log files are
/// named by substituting all instances of `{}` with the file's index in the
/// filename pattern.
///
/// For example, if the filename pattern is `archive/foo.{}.log`, the base index
/// is 0 and the count is 2, the first log file will be archived as
/// `archive/foo.0.log`. When the next log file is archived, `archive/foo.0.log`
/// will be renamed to `archive/foo.1.log` and the new log file will be named
/// `archive/foo.0.log`. When the third log file is archived,
/// `archive/foo.1.log` will be deleted, `archive/foo.0.log` will be renamed to
/// `archive/foo.1.log`, and the new log file will be renamed to
/// `archive/foo.0.log`.
///
/// If the file extension of the pattern is `.gz` and the `gzip` Cargo feature
/// is enabled, the archive files will be gzip-compressed.
///
/// Note that this roller will have to rename every archived file every time the
/// log rolls over. Performance may be negatively impacted by specifying a large
/// count.
#[derive(Clone, Debug)]
pub struct FixedWindowRoller {
    compression: Compression,
    base_count: u32,
    max_count: u32,
    rolled_log_files: Arc<Mutex<VecDeque<RolledLogPath>>>,
    pattern: String,
    parent_varies: bool,
    #[cfg(feature = "background_rotation")]
    cond_pair: Arc<(Mutex<bool>, Condvar)>,
}

impl FixedWindowRoller {
    /// Returns a new builder for the `FixedWindowRoller`.
    pub fn builder() -> FixedWindowRollerBuilder {
        FixedWindowRollerBuilder { base: 0 }
    }
}

impl Roll for FixedWindowRoller {
    #[cfg(not(feature = "background_rotation"))]
    fn roll(&self, file: &LogFile) -> anyhow::Result<()> {
        if self.max_count == 0 {
            return fs::remove_file(file.path()).map_err(Into::into);
        }

        rotate(
            file,
            &self.pattern,
            file.created_on(),
            self.rolled_log_files.clone(),
            self.compression,
            self.base_count,
            self.max_count,
            self.parent_varies,
        )?;

        Ok(())
    }

    #[cfg(feature = "background_rotation")]
    fn roll(&self, file: &Path) -> anyhow::Result<()> {
        if self.count == 0 {
            return fs::remove_file(file).map_err(Into::into);
        }

        // rename the file
        let temp = make_temp_file_name(file);
        move_file(file, &temp)?;

        // Wait for the state to be ready to roll
        let (lock, cvar) = &*self.cond_pair.clone();
        let mut ready = lock.lock();
        if !*ready {
            cvar.wait(&mut ready);
        }
        *ready = false;
        drop(ready);

        let pattern = self.pattern.clone();
        let compression = self.compression;
        let base = self.base;
        let count = self.count;
        let cond_pair = self.cond_pair.clone();
        // rotate in the separate thread
        std::thread::spawn(move || {
            let (lock, cvar) = &*cond_pair;
            let mut ready = lock.lock();

            if let Err(e) = rotate(pattern, compression, base, count, temp) {
                use std::io::Write;
                let _ = writeln!(io::stderr(), "log4rs, error rotating: {}", e);
            }
            *ready = true;
            cvar.notify_one();
        });

        Ok(())
    }
}

fn move_file<P, Q>(src: P, dst: Q) -> io::Result<()>
where
    P: AsRef<Path>,
    Q: AsRef<Path>,
{
    // first try a rename
    match fs::rename(src.as_ref(), dst.as_ref()) {
        Ok(()) => return Ok(()),
        Err(ref e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(_) => {}
    }

    // fall back to a copy and delete if src and dst are on different mounts
    fs::copy(src.as_ref(), dst.as_ref()).and_then(|_| fs::remove_file(src.as_ref()))
}

#[cfg(feature = "background_rotation")]
fn make_temp_file_name<P>(file: P) -> PathBuf
where
    P: AsRef<Path>,
{
    let mut n = std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .unwrap_or_else(|_| std::time::Duration::from_secs(0))
        .as_secs();
    let mut temp = file.as_ref().to_path_buf();
    temp.set_extension(format!("{}", n));
    while temp.exists() {
        n += 1;
        temp.set_extension(format!("{}", n));
    }
    temp
}

fn parent_varies_with_count_change(pattern: &str) -> bool {
    let count_test = "0";
    let pattern_count_incremented = pattern.replace(COUNT_PATTERN, count_test);

    // if this replacement doesn't cause a change in the parent. No replacement of
    // the same kind ever will.
    match (
        Path::new(pattern).parent(),
        Path::new(&pattern_count_incremented).parent(),
    ) {
        (Some(a), Some(b)) => a != b,
        _ => false, // Only case that can actually happen is (None, None)
    }
}

// TODO(eas): compress to tmp file then move into place once prev task is done
#[allow(clippy::too_many_arguments)]
fn rotate(
    log: &LogFile,
    pattern: &str,
    timestamp: &str,
    rolled_log_files: Arc<Mutex<VecDeque<RolledLogPath>>>,
    compression: Compression,
    base_count: u32,
    max_count: u32,
    parent_varies: bool,
) -> anyhow::Result<()> {
    let mut rolled_log_files = rolled_log_files.lock().unwrap();

    // Enforce rolled logs `max_count` invariant
    debug_assert!(rolled_log_files.len() < max_count as usize + 1);
    if rolled_log_files.len() > max_count as usize {
        println!("Rolled logs exceeded max count")
    }

    // This should only be triggered once. If the `max_count` invariant
    // is broken we take the defensive approach.
    while rolled_log_files.len() >= max_count as usize {
        // remove the last log and make room for inserting a new one
        let outdated_log = rolled_log_files
            .pop_back()
            .expect("max_count > 0 guaranteed by the caller");

        fs::remove_file(outdated_log.path())?;
    }

    for src in rolled_log_files.iter_mut().rev() {
        let dst = src.increment_count();

        if parent_varies {
            if let Some(dst_parent) = dst.path().parent() {
                fs::create_dir_all(dst_parent)?;
            }
        }

        move_file(src.path(), dst.path())?;
        *src = dst;
    }

    let base_rolled_log = RolledLogPath::new(pattern, timestamp, base_count);

    if let Some(parent) = base_rolled_log.path().parent() {
        fs::create_dir_all(parent)?;
    }

    compression
        .compress(log.path(), base_rolled_log.path())
        .map_err(|e| {
            println!(
                "err compressing: {:?}, dst: {:?}",
                log.path(),
                base_rolled_log
            );
            e
        })?;

    rolled_log_files.push_front(base_rolled_log);

    Ok(())
}

/// A builder for the `FixedWindowRoller`.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug, Default)]
pub struct FixedWindowRollerBuilder {
    base: u32,
}

impl FixedWindowRollerBuilder {
    /// Sets the base index for archived log files.
    ///
    /// Defaults to 0.
    pub fn base(mut self, base: u32) -> FixedWindowRollerBuilder {
        self.base = base;
        self
    }

    /// Constructs a new `FixedWindowRoller`.
    ///
    /// `pattern` is either an absolute path or lacking a leading `/`, relative
    /// to the `cwd` of your application. The pattern must contain at least one
    /// instance of `{}`, all of which will be replaced with an archived log file's index.
    ///
    /// If the file extension of the pattern is `.gz` and the `gzip` Cargo
    /// feature is enabled, the archive files will be gzip-compressed.
    /// If the extension is `.gz` and the `gzip` feature is *not* enabled, an error will be returned.
    ///
    /// `count` is the maximum number of archived logs to maintain.
    pub fn build(self, pattern: String, max_count: u32) -> anyhow::Result<FixedWindowRoller> {
        if !pattern.contains(COUNT_PATTERN) {
            bail!("pattern does not contain: {}", COUNT_PATTERN);
        }

        let pattern = expand_env_vars(&pattern).to_string();
        let parent_varies = parent_varies_with_count_change(&pattern);

        let compression = match Path::new(&pattern).extension() {
            #[cfg(feature = "gzip")]
            Some(e) if e == "gz" => Compression::Gzip,
            #[cfg(not(feature = "gzip"))]
            Some(e) if e == "gz" => {
                bail!("gzip compression requires the `gzip` feature");
            }
            _ => Compression::None,
        };

        Ok(FixedWindowRoller {
            compression,
            base_count: self.base,
            max_count,
            rolled_log_files: Arc::new(Mutex::new(VecDeque::with_capacity(max_count as usize))),
            pattern,
            parent_varies,
            #[cfg(feature = "background_rotation")]
            cond_pair: Arc::new((Mutex::new(true), Condvar::new())),
        })
    }
}

/// Rolled log path
#[derive(Debug, Clone)]
struct RolledLogPath {
    /// Count of the log file, present in the file path. [COUNT_PATTERN] replacement.
    file_name: String,
    count: u32,
    // indices of current count ocurrences. Used to increment the log file count. refering to file name.
    count_idxs: Vec<usize>,
}

impl RolledLogPath {
    /// Create a new rolled log path. Does timestamp, and count replacement.
    /// based on the [ENV_PATTERN], [TIME_PATTERN] and [COUNT_PATTERN]. Keeps an internal structure
    /// that allows it to increment count occurences in the path.
    pub fn new(file_name_pattern: &str, timestamp: &str, count: u32) -> Self {
        let file_name_pattern = file_name_pattern.replace(TIME_PATTERN, timestamp);
        let (count_idxs, file_name) = Self::replace_count(&file_name_pattern, COUNT_PATTERN, count);

        Self {
            count,
            count_idxs,
            file_name,
        }
    }

    /// Produce a new rolled log path with incremented counts.
    pub fn increment_count(&self) -> Self {
        let (count_idxs, file_name) =
            Self::increment_count_impl(self.file_name.clone(), self.count, &self.count_idxs);

        Self {
            file_name,
            count: self.count + 1,
            count_idxs,
        }
    }

    pub fn path(&self) -> &Path {
        Path::new(&self.file_name)
    }

    /// replace pattern occurences in the src string with count
    /// produce a String with replaced values, and an index map to all the replaced
    /// values start. The index map allows us to perform [Self::increment_count].
    fn replace_count(src: &str, pattern: &str, count: u32) -> (Vec<usize>, String) {
        let mut result = String::new();
        let mut replaced_idx = Vec::new();

        let count = count.to_string();
        let idx_left_shift = pattern.len() - count.len();

        let mut last_end = 0;
        for (num_replaced, (pattern_start, part)) in src.match_indices(pattern).enumerate() {
            result.push_str(unsafe { src.get_unchecked(last_end..pattern_start) });
            result.push_str(&count);
            last_end = pattern_start + part.len();
            replaced_idx.push(pattern_start - num_replaced * idx_left_shift);
        }
        result.push_str(unsafe { src.get_unchecked(last_end..src.len()) });
        (replaced_idx, result)
    }

    /// Use a src and count_idxs generated by [Self::replace_count] to increment
    /// all the counts in the src.
    fn increment_count_impl(
        mut src: String,
        current_count: u32,
        count_idxs: &[usize],
    ) -> (Vec<usize>, String) {
        let incremented_count = (current_count + 1).to_string();
        let current_count = current_count.to_string();

        let left_shift = current_count.len() - incremented_count.len();
        let mut new_count_idx = Vec::new();

        for (num_replacements, start_idx) in count_idxs.iter().enumerate() {
            let start_idx = start_idx - num_replacements * left_shift;
            let end_idx = start_idx + current_count.len();
            // SAFETY:
            // count idxs is made when creating the file_name with replace_count
            // There is no other way to change it
            src.replace_range(start_idx..end_idx, &incremented_count);
            new_count_idx.push(start_idx)
        }

        (new_count_idx, src)
    }
}

/// A deserializer for the `FixedWindowRoller`.
///
/// # Configuration
///
/// ```yaml
/// kind: fixed_window
///
/// # The filename pattern for archived logs. This is either an absolute path or if lacking a leading `/`,
/// # relative to the `cwd` of your application. The pattern must contain at least one
/// # instance of `{}`, all of which will be replaced with an archived log file's index.
/// # If the file extension of the pattern is `.gz` and the `gzip` Cargo feature
/// # is enabled, the archive files will be gzip-compressed.
/// # Required.
/// pattern: archive/foo.{}.log
///
/// # The maximum number of archived logs to maintain. Required.
/// count: 5
///
/// # The base value for archived log indices. Defaults to 0.
/// base: 1
/// ```
#[cfg(feature = "config_parsing")]
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug, Default)]
pub struct FixedWindowRollerDeserializer;

#[cfg(feature = "config_parsing")]
impl Deserialize for FixedWindowRollerDeserializer {
    type Trait = dyn Roll;

    type Config = FixedWindowRollerConfig;

    fn deserialize(
        &self,
        config: FixedWindowRollerConfig,
        _: &Deserializers,
    ) -> anyhow::Result<Box<dyn Roll>> {
        let mut builder = FixedWindowRoller::builder();
        if let Some(base) = config.base {
            builder = builder.base(base);
        }

        Ok(Box::new(builder.build(config.pattern, config.count)?))
    }
}

#[cfg(test)]
mod test {
    use std::{
        fs::File,
        io::{Read, Write},
    };

    use super::*;
    use crate::append::rolling_file::policy::compound::roll::Roll;

    #[cfg(feature = "background_rotation")]
    fn wait_for_roller(roller: &FixedWindowRoller) {
        std::thread::sleep(std::time::Duration::from_millis(100));
        let _lock = roller.cond_pair.0.lock();
    }

    #[cfg(not(feature = "background_rotation"))]
    fn wait_for_roller(_roller: &FixedWindowRoller) {}

    #[test]
    fn rotation() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().to_str().unwrap();
        let pattern = format!("{}/foo.log.{}", base, COUNT_PATTERN);
        let rolled_log_max_count = 2;
        let log_base_count = 0;
        let rolled_log_base_count = 1;

        let roller = FixedWindowRoller::builder()
            .base(rolled_log_base_count)
            .build(pattern.clone(), rolled_log_max_count)
            .unwrap();

        let mut log_file = LogFile::new(false, &pattern, log_base_count).unwrap();

        let test_content = b"file1";
        log_file
            .get_or_init_writer()
            .unwrap()
            .write_all(test_content)
            .unwrap();

        // writer must be closed before making a call to roll
        log_file.close_writer();
        roller.roll(&log_file).unwrap();
        wait_for_roller(&roller);
        assert!(!log_file.path().exists(), "log file got rolled");

        let mut contents = vec![];

        let rolled_log_path = roller
            .rolled_log_files
            .lock()
            .unwrap()
            .front()
            .unwrap()
            .path()
            .to_owned();

        File::open(rolled_log_path)
            .unwrap()
            .read_to_end(&mut contents)
            .unwrap();

        assert_eq!(contents, test_content);

        //File::create(&file).unwrap().write_all(b"file2").unwrap();

        // roller.roll(&file).unwrap();
        // wait_for_roller(&roller);
        // assert!(!file.exists());
        // contents.clear();
        // File::open(dir.path().join("foo.log.1"))
        //     .unwrap()
        //     .read_to_end(&mut contents)
        //     .unwrap();
        // assert_eq!(contents, b"file1");
        // contents.clear();
        // File::open(dir.path().join("foo.log.0"))
        //     .unwrap()
        //     .read_to_end(&mut contents)
        //     .unwrap();
        // assert_eq!(contents, b"file2");

        // File::create(&file).unwrap().write_all(b"file3").unwrap();

        // roller.roll(&file).unwrap();
        // wait_for_roller(&roller);
        // assert!(!file.exists());
        // contents.clear();
        // assert!(!dir.path().join("foo.log.2").exists());
        // File::open(dir.path().join("foo.log.1"))
        //     .unwrap()
        //     .read_to_end(&mut contents)
        //     .unwrap();
        // assert_eq!(contents, b"file2");
        // contents.clear();
        // File::open(dir.path().join("foo.log.0"))
        //     .unwrap()
        //     .read_to_end(&mut contents)
        //     .unwrap();
        // assert_eq!(contents, b"file3");
    }

    // #[test]
    // fn rotation_no_trivial_base() {
    //     let dir = tempfile::tempdir().unwrap();
    //     let base = 3;
    //     let fname = "foo.log";
    //     let fcontent = b"something";
    //     let expected_fist_roll = format!("{}.{}", fname, base);

    //     let base_dir = dir.path().to_str().unwrap();
    //     let roller = FixedWindowRoller::builder()
    //         .base(base)
    //         .build(&format!("{}/{}.{{}}", base_dir, fname), 2)
    //         .unwrap();

    //     let file = dir.path().join(fname);
    //     File::create(&file).unwrap().write_all(fcontent).unwrap();

    //     roller.roll(&file).unwrap();
    //     wait_for_roller(&roller);
    //     assert!(!file.exists());

    //     let mut contents = vec![];

    //     let first_roll = dir.path().join(&expected_fist_roll);

    //     assert!(first_roll.as_path().exists());

    //     File::open(first_roll)
    //         .unwrap()
    //         .read_to_end(&mut contents)
    //         .unwrap();
    //     assert_eq!(contents, fcontent);

    //     // Sanity check general behaviour
    //     roller.roll(&file).unwrap();
    //     wait_for_roller(&roller);
    //     assert!(!file.exists());
    //     contents.clear();
    //     File::open(dir.path().join(&format!("{}.{}", fname, base + 1)))
    //         .unwrap()
    //         .read_to_end(&mut contents)
    //         .unwrap();
    //     assert_eq!(contents, b"something");
    // }

    // #[test]
    // fn create_archive_unvaried() {
    //     let dir = tempfile::tempdir().unwrap();

    //     let base = dir.path().join("log").join("archive");
    //     let pattern = base.join("foo.{}.log");
    //     let roller = FixedWindowRoller::builder()
    //         .build(pattern.to_str().unwrap(), 2)
    //         .unwrap();

    //     let file = dir.path().join("foo.log");
    //     File::create(&file).unwrap().write_all(b"file").unwrap();

    //     roller.roll(&file).unwrap();
    //     wait_for_roller(&roller);

    //     assert!(base.join("foo.0.log").exists());

    //     let file = dir.path().join("foo.log");
    //     File::create(&file).unwrap().write_all(b"file2").unwrap();

    //     roller.roll(&file).unwrap();
    //     wait_for_roller(&roller);

    //     assert!(base.join("foo.0.log").exists());
    //     assert!(base.join("foo.1.log").exists());
    // }

    // #[test]
    // fn create_archive_varied() {
    //     let dir = tempfile::tempdir().unwrap();

    //     let base = dir.path().join("log").join("archive");
    //     let pattern = base.join("{}").join("foo.log");
    //     let roller = FixedWindowRoller::builder()
    //         .build(pattern.to_str().unwrap(), 2)
    //         .unwrap();

    //     let file = dir.path().join("foo.log");
    //     File::create(&file).unwrap().write_all(b"file").unwrap();

    //     roller.roll(&file).unwrap();
    //     wait_for_roller(&roller);

    //     assert!(base.join("0").join("foo.log").exists());

    //     let file = dir.path().join("foo.log");
    //     File::create(&file).unwrap().write_all(b"file2").unwrap();

    //     roller.roll(&file).unwrap();
    //     wait_for_roller(&roller);

    //     assert!(base.join("0").join("foo.log").exists());
    //     assert!(base.join("1").join("foo.log").exists());
    // }

    // #[test]
    // #[cfg_attr(feature = "gzip", ignore)]
    // fn unsupported_gzip() {
    //     let dir = tempfile::tempdir().unwrap();

    //     let pattern = dir.path().join("{}.gz");
    //     assert!(FixedWindowRoller::builder()
    //         .build(pattern.to_str().unwrap(), 2)
    //         .is_err());
    // }

    // #[test]
    // #[cfg_attr(not(feature = "gzip"), ignore)]
    // // or should we force windows user to install gunzip
    // #[cfg(not(windows))]
    // fn supported_gzip() {
    //     use std::process::Command;

    //     let dir = tempfile::tempdir().unwrap();

    //     let pattern = dir.path().join("{}.gz");
    //     let roller = FixedWindowRoller::builder()
    //         .build(pattern.to_str().unwrap(), 2)
    //         .unwrap();

    //     let contents = (0..10000).map(|i| i as u8).collect::<Vec<_>>();

    //     let file = dir.path().join("foo.log");
    //     File::create(&file).unwrap().write_all(&contents).unwrap();

    //     roller.roll(&file).unwrap();
    //     wait_for_roller(&roller);

    //     assert!(Command::new("gunzip")
    //         .arg(dir.path().join("0.gz"))
    //         .status()
    //         .unwrap()
    //         .success());

    //     let mut file = File::open(dir.path().join("0")).unwrap();
    //     let mut actual = vec![];
    //     file.read_to_end(&mut actual).unwrap();

    //     assert_eq!(contents, actual);
    // }

    // #[test]
    // fn roll_with_env_var() {
    //     std::env::set_var("LOG_DIR", "test_log_dir");
    //     let fcontent = b"file1";
    //     let dir = tempfile::tempdir().unwrap();

    //     let base = dir.path().to_str().unwrap();
    //     let roller = FixedWindowRoller::builder()
    //         .build(&format!("{}/$ENV{{LOG_DIR}}/foo.log.{{}}", base), 2)
    //         .unwrap();

    //     let file = dir.path().join("foo.log");
    //     File::create(&file).unwrap().write_all(fcontent).unwrap();

    //     //Check file exists before roll is called
    //     assert!(file.exists());

    //     roller.roll(&file).unwrap();
    //     wait_for_roller(&roller);

    //     //Check file does not exists after roll is called
    //     assert!(!file.exists());

    //     let rolled_file = dir.path().join("test_log_dir").join("foo.log.0");
    //     //Check the new rolled file exists
    //     assert!(rolled_file.exists());

    //     let mut contents = vec![];

    //     File::open(rolled_file)
    //         .unwrap()
    //         .read_to_end(&mut contents)
    //         .unwrap();
    //     //Check the new rolled file has the same contents as the old one
    //     assert_eq!(contents, fcontent);
    // }
}
