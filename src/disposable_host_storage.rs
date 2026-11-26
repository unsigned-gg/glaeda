//! Fail-closed host-filesystem admission for one disposable Lima worker.
//!
//! This boundary observes only the filesystem backing the exact enrolled `LIMA_HOME`. It grants
//! no VM discovery, deletion, resize, or filesystem mutation authority.

use std::fmt;
use std::path::PathBuf;

use rustix::fs::{self, FileType, Mode, OFlags};
use rustix::process::geteuid;

const GIB: u64 = 1 << 30;
const HOST_FREE_SPACE_RESERVE_BYTES: u64 = 10 * GIB;
pub(crate) const HOST_STORAGE_UNAVAILABLE_CODE: &str = "disposable_host_storage_unavailable";

const DIRECTORY_FLAGS: OFlags = OFlags::RDONLY
    .union(OFlags::CLOEXEC)
    .union(OFlags::DIRECTORY)
    .union(OFlags::NOFOLLOW)
    .union(OFlags::NONBLOCK);

pub(crate) trait DisposableHostStorageSource {
    fn admits_new_worker(&self) -> Result<bool, DisposableHostStorageError>;

    /// How many more workers fit beside `occupied` workers already on the host.
    ///
    /// The default is the single-worker rule: one new worker on an empty host when
    /// `admits_new_worker` says so, and none beside any occupied worker. A source that can
    /// count per-clone disk headroom overrides this.
    fn new_worker_room(&self, occupied: u16) -> Result<u16, DisposableHostStorageError> {
        if occupied != 0 {
            return Ok(0);
        }
        self.admits_new_worker().map(u16::from)
    }
}

/// Read-only admission against the exact filesystem backing one enrolled Lima home.
pub(crate) struct DisposableHostStorage {
    lima_home: PathBuf,
    required_available_bytes: u64,
    worker_disk_ceiling_bytes: u64,
}

impl DisposableHostStorage {
    pub(crate) fn new(
        lima_home: PathBuf,
        worker_disk_ceiling_bytes: u64,
    ) -> Result<Self, DisposableHostStorageError> {
        let required_available_bytes = worker_disk_ceiling_bytes
            .checked_add(HOST_FREE_SPACE_RESERVE_BYTES)
            .ok_or_else(storage_error)?;
        Ok(Self {
            lima_home,
            required_available_bytes,
            worker_disk_ceiling_bytes,
        })
    }

    #[cfg(test)]
    pub(crate) fn test_lima_home(&self) -> &std::path::Path {
        &self.lima_home
    }

    #[cfg(test)]
    pub(crate) const fn test_required_available_bytes(&self) -> u64 {
        self.required_available_bytes
    }
}

impl DisposableHostStorage {
    fn available_bytes(&self) -> Result<u64, DisposableHostStorageError> {
        let directory = fs::open(&self.lima_home, DIRECTORY_FLAGS, Mode::empty())
            .map_err(|_| storage_error())?;
        let stat = fs::fstat(&directory).map_err(|_| storage_error())?;
        if !FileType::from_raw_mode(stat.st_mode).is_dir()
            || stat.st_uid != geteuid().as_raw()
            || stat.st_mode & 0o7777 != 0o700
        {
            return Err(storage_error());
        }
        let filesystem = fs::fstatvfs(&directory).map_err(|_| storage_error())?;
        checked_available_bytes(filesystem.f_bavail, filesystem.f_frsize)
    }
}

impl DisposableHostStorageSource for DisposableHostStorage {
    fn admits_new_worker(&self) -> Result<bool, DisposableHostStorageError> {
        Ok(has_required_available_bytes(
            self.available_bytes()?,
            self.required_available_bytes,
        ))
    }

    fn new_worker_room(&self, occupied: u16) -> Result<u16, DisposableHostStorageError> {
        Ok(worker_room(
            self.available_bytes()?,
            self.worker_disk_ceiling_bytes,
            occupied,
        ))
    }
}

/// New workers that fit in `available_bytes` after the host reserve when each of the `occupied`
/// clones may still grow to its full disk ceiling (copy-on-write clones start small). With nothing
/// occupied this is at least one exactly when `has_required_available_bytes` holds.
fn worker_room(available_bytes: u64, worker_disk_ceiling_bytes: u64, occupied: u16) -> u16 {
    if worker_disk_ceiling_bytes == 0 {
        return 0;
    }
    let Some(held) = worker_disk_ceiling_bytes
        .checked_mul(u64::from(occupied))
        .and_then(|held| held.checked_add(HOST_FREE_SPACE_RESERVE_BYTES))
    else {
        return 0;
    };
    let room = available_bytes.saturating_sub(held) / worker_disk_ceiling_bytes;
    u16::try_from(room).unwrap_or(u16::MAX)
}

const fn has_required_available_bytes(available_bytes: u64, required_bytes: u64) -> bool {
    available_bytes >= required_bytes
}

fn checked_available_bytes(
    available_blocks: u64,
    fragment_bytes: u64,
) -> Result<u64, DisposableHostStorageError> {
    available_blocks
        .checked_mul(fragment_bytes)
        .ok_or_else(storage_error)
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct DisposableHostStorageError {
    code: &'static str,
}

impl DisposableHostStorageError {
    pub(crate) const fn code(self) -> &'static str {
        self.code
    }
}

impl fmt::Debug for DisposableHostStorageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DisposableHostStorageError")
            .field("code", &self.code)
            .finish()
    }
}

impl fmt::Display for DisposableHostStorageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("host storage admission is unavailable")
    }
}

impl std::error::Error for DisposableHostStorageError {}

const fn storage_error() -> DisposableHostStorageError {
    DisposableHostStorageError {
        code: HOST_STORAGE_UNAVAILABLE_CODE,
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::{PermissionsExt as _, symlink};
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    static NEXT_ROOT: AtomicU64 = AtomicU64::new(1);

    struct TempRoot(PathBuf);

    impl TempRoot {
        fn new(mode: u32) -> Self {
            let path = std::env::temp_dir().join(format!(
                "smolrunner-host-storage-{}-{}",
                std::process::id(),
                NEXT_ROOT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&path).unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(mode)).unwrap();
            Self(path)
        }
    }

    impl Drop for TempRoot {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn exact_threshold_and_checked_arithmetic_fail_closed() {
        let storage = DisposableHostStorage::new(PathBuf::from("/unused"), 20 * GIB).unwrap();
        let required = storage.required_available_bytes;
        assert_eq!(required, 30 * GIB);
        assert_eq!(checked_available_bytes(required, 1).unwrap(), required);
        assert!(has_required_available_bytes(required, required));
        assert!(!has_required_available_bytes(required - 1, required));
        assert!(checked_available_bytes(u64::MAX, 2).is_err());
        assert!(DisposableHostStorage::new(PathBuf::from("/unused"), u64::MAX).is_err());
        assert_eq!(storage_error().code(), HOST_STORAGE_UNAVAILABLE_CODE);
    }

    #[test]
    fn exact_private_directory_is_observed_without_exposing_its_path() {
        let root = TempRoot::new(0o700);
        let storage = DisposableHostStorage {
            lima_home: root.0.clone(),
            required_available_bytes: 0,
            worker_disk_ceiling_bytes: 1,
        };
        assert!(storage.admits_new_worker().unwrap());
    }

    #[test]
    fn worker_room_holds_every_occupied_clone_at_its_ceiling() {
        // One worker exactly at the single-worker threshold, none below it.
        assert_eq!(worker_room(30 * GIB, 20 * GIB, 0), 1);
        assert_eq!(worker_room(30 * GIB - 1, 20 * GIB, 0), 0);
        // 70 GiB free, 20 GiB clones, 10 GiB reserve: three beside nothing, two beside one,
        // one beside two, none beside three.
        assert_eq!(worker_room(70 * GIB, 20 * GIB, 0), 3);
        assert_eq!(worker_room(70 * GIB, 20 * GIB, 1), 2);
        assert_eq!(worker_room(70 * GIB, 20 * GIB, 2), 1);
        assert_eq!(worker_room(70 * GIB, 20 * GIB, 3), 0);
        assert_eq!(worker_room(u64::MAX, u64::MAX, 2), 0);
        assert_eq!(worker_room(u64::MAX, 1, 0), u16::MAX);
        assert_eq!(worker_room(u64::MAX, 0, 0), 0);
    }

    #[test]
    fn symlink_and_nonprivate_directory_are_refused() {
        let root = TempRoot::new(0o700);
        let alias = root.0.with_extension("alias");
        symlink(&root.0, &alias).unwrap();
        let alias_storage = DisposableHostStorage::new(alias.clone(), 1).unwrap();
        let alias_error = alias_storage.admits_new_worker().unwrap_err();
        assert_eq!(alias_error.code(), HOST_STORAGE_UNAVAILABLE_CODE);
        fs::remove_file(alias).unwrap();

        fs::set_permissions(&root.0, fs::Permissions::from_mode(0o755)).unwrap();
        let unsafe_storage = DisposableHostStorage::new(root.0.clone(), 1).unwrap();
        let error = unsafe_storage.admits_new_worker().unwrap_err();
        assert_eq!(error.code(), HOST_STORAGE_UNAVAILABLE_CODE);
        assert!(!format!("{error:?}").contains(root.0.to_str().unwrap()));
    }
}
