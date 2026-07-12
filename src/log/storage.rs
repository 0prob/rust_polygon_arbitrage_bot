use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;

pub(super) fn secure_directory(path: &Path) -> io::Result<()> {
    if !path.exists() {
        create_private_directory(path)?;
    }
    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "log root must be a directory",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let process_owner = std::fs::metadata("/proc/self")?.uid();
        if metadata.uid() != process_owner {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "log root has a different owner",
            ));
        }
    }
    set_private_permissions(path)
}

pub(super) fn prune_run_directories(root: &Path, keep: usize) -> io::Result<()> {
    let mut runs = std::fs::read_dir(root)?
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("run-"))
        .filter(|entry| {
            entry
                .file_type()
                .is_ok_and(|kind| kind.is_dir() && !kind.is_symlink())
        })
        .collect::<Vec<_>>();
    runs.sort_by_key(std::fs::DirEntry::file_name);
    let remove_count = runs.len().saturating_sub(keep);
    for entry in runs.into_iter().take(remove_count) {
        std::fs::remove_dir_all(entry.path())?;
    }
    Ok(())
}

pub(super) fn create_private_directory(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        let mut builder = std::fs::DirBuilder::new();
        builder.mode(0o700).create(path)
    }
    #[cfg(not(unix))]
    std::fs::create_dir(path)
}

pub(super) fn private_file(path: &Path) -> io::Result<File> {
    private_options().create_new(true).open(path)
}

pub(super) fn truncate_private_file(path: &Path) -> io::Result<File> {
    private_options().truncate(true).open(path)
}

fn private_options() -> OpenOptions {
    let mut options = OpenOptions::new();
    options.write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
}

fn set_private_permissions(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(())
    }
}
