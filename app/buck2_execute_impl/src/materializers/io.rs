/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use buck2_core::fs::project::ProjectRoot;
use buck2_core::fs::project_rel_path::ProjectRelativePathBuf;
use buck2_directory::directory::directory::Directory;
use buck2_directory::directory::entry::DirectoryEntry;
use buck2_execute::directory::ActionDirectory;
use buck2_execute::directory::ActionDirectoryEntry;
use buck2_execute::directory::ActionDirectoryMember;
use buck2_execute::directory::ActionDirectoryRef;
use buck2_execute::directory::ActionSharedDirectory;
use buck2_execute::execute::blocking::IoRequest;
use buck2_fs::error::IoResultExt;
use buck2_fs::fs_util;
use buck2_fs::paths::abs_norm_path::AbsNormPath;
use buck2_fs::paths::abs_norm_path::AbsNormPathBuf;
#[cfg(test)]
use buck2_fs::paths::forward_rel_path::ForwardRelativePath;
use buck2_hash::StdBuckHashMap;

pub struct MaterializeTreeStructure {
    pub path: ProjectRelativePathBuf,
    pub entry: ActionDirectoryEntry<ActionSharedDirectory>,
}

impl IoRequest for MaterializeTreeStructure {
    fn execute(self: Box<Self>, project_fs: &ProjectRoot) -> buck2_error::Result<()> {
        materialize_dirs_and_syms(self.entry.as_ref(), project_fs.root().join(&self.path))?;

        Ok(())
    }
}

/// Materializes the entry at `dest`.
///
/// - `materialize_dirs_and_syms`: if `true`, materializes directories and
///   symlinks.
/// - `file_src`: takes the destination path of a file, and returns its
///   source path (where it should be copied from). If it returns [`None`],
///   the file is not materialized.
fn materialize<F, D>(
    entry: DirectoryEntry<&D, &ActionDirectoryMember>,
    dest: &AbsNormPath,
    materialize_dirs_and_syms: bool,
    mut file_src: F,
    executable_bit_override: Option<bool>,
) -> buck2_error::Result<()>
where
    F: FnMut(&AbsNormPath) -> Option<AbsNormPathBuf>,
    D: ActionDirectory,
{
    let mut dest = dest.to_owned();
    if materialize_dirs_and_syms {
        // create the directory where we'll materialize the entry
        if let Some(parent) = dest.parent() {
            fs_util::create_dir_all(parent)?;
        }
    }
    materialize_recursively(
        entry.map_dir(|d| Directory::as_ref(d)),
        &mut dest,
        materialize_dirs_and_syms,
        &mut file_src,
        executable_bit_override,
    )
}

/// Materializes the directories and symlinks of an entry at `dest`. Files
/// are not materialized.
pub(crate) fn materialize_dirs_and_syms<P, D>(
    entry: DirectoryEntry<&D, &ActionDirectoryMember>,
    dest: P,
) -> buck2_error::Result<()>
where
    P: AsRef<AbsNormPath>,
    D: ActionDirectory,
{
    materialize(entry, dest.as_ref(), true, |_: &AbsNormPath| None, None)
}

/// Materializes the files of an the entry rooted at `dest`.
///
/// Files are copied from `src`. In other words, if a file would be
/// materialized at `dest/p`, then it's copied from `src/p`.
pub(crate) fn materialize_files<P, D>(
    entry: DirectoryEntry<&D, &ActionDirectoryMember>,
    src: P,
    dest: P,
    executable_bit_override: Option<bool>,
) -> buck2_error::Result<()>
where
    P: AsRef<AbsNormPath>,
    D: ActionDirectory,
{
    let src = src.as_ref();
    let dest = dest.as_ref();
    let file_src = |d: &AbsNormPath| {
        // It's safe to unwrap because `materialize_impl` always gives us a
        // path inside `dest`.
        let subpath = d.strip_prefix(dest).unwrap();
        if subpath.as_str().is_empty() {
            // `dest` itself is a file
            Some(src.to_buf())
        } else {
            Some(src.join(subpath))
        }
    };
    materialize(entry, dest, false, file_src, executable_bit_override)
}

/// Materializes the files of an entry rooted at `dest`.
///
/// For a file at path `file_dest` in the entry, if `file_dest` exists in
/// `srcs` with value `file_src`, the file is copied from `file_src` to
/// `file_dest`. It's then removed from `srcs`.
fn _materialize_files_from_map<P, D>(
    entry: DirectoryEntry<&D, &ActionDirectoryMember>,
    srcs: &mut StdBuckHashMap<AbsNormPathBuf, AbsNormPathBuf>,
    dest: P,
) -> buck2_error::Result<()>
where
    P: AsRef<AbsNormPath>,
    D: ActionDirectory,
{
    let file_src = |d: &AbsNormPath| srcs.remove(d);
    materialize(entry, dest.as_ref(), false, file_src, None)
}

fn materialize_recursively<'a, F, D>(
    entry: DirectoryEntry<D, &ActionDirectoryMember>,
    dest: &mut AbsNormPathBuf,
    materialize_dirs_and_syms: bool,
    file_src: &mut F,
    executable_bit_override: Option<bool>,
) -> buck2_error::Result<()>
where
    F: FnMut(&AbsNormPath) -> Option<AbsNormPathBuf>,
    D: ActionDirectoryRef<'a>,
{
    match entry {
        DirectoryEntry::Dir(d) => {
            if materialize_dirs_and_syms {
                fs_util::create_dir_all(&dest)?;
            }
            for (name, entry) in d.entries() {
                dest.push(name);
                materialize_recursively(
                    entry,
                    dest,
                    materialize_dirs_and_syms,
                    file_src,
                    executable_bit_override,
                )?;
                dest.pop();
            }
            Ok(())
        }
        DirectoryEntry::Leaf(ActionDirectoryMember::File(_)) => {
            if let Some(src) = file_src(dest) {
                // A file already at `dest` may be a hard link into the shared CAS directory,
                // left by an earlier materialization; copying onto it would open the store's
                // inode for writing. Replace the name instead.
                fs_util::remove_all(&dest).categorize_internal()?;
                fs_util::copy(src, &dest).categorize_internal()?;
                if let Some(executable_bit_override) = executable_bit_override {
                    fs_util::set_executable(&dest, executable_bit_override)
                        .categorize_internal()?;
                }
            }
            Ok(())
        }
        DirectoryEntry::Leaf(ActionDirectoryMember::Symlink(s)) => {
            if materialize_dirs_and_syms
                && fs_util::symlink_metadata(&dest)
                    .categorize_internal()
                    .is_err()
            {
                fs_util::symlink(s.target().as_str(), dest).categorize_internal()?;
            }
            Ok(())
        }
        DirectoryEntry::Leaf(ActionDirectoryMember::ExternalSymlink(s)) => {
            if materialize_dirs_and_syms
                && fs_util::symlink_metadata(&dest)
                    .categorize_internal()
                    .is_err()
            {
                fs_util::symlink(s.target(), dest).categorize_internal()?;
            }
            Ok(())
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::os::unix::fs::MetadataExt;
    use std::os::unix::fs::PermissionsExt;

    use buck2_common::file_ops::metadata::FileMetadata;
    use buck2_execute::digest_config::DigestConfig;

    use super::*;

    #[test]
    fn copying_over_a_hard_link_replaces_the_name_not_the_inode() {
        let dir = tempfile::tempdir().unwrap();
        let root = AbsNormPathBuf::new(dir.path().to_owned()).unwrap();
        let blob = root.join(ForwardRelativePath::new("blob").unwrap());
        std::fs::write(&blob, b"stored").unwrap();
        std::fs::set_permissions(&blob, std::fs::Permissions::from_mode(0o444)).unwrap();
        let dest = root.join(ForwardRelativePath::new("dest").unwrap());
        std::fs::hard_link(&blob, &dest).unwrap();
        let src = root.join(ForwardRelativePath::new("src").unwrap());
        std::fs::write(&src, b"fresh").unwrap();

        let file = ActionDirectoryMember::File(FileMetadata::empty(
            DigestConfig::testing_default().cas_digest_config(),
        ));
        materialize_files(
            DirectoryEntry::<&ActionSharedDirectory, _>::Leaf(&file),
            &src,
            &dest,
            None,
        )
        .unwrap();

        assert_eq!(std::fs::read(&dest).unwrap(), b"fresh");
        let b = std::fs::metadata(&blob).unwrap();
        assert_eq!(std::fs::read(&blob).unwrap(), b"stored");
        assert_eq!((b.mode() & 0o777, b.nlink()), (0o444, 1));
        assert_ne!(std::fs::metadata(&dest).unwrap().ino(), b.ino());
    }
}
