/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! josh: a layout-invariant ("canonical") view of the cells, so that one tree imported into
//! another repository at another path, under another cell name, keys its actions as it does in
//! its own home.
//!
//! The map is read once, at daemon start, from the root cell's `[josh]` section:
//!
//! ```ini
//! [josh]
//!   enabled = true
//!   # <real cell name> = <canonical name>:<canonical path, "." for the project root>
//!   cell.mwb = root:.
//!   cell.root = lab:.josh/lab
//! ```
//!
//! A cell with no entry maps to itself. When every cell maps to itself (the home the tree comes
//! from), the map is the identity and nothing changes: [`active`] returns `None`, so every key is
//! computed by exactly the code paths that run with josh off.
//!
//! When the map is not the identity, actions are rendered in the canonical view: source paths,
//! the cell name in buck-out paths and the configuration hash use canonical names and paths. On
//! disk, buck-out uses the canonical cell names and configuration hashes too, so the only
//! differences between the views are where sources sit, which [`JoshMap::to_real`] undoes, and
//! the isolation dir, which this version still renders as it is on disk.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;

use allocative::Allocative;


use crate::cells::cell_root_path::CellRootPath;
use crate::cells::cell_root_path::CellRootPathBuf;
use crate::cells::name::CellName;
use buck2_fs::paths::forward_rel_path::ForwardRelativePath;
use crate::fs::project_rel_path::ProjectRelativePath;
use crate::fs::project_rel_path::ProjectRelativePathBuf;

/// The top-level directory a remapped cell's canonical path may use for cells that must move out
/// of the way. A cell mapped to the project root must not contain it.
pub const RESERVED_DIR: &str = ".josh";

#[derive(Debug, buck2_error::Error)]
#[buck2(tag = Input)]
pub enum JoshError {
    #[error("[josh] {0}: expected `<canonical name>:<canonical path>`, got `{1}`")]
    BadEntry(String, String),
    #[error("[josh] {0} names a cell that does not exist")]
    UnknownCell(String),
    #[error("[josh] the map is not injective on names: `{0}` and `{1}` both map to `{2}`")]
    NameCollision(String, String, String),
    #[error("[josh] the map is not injective on paths: `{0}` and `{1}` both map to `{2}`")]
    PathCollision(String, String, String),
    #[error(
        "[josh] canonical paths must nest as the real ones do: `{0}` is under `{1}` in one view but not the other"
    )]
    Nesting(String, String),
    #[error("[josh] `{0}` is reserved and must not exist in cell `{1}` (it maps to the project root)")]
    Reserved(String, String),
    #[error(
        "[josh] cell `{0}` maps to `{1}`, which is at or under `{2}`, an entry of cell `{3}` (it maps to the project root); the exec root would bind one over the other"
    )]
    Shadow(String, String, String, String),
    #[error(
        "[josh] cell `{0}` is an external cell; its sources are rendered under buck-out from its origin, not from its cell path, so the map cannot move it. Give it the path in `[cells]` instead (nothing of it is on disk there)"
    )]
    External(String),
    #[error("[josh] the map changed since the daemon started; restart the daemon (`buck2 kill`)")]
    Changed,
    #[error(
        "[josh] the cells changed since the daemon built its map ({0}); the map, the exec root and the overmount checks would still use the old cells, so restart the daemon (`buck2 kill`)"
    )]
    CellsChanged(String),
    #[error("[josh] verify_hits must be a fraction between 0 and 1, got `{0}`")]
    BadVerify(String),
    #[error(
        "[josh] {0}. The daemon builds its map once, from the root config's files, so `-c josh.*` and `--config-file` cannot change it; set [josh] in .buckconfig or .buckconfig.local, which starts a new daemon"
    )]
    Override(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Allocative)]
pub struct JoshCell {
    pub real_name: CellName,
    pub real_path: CellRootPathBuf,
    pub canonical_name: CellName,
    pub canonical_path: CellRootPathBuf,
    /// A bundled or git external cell. Nothing of it is at its cell path on disk: its sources
    /// render as `buck-out/v2/external_cells/<origin>/...` in both views, so the exec root binds
    /// nothing for it and no other cell's entry can overmount it.
    pub external: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Allocative)]
pub struct JoshMap {
    /// Every cell, sorted by canonical path, longest first, so that the first canonical prefix
    /// that matches a path is the innermost cell.
    cells: Vec<JoshCell>,
    by_real: HashMap<CellName, usize>,
    identity: bool,
}

fn is_under(p: &ProjectRelativePath, base: &ProjectRelativePath) -> bool {
    base.is_empty() || p.starts_with(base)
}

impl JoshMap {
    /// `entries` are the `[josh]` keys `cell.<real name>` with their values; `cells` is every
    /// cell the resolver knows, with its real path. `exists` reports whether a project-relative
    /// path exists on disk (for the reserved-dir check).
    pub fn new(
        entries: &[(String, String)],
        cells: &[(CellName, CellRootPathBuf)],
        exists: &dyn Fn(&ProjectRelativePath) -> bool,
    ) -> buck2_error::Result<JoshMap> {
        Self::new_with_external(entries, cells, &[], exists)
    }

    /// [`JoshMap::new`] where `external` names the cells that are bundled or git external cells.
    pub fn new_with_external(
        entries: &[(String, String)],
        cells: &[(CellName, CellRootPathBuf)],
        external: &[CellName],
        exists: &dyn Fn(&ProjectRelativePath) -> bool,
    ) -> buck2_error::Result<JoshMap> {
        let mut mapped: BTreeMap<&str, (CellName, CellRootPathBuf)> = BTreeMap::new();
        for (key, value) in entries {
            let Some(real) = key.strip_prefix("cell.") else {
                continue;
            };
            let (name, path) = value
                .split_once(':')
                .ok_or_else(|| JoshError::BadEntry(key.clone(), value.clone()))?;
            let name = name.trim();
            let path = path.trim();
            if name.is_empty() {
                return Err(JoshError::BadEntry(key.clone(), value.clone()).into());
            }
            let path = if path == "." || path.is_empty() {
                ProjectRelativePathBuf::unchecked_new(String::new())
            } else {
                ProjectRelativePathBuf::try_from(path.to_owned())
                    .map_err(|_| JoshError::BadEntry(key.clone(), value.clone()))?
            };
            if !cells.iter().any(|(c, _)| c.as_str() == real) {
                return Err(JoshError::UnknownCell(key.clone()).into());
            }
            mapped.insert(
                real,
                (CellName::unchecked_new(name)?, CellRootPathBuf::new(path)),
            );
        }
        let mut list = Vec::new();
        for (real_name, real_path) in cells {
            let (canonical_name, canonical_path) = match mapped.get(real_name.as_str()) {
                Some((n, p)) => (*n, p.clone()),
                None => (*real_name, real_path.clone()),
            };
            let is_external = external.contains(real_name);
            // The bundled path is named by the bundled cell and the git path by the commit, so
            // neither follows a map entry: a moved external cell would render `cell_root` at the
            // entry and its sources at the origin. Its `[cells]` path names nothing on disk, so a
            // home can choose it freely and never needs the map to move it. A new name is fine:
            // names key labels and configurations the same way for every kind of cell.
            if is_external && canonical_path != *real_path {
                return Err(JoshError::External(real_name.to_string()).into());
            }
            list.push(JoshCell {
                real_name: *real_name,
                real_path: real_path.clone(),
                canonical_name,
                canonical_path,
                external: is_external,
            });
        }
        // Injective on names and paths.
        for (i, a) in list.iter().enumerate() {
            for b in &list[i + 1..] {
                if a.canonical_name == b.canonical_name {
                    return Err(JoshError::NameCollision(
                        a.real_name.to_string(),
                        b.real_name.to_string(),
                        a.canonical_name.to_string(),
                    )
                    .into());
                }
                if a.canonical_path == b.canonical_path {
                    return Err(JoshError::PathCollision(
                        a.real_name.to_string(),
                        b.real_name.to_string(),
                        a.canonical_path.as_project_relative_path().to_string(),
                    )
                    .into());
                }
            }
        }
        // Nesting: if a cell is under another in one view, it must be under it in the other, at
        // the same relative path. Otherwise one path would have two readings.
        for a in &list {
            for b in &list {
                if a.real_name == b.real_name {
                    continue;
                }
                let ra = a.real_path.as_project_relative_path();
                let rb = b.real_path.as_project_relative_path();
                let ca = a.canonical_path.as_project_relative_path();
                let cb = b.canonical_path.as_project_relative_path();
                let real_under = is_under(ra, rb);
                let canon_under = is_under(ca, cb);
                // A cell moved to the reserved dir may leave a cell that maps to the root.
                let moved_out = ca
                    .as_forward_relative_path()
                    .iter()
                    .next()
                    .is_some_and(|s| s.as_str() == RESERVED_DIR);
                if real_under && canon_under {
                    let rr = ra.strip_prefix_opt(rb).map(|x| x.as_str().to_owned());
                    let cr = ca.strip_prefix_opt(cb).map(|x| x.as_str().to_owned());
                    let rr = if rb.is_empty() { Some(ra.as_str().to_owned()) } else { rr };
                    let cr = if cb.is_empty() { Some(ca.as_str().to_owned()) } else { cr };
                    if rr != cr && !(cb.is_empty() && moved_out) {
                        return Err(JoshError::Nesting(
                            a.real_name.to_string(),
                            b.real_name.to_string(),
                        )
                        .into());
                    }
                } else if real_under != canon_under {
                    // Under the project root in the canonical view only because the root is
                    // mapped, or moved out to the reserved dir: both are what the map is for.
                    if canon_under && cb.is_empty() {
                        continue;
                    }
                    if real_under && rb.is_empty() {
                        continue;
                    }
                    return Err(
                        JoshError::Nesting(a.real_name.to_string(), b.real_name.to_string())
                            .into(),
                    );
                }
            }
        }
        // The reserved dir and buck-out must not exist in a cell that maps to the root.
        for c in &list {
            if c.canonical_path.as_project_relative_path().is_empty()
                && c.real_path != c.canonical_path
            {
                for reserved in [RESERVED_DIR, "buck-out"] {
                    let p = c
                        .real_path
                        .as_project_relative_path()
                        .join(ForwardRelativePath::unchecked_new(reserved));
                    if exists(&p) {
                        return Err(
                            JoshError::Reserved(p.to_string(), c.real_name.to_string()).into()
                        );
                    }
                }
            }
        }
        // A cell mapped to the project root brings every real top-level entry of its tree into
        // the canonical view. Another cell whose canonical path is at or under one of those
        // entries would be bound over it or into it, and an action keyed on one cell's file would
        // read the other's. Only a cell that really sits at that place in the root-mapped tree
        // may use the name, and no remapped cell may use buck-out, which the rendered buck-out
        // takes.
        for r in &list {
            if !r.canonical_path.as_project_relative_path().is_empty() {
                continue;
            }
            let rr = r.real_path.as_project_relative_path();
            for c in &list {
                if c.real_name == r.real_name || c.external {
                    continue;
                }
                let cp = c.canonical_path.as_project_relative_path();
                let Some(first) = cp.as_forward_relative_path().iter().next() else {
                    continue;
                };
                let at_home = rr.join(cp.as_forward_relative_path());
                if c.real_path.as_project_relative_path().as_str() == at_home.as_str() {
                    continue;
                }
                let entry = rr.join(ForwardRelativePath::unchecked_new(first.as_str()));
                if first.as_str() == "buck-out" || exists(&entry) {
                    return Err(JoshError::Shadow(
                        c.real_name.to_string(),
                        cp.to_string(),
                        entry.to_string(),
                        r.real_name.to_string(),
                    )
                    .into());
                }
            }
        }
        let identity = list
            .iter()
            .all(|c| c.real_name == c.canonical_name && c.real_path == c.canonical_path);
        list.sort_by(|a, b| {
            b.canonical_path
                .as_project_relative_path()
                .as_str()
                .len()
                .cmp(&a.canonical_path.as_project_relative_path().as_str().len())
        });
        let by_real = list
            .iter()
            .enumerate()
            .map(|(i, c)| (c.real_name, i))
            .collect();
        Ok(JoshMap {
            cells: list,
            by_real,
            identity,
        })
    }

    pub fn is_identity(&self) -> bool {
        self.identity
    }

    pub fn cells(&self) -> &[JoshCell] {
        &self.cells
    }

    /// Refuses a command whose cell resolver no longer has the cells this map was built from.
    /// The map is built once per daemon, so a cell added or moved since would be missing from
    /// the validator and the exec root: its sources would render at their real path and resolve
    /// to whatever the root-mapped cell has there, under the new cell's digests.
    pub fn check_cells(
        &self,
        cells: &[(CellName, CellRootPathBuf)],
        external: &[CellName],
    ) -> buck2_error::Result<()> {
        let mut diffs = Vec::new();
        for (name, path) in cells {
            match self.get(*name) {
                None => diffs.push(format!("`{}` added at `{}`", name, path)),
                Some(c) if c.real_path != *path => diffs.push(format!(
                    "`{}` moved from `{}` to `{}`",
                    name, c.real_path, path
                )),
                Some(c) if c.external != external.contains(name) => {
                    diffs.push(format!("`{}` changed kind", name))
                }
                Some(_) => {}
            }
        }
        for c in &self.cells {
            if !cells.iter().any(|(n, _)| *n == c.real_name) {
                diffs.push(format!("`{}` removed", c.real_name));
            }
        }
        if diffs.is_empty() {
            Ok(())
        } else {
            Err(JoshError::CellsChanged(diffs.join(", ")).into())
        }
    }

    fn get(&self, real: CellName) -> Option<&JoshCell> {
        self.by_real.get(&real).map(|i| &self.cells[*i])
    }

    pub fn canonical_cell_name(&self, real: CellName) -> CellName {
        self.get(real).map_or(real, |c| c.canonical_name)
    }

    pub fn canonical_cell_path(&self, real: CellName) -> Option<&CellRootPath> {
        self.get(real).map(|c| c.canonical_path.as_path())
    }

    /// Rewrite the cell of a `cell//...` label string to its canonical name. Strings that are not
    /// labels, or name no remapped cell, are returned unchanged.
    pub fn canonical_label_str(&self, s: &str) -> Option<String> {
        let (cell, rest) = s.split_once("//")?;
        let c = self.cells.iter().find(|c| c.real_name.as_str() == cell)?;
        if c.canonical_name == c.real_name {
            return None;
        }
        Some(format!("{}//{}", c.canonical_name, rest))
    }

    /// A project-relative path in the canonical view, as it is on disk. `buck_out` is the
    /// on-disk buck-out root (`buck-out/<isolation dir>`), which renders as itself in this version.
    pub fn to_real(
        &self,
        p: &ProjectRelativePath,
        buck_out: Option<&ProjectRelativePath>,
    ) -> ProjectRelativePathBuf {
        if p.starts_with(ProjectRelativePath::unchecked_new("buck-out"))
            || buck_out.is_some_and(|b| p.starts_with(b))
        {
            return p.to_buf();
        }
        for c in &self.cells {
            let cp = c.canonical_path.as_project_relative_path();
            if cp.is_empty() {
                return c.real_path.as_project_relative_path().join(p.as_forward_relative_path());
            }
            if let Some(rest) = p.strip_prefix_opt(cp) {
                return c.real_path.as_project_relative_path().join(rest);
            }
        }
        p.to_buf()
    }
}

static JOSH: OnceLock<Arc<JoshMap>> = OnceLock::new();

/// The root config's `[josh]` section, sorted, as the daemon read it at startup from the config
/// files alone (`None` without one). The map, the verify fraction and the exec root all come
/// from it.
static STARTUP_SECTION: OnceLock<Option<Vec<(String, String)>>> = OnceLock::new();

/// Record the `[josh]` section the daemon started with. Called once at daemon start, whether or
/// not the section enables josh.
pub fn init_startup_section(section: Option<Vec<(String, String)>>) {
    let _ = STARTUP_SECTION.set(section);
}

/// The keys on which `command` (a command's root `[josh]` section, sorted) differs from
/// `started` (the daemon's), as one message, or `None` when they agree. A missing section is
/// the same as an empty one.
pub fn section_diff(
    started: Option<&[(String, String)]>,
    command: Option<&[(String, String)]>,
) -> Option<String> {
    let started = started.unwrap_or(&[]);
    let command = command.unwrap_or(&[]);
    if started == command {
        return None;
    }
    let get = |s: &[(String, String)], k: &str| {
        s.iter()
            .find(|(x, _)| x == k)
            .map_or_else(|| "unset".to_owned(), |(_, v)| format!("`{v}`"))
    };
    let keys: std::collections::BTreeSet<&str> =
        started.iter().chain(command).map(|(k, _)| k.as_str()).collect();
    let diffs: Vec<String> = keys
        .into_iter()
        .filter_map(|k| {
            let (a, b) = (get(started, k), get(command, k));
            (a != b).then(|| format!("`josh.{k}` is {b} for this command but {a} in the daemon"))
        })
        .collect();
    Some(diffs.join(", "))
}

/// Refuses a command whose root config has another `[josh]` section than the daemon started
/// with. The client restarts the daemon when the files change, but a `-c josh.*` or a
/// `--config-file` reaches only the command's config: the map would silently stay the
/// daemon's, and an "off" comparison made that way would run with josh on.
pub fn check_section(command: Option<&[(String, String)]>) -> buck2_error::Result<()> {
    let Some(started) = STARTUP_SECTION.get() else {
        return Ok(());
    };
    match section_diff(started.as_deref(), command) {
        None => Ok(()),
        Some(d) => Err(JoshError::Override(d).into()),
    }
}

/// Install the daemon's map. A second call with an equal map is a no-op; a different one is an
/// error, because configurations and paths computed under the first are already interned.
pub fn init(map: JoshMap) -> buck2_error::Result<()> {
    let map = Arc::new(map);
    let got = JOSH.get_or_init(|| map.clone());
    if **got != *map {
        return Err(JoshError::Changed.into());
    }
    Ok(())
}

/// The installed map when it is not the identity. `None` means every key is computed as it is
/// with josh off.
pub fn active() -> Option<&'static JoshMap> {
    JOSH.get().map(|m| &**m).filter(|m| !m.identity)
}

/// The installed map, identity included, for recording which view a build ran in.
pub fn installed() -> Option<&'static JoshMap> {
    JOSH.get().map(|m| &**m)
}

/// The buck-out root every action is rendered with when the map is active, whatever the daemon's
/// isolation dir (design D4). On disk the outputs stay under `buck-out/<isolation dir>`;
/// [`disk_path`] maps a rendered path there.
pub const CANONICAL_BUCK_OUT: &str = "buck-out/v2";

struct JoshDisk {
    real_buck_out: ProjectRelativePathBuf,
    project_root: std::path::PathBuf,
    /// The isolation dir differs from the canonical one, so buck-out paths need mapping.
    remap_buck_out: bool,
}

static DISK: OnceLock<JoshDisk> = OnceLock::new();

/// Record the daemon's on-disk buck-out root and project root. Called once at daemon start, after
/// [`init`].
pub fn init_disk(real_buck_out: ProjectRelativePathBuf, project_root: std::path::PathBuf) {
    let remap_buck_out = active().is_some() && real_buck_out.as_str() != CANONICAL_BUCK_OUT;
    let _ = DISK.set(JoshDisk {
        real_buck_out,
        project_root,
        remap_buck_out,
    });
}

/// The buck-out root to render actions with: [`CANONICAL_BUCK_OUT`] when the map is active,
/// `real` otherwise, so with josh off or an identity map nothing changes.
pub fn rendered_buck_out(real: ProjectRelativePathBuf) -> ProjectRelativePathBuf {
    if active().is_some() {
        ProjectRelativePathBuf::unchecked_new(CANONICAL_BUCK_OUT.to_owned())
    } else {
        real
    }
}

/// Where a rendered project-relative path is on disk. Only the buck-out root differs: a rendered
/// `buck-out/v2/...` is `buck-out/<isolation dir>/...` on disk. Sources are mapped by the callers
/// that know a path is a rendered source ([`JoshMap::to_real`]), because a real source path read
/// through the cell resolver must not be mapped twice.
pub fn disk_path(p: &ProjectRelativePath) -> std::borrow::Cow<'_, ProjectRelativePath> {
    if let Some(d) = DISK.get() {
        if d.remap_buck_out {
            if let Some(rest) =
                p.strip_prefix_opt(ProjectRelativePath::unchecked_new(CANONICAL_BUCK_OUT))
            {
                return std::borrow::Cow::Owned(d.real_buck_out.join(rest));
            }
        }
    }
    std::borrow::Cow::Borrowed(p)
}

/// Lexically resolve `text` relative to the directory components `dir`. `None` when it climbs
/// out of the project.
fn lexical_join<'a>(dir: &[&'a str], text: &'a str) -> Option<Vec<&'a str>> {
    let mut parts: Vec<&str> = dir.to_vec();
    for comp in text.split('/') {
        match comp {
            "" | "." => {}
            ".." => {
                parts.pop()?;
            }
            c => parts.push(c),
        }
    }
    Some(parts)
}

impl JoshMap {
    /// The text a relative symlink at project-relative `link` should have on disk, given the
    /// text it has in the canonical view, for a project at absolute path `project_root`. A
    /// symlink into buck-out keeps its text (buck-out sits at the same depth in both views). One
    /// to a source whose real path differs from its canonical path is re-pointed at the source's
    /// absolute real path. `None` when the text needs no change.
    ///
    /// No relative text names the same file in both views: from buck-out, `..` climbs to the
    /// exec root inside the namespace and to the project root on the host. A relative real-view
    /// text resolved in the exec root only while its first component was not a canonical
    /// top-level name, and that set changes whenever the root-mapped tree gains or loses an
    /// entry. A link written before such a change read the other tree's file under this one's
    /// key, and kept doing so after a restart. The namespace mounts nothing outside the exec
    /// root, so an absolute real path is the same file in the exec root and on the host,
    /// whatever the canonical top-level names are. It names this checkout, so the disk state
    /// that remembers the link is keyed on the project root ([`disk_state_version`]).
    pub fn disk_symlink_text_rel(
        &self,
        link: &str,
        text: &str,
        project_root: &str,
    ) -> Option<String> {
        if text.starts_with('/') {
            return None;
        }
        let comps: Vec<&str> = link.split('/').filter(|c| !c.is_empty()).collect();
        let dir = &comps[..comps.len().checked_sub(1)?];
        let target = lexical_join(dir, text)?;
        if target.first() == Some(&"buck-out") {
            return None;
        }
        let target_s = target.join("/");
        let canonical = if target_s.is_empty() {
            ProjectRelativePathBuf::unchecked_new(String::new())
        } else {
            ProjectRelativePathBuf::try_from(target_s.clone()).ok()?
        };
        let real = self.to_real(&canonical, None);
        if real.as_str() == target_s {
            return None;
        }
        let root = project_root.trim_end_matches('/');
        Some(if real.as_str().is_empty() {
            root.to_owned()
        } else {
            format!("{}/{}", root, real.as_str())
        })
    }

    /// One line per fact of the map, for the event log and the build report (design D7).
    pub fn describe(&self) -> Vec<String> {
        let mut v: Vec<String> = self
            .cells
            .iter()
            .map(|c| {
                format!(
                    "cell {}={}:{}",
                    c.real_name,
                    c.canonical_name,
                    if c.canonical_path.as_project_relative_path().is_empty() {
                        ".".to_owned()
                    } else {
                        c.canonical_path.as_project_relative_path().to_string()
                    }
                )
            })
            .collect();
        v.sort();
        v
    }
}

/// [`JoshMap::disk_symlink_text_rel`] for a symlink at absolute path `link`.
pub fn disk_symlink_text(link: &std::path::Path, text: &str) -> Option<String> {
    let josh = active()?;
    let d = DISK.get()?;
    let rel = link.strip_prefix(&d.project_root).ok()?.to_str()?;
    josh.disk_symlink_text_rel(rel, text, d.project_root.to_str()?)
}

/// The version the daemon's disk state (the materializer's above all) is kept under while the
/// map is active. On-disk symlinks to sources name the project root and follow the map, so
/// state written under another root, another map or the relative text of earlier versions is
/// discarded at startup, and every link it would have reused is written again. `None` with josh
/// off or an identity map, so their version keys are exactly as before.
pub fn disk_state_version() -> Option<String> {
    let josh = active()?;
    let d = DISK.get()?;
    Some(format!(
        "source-links=absolute root={} {}",
        d.project_root.display(),
        josh.describe().join(" ")
    ))
}

/// Rewrite every relative symlink at or under `path` (an output a local action just wrote, in the
/// canonical view) to its on-disk text. The action's output digest was already taken from the
/// canonical text, so the key and the cache entry are those of the tree's own home.
pub fn rewrite_disk_symlinks(path: &std::path::Path) -> std::io::Result<u64> {
    if active().is_none() {
        return Ok(0);
    }
    let md = match std::fs::symlink_metadata(path) {
        Ok(md) => md,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e),
    };
    if md.file_type().is_symlink() {
        let text = std::fs::read_link(path)?;
        if let Some(new) = text.to_str().and_then(|t| disk_symlink_text(path, t)) {
            std::fs::remove_file(path)?;
            std::os::unix::fs::symlink(new, path)?;
            return Ok(1);
        }
        Ok(0)
    } else if md.is_dir() {
        let mut n = 0;
        for e in std::fs::read_dir(path)? {
            n += rewrite_disk_symlinks(&e?.path())?;
        }
        Ok(n)
    } else {
        Ok(0)
    }
}

/// Verify mode (design D6): the fraction of action-cache hits that are re-run locally and whose
/// outputs are compared with the ones the cache served. Read once at daemon start from
/// `[josh] verify_hits`; unset or 0 leaves every hit as it is.
static VERIFY: OnceLock<f64> = OnceLock::new();

/// Parse and install `[josh] verify_hits`.
pub fn init_verify(value: Option<&str>) -> buck2_error::Result<()> {
    let Some(v) = value else {
        return Ok(());
    };
    let f: f64 = v
        .trim()
        .parse()
        .map_err(|_| JoshError::BadVerify(v.to_owned()))?;
    if !(0.0..=1.0).contains(&f) {
        return Err(JoshError::BadVerify(v.to_owned()).into());
    }
    let got = VERIFY.get_or_init(|| f);
    if *got != f {
        return Err(JoshError::Changed.into());
    }
    Ok(())
}

pub fn verify_fraction() -> f64 {
    VERIFY.get().copied().unwrap_or(0.0)
}

/// Whether the hit for an action with digest bytes `digest` is verified. The choice depends only
/// on the digest, so a sample picks the same actions in every build and every home.
pub fn verify_selects_fraction(fraction: f64, digest: &[u8]) -> bool {
    if fraction <= 0.0 {
        return false;
    }
    if fraction >= 1.0 {
        return true;
    }
    let mut b = [0u8; 8];
    let n = digest.len().min(8);
    b[..n].copy_from_slice(&digest[..n]);
    (u64::from_be_bytes(b) as f64) < fraction * (u64::MAX as f64)
}

pub fn verify_selects(digest: &[u8]) -> bool {
    verify_selects_fraction(verify_fraction(), digest)
}

/// The outcome of verifying one hit, as an event-log tag. `JoshFalseShare` is the one a reader
/// counts: the cache served outputs that a local run of the same action did not produce.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerifyOutcome {
    Ok,
    FalseShare,
    LocalFailed,
    /// Not checked: the action's executor cannot run it locally, or the re-run was not local.
    NoLocal,
}

pub fn verify_tag(outcome: VerifyOutcome, digest: &str, what: &str) -> String {
    let o = match outcome {
        VerifyOutcome::Ok => "josh:verify=ok",
        VerifyOutcome::FalseShare => "josh:JoshFalseShare",
        VerifyOutcome::LocalFailed => "josh:verify=local-failed",
        VerifyOutcome::NoLocal => "josh:verify=no-local",
    };
    format!("{o} {digest} {what}")
}

/// What a command ran under, as event-log tags (design D7): nothing without a `[josh]` section,
/// so an event log with josh off is as before; `josh:identity` in the tree's own home;
/// `josh:active` plus the map and the rendered and on-disk buck-out roots otherwise.
pub fn describe_tags() -> Vec<String> {
    match installed() {
        None => Vec::new(),
        Some(m) if m.is_identity() => {
            let mut v = vec!["josh:identity".to_owned()];
            if verify_fraction() > 0.0 {
                v.push(format!("josh:verify_hits={}", verify_fraction()));
            }
            v
        }
        Some(m) => {
            let mut v = vec!["josh:active".to_owned(), "josh:view=canonical".to_owned()];
            if let Some(d) = DISK.get() {
                v.push(format!(
                    "josh:buck-out={}->{}",
                    CANONICAL_BUCK_OUT, d.real_buck_out
                ));
            }
            v.extend(m.describe().into_iter().map(|l| format!("josh:{l}")));
            if verify_fraction() > 0.0 {
                v.push(format!("josh:verify_hits={}", verify_fraction()));
            }
            v
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cells(v: &[(&str, &str)]) -> Vec<(CellName, CellRootPathBuf)> {
        v.iter()
            .map(|(n, p)| (CellName::testing_new(n), CellRootPathBuf::testing_new(p)))
            .collect()
    }

    fn entries(v: &[(&str, &str)]) -> Vec<(String, String)> {
        v.iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    fn never(_: &ProjectRelativePath) -> bool {
        false
    }

    #[test]
    fn check_cells_refuses_a_cell_added_moved_or_removed() -> buck2_error::Result<()> {
        let map = JoshMap::new(&lab_map(), &lab(), &never)?;
        map.check_cells(&lab(), &[])?;
        let mut added = lab();
        added.push((
            CellName::testing_new("labpkg"),
            CellRootPathBuf::testing_new("pkg"),
        ));
        let e = map.check_cells(&added, &[]).unwrap_err().to_string();
        assert!(e.contains("`labpkg` added at `pkg`"), "{e}");
        let moved: Vec<_> = lab()
            .into_iter()
            .map(|(n, p)| {
                if n.as_str() == "mwb" {
                    (n, CellRootPathBuf::testing_new("services/wb2"))
                } else {
                    (n, p)
                }
            })
            .collect();
        let e = map.check_cells(&moved, &[]).unwrap_err().to_string();
        assert!(
            e.contains("`mwb` moved from `services/web-backend` to `services/wb2`"),
            "{e}"
        );
        let removed: Vec<_> = lab()
            .into_iter()
            .filter(|(n, _)| n.as_str() != "ftools")
            .collect();
        let e = map.check_cells(&removed, &[]).unwrap_err().to_string();
        assert!(e.contains("`ftools` removed"), "{e}");
        let e = map
            .check_cells(&lab(), &[CellName::testing_new("ftools")])
            .unwrap_err()
            .to_string();
        assert!(e.contains("`ftools` changed kind"), "{e}");
        Ok(())
    }

    fn lab() -> Vec<(CellName, CellRootPathBuf)> {
        cells(&[
            ("root", ""),
            ("mwb", "services/web-backend"),
            ("mwb_ftools", "services/web-backend/ftools"),
            ("ftools", "tools"),
        ])
    }

    fn lab_map() -> Vec<(String, String)> {
        entries(&[
            ("cell.mwb", "root:."),
            ("cell.mwb_ftools", "ftools:ftools"),
            ("cell.ftools", "lab_ftools:.josh/lab_ftools"),
            ("cell.root", "lab:.josh/lab"),
        ])
    }

    #[test]
    fn identity_without_entries() {
        let m = JoshMap::new(&[], &cells(&[("root", ""), ("ftools", "ftools")]), &never).unwrap();
        assert!(m.is_identity());
    }

    #[test]
    fn lab_shape_maps_and_resolves() {
        let m = JoshMap::new(&lab_map(), &lab(), &never).unwrap();
        assert!(!m.is_identity());
        let mwb = CellName::testing_new("mwb");
        assert_eq!(m.canonical_cell_name(mwb).as_str(), "root");
        assert_eq!(
            m.canonical_cell_name(CellName::testing_new("root")).as_str(),
            "lab"
        );
        assert_eq!(
            m.to_real(ProjectRelativePath::unchecked_new("pkg/a.src"), None)
                .as_str(),
            "services/web-backend/pkg/a.src"
        );
        assert_eq!(
            m.to_real(ProjectRelativePath::unchecked_new("ftools/cc.sh"), None)
                .as_str(),
            "services/web-backend/ftools/cc.sh"
        );
        assert_eq!(
            m.to_real(
                ProjectRelativePath::unchecked_new(".josh/lab_ftools/cc.sh"),
                None
            )
            .as_str(),
            "tools/cc.sh"
        );
        assert_eq!(
            m.to_real(ProjectRelativePath::unchecked_new(".josh/lab/lab/l.src"), None)
                .as_str(),
            "lab/l.src"
        );
        assert_eq!(
            m.to_real(ProjectRelativePath::unchecked_new("buck-out/v2/art/x"), None)
                .as_str(),
            "buck-out/v2/art/x"
        );
        assert_eq!(
            m.canonical_label_str("mwb//platforms:default").as_deref(),
            Some("root//platforms:default")
        );
        assert_eq!(m.canonical_label_str("builtin"), None);
    }

    #[test]
    fn refuses_name_collision() {
        // lab's own cell `ftools` left unmapped collides with mwb_ftools -> ftools.
        let e = entries(&[
            ("cell.mwb", "root:."),
            ("cell.mwb_ftools", "ftools:ftools"),
            ("cell.root", "lab:.josh/lab"),
        ]);
        let err = JoshMap::new(&e, &lab(), &never).unwrap_err();
        assert!(format!("{err}").contains("not injective on names"), "{err}");
    }

    #[test]
    fn refuses_path_collision() {
        // lab's root left at the project root collides with mwb mapped there.
        let e = entries(&[
            ("cell.mwb", "root:."),
            ("cell.mwb_ftools", "ftools:ftools"),
            ("cell.ftools", "lab_ftools:.josh/lab_ftools"),
            ("cell.root", "lab:."),
        ]);
        let err = JoshMap::new(&e, &lab(), &never).unwrap_err();
        assert!(format!("{err}").contains("not injective on paths"), "{err}");
    }

    #[test]
    fn refuses_inconsistent_nesting() {
        // mwb_ftools is under mwb at `ftools` really, but mapped to `tools2`.
        let e = entries(&[
            ("cell.mwb", "root:."),
            ("cell.mwb_ftools", "ftools:tools2"),
            ("cell.ftools", "lab_ftools:.josh/lab_ftools"),
            ("cell.root", "lab:.josh/lab"),
        ]);
        let err = JoshMap::new(&e, &lab(), &never).unwrap_err();
        assert!(format!("{err}").contains("nest"), "{err}");
    }

    #[test]
    fn refuses_reserved_dir_in_root_mapped_cell() {
        let exists = |p: &ProjectRelativePath| p.as_str() == "services/web-backend/.josh";
        let err = JoshMap::new(&lab_map(), &lab(), &exists).unwrap_err();
        assert!(format!("{err}").contains("reserved"), "{err}");
    }

    fn lab_with(extra: (&str, &str)) -> Vec<(CellName, CellRootPathBuf)> {
        let mut v = lab();
        v.extend(cells(&[extra]));
        v
    }

    #[test]
    fn refuses_cell_at_root_mapped_entry() {
        // The review's case: a lab cell left at `pkg` while mwb, mapped to the root, has its own
        // `pkg`. Binding both at `pkg` let an action keyed on lab's file read mwb's.
        let exists = |p: &ProjectRelativePath| p.as_str() == "services/web-backend/pkg";
        let err = JoshMap::new(&lab_map(), &lab_with(("labpkg", "pkg")), &exists).unwrap_err();
        assert!(format!("{err}").contains("bind one over the other"), "{err}");
        // Without mwb's `pkg` the same map is fine.
        JoshMap::new(&lab_map(), &lab_with(("labpkg", "pkg")), &never).unwrap();
    }

    #[test]
    fn refuses_cell_under_root_mapped_entry() {
        let exists = |p: &ProjectRelativePath| p.as_str() == "services/web-backend/pkg";
        let mut e = lab_map();
        e.extend(entries(&[("cell.labd", "labd:pkg/deep")]));
        let err = JoshMap::new(&e, &lab_with(("labd", "labd")), &exists).unwrap_err();
        assert!(format!("{err}").contains("bind one over the other"), "{err}");
    }

    #[test]
    fn refuses_remapped_cell_in_buck_out() {
        let mut e = lab_map();
        e.extend(entries(&[("cell.labd", "labd:buck-out/x")]));
        let err = JoshMap::new(&e, &lab_with(("labd", "labd")), &never).unwrap_err();
        assert!(format!("{err}").contains("bind one over the other"), "{err}");
    }

    #[test]
    fn accepts_cell_really_inside_root_mapped_cell() {
        // mwb_ftools is mwb's own `ftools`, so mwb's entry is that cell's tree.
        let exists = |p: &ProjectRelativePath| p.as_str() == "services/web-backend/ftools";
        JoshMap::new(&lab_map(), &lab(), &exists).unwrap();
    }

    fn lab_external() -> Vec<(CellName, CellRootPathBuf)> {
        let mut v = lab();
        v.extend(cells(&[
            ("prelude", "prelude"),
            ("buck2-haskell", "buck2-haskell"),
        ]));
        v
    }

    fn external() -> Vec<CellName> {
        vec![
            CellName::testing_new("prelude"),
            CellName::testing_new("buck2-haskell"),
        ]
    }

    #[test]
    fn external_cells_stay_as_they_are() {
        let m =
            JoshMap::new_with_external(&lab_map(), &lab_external(), &external(), &never).unwrap();
        for c in m.cells() {
            let ext = c.real_name.as_str() == "prelude" || c.real_name.as_str() == "buck2-haskell";
            assert_eq!(c.external, ext, "{c:?}");
            if ext {
                assert_eq!(c.canonical_name, c.real_name);
                assert_eq!(c.canonical_path, c.real_path);
            }
        }
    }

    #[test]
    fn refuses_moving_an_external_cell() {
        for entry in ["buck2-haskell:.josh/bh", "bh:bh"] {
            let mut e = lab_map();
            e.extend(entries(&[("cell.buck2-haskell", entry)]));
            let err =
                JoshMap::new_with_external(&e, &lab_external(), &external(), &never).unwrap_err();
            assert!(
                format!("{err}").contains("is an external cell"),
                "{entry}: {err}"
            );
        }
        // Spelling out the identity is not a move.
        let mut e = lab_map();
        e.extend(entries(&[("cell.prelude", "prelude:prelude")]));
        JoshMap::new_with_external(&e, &lab_external(), &external(), &never).unwrap();
    }

    #[test]
    fn renames_an_external_cell_in_place() {
        // Lab's two pins of buck2-haskell: mwb's at the path mwb gives it, under mwb's name, and
        // lab's own under another name at a path of its own.
        let mut c = lab();
        c.extend(cells(&[
            ("buck2-haskell", "lab-buck2-haskell"),
            ("mwb_buck2_haskell", "buck2-haskell"),
        ]));
        let ext = vec![
            CellName::testing_new("buck2-haskell"),
            CellName::testing_new("mwb_buck2_haskell"),
        ];
        let mut e = lab_map();
        e.extend(entries(&[
            ("cell.mwb_buck2_haskell", "buck2-haskell:buck2-haskell"),
            ("cell.buck2-haskell", "lab_buck2_haskell:lab-buck2-haskell"),
        ]));
        let m = JoshMap::new_with_external(&e, &c, &ext, &never).unwrap();
        assert_eq!(
            m.canonical_cell_name(CellName::testing_new("mwb_buck2_haskell"))
                .as_str(),
            "buck2-haskell"
        );
        // Both under one canonical name is still a collision.
        let mut e = lab_map();
        e.extend(entries(&[(
            "cell.mwb_buck2_haskell",
            "buck2-haskell:buck2-haskell",
        )]));
        assert!(JoshMap::new_with_external(&e, &c, &ext, &never).is_err());
    }

    #[test]
    fn external_cell_is_not_shadowed_by_a_root_mapped_entry() {
        // Nothing is bound at an external cell's path, so mwb having an entry of that name is
        // what mwb's own home shows there too.
        let exists = |p: &ProjectRelativePath| p.as_str() == "services/web-backend/prelude";
        JoshMap::new_with_external(&lab_map(), &lab_external(), &external(), &exists).unwrap();
        // The same name for an ordinary cell is still refused.
        let err = JoshMap::new(&lab_map(), &lab_external(), &exists).unwrap_err();
        assert!(
            format!("{err}").contains("bind one over the other"),
            "{err}"
        );
    }

    #[test]
    fn refuses_unknown_cell_and_bad_entry() {
        let e = entries(&[("cell.nope", "root:.")]);
        assert!(JoshMap::new(&e, &lab(), &never).is_err());
        let e = entries(&[("cell.mwb", "root")]);
        assert!(JoshMap::new(&e, &lab(), &never).is_err());
    }

    const ROOT: &str = "/w/lab";

    #[test]
    fn symlink_text_to_source_points_at_absolute_real_path() {
        let m = JoshMap::new(&lab_map(), &lab(), &never).unwrap();
        // A symlinked_dir entry in buck-out pointing at mwb's pkg/a.src.
        assert_eq!(
            m.disk_symlink_text_rel(
                "buck-out/josh-b/art/root/pkg/__s_dirs__/sd/a.txt",
                "../../../../../../../pkg/a.src",
                ROOT
            )
            .as_deref(),
            Some("/w/lab/services/web-backend/pkg/a.src")
        );
        // Into buck-out: unchanged.
        assert_eq!(
            m.disk_symlink_text_rel("buck-out/v2/art/root/pkg/__x__/l", "../../__y__/out", ROOT),
            None
        );
        // A lab-only cell's source; a trailing slash on the root changes nothing.
        assert_eq!(
            m.disk_symlink_text_rel(
                "buck-out/v2/art/lab/l/__t__/s",
                "../../../../../../.josh/lab/l/f",
                "/w/lab/"
            )
            .as_deref(),
            Some("/w/lab/l/f")
        );
        // Absolute and out-of-project text: unchanged.
        assert_eq!(m.disk_symlink_text_rel("buck-out/v2/x", "/etc/passwd", ROOT), None);
        assert_eq!(m.disk_symlink_text_rel("buck-out/v2/x", "../../../up", ROOT), None);
    }

    #[test]
    fn symlink_text_does_not_depend_on_the_canonical_top_level_names() {
        // Round 5 kept the canonical text when the real target's top-level name was also a
        // canonical one, and wrote real-view relative text otherwise, so one link's meaning in
        // the exec root changed with the root-mapped tree's entries. The text now names the
        // real file directly: an mwb source whose real path goes through `services` (which the
        // real mwb tree also has) and a lab source under a name mwb also has both get it.
        let m = JoshMap::new(&lab_map(), &lab(), &never).unwrap();
        assert_eq!(
            m.disk_symlink_text_rel("buck-out/v2/art/root/pr/__sym__/sym.txt", "../../../../../../pr/x.txt", ROOT)
                .as_deref(),
            Some("/w/lab/services/web-backend/pr/x.txt")
        );
        assert_eq!(
            m.disk_symlink_text_rel(
                "buck-out/v2/art/lab/pq/__sym__/sym.txt",
                "../../../../../../.josh/lab/pq/x.txt",
                ROOT
            )
            .as_deref(),
            Some("/w/lab/pq/x.txt")
        );
    }

    #[test]
    fn identity_symlink_text_unchanged() {
        let m = JoshMap::new(&[], &cells(&[("root", ""), ("ftools", "ftools")]), &never).unwrap();
        assert_eq!(
            m.disk_symlink_text_rel("buck-out/v2/art/root/pkg/__t__/l", "../../../../../../pkg/a.src", ROOT),
            None
        );
    }

    #[test]
    fn section_diff_names_each_changed_key() {
        let on = entries(&[("enabled", "true"), ("cell.mwb", "root:.")]);
        let mut sorted = on.clone();
        sorted.sort();
        assert_eq!(section_diff(Some(&sorted), Some(&sorted)), None);
        assert_eq!(section_diff(None, None), None);
        assert_eq!(section_diff(None, Some(&[])), None);
        let off = entries(&[("cell.mwb", "root:."), ("enabled", "false")]);
        assert_eq!(
            section_diff(Some(&sorted), Some(&off)).as_deref(),
            Some("`josh.enabled` is `false` for this command but `true` in the daemon")
        );
        let more = entries(&[("cell.mwb", "root:."), ("enabled", "true"), ("verify_hits", "1")]);
        assert_eq!(
            section_diff(Some(&sorted), Some(&more)).as_deref(),
            Some("`josh.verify_hits` is `1` for this command but unset in the daemon")
        );
        assert_eq!(
            section_diff(None, Some(&entries(&[("enabled", "true")]))).as_deref(),
            Some("`josh.enabled` is `true` for this command but unset in the daemon")
        );
    }

    #[test]
    fn verify_sampling_is_by_digest() {
        assert!(!verify_selects_fraction(0.0, &[0; 32]));
        assert!(verify_selects_fraction(1.0, &[0xff; 32]));
        assert!(verify_selects_fraction(0.5, &[0x10; 32]));
        assert!(!verify_selects_fraction(0.5, &[0xf0; 32]));
        // Same digest, same answer.
        let d = [0x7f, 0xff, 0, 0, 0, 0, 0, 0];
        assert_eq!(verify_selects_fraction(0.5, &d), verify_selects_fraction(0.5, &d));
        assert_eq!(
            verify_tag(VerifyOutcome::FalseShare, "ab:1", "c i"),
            "josh:JoshFalseShare ab:1 c i"
        );
    }
}
