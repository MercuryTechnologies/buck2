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
    #[error("[josh] the map changed since the daemon started; restart the daemon (`buck2 kill`)")]
    Changed,
    #[error("[josh] verify_hits must be a fraction between 0 and 1, got `{0}`")]
    BadVerify(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Allocative)]
pub struct JoshCell {
    pub real_name: CellName,
    pub real_path: CellRootPathBuf,
    pub canonical_name: CellName,
    pub canonical_path: CellRootPathBuf,
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
            list.push(JoshCell {
                real_name: *real_name,
                real_path: real_path.clone(),
                canonical_name,
                canonical_path,
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

/// The relative path from directory `from` to `to`, both as components from the project root.
fn relativize(from: &[&str], to: &[&str]) -> String {
    let n = from.iter().zip(to.iter()).take_while(|(a, b)| a == b).count();
    let mut out: Vec<&str> = std::iter::repeat_n("..", from.len() - n).collect();
    out.extend(&to[n..]);
    if out.is_empty() {
        ".".to_owned()
    } else {
        out.join("/")
    }
}

impl JoshMap {
    /// The text a relative symlink at project-relative `link` should have on disk, given the
    /// text it has in the canonical view. A symlink into buck-out keeps its text (buck-out sits
    /// at the same depth in both views); one to a source is re-pointed at the source's real path,
    /// so it resolves outside the exec root too. `None` when the text needs no change.
    pub fn disk_symlink_text_rel(&self, link: &str, text: &str) -> Option<String> {
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
        let real_comps: Vec<&str> = real.as_str().split('/').filter(|c| !c.is_empty()).collect();
        let new = relativize(dir, &real_comps);
        if new == text { None } else { Some(new) }
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
    josh.disk_symlink_text_rel(rel, text)
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
}

pub fn verify_tag(outcome: VerifyOutcome, digest: &str, what: &str) -> String {
    let o = match outcome {
        VerifyOutcome::Ok => "josh:verify=ok",
        VerifyOutcome::FalseShare => "josh:JoshFalseShare",
        VerifyOutcome::LocalFailed => "josh:verify=local-failed",
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

    #[test]
    fn refuses_unknown_cell_and_bad_entry() {
        let e = entries(&[("cell.nope", "root:.")]);
        assert!(JoshMap::new(&e, &lab(), &never).is_err());
        let e = entries(&[("cell.mwb", "root")]);
        assert!(JoshMap::new(&e, &lab(), &never).is_err());
    }

    #[test]
    fn symlink_text_to_source_points_at_real_path() {
        let m = JoshMap::new(&lab_map(), &lab(), &never).unwrap();
        // A symlinked_dir entry in buck-out pointing at mwb's pkg/a.src.
        assert_eq!(
            m.disk_symlink_text_rel(
                "buck-out/josh-b/art/root/pkg/__s_dirs__/sd/a.txt",
                "../../../../../../../pkg/a.src"
            )
            .as_deref(),
            Some("../../../../../../../services/web-backend/pkg/a.src")
        );
        // Into buck-out: unchanged.
        assert_eq!(
            m.disk_symlink_text_rel(
                "buck-out/v2/art/root/pkg/__x__/l",
                "../../__y__/out"
            ),
            None
        );
        // A lab-only cell's source.
        assert_eq!(
            m.disk_symlink_text_rel("buck-out/v2/art/lab/l/__t__/s", "../../../../../../.josh/lab/l/f"),
            Some("../../../../../../l/f".to_owned())
        );
        // Absolute and out-of-project text: unchanged.
        assert_eq!(m.disk_symlink_text_rel("buck-out/v2/x", "/etc/passwd"), None);
        assert_eq!(m.disk_symlink_text_rel("buck-out/v2/x", "../../../up"), None);
    }

    #[test]
    fn identity_symlink_text_unchanged() {
        let m = JoshMap::new(&[], &cells(&[("root", ""), ("ftools", "ftools")]), &never).unwrap();
        assert_eq!(
            m.disk_symlink_text_rel("buck-out/v2/art/root/pkg/__t__/l", "../../../../../../pkg/a.src"),
            None
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

    #[test]
    fn relativize_paths() {
        assert_eq!(relativize(&["a", "b"], &["a", "c"]), "../c");
        assert_eq!(relativize(&[], &["a"]), "a");
        assert_eq!(relativize(&["a"], &["a"]), ".");
    }
}
