/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use std::sync::Arc;
use std::sync::LazyLock;

use allocative::Allocative;
use buck2_core::cells::CellResolver;
use buck2_core::cells::cell_root_path::CellRootPathBuf;
use buck2_core::cells::name::CellName;
use buck2_core::cells::paths::CellRelativePath;
use buck2_core::fs::project_rel_path::ProjectRelativePath;
use buck2_core::fs::project_rel_path::ProjectRelativePathBuf;
use buck2_error::ErrorTag;
use buck2_error::buck2_error;
use buck2_error::internal_error;
use buck2_hash::BuckMutMap;
use dice::DiceData;
use dice::DiceDataBuilder;
use dupe::Dupe;
use globset::GlobSetBuilder;
use pagable::Pagable;
use pagable::PagableDeserialize;
use pagable::PagableDeserializer;
use pagable::PagableSerialize;
use pagable::PagableSerializer;
use pagable::PagableUnsupported;
use regex::Regex;

use crate::legacy_configs::configs::LegacyBuckConfig;
use crate::legacy_configs::key::BuckconfigKeyRef;

static GLOB_CHARS: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"[*?{\[]").unwrap());

pub fn compute_ignore_set(
    cells: CellResolver,
    configs: &BuckMutMap<CellName, LegacyBuckConfig>,
) -> buck2_error::Result<IgnoreSet> {
    let by_cell = configs
        .iter()
        .map(|(cell_name, config)| {
            buck2_error::Ok((
                *cell_name,
                PerCellIgnoreSet::from_config(config, cells.is_root_cell(*cell_name))?,
            ))
        })
        .collect::<buck2_error::Result<_>>()?;

    Ok(IgnoreSet { by_cell, cells })
}

/// The `project.ignore` patterns of every cell.
///
/// The set answers for a path taken by itself and says nothing about the directories above it;
/// the callers take a path under an ignored directory to be ignored, whatever the set says about
/// the path.
#[derive(Debug, Allocative, PagableUnsupported, PartialEq, Eq)]
pub struct IgnoreSet {
    by_cell: BuckMutMap<CellName, PerCellIgnoreSet>,
    cells: CellResolver,
}

impl IgnoreSet {
    /// The pattern that ignores the path, if one does and no `!` pattern un-ignores it.
    pub(crate) fn matches_candidate<'a>(&'a self, path: &ProjectRelativePath) -> Option<&'a str> {
        let (cell, path) = self.cells.split_cell_path(path);
        self.by_cell
            .get(&cell)
            .and_then(|ignore_set| ignore_set.matches_candidate(path))
    }

    /// Whether a pattern ignores the path and no `!` pattern un-ignores it.
    pub fn is_match(&self, path: &ProjectRelativePath) -> bool {
        let (cell, path) = self.cells.split_cell_path(path);
        self.by_cell
            .get(&cell)
            .is_some_and(|ignore_set| ignore_set.is_match(path))
    }

    /// Whether the path and everything under it are ignored, so that a walk of the tree can skip
    /// the path whole: a pattern of its cell that covers whole subtrees matches it, no `!` pattern
    /// of that cell can match it or anything under it, and no other cell's root is under it,
    /// since that cell's own patterns decide what under its root is ignored.
    ///
    /// The answer is conservative. A no means only that the path is to be walked.
    pub fn ignores_subtree(&self, path: &ProjectRelativePath) -> bool {
        let (cell, cell_path) = self.cells.split_cell_path(path);
        self.by_cell
            .get(&cell)
            .is_some_and(|ignore_set| ignore_set.ignores_subtree(cell_path))
            && !self.cells.cells().any(|(_, instance)| {
                let cell_root = instance.path().as_project_relative_path();
                cell_root != path && cell_root.starts_with(path)
            })
    }

    /// Whether the `project.ignore` of `config` is the one this set was built from for `cell`.
    pub fn built_from(
        &self,
        cell: CellName,
        config: &LegacyBuckConfig,
        root_cell: bool,
    ) -> buck2_error::Result<bool> {
        Ok(self.by_cell.get(&cell) == Some(&PerCellIgnoreSet::from_config(config, root_cell)?))
    }

    /// Ignores nothing (beyond `buck-out`), whatever the cells are.
    pub fn testing_empty() -> Self {
        let root = CellName::testing_new("root");
        let cells = CellResolver::testing_with_name_and_path(
            root,
            CellRootPathBuf::new(ProjectRelativePathBuf::unchecked_new(String::new())),
        );
        Self::testing_new_single_cell(cells, root, "").unwrap()
    }

    /// The set with `specs` as the `project.ignore` of each cell named there.
    pub fn testing_new(
        cells: CellResolver,
        specs: &[(CellName, &str)],
    ) -> buck2_error::Result<Self> {
        let mut by_cell = BuckMutMap::default();
        for (cell, spec) in specs {
            by_cell.insert(
                *cell,
                PerCellIgnoreSet::from_ignore_spec(spec, cells.is_root_cell(*cell))?,
            );
        }
        Ok(Self { by_cell, cells })
    }

    pub fn testing_new_single_cell(
        cells: CellResolver,
        cell: CellName,
        spec: &str,
    ) -> buck2_error::Result<Self> {
        let root_cell = cells.is_root_cell(cell);
        let mut by_cell = BuckMutMap::default();
        by_cell.insert(cell, PerCellIgnoreSet::from_ignore_spec(spec, root_cell)?);
        Ok(Self { by_cell, cells })
    }
}

pub trait HasIgnoreSet {
    fn get_ignore_set(&self) -> Arc<IgnoreSet>;
}

pub trait SetIgnoreSet {
    fn set_ignore_set(&mut self, ignore_set: Arc<IgnoreSet>);
}

impl HasIgnoreSet for DiceData {
    fn get_ignore_set(&self) -> Arc<IgnoreSet> {
        self.get::<Arc<IgnoreSet>>()
            .expect("ignore set should be set")
            .dupe()
    }
}

impl SetIgnoreSet for DiceDataBuilder {
    fn set_ignore_set(&mut self, ignore_set: Arc<IgnoreSet>) {
        self.set(ignore_set)
    }
}

impl SetIgnoreSet for DiceData {
    fn set_ignore_set(&mut self, ignore_set: Arc<IgnoreSet>) {
        self.set(ignore_set)
    }
}

/// One cell's patterns as written, less the `!` of the negated ones.
#[derive(Debug, Clone, Default, PartialEq, Eq, Allocative, Pagable)]
struct IgnorePatterns {
    ignored: Vec<String>,
    unignored: Vec<String>,
}

#[derive(Debug, Clone, Allocative)]
struct PerCellIgnoreSet {
    /// Indexed like `patterns.ignored`, which is how a match is reported as its pattern.
    #[allocative(skip)]
    ignored: globset::GlobSet,
    #[allocative(skip)]
    unignored: globset::GlobSet,
    /// The `patterns.ignored` that ignore everything under what they match as well: those without
    /// glob characters, and those that end in `/**`.
    #[allocative(skip)]
    subtrees: globset::GlobSet,
    /// For each of `patterns.unignored`, its whole path components before the first one with a
    /// glob character or an escape. Everything the pattern matches is at or under it.
    unignored_prefixes: Vec<String>,
    patterns: IgnorePatterns,
}

impl PagableSerialize for PerCellIgnoreSet {
    fn pagable_serialize(&self, serializer: &mut dyn PagableSerializer) -> pagable::Result<()> {
        self.patterns.pagable_serialize(serializer)
    }
}

impl<'de> PagableDeserialize<'de> for PerCellIgnoreSet {
    fn pagable_deserialize<D: PagableDeserializer<'de> + ?Sized>(
        deserializer: &mut D,
    ) -> pagable::Result<Self> {
        let patterns = IgnorePatterns::pagable_deserialize(deserializer)?;
        Self::from_patterns(patterns)
            .map_err(|e| pagable::Error::new(e).context("rebuilding IgnoreSet globset"))
    }
}

impl PartialEq for PerCellIgnoreSet {
    fn eq(&self, other: &Self) -> bool {
        // The globsets are derived from the patterns.
        self.patterns == other.patterns
    }
}

impl Eq for PerCellIgnoreSet {}

impl PerCellIgnoreSet {
    fn from_config(config: &LegacyBuckConfig, root_cell: bool) -> buck2_error::Result<Self> {
        let spec = config
            .get(BuckconfigKeyRef {
                section: "project",
                property: "ignore",
            })
            .unwrap_or_default();
        Self::from_ignore_spec(spec, root_cell)
    }

    /// Creates an IgnoreSet from an "ignore spec": a comma-separated list of patterns relative
    /// to the cell root, in the syntax buck1 used.
    ///
    /// A pattern without glob characters names a directory and covers everything under it. One
    /// with glob characters is matched like a `java.nio.file` glob, in which `*` does not cross
    /// `/`. A pattern starting with `!` un-ignores what it matches. It wins wherever in the list
    /// it appears, rather than by gitignore's last-match-wins: the list is spliced together from
    /// several config files, and its order is nothing to lean on.
    ///
    /// Always ignores `buck-out` if it is a `root_cell`.
    fn from_ignore_spec(spec: &str, root_cell: bool) -> buck2_error::Result<Self> {
        // TODO(cjhopman): There's opportunity to greatly improve the performance of IgnoreSet by
        // constructing special cases for a couple of common patterns we see in ignore specs. We
        // know that these can get large wins in some places where we've done this same ignore (watchman, buck1's ignores).
        // `**/filename`: a filename filter. These can all be merged into one hashset lookup.
        // `**/*.ext`: an extension filter. These can all be merged into one hashset lookup.
        // `**/*x*x*`: just some general glob on the filename alone, can merge these into one GlobSet that just needs to check against the filename.
        // `some/prefix/**`: a directory prefix. These can all be merged into one trie lookup.
        let mut patterns = IgnorePatterns::default();
        let buck_out = if root_cell { Some("buck-out") } else { None };
        for val in buck_out.into_iter().chain(spec.split(',')) {
            let val = val.trim();
            if val.is_empty() {
                continue;
            }

            let val = val.trim_end_matches('/');
            match val.strip_prefix('!') {
                Some(unignored) => {
                    let unignored = unignored.trim_start();
                    if unignored.is_empty() {
                        return Err(buck2_error!(
                            ErrorTag::Input,
                            "`project.ignore` has a `!` entry that names no path"
                        ));
                    }
                    patterns.unignored.push(unignored.to_owned());
                }
                None => patterns.ignored.push(val.to_owned()),
            }
        }

        Self::from_patterns(patterns).map_err(|e| internal_error!("{}", e))
    }

    fn from_patterns(patterns: IgnorePatterns) -> Result<Self, globset::Error> {
        Ok(Self {
            ignored: Self::build_globset(&patterns.ignored)?,
            unignored: Self::build_globset(&patterns.unignored)?,
            subtrees: Self::build_subtrees(&patterns.ignored)?,
            unignored_prefixes: patterns
                .unignored
                .iter()
                .map(|pattern| Self::literal_prefix(pattern).to_owned())
                .collect(),
            patterns,
        })
    }

    /// Build a `GlobSet` from the given patterns.
    ///
    /// Glob-containing patterns use `literal_separator(true)`, while plain
    /// directory names are turned into `{name,name/**}` matchers.
    fn build_globset(patterns: &[String]) -> Result<globset::GlobSet, globset::Error> {
        let mut builder = GlobSetBuilder::new();
        for val in patterns {
            if GLOB_CHARS.is_match(val) {
                builder.add(
                    globset::GlobBuilder::new(val)
                        .literal_separator(true)
                        .build()?,
                );
            } else {
                // Escape so that a `}` or `]` in the name does not break the alternation.
                let val = globset::escape(val);
                builder.add(globset::Glob::new(&format!("{{{val},{val}/**}}"))?);
            }
        }
        builder.build()
    }

    /// Build a `GlobSet` of the patterns that match everything under a path they match. A plain
    /// directory name becomes `{name,name/**}` as in `build_globset`, and a glob that ends in `/**`
    /// matches any path under one it matches. Any other glob may match a directory and not what
    /// is in it: `gen/*` matches `gen/x` but not `gen/x/y`.
    fn build_subtrees(patterns: &[String]) -> Result<globset::GlobSet, globset::Error> {
        let mut builder = GlobSetBuilder::new();
        for val in patterns {
            if !GLOB_CHARS.is_match(val) {
                let val = globset::escape(val);
                builder.add(globset::Glob::new(&format!("{{{val},{val}/**}}"))?);
            } else if val == "**" || val.ends_with("/**") {
                builder.add(
                    globset::GlobBuilder::new(val)
                        .literal_separator(true)
                        .build()?,
                );
            }
        }
        builder.build()
    }

    /// The whole path components of `pattern` before the first that a glob could match other than
    /// literally, so that every path the pattern matches is this prefix or under it. A plain
    /// pattern is matched literally, so it is its own prefix.
    fn literal_prefix(pattern: &str) -> &str {
        if !GLOB_CHARS.is_match(pattern) {
            return pattern;
        }
        let mut end = None;
        for component in pattern.split('/') {
            if component.contains(['*', '?', '{', '}', '[', ']', '\\']) {
                break;
            }
            end = Some(end.map_or(component.len(), |end| end + 1 + component.len()));
        }
        &pattern[..end.unwrap_or(0)]
    }

    /// Whether the path and everything under it are ignored, and no `!` pattern can reach any
    /// of them.
    fn ignores_subtree(&self, path: &CellRelativePath) -> bool {
        let path = path.as_str();
        // Whether a `!` pattern whose matches are all at or under `prefix` could match `path` or
        // something under it: it could unless the two diverge at some component.
        let reaches = |prefix: &str| {
            prefix.is_empty()
                || path.is_empty()
                || prefix == path
                || path
                    .strip_prefix(prefix)
                    .is_some_and(|rest| rest.starts_with('/'))
                || prefix
                    .strip_prefix(path)
                    .is_some_and(|rest| rest.starts_with('/'))
        };
        self.subtrees.is_match(path) && !self.unignored_prefixes.iter().any(|p| reaches(p))
    }

    /// The pattern that ignores the candidate, if one does and no `!` pattern un-ignores it.
    pub(crate) fn matches_candidate(&self, candidate: &CellRelativePath) -> Option<&str> {
        let candidate = globset::Candidate::new(candidate.as_str());
        match self.ignored.matches_candidate(&candidate).first() {
            Some(v) if !self.unignored.is_match_candidate(&candidate) => {
                Some(&self.patterns.ignored[*v])
            }
            _ => None,
        }
    }

    /// Whether a pattern ignores the path and no `!` pattern un-ignores it.
    fn is_match(&self, path: &CellRelativePath) -> bool {
        let candidate = globset::Candidate::new(path.as_str());
        self.ignored.is_match_candidate(&candidate)
            && !self.unignored.is_match_candidate(&candidate)
    }
}

#[cfg(test)]
mod tests {
    use buck2_core::cells::cell_root_path::CellRootPathBuf;

    use super::*;

    fn set(spec: &str) -> buck2_error::Result<IgnoreSet> {
        let root = CellName::testing_new("root");
        let cells =
            CellResolver::testing_with_name_and_path(root, CellRootPathBuf::testing_new(""));
        IgnoreSet::testing_new_single_cell(cells, root, spec)
    }

    fn p(s: &str) -> &ProjectRelativePath {
        ProjectRelativePath::new(s).unwrap()
    }

    #[test]
    fn test_root_cell_defaults_include_buck_out() {
        let set = set("").unwrap();
        assert!(set.is_match(p("buck-out/gen/src/file.txt")));
        assert!(set.is_match(p("buck-out/art/src/file.txt")));
        assert!(!set.is_match(p("src/file.txt")));
    }

    #[test]
    fn test_non_root_cell_prefixes_patterns() {
        let set = set("recursive, some/*").unwrap();
        assert!(set.is_match(p("recursive")));
        assert!(set.is_match(p("recursive/sub")));
        assert!(set.is_match(p("some/x")));
        assert!(!set.is_match(p("x/recursive")));
        assert!(!set.is_match(p("foo/bar/some/x/y")));
    }

    /// A `!` pattern un-ignores exactly what it matches, before or after the pattern it
    /// counters, and like any other pattern covers a directory it names in full.
    #[test]
    fn test_negated_patterns() {
        let set = set("!kept.gen, **/*.gen, **/*.gen/**, !shown/*.gen/keep").unwrap();
        assert_eq!(set.matches_candidate(p("a/x.gen")), Some("**/*.gen"));
        assert!(set.is_match(p("a/x.gen/f")));
        assert!(!set.is_match(p("kept.gen")));
        assert!(!set.is_match(p("kept.gen/sub/f")));
        assert_eq!(set.matches_candidate(p("kept.gen/sub/f")), None);
        assert!(set.is_match(p("shown/x.gen")));
        assert!(!set.is_match(p("shown/x.gen/keep")));
        assert!(set.is_match(p("shown/x.gen/other")));
        assert!(!set.is_match(p("shown/keep")));
    }

    #[test]
    fn test_negated_pattern_must_name_a_path() {
        assert!(set("a, !").is_err());
        assert!(set("a, !/").is_err());
        assert!(set("a, ! b/").is_ok());
    }

    /// A plain pattern is escaped before it is wrapped in `{name,name/**}`, so glob
    /// metacharacters in it are matched literally.
    #[test]
    fn test_closing_brace_in_plain_pattern_is_literal() {
        let root = CellName::testing_new("root");
        let cells =
            CellResolver::testing_with_name_and_path(root, CellRootPathBuf::testing_new(""));
        let set = IgnoreSet::testing_new_single_cell(cells, root, "a}").unwrap();
        assert!(set.is_match(ProjectRelativePath::new("a}").unwrap()));
        assert!(set.is_match(ProjectRelativePath::new("a}/x").unwrap()));
        assert!(!set.is_match(ProjectRelativePath::new("a").unwrap()));
    }

    /// Only a pattern that ignores everything under what it matches lets a walk skip a
    /// directory: a plain name or a glob ending in `/**`, and not `gen/*`, which matches `gen/x`
    /// and not `gen/x/y`.
    #[test]
    fn test_ignores_subtree_only_for_patterns_that_cover_subtrees() {
        let set = set(".jj, .claude/worktrees/, gen/*, out/*/**").unwrap();
        assert!(set.ignores_subtree(p("buck-out")));
        assert!(set.ignores_subtree(p(".jj")));
        assert!(set.ignores_subtree(p(".jj/store")));
        assert!(set.ignores_subtree(p(".claude/worktrees")));
        assert!(!set.ignores_subtree(p(".claude")));
        assert!(!set.ignores_subtree(p("src")));
        assert!(set.is_match(p("gen/x")));
        assert!(!set.is_match(p("gen/x/y")));
        assert!(!set.ignores_subtree(p("gen/x")));
        assert!(set.ignores_subtree(p("out/x/y")));
        assert!(set.is_match(p("out/x/y/z")));
        assert!(!set.ignores_subtree(p("out")));
        // Every path a skipped directory holds is one the per-path filter ignores.
        for path in [".jj/a/b", ".claude/worktrees/w/f", "out/x/y/z/f"] {
            assert!(set.is_match(p(path)), "{path}");
        }
    }

    /// A directory is walked when a `!` pattern could match it, a directory above it, or
    /// anything under it.
    #[test]
    fn test_ignores_subtree_respects_negations() {
        let set = set("third-party, vendor, cache, node_modules, \
             !third-party/kept, !vendor/*/keep, !cache/a, !node_modules/x[yz]/f")
        .unwrap();
        // `!third-party/kept` is under `third-party`, so neither is skipped; a sibling is.
        assert!(!set.ignores_subtree(p("third-party")));
        assert!(!set.ignores_subtree(p("third-party/kept")));
        assert!(!set.ignores_subtree(p("third-party/kept/sub")));
        assert!(set.ignores_subtree(p("third-party/other")));
        assert!(set.is_match(p("third-party/other/f")));
        assert!(!set.is_match(p("third-party/kept/f")));
        // `!vendor/*/keep` can reach anything under `vendor`.
        assert!(!set.ignores_subtree(p("vendor")));
        assert!(!set.ignores_subtree(p("vendor/a")));
        assert!(!set.ignores_subtree(p("vendor/a/b")));
        assert!(!set.is_match(p("vendor/a/keep")));
        // `!cache/a` un-ignores `cache/a`, which a skip of `cache` would miss.
        assert!(!set.ignores_subtree(p("cache")));
        assert!(!set.ignores_subtree(p("cache/a/b")));
        assert!(set.ignores_subtree(p("cache/ab")));
        // The literal part of `!node_modules/x[yz]/f` is `node_modules`.
        assert!(!set.ignores_subtree(p("node_modules")));
        assert!(!set.ignores_subtree(p("node_modules/xy")));
        assert!(!set.is_match(p("node_modules/xy/f")));

        // `!**/BUCK` can match anywhere, so nothing is skipped.
        let set_anywhere = super::tests::set("vendor, !**/BUCK").unwrap();
        assert!(!set_anywhere.ignores_subtree(p("vendor")));
        assert!(!set_anywhere.ignores_subtree(p("vendor/a")));
        assert!(!set_anywhere.is_match(p("vendor/a/BUCK")));
        // `buck-out` too: a `!` pattern can reach under it like under any other directory.
        assert!(!set_anywhere.ignores_subtree(p("buck-out")));
    }

    /// An escaped character in a `!` glob ends its literal prefix, since the glob matches the
    /// character and not the backslash.
    #[test]
    fn test_ignores_subtree_escaped_negation() {
        let set = set("foo, !fo\\o/x*").unwrap();
        assert!(!set.is_match(p("foo/xa")));
        assert!(!set.ignores_subtree(p("foo")));
    }

    /// A directory that holds another cell's root is walked, whatever the outer cell's patterns
    /// say, because the inner cell's own patterns decide what under its root is ignored. Each
    /// cell's patterns and negations are its own.
    #[test]
    fn test_ignores_subtree_by_cell() {
        let root = CellName::testing_new("root");
        let inner = CellName::testing_new("inner");
        let other = CellName::testing_new("other");
        let cells = CellResolver::testing_with_names_and_paths(&[
            (root, CellRootPathBuf::testing_new("")),
            (inner, CellRootPathBuf::testing_new("ignored/inner")),
            (other, CellRootPathBuf::testing_new("other")),
        ]);
        let set = IgnoreSet::testing_new(
            cells,
            &[
                (root, "ignored, skipped, other, gen, !skipped/kept"),
                (inner, "gen, !gen/kept"),
                (other, "vendor, gen, skipped, !gen/kept"),
            ],
        )
        .unwrap();
        // `ignored` holds the cell `inner`, so it is walked, and only its other entries skipped.
        assert!(!set.ignores_subtree(p("ignored")));
        assert!(set.ignores_subtree(p("ignored/sub")));
        assert!(!set.ignores_subtree(p("ignored/inner")));
        // Under `inner`, `inner`'s patterns: `ignored` does not cover them.
        assert!(!set.ignores_subtree(p("ignored/inner/src")));
        assert!(!set.ignores_subtree(p("ignored/inner/gen")));
        assert!(set.ignores_subtree(p("ignored/inner/gen/other")));
        // The root cell's `other` names the cell `other`'s root, which belongs to that cell.
        assert!(!set.ignores_subtree(p("other")));
        assert!(set.ignores_subtree(p("other/vendor")));
        // Each cell's `!` patterns reach only under that cell's root: `other`'s `!gen/kept`
        // keeps `other/gen` walked and not the root cell's `gen`, and the root cell's
        // `!skipped/kept` keeps `skipped` walked and not `other/skipped`.
        assert!(set.ignores_subtree(p("gen")));
        assert!(!set.ignores_subtree(p("other/gen")));
        assert!(!set.ignores_subtree(p("skipped")));
        assert!(set.ignores_subtree(p("skipped/other")));
        assert!(set.ignores_subtree(p("other/skipped")));
    }
}
