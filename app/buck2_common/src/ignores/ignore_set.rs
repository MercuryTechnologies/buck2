/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use std::sync::LazyLock;

use allocative::Allocative;
use buck2_core::cells::paths::CellRelativePath;
use buck2_error::internal_error;
use globset::Candidate;
use globset::GlobSetBuilder;
use pagable::PagableDeserialize;
use pagable::PagableDeserializer;
use pagable::PagableSerialize;
use pagable::PagableSerializer;
use regex::Regex;

static GLOB_CHARS: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"[*?{\[]").unwrap());

#[derive(Debug, Clone, Allocative)]
pub struct IgnoreSet {
    #[allocative(skip)]
    globset: globset::GlobSet,
    // The patterns without glob characters, each of which ignores a path and everything under
    // it, so that a directory one of them matches can be skipped whole.
    #[allocative(skip)]
    subtrees: globset::GlobSet,
    // Patterns that were added to the globset. Storing this seprately to support ser/de and
    // so that error messages can refer to the specific pattern that was matched.
    // This should be in the same order as the strings were added to the GlobSet to match the indices returned from it.
    patterns: Vec<String>,
}

impl PagableSerialize for IgnoreSet {
    fn pagable_serialize(&self, serializer: &mut dyn PagableSerializer) -> pagable::Result<()> {
        self.patterns.pagable_serialize(serializer)
    }
}

impl<'de> PagableDeserialize<'de> for IgnoreSet {
    fn pagable_deserialize<D: PagableDeserializer<'de> + ?Sized>(
        deserializer: &mut D,
    ) -> pagable::Result<Self> {
        let patterns = Vec::<String>::pagable_deserialize(deserializer)?;
        let globset = Self::build_globset(&patterns)
            .map_err(|e| pagable::Error::new(e).context("rebuilding IgnoreSet globset"))?;
        let subtrees = Self::build_subtrees(&patterns)
            .map_err(|e| pagable::Error::new(e).context("rebuilding IgnoreSet subtrees"))?;
        Ok(Self {
            globset,
            subtrees,
            patterns,
        })
    }
}

impl PartialEq for IgnoreSet {
    fn eq(&self, other: &Self) -> bool {
        // Only compare patterns because globset is derived from patterns.
        self.patterns == other.patterns
    }
}

impl Eq for IgnoreSet {}

impl IgnoreSet {
    /// Creates an IgnoreSet from an "ignore spec".
    ///
    /// This is modeled after buck1's parsing of project.ignores.
    ///
    /// An ignore spec is a comma-separated list of ignore patterns. If an ignore pattern
    /// contains a glob character, then it uses java.nio.file.FileSystem.getPathMatcher,
    /// otherwise it creates a com.facebook.buck.io.filesystem.RecursivePathMatcher
    ///
    /// Java's path matcher does not allow  '*' to cross directory boundaries. We get
    /// the RecursivePathMatcher behavior by identifying non-globby things and appending
    /// a '/**'.
    ///
    /// Always ignores `buck-out` if it is a `root_cell`.
    pub fn from_ignore_spec(spec: &str, root_cell: bool) -> buck2_error::Result<Self> {
        // TODO(cjhopman): There's opportunity to greatly improve the performance of IgnoreSet by
        // constructing special cases for a couple of common patterns we see in ignore specs. We
        // know that these can get large wins in some places where we've done this same ignore (watchman, buck1's ignores).
        // `**/filename`: a filename filter. These can all be merged into one hashset lookup.
        // `**/*.ext`: an extension filter. These can all be merged into one hashset lookup.
        // `**/*x*x*`: just some general glob on the filename alone, can merge these into one GlobSet that just needs to check against the filename.
        // `some/prefix/**`: a directory prefix. These can all be merged into one trie lookup.
        let mut patterns = Vec::new();
        let buck_out = if root_cell { Some("buck-out") } else { None };
        for val in buck_out.into_iter().chain(spec.split(',')) {
            let val = val.trim();
            if val.is_empty() {
                continue;
            }

            let val = val.trim_end_matches('/');
            patterns.push(val.to_owned());
        }

        let globset = Self::build_globset(&patterns).map_err(|e| internal_error!("{}", e))?;
        let subtrees = Self::build_subtrees(&patterns).map_err(|e| internal_error!("{}", e))?;

        Ok(Self {
            globset,
            subtrees,
            patterns,
        })
    }

    /// Build a `GlobSet` of the patterns without glob characters, which `build_globset` turns
    /// into `{name,name/**}` and so ignore everything under what they match.
    fn build_subtrees(patterns: &[String]) -> Result<globset::GlobSet, globset::Error> {
        let mut builder = GlobSetBuilder::new();
        for val in patterns {
            if !GLOB_CHARS.is_match(val) {
                builder.add(globset::Glob::new(&format!("{{{val},{val}/**}}"))?);
            }
        }
        builder.build()
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
                builder.add(globset::Glob::new(&format!("{{{val},{val}/**}}"))?);
            }
        }
        builder.build()
    }

    /// Returns a pattern that matches the candidate if there is one.
    pub(crate) fn matches_candidate(&self, candidate: &Candidate) -> Option<&str> {
        match self.globset.matches_candidate(candidate).as_slice() {
            [] => None,
            [v, ..] => Some(&self.patterns[*v]),
        }
    }

    /// Returns whether any pattern matches.
    pub fn is_match(&self, path: &CellRelativePath) -> bool {
        self.globset.is_match(path.as_str())
    }

    /// Whether everything under `path` is ignored as well as `path` itself. A glob such as
    /// `foo/*` matches the directory `foo/bar` but not `foo/bar/baz`, so only the patterns
    /// without glob characters answer yes.
    pub fn ignores_subtree(&self, path: &CellRelativePath) -> bool {
        self.subtrees.is_match(path.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ignore_set_defaults() {
        let set = IgnoreSet::from_ignore_spec("", true).unwrap();
        assert!(set.is_match(CellRelativePath::testing_new("buck-out/gen/src/file.txt")));
        assert!(set.is_match(CellRelativePath::testing_new("buck-out/art/src/file.txt")));
        assert!(!set.is_match(CellRelativePath::testing_new("src/file.txt")));
    }

    #[test]
    fn test_ignores_subtree_only_for_patterns_without_globs() {
        let set = IgnoreSet::from_ignore_spec(".jj, .claude/worktrees/, gen/*", true).unwrap();
        assert!(set.ignores_subtree(CellRelativePath::testing_new("buck-out")));
        assert!(set.ignores_subtree(CellRelativePath::testing_new(".jj")));
        assert!(set.ignores_subtree(CellRelativePath::testing_new(".claude/worktrees")));
        assert!(!set.ignores_subtree(CellRelativePath::testing_new(".claude")));
        // `gen/*` ignores `gen/x` but not `gen/x/y`, so `gen/x` is walked.
        assert!(set.is_match(CellRelativePath::testing_new("gen/x")));
        assert!(!set.is_match(CellRelativePath::testing_new("gen/x/y")));
        assert!(!set.ignores_subtree(CellRelativePath::testing_new("gen/x")));
    }
}
