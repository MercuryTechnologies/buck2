/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! A project's channels and profiles, read from a TOML file in the project. The library knows
//! nothing about any one project: which binaries exist, which configs, what needs a key.

use std::collections::BTreeMap;
use std::path::Path;
use std::path::PathBuf;

use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    /// The profile in force when nothing is selected.
    pub default: String,
    /// Where the selection is remembered, relative to the project root.
    #[serde(default = "default_state_file")]
    pub state_file: PathBuf,
    /// Where a profile's config file is copied, relative to the project root.
    #[serde(default = "default_config_dest")]
    pub config_dest: PathBuf,
    /// The prefix of every line printed.
    #[serde(default = "default_name")]
    pub name: String,
    #[serde(default)]
    pub credentials: Option<Credentials>,
    pub channels: BTreeMap<String, Channel>,
    pub profiles: BTreeMap<String, Profile>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Channel {
    /// The program that is this channel's buck2, relative to the project root: a dotslash file or
    /// any executable.
    pub exec: PathBuf,
    /// What the channel is built from, for the output: a branch, a release.
    pub from: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Profile {
    pub channel: String,
    /// The config file copied into `config_dest`, relative to the project root. None writes
    /// nothing and removes what a previous profile wrote.
    #[serde(default)]
    pub config: Option<PathBuf>,
    /// One line on what the profile does, for the output.
    pub summary: String,
    /// Whether the profile needs the credentials file.
    #[serde(default)]
    pub needs_key: bool,
    /// Operating systems (`std::env::consts::OS`) on which the profile warns.
    #[serde(default)]
    pub warn_on_os: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Credentials {
    /// The credentials file, `~` for the home directory. Only whether it exists is read.
    pub file: String,
    /// The BuildBuddy web app whose `/cli-login` page mints a personal key, for `--login`.
    #[serde(default)]
    pub login_url: Option<String>,
    /// The BuildBuddy org (its URL slug) to sign in to.
    #[serde(default)]
    pub org: Option<String>,
    /// What `--login` writes, with `{key}` for the key.
    #[serde(default = "default_template")]
    pub template: String,
}

fn default_template() -> String {
    "{key}\n".to_owned()
}

fn default_state_file() -> PathBuf {
    PathBuf::from(".buck2-profile")
}

fn default_config_dest() -> PathBuf {
    PathBuf::from(".buckconfig.d/50-local-profile")
}

fn default_name() -> String {
    "buck2".to_owned()
}

impl Manifest {
    pub fn parse(text: &str) -> Result<Self, String> {
        let manifest: Manifest = toml::from_str(text).map_err(|e| e.to_string())?;
        manifest.check()?;
        Ok(manifest)
    }

    pub fn load(path: &Path) -> Result<Self, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("cannot read {}: {e}", path.display()))?;
        Self::parse(&text).map_err(|e| format!("{}: {e}", path.display()))
    }

    fn check(&self) -> Result<(), String> {
        if !self.profiles.contains_key(&self.default) {
            return Err(format!("default profile `{}` is not defined", self.default));
        }
        for (name, profile) in &self.profiles {
            if crate::args::is_buck2_subcommand(name) {
                return Err(format!("profile `{name}` is named like a buck2 subcommand"));
            }
            if !self.channels.contains_key(&profile.channel) {
                return Err(format!(
                    "profile `{name}` names channel `{}`, which is not defined",
                    profile.channel
                ));
            }
            if profile.needs_key && self.credentials.is_none() {
                return Err(format!(
                    "profile `{name}` needs a key, but no credentials file is set"
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
pub(crate) const EXAMPLE: &str = r#"
default = "current"

[credentials]
file = "~/.buckconfig.d/mercury-credentials"
login_url = "https://hub.example"
org = "mercury"
template = "[credentials]\nbuildbuddy_api_key = {key}\n"

[channels.mercury-head]
exec = "nix/bin/buck2-current"
from = "mercury-head"

[channels.mercury-head-next]
exec = "nix/bin/buck2-next"
from = "mercury-head-next"

[profiles.current]
channel = "mercury-head"
summary = "builds local, cache read-only"

[profiles.next]
channel = "mercury-head-next"
config = "tools/buck2/buckconfig.d/next"
summary = "tests on hub RE"
needs_key = true

[profiles.next.warn_on_os]
macos = "shell tests can run on hub; specs cannot (their binary is built here)"
"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_example_parses() {
        let m = Manifest::parse(EXAMPLE).unwrap();
        assert_eq!(m.default, "current");
        assert_eq!(m.state_file, PathBuf::from(".buck2-profile"));
        assert!(m.profiles["next"].needs_key);
        assert!(m.profiles["current"].config.is_none());
    }

    #[test]
    fn a_profile_named_like_a_subcommand_is_refused() {
        let bad = EXAMPLE
            .replace("[profiles.next]", "[profiles.build]")
            .replace("[profiles.next.warn_on_os]", "[profiles.build.warn_on_os]");
        assert!(Manifest::parse(&bad).unwrap_err().contains("subcommand"));
    }

    #[test]
    fn an_undefined_channel_or_default_is_refused() {
        assert!(
            Manifest::parse(&EXAMPLE.replace("default = \"current\"", "default = \"x\"")).is_err()
        );
        assert!(
            Manifest::parse(
                &EXAMPLE.replace("channel = \"mercury-head-next\"", "channel = \"nope\"")
            )
            .is_err()
        );
    }
}
