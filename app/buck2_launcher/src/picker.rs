/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! What the picker does with a project: which profile and channel are in force, switching them,
//! and the lines it prints. Nothing here prints or runs buck2 by itself except `restart_daemon`;
//! the caller prints the lines it is given.

use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use std::sync::LazyLock;

use regex::Regex;

use crate::manifest::Manifest;

/// The first line of every config file the picker writes, so that it never overwrites a file a
/// person wrote at the same path.
const HEADER: &str = "# written by buck2 --profile:";

/// What is remembered for a worktree: the selection, and the binary and config the daemon was
/// last started under.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct State {
    pub profile: Option<String>,
    pub channel: Option<String>,
    /// The program that last ran, and the config it ran with, to tell whether a daemon needs a
    /// restart.
    pub ran: Option<String>,
}

impl State {
    pub fn read(path: &Path) -> State {
        let Ok(text) = std::fs::read_to_string(path) else {
            return State::default();
        };
        let mut state = State::default();
        for line in text.lines() {
            match line.split_once('=') {
                Some(("profile", v)) => state.profile = Some(v.trim().to_owned()),
                Some(("channel", v)) => state.channel = Some(v.trim().to_owned()),
                Some(("ran", v)) => state.ran = Some(v.trim().to_owned()),
                _ => {}
            }
        }
        state
    }

    pub fn write(&self, path: &Path) -> std::io::Result<()> {
        if *self == State::default() {
            return match std::fs::remove_file(path) {
                Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e),
                _ => Ok(()),
            };
        }
        let mut text = String::from("# buck2 --profile state for this worktree; not checked in\n");
        for (key, value) in [
            ("profile", &self.profile),
            ("channel", &self.channel),
            ("ran", &self.ran),
        ] {
            if let Some(value) = value {
                text.push_str(&format!("{key}={value}\n"));
            }
        }
        std::fs::write(path, text)
    }
}

/// The profile and channel in force, resolved against the manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resolved {
    pub profile: String,
    /// The channel name, or a commit given with `--channel`.
    pub channel: String,
    pub overridden: bool,
    /// The program to run, absolute.
    pub exec: PathBuf,
    /// Where the channel comes from, for the output.
    pub from: String,
    /// The buck2 commit the program names, when it can be read from it.
    pub rev: Option<String>,
}

pub struct Picker<'a> {
    pub root: &'a Path,
    pub manifest: &'a Manifest,
    pub home: Option<PathBuf>,
}

impl<'a> Picker<'a> {
    pub fn state_path(&self) -> PathBuf {
        self.root.join(&self.manifest.state_file)
    }

    pub fn config_dest(&self) -> PathBuf {
        self.root.join(&self.manifest.config_dest)
    }

    pub fn state(&self) -> State {
        State::read(&self.state_path())
    }

    pub fn resolve(
        &self,
        profile: Option<&str>,
        channel: Option<&str>,
    ) -> Result<Resolved, String> {
        let state = self.state();
        let profile_name = profile
            .map(str::to_owned)
            .or(state.profile.clone())
            .unwrap_or_else(|| self.manifest.default.clone());
        let profile = self.manifest.profiles.get(&profile_name).ok_or_else(|| {
            format!(
                "no profile `{profile_name}`; profiles: {}",
                self.manifest
                    .profiles
                    .keys()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        })?;
        // A stored channel override belongs to the stored profile only.
        let channel_override = match (channel, profile) {
            (Some(c), _) => Some(c.to_owned()),
            (None, _) if Some(&profile_name) == state.profile.as_ref() => state.channel.clone(),
            _ => None,
        };
        let channel_name = channel_override
            .clone()
            .unwrap_or_else(|| profile.channel.clone());
        let channel = self.manifest.channels.get(&channel_name).ok_or_else(|| {
            format!(
                "channel {channel_name} is not one of this project's channels ({}); a channel \
                 built from another commit is not supported yet",
                self.manifest
                    .channels
                    .keys()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        })?;
        let exec = self.root.join(&channel.exec);
        Ok(Resolved {
            overridden: channel_override.is_some() && channel_name != profile.channel,
            rev: rev_of(&exec),
            profile: profile_name,
            channel: channel_name,
            from: channel.from.clone(),
            exec,
        })
    }

    fn key_file(&self) -> Option<PathBuf> {
        let file = &self.manifest.credentials.as_ref()?.file;
        match file.strip_prefix("~/") {
            Some(rest) => self.home.as_ref().map(|home| home.join(rest)),
            None => Some(PathBuf::from(file)),
        }
    }

    pub fn key_present(&self) -> bool {
        self.key_file().is_some_and(|f| f.is_file())
    }

    /// What `ran` records for a resolved profile: the program, and the config file's contents'
    /// identity, so that a daemon started under either is restarted when it changes.
    fn ran_marker(&self, resolved: &Resolved) -> String {
        let config = self.manifest.profiles[&resolved.profile]
            .config
            .as_ref()
            .map_or("none".to_owned(), |c| c.display().to_string());
        format!("{}|{}", resolved.exec.display(), config)
    }

    /// Switches the worktree to `profile` (and `channel`, if given), writing or removing the
    /// config file. Returns the lines to print. Refuses when the profile needs a key the machine
    /// does not have; the caller offers a login first.
    pub fn switch(&self, profile: &str, channel: Option<&str>) -> Result<Vec<String>, String> {
        let before = self.resolve(None, None)?;
        let after = self.resolve(Some(profile), channel)?;
        let p = &self.manifest.profiles[&after.profile];
        let name = &self.manifest.name;
        let mut lines = Vec::new();
        if let Some(warning) = p.warn_on_os.get(std::env::consts::OS) {
            lines.push(format!(
                "{name}: warning: {} on {}: {warning}",
                after.profile,
                std::env::consts::OS
            ));
        }
        if p.needs_key && !self.key_present() {
            return Err(format!(
                "{name}: profile {} needs a key; none on this machine. Nothing changed.",
                after.profile
            ));
        }
        let dest = self.config_dest();
        let config_line = match &p.config {
            Some(source) => {
                let text = std::fs::read_to_string(self.root.join(source))
                    .map_err(|e| format!("cannot read {}: {e}", source.display()))?;
                if dest.exists() && !written_by_us(&dest) {
                    return Err(format!(
                        "{name}: {} exists and was not written by buck2 --profile; move it \
                         aside first. Nothing changed.",
                        self.manifest.config_dest.display()
                    ));
                }
                if let Some(parent) = dest.parent() {
                    std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
                }
                std::fs::write(
                    &dest,
                    format!(
                        "{HEADER} profile {} from {}\n{text}",
                        after.profile,
                        source.display()
                    ),
                )
                .map_err(|e| e.to_string())?;
                format!(
                    "  config  wrote {}\n            from {}\n          {}",
                    self.manifest.config_dest.display(),
                    source.display(),
                    p.summary
                )
            }
            None => {
                if dest.exists() {
                    if !written_by_us(&dest) {
                        return Err(format!(
                            "{name}: {} was not written by buck2 --profile; left as it is",
                            self.manifest.config_dest.display()
                        ));
                    }
                    std::fs::remove_file(&dest).map_err(|e| e.to_string())?;
                    format!("  config  removed {}", self.manifest.config_dest.display())
                } else {
                    format!("  config  none written; {}", p.summary)
                }
            }
        };
        let state = State {
            profile: (after.profile != self.manifest.default || channel.is_some())
                .then(|| after.profile.clone()),
            channel: after.overridden.then(|| after.channel.clone()),
            ran: self.state().ran,
        };
        state.write(&self.state_path()).map_err(|e| e.to_string())?;

        let star = if after.overridden { "*" } else { "" };
        lines.push(format!(
            "{name}: profile {}{star} (was {})",
            after.profile, before.profile
        ));
        lines.push(format!("  binary  {}", describe_binary(&after)));
        lines.push(config_line);
        if self.manifest.credentials.is_some() {
            lines.push(format!(
                "  key     {}",
                if self.key_present() {
                    "present"
                } else {
                    "absent"
                }
            ));
        }
        Ok(lines)
    }

    /// Whether the daemon in this worktree was started under another binary or config than
    /// `resolved`, from what the last run recorded.
    /// With no record, the daemon is taken to be the default profile's: zero config writes no
    /// state file at all.
    pub fn daemon_is_stale(&self, resolved: &Resolved) -> Option<String> {
        let ran = match self.state().ran {
            Some(ran) => ran,
            None => self.ran_marker(&self.default_resolved().ok()?),
        };
        (ran != self.ran_marker(resolved)).then(|| ran.split('|').next().unwrap_or("").to_owned())
    }

    fn default_resolved(&self) -> Result<Resolved, String> {
        let profile = &self.manifest.profiles[&self.manifest.default];
        let channel = &self.manifest.channels[&profile.channel];
        let exec = self.root.join(&channel.exec);
        Ok(Resolved {
            profile: self.manifest.default.clone(),
            channel: profile.channel.clone(),
            overridden: false,
            rev: rev_of(&exec),
            from: channel.from.clone(),
            exec,
        })
    }

    /// Records that `resolved` is about to run, for the next staleness check. In the default
    /// profile with nothing selected, it records nothing, so the worktree holds no state file.
    pub fn record_run(&self, resolved: &Resolved) -> std::io::Result<()> {
        let mut state = self.state();
        let is_default = resolved.profile == self.manifest.default && !resolved.overridden;
        state.ran = (!(is_default && state.profile.is_none())).then(|| self.ran_marker(resolved));
        state.write(&self.state_path())
    }

    /// A program the daemon was started by, as the output names it: its channel and commit when
    /// it is one of the manifest's channels, else its path.
    pub fn describe_exec(&self, exec: &str) -> String {
        let path = Path::new(exec);
        let rev = rev_of(path).map(|r| short(&r).to_owned());
        let channel = self
            .manifest
            .channels
            .values()
            .find(|c| self.root.join(&c.exec) == path);
        match (channel, rev) {
            (Some(c), Some(rev)) => format!("{} {rev}", c.from),
            (Some(c), None) => c.from.clone(),
            (None, Some(rev)) => rev,
            (None, None) => exec.to_owned(),
        }
    }

    /// The one line printed before each buck2 call, or none in the default profile.
    pub fn banner(&self, resolved: &Resolved) -> Option<String> {
        if resolved.profile == self.manifest.default && !resolved.overridden {
            return None;
        }
        let star = if resolved.overridden { "*" } else { "" };
        let rev = resolved.rev.as_deref().map(short).unwrap_or("?");
        Some(format!(
            "{}: {}{star} · buck2 {rev} · {}",
            self.manifest.name, resolved.profile, self.manifest.profiles[&resolved.profile].summary
        ))
    }

    pub fn status(&self) -> Result<Vec<String>, String> {
        let resolved = self.resolve(None, None)?;
        let name = &self.manifest.name;
        let p = &self.manifest.profiles[&resolved.profile];
        let default = if resolved.profile == self.manifest.default {
            " (default)"
        } else {
            ""
        };
        let star = if resolved.overridden { "*" } else { "" };
        let mut lines = vec![
            format!("{name}: profile {}{star}{default}", resolved.profile),
            format!("  binary  {}", describe_binary(&resolved)),
        ];
        let dest = self.config_dest();
        lines.push(match (&p.config, dest.exists()) {
            (Some(source), true) => {
                let edited = std::fs::read_to_string(&dest)
                    .ok()
                    .zip(std::fs::read_to_string(self.root.join(source)).ok());
                let note = match edited {
                    Some((written, source_text)) if written.ends_with(&source_text) => "",
                    _ => " (differs from its source)",
                };
                format!(
                    "  config  {} from {}{note}\n          {}",
                    self.manifest.config_dest.display(),
                    source.display(),
                    p.summary
                )
            }
            (Some(_), false) => format!(
                "  config  missing: run `buck2 --profile {}` again",
                resolved.profile
            ),
            (None, _) => format!("  config  none written; {}", p.summary),
        });
        if self.manifest.credentials.is_some() {
            lines.push(format!(
                "  key     {}",
                if self.key_present() {
                    "present"
                } else {
                    "absent (`buck2 --login` adds one)"
                }
            ));
        }
        lines.push(match self.daemon_is_stale(&resolved) {
            Some(was) => format!(
                "  daemon  started by {}; restarts on next call",
                self.describe_exec(&was)
            ),
            None => "  daemon  same binary and config".to_owned(),
        });
        Ok(lines)
    }
}

/// Stops the daemon a previous binary started in this worktree, with that binary.
pub fn restart_daemon(root: &Path, previous_exec: &Path) -> std::io::Result<()> {
    if !previous_exec.exists() {
        return Ok(());
    }
    Command::new(previous_exec)
        .arg("kill")
        .current_dir(root)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|_| ())
}

fn written_by_us(path: &Path) -> bool {
    std::fs::read_to_string(path).is_ok_and(|t| t.starts_with(HEADER))
}

fn short(rev: &str) -> &str {
    &rev[..rev.len().min(10)]
}

fn describe_binary(r: &Resolved) -> String {
    match &r.rev {
        Some(rev) => format!("{} {}", r.from, short(rev)),
        None => format!("{} ({})", r.from, r.exec.display()),
    }
}

/// The buck2 commit a dotslash file or wrapper names, from its `buck2-git-<rev>` store path.
fn rev_of(exec: &Path) -> Option<String> {
    static RE: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"buck2-git-([0-9a-f]{7,40})").unwrap());
    let text = std::fs::read(exec).ok()?;
    let text = String::from_utf8_lossy(&text[..text.len().min(64 * 1024)]);
    RE.captures(&text).map(|c| c[1].to_owned())
}
