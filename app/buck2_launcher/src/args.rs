/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! The picker's own flags, taken only from before buck2's subcommand, so that
//! everything from the subcommand on reaches buck2 unchanged.

use std::ffi::OsString;

#[derive(Debug, Default, PartialEq, Eq)]
pub struct PickerArgs {
    /// `--profile` with no value shows the profile in force; with one, switches to it.
    pub profile: Option<Option<String>>,
    /// `--channel <name|commit>` overrides the profile's channel.
    pub channel: Option<String>,
    pub login: bool,
    pub logout: bool,
    /// Everything else, in order, for buck2.
    pub rest: Vec<OsString>,
}

impl PickerArgs {
    pub fn is_picker_only(&self) -> bool {
        self.rest.is_empty()
            && (self.profile.is_some() || self.channel.is_some() || self.login || self.logout)
    }
}

/// Splits the picker's flags from buck2's arguments. Only flags before the first argument that
/// does not start with `-` are the picker's, and only these four names: buck2's own flags before
/// the subcommand (`--isolation-dir`, `-v` and the rest) stay in `rest`, in place.
pub fn parse(args: impl IntoIterator<Item = OsString>) -> Result<PickerArgs, String> {
    let mut out = PickerArgs::default();
    let mut args = args.into_iter().peekable();
    let mut in_subcommand = false;
    while let Some(arg) = args.next() {
        if in_subcommand {
            out.rest.push(arg);
            continue;
        }
        let Some(s) = arg.to_str() else {
            out.rest.push(arg);
            continue;
        };
        if s == "--profile" {
            // A value follows unless the next argument is a flag or a buck2 subcommand. A
            // profile's name is never a buck2 subcommand, which the caller checks.
            let value = match args.peek().and_then(|a| a.to_str()) {
                Some(next) if !next.starts_with('-') && !is_buck2_subcommand(next) => {
                    let v = next.to_owned();
                    args.next();
                    Some(v)
                }
                _ => None,
            };
            out.profile = Some(value);
        } else if let Some(v) = value_of("--profile", s) {
            out.profile = Some(Some(v.to_owned()));
        } else if s == "--channel" {
            let v = args
                .next()
                .and_then(|a| a.into_string().ok())
                .ok_or("--channel needs a channel name or a commit")?;
            out.channel = Some(v);
        } else if let Some(v) = value_of("--channel", s) {
            out.channel = Some(v.to_owned());
        } else if s == "--login" {
            out.login = true;
        } else if s == "--logout" {
            out.logout = true;
        } else if takes_value(s) {
            out.rest.push(arg);
            if let Some(value) = args.next() {
                out.rest.push(value);
            }
        } else {
            if !s.starts_with('-') {
                in_subcommand = true;
            }
            out.rest.push(arg);
        }
    }
    if out.login && out.logout {
        return Err("--login and --logout are each other's opposite; pass one".to_owned());
    }
    Ok(out)
}

/// The value of `--name=value`.
fn value_of<'a>(name: &str, s: &'a str) -> Option<&'a str> {
    s.strip_prefix(name).and_then(|v| v.strip_prefix('='))
}

/// buck2's flags before the subcommand that take a separate value (buck2's
/// `BeforeSubcommandOptions`), so that the value is not read as the subcommand.
fn takes_value(s: &str) -> bool {
    matches!(
        s,
        "--isolation-dir"
            | "-v"
            | "--verbose"
            | "--oncall"
            | "--client-metadata"
            | "--setting"
            | "--agent-context"
    )
}

/// buck2's subcommands, so that `buck2 --profile build //x` reads as "show, then build" rather
/// than as a profile named `build`.
pub fn is_buck2_subcommand(s: &str) -> bool {
    const SUBCOMMANDS: &[&str] = &[
        "aquery",
        "audit",
        "build",
        "bxl",
        "clean",
        "cquery",
        "ctargets",
        "debug",
        "docs",
        "expand-external-cell",
        "explain",
        "help",
        "init",
        "install",
        "kill",
        "killall",
        "log",
        "lsp",
        "profile",
        "query",
        "rage",
        "root",
        "run",
        "server",
        "starlark",
        "status",
        "subscribe",
        "targets",
        "test",
        "uquery",
        "utargets",
    ];
    SUBCOMMANDS.contains(&s)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(args: &[&str]) -> PickerArgs {
        parse(args.iter().map(OsString::from)).unwrap()
    }

    fn rest(args: &[&str]) -> Vec<OsString> {
        args.iter().map(OsString::from).collect()
    }

    #[test]
    fn plain_buck2_arguments_pass_through() {
        let a = p(&["build", "//x", "--profile", "y"]);
        assert_eq!(a.profile, None);
        assert_eq!(a.rest, rest(&["build", "//x", "--profile", "y"]));
    }

    #[test]
    fn profile_alone_shows() {
        assert_eq!(p(&["--profile"]).profile, Some(None));
        assert!(p(&["--profile"]).is_picker_only());
    }

    #[test]
    fn profile_with_a_name_switches_and_runs_the_rest() {
        let a = p(&["--profile", "next", "build", "//x"]);
        assert_eq!(a.profile, Some(Some("next".to_owned())));
        assert_eq!(a.rest, rest(&["build", "//x"]));
        assert!(!a.is_picker_only());
    }

    #[test]
    fn profile_before_a_subcommand_is_a_show() {
        let a = p(&["--profile", "build", "//x"]);
        assert_eq!(a.profile, Some(None));
        assert_eq!(a.rest, rest(&["build", "//x"]));
    }

    #[test]
    fn buck2_flags_before_the_subcommand_stay_in_place() {
        let a = p(&[
            "--isolation-dir",
            "v2",
            "--profile=next",
            "-v",
            "2",
            "test",
            "//x",
        ]);
        assert_eq!(a.profile, Some(Some("next".to_owned())));
        assert_eq!(
            a.rest,
            rest(&["--isolation-dir", "v2", "-v", "2", "test", "//x"])
        );
    }

    #[test]
    fn channel_and_login() {
        let a = p(&["--profile", "next", "--channel", "7288c87a83", "--login"]);
        assert_eq!(a.channel.as_deref(), Some("7288c87a83"));
        assert!(a.login);
        assert!(parse(["--login", "--logout"].map(OsString::from)).is_err());
        assert!(parse(["--channel"].map(OsString::from)).is_err());
    }
}
