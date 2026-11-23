/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use std::ffi::OsString;
use std::path::Path;
use std::path::PathBuf;

use crate::Next;
use crate::manifest::EXAMPLE;

struct Project {
    _dir: tempfile::TempDir,
    root: PathBuf,
    home: PathBuf,
}

/// A project with the example manifest, two fake buck2 programs that log `kill`, and the next
/// profile's config.
fn project() -> Project {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("repo");
    let home = dir.path().join("home");
    std::fs::create_dir_all(root.join("tools/buck2/buckconfig.d")).unwrap();
    std::fs::create_dir_all(root.join("nix/bin")).unwrap();
    std::fs::create_dir_all(&home).unwrap();
    std::fs::write(root.join("tools/buck2/profiles.toml"), EXAMPLE).unwrap();
    std::fs::write(
        root.join("tools/buck2/buckconfig.d/next"),
        "<file:../tools/buck2/includes/re/base>\n",
    )
    .unwrap();
    for (name, rev) in [
        ("buck2-current", "9a95ed57f0aa"),
        ("buck2-next", "2b5f3188d9bb"),
    ] {
        let path = root.join("nix/bin").join(name);
        std::fs::write(
            &path,
            format!(
                "#!/bin/sh\n# /nix/store/x-buck2-git-{rev}-wrapped\n\
                 [ \"$1\" = kill ] && echo {name} >> \"$(dirname \"$0\")/kills\"\nexit 0\n"
            ),
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
    }
    Project {
        _dir: dir,
        root,
        home,
    }
}

impl Project {
    fn key(&self) {
        std::fs::create_dir_all(self.home.join(".buckconfig.d")).unwrap();
        std::fs::write(
            self.home.join(".buckconfig.d/mercury-credentials"),
            "[credentials]\n",
        )
        .unwrap();
    }

    fn run(&self, args: &[&str]) -> (Vec<String>, Next) {
        let mut lines = Vec::new();
        let next = crate::run(
            crate::Invocation {
                manifest: Path::new("tools/buck2/profiles.toml"),
                this_exe: None,
                interactive: false,
                cwd: &self.root.join("src"),
                home: Some(self.home.clone()),
            },
            |_| None,
            args.iter().map(OsString::from).collect(),
            &mut |l| lines.push(l.to_owned()),
        );
        (lines, next)
    }

    fn kills(&self) -> String {
        std::fs::read_to_string(self.root.join("nix/bin/kills")).unwrap_or_default()
    }
}

fn exec_of(next: &Next) -> (PathBuf, Vec<String>) {
    match next {
        Next::Exec { program, args } => (
            program.clone(),
            args.iter()
                .map(|a| a.to_string_lossy().into_owned())
                .collect(),
        ),
        Next::Exit(code) => panic!("expected exec, got exit {code}"),
    }
}

#[test]
fn zero_config_prints_nothing_writes_nothing_and_runs_the_default_channel() {
    let p = project();
    std::fs::create_dir_all(p.root.join("src")).unwrap();
    let (lines, next) = p.run(&["build", "//x"]);
    assert!(lines.is_empty(), "{lines:?}");
    let (program, args) = exec_of(&next);
    assert!(program.ends_with("nix/bin/buck2-current"));
    assert_eq!(args, ["build", "//x"]);
    assert!(!p.root.join(".buck2-profile").exists());
    assert!(!p.root.join(".buckconfig.d/50-local-profile").exists());
}

#[test]
fn a_profile_that_needs_a_key_is_refused_without_one() {
    let p = project();
    std::fs::create_dir_all(p.root.join("src")).unwrap();
    let (lines, next) = p.run(&["--profile", "next"]);
    assert!(matches!(next, Next::Exit(2)));
    assert!(
        lines.last().unwrap().contains("needs a BuildBuddy key"),
        "{lines:?}"
    );
    assert!(
        lines
            .last()
            .unwrap()
            .contains("no terminal to sign in from"),
        "{lines:?}"
    );
    assert!(!p.root.join(".buckconfig.d/50-local-profile").exists());
}

#[test]
fn switching_writes_the_config_restarts_the_daemon_and_says_so() {
    let p = project();
    std::fs::create_dir_all(p.root.join("src")).unwrap();
    p.key();
    let (lines, next) = p.run(&["--profile", "next"]);
    assert!(matches!(next, Next::Exit(0)), "{lines:?}");
    // The example manifest warns about next on macOS, first.
    let (warnings, lines): (Vec<String>, Vec<String>) = lines
        .into_iter()
        .partition(|l| l.starts_with("buck2: warning:"));
    assert_eq!(
        warnings.len(),
        usize::from(std::env::consts::OS == "macos"),
        "{warnings:?}"
    );
    assert_eq!(lines[0], "buck2: profile next (was current)");
    assert_eq!(lines[1], "  binary  mercury-head-next 2b5f3188d9");
    assert!(lines[2].contains("wrote .buckconfig.d/50-local-profile"));
    assert!(lines.iter().any(|l| l == "  key     present"));
    assert!(lines.last().unwrap().starts_with("  daemon  stopped (was "));
    assert_eq!(p.kills(), "buck2-current\n");
    let written = std::fs::read_to_string(p.root.join(".buckconfig.d/50-local-profile")).unwrap();
    assert!(written.starts_with("# written by buck2 --profile: profile next"));
    assert!(written.ends_with("<file:../tools/buck2/includes/re/base>\n"));

    // A later call: one line, the next channel, no second restart.
    let (lines, next) = p.run(&["test", "//y"]);
    assert_eq!(lines, ["buck2: next · buck2 2b5f3188d9 · tests on hub RE"]);
    assert!(exec_of(&next).0.ends_with("nix/bin/buck2-next"));
    assert_eq!(p.kills(), "buck2-current\n");

    // Back to zero config: the file goes, the daemon restarts once, and no state is left.
    let (lines, _) = p.run(&["--profile", "current"]);
    assert_eq!(lines[0], "buck2: profile current (was next)");
    assert!(
        lines
            .iter()
            .any(|l| l == "  config  removed .buckconfig.d/50-local-profile")
    );
    assert_eq!(p.kills(), "buck2-current\nbuck2-next\n");
    let (lines, _) = p.run(&["build", "//x"]);
    assert!(lines.is_empty(), "{lines:?}");
    assert!(!p.root.join(".buckconfig.d/50-local-profile").exists());
    assert!(!p.root.join(".buck2-profile").exists());
    assert_eq!(p.kills(), "buck2-current\nbuck2-next\n");
}

#[test]
fn switch_then_run_in_one_call() {
    let p = project();
    std::fs::create_dir_all(p.root.join("src")).unwrap();
    p.key();
    let (lines, next) = p.run(&["--profile", "next", "build", "//x"]);
    assert!(
        lines
            .iter()
            .any(|l| l == "buck2: next · buck2 2b5f3188d9 · tests on hub RE")
    );
    assert_eq!(exec_of(&next).1, ["build", "//x"]);
}

#[test]
fn a_file_a_person_wrote_is_never_overwritten() {
    let p = project();
    std::fs::create_dir_all(p.root.join("src")).unwrap();
    std::fs::create_dir_all(p.root.join(".buckconfig.d")).unwrap();
    std::fs::write(p.root.join(".buckconfig.d/50-local-profile"), "[mine]\n").unwrap();
    p.key();
    let (lines, next) = p.run(&["--profile", "next"]);
    assert!(matches!(next, Next::Exit(2)));
    assert!(
        lines
            .last()
            .unwrap()
            .contains("not written by buck2 --profile")
    );
    assert_eq!(
        std::fs::read_to_string(p.root.join(".buckconfig.d/50-local-profile")).unwrap(),
        "[mine]\n"
    );
}

#[test]
fn status_names_the_profile_and_never_the_key() {
    let p = project();
    std::fs::create_dir_all(p.root.join("src")).unwrap();
    std::fs::create_dir_all(p.home.join(".buckconfig.d")).unwrap();
    std::fs::write(
        p.home.join(".buckconfig.d/mercury-credentials"),
        "[credentials]\nbuildbuddy_api_key = SECRETVALUE\n",
    )
    .unwrap();
    let (lines, next) = p.run(&["--profile"]);
    assert!(matches!(next, Next::Exit(0)));
    assert_eq!(lines[0], "buck2: profile current (default)");
    assert_eq!(lines[1], "  binary  mercury-head 9a95ed57f0");
    assert!(lines.iter().any(|l| l == "  key     present"));
    assert!(lines.iter().all(|l| !l.contains("SECRETVALUE")));
}

#[test]
fn ci_rollout_bin_chooses_the_binary_and_says_so() {
    let p = project();
    std::fs::create_dir_all(p.root.join("src")).unwrap();
    let ci = p.root.join("ci-bin");
    std::fs::create_dir_all(&ci).unwrap();
    std::fs::write(ci.join("buck2"), "#!/bin/sh\n").unwrap();
    let mut lines = Vec::new();
    let ci_dir = ci.clone();
    let next = crate::run(
        crate::Invocation {
            manifest: Path::new("tools/buck2/profiles.toml"),
            this_exe: None,
            interactive: false,
            cwd: &p.root.join("src"),
            home: Some(p.home.clone()),
        },
        move |name| (name == "BUCK2_ROLLOUT_BIN").then(|| ci_dir.clone().into_os_string()),
        vec![OsString::from("build")],
        &mut |l| lines.push(l.to_owned()),
    );
    assert_eq!(exec_of(&next).0, ci.join("buck2"));
    assert!(lines[0].contains("BUCK2_ROLLOUT_BIN chooses the binary"));
}

#[test]
fn an_unknown_profile_or_channel_lists_the_known_ones() {
    let p = project();
    std::fs::create_dir_all(p.root.join("src")).unwrap();
    let (lines, _) = p.run(&["--profile", "nope"]);
    assert!(
        lines.last().unwrap().contains("profiles: current, next"),
        "{lines:?}"
    );
    let (lines, _) = p.run(&["--profile", "current", "--channel", "0123abcd"]);
    assert!(
        lines
            .last()
            .unwrap()
            .contains("not one of this project's channels"),
        "{lines:?}"
    );
}
