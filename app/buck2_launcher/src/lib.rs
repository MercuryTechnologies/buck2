/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! Profiles for buck2: a project names its buck2 binaries (channels) and its buckconfig
//! profiles in a manifest, and `buck2 --profile <name>` picks one for a worktree, writes its
//! config into `.buckconfig.d`, restarts the daemon when the binary or config changed, and says
//! so. With nothing selected the project's default profile runs, which prints nothing extra.
//!
//! The library holds no project's names: a project ships a manifest and a small binary that
//! calls [`run`] with its path.

pub mod args;
pub mod manifest;
pub mod picker;

use std::ffi::OsString;
use std::path::Path;
use std::path::PathBuf;

use crate::manifest::Manifest;
use crate::picker::Picker;

/// The project root: the nearest directory, from `start` up, that holds the manifest.
pub fn find_root(start: &Path, manifest: &Path) -> Option<PathBuf> {
    start
        .ancestors()
        .find(|dir| dir.join(manifest).is_file())
        .map(Path::to_path_buf)
}

/// What to do after the picker's own work: run this program with these arguments, or exit.
pub enum Next {
    Exit(i32),
    Exec {
        program: PathBuf,
        args: Vec<OsString>,
    },
}

/// Runs the picker for `args` (without the program name), printing to `out`. `manifest` is the
/// manifest's path relative to the project root, and `this_exe` the picker's own program, so that
/// a channel whose program is the picker is refused instead of looping.
pub fn run(
    manifest_rel: &Path,
    this_exe: Option<&Path>,
    cwd: &Path,
    home: Option<PathBuf>,
    env: impl Fn(&str) -> Option<OsString>,
    args: Vec<OsString>,
    out: &mut dyn FnMut(&str),
) -> Next {
    match run_inner(manifest_rel, this_exe, cwd, home, &env, args, out) {
        Ok(next) => next,
        Err(message) => {
            out(&message);
            Next::Exit(2)
        }
    }
}

fn run_inner(
    manifest_rel: &Path,
    this_exe: Option<&Path>,
    cwd: &Path,
    home: Option<PathBuf>,
    env: &impl Fn(&str) -> Option<OsString>,
    args: Vec<OsString>,
    out: &mut dyn FnMut(&str),
) -> Result<Next, String> {
    let args = args::parse(args)?;
    let root = find_root(cwd, manifest_rel).ok_or_else(|| {
        format!(
            "buck2: no {} above {}",
            manifest_rel.display(),
            cwd.display()
        )
    })?;
    let manifest = Manifest::load(&root.join(manifest_rel))?;
    let picker = Picker {
        root: &root,
        manifest: &manifest,
        home,
    };
    let name = manifest.name.clone();

    if args.login || args.logout {
        return Err(format!(
            "{name}: --login and --logout are not built yet; write the key file by hand \
             (see the README)"
        ));
    }

    // CI picks its binary with BUCK2_ROLLOUT_BIN, as mercury's nix wrapper does; the picker
    // still writes a profile's config, but the program is CI's.
    let rollout = env("BUCK2_ROLLOUT_BIN")
        .map(PathBuf::from)
        .filter(|p| p.join("buck2").is_file());

    let switching = matches!(args.profile, Some(Some(_))) || args.channel.is_some();
    if switching {
        let profile = match &args.profile {
            Some(Some(p)) => p.clone(),
            _ => picker.resolve(None, None)?.profile,
        };
        for line in picker.switch(&profile, args.channel.as_deref())? {
            out(&line);
        }
        let resolved = picker.resolve(None, None)?;
        if let Some(was) = picker.daemon_is_stale(&resolved) {
            picker::restart_daemon(&root, Path::new(&was)).map_err(|e| e.to_string())?;
            picker.record_run(&resolved).map_err(|e| e.to_string())?;
            out(&format!(
                "  daemon  stopped (was {}); restarts on next call",
                picker.describe_exec(&was)
            ));
        }
    } else if matches!(args.profile, Some(None)) {
        for line in picker.status()? {
            out(&line);
        }
    }
    if args.is_picker_only() {
        return Ok(Next::Exit(0));
    }

    let resolved = picker.resolve(None, None)?;
    if let Some(was) = picker.daemon_is_stale(&resolved) {
        picker::restart_daemon(&root, Path::new(&was)).map_err(|e| e.to_string())?;
        out(&format!(
            "{name}: stopped the daemon {} started; this call starts a new one",
            picker.describe_exec(&was)
        ));
    }
    let program = match rollout {
        Some(dir) => {
            out(&format!(
                "{name}: BUCK2_ROLLOUT_BIN chooses the binary ({})",
                dir.display()
            ));
            dir.join("buck2")
        }
        None => {
            if let Some(line) = picker.banner(&resolved) {
                out(&line);
            }
            resolved.exec.clone()
        }
    };
    if let (Some(this), Ok(target)) = (this_exe, program.canonicalize())
        && this.canonicalize().is_ok_and(|this| this == target)
    {
        return Err(format!(
            "{name}: channel {} runs the profile picker itself; check its `exec` in {}",
            resolved.channel,
            manifest_rel.display()
        ));
    }
    picker.record_run(&resolved).map_err(|e| e.to_string())?;
    Ok(Next::Exec {
        program,
        args: args.rest,
    })
}

#[cfg(test)]
mod tests;
