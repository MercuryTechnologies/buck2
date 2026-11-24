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
pub mod login;
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

/// Set to `never` to be refused, instead of offered a login, when a profile needs a key.
const LOGIN_VAR: &str = "BUCK2_LAUNCHER_LOGIN";

/// Where and how the launcher runs.
pub struct Invocation<'a> {
    /// The manifest's path relative to the project root.
    pub manifest: &'a Path,
    /// The launcher's own program, so that a channel whose program is the launcher is refused
    /// instead of looping.
    pub this_exe: Option<&'a Path>,
    /// Whether a person is at a terminal, which a login needs.
    pub interactive: bool,
    pub cwd: &'a Path,
    pub home: Option<PathBuf>,
}

/// Runs the launcher for `args` (without the program name), printing to `out`.
pub fn run(
    inv: Invocation,
    env: impl Fn(&str) -> Option<OsString>,
    args: Vec<OsString>,
    out: &mut dyn FnMut(&str),
) -> Next {
    match run_inner(inv, &env, args, out) {
        Ok(next) => next,
        Err(message) => {
            out(&message);
            Next::Exit(2)
        }
    }
}

fn run_inner(
    inv: Invocation,
    env: &impl Fn(&str) -> Option<OsString>,
    args: Vec<OsString>,
    out: &mut dyn FnMut(&str),
) -> Result<Next, String> {
    let Invocation {
        manifest: manifest_rel,
        this_exe,
        interactive,
        cwd,
        home,
    } = inv;
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

    if args.logout {
        let file = picker
            .key_file()
            .ok_or(format!("{name}: this project has no key file"))?;
        match std::fs::remove_file(&file) {
            Ok(()) => out(&format!("{name}: removed {}", file.display())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                out(&format!("{name}: no key at {}", file.display()))
            }
            Err(e) => return Err(format!("{name}: cannot remove {}: {e}", file.display())),
        }
    }
    if args.login {
        login(&picker, interactive, out)?;
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
        let needs_key = manifest.profiles.get(&profile).is_some_and(|p| p.needs_key);
        if needs_key && !picker.key_present() {
            let opted_out = env(LOGIN_VAR).is_some_and(|v| v == "never");
            if !interactive || opted_out {
                let why = if opted_out {
                    format!("{LOGIN_VAR}=never")
                } else {
                    "no terminal to sign in from".to_owned()
                };
                return Err(format!(
                    "{name}: profile {profile} needs a BuildBuddy key; none on this machine, and \
                     {why}.\n  Run `buck2 --login` in a terminal. Nothing changed."
                ));
            }
            out(&format!(
                "{name}: profile {profile} needs a BuildBuddy key; none on this machine."
            ));
            login(&picker, interactive, out)?;
        }
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

/// Signs in through the browser, or takes a pasted key, and writes the credentials file.
fn login(picker: &Picker, interactive: bool, out: &mut dyn FnMut(&str)) -> Result<(), String> {
    let name = &picker.manifest.name;
    let creds = picker
        .manifest
        .credentials
        .as_ref()
        .ok_or(format!("{name}: this project has no key file"))?;
    let base = creds
        .login_url
        .as_deref()
        .ok_or(format!("{name}: the manifest names no login_url"))?;
    if !interactive {
        return Err(format!("{name}: --login needs a terminal"));
    }
    let file = picker
        .key_file()
        .ok_or(format!("{name}: no home directory for the key file"))?;
    let listener = std::net::TcpListener::bind("127.0.0.1:0").map_err(|e| e.to_string())?;
    let port = listener.local_addr().map_err(|e| e.to_string())?.port();
    let url = login::login_url(base, port, creds.org.as_deref(), picker.root);
    let opened = login::open_browser(&url);
    out(&format!(
        "  sign in: {url}{}",
        if opened { "   (opening browser)" } else { "" }
    ));
    out(&format!(
        "  or paste a key and press Enter, or Ctrl-C and set {LOGIN_VAR}=never"
    ));
    let key = login::wait_for_key(
        listener,
        &url,
        Some(login::stdin_lines()),
        std::time::Duration::from_secs(600),
    )
    .map_err(|e| format!("{name}: {e}"))?;
    login::write_secret(&file, &creds.template.replace("{key}", &key))
        .map_err(|e| format!("{name}: cannot write {}: {e}", file.display()))?;
    out(&format!("{name}: key saved to {}", file.display()));
    Ok(())
}

#[cfg(test)]
mod tests;
