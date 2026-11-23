/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! `buck2` for a project with profiles: reads the manifest at `BUCK2_PROFILES_MANIFEST`
//! (relative to the project root, `tools/buck2/profiles.toml` when unset) and runs buck2.

use std::path::PathBuf;

fn main() {
    let manifest = std::env::var_os("BUCK2_PROFILES_MANIFEST")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("tools/buck2/profiles.toml"));
    let cwd = std::env::current_dir().expect("the current directory");
    let home = std::env::var_os("HOME").map(PathBuf::from);
    let args = std::env::args_os().skip(1).collect();
    let this_exe = std::env::current_exe().ok();
    use std::io::IsTerminal;
    let interactive = std::io::stdin().is_terminal() && std::io::stderr().is_terminal();
    let next = buck2_launcher::run(
        buck2_launcher::Invocation {
            manifest: &manifest,
            this_exe: this_exe.as_deref(),
            interactive,
            cwd: &cwd,
            home,
        },
        |name| std::env::var_os(name),
        args,
        &mut |line| eprintln!("{line}"),
    );
    match next {
        buck2_launcher::Next::Exit(code) => std::process::exit(code),
        buck2_launcher::Next::Exec { program, args } => {
            let mut command = std::process::Command::new(&program);
            command.args(args);
            #[cfg(unix)]
            {
                use std::os::unix::process::CommandExt;
                let err = command.exec();
                eprintln!("buck2: cannot run {}: {err}", program.display());
                std::process::exit(127);
            }
            #[cfg(not(unix))]
            {
                let status = command.status().unwrap_or_else(|err| {
                    eprintln!("buck2: cannot run {}: {err}", program.display());
                    std::process::exit(127)
                });
                std::process::exit(status.code().unwrap_or(1));
            }
        }
    }
}
