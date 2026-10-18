/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! Where `buck2 suspend` leaves its image under a tenant's cache dir, and the parts of its
//! `MANIFEST` a client reads. The daemon writes and checks the image; the client carries it
//! between machines (`buck2 suspend --export`, `buck2 resume --import`) while no daemon runs,
//! and the client cannot depend on the server crate, so the names live here.
//!
//! # Which machines may resume an image
//!
//! An image says its values are what this graph computes. Most values come from the remote
//! cache or from files the resume checks again, but an action that ran on the suspending
//! machine (a nix realisation, a `local_only` action) computed its output with that machine's
//! tools, and so may every value derived from it. A resume is sound only on a machine that
//! would run those actions the same way, and it must refuse anywhere else: a refusal costs a
//! cold build, an acceptance on another toolchain reports stale values as valid.
//!
//! The daemon therefore checks, beside the binary ([`exe_digest`]) and the user:
//!
//! - the **host identity**, `hydration.host_identity` (legacy `buck2_hydration.host_identity`),
//!   or the hostname when it is unset. The hostname keeps an image on the machine that wrote
//!   it. Fresh CI runners each have a hostname of their own, so they set the identity to what
//!   their local actions depend on and the hostname does not: the runner image's digest, or a
//!   declared toolchain digest, and the architecture. Never derive it from the whole NixOS
//!   system (`/run/current-system`): that closure carries the hostname (`/etc/hostname`,
//!   `/etc/hosts`, the units and the initrd), so two runners of one image never match.
//! - the **system** ([`system_fingerprint`]): the architecture, the OS and, where it exists,
//!   the store path `/run/current-system/sw` resolves to. That path is the NixOS (or
//!   nix-darwin) `system-path`, the closure of every package on the system's `PATH`, and it
//!   holds no hostname, so every runner of one image has the same one and any change of a
//!   system package changes it. The daemon reads it itself and checks it whatever the identity
//!   says, so a declared identity that forgets the toolchain cannot accept an image across a
//!   system switch, and neither can the hostname default on the machine that wrote it.
//!
//! What the system leaves out: `/etc` and the activation's links outside `sw` (`/bin/sh`,
//! `/usr/bin/env`), the kernel, and every machine without `/run/current-system`. On such a
//! machine the identity is the only toolchain check, and must name the image.
//!
//! `buck2 resume --import` runs the same checks on the bundle's MANIFEST before it unpacks
//! anything ([`check_head`]), so a bundle for another machine is refused in milliseconds and
//! no daemon starts. The daemon checks the MANIFEST again when it starts, and its check is the
//! one that decides: the client's is only a fast refusal.

use std::path::Path;

use buck2_error::BuckErrorContext;
use serde::Deserialize;

use crate::init::DaemonStartupConfig;

/// The directory under the cache dir that holds an image.
pub const IMAGE_DIR: &str = "suspend";
/// The paging store's directory under the cache dir (`TenantPaths::dice_state_path`).
pub const DICE_STATE_DIR: &str = "dice_state";
/// Written last by a suspend; a start resumes only when it is present.
pub const MANIFEST: &str = "MANIFEST";
/// What a starting daemon renames the MANIFEST to before it checks it, so an image is used once.
pub const MANIFEST_CONSUMED: &str = "MANIFEST.consumed";
/// The graph, artifact and file-watcher image.
pub const IMAGE: &str = "image.bin";
pub const MAGIC: &str = "BUCK2SUS";
/// Bumped on any change to the daemon's `Manifest` or `ImageFile`, or to what their rows hold.
/// 2: the host identity, the snapshot's digests and the artifacts' CAS recipes.
/// 3: the system ([`system_fingerprint`]).
pub const FORMAT: u32 = 3;

/// The `MANIFEST` fields a client reads; the daemon's own struct has more, which serde skips.
#[derive(Deserialize, Debug, Clone)]
pub struct ManifestHead {
    pub magic: String,
    pub format: u32,
    /// The blake3 of the daemon binary that wrote the image.
    pub exe: String,
    pub created_at_ms: u64,
    /// Written by `buck2 suspend --portable`: every artifact has a CAS recipe, and the image
    /// lists the actions that ran locally, for the resumed daemon to run again.
    #[serde(default)]
    pub portable: bool,
    /// [`host_identity`] of the daemon that wrote the image.
    pub host_identity: String,
    /// [`system_fingerprint`] of the machine that wrote the image.
    pub system: String,
    pub uid: u32,
}

impl ManifestHead {
    pub fn read(path: &Path) -> buck2_error::Result<Self> {
        let bytes = std::fs::read(path)
            .with_buck_error_context(|| format!("Reading `{}`", path.display()))?;
        serde_json::from_slice(&bytes)
            .with_buck_error_context(|| format!("Parsing `{}`", path.display()))
    }
}

/// The blake3 of an executable, hex: what a MANIFEST's `exe` records. The daemon passes
/// `/proc/self/exe` on Linux, the image it runs even after the file at its path was replaced.
pub fn exe_digest(exe: &Path) -> buck2_error::Result<String> {
    let mut hasher = blake3::Hasher::new();
    let mut file = std::fs::File::open(exe)
        .with_buck_error_context(|| format!("Opening `{}`", exe.display()))?;
    std::io::copy(&mut file, &mut hasher)
        .with_buck_error_context(|| format!("Reading `{}`", exe.display()))?;
    Ok(hasher.finalize().to_hex().to_string())
}

/// What stands for this machine in the on-disk state a daemon with this startup config keeps:
/// `hydration.host_identity` when it is set, the hostname otherwise, and empty when neither is
/// known. The client computes it from the startup config it hands the daemon, so both sides
/// get the same string.
pub fn host_identity(startup_config: &DaemonStartupConfig) -> String {
    match &startup_config.host_identity {
        Some(identity) => identity.clone(),
        None => buck2_events::metadata::hostname().unwrap_or_default(),
    }
}

/// The machine's system as far as it decides what a local action computes: see the module
/// docs. `arch=<arch>;os=<os>`, then `;sw=<store path>` where `/run/current-system/sw`
/// resolves.
pub fn system_fingerprint() -> String {
    system_fingerprint_at(Path::new("/"))
}

/// [`system_fingerprint`] with `/run/current-system/sw` looked up under `root`.
pub fn system_fingerprint_at(root: &Path) -> String {
    let mut out = format!(
        "arch={};os={}",
        std::env::consts::ARCH,
        std::env::consts::OS
    );
    // The resolved path, not the link: `/run/current-system` names the whole system, hostname
    // included, and `sw` inside it the system-path, which is the same on every machine of one
    // image.
    if let Ok(sw) = std::fs::canonicalize(root.join("run/current-system/sw")) {
        out.push_str(";sw=");
        out.push_str(&sw.to_string_lossy());
    }
    out
}

/// The reason texts a refusal gives, shared by the daemon (`cold: <reason>`) and by
/// `buck2 resume --import` (`Not importing the suspend bundle: <reason>`), so that the same
/// mismatch reads the same wherever it is caught.
pub mod reason {
    pub const BINARY: &str = "the buck2 binary differs from the suspended daemon's";
    pub const HOST_IDENTITY: &str = "the host identity differs from the suspended daemon's";
    pub const USER: &str = "the user differs from the suspended daemon's";
    pub const SYSTEM: &str = "the system (architecture, OS, /run/current-system/sw) differs from \
         the suspended daemon's";
}

/// What a machine about to import an image observes, for [`check_head`].
#[derive(Debug, Clone)]
pub struct HeadObserved {
    /// [`exe_digest`] of the binary the daemon will run.
    pub exe: String,
    /// [`host_identity`] of the startup config the daemon will be given.
    pub host_identity: String,
    /// [`system_fingerprint`] of this machine.
    pub system: String,
    pub uid: u32,
}

/// A MANIFEST check that fails without unpacking the image.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeadMismatch {
    Binary,
    HostIdentity,
    User,
    System,
}

impl std::fmt::Display for HeadMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            HeadMismatch::Binary => reason::BINARY,
            HeadMismatch::HostIdentity => reason::HOST_IDENTITY,
            HeadMismatch::User => reason::USER,
            HeadMismatch::System => reason::SYSTEM,
        })
    }
}

/// The checks of a MANIFEST of this [`FORMAT`] that need nothing but the MANIFEST, in the
/// order the daemon makes them. Passing them does not mean the daemon will resume: it checks
/// these again and much else besides.
pub fn check_head(head: &ManifestHead, here: &HeadObserved) -> Result<(), HeadMismatch> {
    if head.exe != here.exe {
        return Err(HeadMismatch::Binary);
    }
    if head.host_identity != here.host_identity {
        return Err(HeadMismatch::HostIdentity);
    }
    if head.uid != here.uid {
        return Err(HeadMismatch::User);
    }
    if head.system != here.system {
        return Err(HeadMismatch::System);
    }
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use std::path::PathBuf;

    use super::*;

    /// A NixOS machine as far as the fingerprint looks: `/run/current-system` links to a
    /// toplevel named after the host, whose `sw` links to a system-path.
    fn machine(dir: &Path, host: &str, system_path: &str) -> PathBuf {
        let root = dir.join(host);
        let store = dir.join("store");
        std::fs::create_dir_all(store.join(system_path)).unwrap();
        let toplevel = store.join(format!("aaaa-nixos-system-{host}-26.05"));
        std::fs::create_dir_all(&toplevel).unwrap();
        let sw = toplevel.join("sw");
        if !sw.exists() {
            std::os::unix::fs::symlink(store.join(system_path), sw).unwrap();
        }
        std::fs::create_dir_all(root.join("run")).unwrap();
        std::os::unix::fs::symlink(&toplevel, root.join("run/current-system")).unwrap();
        root
    }

    #[test]
    fn runners_of_one_image_have_one_system_whatever_their_hostnames() {
        let dir = tempfile::tempdir().unwrap();
        let a = machine(dir.path(), "wb-10170838", "7il4-system-path");
        let b = machine(dir.path(), "wb-10171911", "7il4-system-path");
        // The toplevels differ, as the hostname is in them...
        assert_ne!(
            std::fs::canonicalize(a.join("run/current-system")).unwrap(),
            std::fs::canonicalize(b.join("run/current-system")).unwrap()
        );
        // ...and the system does not.
        let fa = system_fingerprint_at(&a);
        assert_eq!(fa, system_fingerprint_at(&b));
        assert!(fa.contains(";sw="), "{fa}");
        assert!(fa.ends_with("7il4-system-path"), "{fa}");
        assert!(!fa.contains("wb-1017"), "{fa}");
    }

    #[test]
    fn another_system_path_is_another_system() {
        let dir = tempfile::tempdir().unwrap();
        let before = machine(dir.path(), "h1", "7il4-system-path");
        let after = machine(dir.path(), "h2", "9zzz-system-path");
        assert_ne!(
            system_fingerprint_at(&before),
            system_fingerprint_at(&after)
        );
    }

    #[test]
    fn a_machine_without_a_nixos_system_has_arch_and_os_only() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(
            system_fingerprint_at(dir.path()),
            format!(
                "arch={};os={}",
                std::env::consts::ARCH,
                std::env::consts::OS
            )
        );
    }

    fn head() -> ManifestHead {
        ManifestHead {
            magic: MAGIC.to_owned(),
            format: FORMAT,
            exe: "e1".to_owned(),
            created_at_ms: 1,
            portable: true,
            host_identity: "img-1".to_owned(),
            system: "arch=x86_64;os=linux;sw=/nix/store/7il4-system-path".to_owned(),
            uid: 1001,
        }
    }

    fn here() -> HeadObserved {
        let h = head();
        HeadObserved {
            exe: h.exe,
            host_identity: h.host_identity,
            system: h.system,
            uid: h.uid,
        }
    }

    #[test]
    fn the_head_checks_name_the_first_difference() {
        assert_eq!(check_head(&head(), &here()), Ok(()));
        let with = |f: fn(&mut HeadObserved)| {
            let mut h = here();
            f(&mut h);
            check_head(&head(), &h)
        };
        assert_eq!(with(|h| h.exe = "e2".to_owned()), Err(HeadMismatch::Binary));
        assert_eq!(
            with(|h| h.host_identity = "img-2".to_owned()),
            Err(HeadMismatch::HostIdentity)
        );
        assert_eq!(with(|h| h.uid = 0), Err(HeadMismatch::User));
        assert_eq!(
            with(|h| h.system = "arch=x86_64;os=linux;sw=/nix/store/9zzz-system-path".to_owned()),
            Err(HeadMismatch::System)
        );
        assert_eq!(
            with(|h| {
                h.exe = "e2".to_owned();
                h.host_identity = "img-2".to_owned();
            }),
            Err(HeadMismatch::Binary)
        );
    }
}
