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

use std::path::Path;

use buck2_error::BuckErrorContext;
use serde::Deserialize;

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
/// Bumped on any change to the daemon's `Manifest` or `ImageFile`.
pub const FORMAT: u32 = 1;

/// The `MANIFEST` fields a client reads; the daemon's own struct has more, which serde skips.
#[derive(Deserialize, Debug, Clone)]
pub struct ManifestHead {
    pub magic: String,
    pub format: u32,
    /// The blake3 of the daemon binary that wrote the image.
    pub exe: String,
    pub created_at_ms: u64,
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
