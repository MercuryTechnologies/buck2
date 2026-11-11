/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! A local directory store of records, laid out as a remote one would be:
//!
//! ```text
//! <dir>/commits/<commit>              the version V the commit was primed under
//! <dir>/<V>/manifest.json             the components of V, by name
//! <dir>/<V>/pointer/<commit>          the shard list for the commit
//! <dir>/<V>/shards/<commit>/<xx>.json sorted (record key, record digest)
//! <dir>/<V>/records/<digest>.json     a record
//! <dir>/<V>/inv/<E>.json              an invalidation body
//! <dir>/<V>/unshareable/<commit>.tsv  kind, key id, reason
//! ```
//!
//! Every file is written to a temporary name and renamed, the pointer and the commit last, so a
//! reader never sees a half-written prime. Value rows are not copied: in phases 1 to 3 the shadow
//! reader compares value-row digests, and never decodes a record's value.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Mutex;

use serde::Deserialize;
use serde::Serialize;

use crate::body::Body;
use crate::body::BodySource;
use crate::body::Digest;
use crate::walk::Primed;
use crate::walk::Record;
use crate::walk::RecordSource;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    pub format: u32,
    pub version: Digest,
    /// Component name to digest.
    pub components: BTreeMap<String, String>,
}

fn write_atomic(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension(format!("tmp.{}", std::process::id()));
    std::fs::write(&tmp, bytes)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

fn safe(name: &str) -> anyhow::Result<&str> {
    if name.is_empty()
        || !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
        || name.starts_with('.')
    {
        return Err(anyhow::anyhow!("not a safe store name: {name:?}"));
    }
    Ok(name)
}

/// Writes a prime. Returns the bytes written by kind of file.
pub fn write(
    dir: &Path,
    manifest: &Manifest,
    commit: &str,
    primed: &Primed,
) -> anyhow::Result<BTreeMap<&'static str, u64>> {
    let commit = safe(commit)?;
    let v = safe(&manifest.version)?;
    let vdir = dir.join(v);
    let mut bytes: BTreeMap<&'static str, u64> = BTreeMap::new();
    let m = serde_json::to_vec_pretty(manifest)?;
    *bytes.entry("manifest").or_default() += m.len() as u64;
    write_atomic(&vdir.join("manifest.json"), &m)?;
    for (e, body) in &primed.bodies {
        let p = vdir.join("inv").join(format!("{}.json", safe(e)?));
        if p.exists() {
            continue;
        }
        let b = serde_json::to_vec(body)?;
        *bytes.entry("bodies").or_default() += b.len() as u64;
        write_atomic(&p, &b)?;
    }
    let mut shards: BTreeMap<String, Vec<(String, String)>> = BTreeMap::new();
    for (key, record) in &primed.records {
        let digest = record.digest();
        let p = vdir.join("records").join(format!("{digest}.json"));
        if !p.exists() {
            let b = serde_json::to_vec(record)?;
            *bytes.entry("records").or_default() += b.len() as u64;
            write_atomic(&p, &b)?;
        }
        shards
            .entry(key[..2].to_owned())
            .or_default()
            .push((key.clone(), digest));
    }
    let mut names = Vec::new();
    for (prefix, entries) in &shards {
        let b = serde_json::to_vec(entries)?;
        *bytes.entry("shards").or_default() += b.len() as u64;
        write_atomic(
            &vdir
                .join("shards")
                .join(commit)
                .join(format!("{prefix}.json")),
            &b,
        )?;
        names.push(prefix.clone());
    }
    let mut tsv = String::from("kind\tkey_id\treason\n");
    for (kind, key, reason) in &primed.unshareable {
        tsv.push_str(&format!("{kind}\t{key}\t{reason}\n"));
    }
    write_atomic(
        &vdir.join("unshareable").join(format!("{commit}.tsv")),
        tsv.as_bytes(),
    )?;
    write_atomic(
        &vdir.join("pointer").join(commit),
        &serde_json::to_vec(&names)?,
    )?;
    write_atomic(&dir.join("commits").join(commit), v.as_bytes())?;
    Ok(bytes)
}

/// A reader of one commit's prime under one version.
pub struct StoreReader {
    vdir: PathBuf,
    commit: String,
    shards: Mutex<HashMap<String, Option<HashMap<String, String>>>>,
    /// The shard prefixes the pointer lists; `None` if the version has no pointer for the commit.
    listed: Option<Vec<String>>,
}

impl StoreReader {
    pub fn open(dir: &Path, version: &str, commit: &str) -> anyhow::Result<Self> {
        let vdir = dir.join(safe(version)?);
        let commit = safe(commit)?.to_owned();
        let pointer = vdir.join("pointer").join(&commit);
        let listed = match std::fs::read(&pointer) {
            Ok(b) => Some(serde_json::from_slice(&b)?),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(e.into()),
        };
        Ok(StoreReader {
            vdir,
            commit,
            shards: Mutex::new(HashMap::new()),
            listed,
        })
    }

    pub fn has_pointer(&self) -> bool {
        self.listed.is_some()
    }

    /// The version the commit was primed under, and its manifest, if it was primed at all.
    pub fn primed_manifest(dir: &Path, commit: &str) -> anyhow::Result<Option<Manifest>> {
        let commit = safe(commit)?;
        let v = match std::fs::read_to_string(dir.join("commits").join(commit)) {
            Ok(v) => v,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let m = std::fs::read(dir.join(safe(v.trim())?).join("manifest.json"))?;
        Ok(Some(serde_json::from_slice(&m)?))
    }

    fn shard(&self, prefix: &str) -> anyhow::Result<Option<HashMap<String, String>>> {
        let mut shards = self.shards.lock().unwrap();
        if let Some(s) = shards.get(prefix) {
            return Ok(s.clone());
        }
        let listed = self
            .listed
            .as_ref()
            .is_some_and(|l| l.iter().any(|p| p == prefix));
        let s = if listed {
            let b = std::fs::read(
                self.vdir
                    .join("shards")
                    .join(&self.commit)
                    .join(format!("{}.json", safe(prefix)?)),
            )?;
            let entries: Vec<(String, String)> = serde_json::from_slice(&b)?;
            Some(entries.into_iter().collect())
        } else {
            None
        };
        shards.insert(prefix.to_owned(), s.clone());
        Ok(s)
    }
}

impl BodySource for StoreReader {
    fn body(&self, e: &str) -> anyhow::Result<Body> {
        let b = std::fs::read(self.vdir.join("inv").join(format!("{}.json", safe(e)?)))?;
        Ok(serde_json::from_slice(&b)?)
    }
}

impl RecordSource for StoreReader {
    fn record(&self, record_key: &str) -> anyhow::Result<Option<Record>> {
        if record_key.len() < 2 {
            return Ok(None);
        }
        let Some(shard) = self.shard(&record_key[..2])? else {
            return Ok(None);
        };
        let Some(digest) = shard.get(record_key) else {
            return Ok(None);
        };
        let b = std::fs::read(
            self.vdir
                .join("records")
                .join(format!("{}.json", safe(digest)?)),
        )?;
        let r: Record = serde_json::from_slice(&b)?;
        if r.digest() != *digest {
            return Err(anyhow::anyhow!("record {digest} does not hash to its name"));
        }
        Ok(Some(r))
    }
}

/// Which components of two manifests differ.
pub fn differing_components(a: &Manifest, b: &Manifest) -> Vec<String> {
    let mut out = Vec::new();
    for (k, v) in &a.components {
        if b.components.get(k) != Some(v) {
            out.push(k.clone());
        }
    }
    for k in b.components.keys() {
        if !a.components.contains_key(k) {
            out.push(k.clone());
        }
    }
    out.sort();
    out.dedup();
    out
}
