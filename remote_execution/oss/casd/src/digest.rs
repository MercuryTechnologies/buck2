/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! Digests and the hash functions that produce them.

use std::fmt;
use std::io;
use std::io::Read;
use std::path::Path;
use std::str::FromStr;

use anyhow::Context;
use re_grpc_proto::build::bazel::remote::execution::v2 as re;

/// The hash function every blob served by this daemon is addressed by. It must match what the
/// buck2 daemons and the upstream CAS use (`[buck2] digest_algorithms`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DigestFunction {
    Sha256,
    Sha1,
    Blake3,
}

impl DigestFunction {
    /// Length of a hash in hex characters.
    pub fn hex_len(self) -> usize {
        match self {
            Self::Sha256 | Self::Blake3 => 64,
            Self::Sha1 => 40,
        }
    }

    pub fn proto_value(self) -> re::digest_function::Value {
        match self {
            Self::Sha256 => re::digest_function::Value::Sha256,
            Self::Sha1 => re::digest_function::Value::Sha1,
            Self::Blake3 => re::digest_function::Value::Blake3,
        }
    }

    pub fn hasher(self) -> Hasher {
        match self {
            Self::Sha256 => Hasher::Sha256(sha2::Sha256::default()),
            Self::Sha1 => Hasher::Sha1(sha1::Sha1::default()),
            Self::Blake3 => Hasher::Blake3(Box::new(blake3::Hasher::new())),
        }
    }

    pub fn hash_bytes(self, data: &[u8]) -> String {
        let mut h = self.hasher();
        h.update(data);
        h.finish_hex()
    }

    /// Hashes a file in blocking fashion.
    pub fn hash_file(self, path: &Path) -> io::Result<String> {
        let mut file = std::fs::File::open(path)?;
        let mut hasher = self.hasher();
        let mut buf = vec![0u8; 1 << 20];
        loop {
            let n = file.read(&mut buf)?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }
        Ok(hasher.finish_hex())
    }
}

impl FromStr for DigestFunction {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "sha256" => Ok(Self::Sha256),
            "sha1" => Ok(Self::Sha1),
            "blake3" => Ok(Self::Blake3),
            other => Err(anyhow::anyhow!(
                "Unknown digest function `{other}` (expected sha256, sha1 or blake3)"
            )),
        }
    }
}

impl fmt::Display for DigestFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sha256 => write!(f, "sha256"),
            Self::Sha1 => write!(f, "sha1"),
            Self::Blake3 => write!(f, "blake3"),
        }
    }
}

pub enum Hasher {
    Sha256(sha2::Sha256),
    Sha1(sha1::Sha1),
    Blake3(Box<blake3::Hasher>),
}

impl Hasher {
    pub fn update(&mut self, data: &[u8]) {
        use sha1::Digest as _;
        match self {
            Self::Sha256(h) => h.update(data),
            Self::Sha1(h) => h.update(data),
            Self::Blake3(h) => {
                h.update(data);
            }
        }
    }

    pub fn finish_hex(self) -> String {
        use sha1::Digest as _;
        match self {
            Self::Sha256(h) => hex::encode(h.finalize()),
            Self::Sha1(h) => hex::encode(h.finalize()),
            Self::Blake3(h) => h.finalize().to_hex().to_string(),
        }
    }
}

/// A validated digest: lowercase hex of the expected length and a non-negative size.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Digest {
    pub hash: String,
    pub size: i64,
}

impl Digest {
    pub fn new(hash: &str, size: i64, function: DigestFunction) -> anyhow::Result<Self> {
        if hash.len() != function.hex_len()
            || !hash
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(anyhow::anyhow!(
                "`{hash}` is not a lowercase hex {function} hash of {} characters",
                function.hex_len()
            ));
        }
        if size < 0 {
            return Err(anyhow::anyhow!("Digest `{hash}` has negative size {size}"));
        }
        Ok(Self {
            hash: hash.to_owned(),
            size,
        })
    }

    pub fn from_proto(d: &re::Digest, function: DigestFunction) -> anyhow::Result<Self> {
        Self::new(&d.hash, d.size_bytes, function).context("Invalid digest")
    }

    pub fn to_proto(&self) -> re::Digest {
        re::Digest {
            hash: self.hash.clone(),
            size_bytes: self.size,
        }
    }

    pub fn to_tdigest(&self) -> remote_execution::TDigest {
        remote_execution::TDigest {
            hash: self.hash.clone(),
            size_in_bytes: self.size,
            ..Default::default()
        }
    }
}

impl fmt::Display for Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.hash, self.size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_bytes() {
        assert_eq!(
            DigestFunction::Sha256.hash_bytes(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(
            DigestFunction::Sha1.hash_bytes(b"abc"),
            "a9993e364706816aba3e25717850c26c9cd0d89d"
        );
        assert_eq!(
            DigestFunction::Blake3.hash_bytes(b"abc"),
            "6437b3ac38465133ffb63b75273a8db548c558465d79db03fd359c6cd5bd9d85"
        );
    }

    #[test]
    fn test_digest_validation() {
        let ok = "a".repeat(64);
        assert!(Digest::new(&ok, 0, DigestFunction::Sha256).is_ok());
        assert!(Digest::new(&ok, -1, DigestFunction::Sha256).is_err());
        assert!(Digest::new(&ok, 1, DigestFunction::Sha1).is_err());
        assert!(Digest::new(&"A".repeat(64), 1, DigestFunction::Sha256).is_err());
        assert!(Digest::new("../x", 1, DigestFunction::Sha256).is_err());
    }
}
