/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use std::collections::HashMap;

use buck2_e2e::prelude::*;
use serde_json::Value;
use serde_json::json;

/// The records of `targets --json --streaming --imports` that report imports, by file.
fn import_records(xs: &[Value]) -> HashMap<&str, &Value> {
    xs.iter()
        .filter(|x| x.get("buck.imports").is_some())
        .map(|x| (x["buck.file"].as_str().unwrap(), x))
        .collect()
}

fn imports_contain(record: &Value, import: &str) -> bool {
    record["buck.imports"]
        .as_array()
        .unwrap()
        .iter()
        .any(|x| x == import)
}

/// A `load(":<file>?as=toml", ...)` is reported as a leaf import by
/// `targets --streaming --imports`, and is parsed as TOML, even though the
/// file's extension (`.lock`) does not imply TOML. The `?as=toml` hint is
/// stripped from the reported path. Covers both a nested load (via a `.bzl`)
/// and a top-level load directly in the build file.
#[buck_test]
async fn test_imports_as_toml(buck: &Buck) -> anyhow::Result<()> {
    let result = buck
        .targets(&["//...", "--json", "--streaming", "--imports"])
        .await?;
    let xs: Vec<Value> = serde_json::from_str(&result.stdout)?;
    let files = import_records(&xs);

    let targets = files
        .get("root//TARGETS.fixture")
        .expect("TARGETS.fixture imports should be reported");
    assert!(imports_contain(targets, "root//uses_lock.bzl"));
    // Top-level `?as=toml` load, reported without the hint.
    assert!(imports_contain(targets, "root//direct.lock"));

    let bzl = files
        .get("root//uses_lock.bzl")
        .expect("uses_lock.bzl imports (including uv.lock) should be reported");
    // The `?as=toml` hint is not part of the reported import path.
    assert!(imports_contain(bzl, "root//uv.lock"));

    // Parsed as TOML (empty sub-imports), not as a bzl file (which would carry
    // an implicit `prelude//prelude.bzl` import).
    let lock = files
        .get("root//uv.lock")
        .expect("nested uv.lock?as=toml should appear as a leaf import with empty sub-imports");
    assert_eq!(lock["buck.imports"], json!([]));

    let direct = files.get("root//direct.lock").expect(
        "top-level direct.lock?as=toml should appear as a leaf import with empty sub-imports",
    );
    assert_eq!(direct["buck.imports"], json!([]));
    Ok(())
}

/// `*` in `buck2.load_as_allowlist` allows every file.
#[buck_test]
async fn test_imports_as_toml_star_allows_all(buck: &Buck) -> anyhow::Result<()> {
    let result = buck
        .targets(&[
            "//...",
            "--json",
            "--streaming",
            "--imports",
            "-c",
            "buck2.load_as_allowlist=*",
        ])
        .await?;
    let xs: Vec<Value> = serde_json::from_str(&result.stdout)?;
    let files = import_records(&xs);
    assert_eq!(
        files.get("root//uv.lock").map(|x| &x["buck.imports"]),
        Some(&json!([])),
        "uv.lock?as=toml should load when all extensions are allowed via `*`"
    );
    Ok(())
}

/// An entry in `buck2.load_as_allowlist` may be a whole file name
/// (e.g. `uv.lock`), not just an extension, so specific package-manager
/// lockfiles can be allowed without opening up every `*.lock` file.
#[buck_test]
async fn test_imports_as_toml_whole_filename_allowlist(buck: &Buck) -> anyhow::Result<()> {
    let result = buck
        .targets(&[
            "//...",
            "--json",
            "--streaming",
            "--imports",
            "-c",
            "buck2.load_as_allowlist=uv.lock,direct.lock",
        ])
        .await?;
    let xs: Vec<Value> = serde_json::from_str(&result.stdout)?;
    let files = import_records(&xs);
    assert!(files.contains_key("root//uv.lock"));
    assert!(files.contains_key("root//direct.lock"));
    Ok(())
}
