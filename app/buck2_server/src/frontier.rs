/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! `buck2 debug hydration frontier-prime` and `frontier-shadow`: phases 2 and 3 of sharing DICE
//! nodes between daemons. The primer writes records of the covered node kinds to a local
//! directory store; the shadow reader checks each record against this daemon's own leaves and
//! values, and installs nothing. Both need `frontier.mode` set, and paging on.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

use buck2_build_api::actions::calculation::BuildKey;
use buck2_build_api::actions::local_actions::LocalActions;
use buck2_common::frontier::Canon;
use buck2_common::frontier::Inspected;
use buck2_common::frontier::LeafId;
use buck2_common::frontier::hex;
use buck2_common::legacy_configs::configs::LegacyBuckConfig;
use buck2_common::legacy_configs::dice::HasLegacyConfigs;
use buck2_common::dice::cells::HasCellResolver;
use buck2_common::settings::settings::FrontierMode;
use buck2_common::settings::settings::FrontierSection;
use buck2_core::fs::project::ProjectRoot;
use buck2_error::ErrorTag;
use buck2_error::buck2_error;
use buck2_frontier::body::Digest;
use buck2_frontier::body::FORMAT;
use buck2_frontier::body::LeafOracle;
use buck2_frontier::policy::DataClass;
use buck2_frontier::policy::classify;
use buck2_frontier::policy::is_record_kind;
use buck2_frontier::store::Manifest;
use buck2_frontier::store::StoreReader;
use buck2_frontier::store::differing_components;
use buck2_frontier::walk::LeafValue;
use buck2_frontier::walk::Node;
use buck2_frontier::walk::Role;
use dice::Dice;
use dice::DiceData;
use dice::DiceTransaction;
use dice::FrontierInspect;
use dice::FrontierSource;

/// What the commands of this daemon put in their data, since it started: the union over every
/// command, so that a prime after a build sees what the build set.
#[derive(Default, Clone, Debug)]
pub(crate) struct CommandData {
    pub(crate) types: BTreeSet<&'static str>,
    pub(crate) skip_cache_read: bool,
    pub(crate) skip_cache_write: bool,
    pub(crate) strategies: BTreeSet<String>,
    pub(crate) upload_all: bool,
}

static COMMAND_DATA: Mutex<Option<CommandData>> = Mutex::new(None);

/// Called for every command while the frontier is on, with the data `ctx.rs` built.
pub(crate) fn note_command_data(
    data: &DiceData,
    skip_cache_read: bool,
    skip_cache_write: bool,
    strategy: String,
    upload_all: bool,
) {
    let mut g = COMMAND_DATA.lock().unwrap();
    let c = g.get_or_insert_with(CommandData::default);
    c.types.extend(data.type_names());
    c.skip_cache_read |= skip_cache_read;
    c.skip_cache_write |= skip_cache_write;
    c.strategies.insert(strategy);
    c.upload_all |= upload_all;
}

fn command_data() -> CommandData {
    COMMAND_DATA.lock().unwrap().clone().unwrap_or_default()
}

/// The command data's part of the version, or why the frontier is off for it.
fn command_data_digest(c: &CommandData) -> Result<String, String> {
    let unclassified: Vec<&str> = c
        .types
        .iter()
        .copied()
        .filter(|t| classify(t).is_none())
        .collect();
    if !unclassified.is_empty() {
        return Err(format!(
            "unclassified command data: {}",
            unclassified.join(", ")
        ));
    }
    let mut h = blake3::Hasher::new();
    for t in &c.types {
        match classify(t) {
            None => unreachable!("checked above"),
            Some(DataClass::Version) => {
                h.update(t.as_bytes());
                h.update(&[0]);
            }
            Some(_) => {}
        }
    }
    for s in &c.strategies {
        h.update(s.as_bytes());
        h.update(&[0]);
    }
    h.update(&[c.upload_all as u8]);
    Ok(hex(h.finalize().as_bytes()))
}

/// What the frontier walk learnt about a node, beyond `buck2_common`'s leaves.
enum Seen {
    Common(Inspected),
    Build { local: bool },
}

struct Inspector<'a> {
    wanted: HashMap<&'static str, bool>,
    local: Option<&'a LocalActions>,
}

impl<'a> Inspector<'a> {
    fn new(local: Option<&'a LocalActions>) -> Self {
        let mut wanted: HashMap<&'static str, bool> = HashMap::new();
        for t in buck2_common::file_ops::dice::frontier_file_key_types() {
            wanted.insert(t, true);
        }
        // A `ReadFileKey`'s value is a handle; its leaf is computed through `PathMetadataKey`.
        wanted.insert(buck2_common::file_ops::dice::frontier_file_key_types()[0], false);
        wanted.insert(
            buck2_common::file_ops::baseline::frontier_baseline_key_type(),
            false,
        );
        wanted.insert(buck2_common::dice::cells::frontier_cell_key_types()[0], true);
        for t in buck2_common::legacy_configs::dice::frontier_config_key_types() {
            wanted.insert(t, false);
        }
        // The error projection's value is the leaf.
        wanted.insert(
            buck2_common::legacy_configs::dice::frontier_config_key_types()[1],
            true,
        );
        for t in [
            buck2_interpreter_for_build::interpreter::context::frontier_build_context_key_type(),
            buck2_build_api::context::frontier_build_data_key_type(),
            buck2_interpreter::dice::starlark_types::frontier_starlark_types_key_type(),
            buck2_interpreter::starlark_profiler::config::frontier_profiler_key_type(),
            buck2_validation::enabled_optional_validations_key::frontier_validations_key_type(),
        ] {
            wanted.insert(t, true);
        }
        wanted.insert(<BuildKey as dice::Key>::key_type_name(), false);
        Inspector { wanted, local }
    }
}

impl FrontierInspect for Inspector<'_> {
    fn wants(&self, key_type: &'static str) -> Option<bool> {
        self.wanted.get(key_type).copied()
    }

    fn inspect(
        &self,
        key: &dyn std::any::Any,
        base: Option<&dyn std::any::Any>,
        value: Option<&dyn std::any::Any>,
    ) -> Option<Box<dyn std::any::Any + Send + Sync>> {
        if let Some(k) = key.downcast_ref::<BuildKey>() {
            let local = self.local.is_some_and(|l| l.contains(&k.0));
            return Some(Box::new(Seen::Build { local }));
        }
        let found = buck2_common::file_ops::dice::frontier_inspect_file_op(key, value)
            .or_else(|| buck2_common::file_ops::baseline::frontier_inspect_baseline(key))
            .or_else(|| buck2_common::dice::cells::frontier_inspect_cells(key, value))
            .or_else(|| {
                buck2_common::legacy_configs::dice::frontier_inspect_config(key, base, value)
            })
            .or_else(|| {
                buck2_interpreter_for_build::interpreter::context::frontier_inspect_build_context(
                    key, value,
                )
            })
            .or_else(|| buck2_build_api::context::frontier_inspect_build_data(key, value))
            .or_else(|| {
                buck2_interpreter::dice::starlark_types::frontier_inspect_starlark_types(
                    key, value,
                )
            })
            .or_else(|| {
                buck2_interpreter::starlark_profiler::config::frontier_inspect_profiler(key, value)
            })
            .or_else(|| {
                buck2_validation::enabled_optional_validations_key::frontier_inspect_validations(
                    key, value,
                )
            })?;
        Some(Box::new(Seen::Common(found)))
    }
}

/// A daemon's graph as the walk sees it, with the leaf values the graph itself held.
struct Walked {
    nodes: Vec<Node>,
    /// Leaf values read from the graph's own nodes.
    leaves: HashMap<LeafId, Canon>,
    /// Leaves whose value is computed through DICE (a `ReadFileKey`'s path, config leaves).
    to_compute: BTreeSet<LeafId>,
    /// `(kind, key id)` to value row, for local-out leaves.
    rows: HashMap<(String, String), String>,
    keys: usize,
    candidates: BTreeMap<&'static str, usize>,
}

async fn walk(dice: &Arc<Dice>, tx: &DiceTransaction) -> buck2_error::Result<Walked> {
    let local = LocalActions::of(tx.global_data());
    let inspector = Inspector::new(local);
    let graph = dice
        .frontier_graph(tx.version(), &inspector)
        .await
        .map_err(|e| buck2_error!(ErrorTag::Tier0, "reading the graph: {e:#}"))?;
    let mut nodes = Vec::with_capacity(graph.nodes.len());
    let mut leaves = HashMap::new();
    let mut to_compute = BTreeSet::new();
    let mut rows = HashMap::new();
    for n in graph.nodes {
        let seen = n.info.and_then(|i| i.downcast::<Seen>().ok()).map(|b| *b);
        let role = match seen {
            Some(Seen::Common(Inspected::Leaf { id, canon })) => {
                match canon {
                    Some(c) => {
                        leaves.insert(id.clone(), c);
                    }
                    None => {
                        to_compute.insert(id.clone());
                    }
                }
                if matches!(id, LeafId::Config { .. } | LeafId::Sections { .. }) {
                    to_compute.insert(id.clone());
                }
                Role::Leaf(id)
            }
            Some(Seen::Common(Inspected::Barrier(r))) => Role::Barrier(r),
            Some(Seen::Common(Inspected::Skip)) => Role::Skip,
            Some(Seen::Build { local: true }) => Role::LocalOut,
            Some(Seen::Build { local: false }) => Role::Inner,
            None => match n.source {
                FrontierSource::Asserted { .. } => Role::Barrier(format!(
                    "injected key without a frontier digest: {}",
                    n.key_type
                )),
                FrontierSource::Certified => Role::Inner,
            },
        };
        if let (Some(k), Some(v)) = (&n.key_id, &n.value_row) {
            rows.insert((n.key_type.to_owned(), hex(k)), hex(v));
        }
        nodes.push(Node {
            kind: n.key_type.to_owned(),
            key_id: n.key_id,
            value_row: n.value_row,
            deps: n.deps,
            deps_complete: n.deps_complete,
            role,
        });
    }
    let keys = nodes.len();
    Ok(Walked {
        nodes,
        leaves,
        to_compute,
        rows,
        keys,
        candidates: graph.candidates,
    })
}

/// Paths the writer's checkout tracks, absolute: what a config origin must be entirely inside.
fn tracked_files(root: &Path) -> HashSet<PathBuf> {
    let out = std::process::Command::new("git")
        .arg("-C")
        .arg(root)
        .args(["ls-files", "-z", "--full-name"])
        .stderr(std::process::Stdio::null())
        .output();
    let mut set = HashSet::new();
    if let Ok(out) = out
        && out.status.success()
    {
        for p in out.stdout.split(|b| *b == 0) {
            if !p.is_empty() {
                set.insert(root.join(String::from_utf8_lossy(p).as_ref()));
            }
        }
    }
    set
}

/// Computes leaves through this daemon's DICE. `writer` decides whether a config value from an
/// untracked origin is named by property alone.
async fn compute_leaves(
    tx: &DiceTransaction,
    ids: impl IntoIterator<Item = LeafId>,
    root: &Path,
    writer: bool,
) -> buck2_error::Result<HashMap<LeafId, LeafValue>> {
    let tracked = tracked_files(root);
    let is_tracked = |f: &str| {
        let p = Path::new(f);
        let p = if p.is_absolute() {
            p.to_path_buf()
        } else {
            root.join(p)
        };
        let p = std::fs::canonicalize(&p).unwrap_or(p);
        tracked.contains(&p)
            || tracked
                .iter()
                .any(|t| std::fs::canonicalize(t).ok().as_deref() == Some(p.as_path()))
    };
    let mut ctx = tx.ctx();
    let mut out = HashMap::new();
    for id in ids {
        let v = match &id {
            LeafId::Path { .. } | LeafId::Listing { .. } | LeafId::Exists { .. } => {
                match buck2_common::file_ops::dice::frontier_compute_file_leaf(&mut ctx, &id).await
                {
                    Ok(Some(c)) => LeafValue::Canon(c),
                    Ok(None) => LeafValue::Unavailable(id.kind().to_owned()),
                    Err(e) => LeafValue::Unavailable(format!("{}: {e}", id.kind())),
                }
            }
            LeafId::Config { .. } | LeafId::Sections { .. } => {
                match buck2_common::legacy_configs::dice::frontier_config_leaf(
                    &mut ctx,
                    &id,
                    &is_tracked,
                )
                .await
                {
                    Ok(Some((c, Ok(())))) => LeafValue::Canon(c),
                    Ok(Some((_, Err(property)))) if writer => LeafValue::External(property),
                    Ok(Some((c, Err(_)))) => LeafValue::Canon(c),
                    Ok(None) => LeafValue::Unavailable(id.kind().to_owned()),
                    Err(e) => LeafValue::Unavailable(format!("{}: {e}", id.kind())),
                }
            }
            LeafId::ConfigError { .. } => {
                LeafValue::Unavailable("config error leaf not in this graph".to_owned())
            }
            _ => LeafValue::Unavailable(format!("{} not in this graph", id.kind())),
        };
        out.insert(id, v);
    }
    Ok(out)
}

/// Names that look like credentials: left out of the version entirely, never hashed.
fn credential_like(section: &str, property: &str) -> bool {
    let s = format!("{section}.{property}").to_ascii_lowercase();
    [
        "key", "token", "secret", "password", "passwd", "auth", "credential", "header", "cert",
        "tls", "cookie",
    ]
    .iter()
    .any(|w| s.contains(w))
}

fn config_digest(config: &LegacyBuckConfig) -> String {
    let mut h = blake3::Hasher::new();
    for (section, values) in config.iter() {
        for (key, value) in values {
            if credential_like(section, key) {
                continue;
            }
            for part in [section, key, value] {
                h.update(part.as_bytes());
                h.update(&[0]);
            }
        }
    }
    hex(h.finalize().as_bytes())
}

/// The frontier version: a digest of named components, each kept in the manifest by name.
async fn version(
    tx: &DiceTransaction,
    project_root: &ProjectRoot,
    command: &Result<String, String>,
    daemon_settings: &str,
) -> buck2_error::Result<Manifest> {
    let mut ctx = tx.ctx();
    let root_cell = ctx.get_cell_resolver().await?.root_cell();
    let config = ctx.get_legacy_config_for_cell(root_cell).await?;
    let (env, run) = crate::suspend::frontier_env_digests();
    let mut components = BTreeMap::new();
    components.insert("format".to_owned(), FORMAT.to_string());
    components.insert("binary".to_owned(), crate::suspend::frontier_exe_digest()?);
    components.insert(
        "system".to_owned(),
        buck2_common::suspend_image::system_fingerprint(),
    );
    components.insert("startup_config".to_owned(), config_digest(config));
    components.insert("environment".to_owned(), env);
    components.insert("environment_run".to_owned(), run);
    components.insert(
        "command_data".to_owned(),
        match command {
            Ok(d) => d.clone(),
            Err(e) => format!("off: {e}"),
        },
    );
    components.insert("daemon_settings".to_owned(), daemon_settings.to_owned());
    components.insert(
        "project_root".to_owned(),
        project_root.root().to_string(),
    );
    let mut h = blake3::Hasher::new();
    for (k, v) in &components {
        h.update(k.as_bytes());
        h.update(&[0]);
        h.update(v.as_bytes());
        h.update(&[0]);
    }
    let version = hex(h.finalize().as_bytes());
    Ok(Manifest {
        format: FORMAT,
        version,
        components,
    })
}

pub(crate) struct FrontierInputs<'a> {
    pub(crate) dice: &'a Arc<Dice>,
    pub(crate) transaction: &'a DiceTransaction,
    pub(crate) settings: &'a FrontierSection,
    pub(crate) project_root: &'a ProjectRoot,
    pub(crate) store: Option<String>,
    pub(crate) commit: Option<String>,
    pub(crate) log_dir: PathBuf,
    pub(crate) trace_id: String,
    /// A digest of the daemon's settings other than `frontier` (package visibility and the
    /// like reach node values through the command data).
    pub(crate) daemon_settings: String,
}

fn store_and_commit(inputs: &FrontierInputs<'_>) -> buck2_error::Result<(PathBuf, String)> {
    let store = inputs
        .store
        .clone()
        .or_else(|| inputs.settings.store().map(|s| s.to_owned()))
        .ok_or_else(|| {
            buck2_error!(
                ErrorTag::Input,
                "the frontier needs a store: `--store` or `frontier.store`"
            )
        })?;
    let commit = inputs
        .commit
        .clone()
        .or_else(|| inputs.settings.base_commit().map(|s| s.to_owned()))
        .ok_or_else(|| {
            buck2_error!(
                ErrorTag::Input,
                "the frontier needs a commit: `--commit` or `frontier.base_commit`"
            )
        })?;
    Ok((PathBuf::from(store), commit))
}

pub(crate) async fn prime(inputs: FrontierInputs<'_>) -> buck2_error::Result<String> {
    if inputs.settings.mode() != FrontierMode::Prime {
        return Err(buck2_error!(
            ErrorTag::Input,
            "`frontier-prime` needs `frontier.mode = prime`"
        ));
    }
    let (store, commit) = store_and_commit(&inputs)?;
    let data = command_data();
    if data.skip_cache_write {
        return Err(buck2_error!(
            ErrorTag::Input,
            "a command of this daemon was told not to write caches: no records"
        ));
    }
    let command = command_data_digest(&data);
    if let Err(e) = &command {
        return Err(buck2_error!(ErrorTag::Input, "frontier off: {e}"));
    }
    let start = Instant::now();
    let walked = walk(inputs.dice, inputs.transaction).await?;
    let walk_time = start.elapsed();
    // A covered node that is dirty at this version was invalidated after the build (a file or
    // config change, or a prime run with other `-c` arguments): its record would describe a
    // graph that no longer stands, so the primer refuses rather than leave it out silently.
    let narrowed_kinds = inputs.settings.kinds();
    let dirty: Vec<String> = walked
        .candidates
        .iter()
        .filter(|(k, _)| is_record_kind(k, narrowed_kinds.as_deref()))
        .map(|(k, n)| format!("{n} {k}"))
        .collect();
    if !dirty.is_empty() {
        return Err(buck2_error!(
            ErrorTag::Input,
            "the graph moved since the build (dirty covered nodes: {}); prime right after the \
             build, with the same config",
            dirty.join(", ")
        ));
    }
    let mut leaves: HashMap<LeafId, LeafValue> = walked
        .leaves
        .iter()
        .map(|(k, v)| (k.clone(), LeafValue::Canon(v.clone())))
        .collect();
    let computed = compute_leaves(
        inputs.transaction,
        walked.to_compute.iter().cloned(),
        inputs.project_root.root().as_path(),
        true,
    )
    .await?;
    leaves.extend(computed);
    let manifest = version(
        inputs.transaction,
        inputs.project_root,
        &command,
        &inputs.daemon_settings,
    )
    .await?;
    let narrowed = inputs.settings.kinds();
    let covered = |k: &str| is_record_kind(k, narrowed.as_deref());
    let exec_class = |n: &Node| {
        (n.kind == <BuildKey as dice::Key>::key_type_name()).then(|| {
            if n.role == Role::LocalOut {
                "local".to_owned()
            } else {
                "remote-or-cache".to_owned()
            }
        })
    };
    let inv_start = Instant::now();
    let primed = buck2_frontier::walk::prime(
        &walked.nodes,
        &leaves,
        &manifest.version,
        &covered,
        &exec_class,
        &commit,
    );
    let inv_time = inv_start.elapsed();
    let bytes = buck2_frontier::store::write(&store, &manifest, &commit, &primed)
        .map_err(|e| buck2_error!(ErrorTag::Environment, "writing the store: {e:#}"))?;
    let mut by_kind: BTreeMap<&str, usize> = BTreeMap::new();
    for r in primed.records.values() {
        *by_kind.entry(&r.kind).or_default() += 1;
    }
    let mut reasons: BTreeMap<String, usize> = BTreeMap::new();
    for (_, _, r) in &primed.unshareable {
        *reasons.entry(r.clone()).or_default() += 1;
    }
    let mut out = format!(
        "frontier prime: {} keys, {} records, {} bodies, {} unshareable; version {}\n",
        walked.keys,
        primed.records.len(),
        primed.bodies.len(),
        primed.unshareable.len(),
        &manifest.version[..16]
    );
    out.push_str(&format!(
        "times: walk {:.3}s invalidation {:.3}s\n",
        walk_time.as_secs_f64(),
        inv_time.as_secs_f64()
    ));
    out.push_str("bytes:");
    for (k, v) in &bytes {
        out.push_str(&format!(" {k} {v}"));
    }
    out.push('\n');
    for (k, n) in &by_kind {
        out.push_str(&format!("  records: {n} {k}\n"));
    }
    for (r, n) in &reasons {
        out.push_str(&format!("  unshareable: {n} ({r})\n"));
    }
    Ok(out)
}

struct Oracle {
    leaves: HashMap<LeafId, Canon>,
    rows: HashMap<(String, String), String>,
    version: Digest,
}

impl LeafOracle for Oracle {
    fn leaf(&self, id: &LeafId) -> Option<Canon> {
        match id {
            LeafId::LocalOut { key_type, key_id } => {
                self.rows.get(&(key_type.clone(), key_id.clone())).cloned()
            }
            _ => self.leaves.get(id).cloned(),
        }
    }

    fn version(&self) -> Digest {
        self.version.clone()
    }
}

pub(crate) async fn shadow(inputs: FrontierInputs<'_>) -> buck2_error::Result<String> {
    if inputs.settings.mode() != FrontierMode::Shadow {
        return Err(buck2_error!(
            ErrorTag::Input,
            "`frontier-shadow` needs `frontier.mode = shadow`"
        ));
    }
    let (store, commit) = store_and_commit(&inputs)?;
    let data = command_data();
    if data.skip_cache_read {
        return Ok(
            "frontier shadow: off, a command of this daemon was told not to read caches\n"
                .to_owned(),
        );
    }
    let command = command_data_digest(&data);
    if let Err(e) = &command {
        return Ok(format!("frontier shadow: off, {e}\n"));
    }
    let walked = walk(inputs.dice, inputs.transaction).await?;
    let manifest = version(
        inputs.transaction,
        inputs.project_root,
        &command,
        &inputs.daemon_settings,
    )
    .await?;
    let narrowed = inputs.settings.kinds();
    let covered = |k: &str| is_record_kind(k, narrowed.as_deref());
    let reader = StoreReader::open(&store, &manifest.version, &commit)
        .map_err(|e| buck2_error!(ErrorTag::Environment, "opening the store: {e:#}"))?;
    let mut version_note = String::new();
    let mut version_differs = false;
    if !reader.has_pointer() {
        match StoreReader::primed_manifest(&store, &commit)
            .map_err(|e| buck2_error!(ErrorTag::Environment, "reading the store: {e:#}"))?
        {
            Some(theirs) => {
                version_differs = true;
                version_note = format!(
                    "version differs from the primer's in: {}\n",
                    differing_components(&theirs, &manifest).join(", ")
                )
            }
            None => version_note = format!("commit {commit} was never primed\n"),
        }
    }
    let needed = buck2_frontier::walk::needed_leaves(
        &walked.nodes,
        &manifest.version,
        &covered,
        &reader,
    )
    .map_err(|e| buck2_error!(ErrorTag::Environment, "reading the store: {e:#}"))?;
    let mut leaves = walked.leaves.clone();
    // Config leaves are always recomputed: the graph holds their key, not their value.
    let missing: Vec<LeafId> = needed
        .into_iter()
        .filter(|id| {
            !leaves.contains_key(id)
                || matches!(id, LeafId::Config { .. } | LeafId::Sections { .. })
        })
        .filter(|id| !matches!(id, LeafId::LocalOut { .. }))
        .collect();
    let computed = compute_leaves(
        inputs.transaction,
        missing,
        inputs.project_root.root().as_path(),
        false,
    )
    .await?;
    for (id, v) in computed {
        match v {
            LeafValue::Canon(c) => {
                leaves.insert(id, c);
            }
            _ => {
                leaves.remove(&id);
            }
        }
    }
    let oracle = Oracle {
        leaves,
        rows: walked.rows.clone(),
        version: manifest.version.clone(),
    };
    let mut entries = buck2_frontier::walk::shadow(&walked.nodes, &oracle, &covered, &reader)
        .map_err(|e| buck2_error!(ErrorTag::Environment, "shadow pass: {e:#}"))?;
    if version_differs {
        // The commit was primed, under another version: every lookup misses for that reason.
        for e in &mut entries {
            if e.outcome == buck2_frontier::walk::Outcome::Miss {
                e.outcome = buck2_frontier::walk::Outcome::Version;
            }
        }
    }
    let summary = buck2_frontier::walk::summarize(&entries);
    std::fs::create_dir_all(&inputs.log_dir)?;
    let report_path = inputs
        .log_dir
        .join(format!("frontier-shadow-{}.json", inputs.trace_id));
    let report = serde_json::json!({
        "base_commit": commit,
        "version": manifest.version,
        "components": manifest.components,
        "version_note": version_note,
        "summary": summary,
        "entries": entries,
    });
    std::fs::write(&report_path, serde_json::to_vec_pretty(&report).unwrap())?;
    let mut totals: BTreeMap<String, usize> = BTreeMap::new();
    for e in &entries {
        *totals.entry(e.outcome.label()).or_default() += 1;
    }
    let mut out = format!(
        "frontier shadow: {} covered nodes:{}\n",
        entries.len(),
        totals
            .iter()
            .map(|(k, v)| format!(" {k} {v}"))
            .collect::<Vec<_>>()
            .join(",")
    );
    out.push_str(&version_note);
    for (kind, outcomes) in &summary {
        out.push_str(&format!(
            "  {kind}:{}\n",
            outcomes
                .iter()
                .map(|(k, v)| format!(" {k} {v}"))
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    for e in &entries {
        if let Some(leaf) = &e.leaf {
            out.push_str(&format!(
                "  {} {} {}: {}\n",
                e.kind,
                &e.key_id[..12.min(e.key_id.len())],
                e.outcome.label(),
                serde_json::to_string(leaf).unwrap()
            ));
        }
    }
    out.push_str(&format!("report: {}\n", report_path.display()));
    Ok(out)
}
