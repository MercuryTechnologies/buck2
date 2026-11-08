/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! josh: the hash an anon target's outputs are placed under when the map is active.
//!
//! An anon target's output path is `art-anon/<cell>/<exec cfg hash><pkg>/<hash>/__<name>__/...`,
//! where `<hash>` is the target's strong hash over its rule's import path, its name and its
//! attributes, all of which name real cells. In the tree's own home (josh off, or the identity
//! map) that hash is the path hash, unchanged. When the map is active, the rule type, name and
//! attributes are rebuilt with canonical cell names and hashed by the same code as
//! [`crate::anon_target_node::AnonTarget::new`], so the imported tree's anon target lands where
//! it does in its home. Configurations hash by their output hash, which is canonical already.
//!
//! DICE and the target's identity keep the real strong hash; only the path changes.
//!
//! When a part cannot be rebuilt (a BXL rule or variant, an arg with macros, a promise, an
//! artifact owned by anything but a target label), the whole hash falls back to the real strong
//! hash under a josh salt. That costs a miss between homes; the salt keeps it from colliding with
//! a canonical hash computed for another target in the same daemon.

use std::hash::Hash;
use std::hash::Hasher;

use buck2_artifact::actions::key::ActionKey;
use buck2_artifact::artifact::artifact_type::Artifact;
use buck2_artifact::artifact::artifact_type::BaseArtifactKind;
use buck2_artifact::artifact::build_artifact::BuildArtifact;
use buck2_artifact::artifact::source_artifact::SourceArtifact;
use buck2_core::bzl::ImportPath;
use buck2_core::cells::build_file_cell::BuildFileCell;
use buck2_core::cells::cell_path::CellPath;
use buck2_core::configuration::pair::ConfigurationNoExec;
use buck2_core::deferred::base_deferred_key::BaseDeferredKey;
use buck2_core::deferred::key::DeferredHolderKey;
use buck2_core::fs::buck_out_path::BuildArtifactPath;
use buck2_core::josh::JoshMap;
use buck2_core::package::PackageLabel;
use buck2_core::package::source_path::SourcePath;
use buck2_core::provider::label::ConfiguredProvidersLabel;
use buck2_core::provider::label::ProvidersLabel;
use buck2_core::target::configured_target_label::ConfiguredTargetLabel;
use buck2_core::target::label::label::TargetLabel;
use buck2_node::attrs::attr_type::dep::DepAttr;
use buck2_node::attrs::attr_type::dict::DictLiteral;
use buck2_node::attrs::attr_type::list::ListLiteral;
use buck2_node::attrs::attr_type::tuple::TupleLiteral;
use buck2_node::bzl_or_bxl_path::BzlOrBxlPath;
use buck2_node::rule_type::StarlarkRuleType;
use buck2_util::arc_str::ArcSlice;
use buck2_util::strong_hasher::Blake3StrongHasher;
use dupe::Dupe;
use starlark_map::sorted_map::SortedMap;

use crate::anon_target_attr::AnonTargetAttr;
use crate::anon_target_node::AnonTargetVariant;

/// The path hash of an anon target whose real strong hash is `strong_hash`.
pub(crate) fn josh_path_hash(
    rule_type: &StarlarkRuleType,
    name: &TargetLabel,
    attrs: &SortedMap<String, AnonTargetAttr>,
    exec_cfg: &ConfigurationNoExec,
    variant: &AnonTargetVariant,
    strong_hash: u64,
) -> u64 {
    let Some(josh) = buck2_core::josh::active() else {
        return strong_hash;
    };
    match canonical_strong_hash(josh, rule_type, name, attrs, exec_cfg, variant) {
        Some(h) => h,
        None => {
            let mut h = Blake3StrongHasher::new();
            "josh-unshared-anon".hash(&mut h);
            strong_hash.hash(&mut h);
            h.finish()
        }
    }
}

/// What `AnonTarget::new` computes as the strong hash, over the canonical values.
pub(crate) fn canonical_strong_hash(
    josh: &JoshMap,
    rule_type: &StarlarkRuleType,
    name: &TargetLabel,
    attrs: &SortedMap<String, AnonTargetAttr>,
    exec_cfg: &ConfigurationNoExec,
    variant: &AnonTargetVariant,
) -> Option<u64> {
    if !matches!(variant, AnonTargetVariant::Bzl) {
        return None;
    }
    let rule_type = StarlarkRuleType {
        path: canonical_rule_path(josh, &rule_type.path)?,
        name: rule_type.name.clone(),
    };
    let name = target_label(josh, name)?;
    let attrs: SortedMap<String, AnonTargetAttr> = attrs
        .iter()
        .map(|(k, v)| Some((k.clone(), attr(josh, v)?)))
        .collect::<Option<Vec<_>>>()?
        .into_iter()
        .collect();
    let mut h = Blake3StrongHasher::new();
    rule_type.hash(&mut h);
    name.hash(&mut h);
    attrs.hash(&mut h);
    exec_cfg.hash(&mut h);
    variant.hash(&mut h);
    Some(h.finish())
}

fn canonical_rule_path(josh: &JoshMap, p: &BzlOrBxlPath) -> Option<BzlOrBxlPath> {
    let BzlOrBxlPath::Bzl(p) = p else {
        return None;
    };
    let path = CellPath::new(
        josh.canonical_cell_name(p.path().cell()),
        p.path().path().to_buf(),
    );
    let import = match p.format_override() {
        None if p.build_file_cell().name() == p.path().cell() => {
            ImportPath::new_same_cell(path).ok()?
        }
        None => return None,
        Some(format) => ImportPath::new_with_explicit_format(
            path,
            BuildFileCell::new(josh.canonical_cell_name(p.build_file_cell().name())),
            format,
        )
        .ok()?,
    };
    Some(BzlOrBxlPath::Bzl(import))
}

fn package(josh: &JoshMap, p: PackageLabel) -> Option<PackageLabel> {
    let cell = josh.canonical_cell_name(p.cell_name());
    if cell == p.cell_name() {
        return Some(p);
    }
    PackageLabel::new(cell, p.cell_relative_path()).ok()
}

fn target_label(josh: &JoshMap, t: &TargetLabel) -> Option<TargetLabel> {
    Some(TargetLabel::new(package(josh, t.pkg())?, t.name()))
}

fn configured_target_label(
    josh: &JoshMap,
    t: &ConfiguredTargetLabel,
) -> Option<ConfiguredTargetLabel> {
    Some(target_label(josh, t.unconfigured())?.configure_pair(t.cfg_pair().dupe()))
}

fn attr(josh: &JoshMap, a: &AnonTargetAttr) -> Option<AnonTargetAttr> {
    Some(match a {
        AnonTargetAttr::Bool(_)
        | AnonTargetAttr::Int(_)
        | AnonTargetAttr::String(_)
        | AnonTargetAttr::EnumVariant(_)
        | AnonTargetAttr::None => a.clone(),
        AnonTargetAttr::List(l) => AnonTargetAttr::List(ListLiteral(
            l.iter()
                .map(|x| attr(josh, x))
                .collect::<Option<Vec<_>>>()
                .map(ArcSlice::from)?,
        )),
        AnonTargetAttr::Tuple(l) => AnonTargetAttr::Tuple(TupleLiteral(
            l.iter()
                .map(|x| attr(josh, x))
                .collect::<Option<Vec<_>>>()
                .map(ArcSlice::from)?,
        )),
        AnonTargetAttr::Dict(d) => AnonTargetAttr::Dict(DictLiteral(
            d.iter()
                .map(|(k, v)| Some((attr(josh, k)?, attr(josh, v)?)))
                .collect::<Option<Vec<_>>>()
                .map(ArcSlice::from)?,
        )),
        AnonTargetAttr::OneOf(x, i) => AnonTargetAttr::OneOf(Box::new(attr(josh, x)?), *i),
        AnonTargetAttr::Dep(d) => AnonTargetAttr::Dep(Box::new(DepAttr {
            attr_type: d.attr_type.clone(),
            label: ConfiguredProvidersLabel::new(
                configured_target_label(josh, d.label.target())?,
                d.label.name().clone(),
            ),
        })),
        AnonTargetAttr::Label(l) => AnonTargetAttr::Label(ProvidersLabel::new(
            target_label(josh, l.target())?,
            l.name().clone(),
        )),
        AnonTargetAttr::Artifact(x) => AnonTargetAttr::Artifact(artifact(josh, x)?),
        AnonTargetAttr::PromiseArtifact(_) | AnonTargetAttr::Arg(_) => return None,
    })
}

fn artifact(josh: &JoshMap, a: &Artifact) -> Option<Artifact> {
    let kind = a.data();
    let base = match &kind.base {
        BaseArtifactKind::Source(s) => {
            let p = s.get_path();
            BaseArtifactKind::Source(SourceArtifact::new(SourcePath::new(
                package(josh, p.package())?,
                p.path().to_arc(),
            )))
        }
        BaseArtifactKind::Build(b) => {
            let path = b.get_path();
            let DeferredHolderKey::Base(BaseDeferredKey::TargetLabel(t)) = path.owner() else {
                return None;
            };
            if b.key().holder_key() != path.owner() {
                return None;
            }
            let owner =
                DeferredHolderKey::Base(BaseDeferredKey::TargetLabel(configured_target_label(josh, t)?));
            let cpath = BuildArtifactPath::with_dynamic_actions_action_key(
                owner.clone(),
                path.path().to_buf(),
                path.path_resolution_method(),
            );
            BaseArtifactKind::Build(
                BuildArtifact::new(cpath, ActionKey::new(owner, b.key().action_index()), b.output_type())
                    .ok()?,
            )
        }
    };
    Some(Artifact::new(
        base,
        kind.path.dupe(),
        a.get_path().hidden_components_count,
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use buck2_artifact::artifact::artifact_type::Artifact;
    use buck2_artifact::artifact::source_artifact::SourceArtifact;
    use buck2_core::bzl::ImportPath;
    use buck2_core::cells::cell_root_path::CellRootPathBuf;
    use buck2_core::cells::name::CellName;
    use buck2_core::configuration::data::ConfigurationData;
    use buck2_core::configuration::pair::ConfigurationNoExec;
    use buck2_core::deferred::base_deferred_key::BaseDeferredKeyDyn;
    use buck2_core::josh::JoshMap;
    use buck2_core::package::source_path::SourcePath;
    use buck2_core::provider::label::ProvidersLabel;
    use buck2_core::provider::label::ProvidersName;
    use buck2_core::target::label::label::TargetLabel;
    use buck2_node::attrs::attr_type::string::StringLiteral;
    use buck2_node::bzl_or_bxl_path::BzlOrBxlPath;
    use buck2_node::rule_type::StarlarkRuleType;
    use starlark_map::sorted_map::SortedMap;

    use crate::anon_target_attr::AnonTargetAttr;
    use crate::anon_target_node::AnonTarget;
    use crate::anon_target_node::AnonTargetVariant;

    fn lab_map() -> JoshMap {
        let cells = vec![
            (CellName::testing_new("root"), CellRootPathBuf::testing_new("")),
            (
                CellName::testing_new("mwb"),
                CellRootPathBuf::testing_new("services/web-backend"),
            ),
        ];
        JoshMap::new(
            &[
                ("cell.mwb".to_owned(), "root:.".to_owned()),
                ("cell.root".to_owned(), "lab:.josh/lab".to_owned()),
            ],
            &cells,
            &|_| false,
        )
        .unwrap()
    }

    type Parts = (
        Arc<StarlarkRuleType>,
        TargetLabel,
        SortedMap<String, AnonTargetAttr>,
        ConfigurationNoExec,
    );

    /// One anon target's parts as cell `cell` names them.
    fn parts(cell: &str) -> Parts {
        let rule_type = Arc::new(StarlarkRuleType {
            path: BzlOrBxlPath::Bzl(ImportPath::testing_new(&format!("{cell}//rules:defs.bzl"))),
            name: "_builder".to_owned(),
        });
        let name = TargetLabel::testing_parse(&format!("{cell}//pkg:s_anon"));
        let attrs = SortedMap::from_iter([
            (
                "label".to_owned(),
                AnonTargetAttr::Label(ProvidersLabel::new(
                    TargetLabel::testing_parse(&format!("{cell}//pkg:s_compile")),
                    ProvidersName::Default,
                )),
            ),
            (
                "src".to_owned(),
                AnonTargetAttr::Artifact(Artifact::from(SourceArtifact::new(
                    SourcePath::testing_new(&format!("{cell}//pkg"), "a.src"),
                ))),
            ),
            (
                "tag".to_owned(),
                AnonTargetAttr::String(StringLiteral("anon-tag".into())),
            ),
        ]);
        let exec_cfg = ConfigurationNoExec::new(ConfigurationData::testing_new());
        (rule_type, name, attrs, exec_cfg)
    }

    fn home_strong_hash(p: Parts) -> u64 {
        // No map is installed in unit tests, so this is the hash josh off computes.
        let t = AnonTarget::new(p.0, p.1, p.2, p.3, AnonTargetVariant::Bzl);
        BaseDeferredKeyDyn::strong_hash(&t)
    }

    fn canonical(p: &Parts) -> Option<u64> {
        super::canonical_strong_hash(&lab_map(), &p.0, &p.1, &p.2, &p.3, &AnonTargetVariant::Bzl)
    }

    /// The imported tree's anon target, rebuilt in the canonical view, hashes as the same target
    /// does in its home, by the code that home runs.
    #[test]
    fn canonical_anon_hash_equals_home_strong_hash() {
        let home = home_strong_hash(parts("root"));
        assert_eq!(canonical(&parts("mwb")), Some(home));
        // and the real hash in the imported home differs, which is what josh fixes
        assert_ne!(home_strong_hash(parts("mwb")), home);
    }

    /// The cell that gives up the name `root` (lab's own root, canonically `lab`) hashes apart
    /// from the imported tree's `root`, so the two cannot share an output path.
    #[test]
    fn canonical_anon_hash_stays_injective() {
        let home = home_strong_hash(parts("root"));
        let lab_root = canonical(&parts("root")).unwrap();
        assert_ne!(lab_root, home);
        assert_eq!(lab_root, home_strong_hash(parts("lab")));
    }

    /// A part josh cannot rebuild makes the whole target fall back, never a partial rebuild.
    #[test]
    fn canonical_anon_hash_falls_back_on_bxl_variant() {
        let p = parts("mwb");
        let bxl = AnonTargetVariant::Bxl(Default::default());
        assert_eq!(
            super::canonical_strong_hash(&lab_map(), &p.0, &p.1, &p.2, &p.3, &bxl),
            None
        );
    }
}
