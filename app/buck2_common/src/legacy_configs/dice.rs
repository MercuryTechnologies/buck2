/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! Dice operations for legacy configuration

use std::future::Future;
use std::str::FromStr;
use std::sync::Arc;

use allocative::Allocative;
use async_trait::async_trait;
use buck2_core::cells::name::CellName;
use buck2_error::BuckErrorContext;
use buck2_error::BuckErrorOptionContext;
use buck2_events::dispatch::get_dispatcher;
use derive_more::Display;
use dice::DiceComputations;
use dice::DiceProjectionComputations;
use dice::DiceTransactionUpdater;
use dice::EqualityBehavior;
use dice::InjectedKey;
use dice::Key;
use dice::NoneValueSerialize;
use dice::OkPagableValueSerialize;
use dice::OpaqueValue;
use dice::PagableValueSerialize;
use dice::ProjectionKey;
use dice::ResidentValueSerialize;
use dice::ValueSerialize;
use dice_futures::cancellation::CancellationContext;
use dupe::Dupe;
use dupe::ResultDupedErrExt;
use pagable::Pagable;
use pagable::pagable_typetag;

use crate::dice::cells::HasCellResolver;
use crate::legacy_configs::cells::BuckConfigBasedCells;
use crate::legacy_configs::cells::ExternalBuckconfigData;
use crate::legacy_configs::configs::LegacyBuckConfig;
use crate::legacy_configs::key::BuckconfigKeyRef;
use crate::legacy_configs::view::LegacyBuckConfigView;

/// Buckconfig view which queries buckconfig entry from DICE.
#[derive(Clone, Dupe)]
pub struct OpaqueLegacyBuckConfigOnDice<'d> {
    config: Arc<OpaqueValue<'d, LegacyBuckConfigForCellKey>>,
}

impl<'d> std::fmt::Debug for OpaqueLegacyBuckConfigOnDice<'d> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LegacyBuckConfigOnDice")
            .field("config", &self.config)
            .finish()
    }
}

impl<'d> OpaqueLegacyBuckConfigOnDice<'d> {
    pub fn lookup(
        &self,
        ctx: &mut DiceComputations<'d>,
        key: BuckconfigKeyRef,
    ) -> buck2_error::Result<Option<Arc<str>>> {
        let BuckconfigKeyRef { section, property } = key;
        Ok(ctx.projection(
            &*self.config,
            &LegacyBuckConfigPropertyProjectionKey {
                section: section.to_owned(),
                property: property.to_owned(),
            },
        )?)
    }

    pub fn view<'a>(&'a self, ctx: &'a mut DiceComputations<'d>) -> LegacyBuckConfigOnDice<'a, 'd> {
        LegacyBuckConfigOnDice { ctx, config: self }
    }
}

pub struct LegacyBuckConfigOnDice<'a, 'd> {
    ctx: &'a mut DiceComputations<'d>,
    config: &'a OpaqueLegacyBuckConfigOnDice<'d>,
}

impl LegacyBuckConfigOnDice<'_, '_> {
    pub fn parse<T: FromStr>(&mut self, key: BuckconfigKeyRef) -> buck2_error::Result<Option<T>>
    where
        buck2_error::Error: From<<T as FromStr>::Err>,
    {
        LegacyBuckConfig::parse_value(key, self.get(key)?.as_deref())
    }
}

impl std::fmt::Debug for LegacyBuckConfigOnDice<'_, '_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LegacyBuckConfigOnDice")
            .field("config", &self.config)
            .finish()
    }
}

impl LegacyBuckConfigView for LegacyBuckConfigOnDice<'_, '_> {
    fn get(&mut self, key: BuckconfigKeyRef) -> buck2_error::Result<Option<Arc<str>>> {
        self.config.lookup(self.ctx, key)
    }
}

pub trait HasInjectedLegacyConfigs<'d> {
    fn get_injected_external_buckconfig_data(
        &mut self,
    ) -> impl Future<Output = buck2_error::Result<&'d ExternalBuckconfigData>>;

    fn is_injected_external_buckconfig_data_key_set(
        &mut self,
    ) -> impl Future<Output = buck2_error::Result<bool>>;
}

#[async_trait]
pub trait HasLegacyConfigs<'d> {
    /// Get buckconfigs.
    ///
    /// This operation does not record buckconfig as a dependency of current computation.
    /// Accessing specific buckconfig property, records that key as dependency.
    async fn get_legacy_config_on_dice(
        &mut self,
        cell_name: CellName,
    ) -> buck2_error::Result<OpaqueLegacyBuckConfigOnDice<'d>>;

    async fn get_legacy_root_config_on_dice(
        &mut self,
    ) -> buck2_error::Result<OpaqueLegacyBuckConfigOnDice<'d>>;

    /// Use this function carefully: a computation which fetches this key will be recomputed
    /// if any buckconfig property changes.
    ///
    /// Consider using `get_legacy_config_property` instead.
    async fn get_legacy_config_for_cell(
        &mut self,
        cell_name: CellName,
    ) -> buck2_error::Result<&'d LegacyBuckConfig>;

    async fn get_legacy_config_property(
        &mut self,
        cell_name: CellName,
        key: BuckconfigKeyRef<'_>,
    ) -> buck2_error::Result<Option<Arc<str>>>;

    async fn parse_legacy_config_property<T: FromStr>(
        &mut self,
        cell_name: CellName,
        key: BuckconfigKeyRef<'_>,
    ) -> buck2_error::Result<Option<T>>
    where
        buck2_error::Error: From<<T as FromStr>::Err>,
        T: Send + Sync + 'static;

    async fn parse_legacy_config_list_property<T: FromStr>(
        &mut self,
        cell_name: CellName,
        key: BuckconfigKeyRef<'_>,
    ) -> buck2_error::Result<Option<Vec<T>>>
    where
        buck2_error::Error: From<<T as FromStr>::Err>,
        T: Send + Sync + 'static;
}

pub trait SetLegacyConfigs {
    fn set_legacy_config_external_data(
        &mut self,
        overrides: ExternalBuckconfigData,
    ) -> buck2_error::Result<()>;

    fn set_none_legacy_config_external_data(&mut self) -> buck2_error::Result<()>;
}

#[derive(Clone, Dupe, Display, Debug, Eq, Hash, PartialEq, Allocative, Pagable)]
#[display("{:?}", self)]
#[pagable_typetag(dice::DiceKeyDyn)]
struct LegacyExternalBuckConfigDataKey;

impl InjectedKey for LegacyExternalBuckConfigDataKey {
    type Value = Option<ExternalBuckconfigData>;

    fn equality_behavior() -> EqualityBehavior<Self::Value> {
        EqualityBehavior::Compare(|x, y| x == y)
    }

    fn value_serialize() -> impl ValueSerialize<Value = Self::Value> {
        PagableValueSerialize::<Self::Value>::new()
    }
}

#[derive(Clone, Display, Debug, Hash, Eq, PartialEq, Allocative, Pagable)]
#[display("LegacyBuckConfigForCellKey({})", self.cell_name)]
#[pagable_typetag(dice::DiceKeyDyn)]
struct LegacyBuckConfigForCellKey {
    cell_name: CellName,
}

#[async_trait]
impl Key for LegacyBuckConfigForCellKey {
    type Value = buck2_error::Result<LegacyBuckConfig>;

    async fn compute(
        &self,
        ctx: &mut DiceComputations,
        _cancellations: &CancellationContext,
    ) -> buck2_error::Result<LegacyBuckConfig> {
        let cells = ctx.get_cell_resolver().await?;
        let this_cell = cells.get(self.cell_name)?;
        let config = BuckConfigBasedCells::parse_single_cell_with_dice(ctx, this_cell.path())
            .await
            .with_buck_error_context(|| {
                format!("Computing legacy buckconfigs for cell `{}`", self.cell_name)
            })?;
        let config = config.filter_values(is_config_invisible_to_dice);

        let event = buck2_data::CellHasNewConfigs {
            cell: self.cell_name.as_str().to_owned(),
        };
        get_dispatcher().instant_event(event);

        Ok(config)
    }

    fn equality_behavior() -> EqualityBehavior<Self::Value> {
        EqualityBehavior::Compare(|x, y| match (x, y) {
            (Ok(x), Ok(y)) => x.compare(y),
            _ => false,
        })
    }

    fn value_serialize() -> impl ValueSerialize<Value = Self::Value> {
        OkPagableValueSerialize::<Self::Value>::new()
    }
}

/// The computation `LegacyBuckConfigForCellKey` computation might encounter an error.
///
/// We can't return that error immediately, because we only compute the opaque value. We could
/// return the error when doing the projection to the buckconfig values, but that would result in us
/// increasing the size of the value returned from that computation. Instead, we'll use a different
/// projection key to extract just the error from the cell computation, and compute that when
/// constructing the `OpaqueLegacyBuckConfigOnDice`.
#[derive(Debug, Display, Hash, Eq, PartialEq, Clone, Allocative, Pagable)]
#[pagable_typetag(dice::DiceProjectionDyn)]
struct LegacyBuckConfigErrorKey();

impl ProjectionKey for LegacyBuckConfigErrorKey {
    type DeriveFromKey = LegacyBuckConfigForCellKey;
    type Value = Option<buck2_error::Error>;

    fn compute(
        &self,
        config: &buck2_error::Result<LegacyBuckConfig>,
        _ctx: &DiceProjectionComputations,
    ) -> Option<buck2_error::Error> {
        config.as_ref().err().cloned()
    }

    fn equality_behavior() -> EqualityBehavior<Self::Value> {
        EqualityBehavior::Compare(|x, y| x.is_none() && y.is_none())
    }

    /// Projections are read without a page-in, so their values stay resident; `buck2 suspend`
    /// still writes the `None` that every parsed config projects, or every key that read a
    /// config would be recomputed on resume.
    fn value_serialize() -> impl ValueSerialize<Value = Self::Value> {
        ResidentValueSerialize::new(NoneValueSerialize::<Self::Value>::new())
    }
}

#[derive(Debug, Display, Hash, Eq, PartialEq, Clone, Allocative, Pagable)]
#[display("{}.{}", section, property)]
#[pagable_typetag(dice::DiceProjectionDyn)]
struct LegacyBuckConfigPropertyProjectionKey {
    section: String,
    property: String,
}

impl ProjectionKey for LegacyBuckConfigPropertyProjectionKey {
    type DeriveFromKey = LegacyBuckConfigForCellKey;
    type Value = Option<Arc<str>>;

    fn compute(
        &self,
        config: &buck2_error::Result<LegacyBuckConfig>,
        _ctx: &DiceProjectionComputations,
    ) -> Option<Arc<str>> {
        // See the comment in `LegacyBuckConfigErrorKey` for why this is safe
        let config = config.as_ref().unwrap();
        config
            .get(BuckconfigKeyRef {
                section: &self.section,
                property: &self.property,
            })
            .map(|s| s.to_owned().into())
    }

    fn equality_behavior() -> EqualityBehavior<Self::Value> {
        EqualityBehavior::Compare(|x, y| x == y)
    }

    /// Resident, like every projection, and written by `buck2 suspend`: see
    /// `LegacyBuckConfigErrorKey`.
    fn value_serialize() -> impl ValueSerialize<Value = Self::Value> {
        ResidentValueSerialize::new(PagableValueSerialize::<Self::Value>::new())
    }
}

impl<'d> HasInjectedLegacyConfigs<'d> for DiceComputations<'d> {
    async fn get_injected_external_buckconfig_data(
        &mut self,
    ) -> buck2_error::Result<&'d ExternalBuckconfigData> {
        self.compute(&LegacyExternalBuckConfigDataKey).await?.as_ref().internal_error("Tried to retrieve LegacyExternalBuckConfigDataKey from the graph, but key has None value")
    }

    async fn is_injected_external_buckconfig_data_key_set(&mut self) -> buck2_error::Result<bool> {
        Ok(self
            .compute(&LegacyExternalBuckConfigDataKey)
            .await?
            .is_some())
    }
}

pub fn inject_legacy_config_for_test(
    dice: &mut DiceTransactionUpdater,
    cell_name: CellName,
    configs: LegacyBuckConfig,
) -> buck2_error::Result<()> {
    inject_legacy_configs_for_test(dice, [(cell_name, configs)])
}

pub fn inject_legacy_configs_for_test(
    dice: &mut DiceTransactionUpdater,
    configs: impl IntoIterator<Item = (CellName, LegacyBuckConfig)> + Send + Sync + 'static,
) -> buck2_error::Result<()> {
    let configs = configs
        .into_iter()
        .map(|(cell_name, configs)| (LegacyBuckConfigForCellKey { cell_name }, Ok(configs)))
        .collect::<Vec<_>>();
    dice.changed_to(configs)?;
    dice.changed_to([(LegacyExternalBuckConfigDataKey, None)])?;
    Ok(())
}

#[async_trait]
impl<'d> HasLegacyConfigs<'d> for DiceComputations<'d> {
    async fn get_legacy_config_on_dice(
        &mut self,
        cell_name: CellName,
    ) -> buck2_error::Result<OpaqueLegacyBuckConfigOnDice<'d>> {
        let config = self
            .compute_opaque(&LegacyBuckConfigForCellKey { cell_name })
            .await?;
        if let Some(error) = self.projection(&config, &LegacyBuckConfigErrorKey())? {
            return Err(error);
        }
        Ok(OpaqueLegacyBuckConfigOnDice {
            config: Arc::new(config),
        })
    }

    async fn get_legacy_root_config_on_dice(
        &mut self,
    ) -> buck2_error::Result<OpaqueLegacyBuckConfigOnDice<'d>> {
        let cell_resolver = self.get_cell_resolver().await?;
        self.get_legacy_config_on_dice(cell_resolver.root_cell())
            .await
    }

    async fn get_legacy_config_for_cell(
        &mut self,
        cell_name: CellName,
    ) -> buck2_error::Result<&'d LegacyBuckConfig> {
        self.compute(&LegacyBuckConfigForCellKey { cell_name })
            .await?
            .as_ref()
            .duped_err()
    }

    async fn get_legacy_config_property(
        &mut self,
        cell_name: CellName,
        key: BuckconfigKeyRef<'_>,
    ) -> buck2_error::Result<Option<Arc<str>>> {
        self.get_legacy_config_on_dice(cell_name)
            .await?
            .lookup(self, key)
    }

    async fn parse_legacy_config_property<T: FromStr>(
        &mut self,
        cell_name: CellName,
        key: BuckconfigKeyRef<'_>,
    ) -> buck2_error::Result<Option<T>>
    where
        buck2_error::Error: From<<T as FromStr>::Err>,
        T: Send + Sync + 'static,
    {
        LegacyBuckConfig::parse_value(
            key,
            self.get_legacy_config_property(cell_name, key)
                .await?
                .as_deref(),
        )
    }

    async fn parse_legacy_config_list_property<T: FromStr>(
        &mut self,
        cell_name: CellName,
        key: BuckconfigKeyRef<'_>,
    ) -> buck2_error::Result<Option<Vec<T>>>
    where
        buck2_error::Error: From<<T as FromStr>::Err>,
        T: Send + Sync + 'static,
    {
        LegacyBuckConfig::parse_list_value(
            key,
            self.get_legacy_config_property(cell_name, key)
                .await?
                .as_deref(),
        )
    }
}

impl SetLegacyConfigs for DiceTransactionUpdater {
    fn set_legacy_config_external_data(
        &mut self,
        data: ExternalBuckconfigData,
    ) -> buck2_error::Result<()> {
        let data = data.filter_values(is_config_invisible_to_dice);
        Ok(self.changed_to(vec![(LegacyExternalBuckConfigDataKey, Some(data))])?)
    }

    fn set_none_legacy_config_external_data(&mut self) -> buck2_error::Result<()> {
        Ok(self.changed_to(vec![(LegacyExternalBuckConfigDataKey, None)])?)
    }
}

fn is_config_invisible_to_dice(key: &BuckconfigKeyRef) -> bool {
    !CONFIGS_INVISIBLE_TO_DICE.contains(key)
}

/// A set of buckconfigs that are visibile outside of dice, but not within it. Importantly, changes
/// to these configs do not cause state invalidations.
// FIXME(JakobDegen): Error if someone tries to read any of these from in dice
const CONFIGS_INVISIBLE_TO_DICE: &[BuckconfigKeyRef<'static>] = &[
    BuckconfigKeyRef {
        section: "buck2_re_client",
        property: "override_use_case",
    },
    BuckconfigKeyRef {
        section: "buck2_re_client",
        property: "request_metadata_tool_name",
    },
    BuckconfigKeyRef {
        section: "buck2_re_client",
        property: "invocation_env_override",
    },
    BuckconfigKeyRef {
        section: "buck2_re_client",
        property: "connect_timeout_s",
    },
    BuckconfigKeyRef {
        section: "scuba",
        property: "defaults",
    },
];

#[cfg(test)]
mod tests {
    use buck2_cli_proto::ConfigOverride;

    use crate::legacy_configs::configs::testing::parse_with_config_args;

    #[test]
    fn config_equals() -> buck2_error::Result<()> {
        let path = "test";
        let config1 = parse_with_config_args(
            &[("test", "[sec1]\na=b\n[sec2]\nx=y")],
            path,
            &[ConfigOverride::flag_no_cell("sec1.a=c")],
        )?;

        let config2 = parse_with_config_args(&[("test", "[sec1]\na=c\n[sec2]\nx=y")], path, &[])?;

        let config3 = parse_with_config_args(
            &[("test", "[sec1]\na=b\n[sec2]\nx=y")],
            path,
            &[ConfigOverride::flag_no_cell("sec1.d=e")],
        )?;

        assert!(config1.compare(&config1));
        assert!(config2.compare(&config2));
        assert!(config3.compare(&config3));
        assert!(config1.compare(&config2));
        assert!(!config1.compare(&config3));
        assert!(!config2.compare(&config3));

        Ok(())
    }
}

/// Some whole sections of a cell's config: for the readers that need a section rather than a
/// property (the cell and target alias resolvers), so that they depend on those sections and not
/// on the whole config. Used only with the frontier on.
#[derive(Debug, Display, Hash, Eq, PartialEq, Clone, Allocative, Pagable)]
#[display("{}", sections.join(","))]
#[pagable_typetag(dice::DiceProjectionDyn)]
struct LegacyBuckConfigSectionsProjectionKey {
    /// Sorted.
    sections: Vec<String>,
}

impl ProjectionKey for LegacyBuckConfigSectionsProjectionKey {
    type DeriveFromKey = LegacyBuckConfigForCellKey;
    type Value = LegacyBuckConfig;

    fn compute(
        &self,
        config: &buck2_error::Result<LegacyBuckConfig>,
        _ctx: &DiceProjectionComputations,
    ) -> LegacyBuckConfig {
        // See the comment in `LegacyBuckConfigErrorKey` for why this is safe: the accessor
        // below reads the error projection first.
        let config = config.as_ref().unwrap();
        config.filter_values(|k| self.sections.iter().any(|s| s == k.section))
    }

    fn equality_behavior() -> EqualityBehavior<Self::Value> {
        EqualityBehavior::Compare(|x, y| x.compare(y))
    }

    fn value_serialize() -> impl ValueSerialize<Value = Self::Value> {
        ResidentValueSerialize::new(PagableValueSerialize::<Self::Value>::new())
    }
}

/// The named sections of a cell's config, through a projection that depends on them alone.
pub async fn get_legacy_config_sections(
    ctx: &mut DiceComputations<'_>,
    cell_name: CellName,
    sections: &[&str],
) -> buck2_error::Result<LegacyBuckConfig> {
    let config = ctx
        .compute_opaque(&LegacyBuckConfigForCellKey { cell_name })
        .await?;
    if let Some(error) = ctx.projection(&config, &LegacyBuckConfigErrorKey())? {
        return Err(error);
    }
    let mut sections: Vec<String> = sections.iter().map(|s| (*s).to_owned()).collect();
    sections.sort();
    sections.dedup();
    Ok(ctx.projection(&config, &LegacyBuckConfigSectionsProjectionKey { sections })?)
}

pub fn frontier_config_key_types() -> [&'static str; 5] {
    [
        <LegacyBuckConfigPropertyProjectionKey as ProjectionKey>::key_type_name(),
        <LegacyBuckConfigErrorKey as ProjectionKey>::key_type_name(),
        <LegacyBuckConfigSectionsProjectionKey as ProjectionKey>::key_type_name(),
        <LegacyBuckConfigForCellKey as Key>::key_type_name(),
        <LegacyExternalBuckConfigDataKey as Key>::key_type_name(),
    ]
}

/// Config keys as frontier leaves or barriers. A property or section leaf's value is computed
/// later, with its origin, by [`frontier_config_leaf`].
pub fn frontier_inspect_config(
    key: &dyn std::any::Any,
    base: Option<&dyn std::any::Any>,
    value: Option<&dyn std::any::Any>,
) -> Option<crate::frontier::Inspected> {
    use crate::frontier::Inspected;
    use crate::frontier::LeafId;
    let cell = || {
        base.and_then(|b| b.downcast_ref::<LegacyBuckConfigForCellKey>())
            .map(|b| b.cell_name.as_str().to_owned())
    };
    if let Some(k) = key.downcast_ref::<LegacyBuckConfigPropertyProjectionKey>() {
        return Some(match cell() {
            Some(cell) => Inspected::Leaf {
                id: LeafId::Config {
                    cell,
                    section: k.section.clone(),
                    property: k.property.clone(),
                },
                canon: None,
            },
            None => Inspected::Barrier("config projection without its cell".to_owned()),
        });
    }
    if let Some(k) = key.downcast_ref::<LegacyBuckConfigSectionsProjectionKey>() {
        return Some(match cell() {
            Some(cell) => Inspected::Leaf {
                id: LeafId::Sections {
                    cell,
                    sections: k.sections.clone(),
                },
                canon: None,
            },
            None => Inspected::Barrier("config projection without its cell".to_owned()),
        });
    }
    if key.downcast_ref::<LegacyBuckConfigErrorKey>().is_some() {
        let is_error = value
            .and_then(|v| v.downcast_ref::<Option<buck2_error::Error>>())
            .map(|e| e.is_some());
        return Some(match (cell(), is_error) {
            (Some(cell), Some(is_error)) => Inspected::Leaf {
                id: LeafId::ConfigError { cell },
                canon: Some(crate::frontier::canonical_config_error(is_error)),
            },
            _ => Inspected::Barrier("config error projection unreadable".to_owned()),
        });
    }
    if key.downcast_ref::<LegacyBuckConfigForCellKey>().is_some() {
        return Some(Inspected::Barrier(
            "whole-config premise via LegacyBuckConfigForCellKey".to_owned(),
        ));
    }
    if key.downcast_ref::<LegacyExternalBuckConfigDataKey>().is_some() {
        return Some(Inspected::Barrier(
            "whole-config premise via LegacyExternalBuckConfigDataKey".to_owned(),
        ));
    }
    None
}

/// Where one config value came from.
pub fn frontier_config_origin(
    config: &LegacyBuckConfig,
    section: &str,
    property: &str,
) -> crate::frontier::ConfigOrigin {
    use crate::frontier::ConfigOrigin;
    let Some(value) = config.get_section(section).and_then(|s| s.get(property)) else {
        return ConfigOrigin::Absent;
    };
    if value.raw_value() != value.as_str() {
        return ConfigOrigin::Interpolated;
    }
    let stack = value.location_stack();
    if stack.is_empty() {
        return ConfigOrigin::CommandLine;
    }
    let mut files = Vec::new();
    for loc in stack {
        match loc {
            crate::legacy_configs::configs::LegacyBuckConfigLocation::File(f, _) => {
                files.push(f.to_owned())
            }
            crate::legacy_configs::configs::LegacyBuckConfigLocation::CommandLineArgument => {
                return ConfigOrigin::CommandLine;
            }
        }
    }
    ConfigOrigin::Files(files)
}

/// A config leaf's value in this daemon, and whether a record may name it: `Err(property)` when
/// some value comes from outside the files `tracked` accepts. The canonical value is for
/// comparison in this process; a primer writes it only when the origin is `Ok`.
pub async fn frontier_config_leaf(
    ctx: &mut DiceComputations<'_>,
    id: &crate::frontier::LeafId,
    tracked: &(dyn Fn(&str) -> bool + Sync),
) -> buck2_error::Result<Option<(crate::frontier::Canon, Result<(), String>)>> {
    use crate::frontier::ConfigOrigin;
    use crate::frontier::LeafId;
    let origin_ok = |config: &LegacyBuckConfig, section: &str, property: &str| -> Result<(), String> {
        match frontier_config_origin(config, section, property) {
            ConfigOrigin::Absent => Ok(()),
            ConfigOrigin::Files(files) if files.iter().all(|f| tracked(f)) => Ok(()),
            _ => Err(format!("{section}.{property}")),
        }
    };
    Ok(match id {
        LeafId::Config {
            cell,
            section,
            property,
        } => {
            let config = ctx
                .get_legacy_config_for_cell(CellName::unchecked_new(cell)?)
                .await?;
            let value = config.get(BuckconfigKeyRef { section, property });
            Some((
                crate::frontier::canonical_config_value(value),
                origin_ok(config, section, property),
            ))
        }
        LeafId::Sections { cell, sections } => {
            let config = ctx
                .get_legacy_config_for_cell(CellName::unchecked_new(cell)?)
                .await?;
            let mut entries = Vec::new();
            let mut ok = Ok(());
            for s in sections {
                if let Some(sec) = config.get_section(s) {
                    for (p, v) in sec.iter() {
                        entries.push((s.as_str(), p, v.as_str()));
                        if ok.is_ok() {
                            ok = origin_ok(config, s, p);
                        }
                    }
                }
            }
            Some((crate::frontier::canonical_sections(entries), ok))
        }
        _ => None,
    })
}
