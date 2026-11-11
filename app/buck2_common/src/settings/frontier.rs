/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! Settings for sharing DICE nodes between daemons (the frontier). Everything is off by default:
//! with `mode = off` no new key is requested, nothing is recorded and nothing is written.

use std::sync::Arc;

use allocative::Allocative;
use dupe::Dupe;
use serde::Deserialize;
use serde::Serialize;

use crate::settings::metadata::OverrideSource;
use crate::settings::metadata::SectionMetadata;
use crate::settings::metadata::SettingKey;
use crate::settings::metadata::SettingKeyMetadata;

#[derive(
    Allocative,
    Clone,
    Copy,
    Debug,
    Default,
    Deserialize,
    Serialize,
    PartialEq,
    Eq
)]
#[serde(rename_all = "snake_case")]
pub enum FrontierMode {
    #[default]
    Off,
    /// `buck2 debug hydration frontier-prime` may write records.
    Prime,
    /// `buck2 debug hydration frontier-shadow` checks records and installs none.
    Shadow,
}

const MODE: SettingKey<FrontierMode> = SettingKey {
    metadata: SettingKeyMetadata {
        name: "mode",
        overridable_in: &[OverrideSource::CommandLine, OverrideSource::LocalSettings],
    },
    internal_default: Some(FrontierMode::Off),
    oss_default: Some(FrontierMode::Off),
};

const STORE: SettingKey<&'static str> = SettingKey {
    metadata: SettingKeyMetadata {
        name: "store",
        overridable_in: &[OverrideSource::CommandLine, OverrideSource::LocalSettings],
    },
    internal_default: None,
    oss_default: None,
};

const BASE_COMMIT: SettingKey<&'static str> = SettingKey {
    metadata: SettingKeyMetadata {
        name: "base_commit",
        overridable_in: &[OverrideSource::CommandLine, OverrideSource::LocalSettings],
    },
    internal_default: None,
    oss_default: None,
};

const KINDS: SettingKey<&'static str> = SettingKey {
    metadata: SettingKeyMetadata {
        name: "kinds",
        overridable_in: &[OverrideSource::CommandLine, OverrideSource::LocalSettings],
    },
    internal_default: None,
    oss_default: None,
};

#[derive(Debug, Default, Deserialize, Serialize, PartialEq, Eq, Allocative)]
#[serde(deny_unknown_fields)]
pub(super) struct FrontierSectionData {
    mode: Option<FrontierMode>,
    store: Option<String>,
    base_commit: Option<String>,
    kinds: Option<String>,
}

#[derive(
    Clone,
    Dupe,
    Debug,
    Default,
    Serialize,
    Deserialize,
    PartialEq,
    Eq,
    Allocative
)]
#[serde(transparent)]
pub struct FrontierSection(pub(super) Arc<FrontierSectionData>);

impl FrontierSection {
    pub(crate) const METADATA: SectionMetadata = SectionMetadata {
        section_name: "frontier",
        section_version: 1,
        settings: &[
            MODE.metadata,
            STORE.metadata,
            BASE_COMMIT.metadata,
            KINDS.metadata,
        ],
    };

    pub fn mode(&self) -> FrontierMode {
        MODE.resolve(self.0.mode)
            .expect("frontier.mode should have a default")
    }

    /// The local directory store.
    pub fn store(&self) -> Option<&str> {
        self.0.store.as_deref().or_else(|| STORE.default_value())
    }

    /// The commit whose records a shadow reader looks up.
    pub fn base_commit(&self) -> Option<&str> {
        self.0
            .base_commit
            .as_deref()
            .or_else(|| BASE_COMMIT.default_value())
    }

    /// Narrows the kinds that get records, comma-separated; `None` for every listed kind.
    pub fn kinds(&self) -> Option<Vec<String>> {
        self.0
            .kinds
            .as_deref()
            .or_else(|| KINDS.default_value())
            .map(|k| {
                k.split(',')
                    .map(|s| s.trim().to_owned())
                    .filter(|s| !s.is_empty())
                    .collect()
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::settings::BuckSettings;
    use crate::settings::parser::resolve_setting_flags;
    use crate::settings::parser::table;

    #[test]
    fn off_by_default() {
        let f = BuckSettings::empty().daemon.frontier;
        assert_eq!(f.mode(), FrontierMode::Off);
        assert_eq!(f.store(), None);
        assert_eq!(f.base_commit(), None);
        assert_eq!(f.kinds(), None);
    }

    #[test]
    fn parsed() -> buck2_error::Result<()> {
        let settings = resolve_setting_flags(vec![table(
            "[frontier]\nmode = \"shadow\"\nstore = \"/s\"\nbase_commit = \"abc\"\nkinds = \"BuildKey, PackageListingKey\"",
        )])?;
        let f = settings.daemon.frontier;
        assert_eq!(f.mode(), FrontierMode::Shadow);
        assert_eq!(f.store(), Some("/s"));
        assert_eq!(f.base_commit(), Some("abc"));
        assert_eq!(
            f.kinds(),
            Some(vec!["BuildKey".to_owned(), "PackageListingKey".to_owned()])
        );
        Ok(())
    }
}
