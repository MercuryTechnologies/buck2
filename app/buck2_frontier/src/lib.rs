/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! Sharing DICE nodes between daemons (the frontier), phases 1 to 3: records written by a primer
//! to a local directory store, and a shadow reader that checks them against its own daemon and
//! never installs one. Everything here is reached only with `frontier.mode` set.

pub mod body;
pub mod inv_key;
pub mod policy;
pub mod store;
pub mod walk;
