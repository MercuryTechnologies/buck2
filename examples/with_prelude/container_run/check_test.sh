#!/bin/sh
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is dual-licensed under either the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree or the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree. You may select, at your option, one of the
# above-listed licenses.

# The test behind both `check_test_bin` and `check_test` (its `container_run`
# wrapper). Only the wrapper sets VIA_LAUNCHER, and then the test's own `env`
# must still arrive.
if [ -z "${VIA_LAUNCHER+x}" ]; then
    printf 'not wrapped\n'
    exit 0
fi
[ "$VIA_LAUNCHER" = 1 ] || exit 1
if [ "${TEST_ENV-}" != 1 ]; then
    printf 'the test env was lost\n'
    exit 1
fi
printf 'ok\n'
