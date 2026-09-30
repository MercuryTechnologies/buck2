#!/bin/sh
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is dual-licensed under either the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree or the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree. You may select, at your option, one of the
# above-listed licenses.

# Usage: check_container_path.sh FAKE_CONTAINER COMMAND
#
# Runs COMMAND, a `container_run` target's command line (as `$(exe_target)`
# expands it: one argument), through the launcher's container path
# (BUCK_CONTAINER_RUN=always) with a fake `container` CLI, and checks that the
# project root is mounted at the same path and that `env` reaches the program.
set -u
fake=$1
shift
eval "set -- $*"
d=$(mktemp -d)
trap 'rm -rf "$d"' EXIT
cp "$fake" "$d/container"
chmod +x "$d/container"
out=$(PATH="$d:$PATH" BUCK_CONTAINER_RUN=always "$@" 2>&1)
printf '%s\n' "$out"
case $out in
    *"via launcher: yes"*) ;;
    *)
        printf 'FAIL: the program did not run with the target env\n'
        exit 1
        ;;
esac
volume=$(printf '%s\n' "$out" | sed -n 's/^volume=//p')
root=${volume%%:*}
if [ "$volume" != "$root:$root" ] || [ ! -f "$root/.buckconfig" ]; then
    printf 'FAIL: expected the project root mounted at the same path, got %s\n' "$volume"
    exit 1
fi
printf 'ok\n'
