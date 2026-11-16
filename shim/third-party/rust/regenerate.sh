#!/usr/bin/env bash
# Regenerate BUCK in this directory from Cargo.toml, with the reindeer that
# bootstrap/reindeer pins. Run it from buck2's own dev shell, which provides
# the cargo and rustc of rust-toolchain.toml and dotslash:
#
#   nix develop -c shim/third-party/rust/regenerate.sh          # after a Cargo.toml change
#   nix develop -c shim/third-party/rust/regenerate.sh --check  # prove the checked-in BUCK
#
# The buck build of buck2 compiles the crates this BUCK names, and a cargo
# build compiles the ones the root Cargo.lock names. Seeding this directory's
# lockfile from the root one makes the two builds agree on every version they
# share; reindeer's `cargo metadata` then only adds what the shim asks for
# beyond the workspace. The resolved Cargo.lock is checked in next to BUCK, so
# BUCK is a function of tracked files only, and `--check` reruns reindeer
# against it with --locked and fails on any difference.
#
# BUCK.reindeer is the copy buildbuddy/run_stack_test_matrix.sh installs into
# a worktree; it is kept identical to BUCK so the matrix builds this tree.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../../.." && pwd)
cd "$root"

check=false
case "${1:-}" in
    --check) check=true ;;
    "") ;;
    *) echo "usage: $0 [--check]" >&2; exit 2 ;;
esac

for tool in cargo rustc dotslash; do
    command -v "$tool" >/dev/null || { echo "$0: no $tool on PATH; run it under nix develop" >&2; exit 1; }
done

if $check; then
    dotslash bootstrap/reindeer --cargo-options=--locked \
        --third-party-dir shim/third-party/rust buckify
else
    cp Cargo.lock shim/third-party/rust/Cargo.lock
    dotslash bootstrap/reindeer --third-party-dir shim/third-party/rust buckify
fi
cp shim/third-party/rust/BUCK shim/third-party/rust/BUCK.reindeer

if $check; then
    git diff --exit-code --stat -- shim/third-party/rust/BUCK \
        shim/third-party/rust/BUCK.reindeer shim/third-party/rust/Cargo.lock
fi
