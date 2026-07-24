# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is licensed under both the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree and the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree.

# A hermetic Rust toolchain built from a pinned upstream `rustc` + `rust-std`.
#
# Why this exists: the aarch64 `__stack_chk_guard` link failure only reproduces
# with an *upstream* toolchain. Fedora's system `rustc` ships a hardened
# `compiler_builtins` that references `__stack_chk_guard` ahead of `-lc`, which
# drags `ld-linux` (the only definer of that symbol on aarch64) onto the link
# and masks the bug. Upstream nightly's `compiler_builtins` has no such
# reference, so the guard reference emitted by ring's `-fstack-protector-strong`
# C code — which buck appends to the link *after* `-lc` under `--as-needed` —
# has no DSO to resolve against. See `//rust_toolchain:BUCK`.
#
# `sysroot` is a directory artifact laid out like a normal rustc sysroot
# (`bin/rustc`, `lib/...`, `lib/rustlib/<triple>/lib/*.rlib`). rustc derives its
# implicit sysroot from the real path of its own binary, so invoking
# `<sysroot>/bin/rustc` finds the overlaid std with no `--sysroot` needed.

load("@prelude//rust:rust_toolchain.bzl", "PanicRuntime", "RustToolchainInfo")

def _hermetic_rust_toolchain_impl(ctx):
    sysroot = ctx.attrs.sysroot[DefaultInfo].default_outputs[0]
    return [
        DefaultInfo(),
        RustToolchainInfo(
            compiler = RunInfo(args = cmd_args(sysroot.project("bin/rustc"))),
            rustdoc = RunInfo(args = cmd_args(sysroot.project("bin/rustdoc"))),
            # No clippy in the `rustc` component; point it at rustc so the field
            # is populated. Clippy is not invoked when building the repro.
            clippy_driver = RunInfo(args = cmd_args(sysroot.project("bin/rustc"))),
            default_edition = ctx.attrs.default_edition,
            panic_runtime = PanicRuntime("unwind"),
            rustc_target_triple = ctx.attrs.rustc_target_triple,
            nightly_features = True,
        ),
    ]

hermetic_rust_toolchain = rule(
    impl = _hermetic_rust_toolchain_impl,
    attrs = {
        "default_edition": attrs.option(attrs.string(), default = None),
        "rustc_target_triple": attrs.string(default = "aarch64-unknown-linux-gnu"),
        "sysroot": attrs.dep(providers = [DefaultInfo]),
    },
    is_toolchain_rule = True,
)
