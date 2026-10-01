# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is dual-licensed under either the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree or the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree. You may select, at your option, one of the
# above-listed licenses.

load("@prelude//container:toolchain.bzl", "ContainerRunToolchainInfo", "check_env_name")

# Debian trixie (glibc 2.41) with ca-certificates and curl. Pinned by digest so
# that every developer runs the same image: the CLI never re-pulls a tag it has
# already cached. Repos should set `default_image` to the image their remote
# execution workers use, so that shared libraries match.
_DEFAULT_IMAGE = "docker.io/library/buildpack-deps:trixie-curl@sha256:1fc77c1cf7efed94e9a8311321dd7c3c4e7294d1d2ddabb4f63b8ffbebb79ccf"

def _is_number(s: str) -> bool:
    # A positive decimal number such as "16" or "1.5".
    parts = s.split(".")
    return len(parts) <= 2 and not [p for p in parts if not p.isdigit()] and int("".join(parts)) > 0

def _is_memory_size(size: str) -> bool:
    # A number with a K/M/G/T/P unit and an optional B or iB suffix, which both
    # the Apple and Docker CLIs accept. A unit is required: without one, both
    # CLIs read the number as bytes.
    s = size.lower()
    for suffix in ("ib", "b"):
        if s.endswith(suffix) and len(s) > len(suffix):
            s = s[:-len(suffix)]
            break
    return len(s) > 1 and s[-1] in "kmgtp" and _is_number(s[:-1])

def _system_container_run_toolchain_impl(ctx: AnalysisContext) -> list[Provider]:
    for m in ctx.attrs.mounts:
        if not m.startswith("/") or ":" in m:
            fail("system_container_run_toolchain: mount `{}` must be an absolute path without `:`".format(m))
    for name in ctx.attrs.env_passthrough:
        check_env_name(name, "env_passthrough")
    # Apple's CLI only takes whole CPUs; Docker's also takes fractions.
    cpus = ctx.attrs.cpus
    if cpus not in ("", "host") and not (cpus.isdigit() and int(cpus) > 0) and not (ctx.attrs.cli_flavor == "docker" and _is_number(cpus)):
        fail("system_container_run_toolchain: `cpus` must be `host`, a whole number or empty, not `{}`".format(cpus))
    if ctx.attrs.memory not in ("", "host") and not _is_memory_size(ctx.attrs.memory):
        fail("system_container_run_toolchain: `memory` must be `host`, a size with a unit such as `16G` or `1.5G`, or empty, not `{}`".format(ctx.attrs.memory))
    return [
        DefaultInfo(),
        ContainerRunToolchainInfo(
            cli = ctx.attrs.cli,
            cli_flavor = ctx.attrs.cli_flavor,
            cpus = ctx.attrs.cpus,
            memory = ctx.attrs.memory,
            default_image = ctx.attrs.default_image,
            env_passthrough = ctx.attrs.env_passthrough,
            mounts = ctx.attrs.mounts,
            run_args = ctx.attrs.run_args,
        ),
    ]

system_container_run_toolchain = rule(
    impl = _system_container_run_toolchain_impl,
    doc = """
    Toolchain for `container_run`, which runs Linux programs inside a container
    when they are `buck2 run` on macOS. Defaults to Apple's `container` CLI.
    """,
    attrs = {
        "cli": attrs.string(default = "container", doc = "Container CLI name or absolute path."),
        "cli_flavor": attrs.enum(["apple", "docker"], default = "apple", doc = "`apple` for Apple's `container` CLI, `docker` for Docker-compatible CLIs (Docker Desktop, OrbStack, Colima, Podman)."),
        "cpus": attrs.string(default = "host", doc = "CPUs per container: `host` (the Mac's CPU count; Apple flavor only), a whole number (or a fraction with the Docker flavor), or empty for the CLI default."),
        "default_image": attrs.string(default = _DEFAULT_IMAGE, doc = "Image for targets that do not set `image`. Prefer the image your remote execution workers use, pinned by digest."),
        "env_passthrough": attrs.list(attrs.string(), default = ["NO_COLOR", "RUST_BACKTRACE", "RUST_LOG", "TZ"], doc = "Environment variables copied from the host into every container, by name."),
        "memory": attrs.string(default = "host", doc = "Memory per container: `host` (half of the Mac's RAM; Apple flavor only), a size with a unit such as `16G` or `1.5G`, or empty for the CLI default."),
        "mounts": attrs.list(attrs.string(), default = [], doc = "Absolute host paths to mount at the same path in every container. Paths missing on a machine are skipped."),
        "run_args": attrs.list(attrs.string(), default = [], doc = "Extra flags for `<cli> run`."),
    },
    is_toolchain_rule = True,
)
