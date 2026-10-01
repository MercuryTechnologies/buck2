# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is dual-licensed under either the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree or the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree. You may select, at your option, one of the
# above-listed licenses.

ContainerRunToolchainInfo = provider(
    doc = "Repo-wide settings for `container_run`, which runs Linux programs in a container on macOS.",
    # @unsorted-dict-items
    fields = {
        # Container CLI: a name looked up on PATH (then in /usr/local/bin and
        # /opt/homebrew/bin), or an absolute path.
        "cli": provider_field(str),
        # "apple" for Apple's `container` CLI, or "docker" for Docker-compatible
        # CLIs (Docker Desktop, OrbStack, Colima, Podman).
        "cli_flavor": provider_field(str),
        # CPUs per container: "host" for the Mac's CPU count, a number, or ""
        # for the CLI default.
        "cpus": provider_field(str),
        # Memory per container: "host" for half of the Mac's RAM, a size such as
        # "16G", or "" for the CLI default.
        "memory": provider_field(str),
        # Image used when a target does not set `image`.
        "default_image": provider_field(str),
        # Names of environment variables copied from the host into every container.
        "env_passthrough": provider_field(list[str]),
        # Absolute host paths mounted at the same path in every container, in
        # addition to the project root.
        "mounts": provider_field(list[str]),
        # Extra flags passed to `<cli> run` for every container.
        "run_args": provider_field(list[str]),
    },
)

_ENV_NAME_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_"

def is_env_name(name: str) -> bool:
    return bool(name) and name[0] not in "0123456789" and not [c for c in name.elems() if c not in _ENV_NAME_CHARS]

def check_env_name(name: str, what: str):
    if not is_env_name(name):
        fail("container_run: invalid environment variable name `{}` in `{}`".format(name, what))

    # The launcher's own variables use this prefix.
    if name.startswith("_cr_"):
        fail("container_run: environment variable names starting with `_cr_` are reserved (`{}` in `{}`)".format(name, what))
