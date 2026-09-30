---
id: container_run
title: Run Linux binaries on macOS
---

# Run Linux binaries on macOS

Many repos build Linux binaries on remote execution, even when the
developer is on a Mac. `buck2 run` then fails with `exec format error`,
because macOS cannot run a Linux binary. The prelude's `container_run`
rule fixes this: on macOS it runs the program in a Linux container using
Apple's [`container`](https://github.com/apple/container) CLI, and
everywhere else it runs the program directly.

| Host                       | What `buck2 run //svc:server` does             |
| -------------------------- | ---------------------------------------------- |
| Linux, CI, remote executor | Runs the binary directly, exactly as before    |
| macOS                      | `container run ...` with the repo mounted      |
| Any, with an override      | `BUCK_CONTAINER_RUN=always` or `never` decides |

Only targets that are configured for Linux (`config//os:linux`) are
wrapped. For any other configuration `container_run` is a plain alias,
so the same target also works when a Mac builds it natively.

## Requirements

- Apple silicon, macOS 26 or later, and `container` 1.0 or later,
  installed from the signed package on the
  [releases page](https://github.com/apple/container/releases). It
  installs to `/usr/local/bin/container`.
- Run `container system start` once, and accept the offer to install the
  Linux kernel (or pass `--enable-kernel-install`). After a reboot,
  `container_run` starts the services again by itself.
- For `x86_64` binaries, install Rosetta:
  `softwareupdate --install-rosetta`. The container CLI uses it
  automatically for `linux/amd64`.
- Your repo must already be able to build the Linux target from a Mac,
  typically with remote execution.

Intel Macs and older macOS versions can use a Docker-compatible CLI
instead; see
[Docker, OrbStack, Colima or Podman](#docker-orbstack-colima-or-podman).

## Set up the toolchain

`container_run` reads repo-wide settings from
`toolchains//:container_run`. `system_demo_toolchains()` already defines
it. If your repo defines its toolchains one by one, add:

```python
load("@prelude//toolchains:container.bzl", "system_container_run_toolchain")

system_container_run_toolchain(
    name = "container_run",
    # The image your remote execution workers use, pinned by digest, so that
    # glibc and other shared libraries match what the binary was built against.
    default_image = "ghcr.io/acme/re-worker@sha256:...",
    visibility = ["PUBLIC"],
)
```

| Attribute         | Default                                        | Meaning                                                                                        |
| ----------------- | ---------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| `cli`             | `"container"`                                  | CLI name (looked up on `PATH`, then `/usr/local/bin` and `/opt/homebrew/bin`) or absolute path |
| `cli_flavor`      | `"apple"`                                      | `"apple"`, or `"docker"` for Docker-compatible CLIs                                            |
| `default_image`   | Debian trixie with curl, pinned by digest      | Image for targets that don't set `image`                                                       |
| `cpus`            | `"host"`                                       | `"host"` (all of the Mac's CPUs), a number, or `""` for the CLI default                        |
| `memory`          | `"host"`                                       | `"host"` (half of the Mac's RAM), a size like `"16G"`, or `""` for the CLI default             |
| `env_passthrough` | `NO_COLOR`, `RUST_BACKTRACE`, `RUST_LOG`, `TZ` | Environment variables copied from your shell into every container                              |
| `mounts`          | `[]`                                           | Extra absolute host paths mounted at the same path; skipped on machines where they don't exist |
| `run_args`        | `[]`                                           | Extra flags for `container run`                                                                |

`cpus` and `memory` default to `host` because the Apple CLI otherwise
gives each container 4 CPUs and 1 GiB of memory, which is not enough for
many programs.

## Wrap a binary

```python
rust_binary(
    name = "server-bin",
    srcs = ["main.rs"],
)

container_run(
    name = "server",
    binary = ":server-bin",
    ports = [8080],
    env = {"RUST_LOG": "info"},
)
```

`buck2 run //svc:server -- --port 8080` now works on a Mac as well as on
Linux.

`container_run` forwards every provider of `binary`, like `alias`, and
only replaces `RunInfo`. `buck2 build //svc:server` builds the same
outputs, and `buck2 test` on a wrapped test runs the test through the
same launcher.

| Attribute           | Default   | Meaning                                                                                                                         |
| ------------------- | --------- | ------------------------------------------------------------------------------------------------------------------------------- |
| `binary`            |           | The program to run. It is configured like the `container_run` target.                                                           |
| `image`             | toolchain | Image to run in. Useful for programs that need an interpreter, such as Python or a JVM.                                         |
| `env`               | `{}`      | Environment for the program. Values may use `$(location ...)`. They are visible on the command line, so don't put secrets here. |
| `env_passthrough`   | `[]`      | More variable names to copy from your shell. Only names appear on the command line.                                             |
| `ports`             | `[]`      | Ports to publish on the Mac's `127.0.0.1`, under the same number.                                                               |
| `run_args`          | `[]`      | Extra flags for `container run`.                                                                                                |
| `enabled`           | `None`    | `None` wraps only Linux-configured targets. `True` or `False` forces the choice.                                                |
| `platform_override` | `None`    | Configure this target (and `binary`) for a given platform. See below.                                                           |

## Make it invisible

To let people keep typing the target names they already know, have your
repo's macros create the wrapper under the public name:

```python
# build_defs/linux.bzl
LINUX = "root//platforms:linux-arm64"  # The platform your CI and RE build with.

def linux_rust_binary(name, visibility = None, **kwargs):
    native.rust_binary(
        name = "__{}_bin".format(name),
        default_target_platform = LINUX,
        **kwargs
    )
    native.container_run(
        name = name,
        binary = ":__{}_bin".format(name),
        default_target_platform = LINUX,
        visibility = visibility,
    )
```

`default_target_platform` makes both targets Linux builds when they are
built directly, so on a Mac `buck2 run //svc:server` builds the Linux
binary remotely and runs it in a container. Linux hosts see no
difference.

If a target must be Linux even when it is a dependency, or when someone
passes `--target-platforms`, use `platform_override = LINUX` instead. It
requires listing the platform under `[buck2] platforms` in
`.buckconfig`.

A `container_run` target decides whether to wrap from its own
configuration. If you point it at a `configured_alias`, set
`enabled = True`.

## What happens at run time

On macOS, `buck2 run` executes a small POSIX shell launcher, which runs:

```sh
container run --rm -i [-t] --init --platform linux/arm64 \
  --volume <repo>:<repo> --workdir <cwd> [--cpus N --memory M] \
  --env BUCK_RUN_BUILD_ID [--env NAME]... [--env K=V]... [--publish ...] \
  --entrypoint <binary> <image> <args>...
```

- The project root is mounted at the same path, so every path `buck2`
  passes to the program is valid inside the container. Nothing else from
  the Mac is visible: not your home directory, and not files that
  symlinks point to outside the repo. Use the toolchain's `mounts`, or
  `BUCK_CONTAINER_RUN_ARGS`, to add more.
- The working directory is your current directory, if it is inside the
  repo; otherwise it is the project root.
- `--platform` follows the target's CPU: `linux/arm64`, or `linux/amd64`
  for `x86_64` targets (run through Rosetta).
- Only `BUCK_RUN_BUILD_ID`, the passthrough names and `env` reach the
  program. `HOME` is the image's (usually `/root`).
- `-t` is added only when stdin, stdout and stderr are all terminals, so
  `2>err.log` keeps working. Image-pull progress is shown only on a
  terminal.
- The exit code is the program's. The container CLI's own errors (for
  example a failed image pull) also exit with 1.
- Each run starts a lightweight VM, which takes about a second.

## Environment variables

| Variable                     | Effect                                                                                                                 |
| ---------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| `BUCK_CONTAINER_RUN`         | `auto` (default): container on macOS, direct elsewhere. `always` or `never` force it.                                  |
| `BUCK_CONTAINER_RUN_ARGS`    | Extra `container run` flags for this run, split on whitespace (no quoting). They come last, so they override defaults. |
| `BUCK_CONTAINER_RUN_VERBOSE` | `1` prints the exact command before running it.                                                                        |

For example:

```sh
# Publish a port that is only known at run time, and share AWS credentials.
BUCK_CONTAINER_RUN_ARGS="-p 127.0.0.1:9090:9090 -v $HOME/.aws:/root/.aws:ro -e AWS_PROFILE" \
  buck2 run //svc:server -- --port 9090
```

## Recipes

**Servers.** Listen on `0.0.0.0` inside the container, and list the port
in `ports` (published on `127.0.0.1` only). To reach services running on
the Mac from inside the container, see the networking section of the
`container` documentation.

**Images.** Set the toolchain's `default_image` to your remote execution
worker image, pinned by digest, so the shared libraries match. The CLI
never re-pulls a tag that it already has, so an unpinned tag can differ
between developers.

**Debugging.** `--emit-shell` and `--command-args-file` show the
launcher command, and an IDE that attaches a debugger to `buck2 run`
attaches to the shell. Run `gdbserver` in the container with a published
port instead.

**Docker, OrbStack, Colima or Podman.** Set `cli = "docker"` (or
`"podman"`) and `cli_flavor = "docker"`. The repo must be inside a
directory that the tool shares with its VM: OrbStack shares everything,
Docker Desktop shares `/Users`, `/Volumes`, `/private`, `/tmp` and
`/var/folders` by default, and Colima and Podman share your home
directory.

## Limitations

- Sub-targets such as `buck2 run //svc:server[debug]` are not wrapped,
  and run natively.
- `$(exe ...)` of a wrapped target inside a build action that runs
  locally on a Mac would start a container inside the action. Use such
  targets as tools only on Linux executors.
- Tests inherit the buck2 daemon's environment rather than your shell's,
  so `BUCK_CONTAINER_RUN` does not affect `buck2 test`.
- Paths that contain `:` cannot be mounted.
- In a `while read ...; do buck2 run ...; done < file` loop, the
  container reads stdin eagerly; add `</dev/null` to the `buck2 run`
  command.

## Troubleshooting

| Symptom                                    | Fix                                                                                                  |
| ------------------------------------------ | ---------------------------------------------------------------------------------------------------- |
| `container_run: 'container' was not found` | Install the `container` CLI, or set `BUCK_CONTAINER_RUN=never`.                                      |
| `Unknown option '--init'`                  | The CLI is too old; update it with `/usr/local/bin/update-container.sh`.                             |
| `default kernel not configured`            | Run `container system start` in a terminal and install the kernel.                                   |
| `exec format error`                        | The `container_run` target is not configured for Linux; see [Make it invisible](#make-it-invisible). |
| Exit code 137                              | Out of memory; raise the toolchain's `memory`.                                                       |
| Exit code 132 for an `x86_64` binary       | Rosetta can't run it (for example AVX-512 code); build for `arm64` instead.                          |
| `path ... does not exist`                  | A mount source is missing on this Mac.                                                               |
| Connection refused                         | Listen on `0.0.0.0` and add the port to `ports`.                                                     |
| `note: ... is not inside the project root` | Run from inside the repo so that relative paths resolve.                                             |
