# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is dual-licensed under either the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree or the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree. You may select, at your option, one of the
# above-listed licenses.

load("@prelude//container:toolchain.bzl", "ContainerRunToolchainInfo", "check_env_name", "is_env_name")
load("@prelude//decls:common.bzl", "buck")
load("@prelude//decls:toolchains_common.bzl", "toolchains_common")
load("@prelude//transitions:constraint_overrides.bzl", "constraint_overrides")
load("@prelude//user:rule_spec.bzl", "RuleRegistrationSpec")

# `container_run` wraps a runnable target so that its RunInfo (and its test
# command) go through `tools/container_run.sh`. Whether to wrap is decided here,
# from the target's configuration: only Linux targets are wrapped. How to run is
# decided by the launcher at run time: inside a container on macOS, directly
# everywhere else. Deciding the host at run time rather than with `host_info()`
# keeps configurations and action digests identical on every host, and keeps
# the wrapped target usable as a tool on Linux (remote) executors.

def _launcher_args(
        ctx: AnalysisContext,
        tc: ContainerRunToolchainInfo,
        extra_passthrough: list[str] = [],
        extra_env: dict[str, typing.Any] = {}) -> cmd_args:
    args = cmd_args(
        "/bin/sh",
        ctx.attrs._launcher,
        "--target",
        str(ctx.label.raw_target()),
        # Renders as an absolute path under `buck2 run`, which lets the launcher
        # mount the project root at the same path inside the container.
        "--project-root",
        ctx.label.project_root,
        "--cli",
        tc.cli,
        "--flavor",
        tc.cli_flavor,
        "--image",
        ctx.attrs.image or tc.default_image,
        "--platform",
        ctx.attrs._platform,
    )
    if tc.cpus:
        args.add("--cpus", tc.cpus)
    if tc.memory:
        args.add("--memory", tc.memory)
    for m in tc.mounts:
        args.add("--mount", m)

    # Explicit values win over passed-through names, the target's `env` wins
    # over `extra_env`, and every name is passed once.
    env = dict(extra_env)
    env.update(ctx.attrs.env)
    seen = {k: None for k in env}
    for k in tc.env_passthrough + ctx.attrs.env_passthrough + extra_passthrough:
        if k not in seen:
            seen[k] = None
            args.add("--env-passthrough", k)
    for k, v in env.items():
        args.add("--env", cmd_args(k + "=", cmd_args(v, delimiter = " "), delimiter = ""))
    for p in dedupe(ctx.attrs.ports):
        args.add("--run-arg", "--publish", "--run-arg", "127.0.0.1:{}:{}".format(p, p))
    for a in tc.run_args + ctx.attrs.run_args:
        args.add("--run-arg", a)
    args.add("--")
    return args

def _wrap_test(ctx: AnalysisContext, tc: ContainerRunToolchainInfo, test: ExternalRunnerTestInfo) -> ExternalRunnerTestInfo:
    # The test runner sets the test's `env` on the launcher process, so those
    # names have to be passed through to the container.
    return ExternalRunnerTestInfo(
        type = test.test_type,
        command = [_launcher_args(ctx, tc, extra_passthrough = list((test.env or {}).keys()))] + list(test.command or []),
        env = test.env,
        labels = test.labels,
        contacts = test.contacts,
        use_project_relative_paths = test.use_project_relative_paths,
        run_from_project_root = test.run_from_project_root,
        default_executor = test.default_executor,
        executor_overrides = test.executor_overrides,
        local_resources = test.local_resources,
        required_local_resources = test.required_local_resources,
        worker = test.worker,
        supports_test_execution_caching = test.supports_test_execution_caching,
    )

def _container_run_impl(ctx: AnalysisContext) -> list[Provider]:
    tc = ctx.attrs._container_run_toolchain[ContainerRunToolchainInfo]

    # Validate on every host, so that mistakes made on Linux don't only show up
    # on Macs.
    for k in ctx.attrs.env:
        check_env_name(k, "env")
    for k in ctx.attrs.env_passthrough:
        check_env_name(k, "env_passthrough")
    for p in ctx.attrs.ports:
        if p < 1 or p > 65535:
            fail("container_run: port {} is out of range 1-65535".format(p))
    if not (ctx.attrs.image or tc.default_image):
        fail("container_run: no image: set `image` on the target or `default_image` on the toolchain")

    enabled = ctx.attrs.enabled if ctx.attrs.enabled != None else ctx.attrs._auto_enabled
    if not enabled:
        return ctx.attrs.binary.providers

    # Forward everything, like `alias`, but run and test through the launcher.
    providers = []
    run_info = None
    for p in ctx.attrs.binary.providers:
        if isinstance(p, RunInfo):
            continue
        if isinstance(p, ExternalRunnerTestInfo):
            # For a test with `env`, the prelude's RunInfo runs a Python env
            # injector around the test command (see inject_test_run_info.bzl),
            # and the image may not have Python. Recognize that RunInfo and
            # pass the env to the launcher instead; leave any other RunInfo
            # (e.g. erlang_test's shell) alone.
            if (p.env and not [k for k in p.env if not is_env_name(k)] and
                "inject_test_env" in repr(ctx.attrs.binary[RunInfo])):
                run_info = RunInfo(args = cmd_args(_launcher_args(ctx, tc, extra_env = p.env), p.command))
            p = _wrap_test(ctx, tc, p)
        providers.append(p)
    providers.append(run_info or RunInfo(args = cmd_args(_launcher_args(ctx, tc), ctx.attrs.binary[RunInfo])))
    return providers

registration_spec = RuleRegistrationSpec(
    name = "container_run",
    impl = _container_run_impl,
    cfg = constraint_overrides.transition,
    doc = """
    Runs a Linux program inside a container when it is `buck2 run` (or
    `buck2 test`) on macOS, and directly everywhere else.

    `container_run` forwards every provider of `binary`, like `alias`, and
    replaces its `RunInfo` (and `ExternalRunnerTestInfo`) with a launcher. On
    macOS the launcher runs the program with Apple's `container` CLI (or a
    Docker-compatible CLI, see `system_container_run_toolchain`), mounting the
    project root at the same path so that the paths buck2 passes stay valid.
    On every other host it execs the program directly, so the target keeps
    working on Linux machines, in CI and as a tool in remote actions.

    Only targets configured for Linux (`config//os:linux`) are wrapped; for
    other configurations the rule is a plain alias. Set
    `BUCK_CONTAINER_RUN=always|never` to override the run-time choice.

    Example:

    ```
    container_run(
        name = "server",
        binary = ":server-bin",
        ports = [8080],
        env = {"RUST_LOG": "info"},
    )
    ```

    See docs/users/how_tos/container_run.md for setup and details.
    """,
    attrs = buck.labels_arg() | buck.contacts_arg() | constraint_overrides.attributes | {
        "binary": attrs.dep(
            providers = [RunInfo],
            doc = "The program to run. It is configured like this target, so it is a Linux build whenever this target is.",
        ),
        "enabled": attrs.option(
            attrs.bool(),
            default = None,
            doc = "Whether to wrap `binary`. `None` wraps it only when the target is configured for Linux.",
        ),
        "env": attrs.dict(
            key = attrs.string(),
            value = attrs.arg(),
            sorted = False,
            default = {},
            doc = "Environment variables for the program. Values may use `$(location)` and appear on the command line.",
        ),
        "env_passthrough": attrs.list(
            attrs.string(),
            default = [],
            doc = "Names of environment variables to copy from the host into the container, in addition to the toolchain's.",
        ),
        "image": attrs.option(
            attrs.string(),
            default = None,
            doc = "Image to run in. Defaults to the toolchain's `default_image`.",
        ),
        "ports": attrs.list(
            attrs.int(),
            default = [],
            doc = "Container ports to publish on the Mac's 127.0.0.1 under the same port number.",
        ),
        "run_args": attrs.list(
            attrs.string(),
            default = [],
            doc = "Extra flags for `<cli> run`, after the toolchain's.",
        ),
        "_auto_enabled": attrs.default_only(attrs.bool(default = select({
            "DEFAULT": False,
            "config//os:linux": True,
        }))),
        "_container_run_toolchain": toolchains_common.container_run(),
        "_launcher": attrs.default_only(attrs.source(default = "prelude//container/tools:container_run.sh")),
        # Without a CPU constraint, leave the choice to the CLI (the host's).
        "_platform": attrs.default_only(attrs.string(default = select({
            "DEFAULT": "",
            "config//cpu:arm64": "linux/arm64",
            "config//cpu:x86_64": "linux/amd64",
        }))),
    },
)
