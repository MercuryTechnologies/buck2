---
id: remote_execution
title: Remote Execution
---

Buck2 can use services that expose
[Bazel's remote execution API](https://github.com/bazelbuild/remote-apis) in
order to run actions remotely.

Buck2 projects have been successfully tested for remote execution against
[EngFlow](https://www.engflow.com/),
[BuildBarn](https://github.com/buildbarn/bb-remote-execution) and
[BuildBuddy](https://www.buildbuddy.io). Sample project configurations for those
providers are available under
[examples/remote_execution](https://github.com/facebook/buck2/tree/main/examples/remote_execution).

## RE configuration in `.buckconfig`

Configuration for remote execution can be found under `[buck2_re_client]` in
`.buckconfig`.

Keys supported include:

- `engine_address` - address to your RE's engine.
- `action_cache_address` - address to your action cache endpoint.
- `cas_address` - address to your content-addressable storage (CAS) endpoint.
- `tls_ca_certs` - path to a CA certificates bundle. This must be PEM-encoded.
  If none is set, a default bundle will be used. This path contains environment
  variables using shell interpolation syntax (i.e. $VAR). They will be
  substituted before reading the file.
- `tls_client_cert` - path to a client certificate (and intermediate chain), as
  well as its associated private key. This must be PEM-encoded. This path can
  contain environment variables using shell interpolation syntax (i.e. $VAR).
  They will be substituted before reading the file.
- `http_headers` - HTTP headers to inject in all requests to RE. This is a
  comma-separated list of `Header: Value` pairs. Minimal validation of those
  headers is done here. This can contain environment variables using shell
  interpolation syntax ($VAR). They will be substituted before reading the file.
- `instance_name` - an instance name to pass on execution, action cache, and CAS
  requests.
- `credential_helper` - command line of a
  [credential helper](#credential-helpers) that provides, and refreshes,
  credentials for the RE endpoints. The value is split into a program and its
  arguments using shell-like quoting rules, and can contain environment
  variables using shell interpolation syntax ($VAR).
- `credential_helper_timeout_secs` - how long to wait for the credential helper
  to respond, in seconds. Defaults to 10.
- `credential_helper_cache_secs` - how long to cache the credentials returned
  by the helper when the helper does not report an `expires` timestamp, in
  seconds. Defaults to 1800 (30 minutes).

## Credential helpers

Credentials given in `.buckconfig` (`http_headers`) are read once when the
buck2 daemon connects to RE. If they expire while the daemon is running (for
example a short-lived bearer token), builds start failing until the daemon is
restarted. A credential helper is an executable that buck2 invokes instead to
obtain the current credentials, and invokes again when they expire.

```ini
[buck2_re_client]
engine_address = grpcs.example.com:443
action_cache_address = grpcs.example.com:443
cas_address = grpcs.example.com:443
credential_helper = /usr/local/bin/my-credential-helper --some-flag
```

The helper follows the
[Bazel credential helper protocol](https://github.com/EngFlow/credential-helper-spec),
so helpers written for Bazel (`--credential_helper`) work with buck2. buck2 runs
the helper as `<helper> get`, writes a JSON request to its stdin, and reads a
JSON response from its stdout:

```json
{"uri": "https://grpcs.example.com:443/"}
```

```json
{
  "headers": {"Authorization": ["Bearer <token>"]},
  "expires": "2030-01-01T12:00:00Z"
}
```

- `headers` - headers to add to every request to that endpoint. They are added
  to the ones in `http_headers`; a header from the helper replaces a static
  header of the same name.
- `expires` - optional RFC 3339 timestamp after which the helper is invoked
  again. Without it, the credentials are cached for
  `credential_helper_cache_secs`.

The helper is invoked once per endpoint address and its response is cached
until it expires. If the remote rejects a request with `UNAUTHENTICATED`, the
cached credentials are dropped and the request is retried with fresh ones. A
helper that exits with a non-zero status fails the requests that needed its
credentials; its stderr is included in the error.

Buck2 uses `SHA256` for all its hashing by default. If your RE engine requires
something else, this can be configured in `.buckconfig` as follows:

```ini
[buck2]
# Accepts BLAKE3, SHA1, or SHA256
digest_algorithms = BLAKE3
```

## RE platform configuration

Next, your build will need an
[execution platform](https://buck2.build/docs/concepts/glossary/#execution-platform)
that specifies how and where actions should be executed. For a sample platform
definition that sets up an execution platform to utilize RE, take a look at the
[EngFlow example](https://github.com/facebook/buck2/blob/main/examples/remote_execution/engflow/platforms/defs.bzl),
[BuildBarn example](https://github.com/facebook/buck2/blob/main/examples/remote_execution/buildbarn/platforms/defs.bzl),
or the
[BuildBuddy example](https://github.com/facebook/buck2/blob/main/examples/remote_execution/buildbuddy/platforms/defs.bzl).

To enable remote execution, configure the following fields in
[CommandExecutorConfig](https://buck2.build/docs/api/build/globals/#commandexecutorconfig)
as follows:

- `remote_enabled` - set to `True`.
- `local_enabled` - set to `True` if you also want to run actions locally.
- `use_limited_hybrid` - set to `False` unless you want to exclusively run
  remotely when possible.
- `remote_execution_properties` - other additional properties.
  - If the RE engine requires a container image, this can be done by setting
    `container-image` to an image URL, as is done in the example above.
