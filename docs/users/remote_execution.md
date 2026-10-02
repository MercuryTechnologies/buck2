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
- `find_missing_blobs_batch_size` - the most digests Buck2 asks about in one
  `FindMissingBlobs` request before an upload. Defaults to 1000, about 73 KB of
  SHA-256 digests. A value above `find_missing_blobs_max_digests_in_flight` is
  lowered to it.
- `find_missing_blobs_concurrency` - how many `FindMissingBlobs` requests one
  upload keeps in flight when its digests span several batches. Defaults to 16.
- `find_missing_blobs_max_digests_in_flight` - the most digests in flight in
  `FindMissingBlobs` requests across the whole daemon, shared by every upload.
  Defaults to 100000. A request waits until there is room for all its digests.
  The limit counts digests, not requests, so it bounds the server's work but not
  the number of streams open on the CAS connection.

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
