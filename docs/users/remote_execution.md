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
  - Supported schemes: `grpc://`, `grpcs://`, `http://`, `https://`, and gRPC
    resolver schemes (`dns://`, `ipv4://`, `ipv6://`).
  - If no scheme is provided, Buck2 treats the endpoint as TLS-enabled.
- `tls_ca_certs` - path to a CA certificates bundle. This must be PEM-encoded.
  If set, this replaces the default trust roots. If none is set, a default
  bundle will be used. This path contains environment variables using shell
  interpolation syntax (i.e. $VAR). They will be substituted before reading the
  file.
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
- `request_metadata_tool_name` - optional override for
  `RequestMetadata.tool_details.tool_name` in the Bazel remote execution request
  metadata. This defaults to `buck2`.
- `capabilities` - whether Buck2 should query the RE capabilities service. This
  defaults to enabled.
- `max_total_batch_size` - optional client-side cap for cumulative blob size in
  batch CAS requests. Buck2 also honors a smaller server-advertised
  `max_batch_total_size_bytes`.
- `remote_cache_chunking` - enables FastCDC 2020 chunked uploads and downloads
  for large CAS blobs. This requires `capabilities` to be enabled and the server
  to advertise SplitBlob, SpliceBlob, and FastCDC 2020 parameters.
- `remote_cache_chunk_cache_dir` - optional local directory for FastCDC chunk
  blobs. When set with `remote_cache_chunking`, Buck2 checks this directory
  before downloading chunks from remote CAS and writes validated chunks after
  chunked uploads or downloads. The directory is managed by the user.

Buck2 uses `SHA256` for all its hashing by default. If your RE engine requires
something else, this can be configured in `.buckconfig` as follows:

```ini
[buck2]
# Accepts BLAKE3, SHA1, or SHA256
digest_algorithms = BLAKE3
```

When capabilities are enabled, Buck2 records the advertised digest functions,
compressed ByteStream support, action-cache update support, SplitBlob/SpliceBlob
support, FastCDC 2020 chunking parameters, execution priority ranges, and CAS
upload limits in the daemon logs. Buck2 also validates the server's advertised
RE API version range and fails connection setup if there is no compatible
version overlap. If the server advertises `max_cas_blob_size_bytes`, Buck2
rejects larger CAS uploads locally instead of waiting for the server to return
an upload error. Buck2 also checks that the remote cache and enabled remote
execution capabilities advertise the effective `[buck2] digest_algorithms` used
by the daemon. Older servers may omit the remote cache digest function list; in
that case Buck2 warns and assumes SHA256 for remote cache compatibility.
When the server does not advertise enabled remote execution, or when a nonzero
execution priority is outside the advertised supported ranges, Buck2 rejects the
`Execute` request locally.

## Sharing downloaded blobs between daemons

Every buck2 daemon keeps its outputs under `buck-out/<isolation-dir>`, so two
daemons with different [isolation directories](../../concepts/isolation_dir.md),
or two checkouts of the same repository, each download and store their own copy
of every blob they need. `buck2-casd`, a machine-local CAS daemon that ships
with buck2, removes that duplication. It is the open-source counterpart of the
shared CAS daemon buck2 uses at Meta and takes the same configuration keys.

The daemon speaks the remote execution API's CAS and ByteStream services over a
Unix socket inside its directory, and never listens anywhere off the machine.
Buck2 sends all CAS traffic to it; it passes misses and uploads through to the
real CAS and keeps every blob it has seen as a raw, read-only file in a
directory it alone owns. Given that directory, buck2 materializes outputs by cloning those
files instead of receiving bytes over gRPC. On btrfs, XFS and APFS the clone is
a reflink, so the data exists once on disk however many daemons use it. One
store, one downloader, one eviction policy, any number of isolation dirs.

Configure it in `.buckconfig` next to the other remote execution settings:

```ini
[buck2_re_client]
engine_address = grpc://re.example.com:443
action_cache_address = grpc://re.example.com:443
cas_address = grpc://cas.example.com:443
tls = true
cas_shared_cache = /var/cache/buck2-casd
cas_shared_cache_max_size_bytes = 53687091200
```

Buck2 starts the daemon itself the first time nothing answers at
`/var/cache/buck2-casd/buck2-casd.sock`, using the `buck2-casd` binary next to
the `buck2` executable (or on `PATH`), and hands it the directory, the digest
function from `[buck2] digest_algorithms`, the size cap, and the `cas_address`,
TLS and instance name settings above as its upstream. HTTP headers are passed
through the environment, not the command line. The daemon runs in its own
session and outlives the buck2 daemon that started it; when several buck2
daemons find it missing at once, a lock file in the directory makes one of them
start it while the others wait for the socket. Its output goes to
`buck2-casd.log` and its pid to `buck2-casd.pid`, both in the directory.

The daemon then runs until `buck2 killall`, which stops it along with the buck2
daemons, or a reboot. If it goes away under a running buck2 daemon (killed,
crashed, or its directory removed) it takes its socket file with it, and the
next CAS call starts a new one; a client mid-request may see that one request
fail. The protocol between buck2 and the daemon is the remote execution API, so
an older daemon keeps working with a newer buck2; to pick up a new daemon
binary after upgrading buck2, run `buck2 killall`. To run it under a service
manager instead, start it yourself and set `cas_shared_cache_autostart = false`:

```sh
$ buck2-casd --dir /var/cache/buck2-casd \
    --upstream grpc://cas.example.com:443 --upstream-tls \
    --max-size-bytes 53687091200 \
    --digest-function sha256
```

- `cas_shared_cache` - the daemon's `--dir`. Blobs found there are cloned into
  `buck-out`; buck2 never writes to it. Environment variables in `$VAR` form are
  substituted. Unset disables directory access.
- `cas_shared_cache_address` - only needed to move the daemon off its default
  socket: `unix:///path/to/socket`, or a loopback TCP port number (the only
  option on Windows, which has no Unix sockets). Whenever a daemon is
  configured, all CAS traffic goes to it in place of `cas_address`, without TLS.
  Engine and action cache traffic still goes to the addresses configured for
  them.
- `cas_shared_cache_copy_policy` - how blobs are cloned out of the directory.
  `hybrid` (the default) reflinks where the filesystem supports it and copies
  otherwise; `reflink` fails instead of falling back; `copy` always copies.
  `hardlink` reflinks where it can and otherwise hard-links the stored blob when
  the directory and `buck-out` share a filesystem, which saves disk on ext4.
  Outputs placed that way are read-only (0444, or 0555 through a `.x` copy of
  the blob kept for executables) and share one inode with the store and every
  other checkout, so a tool that makes one writable and writes into it changes
  them all. Root ignores the read-only mode, so don't use `hardlink` for builds
  that run as root.
- `cas_shared_cache_mode` - `local_without_sync` (the default) clones from the
  directory and only fetches over gRPC when the daemon does not have a blob yet;
  `remote` never reads the directory and only talks gRPC to the daemon.
- `cas_shared_cache_autostart` - start the daemon on demand (the default).
- `cas_shared_cache_binary` - the `buck2-casd` executable to start, if it is not
  next to `buck2` or on `PATH`. Environment variables in `$VAR` form are
  substituted.
- `cas_shared_cache_max_size_bytes` - the size cap given to an auto-started
  daemon. Unset means it never evicts.

On a miss buck2 asks the daemon for the first byte of the blob, which makes the
daemon fetch and store all of it, and then clones it from the directory, so
even the first daemon to need a blob gets a shared copy rather than a private
one. If the blob still is not in the directory, buck2 falls back to receiving it
over gRPC.

Disk space is only shared when the clone is a reflink, which needs the daemon's
directory and `buck-out` to be on the same btrfs, XFS or APFS filesystem. On
other filesystems, or across filesystems, buck2 copies out of the directory: the
daemons still share the network fetch and the daemon's store, but not the
extents in `buck-out`. With the `hybrid` policy the buck2 daemon logs a warning
the first time it has to fall back. The daemon and the buck2 daemons must run
as users that can read each other's files.

Files buck2 uploads (sources and locally built outputs) pass through the daemon
too, so a second buck2 daemon that gets an action cache hit for the same action
clones the outputs without a download. The daemon verifies the hash of every
blob it stores, whether it came from a client or from upstream.

Eviction is the daemon's job. With a size cap it removes least recently
used blobs in the background once the store exceeds the cap; a clone by buck2
counts as a use. Sizes are nominal: a blob reflinked into a `buck-out` shares
its extents with that clone, and removing it from the store frees the space
only once every clone is gone too. Without a cap nothing is ever removed.
Hits and misses are reported in the `local_cache_hits_files` and related fields
of the invocation record.

Without `--upstream` the daemon is a standalone CAS, which is handy for tests
and for a purely local setup. Its other flags mirror the `[buck2_re_client]`
keys for TLS certificates, HTTP headers and the instance name used upstream;
see `buck2-casd --help`.

One thing this does not change: an action's digest includes its output paths,
and those contain the isolation directory (`buck-out/<isolation-dir>/...`), so
two isolation directories never get action cache hits from each other. What
they do share is content: any blob one of them uploads or fetches, such as an
identical output produced by both, is stored once in the daemon and cloned into
each `buck-out` on demand. Action cache hits, and with them full materialization
from the daemon's directory, happen across checkouts of the same repository,
after `buck2 clean`, and across daemon restarts.

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

## Remote cache policy

Remote-enabled executors can use the same RE backend for action-cache lookups,
dep-file-cache lookups, uploads, and execution. These settings are controlled on
`CommandExecutorConfig`:

- `remote_cache_enabled` - query the remote action cache before executing.
- `remote_dep_file_cache_enabled` - query the remote dep-file cache.
- `allow_cache_uploads` - upload locally produced action results to the remote
  cache.
- `max_cache_upload_mebibytes` - skip remote cache uploads above this size.
- `remote_cache_unavailable_fallback` - treat transient remote cache lookup
  failures as misses and continue with the next executor.

`remote_cache_unavailable_fallback` is intended for availability incidents where
execution can still proceed locally or remotely after a cache read fails. It
applies to cache lookup failures such as unavailable or timed-out cache
requests; non-cache execution failures still follow the executor's normal
fallback policy.

Buck2 also treats a stale action-cache hit as a cache miss when the action-cache
entry exists but one of the referenced output blobs is missing from CAS during
cache materialization. This allows the action to be re-executed instead of
failing the build on the stale cache entry. When this happens, Buck2 remembers
the CAS digests referenced by the stale action result for the lifetime of the
current RE client and ignores later action-cache hits that refer to those
digests. This avoids repeatedly accepting the same stale action-cache entry
after a CAS eviction.

Deferred CAS-backed outputs include their action digest, RE use case, and
expiration metadata. Buck2 refreshes those leases through the RE client before
materializing or re-uploading deferred outputs. If a deferred output is old
enough that CAS can no longer provide it, Buck2 reports the stale digest and
its action-cache origin instead of uploading an incomplete result. This recovery
policy applies to outputs from the current remote-cache result; Buck2 does not
rewind arbitrary already-declared deferred CAS dependencies after they have
expired.

If the server capabilities do not advertise enabled action-cache updates, Buck2
skips local-result cache uploads instead of issuing an unsupported
`UpdateActionResult` RPC. When an upload path asks to upload only missing blobs,
Buck2 checks CAS first and skips blobs that the server already has. Buck2 also
validates uploaded bytes against the digest advertised for local cache uploads
where the RE protocol path exposes the uploaded content to Buck2 before sending
it. Buck2 rejects size or digest mismatches locally rather than writing a
corrupt CAS blob or action-cache result. Buck2 also
rejects malformed `BatchUpdateBlobs` replies and `BatchReadBlobs` replies where
the returned digests do not match the requested batch, so batch cache operations
require a successful response for every requested digest. `FindMissingBlobs`
replies are also checked so a server cannot report unexpected or duplicate
missing digests. Buck2's SplitBlob and SpliceBlob wrappers also validate that
chunk digests can reconstruct the declared blob before those chunk lists are
used by future chunked cache transfers.
