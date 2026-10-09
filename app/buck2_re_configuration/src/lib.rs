/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use std::str::FromStr;

use allocative::Allocative;
use buck2_common::legacy_configs::configs::LegacyBuckConfig;
use buck2_common::legacy_configs::key::BuckconfigKeyRef;
use buck2_core::rollout_percentage::RolloutPercentage;
use buck2_credential_helper::CredentialHelperSettings;

static BUCK2_RE_CLIENT_CFG_SECTION: &str = "buck2_re_client";

/// We put functions here that both things need to implement for code that isn't gated behind a
/// fbcode_build or not(fbcode_build)
pub trait RemoteExecutionStaticMetadataImpl: Sized {
    fn from_legacy_config(
        legacy_config: &LegacyBuckConfig,
        digest_algorithms: Vec<String>,
    ) -> buck2_error::Result<Self>;
    fn cas_semaphore_size(&self) -> usize;
    fn exec_semaphore_size(&self) -> usize;
}

#[derive(Clone, Debug, Allocative)]
pub enum CASdAddress {
    Tcp(u16),
    Uds(String),
}

impl FromStr for CASdAddress {
    type Err = buck2_error::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(path) = s.strip_prefix("unix://") {
            Ok(CASdAddress::Uds(path.to_owned()))
        } else {
            Ok(CASdAddress::Tcp(s.parse()?))
        }
    }
}

#[derive(Clone, Debug, Allocative)]
pub enum CASdMode {
    LocalWithSync,
    LocalWithoutSync,
    Remote,
}

impl FromStr for CASdMode {
    type Err = buck2_error::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "local_with_sync" => Ok(CASdMode::LocalWithSync),
            "local_without_sync" => Ok(CASdMode::LocalWithoutSync),
            "remote" => Ok(CASdMode::Remote),
            _ => Err(buck2_error::buck2_error!(
                buck2_error::ErrorTag::Input,
                "Invalid CASd mode: {}",
                s
            )),
        }
    }
}

/// How a blob held in a local CAS cache is materialized into `buck-out`.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Allocative)]
pub enum CopyPolicy {
    /// Always write a full copy.
    Copy,
    /// Always make a copy-on-write clone; fail if the filesystem cannot.
    Reflink,
    /// Make a copy-on-write clone where the filesystem supports it, otherwise copy.
    Hybrid,
}

impl FromStr for CopyPolicy {
    type Err = buck2_error::Error;

    /// Unknown values mean `Copy`; `cas_shared_cache_copy_policy_v2` relies on that. The
    /// open-source `cas_shared_cache_copy_policy` key is parsed strictly instead, see
    /// [`CopyPolicy::parse_strict`].
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "hybrid" => Ok(CopyPolicy::Hybrid),
            "reflink" => Ok(CopyPolicy::Reflink),
            _ => Ok(CopyPolicy::Copy),
        }
    }
}

impl CopyPolicy {
    /// Like `from_str`, but a typo is an error rather than a silent `Copy`.
    pub fn parse_strict(s: &str) -> buck2_error::Result<Self> {
        match s.trim() {
            "hybrid" => Ok(CopyPolicy::Hybrid),
            "reflink" => Ok(CopyPolicy::Reflink),
            "copy" => Ok(CopyPolicy::Copy),
            other => Err(buck2_error::buck2_error!(
                buck2_error::ErrorTag::Input,
                "Invalid copy policy `{}` (expected `copy`, `reflink` or `hybrid`)",
                other
            )),
        }
    }
}

#[allow(unused)]
mod fbcode {
    use buck2_common::legacy_configs::key::BuckconfigKeyRef;

    use super::*;

    /// Metadata that doesn't change between executions
    #[derive(Clone, Debug, Default, Allocative)]
    pub struct RemoteExecutionStaticMetadata {
        // gRPC settings
        pub cas_address: Option<String>,
        pub cas_connection_count: i32,
        pub shared_casd_cache_path: Option<String>,
        pub legacy_shared_casd_mode: Option<String>,
        pub shared_casd_mode_small_files: Option<CASdMode>,
        pub shared_casd_mode_large_files: Option<CASdMode>,
        pub shared_casd_cache_sync_wal_files_count: Option<u8>,
        pub shared_casd_cache_sync_wal_file_max_size: Option<u64>,
        pub shared_casd_cache_sync_max_batch_size: Option<u32>,
        pub shared_casd_cache_sync_max_delay_ms: Option<u32>,
        pub shared_casd_copy_policy: Option<CopyPolicy>,
        pub shared_casd_address: Option<CASdAddress>,
        pub shared_casd_use_tls: Option<bool>,
        pub cas_client_label: Option<String>,
        pub action_cache_address: Option<String>,
        pub action_cache_connection_count: i32,
        pub engine_address: Option<String>,
        pub engine_connection_count: i32,
        // End gRPC settings
        pub verbose_logging: bool,

        pub use_manifold_rich_client: bool,
        pub use_zippy_rich_client: bool,
        pub use_p2p: bool,

        pub cas_thread_count: i32,
        pub cas_thread_count_ratio: f32,

        pub rich_client_channels_per_blob: Option<i32>,
        pub rich_client_attempt_timeout_ms: Option<i32>,
        pub rich_client_retries_count: Option<i32>,
        pub force_enable_deduplicate_find_missing: Option<bool>,

        pub features_config_path: Option<String>,
        pub client_config_path: Option<String>,

        // curl reactor
        pub curl_reactor_max_number_of_retries: Option<i32>,
        pub curl_reactor_connection_timeout_ms: Option<i32>,
        pub curl_reactor_request_timeout_ms: Option<i32>,

        // ttl management
        pub minimal_blob_ttl_seconds: Option<i64>,
        // When less than (X*100)% of TTL remains, refresh data in the store
        pub remaining_ttl_fraction_refresh_threshold: Option<f32>,
        // Adds a randomness to when refresh the TTL
        pub remaining_ttl_random_extra_threshold: Option<f32>,

        pub disable_fallocate: bool,
        pub respect_file_symlinks: bool,

        // Thrift settings
        pub execution_concurrency_limit: i32,
        pub engine_tier: Option<String>,
        pub engine_host: Option<String>,
        pub engine_port: Option<i32>,
        // End Thrift settings
        /// When set to True, allows for cancellation of RE downloads when futures are dropped
        pub enable_download_cancellation: bool,
    }

    impl RemoteExecutionStaticMetadataImpl for RemoteExecutionStaticMetadata {
        fn from_legacy_config(
            legacy_config: &LegacyBuckConfig,
            _digest_algorithms: Vec<String>,
        ) -> buck2_error::Result<Self> {
            Ok(Self {
                cas_address: legacy_config.parse(BuckconfigKeyRef {
                    section: BUCK2_RE_CLIENT_CFG_SECTION,
                    property: "cas_address",
                })?,
                cas_connection_count: legacy_config
                    .parse(BuckconfigKeyRef {
                        section: BUCK2_RE_CLIENT_CFG_SECTION,
                        property: "cas_connection_count",
                    })?
                    .unwrap_or(16),
                shared_casd_cache_path: legacy_config.parse(BuckconfigKeyRef {
                    section: BUCK2_RE_CLIENT_CFG_SECTION,
                    property: "cas_shared_cache",
                })?,
                legacy_shared_casd_mode: legacy_config.parse(BuckconfigKeyRef {
                    section: BUCK2_RE_CLIENT_CFG_SECTION,
                    property: "cas_shared_cache_mode",
                })?,
                shared_casd_mode_small_files: legacy_config.parse(BuckconfigKeyRef {
                    section: BUCK2_RE_CLIENT_CFG_SECTION,
                    property: "cas_shared_cache_mode_small_files_v2",
                })?,
                shared_casd_mode_large_files: legacy_config.parse(BuckconfigKeyRef {
                    section: BUCK2_RE_CLIENT_CFG_SECTION,
                    property: "cas_shared_cache_mode_large_files_v2",
                })?,
                shared_casd_cache_sync_wal_files_count: legacy_config.parse(BuckconfigKeyRef {
                    section: BUCK2_RE_CLIENT_CFG_SECTION,
                    property: "cas_shared_cache_sync_wal_files_count_v2",
                })?,
                shared_casd_cache_sync_wal_file_max_size: legacy_config.parse(
                    BuckconfigKeyRef {
                        section: BUCK2_RE_CLIENT_CFG_SECTION,
                        property: "cas_shared_cache_sync_wal_file_max_size_v2",
                    },
                )?,
                shared_casd_cache_sync_max_batch_size: legacy_config.parse(BuckconfigKeyRef {
                    section: BUCK2_RE_CLIENT_CFG_SECTION,
                    property: "cas_shared_cache_sync_max_batch_size_v2",
                })?,
                shared_casd_cache_sync_max_delay_ms: legacy_config.parse(BuckconfigKeyRef {
                    section: BUCK2_RE_CLIENT_CFG_SECTION,
                    property: "cas_shared_cache_sync_max_delay_ms_v2",
                })?,
                shared_casd_copy_policy: legacy_config.parse(BuckconfigKeyRef {
                    section: BUCK2_RE_CLIENT_CFG_SECTION,
                    property: "cas_shared_cache_copy_policy_v2",
                })?,
                shared_casd_address: {
                    let port_result = legacy_config.parse(BuckconfigKeyRef {
                        section: BUCK2_RE_CLIENT_CFG_SECTION,
                        property: "cas_shared_cache_port",
                    });
                    match port_result {
                        Ok(Some(port)) => Some(port),
                        _ => legacy_config.parse(BuckconfigKeyRef {
                            section: BUCK2_RE_CLIENT_CFG_SECTION,
                            property: "cas_shared_cache_address_v2",
                        })?,
                    }
                },
                shared_casd_use_tls: legacy_config.parse(BuckconfigKeyRef {
                    section: BUCK2_RE_CLIENT_CFG_SECTION,
                    property: "cas_shared_cache_tls",
                })?,
                cas_client_label: legacy_config.parse(BuckconfigKeyRef {
                    section: BUCK2_RE_CLIENT_CFG_SECTION,
                    property: "cas_client_label_v2",
                })?,
                action_cache_address: legacy_config.parse(BuckconfigKeyRef {
                    section: BUCK2_RE_CLIENT_CFG_SECTION,
                    property: "action_cache_address",
                })?,
                action_cache_connection_count: legacy_config
                    .parse(BuckconfigKeyRef {
                        section: BUCK2_RE_CLIENT_CFG_SECTION,
                        property: "action_cache_connection_count",
                    })?
                    .unwrap_or(4),
                engine_address: legacy_config.parse(BuckconfigKeyRef {
                    section: BUCK2_RE_CLIENT_CFG_SECTION,
                    property: "engine_address",
                })?,
                engine_connection_count: legacy_config
                    .parse(BuckconfigKeyRef {
                        section: BUCK2_RE_CLIENT_CFG_SECTION,
                        property: "engine_connection_count",
                    })?
                    .unwrap_or(4),
                verbose_logging: legacy_config
                    .parse(BuckconfigKeyRef {
                        section: BUCK2_RE_CLIENT_CFG_SECTION,
                        property: "verbose_logging",
                    })?
                    .unwrap_or(false),
                cas_thread_count: legacy_config
                    .parse(BuckconfigKeyRef {
                        section: BUCK2_RE_CLIENT_CFG_SECTION,
                        property: "cas_thread_count",
                    })?
                    .unwrap_or(4),
                cas_thread_count_ratio: legacy_config
                    .parse(BuckconfigKeyRef {
                        section: BUCK2_RE_CLIENT_CFG_SECTION,
                        property: "cas_thread_count_ratio",
                    })?
                    .unwrap_or(0.0),
                use_manifold_rich_client: legacy_config
                    .parse(BuckconfigKeyRef {
                        section: BUCK2_RE_CLIENT_CFG_SECTION,
                        property: "use_manifold_rich_client_new",
                    })?
                    .unwrap_or(true),
                use_zippy_rich_client: legacy_config
                    .parse(BuckconfigKeyRef {
                        section: BUCK2_RE_CLIENT_CFG_SECTION,
                        property: "use_zippy_rich_client",
                    })?
                    .unwrap_or(false),
                use_p2p: legacy_config
                    .parse(BuckconfigKeyRef {
                        section: BUCK2_RE_CLIENT_CFG_SECTION,
                        property: "use_p2p",
                    })?
                    .unwrap_or(false),
                rich_client_channels_per_blob: legacy_config.parse(BuckconfigKeyRef {
                    section: BUCK2_RE_CLIENT_CFG_SECTION,
                    property: "rich_client_channels_per_blob",
                })?,
                rich_client_attempt_timeout_ms: legacy_config.parse(BuckconfigKeyRef {
                    section: BUCK2_RE_CLIENT_CFG_SECTION,
                    property: "rich_client_attempt_timeout_ms",
                })?,
                rich_client_retries_count: legacy_config.parse(BuckconfigKeyRef {
                    section: BUCK2_RE_CLIENT_CFG_SECTION,
                    property: "rich_client_retries_count",
                })?,
                force_enable_deduplicate_find_missing: legacy_config.parse(BuckconfigKeyRef {
                    section: BUCK2_RE_CLIENT_CFG_SECTION,
                    property: "force_enable_deduplicate_find_missing",
                })?,
                features_config_path: legacy_config.parse(BuckconfigKeyRef {
                    section: BUCK2_RE_CLIENT_CFG_SECTION,
                    property: "features_config_path",
                })?,
                client_config_path: legacy_config.parse(BuckconfigKeyRef {
                    section: BUCK2_RE_CLIENT_CFG_SECTION,
                    property: "client_config_path",
                })?,
                curl_reactor_max_number_of_retries: legacy_config.parse(BuckconfigKeyRef {
                    section: BUCK2_RE_CLIENT_CFG_SECTION,
                    property: "curl_reactor_max_number_of_retries",
                })?,
                curl_reactor_connection_timeout_ms: legacy_config.parse(BuckconfigKeyRef {
                    section: BUCK2_RE_CLIENT_CFG_SECTION,
                    property: "curl_reactor_connection_timeout_ms",
                })?,
                curl_reactor_request_timeout_ms: legacy_config.parse(BuckconfigKeyRef {
                    section: BUCK2_RE_CLIENT_CFG_SECTION,
                    property: "curl_reactor_request_timeout_ms",
                })?,
                minimal_blob_ttl_seconds: legacy_config.parse(BuckconfigKeyRef {
                    section: BUCK2_RE_CLIENT_CFG_SECTION,
                    property: "minimal_blob_ttl_seconds",
                })?,
                disable_fallocate: legacy_config
                    .parse::<RolloutPercentage>(BuckconfigKeyRef {
                        section: BUCK2_RE_CLIENT_CFG_SECTION,
                        property: "disable_fallocate",
                    })?
                    .unwrap_or(RolloutPercentage::never())
                    .roll(),
                remaining_ttl_fraction_refresh_threshold: legacy_config.parse(
                    BuckconfigKeyRef {
                        section: BUCK2_RE_CLIENT_CFG_SECTION,
                        property: "remaining_ttl_fraction_refresh_threshold",
                    },
                )?,
                remaining_ttl_random_extra_threshold: legacy_config.parse(BuckconfigKeyRef {
                    section: BUCK2_RE_CLIENT_CFG_SECTION,
                    property: "remaining_ttl_random_extra_threshold",
                })?,
                respect_file_symlinks: legacy_config
                    .parse::<RolloutPercentage>(BuckconfigKeyRef {
                        section: BUCK2_RE_CLIENT_CFG_SECTION,
                        property: "respect_file_symlinks",
                    })?
                    .unwrap_or(RolloutPercentage::never())
                    .roll(),
                execution_concurrency_limit: legacy_config
                    .parse(BuckconfigKeyRef {
                        section: BUCK2_RE_CLIENT_CFG_SECTION,
                        property: "execution_concurrency_limit",
                    })?
                    .unwrap_or(4000),
                engine_tier: legacy_config.parse(BuckconfigKeyRef {
                    section: BUCK2_RE_CLIENT_CFG_SECTION,
                    property: "engine_tier",
                })?,
                engine_host: legacy_config.parse(BuckconfigKeyRef {
                    section: BUCK2_RE_CLIENT_CFG_SECTION,
                    property: "engine_host",
                })?,
                engine_port: legacy_config.parse(BuckconfigKeyRef {
                    section: BUCK2_RE_CLIENT_CFG_SECTION,
                    property: "engine_port",
                })?,
                enable_download_cancellation: legacy_config
                    .parse(BuckconfigKeyRef {
                        section: BUCK2_RE_CLIENT_CFG_SECTION,
                        property: "enable_download_cancellation",
                    })?
                    .unwrap_or(false),
            })
        }

        fn cas_semaphore_size(&self) -> usize {
            self.cas_connection_count as usize * 30
        }

        fn exec_semaphore_size(&self) -> usize {
            self.execution_concurrency_limit as usize
        }
    }
}

#[allow(unused)]
mod not_fbcode {
    use super::*;

    /// Metadata that doesn't change between executions
    #[derive(Clone, Debug, Default, Allocative)]
    pub struct RemoteExecutionStaticMetadata(pub Buck2OssReConfiguration);

    impl RemoteExecutionStaticMetadataImpl for RemoteExecutionStaticMetadata {
        fn from_legacy_config(
            legacy_config: &LegacyBuckConfig,
            digest_algorithms: Vec<String>,
        ) -> buck2_error::Result<Self> {
            Ok(Self(Buck2OssReConfiguration::from_legacy_config(
                legacy_config,
                digest_algorithms,
            )?))
        }

        fn cas_semaphore_size(&self) -> usize {
            // FIXME: make this configurable?
            1024
        }

        fn exec_semaphore_size(&self) -> usize {
            self.0.execution_concurrency_limit.unwrap_or(400)
        }
    }
}

/// A configuration used only in our OSS builds. We still compile this always, which lets us
/// gate less code behind fbcode_build.
#[derive(Clone, Debug, Default, Allocative)]
pub struct Buck2OssReConfiguration {
    /// Address for RBE Content Addresable Storage service (including bytestream uploads service).
    /// Accepted schemes: grpc, grpcs, http, https, dns, ipv4, ipv6. If no scheme is provided,
    /// TLS is enabled by default.
    pub cas_address: Option<String>,
    /// Address for RBE Engine service (including capabilities service). Accepted schemes:
    /// grpc, grpcs, http, https, dns, ipv4, ipv6. If no scheme is provided, TLS is enabled by
    /// default.
    pub engine_address: Option<String>,
    /// Number of gRPC connections to use for RBE Engine execute requests.
    pub engine_connection_count: Option<usize>,
    /// Address for RBE Action Cache service. Accepted schemes: grpc, grpcs, http, https, dns,
    /// ipv4, ipv6. If no scheme is provided, TLS is enabled by default.
    pub action_cache_address: Option<String>,
    /// Number of gRPC connections to use for RBE Action Cache requests. Unset, it is the number
    /// `engine_connection_count` defaults to.
    pub action_cache_connection_count: Option<usize>,
    /// Whether to use TLS. Unset, TLS follows the address scheme (grpcs, https, or no scheme
    /// mean TLS). Set, it overrides the scheme: upstream buck2 takes TLS from this key alone,
    /// and tools that write its config, nsc among them, write `grpc://host:port` next to
    /// `tls = true`.
    pub tls: Option<bool>,
    /// Path to a CA certificates bundle. This must be PEM-encoded. If set, this replaces the
    /// default trust roots. If none is set, a default bundle will be used when TLS is enabled by
    /// endpoint scheme.
    ///
    /// This can contain environment variables using shell interpolation syntax (i.e. $VAR). They
    /// will be substituted before using the value.
    pub tls_ca_certs: Option<String>,
    /// Path to a client certificate (and intermediate chain), as well as its associated private
    /// key. This must be PEM-encoded and is only used when TLS is enabled by endpoint scheme.
    ///
    /// This can contain environment variables using shell interpolation syntax (i.e. $VAR). They
    /// will be substituted before using the value.
    pub tls_client_cert: Option<String>,
    /// HTTP headers to inject in all requests to RE. This is a comma-separated list of `Header:
    /// Value` pairs. Minimal validation of those headers is done here.
    ///
    /// This can contain environment variables using shell interpolation syntax (i.e. $VAR). They
    /// will be substituted before using the value.
    pub http_headers: Vec<HttpHeader>,
    /// Command line of a credential helper that provides (and refreshes) credentials for RE
    /// endpoints. The helper follows the Bazel credential helper protocol: it is invoked as
    /// `<helper> get` with a JSON request on stdin and returns a JSON response on stdout
    /// containing `headers` and an optional `expires` timestamp.
    ///
    /// The value is split into a program and its arguments using shell-like quoting rules. It
    /// can contain environment variables using shell interpolation syntax (i.e. $VAR). They will
    /// be substituted before using the value.
    pub credential_helper: Option<String>,
    /// Maximum time in seconds to wait for the credential helper to respond. Defaults to 10.
    pub credential_helper_timeout_secs: Option<u64>,
    /// How long in seconds to cache credentials returned by the credential helper when the
    /// helper does not report an `expires` timestamp. Defaults to 1800 (30 minutes).
    pub credential_helper_cache_secs: Option<u64>,
    /// Whether to query capabilities from the RBE backend.
    pub capabilities: Option<bool>,
    /// The instance name to use in requests.
    pub instance_name: Option<String>,
    /// Use the Meta version of the request metadata
    pub use_fbcode_metadata: bool,
    /// Optional override for RequestMetadata.tool_details.tool_name.
    pub request_metadata_tool_name: Option<String>,
    /// The name of an environment variable that every remotely executed action gets set to this
    /// invocation's build id, through BuildBuddy's `x-buildbuddy-platform.env-overrides` header
    /// on Execute, which leaves the Action and its digest as they are. Unset sends nothing.
    pub invocation_env_override: Option<String>,
    /// Seconds one attempt to connect to remote execution may take before it counts as failed
    /// and is retried. Unset is 60; 0 waits forever, as before this key existed.
    pub connect_timeout_s: Option<u64>,
    /// The max size for a GRPC message to be decoded.
    pub max_decoding_message_size: Option<usize>,
    /// The max cumulative blob size for batch CAS methods.
    pub max_total_batch_size: Option<usize>,
    /// Minimum blob size for remote cache compression.
    pub remote_cache_compression_threshold: Option<usize>,
    /// Maximum number of concurrent upload requests for each action.
    pub max_concurrent_uploads_per_action: Option<usize>,
    /// Maximum number of digests to ask about in a single
    /// `FindMissingBlobs` (a.k.a. `GetDigestsTtl`) RPC. Larger values
    /// reduce per-call wall-clock latency by issuing fewer round-trips,
    /// at the cost of bigger requests and more concurrent server load
    /// when many actions issue independent calls. Recommended to raise
    /// only in combination with `[buck2] deduplicate_get_digests_ttl_calls`.
    pub find_missing_blobs_batch_size: Option<usize>,
    /// How long, in milliseconds, a check of which blobs the CAS holds that is not part of an
    /// upload waits for other checks to share its `FindMissingBlobs` RPC. 0 sends each check
    /// as its own RPC.
    pub find_missing_blobs_batch_window_ms: Option<u64>,
    /// How many bytes of small blobs the client keeps in memory after reading them, so a blob
    /// read again does not reach the CAS. 0 turns the cache off.
    pub read_cache_bytes: Option<usize>,
    /// The largest blob, in bytes, the client keeps in memory after reading it. 0 turns the
    /// cache off.
    pub read_cache_max_blob_bytes: Option<usize>,
    /// How long, in milliseconds, a read of blobs small enough for `BatchReadBlobs` waits for
    /// other reads to share its RPC. 0 sends each read as its own RPC.
    pub batch_read_blobs_window_ms: Option<u64>,
    /// Time that digests are assumed to live in CAS after being touched.
    pub cas_ttl_secs: Option<i64>,
    /// Whether to chunk large remote-cache blobs using FastCDC 2020 and SpliceBlob.
    pub remote_cache_chunking: bool,
    /// Optional local directory used to cache FastCDC chunk blobs.
    pub remote_cache_chunk_cache_dir: Option<String>,
    /// Number of retry attempts for transient gRPC errors. This is the number of retries, not
    /// total attempts, so a value of 5 means each RPC may be attempted up to 6 times.
    pub retries: Option<usize>,
    /// Maximum backoff delay in milliseconds between retry attempts.
    pub retry_max_delay_ms: Option<u64>,
    /// Per-attempt timeout in seconds for unary gRPC requests.
    pub grpc_request_timeout_secs: Option<u64>,
    /// Maximum time in seconds a ByteStream download may make no read progress.
    pub bytestream_progress_timeout_secs: Option<u64>,
    /// Time in seconds an operation may stay QUEUED after its Execute before the action is
    /// executed again, within `retries`. 0 turns it off. Must exceed the time the server merges
    /// a new Execute onto a queued operation of the same action, or the new Execute joins the
    /// operation that is stuck.
    pub queued_operation_timeout_secs: Option<u64>,
    /// Time in seconds a claimed operation may go without a new message on its stream before
    /// the action is executed once more, as an Action the server neither merges nor caches. A
    /// second such stall fails the action. 0 turns it off.
    pub stalled_operation_timeout_secs: Option<u64>,
    /// Time in seconds an Execute may wait for its response headers, which the server sends with
    /// its first Operation, before it is sent again on a new connection, within `retries`. 0
    /// turns it off.
    pub execute_response_timeout_secs: Option<u64>,
    /// Interval in seconds for HTTP/2 ping frames to detect stale connections.
    pub grpc_keepalive_time_secs: Option<u64>,
    /// Timeout in seconds for receiving HTTP/2 ping acknowledgement.
    pub grpc_keepalive_timeout_secs: Option<u64>,
    /// Whether to send HTTP/2 pings when connection is idle.
    pub grpc_keepalive_while_idle: Option<bool>,
    /// Maximum number of concurrent execution requests.
    pub execution_concurrency_limit: Option<usize>,
    /// Interval in seconds for TCP keepalive probes on the socket.
    pub tcp_keepalive_secs: Option<u64>,
    /// Effective digest algorithms used by the daemon.
    /// This is used by OSS RE clients to disambiguate hash validation when
    /// multiple algorithms share the same digest length (for example, SHA256 and BLAKE3).
    pub digest_algorithms: Vec<String>,
    /// Directory of the machine-local CAS daemon (`buck2-casd --dir`). Blobs the daemon holds
    /// are cloned into `buck-out` straight from this directory instead of being received over
    /// gRPC, so every buck2 daemon on the host (any isolation dir, any checkout) shares one copy.
    /// Buck2 only ever reads from it. Unset disables directory access.
    ///
    /// This can contain environment variables using shell interpolation syntax (i.e. $VAR). They
    /// will be substituted before using the value.
    pub cas_shared_cache: Option<String>,
    /// Where the machine-local CAS daemon listens, if not at its default of a Unix socket named
    /// `buck2-casd.sock` inside `cas_shared_cache`: `unix:///path/to/socket` or a loopback TCP
    /// port number. The daemon never listens anywhere else. Whenever a daemon is configured, all
    /// CAS traffic (`cas_address`) goes to it, and it passes misses and uploads through to the
    /// real CAS.
    pub cas_shared_cache_address: Option<CASdAddress>,
    /// How blobs are cloned out of the shared cache directory. Defaults to `hybrid`.
    pub cas_shared_cache_copy_policy: Option<CopyPolicy>,
    /// `local_without_sync` (default) clones blobs from the directory; `remote` never touches
    /// the directory and only talks gRPC to the daemon. `local_with_sync` is accepted and behaves
    /// like `local_without_sync`, since the daemon owns synchronization.
    pub cas_shared_cache_mode: Option<CASdMode>,
    /// Start `buck2-casd` on demand when nothing answers at its socket or port. Defaults to
    /// true. The daemon is started with the upstream settings from this section.
    pub cas_shared_cache_autostart: Option<bool>,
    /// The `buck2-casd` executable to start. Defaults to a `buck2-casd` next to the running
    /// `buck2` binary, then to `buck2-casd` on `PATH`.
    ///
    /// This can contain environment variables using shell interpolation syntax (i.e. $VAR). They
    /// will be substituted before using the value.
    pub cas_shared_cache_binary: Option<String>,
    /// Size cap passed to an auto-started daemon as `--max-size-bytes`. Unset never evicts.
    pub cas_shared_cache_max_size_bytes: Option<u64>,
}

#[derive(Clone, Debug, Default, Allocative)]
pub struct HttpHeader {
    pub key: String,
    pub value: String,
}

impl FromStr for HttpHeader {
    type Err = buck2_error::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut iter = s.splitn(2, ':');
        match (iter.next(), iter.next()) {
            (Some(key), Some(value)) => Ok(Self {
                key: key.trim().to_owned(),
                value: value.trim().to_owned(),
            }),
            _ => Err(buck2_error::buck2_error!(
                buck2_error::ErrorTag::Input,
                "Invalid header (expect name and value separated by `:`): `{}`",
                s
            )),
        }
    }
}

impl Buck2OssReConfiguration {
    pub fn from_legacy_config(
        legacy_config: &LegacyBuckConfig,
        digest_algorithms: Vec<String>,
    ) -> buck2_error::Result<Self> {
        // this is used for all three services by default, if given; if one of
        // them has an explicit address given as well though, use that instead
        let default_address: Option<String> = legacy_config.parse(BuckconfigKeyRef {
            section: BUCK2_RE_CLIENT_CFG_SECTION,
            property: "address",
        })?;

        Ok(Self {
            cas_address: legacy_config
                .parse(BuckconfigKeyRef {
                    section: BUCK2_RE_CLIENT_CFG_SECTION,
                    property: "cas_address",
                })?
                .or(default_address.clone()),
            engine_address: legacy_config
                .parse(BuckconfigKeyRef {
                    section: BUCK2_RE_CLIENT_CFG_SECTION,
                    property: "engine_address",
                })?
                .or(default_address.clone()),
            engine_connection_count: legacy_config.parse(BuckconfigKeyRef {
                section: BUCK2_RE_CLIENT_CFG_SECTION,
                property: "engine_connection_count",
            })?,
            action_cache_address: legacy_config
                .parse(BuckconfigKeyRef {
                    section: BUCK2_RE_CLIENT_CFG_SECTION,
                    property: "action_cache_address",
                })?
                .or(default_address),
            action_cache_connection_count: legacy_config.parse(BuckconfigKeyRef {
                section: BUCK2_RE_CLIENT_CFG_SECTION,
                property: "action_cache_connection_count",
            })?,
            tls: legacy_config.parse(BuckconfigKeyRef {
                section: BUCK2_RE_CLIENT_CFG_SECTION,
                property: "tls",
            })?,
            // An empty value unsets the key, so a `.buckconfig.local` can drop a certificate
            // that a file it includes, or one written by a tool such as nsc, has set.
            tls_ca_certs: legacy_config
                .parse::<String>(BuckconfigKeyRef {
                    section: BUCK2_RE_CLIENT_CFG_SECTION,
                    property: "tls_ca_certs",
                })?
                .filter(|path| !path.trim().is_empty()),
            tls_client_cert: legacy_config
                .parse::<String>(BuckconfigKeyRef {
                    section: BUCK2_RE_CLIENT_CFG_SECTION,
                    property: "tls_client_cert",
                })?
                .filter(|path| !path.trim().is_empty()),
            http_headers: legacy_config
                .parse_list(BuckconfigKeyRef {
                    section: BUCK2_RE_CLIENT_CFG_SECTION,
                    property: "http_headers",
                })?
                .unwrap_or_default(), // Empty list is as good None.
            credential_helper: legacy_config.parse(BuckconfigKeyRef {
                section: BUCK2_RE_CLIENT_CFG_SECTION,
                property: "credential_helper",
            })?,
            credential_helper_timeout_secs: legacy_config.parse(BuckconfigKeyRef {
                section: BUCK2_RE_CLIENT_CFG_SECTION,
                property: "credential_helper_timeout_secs",
            })?,
            credential_helper_cache_secs: legacy_config.parse(BuckconfigKeyRef {
                section: BUCK2_RE_CLIENT_CFG_SECTION,
                property: "credential_helper_cache_secs",
            })?,
            capabilities: legacy_config.parse(BuckconfigKeyRef {
                section: BUCK2_RE_CLIENT_CFG_SECTION,
                property: "capabilities",
            })?,
            instance_name: legacy_config.parse(BuckconfigKeyRef {
                section: BUCK2_RE_CLIENT_CFG_SECTION,
                property: "instance_name",
            })?,
            use_fbcode_metadata: legacy_config
                .parse(BuckconfigKeyRef {
                    section: BUCK2_RE_CLIENT_CFG_SECTION,
                    property: "use_fbcode_metadata",
                })?
                .unwrap_or(false),
            request_metadata_tool_name: legacy_config.parse(BuckconfigKeyRef {
                section: BUCK2_RE_CLIENT_CFG_SECTION,
                property: "request_metadata_tool_name",
            })?,
            invocation_env_override: legacy_config.parse(BuckconfigKeyRef {
                section: BUCK2_RE_CLIENT_CFG_SECTION,
                property: "invocation_env_override",
            })?,
            connect_timeout_s: legacy_config.parse(BuckconfigKeyRef {
                section: BUCK2_RE_CLIENT_CFG_SECTION,
                property: "connect_timeout_s",
            })?,
            max_decoding_message_size: legacy_config.parse(BuckconfigKeyRef {
                section: BUCK2_RE_CLIENT_CFG_SECTION,
                property: "max_decoding_message_size",
            })?,
            max_total_batch_size: legacy_config.parse(BuckconfigKeyRef {
                section: BUCK2_RE_CLIENT_CFG_SECTION,
                property: "max_total_batch_size",
            })?,
            remote_cache_compression_threshold: legacy_config.parse(BuckconfigKeyRef {
                section: BUCK2_RE_CLIENT_CFG_SECTION,
                property: "remote_cache_compression_threshold",
            })?,
            max_concurrent_uploads_per_action: legacy_config.parse(BuckconfigKeyRef {
                section: BUCK2_RE_CLIENT_CFG_SECTION,
                property: "max_concurrent_uploads_per_action",
            })?,
            find_missing_blobs_batch_size: legacy_config.parse(BuckconfigKeyRef {
                section: BUCK2_RE_CLIENT_CFG_SECTION,
                property: "find_missing_blobs_batch_size",
            })?,
            find_missing_blobs_batch_window_ms: legacy_config.parse(BuckconfigKeyRef {
                section: BUCK2_RE_CLIENT_CFG_SECTION,
                property: "find_missing_blobs_batch_window_ms",
            })?,
            read_cache_bytes: legacy_config.parse(BuckconfigKeyRef {
                section: BUCK2_RE_CLIENT_CFG_SECTION,
                property: "read_cache_bytes",
            })?,
            read_cache_max_blob_bytes: legacy_config.parse(BuckconfigKeyRef {
                section: BUCK2_RE_CLIENT_CFG_SECTION,
                property: "read_cache_max_blob_bytes",
            })?,
            batch_read_blobs_window_ms: legacy_config.parse(BuckconfigKeyRef {
                section: BUCK2_RE_CLIENT_CFG_SECTION,
                property: "batch_read_blobs_window_ms",
            })?,
            cas_ttl_secs: legacy_config.parse(BuckconfigKeyRef {
                section: BUCK2_RE_CLIENT_CFG_SECTION,
                property: "cas_ttl_secs",
            })?,
            remote_cache_chunking: legacy_config
                .parse(BuckconfigKeyRef {
                    section: BUCK2_RE_CLIENT_CFG_SECTION,
                    property: "remote_cache_chunking",
                })?
                .unwrap_or(false),
            remote_cache_chunk_cache_dir: legacy_config.parse(BuckconfigKeyRef {
                section: BUCK2_RE_CLIENT_CFG_SECTION,
                property: "remote_cache_chunk_cache_dir",
            })?,
            retries: legacy_config.parse(BuckconfigKeyRef {
                section: BUCK2_RE_CLIENT_CFG_SECTION,
                property: "retries",
            })?,
            retry_max_delay_ms: legacy_config.parse(BuckconfigKeyRef {
                section: BUCK2_RE_CLIENT_CFG_SECTION,
                property: "retry_max_delay_ms",
            })?,
            grpc_request_timeout_secs: legacy_config.parse(BuckconfigKeyRef {
                section: BUCK2_RE_CLIENT_CFG_SECTION,
                property: "grpc_request_timeout_secs",
            })?,
            bytestream_progress_timeout_secs: legacy_config.parse(BuckconfigKeyRef {
                section: BUCK2_RE_CLIENT_CFG_SECTION,
                property: "bytestream_progress_timeout_secs",
            })?,
            queued_operation_timeout_secs: legacy_config.parse(BuckconfigKeyRef {
                section: BUCK2_RE_CLIENT_CFG_SECTION,
                property: "queued_operation_timeout_secs",
            })?,
            stalled_operation_timeout_secs: legacy_config.parse(BuckconfigKeyRef {
                section: BUCK2_RE_CLIENT_CFG_SECTION,
                property: "stalled_operation_timeout_secs",
            })?,
            execute_response_timeout_secs: legacy_config.parse(BuckconfigKeyRef {
                section: BUCK2_RE_CLIENT_CFG_SECTION,
                property: "execute_response_timeout_secs",
            })?,
            grpc_keepalive_time_secs: legacy_config.parse(BuckconfigKeyRef {
                section: BUCK2_RE_CLIENT_CFG_SECTION,
                property: "grpc_keepalive_time_secs",
            })?,
            grpc_keepalive_timeout_secs: legacy_config.parse(BuckconfigKeyRef {
                section: BUCK2_RE_CLIENT_CFG_SECTION,
                property: "grpc_keepalive_timeout_secs",
            })?,
            grpc_keepalive_while_idle: legacy_config.parse(BuckconfigKeyRef {
                section: BUCK2_RE_CLIENT_CFG_SECTION,
                property: "grpc_keepalive_while_idle",
            })?,
            execution_concurrency_limit: legacy_config.parse(BuckconfigKeyRef {
                section: BUCK2_RE_CLIENT_CFG_SECTION,
                property: "execution_concurrency_limit",
            })?,
            tcp_keepalive_secs: legacy_config.parse(BuckconfigKeyRef {
                section: BUCK2_RE_CLIENT_CFG_SECTION,
                property: "tcp_keepalive_secs",
            })?,
            digest_algorithms,
            cas_shared_cache: legacy_config.parse(BuckconfigKeyRef {
                section: BUCK2_RE_CLIENT_CFG_SECTION,
                property: "cas_shared_cache",
            })?,
            cas_shared_cache_address: legacy_config.parse(BuckconfigKeyRef {
                section: BUCK2_RE_CLIENT_CFG_SECTION,
                property: "cas_shared_cache_address",
            })?,
            cas_shared_cache_copy_policy: legacy_config
                .parse::<String>(BuckconfigKeyRef {
                    section: BUCK2_RE_CLIENT_CFG_SECTION,
                    property: "cas_shared_cache_copy_policy",
                })?
                .map(|s| CopyPolicy::parse_strict(&s))
                .transpose()?,
            cas_shared_cache_mode: legacy_config.parse(BuckconfigKeyRef {
                section: BUCK2_RE_CLIENT_CFG_SECTION,
                property: "cas_shared_cache_mode",
            })?,
            cas_shared_cache_autostart: legacy_config.parse(BuckconfigKeyRef {
                section: BUCK2_RE_CLIENT_CFG_SECTION,
                property: "cas_shared_cache_autostart",
            })?,
            cas_shared_cache_binary: legacy_config.parse(BuckconfigKeyRef {
                section: BUCK2_RE_CLIENT_CFG_SECTION,
                property: "cas_shared_cache_binary",
            })?,
            cas_shared_cache_max_size_bytes: legacy_config.parse(BuckconfigKeyRef {
                section: BUCK2_RE_CLIENT_CFG_SECTION,
                property: "cas_shared_cache_max_size_bytes",
            })?,
        })
    }

    /// How long one connection attempt may take, or `None` to wait as long as it takes. A
    /// connect that never returns would otherwise hold every action's first cache lookup behind
    /// it without an error to retry on.
    pub fn connect_timeout(&self) -> Option<std::time::Duration> {
        match self.connect_timeout_s.unwrap_or(DEFAULT_CONNECT_TIMEOUT_S) {
            0 => None,
            secs => Some(std::time::Duration::from_secs(secs)),
        }
    }
}

const DEFAULT_CONNECT_TIMEOUT_S: u64 = 60;

#[cfg(fbcode_build)]
pub use fbcode::RemoteExecutionStaticMetadata;
#[cfg(not(fbcode_build))]
pub use not_fbcode::RemoteExecutionStaticMetadata;

/// How the Build Event Service sink reaches its backend under `[bes] connection = re_client`:
/// the remote execution engine, with the same TLS identity and the same headers. Namespace
/// serves both on one endpoint, and `nsc reapi setup buck2` writes only `[buck2_re_client]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BesConnection {
    /// `grpcs://host:port` or `grpc://host:port`, the spelling `[bes] backend` takes.
    pub backend: String,
    pub headers: Vec<(String, String)>,
    /// PEM file with the client certificate and its key, when TLS is on and one is configured.
    pub tls_client_cert: Option<String>,
    pub tls_ca_certs: Option<String>,
    /// `[buck2_re_client] credential_helper`, when set: the sink asks it for the headers the
    /// remote would otherwise reject once `http_headers` expire.
    pub credential_helper: Option<CredentialHelperSettings>,
}

impl BesConnection {
    /// `Ok(None)` when no remote execution engine is configured: a checked-in
    /// `[bes] connection = re_client` then means "report when there is somewhere to report to",
    /// and a machine without `nsc reapi setup buck2` builds without a sink rather than failing.
    pub fn from_re_client(legacy_config: &LegacyBuckConfig) -> buck2_error::Result<Option<Self>> {
        let config = Buck2OssReConfiguration::from_legacy_config(legacy_config, Vec::new())?;
        let Some(engine) = config
            .engine_address
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        else {
            return Ok(None);
        };
        let (backend, tls) = grpc_backend(engine, config.tls);
        Ok(Some(Self {
            backend,
            headers: config
                .http_headers
                .iter()
                .map(|header| (header.key.clone(), header.value.clone()))
                .collect(),
            tls_client_cert: config.tls_client_cert.filter(|_| tls),
            tls_ca_certs: config.tls_ca_certs.filter(|_| tls),
            credential_helper: CredentialHelperSettings::from_options(
                config.credential_helper.as_deref(),
                config.credential_helper_timeout_secs,
                config.credential_helper_cache_secs,
            ),
        }))
    }
}

/// Where the Build Event Service sink uploads the files of a Bazel-format stream when no
/// `[bes] bazel_artifact_upload_backend` is set: the remote execution CAS, spelled as
/// `[bes] backend` is. The scheme alone does not say whether that CAS takes TLS, because
/// `tls = true` beside `grpc://host:443` turns it on, so a sink that read only the address
/// dialled a TLS port in plaintext and uploaded nothing.
pub fn bes_cas_address(legacy_config: &LegacyBuckConfig) -> buck2_error::Result<Option<String>> {
    let address = legacy_config
        .parse::<String>(BuckconfigKeyRef {
            section: BUCK2_RE_CLIENT_CFG_SECTION,
            property: "cas_address",
        })?
        .or(legacy_config.parse::<String>(BuckconfigKeyRef {
            section: BUCK2_RE_CLIENT_CFG_SECTION,
            property: "address",
        })?);
    let Some(address) = address.as_deref().map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok(None);
    };
    let tls = legacy_config.parse::<bool>(BuckconfigKeyRef {
        section: BUCK2_RE_CLIENT_CFG_SECTION,
        property: "tls",
    })?;
    Ok(Some(grpc_backend(address, tls).0))
}

/// `grpcs://host` or `grpc://host` for a remote execution address, and whether that is TLS.
/// The `tls` key wins over the scheme, as it does for the remote execution client.
fn grpc_backend(address: &str, tls: Option<bool>) -> (String, bool) {
    let (scheme, host) = match address.split_once("://") {
        Some((scheme, host)) => (Some(scheme.to_ascii_lowercase()), host),
        None => (None, address),
    };
    let tls = tls.unwrap_or_else(|| !matches!(scheme.as_deref(), Some("grpc") | Some("http")));
    (
        format!(
            "{}://{}",
            if tls { "grpcs" } else { "grpc" },
            host.trim_end_matches('/')
        ),
        tls,
    )
}

/// The `[bes]` keys that bound what a broken stream keeps and for how long. The daemon's sink
/// and the client's both read them; unset keys take the sink's defaults.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BesReplaySettings {
    pub retry_window_secs: Option<u64>,
    pub replay_spill_max_bytes: Option<u64>,
}

impl BesReplaySettings {
    pub fn from_legacy_config(legacy_config: &LegacyBuckConfig) -> buck2_error::Result<Self> {
        Ok(Self {
            retry_window_secs: legacy_config.parse(BuckconfigKeyRef {
                section: "bes",
                property: "retry_window_secs",
            })?,
            replay_spill_max_bytes: legacy_config.parse(BuckconfigKeyRef {
                section: "bes",
                property: "replay_spill_max_bytes",
            })?,
        })
    }
}

#[cfg(test)]
mod tests {
    use buck2_common::legacy_configs::configs::testing::parse;

    use super::*;

    #[test]
    fn bes_replay_settings_read_the_bes_keys() -> buck2_error::Result<()> {
        let unset = parse(&[("config", "")], "config")?;
        let set = parse(
            &[(
                "config",
                "[bes]\nretry_window_secs = 300\nreplay_spill_max_bytes = 1048576\n",
            )],
            "config",
        )?;

        assert_eq!(
            BesReplaySettings::from_legacy_config(&unset)?,
            BesReplaySettings::default()
        );
        assert_eq!(
            BesReplaySettings::from_legacy_config(&set)?,
            BesReplaySettings {
                retry_window_secs: Some(300),
                replay_spill_max_bytes: Some(1048576),
            }
        );
        let invalid = parse(&[("config", "[bes]\nretry_window_secs = soon\n")], "config")?;
        assert!(BesReplaySettings::from_legacy_config(&invalid).is_err());
        Ok(())
    }

    #[test]
    fn oss_config_parses_engine_connection_count() -> buck2_error::Result<()> {
        let legacy_config = parse(
            &[("config", "[buck2_re_client]\nengine_connection_count = 8\n")],
            "config",
        )?;
        let config = Buck2OssReConfiguration::from_legacy_config(&legacy_config, Vec::new())?;

        assert_eq!(config.engine_connection_count, Some(8));
        Ok(())
    }

    #[test]
    fn oss_config_parses_action_cache_connection_count() -> buck2_error::Result<()> {
        let unset = parse(&[("config", "")], "config")?;
        let set = parse(
            &[(
                "config",
                "[buck2_re_client]\naction_cache_connection_count = 8\n",
            )],
            "config",
        )?;

        assert_eq!(
            Buck2OssReConfiguration::from_legacy_config(&unset, Vec::new())?
                .action_cache_connection_count,
            None
        );
        assert_eq!(
            Buck2OssReConfiguration::from_legacy_config(&set, Vec::new())?
                .action_cache_connection_count,
            Some(8)
        );
        Ok(())
    }

    #[test]
    fn oss_config_parses_queued_operation_timeout_secs() -> buck2_error::Result<()> {
        let unset = parse(&[("config", "")], "config")?;
        let set = parse(
            &[(
                "config",
                "[buck2_re_client]\nqueued_operation_timeout_secs = 0\n",
            )],
            "config",
        )?;

        assert_eq!(
            Buck2OssReConfiguration::from_legacy_config(&unset, Vec::new())?
                .queued_operation_timeout_secs,
            None
        );
        assert_eq!(
            Buck2OssReConfiguration::from_legacy_config(&set, Vec::new())?
                .queued_operation_timeout_secs,
            Some(0)
        );
        Ok(())
    }

    #[test]
    fn oss_config_parses_stalled_operation_timeout_secs() -> buck2_error::Result<()> {
        let unset = parse(&[("config", "")], "config")?;
        let set = parse(
            &[(
                "config",
                "[buck2_re_client]\nstalled_operation_timeout_secs = 0\n",
            )],
            "config",
        )?;

        assert_eq!(
            Buck2OssReConfiguration::from_legacy_config(&unset, Vec::new())?
                .stalled_operation_timeout_secs,
            None
        );
        assert_eq!(
            Buck2OssReConfiguration::from_legacy_config(&set, Vec::new())?
                .stalled_operation_timeout_secs,
            Some(0)
        );
        Ok(())
    }

    #[test]
    fn oss_config_parses_execute_response_timeout_secs() -> buck2_error::Result<()> {
        let unset = parse(&[("config", "")], "config")?;
        let set = parse(
            &[(
                "config",
                "[buck2_re_client]\nexecute_response_timeout_secs = 0\n",
            )],
            "config",
        )?;

        assert_eq!(
            Buck2OssReConfiguration::from_legacy_config(&unset, Vec::new())?
                .execute_response_timeout_secs,
            None
        );
        assert_eq!(
            Buck2OssReConfiguration::from_legacy_config(&set, Vec::new())?
                .execute_response_timeout_secs,
            Some(0)
        );
        Ok(())
    }

    #[test]
    fn oss_config_parses_remote_cache_compression_threshold() -> buck2_error::Result<()> {
        let legacy_config = parse(
            &[(
                "config",
                "[buck2_re_client]\nremote_cache_compression_threshold = 100\n",
            )],
            "config",
        )?;
        let config = Buck2OssReConfiguration::from_legacy_config(&legacy_config, Vec::new())?;

        assert_eq!(config.remote_cache_compression_threshold, Some(100));
        Ok(())
    }

    #[test]
    fn oss_config_parses_request_metadata_tool_name() -> buck2_error::Result<()> {
        let legacy_config = parse(
            &[(
                "config",
                "[buck2_re_client]\nrequest_metadata_tool_name = bazel\n",
            )],
            "config",
        )?;
        let config = Buck2OssReConfiguration::from_legacy_config(&legacy_config, Vec::new())?;

        assert_eq!(config.request_metadata_tool_name.as_deref(), Some("bazel"));
        Ok(())
    }

    #[test]
    fn oss_config_parses_invocation_env_override() -> buck2_error::Result<()> {
        let legacy_config = parse(
            &[(
                "config",
                "[buck2_re_client]\ninvocation_env_override = GHC_WORKER_BUILD_KEY\n",
            )],
            "config",
        )?;
        let config = Buck2OssReConfiguration::from_legacy_config(&legacy_config, Vec::new())?;

        assert_eq!(
            config.invocation_env_override.as_deref(),
            Some("GHC_WORKER_BUILD_KEY")
        );
        Ok(())
    }

    #[test]
    fn oss_config_leaves_invocation_env_override_unset_by_default() -> buck2_error::Result<()> {
        let legacy_config = parse(&[("config", "")], "config")?;
        let config = Buck2OssReConfiguration::from_legacy_config(&legacy_config, Vec::new())?;

        assert_eq!(config.invocation_env_override, None);
        Ok(())
    }

    #[test]
    fn oss_config_bounds_a_connect_attempt_at_60s_unless_told_otherwise() -> buck2_error::Result<()>
    {
        let timeout = |text: &str| -> buck2_error::Result<Option<std::time::Duration>> {
            let legacy_config = parse(&[("config", text)], "config")?;
            Ok(
                Buck2OssReConfiguration::from_legacy_config(&legacy_config, Vec::new())?
                    .connect_timeout(),
            )
        };

        assert_eq!(timeout("")?, Some(std::time::Duration::from_secs(60)));
        assert_eq!(
            timeout("[buck2_re_client]\nconnect_timeout_s = 15\n")?,
            Some(std::time::Duration::from_secs(15))
        );
        assert_eq!(timeout("[buck2_re_client]\nconnect_timeout_s = 0\n")?, None);
        Ok(())
    }

    #[test]
    fn oss_config_leaves_request_metadata_tool_name_unset_by_default() -> buck2_error::Result<()> {
        let legacy_config = parse(&[("config", "")], "config")?;
        let config = Buck2OssReConfiguration::from_legacy_config(&legacy_config, Vec::new())?;

        assert_eq!(config.request_metadata_tool_name, None);
        Ok(())
    }

    #[test]
    fn bes_connection_takes_tls_from_the_tls_key_over_the_scheme() -> buck2_error::Result<()> {
        let legacy_config = parse(
            &[(
                "config",
                "[buck2_re_client]\nengine_address = grpc://reapi.example:444\ntls = true\ntls_client_cert = /tmp/client.pem\nhttp_headers = x-a:1, x-b:two words\n",
            )],
            "config",
        )?;
        let connection = BesConnection::from_re_client(&legacy_config)?.expect("engine configured");
        assert_eq!(connection.backend, "grpcs://reapi.example:444");
        assert_eq!(connection.tls_client_cert.as_deref(), Some("/tmp/client.pem"));
        assert_eq!(
            connection.headers,
            vec![
                ("x-a".to_owned(), "1".to_owned()),
                ("x-b".to_owned(), "two words".to_owned()),
            ]
        );
        Ok(())
    }

    #[test]
    fn empty_tls_paths_unset_the_certificates() -> buck2_error::Result<()> {
        let legacy_config = parse(
            &[(
                "config",
                "[buck2_re_client]\nengine_address = grpcs://reapi.example:443\ntls_ca_certs =\ntls_client_cert =\n",
            )],
            "config",
        )?;
        let config = Buck2OssReConfiguration::from_legacy_config(&legacy_config, Vec::new())?;
        assert_eq!(config.tls_ca_certs, None);
        assert_eq!(config.tls_client_cert, None);

        let connection = BesConnection::from_re_client(&legacy_config)?.expect("engine configured");
        assert_eq!(connection.tls_ca_certs, None);
        assert_eq!(connection.tls_client_cert, None);
        Ok(())
    }

    #[test]
    fn bes_connection_infers_tls_from_the_scheme_when_the_key_is_unset() -> buck2_error::Result<()> {
        let bare = parse(
            &[("config", "[buck2_re_client]\nengine_address = reapi.example:443\n")],
            "config",
        )?;
        assert_eq!(
            BesConnection::from_re_client(&bare)?.expect("engine configured").backend,
            "grpcs://reapi.example:443"
        );

        let plain = parse(
            &[(
                "config",
                "[buck2_re_client]\nengine_address = grpc://localhost:8980\ntls_client_cert = /tmp/client.pem\n",
            )],
            "config",
        )?;
        let connection = BesConnection::from_re_client(&plain)?.expect("engine configured");
        assert_eq!(connection.backend, "grpc://localhost:8980");
        assert_eq!(connection.tls_client_cert, None);
        Ok(())
    }

    #[test]
    fn bes_cas_address_takes_tls_from_the_tls_key_over_the_scheme() -> buck2_error::Result<()> {
        let legacy_config = parse(
            &[(
                "config",
                "[buck2_re_client]\nengine_address = grpc://reapi.example:443\ncas_address = grpc://cas.example:443\ntls = true\n",
            )],
            "config",
        )?;
        assert_eq!(
            bes_cas_address(&legacy_config)?.as_deref(),
            Some("grpcs://cas.example:443")
        );

        let shared = parse(
            &[("config", "[buck2_re_client]\naddress = grpc://localhost:1985\n")],
            "config",
        )?;
        assert_eq!(
            bes_cas_address(&shared)?.as_deref(),
            Some("grpc://localhost:1985")
        );

        let unset = parse(&[("config", "")], "config")?;
        assert_eq!(bes_cas_address(&unset)?, None);
        Ok(())
    }

    #[test]
    fn bes_connection_is_none_without_an_engine_address() -> buck2_error::Result<()> {
        let legacy_config = parse(&[("config", "")], "config")?;
        assert_eq!(BesConnection::from_re_client(&legacy_config)?, None);
        Ok(())
    }
}
