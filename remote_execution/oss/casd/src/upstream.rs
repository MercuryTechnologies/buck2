/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! The remote CAS this daemon fronts, reached through the same gRPC client buck2 itself uses, so
//! TLS, headers, keepalives, batching, compression and retries behave identically.

use std::path::Path;

use anyhow::Context;
use buck2_re_configuration::Buck2OssReConfiguration;
use buck2_re_configuration::HttpHeader;
use remote_execution::DownloadRequest;
use remote_execution::GetDigestsTtlRequest;
use remote_execution::InlinedBlobWithDigest;
use remote_execution::NamedDigest;
use remote_execution::NamedDigestWithPermissions;
use remote_execution::REClient;
use remote_execution::REClientBuilder;
use remote_execution::REClientError;
use remote_execution::RemoteExecutionMetadata;
use remote_execution::TCode;
use remote_execution::UploadRequest;

use crate::digest::Digest;
use crate::digest::DigestFunction;

#[derive(Clone, Debug, Default)]
pub struct UpstreamConfig {
    /// gRPC address of the remote CAS, e.g. `grpc://cas.example.com:443`; TLS is a separate flag.
    pub address: String,
    pub tls: bool,
    pub tls_ca_certs: Option<String>,
    pub tls_client_cert: Option<String>,
    /// `Header: value` pairs added to every upstream request.
    pub http_headers: Vec<String>,
    pub instance_name: Option<String>,
}

pub struct Upstream {
    client: REClient,
    metadata: RemoteExecutionMetadata,
}

impl Upstream {
    pub async fn connect(
        config: &UpstreamConfig,
        digest_function: DigestFunction,
    ) -> anyhow::Result<Self> {
        let http_headers = config
            .http_headers
            .iter()
            .map(|h| {
                h.parse::<HttpHeader>()
                    .map_err(|e| anyhow::anyhow!("Invalid upstream header `{h}`: {e}"))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let opts = Buck2OssReConfiguration {
            cas_address: Some(config.address.clone()),
            engine_address: Some(config.address.clone()),
            action_cache_address: Some(config.address.clone()),
            // Unset lets the address scheme decide, as it does for buck2 without `tls`.
            tls: config.tls.then_some(true),
            tls_ca_certs: config.tls_ca_certs.clone(),
            tls_client_cert: config.tls_client_cert.clone(),
            http_headers,
            capabilities: Some(true),
            instance_name: config.instance_name.clone(),
            // So the client names blobs for the upstream the way buck2 does, including the
            // `blake3` resource-name segment.
            digest_algorithms: vec![digest_function.to_string().to_ascii_uppercase()],
            ..Default::default()
        };
        let client = REClientBuilder::build_and_connect(&opts)
            .await
            .with_context(|| format!("Error connecting to upstream CAS `{}`", config.address))?;
        Ok(Self {
            client,
            metadata: RemoteExecutionMetadata {
                use_case_id: "buck2-casd".to_owned(),
                ..Default::default()
            },
        })
    }

    /// Downloads `digest` to `tmp`. Returns `Ok(false)` if the upstream does not have it.
    pub async fn fetch_to(&self, digest: &Digest, tmp: &Path) -> anyhow::Result<bool> {
        let request = DownloadRequest {
            file_digests: Some(vec![NamedDigestWithPermissions {
                named_digest: NamedDigest {
                    name: tmp
                        .to_str()
                        .context("temporary path is not UTF-8")?
                        .to_owned(),
                    digest: digest.to_tdigest(),
                    ..Default::default()
                },
                is_executable: false,
                ..Default::default()
            }]),
            ..Default::default()
        };
        match self.client.download(&self.metadata, request).await {
            Ok(_) => Ok(true),
            Err(e) if is_not_found(&e) => Ok(false),
            Err(e) => Err(e.context(format!("Error fetching `{digest}` from upstream"))),
        }
    }

    pub async fn upload_file(&self, digest: &Digest, path: &Path) -> anyhow::Result<()> {
        let request = UploadRequest {
            files_with_digest: Some(vec![NamedDigest {
                name: path.to_str().context("blob path is not UTF-8")?.to_owned(),
                digest: digest.to_tdigest(),
                ..Default::default()
            }]),
            upload_only_missing: false,
            ..Default::default()
        };
        self.client
            .upload(self.metadata.clone(), request)
            .await
            .with_context(|| format!("Error uploading `{digest}` to upstream"))?;
        Ok(())
    }

    pub async fn upload_blobs(&self, blobs: Vec<(Digest, Vec<u8>)>) -> anyhow::Result<()> {
        let request = UploadRequest {
            inlined_blobs_with_digest: Some(
                blobs
                    .into_iter()
                    .map(|(digest, blob)| InlinedBlobWithDigest {
                        digest: digest.to_tdigest(),
                        blob,
                        ..Default::default()
                    })
                    .collect(),
            ),
            upload_only_missing: false,
            ..Default::default()
        };
        self.client
            .upload(self.metadata.clone(), request)
            .await
            .context("Error uploading blobs to upstream")?;
        Ok(())
    }

    /// Which of `digests` the upstream does not have.
    pub async fn find_missing(&self, digests: &[Digest]) -> anyhow::Result<Vec<Digest>> {
        let response = self
            .client
            .get_digests_ttl(
                &self.metadata,
                GetDigestsTtlRequest {
                    digests: digests.iter().map(Digest::to_tdigest).collect(),
                    ..Default::default()
                },
            )
            .await
            .context("Error querying upstream for missing blobs")?;
        let mut missing = Vec::new();
        for entry in response.digests_with_ttl {
            if entry.ttl <= 0 {
                missing.push(Digest {
                    hash: entry.digest.hash,
                    size: entry.digest.size_in_bytes,
                });
            }
        }
        Ok(missing)
    }
}

fn is_not_found(e: &anyhow::Error) -> bool {
    e.chain().any(|cause| {
        cause
            .downcast_ref::<REClientError>()
            .is_some_and(|re| re.code == TCode::NOT_FOUND)
            || cause
                .downcast_ref::<tonic::Status>()
                .is_some_and(|s| s.code() == tonic::Code::NotFound)
    })
}
