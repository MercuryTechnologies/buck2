/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use std::sync::Mutex;
use std::sync::LazyLock;
use std::path::Path;
use std::str::FromStr;

use buck2_common::file_ops::metadata::FileDigest;
use buck2_common::file_ops::metadata::TrackedFileDigest;
use buck2_core::buck2_env;
use buck2_core::execution_types::executor_config::RemoteExecutorUseCase;
use buck2_core::fs::project::ProjectRoot;
use buck2_core::fs::project_rel_path::ProjectRelativePath;
use buck2_core::soft_error;
use buck2_data::ReUploadMetrics;
use buck2_directory::directory::directory::Directory;
use buck2_directory::directory::directory_iterator::DirectoryIterator;
use buck2_directory::directory::directory_iterator::DirectoryIteratorPathStack;
use buck2_directory::directory::directory_ref::FingerprintedDirectoryRef;
use buck2_directory::directory::entry::DirectoryEntry;
use buck2_directory::directory::fingerprinted_directory::FingerprintedDirectory;
use buck2_error::BuckErrorContext;
use buck2_error::conversion::from_any_with_tag;
use buck2_hash::BuckMutSet;
use buck2_hash::IntentionallyStdHashMap;
use dupe::Dupe;
use itertools::Itertools;
use jiff::SignedDuration;
use remote_execution::InlinedBlobWithDigest;
use remote_execution::NamedDigest;
use remote_execution::TDigest;
use remote_execution::UploadRequest;

use crate::artifact_value::ArtifactValue;
use crate::digest::CasDigestFromReExt;
use crate::digest::CasDigestToReExt;
use crate::digest_config::DigestConfig;
use crate::directory::ActionDirectoryMember;
use crate::directory::ActionFingerprintedDirectoryRef;
use crate::directory::ActionImmutableDirectory;
use crate::directory::ReDirectorySerializer;
use crate::execute::blobs::ActionBlobs;
use crate::materialize::materializer::ArtifactNotMaterializedReason;
use crate::materialize::materializer::CasDownloadInfo;
use crate::materialize::materializer::MaterializationPurpose;
use crate::materialize::materializer::MaterializeRequest;
use crate::materialize::materializer::Materializer;
use crate::materialize::materializer::ReLostInput;
use crate::materialize::materializer::ReadLease;
use crate::re::action_identity::ReActionIdentity;
use crate::re::client::RemoteExecutionClient;
use crate::re::error::with_error_handler;
use crate::re::metadata::RemoteExecutionMetadataExt;
use crate::re::presence::NegativeCache;

#[derive(Clone, Debug, Default)]
pub struct UploadStats {
    pub total: ReUploadMetrics,
    pub by_extension: IntentionallyStdHashMap<String, ReUploadMetrics>,
}

/// Files the materializer holds in memory are sent from there when they are
/// at most this large. A larger one is written to disk and streamed from the
/// file, so an upload never holds a big decompressed copy in memory.
const MAX_IN_MEMORY_UPLOAD_BYTES: u64 = 64 * 1024 * 1024;

pub struct UploadContents {
    pub files: Vec<NamedDigest>,
    pub blobs: Vec<InlinedBlobWithDigest>,
    pub stats: UploadStats,
    /// Covers the files in `files` that were materialized for the upload, until it has read them.
    pub lease: Option<ReadLease>,
}

pub struct Uploader {}

impl Uploader {
    async fn find_missing<'a>(
        client: &RemoteExecutionClient,
        input_dir: &'a ActionImmutableDirectory,
        blobs: &'a ActionBlobs,
        use_case: RemoteExecutorUseCase,
    ) -> buck2_error::Result<(
        Vec<InlinedBlobWithDigest>,
        BuckMutSet<&'a TrackedFileDigest>,
    )> {
        // Everything the action refers to; the presence check answers from the digest for
        // whatever is known to be in the CAS with time to spare.
        let mut input_digests = blobs.keys().collect::<BuckMutSet<_>>();
        for entry in input_dir.unordered_walk().without_paths() {
            let digest = match entry {
                DirectoryEntry::Dir(d) => d.as_fingerprinted_dyn().fingerprint(),
                DirectoryEntry::Leaf(ActionDirectoryMember::File(f)) => &f.digest,
                DirectoryEntry::Leaf(..) => continue,
            };
            input_digests.insert(digest);
        }
        input_digests.insert(input_dir.fingerprint());

        let mut injectable_input_digests = input_digests.clone();
        {
            for entry in input_dir.unordered_walk().without_paths() {
                let digest = match entry {
                    DirectoryEntry::Dir(d) => d.as_fingerprinted_dyn().fingerprint(),
                    DirectoryEntry::Leaf(ActionDirectoryMember::File(f)) => &f.digest,
                    DirectoryEntry::Leaf(..) => continue,
                };
                injectable_input_digests.insert(digest);
            }

            injectable_input_digests.insert(input_dir.fingerprint());
        };

        let mut upload_blobs = Vec::new();
        let mut missing_digests = BuckMutSet::default();
        add_injected_missing_digests(&injectable_input_digests, &mut missing_digests)?;
        let input_digests = input_digests.into_iter().collect::<Vec<_>>();

        // Whatever is missing gets uploaded below, so a stale miss costs one redundant upload.
        let presence = client
            .check_presence(
                use_case,
                input_digests
                    .iter()
                    .map(|digest| (*digest).dupe())
                    .collect(),
                NegativeCache::Allowed,
            )
            .await?;

        tracing::debug!("Got digests for {}", input_dir.fingerprint());

        // Now find the blobs that need to be uploaded
        for (digest, presence) in input_digests.into_iter().zip_eq(presence) {
            match presence {
                Some(expiration) => {
                    tracing::debug!(digest=%digest, %expiration, "Not uploading");
                }
                _ => {
                    tracing::debug!(digest=%digest, "Mark for upload");

                    match blobs.get(digest) {
                        Some(blob) => {
                            upload_blobs.push(InlinedBlobWithDigest {
                                blob: blob.clone().0,
                                digest: digest.to_re(),
                                ..Default::default()
                            });
                        }
                        None => {
                            missing_digests.insert(digest);
                        }
                    }
                }
            }
        }

        Ok((upload_blobs, missing_digests))
    }

    /// Separate from `upload`, and public, so that the deferred materializer's
    /// tests can see what an upload would send without an RE client.
    pub async fn collect_upload_contents<'a>(
        fs: &ProjectRoot,
        materializer: &dyn Materializer,
        dir_path: &ProjectRelativePath,
        input_dir: &'a ActionImmutableDirectory,
        mut upload_blobs: Vec<InlinedBlobWithDigest>,
        mut missing_digests: StdBuckHashSet<&'a TrackedFileDigest>,
        use_case: RemoteExecutorUseCase,
    ) -> buck2_error::Result<UploadContents> {
        // Find the file paths and directory blobs that need to be uploaded
        let mut upload_files = Vec::new();

        // Track what files should be materialized before we upload.
        let mut artifacts_to_materialize = Vec::new();
        let mut artifact_lost_inputs = Vec::new();

        // Files whose contents the materializer holds in memory, sent as blobs
        // so they need not be written to disk first.
        let mut in_memory_files = Vec::new();

        if !missing_digests.is_empty() {
            let mut upload_file_paths = Vec::new();
            let mut upload_file_digests = Vec::new();
            let mut upload_file_metadata = Vec::new();

            {
                let mut walk = input_dir.unordered_walk();
                while let Some((path, entry)) = walk.next() {
                    let digest = match entry {
                        DirectoryEntry::Dir(d) => d.as_fingerprinted_dyn().fingerprint(),
                        DirectoryEntry::Leaf(ActionDirectoryMember::File(f)) => &f.digest,
                        DirectoryEntry::Leaf(..) => continue,
                    };

                    if !missing_digests.remove(digest) {
                        continue;
                    }

                    match entry {
                        DirectoryEntry::Dir(d) => {
                            upload_blobs.push(directory_to_blob(d));
                        }
                        DirectoryEntry::Leaf(ActionDirectoryMember::File(f)) => {
                            upload_file_paths.push(dir_path.join(path.get()));
                            upload_file_digests.push(digest.to_re());
                            upload_file_metadata.push(f.dupe());
                        }
                        DirectoryEntry::Leaf(..) => unreachable!(), // TODO: Better representation of this.
                    };
                }
            }

            if missing_digests.remove(input_dir.fingerprint()) {
                upload_blobs.push(directory_to_blob(input_dir.as_fingerprinted_ref()));
            }

            assert!(
                missing_digests.is_empty(),
                "Expected a path to be found for every digest, traversal code is inconsistent. Left with {missing_digests:?}."
            );

            // Get the real path of the files we are going to upload.
            // This needs to be done because we could have A copied to B, and
            // we are asked to upload B. But since we defer local copies until
            // it's actually needed for a local run, B might not have been
            // copied yet (or ever), so we should upload A directly instead.
            let upload_file_paths = materializer
                .get_materialized_file_paths(upload_file_paths)
                .await?;

            for ((name, digest), metadata) in upload_file_paths
                .into_iter()
                .zip(upload_file_digests)
                .zip(upload_file_metadata)
            {
                match name {
                    Ok(name) => {
                        upload_files.push(NamedDigest {
                            name: fs.resolve(&name).as_maybe_relativized_str()?.to_owned(),
                            digest,
                            ..Default::default()
                        });
                    }
                    Err(
                        ref err @ ArtifactNotMaterializedReason::RequiresCasDownload {
                            ref path,
                            ref entry,
                            ref info,
                        },
                    ) => {
                        if let DirectoryEntry::Leaf(ActionDirectoryMember::File(file)) =
                            entry.as_ref()
                        {
                            // NOTE: FindMissingBlobs can report an artifact missing even when Buck
                            // can still materialize it locally from the deferred materializer.
                            // On the flip side, if a digest has been in the CAS for a very long
                            // time, it might have expired.
                            if file.digest.to_re() == digest {
                                let lost_input = ReLostInput {
                                    path: path.clone(),
                                    digest: *file.digest.data(),
                                };
                                if should_error_for_missing_digest(info) {
                                    soft_error!(
                                        "cas_missing_fatal",
                                        buck2_error::buck2_error!(
                                            buck2_error::ErrorTag::Input,
                                            "{} missing (origin: {})",
                                            file.digest,
                                            info.origin.as_display_for_not_found(),
                                        ),
                                        daemon_in_memory_state_is_corrupted: true,
                                        action_cache_is_corrupted: info.origin.guaranteed_by_action_cache()
                                    )?;

                                    return Err(buck2_error::buck2_error!(
                                        buck2_error::ErrorTag::ReCasArtifactExpired,
                                        "Your build requires an artifact that has expired in the RE CAS \
                                        and Buck does not have it. This likely happened because your Buck daemon \
                                        has been online for a long time. This error is currently unrecoverable. \
                                        To proceed, you should restart Buck using `buck2 killall`. \
                                        Debug information: {:#}",
                                        err
                                    )
                                    .context(lost_input));
                                }

                                soft_error!(
                                    "cas_missing",
                                    buck2_error::buck2_error!(
                                        buck2_error::ErrorTag::Input,
                                        "{} (expires = {}) is missing in the CAS but expected to exist as per: {:#}",
                                        file.digest,
                                        file.digest.expires()?,
                                        err
                                    ),
                                    quiet: true
                                )
                                .map_err(|e| e.context(lost_input.clone()))?;

                                // Materialize this file from CAS and include it in this upload.
                                // Skipping it would leave the downstream action with an
                                // incomplete input set after FindMissingBlobs reported it absent.
                                artifacts_to_materialize
                                    .push((path.clone(), ArtifactValue::file(metadata.clone())));
                                artifact_lost_inputs.push(Some(lost_input));
                                upload_files.push(NamedDigest {
                                    name: fs.resolve(path).as_maybe_relativized_str()?.to_owned(),
                                    digest,
                                    ..Default::default()
                                });
                                continue;
                            }
                        }

                        return Err(error_for_missing_file(&digest, err));
                    }
                    Err(ArtifactNotMaterializedReason::RequiresMaterialization { path }) => {
                        upload_files.push(NamedDigest {
                            name: fs.resolve(&path).as_maybe_relativized_str()?.to_owned(),
                            digest,
                            ..Default::default()
                        });
                        artifacts_to_materialize.push((path, ArtifactValue::file(metadata)));
                        artifact_lost_inputs.push(None);
                    }
                    Err(ArtifactNotMaterializedReason::InMemoryWrite { path, content }) => {
                        let name = fs.resolve(&path).as_maybe_relativized_str()?.to_owned();
                        if content.size_bytes() <= MAX_IN_MEMORY_UPLOAD_BYTES {
                            in_memory_files.push((name, digest, content));
                        } else {
                            upload_files.push(NamedDigest {
                                name,
                                digest,
                                ..Default::default()
                            });
                            artifacts_to_materialize.push((path, ArtifactValue::file(metadata)));
                            artifact_lost_inputs.push(None);
                        }
                    }
                    Err(
                        ref err @ ArtifactNotMaterializedReason::DeferredMaterializerCorruption {
                            ..
                        },
                    ) => {
                        return Err(error_for_missing_file(&digest, err));
                    }
                };
            }
        }

        // The upload reads these files; the lease covers it.
        let materialized_lease = if artifacts_to_materialize.is_empty() {
            None
        } else {
            let response = materializer
                .materialize(MaterializeRequest {
                    artifacts: artifacts_to_materialize,
                    outputs: Vec::new(),
                    purpose: MaterializationPurpose::IntermediateOnly,
                    re_use_case: use_case,
                })
                .await
                .buck_error_context("Error materializing paths for upload")?;
            for (result, lost_input) in response.results.into_iter().zip(artifact_lost_inputs) {
                if let Err(error) = result {
                    // A file FindMissingBlobs reported gone from the CAS names its lost input, so
                    // the action that produced it can be rewound.
                    let error = buck2_error::Error::from(error)
                        .context("Error materializing paths for upload");
                    return Err(match lost_input {
                        Some(lost_input) => error.context(lost_input),
                        None => error,
                    });
                }
            }
            Some(response.lease)
        };

        let mut in_memory_file_sizes = Vec::with_capacity(in_memory_files.len());
        for (name, digest, content) in in_memory_files {
            let blob = content
                .read()
                .with_buck_error_context(|| format!("Error reading `{name}` for upload"))?;
            if i64::try_from(blob.len()).ok() != Some(digest.size_in_bytes) {
                return Err(internal_error!(
                    "In-memory contents of `{}` are {} bytes, but its digest is {}",
                    name,
                    blob.len(),
                    digest
                ));
            }
            in_memory_file_sizes.push((name, blob.len() as u64));
            upload_blobs.push(InlinedBlobWithDigest {
                blob,
                digest,
                ..Default::default()
            });
        }

        // Compute stats of digests we're about to upload so we can report them
        // to the span end event of this stage of execution. Files sent from
        // memory are counted by their extension as well as among the blobs.
        let stats = {
            let mut stats_by_extension = IntentionallyStdHashMap::new();
            let named_files = upload_files
                .iter()
                .map(|nd| {
                    let byte_count: u64 = nd.digest.size_in_bytes.try_into().unwrap_or_default();
                    (nd.name.as_str(), byte_count)
                })
                .chain(
                    in_memory_file_sizes
                        .iter()
                        .map(|(name, byte_count)| (name.as_str(), *byte_count)),
                );
            for (name, byte_count) in named_files {
                // Aggregate metrics by file extension.
                let extension = extract_file_extension(name);
                let ext_stats: &mut ReUploadMetrics =
                    stats_by_extension.entry(extension).or_default();
                ext_stats.digests_uploaded += 1;
                ext_stats.bytes_uploaded += byte_count;
            }
            let named_digest_byte_count: u64 = upload_files
                .iter()
                .map(|nd| {
                    let byte_count: u64 = nd.digest.size_in_bytes.try_into().unwrap_or_default();
                    byte_count
                })
                .sum();
            let blob_byte_count: u64 = upload_blobs
                .iter()
                .map(|blob| {
                    let byte_count: u64 = blob.digest.size_in_bytes.try_into().unwrap_or_default();
                    byte_count
                })
                .sum();

            UploadStats {
                total: ReUploadMetrics {
                    digests_uploaded: (upload_files.len() + upload_blobs.len()) as u64,
                    bytes_uploaded: named_digest_byte_count + blob_byte_count,
                },
                by_extension: stats_by_extension,
            }
        };

        Ok(UploadContents {
            files: upload_files,
            blobs: upload_blobs,
            stats,
            lease: materialized_lease,
        })
    }

    pub async fn upload(
        fs: &ProjectRoot,
        client: &RemoteExecutionClient,
        materializer: &dyn Materializer,
        dir_path: &ProjectRelativePath,
        input_dir: &ActionImmutableDirectory,
        blobs: &ActionBlobs,
        use_case: RemoteExecutorUseCase,
        identity: Option<&ReActionIdentity<'_>>,
    ) -> buck2_error::Result<UploadStats> {
        let (upload_blobs, missing_digests) =
            Self::find_missing(client, input_dir, blobs, use_case).await?;

        if upload_blobs.is_empty() && missing_digests.is_empty() {
            return Ok(UploadStats::default());
        }

        // The lease is held until the upload below has read the files.
        let UploadContents {
            files: upload_files,
            blobs: upload_blobs,
            stats,
            lease: _materialized_lease,
        } = Self::collect_upload_contents(
            fs,
            materializer,
            dir_path,
            input_dir,
            upload_blobs,
            missing_digests,
            use_case,
        )
        .await?;

        if !upload_files.is_empty() || !upload_blobs.is_empty() {
            let mut metadata = use_case.metadata(identity);
            if let Some(id) = identity.and_then(|id| id.action_id.clone()) {
                metadata.action_id = Some(id);
            }
            with_error_handler(
                "upload",
                client.get_session_id(),
                client
                    .get_raw_re_client()
                    .upload(
                        metadata,
                        UploadRequest {
                            files_with_digest: Some(upload_files),
                            inlined_blobs_with_digest: Some(upload_blobs),
                            // all find missing checks are done previously
                            // and we can skip them and upload all digests
                            upload_only_missing: false,
                            ..Default::default()
                        },
                    )
                    .await,
            )
            .map_err(|e| {
                if e.tags().contains(&buck2_error::ErrorTag::ReInvalidArgument) {
                    buck2_error::buck2_error!(
                        buck2_error::ErrorTag::ReInvalidArgument,
                        "RE Upload failed. It looks like you might have modified files while the build \
                        was in progress. Retry your build to proceed. Debug information: {:#}",
                        e
                    )
                } else {
                    e
                }
            })?;
        };

        Ok(stats)
    }
}

#[cfg(fbcode_build)] // Relies on fbcode future sizes
buck2_util::size_assert::words_of_async_fn_future!(
    Uploader::upload,
    (_, _, _, _, _, _, _, _),
    ~287
);
#[cfg(fbcode_build)] // Relies on fbcode future sizes
buck2_util::size_assert::words_of_async_fn_future!(
    Uploader::find_missing,
    (_, _, _, _),
    ~65
);

fn should_error_for_missing_digest(info: &CasDownloadInfo) -> bool {
    // RE sometimes reports things that exist as missing. We don't fully understand why at this
    // time and this is being investigated, but we know that RE normally ensures that anything it
    // returns to us will last for another 6 hours at least. So, we silence all errors when the
    // download info is less than 5 hours, because we know those are most likely bogus. This
    // basically means that after 5 hours we might tell the user to restart even though they don't
    // need to. However, the alternative is confused users who see errors after a couple days (we
    // had 3 reports of this in a week), so for now we do accept some false positives (when RE
    // tells us a digest doesn't exist even though it does) in order to provide better UX when we
    // hit a true positive.
    if let Some(age) = info.action_age() {
        age >= SignedDuration::from_hours(5)
    } else {
        true
    }
}

fn directory_to_blob<'a, D>(d: D) -> InlinedBlobWithDigest
where
    D: ActionFingerprintedDirectoryRef<'a>,
{
    InlinedBlobWithDigest {
        digest: d.as_fingerprinted_dyn().fingerprint().to_re(),
        blob: ReDirectorySerializer::serialize_entries(d.entries()),
        ..Default::default()
    }
}

fn error_for_missing_file(
    digest: &TDigest,
    cause: &ArtifactNotMaterializedReason,
) -> buck2_error::Error {
    buck2_error::buck2_error!(
        buck2_error::ErrorTag::ReInvalidGetCasResponse,
        "Action execution requires artifact `{}` but the materializer did not return a matching \
        file for this path. This error is unrecoverable and you should restart Buck using \
        `buck2 killall`. We would appreciate a bug report. Debug information: {:#}",
        digest,
        cause,
    )
}

/// This is used for tests. We allow an environment variable to be set to report that some digests
/// are _always_ missing if they are required. This lets us test our upload paths more easily.
fn add_injected_missing_digests<'a>(
    input_digests: &BuckMutSet<&'a TrackedFileDigest>,
    missing_digests: &mut BuckMutSet<&'a TrackedFileDigest>,
) -> buck2_error::Result<()> {
    fn convert_digests(val: &str) -> buck2_error::Result<Vec<FileDigest>> {
        val.split_whitespace()
            .map(|digest| {
                let digest = TDigest::from_str(digest)
                    .map_err(|e| from_any_with_tag(e, buck2_error::ErrorTag::InvalidDigest))
                    .with_buck_error_context(|| format!("Invalid digest: `{digest}`"))?;
                // This code does not run in a test but it is only used for testing.
                let digest = FileDigest::from_re(&digest, DigestConfig::testing_default())?;
                buck2_error::Ok(digest)
            })
            .collect()
    }

    fn add_digests_once<'a>(
        input_digests: &BuckMutSet<&'a TrackedFileDigest>,
        missing_digests: &mut BuckMutSet<&'a TrackedFileDigest>,
        digests: Vec<FileDigest>,
    ) {
        static INJECTED_ONCE: LazyLock<Mutex<BuckMutSet<FileDigest>>> =
            LazyLock::new(|| Mutex::new(BuckMutSet::default()));

        for d in digests {
            let matched = input_digests.get(&d);
            if let Some(i) = matched {
                let mut injected_once = INJECTED_ONCE.lock().expect("Poisoned lock");
                if !injected_once.contains(&d) {
                    injected_once.insert(d);
                    missing_digests.insert(i);
                }
            }
        }
    }

    let ingested_digests = buck2_env!(
        "BUCK2_TEST_INJECTED_MISSING_DIGESTS",
        type=Vec<FileDigest>,
        converter=convert_digests,
        applicability=testing
    )?;
    if let Some(digests) = ingested_digests {
        for d in digests {
            if let Some(i) = input_digests.get(d) {
                missing_digests.insert(i);
            }
        }
    }

    let injected_digests_file = buck2_env!(
        "BUCK2_TEST_INJECTED_MISSING_DIGESTS_ONCE_FILE",
        applicability = testing
    )?;
    if let Some(path) = injected_digests_file {
        let injected_digests = std::fs::read_to_string(path)
            .with_buck_error_context(|| format!("Failed to read {path}"))?;
        add_digests_once(
            input_digests,
            missing_digests,
            convert_digests(&injected_digests)?,
        );
    }

    let ingested_digests_once = buck2_env!(
        "BUCK2_TEST_INJECTED_MISSING_DIGESTS_ONCE",
        type=Vec<FileDigest>,
        converter=convert_digests,
        applicability=testing
    )?;
    if let Some(digests) = ingested_digests_once {
        add_digests_once(input_digests, missing_digests, digests.to_vec());
    }

    Ok(())
}

fn extract_file_extension(path: &str) -> String {
    let path = Path::new(path);
    match path.extension() {
        Some(ext) => ext.to_string_lossy().to_lowercase(),
        None => "<empty>".to_owned(),
    }
}
