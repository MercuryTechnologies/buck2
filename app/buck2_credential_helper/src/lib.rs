/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! Support for external credential helpers.
//!
//! A credential helper is an executable that buck2 invokes to obtain credentials for a remote
//! execution endpoint, instead of (or in addition to) reading static credentials from the
//! configuration. This lets credentials that expire (e.g. bearer tokens) be refreshed while the
//! buck2 daemon keeps running.
//!
//! The protocol is the [Bazel credential helper spec]: the helper is invoked as
//! `<helper> get`, receives a JSON request on stdin (`{"uri": "https://host:port/"}`), and
//! writes a JSON response on stdout:
//!
//! ```json
//! {
//!   "headers": {"Authorization": ["Bearer ..."]},
//!   "expires": "2030-01-01T00:00:00Z"
//! }
//! ```
//!
//! [Bazel credential helper spec]: https://github.com/EngFlow/credential-helper-spec

use std::collections::HashMap;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use anyhow::Context as _;
use buck2_util::env_vars::substitute_env_vars;
use buck2_util::process::async_background_command;
use dupe::Dupe;
use serde::Deserialize;
use serde::Serialize;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tonic::metadata;
use tonic::metadata::MetadataKey;
use tonic::metadata::MetadataValue;

/// Default time to wait for the helper to respond (same as Bazel).
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

/// Default time to cache credentials for when the helper does not report an expiry (same as
/// Bazel).
pub const DEFAULT_CACHE_DURATION: Duration = Duration::from_secs(30 * 60);

#[derive(Serialize)]
struct GetCredentialsRequest<'a> {
    uri: &'a str,
}

#[derive(Deserialize, Default)]
struct GetCredentialsResponse {
    #[serde(default)]
    headers: HashMap<String, Vec<String>>,
    #[serde(default)]
    expires: Option<String>,
}

/// Credentials obtained from a credential helper for one endpoint.
pub struct Credentials {
    /// Headers to attach to every request to the endpoint.
    headers: Vec<(MetadataKey<metadata::Ascii>, MetadataValue<metadata::Ascii>)>,
}

impl Credentials {
    pub fn headers(&self) -> &[(MetadataKey<metadata::Ascii>, MetadataValue<metadata::Ascii>)] {
        &self.headers
    }
}

impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Deliberately does not print header values.
        f.debug_struct("Credentials")
            .field(
                "headers",
                &self
                    .headers
                    .iter()
                    .map(|(k, _)| k.as_str())
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

struct CacheEntry {
    credentials: Arc<Credentials>,
    /// Time at which the credentials must be fetched again.
    refresh_at: SystemTime,
}

/// Invokes a credential helper and caches its responses per endpoint.
pub struct CredentialHelper {
    /// Program followed by its arguments.
    command: Vec<String>,
    timeout: Duration,
    default_cache_duration: Duration,
    /// Cache keyed by request URI. Also serializes helper invocations so that concurrent requests
    /// for the same endpoint run the helper once.
    cache: Mutex<HashMap<String, CacheEntry>>,
}

impl std::fmt::Debug for CredentialHelper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CredentialHelper")
            .field("command", &self.command)
            .field("timeout", &self.timeout)
            .field("default_cache_duration", &self.default_cache_duration)
            .finish_non_exhaustive()
    }
}

impl CredentialHelper {
    /// Create a helper from a command line, which is split into a program and arguments using
    /// shell-like quoting rules.
    pub fn from_command_line(
        command_line: &str,
        timeout: Option<Duration>,
        default_cache_duration: Option<Duration>,
    ) -> anyhow::Result<Self> {
        let command = shlex::split(command_line)
            .with_context(|| format!("Invalid credential helper command line: `{command_line}`"))?;
        Self::new(command, timeout, default_cache_duration)
    }

    pub fn new(
        command: Vec<String>,
        timeout: Option<Duration>,
        default_cache_duration: Option<Duration>,
    ) -> anyhow::Result<Self> {
        if command.first().is_none_or(|program| program.is_empty()) {
            return Err(anyhow::anyhow!(
                "Credential helper command must not be empty"
            ));
        }
        Ok(Self {
            command,
            timeout: timeout.unwrap_or(DEFAULT_TIMEOUT),
            default_cache_duration: default_cache_duration.unwrap_or(DEFAULT_CACHE_DURATION),
            cache: Mutex::new(HashMap::new()),
        })
    }

    /// Get credentials for `uri`, invoking the helper unless valid cached credentials exist.
    pub async fn get(&self, uri: &str) -> anyhow::Result<Arc<Credentials>> {
        self.get_at(uri, SystemTime::now()).await
    }

    async fn get_at(&self, uri: &str, now: SystemTime) -> anyhow::Result<Arc<Credentials>> {
        let mut cache = self.cache.lock().await;

        if let Some(entry) = cache.get(uri) {
            if now < entry.refresh_at {
                return Ok(entry.credentials.dupe());
            }
        }

        tracing::debug!(
            "Invoking credential helper `{}` for `{}`",
            self.command[0],
            uri
        );
        let response = self.invoke(uri).await.with_context(|| {
            format!(
                "Error getting credentials for `{uri}` from credential helper `{}`",
                self.command[0]
            )
        })?;

        let entry = self.parse_response(response, now).with_context(|| {
            format!(
                "Invalid response from credential helper `{}` for `{uri}`",
                self.command[0]
            )
        })?;
        let credentials = entry.credentials.dupe();
        cache.insert(uri.to_owned(), entry);
        Ok(credentials)
    }

    /// Drop all cached credentials, so the next `get` invokes the helper again. Used when the
    /// remote reports that the credentials were rejected.
    pub async fn invalidate(&self) {
        self.cache.lock().await.clear();
    }

    async fn invoke(&self, uri: &str) -> anyhow::Result<GetCredentialsResponse> {
        let (program, args) = self.command.split_first().expect("checked in `new`");

        let mut child = async_background_command(program)
            .args(args)
            .arg("get")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .with_context(|| format!("Error spawning `{program}`"))?;

        let request = serde_json::to_vec(&GetCredentialsRequest { uri })?;
        let output = tokio::time::timeout(self.timeout, async {
            {
                let mut stdin = child.stdin.take().context("stdin was not captured")?;
                // The helper may exit without reading its input: a write error is then reported
                // through the exit status below rather than here.
                let _ignored = stdin.write_all(&request).await;
                let _ignored = stdin.shutdown().await;
            }
            child
                .wait_with_output()
                .await
                .context("Error waiting for the credential helper")
        })
        .await
        .map_err(|_elapsed| {
            anyhow::anyhow!(
                "Credential helper did not respond within {:?}",
                self.timeout
            )
        })??;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow::anyhow!(
                "Credential helper failed with {}: {}",
                output.status,
                stderr.trim()
            ));
        }

        serde_json::from_slice(&output.stdout)
            .context("Error parsing the credential helper's output as JSON")
    }

    fn parse_response(
        &self,
        response: GetCredentialsResponse,
        now: SystemTime,
    ) -> anyhow::Result<CacheEntry> {
        let refresh_at = match response.expires.as_deref() {
            Some(expires) => parse_expires(expires)?,
            None => now + self.default_cache_duration,
        };
        let headers = parse_headers(&response.headers)?;
        Ok(CacheEntry {
            credentials: Arc::new(Credentials { headers }),
            refresh_at,
        })
    }
}

/// The `credential_helper` keys of `[buck2_re_client]`, parsed but not yet running. The Build
/// Event Service sink receives these through its own configuration and builds its own
/// [`CredentialHelper`] from them, so the two clients agree on the command without sharing an
/// instance across crates that cannot depend on each other.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CredentialHelperSettings {
    pub command_line: String,
    pub timeout: Option<Duration>,
    pub cache_duration: Option<Duration>,
}

impl CredentialHelperSettings {
    pub fn from_options(
        command_line: Option<&str>,
        timeout_secs: Option<u64>,
        cache_secs: Option<u64>,
    ) -> Option<Self> {
        Some(Self {
            command_line: command_line?.to_owned(),
            timeout: timeout_secs.map(Duration::from_secs),
            cache_duration: cache_secs.map(Duration::from_secs),
        })
    }

    /// Environment variables in the command line are substituted here rather than when the
    /// configuration is read, as for `http_headers`: the values tend to hold credentials.
    pub fn build(&self) -> anyhow::Result<CredentialHelper> {
        let command_line = substitute_env_vars(&self.command_line)
            .map_err(|e| anyhow::anyhow!("{e:#}"))
            .context("Invalid `credential_helper`")?;
        CredentialHelper::from_command_line(&command_line, self.timeout, self.cache_duration)
            .context("Invalid `credential_helper`")
    }
}

fn parse_expires(expires: &str) -> anyhow::Result<SystemTime> {
    let timestamp: jiff::Timestamp = expires
        .parse()
        .with_context(|| format!("Invalid RFC 3339 timestamp in `expires`: `{expires}`"))?;
    Ok(system_time_from_unix_secs(timestamp.as_second()))
}

fn system_time_from_unix_secs(secs: i64) -> SystemTime {
    match u64::try_from(secs) {
        Ok(secs) => SystemTime::UNIX_EPOCH + Duration::from_secs(secs),
        // Before the epoch: already expired.
        Err(_) => SystemTime::UNIX_EPOCH,
    }
}

fn parse_headers(
    headers: &HashMap<String, Vec<String>>,
) -> anyhow::Result<Vec<(MetadataKey<metadata::Ascii>, MetadataValue<metadata::Ascii>)>> {
    let mut out = Vec::new();
    // Sort for a deterministic order.
    let mut names: Vec<_> = headers.keys().collect();
    names.sort();
    for name in names {
        let key = MetadataKey::<metadata::Ascii>::from_bytes(name.as_bytes())
            .with_context(|| format!("Invalid header name: `{name}`"))?;
        for value in &headers[name] {
            let value = MetadataValue::try_from(value)
                .with_context(|| format!("Invalid value for header `{name}`"))?;
            out.push((key.clone(), value));
        }
    }
    Ok(out)
}

#[cfg(all(test, unix))]
mod tests {
    use std::path::Path;
    use std::path::PathBuf;

    use super::*;

    fn write_helper(dir: &Path, script: &str) -> PathBuf {
        let path = dir.join("helper.sh");
        std::fs::write(&path, script).unwrap();
        path
    }

    /// The command line for a helper script. The script is run through `sh` rather than
    /// executed directly: tests run concurrently, and a fork in one test while another is still
    /// writing its script would make the exec fail with "text file busy".
    fn command(path: &Path) -> Vec<String> {
        vec!["/bin/sh".to_owned(), path.to_str().unwrap().to_owned()]
    }

    fn helper(path: &Path, cache: Option<Duration>) -> CredentialHelper {
        CredentialHelper::new(command(path), Some(Duration::from_secs(5)), cache).unwrap()
    }

    #[tokio::test]
    async fn test_get_headers_and_request() {
        let dir = tempfile::tempdir().unwrap();
        let request_file = dir.path().join("request.json");
        let path = write_helper(
            dir.path(),
            &format!(
                "[ \"$1\" = get ] || exit 3\ncat > {}\necho '{{\"headers\": {{\"Authorization\": [\"Bearer t\"], \"x-multi\": [\"a\", \"b\"]}}}}'",
                request_file.display()
            ),
        );
        let helper = helper(&path, None);

        let credentials = helper.get("https://cas.example.com:443/").await.unwrap();

        assert_eq!(
            std::fs::read_to_string(&request_file).unwrap(),
            r#"{"uri":"https://cas.example.com:443/"}"#
        );
        let headers: Vec<(&str, &str)> = credentials
            .headers()
            .iter()
            .map(|(k, v)| (k.as_str(), v.to_str().unwrap()))
            .collect();
        assert_eq!(
            headers,
            vec![
                ("authorization", "Bearer t"),
                ("x-multi", "a"),
                ("x-multi", "b")
            ]
        );
    }

    #[tokio::test]
    async fn test_caches_until_expiry() {
        let dir = tempfile::tempdir().unwrap();
        let counter = dir.path().join("count");
        let path = write_helper(
            dir.path(),
            &format!(
                "echo x >> {}\necho '{{\"headers\": {{}}, \"expires\": \"2000-01-01T00:00:00+01:00\"}}'",
                counter.display()
            ),
        );
        let helper = helper(&path, None);
        let calls = || std::fs::read_to_string(&counter).unwrap().lines().count();

        let before = system_time_from_unix_secs(946_681_200 - 10);
        let first = helper.get_at("https://a/", before).await.unwrap();
        assert_eq!(calls(), 1);
        let again = helper.get_at("https://a/", before).await.unwrap();
        assert!(Arc::ptr_eq(&first, &again));
        assert_eq!(calls(), 1);

        // A different URI is fetched separately.
        helper.get_at("https://b/", before).await.unwrap();
        assert_eq!(calls(), 2);

        // Expired (2000-01-01T00:00:00+01:00 is 946681200): fetched again.
        let after = system_time_from_unix_secs(946_681_200);
        helper.get_at("https://a/", after).await.unwrap();
        assert_eq!(calls(), 3);

        // Explicit invalidation.
        helper.invalidate().await;
        helper.get_at("https://a/", before).await.unwrap();
        assert_eq!(calls(), 4);
    }

    #[tokio::test]
    async fn test_default_cache_duration_without_expires() {
        let dir = tempfile::tempdir().unwrap();
        let counter = dir.path().join("count");
        let path = write_helper(
            dir.path(),
            &format!("echo x >> {}\necho '{{}}'", counter.display()),
        );
        let helper = helper(&path, Some(Duration::from_secs(100)));
        let calls = || std::fs::read_to_string(&counter).unwrap().lines().count();

        let start = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);
        helper.get_at("https://a/", start).await.unwrap();
        helper
            .get_at("https://a/", start + Duration::from_secs(99))
            .await
            .unwrap();
        assert_eq!(calls(), 1);
        helper
            .get_at("https://a/", start + Duration::from_secs(100))
            .await
            .unwrap();
        assert_eq!(calls(), 2);
    }

    #[tokio::test]
    async fn test_failures_are_reported_and_not_cached() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_helper(dir.path(), "echo 'please log in' >&2\nexit 1");
        let helper = helper(&path, None);

        let err = helper.get("https://a/").await.unwrap_err();
        let message = format!("{err:#}");
        assert!(message.contains("please log in"), "{message}");
        assert!(message.contains("https://a/"), "{message}");

        let path = write_helper(dir.path(), "echo 'not json'");
        let helper = self::helper(&path, None);
        let err = helper.get("https://a/").await.unwrap_err();
        assert!(format!("{err:#}").contains("JSON"), "{err:#}");
    }

    #[tokio::test]
    async fn test_timeout() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_helper(dir.path(), "sleep 30");
        let helper =
            CredentialHelper::new(command(&path), Some(Duration::from_millis(200)), None).unwrap();

        let err = helper.get("https://a/").await.unwrap_err();
        assert!(
            format!("{err:#}").contains("did not respond within"),
            "{err:#}"
        );
    }

    #[tokio::test]
    async fn test_missing_helper() {
        let helper =
            CredentialHelper::new(vec!["/nonexistent/buck2-helper".to_owned()], None, None)
                .unwrap();
        assert!(helper.get("https://a/").await.is_err());
        assert!(CredentialHelper::new(vec![], None, None).is_err());
        assert!(CredentialHelper::from_command_line("", None, None).is_err());
    }

    #[test]
    fn test_from_command_line_splits_arguments() {
        let helper =
            CredentialHelper::from_command_line("/bin/helper --flag 'a b'", None, None).unwrap();
        assert_eq!(helper.command, vec!["/bin/helper", "--flag", "a b"]);
        assert_eq!(helper.timeout, DEFAULT_TIMEOUT);
        assert_eq!(helper.default_cache_duration, DEFAULT_CACHE_DURATION);
    }

    #[test]
    fn test_settings_build_substitutes_env_vars() {
        let settings =
            CredentialHelperSettings::from_options(Some("$HOME/helper --flag"), Some(3), None)
                .unwrap();
        let helper = settings.build().unwrap();
        assert_eq!(
            helper.command,
            vec![
                format!("{}/helper", std::env::var("HOME").unwrap()),
                "--flag".to_owned()
            ]
        );
        assert_eq!(helper.timeout, Duration::from_secs(3));
        assert_eq!(helper.default_cache_duration, DEFAULT_CACHE_DURATION);

        assert!(CredentialHelperSettings::from_options(None, Some(3), Some(4)).is_none());
        let unset = CredentialHelperSettings::from_options(
            Some("/bin/helper $BUCK2_CREDENTIAL_HELPER_TEST_UNSET_VAR"),
            None,
            None,
        )
        .unwrap();
        assert!(unset.build().is_err());
    }

    #[test]
    fn test_invalid_responses() {
        let helper = CredentialHelper::new(vec!["helper".to_owned()], None, None).unwrap();
        let now = SystemTime::now();

        let bad_expires = GetCredentialsResponse {
            expires: Some("tomorrow".to_owned()),
            ..Default::default()
        };
        assert!(helper.parse_response(bad_expires, now).is_err());

        let bad_header = GetCredentialsResponse {
            headers: HashMap::from([("bad header".to_owned(), vec!["v".to_owned()])]),
            ..Default::default()
        };
        assert!(helper.parse_response(bad_header, now).is_err());
    }
}
