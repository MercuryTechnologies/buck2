/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! Optional OpenTelemetry (OTLP) span export.
//!
//! When an OTLP endpoint is configured via the standard `OTEL_EXPORTER_OTLP_*` environment
//! variables, this module ships `tracing` spans to that endpoint over HTTP. Unlike a server, buck2
//! is a short-lived CLI process, so the exported resource attributes describe *this invocation* (a
//! fresh `service.instance.id` per run, the compiled-in `service.version`, the host and pid) rather
//! than a long-running deployment.
//!
//! ## Deferred activation (important)
//!
//! The OTLP batch exporter spawns background threads (a batch-processor thread plus an HTTP client
//! runtime). The buck2 daemon daemonizes via `fork()` *without* a following `exec()`, and `fork()`
//! only copies the calling thread -- any other thread vanishes in the child but leaves the locks and
//! state it held (allocator, TLS/crypto, exporter queues) permanently wedged. A daemon that spawned
//! these threads pre-fork therefore deadlocks or aborts shortly after start. See the
//! "Do not create any threads before this point" invariant in `buck2_daemon::daemon`.
//!
//! So we do *not* build the exporter when the subscriber is installed. [`deferred_layer`] installs
//! an empty, reloadable slot; [`activate`] later builds the exporter and swaps it in. The daemon
//! calls [`activate`] only after it has finished daemonizing.
//!
//! ## Flushing
//!
//! Spans are buffered and only flushed periodically. buck2 exits via `libc::_exit` (see
//! `ExitResult::report`), which runs no destructors, so the buffered batch would be dropped unless
//! we drain it explicitly. [`shutdown`] does that drain and must be called on every exit path. See
//! <https://github.com/open-telemetry/opentelemetry-rust/issues/1961>.

use std::sync::OnceLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use buck2_error::conversion::from_any_with_tag;
use opentelemetry::KeyValue;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::Protocol;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace::BatchConfigBuilder;
use opentelemetry_sdk::trace::BatchSpanProcessor;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing::Subscriber;
use tracing_subscriber::Layer;
use tracing_subscriber::filter::Filtered;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::filter::Targets;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::reload;
use uuid::Uuid;

/// A type-erased layer for the subscriber `S`, so we can swap one into the reloadable slot without
/// naming the (very long) concrete OpenTelemetry layer type.
type BoxedLayer<S> = Box<dyn Layer<S> + Send + Sync>;

/// The provider backing the active OTLP layer, if any. Stored globally so that [`shutdown`] can
/// reach it from the process exit path, which lives in a different crate and runs after the layer's
/// type has been erased into the global subscriber.
static PROVIDER: OnceLock<SdkTracerProvider> = OnceLock::new();

/// Set by [`deferred_layer`]. Calling it builds the exporter (spawning its threads) and swaps the
/// real layer into the reloadable slot. We stash it as a closure so the concrete subscriber type
/// captured by the reload handle never has to escape this module.
#[allow(clippy::type_complexity)]
static ACTIVATOR: OnceLock<Box<dyn Fn() -> buck2_error::Result<()> + Send + Sync>> = OnceLock::new();

/// Guards [`activate`] so the exporter is built at most once, even though it is called from more than
/// one entry point (the daemon after daemonizing, and the client before running a command).
static ACTIVATED: AtomicBool = AtomicBool::new(false);

/// We only enable OTLP export when an endpoint is explicitly configured. This keeps the common case
/// (no telemetry) free of overhead and avoids futile connection attempts to the default
/// `localhost:4318`. These are standard OpenTelemetry variables read by the exporter itself, not
/// buck2-owned configuration, so we check them directly rather than registering them via
/// `buck2_env!` (which would surface them misleadingly in `buck2 help-env`).
fn otlp_endpoint_configured() -> bool {
    [
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
        "OTEL_EXPORTER_OTLP_ENDPOINT",
    ]
    .iter()
    .any(|var| std::env::var_os(var).is_some_and(|v| !v.is_empty()))
}

/// Resource attributes identifying this build invocation, following OpenTelemetry semantic
/// conventions (<https://opentelemetry.io/docs/specs/semconv/resource/>).
fn resource() -> Resource {
    let mut attributes = vec![
        KeyValue::new("service.name", "buck2"),
        // buck2 is not a deployed service, so `service.version` is just the compiled-in build
        // version. `CARGO_PKG_VERSION` is the portable stand-in (the Meta build stamps a richer
        // version via `BUCK2_SET_EXPLICIT_VERSION`, but that is only available in the `buck2` bin
        // crate, not here).
        KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
        // Every buck2 process is its own "instance"; a fresh v4 UUID keeps invocations distinct.
        KeyValue::new("service.instance.id", Uuid::new_v4().to_string()),
        KeyValue::new("host.arch", std::env::consts::ARCH),
        KeyValue::new("os.type", std::env::consts::OS),
        KeyValue::new("process.pid", i64::from(std::process::id())),
    ];
    if let Ok(Some(hostname)) = hostname::get().map(|h| h.into_string().ok()) {
        attributes.push(KeyValue::new("host.name", hostname));
    }
    Resource::builder().with_attributes(attributes).build()
}

/// The filter applied to telemetry. `h2`'s connection spans tick on a keep-alive timer and live far
/// longer than anything else, dominating the exported trace, so we drop them; everything else at
/// INFO and above is exported. (The fmt layer is unaffected and still honours `BUCK_LOG`.)
///
/// This filter lives *outside* the reloadable slot on purpose: per-layer filters are only assigned a
/// `FilterId` when present at subscriber-construction time, so a `Filtered` layer swapped in later
/// via reload would panic ("had no `FilterId`"). Keeping the filter on the (always-present) reload
/// layer and the swapped-in layer filter-free avoids that.
fn telemetry_filter() -> Targets {
    Targets::new()
        .with_default(LevelFilter::INFO)
        .with_target("h2::proto::connection", LevelFilter::OFF)
}

/// Install an empty, reloadable telemetry slot into the subscriber and remember how to fill it.
///
/// This spawns no threads -- it is safe to call before the daemon forks. The real exporter is only
/// created later, by [`activate`]. The returned layer should be `.with(...)`-ed onto the registry.
pub(crate) fn deferred_layer<S>() -> Filtered<reload::Layer<Option<BoxedLayer<S>>, S>, Targets, S>
where
    S: Subscriber + for<'a> LookupSpan<'a> + Send + Sync + 'static,
{
    let (layer, handle) = reload::Layer::new(None::<BoxedLayer<S>>);

    // Capture the handle (which knows the concrete `S`) in a closure so `activate` can swap the real
    // layer in without `S` ever leaving this module. `init_tracing_for_writer` runs once, so a
    // second `set` (e.g. in a test that re-inits) is harmless to ignore.
    let _ = ACTIVATOR.set(Box::new(move || {
        if let Some(built) = build_layer::<S>()? {
            handle
                .modify(|slot| *slot = Some(built))
                .map_err(|e| from_any_with_tag(e, buck2_error::ErrorTag::Tier0))?;
        }
        Ok(())
    }));

    layer.with_filter(telemetry_filter())
}

/// Build the real OTLP layer (spawning the exporter's background threads), or `None` when no
/// endpoint is configured. Boxed so the caller need not name the concrete type.
fn build_layer<S>() -> buck2_error::Result<Option<BoxedLayer<S>>>
where
    S: Subscriber + for<'a> LookupSpan<'a> + Send + Sync + 'static,
{
    if !otlp_endpoint_configured() {
        return Ok(None);
    }

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpBinary)
        .build()
        .map_err(|e| from_any_with_tag(e, buck2_error::ErrorTag::Tier0))?;

    // buck2 emits far more spans than the SDK's defaults (queue 2048, batch 512) can absorb -- a
    // real build produces thousands of action/analysis spans in bursts, overflowing the queue and
    // dropping spans ("BatchSpanProcessor dropped a Span due to queue full"). Use a much larger
    // queue, bigger export batches, and a shorter flush interval so the exporter keeps up.
    let batch_config = BatchConfigBuilder::default()
        .with_max_queue_size(16_384)
        .with_max_export_batch_size(8_192)
        .with_scheduled_delay(Duration::from_secs(10))
        .build();
    let processor = BatchSpanProcessor::builder(exporter)
        .with_batch_config(batch_config)
        .build();

    let provider = SdkTracerProvider::builder()
        .with_resource(resource())
        .with_span_processor(processor)
        .build();
    let tracer = provider.tracer("buck2");

    // Store the provider for `shutdown`. `activate` runs once, so ignore an already-set slot.
    let _ = PROVIDER.set(provider);

    // No per-layer filter here -- filtering lives on the (always-present) reload layer in
    // `deferred_layer`. See `telemetry_filter` for why.
    let layer = tracing_opentelemetry::layer().with_tracer(tracer);

    Ok(Some(Box::new(layer)))
}

/// Build the OTLP exporter and start exporting, if telemetry is configured.
///
/// This spawns the exporter's background threads, so it MUST be called only after the process has
/// finished any `fork()`-without-`exec()`. The daemon calls it right after daemonizing; the client
/// calls it before running a command (the client never `fork()`s-without-`exec()`, so any time is
/// fine). Idempotent: the exporter is built at most once. No-op if telemetry is not configured or
/// [`deferred_layer`] was never installed. Errors are logged rather than propagated -- telemetry
/// must never fail a command.
pub fn activate() {
    if ACTIVATED.swap(true, Ordering::SeqCst) {
        return;
    }
    if let Some(activate) = ACTIVATOR.get() {
        if let Err(e) = activate() {
            tracing::warn!("Failed to start OpenTelemetry exporter: {e}");
        }
    }
}

/// Flush and shut down the OTLP exporter, draining any spans still buffered in the batch processor.
///
/// This must be called before the process exits. buck2 exits via `libc::_exit`, which runs no
/// destructors, so without this the final batch of spans is silently lost. No-op when OTLP export
/// was never activated.
pub fn shutdown() {
    if let Some(provider) = PROVIDER.get() {
        // Best-effort: we are on the way out regardless, so a failed flush only costs us the last
        // batch of spans.
        if let Err(e) = provider.shutdown() {
            tracing::warn!("Failed to shut down OpenTelemetry exporter on exit: {e}");
        }
    }
}
