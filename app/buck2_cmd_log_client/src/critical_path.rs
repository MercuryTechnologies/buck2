/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use std::fmt;
use std::io::Write;
use std::time::Duration;

use buck2_client_ctx::client_ctx::BuckSubcommand;
use buck2_client_ctx::client_ctx::ClientCommandContext;
use buck2_client_ctx::common::BuckArgMatches;
use buck2_client_ctx::event_log_options::EventLogOptions;
use buck2_client_ctx::events_ctx::EventsCtx;
use buck2_client_ctx::exit_result::ClientIoError;
use buck2_client_ctx::exit_result::ExitResult;
use buck2_error::conversion::from_any_with_tag;
use buck2_event_log::stream_value::StreamValue;
use buck2_event_observer::display::CriticalPathEntryDisplay;
use buck2_event_observer::display::TargetDisplayOptions;
use buck2_event_observer::fmt_duration::fmt_duration_precise;
use serde::Serialize;
use tokio_stream::StreamExt;

use crate::LogCommandOutputFormat;
use crate::LogCommandOutputFormatWithWriter;
use crate::transform_format;

/// Show the critical path for a selected build.
///
/// This produces output listing every node on the critical path.
///
/// It includes the kind of node, its name, category and identifier, as well as total duration
/// (runtime of this node), user duration (duration the user can improve), potential improvement
/// before this node stops being on the critical path, non-critical path time, and start offset.
///
/// The `readable` format produces space-aligned columnar output with a header:
/// `<start> <total> <off_path> <user> <potential> <kind> <name> <category> <identifier> <execution_kind>`
/// Its durations are formatted human-readably (e.g. `1.2s`, `30.0ms`, `48.0µs`), consecutive
/// equivalent entries are collapsed into a single `... (×N)` row, and rows are colored by potential
/// (when stdout is a terminal). Pass `--threshold` to also hide low-impact entries.
///
/// The `tabulated` format produces tab-delimited output:
/// `<kind>\t<name>\t<category>\t<identifier>\t<execution_kind>\t<total_duration>\t<user_duration>\t<potential_improvement_duration>\t<non_critical_path_time>\t<start_offset>`
///
/// In the `tabulated`, `json`, and `csv` formats all durations are in microseconds, including the
/// start offset (measured from the beginning of the build); those formats show one row per entry
/// and are unaffected by `--threshold`.
#[derive(Debug, clap::Parser)]
pub struct CriticalPathCommand {
    #[clap(flatten)]
    common: CommonPathArgs,
}

impl BuckSubcommand for CriticalPathCommand {
    const COMMAND_NAME: &'static str = "log-critical-path";

    async fn exec_impl(
        self,
        _matches: BuckArgMatches<'_>,
        ctx: ClientCommandContext<'_>,
        _events_ctx: &mut EventsCtx,
    ) -> ExitResult {
        log_critical_path_command_exec(ctx, self.common, PathKind::Critical).await
    }
}

/// Show the slowest path for a selected build.
///
/// While the critical path represents something closer to the "ideal" build time, the slowest path gives a
/// better idea of what the build actually spends its time on. This can better highlight build overhead.
///
/// This produces output listing every node on the slowest path.
///
/// It includes the kind of node, its name, category and identifier, as well as total duration
/// (runtime of this node), user duration (duration the user can improve), non-critical time, and start offset.
///
/// The `readable` format produces space-aligned columnar output with a header:
/// `<start> <total> <off_path> <user> <potential> <kind> <name> <category> <identifier> <execution_kind>`
/// Its durations are formatted human-readably (e.g. `1.2s`, `30.0ms`, `48.0µs`), consecutive
/// equivalent entries are collapsed into a single `... (×N)` row, and rows are colored by potential
/// (when stdout is a terminal). Pass `--threshold` to also hide low-impact entries.
///
/// The `tabulated` format produces tab-delimited output:
/// `<kind>\t<name>\t<category>\t<identifier>\t<execution_kind>\t<total_duration>\t<user_duration>\t<potential_improvement_duration>\t<non_critical_path_time>\t<start_offset>`
///
/// In the `tabulated`, `json`, and `csv` formats all durations are in microseconds, including the
/// start offset (measured from the beginning of the build); those formats show one row per entry
/// and are unaffected by `--threshold`.
///
/// Note that this prints a "potential improvement" just like `log critical-path`, but for slowest paths it's not computed.
#[derive(Debug, clap::Parser)]
pub struct SlowestPathCommand {
    #[clap(flatten)]
    common: CommonPathArgs,
}

impl BuckSubcommand for SlowestPathCommand {
    const COMMAND_NAME: &'static str = "log-slowest-path";

    async fn exec_impl(
        self,
        _matches: BuckArgMatches<'_>,
        ctx: ClientCommandContext<'_>,
        _events_ctx: &mut EventsCtx,
    ) -> ExitResult {
        log_critical_path_command_exec(ctx, self.common, PathKind::Slowest).await
    }
}

/// Options shared by the `critical-path` and `slowest-path` subcommands.
#[derive(Debug, clap::Parser)]
struct CommonPathArgs {
    #[clap(flatten)]
    event_log: EventLogOptions,
    #[clap(flatten)]
    format: LogCommandOutputFormat,
    /// In the `readable` format, hide entries whose impact -- the largest of
    /// their total, off-path, and potential-improvement durations -- is below
    /// this. Repetitive and structural nodes tend to be near-zero, so a small
    /// value like `1ms` focuses the output on entries that matter. Accepts units
    /// (e.g. `500us`, `1ms`, `1s`). Defaults to `0s`, which hides nothing.
    #[clap(long, default_value = "0s")]
    threshold: humantime::Duration,
}

/// Distinguishes between critical path and slowest path computation.
enum PathKind {
    /// Critical path: the longest dependency chain determining minimum build time.
    Critical,
    /// Slowest path: the actual longest path taken, including build overhead.
    Slowest,
}

async fn log_critical_path_command_exec(
    ctx: ClientCommandContext<'_>,
    common: CommonPathArgs,
    path_kind: PathKind,
) -> ExitResult {
    let CommonPathArgs {
        event_log,
        format,
        threshold,
    } = common;
    let threshold: Duration = threshold.into();
    let log_path = event_log.get(&ctx).await?;

    let (invocation, mut events) = log_path.unpack_stream().await?;
    let path_name = match path_kind {
        PathKind::Critical => "critical path",
        PathKind::Slowest => "slowest path",
    };
    buck2_client_ctx::eprintln!(
        "Showing {} from: {}",
        path_name,
        invocation.display_command_line()
    )?;

    while let Some(event) = events.try_next().await? {
        if let StreamValue::Event(event) = event
            && let Some(buck2_data::buck_event::Data::Instant(instant)) = event.data
            && let Some(buck2_data::instant_event::Data::BuildGraphInfo(build_graph)) = instant.data
        {
            match path_kind {
                PathKind::Critical => {
                    log_critical_path(&build_graph.critical_path2, format.clone(), threshold)
                        .await?;
                }
                PathKind::Slowest => {
                    log_critical_path(&build_graph.slowest_path, format.clone(), threshold).await?;
                }
            }
        }
    }

    ExitResult::success()
}

#[derive(Default)]
struct OptionalDuration {
    inner: Option<Duration>,
}

impl OptionalDuration {
    fn new<T, E>(d: Option<T>) -> Result<Self, E>
    where
        T: TryInto<Duration, Error = E>,
    {
        Ok(Self {
            inner: d.map(|d| d.try_into()).transpose()?,
        })
    }
}

impl fmt::Display for OptionalDuration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(inner) = self.inner {
            inner.as_micros().fmt(f)?;
        } else {
            "".fmt(f)?;
        }
        Ok(())
    }
}

impl Serialize for OptionalDuration {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if let Some(micros) = self.inner.map(|d| d.as_micros()) {
            serializer.serialize_some(&micros)
        } else {
            serializer.serialize_none()
        }
    }
}

/// Represents a single entry on the critical path.
///
/// Contains information about the node type, timing information, and metadata.
#[derive(Default, Serialize)]
struct CriticalPathEntry<'a> {
    /// The kind of critical path entry (e.g., "action", "analysis", "materialization").
    kind: &'a str,
    /// The name/label of the entry (e.g., target label, package name).
    /// Empty string for entries without names.
    #[serde(skip_serializing_if = "String::is_empty")]
    name: String,
    /// Optional category (e.g., for actions).
    #[serde(skip_serializing_if = "Option::is_none")]
    category: Option<&'a str>,
    /// Optional identifier (e.g., action identifier, file path for materializations).
    #[serde(skip_serializing_if = "Option::is_none")]
    identifier: Option<&'a str>,
    /// Optional execution kind for actions (e.g., "local", "remote").
    #[serde(skip_serializing_if = "Option::is_none")]
    execution_kind: Option<&'a str>,
    /// Total wall-clock duration of this entry.
    total_duration: OptionalDuration,
    /// User-improvable duration (portion the user can optimize).
    user_duration: OptionalDuration,
    /// Potential improvement duration before this node drops off the critical path.
    potential_improvement_duration: OptionalDuration,
    /// Time spent off the critical path (non-critical path duration).
    non_critical_path_time: OptionalDuration,
    /// Start offset in microseconds from the beginning of the build.
    start_offset: u64,
}

impl CriticalPathEntry<'_> {
    /// The largest signal this entry carries: how much time it either cost the
    /// build (`total`), spent off the path (`non_critical`), or could save
    /// (`potential`). Used to decide whether the entry is worth showing.
    fn impact(&self) -> Duration {
        [
            self.total_duration.inner,
            self.non_critical_path_time.inner,
            self.user_duration.inner,
            self.potential_improvement_duration.inner,
        ]
        .into_iter()
        .flatten()
        .max()
        .unwrap_or(Duration::ZERO)
    }

    /// Whether two entries are identical apart from their timing, and so can be
    /// collapsed into a single `readable` row. We deliberately require the name,
    /// category, etc. to match: distinct named nodes (e.g. two different actions)
    /// stay on their own rows so you can still see *which* target is slow.
    fn same_identity(&self, other: &Self) -> bool {
        self.kind == other.kind
            && self.name == other.name
            && self.category == other.category
            && self.identifier == other.identifier
            && self.execution_kind == other.execution_kind
    }
}

/// The `kind` shared by all waiting entries (see `CriticalPathEntryDisplay`).
const WAITING_KIND: &str = "waiting";

/// Pick the color for a row from how much time it could save. `console::Style`
/// emits ANSI codes only when colors are enabled (i.e. stdout is a terminal and
/// `NO_COLOR` etc. permit it), so the "normal" band is just an empty style.
fn row_style(potential: Duration) -> console::Style {
    let style = console::Style::new();
    if potential < Duration::from_millis(100) {
        style.black().bright() // gray
    } else if potential < Duration::from_secs(1) {
        style // normal
    } else if potential < Duration::from_secs(5) {
        style.yellow()
    } else if potential < Duration::from_secs(10) {
        style.red()
    } else {
        style.red().bright().bold()
    }
}

/// Collapses consecutive equivalent entries in the `readable` output into a
/// single `... (×N)` row with summed durations, so repetitive bursts don't drown
/// out the entries that matter. Two kinds of run are collapsed:
///  - Identical entries (same kind/name/category/...), e.g. a burst of the same
///    action; distinct named nodes stay separate so you can see *which* is slow.
///  - Any consecutive `waiting` entries, regardless of category: these are all
///    unattributed gaps, so `waiting unknown` interleaved with `waiting for_deps`
///    collapses to a single `waiting unknown / for_deps (×N)` row.
#[derive(Default)]
struct Run<'a> {
    current: Option<RunState<'a>>,
}

struct RunState<'a> {
    /// First entry of the run, used for the columns (category, identifier, ...)
    /// that are shared across the whole run.
    first: CriticalPathEntry<'a>,
    /// Distinct names seen in the run, in first-seen order. Usually one entry;
    /// for a collapsed waiting run it holds each category (e.g. `unknown`).
    names: Vec<String>,
    count: u64,
    /// Summed durations. Each stays `None` until a present value is added, so an
    /// always-absent column (e.g. `potential` on the slowest path) still renders
    /// blank rather than a misleading `0`.
    total: Option<Duration>,
    off_path: Option<Duration>,
    user: Option<Duration>,
    potential: Option<Duration>,
}

/// Add an optional duration into a running total, leaving it `None` until the
/// first present value.
fn add_opt(acc: &mut Option<Duration>, v: Option<Duration>) {
    if let Some(v) = v {
        *acc = Some(acc.unwrap_or_default() + v);
    }
}

/// Render a summed column: blank when absent, human-readable otherwise.
fn fmt_opt(d: Option<Duration>) -> String {
    match d {
        Some(d) => fmt_duration_precise(d),
        None => String::new(),
    }
}

/// Whether `entry` can extend a run whose first entry is `first`.
fn can_extend(first: &CriticalPathEntry, entry: &CriticalPathEntry) -> bool {
    if first.kind == WAITING_KIND && entry.kind == WAITING_KIND {
        true
    } else {
        first.same_identity(entry)
    }
}

impl<'a> Run<'a> {
    /// Add an entry, either extending the current run or flushing it and starting
    /// a new one.
    fn push(
        &mut self,
        writer: &mut dyn std::io::Write,
        entry: CriticalPathEntry<'a>,
    ) -> std::io::Result<()> {
        match &mut self.current {
            Some(state) if can_extend(&state.first, &entry) => {
                state.count += 1;
                add_opt(&mut state.total, entry.total_duration.inner);
                add_opt(&mut state.off_path, entry.non_critical_path_time.inner);
                add_opt(&mut state.user, entry.user_duration.inner);
                add_opt(
                    &mut state.potential,
                    entry.potential_improvement_duration.inner,
                );
                if !state.names.iter().any(|n| n == &entry.name) {
                    state.names.push(entry.name);
                }
            }
            _ => {
                self.flush(writer)?;
                self.current = Some(RunState {
                    names: vec![entry.name.clone()],
                    total: entry.total_duration.inner,
                    off_path: entry.non_critical_path_time.inner,
                    user: entry.user_duration.inner,
                    potential: entry.potential_improvement_duration.inner,
                    count: 1,
                    first: entry,
                });
            }
        }
        Ok(())
    }

    /// Write out the pending run (if any) and clear it.
    fn flush(&mut self, writer: &mut dyn std::io::Write) -> std::io::Result<()> {
        let Some(state) = self.current.take() else {
            return Ok(());
        };
        let e = &state.first;
        let count = if state.count > 1 {
            format!(" (×{})", state.count)
        } else {
            String::new()
        };
        let line = format!(
            "{:>10} {:>10} {:>10} {:>10} {:>10} {} {} {} {} {}{}",
            fmt_duration_precise(Duration::from_micros(e.start_offset)),
            fmt_opt(state.total),
            fmt_opt(state.off_path),
            fmt_opt(state.user),
            fmt_opt(state.potential),
            e.kind,
            state.names.join(" / "),
            e.category.unwrap_or_default(),
            e.identifier.unwrap_or_default(),
            e.execution_kind.unwrap_or_default(),
            count,
        );

        // Color by potential, falling back to total where potential wasn't
        // computed (the slowest path), so those rows still convey magnitude.
        let color_key = state.potential.or(state.total).unwrap_or_default();
        writeln!(writer, "{}", row_style(color_key).apply_to(line))
    }
}

async fn log_critical_path(
    path: &Vec<buck2_data::CriticalPathEntry2>,
    format: LogCommandOutputFormat,
    threshold: Duration,
) -> buck2_error::Result<()> {
    let target_display_options = TargetDisplayOptions::for_log();

    // Aggregate stats for the `readable` footer, so hiding entries stays honest.
    let mut hidden_count: u64 = 0;
    let mut hidden_total = Duration::ZERO;
    // Collapses consecutive equivalent entries in the `readable` output.
    let mut run = Run::default();

    buck2_client_ctx::stdio::print_with_writer::<buck2_error::Error, _>(async move |w| {
        let mut log_writer = transform_format(format, w);
        if let LogCommandOutputFormatWithWriter::Readable(writer) = &mut log_writer {
            // Legend: the raw column names are terse and one of them ("off_path")
            // is easy to misread, so spell out what each duration means once.
            writeln!(
                writer,
                "  start     = offset from the start of the build\n  \
                 total     = wall-clock time for this node\n  \
                 off_path  = time that overlapped other work (did not extend the build)\n  \
                 user      = the portion of the runtime you can actually optimize\n  \
                 potential = build time you'd save if this node were instant\n"
            )?;
            #[allow(clippy::write_literal)] // easier to match the format below
            writeln!(
                writer,
                "{:>10} {:>10} {:>10} {:>10} {:>10} {} {} {} {} {}",
                "start",
                "total",
                "off_path",
                "user",
                "potential",
                "kind",
                "name",
                "category",
                "identifier",
                "execution_kind",
            )?;
        }

        for entry in path {
            let entry_display =
                match CriticalPathEntryDisplay::from_entry(entry, target_display_options)? {
                    Some(display) => display,
                    None => continue,
                };

            let critical_path = CriticalPathEntry {
                kind: entry_display.kind,
                name: entry_display.name,
                category: entry_display.category,
                identifier: entry_display.identifier,
                execution_kind: entry_display.execution_kind,
                total_duration: OptionalDuration::new(entry.total_duration)?,
                user_duration: OptionalDuration::new(entry.user_duration)?,
                potential_improvement_duration: OptionalDuration::new(
                    entry.potential_improvement_duration,
                )?,
                non_critical_path_time: OptionalDuration::new(entry.non_critical_path_duration)?,
                start_offset: entry.start_offset_ns.map_or(0, |v| v / 1000),
            };

            let res: Result<(), ClientIoError> = {
                match &mut log_writer {
                    LogCommandOutputFormatWithWriter::Readable(writer) => {
                        // Hide entries below the threshold -- typically waiting
                        // slivers and zero-cost structural nodes, which are noise
                        // rather than anything actionable. We track them for the
                        // footer. Hidden entries don't flush the current run, so
                        // identical entries separated only by noise still collapse
                        // together. With the default threshold of 0 nothing is
                        // hidden (impact is never negative).
                        let impact = critical_path.impact();
                        if impact < threshold {
                            hidden_count += 1;
                            hidden_total += impact;
                            continue;
                        }

                        run.push(writer, critical_path)?;
                    }
                    LogCommandOutputFormatWithWriter::Tabulated(writer) => {
                        // This should match the format specified in the docstrings on CriticalPathCommand and SlowestPathCommand
                        writeln!(
                            writer,
                            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                            critical_path.kind,
                            critical_path.name,
                            critical_path.category.unwrap_or_default(),
                            critical_path.identifier.unwrap_or_default(),
                            critical_path.execution_kind.unwrap_or_default(),
                            critical_path.total_duration,
                            critical_path.user_duration,
                            critical_path.potential_improvement_duration,
                            critical_path.non_critical_path_time,
                            critical_path.start_offset
                        )?;
                    }
                    LogCommandOutputFormatWithWriter::Json(writer) => {
                        serde_json::to_writer(writer.by_ref(), &critical_path)?;
                        writer.write_all("\n".as_bytes())?;
                    }
                    LogCommandOutputFormatWithWriter::Csv(writer) => {
                        writer
                            .serialize(critical_path)
                            .map_err(|e| from_any_with_tag(e, buck2_error::ErrorTag::LogCmd))?;
                    }
                }
                Ok(())
            };
            res?
        }

        if let LogCommandOutputFormatWithWriter::Readable(writer) = &mut log_writer {
            run.flush(writer)?;
            if hidden_count > 0 {
                writeln!(
                    writer,
                    "\n(hid {} low-impact entries under {}, totaling {})",
                    hidden_count,
                    fmt_duration_precise(threshold),
                    fmt_duration_precise(hidden_total),
                )?;
            }
        }

        Ok(())
    })
    .await
}
