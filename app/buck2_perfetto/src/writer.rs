/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::io::Write;
use std::time::Duration;
use std::time::SystemTime;

use buck2_common::convert::ProstDurationExt;
use buck2_error::internal_error;
use buck2_event_observer::display;
use buck2_event_observer::display::CriticalPathEntryDisplay;
use buck2_event_observer::display::TargetDisplayOptions;
use buck2_events::BuckEvent;

use crate::perfetto_proto::BUILTIN_CLOCK_BOOTTIME;
use crate::perfetto_proto::BUILTIN_CLOCK_REALTIME;
use crate::perfetto_proto::Clock;
use crate::perfetto_proto::ClockSnapshot;
use crate::perfetto_proto::CounterDescriptor;
use crate::perfetto_proto::DebugAnnotation;
use crate::perfetto_proto::SEQ_INCREMENTAL_STATE_CLEARED;
use crate::perfetto_proto::TrackDescriptor;
use crate::perfetto_proto::TrackEvent;
use crate::perfetto_proto::TracePacket;
use crate::perfetto_proto::counter_unit;
use crate::perfetto_proto::sibling_merge_behavior;
use crate::perfetto_proto::track_event_type;

const BYTES_PER_GIGABYTE: f64 = 1_000_000_000.0;
const SEQUENCE_ID: u32 = 1;

// Group-track uuids come from a small reserved range. Span ids are random-ish
// u64s so the collision risk with these tiny constants is negligible for a
// prototype. Bumping them into a high, unlikely-to-collide band anyway.
const GROUP_TRACK_BASE: u64 = 0xFFFF_FFFF_0000_0000;
const TRACK_ACTIONS: u64 = GROUP_TRACK_BASE + 1;
const TRACK_ANALYSIS: u64 = GROUP_TRACK_BASE + 2;
const TRACK_LOADS: u64 = GROUP_TRACK_BASE + 3;
const TRACK_MISC: u64 = GROUP_TRACK_BASE + 4;
const TRACK_CRITICAL_PATH: u64 = GROUP_TRACK_BASE + 5;
/// Group track for the open-span population counters (Change 2).
const TRACK_SPAN_COUNTERS: u64 = GROUP_TRACK_BASE + 6;
/// Group track for per-action "waiting" (queued) slices (Change 3).
const TRACK_QUEUED: u64 = GROUP_TRACK_BASE + 7;
const COUNTER_TRACK_BASE: u64 = GROUP_TRACK_BASE + 0x1000;

/// Salt for deriving a per-action "queued" track uuid from the action root span
/// id: `action_span_id ^ QUEUED_TRACK_SALT`. Span ids are random-ish u64s and
/// the action's own "actions" track uses `action_span_id` verbatim, so we need
/// the queued track to occupy a disjoint value. XOR with a large odd constant
/// gives a bijection on u64 (so two distinct action ids never collide), and the
/// high bit pattern keeps derived uuids clear of the low reserved group-track
/// band (GROUP_TRACK_BASE .. COUNTER_TRACK_BASE): xoring flips the top 32 bits
/// to 0x5A5A_A5A5, pushing derived uuids far below 0xFFFF_FFFF_.... A collision
/// with a raw span id would require that span id to equal
/// `other_action_span_id ^ salt`, which is no more likely than any other span
/// id collision the prototype already tolerates.
const QUEUED_TRACK_SALT: u64 = 0x5A5A_A5A5_C3C3_3C3D;

#[derive(Copy, Clone, PartialEq, Eq)]
enum Category {
    Actions,
    Analysis,
    Loads,
    Misc,
}

impl Category {
    fn group_uuid(self) -> u64 {
        match self {
            Category::Actions => TRACK_ACTIONS,
            Category::Analysis => TRACK_ANALYSIS,
            Category::Loads => TRACK_LOADS,
            Category::Misc => TRACK_MISC,
        }
    }

    fn key(self) -> &'static str {
        match self {
            Category::Actions => "actions",
            Category::Analysis => "analysis",
            Category::Loads => "loads",
            Category::Misc => "misc",
        }
    }
}

struct OpenSpan {
    start: SystemTime,
    track_uuid: u64,
    /// Category is only meaningful for root spans (those that own their track).
    category: Category,
    /// True if this span allocated its own track (is a root), false if it
    /// inherited a parent's track.
    is_root: bool,
    name: String,
    annotations: Vec<DebugAnnotation>,
    /// Extra role of this span used to implement action flattening.
    role: SpanRole,
}

/// How a span participates in the action-flattening logic (Change 1).
enum SpanRole {
    /// Not an action or executor stage; rendered as a plain slice.
    Plain,
    /// An ActionExecution root span. We defer deciding whether to emit its own
    /// action slice until SpanEnd: only if no executor stage child was seen.
    Action {
        /// Whether any ExecutorStage child span has been observed. When true,
        /// the stage slices stand in for the action slice, so the action's own
        /// slice is suppressed.
        saw_stage: bool,
    },
    /// An ExecutorStage span whose parent is an ActionExecution. It is promoted
    /// to a top-level slice, either on the action's "actions" track (work
    /// stages) or on a per-action lane in the "queued" group (waiting stages).
    Stage {
        /// The action root span id. Used as the correlation id (linking queued
        /// and work slices across groups) and as the seed for both the action's
        /// "actions" track uuid and its derived "queued" track uuid.
        action_span_id: u64,
        /// The action identity string (slice name for execution + queued
        /// stages).
        action_identity: String,
        /// True if this stage represents actual execution work (local/worker
        /// Execute or RE execute), which is named with the action identity.
        is_execution: bool,
        /// True if this stage represents time the action spent WAITING (queued,
        /// worker queued/wait, acquiring a local resource, RE queue). Waiting
        /// stages are promoted to the "queued" group instead of "actions".
        is_waiting: bool,
    },
}

pub struct Stats {
    pub packets: u64,
    pub spans_seen: u64,
    pub spans_emitted: u64,
    pub spans_dropped: u64,
    pub counters_emitted: u64,
    pub instants_emitted: u64,
    pub critical_path_slices: u64,
    pub tracks: u64,
}

impl fmt::Display for Stats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "packets={} tracks={} spans_seen={} spans_emitted={} spans_dropped={} counters={} instants={} critical_path_slices={}",
            self.packets,
            self.tracks,
            self.spans_seen,
            self.spans_emitted,
            self.spans_dropped,
            self.counters_emitted,
            self.instants_emitted,
            self.critical_path_slices,
        )
    }
}

pub struct PerfettoWriter<W: Write> {
    out: W,
    min_duration: Duration,
    first_packet: bool,

    open_spans: HashMap<u64, OpenSpan>,
    /// Track uuids whose TrackDescriptor has already been emitted.
    emitted_tracks: HashSet<u64>,
    /// Counter name -> track uuid.
    counter_tracks: HashMap<String, u64>,
    next_counter_uuid: u64,

    /// Current integer value of each open-span population counter (Change 2).
    span_counter_values: HashMap<String, i64>,
    /// span id -> the population-counter name it bumped at SpanStart, so we can
    /// decrement the right counter at SpanEnd.
    span_counter_open: HashMap<u64, String>,

    /// Rate-of-change bookkeeping: key -> (timestamp, amount).
    prev_rate: HashMap<String, (SystemTime, u64)>,

    /// Timestamp of the Command SpanStart, needed for critical-path offsets.
    command_start: Option<SystemTime>,

    stats: Stats,
}

impl<W: Write> PerfettoWriter<W> {
    pub fn new(out: W, min_duration: Duration) -> Self {
        Self {
            out,
            min_duration,
            first_packet: true,
            open_spans: HashMap::new(),
            emitted_tracks: HashSet::new(),
            counter_tracks: HashMap::new(),
            next_counter_uuid: COUNTER_TRACK_BASE,
            span_counter_values: HashMap::new(),
            span_counter_open: HashMap::new(),
            prev_rate: HashMap::new(),
            command_start: None,
            stats: Stats {
                packets: 0,
                spans_seen: 0,
                spans_emitted: 0,
                spans_dropped: 0,
                counters_emitted: 0,
                instants_emitted: 0,
                critical_path_slices: 0,
                tracks: 0,
            },
        }
    }

    fn unix_nanos(ts: SystemTime) -> u64 {
        ts.duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0)
    }

    fn write_packet(&mut self, mut packet: TracePacket) -> buck2_error::Result<()> {
        if self.first_packet {
            self.first_packet = false;
            // The very first packet carries SEQ_INCREMENTAL_STATE_CLEARED and a
            // ClockSnapshot declaring our REALTIME clock. Without the snapshot,
            // trace_processor drops every timestamped packet on this clock.
            let now_nanos = Self::unix_nanos(SystemTime::now());
            let snap = TracePacket {
                trusted_packet_sequence_id: Some(SEQUENCE_ID),
                sequence_flags: Some(SEQ_INCREMENTAL_STATE_CLEARED),
                clock_snapshot: Some(ClockSnapshot {
                    // Map REALTIME to the default trace-time clock (BOOTTIME) at
                    // the same instant, giving trace_processor a conversion path.
                    // Our timestamps are unix-realtime nanos; we simply declare
                    // the two clocks equal.
                    clocks: vec![
                        Clock {
                            clock_id: Some(BUILTIN_CLOCK_BOOTTIME),
                            timestamp: Some(now_nanos),
                        },
                        Clock {
                            clock_id: Some(BUILTIN_CLOCK_REALTIME),
                            timestamp: Some(now_nanos),
                        },
                    ],
                }),
                ..Default::default()
            };
            let bytes = snap.encode_as_trace_field();
            self.out.write_all(&bytes)?;
            self.stats.packets += 1;
        }
        packet.trusted_packet_sequence_id = Some(SEQUENCE_ID);
        let bytes = packet.encode_as_trace_field();
        self.out.write_all(&bytes)?;
        self.stats.packets += 1;
        Ok(())
    }

    fn timed_track_event_packet(&mut self, ts: SystemTime, ev: TrackEvent) -> TracePacket {
        TracePacket {
            timestamp: Some(Self::unix_nanos(ts)),
            timestamp_clock_id: Some(BUILTIN_CLOCK_REALTIME),
            track_event: Some(ev),
            ..Default::default()
        }
    }

    /// Emit a group TrackDescriptor once (the fixed-uuid top-level lane roots).
    fn ensure_group_track(&mut self, category: Category) -> buck2_error::Result<()> {
        let uuid = category.group_uuid();
        if self.emitted_tracks.insert(uuid) {
            self.stats.tracks += 1;
            let td = TrackDescriptor {
                uuid: Some(uuid),
                name: Some(category.key().to_owned()),
                ..Default::default()
            };
            self.write_packet(TracePacket {
                track_descriptor: Some(td),
                ..Default::default()
            })?;
        }
        Ok(())
    }

    /// Ensure a root span's per-span TrackDescriptor is emitted. Nested under the
    /// category group track; `sibling_merge_key` = category so the UI auto-packs
    /// concurrent root spans into lanes.
    fn ensure_span_track(
        &mut self,
        uuid: u64,
        category: Category,
        name: &str,
    ) -> buck2_error::Result<()> {
        if self.emitted_tracks.contains(&uuid) {
            return Ok(());
        }
        self.ensure_group_track(category)?;
        self.emitted_tracks.insert(uuid);
        self.stats.tracks += 1;
        let td = TrackDescriptor {
            uuid: Some(uuid),
            parent_uuid: Some(category.group_uuid()),
            name: Some(name.to_owned()),
            sibling_merge_behavior: Some(
                sibling_merge_behavior::SIBLING_MERGE_BEHAVIOR_BY_SIBLING_MERGE_KEY,
            ),
            sibling_merge_key: Some(category.key().to_owned()),
            ..Default::default()
        };
        self.write_packet(TracePacket {
            track_descriptor: Some(td),
            ..Default::default()
        })
    }

    /// Ensure the top-level "queued" group track (parent for per-action waiting
    /// lanes). Emitted once, lazily, the first time a waiting slice needs it.
    fn ensure_queued_group(&mut self) -> buck2_error::Result<()> {
        if self.emitted_tracks.insert(TRACK_QUEUED) {
            self.stats.tracks += 1;
            let td = TrackDescriptor {
                uuid: Some(TRACK_QUEUED),
                name: Some("queued".to_owned()),
                ..Default::default()
            };
            self.write_packet(TracePacket {
                track_descriptor: Some(td),
                ..Default::default()
            })?;
        }
        Ok(())
    }

    /// Ensure a per-action waiting lane under the "queued" group. All queued
    /// lanes share `sibling_merge_key = "queued"` so the UI packs the whole
    /// waiting population into one merged set of lanes. Named with the action
    /// identity. Idempotent (keyed on `emitted_tracks`).
    fn ensure_queued_track(&mut self, uuid: u64, name: &str) -> buck2_error::Result<()> {
        if self.emitted_tracks.contains(&uuid) {
            return Ok(());
        }
        self.ensure_queued_group()?;
        self.emitted_tracks.insert(uuid);
        self.stats.tracks += 1;
        let td = TrackDescriptor {
            uuid: Some(uuid),
            parent_uuid: Some(TRACK_QUEUED),
            name: Some(name.to_owned()),
            sibling_merge_behavior: Some(
                sibling_merge_behavior::SIBLING_MERGE_BEHAVIOR_BY_SIBLING_MERGE_KEY,
            ),
            sibling_merge_key: Some("queued".to_owned()),
            ..Default::default()
        };
        self.write_packet(TracePacket {
            track_descriptor: Some(td),
            ..Default::default()
        })
    }

    pub fn handle_event(&mut self, event: &BuckEvent) -> buck2_error::Result<()> {
        match event.data() {
            buck2_data::buck_event::Data::SpanStart(buck2_data::SpanStartEvent {
                data: Some(start),
            }) => self.handle_span_start(event, start),
            buck2_data::buck_event::Data::SpanStart(buck2_data::SpanStartEvent { data: None }) => {
                Ok(())
            }
            buck2_data::buck_event::Data::SpanEnd(end) => self.handle_span_end(event, end),
            buck2_data::buck_event::Data::Instant(buck2_data::InstantEvent {
                data: Some(instant),
            }) => self.handle_instant(event, instant),
            buck2_data::buck_event::Data::Instant(buck2_data::InstantEvent { data: None }) => Ok(()),
            buck2_data::buck_event::Data::Record(_) => Ok(()),
        }
    }

    fn handle_span_start(
        &mut self,
        event: &BuckEvent,
        start: &buck2_data::span_start_event::Data,
    ) -> buck2_error::Result<()> {
        let span_id: u64 = match event.span_id() {
            Some(id) => id.into(),
            None => return Ok(()),
        };
        self.stats.spans_seen += 1;

        let (name, category) = span_name_and_category(start)?;

        if matches!(start, buck2_data::span_start_event::Data::Command(_)) {
            self.command_start = Some(event.timestamp());
        }

        // Change 2: bump the open-span population counters for span kinds the
        // chrome converter tracks (executor stages, analysis, load, re_upload).
        if let Some(counter_name) = span_population_counter_name(start) {
            self.bump_span_counter(event.timestamp(), counter_name, 1)?;
            self.span_counter_open
                .insert(span_id, counter_name.to_owned());
        }

        // Look at the parent span (if open) to decide track inheritance and, for
        // executor stages, whether this stage should be promoted to a top-level
        // slice on the parent action's track (Change 1).
        let parent_id = event.parent_id().map(u64::from);
        let parent = parent_id.and_then(|pid| self.open_spans.get(&pid));
        let parent_uuid = parent.map(|p| p.track_uuid);

        // Determine the span's flattening role.
        let role = match start {
            buck2_data::span_start_event::Data::ActionExecution(_) => {
                SpanRole::Action { saw_stage: false }
            }
            buck2_data::span_start_event::Data::ExecutorStage(stage) => {
                // Promote only when the direct parent is an ActionExecution.
                match parent {
                    Some(OpenSpan {
                        role: SpanRole::Action { .. },
                        ..
                    }) => {
                        let parent_id = parent_id.unwrap();
                        // Fetch the action identity from the parent's name (which
                        // was set to display_action_identity at its start).
                        let action_identity = self.open_spans[&parent_id].name.clone();
                        let is_execution = stage
                            .stage
                            .as_ref()
                            .map(stage_is_execution)
                            .unwrap_or(false);
                        let is_waiting = stage
                            .stage
                            .as_ref()
                            .map(stage_is_waiting)
                            .unwrap_or(false);
                        // Mark the parent action as having seen a stage child so
                        // its own slice is suppressed at SpanEnd.
                        if let Some(OpenSpan {
                            role: SpanRole::Action { saw_stage },
                            ..
                        }) = self.open_spans.get_mut(&parent_id)
                        {
                            *saw_stage = true;
                        }
                        SpanRole::Stage {
                            action_span_id: parent_id,
                            action_identity,
                            is_execution,
                            is_waiting,
                        }
                    }
                    _ => SpanRole::Plain,
                }
            }
            _ => SpanRole::Plain,
        };

        // Track inheritance: for a promoted stage, use the parent action's track
        // but render as a top-level (not nested) slice, so it is NOT a "root"
        // (which would try to allocate its own track) yet also not nested under
        // the action slice (which we suppress). Other child spans inherit the
        // parent's track and nest as before.
        let (track_uuid, is_root) = match (&role, parent_uuid) {
            // Waiting stages go on a per-action lane in the "queued" group; work
            // stages go on the action's "actions" track (uuid = action span id).
            (
                SpanRole::Stage {
                    action_span_id,
                    is_waiting: true,
                    ..
                },
                _,
            ) => (queued_track_uuid(*action_span_id), false),
            (SpanRole::Stage { action_span_id, .. }, _) => (*action_span_id, false),
            (_, Some(uuid)) => (uuid, false),
            (_, None) => (span_id, true),
        };

        let annotations = vec![DebugAnnotation::uint("span_id", span_id)];

        self.open_spans.insert(
            span_id,
            OpenSpan {
                start: event.timestamp(),
                track_uuid,
                category,
                is_root,
                name,
                annotations,
                role,
            },
        );
        Ok(())
    }

    fn handle_span_end(
        &mut self,
        event: &BuckEvent,
        end: &buck2_data::SpanEndEvent,
    ) -> buck2_error::Result<()> {
        let span_id: u64 = match event.span_id() {
            Some(id) => id.into(),
            None => return Ok(()),
        };

        // Failed materialization -> instant on the misc track.
        if let Some(buck2_data::span_end_event::Data::Materialization(m)) = end.data.as_ref()
            && !m.success
        {
            self.emit_instant(
                event.timestamp(),
                "materialization_failure",
                vec![
                    DebugAnnotation::string("path", m.path.clone()),
                    DebugAnnotation::uint("file_count", m.file_count),
                    DebugAnnotation::uint("total_bytes", m.total_bytes),
                    DebugAnnotation::string("error", m.error.clone().unwrap_or_default()),
                ],
            )?;
        }

        // Change 2: decrement the open-span population counter, if this span
        // bumped one. Done regardless of the min-duration slice cutoff.
        if let Some(counter_name) = self.span_counter_open.remove(&span_id) {
            self.bump_span_counter(event.timestamp(), &counter_name, -1)?;
        }

        let open = match self.open_spans.remove(&span_id) {
            Some(o) => o,
            None => return Ok(()),
        };

        let duration = end
            .duration
            .as_ref()
            .ok_or_else(|| internal_error!("SpanEnd missing duration"))?
            .try_into_duration()?;

        // Compute the slice name, correlation id and any extra annotations based
        // on the span's flattening role (Change 1).
        let (slice_name, correlation_id, extra_annotations) = match &open.role {
            SpanRole::Action { saw_stage: true } => {
                // The action had executor-stage children: its stage slices stand
                // in for the action slice, so we suppress the action's own slice.
                // (The counter decrement above still ran.)
                return Ok(());
            }
            SpanRole::Action { saw_stage: false } => {
                // Cache hits and other stage-less actions still render normally.
                (open.name.clone(), None, Vec::new())
            }
            SpanRole::Stage {
                action_span_id,
                action_identity,
                is_execution,
                is_waiting,
            } => {
                // Naming:
                //  - Waiting stages live in the "queued" group where the state is
                //    implied, so we name them by action identity (per-action
                //    colors + readable labels), appending the stage kind when it
                //    isn't plain local_queued, e.g. `<id> (worker_wait)`.
                //  - Execution stages take the action identity so the slice color
                //    becomes per-action.
                //  - Other work stages keep their uniform stage name.
                let name = if *is_waiting {
                    match open.name.as_str() {
                        "local_queued" => action_identity.clone(),
                        kind => format!("{action_identity} ({kind})"),
                    }
                } else if *is_execution {
                    action_identity.clone()
                } else {
                    open.name.clone()
                };
                // Every stage slice carries the action identity + root span id in
                // its annotations, and correlation_id = action root span id.
                let extra = vec![
                    DebugAnnotation::string("action", action_identity.clone()),
                    DebugAnnotation::uint("action_span_id", *action_span_id),
                ];
                (name, Some(*action_span_id), extra)
            }
            SpanRole::Plain => (open.name.clone(), None, Vec::new()),
        };

        if duration < self.min_duration {
            self.stats.spans_dropped += 1;
            return Ok(());
        }
        self.stats.spans_emitted += 1;

        // Emit the track descriptor lazily, right before the first slice that
        // *references* the track uuid. Both a root span's own slice and a
        // promoted stage slice may be the first to reference the action's
        // track: the action's own slice is suppressed once it has stage
        // children, so the stage path must be able to create the track itself.
        // `ensure_span_track` is idempotent (keyed on `emitted_tracks`), so
        // whichever slice arrives first wins and the rest are no-ops.
        match &open.role {
            SpanRole::Stage {
                action_identity,
                is_waiting: true,
                ..
            } => {
                // Waiting stage: ensure the per-action lane in the "queued"
                // group. `open.track_uuid` is the derived queued-track uuid. This
                // is independent of the action's "actions" track: an action whose
                // only stages are waiting will only ever ensure this track (and
                // correctly show nothing under "actions").
                self.ensure_queued_track(open.track_uuid, action_identity)?;
            }
            SpanRole::Stage {
                action_identity, ..
            } => {
                // Work stage: the track uuid is the action root span id; name it
                // with the action identity so the lane reads as the action, and
                // file it under the "actions" group like any other action root.
                self.ensure_span_track(open.track_uuid, Category::Actions, action_identity)?;
            }
            _ if open.is_root => {
                self.ensure_span_track(open.track_uuid, open.category, &open.name)?;
            }
            _ => {}
        }

        let end_ts = open.start + duration;

        let mut annotations = open.annotations.clone();
        annotations.extend(extra_annotations);

        // SLICE_BEGIN at the buffered start.
        let begin = self.timed_track_event_packet(
            open.start,
            TrackEvent {
                r#type: Some(track_event_type::TYPE_SLICE_BEGIN),
                track_uuid: Some(open.track_uuid),
                name: Some(slice_name),
                correlation_id,
                debug_annotations: annotations,
                ..Default::default()
            },
        );
        self.write_packet(begin)?;

        // SLICE_END.
        let slice_end = self.timed_track_event_packet(
            end_ts,
            TrackEvent {
                r#type: Some(track_event_type::TYPE_SLICE_END),
                track_uuid: Some(open.track_uuid),
                correlation_id,
                ..Default::default()
            },
        );
        self.write_packet(slice_end)?;
        Ok(())
    }

    fn handle_instant(
        &mut self,
        event: &BuckEvent,
        instant: &buck2_data::instant_event::Data,
    ) -> buck2_error::Result<()> {
        match instant {
            buck2_data::instant_event::Data::Snapshot(snapshot) => {
                self.handle_snapshot(event.timestamp(), snapshot)
            }
            buck2_data::instant_event::Data::CommandPreempted(_) => {
                self.emit_instant(event.timestamp(), "command_preempted", vec![])
            }
            buck2_data::instant_event::Data::BuildGraphInfo(info) => {
                self.write_critical_path(&info.critical_path2)
            }
            _ => Ok(()),
        }
    }

    fn handle_snapshot(
        &mut self,
        ts: SystemTime,
        snapshot: &buck2_data::Snapshot,
    ) -> buck2_error::Result<()> {
        self.emit_double_counter(
            ts,
            "max_rss_gigabyte",
            counter_unit::UNIT_SIZE_BYTES,
            snapshot.buck2_max_rss as f64 / BYTES_PER_GIGABYTE,
        )?;
        if let Some(malloc) = snapshot.malloc_bytes_active {
            self.emit_double_counter(
                ts,
                "malloc_active_gigabyte",
                counter_unit::UNIT_SIZE_BYTES,
                malloc as f64 / BYTES_PER_GIGABYTE,
            )?;
        }
        self.emit_int_counter(
            ts,
            "deferred_materializer_queue_size",
            snapshot.deferred_materializer_queue_size as i64,
        )?;
        self.emit_int_counter(
            ts,
            "blocking_executor_io_queue_size",
            snapshot.blocking_executor_io_queue_size as i64,
        )?;
        self.emit_int_counter(
            ts,
            "tokio_blocking_queue_depth",
            snapshot.tokio_blocking_queue_depth as i64,
        )?;

        self.emit_rate_counter(ts, "user_cpu_us_per_s", snapshot.buck2_user_cpu_us)?;
        self.emit_rate_counter(ts, "system_cpu_us_per_s", snapshot.buck2_system_cpu_us)?;
        self.emit_rate_counter(ts, "re_upload_bytes_per_s", snapshot.re_upload_bytes)?;
        self.emit_rate_counter(ts, "re_download_bytes_per_s", snapshot.re_download_bytes)?;
        self.emit_rate_counter(ts, "http_download_bytes_per_s", snapshot.http_download_bytes)?;
        for (nic, stats) in &snapshot.network_interface_stats {
            self.emit_rate_counter(ts, &format!("{nic}_send_bytes_per_s"), stats.tx_bytes)?;
            self.emit_rate_counter(ts, &format!("{nic}_receive_bytes_per_s"), stats.rx_bytes)?;
        }
        Ok(())
    }

    fn counter_track(&mut self, name: &str, unit: i32) -> buck2_error::Result<u64> {
        self.counter_track_parented(name, unit, None)
    }

    /// Lazily create a counter track, optionally parented under a group track.
    fn counter_track_parented(
        &mut self,
        name: &str,
        unit: i32,
        parent_uuid: Option<u64>,
    ) -> buck2_error::Result<u64> {
        if let Some(uuid) = self.counter_tracks.get(name) {
            return Ok(*uuid);
        }
        let uuid = self.next_counter_uuid;
        self.next_counter_uuid += 1;
        self.counter_tracks.insert(name.to_owned(), uuid);
        self.stats.tracks += 1;
        let td = TrackDescriptor {
            uuid: Some(uuid),
            parent_uuid,
            name: Some(name.to_owned()),
            counter: Some(CounterDescriptor {
                unit: Some(unit),
                ..Default::default()
            }),
            ..Default::default()
        };
        self.write_packet(TracePacket {
            track_descriptor: Some(td),
            ..Default::default()
        })?;
        Ok(uuid)
    }

    /// Ensure the "open spans" group track (parent for population counters).
    fn ensure_span_counter_group(&mut self) -> buck2_error::Result<()> {
        if self.emitted_tracks.insert(TRACK_SPAN_COUNTERS) {
            self.stats.tracks += 1;
            let td = TrackDescriptor {
                uuid: Some(TRACK_SPAN_COUNTERS),
                name: Some("open spans".to_owned()),
                ..Default::default()
            };
            self.write_packet(TracePacket {
                track_descriptor: Some(td),
                ..Default::default()
            })?;
        }
        Ok(())
    }

    /// Change 2: adjust an open-span population counter by `delta` and emit an
    /// int TYPE_COUNTER event immediately (no bucketing). Counter tracks are
    /// lazily created under the "open spans" group track. The tracked counter
    /// name is prefixed with "spans: " so grouping is legible even if a UI/
    /// trace_processor flattens the parenting.
    fn bump_span_counter(
        &mut self,
        ts: SystemTime,
        name: &str,
        delta: i64,
    ) -> buck2_error::Result<()> {
        self.ensure_span_counter_group()?;
        let track_name = format!("spans: {name}");
        let uuid = self.counter_track_parented(
            &track_name,
            counter_unit::UNIT_COUNT,
            Some(TRACK_SPAN_COUNTERS),
        )?;
        let value = {
            let slot = self.span_counter_values.entry(name.to_owned()).or_insert(0);
            *slot += delta;
            *slot
        };
        let ev = TrackEvent {
            r#type: Some(track_event_type::TYPE_COUNTER),
            track_uuid: Some(uuid),
            counter_value: Some(value),
            ..Default::default()
        };
        let pkt = self.timed_track_event_packet(ts, ev);
        self.write_packet(pkt)?;
        self.stats.counters_emitted += 1;
        Ok(())
    }

    fn emit_int_counter(
        &mut self,
        ts: SystemTime,
        name: &str,
        value: i64,
    ) -> buck2_error::Result<()> {
        let uuid = self.counter_track(name, counter_unit::UNIT_COUNT)?;
        let ev = TrackEvent {
            r#type: Some(track_event_type::TYPE_COUNTER),
            track_uuid: Some(uuid),
            counter_value: Some(value),
            ..Default::default()
        };
        let pkt = self.timed_track_event_packet(ts, ev);
        self.write_packet(pkt)?;
        self.stats.counters_emitted += 1;
        Ok(())
    }

    fn emit_double_counter(
        &mut self,
        ts: SystemTime,
        name: &str,
        unit: i32,
        value: f64,
    ) -> buck2_error::Result<()> {
        let uuid = self.counter_track(name, unit)?;
        let ev = TrackEvent {
            r#type: Some(track_event_type::TYPE_COUNTER),
            track_uuid: Some(uuid),
            double_counter_value: Some(value),
            ..Default::default()
        };
        let pkt = self.timed_track_event_packet(ts, ev);
        self.write_packet(pkt)?;
        self.stats.counters_emitted += 1;
        Ok(())
    }

    /// Average rate of change per second, matching chrome_trace.rs.
    fn emit_rate_counter(
        &mut self,
        ts: SystemTime,
        name: &str,
        amount: u64,
    ) -> buck2_error::Result<()> {
        if let Some((prev_ts, prev_amount)) = self.prev_rate.get(name).copied() {
            let secs = ts
                .duration_since(prev_ts)
                .map(|d| d.as_secs_f64())
                .unwrap_or(0.0);
            if secs > 0.0 {
                let delta = amount.saturating_sub(prev_amount) as f64;
                self.emit_double_counter(ts, name, counter_unit::UNIT_COUNT, delta / secs)?;
            }
        }
        self.prev_rate.insert(name.to_owned(), (ts, amount));
        Ok(())
    }

    fn emit_instant(
        &mut self,
        ts: SystemTime,
        name: &str,
        annotations: Vec<DebugAnnotation>,
    ) -> buck2_error::Result<()> {
        self.ensure_group_track(Category::Misc)?;
        let ev = TrackEvent {
            r#type: Some(track_event_type::TYPE_INSTANT),
            track_uuid: Some(TRACK_MISC),
            name: Some(name.to_owned()),
            debug_annotations: annotations,
            ..Default::default()
        };
        let pkt = self.timed_track_event_packet(ts, ev);
        self.write_packet(pkt)?;
        self.stats.instants_emitted += 1;
        Ok(())
    }

    /// Flat single-track critical path (skips the hierarchical waiting-group
    /// logic in chrome_trace.rs). Slices land on a dedicated "critical path"
    /// track, timestamps computed relative to the recorded command_start.
    fn write_critical_path(
        &mut self,
        critical_path: &[buck2_data::CriticalPathEntry2],
    ) -> buck2_error::Result<()> {
        let command_start = match self.command_start {
            Some(cs) => cs,
            None => return Ok(()),
        };
        let opts = TargetDisplayOptions::for_chrome_trace();

        // Dedicated track, its own descriptor.
        let uuid = TRACK_CRITICAL_PATH;
        if self.emitted_tracks.insert(uuid) {
            self.stats.tracks += 1;
            let td = TrackDescriptor {
                uuid: Some(uuid),
                name: Some("critical path".to_owned()),
                ..Default::default()
            };
            self.write_packet(TracePacket {
                track_descriptor: Some(td),
                ..Default::default()
            })?;
        }

        for entry in critical_path {
            let start_offset_ns = entry.start_offset_ns.unwrap_or(0);
            let critical = entry
                .total_duration
                .as_ref()
                .and_then(|d| d.try_into_duration().ok())
                .unwrap_or(Duration::ZERO);
            let non_critical = entry
                .non_critical_path_duration
                .as_ref()
                .and_then(|d| d.try_into_duration().ok())
                .unwrap_or(Duration::ZERO);
            let total = critical + non_critical;
            if total < Duration::from_millis(1) {
                continue;
            }

            let display = match CriticalPathEntryDisplay::from_entry(entry, opts)? {
                Some(d) => d,
                None => continue,
            };
            let name = display.display_name();
            let start = command_start + Duration::from_nanos(start_offset_ns);
            let end = start + total;

            let begin = self.timed_track_event_packet(
                start,
                TrackEvent {
                    r#type: Some(track_event_type::TYPE_SLICE_BEGIN),
                    track_uuid: Some(uuid),
                    name: Some(name),
                    ..Default::default()
                },
            );
            self.write_packet(begin)?;
            let slice_end = self.timed_track_event_packet(
                end,
                TrackEvent {
                    r#type: Some(track_event_type::TYPE_SLICE_END),
                    track_uuid: Some(uuid),
                    ..Default::default()
                },
            );
            self.write_packet(slice_end)?;
            self.stats.critical_path_slices += 1;
        }
        Ok(())
    }

    pub fn finish(mut self) -> buck2_error::Result<Stats> {
        self.out.flush()?;
        Ok(self.stats)
    }
}

/// Port of the span naming from chrome_trace.rs `handle_event`, but without the
/// Omit/ShowIfParent filtering: every span gets a name + category.
fn span_name_and_category(
    start: &buck2_data::span_start_event::Data,
) -> buck2_error::Result<(String, Category)> {
    use buck2_data::span_start_event::Data;

    let opts = TargetDisplayOptions::for_chrome_trace();
    Ok(match start {
        Data::Command(_) => ("command".to_owned(), Category::Misc),
        Data::Analysis(analysis) => {
            let name = format!(
                "analysis {}",
                display::display_analysis_target(
                    analysis
                        .target
                        .as_ref()
                        .ok_or_else(|| internal_error!("AnalysisStart missing target"))?,
                    opts,
                )?
            );
            (name, Category::Analysis)
        }
        Data::Load(eval) => (format!("load {}", eval.module_id), Category::Loads),
        Data::LoadPackage(lp) => (format!("listing {}", lp.path), Category::Loads),
        Data::ActionExecution(action) => {
            let name = display::display_action_identity(
                action.key.as_ref(),
                action.name.as_ref(),
                opts,
            )?;
            (name, Category::Actions)
        }
        Data::ExecutorStage(stage) => {
            let name = stage
                .stage
                .as_ref()
                .and_then(display::display_executor_stage)
                .unwrap_or("executor_stage");
            (name.to_owned(), Category::Actions)
        }
        Data::FinalMaterialization(_) => ("materialization".to_owned(), Category::Actions),
        Data::FileWatcher(_) => ("file_watcher_sync".to_owned(), Category::Misc),
        Data::ReUpload(_) => ("re_upload".to_owned(), Category::Actions),
        // Generic fallback for variants chrome_trace.rs ignores. Debug repr of
        // the oneof gives a readable variant name for the prototype.
        other => (generic_span_name(other), Category::Misc),
    })
}

/// Derive the per-action "queued" track uuid from the action root span id.
/// See [`QUEUED_TRACK_SALT`] for the collision argument.
fn queued_track_uuid(action_span_id: u64) -> u64 {
    action_span_id ^ QUEUED_TRACK_SALT
}

/// True if this executor stage represents actual execution work (as opposed to
/// queueing, materialization, resource acquisition, download/upload, etc.):
/// local/worker Execute in `local_stage`, or RE Execute in `re_stage`.
fn stage_is_execution(stage: &buck2_data::executor_stage_start::Stage) -> bool {
    use buck2_data::executor_stage_start::Stage;
    match stage {
        Stage::Local(local) => {
            use buck2_data::local_stage::Stage as LocalStage;
            matches!(
                local.stage,
                Some(LocalStage::Execute(_)) | Some(LocalStage::WorkerExecute(_))
            )
        }
        Stage::Re(re) => {
            use buck2_data::re_stage::Stage as ReStage;
            matches!(re.stage, Some(ReStage::Execute(_)))
        }
        _ => false,
    }
}

/// True if this executor stage represents time the action spent WAITING rather
/// than doing work: local `Queued`, `WorkerQueued`, `WorkerWait`,
/// `AcquireLocalResource`, and any RE `queue*` stage. These slices are promoted
/// to the "queued" group instead of the "actions" group.
///
/// Judgment calls (see report): `WorkerInit` is treated as WORK (it spins up a
/// worker), matching the prompt's work-stage list, even though chrome_trace.rs
/// lumps it with non-execution stages. RE `QueueCancelled` (`re_cancelled`) is
/// treated as waiting: it is a terminal queue-family stage and never ran.
fn stage_is_waiting(stage: &buck2_data::executor_stage_start::Stage) -> bool {
    use buck2_data::executor_stage_start::Stage;
    match stage {
        Stage::Local(local) => {
            use buck2_data::local_stage::Stage as LocalStage;
            matches!(
                local.stage,
                Some(LocalStage::Queued(_))
                    | Some(LocalStage::WorkerQueued(_))
                    | Some(LocalStage::WorkerWait(_))
                    | Some(LocalStage::AcquireLocalResource(_))
            )
        }
        Stage::Re(re) => {
            use buck2_data::re_stage::Stage as ReStage;
            matches!(
                re.stage,
                Some(ReStage::Queue(_))
                    | Some(ReStage::QueueOverQuota(_))
                    | Some(ReStage::QueueAcquiringDependencies(_))
                    | Some(ReStage::QueueNoWorkerAvailable(_))
                    | Some(ReStage::QueueCancelled(_))
            )
        }
        _ => false,
    }
}

/// The name of the open-span population counter this span kind contributes to,
/// mirroring the `bump_counter_while_span` call sites in chrome_trace.rs:
/// executor stages (by `display_executor_stage` name), analysis, load, and
/// re_upload. Returns None for span kinds that are not counted.
fn span_population_counter_name(
    start: &buck2_data::span_start_event::Data,
) -> Option<&'static str> {
    use buck2_data::span_start_event::Data;
    match start {
        Data::Analysis(_) => Some("analysis"),
        Data::Load(_) => Some("load"),
        Data::ReUpload(_) => Some("re_upload"),
        Data::ExecutorStage(stage) => stage.stage.as_ref().and_then(display::display_executor_stage),
        _ => None,
    }
}

fn generic_span_name(data: &buck2_data::span_start_event::Data) -> String {
    // Debug prints as `VariantName(..)`; take the leading identifier.
    let dbg = format!("{data:?}");
    let name: String = dbg
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect();
    if name.is_empty() {
        "<unknown>".to_owned()
    } else {
        name
    }
}
