/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! Minimal, hand-written prost structs for the subset of the perfetto trace
//! proto that we need to emit a native trace. Field numbers and enum values are
//! copied verbatim from the perfetto protos at
//! `/Users/jade/co/perfetto/protos/perfetto/trace/`:
//!   - `trace.proto`, `trace_packet.proto`
//!   - `track_event/track_event.proto`
//!   - `track_event/track_descriptor.proto`
//!   - `track_event/counter_descriptor.proto`
//!   - `track_event/debug_annotation.proto`
//!   - `../common/builtin_clock.proto`
//!
//! Only wire-compatibility matters. Unused fields are omitted.

use prost::Message;

/// builtin_clock.proto: BUILTIN_CLOCK_REALTIME = 1
pub const BUILTIN_CLOCK_REALTIME: u32 = 1;
/// builtin_clock.proto: BUILTIN_CLOCK_BOOTTIME = 6 (the default trace-time clock)
pub const BUILTIN_CLOCK_BOOTTIME: u32 = 6;

/// trace_packet.proto: SequenceFlags::SEQ_INCREMENTAL_STATE_CLEARED = 1
pub const SEQ_INCREMENTAL_STATE_CLEARED: u32 = 1;

/// track_event.proto: enum Type
pub mod track_event_type {
    pub const TYPE_SLICE_BEGIN: i32 = 1;
    pub const TYPE_SLICE_END: i32 = 2;
    pub const TYPE_INSTANT: i32 = 3;
    pub const TYPE_COUNTER: i32 = 4;
}

/// track_descriptor.proto: SiblingMergeBehavior
pub mod sibling_merge_behavior {
    pub const SIBLING_MERGE_BEHAVIOR_BY_SIBLING_MERGE_KEY: i32 = 3;
}

/// counter_descriptor.proto: Unit
pub mod counter_unit {
    #[allow(dead_code)]
    pub const UNIT_TIME_NS: i32 = 1;
    #[allow(dead_code)]
    pub const UNIT_COUNT: i32 = 2;
    pub const UNIT_SIZE_BYTES: i32 = 3;
}

/// trace.proto: `Trace { repeated TracePacket packet = 1; }`.
/// A concatenation of length-delimited `TracePacket`s with field tag 1 is a
/// valid `Trace`; we exploit that when streaming to disk (see writer.rs).
/// The full `Trace` message. Only used when re-parsing our own output for
/// verification; when writing we stream `TracePacketWrapper`s instead.
#[allow(dead_code)]
#[derive(Clone, PartialEq, Message)]
pub struct Trace {
    #[prost(message, repeated, tag = "1")]
    pub packet: Vec<TracePacket>,
}

/// A single wrapping of one packet, used purely to get prost to emit the
/// field-1 tag + length prefix for us.
#[derive(Clone, PartialEq, Message)]
pub struct TracePacketWrapper {
    #[prost(message, optional, tag = "1")]
    pub packet: Option<TracePacket>,
}

#[derive(Clone, PartialEq, Message)]
pub struct TracePacket {
    #[prost(uint64, optional, tag = "8")]
    pub timestamp: Option<u64>,
    #[prost(uint32, optional, tag = "58")]
    pub timestamp_clock_id: Option<u32>,
    #[prost(uint32, optional, tag = "10")]
    pub trusted_packet_sequence_id: Option<u32>,
    #[prost(uint32, optional, tag = "13")]
    pub sequence_flags: Option<u32>,

    #[prost(message, optional, tag = "11")]
    pub track_event: Option<TrackEvent>,
    #[prost(message, optional, tag = "60")]
    pub track_descriptor: Option<TrackDescriptor>,
    #[prost(message, optional, tag = "6")]
    pub clock_snapshot: Option<ClockSnapshot>,
}

/// clock_snapshot.proto: needed so trace_processor can resolve our REALTIME
/// timestamps to trace time. Without a ClockSnapshot declaring the clock, all
/// timestamped packets on a non-default builtin clock are dropped.
#[derive(Clone, PartialEq, Message)]
pub struct ClockSnapshot {
    #[prost(message, repeated, tag = "1")]
    pub clocks: Vec<Clock>,
}

#[derive(Clone, PartialEq, Message)]
pub struct Clock {
    #[prost(uint32, optional, tag = "1")]
    pub clock_id: Option<u32>,
    #[prost(uint64, optional, tag = "2")]
    pub timestamp: Option<u64>,
}

#[derive(Clone, PartialEq, Message)]
pub struct TrackEvent {
    #[prost(int32, optional, tag = "9")]
    pub r#type: Option<i32>,
    #[prost(uint64, optional, tag = "11")]
    pub track_uuid: Option<u64>,
    #[prost(string, optional, tag = "23")]
    pub name: Option<String>,
    #[prost(string, repeated, tag = "22")]
    pub categories: Vec<String>,
    #[prost(int64, optional, tag = "30")]
    pub counter_value: Option<i64>,
    #[prost(double, optional, tag = "44")]
    pub double_counter_value: Option<f64>,
    /// track_event.proto: `correlation_id` (field 52, part of the
    /// `correlation_id_field` oneof). UIs use this to visually link slices that
    /// belong to the same logical operation.
    #[prost(uint64, optional, tag = "52")]
    pub correlation_id: Option<u64>,
    #[prost(message, repeated, tag = "4")]
    pub debug_annotations: Vec<DebugAnnotation>,
}

#[derive(Clone, PartialEq, Message)]
pub struct TrackDescriptor {
    #[prost(uint64, optional, tag = "1")]
    pub uuid: Option<u64>,
    #[prost(uint64, optional, tag = "5")]
    pub parent_uuid: Option<u64>,
    #[prost(string, optional, tag = "2")]
    pub name: Option<String>,
    #[prost(message, optional, tag = "8")]
    pub counter: Option<CounterDescriptor>,
    #[prost(int32, optional, tag = "15")]
    pub sibling_merge_behavior: Option<i32>,
    #[prost(string, optional, tag = "16")]
    pub sibling_merge_key: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
pub struct CounterDescriptor {
    #[prost(int32, optional, tag = "3")]
    pub unit: Option<i32>,
    #[prost(string, optional, tag = "6")]
    pub unit_name: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
pub struct DebugAnnotation {
    #[prost(string, optional, tag = "10")]
    pub name: Option<String>,
    #[prost(uint64, optional, tag = "3")]
    pub uint_value: Option<u64>,
    #[prost(int64, optional, tag = "4")]
    pub int_value: Option<i64>,
    #[prost(double, optional, tag = "5")]
    pub double_value: Option<f64>,
    #[prost(string, optional, tag = "6")]
    pub string_value: Option<String>,
}

impl TracePacket {
    /// Encode this packet as bytes that, when appended to a file, extend a valid
    /// `Trace` message (i.e. `field 1 (len-delimited) = <this packet>`).
    pub fn encode_as_trace_field(&self) -> Vec<u8> {
        let wrapper = TracePacketWrapper {
            packet: Some(self.clone()),
        };
        wrapper.encode_to_vec()
    }
}

impl DebugAnnotation {
    pub fn string(name: &str, value: impl Into<String>) -> Self {
        DebugAnnotation {
            name: Some(name.to_owned()),
            string_value: Some(value.into()),
            ..Default::default()
        }
    }

    pub fn uint(name: &str, value: u64) -> Self {
        DebugAnnotation {
            name: Some(name.to_owned()),
            uint_value: Some(value),
            ..Default::default()
        }
    }
}
