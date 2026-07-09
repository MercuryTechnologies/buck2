/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! Prototype: convert a buck2 event log into a *native* perfetto protobuf trace
//! (a `Trace` = stream of `TracePacket`s), loadable in ui.perfetto.dev.
//!
//! Unlike the chrome-trace JSON converter (`buck2_cmd_debug_client::chrome_trace`),
//! this is a single streaming pass. Native perfetto packets need not be
//! timestamp-ordered (trace_processor sorts them), and `sibling_merge_key`
//! makes the UI auto-pack concurrent root spans into lanes, so we need neither
//! the two-pass filtering nor the hand-rolled `TrackIdAllocator`.

mod perfetto_proto;
mod writer;

use std::io::BufWriter;
use std::time::Duration;

use buck2_error::BuckErrorContext;
use buck2_event_log::read::EventLogPathBuf;
use buck2_event_log::stream_value::StreamValue;
use buck2_events::BuckEvent;
use buck2_fs::paths::abs_path::AbsPathBuf;
use clap::Parser;
use futures::TryStreamExt;

use crate::writer::PerfettoWriter;

#[derive(Parser, Debug)]
#[command(about = "Convert a buck2 event log to a native perfetto trace")]
struct Args {
    /// Path to the buck2 event log (zstd-compressed protobuf).
    #[arg(long)]
    log: String,

    /// Path to write the perfetto Trace protobuf to.
    #[arg(long)]
    out: String,

    /// Minimum span duration to emit, in milliseconds (default 1ms).
    #[arg(long, default_value_t = 1.0)]
    min_duration: f64,

    /// Emit all spans regardless of duration.
    #[arg(long)]
    full: bool,
}

#[tokio::main]
async fn main() -> buck2_error::Result<()> {
    let args = Args::parse();

    let min_duration = if args.full {
        Duration::ZERO
    } else {
        Duration::from_secs_f64(args.min_duration / 1000.0)
    };

    let log_path = AbsPathBuf::new(std::path::absolute(&args.log)?)?;
    let log = EventLogPathBuf::infer(log_path)?;
    let (_invocation, stream_values) = log.unpack_stream().await?;
    let stream = stream_values.try_filter_map(|sv| async move {
        match sv {
            StreamValue::Event(e) => Ok(Some(BuckEvent::try_from(e)?)),
            _ => Ok(None),
        }
    });
    let mut stream = std::pin::pin!(stream);

    let out_file = std::fs::File::create(&args.out)?;
    let mut writer = PerfettoWriter::new(BufWriter::new(out_file), min_duration);

    while let Some(event) = stream.try_next().await? {
        writer
            .handle_event(&event)
            .buck_error_context("handling event")?;
    }

    let stats = writer.finish()?;

    let out_len = std::fs::metadata(&args.out)?.len();
    eprintln!("wrote {} ({} bytes)", args.out, out_len);
    eprintln!("{stats}");

    Ok(())
}
