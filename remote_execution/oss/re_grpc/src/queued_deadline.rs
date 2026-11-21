/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under both the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree and the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree.
 */

//! An action whose operation stays QUEUED past a deadline is executed again.
//!
//! BuildBuddy sends one QUEUED Operation on the Execute stream and then nothing until an executor
//! claims the task, and HTTP/2 pings keep the stream open, so a task the scheduler lost and a long
//! queue look the same: only a clock ends the wait. Past the deadline the action's Execute is sent
//! again, as a second attempt beside the first, and the stream that is still open keeps being
//! read. Whichever attempt an executor claims first, or that finishes first with an OK response,
//! is the one the action takes; the others are dropped. An attempt that finishes with an error
//! while another is still waiting is dropped too, because the other may still succeed.

use std::sync::Arc;
use std::time::Duration;

use futures::stream::AbortHandle;
use futures::stream::BoxStream;
use futures::stream::SelectAll;
use futures::stream::StreamExt;

use crate::error::TCode;
use crate::response::ExecuteWithProgressResponse;
use crate::response::Stage;

/// Where an attempt's operation is, from the progress it has reported.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct AttemptState {
    /// It has said QUEUED.
    queued: bool,
    /// An executor has it: EXECUTING or COMPLETED, or a CACHE_CHECK after QUEUED, which is what
    /// BuildBuddy's executor publishes when it claims a task, before EXECUTING.
    claimed: bool,
}

fn is_done(response: &ExecuteWithProgressResponse) -> bool {
    response.execute_response.is_some()
}

impl AttemptState {
    fn observe(&mut self, response: &ExecuteWithProgressResponse) {
        match response.stage {
            Stage::QUEUED => self.queued = true,
            Stage::EXECUTING | Stage::COMPLETED => self.claimed = true,
            Stage::CACHE_CHECK if self.queued => self.claimed = true,
            _ => {}
        }
        if is_done(response) {
            self.claimed = true;
        }
    }
}

/// Whether a final response carries an OK status.
fn finished_ok(response: &ExecuteWithProgressResponse) -> bool {
    response
        .execute_response
        .as_ref()
        .is_some_and(|response| response.status.code == TCode::OK)
}

/// The deadline for the `requeue`th re-Execute: `period` doubled `requeue` times, plus up to a
/// quarter more at random, so the actions a build enqueued together do not send their Executes in
/// one burst to a scheduler that may be the part in trouble.
fn deadline_after(period: Duration, requeue: usize, jitter: f64) -> Duration {
    let base = period.saturating_mul(1u32 << requeue.min(16));
    let extra_ms = (base.as_millis() as f64 * jitter.clamp(0.0, 1.0) * 0.25) as u64;
    base.saturating_add(Duration::from_millis(extra_ms))
}

/// Runs `open`'s attempts at an action under a QUEUED deadline. `open` starts one attempt: an
/// Execute, with its own resumption. `period` is the deadline (None turns it off); `max_requeues`
/// bounds the re-Executes, after which buck2 warns and keeps waiting, since a long legitimate
/// queue looks the same as a lost task. `warn` puts a re-Execute on the console.
pub(crate) fn with_queued_deadline<F, W>(
    open: F,
    period: Option<Duration>,
    max_requeues: usize,
    warn: W,
) -> BoxStream<'static, anyhow::Result<ExecuteWithProgressResponse>>
where
    F: Fn() -> BoxStream<'static, anyhow::Result<ExecuteWithProgressResponse>>
        + Send
        + Sync
        + 'static,
    W: Fn(String) + Send + Sync + 'static,
{
    let Some(period) = period.filter(|period| !period.is_zero()) else {
        return open();
    };

    struct State<F, W> {
        open: Arc<F>,
        warn: Arc<W>,
        attempts:
            SelectAll<BoxStream<'static, (usize, anyhow::Result<ExecuteWithProgressResponse>)>>,
        handles: Vec<AbortHandle>,
        states: Vec<AttemptState>,
        live: Vec<bool>,
        winner: Option<usize>,
        timer: Option<std::pin::Pin<Box<tokio::time::Sleep>>>,
        requeues: usize,
        max_requeues: usize,
        period: Duration,
    }

    impl<F, W> State<F, W>
    where
        F: Fn() -> BoxStream<'static, anyhow::Result<ExecuteWithProgressResponse>>
            + Send
            + Sync
            + 'static,
    {
        fn start(&mut self) {
            let id = self.handles.len();
            let (stream, handle) = futures::stream::abortable((self.open)());
            self.attempts
                .push(stream.map(move |item| (id, item)).boxed());
            self.handles.push(handle);
            self.states.push(AttemptState::default());
            self.live.push(true);
        }

        fn live_count(&self) -> usize {
            self.live.iter().filter(|live| **live).count()
        }

        fn drop_attempt(&mut self, id: usize) {
            self.live[id] = false;
            self.handles[id].abort();
        }

        fn take(&mut self, id: usize) {
            self.winner = Some(id);
            self.timer = None;
            for other in 0..self.handles.len() {
                if other != id && self.live[other] {
                    self.drop_attempt(other);
                }
            }
        }
    }

    let mut state = State {
        open: Arc::new(open),
        warn: Arc::new(warn),
        attempts: SelectAll::new(),
        handles: Vec::new(),
        states: Vec::new(),
        live: Vec::new(),
        winner: None,
        timer: None,
        requeues: 0,
        max_requeues,
        period,
    };
    state.start();

    futures::stream::unfold(state, |mut state| async move {
        loop {
            let timer = async {
                match state.timer.as_mut() {
                    Some(timer) => timer.await,
                    None => futures::future::pending().await,
                }
            };
            tokio::select! {
                item = state.attempts.next() => {
                    let (id, item) = item?;
                    if !state.live[id] {
                        continue;
                    }
                    match item {
                        Err(err) => {
                            if state.winner == Some(id) || state.live_count() == 1 {
                                return Some((Err(err), state));
                            }
                            state.drop_attempt(id);
                        }
                        Ok(operation) => {
                            state.states[id].observe(&operation);
                            if state.winner.is_none() {
                                if is_done(&operation)
                                    && !finished_ok(&operation)
                                    && state.live_count() > 1
                                {
                                    state.drop_attempt(id);
                                    continue;
                                }
                                if state.states[id].claimed {
                                    state.take(id);
                                } else if state.states[id].queued && state.timer.is_none() {
                                    let deadline =
                                        deadline_after(state.period, state.requeues, rand::random());
                                    state.timer = Some(Box::pin(tokio::time::sleep(deadline)));
                                }
                            }
                            return Some((Ok(operation), state));
                        }
                    }
                }
                _ = timer => {
                    let waited = deadline_after(state.period, state.requeues, 0.0);
                    if state.requeues < state.max_requeues {
                        state.requeues += 1;
                        (state.warn)(format!(
                            "Executing RE action again (re-Execute {}/{}): its operation stayed QUEUED for at least {}s",
                            state.requeues,
                            state.max_requeues,
                            waited.as_secs(),
                        ));
                        state.start();
                        let deadline =
                            deadline_after(state.period, state.requeues, rand::random());
                        state.timer = Some(Box::pin(tokio::time::sleep(deadline)));
                    } else {
                        (state.warn)(format!(
                            "RE operation is still QUEUED after {} re-Executes; waiting for it",
                            state.requeues,
                        ));
                        state.timer = None;
                    }
                }
            }
        }
    })
    .boxed()
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use futures::TryStreamExt;

    use super::*;
    use crate::response::ExecuteResponse;

    fn staged(stage: Stage, done: bool) -> ExecuteWithProgressResponse {
        ExecuteWithProgressResponse {
            stage,
            execute_response: done.then(ExecuteResponse::default),
            metadata: Default::default(),
        }
    }

    /// Attempts that each send `script[n]` and then wait for ever.
    fn scripted(
        scripts: Vec<Vec<anyhow::Result<ExecuteWithProgressResponse>>>,
    ) -> (
        impl Fn() -> BoxStream<'static, anyhow::Result<ExecuteWithProgressResponse>>
        + Send
        + Sync
        + 'static,
        Arc<Mutex<usize>>,
    ) {
        let opened = Arc::new(Mutex::new(0));
        let scripts = Arc::new(Mutex::new(std::collections::VecDeque::from(scripts)));
        let counter = opened.clone();
        let open = move || {
            *counter.lock().unwrap() += 1;
            let script = scripts.lock().unwrap().pop_front().unwrap_or_default();
            futures::stream::iter(script)
                .chain(futures::stream::pending())
                .boxed()
        };
        (open, opened)
    }

    fn take_until_done(
        stream: BoxStream<'static, anyhow::Result<ExecuteWithProgressResponse>>,
    ) -> impl futures::Future<Output = anyhow::Result<Vec<ExecuteWithProgressResponse>>> {
        stream
            .try_take_while(|operation| futures::future::ready(Ok(!is_done(operation))))
            .try_collect()
    }

    #[tokio::test(start_paused = true)]
    async fn an_operation_queued_past_its_deadline_is_executed_again() -> anyhow::Result<()> {
        let (open, opened) = scripted(vec![
            vec![Ok(staged(Stage::QUEUED, false))],
            vec![
                Ok(staged(Stage::QUEUED, false)),
                Ok(staged(Stage::EXECUTING, false)),
                Ok(staged(Stage::COMPLETED, true)),
            ],
        ]);
        let warnings = Arc::new(Mutex::new(Vec::new()));
        let seen = warnings.clone();
        let stream = with_queued_deadline(open, Some(Duration::from_secs(900)), 5, move |w| {
            seen.lock().unwrap().push(w)
        });
        let mut stream = stream;
        let mut last = None;
        while let Some(operation) = stream.try_next().await? {
            let done = is_done(&operation);
            last = Some(operation);
            if done {
                break;
            }
        }
        assert!(is_done(&last.unwrap()));
        assert_eq!(*opened.lock().unwrap(), 2);
        let warnings = warnings.lock().unwrap();
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("stayed QUEUED"), "{warnings:?}");
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_claimed_operation_has_no_deadline() {
        let (open, opened) = scripted(vec![vec![
            Ok(staged(Stage::QUEUED, false)),
            Ok(staged(Stage::EXECUTING, false)),
        ]]);
        let stream = with_queued_deadline(open, Some(Duration::from_secs(900)), 5, |_| {});
        let result =
            tokio::time::timeout(Duration::from_secs(100_000), take_until_done(stream)).await;
        assert!(result.is_err(), "the stream should still be waiting");
        assert_eq!(*opened.lock().unwrap(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn the_first_attempt_wins_if_it_is_claimed_first() -> anyhow::Result<()> {
        let (open, opened) = scripted(vec![
            vec![Ok(staged(Stage::QUEUED, false))],
            vec![Ok(staged(Stage::QUEUED, false))],
        ]);
        // The first attempt is claimed once the second exists: script it through a channel.
        let (tx, rx) = futures::channel::mpsc::unbounded();
        let rx = Arc::new(Mutex::new(Some(rx)));
        let first = Arc::new(Mutex::new(true));
        let open = move || {
            if std::mem::replace(&mut *first.lock().unwrap(), false) {
                let rx = rx.lock().unwrap().take().unwrap();
                // Counts the call; this attempt is scripted through the channel instead.
                drop(open());
                futures::stream::iter(vec![Ok(staged(Stage::QUEUED, false))])
                    .chain(rx)
                    .boxed()
            } else {
                open()
            }
        };
        let mut stream = with_queued_deadline(open, Some(Duration::from_secs(900)), 5, |_| {});
        assert!(!is_done(&stream.try_next().await?.unwrap()));
        // Past the deadline, a second attempt starts and says QUEUED.
        assert!(!is_done(&stream.try_next().await?.unwrap()));
        assert_eq!(*opened.lock().unwrap(), 2);
        tx.unbounded_send(Ok(staged(Stage::EXECUTING, false)))
            .unwrap();
        tx.unbounded_send(Ok(staged(Stage::COMPLETED, true)))
            .unwrap();
        let mut last = None;
        while let Some(operation) = stream.try_next().await? {
            let done = is_done(&operation);
            last = Some(operation);
            if done {
                break;
            }
        }
        assert!(is_done(&last.unwrap()));
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn spent_re_executes_keep_waiting() {
        let (open, opened) = scripted(vec![
            vec![Ok(staged(Stage::QUEUED, false))],
            vec![Ok(staged(Stage::QUEUED, false))],
        ]);
        let warnings = Arc::new(Mutex::new(Vec::new()));
        let seen = warnings.clone();
        let stream = with_queued_deadline(open, Some(Duration::from_secs(900)), 1, move |w| {
            seen.lock().unwrap().push(w)
        });
        let result =
            tokio::time::timeout(Duration::from_secs(100_000), take_until_done(stream)).await;
        assert!(result.is_err(), "the stream should still be waiting");
        assert_eq!(*opened.lock().unwrap(), 2);
        let warnings = warnings.lock().unwrap();
        assert_eq!(warnings.len(), 2, "{warnings:?}");
        assert!(warnings[1].contains("still QUEUED"), "{warnings:?}");
    }

    #[test]
    fn the_deadline_doubles_and_jitters_by_at_most_a_quarter() {
        let period = Duration::from_secs(900);
        assert_eq!(deadline_after(period, 0, 0.0), period);
        assert_eq!(deadline_after(period, 1, 0.0), Duration::from_secs(1800));
        assert_eq!(deadline_after(period, 0, 1.0), Duration::from_secs(1125));
    }

    #[tokio::test]
    async fn no_deadline_is_the_plain_stream() {
        let (open, opened) = scripted(vec![vec![Ok(staged(Stage::COMPLETED, true))]]);
        let stream = with_queued_deadline(open, None, 5, |_| {});
        let operations: Vec<_> = stream.take(1).collect().await;
        assert!(is_done(operations[0].as_ref().unwrap()));
        assert_eq!(*opened.lock().unwrap(), 1);
    }
}
