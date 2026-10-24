/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use std::fmt::Write;
use std::time::Duration;

use buck2_event_observer::display;
use buck2_event_observer::display::TargetDisplayOptions;
use buck2_event_observer::fmt_duration;
use buck2_event_observer::span_tracker::BuckEventSpanInfo;
use derive_more::From;
use superconsole::Component;
use superconsole::Dimensions;
use superconsole::DrawMode;
use superconsole::Line;
use superconsole::Lines;
use superconsole::Span;
use superconsole::style::StyledContent;
use superconsole::style::Stylize;
use superconsole::style::style;

use crate::subscribers::superconsole::timed_list::Cutoffs;
use crate::subscribers::superconsole::timekeeper::Timekeeper;

const ACTION_DETAIL_MIN_WIDTH: usize = 9;
const ACTION_KIND_WIDTH: usize = 16;

#[derive(Debug, Clone, From)]
pub(crate) enum Row {
    Timed(TimedRow),
    Summary(SummaryRow),
    Line(Line),
}

#[derive(Debug)]
pub(crate) struct Table {
    pub(crate) rows: Vec<Row>,
}

impl Table {
    pub(crate) fn new() -> Self {
        Self { rows: Vec::new() }
    }

    pub(crate) fn len(&self) -> usize {
        self.rows.len()
    }
}

impl Component for Table {
    type Error = buck2_error::Error;

    /// Zips together each time and label lines, but gives the times preferential treatment.
    fn draw_unchecked(
        &self,
        Dimensions { width, .. }: Dimensions,
        _mode: DrawMode,
    ) -> buck2_error::Result<Lines> {
        let combined = self
            .rows
            .iter()
            .cloned()
            .map(|row| match row {
                Row::Timed(row) => {
                    let TimedRow {
                        mut primary,
                        detail,
                        time,
                        aux,
                    } = row;
                    let sep = 1;
                    let available_before_time = width.saturating_sub(sep + time.len());
                    if primary.len() > available_before_time {
                        truncate_line_with_ellipsis(&mut primary, available_before_time);
                    }

                    let mut combined = primary;
                    if let Some(mut detail) = detail {
                        let detail_width = available_before_time.saturating_sub(combined.len());
                        if detail_width > 0 {
                            truncate_line_with_ellipsis(&mut detail, detail_width);
                            combined.extend(detail);
                        }
                    }

                    if let Some(mut aux) = aux {
                        let aux_width = available_before_time.saturating_sub(combined.len() + sep);
                        if aux_width > 0 {
                            truncate_line_with_ellipsis(&mut aux, aux_width);
                            combined.push(Span::padding(sep));
                            combined.extend(aux);
                        }
                    }

                    if combined.len() < available_before_time {
                        combined.push(Span::padding(available_before_time - combined.len()));
                    }
                    combined.push(Span::padding(sep));
                    combined.extend(time);
                    truncate_line_with_ellipsis(&mut combined, width);
                    combined
                }
                Row::Summary(row) => row.line(),
                Row::Line(line) => line,
            })
            .collect();

        Ok(combined)
    }
}

fn truncate_line_with_ellipsis(line: &mut Line, width: usize) {
    if line.len() <= width {
        return;
    }

    let Some(style) = line.iter().last().map(|span| span.style) else {
        return;
    };
    let ellipsis = ".".repeat(width.min(3));
    line.truncate_line(width.saturating_sub(ellipsis.len()));
    if !ellipsis.is_empty() {
        line.push(Span::new_styled_lossy(StyledContent::new(style, ellipsis)));
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SummaryRow {
    pub(crate) visible_roots: usize,
    pub(crate) total_roots: usize,
}

impl SummaryRow {
    fn line(&self) -> Line {
        let summary = format!(
            "{}/{} running tasks shown",
            self.visible_roots, self.total_roots
        );
        Line::from_iter([Span::new_styled_lossy(summary.italic())])
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TimedRow {
    primary: Line,
    detail: Option<Line>,
    time: Line,
    aux: Option<Line>,
}

impl TimedRow {
    pub(crate) fn span(
        padding: usize,
        span: &BuckEventSpanInfo,
        timekeeper: &Timekeeper,
        cutoffs: &Cutoffs,
        display_platform: bool,
    ) -> buck2_error::Result<Self> {
        let event = display::display_event(
            &span.event,
            TargetDisplayOptions::for_console(display_platform),
        )?;
        let elapsed = timekeeper.duration_since(span.start);
        let time = fmt_duration::fmt_duration(elapsed);
        // Escalate the row color on time in the current stage, and only while
        // actively executing -- total elapsed is mostly queueing under contention.
        let style_age = if display::is_active_execution_stage(&span.event) {
            elapsed
        } else {
            Duration::ZERO
        };
        Self::new(
            padding,
            event.label,
            event.detail,
            event.category,
            None,
            time,
            style_age,
            cutoffs,
        )
    }

    pub(crate) fn new(
        padding: usize,
        label: Option<String>,
        detail: String,
        category: Option<String>,
        aux: Option<Aux>,
        time: String,
        age: Duration,
        cutoffs: &Cutoffs,
    ) -> buck2_error::Result<Self> {
        let mut primary = Vec::new();
        let has_aux = aux.is_some();
        if padding > 0 {
            primary.push(Span::padding(padding));
        }

        let detail = match label {
            Some(label) => {
                primary.push(Span::new_styled(style(label))?);
                let age = if has_aux { Duration::ZERO } else { age };
                Some(
                    DetailCell {
                        text: detail,
                        category,
                        has_aux,
                        age,
                    }
                    .line(cutoffs)?,
                )
            }
            None => {
                primary.push(Span::new_styled(styled_for_delay(
                    style(detail),
                    age,
                    cutoffs,
                ))?);
                None
            }
        };

        let aux = match aux {
            Some(aux) => Some(AuxCell { aux, age }.line(cutoffs)?),
            None => None,
        };

        let primary = Line::from_iter(primary);
        let time = Line::from_iter([Span::new_styled(style(time))?]);
        Ok(Self {
            primary,
            detail,
            time,
            aux,
        })
    }
}

struct DetailCell {
    text: String,
    category: Option<String>,
    has_aux: bool,
    age: Duration,
}

impl DetailCell {
    fn line(self, cutoffs: &Cutoffs) -> buck2_error::Result<Line> {
        let Self {
            text,
            category,
            has_aux,
            age,
        } = self;
        let mut spans = Vec::new();
        let Some(category) = category else {
            spans.push(Span::new_styled(style(" · ".to_owned()))?);
            spans.push(Span::new_styled(styled_for_delay(
                style(text),
                age,
                cutoffs,
            ))?);
            return Ok(Line::from_iter(spans));
        };

        let rest = text
            .strip_prefix(&category)
            .filter(|rest| rest.is_empty() || rest.starts_with(' '));
        let Some(rest) = rest else {
            spans.push(Span::new_styled(style(" · ".to_owned()))?);
            spans.push(Span::new_styled(styled_for_delay(
                style(text),
                age,
                cutoffs,
            ))?);
            return Ok(Line::from_iter(spans));
        };

        spans.push(Span::new_styled(style(" [".to_owned()))?);
        spans.push(Span::new_styled(styled_for_delay(
            style(category),
            age,
            cutoffs,
        ))?);
        if !rest.is_empty() {
            let rest = match rest.trim().parse::<u64>() {
                Ok(num) => format!(" {num:>2}"),
                Err(_) => rest.to_owned(),
            };
            spans.push(Span::new_styled(styled_for_delay(
                style(rest),
                age,
                cutoffs,
            ))?);
        }
        spans.push(Span::new_styled(style("]".to_owned()))?);

        let detail_width = spans.iter().map(|span| span.len()).sum::<usize>();
        if has_aux && detail_width < ACTION_DETAIL_MIN_WIDTH {
            spans.push(Span::padding(ACTION_DETAIL_MIN_WIDTH - detail_width));
        }

        Ok(Line::from_iter(spans))
    }
}

/// The first child shown beside a root row: `<name> [<duration>] [+ <remaining>]`.
pub(crate) struct Aux {
    pub(crate) name: String,
    pub(crate) duration: Option<String>,
    pub(crate) remaining_children: usize,
}

struct AuxCell {
    aux: Aux,
    age: Duration,
}

impl AuxCell {
    fn line(self, cutoffs: &Cutoffs) -> buck2_error::Result<Line> {
        let Self {
            aux:
                Aux {
                    name,
                    duration,
                    remaining_children,
                },
            age,
        } = self;
        // Pad the name to the stage column only when a duration follows, so
        // durations align; the name itself may contain spaces and is kept as is.
        let mut text = match duration {
            Some(duration) => {
                let width = ACTION_KIND_WIDTH.max(name.chars().count() + 1);
                format!("{name:<width$}{duration}")
            }
            None => name,
        };
        if remaining_children > 0 {
            write!(text, " + {remaining_children}").expect("Write to String is not fallible");
        }
        Ok(Line::from_iter([Span::new_styled(styled_for_delay(
            style(text),
            age,
            cutoffs,
        ))?]))
    }
}

/// This component echoes the `Lines` that have been stored in it.
#[derive(Debug)]
#[allow(dead_code)]
struct LinesComponent(Lines);

impl Component for LinesComponent {
    type Error = buck2_error::Error;

    fn draw_unchecked(
        &self,
        _dimensions: Dimensions,
        _mode: DrawMode,
    ) -> buck2_error::Result<Lines> {
        Ok(self.0.clone())
    }
}

/// Colorize based on time.
fn styled_for_delay(
    content: StyledContent<String>,
    elapsed: Duration,
    cutoffs: &Cutoffs,
) -> StyledContent<String> {
    if elapsed < cutoffs.inform {
        content
    } else if elapsed < cutoffs.warn {
        content.dark_yellow()
    } else {
        content.dark_red()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use superconsole::Component;

    use crate::subscribers::superconsole::timed_list::Cutoffs;
    use crate::subscribers::superconsole::timed_list::table_builder::Aux;
    use crate::subscribers::superconsole::timed_list::table_builder::AuxCell;
    use crate::subscribers::superconsole::timed_list::table_builder::Row;
    use crate::subscribers::superconsole::timed_list::table_builder::Table;
    use crate::subscribers::superconsole::timed_list::table_builder::TimedRow;

    fn aux(name: &str, duration: Option<&str>, remaining_children: usize) -> Aux {
        Aux {
            name: name.to_owned(),
            duration: duration.map(str::to_owned),
            remaining_children,
        }
    }

    fn aux_text(aux: Aux) -> buck2_error::Result<String> {
        let line = AuxCell {
            aux,
            age: Duration::ZERO,
        }
        .line(&CUTOFFS)?;
        Ok(line.fmt_for_test().to_string())
    }

    #[test]
    fn aux_aligns_stage_durations() -> buck2_error::Result<()> {
        assert_eq!(
            aux_text(aux("local_execute", Some("5.4s"), 0))?,
            "local_execute   5.4s"
        );
        assert_eq!(
            aux_text(aux("prepare", Some("5.0s"), 1))?,
            "prepare         5.0s + 1"
        );
        Ok(())
    }

    #[test]
    fn aux_keeps_space_after_long_stage_name() -> buck2_error::Result<()> {
        assert_eq!(
            aux_text(aux("re_worker_upload", Some("5.4s"), 0))?,
            "re_worker_upload 5.4s"
        );
        assert_eq!(
            aux_text(aux("local_prepare_outputs", Some("5.4s"), 0))?,
            "local_prepare_outputs 5.4s"
        );
        Ok(())
    }

    #[test]
    fn aux_does_not_pad_inside_multi_word_name() -> buck2_error::Result<()> {
        assert_eq!(
            aux_text(aux("Waiting for graph computations", None, 0))?,
            "Waiting for graph computations"
        );
        assert_eq!(
            aux_text(aux("Waiting for graph computations", None, 2))?,
            "Waiting for graph computations + 2"
        );
        assert_eq!(
            aux_text(aux("Waiting for dice", Some("1.2s"), 0))?,
            "Waiting for dice 1.2s"
        );
        assert_eq!(
            aux_text(aux("upload (action)", Some("1.2s"), 3))?,
            "upload (action) 1.2s + 3"
        );
        Ok(())
    }

    const CUTOFFS: Cutoffs = Cutoffs {
        inform: Duration::from_secs(1000),
        warn: Duration::from_secs(2000),
        _notable: Duration::ZERO,
    };

    #[test]
    fn timed_row_keeps_target_before_metadata() -> buck2_error::Result<()> {
        let label = "root//third-party/rust:tikv-jemalloc-sys-0.6-buildscript";
        let mut table = Table::new();
        table.rows.push(Row::Timed(TimedRow::new(
            0,
            None,
            label.to_owned(),
            None,
            Some(aux("local_execute", Some("10.4s"), 0)),
            "1:47.6s".to_owned(),
            Duration::ZERO,
            &CUTOFFS,
        )?));

        let output = table.draw(
            superconsole::Dimensions {
                width: 72,
                height: 1,
            },
            superconsole::DrawMode::Normal,
        )?;
        let output = output.fmt_for_test().to_string();
        let expected = concat!(
            "root//third-party/rust:tikv-jemalloc-sys-0.6-buildscript ",
            "loca... ",
            "1:47.6s\n",
        );

        pretty_assertions::assert_eq!(output, expected);

        Ok(())
    }

    #[test]
    fn timed_row_keeps_time_suffix_when_target_is_truncated() -> buck2_error::Result<()> {
        let mut table = Table::new();
        table.rows.push(Row::Timed(TimedRow::new(
            0,
            None,
            "root//third-party/rust:tikv-jemalloc-sys-0.6-buildscript".to_owned(),
            None,
            Some(aux("local_execute", Some("10.4s"), 0)),
            "1:47.6s".to_owned(),
            Duration::ZERO,
            &CUTOFFS,
        )?));

        let output = table.draw(
            superconsole::Dimensions {
                width: 36,
                height: 1,
            },
            superconsole::DrawMode::Normal,
        )?;
        let output = output.fmt_for_test().to_string();
        let expected = concat!("root//third-party/rust:ti... ", "1:47.6s\n",);

        pretty_assertions::assert_eq!(output, expected);

        Ok(())
    }

    #[test]
    fn timed_row_truncates_aux_before_event_or_time() -> buck2_error::Result<()> {
        let label = "root//third-party/rust:short-target";
        let mut table = Table::new();
        table.rows.push(Row::Timed(TimedRow::new(
            0,
            None,
            label.to_owned(),
            None,
            Some(aux("local_execute", Some("10.4s"), 0)),
            "1:47.6s".to_owned(),
            Duration::ZERO,
            &CUTOFFS,
        )?));

        let output = table.draw(
            superconsole::Dimensions {
                width: 56,
                height: 1,
            },
            superconsole::DrawMode::Normal,
        )?;
        let output = output.fmt_for_test().to_string();
        let expected = concat!(
            "root//third-party/rust:short-target ",
            "local_exe... ",
            "1:47.6s\n",
        );

        pretty_assertions::assert_eq!(output, expected);

        Ok(())
    }

    #[test]
    fn timed_row_delineates_action_category() -> buck2_error::Result<()> {
        let mut table = Table::new();
        table.rows.push(Row::Timed(TimedRow::new(
            0,
            Some("root//third-party/rust:zerocopy".to_owned()),
            "download_file crate".to_owned(),
            Some("download_file".to_owned()),
            None,
            "12.0s".to_owned(),
            Duration::ZERO,
            &CUTOFFS,
        )?));

        let output = table.draw(
            superconsole::Dimensions {
                width: 80,
                height: 1,
            },
            superconsole::DrawMode::Normal,
        )?;
        let output = output.fmt_for_test().to_string();
        let expected = concat!(
            "root//third-party/rust:zerocopy",
            " [download_file crate]",
            "                      ",
            "12.0s\n",
        );

        pretty_assertions::assert_eq!(output, expected);

        Ok(())
    }

    #[test]
    fn timed_row_colors_child_action_not_parent_target() -> buck2_error::Result<()> {
        let mut table = Table::new();
        table.rows.push(Row::Timed(TimedRow::new(
            0,
            Some("root//third-party/rust:serde_bytes-0.11".to_owned()),
            "deps 0".to_owned(),
            Some("deps".to_owned()),
            Some(aux("local_execute", Some("0.1s"), 0)),
            "12.6s".to_owned(),
            Duration::from_secs(3000),
            &CUTOFFS,
        )?));

        let output = table.draw(
            superconsole::Dimensions {
                width: 120,
                height: 1,
            },
            superconsole::DrawMode::Normal,
        )?;
        let output = output.fmt_for_test().to_string();
        let expected = concat!(
            "root//third-party/rust:serde_bytes-0.11",
            " [deps  0] ",
            "<span fg=dark_red>local_execute   0.1s</span>",
            "                                             ",
            "12.6s\n",
        );

        pretty_assertions::assert_eq!(output, expected);

        Ok(())
    }
}
