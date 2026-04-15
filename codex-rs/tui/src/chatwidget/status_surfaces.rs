//! Status-line and terminal-title rendering helpers for `ChatWidget`.
//!
//! Keeping this logic in a focused submodule makes the additive title/status
//! behavior easier to review without paging through the rest of `chatwidget.rs`.

use super::*;
use crate::terminal_palette::best_color;
use ratatui::text::Span;

/// Items shown in the terminal title when the user has not configured a
/// custom selection. Intentionally minimal: spinner + project name.
pub(super) const DEFAULT_TERMINAL_TITLE_ITEMS: [&str; 2] = ["spinner", "project"];

/// Braille-pattern dot-spinner frames for the terminal title animation.
pub(super) const TERMINAL_TITLE_SPINNER_FRAMES: [&str; 10] =
    ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];

/// Time between spinner frame advances in the terminal title.
pub(super) const TERMINAL_TITLE_SPINNER_INTERVAL: Duration = Duration::from_millis(100);

/// Compact runtime states that can be rendered into the terminal title.
///
/// This is intentionally smaller than the full status-header vocabulary. The
/// title needs short, stable labels, so callers map richer lifecycle events
/// onto one of these buckets before rendering.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) enum TerminalTitleStatusKind {
    Working,
    WaitingForBackgroundTerminal,
    Undoing,
    #[default]
    Thinking,
}

#[derive(Debug)]
/// Parsed status-surface configuration for one refresh pass.
///
/// The status line and terminal title share some expensive or stateful inputs
/// (notably git branch lookup and invalid-item warnings). This snapshot lets one
/// refresh pass compute those shared concerns once, then render both surfaces
/// from the same selection set.
struct StatusSurfaceSelections {
    status_line_items: Vec<StatusLineItem>,
    invalid_status_line_items: Vec<String>,
    terminal_title_items: Vec<TerminalTitleItem>,
    invalid_terminal_title_items: Vec<String>,
}

impl StatusSurfaceSelections {
    fn uses_git_branch(&self) -> bool {
        self.status_line_items.contains(&StatusLineItem::GitBranch)
            || self
                .terminal_title_items
                .contains(&TerminalTitleItem::GitBranch)
    }
}

const HUD_CONTEXT_BAR_WIDTH: usize = 10;
const HUD_LIMIT_BAR_WIDTH: usize = 8;
const HUD_MODEL_MAX_CHARS: usize = 24;
const HUD_THREAD_MAX_CHARS: usize = 24;
const HUD_PROJECT_MAX_CHARS: usize = 18;
const HUD_BRANCH_MAX_CHARS: usize = 18;
const HUD_SKY: (u8, u8, u8) = (56, 189, 248);
const HUD_MINT: (u8, u8, u8) = (16, 240, 132);
const HUD_GOLD: (u8, u8, u8) = (254, 240, 60);
const HUD_VIOLET: (u8, u8, u8) = (197, 132, 255);
const HUD_AMBER: (u8, u8, u8) = (255, 171, 27);
const HUD_ROSE: (u8, u8, u8) = (255, 96, 128);
const HUD_CORAL: (u8, u8, u8) = (255, 140, 80);
const HUD_SLATE: (u8, u8, u8) = (148, 163, 184);
const HUD_MUTED: (u8, u8, u8) = (100, 116, 139);
const HUD_ACTIVE_INTERVAL: Duration = Duration::from_millis(140);
const HUD_IDLE_INTERVAL: Duration = Duration::from_millis(1_000);
const HUD_LIVE_SPINNER_FRAMES: [&str; 4] = ["/", "-", "\\", "|"];
const HUD_LIVE_PULSE_FRAMES: [&str; 4] = [".", "o", "O", "o"];

/// Resolve an RGB tuple to a terminal-appropriate `Color`.
///
/// On Windows Terminal, this promotes to TrueColor even when `COLORTERM` is not
/// set, because `supports-color` misreports the terminal's capabilities.
#[inline]
fn hud_color(rgb: (u8, u8, u8)) -> Color {
    best_color(rgb)
}

/// Cached project-root display name keyed by the cwd used for the last lookup.
///
/// Terminal-title refreshes can happen very frequently, so the title path avoids
/// repeatedly walking up the filesystem to rediscover the same project root name
/// while the working directory is unchanged.
#[derive(Clone, Debug)]
pub(super) struct CachedProjectRootName {
    pub(super) cwd: PathBuf,
    pub(super) root_name: Option<String>,
}

impl ChatWidget {
    fn status_surface_selections(&self) -> StatusSurfaceSelections {
        let (status_line_items, invalid_status_line_items) = self.status_line_items_with_invalids();
        let (terminal_title_items, invalid_terminal_title_items) =
            self.terminal_title_items_with_invalids();
        StatusSurfaceSelections {
            status_line_items,
            invalid_status_line_items,
            terminal_title_items,
            invalid_terminal_title_items,
        }
    }

    fn warn_invalid_status_line_items_once(&mut self, invalid_items: &[String]) {
        if self.thread_id.is_some()
            && !invalid_items.is_empty()
            && self
                .status_line_invalid_items_warned
                .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            let label = if invalid_items.len() == 1 {
                "item"
            } else {
                "items"
            };
            let message = format!(
                "Ignored invalid status line {label}: {}.",
                proper_join(invalid_items)
            );
            self.on_warning(message);
        }
    }

    fn warn_invalid_terminal_title_items_once(&mut self, invalid_items: &[String]) {
        if self.thread_id.is_some()
            && !invalid_items.is_empty()
            && self
                .terminal_title_invalid_items_warned
                .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            let label = if invalid_items.len() == 1 {
                "item"
            } else {
                "items"
            };
            let message = format!(
                "Ignored invalid terminal title {label}: {}.",
                proper_join(invalid_items)
            );
            self.on_warning(message);
        }
    }

    fn sync_status_surface_shared_state(&mut self, selections: &StatusSurfaceSelections) {
        if !self.status_surface_needs_git_branch(selections) {
            self.status_line_branch = None;
            self.status_line_branch_pending = false;
            self.status_line_branch_lookup_complete = false;
            return;
        }

        let cwd = self.status_line_cwd().to_path_buf();
        self.sync_status_line_branch_state(&cwd);
        if !self.status_line_branch_lookup_complete {
            self.request_status_line_branch(cwd);
        }
    }

    fn refresh_status_line_from_selections(&mut self, selections: &StatusSurfaceSelections) {
        let enabled = !selections.status_line_items.is_empty();
        self.bottom_pane.set_status_line_enabled(enabled);
        if !enabled {
            self.set_status_line(/*status_line*/ None);
            self.set_status_hud_lines(Vec::new());
            return;
        }

        let mut parts = Vec::new();
        for item in &selections.status_line_items {
            if let Some(value) = self.status_line_value_for_item(item) {
                parts.push(value);
            }
        }

        let line = if parts.is_empty() {
            None
        } else if self.config.tui_status_line.is_none() {
            None
        } else {
            Some(Line::from(parts.join(" | ")))
        };
        self.set_status_line(line);
        self.set_status_hud_lines(self.status_hud_lines());
    }

    fn status_surface_needs_git_branch(&self, selections: &StatusSurfaceSelections) -> bool {
        !selections.status_line_items.is_empty() || selections.uses_git_branch()
    }

    fn status_hud_lines(&self) -> Vec<Line<'static>> {
        let mut sections = Vec::new();
        if let Some(line) = self.status_hud_mode_line() {
            sections.push((HUD_SKY, line));
        }
        if let Some(line) = self.status_hud_session_line() {
            sections.push((HUD_VIOLET, line));
        }
        if let Some(line) = self.status_hud_usage_line() {
            sections.push((HUD_MINT, line));
        }
        if let Some(line) = self.status_hud_activity_line() {
            sections.push((HUD_CORAL, line));
        }
        let total = sections.len();
        sections
            .into_iter()
            .enumerate()
            .map(|(index, (accent, line))| {
                self.status_hud_panel_line(index, total, accent, line)
            })
            .collect()
    }

    fn status_hud_mode_line(&self) -> Option<Line<'static>> {
        let mut spans = Vec::new();
        Self::hud_push_badge(
            &mut spans,
            "MODEL",
            Self::truncate_terminal_title_part(
                self.model_display_name().to_string(),
                HUD_MODEL_MAX_CHARS,
            ),
            hud_color(HUD_SKY),
        );
        Self::hud_push_badge(
            &mut spans,
            "MIND",
            Self::status_line_reasoning_effort_label(self.effective_reasoning_effort()),
            hud_color(HUD_GOLD),
        );
        Self::hud_push_badge(
            &mut spans,
            "TIER",
            self.status_hud_service_tier_segment(),
            self.status_hud_tier_color(),
        );
        Self::hud_push_badge(
            &mut spans,
            "MODE",
            self.active_mode_kind().display_name(),
            hud_color(HUD_VIOLET),
        );

        let state = self.terminal_title_status_text();
        Self::hud_push_badge(&mut spans, "STATE", state.clone(), Self::status_hud_status_color(&state));

        if let Some(plan_type) = self.plan_type {
            Self::hud_push_badge(
                &mut spans,
                "PLAN",
                crate::status::plan_type_display_name(plan_type),
                hud_color(HUD_MINT),
            );
        }

        if let Some((completed, total)) = self.last_plan_progress {
            Self::hud_push_badge(
                &mut spans,
                "TASKS",
                format!("{completed}/{total}"),
                hud_color(HUD_MINT),
            );
        }

        (!spans.is_empty()).then(|| Line::from(spans))
    }

    fn status_hud_session_line(&self) -> Option<Line<'static>> {
        let mut spans = Vec::new();
        Self::hud_push_badge(&mut spans, "THREAD", self.status_hud_thread_display(), hud_color(HUD_VIOLET));
        if let Some(forked_from) = self.forked_from {
            Self::hud_push_badge(&mut spans, "FORK", Self::short_thread_id(forked_from), hud_color(HUD_CORAL));
        }
        Self::hud_push_badge(&mut spans, "SPACE", self.status_hud_workspace_display(), hud_color(HUD_SKY));
        let (git_value, git_color) = self.status_hud_git_display();
        Self::hud_push_badge(&mut spans, "GIT", git_value, git_color);
        if let Some(mcp_summary) = self.status_hud_mcp_summary() {
            Self::hud_push_badge(&mut spans, "MCP", mcp_summary, hud_color(HUD_MINT));
        }

        (!spans.is_empty()).then(|| Line::from(spans))
    }

    pub(crate) fn status_hud_usage_line(&self) -> Option<Line<'static>> {
        let mut spans = Vec::new();
        let used_tokens = self.status_line_context_usage().tokens_in_context_window();
        if let Some(window_size) = self.status_line_context_window_size() {
            let used_percent = self.status_line_context_used_percent().unwrap_or(0);
            let context_color = Self::status_hud_meter_color(100.0f64 - used_percent as f64);
            Self::hud_push_meter(
                &mut spans,
                "CONTEXT",
                used_percent as f64,
                HUD_CONTEXT_BAR_WIDTH,
                context_color,
                format!(
                    "{used_percent}%  {}/{}",
                    format_tokens_compact(used_tokens),
                    format_tokens_compact(window_size),
                ),
            );
        } else if used_tokens > 0 {
            Self::hud_push_badge(
                &mut spans,
                "CONTEXT",
                format!("{} used", format_tokens_compact(used_tokens)),
                hud_color(HUD_MINT),
            );
        }

        if let Some(io) = self.status_hud_io_segment() {
            Self::hud_push_badge(&mut spans, "TOKENS", io, hud_color(HUD_SKY));
        }
        if let Some(credits) = self.status_hud_credits_segment() {
            Self::hud_push_badge(&mut spans, "CREDITS", credits, hud_color(HUD_VIOLET));
        }

        (!spans.is_empty()).then(|| Line::from(spans))
    }

    fn status_hud_activity_line(&self) -> Option<Line<'static>> {
        let mut spans = Vec::new();
        let codex_limits = self.rate_limit_snapshots_by_limit_id.get("codex");
        if let Some(window) = codex_limits.and_then(|limits| limits.primary.as_ref()) {
            Self::hud_push_limit_meter(&mut spans, "5H", window);
        } else {
            Self::hud_push_badge(&mut spans, "5H", "sync", hud_color(HUD_GOLD));
        }
        if let Some(window) = codex_limits.and_then(|limits| limits.secondary.as_ref()) {
            Self::hud_push_limit_meter(&mut spans, "WEEK", window);
        } else {
            Self::hud_push_badge(&mut spans, "WEEK", "sync", hud_color(HUD_VIOLET));
        }

        let total_processes = self.running_commands.len()
            + self.unified_exec_processes.len()
            + self.pending_collab_spawn_requests.len()
            + self.collab_agent_metadata.len();
        let process_label = if total_processes == 1 {
            "process"
        } else {
            "processes"
        };
        Self::hud_push_badge(
            &mut spans,
            "",
            format!("{} {}", total_processes, process_label),
            if total_processes > 0 {
                hud_color(HUD_AMBER)
            } else {
                hud_color(HUD_MUTED)
            },
        );

        (!spans.is_empty()).then(|| Line::from(spans))
    }

    fn status_hud_service_tier_segment(&self) -> String {
        if self.should_show_fast_status(self.current_model(), self.current_service_tier()) {
            "fast".to_string()
        } else if let Some(service_tier) = self.current_service_tier() {
            service_tier.to_string()
        } else {
            "off".to_string()
        }
    }

    fn status_hud_io_segment(&self) -> Option<String> {
        let usage = self.status_line_total_usage();
        if usage.input_tokens <= 0
            && usage.output_tokens <= 0
            && usage.reasoning_output_tokens <= 0
        {
            return None;
        }

        Some(format!(
            "{} in  {} out  {} reason",
            format_tokens_compact(usage.input_tokens),
            format_tokens_compact(usage.output_tokens),
            format_tokens_compact(usage.reasoning_output_tokens),
        ))
    }

    fn status_hud_credits_segment(&self) -> Option<String> {
        let credits = self
            .rate_limit_snapshots_by_limit_id
            .get("codex")
            .and_then(|limits| limits.credits.as_ref())?;

        if !credits.has_credits {
            return None;
        }

        if credits.unlimited {
            return Some("unlimited".to_string());
        }

        credits.balance.clone()
    }

    fn status_hud_mcp_summary(&self) -> Option<String> {
        let current = self.mcp_startup_status.as_ref()?;
        if current.is_empty() {
            return None;
        }

        let mut ready = 0usize;
        let mut starting = 0usize;
        let mut failed = 0usize;
        let mut cancelled = 0usize;
        for status in current.values() {
            match status {
                McpStartupStatus::Starting => starting += 1,
                McpStartupStatus::Ready => ready += 1,
                McpStartupStatus::Failed { .. } => failed += 1,
                McpStartupStatus::Cancelled => cancelled += 1,
            }
        }

        let total = current.len();
        Some(if failed > 0 {
            format!("{ready}/{total} ready  {failed} failed")
        } else if cancelled > 0 {
            format!("{ready}/{total} ready  {cancelled} cancelled")
        } else if starting > 0 {
            format!("{ready}/{total} ready  {starting} starting")
        } else {
            format!("{ready}/{total} ready")
        })
    }

    fn status_hud_thread_display(&self) -> String {
        let mut parts = Vec::new();
        if let Some(thread_name) = self.thread_name.as_ref().map(|name| name.trim())
            && !thread_name.is_empty()
        {
            parts.push(Self::truncate_terminal_title_part(
                thread_name.to_string(),
                HUD_THREAD_MAX_CHARS,
            ));
        }
        if let Some(thread_id) = self.thread_id {
            parts.push(Self::short_thread_id(thread_id));
        }
        if parts.is_empty() {
            "fresh".to_string()
        } else {
            parts.join(" · ")
        }
    }

    fn status_hud_workspace_display(&self) -> String {
        let cwd = format_directory_display(self.status_line_cwd(), Some(24));
        let Some(project) = self.status_line_project_root_name_for_cwd(self.status_line_cwd()) else {
            return cwd;
        };
        let project = Self::truncate_terminal_title_part(project, HUD_PROJECT_MAX_CHARS);
        if project == cwd {
            cwd
        } else {
            Self::truncate_terminal_title_part(format!("{project} @ {cwd}"), 30)
        }
    }

    fn status_hud_git_display(&self) -> (String, Color) {
        if let Some(branch) = self.status_line_branch.as_ref() {
            return (
                Self::truncate_terminal_title_part(branch.clone(), HUD_BRANCH_MAX_CHARS),
                hud_color(HUD_MINT),
            );
        }
        if self.status_line_branch_pending {
            return ("scanning".to_string(), hud_color(HUD_GOLD));
        }
        if self.status_line_branch_lookup_complete {
            return ("no-repo".to_string(), hud_color(HUD_SLATE));
        }
        ("standby".to_string(), hud_color(HUD_SLATE))
    }

    fn status_hud_tier_color(&self) -> Color {
        if self.should_show_fast_status(self.current_model(), self.current_service_tier()) {
            hud_color(HUD_MINT)
        } else if self.current_service_tier().is_some() {
            hud_color(HUD_VIOLET)
        } else {
            hud_color(HUD_SLATE)
        }
    }

    fn status_hud_status_color(status: &str) -> Color {
        match status {
            "Ready" => hud_color(HUD_MINT),
            "Working" => hud_color(HUD_SKY),
            "Waiting" => hud_color(HUD_VIOLET),
            "Undoing" => hud_color(HUD_CORAL),
            "Starting" => hud_color(HUD_GOLD),
            "Thinking" => hud_color(HUD_GOLD),
            _ => hud_color(HUD_SLATE),
        }
    }

    fn status_hud_meter_color(remaining_percent: f64) -> Color {
        if remaining_percent >= 60.0 {
            hud_color(HUD_MINT)
        } else if remaining_percent >= 25.0 {
            hud_color(HUD_GOLD)
        } else {
            hud_color(HUD_ROSE)
        }
    }

    fn status_hud_panel_line(
        &self,
        index: usize,
        total: usize,
        accent: (u8, u8, u8),
        content: Line<'static>,
    ) -> Line<'static> {
        let mut spans = Vec::new();
        let border = if total <= 1 {
            "•"
        } else if index == 0 {
            "┌"
        } else if index + 1 == total {
            "└"
        } else {
            "│"
        };
        spans.push(Span::styled(
            border,
            Style::default().fg(hud_color(accent)).add_modifier(Modifier::BOLD),
        ));
        spans.push(Span::raw(" "));

        if index == 0 {
            spans.push(Span::styled(
                self.status_hud_live_marker(),
                Style::default()
                    .fg(Self::status_hud_status_color(&self.terminal_title_status_text()))
                    .add_modifier(Modifier::BOLD),
            ));
            spans.push(Span::raw(" "));
        }

        spans.extend(content.spans);
        Line::from(spans)
    }

    fn status_hud_live_marker(&self) -> &'static str {
        let now = Instant::now();
        let elapsed = now.saturating_duration_since(self.terminal_title_animation_origin);
        if self.terminal_title_has_active_progress() {
            let frame_index = (elapsed.as_millis() / HUD_ACTIVE_INTERVAL.as_millis()) as usize;
            HUD_LIVE_SPINNER_FRAMES[frame_index % HUD_LIVE_SPINNER_FRAMES.len()]
        } else {
            let frame_index = (elapsed.as_millis() / HUD_IDLE_INTERVAL.as_millis()) as usize;
            HUD_LIVE_PULSE_FRAMES[frame_index % HUD_LIVE_PULSE_FRAMES.len()]
        }
    }

    pub(super) fn should_animate_status_hud(&self) -> bool {
        self.config.animations && !self.configured_status_line_items().is_empty()
    }

    pub(super) fn status_hud_animation_interval(&self) -> Duration {
        if self.terminal_title_has_active_progress() {
            HUD_ACTIVE_INTERVAL
        } else {
            HUD_IDLE_INTERVAL
        }
    }

    fn hud_push_badge(
        spans: &mut Vec<Span<'static>>,
        label: &str,
        value: impl Into<String>,
        color: Color,
    ) {
        if !spans.is_empty() {
            spans.push(Span::styled(" › ", Style::default().fg(color).add_modifier(Modifier::BOLD)));
        }
        spans.push(Self::hud_label_span(label, color));
        spans.push(Self::hud_value_span(value, color));
    }

    fn hud_push_meter(
        spans: &mut Vec<Span<'static>>,
        label: &str,
        percent: f64,
        width: usize,
        color: Color,
        summary: impl Into<String>,
    ) {
        if !spans.is_empty() {
            spans.push(Span::styled(" › ", Style::default().fg(color).add_modifier(Modifier::BOLD)));
        }
        spans.push(Self::hud_label_span(label, color));
        Self::hud_push_bar(spans, percent, width, color);
        spans.push(Span::styled(" ", Style::default().fg(color)));
        spans.push(Self::hud_value_span(summary, color));
    }

    fn hud_push_limit_meter(
        spans: &mut Vec<Span<'static>>,
        label: &str,
        window: &RateLimitWindowDisplay,
    ) {
        let remaining = (100.0f64 - window.used_percent).clamp(0.0f64, 100.0f64);
        let color = Self::status_hud_meter_color(remaining);
        let summary = if let Some(resets_at) = window.resets_at.as_deref() {
            format!("{remaining:.0}% @{resets_at}")
        } else {
            format!("{remaining:.0}%")
        };
        Self::hud_push_meter(spans, label, remaining, HUD_LIMIT_BAR_WIDTH, color, summary);
    }

    fn hud_push_bar(
        spans: &mut Vec<Span<'static>>,
        percent: f64,
        width: usize,
        color: Color,
    ) {
        let clamped = percent.clamp(0.0, 100.0);
        let filled = ((clamped / 100.0) * width as f64).round() as usize;
        let filled = filled.min(width);
        let empty = width.saturating_sub(filled);
        spans.push(Span::styled("[", Style::default().fg(hud_color(HUD_MUTED))));
        if filled > 0 {
            spans.push(Span::styled(
                "█".repeat(filled),
                Style::default().fg(color).add_modifier(Modifier::BOLD),
            ));
        }
        if empty > 0 {
            spans.push(Span::styled("░".repeat(empty), Style::default().fg(hud_color(HUD_MUTED))));
        }
        spans.push(Span::styled("]", Style::default().fg(hud_color(HUD_MUTED))));
    }

    fn hud_label_span(label: &str, color: Color) -> Span<'static> {
        Span::styled(
            format!("{} ", label.to_lowercase()),
            Style::default()
                .fg(color)
                .add_modifier(Modifier::BOLD),
        )
    }

    fn hud_value_span(value: impl Into<String>, color: Color) -> Span<'static> {
        Span::styled(
            value.into(),
            Style::default()
                .fg(color)
                .add_modifier(Modifier::BOLD),
        )
    }

    fn short_thread_id(thread_id: ThreadId) -> String {
        thread_id.to_string().chars().take(8).collect()
    }

    /// Clears the terminal title Codex most recently wrote, if any.
    ///
    /// This does not attempt to restore the shell or terminal's previous title;
    /// it only clears the managed title and updates the cache after a successful
    /// OSC write.
    pub(crate) fn clear_managed_terminal_title(&mut self) -> std::io::Result<()> {
        if self.last_terminal_title.is_some() {
            clear_terminal_title()?;
            self.last_terminal_title = None;
        }

        Ok(())
    }

    /// Renders and applies the terminal title for one parsed selection snapshot.
    ///
    /// Empty selections clear the managed title. Non-empty selections render the
    /// current values in configured order, skip unavailable segments, and cache
    /// the last successfully written title so redundant OSC writes are avoided.
    /// When the `spinner` item is present in an animated running state, this also
    /// schedules the next frame so the spinner keeps advancing.
    fn refresh_terminal_title_from_selections(&mut self, selections: &StatusSurfaceSelections) {
        if selections.terminal_title_items.is_empty() {
            if let Err(err) = self.clear_managed_terminal_title() {
                tracing::debug!(error = %err, "failed to clear terminal title");
            }
            return;
        }

        let now = Instant::now();
        let mut previous = None;
        let title = selections
            .terminal_title_items
            .iter()
            .copied()
            .filter_map(|item| {
                self.terminal_title_value_for_item(item, now)
                    .map(|value| (item, value))
            })
            .fold(String::new(), |mut title, (item, value)| {
                title.push_str(item.separator_from_previous(previous));
                title.push_str(&value);
                previous = Some(item);
                title
            });
        let title = (!title.is_empty()).then_some(title);
        let should_animate_spinner =
            self.should_animate_terminal_title_spinner_with_selections(selections);
        if self.last_terminal_title == title {
            if should_animate_spinner {
                self.frame_requester
                    .schedule_frame_in(TERMINAL_TITLE_SPINNER_INTERVAL);
            }
            return;
        }
        match title {
            Some(title) => match set_terminal_title(&title) {
                Ok(SetTerminalTitleResult::Applied) => {
                    self.last_terminal_title = Some(title);
                }
                Ok(SetTerminalTitleResult::NoVisibleContent) => {
                    if let Err(err) = self.clear_managed_terminal_title() {
                        tracing::debug!(error = %err, "failed to clear terminal title");
                    }
                }
                Err(err) => {
                    tracing::debug!(error = %err, "failed to set terminal title");
                }
            },
            None => {
                if let Err(err) = self.clear_managed_terminal_title() {
                    tracing::debug!(error = %err, "failed to clear terminal title");
                }
            }
        }

        if should_animate_spinner {
            self.frame_requester
                .schedule_frame_in(TERMINAL_TITLE_SPINNER_INTERVAL);
        }
    }

    /// Recomputes both status surfaces from one shared config snapshot.
    ///
    /// This is the common refresh entrypoint for the footer status line and the
    /// terminal title. It parses both configurations once, emits invalid-item
    /// warnings once, synchronizes shared cached state (such as git-branch
    /// lookup), then renders each surface from that shared snapshot.
    pub(crate) fn refresh_status_surfaces(&mut self) {
        let selections = self.status_surface_selections();
        self.warn_invalid_status_line_items_once(&selections.invalid_status_line_items);
        self.warn_invalid_terminal_title_items_once(&selections.invalid_terminal_title_items);
        self.sync_status_surface_shared_state(&selections);
        self.refresh_status_line_from_selections(&selections);
        self.refresh_terminal_title_from_selections(&selections);
    }

    /// Recomputes and emits the terminal title from config and runtime state.
    pub(crate) fn refresh_terminal_title(&mut self) {
        let selections = self.status_surface_selections();
        self.warn_invalid_terminal_title_items_once(&selections.invalid_terminal_title_items);
        self.sync_status_surface_shared_state(&selections);
        self.refresh_terminal_title_from_selections(&selections);
    }

    pub(super) fn request_status_line_branch_refresh(&mut self) {
        let selections = self.status_surface_selections();
        if !self.status_surface_needs_git_branch(&selections) {
            return;
        }
        let cwd = self.status_line_cwd().to_path_buf();
        self.sync_status_line_branch_state(&cwd);
        self.request_status_line_branch(cwd);
    }

    /// Parses configured status-line ids into known items and collects unknown ids.
    ///
    /// Unknown ids are deduplicated in insertion order for warning messages.
    fn status_line_items_with_invalids(&self) -> (Vec<StatusLineItem>, Vec<String>) {
        parse_items_with_invalids(self.configured_status_line_items())
    }

    pub(super) fn configured_status_line_items(&self) -> Vec<String> {
        self.config.tui_status_line.clone().unwrap_or_else(|| {
            DEFAULT_STATUS_LINE_ITEMS
                .iter()
                .map(ToString::to_string)
                .collect()
        })
    }

    /// Parses configured terminal-title ids into known items and collects unknown ids.
    ///
    /// Unknown ids are deduplicated in insertion order for warning messages.
    fn terminal_title_items_with_invalids(&self) -> (Vec<TerminalTitleItem>, Vec<String>) {
        parse_items_with_invalids(self.configured_terminal_title_items())
    }

    /// Returns the configured terminal-title ids, or the default ordering when unset.
    pub(super) fn configured_terminal_title_items(&self) -> Vec<String> {
        self.config.tui_terminal_title.clone().unwrap_or_else(|| {
            DEFAULT_TERMINAL_TITLE_ITEMS
                .iter()
                .map(ToString::to_string)
                .collect()
        })
    }

    fn status_line_cwd(&self) -> &Path {
        self.current_cwd
            .as_deref()
            .unwrap_or(self.config.cwd.as_path())
    }

    /// Resolves the project root associated with `cwd`.
    ///
    /// Git repository root wins when available. Otherwise we fall back to the
    /// nearest project config layer so non-git projects can still surface a
    /// stable project label.
    fn status_line_project_root_for_cwd(&self, cwd: &Path) -> Option<PathBuf> {
        if let Some(repo_root) = get_git_repo_root(cwd) {
            return Some(repo_root);
        }

        self.config
            .config_layer_stack
            .get_layers(
                ConfigLayerStackOrdering::LowestPrecedenceFirst,
                /*include_disabled*/ true,
            )
            .iter()
            .find_map(|layer| match &layer.name {
                ConfigLayerSource::Project { dot_codex_folder } => {
                    dot_codex_folder.as_path().parent().map(Path::to_path_buf)
                }
                _ => None,
            })
    }

    fn status_line_project_root_name_for_cwd(&self, cwd: &Path) -> Option<String> {
        self.status_line_project_root_for_cwd(cwd).map(|root| {
            root.file_name()
                .map(|name| name.to_string_lossy().to_string())
                .unwrap_or_else(|| format_directory_display(&root, /*max_width*/ None))
        })
    }

    /// Returns a cached project-root display name for the active cwd.
    fn status_line_project_root_name(&mut self) -> Option<String> {
        let cwd = self.status_line_cwd().to_path_buf();
        if let Some(cache) = &self.status_line_project_root_name_cache
            && cache.cwd == cwd
        {
            return cache.root_name.clone();
        }

        let root_name = self.status_line_project_root_name_for_cwd(&cwd);
        self.status_line_project_root_name_cache = Some(CachedProjectRootName {
            cwd,
            root_name: root_name.clone(),
        });
        root_name
    }

    /// Produces the terminal-title `project` value.
    ///
    /// This prefers the cached project-root name and falls back to the current
    /// directory name when no project root can be inferred.
    fn terminal_title_project_name(&mut self) -> Option<String> {
        let project = self.status_line_project_root_name().or_else(|| {
            let cwd = self.status_line_cwd();
            Some(
                cwd.file_name()
                    .map(|name| name.to_string_lossy().to_string())
                    .unwrap_or_else(|| format_directory_display(cwd, /*max_width*/ None)),
            )
        })?;
        Some(Self::truncate_terminal_title_part(
            project, /*max_chars*/ 24,
        ))
    }

    /// Resets git-branch cache state when the status-line cwd changes.
    ///
    /// The branch cache is keyed by cwd because branch lookup is performed relative to that path.
    /// Keeping stale branch values across cwd changes would surface incorrect repository context.
    fn sync_status_line_branch_state(&mut self, cwd: &Path) {
        if self
            .status_line_branch_cwd
            .as_ref()
            .is_some_and(|path| path == cwd)
        {
            return;
        }
        self.status_line_branch_cwd = Some(cwd.to_path_buf());
        self.status_line_branch = None;
        self.status_line_branch_pending = false;
        self.status_line_branch_lookup_complete = false;
    }

    /// Starts an async git-branch lookup unless one is already running.
    ///
    /// The resulting `StatusLineBranchUpdated` event carries the lookup cwd so callers can reject
    /// stale completions after directory changes.
    fn request_status_line_branch(&mut self, cwd: PathBuf) {
        if self.status_line_branch_pending {
            return;
        }
        self.status_line_branch_pending = true;
        let tx = self.app_event_tx.clone();
        tokio::spawn(async move {
            let branch = current_branch_name(&cwd).await;
            tx.send(AppEvent::StatusLineBranchUpdated { cwd, branch });
        });
    }

    /// Resolves a display string for one configured status-line item.
    ///
    /// Returning `None` means "omit this item for now", not "configuration error". Callers rely on
    /// this to keep partially available status lines readable while waiting for session, token, or
    /// git metadata.
    pub(super) fn status_line_value_for_item(&mut self, item: &StatusLineItem) -> Option<String> {
        match item {
            StatusLineItem::ModelName => Some(self.model_display_name().to_string()),
            StatusLineItem::ModelWithReasoning => {
                let label =
                    Self::status_line_reasoning_effort_label(self.effective_reasoning_effort());
                let fast_label = if self
                    .should_show_fast_status(self.current_model(), self.config.service_tier)
                {
                    " fast"
                } else {
                    ""
                };
                Some(format!("{} {label}{fast_label}", self.model_display_name()))
            }
            StatusLineItem::CurrentDir => {
                Some(format_directory_display(
                    self.status_line_cwd(),
                    /*max_width*/ None,
                ))
            }
            StatusLineItem::ProjectRoot => self.status_line_project_root_name(),
            StatusLineItem::GitBranch => self.status_line_branch.clone(),
            StatusLineItem::UsedTokens => {
                let usage = self.status_line_total_usage();
                let total = usage.tokens_in_context_window();
                if total <= 0 {
                    None
                } else {
                    Some(format!("{} used", format_tokens_compact(total)))
                }
            }
            StatusLineItem::ContextRemaining => self
                .status_line_context_remaining_percent()
                .map(|remaining| format!("Context {remaining}% left")),
            StatusLineItem::ContextUsed => self
                .status_line_context_used_percent()
                .map(|used| format!("Context {used}% used")),
            StatusLineItem::FiveHourLimit => {
                let window = self
                    .rate_limit_snapshots_by_limit_id
                    .get("codex")
                    .and_then(|s| s.primary.as_ref());
                let label = window
                    .and_then(|window| window.window_minutes)
                    .map(get_limits_duration)
                    .unwrap_or_else(|| "5h".to_string());
                self.status_line_limit_display(window, &label)
            }
            StatusLineItem::WeeklyLimit => {
                let window = self
                    .rate_limit_snapshots_by_limit_id
                    .get("codex")
                    .and_then(|s| s.secondary.as_ref());
                let label = window
                    .and_then(|window| window.window_minutes)
                    .map(get_limits_duration)
                    .unwrap_or_else(|| "weekly".to_string());
                self.status_line_limit_display(window, &label)
            }
            StatusLineItem::CodexVersion => Some(CODEX_CLI_VERSION.to_string()),
            StatusLineItem::ContextWindowSize => self
                .status_line_context_window_size()
                .map(|cws| format!("{} window", format_tokens_compact(cws))),
            StatusLineItem::TotalInputTokens => Some(format!(
                "{} in",
                format_tokens_compact(self.status_line_total_usage().input_tokens)
            )),
            StatusLineItem::TotalOutputTokens => Some(format!(
                "{} out",
                format_tokens_compact(self.status_line_total_usage().output_tokens)
            )),
            StatusLineItem::SessionId => self.thread_id.map(|id| id.to_string()),
            StatusLineItem::FastMode => Some(
                if matches!(self.config.service_tier, Some(ServiceTier::Fast)) {
                    "Fast on".to_string()
                } else {
                    "Fast off".to_string()
                },
            ),
            StatusLineItem::ThreadTitle => self.thread_name.as_ref().and_then(|name| {
                let trimmed = name.trim();
                (!trimmed.is_empty()).then(|| trimmed.to_string())
            }),
        }
    }

    /// Resolves one configured terminal-title item into a displayable segment.
    ///
    /// Returning `None` means "omit this segment for now" so callers can keep
    /// the configured order while hiding values that are not yet available.
    fn terminal_title_value_for_item(
        &mut self,
        item: TerminalTitleItem,
        now: Instant,
    ) -> Option<String> {
        match item {
            TerminalTitleItem::AppName => Some("codex".to_string()),
            TerminalTitleItem::Project => self.terminal_title_project_name(),
            TerminalTitleItem::Spinner => self.terminal_title_spinner_text_at(now),
            TerminalTitleItem::Status => Some(self.terminal_title_status_text()),
            TerminalTitleItem::Thread => self.thread_name.as_ref().and_then(|name| {
                let trimmed = name.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(Self::truncate_terminal_title_part(
                        trimmed.to_string(),
                        /*max_chars*/ 48,
                    ))
                }
            }),
            TerminalTitleItem::GitBranch => self.status_line_branch.as_ref().map(|branch| {
                Self::truncate_terminal_title_part(branch.clone(), /*max_chars*/ 32)
            }),
            TerminalTitleItem::Model => Some(Self::truncate_terminal_title_part(
                self.model_display_name().to_string(),
                /*max_chars*/ 32,
            )),
            TerminalTitleItem::TaskProgress => self.terminal_title_task_progress(),
        }
    }

    /// Computes the compact runtime status label used by the terminal title.
    ///
    /// Startup takes precedence over normal task states, and idle state renders
    /// as `Ready` regardless of the last active status bucket.
    pub(super) fn terminal_title_status_text(&self) -> String {
        if self.mcp_startup_status.is_some() {
            return "Starting".to_string();
        }

        match self.terminal_title_status_kind {
            TerminalTitleStatusKind::Working if !self.bottom_pane.is_task_running() => {
                "Ready".to_string()
            }
            TerminalTitleStatusKind::WaitingForBackgroundTerminal
                if !self.bottom_pane.is_task_running() =>
            {
                "Ready".to_string()
            }
            TerminalTitleStatusKind::Thinking if !self.bottom_pane.is_task_running() => {
                "Ready".to_string()
            }
            TerminalTitleStatusKind::Working => "Working".to_string(),
            TerminalTitleStatusKind::WaitingForBackgroundTerminal => "Waiting".to_string(),
            TerminalTitleStatusKind::Undoing => "Undoing".to_string(),
            TerminalTitleStatusKind::Thinking => "Thinking".to_string(),
        }
    }

    pub(super) fn terminal_title_spinner_text_at(&self, now: Instant) -> Option<String> {
        if !self.config.animations {
            return None;
        }

        if !self.terminal_title_has_active_progress() {
            return None;
        }

        Some(self.terminal_title_spinner_frame_at(now).to_string())
    }

    fn terminal_title_spinner_frame_at(&self, now: Instant) -> &'static str {
        let elapsed = now.saturating_duration_since(self.terminal_title_animation_origin);
        let frame_index =
            (elapsed.as_millis() / TERMINAL_TITLE_SPINNER_INTERVAL.as_millis()) as usize;
        TERMINAL_TITLE_SPINNER_FRAMES[frame_index % TERMINAL_TITLE_SPINNER_FRAMES.len()]
    }

    fn terminal_title_uses_spinner(&self) -> bool {
        self.config
            .tui_terminal_title
            .as_ref()
            .is_none_or(|items| items.iter().any(|item| item == "spinner"))
    }

    fn terminal_title_has_active_progress(&self) -> bool {
        self.mcp_startup_status.is_some()
            || self.bottom_pane.is_task_running()
            || self.terminal_title_status_kind == TerminalTitleStatusKind::Undoing
    }

    pub(super) fn should_animate_terminal_title_spinner(&self) -> bool {
        self.config.animations
            && self.terminal_title_uses_spinner()
            && self.terminal_title_has_active_progress()
    }

    fn should_animate_terminal_title_spinner_with_selections(
        &self,
        selections: &StatusSurfaceSelections,
    ) -> bool {
        self.config.animations
            && selections
                .terminal_title_items
                .contains(&TerminalTitleItem::Spinner)
            && self.terminal_title_has_active_progress()
    }

    /// Formats the last `update_plan` progress snapshot for terminal-title display.
    pub(super) fn terminal_title_task_progress(&self) -> Option<String> {
        let (completed, total) = self.last_plan_progress?;
        if total == 0 {
            return None;
        }
        Some(format!("Tasks {completed}/{total}"))
    }

    /// Truncates a title segment by grapheme cluster and appends `...` when needed.
    pub(super) fn truncate_terminal_title_part(value: String, max_chars: usize) -> String {
        if max_chars == 0 {
            return String::new();
        }

        let mut graphemes = value.graphemes(true);
        let head: String = graphemes.by_ref().take(max_chars).collect();
        if graphemes.next().is_none() || max_chars <= 3 {
            return head;
        }

        let mut truncated = head.graphemes(true).take(max_chars - 3).collect::<String>();
        truncated.push_str("...");
        truncated
    }
}

fn parse_items_with_invalids<T>(ids: impl IntoIterator<Item = String>) -> (Vec<T>, Vec<String>)
where
    T: std::str::FromStr,
{
    let mut invalid = Vec::new();
    let mut invalid_seen = HashSet::new();
    let mut items = Vec::new();
    for id in ids {
        match id.parse::<T>() {
            Ok(item) => items.push(item),
            Err(_) => {
                if invalid_seen.insert(id.clone()) {
                    invalid.push(format!(r#""{id}""#));
                }
            }
        }
    }
    (items, invalid)
}
