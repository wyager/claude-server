use chrono::{DateTime, Local, Utc};

use crate::types::*;

/// State tracked during compaction for rendering into the context block.
pub struct CompactionState {
    pub current_usage: u64,
    pub target_usage: u64,
    pub compaction_script: String,
    pub estimated_post_compaction: u64,
}

/// Stable per-child prefix that renders before event_history. Same bytes
/// across repeated forks → cache hits even when task/attach vary.
#[derive(Debug, Clone, Default)]
pub struct RolePrefix {
    pub context: String,
    pub attach: Vec<String>,
}

/// The rendered context ready for the API call.
pub struct RenderedContext {
    /// Text before prefix_attachments: deploy + role_context. No cache_control
    /// on this block — the breakpoint comes after the prefix images on seg1.
    pub prefix_text: String,
    /// Attachments that render in the cached region (reference images).
    pub prefix_attachments: Vec<Attachment>,
    /// Cached segments after the prefix: event_history content with
    /// cache_control breakpoints. Stride-aligned for stable caching.
    pub cached_segments: Vec<String>,
    /// Full concatenated text (prefix + segments + tail) for dumps/char counts.
    pub text: String,
    /// Tail attachments from the head View work item.
    pub attachments: Vec<Attachment>,
}

/// Agent identity info for rendering into context.
pub struct AgentIdentity<'a> {
    pub name: &'a str,
    pub lineage: &'a str,
    pub turn_counter: u32,
    pub max_turns: Option<u32>,
}

/// Summary of pinned memory for rendering in context metadata.
/// (key_count, total_chars)
pub type PinnedSummary = Option<(usize, usize)>;

pub fn render_context(
    state: &HarnessState,
    deployment_context: &str,
    role_prefix: Option<&RolePrefix>,
    compaction: Option<&CompactionState>,
    config: &RenderConfig,
    compact_at: u64,
    agent: Option<&AgentIdentity>,
    pinned_summary: PinnedSummary,
) -> RenderedContext {
    // Tail attachments: only when a View item is at the head of the queue.
    let attachments: Vec<Attachment> = match state.work_queue.items().first() {
        Some(WorkItem { item_type: WorkItemType::View { paths }, .. }) => {
            paths.iter().map(Attachment::new).collect()
        }
        _ => Vec::new(),
    };

    // Prefix = deploy + role_context. This text block gets NO cache_control —
    // the breakpoint lands on seg1 (after prefix attachments) so the images
    // are included in the cached region.
    let mut prefix_text = String::with_capacity(4096);
    render_deployment_context(&mut prefix_text, deployment_context);
    let prefix_attachments: Vec<Attachment> = if let Some(rp) = role_prefix {
        if !rp.context.is_empty() {
            prefix_text.push_str("<role_context>\n");
            prefix_text.push_str(&rp.context);
            if !prefix_text.ends_with('\n') { prefix_text.push('\n'); }
            prefix_text.push_str("</role_context>\n");
        }
        rp.attach.iter().map(Attachment::new).collect()
    } else {
        Vec::new()
    };

    // Cached segments at stride-aligned boundaries. seg1 now starts at
    // <event_history> since deploy+role live in prefix_text.
    let (prev, cur) = state.event_history.cache_splits(config.cache_stride);
    let total = state.event_history.entries().len();

    let mut seg1 = String::with_capacity(4096);
    seg1.push_str("<event_history>\n");
    render_history_range(&mut seg1, &state.event_history, config, 0..prev);

    let mut seg2 = String::new();
    render_history_range(&mut seg2, &state.event_history, config, prev..cur);

    // Volatile tail: post-stride history + modifiable window + state/meta/queue.
    let mut tail = String::with_capacity(2048);
    render_history_range(&mut tail, &state.event_history, config, cur..total);
    tail.push_str("</event_history>\n");
    render_agent_state(&mut tail, state, config);
    render_context_metadata(&mut tail, state, compaction, compact_at, agent, pinned_summary, attachments.len());
    render_work_queue(&mut tail, &state.work_queue, config);

    // Full text for dumps/char counts. Prefix attachments aren't text so
    // aren't included here.
    let mut text = String::with_capacity(prefix_text.len() + seg1.len() + seg2.len() + tail.len());
    text.push_str(&prefix_text);
    text.push_str(&seg1);
    text.push_str(&seg2);
    text.push_str(&tail);

    // Anthropic ignores cache_control on segments under ~1024 tokens. Merge
    // small segments forward so we don't waste a breakpoint. The threshold
    // counts prefix_text too since it's part of the cached region up to
    // seg1's breakpoint.
    const MIN_CACHE_CHARS: usize = 8192;
    let mut cached_segments: Vec<String> = Vec::new();
    let prefix_boost = prefix_text.len() + prefix_attachments.len() * 4096; // rough image size
    let mut acc = seg1;
    // seg1's effective size for the threshold includes the prefix content
    // that precedes it (they share the same cache_control breakpoint).
    let mut acc_effective = acc.len() + prefix_boost;
    for seg in [seg2] {
        if acc_effective >= MIN_CACHE_CHARS {
            cached_segments.push(std::mem::take(&mut acc));
            acc_effective = 0;
        }
        acc.push_str(&seg);
        acc_effective += seg.len();
    }
    if acc_effective >= MIN_CACHE_CHARS {
        cached_segments.push(acc);
    }

    RenderedContext { prefix_text, prefix_attachments, cached_segments, text, attachments }
}

fn render_deployment_context(out: &mut String, deployment_context: &str) {
    out.push_str("<deployment_context>\n");
    if !deployment_context.is_empty() {
        out.push_str(deployment_context);
        if !deployment_context.ends_with('\n') {
            out.push('\n');
        }
    }
    out.push_str("</deployment_context>\n");
}

fn render_history_range(
    out: &mut String,
    history: &EventHistory,
    config: &RenderConfig,
    range: std::ops::Range<usize>,
) {
    for entry in &history.entries()[range] {
        render_history_entry(out, entry, config);
    }
}

fn render_history_entry(out: &mut String, entry: &HistoryEntry, config: &RenderConfig) {
    match entry {
        HistoryEntry::Execution {
            id,
            time,
            code,
            output,
            is_error,
        } => {
            // Render body first so we can annotate the opening tag with its
            // size. Token estimate stays byte-stable since entry content is
            // immutable — doesn't bust the cache prefix.
            let mut body = String::new();
            body.push_str(&format!("time: {}\n", format_time(time)));
            body.push_str("code:\n");
            body.push_str(&indent_and_truncate(code, 2, config));
            if *is_error {
                body.push_str("output: [ERROR]\n");
            } else {
                body.push_str("output:\n");
            }
            if !output.is_empty() {
                body.push_str(&indent_and_truncate(output, 2, config));
            }
            out.push_str(&format!("<entry id=\"{}\" est_tokens=\"{}\">\n", id, body.len() / 4));
            out.push_str(&body);
            out.push_str("</entry>\n");
        }
        HistoryEntry::Summary {
            id, description, ..
        } => {
            out.push_str(&format!("<entry id=\"{}\">\n", id));
            out.push_str(&format!("summary: {}\n", description));
            out.push_str("</entry>\n");
        }
        HistoryEntry::SystemAlert { id, time, message } => {
            out.push_str(&format!("<entry id=\"{}\">\n", id));
            out.push_str(&format!("time: {}\n", format_time(time)));
            out.push_str(&format!("alert: {}\n", message));
            out.push_str("</entry>\n");
        }
    }
}

fn render_work_queue(out: &mut String, queue: &WorkQueue, config: &RenderConfig) {
    out.push_str("<work_queue>\n");

    let display_count = queue.len().min(config.work_queue_max_display);

    for i in 0..display_count {
        if let Some(item) = queue.get(i) {
            let char_limit = config
                .work_queue_content_limits
                .get(i)
                .copied()
                .unwrap_or(config.work_queue_default_limit);
            render_work_item(out, item, char_limit);
        }
    }

    if queue.len() > display_count {
        out.push_str(&format!(
            "... and {} more items\n",
            queue.len() - display_count
        ));
    }

    out.push_str("</work_queue>\n");
}

fn render_work_item(out: &mut String, item: &WorkItem, content_limit: usize) {
    out.push_str(&format!("<work_item id=\"{}\">\n", item.id));
    out.push_str(&format!("priority: {}\n", item.priority));
    out.push_str(&format!("time: {}\n", format_time(&item.time)));

    match &item.item_type {
        WorkItemType::UserMessage {
            chat_id,
            user,
            content,
        } => {
            out.push_str("type: UserMessage\n");
            out.push_str(&format!("chat_id: {}\n", chat_id));
            out.push_str(&format!("user: {}\n", user));
            out.push_str(&format!(
                "content: {}\n",
                truncate_with_note(content, content_limit)
            ));
        }
        WorkItemType::TimerFired {
            timer_id,
            every,
            description,
        } => {
            out.push_str("type: TimerFired\n");
            out.push_str(&format!("timer_id: {}\n", timer_id));
            if let Some(every) = every {
                out.push_str(&format!("every: {}s\n", every.as_secs()));
            }
            out.push_str(&format!("description: \"{}\"\n", description));
        }
        WorkItemType::ProcessCompleted {
            pid,
            exit_code,
            output_preview,
        } => {
            out.push_str("type: ProcessCompleted\n");
            out.push_str(&format!("pid: {}\n", pid));
            out.push_str(&format!("exit_code: {}\n", exit_code));
            if let Some(preview) = output_preview {
                out.push_str("output_preview:\n");
                for line in preview.lines() {
                    out.push_str(&format!("  {}\n", line));
                }
            }
        }
        WorkItemType::ProcessFailed {
            pid,
            error,
            output_preview,
        } => {
            out.push_str("type: ProcessFailed\n");
            out.push_str(&format!("pid: {}\n", pid));
            out.push_str(&format!(
                "error: {}\n",
                truncate_with_note(error, content_limit)
            ));
            if let Some(preview) = output_preview {
                out.push_str("output_preview:\n");
                for line in preview.lines() {
                    out.push_str(&format!("  {}\n", line));
                }
            }
        }
        WorkItemType::ProcessTimeout { pid } => {
            out.push_str("type: ProcessTimeout\n");
            out.push_str(&format!("pid: {}\n", pid));
        }
        WorkItemType::ChildAgentCompleted {
            child_name,
            turns_used,
            success,
            summary,
            result,
            cost_usd,
            cache_hit_pct,
        } => {
            out.push_str("type: ChildAgentCompleted\n");
            out.push_str(&format!("child_name: {}\n", child_name));
            out.push_str(&format!("success: {}\n", success));
            out.push_str(&format!("turns_used: {}\n", turns_used));
            out.push_str(&format!("cost: ${:.4} ({}% cache hit)\n", cost_usd, cache_hit_pct));

            if result.is_empty() {
                out.push_str("result: (empty)\n");
            } else {
                let total = result.len();
                let display_count = total.min(5);
                out.push_str(&format!(
                    "result ({} {}):\n",
                    total,
                    if total == 1 { "key" } else { "keys" }
                ));
                let mut keys: Vec<&String> = result.keys().collect();
                keys.sort();
                for key in keys.iter().take(display_count) {
                    let val_str = serde_json::to_string(&result[*key])
                        .unwrap_or_else(|_| "?".to_string());
                    let truncated = if val_str.len() > 80 {
                        format!("{}...", &val_str[..80])
                    } else {
                        val_str
                    };
                    out.push_str(&format!("  {}: {}\n", key, truncated));
                }
                if total > display_count {
                    out.push_str(&format!("  ... and {} more keys\n", total - display_count));
                }
            }

            out.push_str(&format!(
                "summary: {}\n",
                truncate_with_note(summary, content_limit)
            ));
        }
        WorkItemType::ExternalEvent {
            source,
            event_type,
            data,
        } => {
            out.push_str("type: ExternalEvent\n");
            out.push_str(&format!("source: \"{}\"\n", source));
            out.push_str(&format!("event_type: \"{}\"\n", event_type));
            let data_str = serde_json::to_string(data).unwrap_or_else(|_| "?".to_string());
            out.push_str(&format!(
                "data: {}\n",
                truncate_with_note(&data_str, content_limit)
            ));
        }
        WorkItemType::AgentMessage { from, content } => {
            out.push_str("type: AgentMessage\n");
            out.push_str(&format!("from: {}\n", from));
            out.push_str(&format!(
                "content: {}\n",
                truncate_with_note(content, content_limit)
            ));
        }
        WorkItemType::View { paths } => {
            out.push_str("type: View\n");
            out.push_str(&format!("paths: {} files\n", paths.len()));
            for p in paths.iter().take(5) {
                out.push_str(&format!("  {}\n", p));
            }
            if paths.len() > 5 {
                out.push_str(&format!("  ... +{} more\n", paths.len() - 5));
            }
            out.push_str("(content rendered as blocks below when this item is at head)\n");
        }
        WorkItemType::Compaction => {
            out.push_str("type: Compaction\n");
            out.push_str("description: \"You must compact your context.\"\n");
        }
        WorkItemType::AgentStartup => {
            out.push_str("type: AgentStartup\n");
            out.push_str("description: \"Harness restarted. Any processes/bridges you were managing are dead — inspect memory and reconnect as needed.\"\n");
        }
    }

    if !item.attachments.is_empty() {
        out.push_str(&format!("attachments: [{}]\n", item.attachments.join(", ")));
    }

    out.push_str("</work_item>\n");
}

fn render_agent_state(out: &mut String, state: &HarnessState, config: &RenderConfig) {
    let has_memory = !state.memory.is_empty();
    let has_timers = !state.timer_manager.list().is_empty();
    let running_processes: Vec<_> = state
        .process_manager
        .processes()
        .iter()
        .filter(|p| matches!(p.status, ProcessStatus::Running))
        .collect();
    let has_processes = !running_processes.is_empty();

    // Skip the block entirely if there's nothing to show
    if !has_memory && !has_timers && !has_processes {
        return;
    }

    out.push_str("<agent_state>\n");

    // Memory: sorted by priority (desc), then alphabetically
    if has_memory {
        let mut entries: Vec<(&String, &serde_json::Value)> = state.memory.iter().collect();
        let priorities = &state.memory_priorities;
        entries.sort_by(|(k1, _), (k2, _)| {
            let p1 = priorities.get(*k1).copied().unwrap_or(5);
            let p2 = priorities.get(*k2).copied().unwrap_or(5);
            p2.cmp(&p1).then_with(|| k1.cmp(k2))
        });

        let total = entries.len();
        let display_count = total.min(config.agent_state_memory_max_display);

        out.push_str(&format!(
            "Memory ({} of {} keys, by priority):\n",
            display_count, total
        ));

        for (key, value) in entries.iter().take(display_count) {
            let prio = priorities.get(*key).copied().unwrap_or(5);
            let val_str = serde_json::to_string(value).unwrap_or_else(|_| "?".to_string());
            let truncated = if val_str.len() > config.agent_state_memory_value_max_chars {
                format!(
                    "{}...",
                    &val_str[..config.agent_state_memory_value_max_chars]
                )
            } else {
                val_str
            };
            out.push_str(&format!("  [{}] {}: {}\n", prio, key, truncated));
        }

        if total > display_count {
            let min_shown_prio = entries
                .get(display_count - 1)
                .map(|(k, _)| priorities.get(*k).copied().unwrap_or(5))
                .unwrap_or(0);
            out.push_str(&format!(
                "  ... {} more keys at priority <={}\n",
                total - display_count,
                min_shown_prio
            ));
        }
        out.push('\n');
    }

    // Timers
    let timers = state.timer_manager.list();
    if has_timers {
        let display_count = timers.len().min(config.agent_state_timer_max_display);
        out.push_str(&format!("Active timers ({}):\n", timers.len()));

        for timer in timers.iter().take(display_count) {
            let schedule_str = match &timer.schedule {
                TimerSchedule::OneShot { at } => {
                    format!("one-shot at {}", format_time(at))
                }
                TimerSchedule::Recurring { every, .. } => {
                    let secs = every.as_secs();
                    if secs >= 3600 {
                        format!("every {}h", secs / 3600)
                    } else if secs >= 60 {
                        format!("every {}m", secs / 60)
                    } else {
                        format!("every {}s", secs)
                    }
                }
            };
            let ack_str = if timer.pending_ack {
                " [awaiting ack]"
            } else {
                ""
            };
            out.push_str(&format!(
                "  {}: \"{}\" | {} | priority {}{}\n",
                timer.id, timer.description, schedule_str, timer.priority, ack_str
            ));
        }
        if timers.len() > display_count {
            out.push_str(&format!(
                "  ... {} more timers\n",
                timers.len() - display_count
            ));
        }
        out.push('\n');
    }

    // Running processes
    if has_processes {
        let display_count = running_processes
            .len()
            .min(config.agent_state_process_max_display);
        out.push_str(&format!("Running processes ({}):\n", running_processes.len()));

        let now = Utc::now();
        for proc in running_processes.iter().take(display_count) {
            let elapsed = now - proc.started_at;
            let elapsed_str = if elapsed.num_hours() > 0 {
                format!("{}h {}m", elapsed.num_hours(), elapsed.num_minutes() % 60)
            } else if elapsed.num_minutes() > 0 {
                format!(
                    "{}m {}s",
                    elapsed.num_minutes(),
                    elapsed.num_seconds() % 60
                )
            } else {
                format!("{}s", elapsed.num_seconds())
            };
            let desc = if proc.description.is_empty() {
                String::new()
            } else {
                format!(" \"{}\"", proc.description)
            };
            out.push_str(&format!(
                "  {}: \"{}\"{} | running {}\n",
                proc.id, proc.cmd, desc, elapsed_str
            ));
        }
        if running_processes.len() > display_count {
            out.push_str(&format!(
                "  ... {} more processes\n",
                running_processes.len() - display_count
            ));
        }
        out.push('\n');
    }

    out.push_str("</agent_state>\n");
}

fn render_context_metadata(
    out: &mut String,
    state: &HarnessState,
    compaction: Option<&CompactionState>,
    compact_at: u64,
    agent: Option<&AgentIdentity>,
    pinned_summary: PinnedSummary,
    attachment_count: usize,
) {
    out.push_str("<context>\n");

    // Agent identity
    if let Some(a) = agent {
        out.push_str(&format!("Agent: {}\n", a.name));
        out.push_str(&format!("Lineage: {}\n", a.lineage));
        if let Some(max) = a.max_turns {
            out.push_str(&format!(
                "Turns: {}/{} ({} remaining)\n",
                a.turn_counter,
                max,
                max.saturating_sub(a.turn_counter)
            ));
        } else {
            out.push_str(&format!("Turns: {} (no limit)\n", a.turn_counter));
        }
    }

    let now: DateTime<Local> = Utc::now().into();
    out.push_str(&format!(
        "Current time: {}\n",
        now.format("%Y-%m-%d %H:%M:%S %Z")
    ));
    out.push_str(&format!(
        "Last turn input tokens: {}\n",
        state.last_input_tokens
    ));

    out.push_str(&format!("Compaction threshold: {} tokens\n", compact_at));

    // Attachments for this turn (ephemeral, visible as content blocks below)
    if attachment_count > 0 {
        out.push_str(&format!(
            "Attachments this turn: {} (ephemeral — visible once, not saved to history)\n",
            attachment_count
        ));
    }

    // Pinned memory summary
    if let Some((count, chars)) = pinned_summary {
        out.push_str(&format!("Pinned memory: {} {}, {} chars\n",
            count,
            if count == 1 { "key" } else { "keys" },
            chars,
        ));
    }

    // Modification boundary
    if let Some(boundary) = state.event_history.modification_boundary() {
        out.push_str(&format!("Can modify entries from: {}\n", boundary));
    }

    // Compaction mode
    if let Some(cs) = compaction {
        out.push_str("COMPACTION MODE:\n");
        out.push_str(&format!("  Current usage: {} tokens\n", cs.current_usage));
        out.push_str(&format!("  Target usage: {} tokens\n", cs.target_usage));
        out.push_str(&format!(
            "  Estimated usage after compaction_script: {} tokens\n",
            cs.estimated_post_compaction
        ));
        out.push_str("  compaction_script:\n");
        if cs.compaction_script.is_empty() {
            out.push_str("    # Empty - build this up, then call compact()\n");
        } else {
            for line in cs.compaction_script.lines() {
                out.push_str(&format!("    {}\n", line));
            }
        }
    }

    out.push_str("</context>\n");
}

// ---- Helpers ----

fn format_time(time: &DateTime<Utc>) -> String {
    let local: DateTime<Local> = (*time).into();
    local.format("%Y-%m-%d %H:%M:%S %Z").to_string()
}

/// Truncate a string and append a note if it exceeds the limit.
fn truncate_with_note(s: &str, max_chars: usize) -> String {
    if s.len() <= max_chars {
        format!("\"{}\"", s)
    } else {
        let truncated: String = s.chars().take(max_chars.saturating_sub(3)).collect();
        format!(
            "\"{}\" [truncated, {} chars]",
            truncated,
            s.len()
        )
    }
}

/// Indent each line and truncate by char/line limits.
fn indent_and_truncate(text: &str, indent: usize, config: &RenderConfig) -> String {
    let prefix = " ".repeat(indent);
    let mut result = String::new();
    let mut char_count = 0;
    let mut line_count = 0;

    for line in text.lines() {
        if line_count >= config.history_entry_max_lines {
            result.push_str(&format!(
                "{}[truncated, {} more lines]\n",
                prefix,
                text.lines().count() - line_count
            ));
            break;
        }

        let remaining_chars = config.history_entry_max_chars.saturating_sub(char_count);
        if remaining_chars == 0 {
            result.push_str(&format!(
                "{}[truncated, {} more chars]\n",
                prefix,
                text.len() - char_count
            ));
            break;
        }

        let line_to_add = if line.len() > remaining_chars {
            &line[..remaining_chars]
        } else {
            line
        };

        result.push_str(&prefix);
        result.push_str(line_to_add);
        result.push('\n');

        char_count += line.len();
        line_count += 1;
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn make_test_state() -> HarnessState {
        let mut state = HarnessState::new(200_000, 16384);

        // Add a history entry
        state.event_history.push(HistoryEntry::Execution {
            id: AgentId("3a6f".to_string()),
            time: Utc.with_ymd_and_hms(2026, 2, 1, 16, 35, 27).unwrap(),
            code: "print(work_queue[0].content)".to_string(),
            output: "Hello Claude, could you please keep an eye out for\nany vehicles coming up the driveway today?".to_string(),
            is_error: false,
        });

        // Add a work item
        state.work_queue.push(WorkItem {
            id: AgentId("1f13".to_string()),
            priority: 9,
            time: Utc.with_ymd_and_hms(2026, 2, 1, 16, 35, 24).unwrap(),
            item_type: WorkItemType::UserMessage {
                chat_id: "81d4".to_string(),
                user: "steve@example.com".to_string(),
                content: "Hello Claude, could you please keep an eye out for any vehicles coming up the driveway today and let me know if you see a contractor van?".to_string(),
            },
            attachments: Vec::new(),
        });

        state
    }

    #[test]
    fn test_render_produces_expected_structure() {
        let state = make_test_state();
        let config = RenderConfig::default();
        let rendered = render_context(&state, "Test deployment.", None, None, &config, 150_000, None, None);

        assert!(rendered.text.contains("<deployment_context>"));
        assert!(rendered.text.contains("Test deployment."));
        assert!(rendered.text.contains("</deployment_context>"));

        assert!(rendered.text.contains("<event_history>"));
        assert!(rendered.text.contains("<entry id=\"3a6f\" est_tokens="));
        assert!(rendered.text.contains("print(work_queue[0].content)"));
        assert!(rendered.text.contains("</event_history>"));

        assert!(rendered.text.contains("<work_queue>"));
        assert!(rendered.text.contains("<work_item id=\"1f13\">"));
        assert!(rendered.text.contains("steve@example.com"));
        assert!(rendered.text.contains("</work_queue>"));

        assert!(rendered.text.contains("<context>"));
        assert!(rendered.text.contains("Current time:"));
        assert!(rendered.text.contains("Compaction threshold:"));
        assert!(rendered.text.contains("Can modify entries from: 3a6f"));
        assert!(rendered.text.contains("</context>"));

        // Work queue should come after context metadata (for KV cache optimization)
        let context_pos = rendered.text.find("</context>").unwrap();
        let work_queue_pos = rendered.text.find("<work_queue>").unwrap();
        assert!(
            context_pos < work_queue_pos,
            "context metadata should appear before work queue"
        );
    }

    #[test]
    fn test_render_empty_state() {
        let state = HarnessState::new(200_000, 16384);
        let config = RenderConfig::default();
        let rendered = render_context(&state, "", None, None, &config, 150_000, None, None);

        assert!(rendered.text.contains("<deployment_context>\n</deployment_context>"));
        assert!(rendered.text.contains("<event_history>\n</event_history>"));
        assert!(rendered.text.contains("<work_queue>\n</work_queue>"));
    }

    #[test]
    fn test_cache_stride_segments() {
        let mut state = HarnessState::new(200_000, 16384);
        // Push 30 entries, each ~1000 chars so segments clear the 8192-char minimum.
        let big_output = "x".repeat(900);
        for i in 0..30 {
            state.event_history.push(HistoryEntry::Execution {
                id: AgentId(format!("{:04x}", i)),
                time: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, i as u32).unwrap(),
                code: format!("# turn {}", i),
                output: big_output.clone(),
                is_error: false,
            });
        }
        let config = RenderConfig { cache_stride: 10, ..RenderConfig::default() };

        // immutable_count = 30-5 = 25; cache_splits(10) = (10, 20)
        let (prev, cur) = state.event_history.cache_splits(config.cache_stride);
        assert_eq!((prev, cur), (10, 20));

        let r1 = render_context(&state, "", None, None, &config, 150_000, None, None);
        // Two segments expected: seg1=entries[0..10], seg2=entries[10..20]
        assert_eq!(r1.cached_segments.len(), 2, "expected 2 cached segments");
        // Concatenation invariant: text = prefix_text ++ cached_segments ++ tail
        let cached_len: usize = r1.cached_segments.iter().map(String::len).sum();
        let p = r1.prefix_text.len();
        assert_eq!(&r1.text[..p], r1.prefix_text);
        assert_eq!(&r1.text[p..p + cached_len], r1.cached_segments.concat());

        // Add one more entry → immutable=26, splits stay (10,20) → segments byte-identical
        state.event_history.push(HistoryEntry::Execution {
            id: AgentId("001e".into()),
            time: Utc.with_ymd_and_hms(2026, 1, 1, 0, 1, 0).unwrap(),
            code: "# turn 30".into(),
            output: big_output.clone(),
            is_error: false,
        });
        let r2 = render_context(&state, "", None, None, &config, 150_000, None, None);
        assert_eq!(r1.cached_segments, r2.cached_segments, "segments must be byte-identical between strides");

        // Advance past next stride: push 9 more → total 40, immutable=35, splits=(20,30)
        for i in 31..40 {
            state.event_history.push(HistoryEntry::Execution {
                id: AgentId(format!("{:04x}", i)),
                time: Utc.with_ymd_and_hms(2026, 1, 1, 0, 2, i as u32).unwrap(),
                code: format!("# turn {}", i),
                output: big_output.clone(),
                is_error: false,
            });
        }
        let (prev2, cur2) = state.event_history.cache_splits(config.cache_stride);
        assert_eq!((prev2, cur2), (20, 30));
        let r3 = render_context(&state, "", None, None, &config, 150_000, None, None);
        // On stride advance: new seg1 = old (seg1+seg2), so its cache entry still hits
        assert_eq!(
            r3.cached_segments[0],
            r1.cached_segments.concat(),
            "new seg1 must equal old seg1+seg2 (transition hit)"
        );
    }

    #[test]
    fn test_cache_small_entries_merge() {
        let mut state = HarnessState::new(200_000, 16384);
        // 20 tiny entries — segments under 4096 chars should not get cache_control
        for i in 0..20 {
            state.event_history.push(HistoryEntry::Execution {
                id: AgentId(format!("{:04x}", i)),
                time: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, i as u32).unwrap(),
                code: "x".into(),
                output: "y".into(),
                is_error: false,
            });
        }
        let config = RenderConfig::default();
        let r = render_context(&state, "", None, None, &config, 150_000, None, None);
        // Tiny segments should be merged/dropped — no wasted breakpoints
        assert!(r.cached_segments.is_empty(), "tiny segments should not get cache breakpoints");
    }

    #[test]
    fn test_render_compaction_mode() {
        let state = HarnessState::new(200_000, 16384);
        let config = RenderConfig::default();
        let compaction = CompactionState {
            current_usage: 142000,
            target_usage: 100000,
            compaction_script: String::new(),
            estimated_post_compaction: 142000,
        };
        let rendered = render_context(&state, "", None, Some(&compaction), &config, 150_000, None, None);

        assert!(rendered.text.contains("COMPACTION MODE:"));
        assert!(rendered.text.contains("Current usage: 142000 tokens"));
        assert!(rendered.text.contains("Target usage: 100000 tokens"));
        assert!(rendered.text.contains("# Empty - build this up"));
    }

    #[test]
    fn test_truncate_with_note() {
        assert_eq!(truncate_with_note("short", 100), "\"short\"");
        let long = "a".repeat(200);
        let result = truncate_with_note(&long, 50);
        assert!(result.contains("[truncated, 200 chars]"));
    }

    #[test]
    fn test_render_agent_state_with_memory_and_timers() {
        let mut state = HarnessState::new(200_000, 16384);

        // Add memory with priorities
        state.memory.insert("high_prio".to_string(), serde_json::json!("important value"));
        state.memory_priorities.insert("high_prio".to_string(), 8);
        state.memory.insert("low_prio".to_string(), serde_json::json!("less important"));
        state.memory_priorities.insert("low_prio".to_string(), 3);
        state.memory.insert("default_prio".to_string(), serde_json::json!({"nested": true}));
        // No priority set — defaults to 5

        // Add a timer
        state.timer_manager.add(Timer {
            id: AgentId("982a".to_string()),
            description: "Check driveway camera".to_string(),
            priority: 6,
            schedule: TimerSchedule::Recurring {
                every: std::time::Duration::from_secs(30),
                next_fire: Utc::now() + chrono::Duration::seconds(30),
            },
            created_at: Utc::now(),
            pending_ack: false,
        });

        let config = RenderConfig::default();
        let rendered = render_context(&state, "", None, None, &config, 150_000, None, None);

        // Agent state should appear
        assert!(rendered.text.contains("<agent_state>"), "Missing agent_state block");
        assert!(rendered.text.contains("</agent_state>"), "Missing closing agent_state tag");

        // Memory should be sorted by priority (high first)
        let high_pos = rendered.text.find("[8] high_prio").expect("high_prio not found");
        let default_pos = rendered.text.find("[5] default_prio").expect("default_prio not found");
        let low_pos = rendered.text.find("[3] low_prio").expect("low_prio not found");
        assert!(high_pos < default_pos, "high_prio should appear before default_prio");
        assert!(default_pos < low_pos, "default_prio should appear before low_prio");

        // Timer should appear
        assert!(rendered.text.contains("982a: \"Check driveway camera\""));
        assert!(rendered.text.contains("every 30s"));
        assert!(rendered.text.contains("priority 6"));
    }

    #[test]
    fn test_render_no_agent_state_when_empty() {
        let state = HarnessState::new(200_000, 16384);
        let config = RenderConfig::default();
        let rendered = render_context(&state, "", None, None, &config, 150_000, None, None);

        // No agent_state block when nothing to show
        assert!(!rendered.text.contains("<agent_state>"));
    }

    #[test]
    fn test_render_with_attachments() {
        let mut state = HarnessState::new(200_000, 16384);
        // View item at head → its paths become rendered attachments
        state.work_queue.push(WorkItem {
            id: AgentId("0001".into()),
            priority: 10,
            time: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            item_type: WorkItemType::View {
                paths: vec!["/tmp/camera.jpg".into(), "/tmp/meta.json".into()],
            },
            attachments: Vec::new(),
        });
        let config = RenderConfig::default();
        let rendered = render_context(&state, "", None, None, &config, 150_000, None, None);

        assert_eq!(rendered.attachments.len(), 2);
        assert!(rendered.text.contains("type: View"));
        assert!(rendered.text.contains("paths: 2 files"));
        assert!(rendered.text.contains("ephemeral"));
    }

    #[test]
    fn test_render_no_attachment_line_when_empty() {
        let state = HarnessState::new(200_000, 16384);
        let config = RenderConfig::default();
        let rendered = render_context(&state, "", None, None, &config, 150_000, None, None);

        assert!(!rendered.text.contains("Attachments this turn"));
    }

    #[test]
    fn test_render_agent_identity() {
        let state = HarnessState::new(200_000, 16384);
        let config = RenderConfig::default();
        let agent = AgentIdentity {
            name: "api-checker",
            lineage: "api-checker, child of plan-builder, child of root",
            turn_counter: 3,
            max_turns: Some(10),
        };
        let rendered = render_context(&state, "", None, None, &config, 150_000, Some(&agent), None);

        assert!(rendered.text.contains("Agent: api-checker"));
        assert!(rendered.text.contains("Lineage: api-checker, child of plan-builder, child of root"));
        assert!(rendered.text.contains("Turns: 3/10 (7 remaining)"));
    }

    #[test]
    fn test_render_agent_identity_no_limit() {
        let state = HarnessState::new(200_000, 16384);
        let config = RenderConfig::default();
        let agent = AgentIdentity {
            name: "root",
            lineage: "root",
            turn_counter: 5,
            max_turns: None,
        };
        let rendered = render_context(&state, "", None, None, &config, 150_000, Some(&agent), None);

        assert!(rendered.text.contains("Agent: root"));
        assert!(rendered.text.contains("Turns: 5 (no limit)"));
    }
}
