use chrono::{DateTime, Local, Utc};

use crate::types::*;

/// State tracked during compaction for rendering into the context block.
pub struct CompactionState {
    pub current_usage: u64,
    pub target_usage: u64,
    pub compaction_script: String,
    pub estimated_post_compaction: u64,
}

/// The rendered context ready for the API call.
pub struct RenderedContext {
    /// The main text content (the user message).
    pub text: String,
    // Future: images from show_in_context
}

pub fn render_context(
    state: &HarnessState,
    deployment_context: &str,
    compaction: Option<&CompactionState>,
    config: &RenderConfig,
) -> RenderedContext {
    let mut out = String::with_capacity(8192);

    // Deployment context
    render_deployment_context(&mut out, deployment_context);

    // Event history
    render_event_history(&mut out, &state.event_history, config);

    // Work queue
    render_work_queue(&mut out, &state.work_queue, config);

    // Context metadata
    render_context_metadata(&mut out, state, compaction);

    RenderedContext { text: out }
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

fn render_event_history(out: &mut String, history: &EventHistory, config: &RenderConfig) {
    out.push_str("<event_history>\n");

    for entry in history.entries() {
        render_history_entry(out, entry, config);
    }

    out.push_str("</event_history>\n");
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
            out.push_str(&format!("<entry id=\"{}\">\n", id));
            out.push_str(&format!("time: {}\n", format_time(time)));
            out.push_str("code:\n");
            out.push_str(&indent_and_truncate(code, 2, config));
            if *is_error {
                out.push_str("output: [ERROR]\n");
            } else {
                out.push_str("output:\n");
            }
            if !output.is_empty() {
                out.push_str(&indent_and_truncate(output, 2, config));
            }
            out.push_str(&format!("</entry>\n"));
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
        WorkItemType::ProcessCompleted { pid, exit_code } => {
            out.push_str("type: ProcessCompleted\n");
            out.push_str(&format!("pid: {}\n", pid));
            out.push_str(&format!("exit_code: {}\n", exit_code));
        }
        WorkItemType::ProcessFailed { pid, error } => {
            out.push_str("type: ProcessFailed\n");
            out.push_str(&format!("pid: {}\n", pid));
            out.push_str(&format!(
                "error: {}\n",
                truncate_with_note(error, content_limit)
            ));
        }
        WorkItemType::ProcessTimeout { pid } => {
            out.push_str("type: ProcessTimeout\n");
            out.push_str(&format!("pid: {}\n", pid));
        }
        WorkItemType::Compaction => {
            out.push_str("type: Compaction\n");
            out.push_str("description: \"You must compact your context.\"\n");
        }
    }

    out.push_str("</work_item>\n");
}

fn render_context_metadata(
    out: &mut String,
    state: &HarnessState,
    compaction: Option<&CompactionState>,
) {
    out.push_str("<context>\n");

    let now: DateTime<Local> = Utc::now().into();
    out.push_str(&format!(
        "Current time: {}\n",
        now.format("%Y-%m-%d %H:%M:%S %Z")
    ));
    out.push_str(&format!(
        "Last turn input tokens: {}\n",
        state.last_input_tokens
    ));

    let threshold = state.context_window.saturating_sub(state.max_tokens);
    let compact_at = (threshold as f64 * 0.8) as u64;
    out.push_str(&format!("Compaction threshold: {} tokens\n", compact_at));

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
        });

        state
    }

    #[test]
    fn test_render_produces_expected_structure() {
        let state = make_test_state();
        let config = RenderConfig::default();
        let rendered = render_context(&state, "Test deployment.", None, &config);

        assert!(rendered.text.contains("<deployment_context>"));
        assert!(rendered.text.contains("Test deployment."));
        assert!(rendered.text.contains("</deployment_context>"));

        assert!(rendered.text.contains("<event_history>"));
        assert!(rendered.text.contains("<entry id=\"3a6f\">"));
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
    }

    #[test]
    fn test_render_empty_state() {
        let state = HarnessState::new(200_000, 16384);
        let config = RenderConfig::default();
        let rendered = render_context(&state, "", None, &config);

        assert!(rendered.text.contains("<deployment_context>\n</deployment_context>"));
        assert!(rendered.text.contains("<event_history>\n</event_history>"));
        assert!(rendered.text.contains("<work_queue>\n</work_queue>"));
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
        let rendered = render_context(&state, "", Some(&compaction), &config);

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
}
