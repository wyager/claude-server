use std::collections::HashMap;

use crate::python;
use crate::renderer;
use crate::types::*;

pub struct CompactionManager {
    pub active: bool,
    pub script: String,
    pub target_usage: u64,
}

impl CompactionManager {
    pub fn new() -> Self {
        Self {
            active: false,
            script: String::new(),
            target_usage: 0,
        }
    }

    /// Check if compaction should be triggered. Compares against the total
    /// prompt-token load (billed + cache read + cache write) because a
    /// multi-hundred-KB cached prefix still consumes the context window even
    /// though it doesn't show up in `last_input_tokens`.
    pub fn should_trigger(state: &HarnessState, compact_at: u64) -> bool {
        state.last_total_input_tokens > compact_at
    }

    /// Insert a Compaction work item and activate compaction mode.
    pub fn trigger(&mut self, state: &mut HarnessState, compact_target: u64) {
        self.active = true;
        self.script.clear();
        self.target_usage = compact_target;

        let id = state.id_generator.next();
        state.work_queue.push(WorkItem {
            id,
            priority: 10,
            time: chrono::Utc::now(),
            item_type: WorkItemType::Compaction,
            attachments: Vec::new(),
        });
    }

    /// Estimate token usage after running the compaction script.
    /// Dry-runs the script against a cloned state, applies history side effects,
    /// re-renders, and estimates tokens as chars/4.
    pub fn estimate_post_compaction(
        &self,
        state: &HarnessState,
        deployment_context: &str,
        config: &RenderConfig,
        compact_at: u64,
    ) -> u64 {
        if self.script.is_empty() {
            let rendered =
                renderer::render_context(state, deployment_context, None, None, config, compact_at, None, None);
            return (rendered.text.len() as u64) / 4;
        }

        // Dry-run the compaction script. execute() already clones internally;
        // on success, committed_state IS the post-compaction state with
        // history mutations applied in place.
        let result = python::execute(state, &self.script, true, &HashMap::new());
        let post = match result.committed_state {
            Some(c) => c,
            None => state.clone(),  // script errored; estimate unchanged
        };

        let rendered =
            renderer::render_context(&post, deployment_context, None, None, config, compact_at, None, None);
        (rendered.text.len() as u64) / 4
    }

    /// Build the CompactionState for rendering.
    pub fn compaction_state(&self, current_usage: u64) -> renderer::CompactionState {
        renderer::CompactionState {
            current_usage,
            target_usage: self.target_usage,
            compaction_script: self.script.clone(),
            estimated_post_compaction: current_usage,
        }
    }

    /// Reset compaction state after successful compaction.
    pub fn complete(&mut self) {
        self.active = false;
        self.script.clear();
        self.target_usage = 0;
    }
}
