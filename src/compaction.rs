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

    /// Check if compaction should be triggered.
    pub fn should_trigger(state: &HarnessState, compact_at: u64) -> bool {
        state.last_input_tokens > compact_at
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
                renderer::render_context(state, deployment_context, None, config, compact_at, None);
            return (rendered.text.len() as u64) / 4;
        }

        // Clone state and dry-run the compaction script
        let mut clone = state.clone();
        let result = python::execute(&clone, &self.script, true, &HashMap::new());

        if !result.is_error {
            // Apply history side effects to the clone
            for id in result.side_effects.history_removes {
                clone.event_history.remove(&AgentId(id));
            }
            for (id, desc) in result.side_effects.history_replaces {
                clone
                    .event_history
                    .replace_with_summary(&AgentId(id), desc);
            }
            for text in result.side_effects.history_adds {
                let id = clone.id_generator.next();
                clone.event_history.push(HistoryEntry::Summary {
                    id,
                    time: chrono::Utc::now(),
                    description: text,
                });
            }
        }

        // Re-render and estimate
        let rendered =
            renderer::render_context(&clone, deployment_context, None, config, compact_at, None);
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
