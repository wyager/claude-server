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
    /// Uses a char-based estimate (chars / 4 ≈ tokens).
    pub fn estimate_post_compaction(
        &self,
        state: &HarnessState,
        deployment_context: &str,
        config: &RenderConfig,
    ) -> u64 {
        // Clone state and simulate the compaction
        // For MVP, just estimate based on the current rendered size minus
        // a rough estimate of what the script would remove
        // Use a large threshold since we're just estimating size, not displaying it
        let rendered = renderer::render_context(state, deployment_context, None, config, u64::MAX);
        (rendered.text.len() as u64) / 4
    }

    /// Build the CompactionState for rendering.
    pub fn compaction_state(&self, current_usage: u64) -> renderer::CompactionState {
        renderer::CompactionState {
            current_usage,
            target_usage: self.target_usage,
            compaction_script: self.script.clone(),
            estimated_post_compaction: current_usage, // Will be updated by estimate
        }
    }

    /// Reset compaction state after successful compaction.
    pub fn complete(&mut self) {
        self.active = false;
        self.script.clear();
        self.target_usage = 0;
    }
}
