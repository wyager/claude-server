use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};

// ---- IDs ----

/// Short hex ID shown to the agent (e.g. "3a6f").
/// Internally backed by a u64 counter with a bijective shuffle.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AgentId(pub String);

/// Internal counter for generating AgentIds.
/// Persisted to SQLite so IDs remain unique across daemon restarts.
#[derive(Debug, Serialize, Deserialize)]
pub struct IdGenerator {
    counter: u64,
}

impl IdGenerator {
    pub fn new() -> Self {
        Self { counter: 0 }
    }

    /// Generate the next short hex ID.
    /// Applies a bijective shuffle to the counter so IDs appear random.
    pub fn next(&mut self) -> AgentId {
        let n = self.counter;
        self.counter += 1;
        // Simple bijective shuffle: multiply by a large odd number, take lower 16 bits
        let shuffled = n.wrapping_mul(0x9E3779B97F4A7C15) & 0xFFFF;
        AgentId(format!("{:04x}", shuffled))
    }
}

// ---- Work Queue ----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WorkItemType {
    UserMessage {
        chat_id: String,
        user: String,
        content: String,
    },
    TimerFired {
        timer_id: AgentId,
        /// For recurring timers
        every: Option<Duration>,
        description: String,
    },
    ProcessCompleted {
        pid: AgentId,
        exit_code: i32,
    },
    ProcessFailed {
        pid: AgentId,
        error: String,
    },
    ProcessTimeout {
        pid: AgentId,
    },
    Compaction,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkItem {
    pub id: AgentId,
    pub priority: u8, // 0-10, 10 = system
    pub time: SystemTime,
    pub item_type: WorkItemType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueFilter {
    pub name: String,
    pub regex: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WorkQueue {
    /// Sorted by (priority DESC, time ASC)
    items: Vec<WorkItem>,
    /// Persistent filters applied to incoming items
    filters: Vec<QueueFilter>,
}

impl WorkQueue {
    pub fn new() -> Self {
        Self {
            items: Vec::new(),
            filters: Vec::new(),
        }
    }

    /// Insert a work item, maintaining sort order.
    /// Returns false if the item was filtered out.
    pub fn push(&mut self, item: WorkItem) -> bool {
        // Check against filters
        if let WorkItemType::UserMessage { ref content, .. } = item.item_type {
            for filter in &self.filters {
                if let Ok(re) = regex::Regex::new(&filter.regex) {
                    if re.is_match(content) {
                        return false;
                    }
                }
            }
        }

        // Insert in sorted position: priority DESC, then time ASC
        let pos = self
            .items
            .partition_point(|existing| {
                if existing.priority != item.priority {
                    existing.priority > item.priority
                } else {
                    existing.time <= item.time
                }
            });
        self.items.insert(pos, item);
        true
    }

    pub fn pop_front(&mut self) -> Option<WorkItem> {
        if self.items.is_empty() {
            None
        } else {
            Some(self.items.remove(0))
        }
    }

    pub fn remove(&mut self, id: &AgentId) -> Option<WorkItem> {
        if let Some(pos) = self.items.iter().position(|item| &item.id == id) {
            Some(self.items.remove(pos))
        } else {
            None
        }
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn get(&self, index: usize) -> Option<&WorkItem> {
        self.items.get(index)
    }

    pub fn add_filter(&mut self, filter: QueueFilter) {
        self.filters.push(filter);
    }

    pub fn remove_filter(&mut self, name: &str) {
        self.filters.retain(|f| f.name != name);
    }
}

// ---- Event History ----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HistoryEntry {
    /// A Python script execution and its output
    Execution {
        id: AgentId,
        time: SystemTime,
        code: String,
        output: String,
        is_error: bool,
    },
    /// A summary replacing one or more older entries
    Summary {
        id: AgentId,
        time: SystemTime,
        description: String,
    },
    /// A system alert (e.g. priority change notification)
    SystemAlert {
        id: AgentId,
        time: SystemTime,
        message: String,
    },
}

impl HistoryEntry {
    pub fn id(&self) -> &AgentId {
        match self {
            HistoryEntry::Execution { id, .. } => id,
            HistoryEntry::Summary { id, .. } => id,
            HistoryEntry::SystemAlert { id, .. } => id,
        }
    }

    pub fn time(&self) -> SystemTime {
        match self {
            HistoryEntry::Execution { time, .. } => *time,
            HistoryEntry::Summary { time, .. } => *time,
            HistoryEntry::SystemAlert { time, .. } => *time,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EventHistory {
    entries: Vec<HistoryEntry>,
    /// Number of recent entries the agent is allowed to modify (replace/delete).
    /// Older entries are read-only until compaction.
    pub modification_window: usize, // typically 5
}

impl EventHistory {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            modification_window: 5,
        }
    }

    /// Returns the ID of the earliest entry the agent can modify,
    /// or None if there are no entries.
    pub fn modification_boundary(&self) -> Option<&AgentId> {
        let len = self.entries.len();
        if len == 0 {
            return None;
        }
        let start = len.saturating_sub(self.modification_window);
        Some(self.entries[start].id())
    }

    /// Whether the given entry is within the modification window.
    pub fn is_modifiable(&self, id: &AgentId) -> bool {
        let len = self.entries.len();
        let start = len.saturating_sub(self.modification_window);
        self.entries[start..].iter().any(|e| e.id() == id)
    }

    pub fn push(&mut self, entry: HistoryEntry) {
        self.entries.push(entry);
    }

    pub fn get(&self, id: &AgentId) -> Option<&HistoryEntry> {
        self.entries.iter().find(|e| e.id() == id)
    }

    pub fn remove(&mut self, id: &AgentId) -> Option<HistoryEntry> {
        if let Some(pos) = self.entries.iter().position(|e| e.id() == id) {
            Some(self.entries.remove(pos))
        } else {
            None
        }
    }

    pub fn replace_with_summary(&mut self, id: &AgentId, description: String) {
        if let Some(entry) = self.entries.iter_mut().find(|e| e.id() == id) {
            let time = entry.time();
            let id = entry.id().clone();
            *entry = HistoryEntry::Summary {
                id,
                time,
                description,
            };
        }
    }

    pub fn entries(&self) -> &[HistoryEntry] {
        &self.entries
    }
}

// ---- Timers ----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Timer {
    pub id: AgentId,
    pub description: String,
    pub priority: u8,
    pub schedule: TimerSchedule,
    pub created_at: SystemTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TimerSchedule {
    /// Fire once at a specific time
    OneShot { at: SystemTime },
    /// Fire repeatedly at an interval
    Recurring {
        every: Duration,
        next_fire: SystemTime,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TimerManager {
    timers: Vec<Timer>,
}

impl TimerManager {
    pub fn new() -> Self {
        Self {
            timers: Vec::new(),
        }
    }

    pub fn add(&mut self, timer: Timer) {
        self.timers.push(timer);
    }

    pub fn cancel(&mut self, id: &AgentId) -> Option<Timer> {
        if let Some(pos) = self.timers.iter().position(|t| &t.id == id) {
            Some(self.timers.remove(pos))
        } else {
            None
        }
    }

    /// Check for timers that should fire at or before `now`.
    /// Returns fired timer events and advances recurring timers.
    pub fn check_and_fire(
        &mut self,
        now: SystemTime,
        id_gen: &mut IdGenerator,
    ) -> Vec<WorkItem> {
        let mut fired = Vec::new();
        let mut to_remove = Vec::new();

        for timer in &mut self.timers {
            match &mut timer.schedule {
                TimerSchedule::OneShot { at } => {
                    if now >= *at {
                        fired.push(WorkItem {
                            id: id_gen.next(),
                            priority: timer.priority,
                            time: now,
                            item_type: WorkItemType::TimerFired {
                                timer_id: timer.id.clone(),
                                every: None,
                                description: timer.description.clone(),
                            },
                        });
                        to_remove.push(timer.id.clone());
                    }
                }
                TimerSchedule::Recurring { every, next_fire } => {
                    if now >= *next_fire {
                        fired.push(WorkItem {
                            id: id_gen.next(),
                            priority: timer.priority,
                            time: now,
                            item_type: WorkItemType::TimerFired {
                                timer_id: timer.id.clone(),
                                every: Some(*every),
                                description: timer.description.clone(),
                            },
                        });
                        *next_fire = now + *every;
                    }
                }
            }
        }

        for id in &to_remove {
            self.cancel(id);
        }

        fired
    }

    pub fn list(&self) -> &[Timer] {
        &self.timers
    }
}

// ---- Process Management ----
//
// Processes run asynchronously. The agent's shell_exec() call returns
// immediately with a hex ID string. The agent cannot block on or interact
// with the process within the same script — results arrive as work queue
// items (ProcessCompleted, ProcessFailed, ProcessTimeout).

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProcessStatus {
    Running,
    Completed { exit_code: i32 },
    Failed { error: String },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ManagedProcess {
    pub id: AgentId,
    pub cmd: String,
    pub args: Vec<String>,
    pub env: HashMap<String, String>,
    pub status: ProcessStatus,
    pub alert_timer: Duration,
    pub success_prio: u8,
    pub fail_prio: u8,
    pub started_at: SystemTime,
    /// OS-level PID for the actual process
    pub os_pid: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProcessManager {
    processes: Vec<ManagedProcess>,
}

impl ProcessManager {
    pub fn new() -> Self {
        Self {
            processes: Vec::new(),
        }
    }

    pub fn add(&mut self, process: ManagedProcess) {
        self.processes.push(process);
    }

    pub fn get(&self, id: &AgentId) -> Option<&ManagedProcess> {
        self.processes.iter().find(|p| &p.id == id)
    }

    pub fn get_mut(&mut self, id: &AgentId) -> Option<&mut ManagedProcess> {
        self.processes.iter_mut().find(|p| &p.id == id)
    }

    /// Check for processes that have exceeded their alert_timer.
    pub fn check_timeouts(
        &self,
        now: SystemTime,
        id_gen: &mut IdGenerator,
    ) -> Vec<WorkItem> {
        let mut alerts = Vec::new();

        for process in &self.processes {
            if matches!(process.status, ProcessStatus::Running) {
                if let Ok(elapsed) = now.duration_since(process.started_at) {
                    if elapsed >= process.alert_timer {
                        alerts.push(WorkItem {
                            id: id_gen.next(),
                            priority: process.fail_prio,
                            time: now,
                            item_type: WorkItemType::ProcessTimeout {
                                pid: process.id.clone(),
                            },
                        });
                    }
                }
            }
        }

        alerts
    }
}

// ---- Memory ----

/// Simple persistent key-value store.
pub type Memory = HashMap<String, String>;

// ---- Harness State ----

/// The complete state of the harness, minus deployment-specific config.
/// Persisted to SQLite so the daemon can restart and pick up where it left off.
#[derive(Debug, Serialize, Deserialize)]
pub struct HarnessState {
    pub work_queue: WorkQueue,
    pub event_history: EventHistory,
    pub timer_manager: TimerManager,
    pub process_manager: ProcessManager,
    pub memory: Memory,
    pub id_generator: IdGenerator,

    /// Last input_tokens from API response, for compaction decisions
    pub last_input_tokens: u64,
    /// Context window size for the current model
    pub context_window: u64,
    /// max_tokens setting for API calls
    pub max_tokens: u64,
}

impl HarnessState {
    pub fn new(context_window: u64, max_tokens: u64) -> Self {
        Self {
            work_queue: WorkQueue::new(),
            event_history: EventHistory::new(),
            timer_manager: TimerManager::new(),
            process_manager: ProcessManager::new(),
            memory: HashMap::new(),
            id_generator: IdGenerator::new(),
            last_input_tokens: 0,
            context_window,
            max_tokens,
        }
    }

    /// Whether compaction should be triggered.
    /// True when input_tokens > 80% of (context_window - max_tokens).
    pub fn should_compact(&self) -> bool {
        let available = self.context_window.saturating_sub(self.max_tokens);
        let threshold = (available as f64 * 0.8) as u64;
        self.last_input_tokens > threshold
    }
}

// ---- Context Rendering ----

/// Configuration for how context is rendered into the user message.
#[derive(Debug, Clone)]
pub struct RenderConfig {
    /// Max chars per history entry before truncation
    pub history_entry_max_chars: usize,
    /// Max lines per history entry before truncation
    pub history_entry_max_lines: usize,

    /// Max chars for work queue item content, by position
    pub work_queue_content_limits: Vec<usize>, // [500, 500, 500, 200, 200, ...]
    /// Default limit for items beyond the explicit list
    pub work_queue_default_limit: usize,       // 80
    /// Max number of work items to display
    pub work_queue_max_display: usize,         // 20
}

impl Default for RenderConfig {
    fn default() -> Self {
        Self {
            history_entry_max_chars: 2000,
            history_entry_max_lines: 50,
            work_queue_content_limits: vec![500, 500, 500, 200, 200, 200, 200, 200, 200, 200],
            work_queue_default_limit: 80,
            work_queue_max_display: 20,
        }
    }
}

// ---- Deployment Plugin ----

/// Trait for deployment-specific behavior.
/// Each deployment (home automation, DevOps, etc.) implements this.
pub trait DeploymentPlugin: Send + Sync {
    /// The deployment context text shown to the agent each turn.
    fn deployment_context(&self) -> String;

    /// Python preamble: source code defining deployment-specific objects
    /// (e.g. `camera_tool`, `home`, etc.) that are available in the interpreter.
    fn python_preamble(&self) -> String;

    /// Handle a deployment-specific command from the Python interpreter.
    /// Called synchronously during script execution for read operations
    /// (e.g. camera_tool.get_interesting_frames).
    fn handle_call(&self, function: &str, args: &serde_json::Value) -> Result<serde_json::Value, String>;
}

// ---- API Types ----

/// The API request we send to Claude.
#[derive(Debug, Serialize)]
pub struct ApiRequest {
    pub model: String,
    pub max_tokens: u64,
    pub system: Vec<SystemBlock>,
    pub tools: Vec<ToolDefinition>,
    pub messages: Vec<Message>,
}

#[derive(Debug, Serialize)]
pub struct SystemBlock {
    #[serde(rename = "type")]
    pub block_type: String, // always "text"
    pub text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_control: Option<CacheControl>,
}

#[derive(Debug, Serialize)]
pub struct CacheControl {
    #[serde(rename = "type")]
    pub control_type: String, // always "ephemeral"
}

#[derive(Debug, Serialize)]
pub struct ToolDefinition {
    pub name: String,
    pub description: String,
    pub input_schema: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Message {
    pub role: String,
    pub content: MessageContent,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MessageContent {
    Text(String),
    Blocks(Vec<ContentBlock>),
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ContentBlock {
    #[serde(rename = "text")]
    Text { text: String },
    #[serde(rename = "tool_use")]
    ToolUse {
        id: String,
        name: String,
        input: serde_json::Value,
    },
    #[serde(rename = "tool_result")]
    ToolResult {
        tool_use_id: String,
        content: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        is_error: Option<bool>,
    },
    #[serde(rename = "image")]
    Image {
        source: ImageSource,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ImageSource {
    #[serde(rename = "type")]
    pub source_type: String, // "base64"
    pub media_type: String,  // "image/png", "image/jpeg"
    pub data: String,        // base64-encoded
}

/// The API response from Claude.
#[derive(Debug, Deserialize)]
pub struct ApiResponse {
    pub content: Vec<ContentBlock>,
    pub stop_reason: String,
    pub usage: Usage,
}

#[derive(Debug, Deserialize)]
pub struct Usage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    #[serde(default)]
    pub cache_creation_input_tokens: u64,
    #[serde(default)]
    pub cache_read_input_tokens: u64,
}
