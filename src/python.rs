use std::collections::{HashMap, HashSet};
use std::ffi::CString;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pyo3::exceptions::PyAttributeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::types::*;

// ---- Helpers ----

/// Extract a duration in seconds from either a number or a datetime.timedelta.
fn extract_seconds(val: &Bound<'_, PyAny>) -> PyResult<f64> {
    if let Ok(f) = val.extract::<f64>() {
        return Ok(f);
    }
    if let Ok(ts) = val.call_method0("total_seconds") {
        return ts.extract::<f64>();
    }
    Err(pyo3::exceptions::PyTypeError::new_err(
        "expected a number (seconds) or datetime.timedelta",
    ))
}

/// Build a Timer from schedule parameters. Shared between the main executor
/// (PyTimerManager.add inlines this) and any other callers.
fn build_timer_schedule(every_secs: Option<u64>, at_epoch: Option<f64>) -> TimerSchedule {
    if let Some(secs) = every_secs {
        TimerSchedule::Recurring {
            every: Duration::from_secs(secs),
            next_fire: chrono::Utc::now()
                + chrono::Duration::from_std(Duration::from_secs(secs))
                    .unwrap_or(chrono::Duration::seconds(1)),
        }
    } else if let Some(epoch) = at_epoch {
        TimerSchedule::OneShot {
            at: chrono::DateTime::from_timestamp(epoch as i64, 0)
                .unwrap_or_else(|| chrono::Utc::now() + chrono::Duration::minutes(1)),
        }
    } else {
        TimerSchedule::OneShot { at: chrono::Utc::now() + chrono::Duration::minutes(1) }
    }
}

// ---- External Effects ----
//
// Things that can't be rolled back by dropping a cloned state: OS process
// spawns, broadcast messages, child-agent forks, SQLite writes. These are
// collected during execution and applied by the core loop only if the script
// succeeds.
//
// In-state mutations (memory, work_queue, timers, hooks, history, process
// bookkeeping) go directly through Mutex<T> on the pyclass instances — the
// whole state is cloned per turn, mutated in place, and committed on success.

#[derive(Debug, Default)]
pub struct ExternalEffects {
    pub messages: Vec<OutboundMessageRequest>,
    pub process_starts: Vec<ProcessStartRequest>,
    pub process_kills: Vec<String>,
    /// (pid, bytes) pairs to write to interactive process stdin.
    pub stdin_writes: Vec<(String, Vec<u8>)>,
    /// pids whose stdin should be closed (EOF).
    pub stdin_closes: Vec<String>,
    /// Child agent names to terminate via HarnessEvent::Shutdown.
    pub child_kills: Vec<String>,
    /// Paths collected by view() calls this turn. Applied as a single View
    /// work item so multiple view() calls don't spam the queue.
    pub view_paths: Vec<String>,
    pub fork_requests: Vec<ForkRequest>,
    pub agent_messages: Vec<AgentMessageRequest>,
    pub compaction_script_appends: Vec<String>,
    pub compact_called: bool,
    pub compaction_requested: bool,
    pub done_called: bool,
    pub done_result: HashMap<String, serde_json::Value>,
    /// (key, content) — written to pinned_memory table, cached in system prompt
    pub memory_pins: Vec<(String, String)>,
    /// keys to remove from pinned storage
    pub memory_unpins: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ForkChildSettings {
    pub name: String,
    pub task: String,
    pub model: Option<String>,  // None = inherit parent's model
    /// None → persistent child: idle-waits like root, no turn limit.
    /// Parent must kill_child() to stop it.
    pub max_turns: Option<u32>,
    pub can_compact: bool,
    /// If false, child starts with fresh history containing only a fork
    /// SystemAlert. Useful for cross-model forks where inheriting the parent's
    /// full context means paying a full re-ingest on the new model's cache.
    pub inherit_history: bool,
    /// File paths to push as a View work item on the child's queue.
    pub attach: Vec<String>,
    /// Stable text rendered between deployment_context and event_history.
    /// Sits in the cached prefix — byte-identical across repeated forks of
    /// the same role, so the cache hits even when task/attach vary.
    pub prefix_context: Option<String>,
    /// Attachments rendered as content blocks in the cached prefix (before
    /// event_history). For reference images that don't change between forks.
    pub prefix_attach: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ForkRequest {
    pub children: Vec<ForkChildSettings>,
}

#[derive(Debug, Clone)]
pub struct AgentMessageRequest {
    pub recipient: String,
    pub content: String,
    pub priority: u8,
}

#[derive(Debug)]
pub struct OutboundMessageRequest {
    pub chat_id: String,
    pub content: String,
    pub attachments: Vec<String>,
    /// If set, bridge sends a reaction (content is the emoji) to the
    /// referenced message instead of a regular message.
    pub react_to: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ProcessStartRequest {
    pub id: AgentId,
    pub cmd: String,
    pub args: Vec<String>,
    pub env: HashMap<String, String>,
    pub description: String,
    pub alert_timer_secs: u64,
    /// Used for the spawn-failure work item. success_prio lives only on the
    /// ManagedProcess bookkeeping entry (added same-turn in shell_exec);
    /// spawn() itself doesn't need it.
    pub fail_prio: u8,
    pub block_for_ms: Option<u64>,
    /// Keep stdin open for shell_input(). When false, stdin is /dev/null.
    pub interactive: bool,
}

// ---- Execution Result ----

pub struct ExecutionResult {
    pub stdout: String,
    pub is_error: bool,
    pub error_text: String,
    pub effects: ExternalEffects,
    /// The mutated state clone, reassembled from pyclass instances.
    /// None if the script errored — drop it, nothing to commit.
    pub committed_state: Option<HarnessState>,
}

// ---- Hook Result ----

/// What a hook commits back. Hooks get a narrower txn than the main turn:
/// only memory + id_generator are cloned and mutated in place. Timers, hooks,
/// work_queue, history are not exposed to hook scripts (the wrapped template
/// only injects memory/shell_exec/send_message).
pub struct HookCommit {
    pub memory: Memory,
    pub memory_priorities: HashMap<String, u8>,
    pub id_generator: IdGenerator,
    pub messages: Vec<OutboundMessageRequest>,
    pub process_starts: Vec<ProcessStartRequest>,
}

// ---- #[pyclass] Types ----

type Effects = Arc<Mutex<ExternalEffects>>;

#[pyclass(from_py_object)]
#[derive(Clone)]
struct PyWorkItem {
    id: String,
    priority: u8,
    time: String,
    item_type: String,
    /// Variant-specific fields, keyed by exact Rust field names.
    /// Exposed via __getattr__: `item.chat_id`, `item.result`, etc.
    fields: serde_json::Map<String, serde_json::Value>,
}

#[pymethods]
impl PyWorkItem {
    #[getter]
    fn id(&self) -> &str {
        &self.id
    }
    #[getter]
    fn priority(&self) -> u8 {
        self.priority
    }
    #[getter]
    fn time(&self) -> &str {
        &self.time
    }

    fn __getattr__<'py>(&self, py: Python<'py>, name: &str) -> PyResult<Py<PyAny>> {
        if name == "type" {
            return Ok(self.item_type.as_str().into_pyobject(py)?.into_any().unbind());
        }
        match self.fields.get(name) {
            Some(val) => json_to_py(py, val),
            None => Err(PyAttributeError::new_err(format!(
                "{} work item has no field '{}'. Available fields: {}",
                self.item_type,
                name,
                self.fields.keys().cloned().collect::<Vec<_>>().join(", ")
            ))),
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "WorkItem(id='{}', type='{}', priority={})",
            self.id, self.item_type, self.priority
        )
    }
}

/// Convert a serde_json::Value to a Python object via json.loads.
/// Used for work item field access and memory retrieval.
fn json_to_py(py: Python<'_>, value: &serde_json::Value) -> PyResult<Py<PyAny>> {
    let json_str = serde_json::to_string(value).map_err(|e| {
        pyo3::exceptions::PyValueError::new_err(format!("json serialize error: {}", e))
    })?;
    let json_mod = py.import("json")?;
    Ok(json_mod.call_method1("loads", (json_str,))?.unbind())
}

/// Serialize each WorkItemType variant into a field map with keys matching
/// the exact Rust field names. This is the single source of truth for what
/// fields are available on each work item type in Python.
fn work_item_to_py(item: &WorkItem) -> PyWorkItem {
    let time_str = item.time.format("%Y-%m-%d %H:%M:%S UTC").to_string();
    let mut fields = serde_json::Map::new();

    let item_type = match &item.item_type {
        WorkItemType::UserMessage { chat_id, user, content, message_ref } => {
            if let Some(r) = message_ref {
                fields.insert("message_ref".into(), r.clone().into());
            }
            fields.insert("chat_id".into(), chat_id.clone().into());
            fields.insert("user".into(), user.clone().into());
            fields.insert("content".into(), content.clone().into());
            "UserMessage"
        }
        WorkItemType::TimerFired { timer_id, every, description } => {
            fields.insert("timer_id".into(), timer_id.0.clone().into());
            fields.insert("every".into(), match every {
                Some(d) => serde_json::Value::from(d.as_secs()),
                None => serde_json::Value::Null,
            });
            fields.insert("description".into(), description.clone().into());
            "TimerFired"
        }
        WorkItemType::ProcessCompleted { pid, exit_code, output_preview, description } => {
            fields.insert("pid".into(), pid.0.clone().into());
            fields.insert("exit_code".into(), (*exit_code).into());
            fields.insert("output_preview".into(), match output_preview {
                Some(s) => s.clone().into(),
                None => serde_json::Value::Null,
            });
            fields.insert("description".into(), description.clone().into());
            "ProcessCompleted"
        }
        WorkItemType::ProcessFailed { pid, error, output_preview, description } => {
            fields.insert("pid".into(), pid.0.clone().into());
            fields.insert("error".into(), error.clone().into());
            fields.insert("output_preview".into(), match output_preview {
                Some(s) => s.clone().into(),
                None => serde_json::Value::Null,
            });
            fields.insert("description".into(), description.clone().into());
            "ProcessFailed"
        }
        WorkItemType::ProcessTimeout { pid } => {
            fields.insert("pid".into(), pid.0.clone().into());
            "ProcessTimeout"
        }
        WorkItemType::ChildAgentCompleted { child_name, result, turns_used, success, summary, cost_usd, cache_hit_pct } => {
            fields.insert("child_name".into(), child_name.clone().into());
            fields.insert("result".into(), serde_json::to_value(result).unwrap_or_default());
            fields.insert("turns_used".into(), (*turns_used).into());
            fields.insert("success".into(), (*success).into());
            fields.insert("summary".into(), summary.clone().into());
            fields.insert("cost_usd".into(), serde_json::json!(cost_usd));
            fields.insert("cache_hit_pct".into(), (*cache_hit_pct).into());
            "ChildAgentCompleted"
        }
        WorkItemType::AgentMessage { from, content } => {
            fields.insert("from_agent".into(), from.clone().into());
            fields.insert("content".into(), content.clone().into());
            "AgentMessage"
        }
        WorkItemType::ExternalEvent { source, event_type, data } => {
            fields.insert("source".into(), source.clone().into());
            fields.insert("event_type".into(), event_type.clone().into());
            fields.insert("data".into(), data.clone());
            "ExternalEvent"
        }
        WorkItemType::View { paths } => {
            fields.insert("paths".into(), serde_json::Value::from(paths.clone()));
            "View"
        }
        WorkItemType::Compaction => {
            fields.insert("description".into(), "You must compact your context.".into());
            "Compaction"
        }
        WorkItemType::AgentStartup { changelog } => {
            if let Some(c) = changelog {
                fields.insert("changelog".into(), c.clone().into());
            }
            fields.insert(
                "description".into(),
                "Harness restarted. Any processes/bridges you were managing are dead — inspect memory and reconnect as needed.".into(),
            );
            "AgentStartup"
        }
        WorkItemType::HookException { hook_name, error, original } => {
            fields.insert("hook_name".into(), hook_name.clone().into());
            fields.insert("error".into(), error.clone().into());
            fields.insert("original".into(), original.clone());
            "HookException"
        }
    };

    fields.insert("attachments".into(), item.attachments.clone().into());

    PyWorkItem {
        id: item.id.0.clone(),
        priority: item.priority,
        time: time_str,
        item_type: item_type.to_string(),
        fields,
    }
}

#[pyclass]
struct PyWorkQueue {
    inner: Mutex<WorkQueue>,
}

#[pymethods]
impl PyWorkQueue {
    fn __getitem__(&self, index: usize) -> PyResult<PyWorkItem> {
        let wq = self.inner.lock().unwrap();
        wq.items()
            .get(index)
            .map(work_item_to_py)
            .ok_or_else(|| pyo3::exceptions::PyIndexError::new_err("work queue index out of range"))
    }

    fn __len__(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    fn pop_front(&self) -> PyResult<Option<PyWorkItem>> {
        let mut wq = self.inner.lock().unwrap();
        if wq.is_empty() {
            return Ok(None);
        }
        let front_id = wq.items()[0].id.clone();
        let item = wq.remove(&front_id).unwrap();
        Ok(Some(work_item_to_py(&item)))
    }

    fn remove(&self, id: String) -> PyResult<()> {
        self.inner.lock().unwrap().remove(&AgentId(id));
        Ok(())
    }
}

/// Bundled local-memory state. One mutex — the methods that touch data also
/// touch priorities (set/setitem), so separate locks would just invite
/// lock-ordering bugs.
#[derive(Default)]
struct MemoryInner {
    data: HashMap<String, serde_json::Value>,
    priorities: HashMap<String, u8>,
    sensitive: HashSet<String>,
}

#[pyclass]
struct PyMemory {
    inner: Mutex<MemoryInner>,
    /// Pinned entries: shared across all agents, injected into the cached
    /// system prompt. Stored separately in SQLite (pinned_memory table).
    /// Read-through: get() checks local `inner.data` first, then falls back here.
    /// Read-only during the turn — pin()/unpin() go to ExternalEffects since
    /// they're SQLite writes shared across agents.
    pinned: HashMap<String, String>,
    effects: Effects,
}

impl PyMemory {
    /// Convert a serde_json::Value back to a Python object via json.loads
    fn value_to_py<'py>(py: Python<'py>, value: &serde_json::Value) -> PyResult<Py<PyAny>> {
        let json_str = serde_json::to_string(value).unwrap();
        let json_mod = py.import("json")?;
        Ok(json_mod.call_method1("loads", (json_str,))?.unbind())
    }

    /// Convert a Python object to serde_json::Value via json.dumps
    fn py_to_value<'py>(py: Python<'py>, value: &Bound<'py, PyAny>) -> PyResult<serde_json::Value> {
        let json_mod = py.import("json")?;
        let json_str: String = json_mod.call_method1("dumps", (value,))?.extract()?;
        serde_json::from_str(&json_str)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("cannot serialize: {}", e)))
    }
}

#[pymethods]
impl PyMemory {
    fn __getitem__<'py>(&self, py: Python<'py>, key: &str) -> PyResult<Py<PyAny>> {
        let inner = self.inner.lock().unwrap();
        let value = inner.data.get(key)
            .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err(key.to_string()))?;
        Self::value_to_py(py, value)
    }

    fn __setitem__<'py>(&self, py: Python<'py>, key: String, value: &Bound<'py, PyAny>) -> PyResult<()> {
        let serde_val = Self::py_to_value(py, value)?;
        let mut inner = self.inner.lock().unwrap();
        // Assign default priority 5 only for new keys (don't override existing)
        if !inner.priorities.contains_key(&key) {
            inner.priorities.insert(key.clone(), 5);
        }
        inner.data.insert(key, serde_val);
        Ok(())
    }

    fn __delitem__(&self, key: &str) -> PyResult<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.data.remove(key);
        inner.priorities.remove(key);
        Ok(())
    }

    fn __contains__(&self, key: &str) -> bool {
        self.inner.lock().unwrap().data.contains_key(key) || self.pinned.contains_key(key)
    }

    #[pyo3(signature = (key, default=None))]
    fn get<'py>(&self, py: Python<'py>, key: &str, default: Option<Py<PyAny>>) -> PyResult<Py<PyAny>> {
        {
            let inner = self.inner.lock().unwrap();
            if let Some(value) = inner.data.get(key) {
                return Self::value_to_py(py, value);
            }
        }
        if let Some(s) = self.pinned.get(key) {
            return Ok(s.as_str().into_pyobject(py)?.into_any().unbind());
        }
        Ok(default.unwrap_or_else(|| py.None()))
    }

    #[pyo3(signature = (key, value, priority=5))]
    fn set<'py>(&self, py: Python<'py>, key: String, value: &Bound<'py, PyAny>, priority: u8) -> PyResult<()> {
        let serde_val = Self::py_to_value(py, value)?;
        let mut inner = self.inner.lock().unwrap();
        inner.data.insert(key.clone(), serde_val);
        inner.priorities.insert(key, priority);
        Ok(())
    }

    fn set_priority(&self, key: String, priority: u8) -> PyResult<()> {
        self.inner.lock().unwrap().priorities.insert(key, priority);
        Ok(())
    }

    fn get_priority(&self, key: &str) -> u8 {
        self.inner.lock().unwrap().priorities.get(key).copied().unwrap_or(5)
    }

    /// Pin a key–value pair into the shared, cached tier. Pinned entries:
    /// - are injected into the system prompt (prompt-cached, cheap to keep)
    /// - are shared across all agents (parent, children, future sessions)
    /// - must be strings (they render as markdown in the system prompt)
    /// Use for stable facts: API endpoints, learned recipes, user prefs.
    fn pin(&self, key: String, value: String) -> PyResult<()> {
        self.effects.lock().unwrap().memory_pins.push((key, value));
        Ok(())
    }

    /// Remove a key from the pinned tier.
    fn unpin(&self, key: String) -> PyResult<()> {
        self.effects.lock().unwrap().memory_unpins.push(key);
        Ok(())
    }

    /// List pinned keys.
    fn list_pinned(&self) -> Vec<String> {
        let mut keys: Vec<String> = self.pinned.keys().cloned().collect();
        keys.sort();
        keys
    }

    /// Mark a key's value as sensitive: it will be redacted from the API
    /// trace ring buffer (and thus from `feedback --with-api-trace` uploads).
    /// You still see the real value in your live context — only the stored
    /// trace is scrubbed.
    fn mark_sensitive(&self, key: String) -> PyResult<()> {
        self.inner.lock().unwrap().sensitive.insert(key);
        Ok(())
    }

    fn unmark_sensitive(&self, key: String) -> PyResult<()> {
        self.inner.lock().unwrap().sensitive.remove(&key);
        Ok(())
    }

    fn __repr__(&self) -> String {
        let inner = self.inner.lock().unwrap();
        format!("Memory({} keys, {} pinned)", inner.data.len(), self.pinned.len())
    }
}

#[pyclass]
struct PyTimerManager {
    inner: Arc<Mutex<TimerManager>>,
    id_gen: Arc<Mutex<IdGenerator>>,
}

#[pymethods]
impl PyTimerManager {
    #[pyo3(signature = (*, every=None, at=None, priority=5, description="".to_string()))]
    fn add<'py>(
        &self,
        every: Option<&Bound<'py, PyAny>>,
        at: Option<&Bound<'py, PyAny>>,
        priority: u8,
        description: String,
    ) -> PyResult<String> {
        let every_secs = match every {
            Some(val) => Some(extract_seconds(val)? as u64),
            None => None,
        };
        // Extract epoch seconds from datetime objects or numeric timestamps
        let at_epoch = match at {
            Some(val) => {
                if let Ok(ts) = val.call_method0("timestamp") {
                    Some(ts.extract::<f64>()?)
                } else if let Ok(f) = val.extract::<f64>() {
                    Some(f)
                } else {
                    return Err(pyo3::exceptions::PyTypeError::new_err(
                        "expected a datetime object or numeric epoch for 'at'",
                    ));
                }
            }
            None => None,
        };
        let id = self.id_gen.lock().unwrap().next();
        let id_str = id.0.clone();
        self.inner.lock().unwrap().add(Timer {
            id,
            description,
            priority,
            schedule: build_timer_schedule(every_secs, at_epoch),
            created_at: chrono::Utc::now(),
            pending_ack: false,
        });
        Ok(id_str)
    }

    fn cancel(&self, timer_id: String) -> PyResult<()> {
        self.inner.lock().unwrap().cancel(&AgentId(timer_id));
        Ok(())
    }

    fn list(&self) -> Vec<(String, String, u8)> {
        self.inner.lock().unwrap()
            .list()
            .iter()
            .map(|t| (t.id.0.clone(), t.description.clone(), t.priority))
            .collect()
    }
}

#[pyclass(from_py_object)]
#[derive(Clone)]
struct PyHistoryEntry {
    code: String,
    output: String,
    full_output: String,
    time: String,
}

#[pymethods]
impl PyHistoryEntry {
    #[getter]
    fn code(&self) -> &str {
        &self.code
    }
    #[getter]
    fn output(&self) -> &str {
        &self.output
    }
    #[getter]
    fn full_output(&self) -> &str {
        &self.full_output
    }
    #[getter]
    fn time(&self) -> &str {
        &self.time
    }
}

fn history_entry_to_py(entry: &HistoryEntry) -> PyHistoryEntry {
    let (code_str, output_str, time_str) = match entry {
        HistoryEntry::Execution { code, output, time, .. } => (
            code.clone(),
            output.clone(),
            time.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
        ),
        HistoryEntry::Summary { description, time, .. } => (
            String::new(),
            description.clone(),
            time.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
        ),
        HistoryEntry::SystemAlert { message, time, .. } => (
            String::new(),
            message.clone(),
            time.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
        ),
    };
    PyHistoryEntry {
        code: code_str,
        output: output_str.clone(),
        full_output: output_str,
        time: time_str,
    }
}

#[pyclass]
struct PyHistoryManager {
    inner: Mutex<EventHistory>,
    id_gen: Arc<Mutex<IdGenerator>>,
    is_compaction: bool,
}

#[pymethods]
impl PyHistoryManager {
    fn __getitem__(&self, id: &str) -> PyResult<PyHistoryEntry> {
        let hist = self.inner.lock().unwrap();
        hist.entries()
            .iter()
            .find(|e| e.id().0 == id)
            .map(history_entry_to_py)
            .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err(format!("No history entry with id {}", id)))
    }

    fn replace_with_description(&self, id: String, description: String) -> PyResult<()> {
        let aid = AgentId(id);
        let mut hist = self.inner.lock().unwrap();
        if self.is_compaction || hist.is_modifiable(&aid) {
            hist.replace_with_summary(&aid, description);
        }
        Ok(())
    }

    fn remove(&self, id: String) -> PyResult<()> {
        let aid = AgentId(id);
        let mut hist = self.inner.lock().unwrap();
        if self.is_compaction || hist.is_modifiable(&aid) {
            hist.remove(&aid);
        }
        Ok(())
    }

    fn add(&self, text: String) -> PyResult<()> {
        if !self.is_compaction {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "history.add() can only be used during compaction",
            ));
        }
        let id = self.id_gen.lock().unwrap().next();
        self.inner.lock().unwrap().push(HistoryEntry::Summary {
            id,
            time: chrono::Utc::now(),
            description: text,
        });
        Ok(())
    }
}

#[pyclass]
struct PyHarness {
    effects: Effects,
    hooks: Mutex<Vec<Hook>>,
    process_manager: Mutex<ProcessManager>,
    id_gen: Arc<Mutex<IdGenerator>>,
    /// Shared with PyTimerManager so acknowledge_timer() can mutate in place.
    timer_manager: Arc<Mutex<TimerManager>>,
    process_outputs: HashMap<String, String>,
    child_depth_remaining: u32,
    agent_name: String,
    agent_lineage: String,
    harness_bin: String,
    /// None in test/compaction-estimate contexts where there's no HTTP server.
    /// When None, send_message skips the routability check and
    /// wait_for_message_channel raises.
    subscribers: Option<Arc<crate::http_server::SubscriberRegistry>>,
    /// For block_on inside wait_for_message_channel. Python runs on a
    /// dedicated std::thread, so we're outside tokio — need the handle.
    tokio_handle: Option<tokio::runtime::Handle>,
    /// When true, methods that block on external results or manipulate
    /// agent lifecycle raise. shell_exec is allowed but block_for is not.
    hook_mode: bool,
}

#[pymethods]
impl PyHarness {
    #[pyo3(signature = (chat_id, content, attach=vec![], react_to=None))]
    fn send_message(&self, chat_id: String, content: String, attach: Vec<String>, react_to: Option<String>) -> PyResult<()> {
        // Fail fast if no subscriber exists for this chat_id. The broadcast
        // channel is fire-and-forget — without this check, messages to typo'd
        // or not-yet-connected chat_ids vanish silently (feedback #30).
        if let Some(subs) = &self.subscribers {
            if !subs.would_reach(&chat_id) {
                return Err(pyo3::exceptions::PyRuntimeError::new_err(
                    format!(
                        "send_message: no subscriber for chat_id '{}'. \
                         If you just spawned the bridge, call \
                         wait_for_message_channel('{}', timeout_ms=...) first.",
                        chat_id, chat_id
                    ),
                ));
            }
        }
        for p in &attach {
            if !std::path::Path::new(p).is_file() {
                return Err(pyo3::exceptions::PyFileNotFoundError::new_err(
                    format!("send_message: attachment not found: {}", p),
                ));
            }
        }
        self.effects
            .lock()
            .unwrap()
            .messages
            .push(OutboundMessageRequest { chat_id, content, attachments: attach, react_to });
        Ok(())
    }

    /// Block until a subscriber for `chat_id` is connected, or raise on
    /// timeout. Use after spawning a bridge and before the first send_message
    /// to it — closes the startup race where the bridge hasn't finished its
    /// SSE handshake yet. timeout_ms eats into the Python execution budget;
    /// keep it well under CLAUDE_SERVER_PYTHON_TIMEOUT (default 5000ms).
    #[pyo3(signature = (chat_id, timeout_ms=3000))]
    fn wait_for_message_channel(&self, chat_id: String, timeout_ms: u64) -> PyResult<()> {
        if self.hook_mode {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "wait_for_message_channel: hooks cannot block on external state",
            ));
        }
        let (subs, handle) = match (&self.subscribers, &self.tokio_handle) {
            (Some(s), Some(h)) => (s, h),
            _ => return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "wait_for_message_channel: not available in this context (no HTTP server)",
            )),
        };
        let dur = std::time::Duration::from_millis(timeout_ms);
        let reached = handle.block_on(subs.wait_for(&chat_id, dur));
        if reached {
            Ok(())
        } else {
            Err(pyo3::exceptions::PyTimeoutError::new_err(
                format!("no subscriber for '{}' after {}ms", chat_id, timeout_ms),
            ))
        }
    }

    #[pyo3(signature = (cmd, args=vec![], env=HashMap::new(), description="".to_string(), alert_timer=None, success_prio=7, fail_prio=8, block_for=None, interactive=false))]
    fn shell_exec<'py>(
        &self,
        cmd: String,
        args: Vec<String>,
        env: HashMap<String, String>,
        description: String,
        alert_timer: Option<&Bound<'py, PyAny>>,
        success_prio: u8,
        fail_prio: u8,
        block_for: Option<&Bound<'py, PyAny>>,
        interactive: bool,
    ) -> PyResult<String> {
        let alert_secs = match alert_timer {
            Some(val) => extract_seconds(val)? as u64,
            None => 300,
        };
        if self.hook_mode && block_for.is_some() {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "shell_exec(block_for=...) not available in hooks — spawn \
                 fire-and-forget, then register a second hook matching the \
                 ProcessCompleted event to handle the result",
            ));
        }
        let block_for_ms = match block_for {
            Some(val) => Some((extract_seconds(val)? * 1000.0) as u64),
            None => None,
        };
        let id = self.id_gen.lock().unwrap().next();
        let id_str = id.0.clone();
        // Add the bookkeeping entry NOW so processes_list() shows it same-turn.
        // The actual OS spawn is deferred to ExternalEffects — it can fail, it
        // has real-world side effects, and it can't be rolled back by dropping
        // a cloned state.
        self.process_manager.lock().unwrap().add(ManagedProcess {
            id: id.clone(),
            cmd: cmd.clone(),
            args: args.clone(),
            env: env.clone(),
            description: description.clone(),
            status: ProcessStatus::Running,
            success_prio,
            fail_prio,
            started_at: chrono::Utc::now(),
            os_pid: None,
        });
        self.effects.lock().unwrap().process_starts.push(ProcessStartRequest {
            id,
            cmd,
            args,
            env,
            description,
            alert_timer_secs: alert_secs,
            fail_prio,
            block_for_ms,
            interactive,
        });
        Ok(id_str)
    }

    fn shell_input(&self, pid: String, data: String) -> PyResult<()> {
        self.effects.lock().unwrap().stdin_writes.push((pid, data.into_bytes()));
        Ok(())
    }

    fn shell_close_stdin(&self, pid: String) -> PyResult<()> {
        self.effects.lock().unwrap().stdin_closes.push(pid);
        Ok(())
    }

    fn kill_child(&self, name: String) -> PyResult<()> {
        self.effects.lock().unwrap().child_kills.push(name);
        Ok(())
    }

    fn shell_status(&self, pid: String) -> PyResult<String> {
        let pm = self.process_manager.lock().unwrap();
        Ok(pm.get(&AgentId(pid))
            .map(|p| match &p.status {
                ProcessStatus::Running => "running".to_string(),
                ProcessStatus::Completed { .. } => "completed".to_string(),
                ProcessStatus::Failed { .. } => "failed".to_string(),
            })
            .unwrap_or_else(|| "unknown".to_string()))
    }

    #[pyo3(signature = (pid, lines=None))]
    fn shell_output(&self, pid: String, lines: Option<usize>) -> PyResult<String> {
        let full = self
            .process_outputs
            .get(&pid)
            .cloned()
            .unwrap_or_default();
        Ok(match lines {
            Some(n) => {
                let all: Vec<&str> = full.lines().collect();
                all[all.len().saturating_sub(n)..].join("\n")
            }
            None => full,
        })
    }

    fn shell_kill(&self, pid: String) -> PyResult<()> {
        self.effects.lock().unwrap().process_kills.push(pid);
        Ok(())
    }

    fn processes_list(&self) -> Vec<(String, String, String, String)> {
        self.process_manager.lock().unwrap()
            .processes()
            .iter()
            .map(|p| {
                let status = match &p.status {
                    ProcessStatus::Running => "running".to_string(),
                    ProcessStatus::Completed { exit_code } => format!("completed (exit {})", exit_code),
                    ProcessStatus::Failed { error } => format!("failed: {}", error),
                };
                (p.id.0.clone(), p.cmd.clone(), p.description.clone(), status)
            })
            .collect()
    }

    #[pyo3(signature = (*paths))]
    fn view(&self, paths: Vec<String>) -> PyResult<()> {
        for p in &paths {
            if !std::path::Path::new(p).is_file() {
                return Err(pyo3::exceptions::PyFileNotFoundError::new_err(
                    format!("view: file not found or not a regular file: {}", p)
                ));
            }
        }
        self.effects.lock().unwrap().view_paths.extend(paths);
        Ok(())
    }

    fn acknowledge_timer(&self, timer_id: String) -> PyResult<()> {
        self.timer_manager.lock().unwrap().acknowledge(&AgentId(timer_id));
        Ok(())
    }

    fn compact(&self) -> PyResult<()> {
        if self.hook_mode {
            return Err(pyo3::exceptions::PyRuntimeError::new_err("compact: not available in hooks"));
        }
        self.effects.lock().unwrap().compact_called = true;
        Ok(())
    }

    fn request_compaction(&self) -> PyResult<()> {
        if self.hook_mode {
            return Err(pyo3::exceptions::PyRuntimeError::new_err("request_compaction: not available in hooks"));
        }
        self.effects.lock().unwrap().compaction_requested = true;
        Ok(())
    }

    /// Fork child agents. Takes a list of ChildSettings objects.
    #[allow(clippy::too_many_arguments)]
    fn fork<'py>(
        &self,
        _py: Python<'py>,
        children: &Bound<'py, PyAny>,
    ) -> PyResult<Vec<String>> {
        if self.hook_mode {
            return Err(pyo3::exceptions::PyRuntimeError::new_err("fork: not available in hooks"));
        }
        if self.child_depth_remaining == 0 {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Cannot fork sub-agents at this depth",
            ));
        }

        let list = children.cast::<pyo3::types::PyList>()
            .map_err(|_| pyo3::exceptions::PyTypeError::new_err("fork() requires a list of ChildSettings"))?;

        if list.is_empty() {
            return Err(pyo3::exceptions::PyValueError::new_err("fork() requires at least one child"));
        }

        let mut child_settings = Vec::new();
        let mut names = Vec::new();

        for item in list.iter() {
            let name: String = item.getattr("name")?.extract()?;
            if let Err(e) = crate::types::AgentName::new_child(&name) {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    format!("ChildSettings.name invalid: {}", e)
                ));
            }
            let task: String = item.getattr("task")?.extract()?;
            let model_obj = item.getattr("model")?;
            let model: Option<String> = if model_obj.is_none() {
                None
            } else {
                Some(model_obj.extract()?)
            };
            let max_turns_obj = item.getattr("max_turns")?;
            let max_turns: Option<u32> = if max_turns_obj.is_none() {
                None
            } else {
                Some(max_turns_obj.extract::<u32>()?.min(50))
            };
            let can_compact: bool = item.getattr("can_compact")?.extract()?;

            // Extract attach paths (None → empty).
            // File existence is NOT checked here — the parent may be queuing a
            // shell_exec that writes the file before the child's turn 1 runs.
            let attach_obj = item.getattr("attach")?;
            let attach: Vec<String> = if attach_obj.is_none() {
                Vec::new()
            } else {
                attach_obj.extract()?
            };

            let inherit_history: bool = item.getattr("inherit_history")?.extract()?;

            let prefix_context: Option<String> = {
                let obj = item.getattr("prefix_context")?;
                if obj.is_none() { None } else { Some(obj.extract()?) }
            };
            let prefix_attach: Vec<String> = {
                let obj = item.getattr("prefix_attach")?;
                if obj.is_none() { Vec::new() } else { obj.extract()? }
            };

            names.push(name.clone());
            child_settings.push(ForkChildSettings {
                name,
                task,
                model,
                max_turns,
                can_compact,
                inherit_history,
                attach,
                prefix_context,
                prefix_attach,
            });
        }

        self.effects.lock().unwrap().fork_requests.push(ForkRequest {
            children: child_settings,
        });

        Ok(names)
    }

    #[pyo3(signature = (name, content, priority=6))]
    fn message_agent(&self, name: String, content: String, priority: u8) -> PyResult<()> {
        self.effects.lock().unwrap().agent_messages.push(AgentMessageRequest {
            recipient: name,
            content,
            priority,
        });
        Ok(())
    }

    /// Register an event hook. `match_expr` and `process` are Python source
    /// strings — both compile-checked here so a typo fails at registration,
    /// not on the first matching event weeks later. `match_expr` is an
    /// expression with `e` bound to the WorkItem; `process` is a script with
    /// `e` bound, returns None (consume) / e or dict (pass/modify) / raises
    /// (→ HookException wrapping the original).
    #[pyo3(signature = (name, priority, match_expr, process, timeout_ms=3000))]
    fn register_hook<'py>(
        &self, py: Python<'py>,
        name: String, priority: i32, match_expr: String, process: String, timeout_ms: u64,
    ) -> PyResult<()> {
        if self.hook_mode {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "register_hook: not available inside a hook",
            ));
        }
        // Syntax-check both. process is wrapped in `def __hook():` at runtime
        // (so `return` works), so validate the same wrapped form here.
        let builtins = py.import("builtins")?;
        let compile = builtins.getattr("compile")?;
        compile.call1((&match_expr, format!("<hook:{}:match>", name), "eval"))
            .map_err(|e| pyo3::exceptions::PySyntaxError::new_err(
                format!("match_expr: {}", e)
            ))?;
        let wrapped_process = format!(
            "def __hook():\n{}",
            process.lines().map(|l| format!("    {}", l)).collect::<Vec<_>>().join("\n"),
        );
        compile.call1((&wrapped_process, format!("<hook:{}:process>", name), "exec"))
            .map_err(|e| pyo3::exceptions::PySyntaxError::new_err(
                format!("process: {}", e)
            ))?;
        // Mutate in place: replace if name exists, push, sort by priority desc.
        // Sort happens here (rarely, at registration) rather than at every
        // event-match time.
        let mut hooks = self.hooks.lock().unwrap();
        hooks.retain(|h| h.name != name);
        hooks.push(Hook { name, priority, match_expr, process, timeout_ms });
        hooks.sort_by(|a, b| b.priority.cmp(&a.priority));
        Ok(())
    }

    fn remove_hook(&self, name: String) -> PyResult<()> {
        if self.hook_mode {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "remove_hook: not available inside a hook",
            ));
        }
        self.hooks.lock().unwrap().retain(|h| h.name != name);
        Ok(())
    }

    fn list_hooks(&self) -> Vec<(String, i32, String)> {
        // (name, priority, match_expr) — enough to see what's registered
        // without dumping full process scripts into context. Reads the live
        // Mutex, so a hook registered earlier this turn is visible here.
        self.hooks.lock().unwrap()
            .iter()
            .map(|h| (h.name.clone(), h.priority, h.match_expr.clone()))
            .collect()
    }

    #[pyo3(signature = (**result))]
    fn done<'py>(&self, py: Python<'py>, result: Option<&Bound<'py, PyDict>>) -> PyResult<()> {
        if self.hook_mode {
            return Err(pyo3::exceptions::PyRuntimeError::new_err("done: not available in hooks"));
        }
        let mut eff = self.effects.lock().unwrap();
        eff.done_called = true;
        if let Some(dict) = result {
            for (key, val) in dict.iter() {
                let key_str: String = key.extract()?;
                let json_mod = py.import("json")?;
                let json_str: String = json_mod.call_method1("dumps", (val,))?.extract()?;
                let serde_val: serde_json::Value = serde_json::from_str(&json_str)
                    .map_err(|e| pyo3::exceptions::PyValueError::new_err(
                        format!("done() kwargs must be JSON-serializable: {}", e)
                    ))?;
                eff.done_result.insert(key_str, serde_val);
            }
        }
        Ok(())
    }

    #[getter]
    fn agent_name(&self) -> &str {
        &self.agent_name
    }

    #[getter]
    fn agent_lineage(&self) -> &str {
        &self.agent_lineage
    }

    #[getter]
    fn harness_bin(&self) -> &str {
        &self.harness_bin
    }
}

#[pyclass]
struct StdoutCapture {
    buffer: Arc<Mutex<String>>,
}

#[pymethods]
impl StdoutCapture {
    fn write(&self, text: &str) -> PyResult<usize> {
        self.buffer.lock().unwrap().push_str(text);
        Ok(text.len())
    }

    fn flush(&self) -> PyResult<()> {
        Ok(())
    }
}

// ---- Hook executor ----

pub enum HookOutcome {
    /// No hook matched — push the item unchanged.
    NoMatch,
    /// Hook returned None — drop the item.
    Consumed { hook_name: String },
    /// Hook returned e (possibly mutated). Push with the updated priority
    /// and optional hook_note.
    Passed { hook_name: String, priority: u8, hook_note: Option<String> },
    /// Hook's process() raised. Wrap in HookException, preserve original.
    Failed { hook_name: String, error: String },
}

/// Single interpreter pass: eval each hook's match_expr against `e` (a dict
/// built from the WorkItem), call process() for the first True. Hooks are
/// pre-sorted by priority descending. `e` is a plain mutable dict —
/// hooks read `e["type"]`, `e["content"]`, etc., and can set `e["priority"]`
/// or `e["hook_note"]` before returning.
///
/// The hook's process() runs with a hook-mode PyHarness: fire-and-forget
/// side effects OK (shell_exec without block_for, send_message, memory),
/// blocking on external results raises.
pub fn run_hooks(
    hooks: &[Hook],
    item: &WorkItem,
    state: &HarnessState,
    subscribers: Option<Arc<crate::http_server::SubscriberRegistry>>,
) -> (HookOutcome, Option<HookCommit>) {
    if hooks.is_empty() {
        return (HookOutcome::NoMatch, None);
    }
    let tokio_handle = tokio::runtime::Handle::try_current().ok();
    Python::attach(|py| {
        run_hooks_inner(py, hooks, item, state, subscribers, tokio_handle)
    })
}

fn run_hooks_inner(
    py: Python<'_>,
    hooks: &[Hook],
    item: &WorkItem,
    state: &HarnessState,
    subscribers: Option<Arc<crate::http_server::SubscriberRegistry>>,
    tokio_handle: Option<tokio::runtime::Handle>,
) -> (HookOutcome, Option<HookCommit>) {
    // Build e as a dict. Use work_item_to_py's field extraction, then
    // flatten id/priority/type/time + variant fields into one dict.
    let pwi = work_item_to_py(item);
    let e = PyDict::new(py);
    let _ = e.set_item("id", &pwi.id);
    let _ = e.set_item("priority", pwi.priority);
    let _ = e.set_item("type", &pwi.item_type);
    let _ = e.set_item("time", &pwi.time);
    for (k, v) in &pwi.fields {
        if let Ok(pyv) = json_to_py(py, v) {
            let _ = e.set_item(k, pyv);
        }
    }

    // Match loop — single interpreter pass, all matches evaluated here.
    let locals = PyDict::new(py);
    let _ = locals.set_item("e", &e);
    let matched: Option<&Hook> = hooks.iter().find(|h| {
        let expr = match CString::new(h.match_expr.as_str()) { Ok(c) => c, Err(_) => return false };
        match py.eval(&expr, None, Some(&locals)) {
            Ok(v) => v.is_truthy().unwrap_or(false),
            Err(_) => false,  // match errors don't escalate; treat as non-match
        }
    });

    let Some(hook) = matched else {
        return (HookOutcome::NoMatch, None);
    };

    // Build hook-mode PyHarness. Memory and id_generator are cloned-and-mutated;
    // messages and process_starts go to effects for the caller to apply.
    let effects = Arc::new(Mutex::new(ExternalEffects::default()));
    let id_gen = Arc::new(Mutex::new(state.id_generator.clone()));
    // Timer/process/hooks aren't exposed to hooks — empty managers.
    let timer_manager = Arc::new(Mutex::new(TimerManager::new()));

    let harness = match Py::new(py, PyHarness {
        effects: effects.clone(),
        hooks: Mutex::new(Vec::new()),
        process_manager: Mutex::new(ProcessManager::new()),
        id_gen: id_gen.clone(),
        timer_manager,
        process_outputs: HashMap::new(),
        child_depth_remaining: 0,
        agent_name: "hook".into(),
        agent_lineage: "hook".into(),
        harness_bin: std::env::current_exe()
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_else(|_| "claude-server".into()),
        subscribers,
        tokio_handle,
        hook_mode: true,
    }) {
        Ok(h) => h,
        Err(e) => return (
            HookOutcome::Failed { hook_name: hook.name.clone(), error: format!("harness init: {}", e) },
            None,
        ),
    };

    // Memory txn: cloned from state, mutated in place, extracted on commit.
    let py_memory = match Py::new(py, PyMemory {
        inner: Mutex::new(MemoryInner {
            data: state.memory.clone(),
            priorities: state.memory_priorities.clone(),
            sensitive: HashSet::new(),  // hooks don't touch sensitive_keys
        }),
        pinned: HashMap::new(),
        effects: effects.clone(),
    }) {
        Ok(m) => m,
        Err(e) => return (
            HookOutcome::Failed { hook_name: hook.name.clone(), error: format!("memory init: {}", e) },
            None,
        ),
    };

    // Use the locals dict as globals too — py.run(code, globals, locals)
    // with globals==locals means `def __hook()` can see e/memory/_harness.
    // Python scoping: a function body can't see the *enclosing exec's* locals,
    // only module-level globals. Passing the same dict for both makes our
    // injected names behave as module-level.
    let _ = locals.set_item("_harness", &harness);
    let _ = locals.set_item("memory", &py_memory);
    let _ = locals.set_item("_result", py.None());

    let wrapped = format!(
        "from datetime import timedelta, datetime\n\
         send_message = _harness.send_message\n\
         shell_exec = _harness.shell_exec\n\
         def __hook():\n{}\n\
         _result = __hook()\n",
        hook.process.lines().map(|l| format!("    {}", l)).collect::<Vec<_>>().join("\n"),
    );
    let code = match CString::new(wrapped) {
        Ok(c) => c,
        Err(e) => return (
            HookOutcome::Failed { hook_name: hook.name.clone(), error: format!("NUL in process: {}", e) },
            None,
        ),
    };

    let (cancel_tx, cancel_rx) = std::sync::mpsc::channel::<()>();
    let timeout = std::time::Duration::from_millis(hook.timeout_ms);
    let watchdog = std::thread::spawn(move || {
        use std::sync::mpsc::RecvTimeoutError;
        // Only fire on Timeout. Disconnected means the hook finished and
        // dropped cancel_tx — normal completion, don't interrupt.
        if matches!(cancel_rx.recv_timeout(timeout), Err(RecvTimeoutError::Timeout)) {
            unsafe { pyo3::ffi::PyErr_SetInterrupt(); }
        }
    });

    let run_res = py.run(&code, Some(&locals), Some(&locals));
    drop(cancel_tx);
    let _ = watchdog.join();

    // Extract the hook's txn: memory + id_gen from the pyclass instances,
    // messages + process_starts from effects. Side effects emitted before an
    // exception still apply — the agent expects fire-and-forget semantics.
    let commit = {
        let m = py_memory.borrow(py);
        let mut inner = std::mem::take(&mut *m.inner.lock().unwrap());
        let mut eff = std::mem::take(&mut *effects.lock().unwrap());
        HookCommit {
            memory: std::mem::take(&mut inner.data),
            memory_priorities: std::mem::take(&mut inner.priorities),
            id_generator: std::mem::take(&mut *id_gen.lock().unwrap()),
            messages: std::mem::take(&mut eff.messages),
            process_starts: std::mem::take(&mut eff.process_starts),
        }
    };

    match run_res {
        Ok(()) => {
            let result = locals.get_item("_result").ok().flatten();
            match result {
                None => (HookOutcome::Consumed { hook_name: hook.name.clone() }, Some(commit)),
                Some(r) if r.is_none() => (HookOutcome::Consumed { hook_name: hook.name.clone() }, Some(commit)),
                Some(_) => {
                    // Hook returned e — read back mutations.
                    let priority = e.get_item("priority").ok().flatten()
                        .and_then(|v| v.extract::<u8>().ok())
                        .unwrap_or(item.priority);
                    let hook_note = e.get_item("hook_note").ok().flatten()
                        .and_then(|v| v.extract::<String>().ok());
                    (HookOutcome::Passed { hook_name: hook.name.clone(), priority, hook_note }, Some(commit))
                }
            }
        }
        Err(exc) => {
            let tb = exc.traceback(py)
                .and_then(|t| t.format().ok())
                .unwrap_or_default();
            (
                HookOutcome::Failed {
                    hook_name: hook.name.clone(),
                    error: format!("{}{}", tb, exc),
                },
                Some(commit),  // side effects before the exception still apply
            )
        }
    }
}

// ---- Python Preamble ----

const PREAMBLE: &str = r#"
from datetime import timedelta, datetime
from dataclasses import dataclass

@dataclass
class ChildSettings:
    name: str
    task: str
    model: str | None = None
    max_turns: int | None = 20
    can_compact: bool = True
    inherit_history: bool = True
    attach: list[str] | None = None
    prefix_context: str | None = None
    prefix_attach: list[str] | None = None

send_message = _harness.send_message
wait_for_message_channel = _harness.wait_for_message_channel
shell_exec = _harness.shell_exec
shell_status = _harness.shell_status
shell_output = _harness.shell_output
shell_input = _harness.shell_input
shell_close_stdin = _harness.shell_close_stdin
shell_kill = _harness.shell_kill
processes_list = _harness.processes_list
acknowledge_timer = _harness.acknowledge_timer
request_compaction = _harness.request_compaction
view = _harness.view
fork = _harness.fork
kill_child = _harness.kill_child
message_agent = _harness.message_agent
done = _harness.done
register_hook = _harness.register_hook
remove_hook = _harness.remove_hook
list_hooks = _harness.list_hooks
agent_name = _harness.agent_name
agent_lineage = _harness.agent_lineage
harness_bin = _harness.harness_bin

def http(method, url, headers=None, body=None, block_for=None, **kw):
    """Thin curl wrapper — same block_for semantics as shell_exec.
    Returns a pid; result arrives as ProcessCompleted with output_preview
    containing the response body followed by the status code on the last line."""
    args = ["-sS", "-X", method.upper(), url, "-w", "\n%{http_code}"]
    for k, v in (headers or {}).items():
        args += ["-H", f"{k}: {v}"]
    if body is not None:
        args += ["-d", body if isinstance(body, str) else __import__("json").dumps(body)]
    return shell_exec(cmd="curl", args=args,
                      description=f"HTTP {method.upper()} {url}",
                      block_for=block_for or timedelta(seconds=3), **kw)
"#;

const COMPACTION_PREAMBLE: &str = r#"
compact = _harness.compact
compaction_script = ""
"#;

// ---- Executor ----

pub fn initialize_python() {
    Python::initialize();
}

/// Execute Python code with a timeout. If `timeout_secs` is 0, no timeout is applied.
pub fn execute(
    state: &HarnessState,
    code: &str,
    is_compaction: bool,
    process_outputs: &HashMap<String, String>,
) -> ExecutionResult {
    execute_with_timeout(state, code, is_compaction, process_outputs, 5, 1, "root", "root", &HashMap::new(), None)
}

#[allow(clippy::too_many_arguments)]
pub fn execute_with_timeout(
    state: &HarnessState,
    code: &str,
    is_compaction: bool,
    process_outputs: &HashMap<String, String>,
    timeout_secs: u64,
    child_depth_remaining: u32,
    agent_name: &str,
    agent_lineage: &str,
    pinned_memory: &HashMap<String, String>,
    subscribers: Option<Arc<crate::http_server::SubscriberRegistry>>,
) -> ExecutionResult {
    // Clone everything the thread needs. This clone IS the transaction —
    // its components move into pyclass Mutex<T> fields, get mutated in place,
    // and are extracted back into committed_state on success.
    let txn = state.clone();
    let code = code.to_string();
    let process_outputs = process_outputs.clone();
    let agent_name = agent_name.to_string();
    let agent_lineage = agent_lineage.to_string();
    let pinned_memory = pinned_memory.clone();
    // Capture the tokio handle if we're in a runtime (we are, in agent_loop).
    // wait_for_message_channel needs this to block_on the async wait. Tests
    // that call execute() directly without a runtime get None → the function
    // raises cleanly.
    let tokio_handle = tokio::runtime::Handle::try_current().ok();

    let (tx, rx) = std::sync::mpsc::sync_channel::<ExecutionResult>(1);

    std::thread::spawn(move || {
        let result = execute_inner(
            txn, &code, is_compaction, &process_outputs,
            child_depth_remaining, &agent_name, &agent_lineage, &pinned_memory,
            subscribers, tokio_handle,
        );
        let _ = tx.send(result);
    });

    if timeout_secs == 0 {
        // No timeout — block indefinitely (used in tests)
        return rx.recv().unwrap_or_else(|_| ExecutionResult {
            stdout: String::new(),
            is_error: true,
            error_text: "Python execution thread panicked".to_string(),
            effects: ExternalEffects::default(),
            committed_state: None,
        });
    }

    match rx.recv_timeout(Duration::from_secs(timeout_secs)) {
        Ok(result) => result,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            eprintln!("[python] Script execution timed out after {}s, sending interrupt", timeout_secs);
            // Send KeyboardInterrupt to the Python interpreter
            unsafe { pyo3::ffi::PyErr_SetInterrupt() };

            // Grace period: wait 1 more second for clean shutdown
            match rx.recv_timeout(Duration::from_secs(1)) {
                Ok(result) => {
                    eprintln!("[python] Script stopped after interrupt");
                    // The script errored with KeyboardInterrupt — return that
                    result
                }
                Err(_) => {
                    eprintln!("[python] Script did not stop after interrupt, abandoning thread");
                    ExecutionResult {
                        stdout: String::new(),
                        is_error: true,
                        error_text: format!(
                            "Script execution timed out after {}s. Your script must not block — \
                            no sleep(), no infinite loops, no blocking I/O. Use shell_exec() for \
                            long-running operations.",
                            timeout_secs
                        ),
                        effects: ExternalEffects::default(),
                        committed_state: None,
                    }
                }
            }
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            ExecutionResult {
                stdout: String::new(),
                is_error: true,
                error_text: "Python execution thread panicked".to_string(),
                effects: ExternalEffects::default(),
                committed_state: None,
            }
        }
    }
}

/// Inner execution function that runs on a dedicated thread.
/// Takes ownership of the cloned state; moves its components into pyclass
/// Mutex<T> fields for in-place mutation; reassembles into committed_state
/// on success.
#[allow(clippy::too_many_arguments)]
fn execute_inner(
    txn: HarnessState,
    code: &str,
    is_compaction: bool,
    process_outputs: &HashMap<String, String>,
    child_depth_remaining: u32,
    agent_name: &str,
    agent_lineage: &str,
    pinned_memory: &HashMap<String, String>,
    subscribers: Option<Arc<crate::http_server::SubscriberRegistry>>,
    tokio_handle: Option<tokio::runtime::Handle>,
) -> ExecutionResult {
    let effects = Arc::new(Mutex::new(ExternalEffects::default()));
    let stdout_buf = Arc::new(Mutex::new(String::new()));

    // Move txn's components into Arc<Mutex<T>> / Mutex<T>. These go into the
    // pyclass instances; on success we extract them back out.
    let id_gen = Arc::new(Mutex::new(txn.id_generator));
    let timer_manager = Arc::new(Mutex::new(txn.timer_manager));

    // These hold the remaining txn pieces we need to reassemble later.
    // The py objects own the data via Mutex; we keep Py<T> handles to extract.
    struct Handles {
        py_work_queue: Py<PyWorkQueue>,
        py_memory: Py<PyMemory>,
        py_history: Py<PyHistoryManager>,
        py_harness: Py<PyHarness>,
    }

    let result: PyResult<Handles> = Python::attach(|py| {
        let globals = PyDict::new(py);
        let locals = PyDict::new(py);

        // Set up stdout capture
        let stdout = Py::new(
            py,
            StdoutCapture {
                buffer: stdout_buf.clone(),
            },
        )?;
        let sys = py.import("sys")?;
        sys.setattr("stdout", &stdout)?;
        sys.setattr("stderr", &stdout)?;

        // Work queue — moved directly, mutated in place by pop_front/remove.
        let py_work_queue = Py::new(py, PyWorkQueue {
            inner: Mutex::new(txn.work_queue),
        })?;
        locals.set_item("work_queue", &py_work_queue)?;

        // Memory — bundled into one mutex since data/priorities/sensitive are
        // accessed together.
        let py_memory = Py::new(py, PyMemory {
            inner: Mutex::new(MemoryInner {
                data: txn.memory,
                priorities: txn.memory_priorities,
                sensitive: txn.sensitive_keys,
            }),
            pinned: pinned_memory.clone(),
            effects: effects.clone(),
        })?;
        locals.set_item("memory", &py_memory)?;

        // Timers — Arc-shared with PyHarness.acknowledge_timer.
        let py_timers = Py::new(py, PyTimerManager {
            inner: timer_manager.clone(),
            id_gen: id_gen.clone(),
        })?;
        locals.set_item("timers", py_timers)?;

        // History — mutated in place by replace_with_description/remove/add.
        let py_history = Py::new(py, PyHistoryManager {
            inner: Mutex::new(txn.event_history),
            id_gen: id_gen.clone(),
            is_compaction,
        })?;
        locals.set_item("history", &py_history)?;

        // Harness — owns hooks + process_manager (bookkeeping), shares
        // id_gen + timer_manager.
        let py_harness = Py::new(py, PyHarness {
            effects: effects.clone(),
            hooks: Mutex::new(txn.hooks),
            process_manager: Mutex::new(txn.process_manager),
            id_gen: id_gen.clone(),
            timer_manager: timer_manager.clone(),
            process_outputs: process_outputs.clone(),
            child_depth_remaining,
            agent_name: agent_name.to_string(),
            agent_lineage: agent_lineage.to_string(),
            harness_bin: std::env::current_exe()
                .map(|p| p.to_string_lossy().into_owned())
                .unwrap_or_else(|_| "claude-server".into()),
            subscribers: subscribers.clone(),
            tokio_handle: tokio_handle.clone(),
            hook_mode: false,
        })?;
        locals.set_item("_harness", &py_harness)?;

        // Run preamble
        let preamble = CString::new(PREAMBLE).unwrap();
        py.run(&preamble, Some(&globals), Some(&locals))?;

        // Run compaction preamble if needed
        if is_compaction {
            let cp = CString::new(COMPACTION_PREAMBLE).unwrap();
            py.run(&cp, Some(&globals), Some(&locals))?;
        }

        // Run agent's code
        let code_cstr = CString::new(code).map_err(|_| {
            pyo3::exceptions::PySyntaxError::new_err("Code contains null bytes")
        })?;
        py.run(&code_cstr, Some(&globals), Some(&locals))?;

        // If compaction, read back compaction_script
        if is_compaction {
            if let Some(script_val) = locals.get_item("compaction_script")? {
                let script: String = script_val.extract()?;
                if !script.is_empty() {
                    effects
                        .lock()
                        .unwrap()
                        .compaction_script_appends
                        .push(script);
                }
            }
        }

        Ok(Handles { py_work_queue, py_memory, py_history, py_harness })
    });

    let stdout = stdout_buf.lock().unwrap().clone();
    let ext_effects = match Arc::try_unwrap(effects) {
        Ok(mutex) => mutex.into_inner().unwrap(),
        Err(arc) => std::mem::take(&mut *arc.lock().unwrap()),
    };

    match result {
        Ok(handles) => {
            // Extract the mutated components from the pyclass instances and
            // reassemble into HarnessState. borrow(py) + mutex-lock-take.
            // Scope carefully so MutexGuard drops before PyRef.
            let committed = Python::attach(|py| {
                let wq;
                {
                    let h = handles.py_work_queue.borrow(py);
                    let mut g = h.inner.lock().unwrap();
                    wq = std::mem::replace(&mut *g, WorkQueue::new());
                }
                let mem_data;
                let mem_prio;
                let sensitive;
                {
                    let h = handles.py_memory.borrow(py);
                    let mut inner = std::mem::take(&mut *h.inner.lock().unwrap());
                    mem_data = std::mem::take(&mut inner.data);
                    mem_prio = std::mem::take(&mut inner.priorities);
                    sensitive = std::mem::take(&mut inner.sensitive);
                }
                let event_history;
                {
                    let h = handles.py_history.borrow(py);
                    let mut g = h.inner.lock().unwrap();
                    event_history = std::mem::replace(&mut *g, EventHistory::new());
                }
                let hooks;
                let process_manager;
                {
                    let h = handles.py_harness.borrow(py);
                    hooks = std::mem::take(&mut *h.hooks.lock().unwrap());
                    let mut g = h.process_manager.lock().unwrap();
                    process_manager = std::mem::replace(&mut *g, ProcessManager::new());
                }
                let timer_mgr = std::mem::replace(
                    &mut *timer_manager.lock().unwrap(), TimerManager::new()
                );
                let id_generator = std::mem::take(&mut *id_gen.lock().unwrap());

                HarnessState {
                    work_queue: wq,
                    event_history,
                    timer_manager: timer_mgr,
                    process_manager,
                    memory: mem_data,
                    memory_priorities: mem_prio,
                    sensitive_keys: sensitive,
                    hooks,
                    id_generator,
                    // These fields weren't moved into pyclass instances — the
                    // script can't touch them — so keep the pre-turn values.
                    last_harness_version: txn.last_harness_version,
                    hook_stats: txn.hook_stats,
                    last_input_tokens: txn.last_input_tokens,
                    context_window: txn.context_window,
                    max_tokens: txn.max_tokens,
                }
            });

            ExecutionResult {
                stdout,
                is_error: false,
                error_text: String::new(),
                effects: ext_effects,
                committed_state: Some(committed),
            }
        }
        Err(e) => {
            let error_text = Python::attach(|py| {
                let tb_str = e
                    .traceback(py)
                    .map(|tb| tb.format().unwrap_or_default())
                    .unwrap_or_default();
                format!("{}{}", tb_str, e)
            });
            ExecutionResult {
                stdout,
                is_error: true,
                error_text,
                effects: ExternalEffects::default(),
                committed_state: None,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn init() {
        static ONCE: std::sync::Once = std::sync::Once::new();
        ONCE.call_once(|| {
            initialize_python();
        });
    }

    #[test]
    fn test_basic_print() {
        init();
        let state = HarnessState::new(200_000, 16384);
        let result = execute(&state, "print('hello world')", false, &HashMap::new());
        assert!(!result.is_error, "Error: {}", result.error_text);
        assert_eq!(result.stdout.trim(), "hello world");
    }

    #[test]
    fn test_memory_operations() {
        init();
        let mut state = HarnessState::new(200_000, 16384);
        state.memory.insert("key1".to_string(), serde_json::json!("val1"));

        let result = execute(
            &state,
            r#"
assert memory["key1"] == "val1"
memory["key2"] = "val2"
print("ok")
"#,
            false,
            &HashMap::new(),
        );
        assert!(!result.is_error, "Error: {}", result.error_text);
        assert_eq!(result.stdout.trim(), "ok");
        let committed = result.committed_state.unwrap();
        assert_eq!(committed.memory.get("key2"), Some(&serde_json::json!("val2")));
        assert_eq!(committed.memory.get("key1"), Some(&serde_json::json!("val1")));
    }

    #[test]
    fn test_memory_structured_values() {
        init();
        let state = HarnessState::new(200_000, 16384);
        let result = execute(
            &state,
            r#"
# Store various types
memory["s"] = "hello"
memory["n"] = 42
memory["d"] = {"pid": "abc", "chat_id": "xyz"}
memory["l"] = ["a", "b", "c"]
memory["b"] = True

# Read them back and verify types
assert memory["s"] == "hello"
assert memory["n"] == 42
assert memory["d"]["pid"] == "abc"
assert memory["l"][1] == "b"
assert memory["b"] == True

# Test .get()
assert memory.get("s") == "hello"
assert memory.get("missing") is None
assert memory.get("missing", "fallback") == "fallback"

print("all passed")
"#,
            false,
            &HashMap::new(),
        );
        assert!(!result.is_error, "Error: {}", result.error_text);
        assert_eq!(result.stdout.trim(), "all passed");
        let committed = result.committed_state.unwrap();
        assert_eq!(committed.memory.len(), 5);
    }

    #[test]
    fn test_timer_add_returns_id() {
        init();
        let state = HarnessState::new(200_000, 16384);
        let result = execute(
            &state,
            r#"
tid = timers.add(every=30, priority=6, description="test timer")
print(tid)
"#,
            false,
            &HashMap::new(),
        );
        assert!(!result.is_error, "Error: {}", result.error_text);
        assert!(!result.stdout.trim().is_empty());
        let committed = result.committed_state.unwrap();
        assert_eq!(committed.timer_manager.list().len(), 1);
    }

    #[test]
    fn test_work_queue_pop() {
        init();
        let mut state = HarnessState::new(200_000, 16384);
        let mut id_gen = IdGenerator::new();
        state.work_queue.push(WorkItem {
            id: id_gen.next(),
            priority: 9,
            time: chrono::Utc::now(),
            item_type: WorkItemType::UserMessage {
                chat_id: "test".to_string(),
                user: "user@test.com".to_string(),
                content: "Hello!".to_string(),
                message_ref: None,
            },
            attachments: Vec::new(),
        });

        let result = execute(
            &state,
            r#"
item = work_queue[0]
print(item.content)
work_queue.pop_front()
"#,
            false,
            &HashMap::new(),
        );
        assert!(!result.is_error, "Error: {}", result.error_text);
        assert_eq!(result.stdout.trim(), "Hello!");
        let committed = result.committed_state.unwrap();
        assert!(committed.work_queue.is_empty());
    }

    #[test]
    fn test_error_handling() {
        init();
        let state = HarnessState::new(200_000, 16384);
        let result = execute(&state, "undefined_variable", false, &HashMap::new());
        assert!(result.is_error);
        assert!(
            result.error_text.contains("NameError"),
            "Expected NameError in: '{}'",
            result.error_text,
        );
        assert!(result.committed_state.is_none());
    }

    #[test]
    fn test_shell_exec_returns_id() {
        init();
        let state = HarnessState::new(200_000, 16384);
        let result = execute(
            &state,
            r#"
pid = shell_exec("echo", ["hello"])
print(pid)
memory["my_pid"] = pid
"#,
            false,
            &HashMap::new(),
        );
        assert!(!result.is_error, "Error: {}", result.error_text);
        assert_eq!(result.effects.process_starts.len(), 1);
        let committed = result.committed_state.unwrap();
        assert_eq!(committed.memory.len(), 1);
        // Bookkeeping entry added same-turn:
        assert_eq!(committed.process_manager.processes().len(), 1);
    }

    #[test]
    fn test_send_message() {
        init();
        let state = HarnessState::new(200_000, 16384);
        let result = execute(
            &state,
            r#"send_message("chat1", "Hello from Claude!")"#,
            false,
            &HashMap::new(),
        );
        assert!(!result.is_error, "Error: {}", result.error_text);
        assert_eq!(result.effects.messages.len(), 1);
        assert_eq!(result.effects.messages[0].chat_id, "chat1");
    }

    #[test]
    fn test_one_shot_timer_with_datetime() {
        init();
        let state = HarnessState::new(200_000, 16384);
        let result = execute(
            &state,
            r#"
tid = timers.add(at=datetime(2026, 2, 1, 17, 0, 0), priority=8, description="dinner reminder")
print(tid)
"#,
            false,
            &HashMap::new(),
        );
        assert!(!result.is_error, "Error: {}", result.error_text);
        let committed = result.committed_state.unwrap();
        let timers = committed.timer_manager.list();
        assert_eq!(timers.len(), 1);
        match &timers[0].schedule {
            TimerSchedule::OneShot { at } => {
                // datetime(2026, 2, 1, 17, 0, 0) should be a reasonable epoch
                assert!(at.timestamp() > 1_700_000_000, "at {} too early", at);
            }
            _ => panic!("expected OneShot"),
        }
    }

    #[test]
    fn test_memory_priority() {
        init();
        let state = HarnessState::new(200_000, 16384);
        let result = execute_with_timeout(
            &state,
            r#"
# Dict syntax assigns default priority 5
memory["key1"] = "value1"
assert memory.get_priority("key1") == 5

# memory.set() with explicit priority
memory.set("key2", "value2", priority=8)
assert memory.get_priority("key2") == 8

# set_priority changes priority without changing value
memory.set_priority("key1", 3)
assert memory.get_priority("key1") == 3

print("all passed")
"#,
            false,
            &HashMap::new(),
            0,
            1,
            "root",
            "root",
            &HashMap::new(),
            None,
        );
        assert!(!result.is_error, "Error: {}", result.error_text);
        assert_eq!(result.stdout.trim(), "all passed");
        let committed = result.committed_state.unwrap();
        assert_eq!(committed.memory.len(), 2);
        assert_eq!(committed.memory_priorities.get("key1"), Some(&3));
        assert_eq!(committed.memory_priorities.get("key2"), Some(&8));
    }

    #[test]
    fn test_execution_timeout() {
        init();
        let state = HarnessState::new(200_000, 16384);
        let start = std::time::Instant::now();
        let result = execute_with_timeout(
            &state,
            "while True: pass",
            false,
            &HashMap::new(),
            2, // 2 second timeout for test speed
            1,
            "root",
            "root",
            &HashMap::new(),
            None,
        );
        let elapsed = start.elapsed();
        assert!(result.is_error, "Should have timed out");
        assert!(
            result.error_text.contains("timed out") || result.error_text.contains("KeyboardInterrupt"),
            "Error should mention timeout or KeyboardInterrupt, got: {}",
            result.error_text
        );
        // Should complete in ~2-3 seconds (2s timeout + 1s grace max)
        assert!(elapsed.as_secs() <= 5, "Took too long: {:?}", elapsed);
    }

    #[test]
    fn test_fork() {
        init();
        let state = HarnessState::new(200_000, 16384);
        let result = execute_with_timeout(
            &state,
            r#"
fork([
    ChildSettings(
        name="test-runner",
        task="Write tests",
        model="claude-sonnet-4-5-20250929",
        max_turns=10,
    ),
    ChildSettings(
        name="linter",
        task="Run linting",
    ),
])
print("forked")
"#,
            false,
            &HashMap::new(),
            0,
            1,
            "root",
            "root",
            &HashMap::new(),
            None,
        );
        assert!(!result.is_error, "Error: {}", result.error_text);
        assert_eq!(result.stdout.trim(), "forked");
        assert_eq!(result.effects.fork_requests.len(), 1);
        let req = &result.effects.fork_requests[0];
        assert_eq!(req.children.len(), 2);
        assert_eq!(req.children[0].name, "test-runner");
        assert_eq!(req.children[0].task, "Write tests");
        assert_eq!(req.children[0].model, Some("claude-sonnet-4-5-20250929".to_string()));
        assert_eq!(req.children[0].max_turns, Some(10));
        assert_eq!(req.children[1].name, "linter");
        assert_eq!(req.children[1].task, "Run linting");
        assert_eq!(req.children[1].model, None);
        assert_eq!(req.children[1].max_turns, Some(20)); // default
        assert!(req.children[1].can_compact); // default is true
        assert!(req.children[0].attach.is_empty()); // default: no attachments
    }

    #[test]
    fn test_fork_rejects_root_name() {
        init();
        let state = HarnessState::new(200_000, 16384);
        let result = execute(
            &state,
            r#"fork([ChildSettings(name="root", task="impersonate")])"#,
            false,
            &HashMap::new(),
        );
        assert!(result.is_error, "fork with name='root' should fail");
        assert!(
            result.error_text.contains("reserved"),
            "Error should mention reserved name: {}",
            result.error_text
        );
    }

    #[test]
    fn test_view() {
        init();
        let state = HarnessState::new(200_000, 16384);

        // Create a temp file so the existence check passes
        let tmp = std::env::temp_dir().join("claude-server-test-attachment.txt");
        std::fs::write(&tmp, "test content").unwrap();

        let code = format!(
            r#"
view({path:?})
print("ok")
"#,
            path = tmp.to_str().unwrap()
        );
        let result = execute(&state, &code, false, &HashMap::new());
        assert!(!result.is_error, "Error: {}", result.error_text);
        assert_eq!(result.stdout.trim(), "ok");
        assert_eq!(result.effects.view_paths.len(), 1);
        assert_eq!(result.effects.view_paths[0], tmp.to_str().unwrap());

        std::fs::remove_file(&tmp).ok();
    }

    #[test]
    fn test_view_file_not_found() {
        init();
        let state = HarnessState::new(200_000, 16384);
        let result = execute(
            &state,
            r#"view("/nonexistent/path/xyz.jpg")"#,
            false,
            &HashMap::new(),
        );
        assert!(result.is_error, "Should fail on nonexistent file");
        assert!(
            result.error_text.contains("FileNotFoundError")
                || result.error_text.contains("file not found"),
            "Error: {}",
            result.error_text
        );
    }

    #[test]
    fn test_fork_with_attach() {
        init();
        let state = HarnessState::new(200_000, 16384);
        let result = execute_with_timeout(
            &state,
            r#"
fork([
    ChildSettings(
        name="investigator",
        task="Look at this image",
        attach=["/tmp/snapshot.jpg", "/tmp/metadata.json"],
    ),
])
"#,
            false,
            &HashMap::new(),
            0,
            1,
            "root",
            "root",
            &HashMap::new(),
            None,
        );
        assert!(!result.is_error, "Error: {}", result.error_text);
        let req = &result.effects.fork_requests[0];
        assert_eq!(req.children.len(), 1);
        assert_eq!(req.children[0].attach.len(), 2);
        assert_eq!(req.children[0].attach[0], "/tmp/snapshot.jpg");
        assert_eq!(req.children[0].attach[1], "/tmp/metadata.json");
    }

    #[test]
    fn test_timedelta_in_timer_and_shell_exec() {
        init();
        let state = HarnessState::new(200_000, 16384);
        let result = execute(
            &state,
            r#"
# timedelta should work in timers.add
tid = timers.add(every=timedelta(seconds=30), priority=6, description="test")
print(tid)

# timedelta should work in shell_exec
pid = shell_exec("echo", ["hi"], alert_timer=timedelta(minutes=5))
print(pid)

# plain numbers should still work too
tid2 = timers.add(every=60, priority=5, description="numeric")
pid2 = shell_exec("echo", ["hi"], alert_timer=300)
"#,
            false,
            &HashMap::new(),
        );
        assert!(!result.is_error, "Error: {}", result.error_text);
        let committed = result.committed_state.unwrap();
        let timers = committed.timer_manager.list();
        assert_eq!(timers.len(), 2);
        match &timers[0].schedule {
            TimerSchedule::Recurring { every, .. } => assert_eq!(every.as_secs(), 30),
            _ => panic!("expected Recurring"),
        }
        match &timers[1].schedule {
            TimerSchedule::Recurring { every, .. } => assert_eq!(every.as_secs(), 60),
            _ => panic!("expected Recurring"),
        }
        assert_eq!(result.effects.process_starts.len(), 2);
        assert_eq!(result.effects.process_starts[0].alert_timer_secs, 300);
        assert_eq!(result.effects.process_starts[1].alert_timer_secs, 300);
    }

    #[test]
    fn test_message_agent() {
        init();
        let state = HarnessState::new(200_000, 16384);
        let result = execute(
            &state,
            r#"
message_agent("sibling-a", "check the API status")
message_agent("sibling-b", "done with my part", priority=9)
print(agent_name)
print(agent_lineage)
"#,
            false,
            &HashMap::new(),
        );
        assert!(!result.is_error, "Error: {}", result.error_text);
        assert_eq!(result.effects.agent_messages.len(), 2);
        assert_eq!(result.effects.agent_messages[0].recipient, "sibling-a");
        assert_eq!(result.effects.agent_messages[0].content, "check the API status");
        assert_eq!(result.effects.agent_messages[0].priority, 6); // default
        assert_eq!(result.effects.agent_messages[1].recipient, "sibling-b");
        assert_eq!(result.effects.agent_messages[1].priority, 9);
        assert_eq!(result.stdout.trim(), "root\nroot");
    }

    #[test]
    fn test_agent_identity_in_child() {
        init();
        let state = HarnessState::new(200_000, 16384);
        let result = execute_with_timeout(
            &state,
            r#"
print(agent_name)
print(agent_lineage)
"#,
            false,
            &HashMap::new(),
            0,
            1,
            "api-checker",
            "api-checker, child of plan-builder, child of root",
            &HashMap::new(),
            None,
        );
        assert!(!result.is_error, "Error: {}", result.error_text);
        let lines: Vec<&str> = result.stdout.trim().lines().collect();
        assert_eq!(lines[0], "api-checker");
        assert_eq!(lines[1], "api-checker, child of plan-builder, child of root");
    }

    #[test]
    fn test_harness_bin() {
        init();
        let state = HarnessState::new(200_000, 16384);
        let result = execute(
            &state,
            r#"
print(harness_bin)
assert isinstance(harness_bin, str) and len(harness_bin) > 0
"#,
            false,
            &HashMap::new(),
        );
        assert!(!result.is_error, "Error: {}", result.error_text);
        assert!(!result.stdout.trim().is_empty());
    }

    #[test]
    fn test_request_compaction() {
        init();
        let state = HarnessState::new(200_000, 16384);
        let result = execute(&state, "request_compaction()", false, &HashMap::new());
        assert!(!result.is_error, "Error: {}", result.error_text);
        assert!(result.effects.compaction_requested);
    }

    #[test]
    fn test_memory_pin() {
        init();
        let state = HarnessState::new(200_000, 16384);
        let mut pinned = HashMap::new();
        pinned.insert("existing".to_string(), "old value".to_string());
        let result = execute_with_timeout(
            &state,
            r#"
# Pinned entries are readable via memory.get() (local-first fallback)
print(f"pinned: {memory.list_pinned()}")
print(f"existing: {memory.get('existing')}")
print(f"missing: {memory.get('missing')}")
print(f"contains: {'existing' in memory}")

# Pin new entries
memory.pin("api_info", "HA API at :8123")
memory.pin("user_prefs", "Prefers SMS alerts")

# Unpin
memory.unpin("existing")
"#,
            false,
            &HashMap::new(),
            0,
            1,
            "root",
            "root",
            &pinned,
            None,
        );
        assert!(!result.is_error, "Error: {}", result.error_text);
        assert!(result.stdout.contains("pinned: ['existing']"));
        assert!(result.stdout.contains("existing: old value"));
        assert!(result.stdout.contains("missing: None"));
        assert!(result.stdout.contains("contains: True"));
        assert_eq!(result.effects.memory_pins.len(), 2);
        assert_eq!(result.effects.memory_pins[0].0, "api_info");
        assert_eq!(result.effects.memory_pins[0].1, "HA API at :8123");
        assert_eq!(result.effects.memory_pins[1].0, "user_prefs");
        assert_eq!(result.effects.memory_unpins.len(), 1);
        assert_eq!(result.effects.memory_unpins[0], "existing");
    }

    #[test]
    fn test_done_with_result() {
        init();
        let state = HarnessState::new(200_000, 16384);
        let result = execute(
            &state,
            r#"
done(verdict="all clear", confidence=0.95, details={"camera": "front", "count": 5})
"#,
            false,
            &HashMap::new(),
        );
        assert!(!result.is_error, "Error: {}", result.error_text);
        assert!(result.effects.done_called);
        assert_eq!(result.effects.done_result.len(), 3);
        assert_eq!(
            result.effects.done_result["verdict"],
            serde_json::json!("all clear")
        );
        assert_eq!(
            result.effects.done_result["confidence"],
            serde_json::json!(0.95)
        );
        assert_eq!(
            result.effects.done_result["details"],
            serde_json::json!({"camera": "front", "count": 5})
        );
    }

    #[test]
    fn test_done_no_args() {
        init();
        let state = HarnessState::new(200_000, 16384);
        let result = execute(&state, "done()", false, &HashMap::new());
        assert!(!result.is_error, "Error: {}", result.error_text);
        assert!(result.effects.done_called);
        assert!(result.effects.done_result.is_empty());
    }

    #[test]
    fn test_work_item_field_access() {
        init();
        let mut state = HarnessState::new(200_000, 16384);
        let mut id_gen = IdGenerator::new();

        // Push a ChildAgentCompleted to test the new principled field mapping
        let mut result_map = HashMap::new();
        result_map.insert("verdict".to_string(), serde_json::json!("safe"));
        state.work_queue.push(WorkItem {
            id: id_gen.next(),
            priority: 7,
            time: chrono::Utc::now(),
            item_type: WorkItemType::ChildAgentCompleted {
                child_name: "investigator".to_string(),
                result: result_map,
                turns_used: 2,
                success: true,
                summary: "done".to_string(),
                cost_usd: 0.0123,
                cache_hit_pct: 85,
            },
            attachments: Vec::new(),
        });

        let result = execute(
            &state,
            r#"
item = work_queue[0]
print(item.type)
print(item.child_name)
print(item.turns_used)
print(item.success)
print(item.result["verdict"])
# Accessing a field that doesn't exist on this variant should raise with available fields listed
try:
    _ = item.chat_id
    print("FAIL: should have raised")
except AttributeError as e:
    print(f"attr error: {e}")
"#,
            false,
            &HashMap::new(),
        );
        assert!(!result.is_error, "Error: {}", result.error_text);
        assert!(result.stdout.contains("ChildAgentCompleted"));
        assert!(result.stdout.contains("investigator"));
        assert!(result.stdout.contains("2"));
        assert!(result.stdout.contains("True"));
        assert!(result.stdout.contains("safe"));
        assert!(result.stdout.contains("has no field 'chat_id'"));
        assert!(result.stdout.contains("Available fields"));
    }

    // ---- Hook tests ----

    #[test]
    fn test_register_hook_syntax_validation() {
        init();
        let state = HarnessState::new(200_000, 16384);
        // Bad match_expr
        let r = execute(&state,
            r#"register_hook("h", 5, "e.type ==", "return None")"#,
            false, &HashMap::new());
        assert!(r.is_error, "expected syntax error for bad match_expr");
        assert!(r.error_text.contains("match_expr"), "error: {}", r.error_text);
        // Bad process
        let r = execute(&state,
            r#"register_hook("h", 5, "True", "return return")"#,
            false, &HashMap::new());
        assert!(r.is_error, "expected syntax error for bad process");
        assert!(r.error_text.contains("process"), "error: {}", r.error_text);
        // Good
        let r = execute(&state,
            r#"register_hook("h", 5, "e['type'] == 'UserMessage'", "return None")"#,
            false, &HashMap::new());
        assert!(!r.is_error, "Error: {}", r.error_text);
        let committed = r.committed_state.unwrap();
        assert_eq!(committed.hooks.len(), 1);
        assert_eq!(committed.hooks[0].name, "h");
        assert_eq!(committed.hooks[0].priority, 5);
    }

    /// The motivating case for clone-and-mutate: register_hook() then
    /// list_hooks() in the same turn shows the hook. No merge logic — it's
    /// just reading the same Mutex<Vec<Hook>> that register_hook wrote to.
    #[test]
    fn test_list_hooks_same_turn_visibility() {
        init();
        let state = HarnessState::new(200_000, 16384);
        let r = execute(&state,
            r#"
register_hook("first", 5, "True", "return None")
register_hook("second", 10, "e['type'] == 'TimerFired'", "return e")
hooks = list_hooks()
# Sorted by priority desc, so "second" (10) comes before "first" (5)
assert len(hooks) == 2, f"expected 2, got {len(hooks)}"
assert hooks[0] == ("second", 10, "e['type'] == 'TimerFired'"), f"got {hooks[0]}"
assert hooks[1] == ("first", 5, "True"), f"got {hooks[1]}"
remove_hook("first")
hooks = list_hooks()
assert len(hooks) == 1, f"after remove: expected 1, got {len(hooks)}"
assert hooks[0][0] == "second"
print("ok")
"#,
            false, &HashMap::new());
        assert!(!r.is_error, "Error: {}", r.error_text);
        assert_eq!(r.stdout.trim(), "ok");
        let committed = r.committed_state.unwrap();
        assert_eq!(committed.hooks.len(), 1);
        assert_eq!(committed.hooks[0].name, "second");
    }

    fn mk_usermsg_item(content: &str) -> WorkItem {
        WorkItem {
            id: AgentId("test".into()),
            priority: 5,
            time: chrono::Utc::now(),
            item_type: WorkItemType::UserMessage {
                chat_id: "c1".into(), user: "u".into(),
                content: content.into(), message_ref: None,
            },
            attachments: Vec::new(),
        }
    }

    #[test]
    fn test_hook_consumed() {
        init();
        let state = HarnessState::new(200_000, 16384);
        let hooks = vec![Hook {
            name: "drop-hi".into(), priority: 5,
            match_expr: "e['type'] == 'UserMessage' and 'hi' in e['content']".into(),
            process: "return None".into(),
            timeout_ms: 3000,
        }];
        let (out, commit) = run_hooks(&hooks, &mk_usermsg_item("hi there"), &state, None);
        assert!(matches!(out, HookOutcome::Consumed { .. }));
        assert!(commit.unwrap().messages.is_empty());
    }

    #[test]
    fn test_hook_passed_with_priority_bump() {
        init();
        let state = HarnessState::new(200_000, 16384);
        let hooks = vec![Hook {
            name: "escalate".into(), priority: 5,
            match_expr: "'urgent' in e.get('content', '')".into(),
            process: "e['priority'] = 9\ne['hook_note'] = 'bumped'\nreturn e".into(),
            timeout_ms: 3000,
        }];
        let (out, _) = run_hooks(&hooks, &mk_usermsg_item("urgent thing"), &state, None);
        match out {
            HookOutcome::Passed { hook_name, priority, hook_note } => {
                assert_eq!(hook_name, "escalate");
                assert_eq!(priority, 9);
                assert_eq!(hook_note, Some("bumped".into()));
            }
            _ => panic!("expected Passed, got {:?}", std::mem::discriminant(&out)),
        }
    }

    #[test]
    fn test_hook_no_match() {
        init();
        let state = HarnessState::new(200_000, 16384);
        let hooks = vec![Hook {
            name: "never".into(), priority: 5,
            match_expr: "False".into(),
            process: "return None".into(),
            timeout_ms: 3000,
        }];
        let (out, commit) = run_hooks(&hooks, &mk_usermsg_item("anything"), &state, None);
        assert!(matches!(out, HookOutcome::NoMatch));
        assert!(commit.is_none());
    }

    #[test]
    fn test_hook_raises_preserves_original() {
        init();
        let state = HarnessState::new(200_000, 16384);
        let hooks = vec![Hook {
            name: "boom".into(), priority: 5,
            match_expr: "True".into(),
            process: "raise ValueError('kaboom')".into(),
            timeout_ms: 3000,
        }];
        let (out, _) = run_hooks(&hooks, &mk_usermsg_item("trigger"), &state, None);
        match out {
            HookOutcome::Failed { hook_name, error } => {
                assert_eq!(hook_name, "boom");
                assert!(error.contains("kaboom"), "error: {}", error);
            }
            _ => panic!("expected Failed"),
        }
    }

    #[test]
    fn test_hook_priority_order() {
        init();
        let state = HarnessState::new(200_000, 16384);
        // Both match — higher priority wins. Caller (register_hook) now
        // maintains sort order; simulate that here.
        let mut hooks = vec![
            Hook { name: "low".into(), priority: 1, match_expr: "True".into(),
                   process: "return None".into(), timeout_ms: 3000 },
            Hook { name: "high".into(), priority: 10, match_expr: "True".into(),
                   process: "return None".into(), timeout_ms: 3000 },
        ];
        hooks.sort_by(|a, b| b.priority.cmp(&a.priority));
        let (out, _) = run_hooks(&hooks, &mk_usermsg_item("x"), &state, None);
        match out {
            HookOutcome::Consumed { hook_name } => assert_eq!(hook_name, "high"),
            _ => panic!("expected Consumed"),
        }
    }

    #[test]
    fn test_hook_block_for_disallowed() {
        init();
        let state = HarnessState::new(200_000, 16384);
        let hooks = vec![Hook {
            name: "tries-block".into(), priority: 5,
            match_expr: "True".into(),
            process: "shell_exec(cmd='echo', args=['hi'], block_for=timedelta(seconds=1))".into(),
            timeout_ms: 3000,
        }];
        let (out, _) = run_hooks(&hooks, &mk_usermsg_item("x"), &state, None);
        match out {
            HookOutcome::Failed { error, .. } => {
                assert!(error.contains("block_for") && error.contains("second hook"),
                    "error: {}", error);
            }
            _ => panic!("expected Failed from block_for restriction"),
        }
    }

    #[test]
    fn test_hook_shell_exec_fire_and_forget_ok() {
        init();
        let state = HarnessState::new(200_000, 16384);
        let hooks = vec![Hook {
            name: "spawn".into(), priority: 5,
            match_expr: "True".into(),
            process: "shell_exec(cmd='ffprobe', args=[e['content']], description='probe')\nreturn None".into(),
            timeout_ms: 3000,
        }];
        let (out, commit) = run_hooks(&hooks, &mk_usermsg_item("/tmp/vid.mp4"), &state, None);
        assert!(matches!(out, HookOutcome::Consumed { .. }));
        let commit = commit.unwrap();
        assert_eq!(commit.process_starts.len(), 1);
        assert_eq!(commit.process_starts[0].cmd, "ffprobe");
        assert_eq!(commit.process_starts[0].args, vec!["/tmp/vid.mp4".to_string()]);
    }

    #[test]
    fn test_hook_memory_write() {
        init();
        let state = HarnessState::new(200_000, 16384);
        let hooks = vec![Hook {
            name: "counter".into(), priority: 5,
            match_expr: "True".into(),
            process: "memory['hits'] = memory.get('hits', 0) + 1\nreturn None".into(),
            timeout_ms: 3000,
        }];
        let (out, commit) = run_hooks(&hooks, &mk_usermsg_item("x"), &state, None);
        assert!(matches!(out, HookOutcome::Consumed { .. }));
        let commit = commit.unwrap();
        assert_eq!(commit.memory.get("hits"), Some(&serde_json::json!(1)));
    }


}
