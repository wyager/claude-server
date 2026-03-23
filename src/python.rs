use std::collections::HashMap;
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

// ---- Side Effect Collection ----

/// Collects all mutations and side effects from a Python script execution.
/// Applied atomically by the core loop after execution completes.
#[derive(Debug, Default)]
pub struct SideEffectCollector {
    pub id_gen: IdGenerator,
    pub memory_sets: Vec<(String, serde_json::Value)>,
    pub memory_deletes: Vec<String>,
    pub memory_priority_sets: Vec<(String, u8)>,
    pub queue_removes: Vec<String>,
    pub timer_adds: Vec<TimerAddRequest>,
    pub timer_cancels: Vec<String>,
    pub timer_acks: Vec<String>,
    pub filter_adds: Vec<QueueFilter>,
    pub filter_removes: Vec<String>,
    pub messages: Vec<OutboundMessageRequest>,
    pub process_starts: Vec<ProcessStartRequest>,
    pub process_kills: Vec<String>,
    pub history_removes: Vec<String>,
    pub history_replaces: Vec<(String, String)>,
    pub history_adds: Vec<String>,
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
    pub memory_pins: Vec<(String, String)>,    // (key, content) — written to pinned_memory table, cached in system prompt
    pub memory_unpins: Vec<String>,            // keys to remove from pinned storage
}

#[derive(Debug, Clone)]
pub struct ForkChildSettings {
    pub name: String,
    pub task: String,
    pub model: Option<String>,  // None = inherit parent's model
    pub max_turns: u32,
    pub can_compact: bool,
    /// If false, child starts with fresh history containing only a fork
    /// SystemAlert. Useful for cross-model forks where inheriting the parent's
    /// full context means paying a full re-ingest on the new model's cache.
    pub inherit_history: bool,
    /// File paths to push as a View work item on the child's queue.
    pub attach: Vec<String>,
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
pub struct TimerAddRequest {
    pub id: AgentId,
    pub every_secs: Option<u64>,
    /// Epoch seconds for one-shot timers (from datetime objects or numeric timestamps)
    pub at_epoch: Option<f64>,
    pub priority: u8,
    pub description: String,
}

#[derive(Debug)]
pub struct OutboundMessageRequest {
    pub chat_id: String,
    pub content: String,
    pub attachments: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ProcessStartRequest {
    pub id: AgentId,
    pub cmd: String,
    pub args: Vec<String>,
    pub env: HashMap<String, String>,
    pub description: String,
    pub alert_timer_secs: u64,
    pub success_prio: u8,
    pub fail_prio: u8,
    pub block_for_ms: Option<u64>,
}

// ---- Execution Result ----

pub struct ExecutionResult {
    pub stdout: String,
    pub is_error: bool,
    pub error_text: String,
    pub side_effects: SideEffectCollector,
}

// ---- #[pyclass] Types ----

type Collector = Arc<Mutex<SideEffectCollector>>;

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
        WorkItemType::UserMessage { chat_id, user, content } => {
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
        WorkItemType::ProcessCompleted { pid, exit_code, output_preview } => {
            fields.insert("pid".into(), pid.0.clone().into());
            fields.insert("exit_code".into(), (*exit_code).into());
            fields.insert("output_preview".into(), match output_preview {
                Some(s) => s.clone().into(),
                None => serde_json::Value::Null,
            });
            "ProcessCompleted"
        }
        WorkItemType::ProcessFailed { pid, error, output_preview } => {
            fields.insert("pid".into(), pid.0.clone().into());
            fields.insert("error".into(), error.clone().into());
            fields.insert("output_preview".into(), match output_preview {
                Some(s) => s.clone().into(),
                None => serde_json::Value::Null,
            });
            "ProcessFailed"
        }
        WorkItemType::ProcessTimeout { pid } => {
            fields.insert("pid".into(), pid.0.clone().into());
            "ProcessTimeout"
        }
        WorkItemType::ChildAgentCompleted { child_name, result, turns_used, success, summary } => {
            fields.insert("child_name".into(), child_name.clone().into());
            fields.insert("result".into(), serde_json::to_value(result).unwrap_or_default());
            fields.insert("turns_used".into(), (*turns_used).into());
            fields.insert("success".into(), (*success).into());
            fields.insert("summary".into(), summary.clone().into());
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
        WorkItemType::AgentStartup => {
            fields.insert(
                "description".into(),
                "Harness restarted. Any processes/bridges you were managing are dead — inspect memory and reconnect as needed.".into(),
            );
            "AgentStartup"
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
    items: Vec<PyWorkItem>,
    collector: Collector,
}

#[pymethods]
impl PyWorkQueue {
    fn __getitem__(&self, index: usize) -> PyResult<PyWorkItem> {
        self.items
            .get(index)
            .cloned()
            .ok_or_else(|| pyo3::exceptions::PyIndexError::new_err("work queue index out of range"))
    }

    fn __len__(&self) -> usize {
        self.items.len()
    }

    fn pop_front(&mut self) -> PyResult<Option<PyWorkItem>> {
        if self.items.is_empty() {
            return Ok(None);
        }
        let item = self.items.remove(0);
        self.collector
            .lock()
            .unwrap()
            .queue_removes
            .push(item.id.clone());
        Ok(Some(item))
    }

    fn remove(&mut self, id: String) -> PyResult<()> {
        self.items.retain(|i| i.id != id);
        self.collector.lock().unwrap().queue_removes.push(id);
        Ok(())
    }

    fn add_filter(&self, name: String, regex: String) -> PyResult<()> {
        self.collector
            .lock()
            .unwrap()
            .filter_adds
            .push(QueueFilter { name, regex });
        Ok(())
    }

    fn remove_filter(&self, name: String) -> PyResult<()> {
        self.collector.lock().unwrap().filter_removes.push(name);
        Ok(())
    }
}

#[pyclass]
struct PyMemory {
    data: HashMap<String, serde_json::Value>,
    priorities: HashMap<String, u8>,
    /// Pinned entries: shared across all agents, injected into the cached
    /// system prompt. Stored separately in SQLite (pinned_memory table).
    /// Read-through: get() checks local `data` first, then falls back here.
    pinned: HashMap<String, String>,
    collector: Collector,
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
        let value = self.data.get(key)
            .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err(key.to_string()))?;
        Self::value_to_py(py, value)
    }

    fn __setitem__<'py>(&mut self, py: Python<'py>, key: String, value: &Bound<'py, PyAny>) -> PyResult<()> {
        let serde_val = Self::py_to_value(py, value)?;
        self.data.insert(key.clone(), serde_val.clone());
        let mut col = self.collector.lock().unwrap();
        col.memory_sets.push((key.clone(), serde_val));
        // Assign default priority 5 only for new keys (don't override existing)
        if !self.priorities.contains_key(&key) {
            self.priorities.insert(key.clone(), 5);
            col.memory_priority_sets.push((key, 5));
        }
        Ok(())
    }

    fn __delitem__(&mut self, key: &str) -> PyResult<()> {
        self.data.remove(key);
        self.collector.lock().unwrap().memory_deletes.push(key.to_string());
        Ok(())
    }

    fn __contains__(&self, key: &str) -> bool {
        self.data.contains_key(key) || self.pinned.contains_key(key)
    }

    #[pyo3(signature = (key, default=None))]
    fn get<'py>(&self, py: Python<'py>, key: &str, default: Option<Py<PyAny>>) -> PyResult<Py<PyAny>> {
        if let Some(value) = self.data.get(key) {
            return Self::value_to_py(py, value);
        }
        if let Some(s) = self.pinned.get(key) {
            return Ok(s.as_str().into_pyobject(py)?.into_any().unbind());
        }
        Ok(default.unwrap_or_else(|| py.None().into()))
    }

    #[pyo3(signature = (key, value, priority=5))]
    fn set<'py>(&mut self, py: Python<'py>, key: String, value: &Bound<'py, PyAny>, priority: u8) -> PyResult<()> {
        let serde_val = Self::py_to_value(py, value)?;
        self.data.insert(key.clone(), serde_val.clone());
        self.priorities.insert(key.clone(), priority);
        let mut col = self.collector.lock().unwrap();
        col.memory_sets.push((key.clone(), serde_val));
        col.memory_priority_sets.push((key, priority));
        Ok(())
    }

    fn set_priority(&mut self, key: String, priority: u8) -> PyResult<()> {
        self.priorities.insert(key.clone(), priority);
        self.collector.lock().unwrap().memory_priority_sets.push((key, priority));
        Ok(())
    }

    fn get_priority(&self, key: &str) -> u8 {
        self.priorities.get(key).copied().unwrap_or(5)
    }

    /// Pin a key–value pair into the shared, cached tier. Pinned entries:
    /// - are injected into the system prompt (prompt-cached, cheap to keep)
    /// - are shared across all agents (parent, children, future sessions)
    /// - must be strings (they render as markdown in the system prompt)
    /// Use for stable facts: API endpoints, learned recipes, user prefs.
    fn pin(&mut self, key: String, value: String) -> PyResult<()> {
        self.pinned.insert(key.clone(), value.clone());
        self.collector.lock().unwrap().memory_pins.push((key, value));
        Ok(())
    }

    /// Remove a key from the pinned tier.
    fn unpin(&mut self, key: String) -> PyResult<()> {
        self.pinned.remove(&key);
        self.collector.lock().unwrap().memory_unpins.push(key);
        Ok(())
    }

    /// List pinned keys.
    fn list_pinned(&self) -> Vec<String> {
        let mut keys: Vec<String> = self.pinned.keys().cloned().collect();
        keys.sort();
        keys
    }

    fn __repr__(&self) -> String {
        format!("Memory({} keys, {} pinned)", self.data.len(), self.pinned.len())
    }
}

#[pyclass]
struct PyTimerManager {
    timers_info: Vec<(String, String, u8)>, // (id, description, priority)
    collector: Collector,
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
        let mut col = self.collector.lock().unwrap();
        let id = col.id_gen.next();
        let id_str = id.0.clone();
        col.timer_adds.push(TimerAddRequest {
            id,
            every_secs,
            at_epoch,
            priority,
            description,
        });
        Ok(id_str)
    }

    fn cancel(&self, timer_id: String) -> PyResult<()> {
        self.collector.lock().unwrap().timer_cancels.push(timer_id);
        Ok(())
    }

    fn list(&self) -> Vec<(String, String, u8)> {
        self.timers_info.clone()
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

#[pyclass]
struct PyHistoryManager {
    entries: HashMap<String, PyHistoryEntry>,
    collector: Collector,
    is_compaction: bool,
}

#[pymethods]
impl PyHistoryManager {
    fn __getitem__(&self, id: &str) -> PyResult<PyHistoryEntry> {
        self.entries
            .get(id)
            .cloned()
            .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err(format!("No history entry with id {}", id)))
    }

    fn replace_with_description(&self, id: String, description: String) -> PyResult<()> {
        self.collector
            .lock()
            .unwrap()
            .history_replaces
            .push((id, description));
        Ok(())
    }

    fn remove(&self, id: String) -> PyResult<()> {
        self.collector.lock().unwrap().history_removes.push(id);
        Ok(())
    }

    fn add(&self, text: String) -> PyResult<()> {
        if !self.is_compaction {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "history.add() can only be used during compaction",
            ));
        }
        self.collector.lock().unwrap().history_adds.push(text);
        Ok(())
    }
}

#[pyclass]
struct PyHarness {
    collector: Collector,
    process_outputs: HashMap<String, String>,
    process_statuses: HashMap<String, String>,
    /// (pid, cmd, description, status) for all tracked processes
    process_info: Vec<(String, String, String, String)>,
    child_depth_remaining: u32,
    agent_name: String,
    agent_lineage: String,
    harness_bin: String,
}

#[pymethods]
impl PyHarness {
    #[pyo3(signature = (chat_id, content, attach=vec![]))]
    fn send_message(&self, chat_id: String, content: String, attach: Vec<String>) -> PyResult<()> {
        for p in &attach {
            if !std::path::Path::new(p).is_file() {
                return Err(pyo3::exceptions::PyFileNotFoundError::new_err(
                    format!("send_message: attachment not found: {}", p),
                ));
            }
        }
        self.collector
            .lock()
            .unwrap()
            .messages
            .push(OutboundMessageRequest { chat_id, content, attachments: attach });
        Ok(())
    }

    #[pyo3(signature = (cmd, args=vec![], env=HashMap::new(), description="".to_string(), alert_timer=None, success_prio=7, fail_prio=8, block_for=None))]
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
    ) -> PyResult<String> {
        let alert_secs = match alert_timer {
            Some(val) => extract_seconds(val)? as u64,
            None => 300,
        };
        let block_for_ms = match block_for {
            Some(val) => Some((extract_seconds(val)? * 1000.0) as u64),
            None => None,
        };
        let mut col = self.collector.lock().unwrap();
        let id = col.id_gen.next();
        let id_str = id.0.clone();
        col.process_starts.push(ProcessStartRequest {
            id,
            cmd,
            args,
            env,
            description,
            alert_timer_secs: alert_secs,
            success_prio,
            fail_prio,
            block_for_ms,
        });
        Ok(id_str)
    }

    fn shell_status(&self, pid: String) -> PyResult<String> {
        Ok(self
            .process_statuses
            .get(&pid)
            .cloned()
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
        self.collector.lock().unwrap().process_kills.push(pid);
        Ok(())
    }

    fn processes_list(&self) -> Vec<(String, String, String, String)> {
        self.process_info.clone()
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
        self.collector.lock().unwrap().view_paths.extend(paths);
        Ok(())
    }

    fn acknowledge_timer(&self, timer_id: String) -> PyResult<()> {
        self.collector.lock().unwrap().timer_acks.push(timer_id);
        Ok(())
    }

    fn compact(&self) -> PyResult<()> {
        self.collector.lock().unwrap().compact_called = true;
        Ok(())
    }

    fn request_compaction(&self) -> PyResult<()> {
        self.collector.lock().unwrap().compaction_requested = true;
        Ok(())
    }

    /// Fork child agents. Takes a list of ChildSettings objects.
    fn fork<'py>(
        &self,
        _py: Python<'py>,
        children: &Bound<'py, PyAny>,
    ) -> PyResult<Vec<String>> {
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
            let task: String = item.getattr("task")?.extract()?;
            let model_obj = item.getattr("model")?;
            let model: Option<String> = if model_obj.is_none() {
                None
            } else {
                Some(model_obj.extract()?)
            };
            let max_turns: u32 = item.getattr("max_turns")?.extract::<u32>()?.min(50);
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

            names.push(name.clone());
            child_settings.push(ForkChildSettings {
                name,
                task,
                model,
                max_turns,
                can_compact,
                inherit_history,
                attach,
            });
        }

        self.collector.lock().unwrap().fork_requests.push(ForkRequest {
            children: child_settings,
        });

        Ok(names)
    }

    #[pyo3(signature = (name, content, priority=6))]
    fn message_agent(&self, name: String, content: String, priority: u8) -> PyResult<()> {
        self.collector.lock().unwrap().agent_messages.push(AgentMessageRequest {
            recipient: name,
            content,
            priority,
        });
        Ok(())
    }

    #[pyo3(signature = (**result))]
    fn done<'py>(&self, py: Python<'py>, result: Option<&Bound<'py, PyDict>>) -> PyResult<()> {
        let mut col = self.collector.lock().unwrap();
        col.done_called = true;
        if let Some(dict) = result {
            for (key, val) in dict.iter() {
                let key_str: String = key.extract()?;
                let json_mod = py.import("json")?;
                let json_str: String = json_mod.call_method1("dumps", (val,))?.extract()?;
                let serde_val: serde_json::Value = serde_json::from_str(&json_str)
                    .map_err(|e| pyo3::exceptions::PyValueError::new_err(
                        format!("done() kwargs must be JSON-serializable: {}", e)
                    ))?;
                col.done_result.insert(key_str, serde_val);
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

// ---- Python Preamble ----

const PREAMBLE: &str = r#"
from datetime import timedelta, datetime
from dataclasses import dataclass

@dataclass
class ChildSettings:
    name: str
    task: str
    model: str | None = None
    max_turns: int = 20
    can_compact: bool = True
    inherit_history: bool = True
    attach: list[str] | None = None

send_message = _harness.send_message
shell_exec = _harness.shell_exec
shell_status = _harness.shell_status
shell_output = _harness.shell_output
shell_kill = _harness.shell_kill
processes_list = _harness.processes_list
acknowledge_timer = _harness.acknowledge_timer
request_compaction = _harness.request_compaction
view = _harness.view
fork = _harness.fork
message_agent = _harness.message_agent
done = _harness.done
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
    execute_with_timeout(state, code, is_compaction, process_outputs, 5, 1, "root", "root", &HashMap::new())
}

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
) -> ExecutionResult {
    // Clone everything the thread needs (state is already Clone)
    let state = state.clone();
    let code = code.to_string();
    let process_outputs = process_outputs.clone();
    let agent_name = agent_name.to_string();
    let agent_lineage = agent_lineage.to_string();
    let pinned_memory = pinned_memory.clone();

    let (tx, rx) = std::sync::mpsc::sync_channel::<ExecutionResult>(1);

    std::thread::spawn(move || {
        let result = execute_inner(&state, &code, is_compaction, &process_outputs, child_depth_remaining, &agent_name, &agent_lineage, &pinned_memory);
        let _ = tx.send(result);
    });

    if timeout_secs == 0 {
        // No timeout — block indefinitely (used in tests)
        return rx.recv().unwrap_or_else(|_| ExecutionResult {
            stdout: String::new(),
            is_error: true,
            error_text: "Python execution thread panicked".to_string(),
            side_effects: SideEffectCollector::default(),
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
                        side_effects: SideEffectCollector::default(),
                    }
                }
            }
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            ExecutionResult {
                stdout: String::new(),
                is_error: true,
                error_text: "Python execution thread panicked".to_string(),
                side_effects: SideEffectCollector::default(),
            }
        }
    }
}

/// Inner execution function that runs on a dedicated thread.
fn execute_inner(
    state: &HarnessState,
    code: &str,
    is_compaction: bool,
    process_outputs: &HashMap<String, String>,
    child_depth_remaining: u32,
    agent_name: &str,
    agent_lineage: &str,
    pinned_memory: &HashMap<String, String>,
) -> ExecutionResult {
    let collector = Arc::new(Mutex::new(SideEffectCollector {
        id_gen: state.id_generator.clone(),
        ..Default::default()
    }));

    let stdout_buf = Arc::new(Mutex::new(String::new()));

    let result = Python::attach(|py| -> PyResult<()> {
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

        // Inject work queue
        let py_items: Vec<PyWorkItem> = state
            .work_queue
            .items()
            .iter()
            .map(work_item_to_py)
            .collect();
        let wq = Py::new(
            py,
            PyWorkQueue {
                items: py_items,
                collector: collector.clone(),
            },
        )?;
        locals.set_item("work_queue", wq)?;

        // Inject memory
        let mem = Py::new(
            py,
            PyMemory {
                data: state.memory.clone(),
                priorities: state.memory_priorities.clone(),
                pinned: pinned_memory.clone(),
                collector: collector.clone(),
            },
        )?;
        locals.set_item("memory", mem)?;

        // Inject timer manager
        let timers_info: Vec<(String, String, u8)> = state
            .timer_manager
            .list()
            .iter()
            .map(|t| (t.id.0.clone(), t.description.clone(), t.priority))
            .collect();
        let tm = Py::new(
            py,
            PyTimerManager {
                timers_info,
                collector: collector.clone(),
            },
        )?;
        locals.set_item("timers", tm)?;

        // Inject history manager
        let mut history_entries = HashMap::new();
        for entry in state.event_history.entries() {
            let (code_str, output_str, time_str) = match entry {
                HistoryEntry::Execution {
                    code, output, time, ..
                } => (
                    code.clone(),
                    output.clone(),
                    time.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
                ),
                HistoryEntry::Summary {
                    description, time, ..
                } => (
                    String::new(),
                    description.clone(),
                    time.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
                ),
                HistoryEntry::SystemAlert {
                    message, time, ..
                } => (
                    String::new(),
                    message.clone(),
                    time.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
                ),
            };
            history_entries.insert(
                entry.id().0.clone(),
                PyHistoryEntry {
                    code: code_str.clone(),
                    output: output_str.clone(),
                    full_output: output_str,
                    time: time_str,
                },
            );
        }
        let hm = Py::new(
            py,
            PyHistoryManager {
                entries: history_entries,
                collector: collector.clone(),
                is_compaction,
            },
        )?;
        locals.set_item("history", hm)?;

        // Inject harness (for send_message, shell_exec, etc.)
        let process_statuses: HashMap<String, String> = state
            .process_manager
            .processes()
            .iter()
            .map(|p| {
                let status = match &p.status {
                    ProcessStatus::Running => "running",
                    ProcessStatus::Completed { .. } => "completed",
                    ProcessStatus::Failed { .. } => "failed",
                };
                (p.id.0.clone(), status.to_string())
            })
            .collect();

        let process_info: Vec<(String, String, String, String)> = state
            .process_manager
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
            .collect();

        let harness = Py::new(
            py,
            PyHarness {
                collector: collector.clone(),
                process_outputs: process_outputs.clone(),
                process_statuses,
                process_info,
                child_depth_remaining,
                agent_name: agent_name.to_string(),
                agent_lineage: agent_lineage.to_string(),
                harness_bin: std::env::current_exe()
                    .map(|p| p.to_string_lossy().into_owned())
                    .unwrap_or_else(|_| "claude-server".into()),
            },
        )?;
        locals.set_item("_harness", harness)?;

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
                    collector
                        .lock()
                        .unwrap()
                        .compaction_script_appends
                        .push(script);
                }
            }
        }

        Ok(())
    });

    let stdout = stdout_buf.lock().unwrap().clone();
    let side_effects = match Arc::try_unwrap(collector) {
        Ok(mutex) => mutex.into_inner().unwrap(),
        Err(arc) => std::mem::take(&mut *arc.lock().unwrap()),
    };

    match result {
        Ok(()) => ExecutionResult {
            stdout,
            is_error: false,
            error_text: String::new(),
            side_effects,
        },
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
                side_effects: SideEffectCollector::default(),
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
        assert_eq!(result.side_effects.memory_sets.len(), 1);
        assert_eq!(result.side_effects.memory_sets[0].0, "key2");
        assert_eq!(result.side_effects.memory_sets[0].1, serde_json::json!("val2"));
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
        assert_eq!(result.side_effects.memory_sets.len(), 5);
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
        assert_eq!(result.side_effects.timer_adds.len(), 1);
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
        assert_eq!(result.side_effects.queue_removes.len(), 1);
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
        assert_eq!(result.side_effects.process_starts.len(), 1);
        assert_eq!(result.side_effects.memory_sets.len(), 1);
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
        assert_eq!(result.side_effects.messages.len(), 1);
        assert_eq!(result.side_effects.messages[0].chat_id, "chat1");
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
        assert_eq!(result.side_effects.timer_adds.len(), 1);
        assert!(result.side_effects.timer_adds[0].at_epoch.is_some());
        assert!(result.side_effects.timer_adds[0].every_secs.is_none());
        // datetime(2026, 2, 1, 17, 0, 0) should be a reasonable epoch
        let epoch = result.side_effects.timer_adds[0].at_epoch.unwrap();
        assert!(epoch > 1_700_000_000.0, "epoch {} too small", epoch);
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
        );
        assert!(!result.is_error, "Error: {}", result.error_text);
        assert_eq!(result.stdout.trim(), "all passed");
        assert_eq!(result.side_effects.memory_sets.len(), 2);
        // Priority sets: key1 default(5), key2 explicit(8), key1 updated(3)
        assert_eq!(result.side_effects.memory_priority_sets.len(), 3);
        assert_eq!(result.side_effects.memory_priority_sets[0], ("key1".to_string(), 5));
        assert_eq!(result.side_effects.memory_priority_sets[1], ("key2".to_string(), 8));
        assert_eq!(result.side_effects.memory_priority_sets[2], ("key1".to_string(), 3));
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
        );
        assert!(!result.is_error, "Error: {}", result.error_text);
        assert_eq!(result.stdout.trim(), "forked");
        assert_eq!(result.side_effects.fork_requests.len(), 1);
        let req = &result.side_effects.fork_requests[0];
        assert_eq!(req.children.len(), 2);
        assert_eq!(req.children[0].name, "test-runner");
        assert_eq!(req.children[0].task, "Write tests");
        assert_eq!(req.children[0].model, Some("claude-sonnet-4-5-20250929".to_string()));
        assert_eq!(req.children[0].max_turns, 10);
        assert_eq!(req.children[1].name, "linter");
        assert_eq!(req.children[1].task, "Run linting");
        assert_eq!(req.children[1].model, None);
        assert_eq!(req.children[1].max_turns, 20); // default
        assert!(req.children[1].can_compact); // default is true
        assert!(req.children[0].attach.is_empty()); // default: no attachments
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
        assert_eq!(result.side_effects.view_paths.len(), 1);
        assert_eq!(result.side_effects.view_paths[0], tmp.to_str().unwrap());

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
        );
        assert!(!result.is_error, "Error: {}", result.error_text);
        let req = &result.side_effects.fork_requests[0];
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
        assert_eq!(result.side_effects.timer_adds.len(), 2);
        assert_eq!(result.side_effects.timer_adds[0].every_secs, Some(30));
        assert_eq!(result.side_effects.timer_adds[1].every_secs, Some(60));
        assert_eq!(result.side_effects.process_starts.len(), 2);
        assert_eq!(result.side_effects.process_starts[0].alert_timer_secs, 300);
        assert_eq!(result.side_effects.process_starts[1].alert_timer_secs, 300);
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
        assert_eq!(result.side_effects.agent_messages.len(), 2);
        assert_eq!(result.side_effects.agent_messages[0].recipient, "sibling-a");
        assert_eq!(result.side_effects.agent_messages[0].content, "check the API status");
        assert_eq!(result.side_effects.agent_messages[0].priority, 6); // default
        assert_eq!(result.side_effects.agent_messages[1].recipient, "sibling-b");
        assert_eq!(result.side_effects.agent_messages[1].priority, 9);
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
        assert!(result.side_effects.compaction_requested);
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
        );
        assert!(!result.is_error, "Error: {}", result.error_text);
        assert!(result.stdout.contains("pinned: ['existing']"));
        assert!(result.stdout.contains("existing: old value"));
        assert!(result.stdout.contains("missing: None"));
        assert!(result.stdout.contains("contains: True"));
        assert_eq!(result.side_effects.memory_pins.len(), 2);
        assert_eq!(result.side_effects.memory_pins[0].0, "api_info");
        assert_eq!(result.side_effects.memory_pins[0].1, "HA API at :8123");
        assert_eq!(result.side_effects.memory_pins[1].0, "user_prefs");
        assert_eq!(result.side_effects.memory_unpins.len(), 1);
        assert_eq!(result.side_effects.memory_unpins[0], "existing");
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
        assert!(result.side_effects.done_called);
        assert_eq!(result.side_effects.done_result.len(), 3);
        assert_eq!(
            result.side_effects.done_result["verdict"],
            serde_json::json!("all clear")
        );
        assert_eq!(
            result.side_effects.done_result["confidence"],
            serde_json::json!(0.95)
        );
        assert_eq!(
            result.side_effects.done_result["details"],
            serde_json::json!({"camera": "front", "count": 5})
        );
    }

    #[test]
    fn test_done_no_args() {
        init();
        let state = HarnessState::new(200_000, 16384);
        let result = execute(&state, "done()", false, &HashMap::new());
        assert!(!result.is_error, "Error: {}", result.error_text);
        assert!(result.side_effects.done_called);
        assert!(result.side_effects.done_result.is_empty());
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
}
