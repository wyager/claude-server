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
    pub queue_removes: Vec<String>,
    pub timer_adds: Vec<TimerAddRequest>,
    pub timer_cancels: Vec<String>,
    pub filter_adds: Vec<QueueFilter>,
    pub filter_removes: Vec<String>,
    pub messages: Vec<OutboundMessageRequest>,
    pub process_starts: Vec<ProcessStartRequest>,
    pub process_kills: Vec<String>,
    pub history_removes: Vec<String>,
    pub history_replaces: Vec<(String, String)>,
    pub history_adds: Vec<String>,
    pub show_in_context: Vec<String>,
    pub compaction_script_appends: Vec<String>,
    pub compact_called: bool,
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

#[pyclass]
#[derive(Clone)]
struct PyWorkItem {
    id: String,
    priority: u8,
    time: String,
    item_type: String,
    chat_id: Option<String>,
    user: Option<String>,
    content: Option<String>,
    timer_id: Option<String>,
    every: Option<String>,
    description: Option<String>,
    pid: Option<String>,
    exit_code: Option<i32>,
    error: Option<String>,
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
    fn __getattr__(&self, name: &str) -> PyResult<PyObject> {
        Python::with_gil(|py| {
            // Handle "type" specially since it's a Python keyword
            if name == "type" {
                return Ok(self.item_type.as_str().into_pyobject(py)?.into_any().unbind());
            }
            let val = match name {
                "chat_id" => self.chat_id.as_deref(),
                "user" => self.user.as_deref(),
                "content" => self.content.as_deref(),
                "timer_id" => self.timer_id.as_deref(),
                "every" => self.every.as_deref(),
                "description" => self.description.as_deref(),
                "pid" => self.pid.as_deref(),
                "error" => self.error.as_deref(),
                _ => {
                    return Err(PyAttributeError::new_err(format!(
                        "'WorkItem' has no attribute '{}'",
                        name
                    )))
                }
            };
            match val {
                Some(s) => Ok(s.into_pyobject(py)?.into_any().unbind()),
                None => match name {
                    "exit_code" => match self.exit_code {
                        Some(c) => Ok(c.into_pyobject(py)?.into_any().unbind()),
                        None => Err(PyAttributeError::new_err(format!(
                            "This {} item has no '{}' field",
                            self.item_type, name
                        ))),
                    },
                    _ => Err(PyAttributeError::new_err(format!(
                        "This {} item has no '{}' field",
                        self.item_type, name
                    ))),
                },
            }
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "WorkItem(id='{}', type='{}', priority={})",
            self.id, self.item_type, self.priority
        )
    }
}

fn work_item_to_py(item: &WorkItem) -> PyWorkItem {
    let time_str = item.time.format("%Y-%m-%d %H:%M:%S UTC").to_string();
    let (item_type, chat_id, user, content, timer_id, every, description, pid, exit_code, error) =
        match &item.item_type {
            WorkItemType::UserMessage {
                chat_id,
                user,
                content,
            } => (
                "UserMessage",
                Some(chat_id.clone()),
                Some(user.clone()),
                Some(content.clone()),
                None,
                None,
                None,
                None,
                None,
                None,
            ),
            WorkItemType::TimerFired {
                timer_id,
                every,
                description,
            } => (
                "TimerFired",
                None,
                None,
                None,
                Some(timer_id.0.clone()),
                every.map(|d| format!("{}s", d.as_secs())),
                Some(description.clone()),
                None,
                None,
                None,
            ),
            WorkItemType::ProcessCompleted { pid, exit_code } => (
                "ProcessCompleted",
                None,
                None,
                None,
                None,
                None,
                None,
                Some(pid.0.clone()),
                Some(*exit_code),
                None,
            ),
            WorkItemType::ProcessFailed { pid, error } => (
                "ProcessFailed",
                None,
                None,
                None,
                None,
                None,
                None,
                Some(pid.0.clone()),
                None,
                Some(error.clone()),
            ),
            WorkItemType::ProcessTimeout { pid } => (
                "ProcessTimeout",
                None,
                None,
                None,
                None,
                None,
                None,
                Some(pid.0.clone()),
                None,
                None,
            ),
            WorkItemType::Compaction => (
                "Compaction",
                None,
                None,
                None,
                None,
                None,
                Some("You must compact your context.".to_string()),
                None,
                None,
                None,
            ),
        };

    PyWorkItem {
        id: item.id.0.clone(),
        priority: item.priority,
        time: time_str,
        item_type: item_type.to_string(),
        chat_id,
        user,
        content,
        timer_id,
        every,
        description,
        pid,
        exit_code,
        error,
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
    collector: Collector,
}

impl PyMemory {
    /// Convert a serde_json::Value back to a Python object via json.loads
    fn value_to_py<'py>(py: Python<'py>, value: &serde_json::Value) -> PyResult<PyObject> {
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
    fn __getitem__<'py>(&self, py: Python<'py>, key: &str) -> PyResult<PyObject> {
        let value = self.data.get(key)
            .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err(key.to_string()))?;
        Self::value_to_py(py, value)
    }

    fn __setitem__<'py>(&mut self, py: Python<'py>, key: String, value: &Bound<'py, PyAny>) -> PyResult<()> {
        let serde_val = Self::py_to_value(py, value)?;
        self.data.insert(key.clone(), serde_val.clone());
        self.collector.lock().unwrap().memory_sets.push((key, serde_val));
        Ok(())
    }

    fn __delitem__(&mut self, key: &str) -> PyResult<()> {
        self.data.remove(key);
        self.collector.lock().unwrap().memory_deletes.push(key.to_string());
        Ok(())
    }

    fn __contains__(&self, key: &str) -> bool {
        self.data.contains_key(key)
    }

    #[pyo3(signature = (key, default=None))]
    fn get<'py>(&self, py: Python<'py>, key: &str, default: Option<PyObject>) -> PyResult<PyObject> {
        match self.data.get(key) {
            Some(value) => Self::value_to_py(py, value),
            None => Ok(default.unwrap_or_else(|| py.None().into())),
        }
    }

    fn __repr__(&self) -> String {
        format!("Memory({} keys)", self.data.len())
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

#[pyclass]
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
    fn __getitem__(&self, id: &str) -> PyResult<PyRef<'_, PyHistoryEntry>> {
        Err(pyo3::exceptions::PyKeyError::new_err(format!(
            "Direct __getitem__ not supported; use history entries by ID"
        )))
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
}

#[pymethods]
impl PyHarness {
    fn send_message(&self, chat_id: String, content: String) -> PyResult<()> {
        self.collector
            .lock()
            .unwrap()
            .messages
            .push(OutboundMessageRequest { chat_id, content });
        Ok(())
    }

    #[pyo3(signature = (cmd, args=vec![], env=HashMap::new(), description="".to_string(), alert_timer=None, success_prio=5, fail_prio=7))]
    fn shell_exec<'py>(
        &self,
        cmd: String,
        args: Vec<String>,
        env: HashMap<String, String>,
        description: String,
        alert_timer: Option<&Bound<'py, PyAny>>,
        success_prio: u8,
        fail_prio: u8,
    ) -> PyResult<String> {
        let alert_secs = match alert_timer {
            Some(val) => extract_seconds(val)? as u64,
            None => 300,
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

    fn shell_output(&self, pid: String) -> PyResult<String> {
        Ok(self
            .process_outputs
            .get(&pid)
            .cloned()
            .unwrap_or_default())
    }

    fn shell_kill(&self, pid: String) -> PyResult<()> {
        self.collector.lock().unwrap().process_kills.push(pid);
        Ok(())
    }

    fn processes_list(&self) -> Vec<(String, String, String, String)> {
        self.process_info.clone()
    }

    fn show_in_context(&self, data: String) -> PyResult<()> {
        self.collector.lock().unwrap().show_in_context.push(data);
        Ok(())
    }

    fn compact(&self) -> PyResult<()> {
        self.collector.lock().unwrap().compact_called = true;
        Ok(())
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
send_message = _harness.send_message
shell_exec = _harness.shell_exec
shell_status = _harness.shell_status
shell_output = _harness.shell_output
shell_kill = _harness.shell_kill
processes_list = _harness.processes_list
show_in_context = _harness.show_in_context
"#;

const COMPACTION_PREAMBLE: &str = r#"
compact = _harness.compact
compaction_script = ""
"#;

// ---- Executor ----

pub fn initialize_python() {
    pyo3::prepare_freethreaded_python();
}

pub fn execute(
    state: &HarnessState,
    code: &str,
    is_compaction: bool,
    process_outputs: &HashMap<String, String>,
) -> ExecutionResult {
    let collector = Arc::new(Mutex::new(SideEffectCollector {
        id_gen: state.id_generator.clone(),
        ..Default::default()
    }));

    let stdout_buf = Arc::new(Mutex::new(String::new()));

    let result = Python::with_gil(|py| -> PyResult<()> {
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
            let error_text = Python::with_gil(|py| {
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
}
