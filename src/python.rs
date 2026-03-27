//! RustPython-based Python executor.
//!
//! Zero libpython linkage — stdlib is frozen into the binary via the
//! `freeze-stdlib` feature. Replaces the previous PyO3 implementation.
//!
//! Architecture:
//!
//! - `Executor` owns the `vm::Interpreter` directly as a struct field
//!   to avoid RustPython's Drop-panic on the PyObjectRef graph at teardown).
//!   No global singleton, no thread-locals.
//!
//! - Each `#[pyclass]` carries its per-turn state as `Mutex<T>` fields. At
//!   the top of `execute()` the pyclass instances are built with the turn's
//!   data and injected into the Python scope as `_memory`, `_work_queue`, etc.
//!   The PREAMBLE wraps them with thin Python classes that provide dunders.
//!
//! - All top-level "functions" are `#[pymethod]`s on `PyHarness`. Since
//!   RustPython's `#[pymethod]` macro doesn't expose parameter names for
//!   kwargs dispatch (unlike `#[pyfunction]`), the PREAMBLE wraps each
//!   method with a named-parameter Python `def`.
//!
//! - Timeout uses RustPython's `vm::signal::user_signal_channel()`. The
//!   receiver is wired into the VM via `.init_hook()`; the sender is stored
//!   in `Executor.signal_tx`. A watchdog thread races a cancel channel against
//!   the timeout; on timeout it sends a closure that raises KeyboardInterrupt.
//!
//! - libffi cannot be dropped: rustpython-stdlib at rev 3f92c3a has an
//!   unconditional `libffi` dep. Dropping it would require forking RustPython.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::{mpsc, Arc, Mutex};

use rustpython::InterpreterBuilderExt;
use rustpython_vm as vm;
use vm::builtins::{PyBaseExceptionRef, PyDict, PyList, PyStrRef};
use vm::function::{ArgIntoFloat, FuncArgs, KwArgs, OptionalArg, PosArgs};
use vm::{
    pymodule, PyObjectRef, PyPayload, PyRef, PyResult, TryFromObject, VirtualMachine,
};

use crate::types::*;

// ---- Side Effect Collection ----

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
    pub stdin_writes: Vec<(String, Vec<u8>)>,
    pub stdin_closes: Vec<String>,
    pub child_kills: Vec<String>,
    pub history_removes: Vec<String>,
    pub history_replaces: Vec<(String, String)>,
    pub history_adds: Vec<String>,
    pub view_paths: Vec<String>,
    pub fork_requests: Vec<ForkRequest>,
    pub agent_messages: Vec<AgentMessageRequest>,
    pub compaction_script_appends: Vec<String>,
    pub compact_called: bool,
    pub compaction_requested: bool,
    pub done_called: bool,
    pub done_result: HashMap<String, serde_json::Value>,
    pub memory_pins: Vec<(String, String)>,
    pub sensitive_marks: Vec<(String, bool)>,
    pub memory_unpins: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ForkChildSettings {
    pub name: String,
    pub task: String,
    pub model: Option<String>,
    pub max_turns: Option<u32>,
    pub can_compact: bool,
    pub inherit_history: bool,
    pub attach: Vec<String>,
    pub prefix_context: Option<String>,
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
pub struct TimerAddRequest {
    pub id: AgentId,
    pub every_secs: Option<u64>,
    pub at_epoch: Option<f64>,
    pub priority: u8,
    pub description: String,
}

#[derive(Debug)]
pub struct OutboundMessageRequest {
    pub chat_id: String,
    pub content: String,
    pub attachments: Vec<String>,
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
    pub success_prio: u8,
    pub fail_prio: u8,
    pub block_for_ms: Option<u64>,
    pub interactive: bool,
}

pub struct ExecutionResult {
    pub stdout: String,
    pub is_error: bool,
    pub error_text: String,
    pub side_effects: SideEffectCollector,
}

// ---- Per-turn data carried by pyclass instances ----

#[derive(Debug, Clone)]
struct WorkItemData {
    id: String,
    priority: u8,
    time: String,
    item_type: String,
    fields: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone)]
struct HistoryEntryData {
    code: String,
    output: String,
    full_output: String,
    time: String,
}

#[derive(Debug, Default)]
struct MemoryState {
    data: HashMap<String, serde_json::Value>,
    priorities: HashMap<String, u8>,
    pinned: HashMap<String, String>,
}

#[derive(Debug, Default)]
struct HarnessInfo {
    process_outputs: HashMap<String, String>,
    process_statuses: HashMap<String, String>,
    process_info: Vec<(String, String, String, String)>,
    child_depth_remaining: u32,
    agent_name: String,
    agent_lineage: String,
    harness_bin: String,
    is_compaction: bool,
}

// ---- Helpers ----

/// Extract a duration in seconds from a number (int/float) or datetime.timedelta.
fn extract_seconds(vm: &VirtualMachine, val: &PyObjectRef) -> PyResult<f64> {
    if let Ok(f) = ArgIntoFloat::try_from_object(vm, val.clone()) {
        return Ok(f.into_float());
    }
    if let Ok(ts) = vm.call_method(val, "total_seconds", ()) {
        return ArgIntoFloat::try_from_object(vm, ts).map(|f| f.into_float());
    }
    Err(vm.new_type_error("expected a number (seconds) or datetime.timedelta"))
}

/// Convert a serde_json::Value to a Python object via json.loads.
fn json_to_py(vm: &VirtualMachine, value: &serde_json::Value) -> PyResult<PyObjectRef> {
    let json_str = serde_json::to_string(value)
        .map_err(|e| vm.new_value_error(format!("json serialize error: {}", e)))?;
    let json_mod = vm.import("json", 0)?;
    let loads = json_mod.get_attr("loads", vm)?;
    loads.call((json_str,), vm)
}

/// Convert a Python object to serde_json::Value via json.dumps.
fn py_to_json(vm: &VirtualMachine, value: &PyObjectRef) -> PyResult<serde_json::Value> {
    let json_mod = vm.import("json", 0)?;
    let dumps = json_mod.get_attr("dumps", vm)?;
    let json_str: String = dumps.call((value.clone(),), vm)?.try_into_value(vm)?;
    serde_json::from_str(&json_str)
        .map_err(|e| vm.new_value_error(format!("cannot serialize: {}", e)))
}

/// Extract a Vec<String> from a Python list/tuple/None.
fn extract_str_list(vm: &VirtualMachine, obj: &PyObjectRef) -> PyResult<Vec<String>> {
    if vm.is_none(obj) {
        return Ok(Vec::new());
    }
    vm.extract_elements_with(obj, |o| String::try_from_object(vm, o))
}

/// Raise FileNotFoundError. OSError subtypes carry extra payload, so
/// `vm.new_exception_msg()` panics on them; go through the OSError helper
/// and downcast to the base exception ref.
fn file_not_found(vm: &VirtualMachine, msg: String) -> PyBaseExceptionRef {
    let os_err = vm.new_os_subtype_error(
        vm.ctx.exceptions.file_not_found_error.to_owned(),
        None,
        msg,
    );
    let obj: PyObjectRef = os_err.into();
    obj.downcast().expect("PyOSError is a PyBaseException")
}

// ---- WorkItem → data conversion (pure Rust) ----

fn work_item_to_data(item: &WorkItem) -> WorkItemData {
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
    };

    fields.insert("attachments".into(), item.attachments.clone().into());

    WorkItemData {
        id: item.id.0.clone(),
        priority: item.priority,
        time: time_str,
        item_type: item_type.to_string(),
        fields,
    }
}

// ---- #[pymodule] ----
//
// Types are registered statically; instances carry per-turn state in Mutex
// fields and are built fresh each turn via `PyPayload::into_ref()`.

#[pymodule(name = "_harness")]
mod harness {
    use super::*;
    use rustpython_vm::pyclass;

    // ------ PyWorkItem ------

    #[pyattr]
    #[pyclass(module = "_harness", name = "WorkItem")]
    #[derive(Debug, PyPayload, Default)]
    pub struct PyWorkItem {
        pub data: Mutex<Option<WorkItemData>>,
    }

    #[pyclass]
    impl PyWorkItem {
        fn d(&self) -> WorkItemData {
            self.data.lock().unwrap().clone().expect("uninitialized WorkItem")
        }

        #[pygetset]
        fn id(&self) -> String { self.d().id }
        #[pygetset]
        fn priority(&self) -> u8 { self.d().priority }
        #[pygetset]
        fn time(&self) -> String { self.d().time }

        #[pymethod]
        fn _field(&self, name: PyStrRef, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
            let d = self.d();
            let name: &str = name.as_ref();
            if name == "type" {
                return Ok(vm.ctx.new_str(d.item_type).into());
            }
            match d.fields.get(name) {
                Some(val) => json_to_py(vm, val),
                None => Err(vm.new_attribute_error(format!(
                    "{} work item has no field '{}'. Available fields: {}",
                    d.item_type, name,
                    d.fields.keys().cloned().collect::<Vec<_>>().join(", ")
                ))),
            }
        }

        #[pymethod]
        fn _repr(&self) -> String {
            let d = self.d();
            format!("WorkItem(id='{}', type='{}', priority={})", d.id, d.item_type, d.priority)
        }
    }

    // ------ PyWorkQueue ------

    #[pyattr]
    #[pyclass(module = "_harness", name = "WorkQueue")]
    #[derive(Debug, PyPayload, Default)]
    pub struct PyWorkQueue {
        pub items: Mutex<Vec<WorkItemData>>,
        pub collector: Mutex<Option<Arc<Mutex<SideEffectCollector>>>>,
    }

    impl PyWorkQueue {
        fn col(&self) -> Arc<Mutex<SideEffectCollector>> {
            self.collector.lock().unwrap().clone().expect("collector not set")
        }
    }

    #[pyclass]
    impl PyWorkQueue {
        #[pymethod]
        fn _getitem(&self, index: usize, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
            let items = self.items.lock().unwrap();
            items.get(index)
                .map(|d| PyWorkItem { data: Mutex::new(Some(d.clone())) }.into_pyobject(vm))
                .ok_or_else(|| vm.new_index_error("work queue index out of range"))
        }

        #[pymethod]
        fn _len(&self) -> usize {
            self.items.lock().unwrap().len()
        }

        #[pymethod]
        fn pop_front(&self, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
            let mut items = self.items.lock().unwrap();
            if items.is_empty() {
                return Ok(vm.ctx.none());
            }
            let item = items.remove(0);
            self.col().lock().unwrap().queue_removes.push(item.id.clone());
            Ok(PyWorkItem { data: Mutex::new(Some(item)) }.into_pyobject(vm))
        }

        #[pymethod]
        fn remove(&self, id: String) -> PyResult<()> {
            self.items.lock().unwrap().retain(|i| i.id != id);
            self.col().lock().unwrap().queue_removes.push(id);
            Ok(())
        }

        #[pymethod]
        fn add_filter(&self, name: String, regex: String) -> PyResult<()> {
            self.col().lock().unwrap().filter_adds.push(QueueFilter { name, regex });
            Ok(())
        }

        #[pymethod]
        fn remove_filter(&self, name: String) -> PyResult<()> {
            self.col().lock().unwrap().filter_removes.push(name);
            Ok(())
        }
    }

    // ------ PyMemory ------

    #[pyattr]
    #[pyclass(module = "_harness", name = "Memory")]
    #[derive(Debug, PyPayload, Default)]
    pub struct PyMemory {
        pub state: Mutex<MemoryState>,
        pub collector: Mutex<Option<Arc<Mutex<SideEffectCollector>>>>,
    }

    impl PyMemory {
        fn col(&self) -> Arc<Mutex<SideEffectCollector>> {
            self.collector.lock().unwrap().clone().expect("collector not set")
        }
    }

    #[pyclass]
    impl PyMemory {
        #[pymethod]
        fn _getitem(&self, key: PyStrRef, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
            let key: &str = key.as_ref();
            let state = self.state.lock().unwrap();
            match state.data.get(key) {
                Some(val) => json_to_py(vm, val),
                None => Err(vm.new_key_error(vm.ctx.new_str(key.to_owned()).into())),
            }
        }

        #[pymethod]
        fn _setitem(&self, key: String, value: PyObjectRef, vm: &VirtualMachine) -> PyResult<()> {
            let serde_val = py_to_json(vm, &value)?;
            let mut state = self.state.lock().unwrap();
            state.data.insert(key.clone(), serde_val.clone());
            let col = self.col();
            let mut c = col.lock().unwrap();
            c.memory_sets.push((key.clone(), serde_val));
            if !state.priorities.contains_key(&key) {
                state.priorities.insert(key.clone(), 5);
                c.memory_priority_sets.push((key, 5));
            }
            Ok(())
        }

        #[pymethod]
        fn _delitem(&self, key: String) -> PyResult<()> {
            self.state.lock().unwrap().data.remove(&key);
            self.col().lock().unwrap().memory_deletes.push(key);
            Ok(())
        }

        #[pymethod]
        fn _contains(&self, key: PyStrRef) -> bool {
            let key: &str = key.as_ref();
            let state = self.state.lock().unwrap();
            state.data.contains_key(key) || state.pinned.contains_key(key)
        }

        #[pymethod]
        fn get(&self, key: PyStrRef, default: OptionalArg<PyObjectRef>, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
            let key: &str = key.as_ref();
            let state = self.state.lock().unwrap();
            if let Some(val) = state.data.get(key) {
                return json_to_py(vm, val);
            }
            if let Some(s) = state.pinned.get(key) {
                return Ok(vm.ctx.new_str(s.clone()).into());
            }
            Ok(default.unwrap_or_else(|| vm.ctx.none()))
        }

        #[pymethod]
        fn set(&self, key: String, value: PyObjectRef, priority: OptionalArg<u8>, vm: &VirtualMachine) -> PyResult<()> {
            let priority = priority.unwrap_or(5);
            let serde_val = py_to_json(vm, &value)?;
            let mut state = self.state.lock().unwrap();
            state.data.insert(key.clone(), serde_val.clone());
            state.priorities.insert(key.clone(), priority);
            let col = self.col();
            let mut c = col.lock().unwrap();
            c.memory_sets.push((key.clone(), serde_val));
            c.memory_priority_sets.push((key, priority));
            Ok(())
        }

        #[pymethod]
        fn set_priority(&self, key: String, priority: u8) -> PyResult<()> {
            self.state.lock().unwrap().priorities.insert(key.clone(), priority);
            self.col().lock().unwrap().memory_priority_sets.push((key, priority));
            Ok(())
        }

        #[pymethod]
        fn get_priority(&self, key: PyStrRef) -> u8 {
            let key: &str = key.as_ref();
            self.state.lock().unwrap().priorities.get(key).copied().unwrap_or(5)
        }

        #[pymethod]
        fn pin(&self, key: String, value: String) -> PyResult<()> {
            self.state.lock().unwrap().pinned.insert(key.clone(), value.clone());
            self.col().lock().unwrap().memory_pins.push((key, value));
            Ok(())
        }

        #[pymethod]
        fn unpin(&self, key: String) -> PyResult<()> {
            self.state.lock().unwrap().pinned.remove(&key);
            self.col().lock().unwrap().memory_unpins.push(key);
            Ok(())
        }

        #[pymethod]
        fn list_pinned(&self, vm: &VirtualMachine) -> PyObjectRef {
            let mut keys: Vec<String> = self.state.lock().unwrap().pinned.keys().cloned().collect();
            keys.sort();
            let objs: Vec<PyObjectRef> = keys.into_iter().map(|k| vm.ctx.new_str(k).into()).collect();
            vm.ctx.new_list(objs).into()
        }

        #[pymethod]
        fn mark_sensitive(&self, key: String) -> PyResult<()> {
            self.col().lock().unwrap().sensitive_marks.push((key, true));
            Ok(())
        }

        #[pymethod]
        fn unmark_sensitive(&self, key: String) -> PyResult<()> {
            self.col().lock().unwrap().sensitive_marks.push((key, false));
            Ok(())
        }

        #[pymethod]
        fn _repr(&self) -> String {
            let state = self.state.lock().unwrap();
            format!("Memory({} keys, {} pinned)", state.data.len(), state.pinned.len())
        }
    }

    // ------ PyTimerManager ------

    #[pyattr]
    #[pyclass(module = "_harness", name = "TimerManager")]
    #[derive(Debug, PyPayload, Default)]
    pub struct PyTimerManager {
        pub timers_info: Mutex<Vec<(String, String, u8)>>,
        pub collector: Mutex<Option<Arc<Mutex<SideEffectCollector>>>>,
    }

    impl PyTimerManager {
        fn col(&self) -> Arc<Mutex<SideEffectCollector>> {
            self.collector.lock().unwrap().clone().expect("collector not set")
        }
    }

    #[pyclass]
    impl PyTimerManager {
        #[pymethod]
        fn add(&self, args: FuncArgs, vm: &VirtualMachine) -> PyResult<String> {
            let mut args = args;
            let every = args.take_keyword("every");
            let at = args.take_keyword("at");
            let priority: u8 = args.take_keyword("priority")
                .map(|o| u8::try_from_object(vm, o))
                .transpose()?.unwrap_or(5);
            let description: String = args.take_keyword("description")
                .map(|o| String::try_from_object(vm, o))
                .transpose()?.unwrap_or_default();

            let every_secs = match every {
                Some(val) if !vm.is_none(&val) => Some(extract_seconds(vm, &val)? as u64),
                _ => None,
            };
            let at_epoch = match at {
                Some(val) if !vm.is_none(&val) => {
                    if let Ok(ts) = vm.call_method(&val, "timestamp", ()) {
                        Some(ArgIntoFloat::try_from_object(vm, ts)?.into_float())
                    } else if let Ok(f) = ArgIntoFloat::try_from_object(vm, val.clone()) {
                        Some(f.into_float())
                    } else {
                        return Err(vm.new_type_error("expected a datetime object or numeric epoch for 'at'"));
                    }
                }
                _ => None,
            };

            let col = self.col();
            let mut c = col.lock().unwrap();
            let id = c.id_gen.next();
            let id_str = id.0.clone();
            c.timer_adds.push(TimerAddRequest { id, every_secs, at_epoch, priority, description });
            Ok(id_str)
        }

        #[pymethod]
        fn cancel(&self, timer_id: String) -> PyResult<()> {
            self.col().lock().unwrap().timer_cancels.push(timer_id);
            Ok(())
        }

        #[pymethod]
        fn list(&self, vm: &VirtualMachine) -> PyObjectRef {
            let items: Vec<PyObjectRef> = self.timers_info.lock().unwrap().iter()
                .map(|(id, desc, prio)| {
                    vm.ctx.new_tuple(vec![
                        vm.ctx.new_str(id.clone()).into(),
                        vm.ctx.new_str(desc.clone()).into(),
                        vm.ctx.new_int(*prio).into(),
                    ]).into()
                }).collect();
            vm.ctx.new_list(items).into()
        }
    }

    // ------ PyHistoryEntry ------

    #[pyattr]
    #[pyclass(module = "_harness", name = "HistoryEntry")]
    #[derive(Debug, PyPayload, Default)]
    pub struct PyHistoryEntry {
        pub data: Mutex<Option<HistoryEntryData>>,
    }

    #[pyclass]
    impl PyHistoryEntry {
        fn d(&self) -> HistoryEntryData {
            self.data.lock().unwrap().clone().expect("uninitialized HistoryEntry")
        }
        #[pygetset]
        fn code(&self) -> String { self.d().code }
        #[pygetset]
        fn output(&self) -> String { self.d().output }
        #[pygetset]
        fn full_output(&self) -> String { self.d().full_output }
        #[pygetset]
        fn time(&self) -> String { self.d().time }
    }

    // ------ PyHistoryManager ------

    #[pyattr]
    #[pyclass(module = "_harness", name = "HistoryManager")]
    #[derive(Debug, PyPayload, Default)]
    pub struct PyHistoryManager {
        pub entries: Mutex<HashMap<String, HistoryEntryData>>,
        pub is_compaction: Mutex<bool>,
        pub collector: Mutex<Option<Arc<Mutex<SideEffectCollector>>>>,
    }

    impl PyHistoryManager {
        fn col(&self) -> Arc<Mutex<SideEffectCollector>> {
            self.collector.lock().unwrap().clone().expect("collector not set")
        }
    }

    #[pyclass]
    impl PyHistoryManager {
        #[pymethod]
        fn _getitem(&self, id: PyStrRef, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
            let id: &str = id.as_ref();
            let entries = self.entries.lock().unwrap();
            entries.get(id)
                .map(|d| PyHistoryEntry { data: Mutex::new(Some(d.clone())) }.into_pyobject(vm))
                .ok_or_else(|| vm.new_key_error(
                    vm.ctx.new_str(format!("No history entry with id {}", id)).into()
                ))
        }

        #[pymethod]
        fn replace_with_description(&self, id: String, description: String) -> PyResult<()> {
            self.col().lock().unwrap().history_replaces.push((id, description));
            Ok(())
        }

        #[pymethod]
        fn remove(&self, id: String) -> PyResult<()> {
            self.col().lock().unwrap().history_removes.push(id);
            Ok(())
        }

        #[pymethod]
        fn add(&self, text: String, vm: &VirtualMachine) -> PyResult<()> {
            if !*self.is_compaction.lock().unwrap() {
                return Err(vm.new_runtime_error("history.add() can only be used during compaction"));
            }
            self.col().lock().unwrap().history_adds.push(text);
            Ok(())
        }
    }

    // ------ PyHarness (top-level functions, exposed as methods) ------

    #[pyattr]
    #[pyclass(module = "_harness", name = "Harness")]
    #[derive(Debug, PyPayload, Default)]
    pub struct PyHarness {
        pub info: Mutex<HarnessInfo>,
        pub collector: Mutex<Option<Arc<Mutex<SideEffectCollector>>>>,
    }

    impl PyHarness {
        fn col(&self) -> Arc<Mutex<SideEffectCollector>> {
            self.collector.lock().unwrap().clone().expect("collector not set")
        }
    }

    #[pyclass]
    impl PyHarness {
        #[pymethod]
        fn send_message(
            &self,
            chat_id: String,
            content: String,
            attach: OptionalArg<PyObjectRef>,
            react_to: OptionalArg<Option<String>>,
            vm: &VirtualMachine,
        ) -> PyResult<()> {
            let attach: Vec<String> = match attach {
                OptionalArg::Present(o) => extract_str_list(vm, &o)?,
                OptionalArg::Missing => Vec::new(),
            };
            for p in &attach {
                if !std::path::Path::new(p).is_file() {
                    return Err(file_not_found(vm, format!("send_message: attachment not found: {}", p)));
                }
            }
            let react_to = react_to.flatten();
            self.col().lock().unwrap().messages.push(OutboundMessageRequest { chat_id, content, attachments: attach, react_to });
            Ok(())
        }

        #[pymethod]
        fn shell_exec(&self, args: FuncArgs, vm: &VirtualMachine) -> PyResult<String> {
            let mut args = args;
            let cmd: String = args.take_positional_keyword("cmd")
                .map(|o| String::try_from_object(vm, o))
                .transpose()?
                .ok_or_else(|| vm.new_type_error("shell_exec() missing required argument: 'cmd'"))?;
            let proc_args: Vec<String> = args.take_positional_keyword("args")
                .map(|o| extract_str_list(vm, &o))
                .transpose()?.unwrap_or_default();
            let env: HashMap<String, String> = match args.take_keyword("env") {
                Some(o) if !vm.is_none(&o) => {
                    let dict: PyRef<PyDict> = o.try_into_value(vm)?;
                    let mut m = HashMap::new();
                    for (k, v) in dict {
                        m.insert(String::try_from_object(vm, k)?, String::try_from_object(vm, v)?);
                    }
                    m
                }
                _ => HashMap::new(),
            };
            let description: String = args.take_keyword("description")
                .map(|o| String::try_from_object(vm, o))
                .transpose()?.unwrap_or_default();
            let alert_secs = match args.take_keyword("alert_timer") {
                Some(o) if !vm.is_none(&o) => extract_seconds(vm, &o)? as u64,
                _ => 300,
            };
            let success_prio: u8 = args.take_keyword("success_prio")
                .map(|o| u8::try_from_object(vm, o))
                .transpose()?.unwrap_or(7);
            let fail_prio: u8 = args.take_keyword("fail_prio")
                .map(|o| u8::try_from_object(vm, o))
                .transpose()?.unwrap_or(8);
            let block_for_ms = match args.take_keyword("block_for") {
                Some(o) if !vm.is_none(&o) => Some((extract_seconds(vm, &o)? * 1000.0) as u64),
                _ => None,
            };
            let interactive: bool = args.take_keyword("interactive")
                .map(|o| bool::try_from_object(vm, o))
                .transpose()?.unwrap_or(false);

            let col = self.col();
            let mut c = col.lock().unwrap();
            let id = c.id_gen.next();
            let id_str = id.0.clone();
            c.process_starts.push(ProcessStartRequest {
                id, cmd, args: proc_args, env, description,
                alert_timer_secs: alert_secs, success_prio, fail_prio, block_for_ms, interactive,
            });
            Ok(id_str)
        }

        #[pymethod]
        fn shell_input(&self, pid: String, data: String) -> PyResult<()> {
            self.col().lock().unwrap().stdin_writes.push((pid, data.into_bytes()));
            Ok(())
        }

        #[pymethod]
        fn shell_close_stdin(&self, pid: String) -> PyResult<()> {
            self.col().lock().unwrap().stdin_closes.push(pid);
            Ok(())
        }

        #[pymethod]
        fn kill_child(&self, name: String) -> PyResult<()> {
            self.col().lock().unwrap().child_kills.push(name);
            Ok(())
        }

        #[pymethod]
        fn shell_status(&self, pid: String) -> String {
            self.info.lock().unwrap().process_statuses.get(&pid).cloned().unwrap_or_else(|| "unknown".to_string())
        }

        #[pymethod]
        fn shell_output(&self, pid: String, lines: OptionalArg<usize>) -> String {
            let full = self.info.lock().unwrap().process_outputs.get(&pid).cloned().unwrap_or_default();
            match lines {
                OptionalArg::Present(n) => {
                    let all: Vec<&str> = full.lines().collect();
                    all[all.len().saturating_sub(n)..].join("\n")
                }
                OptionalArg::Missing => full,
            }
        }

        #[pymethod]
        fn shell_kill(&self, pid: String) -> PyResult<()> {
            self.col().lock().unwrap().process_kills.push(pid);
            Ok(())
        }

        #[pymethod]
        fn processes_list(&self, vm: &VirtualMachine) -> PyObjectRef {
            let items: Vec<PyObjectRef> = self.info.lock().unwrap().process_info.iter()
                .map(|(pid, cmd, desc, status)| {
                    vm.ctx.new_tuple(vec![
                        vm.ctx.new_str(pid.clone()).into(),
                        vm.ctx.new_str(cmd.clone()).into(),
                        vm.ctx.new_str(desc.clone()).into(),
                        vm.ctx.new_str(status.clone()).into(),
                    ]).into()
                }).collect();
            vm.ctx.new_list(items).into()
        }

        #[pymethod]
        fn view(&self, paths: PosArgs<String>, vm: &VirtualMachine) -> PyResult<()> {
            let paths: Vec<String> = paths.into_vec();
            for p in &paths {
                if !std::path::Path::new(p).is_file() {
                    return Err(file_not_found(vm, format!("view: file not found or not a regular file: {}", p)));
                }
            }
            self.col().lock().unwrap().view_paths.extend(paths);
            Ok(())
        }

        #[pymethod]
        fn acknowledge_timer(&self, timer_id: String) -> PyResult<()> {
            self.col().lock().unwrap().timer_acks.push(timer_id);
            Ok(())
        }

        #[pymethod]
        fn compact(&self) -> PyResult<()> {
            self.col().lock().unwrap().compact_called = true;
            Ok(())
        }

        #[pymethod]
        fn request_compaction(&self) -> PyResult<()> {
            self.col().lock().unwrap().compaction_requested = true;
            Ok(())
        }

        #[pymethod]
        fn fork(&self, children: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
            let depth = self.info.lock().unwrap().child_depth_remaining;
            if depth == 0 {
                return Err(vm.new_runtime_error("Cannot fork sub-agents at this depth"));
            }

            let list: PyRef<PyList> = children.try_into_value(vm)
                .map_err(|_| vm.new_type_error("fork() requires a list of ChildSettings"))?;
            let elems = list.borrow_vec().to_vec();
            if elems.is_empty() {
                return Err(vm.new_value_error("fork() requires at least one child"));
            }

            let mut child_settings = Vec::new();
            let mut names = Vec::new();

            for item in elems {
                let name: String = item.get_attr("name", vm)?.try_into_value(vm)?;
                if let Err(e) = crate::types::AgentName::new_child(&name) {
                    return Err(vm.new_value_error(format!("ChildSettings.name invalid: {}", e)));
                }
                let task: String = item.get_attr("task", vm)?.try_into_value(vm)?;
                let model: Option<String> = item.get_attr("model", vm)?.try_into_value(vm)?;

                let max_turns_obj = item.get_attr("max_turns", vm)?;
                let max_turns: Option<u32> = if vm.is_none(&max_turns_obj) {
                    None
                } else {
                    Some(u32::try_from_object(vm, max_turns_obj)?.min(50))
                };

                let can_compact: bool = item.get_attr("can_compact", vm)?.try_into_value(vm)?;
                let inherit_history: bool = item.get_attr("inherit_history", vm)?.try_into_value(vm)?;

                let attach = extract_str_list(vm, &item.get_attr("attach", vm)?)?;
                let prefix_context: Option<String> = item.get_attr("prefix_context", vm)?.try_into_value(vm)?;
                let prefix_attach = extract_str_list(vm, &item.get_attr("prefix_attach", vm)?)?;

                names.push(name.clone());
                child_settings.push(ForkChildSettings {
                    name, task, model, max_turns, can_compact, inherit_history,
                    attach, prefix_context, prefix_attach,
                });
            }

            self.col().lock().unwrap().fork_requests.push(ForkRequest { children: child_settings });
            let name_objs: Vec<PyObjectRef> = names.into_iter().map(|n| vm.ctx.new_str(n).into()).collect();
            Ok(vm.ctx.new_list(name_objs).into())
        }

        #[pymethod]
        fn message_agent(&self, name: String, content: String, priority: OptionalArg<u8>) -> PyResult<()> {
            let priority = priority.unwrap_or(6);
            self.col().lock().unwrap().agent_messages.push(AgentMessageRequest { recipient: name, content, priority });
            Ok(())
        }

        #[pymethod]
        fn done(&self, kwargs: KwArgs, vm: &VirtualMachine) -> PyResult<()> {
            let mut result = HashMap::new();
            for (key, val) in kwargs {
                let serde_val = py_to_json(vm, &val)
                    .map_err(|_| vm.new_value_error(format!("done() kwargs must be JSON-serializable (key: {})", key)))?;
                result.insert(key, serde_val);
            }
            let col = self.col();
            let mut c = col.lock().unwrap();
            c.done_called = true;
            c.done_result = result;
            Ok(())
        }

        #[pymethod]
        fn agent_name(&self) -> String {
            self.info.lock().unwrap().agent_name.clone()
        }

        #[pymethod]
        fn agent_lineage(&self) -> String {
            self.info.lock().unwrap().agent_lineage.clone()
        }

        #[pymethod]
        fn harness_bin(&self) -> String {
            self.info.lock().unwrap().harness_bin.clone()
        }
    }

    // ------ StdoutCapture ------

    #[pyattr]
    #[pyclass(module = "_harness", name = "StdoutCapture")]
    #[derive(Debug, PyPayload, Default)]
    pub struct StdoutCapture {
        pub buf: Mutex<String>,
    }

    #[pyclass]
    impl StdoutCapture {
        #[pymethod]
        fn write(&self, text: PyStrRef) -> usize {
            let s: &str = text.as_ref();
            self.buf.lock().unwrap().push_str(s);
            s.len()
        }

        #[pymethod]
        fn flush(&self) -> PyResult<()> {
            Ok(())
        }
    }
}

// ---- Python Preamble ----
//
// Wraps Rust instances (injected as _memory, _work_queue, _timers, _history,
// _harness, _stdout_capture) with thin Python classes that provide dunder
// methods. Also wraps PyHarness methods with named-parameter Python `def`s,
// since RustPython's #[pymethod] macro doesn't expose parameter names for
// kwargs dispatch.

const PREAMBLE: &str = r#"
import sys
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

class _WorkItemWrap:
    __slots__ = ('_i',)
    def __init__(self, i): self._i = i
    def __getattr__(self, name):
        if name == '_i': raise AttributeError(name)
        return self._i._field(name)
    def __repr__(self): return self._i._repr()
    @property
    def id(self): return self._i.id
    @property
    def priority(self): return self._i.priority
    @property
    def time(self): return self._i.time

class _WorkQueueWrap:
    def __init__(self, q): self._q = q
    def __getitem__(self, i): return _WorkItemWrap(self._q._getitem(i))
    def __len__(self): return self._q._len()
    def pop_front(self):
        item = self._q.pop_front()
        return _WorkItemWrap(item) if item is not None else None
    def remove(self, id): self._q.remove(id)
    def add_filter(self, name, regex): self._q.add_filter(name, regex)
    def remove_filter(self, name): self._q.remove_filter(name)

class _MemoryWrap:
    def __init__(self, m): self._m = m
    def __getitem__(self, k): return self._m._getitem(k)
    def __setitem__(self, k, v): self._m._setitem(k, v)
    def __delitem__(self, k): self._m._delitem(k)
    def __contains__(self, k): return self._m._contains(k)
    def __repr__(self): return self._m._repr()
    def get(self, k, default=None): return self._m.get(k, default)
    def set(self, k, v, priority=5): return self._m.set(k, v, priority)
    def set_priority(self, k, p): return self._m.set_priority(k, p)
    def get_priority(self, k): return self._m.get_priority(k)
    def pin(self, k, v): return self._m.pin(k, v)
    def unpin(self, k): return self._m.unpin(k)
    def list_pinned(self): return self._m.list_pinned()
    def mark_sensitive(self, k): return self._m.mark_sensitive(k)
    def unmark_sensitive(self, k): return self._m.unmark_sensitive(k)

class _HistoryWrap:
    def __init__(self, h): self._h = h
    def __getitem__(self, k): return self._h._getitem(k)
    def replace_with_description(self, id, desc): return self._h.replace_with_description(id, desc)
    def remove(self, id): return self._h.remove(id)
    def add(self, text): return self._h.add(text)

class _TimerWrap:
    def __init__(self, t): self._t = t
    def add(self, every=None, at=None, priority=5, description=""):
        return self._t.add(every=every, at=at, priority=priority, description=description)
    def cancel(self, timer_id): return self._t.cancel(timer_id)
    def list(self): return self._t.list()

# Capture stdout/stderr
sys.stdout = _stdout_capture
sys.stderr = _stdout_capture

# Wrap injected Rust instances
work_queue = _WorkQueueWrap(_work_queue)
memory = _MemoryWrap(_memory)
timers = _TimerWrap(_timers)
history = _HistoryWrap(_history)

# Top-level functions — wrap _harness methods with named-parameter defs so
# kwargs work (RustPython #[pymethod] doesn't expose param names to the parser).
def send_message(chat_id, content, attach=None, react_to=None):
    return _harness.send_message(chat_id, content, attach, react_to)
def shell_exec(cmd, args=None, env=None, description="", alert_timer=None,
               success_prio=7, fail_prio=8, block_for=None, interactive=False):
    return _harness.shell_exec(cmd, args=args or [], env=env, description=description,
                               alert_timer=alert_timer, success_prio=success_prio,
                               fail_prio=fail_prio, block_for=block_for, interactive=interactive)
def shell_status(pid): return _harness.shell_status(pid)
def shell_output(pid, lines=None):
    return _harness.shell_output(pid) if lines is None else _harness.shell_output(pid, lines)
def shell_input(pid, data): return _harness.shell_input(pid, data)
def shell_close_stdin(pid): return _harness.shell_close_stdin(pid)
def shell_kill(pid): return _harness.shell_kill(pid)
def processes_list(): return _harness.processes_list()
def acknowledge_timer(timer_id): return _harness.acknowledge_timer(timer_id)
def request_compaction(): return _harness.request_compaction()
def view(*paths): return _harness.view(*paths)
def fork(children): return _harness.fork(children)
def kill_child(name): return _harness.kill_child(name)
def message_agent(name, content, priority=6): return _harness.message_agent(name, content, priority)
def done(**kwargs): return _harness.done(**kwargs)
agent_name = _harness.agent_name()
agent_lineage = _harness.agent_lineage()
harness_bin = _harness.harness_bin()

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
def compact(): return _harness.compact()
compaction_script = ""
"#;

// ---- Executor ----

pub struct Executor {
    interpreter: vm::Interpreter,
    /// For sending a KeyboardInterrupt into the VM from a watchdog thread.
    signal_tx: vm::signal::UserSignalSender,
}

impl Executor {
    pub fn new() -> Self {
        let (signal_tx, signal_rx) = vm::signal::user_signal_channel();
        let builder = rustpython::Interpreter::builder(Default::default());
        let def = harness::module_def(&builder.ctx);
        let interpreter = builder
            .init_stdlib()
            .add_native_module(def)
            .init_hook(move |vm| vm.set_user_signal_channel(signal_rx))
            .build();
        Self { interpreter, signal_tx }
    }

    pub fn execute(
        &self,
        state: &HarnessState,
        code: &str,
        is_compaction: bool,
        process_outputs: &HashMap<String, String>,
    ) -> ExecutionResult {
        self.execute_with_timeout(state, code, is_compaction, process_outputs, 0, 1, "root", "root", &HashMap::new())
    }

    pub fn execute_with_timeout(
        &self,
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
        // Watchdog: race a cancel channel against the timeout. Dropping
        // cancel_tx on normal return makes recv_timeout return Err(Disconnected)
        // immediately, so the watchdog exits without polling.
        let (cancel_tx, cancel_rx) = mpsc::channel::<()>();
        let _watchdog = if timeout_secs > 0 {
            let sig = self.signal_tx.clone();
            Some(std::thread::spawn(move || {
                match cancel_rx.recv_timeout(std::time::Duration::from_secs(timeout_secs)) {
                    Err(mpsc::RecvTimeoutError::Timeout) => {
                        let _ = sig.send(Box::new(|vm| {
                            Err(vm.new_exception_msg(
                                vm.ctx.exceptions.keyboard_interrupt.to_owned(),
                                "Python script execution timed out".into(),
                            ))
                        }));
                    }
                    _ => {} // cancelled or disconnected — do nothing
                }
            }))
        } else {
            None
        };

        let result = self.execute_inner(
            state, code, is_compaction, process_outputs, child_depth_remaining,
            agent_name, agent_lineage, pinned_memory,
        );

        drop(cancel_tx); // signal watchdog to exit
        result
    }

    fn execute_inner(
        &self,
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

        // Build per-turn data from HarnessState.
        let work_items: Vec<WorkItemData> = state.work_queue.items()
            .iter().map(work_item_to_data).collect();

        let timers_info: Vec<(String, String, u8)> = state.timer_manager.list()
            .iter().map(|t| (t.id.0.clone(), t.description.clone(), t.priority)).collect();

        let mut history_entries = HashMap::new();
        for entry in state.event_history.entries() {
            let (code_str, output_str, time_str) = match entry {
                HistoryEntry::Execution { code, output, time, .. } => (
                    code.clone(), output.clone(),
                    time.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
                ),
                HistoryEntry::Summary { description, time, .. } => (
                    String::new(), description.clone(),
                    time.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
                ),
                HistoryEntry::SystemAlert { message, time, .. } => (
                    String::new(), message.clone(),
                    time.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
                ),
            };
            history_entries.insert(entry.id().0.clone(), HistoryEntryData {
                code: code_str, output: output_str.clone(),
                full_output: output_str, time: time_str,
            });
        }

        let process_statuses: HashMap<String, String> = state.process_manager.processes()
            .iter().map(|p| {
                let status = match &p.status {
                    ProcessStatus::Running => "running",
                    ProcessStatus::Completed { .. } => "completed",
                    ProcessStatus::Failed { .. } => "failed",
                };
                (p.id.0.clone(), status.to_string())
            }).collect();

        let process_info: Vec<(String, String, String, String)> = state.process_manager.processes()
            .iter().map(|p| {
                let status = match &p.status {
                    ProcessStatus::Running => "running".to_string(),
                    ProcessStatus::Completed { exit_code } => format!("completed (exit {})", exit_code),
                    ProcessStatus::Failed { error } => format!("failed: {}", error),
                };
                (p.id.0.clone(), p.cmd.clone(), p.description.clone(), status)
            }).collect();

        let harness_info = HarnessInfo {
            process_outputs: process_outputs.clone(),
            process_statuses,
            process_info,
            child_depth_remaining,
            agent_name: agent_name.to_string(),
            agent_lineage: agent_lineage.to_string(),
            harness_bin: std::env::current_exe()
                .map(|p| p.to_string_lossy().into_owned())
                .unwrap_or_else(|_| "claude-server".into()),
            is_compaction,
        };

        let memory_state = MemoryState {
            data: state.memory.clone(),
            priorities: state.memory_priorities.clone(),
            pinned: pinned_memory.clone(),
        };

        let (result, stdout) = self.interpreter.enter(|vm| {
            // Must import _harness before into_ref() or the static type cells
            // remain uninitialized and into_ref() panics with
            // "static type has not been initialized".
            if let Err(e) = vm.import("_harness", 0) {
                let mut s = String::new();
                vm.write_exception(&mut s, &e).ok();
                return (Err(s), String::new());
            }

            // Build pyclass instances carrying this turn's data.
            let wq = harness::PyWorkQueue {
                items: Mutex::new(work_items),
                collector: Mutex::new(Some(collector.clone())),
            }.into_ref(&vm.ctx);
            let mem = harness::PyMemory {
                state: Mutex::new(memory_state),
                collector: Mutex::new(Some(collector.clone())),
            }.into_ref(&vm.ctx);
            let tim = harness::PyTimerManager {
                timers_info: Mutex::new(timers_info),
                collector: Mutex::new(Some(collector.clone())),
            }.into_ref(&vm.ctx);
            let hist = harness::PyHistoryManager {
                entries: Mutex::new(history_entries),
                is_compaction: Mutex::new(is_compaction),
                collector: Mutex::new(Some(collector.clone())),
            }.into_ref(&vm.ctx);
            let har = harness::PyHarness {
                info: Mutex::new(harness_info),
                collector: Mutex::new(Some(collector.clone())),
            }.into_ref(&vm.ctx);
            let cap = harness::StdoutCapture {
                buf: Mutex::new(String::new()),
            }.into_ref(&vm.ctx);

            let scope = vm.new_scope_with_builtins();
            let g = &scope.globals;
            g.set_item("_work_queue", wq.into(), vm).ok();
            g.set_item("_memory", mem.into(), vm).ok();
            g.set_item("_timers", tim.into(), vm).ok();
            g.set_item("_history", hist.into(), vm).ok();
            g.set_item("_harness", har.into(), vm).ok();
            g.set_item("_stdout_capture", cap.clone().into(), vm).ok();

            let run = |src: &str, name: &str| -> Result<(), String> {
                run_code(vm, &scope, src, name).map_err(|e| {
                    let mut s = String::new();
                    vm.write_exception(&mut s, &e).ok();
                    s
                })
            };

            let res = (|| -> Result<(), String> {
                run(PREAMBLE, "<preamble>")?;
                if is_compaction {
                    run(COMPACTION_PREAMBLE, "<compaction_preamble>")?;
                }
                run(code, "<agent>")?;

                if is_compaction {
                    if let Ok(Some(script_val)) = scope.globals.get_item_opt("compaction_script", vm) {
                        if let Ok(script) = String::try_from_object(vm, script_val) {
                            if !script.is_empty() {
                                collector.lock().unwrap().compaction_script_appends.push(script);
                            }
                        }
                    }
                }
                Ok(())
            })();

            let stdout = cap.buf.lock().unwrap().clone();
            (res, stdout)
        });

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
            Err(error_text) => ExecutionResult {
                stdout,
                is_error: true,
                error_text,
                side_effects: SideEffectCollector::default(),
            },
        }
    }
}

impl Default for Executor {
    fn default() -> Self {
        Self::new()
    }
}

fn run_code(vm: &VirtualMachine, scope: &vm::scope::Scope, source: &str, name: &str) -> PyResult<()> {
    let code = vm
        .compile(source, vm::compiler::Mode::Exec, name.to_owned())
        .map_err(|e| vm.new_syntax_error(&e, Some(source)))?;
    vm.run_code_obj(code, scope.clone())?;
    Ok(())
}

// ---- Tests ----

#[cfg(test)]
mod tests {
    use super::*;

    fn exec(state: &HarnessState, code: &str) -> ExecutionResult {
        Executor::new().execute(state, code, false, &HashMap::new())
    }

    fn exec_full(
        state: &HarnessState,
        code: &str,
        timeout: u64,
        depth: u32,
        name: &str,
        lineage: &str,
        pinned: &HashMap<String, String>,
    ) -> ExecutionResult {
        Executor::new().execute_with_timeout(
            state, code, false, &HashMap::new(), timeout, depth, name, lineage, pinned,
        )
    }

    #[test]
    fn test_basic_print() {
        let state = HarnessState::new(200_000, 16384);
        let result = exec(&state, "print('hello world')");
        assert!(!result.is_error, "Error: {}", result.error_text);
        assert_eq!(result.stdout.trim(), "hello world");
    }

    #[test]
    fn test_memory_operations() {
        let mut state = HarnessState::new(200_000, 16384);
        state.memory.insert("key1".to_string(), serde_json::json!("val1"));

        let result = exec(&state, r#"
assert memory["key1"] == "val1"
memory["key2"] = "val2"
assert "key1" in memory
print("ok")
"#);
        assert!(!result.is_error, "Error: {}", result.error_text);
        assert_eq!(result.stdout.trim(), "ok");
        assert_eq!(result.side_effects.memory_sets.len(), 1);
        assert_eq!(result.side_effects.memory_sets[0].0, "key2");
        assert_eq!(result.side_effects.memory_sets[0].1, serde_json::json!("val2"));
    }

    #[test]
    fn test_memory_structured_values() {
        let state = HarnessState::new(200_000, 16384);
        let result = exec(&state, r#"
memory["s"] = "hello"
memory["n"] = 42
memory["d"] = {"pid": "abc", "chat_id": "xyz"}
memory["l"] = ["a", "b", "c"]
memory["b"] = True

assert memory["s"] == "hello"
assert memory["n"] == 42
assert memory["d"]["pid"] == "abc"
assert memory["l"][1] == "b"
assert memory["b"] == True

assert memory.get("s") == "hello"
assert memory.get("missing") is None
assert memory.get("missing", "fallback") == "fallback"

print("all passed")
"#);
        assert!(!result.is_error, "Error: {}", result.error_text);
        assert_eq!(result.stdout.trim(), "all passed");
        assert_eq!(result.side_effects.memory_sets.len(), 5);
    }

    #[test]
    fn test_timer_add_returns_id() {
        let state = HarnessState::new(200_000, 16384);
        let result = exec(&state, r#"
tid = timers.add(every=30, priority=6, description="test timer")
print(tid)
"#);
        assert!(!result.is_error, "Error: {}", result.error_text);
        assert!(!result.stdout.trim().is_empty());
        assert_eq!(result.side_effects.timer_adds.len(), 1);
    }

    #[test]
    fn test_work_queue_pop() {
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

        let result = exec(&state, r#"
item = work_queue[0]
print(item.content)
work_queue.pop_front()
"#);
        assert!(!result.is_error, "Error: {}", result.error_text);
        assert_eq!(result.stdout.trim(), "Hello!");
        assert_eq!(result.side_effects.queue_removes.len(), 1);
    }

    #[test]
    fn test_error_handling() {
        let state = HarnessState::new(200_000, 16384);
        let result = exec(&state, "undefined_variable");
        assert!(result.is_error);
        assert!(
            result.error_text.contains("NameError"),
            "Expected NameError in: '{}'",
            result.error_text,
        );
    }

    #[test]
    fn test_shell_exec_returns_id() {
        let state = HarnessState::new(200_000, 16384);
        let result = exec(&state, r#"
pid = shell_exec("echo", ["hello"])
print(pid)
memory["my_pid"] = pid
"#);
        assert!(!result.is_error, "Error: {}", result.error_text);
        assert_eq!(result.side_effects.process_starts.len(), 1);
        assert_eq!(result.side_effects.memory_sets.len(), 1);
    }

    #[test]
    fn test_send_message() {
        let state = HarnessState::new(200_000, 16384);
        let result = exec(&state, r#"send_message("chat1", "Hello from Claude!")"#);
        assert!(!result.is_error, "Error: {}", result.error_text);
        assert_eq!(result.side_effects.messages.len(), 1);
        assert_eq!(result.side_effects.messages[0].chat_id, "chat1");
    }

    #[test]
    fn test_one_shot_timer_with_datetime() {
        let state = HarnessState::new(200_000, 16384);
        let result = exec(&state, r#"
tid = timers.add(at=datetime(2026, 2, 1, 17, 0, 0), priority=8, description="dinner reminder")
print(tid)
"#);
        assert!(!result.is_error, "Error: {}", result.error_text);
        assert_eq!(result.side_effects.timer_adds.len(), 1);
        assert!(result.side_effects.timer_adds[0].at_epoch.is_some());
        assert!(result.side_effects.timer_adds[0].every_secs.is_none());
        let epoch = result.side_effects.timer_adds[0].at_epoch.unwrap();
        assert!(epoch > 1_700_000_000.0, "epoch {} too small", epoch);
    }

    #[test]
    fn test_memory_priority() {
        let state = HarnessState::new(200_000, 16384);
        let result = exec_full(&state, r#"
memory["key1"] = "value1"
assert memory.get_priority("key1") == 5

memory.set("key2", "value2", priority=8)
assert memory.get_priority("key2") == 8

memory.set_priority("key1", 3)
assert memory.get_priority("key1") == 3

print("all passed")
"#, 0, 1, "root", "root", &HashMap::new());
        assert!(!result.is_error, "Error: {}", result.error_text);
        assert_eq!(result.stdout.trim(), "all passed");
        assert_eq!(result.side_effects.memory_sets.len(), 2);
        assert_eq!(result.side_effects.memory_priority_sets.len(), 3);
        assert_eq!(result.side_effects.memory_priority_sets[0], ("key1".to_string(), 5));
        assert_eq!(result.side_effects.memory_priority_sets[1], ("key2".to_string(), 8));
        assert_eq!(result.side_effects.memory_priority_sets[2], ("key1".to_string(), 3));
    }

    #[test]
    fn test_execution_timeout() {
        let state = HarnessState::new(200_000, 16384);
        let start = std::time::Instant::now();
        let result = exec_full(&state, "while True: pass", 2, 1, "root", "root", &HashMap::new());
        let elapsed = start.elapsed();
        assert!(result.is_error, "Should have timed out");
        assert!(
            result.error_text.contains("timed out") || result.error_text.contains("KeyboardInterrupt"),
            "Error should mention timeout or KeyboardInterrupt, got: {}",
            result.error_text
        );
        assert!(elapsed.as_secs() <= 5, "Took too long: {:?}", elapsed);
    }

    #[test]
    fn test_fork() {
        let state = HarnessState::new(200_000, 16384);
        let result = exec_full(&state, r#"
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
"#, 0, 1, "root", "root", &HashMap::new());
        assert!(!result.is_error, "Error: {}", result.error_text);
        assert_eq!(result.stdout.trim(), "forked");
        assert_eq!(result.side_effects.fork_requests.len(), 1);
        let req = &result.side_effects.fork_requests[0];
        assert_eq!(req.children.len(), 2);
        assert_eq!(req.children[0].name, "test-runner");
        assert_eq!(req.children[0].task, "Write tests");
        assert_eq!(req.children[0].model, Some("claude-sonnet-4-5-20250929".to_string()));
        assert_eq!(req.children[0].max_turns, Some(10));
        assert_eq!(req.children[1].name, "linter");
        assert_eq!(req.children[1].task, "Run linting");
        assert_eq!(req.children[1].model, None);
        assert_eq!(req.children[1].max_turns, Some(20));
        assert!(req.children[1].can_compact);
        assert!(req.children[0].attach.is_empty());
    }

    #[test]
    fn test_fork_rejects_root_name() {
        let state = HarnessState::new(200_000, 16384);
        let result = exec(&state, r#"fork([ChildSettings(name="root", task="impersonate")])"#);
        assert!(result.is_error, "fork with name='root' should fail");
        assert!(
            result.error_text.contains("reserved"),
            "Error should mention reserved name: {}",
            result.error_text
        );
    }

    #[test]
    fn test_view() {
        let state = HarnessState::new(200_000, 16384);
        let tmp = std::env::temp_dir().join("claude-server-test-attachment.txt");
        std::fs::write(&tmp, "test content").unwrap();

        let code = format!(r#"
view({path:?})
print("ok")
"#, path = tmp.to_str().unwrap());
        let result = exec(&state, &code);
        assert!(!result.is_error, "Error: {}", result.error_text);
        assert_eq!(result.stdout.trim(), "ok");
        assert_eq!(result.side_effects.view_paths.len(), 1);
        assert_eq!(result.side_effects.view_paths[0], tmp.to_str().unwrap());

        std::fs::remove_file(&tmp).ok();
    }

    #[test]
    fn test_view_file_not_found() {
        let state = HarnessState::new(200_000, 16384);
        let result = exec(&state, r#"view("/nonexistent/path/xyz.jpg")"#);
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
        let state = HarnessState::new(200_000, 16384);
        let result = exec_full(&state, r#"
fork([
    ChildSettings(
        name="investigator",
        task="Look at this image",
        attach=["/tmp/snapshot.jpg", "/tmp/metadata.json"],
    ),
])
"#, 0, 1, "root", "root", &HashMap::new());
        assert!(!result.is_error, "Error: {}", result.error_text);
        let req = &result.side_effects.fork_requests[0];
        assert_eq!(req.children.len(), 1);
        assert_eq!(req.children[0].attach.len(), 2);
        assert_eq!(req.children[0].attach[0], "/tmp/snapshot.jpg");
        assert_eq!(req.children[0].attach[1], "/tmp/metadata.json");
    }

    #[test]
    fn test_timedelta_in_timer_and_shell_exec() {
        let state = HarnessState::new(200_000, 16384);
        let result = exec(&state, r#"
tid = timers.add(every=timedelta(seconds=30), priority=6, description="test")
print(tid)

pid = shell_exec("echo", ["hi"], alert_timer=timedelta(minutes=5))
print(pid)

tid2 = timers.add(every=60, priority=5, description="numeric")
pid2 = shell_exec("echo", ["hi"], alert_timer=300)
"#);
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
        let state = HarnessState::new(200_000, 16384);
        let result = exec(&state, r#"
message_agent("sibling-a", "check the API status")
message_agent("sibling-b", "done with my part", priority=9)
print(agent_name)
print(agent_lineage)
"#);
        assert!(!result.is_error, "Error: {}", result.error_text);
        assert_eq!(result.side_effects.agent_messages.len(), 2);
        assert_eq!(result.side_effects.agent_messages[0].recipient, "sibling-a");
        assert_eq!(result.side_effects.agent_messages[0].content, "check the API status");
        assert_eq!(result.side_effects.agent_messages[0].priority, 6);
        assert_eq!(result.side_effects.agent_messages[1].recipient, "sibling-b");
        assert_eq!(result.side_effects.agent_messages[1].priority, 9);
        assert_eq!(result.stdout.trim(), "root\nroot");
    }

    #[test]
    fn test_agent_identity_in_child() {
        let state = HarnessState::new(200_000, 16384);
        let result = exec_full(&state, r#"
print(agent_name)
print(agent_lineage)
"#, 0, 1, "api-checker", "api-checker, child of plan-builder, child of root", &HashMap::new());
        assert!(!result.is_error, "Error: {}", result.error_text);
        let lines: Vec<&str> = result.stdout.trim().lines().collect();
        assert_eq!(lines[0], "api-checker");
        assert_eq!(lines[1], "api-checker, child of plan-builder, child of root");
    }

    #[test]
    fn test_harness_bin() {
        let state = HarnessState::new(200_000, 16384);
        let result = exec(&state, r#"
print(harness_bin)
assert isinstance(harness_bin, str) and len(harness_bin) > 0
"#);
        assert!(!result.is_error, "Error: {}", result.error_text);
        assert!(!result.stdout.trim().is_empty());
    }

    #[test]
    fn test_request_compaction() {
        let state = HarnessState::new(200_000, 16384);
        let result = exec(&state, "request_compaction()");
        assert!(!result.is_error, "Error: {}", result.error_text);
        assert!(result.side_effects.compaction_requested);
    }

    #[test]
    fn test_memory_pin() {
        let state = HarnessState::new(200_000, 16384);
        let mut pinned = HashMap::new();
        pinned.insert("existing".to_string(), "old value".to_string());
        let result = exec_full(&state, r#"
print(f"pinned: {memory.list_pinned()}")
print(f"existing: {memory.get('existing')}")
print(f"missing: {memory.get('missing')}")
print(f"contains: {'existing' in memory}")

memory.pin("api_info", "HA API at :8123")
memory.pin("user_prefs", "Prefers SMS alerts")

memory.unpin("existing")
"#, 0, 1, "root", "root", &pinned);
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
        let state = HarnessState::new(200_000, 16384);
        let result = exec(&state, r#"
done(verdict="all clear", confidence=0.95, details={"camera": "front", "count": 5})
"#);
        assert!(!result.is_error, "Error: {}", result.error_text);
        assert!(result.side_effects.done_called);
        assert_eq!(result.side_effects.done_result.len(), 3);
        assert_eq!(result.side_effects.done_result["verdict"], serde_json::json!("all clear"));
        assert_eq!(result.side_effects.done_result["confidence"], serde_json::json!(0.95));
        assert_eq!(result.side_effects.done_result["details"], serde_json::json!({"camera": "front", "count": 5}));
    }

    #[test]
    fn test_done_no_args() {
        let state = HarnessState::new(200_000, 16384);
        let result = exec(&state, "done()");
        assert!(!result.is_error, "Error: {}", result.error_text);
        assert!(result.side_effects.done_called);
        assert!(result.side_effects.done_result.is_empty());
    }

    #[test]
    fn test_work_item_field_access() {
        let mut state = HarnessState::new(200_000, 16384);
        let mut id_gen = IdGenerator::new();

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

        let result = exec(&state, r#"
item = work_queue[0]
print(item.type)
print(item.child_name)
print(item.turns_used)
print(item.success)
print(item.result["verdict"])
try:
    _ = item.chat_id
    print("FAIL: should have raised")
except AttributeError as e:
    print(f"attr error: {e}")
"#);
        assert!(!result.is_error, "Error: {}", result.error_text);
        assert!(result.stdout.contains("ChildAgentCompleted"));
        assert!(result.stdout.contains("investigator"));
        assert!(result.stdout.contains("2"));
        assert!(result.stdout.contains("True"));
        assert!(result.stdout.contains("safe"));
        assert!(result.stdout.contains("has no field 'chat_id'"));
        assert!(result.stdout.contains("Available fields"));
    }

    #[test]
    fn test_error_rolls_back_side_effects() {
        let state = HarnessState::new(200_000, 16384);
        let result = exec(&state, r#"
send_message("chat1", "hello")
raise RuntimeError("boom")
"#);
        assert!(result.is_error);
        assert!(result.error_text.contains("boom"));
        assert_eq!(result.side_effects.messages.len(), 0);
    }

    #[test]
    fn test_shell_exec_kwargs() {
        let state = HarnessState::new(200_000, 16384);
        let result = exec(&state, r#"pid = shell_exec("echo", args=["hi"], description="test")
print(pid)"#);
        assert!(!result.is_error, "Error: {}", result.error_text);
        assert_eq!(result.side_effects.process_starts.len(), 1);
        assert_eq!(result.side_effects.process_starts[0].cmd, "echo");
        assert_eq!(result.side_effects.process_starts[0].args, vec!["hi"]);
    }
}
