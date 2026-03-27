//! RustPython-based Python executor.
//!
//! Zero libpython linkage — stdlib is frozen into the binary via the
//! `freeze-stdlib` feature. Replaces the previous PyO3 implementation.
//!
//! Key design notes:
//!
//! - RustPython's #[pymodule] is static: classes/functions are registered at
//!   module-def time, not instantiated per-turn. Per-turn state (work items,
//!   memory snapshot, collector) lives in a thread-local `ExecContext` that
//!   the #[pyclass] methods read from.
//!
//! - Dunder methods (__getitem__, __contains__, etc.) need AsMapping/AsSequence
//!   trait impls on RustPython HEAD — these require static fn tables with
//!   downcasts. To sidestep that, Rust classes expose plain-named methods
//!   (_getitem, _len, ...) and the Python PREAMBLE wraps them with thin
//!   Python classes that provide the dunders. Behavior is identical from
//!   the agent's perspective.
//!
//! - Interpreter is built once via OnceLock. Each turn gets a fresh scope
//!   (same as the old fresh-PyDict-per-turn pattern).

#![allow(dead_code)]

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use rustpython::InterpreterBuilderExt;
use rustpython_vm as vm;
use vm::builtins::{PyDict, PyList, PyStrRef};
use vm::function::{FuncArgs, KwArgs, OptionalArg, PosArgs};
use vm::{
    pymodule, PyObjectRef, PyPayload, PyResult, TryFromObject, VirtualMachine,
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

// ---- Per-turn execution context (thread-local) ----
//
// RustPython's #[pymodule] is static — we can't inject per-turn objects into
// it. Instead the module's classes/functions read from this thread-local,
// which is set fresh at the top of each execute_inner() call.

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

struct ExecContext {
    collector: Arc<Mutex<SideEffectCollector>>,
    work_items: Vec<WorkItemData>,
    memory_data: HashMap<String, serde_json::Value>,
    memory_priorities: HashMap<String, u8>,
    pinned_memory: HashMap<String, String>,
    timers_info: Vec<(String, String, u8)>,
    history_entries: HashMap<String, HistoryEntryData>,
    process_outputs: HashMap<String, String>,
    process_statuses: HashMap<String, String>,
    process_info: Vec<(String, String, String, String)>,
    child_depth_remaining: u32,
    agent_name: String,
    agent_lineage: String,
    harness_bin: String,
    is_compaction: bool,
    stdout_buf: Arc<Mutex<String>>,
}

thread_local! {
    static EXEC_CTX: RefCell<Option<ExecContext>> = const { RefCell::new(None) };
}

fn with_ctx<R>(f: impl FnOnce(&mut ExecContext) -> R) -> R {
    EXEC_CTX.with(|cell| {
        let mut borrow = cell.borrow_mut();
        let ctx = borrow.as_mut().expect("ExecContext not set — called outside execute()");
        f(ctx)
    })
}

fn with_collector<R>(f: impl FnOnce(&mut SideEffectCollector) -> R) -> R {
    with_ctx(|ctx| {
        let mut guard = ctx.collector.lock().unwrap();
        f(&mut guard)
    })
}

// ---- Helpers ----

/// Extract a duration in seconds from a number (int/float) or datetime.timedelta.
/// RustPython's `f64: TryFromObject` only accepts PyFloat, so we use
/// ArgIntoFloat which coerces ints via __float__.
fn extract_seconds(vm: &VirtualMachine, val: &PyObjectRef) -> PyResult<f64> {
    use vm::function::ArgIntoFloat;
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

#[pymodule(name = "_harness")]
mod harness {
    use super::*;
    use rustpython_vm::builtins::PyType;
    use rustpython_vm::{pyclass, PyObjectRef, PyRef};

    // ------ PyWorkItem ------

    #[pyattr]
    #[pyclass(module = "_harness", name = "WorkItem")]
    #[derive(Debug, PyPayload, Default)]
    pub struct PyWorkItem {
        data: Mutex<Option<WorkItemData>>,
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

        /// Dynamic field access. Called from Python wrapper's __getattr__.
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

    pub fn make_work_item(vm: &VirtualMachine, data: WorkItemData) -> PyObjectRef {
        PyWorkItem { data: Mutex::new(Some(data)) }.into_pyobject(vm)
    }

    // ------ PyWorkQueue ------

    #[pyattr]
    #[pyclass(module = "_harness", name = "WorkQueue")]
    #[derive(Debug, PyPayload, Default)]
    pub struct PyWorkQueue;

    #[pyclass]
    impl PyWorkQueue {
        #[pyslot]
        fn slot_new(cls: PyRef<PyType>, _args: FuncArgs, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
            Self.into_ref_with_type(vm, cls).map(Into::into)
        }

        #[pymethod]
        fn _getitem(&self, index: usize, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
            with_ctx(|ctx| {
                ctx.work_items.get(index)
                    .map(|d| make_work_item(vm, d.clone()))
                    .ok_or_else(|| vm.new_index_error("work queue index out of range"))
            })
        }

        #[pymethod]
        fn _len(&self) -> usize {
            with_ctx(|ctx| ctx.work_items.len())
        }

        #[pymethod]
        fn pop_front(&self, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
            with_ctx(|ctx| {
                if ctx.work_items.is_empty() {
                    return Ok(vm.ctx.none());
                }
                let item = ctx.work_items.remove(0);
                ctx.collector.lock().unwrap().queue_removes.push(item.id.clone());
                Ok(make_work_item(vm, item))
            })
        }

        #[pymethod]
        fn remove(&self, id: String) -> PyResult<()> {
            with_ctx(|ctx| {
                ctx.work_items.retain(|i| i.id != id);
                ctx.collector.lock().unwrap().queue_removes.push(id);
            });
            Ok(())
        }

        #[pymethod]
        fn add_filter(&self, name: String, regex: String) -> PyResult<()> {
            with_collector(|c| c.filter_adds.push(QueueFilter { name, regex }));
            Ok(())
        }

        #[pymethod]
        fn remove_filter(&self, name: String) -> PyResult<()> {
            with_collector(|c| c.filter_removes.push(name));
            Ok(())
        }
    }

    // ------ PyMemory ------

    #[pyattr]
    #[pyclass(module = "_harness", name = "Memory")]
    #[derive(Debug, PyPayload, Default)]
    pub struct PyMemory;

    #[pyclass]
    impl PyMemory {
        #[pyslot]
        fn slot_new(cls: PyRef<PyType>, _args: FuncArgs, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
            Self.into_ref_with_type(vm, cls).map(Into::into)
        }

        #[pymethod]
        fn _getitem(&self, key: PyStrRef, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
            let key: &str = key.as_ref();
            with_ctx(|ctx| {
                match ctx.memory_data.get(key) {
                    Some(val) => json_to_py(vm, val),
                    None => Err(vm.new_key_error(vm.ctx.new_str(key.to_owned()).into())),
                }
            })
        }

        #[pymethod]
        fn _setitem(&self, key: String, value: PyObjectRef, vm: &VirtualMachine) -> PyResult<()> {
            let serde_val = py_to_json(vm, &value)?;
            with_ctx(|ctx| {
                ctx.memory_data.insert(key.clone(), serde_val.clone());
                let mut col = ctx.collector.lock().unwrap();
                col.memory_sets.push((key.clone(), serde_val));
                if !ctx.memory_priorities.contains_key(&key) {
                    ctx.memory_priorities.insert(key.clone(), 5);
                    col.memory_priority_sets.push((key, 5));
                }
            });
            Ok(())
        }

        #[pymethod]
        fn _delitem(&self, key: String) -> PyResult<()> {
            with_ctx(|ctx| {
                ctx.memory_data.remove(&key);
                ctx.collector.lock().unwrap().memory_deletes.push(key);
            });
            Ok(())
        }

        #[pymethod]
        fn _contains(&self, key: PyStrRef) -> bool {
            let key: &str = key.as_ref();
            with_ctx(|ctx| ctx.memory_data.contains_key(key) || ctx.pinned_memory.contains_key(key))
        }

        #[pymethod]
        fn get(&self, key: PyStrRef, default: OptionalArg<PyObjectRef>, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
            let key: &str = key.as_ref();
            with_ctx(|ctx| {
                if let Some(val) = ctx.memory_data.get(key) {
                    return json_to_py(vm, val);
                }
                if let Some(s) = ctx.pinned_memory.get(key) {
                    return Ok(vm.ctx.new_str(s.clone()).into());
                }
                Ok(default.unwrap_or_else(|| vm.ctx.none()))
            })
        }

        #[pymethod]
        fn set(&self, key: String, value: PyObjectRef, priority: OptionalArg<u8>, vm: &VirtualMachine) -> PyResult<()> {
            let priority = priority.unwrap_or(5);
            let serde_val = py_to_json(vm, &value)?;
            with_ctx(|ctx| {
                ctx.memory_data.insert(key.clone(), serde_val.clone());
                ctx.memory_priorities.insert(key.clone(), priority);
                let mut col = ctx.collector.lock().unwrap();
                col.memory_sets.push((key.clone(), serde_val));
                col.memory_priority_sets.push((key, priority));
            });
            Ok(())
        }

        #[pymethod]
        fn set_priority(&self, key: String, priority: u8) -> PyResult<()> {
            with_ctx(|ctx| {
                ctx.memory_priorities.insert(key.clone(), priority);
                ctx.collector.lock().unwrap().memory_priority_sets.push((key, priority));
            });
            Ok(())
        }

        #[pymethod]
        fn get_priority(&self, key: PyStrRef) -> u8 {
            let key: &str = key.as_ref();
            with_ctx(|ctx| ctx.memory_priorities.get(key).copied().unwrap_or(5))
        }

        #[pymethod]
        fn pin(&self, key: String, value: String) -> PyResult<()> {
            with_ctx(|ctx| {
                ctx.pinned_memory.insert(key.clone(), value.clone());
                ctx.collector.lock().unwrap().memory_pins.push((key, value));
            });
            Ok(())
        }

        #[pymethod]
        fn unpin(&self, key: String) -> PyResult<()> {
            with_ctx(|ctx| {
                ctx.pinned_memory.remove(&key);
                ctx.collector.lock().unwrap().memory_unpins.push(key);
            });
            Ok(())
        }

        #[pymethod]
        fn list_pinned(&self, vm: &VirtualMachine) -> PyObjectRef {
            let keys: Vec<PyObjectRef> = with_ctx(|ctx| {
                let mut keys: Vec<String> = ctx.pinned_memory.keys().cloned().collect();
                keys.sort();
                keys.into_iter().map(|k| vm.ctx.new_str(k).into()).collect()
            });
            vm.ctx.new_list(keys).into()
        }

        #[pymethod]
        fn mark_sensitive(&self, key: String) -> PyResult<()> {
            with_collector(|c| c.sensitive_marks.push((key, true)));
            Ok(())
        }

        #[pymethod]
        fn unmark_sensitive(&self, key: String) -> PyResult<()> {
            with_collector(|c| c.sensitive_marks.push((key, false)));
            Ok(())
        }

        #[pymethod]
        fn _repr(&self) -> String {
            with_ctx(|ctx| format!("Memory({} keys, {} pinned)", ctx.memory_data.len(), ctx.pinned_memory.len()))
        }
    }

    // ------ PyTimerManager ------

    #[pyattr]
    #[pyclass(module = "_harness", name = "TimerManager")]
    #[derive(Debug, PyPayload, Default)]
    pub struct PyTimerManager;

    #[pyclass]
    impl PyTimerManager {
        #[pyslot]
        fn slot_new(cls: PyRef<PyType>, _args: FuncArgs, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
            Self.into_ref_with_type(vm, cls).map(Into::into)
        }

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
                    use vm::function::ArgIntoFloat;
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

            with_collector(|c| {
                let id = c.id_gen.next();
                let id_str = id.0.clone();
                c.timer_adds.push(TimerAddRequest { id, every_secs, at_epoch, priority, description });
                id_str
            }).pipe(Ok)
        }

        #[pymethod]
        fn cancel(&self, timer_id: String) -> PyResult<()> {
            with_collector(|c| c.timer_cancels.push(timer_id));
            Ok(())
        }

        #[pymethod]
        fn list(&self, vm: &VirtualMachine) -> PyObjectRef {
            let items: Vec<PyObjectRef> = with_ctx(|ctx| {
                ctx.timers_info.iter().map(|(id, desc, prio)| {
                    vm.ctx.new_tuple(vec![
                        vm.ctx.new_str(id.clone()).into(),
                        vm.ctx.new_str(desc.clone()).into(),
                        vm.ctx.new_int(*prio).into(),
                    ]).into()
                }).collect()
            });
            vm.ctx.new_list(items).into()
        }
    }

    // ------ PyHistoryEntry ------

    #[pyattr]
    #[pyclass(module = "_harness", name = "HistoryEntry")]
    #[derive(Debug, PyPayload, Default)]
    pub struct PyHistoryEntry {
        data: Mutex<Option<HistoryEntryData>>,
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
    pub struct PyHistoryManager;

    #[pyclass]
    impl PyHistoryManager {
        #[pyslot]
        fn slot_new(cls: PyRef<PyType>, _args: FuncArgs, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
            Self.into_ref_with_type(vm, cls).map(Into::into)
        }

        #[pymethod]
        fn _getitem(&self, id: PyStrRef, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
            let id: &str = id.as_ref();
            with_ctx(|ctx| {
                ctx.history_entries.get(id)
                    .map(|d| PyHistoryEntry { data: Mutex::new(Some(d.clone())) }.into_pyobject(vm))
                    .ok_or_else(|| vm.new_key_error(
                        vm.ctx.new_str(format!("No history entry with id {}", id)).into()
                    ))
            })
        }

        #[pymethod]
        fn replace_with_description(&self, id: String, description: String) -> PyResult<()> {
            with_collector(|c| c.history_replaces.push((id, description)));
            Ok(())
        }

        #[pymethod]
        fn remove(&self, id: String) -> PyResult<()> {
            with_collector(|c| c.history_removes.push(id));
            Ok(())
        }

        #[pymethod]
        fn add(&self, text: String, vm: &VirtualMachine) -> PyResult<()> {
            let is_compaction = with_ctx(|ctx| ctx.is_compaction);
            if !is_compaction {
                return Err(vm.new_runtime_error("history.add() can only be used during compaction"));
            }
            with_collector(|c| c.history_adds.push(text));
            Ok(())
        }
    }

    // ------ PyHarness (top-level functions) ------

    #[pyfunction]
    fn send_message(
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
                return Err(vm.new_exception_msg(
                    vm.ctx.exceptions.file_not_found_error.to_owned(),
                    format!("send_message: attachment not found: {}", p).into(),
                ));
            }
        }
        let react_to = react_to.flatten();
        with_collector(|c| c.messages.push(OutboundMessageRequest { chat_id, content, attachments: attach, react_to }));
        Ok(())
    }

    #[pyfunction]
    fn shell_exec(args: FuncArgs, vm: &VirtualMachine) -> PyResult<String> {
        let mut args = args;
        let cmd: String = args.take_positional_keyword("cmd")
            .map(|o| String::try_from_object(vm, o))
            .transpose()?
            .ok_or_else(|| vm.new_type_error("shell_exec() missing required argument: 'cmd'"))?;
        let proc_args: Vec<String> = args.take_keyword("args")
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

        with_collector(|c| {
            let id = c.id_gen.next();
            let id_str = id.0.clone();
            c.process_starts.push(ProcessStartRequest {
                id, cmd, args: proc_args, env, description,
                alert_timer_secs: alert_secs, success_prio, fail_prio, block_for_ms, interactive,
            });
            id_str
        }).pipe(Ok)
    }

    #[pyfunction]
    fn shell_input(pid: String, data: String) -> PyResult<()> {
        with_collector(|c| c.stdin_writes.push((pid, data.into_bytes())));
        Ok(())
    }

    #[pyfunction]
    fn shell_close_stdin(pid: String) -> PyResult<()> {
        with_collector(|c| c.stdin_closes.push(pid));
        Ok(())
    }

    #[pyfunction]
    fn kill_child(name: String) -> PyResult<()> {
        with_collector(|c| c.child_kills.push(name));
        Ok(())
    }

    #[pyfunction]
    fn shell_status(pid: String) -> String {
        with_ctx(|ctx| ctx.process_statuses.get(&pid).cloned().unwrap_or_else(|| "unknown".to_string()))
    }

    #[pyfunction]
    fn shell_output(pid: String, lines: OptionalArg<usize>) -> String {
        let full = with_ctx(|ctx| ctx.process_outputs.get(&pid).cloned().unwrap_or_default());
        match lines {
            OptionalArg::Present(n) => {
                let all: Vec<&str> = full.lines().collect();
                all[all.len().saturating_sub(n)..].join("\n")
            }
            OptionalArg::Missing => full,
        }
    }

    #[pyfunction]
    fn shell_kill(pid: String) -> PyResult<()> {
        with_collector(|c| c.process_kills.push(pid));
        Ok(())
    }

    #[pyfunction]
    fn processes_list(vm: &VirtualMachine) -> PyObjectRef {
        let items: Vec<PyObjectRef> = with_ctx(|ctx| {
            ctx.process_info.iter().map(|(pid, cmd, desc, status)| {
                vm.ctx.new_tuple(vec![
                    vm.ctx.new_str(pid.clone()).into(),
                    vm.ctx.new_str(cmd.clone()).into(),
                    vm.ctx.new_str(desc.clone()).into(),
                    vm.ctx.new_str(status.clone()).into(),
                ]).into()
            }).collect()
        });
        vm.ctx.new_list(items).into()
    }

    #[pyfunction]
    fn view(paths: PosArgs<String>, vm: &VirtualMachine) -> PyResult<()> {
        let paths: Vec<String> = paths.into_vec();
        for p in &paths {
            if !std::path::Path::new(p).is_file() {
                return Err(vm.new_exception_msg(
                    vm.ctx.exceptions.file_not_found_error.to_owned(),
                    format!("view: file not found or not a regular file: {}", p).into(),
                ));
            }
        }
        with_collector(|c| c.view_paths.extend(paths));
        Ok(())
    }

    #[pyfunction]
    fn acknowledge_timer(timer_id: String) -> PyResult<()> {
        with_collector(|c| c.timer_acks.push(timer_id));
        Ok(())
    }

    #[pyfunction]
    fn compact() -> PyResult<()> {
        with_collector(|c| c.compact_called = true);
        Ok(())
    }

    #[pyfunction]
    fn request_compaction() -> PyResult<()> {
        with_collector(|c| c.compaction_requested = true);
        Ok(())
    }

    #[pyfunction]
    fn fork(children: PyObjectRef, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
        let depth = with_ctx(|ctx| ctx.child_depth_remaining);
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

        with_collector(|c| c.fork_requests.push(ForkRequest { children: child_settings }));
        let name_objs: Vec<PyObjectRef> = names.into_iter().map(|n| vm.ctx.new_str(n).into()).collect();
        Ok(vm.ctx.new_list(name_objs).into())
    }

    #[pyfunction]
    fn message_agent(name: String, content: String, priority: OptionalArg<u8>) -> PyResult<()> {
        let priority = priority.unwrap_or(6);
        with_collector(|c| c.agent_messages.push(AgentMessageRequest { recipient: name, content, priority }));
        Ok(())
    }

    #[pyfunction]
    fn done(kwargs: KwArgs, vm: &VirtualMachine) -> PyResult<()> {
        let mut result = HashMap::new();
        for (key, val) in kwargs {
            let serde_val = py_to_json(vm, &val)
                .map_err(|_| vm.new_value_error(format!("done() kwargs must be JSON-serializable (key: {})", key)))?;
            result.insert(key, serde_val);
        }
        with_collector(|c| {
            c.done_called = true;
            c.done_result = result;
        });
        Ok(())
    }

    #[pyfunction]
    fn agent_name() -> String {
        with_ctx(|ctx| ctx.agent_name.clone())
    }

    #[pyfunction]
    fn agent_lineage() -> String {
        with_ctx(|ctx| ctx.agent_lineage.clone())
    }

    #[pyfunction]
    fn harness_bin() -> String {
        with_ctx(|ctx| ctx.harness_bin.clone())
    }

    // ------ StdoutCapture ------

    #[pyattr]
    #[pyclass(module = "_harness", name = "StdoutCapture")]
    #[derive(Debug, PyPayload, Default)]
    pub struct StdoutCapture;

    #[pyclass]
    impl StdoutCapture {
        #[pyslot]
        fn slot_new(cls: PyRef<PyType>, _args: FuncArgs, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
            Self.into_ref_with_type(vm, cls).map(Into::into)
        }

        #[pymethod]
        fn write(&self, text: PyStrRef) -> usize {
            let s: &str = text.as_ref();
            with_ctx(|ctx| ctx.stdout_buf.lock().unwrap().push_str(s));
            s.len()
        }

        #[pymethod]
        fn flush(&self) -> PyResult<()> {
            Ok(())
        }
    }
}

// Helper trait for pipe-style chaining (avoids temp bindings).
trait Pipe: Sized {
    fn pipe<R>(self, f: impl FnOnce(Self) -> R) -> R { f(self) }
}
impl<T> Pipe for T {}

// ---- Python Preamble ----
//
// Wraps Rust classes with thin Python classes that provide dunder methods.
// This sidesteps RustPython's AsMapping/AsSequence trait requirement for
// operator dispatch.

const PREAMBLE: &str = r#"
import _harness
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

# Capture stdout/stderr
_cap = _harness.StdoutCapture()
sys.stdout = _cap
sys.stderr = _cap

# Instantiate harness state objects
work_queue = _WorkQueueWrap(_harness.WorkQueue())
memory = _MemoryWrap(_harness.Memory())
timers = _harness.TimerManager()
history = _HistoryWrap(_harness.HistoryManager())

# Top-level functions
send_message = _harness.send_message
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
compact = _harness.compact
compaction_script = ""
"#;

// ---- Executor ----

// RustPython's Interpreter is !Sync (holds RefCells), so it can't go in a
// OnceLock. Thread-local is fine: the agent loop is single-threaded, and
// each turn's execute() runs on that same thread.
//
// The Interpreter is leaked (Box::leak) because RustPython's Drop impl
// touches the VM's PyObjectRef graph during thread-local destructor cleanup,
// which panics. We only ever build one interpreter per process, so leaking
// is correct — the OS reclaims it at exit.
thread_local! {
    static INTERPRETER: std::cell::OnceCell<&'static vm::Interpreter> = const { std::cell::OnceCell::new() };
}

fn with_interpreter<R>(f: impl FnOnce(&vm::Interpreter) -> R) -> R {
    INTERPRETER.with(|cell| {
        let interp = cell.get_or_init(|| {
            let builder = rustpython::Interpreter::builder(Default::default());
            let def = harness::module_def(&builder.ctx);
            let interp = builder.init_stdlib().add_native_module(def).build();
            Box::leak(Box::new(interp))
        });
        f(interp)
    })
}

pub fn initialize_python() {
    with_interpreter(|_| ());
}

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
    // TODO: timeout/interrupt. RustPython has vm.set_interrupt() — needs a
    // thread-based wrapper similar to python.rs's. For now, run synchronously
    // and ignore timeout_secs.
    let _ = timeout_secs;

    execute_inner(
        state, code, is_compaction, process_outputs, child_depth_remaining,
        agent_name, agent_lineage, pinned_memory,
    )
}

fn run_code(vm: &VirtualMachine, scope: &vm::scope::Scope, source: &str, name: &str) -> PyResult<()> {
    let code = vm
        .compile(source, vm::compiler::Mode::Exec, name.to_owned())
        .map_err(|e| vm.new_syntax_error(&e, Some(source)))?;
    vm.run_code_obj(code, scope.clone())?;
    Ok(())
}

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

    // Build per-turn context from HarnessState.
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

    let ctx = ExecContext {
        collector: collector.clone(),
        work_items,
        memory_data: state.memory.clone(),
        memory_priorities: state.memory_priorities.clone(),
        pinned_memory: pinned_memory.clone(),
        timers_info,
        history_entries,
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
        stdout_buf: stdout_buf.clone(),
    };

    // Install context, run, then tear down.
    EXEC_CTX.with(|cell| *cell.borrow_mut() = Some(ctx));

    let result: Result<(), String> = with_interpreter(|interp| interp.enter(|vm| {
        let scope = vm.new_scope_with_builtins();

        let run = |src: &str, name: &str| -> Result<(), String> {
            run_code(vm, &scope, src, name).map_err(|e| {
                let mut s = String::new();
                vm.write_exception(&mut s, &e).ok();
                s
            })
        };

        run(PREAMBLE, "<preamble>")?;
        if is_compaction {
            run(COMPACTION_PREAMBLE, "<compaction_preamble>")?;
        }
        run(code, "<agent>")?;

        // Read back compaction_script if set.
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
    }));

    EXEC_CTX.with(|cell| *cell.borrow_mut() = None);

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
        Err(error_text) => ExecutionResult {
            stdout,
            is_error: true,
            error_text,
            side_effects: SideEffectCollector::default(),
        },
    }
}

// ---- Tests ----

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_print() {
        let state = HarnessState::new(200_000, 16384);
        let result = execute(&state, "print('hello world')", false, &HashMap::new());
        assert!(!result.is_error, "Error: {}", result.error_text);
        assert_eq!(result.stdout.trim(), "hello world");
    }

    #[test]
    fn test_memory_operations() {
        let mut state = HarnessState::new(200_000, 16384);
        state.memory.insert("key1".to_string(), serde_json::json!("val1"));

        let result = execute(
            &state,
            r#"
assert memory["key1"] == "val1"
memory["key2"] = "val2"
assert "key1" in memory
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
    fn test_send_message() {
        let state = HarnessState::new(200_000, 16384);
        let result = execute(&state, r#"send_message("chat1", "hello")"#, false, &HashMap::new());
        assert!(!result.is_error, "Error: {}", result.error_text);
        assert_eq!(result.side_effects.messages.len(), 1);
        assert_eq!(result.side_effects.messages[0].chat_id, "chat1");
        assert_eq!(result.side_effects.messages[0].content, "hello");
    }

    #[test]
    fn test_shell_exec() {
        let state = HarnessState::new(200_000, 16384);
        let result = execute(
            &state,
            r#"pid = shell_exec("echo", args=["hi"], description="test")
print(pid)"#,
            false,
            &HashMap::new(),
        );
        assert!(!result.is_error, "Error: {}", result.error_text);
        assert_eq!(result.side_effects.process_starts.len(), 1);
        assert_eq!(result.side_effects.process_starts[0].cmd, "echo");
        assert_eq!(result.side_effects.process_starts[0].args, vec!["hi"]);
    }

    #[test]
    fn test_error_rolls_back_side_effects() {
        let state = HarnessState::new(200_000, 16384);
        let result = execute(
            &state,
            r#"
send_message("chat1", "hello")
raise RuntimeError("boom")
"#,
            false,
            &HashMap::new(),
        );
        assert!(result.is_error);
        assert!(result.error_text.contains("boom"));
        assert_eq!(result.side_effects.messages.len(), 0);
    }

    #[test]
    fn test_timer_add() {
        let state = HarnessState::new(200_000, 16384);
        let result = execute(
            &state,
            r#"tid = timers.add(every=60, description="ping")
print(tid)"#,
            false,
            &HashMap::new(),
        );
        assert!(!result.is_error, "Error: {}", result.error_text);
        assert_eq!(result.side_effects.timer_adds.len(), 1);
        assert_eq!(result.side_effects.timer_adds[0].every_secs, Some(60));
        assert_eq!(result.side_effects.timer_adds[0].description, "ping");
    }

    #[test]
    fn test_done_with_kwargs() {
        let state = HarnessState::new(200_000, 16384);
        let result = execute(
            &state,
            r#"done(status="ok", count=42)"#,
            false,
            &HashMap::new(),
        );
        assert!(!result.is_error, "Error: {}", result.error_text);
        assert!(result.side_effects.done_called);
        assert_eq!(result.side_effects.done_result.get("status"), Some(&serde_json::json!("ok")));
        assert_eq!(result.side_effects.done_result.get("count"), Some(&serde_json::json!(42)));
    }
}
