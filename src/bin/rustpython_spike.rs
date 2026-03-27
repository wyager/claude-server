//! Spike: RustPython as a drop-in for PyO3.
//!
//! Goal: prove the core patterns claude-server needs work without libpython:
//!   1. Run a script with a fresh scope
//!   2. Expose a mutable host object (analog of SideEffectCollector)
//!   3. Expose host functions (send_message, memory.set, shell_exec analog)
//!   4. Capture stdout
//!   5. Handle script errors without panicking
//!
//! Run: cargo run --bin rustpython_spike
//!
//! Verdict at bottom of file.

use std::sync::{Arc, Mutex};

use rustpython::InterpreterBuilderExt;
use rustpython_vm as vm;
use vm::builtins::PyStrRef;
use vm::{pymodule, PyPayload, PyResult, VirtualMachine};

/// Analog of claude-server's SideEffectCollector — mutations accumulate here
/// during script execution, applied atomically after.
#[derive(Default, Debug)]
struct Collector {
    messages: Vec<(String, String)>,
    memory_sets: Vec<(String, serde_json::Value)>,
    shell_cmds: Vec<String>,
}

fn main() {
    let collector = Arc::new(Mutex::new(Collector::default()));

    // Full rustpython crate from git HEAD — sqlite is optional on main so
    // no libsqlite3-sys conflict. Gets us stdlib (json, io, os).
    let builder = rustpython::Interpreter::builder(Default::default());
    let def = harness::module_def(&builder.ctx);
    let interp = builder.init_stdlib().add_native_module(def).build();

    interp.enter(|vm| {
        // Inject the collector so harness functions can reach it. Same pattern
        // as PyO3: stash an Arc<Mutex<...>> the pyclass methods pull from.
        harness::set_collector(vm, collector.clone());

        // Fresh scope per turn — same as PyO3's fresh-PyDict-per-turn pattern.
        let scope = vm.new_scope_with_builtins();

        // Analog of the PREAMBLE that exposes harness functions as globals.
        let preamble = r#"
import _harness
send_message = _harness.send_message
shell_exec = _harness.shell_exec
memory = _harness.Memory()
"#;
        run(vm, &scope, preamble, "<preamble>").expect("preamble");

        // Agent script exercising stdlib: json, file I/O, os.path.
        // These are what claude-server's agent actually uses.
        let script = r#"
import json, os, tempfile

prefs = {"theme": "dark", "lang": "en"}
memory.set("user_prefs", json.dumps(prefs))
assert json.loads(memory.get("user_prefs"))["theme"] == "dark"

tmp = os.path.join(tempfile.gettempdir(), "rustpython_spike_test.txt")
with open(tmp, "w") as f:
    f.write("hello from rustpython stdlib")
with open(tmp) as f:
    content = f.read()
assert content == "hello from rustpython stdlib"
os.remove(tmp)

send_message("signal:+1555", f"stdlib works: {content[:10]}...")
pid = shell_exec("echo hi")

xs = [x*x for x in range(5)]
print(f"squares: {xs}, pid: {pid}")
print("json, open(), os.path all work")
"#;

        match run(vm, &scope, script, "<agent>") {
            Ok(()) => println!("✓ script ran clean"),
            Err(e) => {
                // Analog of PyO3's error formatting.
                let mut s = String::new();
                vm.write_exception(&mut s, &e).ok();
                println!("✗ script error:\n{}", s);
            }
        }
    });

    // Side effects collected atomically — same pattern as apply_side_effects.
    let c = collector.lock().unwrap();
    println!("\n--- collected side effects ---");
    println!("messages:    {:?}", c.messages);
    println!("memory_sets: {:?}", c.memory_sets);
    println!("shell_cmds:  {:?}", c.shell_cmds);
}

fn run(vm: &VirtualMachine, scope: &vm::scope::Scope, source: &str, name: &str) -> PyResult<()> {
    let code = vm
        .compile(source, vm::compiler::Mode::Exec, name.to_owned())
        .map_err(|e| vm.new_syntax_error(&e, Some(source)))?;
    vm.run_code_obj(code, scope.clone())?;
    Ok(())
}

#[pymodule(name = "_harness")]
mod harness {
    use super::*;
    use rustpython_vm::{pyclass, PyObjectRef, PyRef};
    use rustpython_vm::builtins::PyType;
    use rustpython_vm::function::FuncArgs;
    use std::cell::RefCell;

    // Thread-local collector handle. RustPython's VM is single-threaded per
    // interpreter, so a thread-local is safe here (same constraint as PyO3).
    thread_local! {
        static COLLECTOR: RefCell<Option<Arc<Mutex<Collector>>>> = RefCell::new(None);
    }

    pub fn set_collector(_vm: &VirtualMachine, c: Arc<Mutex<Collector>>) {
        COLLECTOR.with(|cell| *cell.borrow_mut() = Some(c));
    }

    fn with_collector<R>(f: impl FnOnce(&mut Collector) -> R) -> R {
        let arc = COLLECTOR.with(|cell| cell.borrow().as_ref().expect("collector not set").clone());
        let mut guard = arc.lock().unwrap();
        f(&mut guard)
    }

    #[pyfunction]
    fn send_message(chat_id: String, content: String) -> PyResult<()> {
        with_collector(|c| c.messages.push((chat_id, content)));
        Ok(())
    }

    #[pyfunction]
    fn shell_exec(cmd: String) -> PyResult<String> {
        with_collector(|c| c.shell_cmds.push(cmd));
        Ok("fake-pid-1234".into())
    }

    #[pyattr]
    #[pyclass(module = "_harness", name = "Memory")]
    #[derive(Debug, PyPayload, Default)]
    pub struct Memory {
        // Local view so the script can read back what it set this turn.
        // Real harness uses a PyDict; this is the minimal analog.
        local: Mutex<std::collections::HashMap<String, String>>,
    }

    #[pyclass]
    impl Memory {
        #[pyslot]
        fn slot_new(cls: PyRef<PyType>, _args: FuncArgs, vm: &VirtualMachine) -> PyResult<PyObjectRef> {
            Self::default().into_ref_with_type(vm, cls).map(Into::into)
        }

        #[pymethod]
        fn set(&self, key: String, value: PyStrRef) -> PyResult<()> {
            let v = value.to_string();
            self.local.lock().unwrap().insert(key.clone(), v.clone());
            let parsed: serde_json::Value = serde_json::from_str(&v)
                .unwrap_or(serde_json::Value::String(v));
            with_collector(|c| c.memory_sets.push((key, parsed)));
            Ok(())
        }

        #[pymethod]
        fn get(&self, key: String) -> Option<String> {
            self.local.lock().unwrap().get(&key).cloned()
        }

        // __contains__ needs `impl AsSequence` on HEAD — skipped for the
        // spike. The real port will implement it; not needed to prove stdlib.
        #[pymethod]
        fn contains(&self, key: String) -> bool {
            self.local.lock().unwrap().contains_key(&key)
        }
    }
}

// --- Verdict (2026-03-26, git HEAD with stdlib) ---
//
// WORKS:
//   ✓ Full stdlib: json.dumps/loads, open()/read()/write(), os.path,
//     tempfile, os.remove — all verified via asserts + roundtrip
//   ✓ Interpreter init, fresh scope per run, #[pyfunction], #[pyclass]
//   ✓ Side-effect collection via thread-local Arc<Mutex>
//   ✓ ZERO libpython linkage
//
// DEPS PICKED UP (from stdlib, need feature-flagging off):
//   - liblzma (Python's `lzma` module — agent doesn't use)
//   - libffi (Python's `ctypes` module — agent doesn't use)
//   Both are stdlib modules we can likely disable via rustpython features.
//
// BUILD NOTES:
//   - Git HEAD requires rustc ≥1.93
//   - Must `cargo update` after adding git dep (cascading lock conflicts)
//   - Both `rustpython` AND `rustpython-vm` must be direct deps from the
//     same git rev — proc macros expand to `::rustpython_vm::...` paths
//   - sqlite is optional on main (`default = ["compiler", "host_env"]`),
//     no conflict with rusqlite
//   - Release binary: 41M with freeze-stdlib (vs 14M vm-only, 17M PyO3).
//     The +27M is the frozen Python stdlib bytecode. Fully self-contained.
//
// REMAINING WORK FOR REAL PORT:
//   - __contains__/__getitem__/etc: need `impl AsSequence`/`AsMapping`
//     traits on HEAD, not #[pymethod]. Mechanical but touches every
//     dunder method in python.rs.
//   - stdout capture: not tested, same approach as PyO3 should work
//   - Timeout/interrupt: vm.set_interrupt() exists, untested
//   - Disable lzma/ctypes/ssl stdlib modules to drop liblzma/libffi
//
// MIGRATION ESTIMATE: unchanged — a focused session or two once the
// dunder-method trait impls are sorted.
