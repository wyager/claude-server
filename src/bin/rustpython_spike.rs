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

    // Using rustpython-vm directly (not the full rustpython crate) to dodge
    // the libsqlite3-sys links conflict with rusqlite. This means no stdlib
    // — see verdict for implications.
    let interp = vm::Interpreter::with_init(Default::default(), |vm| {
        vm.add_native_module("_harness".to_owned(), Box::new(harness::make_module));
    });

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

        // The actual agent script. No `import json` here — stdlib is
        // stripped in this spike due to the sqlite conflict. The core
        // language features (f-strings, comprehensions, dicts) are
        // builtins and still work.
        let script = r#"
memory.set("user_prefs", '{"theme": "dark", "lang": "en"}')
send_message("signal:+1555", "Hello from RustPython!")
pid = shell_exec("echo hi")

xs = [x*x for x in range(5)]
print(f"squares: {xs}")
print(f"pid: {pid}")
print(f"memory has user_prefs: {'user_prefs' in memory}")
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
            let v = value.as_str().to_string();
            self.local.lock().unwrap().insert(key.clone(), v.clone());
            let parsed: serde_json::Value = serde_json::from_str(&v)
                .unwrap_or(serde_json::Value::String(v));
            with_collector(|c| c.memory_sets.push((key, parsed)));
            Ok(())
        }

        #[pymethod(name = "__contains__")]
        fn contains(&self, key: String) -> bool {
            self.local.lock().unwrap().contains_key(&key)
        }
    }
}

// --- Verdict (2026-03-26, spike run) ---
//
// WORKS:
//   ✓ Interpreter init, fresh scope per run
//   ✓ #[pyfunction] host functions (send_message, shell_exec)
//   ✓ #[pyclass] with #[pymethod] (memory.set)
//   ✓ Side-effect collection via thread-local Arc<Mutex>
//   ✓ Error formatting with traceback (vm.write_exception)
//   ✓ Core language: f-strings, comprehensions, dicts, range
//   ✓ ZERO libpython linkage — only CoreFoundation/libiconv/libSystem
//   ✓ Release binary: 14M (vs claude-server 17M — roughly a wash)
//
// NEEDS WORK (all solvable):
//   - __contains__ via #[pymethod] not recognized — needs Contains slot/trait
//   - stdlib: using rustpython-vm directly (no stdlib) because
//     rustpython-stdlib pulls libsqlite3-sys 0.28, conflicts with our
//     rusqlite's 0.30. Options: (a) downgrade rusqlite, (b) patch
//     rustpython-stdlib to disable sqlite module, (c) bump rusqlite when
//     rustpython updates. For the agent's actual usage (json, open, os.path),
//     we NEED stdlib — this must be resolved before a real port.
//   - stdout capture: not attempted. RustPython has sys.stdout reassignment
//     like CPython; same approach as PyO3 should work.
//   - Timeout/interrupt: RustPython has vm.set_interrupt() — analog of
//     PyErr_SetInterrupt. Untested here.
//
// MIGRATION ESTIMATE: python.rs is ~1400 LOC. #[pyclass]/#[pymethod]/
// #[pyfunction] map nearly 1:1. Main work is (1) the stdlib/sqlite conflict,
// (2) re-testing every host method, (3) verifying timeout/interrupt works.
// Maybe 2-3 days once stdlib is unblocked.
