# Python Interpreter Integration

How Claude Server embeds and runs Python via RustPython.

## Why RustPython

Prior to 0.3.0 we used PyO3 + CPython. That meant the binary linked against
`libpythonX.Y.so` — build on Python 3.12, deploy to a box with 3.11, get
"cannot open shared object file." RustPython is a pure-Rust Python 3
implementation: the stdlib is frozen into the binary, zero libpython linkage,
`cargo build` produces a self-contained executable.

Tradeoffs: ~2-4× slower than CPython (irrelevant — scripts run microseconds),
no C-extension modules (no numpy/requests; agent uses shell_exec for HTTP
anyway), `import sqlite3` unavailable (we use rusqlite directly in Rust).

## Lifecycle

**Startup**: `AgentLoop::new()` constructs an `Executor` which builds one
`vm::Interpreter` with stdlib frozen in. No global singletons — the interpreter
is a struct field, drops cleanly during normal shutdown.

**Per turn**: `executor.execute()` enters the VM, creates a fresh scope,
constructs new pyclass instances carrying this turn's data (memory snapshot,
work items, collector), injects them into the scope, runs the script. No state
leaks between turns.

## Execution Flow

```
1. Clone IdGenerator from HarnessState into a SideEffectCollector
2. Wrap collector in Arc<Mutex<>>
3. interpreter.enter(|vm| { ... })
4. vm.import("_harness", 0) — REQUIRED before into_ref() or type cells panic
5. Construct PyMemory/PyWorkQueue/PyTimers/PyHistory/PyHarness with this
   turn's data as fields. Each gets a collector.clone().
6. Inject instances into scope as _memory, _work_queue, etc.
7. Run PREAMBLE — wraps instances with Python classes providing __getitem__/
   __setitem__/etc. dunders, aliases _harness methods as bare names
   (send_message = _harness.send_message)
8. If compaction mode: inject compaction_script variable
9. Run the agent's code
10. Extract collector, return ExecutionResult
```

## SideEffectCollector

Same pattern as before. All mutations (memory writes, timer creates, message
sends, process spawns) accumulate in the collector during execution. If the
script crashes, nothing applies. On success, `agent_loop::apply_side_effects()`
applies them atomically.

Each pyclass instance holds `collector: Arc<Mutex<SideEffectCollector>>` as a
field. Methods call `self.collector.lock().unwrap().field.push(...)`.

## Dunder Dispatch

RustPython's `#[pymethod]` doesn't auto-wire `__getitem__` into the `obj[k]`
operator — you'd need `impl AsMapping` trait boilerplate. Instead we expose
plain-named methods from Rust (`_getitem`, `_setitem`, `_contains`) and the
PREAMBLE wraps them with thin Python classes:

```python
class _MemoryWrap:
    def __getitem__(self, k): return self._m._getitem(k)
    def __setitem__(self, k, v): self._m._setitem(k, v)
    # ...
memory = _MemoryWrap(_memory)
```

Agent-visible behavior identical to standard Python. ~20 lines of PREAMBLE
instead of ~100 lines of trait impls.

## Timeout

`Executor` holds a `vm::signal::UserSignalSender` wired via `.init_hook()`.
`execute_with_timeout()` spawns a watchdog thread that races
`mpsc::recv_timeout(timeout)` against a cancel channel. On timeout, sends a
closure that raises `KeyboardInterrupt`; the VM picks it up at the next
bytecode boundary. Normal completion drops the cancel sender → watchdog exits
immediately. No polling.

## stdout/stderr Capture

A `StdoutCapture` pyclass with a `write()` method accumulates into a
`Mutex<String>`. PREAMBLE does `sys.stdout = sys.stderr = _cap`. The captured
output lands in `ExecutionResult.output`.

## RustPython API Quirks (for future maintenance)

- **`into_ref()` needs prior import**: Must `vm.import("_harness", 0)` before
  any `PyPayload::into_ref()` or it panics "static type has not been
  initialized". The module import populates the type cells.
- **`#[pymethod]` kwargs**: Unlike `#[pyfunction]`, method parameter names
  aren't exposed for kwarg binding. `message_agent(priority=9)` fails when
  called directly on the Rust method. PREAMBLE provides Python `def` wrappers
  that accept kwargs and forward positionally.
- **OSError subtypes**: `FileNotFoundError` etc. can't be constructed via
  `vm.new_exception_msg()`. Use `vm.new_os_subtype_error()` → downcast via
  `PyObjectRef` to get `PyBaseExceptionRef`.
- **`Vec<String>` return**: No blanket `IntoPyNativeFn` impl. Manually build
  `vm.ctx.new_list(items.map(|s| vm.new_pyobj(s)).collect()).into()`.

## Build Requirements

- rustc ≥ 1.93 (RustPython git HEAD's MSRV)
- `.cargo/config.toml` sets `RUST_MIN_STACK=16M` — freeze-stdlib's importlib
  bootstrap recurses deeply; default 2MB stack SIGILLs during tests
- Both `rustpython` and `rustpython-vm` as direct git deps (proc macros expand
  to `::rustpython_vm::...` paths)
- Pinned to rev `3f92c3a` — sqlite is optional on this rev (was unconditional
  in 0.4.0 release, conflicted with rusqlite's libsqlite3-sys)

## Runtime Dependencies

`otool -L` shows: libSystem, CoreFoundation, CoreServices, Security, libiconv,
libffi. All ship with macOS/Linux base. **No libpython. No liblzma** (liblzma-sys
forced to static bundled build). libffi is an unconditional RustPython vm-crate
dep at this rev — upstream PR needed to feature-gate it.
