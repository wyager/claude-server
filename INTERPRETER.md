# Python Interpreter Integration

How Claude Server embeds and runs Python via PyO3.

## Lifecycle

**Startup**: `pyo3::prepare_freethreaded_python()` is called once in `main.rs`.
This initializes the CPython interpreter process-wide. All subsequent
`Python::with_gil()` calls share this single interpreter.

**Per turn**: Each turn creates a fresh `PyDict` for globals and locals.
No state leaks between turns — the agent gets a clean namespace every time.
(Module-level imports cached in `sys.modules` persist, but this is harmless.)

**No sub-interpreters**: PyO3 does not support Python sub-interpreters.
The fresh-namespace approach is the practical substitute.

## Execution Flow

```
1. Clone IdGenerator from HarnessState into a SideEffectCollector
2. Create Arc<Mutex<SideEffectCollector>> shared across all #[pyclass] objects
3. Create fresh PyDict globals/locals
4. Redirect sys.stdout and sys.stderr to a StdoutCapture object
5. Inject objects into locals: work_queue, memory, timers, history, _harness
6. Run preamble (creates convenience functions like send_message, shell_exec)
7. If compaction mode: inject compaction_script variable and compact() function
8. Run the agent's code via py.run()
9. If compaction mode: read back compaction_script from locals
10. Extract SideEffectCollector, return ExecutionResult
```

On error at step 8, the Python traceback is formatted and returned.
The `SideEffectCollector` is replaced with an empty default — all side effects
are discarded. This makes execution transactional: either everything succeeds
and all mutations apply, or nothing changes.

## Namespace

The agent's Python code sees these objects:

| Object | Type | Source |
|--------|------|--------|
| `work_queue` | `PyWorkQueue` | Snapshot of `state.work_queue` |
| `memory` | `PyMemory` | Clone of `state.memory` (dict-like) |
| `timers` | `PyTimerManager` | Timer metadata from `state.timer_manager` |
| `history` | `PyHistoryManager` | History entries from `state.event_history` |
| `send_message(chat_id, content)` | function | From `_harness.send_message` |
| `shell_exec(cmd, args, ...)` | function | From `_harness.shell_exec` |
| `shell_status(pid)` | function | From `_harness.shell_status` |
| `shell_output(pid)` | function | From `_harness.shell_output` |
| `shell_kill(pid)` | function | From `_harness.shell_kill` |
| `show_in_context(data)` | function | From `_harness.show_in_context` |
| `timedelta`, `datetime` | classes | From Python's `datetime` module |

In compaction mode, `compact()` and `compaction_script` are also available.

The convenience functions (`send_message`, `shell_exec`, etc.) are created
by the preamble, which assigns `_harness.method_name` to top-level names.

## Side Effect Collection

Every `#[pyclass]` object holds an `Arc<Mutex<SideEffectCollector>>`. When the
agent calls a mutating method (e.g., `work_queue.pop_front()`, `memory["x"] = "y"`,
`timers.add(...)`, `send_message(...)`), the method records the operation in
the shared collector rather than modifying harness state directly.

```
Agent calls memory["key"] = "value"
  → PyMemory.__setitem__ records ("key", "value") in collector.memory_sets
  → Also updates the local dict so subsequent reads see the new value

Agent calls timers.add(every=30, ...)
  → PyTimerManager.add calls collector.id_gen.next() to get a new ID
  → Records a TimerAddRequest in collector.timer_adds
  → Returns the ID string to the agent synchronously
```

After execution, `core_loop::apply_side_effects()` processes the collector:
memory sets/deletes, queue removes, timer adds/cancels, filter changes,
history modifications, process starts/kills, outbound messages, and compaction.

## ID Assignment

The `IdGenerator` is cloned from `HarnessState` into the `SideEffectCollector`
before execution. When the agent calls `timers.add()` or `shell_exec()`,
the `#[pyclass]` method calls `id_gen.next()` on the collector's generator
and returns the hex ID string synchronously. After execution, the updated
`IdGenerator` (with its advanced counter) is moved back into `HarnessState`.

This means IDs are assigned during Python execution, not deferred.

## Stdout Capture

`sys.stdout` and `sys.stderr` are both replaced with a `StdoutCapture` object
that has `write(text)` and `flush()` methods. `write()` appends to a shared
`Arc<Mutex<String>>` buffer. After execution, the buffer contents become the
history entry's `output` field.

Both stdout and stderr go to the same buffer — there is no separation at
the Python level. (Process-level stderr from `shell_exec` is captured
separately by the process supervisor.)

## Type Coercion

The `extract_seconds()` helper (used by `timers.add(every=...)` and
`shell_exec(alert_timer=...)`) accepts both:

- **Numbers**: `30`, `300.0` — interpreted as seconds
- **timedelta objects**: `timedelta(seconds=30)`, `timedelta(minutes=5)` —
  `.total_seconds()` is called to get the numeric value

The `at=` parameter in `timers.add()` similarly accepts both:

- **datetime objects**: `datetime(2026, 2, 1, 17, 0, 0)` — `.timestamp()`
  is called to get epoch seconds
- **Numbers**: treated as epoch seconds directly

## Error Handling

If `py.run()` raises a Python exception:

1. The traceback is formatted via `e.traceback(py).format()`
2. The exception string is appended: `format!("{}{}", traceback, e)`
3. An `ExecutionResult` is returned with `is_error: true` and
   `side_effects: SideEffectCollector::default()` (empty — no mutations applied)
4. The core loop records the error in event history but skips `apply_side_effects()`

The agent sees the error in its history on the next turn and can fix its code.

## Compaction Mode

When compaction is active, two extra items are injected:

- `compaction_script = ""` — a mutable string the agent builds up with `+=`
- `compact()` — sets a flag in the collector

After execution, the value of `compaction_script` is read back from the
Python locals dict and stored in the collector. The core loop then uses this
script to manipulate history entries (removing old ones, adding summaries).

## Build Configuration

The `build.rs` script queries `python3 -c "import sysconfig; ..."` to discover
the Python library directory and bakes the rpath into the binary via
`cargo:rustc-link-arg=-Wl,-rpath,<libdir>`. This means the binary finds
`libpython` at runtime without needing `DYLD_LIBRARY_PATH` or `LD_LIBRARY_PATH`.

To target a specific Python installation: `PYO3_PYTHON=/path/to/python3 cargo build`
