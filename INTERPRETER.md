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
1. Clone HarnessState (the transaction, "txn")
2. Move txn components into pyclass instances:
     PyMemory owns Mutex<HashMap>, PyWorkQueue owns Mutex<WorkQueue>, etc.
   Shared pieces (IdGenerator) wrapped in Arc<Mutex<>> across instances.
3. Create Arc<Mutex<ExternalEffects>> — only for irreversible operations
4. Create fresh PyDict globals/locals, inject pyclass instances + _harness
5. Run preamble (aliases send_message = _harness.send_message, etc.)
6. Run the agent's code via py.run(), bounded by timeout
7. On success: Py::borrow() + mem::take() to extract mutated components
   back into self.state. Then apply ExternalEffects (spawn OS processes,
   broadcast messages, fork children, pin to SQLite).
8. On error: drop txn, drop ExternalEffects. Original state untouched.
```

Step 6 is bounded by a configurable timeout (`CLAUDE_SERVER_PYTHON_TIMEOUT`,
default 5 seconds). If the script blocks beyond this limit, `PyErr_SetInterrupt`
is called to raise a `KeyboardInterrupt` in the Python thread.

**Transactional semantics**: in-state mutations (memory, timers, hooks, queue,
history) happen directly on the clone. External effects (OS process spawn,
message broadcast, child fork, SQLite pin) are deferred because they can't be
un-done by dropping a clone. On error, the clone is discarded and externals
never ran — original state untouched, no partial application.

**Read-after-write works**: `register_hook()` pushes into `Mutex<Vec<Hook>>`;
`list_hooks()` reads the same Mutex. `shell_exec()` adds a `ManagedProcess`
entry to `Mutex<ProcessManager>`; `processes_list()` reads it same-turn. No
snapshot/collector divergence.

## Namespace

The agent's Python code sees these objects:

| Object | Type | Source |
|--------|------|--------|
| `work_queue` | `PyWorkQueue` | Snapshot of `state.work_queue` |
| `memory` | `PyMemory` | Clone of `state.memory` + pinned snapshot (two-tier store) |
| `timers` | `PyTimerManager` | Timer metadata from `state.timer_manager` |
| `history` | `PyHistoryManager` | History entries from `state.event_history` |
| `send_message(chat_id, content)` | function | From `_harness.send_message` |
| `shell_exec(cmd, args, ...)` | function | From `_harness.shell_exec` |
| `shell_status(pid)` | function | From `_harness.shell_status` |
| `shell_output(pid)` | function | From `_harness.shell_output` |
| `shell_kill(pid)` | function | From `_harness.shell_kill` |
| `attach(path)` | function | Queue a file for next turn's context (image → vision) |
| `fork([ChildSettings(...)])` | function | Spawn child agents |
| `message_agent(name, content)` | function | Inter-agent messaging |
| `done(**result)` | function | Exit, passing `result` dict to parent |
| `agent_name`, `agent_lineage` | str | Identity strings |
| `ChildSettings` | dataclass | `name`, `task`, `model`, `max_turns`, `can_compact`, `attach` |
| `timedelta`, `datetime` | classes | From Python's `datetime` module |

In compaction mode, `compact()` and `compaction_script` are also available.

### Memory: two tiers

**Local tier** — per-agent, in `state.memory` + `state.memory_priorities`:

- `memory[k] = v` / `memory.set(k, v, priority=N)` — any JSON type
- `memory.set_priority(k, N)`, `memory.get_priority(k)`
- Higher priority → rendered first in `<agent_state>`, survives truncation

**Pinned tier** — shared across all agents, stored in SQLite (`pinned_memory` table),
injected into the system prompt (cached via `cache_control: ephemeral`):

- `memory.pin(k, v)` — `v` must be a string
- `memory.unpin(k)`, `memory.list_pinned()`
- `memory.get(k)` checks local first, then falls back to pinned
- `k in memory` is true if in either tier

Pins/unpins are external (SQLite, cross-agent) — they flow through
`ExternalEffects.memory_pins`/`memory_unpins` and `agent_loop.rs` writes them
to SQLite via `db.save_pin()`/`db.delete_pin()` on commit.

### WorkItem field access

`PyWorkItem` has fixed fields `id`, `priority`, `time`, `type`, plus a
`fields: serde_json::Map` populated from the `WorkItemType` variant. `__getattr__`
looks up in `fields` — field names match the Rust struct field names exactly.
Wrong-field access raises `AttributeError` listing available fields. See
`work_item_to_py()` in `python.rs` for the single source of truth.

The convenience functions (`send_message`, `shell_exec`, etc.) are created
by the preamble, which assigns `_harness.method_name` to top-level names.

## Clone-and-Mutate

Each pyclass owns a `Mutex<T>` of cloned state. Mutations happen directly:

```
Agent calls memory["key"] = "value"
  → PyMemory.__setitem__ locks self.inner, inserts into the HashMap
  → A subsequent memory["key"] read on the same turn sees it — same Mutex

Agent calls timers.add(every=30, ...)
  → Locks Arc<Mutex<IdGenerator>>, calls .next() for the ID
  → Locks Mutex<TimerManager>, builds+inserts Timer directly
  → Returns the ID string synchronously
```

`ExternalEffects` (an `Arc<Mutex<>>` shared across pyclass instances) collects
only operations that can't be rolled back: `process_starts` (OS spawn),
`messages` (broadcast), `forks`, `agent_messages`, `child_kills`,
`memory_pins`/`unpins` (SQLite, cross-agent), `view_requests`, plus
lifecycle flags (`done_called`, `compact_called`, etc.).

On commit, `apply_side_effects()` swaps in the extracted components
(`self.state.memory = extracted.memory`), then applies externals. The
replay loops for in-state ops are gone — ~120 LOC deleted.

## ID Assignment

`IdGenerator` is wrapped in `Arc<Mutex<>>` and shared between `PyTimers` and
`PyHarness` (both call `.next()`). When the script runs, IDs advance on the
txn's generator. On commit, the advanced generator becomes `self.state.id_generator`.
On error, the txn drops — IDs roll back.

History entry IDs are still generated AFTER commit (in agent_loop's post-turn
code). This prevents collisions between history IDs and IDs assigned to
timers/processes/children during the turn.

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
   `committed: None` — the cloned txn and ExternalEffects are both dropped
4. The core loop records the error in event history; no state swap, no externals

The agent sees the error in its history on the next turn and can fix its code.

## Compaction Mode

When compaction is active, two extra items are injected:

- `compaction_script = ""` — a mutable string the agent builds up with `+=`
- `compact()` — sets a flag in the collector

After execution, the value of `compaction_script` is read back from the
Python locals dict and stored in the collector. The core loop then uses this
script to manipulate history entries (removing old ones, adding summaries).

## Process Completion Guarantees

When a process spawned by `shell_exec()` finishes, the harness guarantees that
all stdout/stderr is flushed to the DB before the `ProcessCompleted` event is
sent. This is achieved by having the completion monitor task await the output
reader task's `JoinHandle` after `child.wait()` returns.

The `block_for` parameter on `shell_exec()` uses a `tokio::sync::oneshot` channel:
the completion monitor signals the oneshot after the process exits and output is
flushed. `apply_side_effects` awaits this oneshot with `tokio::time::timeout`,
returning as soon as the process finishes or the timeout elapses — whichever
comes first. The completion event then flows through the normal channel and is
picked up by `drain_events()` on the next loop iteration.

## Build Configuration

The `build.rs` script queries `python3 -c "import sysconfig; ..."` to discover
the Python library directory and bakes the rpath into the binary via
`cargo:rustc-link-arg=-Wl,-rpath,<libdir>`. This means the binary finds
`libpython` at runtime without needing `DYLD_LIBRARY_PATH` or `LD_LIBRARY_PATH`.

To target a specific Python installation: `PYO3_PYTHON=/path/to/python3 cargo build`
