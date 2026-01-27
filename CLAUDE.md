# Claude Server — Project Guide

## Build & Run

```bash
make          # build and run the daemon (requires ANTHROPIC_API_KEY env var)
make chat     # build, open browser, and run the chat web UI
make run-dump # run with full context/response dumps each turn
make build    # build only
# CLI flags:
#   --dump-turns       print full context/response to stdout each turn
#   --dump-dir <path>  write turn dumps to files (parent-001-dump.txt, child-<id>-001-dump.txt)
#                      dumps include: CONTEXT, RESPONSE, THINKING, and EXECUTION OUTPUT/ERROR
```

Ctrl+C triggers a graceful shutdown (saves state before exiting).

Tests require single-threaded execution due to Python GIL contention:

```bash
cargo test -- --test-threads=1
```

The `build.rs` bakes the Python dylib rpath into the binary, so no `DYLD_LIBRARY_PATH` is needed.
To target a specific Python: `PYO3_PYTHON=/path/to/python3 cargo build`.

## Architecture

The system is a Rust daemon that drives Claude through a work-queue loop. Each turn:
1. Render state (history + work queue + metadata) into text
2. Call Claude API with that text as a single user message
3. Claude responds with Python code via a `tool_use` block
4. Execute the Python in a fresh namespace (PyO3, not a subprocess)
5. Collect side effects atomically, apply to state, persist to SQLite

External events (user messages, process completions) arrive via tokio mpsc
channels and get converted into work queue items. When idle, the core loop
uses `tokio::select!` to race the event channel against a sleep-until-next-timer
future — no polling. The core loop owns all mutable state; no shared-state
concurrency.

See `INTERPRETER.md` for details on the Python integration.

## Source Layout

| File | Purpose |
|------|---------|
| `main.rs` | CLI dispatch: default runs daemon, `chat` runs web UI |
| `config.rs` | Config from env vars (`ANTHROPIC_API_KEY`, model, ports, paths) |
| `types.rs` | Core types: WorkQueue, EventHistory, TimerManager, ProcessManager, Memory, HarnessState, API request/response types |
| `core_loop.rs` | Main event loop: drain events → check timers → render → API call → Python exec → apply side effects → persist |
| `python.rs` | PyO3 executor: #[pyclass] wrappers for work_queue, memory, timers, history, harness functions. SideEffectCollector pattern. |
| `renderer.rs` | Serialize HarnessState into XML-formatted context text for the API call |
| `api_client.rs` | Claude Messages API client (reqwest, retry logic, tool_use extraction) |
| `db.rs` | SQLite persistence (state as JSON blob, process output, outbound messages) |
| `process.rs` | Tokio-based process spawning, output capture, completion/failure/timeout events |
| `compaction.rs` | Compaction state machine (trigger detection, script accumulation, execution) |
| `child_agent.rs` | Sub-agent loop: simplified core loop for spawned child agents (can send messages, no process spawning/child spawning) |
| `http_server.rs` | Axum HTTP API: POST /message, GET /status, GET /messages/:chat_id, GET /messages/:chat_id/stream (SSE), POST /shutdown |
| `chat.rs` | Chat UI subcommand: serves embedded HTML with API URL injection |
| `chat.html` | Single-file HTML/CSS/JS chat interface (embedded via include_str!) |
| `system_prompt.txt` | System prompt sent to Claude on every API call |
| `build.rs` | Discovers Python LIBDIR at build time, bakes rpath into binary |
| `INTERPRETER.md` | How the Python interpreter integration works (PyO3, side effects, etc.) |

## Key Design Patterns

**Side effect collection**: Python scripts don't execute side effects directly.
All mutations (memory writes, timer creates, message sends, process spawns) are
collected into a `SideEffectCollector` during execution. If the script crashes,
nothing is applied. On success, `core_loop::apply_side_effects()` applies them
atomically to the authoritative state.

**Synchronous ID assignment**: The `SideEffectCollector` owns the `IdGenerator`
during Python execution. When Claude calls `timers.add()` or `shell_exec()`,
the #[pyclass] method calls `id_gen.next()` synchronously and returns the ID.
After execution, the updated generator is moved back into HarnessState.

**Single-message context rebuild**: Each API call is a fresh conversation with
one user message containing the full rendered context. No multi-turn replay.
The system prompt is cached via `cache_control: { type: "ephemeral" }`.

**Fresh Python namespace per turn**: PyO3 initializes the interpreter once at
startup. Each turn creates a fresh `PyDict` as globals/locals — no state leaks
between turns. Module imports (`sys.modules`) persist but are harmless.

**Process output guarantees**: The completion monitor awaits the output reader's
JoinHandle before sending events, so output is always fully flushed when the
agent sees ProcessCompleted. The `block_for` parameter uses a oneshot channel
to let the core loop wait for fast processes to finish before the next turn.

**Python execution timeout**: Scripts are bounded by a configurable timeout
(`CLAUDE_SERVER_PYTHON_TIMEOUT`, default 5s). If a script blocks too long,
`PyErr_SetInterrupt` is used to interrupt execution.

**Memory priorities**: Memory keys can have an associated priority (0-10) via
`memory.set(key, value, priority=N)`. Higher-priority keys are rendered first
in the `<agent_state>` block and are less likely to be truncated.

**Agent state in context**: An `<agent_state>` block is rendered between the work
queue and context metadata, showing memory (sorted by priority), active timers,
and running processes. Bounded by RenderConfig limits (20 memory keys, 20 timers,
10 processes).

**Sub-agents**: `spawn_agent(task, model, memory, max_turns, priority)` launches
a child agent loop (`child_agent.rs`) that runs independently and returns a
`ChildAgentCompleted` work item with `result_memory`, `turns_used`, `success`,
and `summary`. Max 3 concurrent children, max 50 turns, no recursion. Children
can send messages via `send_message()` but cannot spawn processes (`shell_exec`)
or spawn their own children (`spawn_agent` raises `RuntimeError`).

**Streaming responses (SSE)**: A `tokio::sync::broadcast` channel delivers
messages in real time. The SSE endpoint (`GET /messages/:chat_id/stream`)
pushes `message` and `status` events to connected clients. The chat UI uses
`EventSource` instead of polling.

## HTTP API

```
POST /message                    { chat_id?, user, content } → { status, chat_id }
GET  /status                     → { status, model }
GET  /messages/:chat_id          → { messages: [...] }
GET  /messages/:chat_id/stream   SSE stream (message + status events)
POST /shutdown                   → { status }
```

All endpoints have CORS enabled (permissive). The chat UI uses these directly.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `ANTHROPIC_API_KEY` | (required) | Anthropic API key |
| `CLAUDE_SERVER_MODEL` | `claude-opus-4-5-20251101` | Model to use |
| `CLAUDE_SERVER_LISTEN` | `127.0.0.1:3000` | API listen address |
| `CLAUDE_SERVER_DB` | `claude-server.db` | SQLite database path |
| `CLAUDE_SERVER_SYSTEM_PROMPT` | `system_prompt.txt` | System prompt file |
| `CLAUDE_SERVER_DEPLOYMENT_CONTEXT` | (none) | Deployment context file |
| `CLAUDE_SERVER_CONTEXT_WINDOW` | `200000` | Model context window size |
| `CLAUDE_SERVER_MODEL` | `claude-sonnet-4-5-20250929` | Model to use |
| `CLAUDE_SERVER_MAX_TOKENS` | `16384` | Max output tokens per turn |
| `CLAUDE_SERVER_PYTHON_TIMEOUT` | `5` | Python script execution timeout (seconds) |
| `CLAUDE_SERVER_MAX_CHILDREN` | `3` | Max concurrent sub-agent children |
