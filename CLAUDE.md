# Claude Server — Project Guide

## Build & Run

```bash
make          # build and run the daemon (requires ANTHROPIC_API_KEY env var)
make chat     # build, open browser, and run the chat web UI
make run-dump # run with full context/response dumps each turn
make build    # build only
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
| `http_server.rs` | Axum HTTP API: POST /message, GET /status, GET /messages/:chat_id, POST /shutdown |
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

## HTTP API

```
POST /message          { chat_id?, user, content } → { status, chat_id }
GET  /status           → { status, model }
GET  /messages/:chat_id → { messages: [...] }
POST /shutdown         → { status }
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
| `CLAUDE_SERVER_MAX_TOKENS` | `16384` | Max output tokens per turn |
