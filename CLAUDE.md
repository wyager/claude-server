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
| `core_loop.rs` | Thin wrapper: creates an `AgentLoop` with parent permissions and runs it |
| `python.rs` | PyO3 executor: #[pyclass] wrappers for work_queue, memory, timers, history, harness functions. SideEffectCollector pattern. |
| `renderer.rs` | Serialize HarnessState into XML-formatted context text for the API call |
| `api_client.rs` | Claude Messages API client (reqwest, retry logic, tool_use extraction) |
| `db.rs` | SQLite persistence (state as JSON blob, process output, outbound messages) |
| `process.rs` | Tokio-based process spawning, output capture, completion/failure/timeout events |
| `compaction.rs` | Compaction state machine (trigger detection, script accumulation, execution) |
| `agent_loop.rs` | Unified agent loop: single `AgentLoop` type parameterized by `AgentPermissions`. Used by both parent and child agents. Children get own ProcessSupervisor + event loop. |
| `http_server.rs` | Axum HTTP API: POST /message, POST /event, GET /status, GET /messages/:chat_id, GET /messages/:chat_id/stream (SSE), POST /shutdown |
| `chat.rs` | Chat UI subcommand: serves embedded HTML with API URL injection |
| `chat.html` | Single-file HTML/CSS/JS chat interface (embedded via include_str!) |
| `system_prompt.txt` | System prompt sent to Claude on every API call |
| `build.rs` | Discovers Python LIBDIR at build time, bakes rpath into binary |
| `INTERPRETER.md` | How the Python interpreter integration works (PyO3, side effects, etc.) |

## Key Design Patterns

**Non-blocking execution**: The core agent loop must never block on external work.
All long operations (HTTP requests, file processing, builds) go through `shell_exec()`
and return results via work queue items. Built-in Python tools must execute in
microseconds. This is why there's no `http_get()` — use `shell_exec("curl", ...)`
with `block_for` instead.

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

**Sub-agents (fork model)**: `fork([ChildSettings(...), ...])` spawns child agents
that inherit the parent's full context (event history + memory) for KV cache reuse.
Children start with a clean work queue, timers, and processes. Each agent has a
globally unique name and lineage (e.g., `"api-checker, child of plan-builder,
child of root"`). The root agent is always named `"root"`. Names are registered
atomically via `AgentRegistry` — if any name collides, the entire fork fails.
`ChildSettings` fields: `name`, `task`, `model` (Optional, inherits parent),
`max_turns` (default 20), `can_compact` (default True), `attach` (list
of file paths to attach on the child's first turn — see Attachments below).
Children can compact their own context. `message_agent(name, content, priority=6)`
enables inter-agent messaging (parent↔child, sibling↔sibling). If any message
targets a nonexistent agent, the entire turn's side effects roll back. Children
return explicit values via `done(**kwargs)` which arrive as `ChildAgentCompleted`
with `child_name`, `result` (the kwargs dict), `turns_used`, `success`, and `summary`.
Max 3 concurrent children.
`child_depth_remaining: u32` controls recursion depth.

**Attachments (vision + large-file injection)**: `attach(path)` queues a
file to appear as a content block on the agent's *next* turn. Images (`.jpg`,
`.jpeg`, `.png`, `.gif`, `.webp`) become vision blocks the model can see; any
other file becomes a text block. Attachments are ephemeral: visible exactly once,
not in `HarnessState`, not persisted, not in history. Storage lives in
`AgentLoop.pending_attachments` which is `std::mem::take`'d into each turn's
render. File paths (not bytes) are stored in `SideEffectCollector` — encoding
is deferred to `api_client::resolve_attachment()`. `ChildSettings.attach`
seeds a child's first-turn attachments via the same mechanism.

**Auto-injected process env**: Every process spawned via `shell_exec()` gets
`CLAUDE_SERVER_EVENT_URL` in its environment (computed from `Config.listen_addr`).
Watcher scripts can `curl -X POST "$CLAUDE_SERVER_EVENT_URL" ...` to send events
back to the agent without hardcoding the listen address.

**Pinned memory (self-improving system prompt)**: `memory.pin(key, content)` writes
to a shared SQLite tier (`agent_notes` table) and injects into the system prompt
(cached via `cache_control: ephemeral`). Shared across all agents and sessions.
`memory.get(k)` checks local first, then pinned. `memory.unpin(k)`, `memory.list_pinned()`.
Pinned entries are strings (render as markdown in the system prompt). Pinned size is
shown in context metadata for self-regulation.

**Streaming responses (SSE)**: A `tokio::sync::broadcast` channel delivers
messages in real time. The SSE endpoint (`GET /messages/:chat_id/stream`)
pushes `message` and `status` events to connected clients. The chat UI uses
`EventSource` instead of polling.

## HTTP API

```
POST /message                    { chat_id?, user, content } → { status, chat_id }
POST /event                      { source, type, data, priority? } → { status }
GET  /status                     → { status, model }
GET  /messages/:chat_id          → { messages: [...] }
GET  /messages/:chat_id/stream   SSE stream (message + status events)
GET  /cost                       → { input_tokens, output_tokens, cache_read_tokens, cache_write_tokens, estimated_cost_usd }
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
| `CLAUDE_SERVER_COST_INPUT` | `3.0` | Input token cost per million tokens (USD) |
| `CLAUDE_SERVER_COST_OUTPUT` | `15.0` | Output token cost per million tokens (USD) |
| `CLAUDE_SERVER_COST_CACHE_READ` | `0.30` | Cache read token cost per million tokens (USD) |
| `CLAUDE_SERVER_COST_CACHE_WRITE` | `3.75` | Cache write token cost per million tokens (USD) |
