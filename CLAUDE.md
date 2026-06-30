# Claude Server — Project Guide

## Build & Run

```bash
make          # build and run the daemon (requires ANTHROPIC_API_KEY env var)
make chat     # build, open browser, and run the chat web UI
make run-dump # run with full context/response dumps each turn
make build    # build only
# CLI flags:
#   --daemon           run headless (no built-in stdin/stdout chat)
#   --dump-turns       print full context/response to stdout each turn
#   --dump-dir <path>  write turn dumps to files (parent-001-dump.txt, child-<id>-001-dump.txt)
#                      dumps include: CONTEXT, RESPONSE, THINKING, and EXECUTION OUTPUT/ERROR
```

By default the daemon launches with a built-in stdin/stdout chat (chat_id `"local"`)
so you can talk to the agent immediately. Pass `--daemon` to suppress it. The HTTP
API runs either way.

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
| `python.rs` | PyO3 executor: #[pyclass] wrappers owning `Mutex<T>` of cloned state components. Clone-and-mutate for transactional semantics; ExternalEffects for irreversible ops. Hook executor. |
| `renderer.rs` | Serialize HarnessState into XML-formatted context text for the API call |
| `api_client.rs` | Claude Messages API client (reqwest, retry logic, tool_use extraction) |
| `db.rs` | SQLite persistence (state as JSON blob, process output, outbound messages) |
| `process.rs` | Tokio-based process spawning, output capture, completion/failure/timeout events |
| `compaction.rs` | Compaction state machine (trigger detection, script accumulation, execution) |
| `agent_loop.rs` | Unified agent loop: single `AgentLoop` type parameterized by `AgentPermissions`. Used by both parent and child agents. Children get own ProcessSupervisor + event loop. |
| `http_server.rs` | Axum HTTP API: POST /message, POST /event, GET /status, GET /messages/:chat_id, GET /messages/:chat_id/stream (SSE), POST /shutdown |
| `chat.rs` | Chat UI subcommand: serves embedded HTML with API URL injection |
| `chat.html` | Single-file HTML/CSS/JS chat interface (embedded via include_str!) |
| `tls.rs` | HTTPS for the chat UI: static PEM files, or ACME (Let's Encrypt) with HTTP-01 / DNS-01 verification. Deadline-driven renewal, hot cert reload. |
| `source_dump.rs` | `source` subcommand: dumps/extracts the embedded source tarball |
| `bridges/` | `bridge` subcommand: messaging relay daemons (stdio, signal, telegram, slack, discord, email, agentchat). Shared `relay_loop` in mod.rs with bidirectional attachment support. |
| `feedback.rs` | `feedback`/`feedback-server` subcommands. Agents POST bug reports to feedback.yager.io. Also hosts `/chat/ws` — cross-deployment agent chat (salted-SHA256 auth, bounded queues, kick-on-reauth, 30s server ping). |
| `watchers/` | `watch` subcommand: one-directional event sources (fs, mqtt, imap). Shared `post_event` helper in mod.rs. |
| `webhook_proxy.rs` | `webhook-proxy` subcommand: HMAC-validated public ingress (GitHub, Slack, generic bearer) that forwards to `/event`. |
| `system_prompt.txt` | System prompt sent to Claude on every API call |
| `build.rs` | Discovers Python LIBDIR at build time, bakes rpath into binary |
| `INTERPRETER.md` | How the Python interpreter integration works (PyO3, side effects, etc.) |

## Key Design Patterns

**Non-blocking execution**: The core agent loop must never block on external work.
All long operations (HTTP requests, file processing, builds) go through `shell_exec()`
and return results via work queue items. Built-in Python tools must execute in
microseconds. This is why there's no `http_get()` — use `shell_exec("curl", ...)`
with `block_for` instead.

**Clone-and-mutate**: Per-turn, clone `HarnessState`, move components into
pyclass `Mutex<T>` fields (PyMemory owns the HashMap, PyWorkQueue owns the
WorkQueue, etc.). Mutations happen directly on the clone — `memory[k]=v` locks
and inserts, `list_hooks()` reads the same Mutex `register_hook()` wrote to.
Read-after-write works without any snapshot/collector divergence. On commit,
`Py::borrow()` + `mem::take()` extract the mutated components back into state.
On error, clone drops — original untouched.

**External effects deferred**: Operations that can't be un-done by dropping a
clone (OS process spawn, message broadcast, child fork, SQLite pin) go into
`ExternalEffects`, applied after commit. `shell_exec` adds the `ManagedProcess`
bookkeeping entry to the txn's process_manager same-turn (so `processes_list()`
shows it) but defers the actual `tokio::process::Command::spawn()`.

**Synchronous ID assignment**: `IdGenerator` wrapped in `Arc<Mutex<>>`, shared
between PyTimers and PyHarness. `.next()` called synchronously during script
execution. On commit the advanced generator swaps in; on error it rolls back
with the clone.

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
`max_turns` (default 20), `can_compact` (default True), `attach` (list of
file paths → View item on child's queue), `prefix_context` + `prefix_attach`
(stable role definition — renders before event_history in the cached region;
use for repeated spawns of the same role so the prefix caches across forks).
Children can compact their own context. `message_agent(name, content, priority=6)`
enables inter-agent messaging (parent↔child, sibling↔sibling). If any message
targets a nonexistent agent, the entire turn's side effects roll back. Children
return explicit values via `done(**kwargs)` which arrive as `ChildAgentCompleted`
with `child_name`, `result` (the kwargs dict), `turns_used`, `success`, and `summary`.
Max 3 concurrent children.
`child_depth_remaining: u32` controls recursion depth.

**Attachments (vision + large-file injection)**: `view(*paths)` pushes a
`WorkItemType::View` item at priority 10. When a View item is at queue head,
`renderer::render_context` emits its paths as content blocks (images →
vision, else text; encoding via `api_client::resolve_attachment()`). Content
stays visible until the item is popped — no separate loop state, so the idle
check is simply `queue.is_empty()`. `WorkItem.attachments: Vec<String>` is
metadata-only (rendered as queue text); bridges populate it (e.g. Signal
images) and the agent promotes paths via `view()`. `ChildSettings.attach`
pushes a View item to the child's queue.

**Auto-injected process env**: Every process spawned via `shell_exec()` gets
`CLAUDE_SERVER_EVENT_URL` (the `/event` endpoint) and `CLAUDE_SERVER_AGENT_NAME`
(the spawning agent's name). POST `/event` with `"agent":"<name>"` in the body
routes to that agent via the registry; omit it to route to root. Watchers spawned
by a child should include `"agent":"$CLAUDE_SERVER_AGENT_NAME"` so events go
straight to the child — root never wakes.

**Message references + reactions**: `UserMessage` work items carry an optional
`message_ref` (bridge-native ID: Signal timestamp, Discord snowflake, Slack ts).
`send_message(chat_id, content, react_to=ref)` sends a reaction instead of a
message. Threaded through `Inbound`/`Outbound` structs in bridges, `BroadcastMsg`,
SSE. Signal bridge maps to `sendReaction` jsonRpc; other bridges can wire up
their native reaction APIs.

**Harness subcommands + `harness_bin`**: The binary bundles helper subcommands
(`source`, `bridge`) that the agent invokes via `shell_exec(cmd=harness_bin, ...)`.
`harness_bin` is a Python global set from `std::env::current_exe()` each turn.
`source` dumps an embedded tarball built by `build.rs` via `git archive HEAD`.
`bridge` runs a relay daemon that connects an external messaging service (Signal,
stdio, ...) to the existing `/message` + SSE endpoints — one `chat_id` per bridge.

**Pinned memory (self-improving system prompt)**: `memory.pin(key, content)` writes
to a shared SQLite tier (`pinned_memory` table) and injects into the system prompt
(cached via `cache_control: ephemeral`). Shared across all agents and sessions.
Renders in full as markdown — unlike local memory's ~120-char truncation in
`<agent_state>`. `memory.get(k)` checks local first, then pinned. Size shown in
context metadata for self-regulation.

**Sensitive memory redaction**: `memory.mark_sensitive(key)` adds the key to
`HarnessState.sensitive_keys`. At API-trace store time, the value is string-replaced
with `<SENSITIVE, REDACTED>` across request and response JSON (both raw and
JSON-escaped forms; skips values <8 chars). Agent's live context unchanged — only
the ring buffer, and thus `feedback --with-api-trace` uploads, are scrubbed.

**Agent-facing changelog**: `HarnessState.last_harness_version` compared against
`CARGO_PKG_VERSION` on resume. `AGENT_CHANGELOG: &[(&str, &str)]` in `main.rs`
keys entries by the version that introduced them; `changelog_since()` range-selects
so a 0.2→0.5 jump shows exactly the 0.3/0.4/0.5 entries. Result goes into
`AgentStartup { changelog: Some(...) }`. Lets deployed agents self-discover new
capabilities without operator intervention. Entries are action-oriented; bump the
Cargo version and add an entry when shipping agent-facing features.

**UTF-8-safe truncation**: `renderer::trunc(s, max_bytes)` snaps back to the
nearest `is_char_boundary` before slicing. Bare `&s[..n]` panics mid-codepoint —
found via a crash loop on a memory value with `→` at exactly the cut point.

**Streaming responses (SSE)**: A `tokio::sync::broadcast` channel delivers
messages in real time. The SSE endpoint (`GET /messages/:chat_id/stream`)
pushes `message` and `status` events to connected clients. A chat_id ending in
`*` matches by prefix (e.g. `agentchat:*` for bridge routing); the full
chat_id is included in the SSE data.

## HTTP API

```
POST /message                    { chat_id?, user, content, attachments?, message_ref? } → { status, chat_id }
POST /event                      { source, type, data, agent?, priority? } → { status }
GET  /status                     → { status, model }
GET  /messages/:chat_id          → { messages: [...] }
GET  /messages/:chat_id/stream   SSE stream (prefix match if chat_id ends in *)
GET  /api-trace                  → last N request/response pairs (sensitive values pre-scrubbed)
GET  /cost                       → { input_tokens, output_tokens, cache_read_tokens, cache_write_tokens, estimated_cost_usd }
GET  /metrics/turns?limit=N      → { entries: [{ts, agent, input/output/cache tokens, cost_usd}, ...], total_in_log, capacity }
GET  /metrics/rate               → { last_5m, last_1h, last_24h } — rolling sums; window_covered_secs shows buffer coverage
GET  /dashboard                  → embedded HTML UI (live view of all agents)
GET  /dashboard/state            → { agent_name: AgentSnapshot, ... } — queue, history tail, memory, timers, processes, hooks, usage
POST /shutdown                   → { status }
```

All endpoints have CORS enabled (permissive). The chat UI uses these directly.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `ANTHROPIC_API_KEY` | (one of these required) | Console API key — `x-api-key` auth, production path |
| `CLAUDE_SERVER_BEARER_TOKEN` | (one of these required) | OAuth bearer token — dev-only, `Authorization: Bearer` + `anthropic-beta: oauth-2025-04-20`. Mutually exclusive with `ANTHROPIC_API_KEY`. Requires TTY + typed "I AGREE" at startup unless `CLAUDE_SERVER_AUTH_ACK=1`. On 401 the daemon exits rather than retry (assumes token expired). |
| `CLAUDE_SERVER_AUTH_ACK` | (unset) | Set to `1` to bypass the Bearer-mode TTY + acknowledgment prompt (for scripted test harnesses only). |
| `CLAUDE_SERVER_USAGE_LOG_CAPACITY` | `1000` | Ring buffer size for per-turn usage entries exposed via `/metrics/turns` and `/metrics/rate`. |
| `CLAUDE_SERVER_MODEL` | `claude-opus-4-8` | Model to use |
| `CLAUDE_SERVER_EFFORT` | `high` | Reasoning effort (`low`\|`medium`\|`high`\|`xhigh`\|`max`); sent as `output_config.effort` when the model supports it |
| `CLAUDE_SERVER_LISTEN` | `127.0.0.1:3000` | API listen address |
| `CLAUDE_SERVER_DB` | `claude-server.db` | SQLite database path |
| `CLAUDE_SERVER_SYSTEM_PROMPT` | `system_prompt.txt` | System prompt file |
| `CLAUDE_SERVER_DEPLOYMENT_CONTEXT` | (none) | Deployment context file |
| `CLAUDE_SERVER_CONTEXT_WINDOW` | `1000000` | Model context window size |
| `CLAUDE_SERVER_MAX_TOKENS` | `16384` | Max output tokens per turn |
| `CLAUDE_SERVER_PYTHON_TIMEOUT` | `5` | Python script execution timeout (seconds) |
| `CLAUDE_SERVER_MAX_CHILDREN` | `3` | Max concurrent sub-agent children |
| `CLAUDE_SERVER_COST_INPUT` | `3.0` | Input token cost per million tokens (USD) |
| `CLAUDE_SERVER_COST_OUTPUT` | `15.0` | Output token cost per million tokens (USD) |
| `CLAUDE_SERVER_COST_CACHE_READ` | `0.30` | Cache read token cost per million tokens (USD) |
| `CLAUDE_SERVER_COST_CACHE_WRITE` | `3.75` | Cache write token cost per million tokens (USD) |
| `CLAUDE_SERVER_CACHE_STRIDE` | `5` | Base stride for geometric cache-tier alignment |
| `CLAUDE_SERVER_CACHE_TIERS` | `2` | Number of geometric cache tiers (capped by 4-breakpoint limit) |
| `CLAUDE_SERVER_API_TRACE_SIZE` | `10` | Ring buffer size for /api-trace (0 disables) |
| `CLAUDE_SERVER_FEEDBACK_URL` | `https://feedback.yager.io:3001/feedback` | Where `feedback` subcommand POSTs |
| `CLAUDE_SERVER_FEEDBACK_ADMIN_TOKEN` | (none) | Bearer token for feedback-server GET/DELETE |

Auto-injected into spawned process env (not daemon config):
- `CLAUDE_SERVER_EVENT_URL` — the `/event` endpoint
- `CLAUDE_SERVER_AGENT_NAME` — name of the agent that spawned the process
