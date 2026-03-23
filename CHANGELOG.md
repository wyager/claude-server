# Changelog

## 2026-03-23

### Watchers (`watch fs|mqtt|imap`)
- New `src/watchers/` subcommand family. Long-lived daemons that POST batched
  `ExternalEvent` items to `/event`.
- **Shared debounce loop** (`watchers/mod.rs::debounce_loop`): events are
  collected with a reset-on-event debounce (default 3s) and a force-flush cap
  (default 10s) so a steady stream can't stall indefinitely. Each batch is one
  work item with `data = {count, events: [...]}`. Both timers configurable
  per-watcher via `--debounce-ms`/`--force-ms`.
- `watch fs` — filesystem events via the `notify` crate. Native backend by
  default; `--poll-interval-ms N` switches to `PollWatcher` for NFS/SMB/sshfs
  where inotify/FSEvents miss remote writes.
- `watch mqtt` — MQTT subscriber via `rumqttc`. Topic wildcards, auth, retain.
- `watch imap` — IMAP IDLE via `async-imap`. Push-based, reconnects, fetches
  `{from, subject, uid}` for new messages.

### Webhook proxy
- `claude-server webhook-proxy` — authenticated public ingress. Routes:
  `/github` (X-Hub-Signature-256 HMAC), `/slack` (X-Slack-Signature with 5-min
  replay protection, handles URL verification challenge), `/generic` (Bearer
  token passthrough). Optional TLS via the same `TlsListener` as
  feedback-server (now `pub`).
- New deps: `hmac`, `sha2`, `hex`, `notify`, `rumqttc`, `async-imap`,
  `async-native-tls`.

### Feedback fixes
- **#14b**: Agent no longer idles with pending attachments — does one more turn
  to see them. One-line change to the idle condition in `agent_loop.rs`.
- **#14a**: Signal bridge now includes attachment paths in forwarded messages
  as `[attachments: /path, ...]`. Parsed from signal-cli's jsonRpc
  `dataMessage.attachments[].id`.
- **#13**: `ChildSettings.inherit_history` (default `True`). When `False`,
  child starts with fresh history containing only a fork SystemAlert. Memory,
  `task`, and `attach` still flow. Avoids cross-model re-ingest cost.

### Prompt caching fix (two-breakpoint stride scheme)
- Previously only the system prompt was cached; the rendered context (event_history
  etc.) paid full input price every turn. First attempt put a breakpoint at the
  immutable-history boundary, but field telemetry showed it never hit — the boundary
  moves every turn so the breakpoint content is never byte-identical to the prior
  cache entry.
- Fixed: `RenderedContext.cached_segments` holds two stride-aligned segments
  (default `cache_stride=10` entries). Both segments keep byte-identical content
  for `stride` turns → guaranteed hits. On stride advance, segment 1 moves to
  segment 2's old position (still hits its cache entry), segment 2 moves forward
  (cache-write on just one stride's worth). `EventHistory::cache_splits()` computes
  the boundaries.
- Per-turn cost + cache hit % logged: `$0.0421/turn, 92% cache hit`. Watch this —
  if hit % stays near the system-prompt-only baseline (~15-20%), something regressed.

### AgentStartup work item (from field feedback #9)
- On daemon restart with resumed state, inject a priority-9 `AgentStartup` work
  item so the agent gets a turn to reconnect dead bridges/processes it tracked
  in memory. Not injected on fresh state.

### Signal bridge rewrite (from field feedback #5-6)
- Switched from `receive` + spawn-per-`send` (broken by signal-cli's file lock)
  to single `jsonRpc` daemon over stdin/stdout. One process, no lock contention.

### Feedback server fixes
- TlsListener now spawns handshakes into background tasks with 10s timeout —
  slow clients can't block the accept loop. Added `ClientAddr` newtype to
  satisfy axum's `Connected` trait for both TCP and TLS paths.
- Embedded cert updated to match the live server (was stale, causing
  `BadSignature`). Error chain now printed so future TLS failures are diagnosable.

## 2026-03-22

### CLI chat over HTTP + `request_compaction()`
- `bridge stdio` is now a proper CLI chat client: same cyan-box rendering and
  idle-status prompt timing as the built-in chat, but connects over HTTP/SSE.
  Use it to attach to a headless daemon (e.g. systemd service). Uses
  `chat_id="local"` so it's the same conversation as the built-in chat.
- Headless daemon startup banner now points at `claude-server bridge stdio`
  and `claude-server chat` instead of the raw curl example.
- `request_compaction()` Python function lets the agent trigger compaction on
  demand (e.g. from a scheduled timer). Previously compaction was threshold-only.

## 2026-03-21 (later)

### New Bridges: Telegram, Slack, Discord
- `bridge telegram --token T --peer ID` — Bot API long-polling, pure HTTP
- `bridge slack --app-token T --bot-token T --channel ID` — Socket Mode websocket
- `bridge discord --token T --channel ID` — Gateway websocket with heartbeat
- New dep: `tokio-tungstenite` for the two websocket bridges

### Harness Feedback
- `claude-server feedback --summary "..." [--details ...] [--repro ...]` — agents
  self-report harness bugs. POSTs to `CLAUDE_SERVER_FEEDBACK_URL` (default
  `https://feedback.yager.io/feedback`). Auto-fills `harness_version` and
  `agent_name` (from `CLAUDE_SERVER_AGENT_NAME` env, now injected by
  ProcessSupervisor alongside `CLAUDE_SERVER_EVENT_URL`).
- `claude-server feedback-server [--listen] [--db] [--admin-token]` — collection
  server. `POST /feedback` is public + rate-limited (10/min/IP). `GET /feedback`
  requires `Authorization: Bearer <admin-token>` — write-only from field agents'
  perspective, a dev Claude with the token triages.

## 2026-03-21

### Built-in Local Chat
- Default launch now includes a stdin/stdout chat interface (chat_id `"local"`).
  Wired directly to the in-process `event_tx`/`broadcast_tx` channels — no HTTP hop.
  Agent replies rendered in a cyan-bordered box; prompt is green `> `. Pass `--daemon`
  to suppress. Stdin EOF triggers graceful shutdown.
- All agent-loop log lines now render in dim gray (`dimlog!` macro, agent_loop.rs)
  so they're visually distinct from chat output. `[message] -> chat:...` log now
  truncates content to first line / 60 chars instead of duplicating full reply.
- Default model bumped to `claude-opus-4-6`.
- Default `CLAUDE_SERVER_CONTEXT_WINDOW` bumped 200k → 1M. Compaction thresholds
  (`compact_at` = 80%, `compact_target` = 50%) derive proportionally so they scale
  automatically.

### Bundled Subcommands + Source Self-Dump
- `claude-server source [--extract DIR]` dumps an embedded tarball of the harness
  source (built at compile time via `git archive HEAD`). Lets the agent inspect
  or modify its own harness.
- `claude-server bridge <type>` runs a long-lived relay daemon between an external
  messaging service and the existing `/message` + SSE endpoints. Bridges own one
  conversation each (`chat_id = "<type>:<peer>"`). Shared `relay_loop` helper in
  `src/bridges/mod.rs` handles the inbound POST + outbound SSE subscription.
  - `bridge stdio` — trivial scaffold example (stdin → agent, agent → stdout)
  - `bridge signal --account N --peer N` — wraps `signal-cli` (external dep)
- `harness_bin` Python global exposes the running binary's path so the agent can
  `shell_exec(cmd=harness_bin, args=["source", ...])` without guessing.
- Core harness (types, core_loop, agent_loop, renderer, db, http_server, process,
  api_client, compaction, config) untouched.

## 2026-02-27

### Attachments (Vision + Large-File Injection)
- `attach(path)` queues a file to appear as a content block on the agent's next turn
- Media-type sniffed by extension: images → vision blocks, anything else → text block
- Ephemeral: visible exactly once, not in `HarnessState`, not persisted, not in history
- Stored as file paths (not bytes) in `SideEffectCollector`; encoding deferred to API call time
- `ChildSettings.attach=[paths]` seeds a child's first-turn attachments (no wasted roundtrip)
- `CLAUDE_SERVER_EVENT_URL` auto-injected into every spawned process's env
- `kill_on_drop(true)` on spawned processes so watchers clean up on daemon shutdown

### API Unification (net -123 lines)
- `done(**kwargs)` takes explicit return values. Parent receives only what child passed,
  not the entire inherited memory. `ChildAgentCompleted.result_memory` → `ChildAgentCompleted.result`
- `PyWorkItem` collapsed from 20-field sparse struct to `{id, priority, time, type, fields: Map}`.
  Field names match Rust `WorkItemType` variant field names exactly. Single `__getattr__`
  with helpful errors listing available fields. No more `child_id` vs `child_name` aliases.
- `notes.*` folded into `memory.pin(k, v)` / `memory.unpin(k)` / `memory.list_pinned()`.
  One namespace, two storage tiers: local per-agent (any JSON) + pinned shared (strings,
  system-prompt cached). `memory.get()` reads through both. `PyNotes` class deleted.
  SQLite table renamed `agent_notes` → `pinned_memory`, column `section` → `key`
  (no production DBs existed at time of rename). DB methods renamed:
  `load_notes`/`save_note`/`delete_note` → `load_pinned`/`save_pin`/`delete_pin`.
- `show_in_context` → `attach` rename; `ChildSettings.show_in_context` → `ChildSettings.attach`
- System prompt `<agent_notes>` block → `<pinned_memory>`
- Work-queue docs in system prompt collapsed from ~60 lines per-type listings to 12-line table
- 3 new tests: `test_done_with_result`, `test_done_no_args`, `test_work_item_field_access`

## 2026-01-27 (cont.)

### Unified Agent Loop
- `agent_loop.rs` replaces `child_agent.rs` — single `AgentLoop` type parameterized by `AgentPermissions`
- `core_loop.rs` is now a thin wrapper that creates an `AgentLoop` with parent permissions
- Children now have full `shell_exec` support (own ProcessSupervisor + event loop)
- `child_depth_remaining: u32` replaces the old boolean — configurable recursion depth
- `AgentPermissions` struct in `types.rs` controls: `can_compact`, `max_turns`, `child_depth_remaining`

### Cost Tracking
- `TokenAccumulator` tracks input/output/cache tokens per session
- `GET /cost` endpoint returns token counts + estimated USD cost
- Chat UI header shows `$X.XX | N turns`
- Pricing configurable via env vars: `CLAUDE_SERVER_COST_INPUT` (default $3/M), `CLAUDE_SERVER_COST_OUTPUT` ($15/M), `CLAUDE_SERVER_COST_CACHE_READ` ($0.30/M), `CLAUDE_SERVER_COST_CACHE_WRITE` ($3.75/M)

### Non-blocking Design Principle
- Added to CLAUDE.md key design patterns and IDEAS.md header
- HTTP request tool marked as REJECTED in IDEAS.md (violates non-blocking principle)

### Child Agent Deployment Preamble Fix
- Updated child agent deployment preamble to accurately tell children they can `send_message()`
- Previously the preamble told children they were fully sandboxed (no messaging), causing
  children to not use `send_message()` even though it was available
- System prompt sub-agents section also updated to document the capability

### ID Collision Fix
- History entry IDs now generated AFTER `apply_side_effects()`, preventing duplicate IDs
  between history entries and agent-created objects (timers, processes, children)

### ChildAgentCompleted Result Preview
- Rendered work queue item for `ChildAgentCompleted` now shows `result_memory` keys
  with truncated values (max 5 keys, 80 chars each)
- Parent agent can see what the child produced without needing to print it
- Success summary simplified to "Completed successfully" (preview provides the detail)
- Error summaries truncate cleanly with "..."

### Child Agent Capabilities Expanded
- Children can now send messages via `send_message()` (saved to DB)
- `spawn_agent()` now raises `RuntimeError("Sub-agents cannot spawn their own children")`
  instead of silently doing nothing
- Process spawning (`shell_exec`) not yet supported (future work)

### Execution Output in Dump Files
- Turn dump files now include a fourth section showing EXECUTION OUTPUT (or EXECUTION ERROR)
  with the Python script's stdout/stderr

### Streaming Responses (SSE)
- Added `tokio::sync::broadcast` channel for real-time message delivery
- `GET /messages/:chat_id/stream` SSE endpoint with `message` and `status` events
- Chat UI rewritten to use EventSource (no more polling)
- Typing indicator (thinking.../executing...) in chat UI
- Added `tokio-stream` (sync feature) and `futures` deps

### Agent State in Context
- `<agent_state>` block rendered between work_queue and context showing memory (sorted by priority), active timers, running processes
- `memory.set(key, value, priority=N)`, `memory.set_priority(key, N)`, `memory.get_priority(key)` Python API
- `memory_priorities` added to HarnessState (backwards compatible with existing DBs)
- Bounded by RenderConfig limits (20 memory keys, 20 timers, 10 processes)

### Sub-agents
- `spawn_agent(task, model, memory={}, max_turns=20, priority=6)` Python API
- `child_agent.rs` with simplified core loop (no message sending, no process spawning)
- `ChildAgentCompleted` work item type with `result_memory`, `turns_used`, `success`, `summary`
- Max 3 concurrent children (`CLAUDE_SERVER_MAX_CHILDREN`), max 50 turns, no recursion

### Python Execution Timeout
- `CLAUDE_SERVER_PYTHON_TIMEOUT` env var (default 5s)
- Scripts that block too long are interrupted via `PyErr_SetInterrupt`

### Context Dump Improvements
- `--dump-dir <path>` CLI flag writes turn dumps to files instead of stdout
- Parent dumps: `parent-001-dump.txt`, children: `child-<id>-001-dump.txt`
- `--dump-turns` still works for stdout (parent only)

## 2026-01-27

### Priority Defaults Revised
- `success_prio` raised from 5→7, `fail_prio` raised from 7→8
- ProcessCompleted now appears above TimerFired (default 5) in the work queue
- Prevents agents from blindly accessing `work_queue[0]` and hitting the wrong item type

### Timer Acknowledgment for Recurring Timers
- Recurring timers no longer re-arm automatically after firing
- Agent must call `acknowledge_timer(timer_id)` to re-arm from current time
- Prevents timer events from piling up if processing takes longer than the interval
- One-shot timers unaffected (removed after firing)

### Default Model Changed to Sonnet 4.5
- Default model changed from `claude-opus-4-5-20251101` to `claude-sonnet-4-5-20250929`
- Configurable via `CLAUDE_SERVER_MODEL` env var

### Process Output Race Condition Fix
- The completion monitor now awaits the output reader task's JoinHandle before
  sending the completion event, guaranteeing all output is flushed to the DB
- Previously, output could be incomplete when the agent read it

### Output Preview on ProcessCompleted/ProcessFailed
- Work items now include `output_preview` with the last ~500 chars of stdout/stderr
- Agent can read `item.output_preview` directly instead of calling `shell_output()`
- Rendered inline in the work queue display

### `block_for` Parameter on shell_exec
- `shell_exec(..., block_for=timedelta(milliseconds=500))` waits for fast commands
- Returns as soon as the process finishes (not the full timeout duration)
- ProcessCompleted appears in the queue on the next turn — no extra round-trip
- Uses a oneshot channel for proper synchronization (no sleep)

### Direct File I/O Prompt Hint
- System prompt now tells the agent to use Python `open()` for file operations
- Eliminates expensive shell_exec round-trips for file writes
- Combined with block_for and output_preview, reduced a coding task from 14 turns to 4

## 2026-01-26

### Extended Thinking
- Added structured thinking support (`thinking: { type: "enabled", budget_tokens: 10000 }`)
- Claude now reasons in a scratchpad before writing code each turn
- Thinking is displayed in `--dump-turns` mode under an "AGENT THINKING" banner

### Structured Memory Values
- Changed memory from `HashMap<String, String>` to `HashMap<String, serde_json::Value>`
- Agent can now store dicts, lists, numbers, booleans — not just strings
- Values round-trip through JSON: a dict comes back as a dict, a string as a string
- Added `memory.get(key, default=None)` method
- Updated system prompt to document supported types and `.get()`

### Configurable Compaction Thresholds
- Added `CLAUDE_SERVER_COMPACT_AT` and `CLAUDE_SERVER_COMPACT_TARGET` env vars
- Direct token counts instead of ratios (defaults: 80% / 50% of context window)
- Thresholds shown in startup log

### Compaction Dry-Run Estimation
- `estimate_post_compaction()` now actually runs the compaction script against a cloned state
- Re-renders the result to estimate post-compaction token count (chars/4)
- Agent sees accurate "Estimated usage after compaction_script" instead of a stub value
- Added `Clone` derive to `HarnessState` and all inner types

### Spawn Failure Notification
- When `shell_exec()` targets a nonexistent command, the agent now immediately receives
  a `ProcessFailed` work item instead of waiting forever for a completion event
- Previously, spawn failures were silently logged to stderr with no agent notification

### Process Description + Listing
- Added `description=""` parameter to `shell_exec` for human-readable process labels
- Added `processes_list()` function returning `[(pid, cmd, description, status), ...]`
- Description stored on `ManagedProcess` and persisted in state

### System Prompt: Staying Oriented
- Added "Staying Oriented" section with guidance on storing breadcrumbs in memory
- Tells agent to save timer IDs, process PIDs, and chat_ids in memory for post-compaction recovery
- Added "Before Sending Messages to Users" and "Think Before Acting" sections

## 2026-01-25

### Initial Build (Phases 1-6)
- Full MVP of Claude Server: Rust daemon driving Claude through a work-queue loop
- Core types: WorkQueue, EventHistory, TimerManager, ProcessManager, Memory, HarnessState
- PyO3 Python executor with #[pyclass] wrappers and atomic SideEffectCollector pattern
- Context renderer producing XML-formatted text for single-message API calls
- Claude Messages API client with retry logic and tool_use extraction
- SQLite persistence (state as JSON blob, process output, outbound messages)
- Tokio-based process spawning with output capture and completion/failure/timeout events
- Compaction state machine with script accumulation and execution
- Axum HTTP API: POST /message, GET /status, GET /messages/:chat_id, POST /shutdown
- System prompt defining the Python API surface and agent guidelines

### Chat UI
- Added `claude-server chat` subcommand serving an embedded HTML chat interface
- Single-file HTML/CSS/JS with polling, auto-scroll, UUID chat_id per session
- Supports `--port` and `--api-url` flags
- Added CORS (permissive) to the API server
- Makefile: `make chat` builds, opens browser, and runs the chat server

### Blocking Timer Architecture
- Replaced 1-second timer polling loop with `tokio::select!` blocking on event channel
  vs sleep-until-next-timer-deadline
- Added `TimerManager::next_deadline()` to compute earliest fire time
- Removed `HarnessEvent::TimerTick`, `timer_tick_loop`, and `timer_poll_interval` config
- Idle message prints once on transition, not on every timer tick wake-up

### Debug / Observability
- Added `--dump-turns` CLI flag printing full context and agent response each turn
- Added `make run-dump` Makefile target

### Python Type Coercion
- `timers.add(every=...)` and `shell_exec(alert_timer=...)` accept both numbers and `timedelta`
- `timers.add(at=...)` accepts `datetime` objects (extracts epoch via `.timestamp()`)
- `extract_seconds()` helper tries `f64` first, falls back to `.total_seconds()`

### Graceful Shutdown
- Ctrl+C sends `HarnessEvent::Shutdown` via `tokio::signal::ctrl_c()` handler
- Core loop breaks cleanly, state is saved before exit

### One-Shot Timer Fix
- `timers.add(at=datetime(...))` now correctly uses the provided timestamp
- Previously ignored the `at` parameter and defaulted to 1 minute from now

### Build System
- `build.rs` auto-discovers Python LIBDIR and bakes rpath into binary
- No `DYLD_LIBRARY_PATH` needed at runtime

### Documentation
- README.md: Architecture spec, worked examples, quick start, implementation details
- CLAUDE.md: Project guide for development (build, source layout, design patterns)
- CLAUDE.local.md: User preference (avoid polling architectures)
- INTERPRETER.md: Python integration details (PyO3, side effects, stdout capture, etc.)
