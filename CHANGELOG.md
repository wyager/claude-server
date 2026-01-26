# Changelog

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
