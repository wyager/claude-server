# Ideas for Future Work

**Design principle: Non-blocking execution.** The core agent loop must never block on
external work (network requests, file I/O, process execution). All long operations go
through `shell_exec` and return results via work queue items. Built-in tools must execute
in microseconds. This is why there's no `http_get()` or `sleep()` — use `shell_exec("curl", ...)`
with `block_for` instead.

## Context Window Improvements

- ~~**Show agent state in context**~~: DONE — `<agent_state>` block rendered between work_queue and context with memory (sorted by priority), active timers, running processes. Bounded by RenderConfig limits.

- **Smarter token estimation**: Replace `chars / 4` approximation with actual tokenizer (tiktoken or the Anthropic counting endpoint). The compaction dry-run estimate would be more accurate, and we could avoid the "compaction loop" issue where thresholds are set too close to base context overhead.

- **Dynamic modification window**: Instead of fixed last-5-entries, compute based on token budget. When context is tight, shrink the window; when it's spacious, expand it.

- ~~**Incremental context diffing**~~: REJECTED — API trace #23 proved the cache requires 100% byte-identical blocks with no partial/diff update surface. You always send the full context; it either hits (exact match → cache_read at 10%) or misses (full re-ingest). Geometric tiers (stride=5/tiers=2) achieve the same goal differently: keep breakpoints byte-stable across many turns so bulk content stays in cache_read; the "diff" is implicit in the bounded uncached tail.

## Agent Capabilities

- ~~**Self-improvement**~~: DONE (partial) — `memory.pin(key, content)` writes persistent string entries to SQLite and injects them into the system prompt (cached). Agent accumulates environment facts, error recovery playbooks, user preferences, workflow recipes across sessions. `memory.get()` reads through both local and pinned tiers. Custom Python functions not implemented (kept runtime stable).

- ~~**Sub-agents**~~: DONE — `fork([ChildSettings(...)])` spawns children that inherit parent context. Named agent registry with lineage tracking. Inter-agent messaging via `message_agent(name, content)`. `done(**result)` exits and passes the kwargs dict to the parent. `ChildAgentCompleted` work item with `child_name`, `result`, clear finish reason.

- ~~**Child process support**~~: DONE — Children now have full `shell_exec` via the unified `AgentLoop` in `agent_loop.rs`, with their own ProcessSupervisor + event loop. `child_depth_remaining: u32` controls recursion depth.

- **Structured tool outputs**: Instead of just stdout strings, let Python scripts return structured data (JSON) that gets rendered more usefully in history and work items.

## New Built-in Tools

- ~~**HTTP request tool**~~: REJECTED — violates the non-blocking principle. The core loop must never block on external work. Use `shell_exec("curl", ...)` with `block_for` instead. Once children have process support, they can do this too.

- **File watcher**: `watch_directory(path, callback_description)` that uses OS-level file notifications (inotify/FSEvents) instead of polling via timers. More efficient and more responsive.

- **Cron-style scheduling**: `schedule(cron_expr, description)` for complex recurring patterns beyond simple intervals.

- **Deployment plugins**: Implement the `DeploymentPlugin` trait for real use cases — home automation (HomeAssistant API), DevOps (kubectl, terraform), monitoring (Prometheus queries), etc.

## Security & Sandboxing

- ~~**Python execution timeout**~~: DONE — `CLAUDE_SERVER_PYTHON_TIMEOUT` env var (default 5s). Uses `PyErr_SetInterrupt` to interrupt blocked scripts.

- **Import restrictions**: Optionally block dangerous imports (`subprocess`, `os.system`, `socket`) to prevent the agent from bypassing harness controls. Could use a custom import hook or a restricted `__builtins__`.

- **Filesystem sandboxing**: Restrict file I/O to specific directories. The agent currently has full filesystem access.

- **Resource limits**: Memory limits on the Python interpreter, process spawn rate limiting, outbound message rate limiting.

## Operations & Observability

- ~~**Web dashboard**~~: DONE — `GET /dashboard` serves a single-file HTML UI; `GET /dashboard/state` returns JSON snapshots of all agents (root + children). Each `AgentLoop` pushes a full snapshot to `AgentRegistry` at the start of each turn ("thinking" — shows what the model sees) and on idle (post-execution — shows results). Memory values included (truncated, sensitive keys redacted), collapsed by default in the UI. Per-agent cards show status, queue, history tail, memory, timers, processes, hooks + stats, last-turn usage/cost. 2s poll; open/closed section state preserved across re-renders.

- **Structured logging**: Replace `println!` statements with structured JSON logs (using `tracing` crate). Enable log levels, filtering, and integration with monitoring tools.

- **Metrics**: Track turns per task, error rate, compaction frequency, cache hit rate, token usage over time. Expose via Prometheus endpoint.

- **Session replay**: Record the full sequence of rendered contexts and agent responses, then replay them for debugging or analysis without making API calls.

- ~~**Cost tracking**~~: DONE — `TokenAccumulator` tracks input/output/cache tokens per session. `GET /cost` endpoint returns token counts + estimated USD cost. Chat UI header shows `$X.XX | N turns`. Pricing configurable via env vars.

## Chat UI

- **Markdown rendering**: The chat UI currently shows raw text. Render markdown (code blocks, headers, lists, bold/italic) for much better readability.

- **Multiple conversations**: Currently one chat per browser tab. Add a sidebar with conversation list, ability to switch between chats.

- **Agent state panel**: Show the agent's memory, timers, and processes in a collapsible sidebar panel in the chat UI.

- **File upload**: Let users drag-and-drop files into the chat, which get saved to a scratch directory and the path sent in the message.

## Architecture

- ~~**Streaming responses**~~: DONE — SSE via `GET /messages/:chat_id/stream` with `message` and `status` events. Chat UI uses EventSource with typing indicators. Uses `tokio::sync::broadcast`.

- **Multi-turn conversation mode**: Optional mode that uses actual multi-turn API messages instead of single-message rebuild. Better quality for interactive conversations, at the cost of losing the stable-prefix caching advantage.

- **Persistent process output ring buffer**: Currently process output grows unbounded in SQLite. Add a configurable max size per process and trim old output.

- **Graceful compaction under pressure**: If the agent's context exceeds the window entirely (e.g., a single huge process output), auto-truncate rather than crashing. Currently untested edge case.

## Testing

- **Integration test suite**: Automated tests that start the daemon, send messages, verify responses, test timers, test compaction, test error recovery. Currently all testing is manual.

- **Turn efficiency benchmarks**: Measure turns-per-task for standard scenarios (file creation, process management, Q&A). Track regressions as the system evolves.

- **Model comparison harness**: Run the same task against different models (Sonnet vs Opus vs Haiku) and compare turn count, error rate, and output quality.

- **Chaos testing**: Randomly kill processes, corrupt files, send malformed messages, fill the work queue with spam — verify the agent recovers gracefully.
