# Ideas for Future Work

## Context Window Improvements

- ~~**Show agent state in context**~~: DONE — `<agent_state>` block rendered between work_queue and context with memory (sorted by priority), active timers, running processes. Bounded by RenderConfig limits.

- **Smarter token estimation**: Replace `chars / 4` approximation with actual tokenizer (tiktoken or the Anthropic counting endpoint). The compaction dry-run estimate would be more accurate, and we could avoid the "compaction loop" issue where thresholds are set too close to base context overhead.

- **Dynamic modification window**: Instead of fixed last-5-entries, compute based on token budget. When context is tight, shrink the window; when it's spacious, expand it.

- **Incremental context diffing**: Instead of re-rendering the entire context each turn, compute the diff from last turn and send only changed portions. Would dramatically improve prompt cache hit rates for the user message (not just the system prompt).

## Agent Capabilities

- **Self-improvement**: Let the agent edit its own system prompt or add custom Python functions that persist across turns (like Conway's self-improvement). The agent could learn project-specific patterns and optimize its own workflow.

- ~~**Sub-agents**~~: DONE — `spawn_agent(task, model, memory, max_turns, priority)` launches child agents via `child_agent.rs`. Max 3 concurrent, max 50 turns, no recursion. Children sandboxed (no message sending, no process spawning). Returns `ChildAgentCompleted` work item.

- **Structured tool outputs**: Instead of just stdout strings, let Python scripts return structured data (JSON) that gets rendered more usefully in history and work items.

- **Agent-initiated questions**: Give the agent a `ask_user(chat_id, question, options=[])` function that sends a question and pauses that task until the user responds. Currently the agent can only send messages, not ask questions.

## New Built-in Tools

- **HTTP request tool**: `http_get(url)` / `http_post(url, body)` that returns the response directly, instead of shelling out to curl. Much cheaper (no process overhead, no extra turn).

- **File watcher**: `watch_directory(path, callback_description)` that uses OS-level file notifications (inotify/FSEvents) instead of polling via timers. More efficient and more responsive.

- **Cron-style scheduling**: `schedule(cron_expr, description)` for complex recurring patterns beyond simple intervals.

- **Deployment plugins**: Implement the `DeploymentPlugin` trait for real use cases — home automation (HomeAssistant API), DevOps (kubectl, terraform), monitoring (Prometheus queries), etc.

## Security & Sandboxing

- ~~**Python execution timeout**~~: DONE — `CLAUDE_SERVER_PYTHON_TIMEOUT` env var (default 5s). Uses `PyErr_SetInterrupt` to interrupt blocked scripts.

- **Import restrictions**: Optionally block dangerous imports (`subprocess`, `os.system`, `socket`) to prevent the agent from bypassing harness controls. Could use a custom import hook or a restricted `__builtins__`.

- **Filesystem sandboxing**: Restrict file I/O to specific directories. The agent currently has full filesystem access.

- **Resource limits**: Memory limits on the Python interpreter, process spawn rate limiting, outbound message rate limiting.

## Operations & Observability

- **Web dashboard**: A real-time web UI showing the agent's memory contents, active timers, running processes, event history, and queue state. Much more useful than `--dump-turns` for understanding what the agent is doing.

- **Structured logging**: Replace `println!` statements with structured JSON logs (using `tracing` crate). Enable log levels, filtering, and integration with monitoring tools.

- **Metrics**: Track turns per task, error rate, compaction frequency, cache hit rate, token usage over time. Expose via Prometheus endpoint.

- **Session replay**: Record the full sequence of rendered contexts and agent responses, then replay them for debugging or analysis without making API calls.

- **Cost tracking**: Track API cost per session (input tokens × rate + output tokens × rate + thinking tokens). Show running total in the chat UI.

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
