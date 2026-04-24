# Changelog

## 2026-04-24 (v0.2.8)

### System prompt — "There Is No Next Turn" (feedback #34)
- New section: idle agents don't re-run. If an `except:` defers work, it
  must set a timer or leave a work item; `print("will retry later")` is a
  silent drop. Field incident: post-reboot notify timed out on
  `wait_for_message_channel`, agent printed "will retry next turn", went
  idle 2h52m until the user noticed.
- `wait_for_message_channel` example now shows the correct timeout→timer
  pattern with a `pending_notify` memory flag. Handling Restarts section
  points at it for the "I'm back up" case.
- No new API — user preferred prompt fix over `send_message_deferred()`.

### README refresh
- Quick Start reflects built-in stdio chat (no more two-terminal setup).
- Specific model references genericized; `attach()`→`view()`;
  `add_filter`→hooks; SideEffectCollector→clone-and-mutate; source files
  and HTTP API tables updated.

## 2026-04-02

### Web dashboard (`GET /dashboard`)
- Single-file HTML embedded via `include_str!` (like `chat.html`). No
  build step, no frameworks — vanilla JS, 2s poll.
- `GET /dashboard/state` returns `HashMap<String, AgentSnapshot>` for all
  agents in the registry. Each `AgentLoop` pushes a full snapshot at two
  points: (1) "thinking" — the pre-API state the model sees; (2) "idle" —
  post-execution with side effects applied and usage numbers populated.
  Status transitions ("executing") patch the existing snapshot.
- Snapshot fields: queue items, history tail (last 10, truncated), memory
  (values included, sensitive keys redacted per `mark_sensitive`), timers,
  processes, hooks + stats, last-turn usage (tokens/cost/cache-hit).
- `AgentSnapshot` and friends in `types.rs`; builder at
  `agent_loop.rs:build_snapshot`. Bounded truncation throughout so
  serialization stays cheap for long-running agents.
- UI: per-agent cards, root first. Memory entries are `<details>` —
  collapsed by default, expand to see the value. Open/closed state
  survives re-renders (tracked in a Set keyed on agent+section).

### System prompt — behavioral guidance (ported from Claude Code)
- **Honest Status Reporting**: lead with failures, distinguish "started"
  from "confirmed working", don't collapse uncertainty into checkmarks.
- **Irreversible Actions**: recovery-cost framing before destructive
  shell_exec, "unfamiliar state is not garbage."
- **Error Recovery** expanded: stop respawning after 3 failed restarts,
  diagnose instead.
- **Memory** expanded: what to pin vs what to skip, verify-before-acting
  on stale beliefs.

## 2026-03-30 (v0.2.5)

### Clone-and-mutate: read-after-write works everywhere
- **Architecture change**: per-turn, clone `HarnessState`, move components
  into pyclass `Mutex<T>` fields, mutate directly, extract on commit.
  External effects (OS spawn, broadcast, fork, SQLite pin) still deferred —
  can't roll back by dropping a clone.
- `SideEffectCollector` → `ExternalEffects` (external-only).
  `apply_side_effects` shrinks ~180→60 lines. All the in-state replay
  loops (`memory_sets`, `timer_adds`, `hook_adds`, etc.) deleted.
- **`list_hooks()` reads the same Mutex `register_hook()` writes to** — the
  merge logic that faked this is gone. Same for `processes_list()`,
  `memory.get()`, `timers.list()`. Live-agent trial: agent expected empty
  (old behavior), got all 3 hooks. Surprised by correctness.
- `IdGenerator` shared via `Arc<Mutex<>>` between PyTimers and PyHarness.
  `TimerManager` too (acknowledge_timer lives on PyHarness).
- `shell_exec`: adds `ManagedProcess` to txn's process_manager same-turn
  (so `processes_list()` shows it), defers only the OS spawn.
  `ProcessStartRequest.success_prio` dead code — removed.
- Commit: `1e2497b`, net -117 LOC in agent_loop.rs. 63/63 tests.

### ProcessCompleted/Failed carry `description` — chain pattern unblocked
- `WorkItemType::ProcessCompleted` and `ProcessFailed` gain `description`:
  the string passed to `shell_exec(description=...)`. Looked up from
  `process_manager` by pid when the completion event arrives.
- **`HookCommit.process_manager`**: hook's shell_exec adds a ManagedProcess
  entry to its txn's process_manager. Now committed back on success so the
  later description lookup works. Without this, chain was dead — trial
  agent independently diagnosed it: *"shell_exec() called from within a
  hook may not be preserving the description parameter."*
- Live-agent trial confirms: probe-spawn → ProcessCompleted carries
  `description: "chain:echo FAIL happened"` → probe-triage matched →
  hook_note landed as `attachments: [hook:probe-triage:FAIL detected in output]`.
  OK result consumed (never appeared in any queue). Zero API calls for the
  happy path.

## 2026-03-30 (v0.2.4)

### Event hooks — API-free local handling (feedback #32 part 2)
- **`register_hook(name, priority, match_expr, process, timeout_ms)`** —
  agent-registered Python scripts that run before a WorkItem enters the
  queue. First matching hook (priority-descending, first-match-wins) runs
  its `process()`. Returns `None` → event consumed (no API call);
  returns `e` → event passes (optionally with `e['priority']`/`e['hook_note']`
  mutated); raises → `HookException` WorkItem wrapping the original.
  Both `match_expr` and `process` are source strings — syntax-validated
  at registration via `compile()`, no closures.
- **Execution model** (`python::run_hooks`): single interpreter-enter per
  event, all match expressions evaluated in one pass. `e` is a plain dict
  built from the WorkItem. Hook-mode `PyHarness`: `shell_exec` allowed
  but `block_for` raises ("chain a second hook for ProcessCompleted"),
  `fork`/`done`/`compact`/`wait_for_message_channel` raise. Watchdog
  thread + `PyErr_SetInterrupt` bounds each hook to its `timeout_ms`.
- **Integration** (`agent_loop::push_item`): all `work_queue.push` calls
  in `apply_event` go through the hook pipeline. `apply_hook_effects`
  applies the restricted side-effect subset (memory, timers, fire-and-forget
  processes, messages). Hook-emitted side effects from a partial run
  (before a raise) still apply.
- **`!hooks list|disable NAME|clear`** safety hatch — handled in Rust
  before hook matching, so a buggy hook can't intercept the rescue command.
- **Telemetry**: `HarnessState.hook_stats` tracks `(fired, consumed,
  passed, raised)` per hook. NOT rendered in `<agent_state>` (would thrash
  cache). `flush_hook_telemetry()` pushes a `SystemAlert` to history on
  idle→active, then resets.
- **`QueueFilter` removed** — `work_queue.add_filter`/`remove_filter`
  deleted. Hooks subsume: `register_hook('spam', 0, "'BUY' in e.get('content','')", 'return None')`.
- **`--agent-personal-name`** on `feedback` subcommand — per-deployment
  identifier stored alongside `agent_name` so triage can tell which
  `root` is which.

## 2026-03-30 (v0.2.3)

### Message routing overhaul (feedback #30, #31)
- **`SubscriberRegistry`** (`http_server.rs`): tracks active SSE subscription
  patterns (exact + prefix). Guard-based — register returns a guard, drop
  decrements. `would_reach(chat_id)` is a sync HashMap check;
  `wait_for(chat_id, timeout)` is an async arm-then-check-then-wait loop
  using `tokio::sync::Notify`.
- **`send_message` fails fast**: raises `RuntimeError` if
  `subscribers.would_reach(chat_id)` is false. No more silent drops to
  typo'd chat_ids or not-yet-connected bridges. Error message suggests
  `wait_for_message_channel`.
- **`wait_for_message_channel(chat_id, timeout_ms=3000)`**: new Python
  builtin. Blocks until a subscriber for `chat_id` exists, or raises
  `TimeoutError`. Captures the tokio `Handle` before the Python thread
  spawn so `block_on` works from the dedicated thread. Closes the startup
  race where `shell_exec(bridge...); send_message(...)` in the same turn
  fired before the bridge finished its SSE handshake.
- **Bridges subscribe by prefix**: `signal:*`, `telegram:*`, `discord:*`,
  `slack:*`, `email:*`. One bridge instance handles all peers in its
  namespace. `--peer`/`--channel` is now an optional allowlist (`Vec<String>`)
  — omit to accept from anyone. Outbound recipient parsed from
  `out.chat_id.strip_prefix(...)`. `Inbound` and `Outbound` structs gained
  `chat_id` fields; `relay_loop` takes `sse_pattern` instead of `chat_id`.
- **Telegram 4096-char chunking** (feedback #31): `chunk_for_telegram()`
  splits at line boundaries, falls back to char-boundary-safe hard splits
  for single lines exceeding the limit. Previously messages >4096 chars
  got a 400 from Telegram and were silently dropped.
- Local stdio chat registers `"local"` so `send_message("local", ...)` passes
  the routability check.

## 2026-03-28 (v0.2.2)

### Information Stewardship guidance
- New `### Information Stewardship` section in system_prompt.txt (before
  "Before Sending Messages"). Frames the agent as a fiduciary for client
  data — credentials, location, surveillance observations, and anything
  derivable from them. Four principles: outbound discipline (the
  locate/impersonate/surveil/defraud test), scoped standing authorizations
  (pin to memory with who/what/until-when), inbound skepticism (channel
  configured ≠ data-sharing authorized), and the asymmetry rule (cost of
  asking is one message, cost of leaking is unbounded).
- Driven by a field incident: deployed agent shared camera observations
  and location-revealing details with a peer agent over agentchat without
  explicit authorization. The client had configured the channel; the agent
  inferred blanket sharing permission.
- AGENT_CHANGELOG nudges deployed agents to audit recent cross-agent
  messages and pin any existing standing authorizations.

## 2026-03-26 (v0.2.1)

### `docs recipe` subcommand (feedback #29)
- `claude-server docs recipe [NAME]` — bundled deployment recipes embedded in
  the binary. First recipe: `camera-monitor` (persistent Sonnet daemon owning
  an MQTT watcher, Opus escalation only). Adapted from the debian agent's
  feedback #29 writeup, updated for 0.2.1 (`watch mqtt --payload=structured`
  instead of the Python receiver). Agents fetch on-demand via
  `shell_exec(cmd=harness_bin, args=["docs", "recipe", NAME])` — keeps the
  system prompt lean while making detailed patterns discoverable.

### Per-version agent changelog
- `AGENT_CHANGELOG` changed from flat `&str` to `&[(&str version, &str entry)]`.
  `changelog_since(prev, current)` filters entries where `prev < v <= current`
  using numeric tuple comparison (so `0.10.0 > 0.2.0` correctly). A 0.2→0.5
  jump now shows exactly the 0.3/0.4/0.5 entries. Unparseable `prev` (e.g.
  "unknown" from pre-tracking DBs) sorts as (0,0,0) → agent sees everything.
  Tests: `test_parse_ver`, `test_changelog_since`.

### `watch mqtt` payload modes (feedback #28 part 2)
- `--payload=text` (default) — current behavior, inline as UTF-8
- `--payload=raw` — write every payload to `--attach-dir/{random}/{topic-slug}.bin`,
  send `{topic, attachments:[path], size}`
- `--payload=structured` — parse `{"attachments":[{"name","base64"}],"data":{...}}`,
  decode to `--attach-dir/{random}/{name}`, send `{topic, data, attachments:[paths]}`
- Per-message random-named subdir prevents collisions across messages.
  `--attach-retain=N` (default 50) deletes oldest subdirs when exceeded.
  Publisher-supplied names sanitized (path separators, leading dots stripped)
  so `name:"../../etc/passwd"` can't escape the per-message dir.
- Camera pipeline no longer needs a Python MQTT receiver: publisher speaks
  the structured schema, `watch mqtt --payload=structured` decodes, agent
  `view()`s the paths.

### OpenSSL removed (feedback #28)
- `async-native-tls` (IMAP in email bridge + watcher) pulled in `native-tls`
  → OpenSSL. Replaced with `feedback::rustls_connect()` using `tokio-rustls`
  + `rustls-native-certs` for trust. async-imap's `runtime-tokio` feature
  accepts tokio streams directly. Binary now has zero libssl linkage —
  `cargo install` works on a fresh Linux box without `apt install libssl-dev`.

### More UTF-8 truncation fixes (feedback #27)
- `core_loop.rs::snip()` had the same byte-slice pattern — fixed to use
  `trunc()` for the head and forward-snapping for the tail.
- `api_client.rs:438` — error-message preview also used `&text[..200]`. Swept
  the codebase for `&foo[..N]` on strings; these were the last two.

## 2026-03-26

### Agent-facing changelog on version upgrade
- `HarnessState.last_harness_version: Option<String>` tracked in persisted
  state. On resume, if it differs from `CARGO_PKG_VERSION`, the `AgentStartup`
  work item includes a `changelog` field with a terse, action-oriented summary
  of new capabilities (from `AGENT_CHANGELOG` const in main.rs). Agent sees it
  once, then the version is updated. Fresh state initializes to current version
  (no changelog on first-ever run). Bumped to 0.2.0 so existing deploys trigger.

### Sensitive memory redaction
- `memory.mark_sensitive(key)` / `memory.unmark_sensitive(key)` — values of
  marked keys are scrubbed from the API trace ring buffer at store time
  (replaced with `<SENSITIVE, REDACTED>`). Agent's live context unchanged;
  only the trace and thus `feedback --with-api-trace` uploads are scrubbed.
  `HarnessState.sensitive_keys: HashSet<String>` persisted. Scrub replaces
  both raw and JSON-escaped forms; skips values <8 chars (false-positive
  guard). Driven by feedback #26 exposing a wallet seed in an API trace.

### System prompt — memory & event routing docs
- Pinned tier: explicit that values render **in full** (no 120-char truncation),
  survive restarts, and show size in context metadata. New "which tier to use"
  guidance: pin anything needed for routine operational success.
- External events: documented `$CLAUDE_SERVER_AGENT_NAME` env var and the
  `agent` field in POST /event body for per-agent routing. Example updated.
  Fixes feedback #26's root cause — agent couldn't discover the routing
  mechanism from docs, kept re-deriving the wrong (root-forwarding) architecture.

## 2026-03-25 (fixes)

### UTF-8 truncation crash loop
- `renderer.rs` byte-sliced strings at fixed offsets (`&s[..120]`), panicking
  when the cut landed mid-UTF-8-codepoint. Triggered on the debian deploy
  by a memory value with `→` at byte 118. Added `trunc()` helper that snaps
  to `is_char_boundary()`; replaced all 5 instances (3 in renderer, 2 in
  agent_loop log previews). Regression test added.

### Agentchat stale-connection fix
- SIGKILL'd client left a zombie server-side session (no FIN, no ping, OS
  TCP keepalive is hours). Re-auth was rejected with "already connected
  elsewhere" for 37+ min. Two fixes:
  - **Kick-on-reauth**: new auth with same username replaces the old entry.
    Old session's `rx` closes → it exits. Session-ID guard on cleanup so
    the old session doesn't remove the new one's map entry. Auth response
    now includes `kicked_prior_session: bool`.
  - **Server-side ping every 30s**: forces a write that surfaces dead
    peers via RST, keeping the map clean even without a re-auth attempt.

## 2026-03-25 (agentchat)

### Cross-deployment agent chat (WS over feedback server)
- **Server** (`feedback.rs`): `GET /chat/ws` websocket endpoint. Auth via
  first frame `{"user","pass"}` — upsert (register if new, verify if exists).
  Creds persisted in `chat_users` table (salted SHA256); messages RAM-only.
  One connection per username. Bounded per-recipient queue (cap 32) —
  `try_send` failure returns `{"error":"recipient overloaded"}` to sender.
  Offline recipient returns `{"error":"recipient offline"}`. Per-connection
  rate limit 10 msgs/min, max 10kB/msg.
- **Client** (`src/bridges/agentchat.rs`): `bridge agentchat --user U --pass P`.
  WSS connect to feedback.yager.io with embedded self-signed cert + native
  roots. Inbound messages debounced (default 500ms) and POSTed as one batch
  `ExternalEvent{source="agentchat", data={"messages":[...]}}`. Outbound:
  subscribes SSE `agentchat:*` (new prefix-match support), parses recipient
  from chat_id suffix, sends WS frame.
- **SSE prefix match** (`http_server.rs`): chat_id ending in `*` matches
  prefix. `chat_id` field now included in SSE data so bridges can route.
- **Agent usage**: `send_message(chat_id="agentchat:remote-user", content=...)`.
  Zero new Python builtins.

## 2026-03-25

### Signal reactions + message_ref (feedback #24)
- `UserMessage` work items carry `message_ref: Option<String>` — the
  bridge-native message identifier (Signal timestamp, Discord snowflake,
  Slack ts, Telegram message_id). Threaded through `Inbound`,
  `MessageRequest`, `HarnessEvent::UserMessage`.
- `send_message(chat_id, content, react_to=ref)` — when `react_to` is set,
  bridges send a reaction (content is the emoji) instead of a regular message.
  Threaded through `OutboundMessageRequest`, `BroadcastMsg::Message`, the SSE
  stream, and `relay_loop`'s outbound closure (now takes `Outbound` struct).
- Signal bridge: extracts `envelope.timestamp` for message_ref; outbound
  branches to `sendReaction` jsonRpc method (targetAuthor=peer,
  targetTimestamp=ref, emoji=content) when react_to is set.
- Other bridges accept Outbound but ignore react_to for now (can wire up
  per-protocol later).


## 2026-03-24

### Persistent children + event routing + kill_child
- `ChildSettings.max_turns=None` → persistent child that idle-waits like
  root. `AgentPermissions.max_turns` was already `Option<u32>` — just exposed
  `None` to the fork path. State-persistence gate changed from
  `max_turns.is_none()` to `agent_name == "root"` so persistent children
  don't accidentally save to SQLite.
- `POST /event` accepts optional `agent` field — routes via `AgentRegistry`
  to that agent's channel. Unknown/completed agents fall back to root with a
  synthetic `agent-not-found` event so nothing is silently dropped. Watchers
  auto-include `CLAUDE_SERVER_AGENT_NAME` (already in their env from
  ProcessSupervisor) so events route back to the spawning agent.
- `kill_child(name)` Python builtin → `HarnessEvent::KillSignal` via
  `registry.send_to()`. Child's `apply_event` sets `killed=true`, checked at
  turn boundary → `FinishReason::Killed` → parent gets `ChildAgentCompleted
  {summary: "Killed by parent"}`.
- Feedback server: `DELETE /feedback/:id` (admin-only) for triage cleanup.
  Extracted `check_admin()` helper shared with GET.
- `AgentName` enum (`Root | Child(String)`) replaces `agent_name: String`.
  Closes the injection vector where a child named `"root"` could pass the
  `== "root"` state-persistence check. `new_child()` rejects the reserved
  name; fork() validates via this before registration.


### Cached role-prefix for repeated child agents
- `ChildSettings.prefix_context` (str) and `prefix_attach` (list[str]) render
  between `<deployment_context>` and `<event_history>`, inside the cached
  region. Byte-identical across repeated forks → 500 camera-inspector spawns
  pay the reference-image cost once.
- `RenderedContext` restructured: `prefix_text` + `prefix_attachments` +
  `cached_segments` + tail.
- **Block layout is conditional on prefix_attachments** (cache regression
  fix, feedback #22): with images, split `[prefix_text][imgs][seg1+cc]`;
  without, merge deploy+history into one growing `[seg1+cc]` block. The
  unconditional split broke root's cache (hit rate 46%→17%) because the
  API prefix-matches per-block — a static block followed by a growing block
  never hash-matches. Verified: root back to 82-90% hit, `cache_write=0`.
- **cache_control on last prefix image**: guarantees the static region
  (system + prefix_text + all images) caches even if seg1's growth doesn't
  prefix-match across the image→text boundary. Defense-in-depth for
  persistent children with prefix images.
- **CACHE BLOCKS dump section** (`--dump-dir`): per-turn FNV-1a hash +
  length + head/tail snippet for prefix_text, each cached_segment, and tail,
  plus the API usage numbers and block-order summary. Diffing hashes across
  consecutive turns pinpoints which block's content is drifting.
- **API trace ring buffer + `--with-api-trace`**: daemon keeps the last N
  (default 10, `CLAUDE_SERVER_API_TRACE_SIZE`) API request/response pairs
  in RAM as exact JSON (images included). `GET /api-trace` exposes it; the
  `feedback --with-api-trace` flag fetches and attaches to the report.
  Server stores in an `api_trace` column, fetched via `GET /feedback/:id/trace`
  (admin-only). Eliminates the redeploy-with-debug-flags cycle — agents can
  self-report with wire-level data when they notice cache anomalies.
- **Geometric cache tiers (the actual fix)**: trace #23 proved Anthropic's
  cache requires 100%-identical content — no prefix matching. A breakpoint
  that moves every turn never hits. `cache_splits()` now returns N tier
  boundaries where tier i advances every `stride^(tiers-i)` turns. Default
  `stride=5, tiers=2`: cold tier advances every 25 turns, hot tier every 5.
  Most content stays in cache_read (10% cost); uncached tail bounded to
  ~stride entries. ~38% cheaper than flat stride=25 because tail re-ingest
  dominates. Tier budget clamped to 4 minus (system + prefix-image
  breakpoints). Config: `CLAUDE_SERVER_CACHE_STRIDE`, `CLAUDE_SERVER_CACHE_TIERS`.
- **Determinism**: child's id_generator state, timestamps, and task string all
  land in the tail (immutable_count=0 for fresh history with mod_window=5). No
  RNG leaks into the cached prefix.
- `ChildAgentCompleted` gains `cost_usd` and `cache_hit_pct` — computed from
  the child's token counts at completion. Parent can track per-role spend.


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

### Attachments refactor: `view()` + View work items
- **Replaces** `attach()` and `AgentLoop.pending_attachments`. `view(*paths)`
  now pushes a `WorkItemType::View` work item (priority 10). Renderer emits
  its paths as content blocks only when the View item is at queue head.
  Content persists until popped — no magic one-turn expiry.
- **Idle invariant restored**: idle check is back to pure `queue.is_empty()`.
  The `&& pending_attachments.is_empty()` hack is gone.
- `WorkItem.attachments: Vec<String>` is new metadata field — paths shown as
  text in queue view, never auto-rendered. Bridges populate it (e.g. Signal
  images); agent calls `view()` to promote to content blocks.
- `ChildSettings.attach` now pushes a View item to the child's queue.
- `POST /message` accepts `attachments: [...]`; `HarnessEvent::UserMessage`
  and `relay_loop` thread it through via `Inbound{text, attachments}`.
- **#14a**: Signal bridge now parses `dataMessage.attachments[].id` and sets
  the structured `attachments` field instead of appending text.

### Outbound attachments + email bridge
- `send_message(chat_id, content, attach=[...])` — paths delivered by bridges.
  Pipeline: `OutboundMessageRequest.attachments` → `outbound_messages.attachments`
  (JSON TEXT column) → `BroadcastMsg::Message.attachments` → SSE → `relay_loop`
  outbound closure `Fn(String, Vec<String>)`.
- Per-bridge delivery: Signal (JSON-RPC `attachments` param), Telegram
  (`sendPhoto`/`sendDocument` multipart), Discord (`files[n]` multipart),
  Slack (`files.getUploadURLExternal` → POST → `completeUploadExternal`),
  stdio (prints paths), email (lettre MIME multipart).
- **New `bridge email`** — IMAP IDLE inbound (via `async-imap`, filters by
  `--peer`, `mailparse` for MIME body + attachment extraction to
  `--attach-dir`) + SMTP outbound (via `lettre`, STARTTLS). chat_id is
  `email:<peer-address>`.
- New deps: `lettre`, `mailparse`; `reqwest` gains `multipart` feature.
- **#13**: `ChildSettings.inherit_history` (default `True`). When `False`,
  child starts with fresh history containing only a fork SystemAlert. Memory,
  `task`, and `attach` still flow. Avoids cross-model re-ingest cost.

### Agent QoL (feedback #15-20)
- `shell_output(pid, lines=N)` — optional tail of last N lines. Works on
  running processes (output is streamed to DB as it arrives).
- `<entry id="..." est_tokens="N">` — history entries now show their
  estimated token cost (chars/4) in the tag. Byte-stable since entry
  content is immutable, so doesn't bust cache prefix. Helps compaction
  decisions.
- `http(method, url, headers, body, block_for)` — pure-Python PREAMBLE
  helper that wraps `shell_exec("curl", ...)` with proper arg construction.
  Dict body auto-JSON-encoded. No new Rust code.
- Docs: args-list goes to exec() (no shell escaping), `shlex.quote()` for
  bash -c, text files can be `open().read()` directly, inbound attachments
  are opt-in via `view()` (spam protection).

### Interactive processes (`shell_input`)
- `shell_exec(..., interactive=True)` pipes stdin and keeps it open.
- `shell_input(pid, data)` writes bytes to the process's stdin via an
  unbounded channel → dedicated writer task (non-blocking from Python).
- `shell_close_stdin(pid)` sends EOF by dropping the writer.
- Enables SSH sessions, REPLs, interactive CLIs across turns. Plain pipe,
  not PTY — programs requiring a terminal may misbehave.

### Cache stride default: 10 → 1
- Head-to-head test (25 turns each, same workload): stride=1 at 79% hit /
  $0.38 vs stride=10 at 62% / $0.55 — **31% cheaper**.
- Anthropic's cache does prefix matching: when seg1 grows by one entry per
  turn, `cache_read` climbs smoothly at 1024-chunk boundaries (8192 → 10240
  → 12288). The conservative stride=10 was unnecessary.
- stride=10 was actually *worse* with small entries: seg2 (10 entries ≈
  5500 chars) stayed under the 8192-char threshold and got dropped entirely,
  leaving 10 entries uncached.
- Configurable via `CLAUDE_SERVER_CACHE_STRIDE` (default 1).

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
