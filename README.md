# Claude Server

Claude Server is a harness for Claude focused on long-running instances
that need to autonomously manage complex systems.

## How it's Different

We don't use the traditional conversation architecture.

Instead, we provide the agent with:

* An event history
* A work queue
* Misc tools like a memory system, async shell commands, sending/receiving DMs, etc.

and it interacts with *all of* those systems *entirely* through python scripting.

Our (Rust) harness has an embedded Python interpreter.
At every single turn, the agent is just writing a short python script.

Even context compaction is done via python scripting!

## Caveat Emptor

I came up with the initial spec for this
(mostly consisting of example context windows and API calls I wrote out by hand)
but then I had fennec actually write everything, so I cannot attest to the quality of the code.

## Quick Start

### Prerequisites

- **Rust** — install via [rustup](https://rustup.rs/): `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
- **Python 3.13+** with a shared library (`libpython3.13.dylib` / `.so`). Most system Pythons and conda/miniforge installs work. You can also use [uv](https://docs.astral.sh/uv/) to install one: `uv python install 3.13`
- **Anthropic API key** — get one from [console.anthropic.com](https://console.anthropic.com/)

### Setup

```bash
git clone <repo-url> && cd claude-server
export ANTHROPIC_API_KEY=sk-ant-...
```

### Run

```bash
# Terminal 1: start the agent daemon
make

# Terminal 2: open the chat UI in your browser
make chat
```

That's it. The daemon runs on port 3000 and the chat UI opens at http://127.0.0.1:8080.

### Other commands

```bash
make build               # build without running
make run-dump            # run with full context/response dumps each turn (debugging)
make chat CHAT_PORT=9090 # chat UI on a custom port
```

You can also talk to the daemon directly via its HTTP API:

```bash
# Send a message
curl -X POST http://127.0.0.1:3000/message \
  -H 'Content-Type: application/json' \
  -d '{"user":"you@example.com","content":"Hello Claude!"}'

# Check for responses
curl http://127.0.0.1:3000/messages/<chat_id>
```

## Architecture Overview

Claude Server is a Rust program that drives Claude through a work-queue-based loop.
The agent interacts with the world exclusively through Python scripts, executed in
a fresh Python interpreter on every turn.

```
                     +-----------------+
                     | External Events |
                     | (users, timers, |
                     |  processes)     |
                     +--------+--------+
                              |
                              v
+------------------------------------------------------------+
|                      Rust Harness                          |
|                                                            |
|  +------------+  +---------+  +--------+  +-------------+ |
|  | Work Queue |  | Event   |  | Timer  |  | Process     | |
|  | (priority) |  | History |  | Manager|  | Manager     | |
|  +------+-----+  +----+----+  +----+---+  +------+------+ |
|         |              |           |              |        |
|  +------+--------------+-----------+--------------+------+ |
|  |              Context Renderer                         | |
|  |  Serializes harness state into a text prompt          | |
|  +---------------------------+---------------------------+ |
|                              |                             |
|  +---------------------------v---------------------------+ |
|  |                   API Client                          | |
|  |  - Sends single-message request to Claude API         | |
|  |  - Defines `execute` tool for Python output           | |
|  |  - Receives tool_use response                         | |
|  +---------------------------+---------------------------+ |
|                              |                             |
|  +---------------------------v---------------------------+ |
|  |              Python Executor                          | |
|  |  - Fresh interpreter per turn                         | |
|  |  - Loads serialized harness state                     | |
|  |  - Runs Claude's script                               | |
|  |  - Side effects execute immediately                   | |
|  |  - Returns stdout + modified state                    | |
|  +-------------------------------------------------------+ |
+------------------------------------------------------------+
```

### Core Loop

```
1. Wait for work queue to be non-empty
2. Render full context (deployment + history + queue + metadata) into text
3. Call Claude API with rendered context as single user message
4. Extract Python code from Claude's tool_use response
5. Execute code in fresh Python interpreter
6. Record code + output in event history
7. Apply any state changes (timers, memory, queue ops, messages)
8. If work queue is still non-empty, go to 2
9. If work queue is empty, go to 1 (sleep)
```

When the work queue is empty, the agent sleeps. External events (user messages,
timer firings, process completions) add items to the work queue, waking the agent.

## API Integration

### Messages API Call Format

Each turn is a single API call. The harness constructs the request as follows:

```json
{
  "model": "claude-opus-4-5-20251101",
  "max_tokens": 16384,
  "system": [
    {
      "type": "text",
      "text": "<system prompt text>",
      "cache_control": { "type": "ephemeral" }
    }
  ],
  "tools": [
    {
      "name": "execute",
      "description": "Execute a Python script in the agent environment. You have access to the work queue, memory, timers, event history, and deployment-specific tools. Use this tool to perform all actions.",
      "input_schema": {
        "type": "object",
        "properties": {
          "code": {
            "type": "string",
            "description": "Python code to execute"
          }
        },
        "required": ["code"]
      }
    }
  ],
  "messages": [
    {
      "role": "user",
      "content": "<rendered context>"
    }
  ]
}
```

The model is configurable (default: `claude-opus-4-5-20251101`).

### Why tool_use Instead of Raw Text

Claude outputs its Python code via a structured `tool_use` block rather than raw text.
This eliminates parsing ambiguity: the `input.code` field contains exactly the Python
to run, with no risk of Claude including explanation text, closing code fences early, etc.

### Prompt Caching

The context is deliberately structured so that the bulk of each API call is a
stable, cacheable prefix that grows monotonically over the agent's lifetime.

**System prompt**: Has `cache_control: { "type": "ephemeral" }` applied. Identical
across all turns, so it's always a cache hit after the first call.

**User message** (the rendered context): Structured as:

```
<deployment_context>STABLE — never changes</deployment_context>
<event_history>
  <entry ...>FROZEN — old entries never change</entry>
  <entry ...>FROZEN</entry>
  ... hundreds of frozen entries over time ...
  <entry ...>modifiable (last ~5 entries)</entry>
</event_history>
<work_queue>changes every turn</work_queue>
<context>changes every turn</context>
```

The deployment context and all history entries outside the modification window
(the most recent ~5) form an immutable, monotonically growing prefix. New entries
are appended and eventually age out of the modification window, at which point
they become permanently frozen. The API caches this entire prefix, so each turn
only pays for the new tail (recent history + work queue + metadata).

The only event that disrupts the cache prefix is **compaction**, which rewrites
old history entries. This is why compaction is done in a single atomic script
rather than incrementally — one cache miss instead of many.

### Token Usage Tracking

The API response includes `usage.input_tokens`. The harness tracks this value
and triggers compaction when it exceeds 80% of (context_window - max_tokens).


## The Agent's View of the World

Each turn, Claude sees a single user message containing the full rendered context:

```
<deployment_context>
{deployment-specific information about this Claude Server instance}
</deployment_context>
<event_history>
{chronological history of past actions and their outputs}
</event_history>
<work_queue>
{priority-ordered queue of pending work items}
</work_queue>
<context>
{current time, token usage, compaction state if applicable}
</context>
```

Claude responds by using the `execute` tool to run a Python script.

### Deployment Context

Varies per Claude Server instance. Contains information about systems, tools, users,
etc. that exist in this particular environment. For example, a home automation deployment
might describe available cameras, smart home APIs, and authorized users.

### Event History

A chronological log consisting of:

1. Python scripts Claude has run and their outputs (stdout + return values)
2. System alert messages (e.g. "high-priority task preempted your current work")
3. Compaction summaries (agent-written descriptions replacing older history)

Each history entry has a stable short hex ID assigned by the harness (e.g. `3a6f`).
IDs never change once assigned, so the agent can reference them in memories or documents.

History entries are truncated at a maximum character/line length for display.
Claude can retrieve full content by printing it in a script (e.g. `print(history["3a6f"].full_output)`).

Claude can replace or delete its most recent history entries (typically the last ~5)
to manage context size. The context metadata tells Claude which entries are within
the modification window (e.g. "Can modify entries from: a3f1"). Older entries
can only be modified during compaction — Claude doesn't need to aggressively
clean up old history, since compaction provides a dedicated opportunity for that.

### Work Queue

A priority-ordered queue of tasks. Priority ranges from 0-10, with 10 reserved
for system events (compaction). Within the same priority, items are sorted by time (ascending).

Work item types (each has different fields):

**All items share:** `id` (hex), `priority` (0-10), `time` (timestamp), `type` (string)

| Type | Extra Fields | Typical Priority |
|------|-------------|-----------------|
| `UserMessage` | `chat_id`, `user`, `content` | 9 |
| `TimerFired` | `timer_id`, `every` (interval or None), `description` | Set by Claude |
| `ProcessCompleted` | `pid`, `exit_code`, `output_preview` | 7 (default) |
| `ProcessFailed` | `pid`, `error`, `output_preview` | 8 (default) |
| `ProcessTimeout` | `pid` | 8 (default) |
| `ChildAgentCompleted` | `child_id`, `result_memory`, `turns_used`, `success`, `summary` | Set by caller |
| `Compaction` | `description` | 10 |

The work queue display is truncated to fit in context:
- First 3 items: up to 500 chars each
- Items 4-10: up to 200 chars each
- Items 11+: type, priority, time, and first 80 chars only

Claude can read full item content in Python (e.g. `print(work_queue[0].content)`).

### Context Metadata

A short block of current state:

```xml
<context>
Current time: 2026-02-01 08:35:26 PST
Last turn input tokens: 14832
Compaction threshold: 150000 tokens
Can modify entries from: a3f1
</context>
```

The "Can modify entries from" field tells Claude which history entries are
within the modification window (typically the most recent ~5). Entries at
or after this ID can be replaced or removed. Older entries are read-only
until compaction.

During compaction, additional fields appear (see Compaction section).


## Python Interpreter Environment

Each turn, Claude's script runs in a fresh Python 3 interpreter.
The harness serializes its state as JSON, loads it into the interpreter as
pre-defined objects, runs Claude's script, then deserializes the modified state back.

Side effects (sending messages, starting processes) execute immediately during
script execution.

**Non-blocking rule:** All Python scripts must complete quickly.
A configurable timeout (`CLAUDE_SERVER_PYTHON_TIMEOUT`, default 5s) enforces
this — scripts that block too long are interrupted via `PyErr_SetInterrupt`.
The agent has no access to blocking operations (no sleep, no waiting on I/O,
no blocking on network requests). All long-lived operations are managed
asynchronously through the work queue: start a process with `shell_exec()`,
set a timer, and handle the result when it arrives as a work queue item.

### Available Objects and Functions

#### Work Queue

```python
work_queue: WorkQueue

# All items share these fields:
item = work_queue[0]       # First (highest priority) item
item.id                    # Hex ID (e.g. "3a6f")
item.priority              # 0-10
item.time                  # Timestamp string
item.type                  # "UserMessage", "TimerFired", etc.
```

Different item types have different fields:

```python
# UserMessage
item.chat_id               # Conversation ID — use this to reply
item.user                  # Sender email (e.g. "steve@example.com")
item.content               # Message text (may be truncated in rendered view)

# TimerFired
item.timer_id              # Hex ID of the timer that fired
item.every                 # Interval string (e.g. "30s") or None for one-shot
item.description           # Description from when the timer was created

# ProcessCompleted
item.pid                   # Hex ID of the process
item.exit_code             # Exit code (0 = success)

# ProcessFailed
item.pid                   # Hex ID of the process
item.error                 # Error message string

# ProcessTimeout
item.pid                   # Hex ID of the process

# Compaction
item.description           # "You must compact your context."
```

Queue operations:

```python
work_queue.pop_front()     # Remove highest-priority item
work_queue.remove(id)      # Remove item by ID
len(work_queue)            # Number of items

# Persistent filters (applied to incoming items before they enter the queue)
work_queue.add_filter(name="spam_filter", regex=r"^spam:.*")
work_queue.remove_filter(name="spam_filter")
```

#### Memory

```python
memory: dict[str, str]

memory["key"] = "value"        # Store a memory (default priority 5)
memory.set("key", "value", priority=8)  # Store with explicit priority (0-10)
memory.set_priority("key", 8) # Change priority of existing key
memory.get_priority("key")    # Read priority of a key
del memory["key"]              # Delete a memory
print(memory["key"])           # Read a memory
"key" in memory                # Check existence
```

Memory is persistent across turns and survives compaction. Use it for information
that must not be lost (ongoing tasks, user preferences, important state).

#### Timers

```python
timers: TimerManager

# Create a recurring timer (returns the assigned hex ID)
timer_id = timers.add(
    every=timedelta(seconds=30),
    priority=6,
    description="Check driveway camera for contractor vans"
)

# Create a one-shot timer
timer_id = timers.add(
    at=datetime(2026, 2, 1, 17, 0, 0),
    priority=8,
    description="Remind Steve about dinner reservation"
)

# Manage timers
timers.cancel(timer_id)        # Cancel a timer
timers.list()                  # List all active timers
```

When a timer fires, a `TimerFired` work item is added to the queue.
Recurring timers keep firing until cancelled.

#### Event History

```python
history: HistoryManager

# Read history
history[id].code               # The Python code that was run
history[id].output             # stdout from execution
history[id].full_output        # Un-truncated output
history[id].time               # Timestamp

# Manipulate recent history (within deletion window)
history.replace_with_description(id, "Short summary of what happened")
history.remove(id)

# During compaction only: manipulate any history
history.add("Summary text to insert as a new history entry")
```

#### Communication

```python
# Send a text message to a user
send_message(chat_id="81d4", content="I see a white van in the driveway!")

# Send an image to a user
send_image(chat_id="81d4", image=frame_data, caption="Driveway camera")
```

#### Process Management

All process operations are non-blocking. `shell_exec` starts a process and
immediately returns a hex ID string. The agent cannot wait on or interact with
the process in the same script — results arrive asynchronously via work queue items.

```python
# Start a background process (returns immediately with a hex ID string)
pid = shell_exec(
    cmd="ffmpeg",
    args=["-i", "input.mp4", "-vf", "scale=640:480", "output.mp4"],
    description="Transcode video",       # Human-readable label
    env={"PATH": "/usr/bin"},
    alert_timer=timedelta(minutes=5),    # Alert if still running after 5 min
    success_prio=5,                      # Work queue priority on success
    fail_prio=7,                         # Work queue priority on failure
    block_for=timedelta(milliseconds=500) # Wait up to 500ms for completion
)

# For short commands, block_for lets the agent see the result on the next turn
# instead of waiting an extra round-trip. The ProcessCompleted work item includes
# output_preview with the last ~500 chars of stdout/stderr.

# These can be called on FUTURE turns to check on a process:
shell_status(pid)        # Returns: "running", "completed", "failed"
shell_output(pid)        # Full stdout/stderr (only if output_preview was truncated)
shell_kill(pid)          # Kill the process
processes_list()         # Returns [(pid, cmd, description, status), ...]
```

#### Sub-agents

```python
# Spawn a child agent to handle a task in parallel
spawn_agent(
    task="Summarize the API documentation at /tmp/api-docs.md",
    model="claude-sonnet-4-5-20250929",   # Optional, defaults to parent's model
    memory={"file_path": "/tmp/api-docs.md"},  # Seed memory for the child
    max_turns=20,                          # Max turns before forced stop (max 50)
    priority=6                             # Priority of the ChildAgentCompleted work item
)
```

Children can send messages via `send_message()` but cannot spawn processes
(`shell_exec`) or spawn their own children (`spawn_agent` raises `RuntimeError`).
Max 3 concurrent children (`CLAUDE_SERVER_MAX_CHILDREN`).
When a child finishes, a `ChildAgentCompleted` work item arrives with `result_memory`,
`turns_used`, `success`, and `summary`.

#### Context Management

```python
# Add data to be shown in the next turn's context (e.g. images)
show_in_context(data)

# Standard output is captured and shown in history
print("This appears in the history output")
```

#### Deployment-Specific Tools

Each deployment provides additional Python objects. For example, a home automation
deployment might provide:

```python
camera_tool.get_interesting_frames(camera="driveway", max_frames=5, from_time=..., to_time=...)
home.set_thermostat(temperature=72)
home.lock_door("front")
```

These are documented in the deployment context section of each turn's prompt.


## Example Agent Session

The agent is not invoked until the work queue is non-empty. Let's say
we have a fresh agent with no event history and it gets a message from a user.
It would "wake up" with:


### Turn 1

**Claude sees** (rendered as the user message in the API call):

```xml
<deployment_context>
Home automation system for 123 Oak Street.
Cameras: driveway, front_door, backyard.
Users: steve@example.com (owner).
Tools: camera_tool, home (see system prompt for API details).
</deployment_context>
<event_history>
</event_history>
<work_queue>
<work_item id="1f13">
priority: 9
time: 2026-02-01 08:35:24 PST
type: UserMessage
chat_id: 81d4
user: steve@example.com
content: "Hello Claude, could you pl" [truncated, 184 chars]
</work_item>
</work_queue>
<context>
Current time: 2026-02-01 08:35:26 PST
Last turn input tokens: 0
Compaction threshold: 150000 tokens
</context>
```

**Claude responds** with a `tool_use` block:

```json
{
  "type": "tool_use",
  "id": "toolu_01A",
  "name": "execute",
  "input": {
    "code": "print(work_queue[0].content)"
  }
}
```

**Harness executes** the Python in a fresh interpreter, capturing stdout:

```
Hello Claude, could you please keep an eye out for
any vehicles coming up the driveway today and let me know if
you see a contractor van?
```

This code + output is recorded as history entry `3a6f`.


### Turn 2

Claude sees the result of the command it just ran:

```xml
<deployment_context>
Home automation system for 123 Oak Street.
Cameras: driveway, front_door, backyard.
Users: steve@example.com (owner).
Tools: camera_tool, home (see system prompt for API details).
</deployment_context>
<event_history>
<entry id="3a6f">
time: 2026-02-01 08:35:27 PST
code:
  print(work_queue[0].content)
output:
  Hello Claude, could you please keep an eye out for
  any vehicles coming up the driveway today and let me know if
  you see a contractor van?
</entry>
</event_history>
<work_queue>
<work_item id="1f13">
priority: 9
time: 2026-02-01 08:35:24 PST
type: UserMessage
chat_id: 81d4
user: steve@example.com
content: "Hello Claude, could you pl" [truncated, 184 chars]
</work_item>
</work_queue>
<context>
Current time: 2026-02-01 08:35:40 PST
Last turn input tokens: 1205
Compaction threshold: 150000 tokens
</context>
```

Claude decides to set up a memory and a timer:

```json
{
  "type": "tool_use",
  "id": "toolu_01B",
  "name": "execute",
  "input": {
    "code": "timer_id = timers.add(\n    every=timedelta(seconds=30),\n    priority=6,\n    description=\"Check driveway camera for contractor vans\"\n)\nmemory[\"watch_driveway\"] = (\n    f\"Alert Steve (steve@example.com, chat 81d4) if contractor van on driveway. \"\n    f\"Timer: {timer_id}. Checking every 30s.\"\n)"
  }
}
```

The harness executes this. `timers.add()` returns harness-assigned ID `982a`.
The memory is stored. Recorded as history entry `e7a1`.


### Turn 3

While Claude was running Turn 2, another message came in from Steve.

```xml
<deployment_context>
Home automation system for 123 Oak Street.
Cameras: driveway, front_door, backyard.
Users: steve@example.com (owner).
Tools: camera_tool, home (see system prompt for API details).
</deployment_context>
<event_history>
<entry id="3a6f">
time: 2026-02-01 08:35:27 PST
code:
  print(work_queue[0].content)
output:
  Hello Claude, could you please keep an eye out for
  any vehicles coming up the driveway today and let me know if
  you see a contractor van?
</entry>
<entry id="e7a1">
time: 2026-02-01 08:35:41 PST
code:
  timer_id = timers.add(
      every=timedelta(seconds=30),
      priority=6,
      description="Check driveway camera for contractor vans"
  )
  memory["watch_driveway"] = (
      f"Alert Steve (steve@example.com, chat 81d4) if contractor van on driveway. "
      f"Timer: {timer_id}. Checking every 30s."
  )
output:
</entry>
</event_history>
<work_queue>
<work_item id="1f13">
priority: 9
time: 2026-02-01 08:35:24 PST
type: UserMessage
chat_id: 81d4
user: steve@example.com
content: "Hello Claude, could you pl" [truncated, 184 chars]
</work_item>
<work_item id="dd31">
priority: 9
time: 2026-02-01 08:35:39 PST
type: UserMessage
chat_id: 81d4
user: steve@example.com
content: "Oh and I forgot to mentio" [truncated, 62 chars]
</work_item>
</work_queue>
<context>
Current time: 2026-02-01 08:35:42 PST
Last turn input tokens: 1580
Compaction threshold: 150000 tokens
</context>
```

Claude pops the first message and reads the second:

```json
{
  "type": "tool_use",
  "id": "toolu_01C",
  "name": "execute",
  "input": {
    "code": "work_queue.pop_front()\nprint(work_queue[0].content)"
  }
}
```

Note that Claude's Python execution context always receives exactly the state that Claude
saw, so it's not accidentally popping a newer higher-priority message that came in.
If any events happen while Claude is working, those events are applied to the state *after*
Claude is done processing it.

Stdout:

```
Oh and I forgot to mention, ignore the red van, that's the electrician
```

Recorded as history entry `4481`.


### Turn 4

```xml
<deployment_context>
Home automation system for 123 Oak Street.
Cameras: driveway, front_door, backyard.
Users: steve@example.com (owner).
Tools: camera_tool, home (see system prompt for API details).
</deployment_context>
<event_history>
<entry id="3a6f">
time: 2026-02-01 08:35:27 PST
code:
  print(work_queue[0].content)
output:
  Hello Claude, could you please keep an eye out for
  any vehicles coming up the driveway today and let me know if
  you see a contractor van?
</entry>
<entry id="e7a1">
time: 2026-02-01 08:35:41 PST
code:
  timer_id = timers.add(
      every=timedelta(seconds=30),
      priority=6,
      description="Check driveway camera for contractor vans"
  )
  memory["watch_driveway"] = (
      f"Alert Steve (steve@example.com, chat 81d4) if contractor van on driveway. "
      f"Timer: {timer_id}. Checking every 30s."
  )
output:
</entry>
<entry id="4481">
time: 2026-02-01 08:35:43 PST
code:
  work_queue.pop_front()
  print(work_queue[0].content)
output:
  Oh and I forgot to mention, ignore the red van, that's the electrician
</entry>
</event_history>
<work_queue>
<work_item id="dd31">
priority: 9
time: 2026-02-01 08:35:39 PST
type: UserMessage
chat_id: 81d4
user: steve@example.com
content: "Oh and I forgot to mentio" [truncated, 62 chars]
</work_item>
</work_queue>
<context>
Current time: 2026-02-01 08:35:44 PST
Last turn input tokens: 1930
Compaction threshold: 150000 tokens
</context>
```

Claude updates its memory and clears the queue:

```json
{
  "type": "tool_use",
  "id": "toolu_01D",
  "name": "execute",
  "input": {
    "code": "memory[\"watch_driveway\"] += \" Ignore the red van (electrician).\"\nwork_queue.pop_front()"
  }
}
```

Because the work queue is now empty, Claude is not invoked until some external event happens.
Recorded as history entry `3881`.


### Turn 5

30 seconds pass. The timer fires, adding a `TimerFired` item to the work queue.

```xml
<deployment_context>
Home automation system for 123 Oak Street.
Cameras: driveway, front_door, backyard.
Users: steve@example.com (owner).
Tools: camera_tool, home (see system prompt for API details).
</deployment_context>
<event_history>
<entry id="3a6f">
time: 2026-02-01 08:35:27 PST
code:
  print(work_queue[0].content)
output:
  Hello Claude, could you please keep an eye out for
  any vehicles coming up the driveway today and let me know if
  you see a contractor van?
</entry>
<entry id="e7a1">
time: 2026-02-01 08:35:41 PST
code:
  timer_id = timers.add(
      every=timedelta(seconds=30),
      priority=6,
      description="Check driveway camera for contractor vans"
  )
  memory["watch_driveway"] = (
      f"Alert Steve (steve@example.com, chat 81d4) if contractor van on driveway. "
      f"Timer: {timer_id}. Checking every 30s."
  )
output:
</entry>
<entry id="4481">
time: 2026-02-01 08:35:43 PST
code:
  work_queue.pop_front()
  print(work_queue[0].content)
output:
  Oh and I forgot to mention, ignore the red van, that's the electrician
</entry>
<entry id="3881">
time: 2026-02-01 08:35:45 PST
code:
  memory["watch_driveway"] += " Ignore the red van (electrician)."
  work_queue.pop_front()
output:
</entry>
</event_history>
<work_queue>
<work_item id="f02c">
priority: 6
time: 2026-02-01 08:36:11 PST
type: TimerFired
every: 30s
timer_id: 982a
description: "Check driveway camera for contractor vans"
</work_item>
</work_queue>
<context>
Current time: 2026-02-01 08:36:12 PST
Last turn input tokens: 2100
Compaction threshold: 150000 tokens
Can modify entries from: 3a6f
</context>
```

Claude checks the camera:

```json
{
  "type": "tool_use",
  "id": "toolu_01E",
  "name": "execute",
  "input": {
    "code": "frames = camera_tool.get_interesting_frames(\n    camera=\"driveway\",\n    max_frames=5,\n    from_time=\"2026-02-01 08:35:41\",\n    to_time=\"2026-02-01 08:36:11\")\nshow_in_context(frames)\nwork_queue.pop_front()"
  }
}
```

This is executed. `camera_tool.get_interesting_frames()` runs synchronously and returns
frame data. `show_in_context(frames)` tells the harness to include the frames in the
next turn's context (as image content blocks in the API call).
Recorded as history entry `d7ea`.


### Turn 6

The camera frames appear in the context. Claude can see the images and reason about them.

```xml
<deployment_context>
Home automation system for 123 Oak Street.
Cameras: driveway, front_door, backyard.
Users: steve@example.com (owner).
Tools: camera_tool, home (see system prompt for API details).
</deployment_context>
<event_history>
<entry id="3a6f">
time: 2026-02-01 08:35:27 PST
code:
  print(work_queue[0].content)
output:
  Hello Claude, could you please keep an eye out for
  any vehicles coming up the driveway today and let me know if
  you see a contractor van?
</entry>
<entry id="e7a1">
time: 2026-02-01 08:35:41 PST
code:
  timer_id = timers.add(
      every=timedelta(seconds=30),
      priority=6,
      description="Check driveway camera for contractor vans"
  )
  memory["watch_driveway"] = (
      f"Alert Steve (steve@example.com, chat 81d4) if contractor van on driveway. "
      f"Timer: {timer_id}. Checking every 30s."
  )
output:
</entry>
<entry id="4481">
time: 2026-02-01 08:35:43 PST
code:
  work_queue.pop_front()
  print(work_queue[0].content)
output:
  Oh and I forgot to mention, ignore the red van, that's the electrician
</entry>
<entry id="3881">
time: 2026-02-01 08:35:45 PST
code:
  memory["watch_driveway"] += " Ignore the red van (electrician)."
  work_queue.pop_front()
output:
</entry>
<entry id="d7ea">
time: 2026-02-01 08:36:13 PST
code:
  frames = camera_tool.get_interesting_frames(
      camera="driveway",
      max_frames=5,
      from_time="2026-02-01 08:35:41",
      to_time="2026-02-01 08:36:11")
  show_in_context(frames)
  work_queue.pop_front()
output:
  1 frame:
  {embedded image}
</entry>
</event_history>
<work_queue>
</work_queue>
<context>
Current time: 2026-02-01 08:36:14 PST
Last turn input tokens: 3500
Compaction threshold: 150000 tokens
Can modify entries from: 3881
</context>
```

Claude sees nothing interesting, replaces the image-heavy history entry with a
short description to save context:

```json
{
  "type": "tool_use",
  "id": "toolu_01F",
  "name": "execute",
  "input": {
    "code": "# Nothing interesting in that frame, just a bird. No contractor yet.\nhistory.replace_with_description(\"d7ea\", \"Checked driveway camera: bird, no vehicles\")"
  }
}
```

Work queue is empty, so the agent sleeps again until the next timer firing.
Recorded as history entry `a3f1`.


### Turn 7: Compaction

To demonstrate how compaction works, we'll trigger a mandatory context compaction.
(In reality this would happen after many more turns, once `input_tokens` exceeds
80% of the compaction threshold. We're triggering it early here to show the mechanics.)

Claude is able to directly manipulate its own context. We require Claude to write
a script which compacts the entire context at once, so we don't have to repeatedly
re-ingest long contexts as Claude does gradual cleanup.

```xml
<deployment_context>
Home automation system for 123 Oak Street.
Cameras: driveway, front_door, backyard.
Users: steve@example.com (owner).
Tools: camera_tool, home (see system prompt for API details).
</deployment_context>
<event_history>
<entry id="3a6f">
time: 2026-02-01 08:35:27 PST
code:
  print(work_queue[0].content)
output:
  Hello Claude, could you please keep an eye out for
  any vehicles coming up the driveway today and let me know if
  you see a contractor van?
</entry>
<entry id="e7a1">
time: 2026-02-01 08:35:41 PST
code:
  timer_id = timers.add(
      every=timedelta(seconds=30),
      priority=6,
      description="Check driveway camera for contractor vans"
  )
  memory["watch_driveway"] = (
      f"Alert Steve (steve@example.com, chat 81d4) if contractor van on driveway. "
      f"Timer: {timer_id}. Checking every 30s."
  )
output:
</entry>
<entry id="4481">
time: 2026-02-01 08:35:43 PST
code:
  work_queue.pop_front()
  print(work_queue[0].content)
output:
  Oh and I forgot to mention, ignore the red van, that's the electrician
</entry>
<entry id="3881">
time: 2026-02-01 08:35:45 PST
code:
  memory["watch_driveway"] += " Ignore the red van (electrician)."
  work_queue.pop_front()
output:
</entry>
<entry id="d7ea">
summary: Checked driveway camera: bird, no vehicles
</entry>
<entry id="a3f1">
time: 2026-02-01 08:36:15 PST
code:
  # Nothing interesting in that frame, just a bird. No contractor yet.
  history.replace_with_description("d7ea", "Checked driveway camera: bird, no vehicles")
output:
</entry>
</event_history>
<work_queue>
<work_item id="373a">
priority: 10
time: 2026-02-01 08:36:20 PST
type: Compaction
description: "You must compact your context."
</work_item>
</work_queue>
<context>
Current time: 2026-02-01 08:36:20 PST
Last turn input tokens: 3800
Compaction threshold: 150000 tokens
COMPACTION MODE:
  Current usage: 3800 tokens
  Target usage: 2000 tokens
  Estimated usage after compaction_script: 3800 tokens
  compaction_script:
    # Empty - build this up, then call compact()
</context>
```

Claude writes a compaction script:

```json
{
  "type": "tool_use",
  "id": "toolu_01G",
  "name": "execute",
  "input": {
    "code": "compaction_script += \"\"\"\nfor id in [\"3a6f\", \"e7a1\", \"4481\", \"3881\", \"d7ea\", \"a3f1\"]:\n    history.remove(id)\nhistory.add(\n    \"Set up driveway cam watch for non-red contractor vans \"\n    \"for steve@example.com (chat 81d4). Timer 982a, memory watch_driveway. \"\n    \"First camera check: just a bird.\")\n\"\"\""
  }
}
```


### Turn 8

The harness has dry-run the compaction script and shows the estimated savings:

```xml
<deployment_context>
Home automation system for 123 Oak Street.
Cameras: driveway, front_door, backyard.
Users: steve@example.com (owner).
Tools: camera_tool, home (see system prompt for API details).
</deployment_context>
<event_history>
<entry id="3a6f">
time: 2026-02-01 08:35:27 PST
code:
  print(work_queue[0].content)
output:
  Hello Claude, could you please keep an eye out for
  any vehicles coming up the driveway today and let me know if
  you see a contractor van?
</entry>
<entry id="e7a1">
time: 2026-02-01 08:35:41 PST
code:
  timer_id = timers.add(
      every=timedelta(seconds=30),
      priority=6,
      description="Check driveway camera for contractor vans"
  )
  memory["watch_driveway"] = (
      f"Alert Steve (steve@example.com, chat 81d4) if contractor van on driveway. "
      f"Timer: {timer_id}. Checking every 30s."
  )
output:
</entry>
<entry id="4481">
time: 2026-02-01 08:35:43 PST
code:
  work_queue.pop_front()
  print(work_queue[0].content)
output:
  Oh and I forgot to mention, ignore the red van, that's the electrician
</entry>
<entry id="3881">
time: 2026-02-01 08:35:45 PST
code:
  memory["watch_driveway"] += " Ignore the red van (electrician)."
  work_queue.pop_front()
output:
</entry>
<entry id="d7ea">
summary: Checked driveway camera: bird, no vehicles
</entry>
<entry id="a3f1">
time: 2026-02-01 08:36:15 PST
code:
  # Nothing interesting in that frame, just a bird. No contractor yet.
  history.replace_with_description("d7ea", "Checked driveway camera: bird, no vehicles")
output:
</entry>
</event_history>
<work_queue>
<work_item id="373a">
priority: 10
time: 2026-02-01 08:36:20 PST
type: Compaction
description: "You must compact your context."
</work_item>
</work_queue>
<context>
Current time: 2026-02-01 08:36:21 PST
Last turn input tokens: 4100
Compaction threshold: 150000 tokens
COMPACTION MODE:
  Current usage: 3800 tokens
  Target usage: 2000 tokens
  Estimated usage after compaction_script: 1200 tokens
  compaction_script:
    for id in ["3a6f", "e7a1", "4481", "3881", "d7ea", "a3f1"]:
        history.remove(id)
    history.add(
        "Set up driveway cam watch for non-red contractor vans "
        "for steve@example.com (chat 81d4). Timer 982a, memory watch_driveway. "
        "First camera check: just a bird.")
</context>
```

The estimated usage (1200) is below the target (2000), so Claude executes:

```json
{
  "type": "tool_use",
  "id": "toolu_01H",
  "name": "execute",
  "input": {
    "code": "compact()"
  }
}
```

The harness runs the compaction script, removes the Compaction work item
automatically, and the agent continues with the reduced context.


### Turn 9

Post-compaction. The old history entries have been replaced with a summary.
The next timer firing wakes the agent up.

```xml
<deployment_context>
Home automation system for 123 Oak Street.
Cameras: driveway, front_door, backyard.
Users: steve@example.com (owner).
Tools: camera_tool, home (see system prompt for API details).
</deployment_context>
<event_history>
<entry id="c4a1">
summary: Set up driveway cam watch for non-red contractor vans for steve@example.com (chat 81d4). Timer 982a, memory watch_driveway. First camera check: just a bird.
</entry>
</event_history>
<work_queue>
<work_item id="ee6f">
priority: 6
time: 2026-02-01 08:36:41 PST
type: TimerFired
every: 30s
timer_id: 982a
description: "Check driveway camera for contractor vans"
</work_item>
</work_queue>
<context>
Current time: 2026-02-01 08:36:41 PST
Last turn input tokens: 1200
Compaction threshold: 150000 tokens
</context>
```

And the cycle continues.


## Implementation Details

### Persistence

The work queue, event history, memory, timers, and process state are persisted
to a SQLite database. If the daemon restarts (crash, reboot, deploy), it loads
state from SQLite and picks up where it left off. Any recurring timers that
should have fired during downtime are caught up on restart.

### Compaction

When the harness detects that `input_tokens` exceeds 80% of the compaction threshold,
it inserts a `Compaction` work item at priority 10 (highest).

During compaction, the context metadata includes additional fields showing the
current `compaction_script` (initially empty), the estimated token savings from
running it, and the target usage. Claude builds up the script incrementally,
checking estimated savings each turn, and calls `compact()` when satisfied.

**Compaction rules:**

- Claude can remove or replace any history entry during compaction
- Memory survives compaction (it's separate from history)
- Timer state survives compaction
- The compaction script runs against the current context state
- If the script crashes, the error is shown and Claude can fix it
- The harness auto-removes the Compaction work item on success
- Compaction is atomic: either the full script succeeds or nothing changes

**Why script-based compaction:** We require Claude to compact everything in a
single script execution rather than doing gradual cleanup, because each change
to older history entries invalidates the prompt cache prefix. By doing it all
at once, we only pay for one cache miss instead of many.

### Ensuring Context Fit

Every history item has a maximum character and line length it will display before truncating.
Claude can extract bits of larger messages across multiple history events.

The work queue will only show a maximum number of items at once. Each item has a maximum
character and line length before truncation of the work queue preview. The first few events
get a longer preview, reducing the probability that the agent has to perform a Python query
to examine the top-priority events.

Because Claude can manipulate the work queue in Python, if something happens resulting in
Claude getting thousands of spam messages, it can figure out the format of the spam from
a few visible examples and write a filter removing the spam from the work queue automatically.

### Queue Sorting

Sorted first by priority (0-10, descending) then by time (ascending).
Priority 10 is reserved for system events.

### Background Processes

```python
pid = shell_exec(
    cmd="long_running_task",
    args=["--flag"],
    alert_timer=timedelta(minutes=5),
    success_prio=5,
    fail_prio=7
)
```

- `alert_timer`: If the process is still running after this duration, a `ProcessTimeout`
  work item is added at the process's `fail_prio`, alerting Claude to check on it.
- When the process completes, a `ProcessCompleted` item is added at `success_prio`.
- When the process fails, a `ProcessFailed` item is added at `fail_prio`.

### No Special "Ask User" Primitive

The harness intentionally does not provide a blocking `ask_user()` function. The agent
can ask questions by sending a message via `send_message()` — the user's reply will arrive
later as a normal `UserMessage` work item. The agent should track what it's waiting for in
memory and continue working on other tasks in the meantime. This keeps the architecture
simple and avoids special-casing human-in-the-loop flows into the harness.

### Concurrent Modifications

Claude's Python execution receives a snapshot of the work queue as it was when
the context was rendered. If new events arrive during Claude's turn, they are
applied to the authoritative state after Claude's modifications are processed.
This prevents Claude from accidentally operating on items it hasn't seen.

### Item IDs

The harness internally uses whatever ID scheme it likes (UUIDs, counters, etc.).
For the agent, all IDs are short hex strings (e.g. `3a6f`, `982a`) assigned by
the harness at creation time. Functions that create new objects (timers.add,
shell_exec, etc.) return the harness-assigned ID.

IDs are stable: once assigned, an ID never changes. IDs are generated from an
internal counter with a bijective shuffle applied, so they appear random to the
agent (preventing off-by-one or sequential substitution errors).

### Error Handling

If Claude's Python script raises an exception, the traceback is recorded in event
history like any other execution output. Claude sees it on the next turn and can
decide how to handle it.

```xml
<entry id="b1c2">
time: 2026-02-01 09:12:03 PST
code:
  prnt(work_queue[0].content)
output: [ERROR]
  Traceback (most recent call last):
    File "<agent>", line 1, in <module>
  NameError: name 'prnt' is not defined
</entry>
```


## Open Design Questions

### Priority change notifications
Should we put an event in the history log when the top priority task changes
out from under Claude? (E.g. a priority-9 user message arrives while Claude is
working on a priority-6 timer check.)

### Interpreter customization
How should Claude be able to customize its interpreter environment?
Options: pip install support, pre-loaded deployment-specific packages,
or a fixed set of standard library only.

### Image handling
How are images (e.g. camera frames from `show_in_context()`) embedded in the
API call? They would be `image` content blocks in the user message, which
costs tokens. Need to decide on resolution limits, max images per turn, etc.


## Implementation

### Source Files

```
claude-server/
  Cargo.toml              -- Dependencies: pyo3, rusqlite, reqwest, tokio, axum, chrono, serde
  build.rs                -- Discovers Python LIBDIR, bakes rpath into binary
  Makefile                -- make (run daemon), make chat (run chat UI), make build
  system_prompt.txt       -- System prompt sent to Claude on every API call
  src/
    main.rs               -- CLI dispatch: default=daemon, chat=web UI
    types.rs              -- Core types (WorkQueue, EventHistory, Timers, Processes, Memory, API types)
    config.rs             -- Configuration from environment variables
    core_loop.rs          -- Main event loop (drain events → render → API → execute → apply)
    python.rs             -- PyO3 executor (#[pyclass] wrappers, SideEffectCollector, stdout capture)
    renderer.rs           -- Serialize HarnessState into XML context text for the API call
    api_client.rs         -- Claude Messages API client (reqwest, retry, tool_use extraction)
    db.rs                 -- SQLite persistence (state JSON blob, process output, messages)
    process.rs            -- Tokio process spawning/monitoring (output capture, completion events)
    compaction.rs         -- Compaction state machine (trigger, script accumulation, execution)
    child_agent.rs        -- Sub-agent loop (simplified core loop, can send msgs, no procs/children)
    http_server.rs        -- Axum HTTP API (POST /message, GET /status, GET /messages/:chat_id, SSE stream)
    chat.rs               -- Chat UI subcommand (serves embedded HTML)
    chat.html             -- Single-file HTML/CSS/JS chat interface
```

### Key Implementation Details

**Side effect collection**: Python scripts don't execute side effects directly.
All mutations (memory writes, timer creates, message sends, process spawns) are
collected into a `SideEffectCollector` during execution. If the script crashes,
nothing is applied. On success, the core loop applies them atomically.

**Synchronous ID assignment**: The `SideEffectCollector` owns the `IdGenerator`
during Python execution. When Claude calls `timers.add()` or `shell_exec()`,
the `#[pyclass]` method calls `id_gen.next()` synchronously and returns the hex
ID string. After execution, the updated generator is moved back into state.

**Fresh Python namespace per turn**: PyO3 initializes the interpreter once at
startup. Each turn creates a fresh `PyDict` as globals/locals. No state leaks
between turns.

**Concurrency**: The core loop owns all mutable state. External events arrive
via tokio mpsc channels. No shared-state concurrency. The Python executor runs
synchronously inside `Python::with_gil`.

### Chat UI

The `chat` subcommand starts a lightweight web server serving an embedded HTML
chat interface:

```bash
claude-server chat                    # default: port 8080, API at localhost:3000
claude-server chat --port 9090        # custom port
claude-server chat --api-url http://myhost:4000  # custom API URL
```

The chat UI:
- Generates a stable UUID chat_id per browser session
- Sends messages via POST /message to the daemon API
- Streams agent responses in real time via SSE (`GET /messages/:chat_id/stream`)
- Shows typing indicators (thinking.../executing...) during agent turns
- Auto-scrolls on new messages

### HTTP API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/message` | POST | Send a user message `{ chat_id?, user, content }` |
| `/status` | GET | Health check `{ status, model }` |
| `/messages/:chat_id` | GET | Get agent responses for a chat `{ messages: [...] }` |
| `/messages/:chat_id/stream` | GET | SSE stream of `message` and `status` events |
| `/shutdown` | POST | Graceful shutdown |

All endpoints have CORS enabled (permissive).

### Environment Variables

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
| `CLAUDE_SERVER_PYTHON_TIMEOUT` | `5` | Python script execution timeout (seconds) |
| `CLAUDE_SERVER_MAX_CHILDREN` | `3` | Max concurrent sub-agent children |

### Building

Requires Rust toolchain and Python 3.13+ with a shared library (`libpython3.13.dylib`).

```bash
cargo build                           # build
cargo test -- --test-threads=1        # run tests (single-threaded for PyO3 GIL)
```

The `build.rs` script auto-discovers the Python library directory and bakes the
rpath into the binary, so no `DYLD_LIBRARY_PATH` is needed at runtime. To
target a specific Python installation:

```bash
PYO3_PYTHON=/path/to/python3 cargo build
```
