# Claude Server

Claude Server is a harness for Claude focused on long-running instances
that need to autonomously manage complex systems.

## Harness Architecture

The Harness is based around the idea of the "Work Queue",
a single priority-based queue of work for Claude to do.

When the Work Queue is empty, the agent sleeps.
When the Work Queue has items in it, the agent is
invoked repeatedly until the Work Queue is empty.

Besides the Work Queue, the agent also sees an Event History,
a chronological history of events related to the agent's work.

The agent has a single API by which to interact with the Work Queue,
the Event History, and the wider world: at every turn, the agent
provides commands which are run inside a python interpreter. All
interaction with users and systems, Work Queue operations,
setting timers, etc. is done via this python interpreter.

The Harness is a Rust program, which invokes a Python interpreter every time the Agent performs an action.

The Python interpreter is either initialized fresh every time the agent does something (and hydrated from serialized data from the Harness)
or kept running in the background, communicating with the Harness via FFI or IPC. We'll start with the simpler
fresh-interpreter-every-agent-action approach.

### The Agent's View Of the World

At every turn, the agent sees a context consisting of

```
{system prompt}
{deployment-specific context}
{event history}
{work queue}
{helpful context}
{agent prompt tag}
```

TODO wyager: What's the best way to "prompt" claude to write Python given this context via the API?
Not sure if we actually need an open tag at the bottom there or if we can just call the API in "give me python"
mode or something.

#### System Prompt

The System Prompt contains general instructions for Claude, including Claude's job
(to manage items in the Work Queue) and details about the harness (what standard functions/objects
are available to it in the pyhton intepreter).

#### Deployment-specific Context

This varies per Claude Server, and contains information about systems, tools, users, etc.
that exist in this particular environment but are not general to all Claude Server instances.

#### Event History

This is a history consisting of:

1. The Python scripts that Claude runs and their outputs
2. Certain system alert messages (e.g. top-priority task changed)
3. Anything Claude chooses to insert during compaction

Claude can trim 

#### Work Queue

This is a priority-ordered queue of tasks for Claude to complete.

This can include:

1. System messages (e.g. context compaction request)
2. User messages (e.g. requests to do something)
3. Async tool usage results (e.g. spawned process completed)
4. Timer Events

#### Helpful Context

A short sequence of information about the current state of the harness

### Example Agent Session

The agent is not invoked until the Work Queue is non-empty. Let's say
we have a fresh agent with no event history and it gets a message from a user. It would
"wake up" with:


#### Turn 1
```
{system prompt}
{deployment-specific context}
<work_queue>
<work_item id=1f13>
priority: 9
time: 2026-02-01 08:35:24 PST
type: UserMessage
chat_id: 81d4
user: steve@example.com
content: "Hello Claude, could you pl" <truncated>
</work_item id=1f13>
</work_queue>
<context>
Current time: 2026-02-01 08:35:26 PST
Unique IDs available for memories, timers, etc.: f73c, 982a, 4d66, 093d
Can delete events starting from: 3a6f
</context>
<agent_commands>
```

At every turn, the agent submits a Python script to be run against the current interpreter context.

In this case, the agent might submit

```python
print(work_queue[0].content)
```

This is evaluated in an interpreter with access to variables representing the task queue,
work history, etc. Claude generally has high flexibility to manipulate the contents of the harness.

TODO wyager: Do we want a fresh interpreter at every step? Or do we want a long-lived REPL?
  Having a fresh interpreter sounds kind of easier for the moment, because we can just JSON-dump
  the relevant stuff from the harness (e.g. timers and work queues), load them in during python setup,
  run Claude's commands, JSON-dump them again at the end, and then load them back into rust. This will
  be slow but it's probably easiest for an MVP.


#### Turn 2

Claude sees the result of the command it just ran:

```
{system prompt}
{deployment-specific context}
<history id=3a6f>
time: 2026-02-01 08:35:27 PST
agent ran:
  print(work_queue[0].content)
output:
  Hello Claude, could you please keep an eye out for
  any vehicles coming up the driveway today and let me know if
  you see a contractor van?
</history id=3a6f>
<work_queue>
<work_item id=1f13>
priority: 9
time: 2026-02-01 08:35:24 PST
type: UserMessage
chat_id: 81d4
user: steve@example.com
content: "Hello Claude, could you pl" <truncated>
</work_item id=1f13>
</work_queue>
<context>
Current time: 2026-02-01 08:35:40 PST
Unique IDs available for memories, timers, etc.: f73c, 982a, 4d66, 093d
Can delete events starting from: 3a6f
</context>
<agent_commands>
```

Claude decides to set up a memory and a timer:

```python
  memory["f73c"] = """
  Need to alert Steve (steve@example.com) if a contractor van comes up
  the driveway on 2026-02-01. chat_id 81d4. Checking recent footage every 30s
  """
  timers.add(
    Timer(
      start=datetime.now(),
      every=timedelta(seconds=30),
      priority=6,
      description="Check driveway camera for contractor vans. See memory f73c",
      id="982a"
    )
  )
```

#### Turn 3

While Claude was writing that script, another message came in from Steve.

```
{system prompt}
{deployment-specific context}
<history id=3a6f>
time: 2026-02-01 08:35:27 PST
agent ran:
  print(work_queue[0].content)
output:
  Hello Claude, could you please keep an eye out for
  any vehicles coming up the driveway today and let me know if
  you see a contractor van?
</history id=3a6f>
<history id=e7a1>
time: 2026-02-01 08:35:29 PST
agent ran:
  memory["f73c"] = """
  Need to alert Steve (steve@example.com) if a contractor van comes up
  the driveway on 2026-02-01. chat_id 81d4. Checking recent footage every 30s
  """
  timers.add(
    Timer(
      start=datetime.now(),
      every=timedelta(seconds=30),
      priority=6,
      description="Check driveway camera for contractor vans. See memory f73c"
    )
  )
output:
</history id=e7a1>
<work_queue>
<work_item id=1f13>
priority: 9
time: 2026-02-01 08:35:24 PST
type: UserMessage
chat_id: 81d4
user: steve@example.com
content: "Hello Claude, could you pl" <truncated>
</work_item id=1f13>
<work_item id=dd31>
priority: 9
time: 2026-02-01 08:35:39 PST
type: UserMessage
chat_id: 81d4
user: steve@example.com
content: "Oh and I forgot to mentio" <truncated>
</work_item id=dd31>
</work_queue>
<context>
Current time: 2026-02-01 08:35:40 PST
Unique IDs available for memories, timers, etc.: 4d66, 093d, 77cf, 8a3b
Can delete events starting from: 3a6f
</context>
<agent_commands>
```

Claude:

```python
work_queue.pop_front()
print(work_queue[0])
```

Note that Claude's python execution context always receives exactly the state that Claude
saw, so it's not accidentally e.g. popping a newer higher-priority message that came in.
If any events happen while Claude is working, those events are applied to the state *after*
Claude is done processing it.


#### Turn 4

```
{system prompt}
{deployment-specific context}
<history id=3a6f>
time: 2026-02-01 08:35:27 PST
agent ran:
  print(work_queue[0].content)
output:
  Hello Claude, could you please keep an eye out for
  any vehicles coming up the driveway today and let me know if
  you see a contractor van?
</history id=3a6f>
<history id=e7a1>
time: 2026-02-01 08:35:29 PST
agent ran:
  memory["f73c"] = """
  Need to alert Steve (steve@example.com) if a contractor van comes up
  the driveway on 2026-02-01. chat_id 81d4. Checking recent footage every 30s.
  """
  timers.add(
    Timer(
      start=datetime.now(),
      every=timedelta(seconds=30),
      priority=6,
      description="Check driveway camera for contractor vans. See memory f73c"
    )
  )
output:
</history id=e7a1>
<history id=4481>
agent ran:
  work_queue.pop_front()
  print(work_queue[0].content)
output:
  Oh and I forgot to mention, ignore the red van, that's the electrician
</history id=4481>
<work_queue>
<work_item id=dd31>
priority: 9
time: 2026-02-01 08:35:39 PST
type: UserMessage
chat_id: 81d4
user: steve@example.com
content: "Oh and I forgot to mentio" <truncated>
</work_item id=dd31>
</work_queue>
<context>
Current time: 2026-02-01 08:35:40 PST
Unique IDs available for memories, timers, etc.: 4d66, 093d, 77cf, 8a3b
Can delete events starting from: 3a6f
</context>
<agent_commands>
```

Claude:

```python
memory["f73c"] += " Ignore the red van (electrician)."
work_queue.pop_front()
```

Because the Work Queue is now empty, Claude is not invoked until some external event happens.

#### Turn 5

```
{system prompt}
{deployment-specific context}
<history id=3a6f>
time: 2026-02-01 08:35:27 PST
agent ran:
  print(work_queue[0].content)
output:
  Hello Claude, could you please keep an eye out for
  any vehicles coming up the driveway today and let me know if
  you see a contractor van?
</history id=3a6f>
<history id=e7a1>
time: 2026-02-01 08:35:29 PST
agent ran:
  memory["f73c"] = """
  Need to alert Steve (steve@example.com) if a contractor van comes up
  the driveway on 2026-02-01. chat_id 81d4. Checking recent footage every 30s.
  """
  timers.add(
    Timer(
      start=datetime.now(),
      every=timedelta(seconds=30),
      priority=6,
      description="Check driveway camera for contractor vans. See memory f73c"
    )
  )
output:
</history id=e7a1>
<history id=4481>
agent ran:
  work_queue.pop_front()
  print(work_queue[0].content)
output:
  Oh and I forgot to mention, ignore the red van, that's the electrician
</history id=4481>
<history id=3881>
agent ran:
  memory["f73c"] += " Ignore the red van (electrician)."
  work_queue.pop_front()
output:
</history id=3881>
<work_queue>
<work_item id=f02c>
priority: 6
time: 2026-02-01 08:35:59 PST
type: TimerFired
every: 30s
timer_id: 982a
description: "Check driveway camera for contractor vans. See memory f73c"
</work_item id=f02c>
</work_queue>
<context>
Current time: 2026-02-01 08:36:00 PST
Unique IDs available for memories, timers, etc.: 4d66, 093d, 77cf, 8a3b
Can delete events starting from: 3a6f
</context>
<agent_commands>
```

Claude:
```python
frames = camera_tool.get_interesting_frames(
  camera="driveway",
  max_frames=5,
  from="2026-02-01 08:35:29",
  to="2026-02-01 08:35:59")
show_in_context(frames)
```



#### Turn 5

```
{system prompt}
{deployment-specific context}
<history id=3a6f>
time: 2026-02-01 08:35:27 PST
agent ran:
  print(work_queue[0].content)
output:
  Hello Claude, could you please keep an eye out for
  any vehicles coming up the driveway today and let me know if
  you see a contractor van?
</history id=3a6f>
<history id=e7a1>
time: 2026-02-01 08:35:29 PST
agent ran:
  memory["f73c"] = """
  Need to alert Steve (steve@example.com) if a contractor van comes up
  the driveway on 2026-02-01. chat_id 81d4. Checking recent footage every 30s.
  """
  timers.add(
    Timer(
      start=datetime.now(),
      every=timedelta(seconds=30),
      priority=6,
      description="Check driveway camera for contractor vans. See memory f73c"
    )
  )
output:
</history id=e7a1>
<history id=4481>
agent ran:
  work_queue.pop_front()
  print(work_queue[0].content)
output:
  Oh and I forgot to mention, ignore the red van, that's the electrician
</history id=4481>
<history id=3881>
agent ran:
  memory["f73c"] += " Ignore the red van (electrician)."
  work_queue.pop_front()
output:
</history id=3881>
<history id=d7ea>
time: 2026-01-01 08:36:00 PST
agent ran:
  frames = camera_tool.get_interesting_frames(
    camera="driveway",
    max_frames=5,
    from="2026-02-01 08:35:29",
    to="2026-02-01 08:35:59")
  show_in_context(frames)
output:
  1 frame:
  {embedded image representation}
</history id=d7ea>
<work_queue>
<work_item id=f02c>
priority: 6
time: 2026-02-01 08:35:59 PST
type: TimerFired
every: 30s
description: "Check driveway camera for contractor vans. See memory f73c"
</work_item id=f02c>
</work_queue>
<context>
Current time: 2026-02-01 08:36:00 PST
Unique IDs available for memories, timers, etc.: 4d66, 093d, 77cf, 8a3b
Can delete events starting from: 3a6f
</context>
<agent_commands>
```

Claude:
```python
# Nothing interesting in that frame, just a bird. No contractor yet.
history.replace_with_description("d7ea", "Got a picture of a bird from driveway camera")
work_queue.pop_front()
```

#### Turn 6

To demonstrate how compaction works in this setting, we'll trigger a mandatory context compaction.

Note that Claude is able to directly manipulate its own context. We require Claude to write a script
which compacts the entire context at once, so we don't have to repeatedly re-ingest long contexts
as Claude does gradual cleanup.

```
{system prompt}
{deployment-specific context}
<history id=3a6f>
time: 2026-02-01 08:35:27 PST
agent ran:
  print(work_queue[0].content)
output:
  Hello Claude, could you please keep an eye out for
  any vehicles coming up the driveway today and let me know if
  you see a contractor van?
</history id=3a6f>
<history id=e7a1>
time: 2026-02-01 08:35:29 PST
agent ran:
  memory["f73c"] = """
  Need to alert Steve (steve@example.com) if a contractor van comes up
  the driveway on 2026-02-01. chat_id 81d4. Checking recent footage every 30s.
  """
  timers.add(
    Timer(
      start=datetime.now(),
      every=timedelta(seconds=30),
      priority=6,
      description="Check driveway camera for contractor vans. See memory f73c"
    )
  )
output:
</history id=e7a1>
<history id=4481>
agent ran:
  work_queue.pop_front()
  print(work_queue[0].content)
output:
  Oh and I forgot to mention, ignore the red van, that's the electrician
</history id=4481>
<history id=3881>
agent ran:
  memory["f73c"] += " Ignore the red van (electrician)."
  work_queue.pop_front()
output:
</history id=3881>
<history id=d7ea>
agent replacement description:
  Got a picture of a bird from driveway camera
</history id=d7ea>
<work_queue>
<work_item id=373a>
priority: 10
time: 2026-02-01 08:36:07 PST
type: Compaction
description: "You must compact your context."
</work_item id=373a>
</work_queue>
<context>
Current time: 2026-02-01 08:36:07 PST
Unique IDs available for memories, timers, etc.: 4d66, 093d, 77cf, 8a3b
Can delete any events
Current context usage: 2000 tokens
Target context usage: 1000 tokens
Context usage after running current compaction_script: 1000 tokens
Agent should modify this `compaction_script` string,
which will be run as a python script against the current context.
When the script is good enough to bring us to or below the Target context usage,
run `compact()`.
Current compaction_script = """
# Modify me
"""
</context>
<agent_commands>
```

Claude:

```python
compaction_script += """
for id in ["3a6f", "e7a1", "4481", "3881"]:
  history.remove(id)
history.add(
  "Compacted info about watching driveway cam for " +
  "non-red contractor vans for steve@example.com. " +
  "On timer 982a, memory f73c")
"""
```


#### Turn 7


```
{system prompt}
{deployment-specific context}
<history id=3a6f>
time: 2026-02-01 08:35:27 PST
agent ran:
  print(work_queue[0].content)
output:
  Hello Claude, could you please keep an eye out for
  any vehicles coming up the driveway today and let me know if
  you see a contractor van?
</history id=3a6f>
<history id=e7a1>
time: 2026-02-01 08:35:29 PST
agent ran:
  memory["f73c"] = """
  Need to alert Steve (steve@example.com) if a contractor van comes up
  the driveway on 2026-02-01. chat_id 81d4. Checking recent footage every 30s.
  """
  timers.add(
    Timer(
      start=datetime.now(),
      every=timedelta(seconds=30),
      priority=6,
      description="Check driveway camera for contractor vans. See memory f73c"
    )
  )
output:
</history id=e7a1>
<history id=4481>
agent ran:
  work_queue.pop_front()
  print(work_queue[0].content)
output:
  Oh and I forgot to mention, ignore the red van, that's the electrician
</history id=4481>
<history id=3881>
agent ran:
  memory["f73c"] += " Ignore the red van (electrician)."
  work_queue.pop_front()
output:
</history id=3881>
<history id=d7ea>
agent replacement description:
  Got a picture of a bird from driveway camera
</history id=d7ea>
<work_queue>
<work_item id=373a>
priority: 10
time: 2026-02-01 08:36:07 PST
type: Compaction
description: "You must compact your context."
</work_item id=373a>
</work_queue>
<context>
Current time: 2026-02-01 08:36:08 PST
Unique IDs available for memories, timers, etc.: 4d66, 093d, 77cf, 8a3b
Can delete any events
Current context usage: 2000 tokens
Target context usage: 1000 tokens
Context usage after running current compaction_script: 300 tokens
Agent should modify this `compaction_script` string,
which will be run as a python script against the current context.
When the script is good enough to bring us to or below the Target context usage,
run `compact()`.
Current compaction_script = """
# Modify me
for id in ["3a6f", "e7a1", "4481", "3881"]:
  history.remove(id)
history.add(
  "Compacted info about watching driveway cam for " +
  "non-red contractor vans for steve@example.com. " +
  "On timer 982a, memory f73c")
"""
</context>
<agent_commands>
```

Claude:

```python
compact()
```

#### Turn 8


```
{system prompt}
{deployment-specific context}
<history id=3a6f>
time: 2026-02-01 08:35:27 PST
agent ran:
  print(work_queue[0].content)
output:
  Hello Claude, could you please keep an eye out for
  any vehicles coming up the driveway today and let me know if
  you see a contractor van?
</history id=3a6f>
<history id=e7a1>
time: 2026-02-01 08:35:29 PST
agent ran:
  memory["f73c"] = """
  Need to alert Steve (steve@example.com) if a contractor van comes up
  the driveway on 2026-02-01. chat_id 81d4. Checking recent footage every 30s.
  """
  timers.add(
    Timer(
      start=datetime.now(),
      every=timedelta(seconds=30),
      priority=6,
      description="Check driveway camera for contractor vans. See memory f73c"
    )
  )
output:
</history id=e7a1>
<history id=4481>
agent ran:
  work_queue.pop_front()
  print(work_queue[0].content)
output:
  Oh and I forgot to mention, ignore the red van, that's the electrician
</history id=4481>
<history id=3881>
agent ran:
  memory["f73c"] += " Ignore the red van (electrician)."
  work_queue.pop_front()
output:
</history id=3881>
<history id=d7ea>
agent replacement description:
  Got a picture of a bird from driveway camera
</history id=d7ea>
<work_queue>
<work_item id=373a>
priority: 10
time: 2026-02-01 08:36:07 PST
type: Compaction
description: "You must compact your context."
</work_item id=373a>
</work_queue>
<context>
Current time: 2026-02-01 08:36:09 PST
Unique IDs available for memories, timers, etc.: 4d66, 093d, 77cf, 8a3b
Can delete any events
Current context usage: 2000 tokens
Target context usage: 1000 tokens
Context usage after running current compaction_script: 300 tokens
Agent should modify this `compaction_script` string,
which will be run as a python script against the current context.
When the script is good enough to bring us to or below the Target context usage,
run `compact()`.
Current compaction_script = """
# Modify me
for id in ["3a6f", "e7a1", "4481", "3881"]:
  history.remove(id)
history.add(
  "Compacted info about watching driveway cam for " +
  "non-red contractor vans for steve@example.com. " +
  "On timer 982a, memory f73c")
"""
</context>
<agent_commands>
```

Claude:

```python
compact()
```


#### Turn 9

Note that the agent didn't remove the compaction item from its work queue. It would have been permitted to do so,
but it neglected to. We're allowed to do our own modifications to the work queue after the agent does its own,
so we can remove it after the compact.

```
{system prompt}
{deployment-specific context}
<history id=3a6f>
agent description:
  Compacted info about watching driveway cam for non-red contractor vans for steve@example.com. On timer 982a, memory f73c
</history id=d7ea>
<work_queue>
<work_item id=ee6f>
priority: 6
time: 2026-02-01 08:36:29 PST
type: TimerFired
every: 30s
description: "Check driveway camera for contractor vans. See memory f73c"
</work_item id=ee6f>
</work_queue>
<context>
Current time: 2026-02-01 08:36:29 PST
Unique IDs available for memories, timers, etc.: 4d66, 093d, 77cf, 8a3b
</context>
<agent_commands>
```

And the cycle continues.


## Implementation details


### Queue Sorting

Queue is sorted first by priority (ranging from 0-10, with 10 reserved for system events like compaction) and then by time (ascending).


### Ensuring Context Fit

Every history item has a maximum character and line length it will display before truncating.
Claude can extract bits of larger messages across multiple history events.

The work queue will only show a maximum number of items at once. Each item has a maximum character and line length before truncation
of the work queue preview. The first few events get a longer preview, reducing the probability that the agent has to perform a python query
to examine the top-priority events.

Because Claude can manipulate the Work Queue in python, if something happens e.g. resulting in Claude getting thousands of spam messages,
it can figure out the format of the spam from a few visible examples and write a filter removing the spam from the work queue automatically.
We can even allow Claude to register e.g. filter regexes agains the work queue that filter out spam before they ever have a chance to displace other
things from the queue.

### Compaction

Claude can delete stuff from its very recent event history, but we don't let it delete substantially older stuff arbitrarily because it would
create a new conversation prefix that wouldn't be cached.

Claude can delete whatever it wants, as far back as it wants, during compaction, but it has to do it all in one shot with a script.
We can run the script against the context and tell Claude "this saved X tokens" or "this crashed" or whatever, because that's cheap,
but we only want to actually re-ingest the trimmed context once.

### Background commands.

Claude has a function `shell_exec(cmd : str, args : list[str], env: dict[str,str], alert_timer: timedelta, success_prio : Priority, fail_prio : Priority) -> pid`.

It can use this to kick off background tasks. It has to specify an alert_timer duration, where if the command is still running
after that duration, the timer fires, alerting claude to check the command. If the command finishes or fails, an alert gets placed in the
work queue at the specified `success_prio` or `fail_prio`.

Claude has various functions to inspect and modify processes using their `pid`.

### Item IDs

In our Rust harness, we can identify items (e.g. history events) however we like (UUID, ordinal index, whatever).

However, for the agent, we always give it short-form IDs that it can easily write out. This mapping should be stable such that
when we re-render the context every time we invoke the agent, the generated prompt prefixes are stable. In fact, we should probably
never change event IDs so that if the agent references an event ID in a text document or something (outside of the management of the harness),
that reference is stable. We probably don't want to expose an incrementing counter to the agent, because that makes substitution errors too easy,
but maybe we generate 40-bit bitcoin-style word-based IDs or something, so the agent sees IDs like "moon-banana-rock-super", with some
kind of bijective shuffle applied to the underlying integer counter or something, so the names are random-looking to the agent.

TODO: get rid of the `Unique IDs available` thing and replace it with a `gen_unique_id()` function.

### Timers

Claude can interact with timers via a `timers` object available in its interpreter.

## Questions

### Should we put an event in the history log when e.g. the top priority task changes out from under Claude?

### How do we want Claude to be able to customize its interpreter environment? Pip commands?
