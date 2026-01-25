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
time: 2026-02-01 08:25 PST
type: UserMessage
chat_id: 81d4
user: steve@example.com
content: "Hello Claude, could you pl" <truncated>
</work_item id=1f13>
</work_queue>
<context>
Current time: 2026-02-01 08:35:40 PST
Unique IDs available for memories: 4d66, 093d, 77cf, 8a3b
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
time: 2026-02-01 08:25 PST
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
Unique IDs available for memories: 4d66, 093d, 77cf, 8a3b
Can delete events starting from: 3a6f
</context>
<agent_commands>
```

At every turn, the agent outputs 
