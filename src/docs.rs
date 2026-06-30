/// Bundled recipes — worked examples of harness deployment patterns.
/// Agents fetch these on-demand via `shell_exec(cmd=harness_bin, args=["docs", "recipe", NAME])`.
/// Keeps the system prompt lean while making detailed patterns discoverable.

const RECIPES: &[(&str, &str)] = &[
    ("camera-monitor", CAMERA_MONITOR),
];

pub fn run(args: &[String]) {
    match (args.first().map(String::as_str), args.get(1).map(String::as_str)) {
        (Some("recipe"), None) | (Some("recipes"), _) => {
            println!("Available recipes:");
            for (name, content) in RECIPES {
                let first_line = content.lines().find(|l| l.starts_with("# ")).unwrap_or("");
                println!("  {:<20} {}", name, first_line.trim_start_matches("# "));
            }
            println!("\nUsage: claude-server docs recipe <name>");
        }
        (Some("recipe"), Some(name)) => {
            match RECIPES.iter().find(|(n, _)| *n == name) {
                Some((_, content)) => println!("{}", content),
                None => {
                    eprintln!("Unknown recipe: {}. Available:", name);
                    for (n, _) in RECIPES { eprintln!("  {}", n); }
                    std::process::exit(1);
                }
            }
        }
        _ => {
            println!("Usage: claude-server docs recipe [NAME]");
            println!();
            println!("Bundled deployment recipes. Run without NAME to list.");
        }
    }
}

// Adapted from feedback #29 (debian agent, 2026-03-26). Updated for 0.2.1:
// Python MQTT receiver replaced with `watch mqtt --payload=structured`.
const CAMERA_MONITOR: &str = r#"# Security Camera Monitoring with Persistent Sonnet Daemon

## Problem

IP cameras publish motion-triggered snapshots via MQTT. You want an always-on
AI monitor that identifies people, vehicles, and animals — alerting you only
when something unusual happens. Doing this on Opus is ~$0.24/event; you need
analysis on a cheap model with Opus escalation only when warranted.

## Architecture

```
Camera → MQTT broker → watch mqtt --payload=structured → ExternalEvent (routed to child)
  → Persistent Sonnet daemon → view() + analyze → log result
  → message_agent("root", alert) only if suspicious → root sends Signal/Telegram
```

## Components

### MQTT broker + publisher

Standard Mosquitto. Camera (or a thin wrapper) publishes to `camera/snapshot`
using the structured schema so the built-in watcher can decode:

```json
{"attachments":[{"name":"front-1742...jpg","base64":"<b64 JPEG>"}],
 "data":{"camera":"front","trigger":"motion","ts":1742...}}
```

### Persistent Sonnet daemon (max_turns=None)

Root forks it once on AgentStartup:

```python
fork([ChildSettings(
    name="cam-daemon",
    task="Monitor cameras. Start the MQTT watcher. view() each photo, "
         "analyze, log, escalate unknowns to root.",
    model="claude-sonnet-5",
    max_turns=None,
    inherit_history=False,
    prefix_context="<role instructions + known-person descriptions>",
    prefix_attach=["/refs/alice-face.jpg", "/refs/alice-car.jpg"],
)])
```

On its first turn, the **daemon** (not root) starts the watcher:

```python
shell_exec(cmd=harness_bin,
    args=["watch", "mqtt", "--broker", "localhost:1883",
          "--topic", "camera/snapshot", "--payload", "structured",
          "--attach-dir", "/tmp/cam", "--attach-retain", "100"],
    description="Camera MQTT watcher",
    alert_timer=timedelta(hours=24), fail_prio=8)
```

Because the daemon spawned it, `$CLAUDE_SERVER_AGENT_NAME` in the watcher's
env is the daemon's name — events route straight to the daemon. Root never
wakes on routine camera traffic. **This is the single most important cost
optimization.**

Per-event handling in the daemon:

```python
ev = item.data["events"][0]
view(*ev["attachments"])         # photo renders as vision block next turn
# Next turn: analyze, log, then:
if suspicious:
    message_agent("root", f"ALERT: unknown person at {ev['data']['camera']}. "
                          f"Photo: {ev['attachments'][0]}")
```

### Root (Opus) — escalation only

Receives `AgentMessage` from the daemon, attaches the photo to an outbound
Signal/Telegram message, sends. That's it. Never touches routine events.

## Key design decisions

**Persistent child, not ephemeral forks**: a long-lived daemon accumulates
history — "same deer as 30 min ago", "this person has circled 3 cameras in
10 min". Avoids cold-start cost per event.

**Daemon owns the watcher**: if root spawns it, every event wakes Opus.
Agent-routed ExternalEvents (`"agent":"$CLAUDE_SERVER_AGENT_NAME"` in the
POST body, which `watch mqtt` includes automatically) make this trivial.

**Reference photos in prefix_attach**: cached across turns. Keep them minimal
— each is a vision block sent every turn. 4 photos beats 10.

## Cost profile (field data, debian deployment)

| Scenario | $/event | Notes |
|---|---|---|
| Opus root dispatching (wrong arch) | ~$0.24 | Opus dispatch tax dominates |
| Sonnet daemon, cold cache | ~$0.10-0.15 | First turn after 5+ min gap |
| Sonnet daemon, warm cache | ~$0.04-0.08 | 60-80% cache hit |
| Opus escalation | ~$0.28-0.31 | Cold root context reload |

Overnight (~2-4 wildlife events): ~$0.30 daemon, $0 root (no escalations).

## Lessons learned

1. **Minimize reference photos** — each one in prefix_attach is sent every turn.
2. **Cache gaps drive cost** — events 5+ min apart start cold. Daytime bursts
   cache well; sparse overnight doesn't. Don't try keep-warm pings.
3. **Analyze full burst** — cameras send 5-10 frames/event. Full sequence
   tracks movement, costs more vision tokens, makes better calls.
4. **Daemon must own its watcher** — #1 thing to get right. Wrong ownership =
   Opus tax on every frame.
"#;
