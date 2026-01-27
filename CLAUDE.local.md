## Architecture Preferences

- Avoid polling architectures. Prefer blocking/event-driven approaches: use select/race patterns, sleep-until-deadline, channels, etc. rather than periodic polling loops.
- Never use sleeps to "mitigate" race conditions. A sleep is not a fix for a race condition — always use proper synchronization (channels, join handles, mutexes, condition variables, etc.).
