## Architecture Preferences

- Avoid polling architectures. Prefer blocking/event-driven approaches: use select/race patterns, sleep-until-deadline, channels, etc. rather than periodic polling loops.
