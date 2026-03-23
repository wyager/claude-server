use anyhow::{Context, Result};
use rusqlite::Connection;
use std::path::Path;
use std::sync::Mutex;

use crate::types::HarnessState;

pub struct Database {
    conn: Mutex<Connection>,
    path: std::path::PathBuf,
}

impl Database {
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path)
            .with_context(|| format!("Failed to open database at {:?}", path))?;

        // Enable WAL mode for better concurrent read performance
        conn.pragma_update(None, "journal_mode", "WAL")?;

        let db = Self {
            conn: Mutex::new(conn),
            path: path.to_path_buf(),
        };
        db.init_schema()?;
        Ok(db)
    }

    fn init_schema(&self) -> Result<()> {
        self.conn.lock().unwrap().execute_batch(
            "
            CREATE TABLE IF NOT EXISTS harness_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                state_json TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS process_output (
                pid TEXT PRIMARY KEY,
                output TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS outbound_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id TEXT NOT NULL,
                content TEXT NOT NULL,
                attachments TEXT NOT NULL DEFAULT '[]',
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_outbound_messages_chat
                ON outbound_messages(chat_id);

            CREATE TABLE IF NOT EXISTS event_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL,
                data_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS pinned_memory (
                key TEXT PRIMARY KEY,
                content TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
            ",
        )?;
        // Idempotent schema migrations for existing DBs. ALTER TABLE fails
        // with "duplicate column" on DBs that already have it — that's fine.
        let conn = self.conn.lock().unwrap();
        let _ = conn.execute(
            "ALTER TABLE outbound_messages ADD COLUMN attachments TEXT NOT NULL DEFAULT '[]'",
            [],
        );

        // Hard-fail if the schema is still wrong after migration. Better to
        // blow up here than silently drop messages at runtime.
        conn.prepare(
            "SELECT id, chat_id, content, attachments, created_at \
             FROM outbound_messages LIMIT 0",
        )
        .map_err(|e| anyhow::anyhow!(
            "DB schema check failed — delete {} and restart: {}",
            self.path.display(), e
        ))?;

        Ok(())
    }

    // ---- Harness State ----

    pub fn save_state(&self, state: &HarnessState) -> Result<()> {
        let json = serde_json::to_string(state)?;
        self.conn.lock().unwrap().execute(
            "INSERT INTO harness_state (id, state_json, updated_at)
             VALUES (1, ?1, datetime('now'))
             ON CONFLICT(id) DO UPDATE SET state_json = ?1, updated_at = datetime('now')",
            [&json],
        )?;
        Ok(())
    }

    pub fn load_state(&self) -> Result<Option<HarnessState>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT state_json FROM harness_state WHERE id = 1")?;
        let mut rows = stmt.query([])?;
        match rows.next()? {
            Some(row) => {
                let json: String = row.get(0)?;
                let state: HarnessState = serde_json::from_str(&json)?;
                Ok(Some(state))
            }
            None => Ok(None),
        }
    }

    // ---- Process Output ----

    pub fn append_process_output(&self, pid: &str, chunk: &str) -> Result<()> {
        self.conn.lock().unwrap().execute(
            "INSERT INTO process_output (pid, output, updated_at)
             VALUES (?1, ?2, datetime('now'))
             ON CONFLICT(pid) DO UPDATE SET
                output = output || ?2,
                updated_at = datetime('now')",
            rusqlite::params![pid, chunk],
        )?;
        Ok(())
    }

    pub fn load_process_output(&self, pid: &str) -> Result<String> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT output FROM process_output WHERE pid = ?1")?;
        let mut rows = stmt.query([pid])?;
        match rows.next()? {
            Some(row) => Ok(row.get(0)?),
            None => Ok(String::new()),
        }
    }

    /// Load outputs for multiple processes at once (for Python executor).
    pub fn load_all_process_outputs(&self) -> Result<std::collections::HashMap<String, String>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT pid, output FROM process_output")?;
        let mut map = std::collections::HashMap::new();
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            let pid: String = row.get(0)?;
            let output: String = row.get(1)?;
            map.insert(pid, output);
        }
        Ok(map)
    }

    // ---- Outbound Messages ----

    pub fn save_outbound_message(&self, chat_id: &str, content: &str, attachments: &[String]) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        let att = serde_json::to_string(attachments)?;
        conn.execute(
            "INSERT INTO outbound_messages (chat_id, content, attachments) VALUES (?1, ?2, ?3)",
            rusqlite::params![chat_id, content, att],
        )?;
        Ok(conn.last_insert_rowid())
    }

    pub fn load_outbound_messages(&self, chat_id: &str) -> Result<Vec<OutboundMessage>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, chat_id, content, attachments, created_at
             FROM outbound_messages WHERE chat_id = ?1
             ORDER BY created_at ASC",
        )?;
        let mut messages = Vec::new();
        let mut rows = stmt.query([chat_id])?;
        while let Some(row) = rows.next()? {
            let att_json: String = row.get(3)?;
            messages.push(OutboundMessage {
                id: row.get(0)?,
                chat_id: row.get(1)?,
                content: row.get(2)?,
                attachments: serde_json::from_str(&att_json).unwrap_or_default(),
                created_at: row.get(4)?,
            });
        }
        Ok(messages)
    }


    // ---- Pinned Memory ----

    /// Load all pinned memory entries, sorted by key.
    pub fn load_pinned(&self) -> Result<Vec<(String, String)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT key, content FROM pinned_memory ORDER BY key")?;
        let mut entries = Vec::new();
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            entries.push((row.get(0)?, row.get(1)?));
        }
        Ok(entries)
    }

    /// Pin a key, overwriting any existing value.
    pub fn save_pin(&self, key: &str, content: &str) -> Result<()> {
        self.conn.lock().unwrap().execute(
            "INSERT INTO pinned_memory (key, content, updated_at)
             VALUES (?1, ?2, datetime('now'))
             ON CONFLICT(key) DO UPDATE SET content = ?2, updated_at = datetime('now')",
            rusqlite::params![key, content],
        )?;
        Ok(())
    }

    /// Unpin a key.
    pub fn delete_pin(&self, key: &str) -> Result<()> {
        self.conn.lock().unwrap().execute(
            "DELETE FROM pinned_memory WHERE key = ?1",
            [key],
        )?;
        Ok(())
    }
}

#[derive(Debug, serde::Serialize)]
pub struct OutboundMessage {
    pub id: i64,
    pub chat_id: String,
    pub content: String,
    pub attachments: Vec<String>,
    pub created_at: String,
}
