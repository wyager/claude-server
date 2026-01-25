use anyhow::{Context, Result};
use rusqlite::Connection;
use std::path::Path;

use crate::types::HarnessState;

pub struct Database {
    conn: Connection,
}

impl Database {
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path)
            .with_context(|| format!("Failed to open database at {:?}", path))?;

        // Enable WAL mode for better concurrent read performance
        conn.pragma_update(None, "journal_mode", "WAL")?;

        let db = Self { conn };
        db.init_schema()?;
        Ok(db)
    }

    fn init_schema(&self) -> Result<()> {
        self.conn.execute_batch(
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
            ",
        )?;
        Ok(())
    }

    // ---- Harness State ----

    pub fn save_state(&self, state: &HarnessState) -> Result<()> {
        let json = serde_json::to_string(state)?;
        self.conn.execute(
            "INSERT INTO harness_state (id, state_json, updated_at)
             VALUES (1, ?1, datetime('now'))
             ON CONFLICT(id) DO UPDATE SET state_json = ?1, updated_at = datetime('now')",
            [&json],
        )?;
        Ok(())
    }

    pub fn load_state(&self) -> Result<Option<HarnessState>> {
        let mut stmt = self
            .conn
            .prepare("SELECT state_json FROM harness_state WHERE id = 1")?;
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
        self.conn.execute(
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
        let mut stmt = self
            .conn
            .prepare("SELECT output FROM process_output WHERE pid = ?1")?;
        let mut rows = stmt.query([pid])?;
        match rows.next()? {
            Some(row) => Ok(row.get(0)?),
            None => Ok(String::new()),
        }
    }

    /// Load outputs for multiple processes at once (for Python executor).
    pub fn load_all_process_outputs(&self) -> Result<std::collections::HashMap<String, String>> {
        let mut stmt = self.conn.prepare("SELECT pid, output FROM process_output")?;
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

    pub fn save_outbound_message(&self, chat_id: &str, content: &str) -> Result<()> {
        self.conn.execute(
            "INSERT INTO outbound_messages (chat_id, content) VALUES (?1, ?2)",
            rusqlite::params![chat_id, content],
        )?;
        Ok(())
    }

    pub fn load_outbound_messages(&self, chat_id: &str) -> Result<Vec<OutboundMessage>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, chat_id, content, created_at
             FROM outbound_messages WHERE chat_id = ?1
             ORDER BY created_at ASC",
        )?;
        let mut messages = Vec::new();
        let mut rows = stmt.query([chat_id])?;
        while let Some(row) = rows.next()? {
            messages.push(OutboundMessage {
                id: row.get(0)?,
                chat_id: row.get(1)?,
                content: row.get(2)?,
                created_at: row.get(3)?,
            });
        }
        Ok(messages)
    }

    // ---- Event Log ----

    pub fn log_event(&self, event_type: &str, data: &serde_json::Value) -> Result<()> {
        let json = serde_json::to_string(data)?;
        self.conn.execute(
            "INSERT INTO event_log (event_type, data_json) VALUES (?1, ?2)",
            rusqlite::params![event_type, json],
        )?;
        Ok(())
    }
}

#[derive(Debug, serde::Serialize)]
pub struct OutboundMessage {
    pub id: i64,
    pub chat_id: String,
    pub content: String,
    pub created_at: String,
}
