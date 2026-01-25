use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use tokio::io::AsyncBufReadExt;
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::sync::Mutex;

use crate::db::Database;
use crate::python::ProcessStartRequest;
use crate::types::*;

/// Events sent from process supervisor to the core loop.
#[derive(Debug)]
pub enum ProcessEvent {
    Completed { pid: AgentId, exit_code: i32 },
    Failed { pid: AgentId, error: String },
    Timeout { pid: AgentId },
}

struct RunningProcess {
    os_pid: u32,
    kill_handle: tokio::process::Child,
}

pub struct ProcessSupervisor {
    event_tx: mpsc::UnboundedSender<ProcessEvent>,
    db: Arc<Database>,
    running: Arc<Mutex<HashMap<String, u32>>>, // agent_id -> os_pid
}

impl ProcessSupervisor {
    pub fn new(event_tx: mpsc::UnboundedSender<ProcessEvent>, db: Arc<Database>) -> Self {
        Self {
            event_tx,
            db,
            running: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn spawn(&self, request: ProcessStartRequest) -> Result<()> {
        let mut cmd = Command::new(&request.cmd);
        cmd.args(&request.args);
        for (k, v) in &request.env {
            cmd.env(k, v);
        }
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());

        let mut child = cmd.spawn()?;
        let os_pid = child.id().unwrap_or(0);
        let pid = request.id.clone();
        let pid_str = pid.0.clone();

        // Track running process
        {
            let running = self.running.clone();
            let pid_str = pid_str.clone();
            tokio::spawn(async move {
                running.lock().await.insert(pid_str, os_pid);
            });
        }

        // Spawn stdout/stderr reader
        let db = self.db.clone();
        let stdout = child.stdout.take();
        let stderr = child.stderr.take();
        let reader_pid = pid_str.clone();
        tokio::spawn(async move {
            if let Some(stdout) = stdout {
                let reader = tokio::io::BufReader::new(stdout);
                let mut lines = reader.lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    let _ = db.append_process_output(&reader_pid, &format!("{}\n", line));
                }
            }
            if let Some(stderr) = stderr {
                let reader = tokio::io::BufReader::new(stderr);
                let mut lines = reader.lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    let _ = db.append_process_output(&reader_pid, &format!("[stderr] {}\n", line));
                }
            }
        });

        // Spawn completion monitor
        let event_tx = self.event_tx.clone();
        let running = self.running.clone();
        let completion_pid = pid.clone();
        let completion_pid_str = pid_str.clone();
        tokio::spawn(async move {
            match child.wait().await {
                Ok(status) => {
                    running.lock().await.remove(&completion_pid_str);
                    if status.success() {
                        let _ = event_tx.send(ProcessEvent::Completed {
                            pid: completion_pid,
                            exit_code: status.code().unwrap_or(0),
                        });
                    } else {
                        let _ = event_tx.send(ProcessEvent::Failed {
                            pid: completion_pid,
                            error: format!("exit code {}", status.code().unwrap_or(-1)),
                        });
                    }
                }
                Err(e) => {
                    running.lock().await.remove(&completion_pid_str);
                    let _ = event_tx.send(ProcessEvent::Failed {
                        pid: completion_pid,
                        error: format!("wait error: {}", e),
                    });
                }
            }
        });

        // Spawn alert timer
        let alert_secs = request.alert_timer_secs;
        let timeout_pid = pid.clone();
        let timeout_pid_str = pid_str;
        let event_tx = self.event_tx.clone();
        let running = self.running.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(alert_secs)).await;
            // Only send timeout if still running
            if running.lock().await.contains_key(&timeout_pid_str) {
                let _ = event_tx.send(ProcessEvent::Timeout {
                    pid: timeout_pid,
                });
            }
        });

        Ok(())
    }

    pub async fn kill(&self, pid: &str) -> Result<()> {
        let running = self.running.lock().await;
        if let Some(&os_pid) = running.get(pid) {
            // Use kill command to send SIGTERM
            let _ = Command::new("kill")
                .arg(os_pid.to_string())
                .status()
                .await;
        }
        Ok(())
    }
}
