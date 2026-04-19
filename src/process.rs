use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use tokio::io::AsyncBufReadExt;
use tokio::process::Command;
use tokio::sync::{mpsc, oneshot, Mutex};

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

pub struct ProcessSupervisor {
    event_tx: mpsc::UnboundedSender<ProcessEvent>,
    db: Arc<Database>,
    running: Arc<Mutex<HashMap<String, u32>>>, // agent_id -> os_pid
    /// URL of the /event endpoint. Injected as CLAUDE_SERVER_EVENT_URL into
    /// every spawned process's environment so watcher scripts can POST events
    /// back to the agent without hardcoding the listen address.
    event_url: String,
    /// Owning agent's name. Injected as CLAUDE_SERVER_AGENT_NAME so spawned
    /// subcommands (e.g. `feedback`) can auto-tag which agent invoked them.
    agent_name: String,
    /// Stdin channels for interactive processes. Sending None closes the pipe.
    stdins: Arc<Mutex<HashMap<String, mpsc::UnboundedSender<Option<Vec<u8>>>>>>,
}

impl ProcessSupervisor {
    pub fn new(
        event_tx: mpsc::UnboundedSender<ProcessEvent>,
        db: Arc<Database>,
        event_url: String,
        agent_name: String,
    ) -> Self {
        Self {
            event_tx,
            db,
            running: Arc::new(Mutex::new(HashMap::new())),
            event_url,
            agent_name,
            stdins: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Queue bytes to an interactive process's stdin. No-op if the process
    /// wasn't spawned with interactive=true or has already exited.
    pub async fn send_stdin(&self, pid: &str, data: Vec<u8>) {
        if let Some(tx) = self.stdins.lock().await.get(pid) {
            let _ = tx.send(Some(data));
        }
    }

    /// Close an interactive process's stdin (sends EOF).
    pub async fn close_stdin(&self, pid: &str) {
        if let Some(tx) = self.stdins.lock().await.remove(pid) {
            let _ = tx.send(None);
        }
    }

    /// Spawn a process. Returns a oneshot receiver if block_for_ms is set,
    /// which resolves when the process completes (after all output is flushed).
    pub fn spawn(
        &self,
        request: ProcessStartRequest,
    ) -> Result<Option<oneshot::Receiver<()>>> {
        let mut cmd = Command::new(&request.cmd);
        cmd.args(&request.args);
        // Auto-inject event URL first so agent-supplied env can override it.
        cmd.env("CLAUDE_SERVER_EVENT_URL", &self.event_url);
        cmd.env("CLAUDE_SERVER_AGENT_NAME", &self.agent_name);
        for (k, v) in &request.env {
            cmd.env(k, v);
        }
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());
        cmd.stdin(if request.interactive {
            std::process::Stdio::piped()
        } else {
            std::process::Stdio::null()
        });
        // Kill the child if the daemon exits. Long-running watchers would
        // otherwise orphan and keep POSTing to a dead endpoint.
        cmd.kill_on_drop(true);

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

        // For interactive processes, spawn a stdin writer task fed by an
        // unbounded channel. send_stdin() queues bytes; None closes the pipe.
        if request.interactive {
            if let Some(mut stdin) = child.stdin.take() {
                let (tx, mut rx) = mpsc::unbounded_channel::<Option<Vec<u8>>>();
                let stdins = self.stdins.clone();
                let pid = pid_str.clone();
                tokio::spawn(async move {
                    stdins.lock().await.insert(pid.clone(), tx);
                    while let Some(Some(bytes)) = rx.recv().await {
                        use tokio::io::AsyncWriteExt;
                        if stdin.write_all(&bytes).await.is_err()
                            || stdin.flush().await.is_err()
                        {
                            break;
                        }
                    }
                    drop(stdin); // closes the pipe → child sees EOF
                    stdins.lock().await.remove(&pid);
                });
            }
        }

        // Spawn output reader — capture its JoinHandle so the completion
        // monitor can wait for all output to be flushed before signaling
        let db = self.db.clone();
        let stdout = child.stdout.take();
        let stderr = child.stderr.take();
        let reader_pid = pid_str.clone();
        let reader_handle = tokio::spawn(async move {
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

        // Create oneshot channel if block_for is requested
        let (block_tx, block_rx) = if request.block_for_ms.is_some() {
            let (tx, rx) = oneshot::channel();
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };

        // Spawn completion monitor — waits for process exit AND output flush
        let event_tx = self.event_tx.clone();
        let running = self.running.clone();
        let completion_pid = pid.clone();
        let completion_pid_str = pid_str.clone();
        tokio::spawn(async move {
            let event = match child.wait().await {
                Ok(status) => {
                    // Wait for reader to finish flushing all output before signaling
                    let _ = reader_handle.await;
                    running.lock().await.remove(&completion_pid_str);

                    if status.success() {
                        ProcessEvent::Completed {
                            pid: completion_pid,
                            exit_code: status.code().unwrap_or(0),
                        }
                    } else {
                        ProcessEvent::Failed {
                            pid: completion_pid,
                            error: format!("exit code {}", status.code().unwrap_or(-1)),
                        }
                    }
                }
                Err(e) => {
                    let _ = reader_handle.await;
                    running.lock().await.remove(&completion_pid_str);
                    ProcessEvent::Failed {
                        pid: completion_pid,
                        error: format!("wait error: {}", e),
                    }
                }
            };

            // Send on oneshot first (for block_for callers), then normal channel
            if let Some(tx) = block_tx {
                // We need to clone-ish the event for the oneshot.
                // Since ProcessEvent isn't Clone, send on oneshot and reconstruct for the channel.
                // Actually, let's just send on the normal channel — the block_for caller
                // will receive it via drain_events on the next loop iteration.
                // The oneshot just needs to signal "done" so the caller stops waiting.
                let _ = tx.send(());
            }
            let _ = event_tx.send(event);
        });

        // Spawn alert timer
        let alert_secs = request.alert_timer_secs;
        let timeout_pid = pid.clone();
        let timeout_pid_str = pid_str;
        let event_tx = self.event_tx.clone();
        let running = self.running.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(alert_secs)).await;
            if running.lock().await.contains_key(&timeout_pid_str) {
                let _ = event_tx.send(ProcessEvent::Timeout {
                    pid: timeout_pid,
                });
            }
        });

        Ok(block_rx)
    }

    pub async fn kill(&self, pid: &str) -> Result<()> {
        let running = self.running.lock().await;
        if let Some(&os_pid) = running.get(pid) {
            let _ = Command::new("kill")
                .arg(os_pid.to_string())
                .status()
                .await;
        }
        Ok(())
    }
}
