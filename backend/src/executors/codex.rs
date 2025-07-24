use async_trait::async_trait;
use serde_json::Value;
use uuid::Uuid;

use crate::{
    command_runner::{CommandProcess, CommandRunner},
    executor::{
        ActionType, Executor, ExecutorError, NormalizedConversation, NormalizedEntry,
        NormalizedEntryType,
    },
    models::task::Task,
    utils::shell::get_shell_command,
};







/// An executor that uses Codex CLI to process tasks
pub struct CodexExecutor {
    executor_type: String,
    command: String,
}

impl Default for CodexExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl CodexExecutor {
    /// Create a new CodexExecutor with default settings
    pub fn new() -> Self {
        Self {
            executor_type: "Codex".to_string(),
            command: "npx @openai/codex exec --dangerously-bypass-approvals-and-sandbox --skip-git-repo-check".to_string(),
        }
    }


}

#[async_trait]
impl Executor for CodexExecutor {
    async fn spawn(
        &self,
        pool: &sqlx::SqlitePool,
        task_id: Uuid,
        worktree_path: &str,
    ) -> Result<CommandProcess, ExecutorError> {
        // Get the task to fetch its description
        let task = Task::find_by_id(pool, task_id)
            .await?
            .ok_or(ExecutorError::TaskNotFound)?;

        let prompt = if let Some(task_description) = task.description {
            format!(
                r#"project_id: {}
            
Task title: {}
Task description: {}"#,
                task.project_id, task.title, task_description
            )
        } else {
            format!(
                r#"project_id: {}
            
Task title: {}"#,
                task.project_id, task.title
            )
        };

        // Use shell command for cross-platform compatibility
        let (shell_cmd, shell_arg) = get_shell_command();
        let codex_command = &self.command;

        let mut command = CommandRunner::new();
        command
            .command(shell_cmd)
            .arg(shell_arg)
            .arg(codex_command)
            .stdin(&prompt)
            .working_dir(worktree_path)
            .env("NODE_NO_WARNINGS", "1")
            .env("RUST_LOG", "info");

        let proc = command.start().await.map_err(|e| {
            crate::executor::SpawnContext::from_command(&command, &self.executor_type)
                .with_task(task_id, Some(task.title.clone()))
                .with_context(format!("{} CLI execution for new task", self.executor_type))
                .spawn_error(e)
        })?;
        Ok(proc)
    }

    async fn spawn_followup(
        &self,
        _pool: &sqlx::SqlitePool,
        _task_id: Uuid,
        session_id: &str,
        prompt: &str,
        worktree_path: &str,
    ) -> Result<CommandProcess, ExecutorError> {
        // For now, just use the same command as spawn since followup functionality is not fully implemented
        let codex_command = &self.command;

        // Use shell command for cross-platform compatibility
        let (shell_cmd, shell_arg) = get_shell_command();

        let mut command = CommandRunner::new();
        command
            .command(shell_cmd)
            .arg(shell_arg)
            .arg(codex_command)
            .stdin(prompt)
            .working_dir(worktree_path)
            .env("NODE_NO_WARNINGS", "1")
            .env("RUST_LOG", "info");

        let proc = command.start().await.map_err(|e| {
            crate::executor::SpawnContext::from_command(&command, &self.executor_type)
                .with_context(format!(
                    "{} CLI followup execution for session {}",
                    self.executor_type, session_id
                ))
                .spawn_error(e)
        })?;

        Ok(proc)
    }



    fn normalize_logs(
        &self,
        logs: &str,
        _worktree_path: &str,
    ) -> Result<NormalizedConversation, String> {
        let mut entries = Vec::new();
        let mut session_id = None;

        for line in logs.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }

            // Try to parse as JSON from codex jsonl output
            let json: Value = match serde_json::from_str(trimmed) {
                Ok(json) => json,
                Err(_) => {
                    // If line isn't valid JSON, add it as raw text
                    entries.push(NormalizedEntry {
                        timestamp: None,
                        entry_type: NormalizedEntryType::SystemMessage,
                        content: format!("Raw output: {}", trimmed),
                        metadata: None,
                    });
                    continue;
                }
            };

            // Extract session ID if not already set
            if session_id.is_none() {
                if let Some(sess_id) = json.get("session_id").and_then(|v| v.as_str()) {
                    session_id = Some(sess_id.to_string());
                }
            }

            // Process different message types based on codex jsonl format
            if let Some(msg) = json.get("msg") {
                if let Some(msg_type) = msg.get("type").and_then(|t| t.as_str()) {
                    match msg_type {
                        "task_started" => {
                            entries.push(NormalizedEntry {
                                timestamp: None,
                                entry_type: NormalizedEntryType::SystemMessage,
                                content: "Task started".to_string(),
                                metadata: Some(json.clone()),
                            });
                        }
                        "agent_reasoning" => {
                            if let Some(text) = msg.get("text").and_then(|t| t.as_str()) {
                                entries.push(NormalizedEntry {
                                    timestamp: None,
                                    entry_type: NormalizedEntryType::Thinking,
                                    content: text.to_string(),
                                    metadata: Some(json.clone()),
                                });
                            }
                        }
                        "exec_command_begin" => {
                            if let Some(command_array) =
                                msg.get("command").and_then(|c| c.as_array())
                            {
                                let command = command_array
                                    .iter()
                                    .filter_map(|v| v.as_str())
                                    .collect::<Vec<_>>()
                                    .join(" ");

                                // Map shell command to bash tool
                                let (tool_name, action_type) =
                                    if command_array.first().and_then(|v| v.as_str())
                                        == Some("bash")
                                    {
                                        (
                                            "bash".to_string(),
                                            ActionType::CommandRun {
                                                command: command.clone(),
                                            },
                                        )
                                    } else {
                                        (
                                            "shell".to_string(),
                                            ActionType::CommandRun {
                                                command: command.clone(),
                                            },
                                        )
                                    };

                                entries.push(NormalizedEntry {
                                    timestamp: None,
                                    entry_type: NormalizedEntryType::ToolUse {
                                        tool_name,
                                        action_type,
                                    },
                                    content: format!("`{}`", command),
                                    metadata: Some(json.clone()),
                                });
                            }
                        }
                        "exec_command_end" => {
                            // Skip command end entries to avoid duplication
                            continue;
                        }
                        "task_complete" => {
                            if let Some(last_message) =
                                msg.get("last_agent_message").and_then(|m| m.as_str())
                            {
                                entries.push(NormalizedEntry {
                                    timestamp: None,
                                    entry_type: NormalizedEntryType::AssistantMessage,
                                    content: last_message.to_string(),
                                    metadata: Some(json.clone()),
                                });
                            }
                            entries.push(NormalizedEntry {
                                timestamp: None,
                                entry_type: NormalizedEntryType::SystemMessage,
                                content: "Task completed".to_string(),
                                metadata: Some(json.clone()),
                            });
                        }
                        "token_count" => {
                            // Skip token count entries
                            continue;
                        }
                        _ => {
                            // Unknown message type, add as system message
                            entries.push(NormalizedEntry {
                                timestamp: None,
                                entry_type: NormalizedEntryType::SystemMessage,
                                content: format!("Unknown message type: {}", msg_type),
                                metadata: Some(json.clone()),
                            });
                        }
                    }
                }
            } else {
                // JSON without msg field, add as unrecognized
                entries.push(NormalizedEntry {
                    timestamp: None,
                    entry_type: NormalizedEntryType::SystemMessage,
                    content: format!("Unrecognized JSON: {}", trimmed),
                    metadata: Some(json),
                });
            }
        }

        Ok(NormalizedConversation {
            entries,
            session_id,
            executor_type: self.executor_type.clone(),
            prompt: None,
            summary: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_session_id_from_line() {
        let line = "2025-07-23T15:47:59.877058Z  INFO codex_exec: Codex initialized with event: Event { id: \"0\", msg: SessionConfigured(SessionConfiguredEvent { session_id: 3cdcc4df-c7c3-4cca-8902-48c3d4a0f96b, model: \"codex-mini-latest\", history_log_id: 9104228, history_entry_count: 1 }) }";
        let session_id = extract_session_id_from_line(line);
        assert_eq!(
            session_id,
            Some("3cdcc4df-c7c3-4cca-8902-48c3d4a0f96b".to_string())
        );
    }

    #[test]
    fn test_extract_session_id_no_match() {
        let line = "Some random log line without session id";
        let session_id = extract_session_id_from_line(line);
        assert_eq!(session_id, None);
    }

    #[test]
    fn test_normalize_logs_basic() {
        let executor = CodexExecutor::new();
        let logs = r#"{"id":"1","msg":{"type":"task_started"}}
{"id":"1","msg":{"type":"agent_reasoning","text":"**Inspecting the directory tree**\n\nI want to check the root directory tree and I think using `ls -1` is acceptable since the guidelines don't explicitly forbid it, unlike `ls -R`, `find`, or `grep`. I could also consider using `rg --files`, but that might be too overwhelming if there are many files. Focusing on the top-level files and directories seems like a better approach. I'm particularly interested in `LICENSE`, `README.md`, and any relevant README files. So, let's start with `ls -1`."}}
{"id":"1","msg":{"type":"exec_command_begin","call_id":"call_I1o1QnQDtlLjGMg4Vd9HXJLd","command":["bash","-lc","ls -1"],"cwd":"/Users/user/dev/vk-wip"}}
{"id":"1","msg":{"type":"exec_command_end","call_id":"call_I1o1QnQDtlLjGMg4Vd9HXJLd","stdout":"AGENT.md\nCLAUDE.md\nCODE-OF-CONDUCT.md\nCargo.lock\nCargo.toml\nDockerfile\nLICENSE\nREADME.md\nbackend\nbuild-npm-package.sh\ndev_assets\ndev_assets_seed\nfrontend\nnode_modules\nnpx-cli\npackage-lock.json\npackage.json\npnpm-lock.yaml\npnpm-workspace.yaml\nrust-toolchain.toml\nrustfmt.toml\nscripts\nshared\ntest-npm-package.sh\n","stderr":"","exit_code":0}}
{"id":"1","msg":{"type":"task_complete","last_agent_message":"I can see the directory structure of your project. This appears to be a Rust project with a frontend/backend architecture, using pnpm for package management. The project includes various configuration files, documentation, and development assets."}}"#;

        let result = executor.normalize_logs(logs, "/tmp/test").unwrap();

        // Should have: task_started, agent_reasoning, exec_command_begin, task_complete with message, task completed
        assert_eq!(result.entries.len(), 5);

        // Check task started
        assert!(matches!(
            result.entries[0].entry_type,
            NormalizedEntryType::SystemMessage
        ));
        assert_eq!(result.entries[0].content, "Task started");

        // Check agent reasoning (thinking)
        assert!(matches!(
            result.entries[1].entry_type,
            NormalizedEntryType::Thinking
        ));
        assert!(result.entries[1]
            .content
            .contains("Inspecting the directory tree"));

        // Check bash command
        assert!(matches!(
            result.entries[2].entry_type,
            NormalizedEntryType::ToolUse { .. }
        ));
        if let NormalizedEntryType::ToolUse {
            tool_name,
            action_type,
        } = &result.entries[2].entry_type
        {
            assert_eq!(tool_name, "bash");
            assert!(matches!(action_type, ActionType::CommandRun { .. }));
        }
        assert_eq!(result.entries[2].content, "`bash -lc ls -1`");
    }

    #[test]
    fn test_normalize_logs_shell_vs_bash_mapping() {
        let executor = CodexExecutor::new();

        // Test shell command (not bash)
        let shell_logs = r#"{"id":"1","msg":{"type":"exec_command_begin","call_id":"call_test","command":["sh","-c","echo hello"],"cwd":"/tmp"}}"#;
        let result = executor.normalize_logs(shell_logs, "/tmp").unwrap();
        assert_eq!(result.entries.len(), 1);

        if let NormalizedEntryType::ToolUse { tool_name, .. } = &result.entries[0].entry_type {
            assert_eq!(tool_name, "shell"); // Maps to shell, not bash
        }

        // Test bash command
        let bash_logs = r#"{"id":"1","msg":{"type":"exec_command_begin","call_id":"call_test","command":["bash","-c","echo hello"],"cwd":"/tmp"}}"#;
        let result = executor.normalize_logs(bash_logs, "/tmp").unwrap();
        assert_eq!(result.entries.len(), 1);

        if let NormalizedEntryType::ToolUse { tool_name, .. } = &result.entries[0].entry_type {
            assert_eq!(tool_name, "bash"); // Maps to bash
        }
    }

    #[test]
    fn test_normalize_logs_token_count_skipped() {
        let executor = CodexExecutor::new();
        let logs = r#"{"id":"1","msg":{"type":"task_started"}}
{"id":"1","msg":{"type":"token_count","input_tokens":1674,"cached_input_tokens":1627,"output_tokens":384,"reasoning_output_tokens":384,"total_tokens":2058}}
{"id":"1","msg":{"type":"task_complete","last_agent_message":"Done!"}}"#;

        let result = executor.normalize_logs(logs, "/tmp").unwrap();

        // Should have: task_started, task_complete with message, task completed (token_count should be skipped)
        assert_eq!(result.entries.len(), 3);

        // Verify no entry contains token count info
        for entry in &result.entries {
            assert!(!entry.content.contains("token"));
        }
    }

    #[test]
    fn test_normalize_logs_malformed_json() {
        let executor = CodexExecutor::new();
        let logs = r#"{"id":"1","msg":{"type":"task_started"}}
invalid json line here
{"id":"1","msg":{"type":"task_complete","last_agent_message":"Done!"}}"#;

        let result = executor.normalize_logs(logs, "/tmp").unwrap();

        // Should have: task_started, raw output, task_complete message, task completed
        assert_eq!(result.entries.len(), 4);

        // Check that malformed JSON becomes raw output
        assert!(matches!(
            result.entries[1].entry_type,
            NormalizedEntryType::SystemMessage
        ));
        assert!(result.entries[1]
            .content
            .contains("Raw output: invalid json line here"));
    }

    #[test]
    fn test_find_rollout_file_path_basic() {
        // Test the rollout file path logic (this is a unit test, won't actually find files)
        let session_id = "3cdcc4df-c7c3-4cca-8902-48c3d4a0f96b";

        // This will likely fail because the directory doesn't exist, but we can test the logic
        let result = find_rollout_file_path(session_id);

        // Should return an error since directory doesn't exist
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Could not find rollout file"));
    }
}
