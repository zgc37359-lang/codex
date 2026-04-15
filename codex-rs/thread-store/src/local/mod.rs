use std::ffi::OsStr;
use std::ffi::OsString;
use std::fs::FileTimes;
use std::fs::OpenOptions;
use std::path::Path;
use std::path::PathBuf;
use std::time::SystemTime;

use async_trait::async_trait;
use chrono::DateTime;
use chrono::Utc;
use codex_git_utils::GitSha;
use codex_protocol::protocol::AskForApproval;
use codex_protocol::protocol::GitInfo;
use codex_protocol::protocol::SandboxPolicy;
use codex_protocol::protocol::SessionSource;
use codex_rollout::RolloutConfig;
use codex_rollout::RolloutRecorder;
use codex_rollout::ThreadItem;
use codex_rollout::find_archived_thread_path_by_id_str;
use codex_rollout::find_thread_path_by_id_str;
use codex_rollout::parse_cursor;
use codex_rollout::read_thread_item_from_rollout;
use codex_rollout::rollout_date_parts;

use crate::AppendThreadItemsParams;
use crate::ArchiveThreadParams;
use crate::CreateThreadParams;
use crate::ListThreadsParams;
use crate::LoadThreadHistoryParams;
use crate::ReadThreadParams;
use crate::ResumeThreadRecorderParams;
use crate::SetThreadNameParams;
use crate::StoredThread;
use crate::StoredThreadHistory;
use crate::ThreadPage;
use crate::ThreadRecorder;
use crate::ThreadSortKey;
use crate::ThreadStore;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;
use crate::UpdateThreadMetadataParams;

/// Local filesystem/SQLite-backed implementation of [`ThreadStore`].
#[derive(Clone, Debug)]
pub struct LocalThreadStore {
    config: RolloutConfig,
}

/// A local archive operation whose rollout path has been resolved and validated.
#[derive(Debug)]
pub struct PreparedLocalThreadArchive {
    config: RolloutConfig,
    thread_id: codex_protocol::ThreadId,
    canonical_rollout_path: PathBuf,
    file_name: OsString,
}

impl LocalThreadStore {
    /// Create a local store from the rollout configuration used by existing local persistence.
    pub fn new(config: RolloutConfig) -> Self {
        Self { config }
    }

    /// Prepare to archive a local thread by resolving and validating its active rollout.
    pub async fn prepare_archive_thread(
        &self,
        params: ArchiveThreadParams,
    ) -> ThreadStoreResult<PreparedLocalThreadArchive> {
        let thread_id = params.thread_id;
        let rollout_path =
            find_thread_path_by_id_str(self.config.codex_home.as_path(), &thread_id.to_string())
                .await
                .map_err(|err| ThreadStoreError::InvalidRequest {
                    message: format!("failed to locate thread id {thread_id}: {err}"),
                })?
                .ok_or_else(|| ThreadStoreError::InvalidRequest {
                    message: format!("no rollout found for thread id {thread_id}"),
                })?;

        let canonical_rollout_path = scoped_rollout_path(
            self.config.codex_home.join(codex_rollout::SESSIONS_SUBDIR),
            rollout_path.as_path(),
            "sessions",
        )?;
        let file_name = matching_rollout_file_name(
            canonical_rollout_path.as_path(),
            thread_id,
            rollout_path.as_path(),
        )?;

        Ok(PreparedLocalThreadArchive {
            config: self.config.clone(),
            thread_id,
            canonical_rollout_path,
            file_name,
        })
    }
}

impl PreparedLocalThreadArchive {
    /// Move the prepared rollout into the archived collection.
    pub async fn complete(self) -> ThreadStoreResult<()> {
        let archive_folder = self
            .config
            .codex_home
            .join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR);
        std::fs::create_dir_all(&archive_folder).map_err(|err| ThreadStoreError::Internal {
            message: format!("failed to archive thread: {err}"),
        })?;
        let archived_path = archive_folder.join(&self.file_name);
        std::fs::rename(&self.canonical_rollout_path, &archived_path).map_err(|err| {
            ThreadStoreError::Internal {
                message: format!("failed to archive thread: {err}"),
            }
        })?;

        if let Some(ctx) = codex_rollout::state_db::get_state_db(&self.config).await {
            let _ = ctx
                .mark_archived(self.thread_id, archived_path.as_path(), Utc::now())
                .await;
        }
        Ok(())
    }
}

#[async_trait]
impl ThreadStore for LocalThreadStore {
    async fn create_thread(
        &self,
        _params: CreateThreadParams,
    ) -> ThreadStoreResult<Box<dyn ThreadRecorder>> {
        unsupported("create_thread")
    }

    async fn resume_thread_recorder(
        &self,
        _params: ResumeThreadRecorderParams,
    ) -> ThreadStoreResult<Box<dyn ThreadRecorder>> {
        unsupported("resume_thread_recorder")
    }

    async fn append_items(&self, _params: AppendThreadItemsParams) -> ThreadStoreResult<()> {
        unsupported("append_items")
    }

    async fn load_history(
        &self,
        _params: LoadThreadHistoryParams,
    ) -> ThreadStoreResult<StoredThreadHistory> {
        unsupported("load_history")
    }

    async fn read_thread(&self, _params: ReadThreadParams) -> ThreadStoreResult<StoredThread> {
        unsupported("read_thread")
    }

    async fn list_threads(&self, params: ListThreadsParams) -> ThreadStoreResult<ThreadPage> {
        let cursor = params
            .cursor
            .as_deref()
            .map(|cursor| {
                parse_cursor(cursor).ok_or_else(|| ThreadStoreError::InvalidRequest {
                    message: format!("invalid cursor: {cursor}"),
                })
            })
            .transpose()?;
        let sort_key = match params.sort_key {
            ThreadSortKey::CreatedAt => codex_rollout::ThreadSortKey::CreatedAt,
            ThreadSortKey::UpdatedAt => codex_rollout::ThreadSortKey::UpdatedAt,
        };
        let page = list_rollout_threads(&self.config, &params, cursor.as_ref(), sort_key).await?;

        let next_cursor = page
            .next_cursor
            .as_ref()
            .and_then(|cursor| serde_json::to_value(cursor).ok())
            .and_then(|value| value.as_str().map(str::to_owned));
        let items = page
            .items
            .into_iter()
            .filter_map(|item| {
                stored_thread_from_rollout_item(
                    item,
                    params.archived,
                    self.config.model_provider_id.as_str(),
                )
            })
            .collect::<Vec<_>>();

        Ok(ThreadPage { items, next_cursor })
    }

    async fn set_thread_name(&self, _params: SetThreadNameParams) -> ThreadStoreResult<()> {
        unsupported("set_thread_name")
    }

    async fn update_thread_metadata(
        &self,
        _params: UpdateThreadMetadataParams,
    ) -> ThreadStoreResult<StoredThread> {
        unsupported("update_thread_metadata")
    }

    async fn archive_thread(&self, params: ArchiveThreadParams) -> ThreadStoreResult<()> {
        self.prepare_archive_thread(params).await?.complete().await
    }

    async fn unarchive_thread(
        &self,
        params: ArchiveThreadParams,
    ) -> ThreadStoreResult<StoredThread> {
        let thread_id = params.thread_id;
        let archived_path = find_archived_thread_path_by_id_str(
            self.config.codex_home.as_path(),
            &thread_id.to_string(),
        )
        .await
        .map_err(|err| ThreadStoreError::InvalidRequest {
            message: format!("failed to locate archived thread id {thread_id}: {err}"),
        })?
        .ok_or_else(|| ThreadStoreError::InvalidRequest {
            message: format!("no archived rollout found for thread id {thread_id}"),
        })?;

        let canonical_archived_path = scoped_rollout_path(
            self.config
                .codex_home
                .join(codex_rollout::ARCHIVED_SESSIONS_SUBDIR),
            archived_path.as_path(),
            "archived",
        )?;
        let file_name = matching_rollout_file_name(
            canonical_archived_path.as_path(),
            thread_id,
            archived_path.as_path(),
        )?;
        let Some((year, month, day)) = rollout_date_parts(&file_name) else {
            return Err(ThreadStoreError::InvalidRequest {
                message: format!(
                    "rollout path `{}` missing filename timestamp",
                    archived_path.display()
                ),
            });
        };

        let dest_dir = self
            .config
            .codex_home
            .join(codex_rollout::SESSIONS_SUBDIR)
            .join(year)
            .join(month)
            .join(day);
        std::fs::create_dir_all(&dest_dir).map_err(|err| ThreadStoreError::Internal {
            message: format!("failed to unarchive thread: {err}"),
        })?;
        let restored_path = dest_dir.join(&file_name);
        std::fs::rename(&canonical_archived_path, &restored_path).map_err(|err| {
            ThreadStoreError::Internal {
                message: format!("failed to unarchive thread: {err}"),
            }
        })?;
        touch_modified_time(restored_path.as_path()).map_err(|err| ThreadStoreError::Internal {
            message: format!("failed to update unarchived thread timestamp: {err}"),
        })?;

        if let Some(ctx) = codex_rollout::state_db::get_state_db(&self.config).await {
            let _ = ctx
                .mark_unarchived(thread_id, restored_path.as_path())
                .await;
        }

        let item = read_thread_item_from_rollout(restored_path.clone())
            .await
            .ok_or_else(|| ThreadStoreError::Internal {
                message: format!(
                    "failed to read unarchived thread {}",
                    restored_path.display()
                ),
            })?;
        stored_thread_from_rollout_item(
            item,
            /*archived*/ false,
            self.config.model_provider_id.as_str(),
        )
        .ok_or_else(|| ThreadStoreError::Internal {
            message: format!(
                "failed to read unarchived thread id from {}",
                restored_path.display()
            ),
        })
    }
}

fn unsupported<T>(operation: &str) -> ThreadStoreResult<T> {
    Err(ThreadStoreError::Internal {
        message: format!("local thread store does not implement {operation} in this slice"),
    })
}

fn scoped_rollout_path(
    root: PathBuf,
    rollout_path: &Path,
    root_name: &str,
) -> ThreadStoreResult<PathBuf> {
    let canonical_root =
        std::fs::canonicalize(&root).map_err(|err| ThreadStoreError::Internal {
            message: format!(
                "failed to resolve {root_name} directory `{}`: {err}",
                root.display()
            ),
        })?;
    let canonical_rollout_path =
        std::fs::canonicalize(rollout_path).map_err(|_| ThreadStoreError::InvalidRequest {
            message: format!(
                "rollout path `{}` must be in {root_name} directory",
                rollout_path.display()
            ),
        })?;
    if canonical_rollout_path.starts_with(&canonical_root) {
        Ok(canonical_rollout_path)
    } else {
        Err(ThreadStoreError::InvalidRequest {
            message: format!(
                "rollout path `{}` must be in {root_name} directory",
                rollout_path.display()
            ),
        })
    }
}

fn matching_rollout_file_name(
    rollout_path: &Path,
    thread_id: codex_protocol::ThreadId,
    display_path: &Path,
) -> ThreadStoreResult<std::ffi::OsString> {
    let Some(file_name) = rollout_path.file_name().map(OsStr::to_owned) else {
        return Err(ThreadStoreError::InvalidRequest {
            message: format!(
                "rollout path `{}` missing file name",
                display_path.display()
            ),
        });
    };
    let required_suffix = format!("{thread_id}.jsonl");
    if file_name
        .to_string_lossy()
        .ends_with(required_suffix.as_str())
    {
        Ok(file_name)
    } else {
        Err(ThreadStoreError::InvalidRequest {
            message: format!(
                "rollout path `{}` does not match thread id {thread_id}",
                display_path.display()
            ),
        })
    }
}

fn touch_modified_time(path: &Path) -> std::io::Result<()> {
    let times = FileTimes::new().set_modified(SystemTime::now());
    OpenOptions::new().append(true).open(path)?.set_times(times)
}

async fn list_rollout_threads(
    config: &RolloutConfig,
    params: &ListThreadsParams,
    cursor: Option<&codex_rollout::Cursor>,
    sort_key: codex_rollout::ThreadSortKey,
) -> ThreadStoreResult<codex_rollout::ThreadsPage> {
    let page = if params.archived {
        RolloutRecorder::list_archived_threads(
            config,
            params.page_size,
            cursor,
            sort_key,
            params.allowed_sources.as_slice(),
            params.model_providers.as_deref(),
            config.model_provider_id.as_str(),
            params.search_term.as_deref(),
        )
        .await
    } else {
        RolloutRecorder::list_threads(
            config,
            params.page_size,
            cursor,
            sort_key,
            params.allowed_sources.as_slice(),
            params.model_providers.as_deref(),
            config.model_provider_id.as_str(),
            params.search_term.as_deref(),
        )
        .await
    };
    page.map_err(|err| ThreadStoreError::Internal {
        message: format!("failed to list threads: {err}"),
    })
}

fn stored_thread_from_rollout_item(
    item: ThreadItem,
    archived: bool,
    default_provider: &str,
) -> Option<StoredThread> {
    let thread_id = item
        .thread_id
        .or_else(|| thread_id_from_rollout_path(item.path.as_path()))?;
    let created_at = parse_rfc3339(item.created_at.as_deref()).unwrap_or_else(Utc::now);
    let updated_at = parse_rfc3339(item.updated_at.as_deref()).unwrap_or(created_at);
    let archived_at = archived.then_some(updated_at);
    let git_info = git_info_from_parts(
        item.git_sha.clone(),
        item.git_branch.clone(),
        item.git_origin_url.clone(),
    );
    let source = item.source.unwrap_or(SessionSource::Unknown);
    let preview = item.first_user_message.clone().unwrap_or_default();

    Some(StoredThread {
        thread_id,
        rollout_path: Some(item.path),
        forked_from_id: None,
        preview,
        name: None,
        model_provider: item
            .model_provider
            .filter(|provider| !provider.is_empty())
            .unwrap_or_else(|| default_provider.to_string()),
        model: None,
        reasoning_effort: None,
        created_at,
        updated_at,
        archived_at,
        cwd: item.cwd.unwrap_or_default(),
        cli_version: item.cli_version.unwrap_or_default(),
        source,
        agent_nickname: item.agent_nickname,
        agent_role: item.agent_role,
        agent_path: None,
        git_info,
        approval_mode: AskForApproval::OnRequest,
        sandbox_policy: SandboxPolicy::new_read_only_policy(),
        token_usage: None,
        first_user_message: item.first_user_message,
        history: None,
    })
}

fn parse_rfc3339(value: Option<&str>) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value?)
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
}

fn git_info_from_parts(
    sha: Option<String>,
    branch: Option<String>,
    origin_url: Option<String>,
) -> Option<GitInfo> {
    if sha.is_none() && branch.is_none() && origin_url.is_none() {
        return None;
    }
    Some(GitInfo {
        commit_hash: sha.as_deref().map(GitSha::new),
        branch,
        repository_url: origin_url,
    })
}

fn thread_id_from_rollout_path(path: &std::path::Path) -> Option<codex_protocol::ThreadId> {
    let file_name = path.file_name()?.to_str()?;
    let stem = file_name.strip_suffix(".jsonl")?;
    if stem.len() < 37 {
        return None;
    }
    let uuid_start = stem.len().saturating_sub(36);
    if !stem[..uuid_start].ends_with('-') {
        return None;
    }
    codex_protocol::ThreadId::from_string(&stem[uuid_start..]).ok()
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::Write;
    use std::path::Path;
    use std::path::PathBuf;

    use codex_protocol::ThreadId;
    use codex_protocol::protocol::SessionSource;
    use codex_rollout::ARCHIVED_SESSIONS_SUBDIR;
    use pretty_assertions::assert_eq;
    use tempfile::TempDir;
    use uuid::Uuid;

    use super::*;

    fn test_config(codex_home: &Path) -> RolloutConfig {
        RolloutConfig {
            codex_home: codex_home.to_path_buf(),
            sqlite_home: codex_home.to_path_buf(),
            cwd: codex_home.to_path_buf(),
            model_provider_id: "test-provider".to_string(),
            generate_memories: true,
        }
    }

    fn write_session_file(root: &Path, ts: &str, uuid: Uuid) -> std::io::Result<PathBuf> {
        write_session_file_with(
            root,
            root.join("sessions/2025/01/03"),
            ts,
            uuid,
            "Hello from user",
            Some("test-provider"),
        )
    }

    fn write_archived_session_file(root: &Path, ts: &str, uuid: Uuid) -> std::io::Result<PathBuf> {
        write_session_file_with(
            root,
            root.join(ARCHIVED_SESSIONS_SUBDIR),
            ts,
            uuid,
            "Archived user message",
            Some("test-provider"),
        )
    }

    fn write_session_file_with(
        root: &Path,
        day_dir: PathBuf,
        ts: &str,
        uuid: Uuid,
        first_user_message: &str,
        model_provider: Option<&str>,
    ) -> std::io::Result<PathBuf> {
        fs::create_dir_all(&day_dir)?;
        let path = day_dir.join(format!("rollout-{ts}-{uuid}.jsonl"));
        let mut file = fs::File::create(&path)?;
        let meta = serde_json::json!({
            "timestamp": ts,
            "type": "session_meta",
            "payload": {
                "id": uuid,
                "timestamp": ts,
                "cwd": root,
                "originator": "test_originator",
                "cli_version": "test_version",
                "source": "cli",
                "model_provider": model_provider,
                "git": {
                    "commit_hash": "abcdef",
                    "branch": "main",
                    "repository_url": "https://example.com/repo.git"
                }
            },
        });
        writeln!(file, "{meta}")?;
        let user_event = serde_json::json!({
            "timestamp": ts,
            "type": "event_msg",
            "payload": {
                "type": "user_message",
                "message": first_user_message,
                "kind": "plain",
            },
        });
        writeln!(file, "{user_event}")?;
        Ok(path)
    }

    #[tokio::test]
    async fn archive_thread_moves_rollout_to_archived_collection() {
        let home = TempDir::new().expect("temp dir");
        let store = LocalThreadStore::new(test_config(home.path()));
        let uuid = Uuid::from_u128(201);
        let thread_id = ThreadId::from_string(&uuid.to_string()).expect("valid thread id");
        let active_path =
            write_session_file(home.path(), "2025-01-03T12-00-00", uuid).expect("session file");

        store
            .archive_thread(ArchiveThreadParams { thread_id })
            .await
            .expect("archive thread");

        assert!(!active_path.exists());
        let archived_path = home
            .path()
            .join(ARCHIVED_SESSIONS_SUBDIR)
            .join(active_path.file_name().expect("file name"));
        assert!(archived_path.exists());

        let archived = store
            .list_threads(ListThreadsParams {
                page_size: 10,
                cursor: None,
                sort_key: ThreadSortKey::CreatedAt,
                allowed_sources: Vec::new(),
                model_providers: None,
                archived: true,
                search_term: None,
            })
            .await
            .expect("archived listing");
        assert_eq!(archived.items.len(), 1);
        assert_eq!(archived.items[0].thread_id, thread_id);
        assert_eq!(archived.items[0].rollout_path, Some(archived_path));
        assert_eq!(
            archived.items[0].archived_at,
            Some(archived.items[0].updated_at)
        );
    }

    #[tokio::test]
    async fn prepare_archive_thread_resolves_rollout_before_complete() {
        let home = TempDir::new().expect("temp dir");
        let store = LocalThreadStore::new(test_config(home.path()));
        let uuid = Uuid::from_u128(205);
        let thread_id = ThreadId::from_string(&uuid.to_string()).expect("valid thread id");
        let active_path =
            write_session_file(home.path(), "2025-01-03T12-00-00", uuid).expect("session file");

        let prepared_archive = store
            .prepare_archive_thread(ArchiveThreadParams { thread_id })
            .await
            .expect("prepare archive");

        assert!(active_path.exists());
        prepared_archive.complete().await.expect("complete archive");
        assert!(!active_path.exists());
    }

    #[tokio::test]
    async fn prepare_archive_thread_fails_without_rollout() {
        let home = TempDir::new().expect("temp dir");
        let store = LocalThreadStore::new(test_config(home.path()));
        let uuid = Uuid::from_u128(206);
        let thread_id = ThreadId::from_string(&uuid.to_string()).expect("valid thread id");

        let err = store
            .prepare_archive_thread(ArchiveThreadParams { thread_id })
            .await
            .expect_err("archive should fail without rollout");

        let ThreadStoreError::InvalidRequest { message } = err else {
            panic!("expected invalid request error");
        };
        assert_eq!(
            message,
            format!("no rollout found for thread id {thread_id}")
        );
    }

    #[tokio::test]
    async fn archive_thread_updates_sqlite_metadata_when_present() {
        let home = TempDir::new().expect("temp dir");
        let config = test_config(home.path());
        let store = LocalThreadStore::new(config.clone());
        let uuid = Uuid::from_u128(202);
        let thread_id = ThreadId::from_string(&uuid.to_string()).expect("valid thread id");
        let active_path =
            write_session_file(home.path(), "2025-01-03T12-00-00", uuid).expect("session file");
        let runtime = codex_state::StateRuntime::init(
            home.path().to_path_buf(),
            config.model_provider_id.clone(),
        )
        .await
        .expect("state db should initialize");
        runtime
            .mark_backfill_complete(/*last_watermark*/ None)
            .await
            .expect("backfill should be complete");
        let mut builder = codex_state::ThreadMetadataBuilder::new(
            thread_id,
            active_path.clone(),
            Utc::now(),
            SessionSource::Cli,
        );
        builder.model_provider = Some(config.model_provider_id.clone());
        builder.cwd = home.path().to_path_buf();
        builder.cli_version = Some("test_version".to_string());
        let metadata = builder.build(config.model_provider_id.as_str());
        runtime
            .upsert_thread(&metadata)
            .await
            .expect("state db upsert should succeed");

        store
            .archive_thread(ArchiveThreadParams { thread_id })
            .await
            .expect("archive thread");

        let archived_path = home
            .path()
            .join(ARCHIVED_SESSIONS_SUBDIR)
            .join(active_path.file_name().expect("file name"));
        let updated = runtime
            .get_thread(thread_id)
            .await
            .expect("state db read should succeed")
            .expect("thread metadata should exist");
        assert_eq!(updated.rollout_path, archived_path);
        assert!(updated.archived_at.is_some());
    }

    #[tokio::test]
    async fn unarchive_thread_restores_rollout_and_returns_updated_thread() {
        let home = TempDir::new().expect("temp dir");
        let store = LocalThreadStore::new(test_config(home.path()));
        let uuid = Uuid::from_u128(203);
        let thread_id = ThreadId::from_string(&uuid.to_string()).expect("valid thread id");
        let archived_path = write_archived_session_file(home.path(), "2025-01-03T13-00-00", uuid)
            .expect("archived session file");

        let thread = store
            .unarchive_thread(ArchiveThreadParams { thread_id })
            .await
            .expect("unarchive thread");

        assert!(!archived_path.exists());
        let restored_path = home
            .path()
            .join("sessions/2025/01/03")
            .join(archived_path.file_name().expect("file name"));
        assert!(restored_path.exists());
        assert_eq!(thread.thread_id, thread_id);
        assert_eq!(thread.rollout_path, Some(restored_path));
        assert_eq!(thread.archived_at, None);
        assert_eq!(thread.preview, "Archived user message");
        assert_eq!(
            thread.first_user_message.as_deref(),
            Some("Archived user message")
        );
    }

    #[tokio::test]
    async fn unarchive_thread_updates_sqlite_metadata_when_present() {
        let home = TempDir::new().expect("temp dir");
        let config = test_config(home.path());
        let store = LocalThreadStore::new(config.clone());
        let uuid = Uuid::from_u128(204);
        let thread_id = ThreadId::from_string(&uuid.to_string()).expect("valid thread id");
        let archived_path = write_archived_session_file(home.path(), "2025-01-03T13-00-00", uuid)
            .expect("archived session file");
        let runtime = codex_state::StateRuntime::init(
            home.path().to_path_buf(),
            config.model_provider_id.clone(),
        )
        .await
        .expect("state db should initialize");
        runtime
            .mark_backfill_complete(/*last_watermark*/ None)
            .await
            .expect("backfill should be complete");
        let mut builder = codex_state::ThreadMetadataBuilder::new(
            thread_id,
            archived_path.clone(),
            Utc::now(),
            SessionSource::Cli,
        );
        builder.model_provider = Some(config.model_provider_id.clone());
        builder.cwd = home.path().to_path_buf();
        builder.cli_version = Some("test_version".to_string());
        let mut metadata = builder.build(config.model_provider_id.as_str());
        metadata.archived_at = Some(metadata.updated_at);
        runtime
            .upsert_thread(&metadata)
            .await
            .expect("state db upsert should succeed");

        store
            .unarchive_thread(ArchiveThreadParams { thread_id })
            .await
            .expect("unarchive thread");

        let restored_path = home
            .path()
            .join("sessions/2025/01/03")
            .join(archived_path.file_name().expect("file name"));
        let updated = runtime
            .get_thread(thread_id)
            .await
            .expect("state db read should succeed")
            .expect("thread metadata should exist");
        assert_eq!(updated.rollout_path, restored_path);
        assert_eq!(updated.archived_at, None);
    }

    #[tokio::test]
    async fn list_threads_uses_default_provider_when_rollout_omits_provider() {
        let home = TempDir::new().expect("temp dir");
        let store = LocalThreadStore::new(test_config(home.path()));
        write_session_file_with(
            home.path(),
            home.path().join("sessions/2025/01/03"),
            "2025-01-03T12-00-00",
            Uuid::from_u128(102),
            "Hello from user",
            /*model_provider*/ None,
        )
        .expect("session file");

        let page = store
            .list_threads(ListThreadsParams {
                page_size: 10,
                cursor: None,
                sort_key: ThreadSortKey::CreatedAt,
                allowed_sources: Vec::new(),
                model_providers: None,
                archived: false,
                search_term: None,
            })
            .await
            .expect("thread listing");

        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].model_provider, "test-provider");
    }

    #[tokio::test]
    async fn list_threads_preserves_sqlite_title_search_results() {
        let home = TempDir::new().expect("temp dir");
        let config = test_config(home.path());
        let store = LocalThreadStore::new(config.clone());
        let uuid = Uuid::from_u128(103);
        let thread_id = ThreadId::from_string(&uuid.to_string()).expect("valid thread id");
        let rollout_path = home.path().join("rollout-title-search.jsonl");
        fs::write(&rollout_path, "").expect("placeholder rollout file");

        let runtime = codex_state::StateRuntime::init(
            home.path().to_path_buf(),
            config.model_provider_id.clone(),
        )
        .await
        .expect("state db should initialize");
        runtime
            .mark_backfill_complete(/*last_watermark*/ None)
            .await
            .expect("backfill should be complete");
        let created_at = Utc::now();
        let mut builder = codex_state::ThreadMetadataBuilder::new(
            thread_id,
            rollout_path,
            created_at,
            SessionSource::Cli,
        );
        builder.model_provider = Some(config.model_provider_id.clone());
        builder.cwd = home.path().to_path_buf();
        builder.cli_version = Some("test_version".to_string());
        let mut metadata = builder.build(config.model_provider_id.as_str());
        metadata.title = "needle title".to_string();
        metadata.first_user_message = Some("plain preview".to_string());
        runtime
            .upsert_thread(&metadata)
            .await
            .expect("state db upsert should succeed");

        let page = store
            .list_threads(ListThreadsParams {
                page_size: 10,
                cursor: None,
                sort_key: ThreadSortKey::CreatedAt,
                allowed_sources: Vec::new(),
                model_providers: None,
                archived: false,
                search_term: Some("needle".to_string()),
            })
            .await
            .expect("thread listing");

        let ids = page
            .items
            .iter()
            .map(|item| item.thread_id)
            .collect::<Vec<_>>();
        assert_eq!(ids, vec![thread_id]);
        assert_eq!(
            page.items[0].first_user_message.as_deref(),
            Some("plain preview")
        );
    }

    #[tokio::test]
    async fn list_threads_selects_active_or_archived_collection() {
        let home = TempDir::new().expect("temp dir");
        let store = LocalThreadStore::new(test_config(home.path()));
        let active_uuid = Uuid::from_u128(105);
        let archived_uuid = Uuid::from_u128(106);
        write_session_file(home.path(), "2025-01-03T12-00-00", active_uuid)
            .expect("active session file");
        write_archived_session_file(home.path(), "2025-01-03T13-00-00", archived_uuid)
            .expect("archived session file");

        let active = store
            .list_threads(ListThreadsParams {
                page_size: 10,
                cursor: None,
                sort_key: ThreadSortKey::CreatedAt,
                allowed_sources: Vec::new(),
                model_providers: None,
                archived: false,
                search_term: None,
            })
            .await
            .expect("active listing");
        let archived = store
            .list_threads(ListThreadsParams {
                page_size: 10,
                cursor: None,
                sort_key: ThreadSortKey::CreatedAt,
                allowed_sources: Vec::new(),
                model_providers: None,
                archived: true,
                search_term: None,
            })
            .await
            .expect("archived listing");

        let active_id = ThreadId::from_string(&active_uuid.to_string()).expect("valid thread id");
        let archived_id =
            ThreadId::from_string(&archived_uuid.to_string()).expect("valid thread id");
        assert_eq!(
            active
                .items
                .iter()
                .map(|item| item.thread_id)
                .collect::<Vec<_>>(),
            vec![active_id]
        );
        assert_eq!(
            archived
                .items
                .iter()
                .map(|item| item.thread_id)
                .collect::<Vec<_>>(),
            vec![archived_id]
        );
        assert_eq!(active.items[0].archived_at, None);
        assert_eq!(
            archived.items[0].archived_at,
            Some(archived.items[0].updated_at)
        );
    }

    #[tokio::test]
    async fn list_threads_returns_local_rollout_summary() {
        let home = TempDir::new().expect("temp dir");
        let config = test_config(home.path());
        let store = LocalThreadStore::new(config);
        let uuid = Uuid::from_u128(101);
        let path =
            write_session_file(home.path(), "2025-01-03T12-00-00", uuid).expect("session file");

        let page = store
            .list_threads(ListThreadsParams {
                page_size: 10,
                cursor: None,
                sort_key: ThreadSortKey::CreatedAt,
                allowed_sources: vec![SessionSource::Cli],
                model_providers: Some(vec!["test-provider".to_string()]),
                archived: false,
                search_term: None,
            })
            .await
            .expect("thread listing");

        let thread_id =
            codex_protocol::ThreadId::from_string(&uuid.to_string()).expect("valid thread id");
        assert_eq!(page.next_cursor, None);
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].thread_id, thread_id);
        assert_eq!(page.items[0].rollout_path, Some(path));
        assert_eq!(page.items[0].preview, "Hello from user");
        assert_eq!(
            page.items[0].first_user_message.as_deref(),
            Some("Hello from user")
        );
        assert_eq!(page.items[0].model_provider, "test-provider");
        assert_eq!(page.items[0].cli_version, "test_version");
        assert_eq!(page.items[0].source, SessionSource::Cli);
    }

    #[tokio::test]
    async fn list_threads_rejects_invalid_cursor() {
        let home = TempDir::new().expect("temp dir");
        let store = LocalThreadStore::new(test_config(home.path()));

        let err = store
            .list_threads(ListThreadsParams {
                page_size: 10,
                cursor: Some("not-a-cursor".to_string()),
                sort_key: ThreadSortKey::CreatedAt,
                allowed_sources: Vec::new(),
                model_providers: None,
                archived: false,
                search_term: None,
            })
            .await
            .expect_err("invalid cursor should fail");

        assert!(matches!(err, ThreadStoreError::InvalidRequest { .. }));
    }
}
