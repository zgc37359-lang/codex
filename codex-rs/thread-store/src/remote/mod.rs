use std::path::PathBuf;
use std::str::FromStr;

use async_trait::async_trait;
use chrono::DateTime;
use chrono::Utc;
use codex_git_utils::GitSha;
use codex_protocol::AgentPath;
use codex_protocol::ThreadId;
use codex_protocol::openai_models::ReasoningEffort;
use codex_protocol::protocol::AskForApproval;
use codex_protocol::protocol::GitInfo;
use codex_protocol::protocol::ReadOnlyAccess;
use codex_protocol::protocol::SandboxPolicy;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentSource;

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
use proto::thread_store_client::ThreadStoreClient;

mod proto {
    tonic::include_proto!("codex.thread_store.v1");
}

/// gRPC-backed [`ThreadStore`] implementation for deployments whose durable thread data lives
/// outside the app-server process.
#[derive(Clone, Debug)]
pub struct RemoteThreadStore {
    endpoint: String,
}

impl RemoteThreadStore {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
        }
    }

    async fn client(&self) -> ThreadStoreResult<ThreadStoreClient<tonic::transport::Channel>> {
        ThreadStoreClient::connect(self.endpoint.clone())
            .await
            .map_err(|err| ThreadStoreError::Internal {
                message: format!("failed to connect to remote thread store: {err}"),
            })
    }
}

#[async_trait]
impl ThreadStore for RemoteThreadStore {
    async fn create_thread(
        &self,
        _params: CreateThreadParams,
    ) -> ThreadStoreResult<Box<dyn ThreadRecorder>> {
        Err(not_implemented("create_thread"))
    }

    async fn resume_thread_recorder(
        &self,
        _params: ResumeThreadRecorderParams,
    ) -> ThreadStoreResult<Box<dyn ThreadRecorder>> {
        Err(not_implemented("resume_thread_recorder"))
    }

    async fn append_items(&self, _params: AppendThreadItemsParams) -> ThreadStoreResult<()> {
        Err(not_implemented("append_items"))
    }

    async fn load_history(
        &self,
        _params: LoadThreadHistoryParams,
    ) -> ThreadStoreResult<StoredThreadHistory> {
        Err(not_implemented("load_history"))
    }

    async fn read_thread(&self, _params: ReadThreadParams) -> ThreadStoreResult<StoredThread> {
        Err(not_implemented("read_thread"))
    }

    async fn list_threads(&self, params: ListThreadsParams) -> ThreadStoreResult<ThreadPage> {
        let request = proto::ListThreadsRequest {
            page_size: params.page_size.try_into().map_err(|_| {
                ThreadStoreError::InvalidRequest {
                    message: format!("page_size is too large: {}", params.page_size),
                }
            })?,
            cursor: params.cursor,
            sort_key: proto_sort_key(params.sort_key).into(),
            allowed_sources: params
                .allowed_sources
                .iter()
                .map(proto_session_source)
                .collect(),
            model_provider_filter: params
                .model_providers
                .map(|values| proto::ModelProviderFilter { values }),
            archived: params.archived,
            search_term: params.search_term,
        };

        let response = self
            .client()
            .await?
            .list_threads(request)
            .await
            .map_err(remote_status_to_error)?
            .into_inner();

        let items = response
            .threads
            .into_iter()
            .map(stored_thread_from_proto)
            .collect::<ThreadStoreResult<Vec<_>>>()?;

        Ok(ThreadPage {
            items,
            next_cursor: response.next_cursor,
        })
    }

    async fn set_thread_name(&self, _params: SetThreadNameParams) -> ThreadStoreResult<()> {
        Err(not_implemented("set_thread_name"))
    }

    async fn update_thread_metadata(
        &self,
        _params: UpdateThreadMetadataParams,
    ) -> ThreadStoreResult<StoredThread> {
        Err(not_implemented("update_thread_metadata"))
    }

    async fn archive_thread(&self, _params: ArchiveThreadParams) -> ThreadStoreResult<()> {
        Err(not_implemented("archive_thread"))
    }

    async fn unarchive_thread(
        &self,
        _params: ArchiveThreadParams,
    ) -> ThreadStoreResult<StoredThread> {
        Err(not_implemented("unarchive_thread"))
    }
}

fn not_implemented(method: &str) -> ThreadStoreError {
    ThreadStoreError::Internal {
        message: format!("remote thread store does not implement {method} yet"),
    }
}

fn remote_status_to_error(status: tonic::Status) -> ThreadStoreError {
    match status.code() {
        tonic::Code::InvalidArgument => ThreadStoreError::InvalidRequest {
            message: status.message().to_string(),
        },
        tonic::Code::AlreadyExists | tonic::Code::FailedPrecondition | tonic::Code::Aborted => {
            ThreadStoreError::Conflict {
                message: status.message().to_string(),
            }
        }
        _ => ThreadStoreError::Internal {
            message: format!("remote thread store request failed: {status}"),
        },
    }
}

fn proto_sort_key(sort_key: ThreadSortKey) -> proto::ThreadSortKey {
    match sort_key {
        ThreadSortKey::CreatedAt => proto::ThreadSortKey::CreatedAt,
        ThreadSortKey::UpdatedAt => proto::ThreadSortKey::UpdatedAt,
    }
}

fn proto_session_source(source: &SessionSource) -> proto::SessionSource {
    match source {
        SessionSource::Cli => proto_source(proto::SessionSourceKind::Cli),
        SessionSource::VSCode => proto_source(proto::SessionSourceKind::Vscode),
        SessionSource::Exec => proto_source(proto::SessionSourceKind::Exec),
        SessionSource::Mcp => proto_source(proto::SessionSourceKind::AppServer),
        SessionSource::Custom(custom) => proto::SessionSource {
            kind: proto::SessionSourceKind::Custom.into(),
            custom: Some(custom.clone()),
            ..Default::default()
        },
        SessionSource::SubAgent(SubAgentSource::Review) => {
            proto_source(proto::SessionSourceKind::SubAgentReview)
        }
        SessionSource::SubAgent(SubAgentSource::Compact) => {
            proto_source(proto::SessionSourceKind::SubAgentCompact)
        }
        SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            parent_thread_id,
            depth,
            agent_path,
            agent_nickname,
            agent_role,
        }) => proto::SessionSource {
            kind: proto::SessionSourceKind::SubAgentThreadSpawn.into(),
            sub_agent_parent_thread_id: Some(parent_thread_id.to_string()),
            sub_agent_depth: Some(*depth),
            sub_agent_path: agent_path.as_ref().map(|path| path.as_str().to_string()),
            sub_agent_nickname: agent_nickname.clone(),
            sub_agent_role: agent_role.clone(),
            ..Default::default()
        },
        SessionSource::SubAgent(SubAgentSource::MemoryConsolidation) => {
            proto_source(proto::SessionSourceKind::SubAgentMemoryConsolidation)
        }
        SessionSource::SubAgent(SubAgentSource::Other(other)) => proto::SessionSource {
            kind: proto::SessionSourceKind::SubAgentOther.into(),
            sub_agent_other: Some(other.clone()),
            ..Default::default()
        },
        SessionSource::Unknown => proto_source(proto::SessionSourceKind::Unknown),
    }
}

fn proto_source(kind: proto::SessionSourceKind) -> proto::SessionSource {
    proto::SessionSource {
        kind: kind.into(),
        ..Default::default()
    }
}

fn stored_thread_from_proto(thread: proto::StoredThread) -> ThreadStoreResult<StoredThread> {
    let source = thread
        .source
        .as_ref()
        .map(session_source_from_proto)
        .transpose()?
        .unwrap_or(SessionSource::Unknown);
    let thread_id = ThreadId::from_string(&thread.thread_id).map_err(|err| {
        ThreadStoreError::InvalidRequest {
            message: format!("remote thread store returned invalid thread_id: {err}"),
        }
    })?;
    let forked_from_id = thread
        .forked_from_id
        .as_deref()
        .map(ThreadId::from_string)
        .transpose()
        .map_err(|err| ThreadStoreError::InvalidRequest {
            message: format!("remote thread store returned invalid forked_from_id: {err}"),
        })?;

    Ok(StoredThread {
        thread_id,
        rollout_path: None,
        forked_from_id,
        preview: thread.preview,
        name: thread.name,
        model_provider: thread.model_provider,
        model: thread.model,
        reasoning_effort: thread
            .reasoning_effort
            .as_deref()
            .map(parse_reasoning_effort)
            .transpose()?,
        created_at: datetime_from_unix(thread.created_at)?,
        updated_at: datetime_from_unix(thread.updated_at)?,
        archived_at: thread.archived_at.map(datetime_from_unix).transpose()?,
        cwd: PathBuf::from(thread.cwd),
        cli_version: thread.cli_version,
        source,
        agent_nickname: thread.agent_nickname,
        agent_role: thread.agent_role,
        agent_path: thread.agent_path,
        git_info: thread.git_info.map(git_info_from_proto),
        approval_mode: AskForApproval::OnRequest,
        sandbox_policy: SandboxPolicy::ReadOnly {
            access: ReadOnlyAccess::FullAccess,
            network_access: false,
        },
        token_usage: None,
        first_user_message: None,
        history: None,
    })
}

fn datetime_from_unix(timestamp: i64) -> ThreadStoreResult<DateTime<Utc>> {
    DateTime::from_timestamp(timestamp, 0).ok_or_else(|| ThreadStoreError::InvalidRequest {
        message: format!("remote thread store returned invalid timestamp: {timestamp}"),
    })
}

fn session_source_from_proto(source: &proto::SessionSource) -> ThreadStoreResult<SessionSource> {
    let kind = proto::SessionSourceKind::try_from(source.kind).unwrap_or_default();
    Ok(match kind {
        proto::SessionSourceKind::Unknown => SessionSource::Unknown,
        proto::SessionSourceKind::Cli => SessionSource::Cli,
        proto::SessionSourceKind::Vscode => SessionSource::VSCode,
        proto::SessionSourceKind::Exec => SessionSource::Exec,
        proto::SessionSourceKind::AppServer => SessionSource::Mcp,
        proto::SessionSourceKind::Custom => {
            SessionSource::Custom(source.custom.clone().unwrap_or_default())
        }
        proto::SessionSourceKind::SubAgentReview => SessionSource::SubAgent(SubAgentSource::Review),
        proto::SessionSourceKind::SubAgentCompact => {
            SessionSource::SubAgent(SubAgentSource::Compact)
        }
        proto::SessionSourceKind::SubAgentThreadSpawn => {
            let parent_thread_id = source
                .sub_agent_parent_thread_id
                .as_deref()
                .map(ThreadId::from_string)
                .transpose()
                .map_err(|err| ThreadStoreError::InvalidRequest {
                    message: format!(
                        "remote thread store returned invalid sub-agent parent thread id: {err}"
                    ),
                })?
                .ok_or_else(|| ThreadStoreError::InvalidRequest {
                    message: "remote thread store omitted sub-agent parent thread id".to_string(),
                })?;
            SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
                parent_thread_id,
                depth: source.sub_agent_depth.unwrap_or_default(),
                agent_path: source
                    .sub_agent_path
                    .clone()
                    .map(AgentPath::from_string)
                    .transpose()
                    .map_err(|message| ThreadStoreError::InvalidRequest { message })?,
                agent_nickname: source.sub_agent_nickname.clone(),
                agent_role: source.sub_agent_role.clone(),
            })
        }
        proto::SessionSourceKind::SubAgentMemoryConsolidation => {
            SessionSource::SubAgent(SubAgentSource::MemoryConsolidation)
        }
        proto::SessionSourceKind::SubAgentOther => SessionSource::SubAgent(SubAgentSource::Other(
            source.sub_agent_other.clone().unwrap_or_default(),
        )),
    })
}

fn git_info_from_proto(info: proto::GitInfo) -> GitInfo {
    GitInfo {
        commit_hash: info.sha.as_deref().map(GitSha::new),
        branch: info.branch,
        repository_url: info.origin_url,
    }
}

fn parse_reasoning_effort(value: &str) -> ThreadStoreResult<ReasoningEffort> {
    ReasoningEffort::from_str(value).map_err(|message| ThreadStoreError::InvalidRequest {
        message: format!("remote thread store returned {message}"),
    })
}

#[cfg(test)]
mod tests {
    use super::proto::thread_store_server;
    use super::proto::thread_store_server::ThreadStoreServer;
    use super::*;
    use pretty_assertions::assert_eq;
    use tonic::Request;
    use tonic::Response;
    use tonic::Status;
    use tonic::transport::Server;

    #[derive(Default)]
    struct TestServer;

    #[tonic::async_trait]
    impl thread_store_server::ThreadStore for TestServer {
        async fn list_threads(
            &self,
            request: Request<proto::ListThreadsRequest>,
        ) -> Result<Response<proto::ListThreadsResponse>, Status> {
            let request = request.into_inner();
            assert_eq!(request.page_size, 2);
            assert_eq!(request.cursor.as_deref(), Some("cursor-1"));
            assert_eq!(
                proto::ThreadSortKey::try_from(request.sort_key),
                Ok(proto::ThreadSortKey::UpdatedAt)
            );
            assert_eq!(request.archived, true);
            assert_eq!(request.search_term.as_deref(), Some("needle"));
            assert_eq!(
                request.model_provider_filter,
                Some(proto::ModelProviderFilter {
                    values: vec!["openai".to_string()],
                })
            );
            assert_eq!(request.allowed_sources.len(), 1);
            assert_eq!(
                proto::SessionSourceKind::try_from(request.allowed_sources[0].kind),
                Ok(proto::SessionSourceKind::Cli)
            );

            Ok(Response::new(proto::ListThreadsResponse {
                threads: vec![proto::StoredThread {
                    thread_id: "11111111-1111-1111-1111-111111111111".to_string(),
                    forked_from_id: None,
                    preview: "hello".to_string(),
                    name: Some("named thread".to_string()),
                    model_provider: "openai".to_string(),
                    model: Some("gpt-5".to_string()),
                    created_at: 100,
                    updated_at: 200,
                    archived_at: Some(300),
                    cwd: "/workspace".to_string(),
                    cli_version: "1.2.3".to_string(),
                    source: Some(proto::SessionSource {
                        kind: proto::SessionSourceKind::Cli.into(),
                        ..Default::default()
                    }),
                    git_info: Some(proto::GitInfo {
                        sha: Some("abc123".to_string()),
                        branch: Some("main".to_string()),
                        origin_url: Some("https://example.test/repo.git".to_string()),
                    }),
                    agent_nickname: None,
                    agent_role: None,
                    agent_path: None,
                    reasoning_effort: Some("medium".to_string()),
                }],
                next_cursor: Some("cursor-2".to_string()),
            }))
        }
    }

    #[tokio::test]
    async fn list_threads_calls_remote_service() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let addr = listener.local_addr().expect("test server addr");
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(ThreadStoreServer::new(TestServer))
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                    async {
                        let _ = shutdown_rx.await;
                    },
                )
                .await
        });

        let store = RemoteThreadStore::new(format!("http://{addr}"));
        let page = store
            .list_threads(ListThreadsParams {
                page_size: 2,
                cursor: Some("cursor-1".to_string()),
                sort_key: ThreadSortKey::UpdatedAt,
                allowed_sources: vec![SessionSource::Cli],
                model_providers: Some(vec!["openai".to_string()]),
                archived: true,
                search_term: Some("needle".to_string()),
            })
            .await
            .expect("list threads");

        assert_eq!(page.next_cursor.as_deref(), Some("cursor-2"));
        assert_eq!(page.items.len(), 1);
        let item = &page.items[0];
        assert_eq!(
            item.thread_id.to_string(),
            "11111111-1111-1111-1111-111111111111"
        );
        assert_eq!(item.name.as_deref(), Some("named thread"));
        assert_eq!(item.preview, "hello");
        assert_eq!(item.model_provider, "openai");
        assert_eq!(item.model.as_deref(), Some("gpt-5"));
        assert_eq!(item.created_at.timestamp(), 100);
        assert_eq!(item.updated_at.timestamp(), 200);
        assert_eq!(item.archived_at.map(|ts| ts.timestamp()), Some(300));
        assert_eq!(item.cwd, PathBuf::from("/workspace"));
        assert_eq!(item.cli_version, "1.2.3");
        assert_eq!(item.source, SessionSource::Cli);
        assert_eq!(item.reasoning_effort, Some(ReasoningEffort::Medium));
        assert_eq!(
            item.git_info.as_ref().and_then(|git| git.branch.as_deref()),
            Some("main")
        );

        let _ = shutdown_tx.send(());
        server.await.expect("join server").expect("server");
    }
}
