use std::{collections::HashMap, sync::Arc};

use thiserror::Error;
use tokio::{
    sync::mpsc,
    task::{JoinError, JoinSet},
};
use tokio_util::sync::CancellationToken;

use crate::{
    app::{
        command::{CommitOffsetsCommand, ListCommittedOffsetsCommand},
        fan_out::{self, FanOut},
        ownership::{Ownership, Partitioned},
        response::{CommandError, ErrorKind},
    },
    logs::{StorageErr, Store},
    offsets::CommittedOffsets,
    transport::{Transport, TransportErr},
};

use super::command::{AppCommand, Command, CommandSource, PollCommand, ReplyContext, SendCommand};
use super::response::{
    CommandResponse, CommitOffsetsRes, ListCommittedOffsetsRes, PollRes, SendRes,
};

/// Errors that stop the app. Failures of single commands are replied to the client instead.
#[derive(Debug, Error)]
pub enum AppErr {
    /// The log storage failed other than by shutting down.
    #[error(transparent)]
    Storage(#[from] StorageErr),
    /// Internode transport failed; [`TransportErr::Cancelled`] only means the node is stopping.
    #[error(transparent)]
    Transport(#[from] TransportErr),
    /// A command handler or forward task panicked.
    #[error("command handler task failed: {0}")]
    Handler(#[from] JoinError),
}

pub struct App {
    logs: Store,
    transport: Transport,
    committed_offsets: CommittedOffsets,
    ownership: Ownership,
    poll_max_msgs_size: usize,
    max_in_flight: usize,
}

impl App {
    pub fn new(
        logs: Store,
        transport: Transport,
        committed_offsets: CommittedOffsets,
        ownership: Ownership,
        poll_max_msgs_size: usize,
        max_in_flight: usize,
    ) -> Self {
        Self {
            logs,
            transport,
            committed_offsets,
            ownership,
            poll_max_msgs_size,
            // 0 would never accept a command
            max_in_flight: max_in_flight.max(1),
        }
    }

    /// Handles commands from `command_rx` concurrently, at most `max_in_flight` at a time, until
    /// `token` is cancelled or the channel closes.
    ///
    /// # Errors
    ///
    /// The first [`AppErr`] of a handler. The other handlers are then cancelled and awaited;
    /// their errors are only logged.
    pub async fn run(
        self,
        token: CancellationToken,
        mut command_rx: mpsc::Receiver<AppCommand>,
    ) -> Result<(), AppErr> {
        let app = Arc::new(self);
        let handlers_token = token.child_token();
        let mut handlers = JoinSet::new();

        let result = loop {
            let has_capacity = handlers.len() < app.max_in_flight;

            tokio::select! {
                biased;
                _ = token.cancelled() => break Ok(()),
                Some(joined) = handlers.join_next() => {
                    if let Err(e) = joined.map_err(AppErr::from).flatten() {
                        break Err(e);
                    }
                }
                command = command_rx.recv(), if has_capacity => match command {
                    Some(command) => {
                        let app = Arc::clone(&app);
                        let token = handlers_token.clone();
                        handlers.spawn(async move { app.handle(&token, command).await });
                    }
                    None => break Ok(()),
                },
            }
        };

        if result.is_err() {
            handlers_token.cancel();
        }
        app.reject_queued(&mut command_rx).await;
        while let Some(joined) = handlers.join_next().await {
            if let Err(e) = joined.map_err(AppErr::from).flatten() {
                tracing::warn!(error = %e, "command handler failed while app was stopping");
            }
        }
        if app.logs.shutdown().await.is_err() {
            tracing::warn!("log storage did not finish accepted appends in time");
        }

        result
    }

    /// Answers the commands still queued with
    /// [`ErrorKind::Unavailable`]: they were not executed, so the sender may retry them.
    async fn reject_queued(&self, command_rx: &mut mpsc::Receiver<AppCommand>) {
        command_rx.close();
        while let Some(command) = command_rx.recv().await {
            let error = CommandError::new(ErrorKind::Unavailable, "node is shutting down");
            if let Err(e) = self.transport.reply_error(error, command.reply_ctx).await {
                tracing::warn!(error = %e, "can't reject a queued command");
                return;
            }
        }
    }

    async fn handle(&self, token: &CancellationToken, command: AppCommand) -> Result<(), AppErr> {
        let AppCommand {
            reply_ctx,
            kind,
            source,
        } = command;

        let result = match kind {
            Command::Send(command) => self.handle_send(token, source, reply_ctx, command).await,
            Command::Poll(command) => self.handle_poll(token, source, reply_ctx, command).await,
            Command::CommitOffsets(command) => {
                self.handle_commit_offsets(token, source, reply_ctx, command)
                    .await
            }
            Command::ListCommittedOffsets(command) => {
                self.handle_list_committed_offsets(token, source, reply_ctx, command)
                    .await
            }
        };

        match result {
            Err(AppErr::Transport(TransportErr::Cancelled)) => Ok(()),
            result => result,
        }
    }

    async fn handle_send(
        &self,
        token: &CancellationToken,
        source: CommandSource,
        reply_ctx: ReplyContext,
        command: SendCommand,
    ) -> Result<(), AppErr> {
        if source == CommandSource::Client && !self.ownership.is_local(&command.key) {
            let owner = self.ownership.owner(&command.key);
            let result = self
                .transport
                .forward(token, owner, Command::Send(command))
                .await;
            return match result {
                Ok(response @ CommandResponse::Send(_)) => {
                    Ok(self.transport.reply(response, reply_ctx).await?)
                }
                Ok(response) => {
                    let error = fan_out::unexpected_response(owner, &response);
                    Ok(self.transport.reply_error(error, reply_ctx).await?)
                }
                Err(e) => match fan_out::forward_failure(owner, e)? {
                    Some(error) => Ok(self.transport.reply_error(error, reply_ctx).await?),
                    None => Ok(()),
                },
            };
        }

        match self.logs.append(&command.key, command.msg).await {
            Ok(offset) => {
                let response = CommandResponse::Send(SendRes { offset });
                Ok(self.transport.reply(response, reply_ctx).await?)
            }
            Err(StorageErr::ShuttingDown) => {
                let error = CommandError::new(ErrorKind::Unavailable, "node is shutting down");
                Ok(self.transport.reply_error(error, reply_ctx).await?)
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn handle_poll(
        &self,
        token: &CancellationToken,
        source: CommandSource,
        reply_ctx: ReplyContext,
        command: PollCommand,
    ) -> Result<(), AppErr> {
        match source {
            CommandSource::Peer => {
                let msgs = self.logs.poll(command.offsets, self.poll_max_msgs_size);
                let response = CommandResponse::Poll(PollRes { msgs });
                Ok(self.transport.reply(response, reply_ctx).await?)
            }
            CommandSource::Client => {
                let parts: Partitioned<HashMap<_, _>> = self
                    .ownership
                    .partition(command.offsets, |(key, _)| key.as_str());
                let peer_poll = |offsets| Command::Poll(PollCommand { offsets });

                let remote = FanOut::scatter(&self.transport, token, parts.remote, peer_poll);
                let mut msgs = self.logs.poll(parts.local, self.poll_max_msgs_size);

                let remote_parts = remote.join::<PollRes>().await?;
                for part in remote_parts.into_iter().filter_map(Result::ok) {
                    msgs.extend(part.msgs);
                }

                let response = CommandResponse::Poll(PollRes { msgs });
                Ok(self.transport.reply(response, reply_ctx).await?)
            }
        }
    }

    async fn handle_list_committed_offsets(
        &self,
        token: &CancellationToken,
        source: CommandSource,
        reply_ctx: ReplyContext,
        command: ListCommittedOffsetsCommand,
    ) -> Result<(), AppErr> {
        match source {
            CommandSource::Peer => {
                let offsets = self.committed_offsets.list(command.keys);
                let response =
                    CommandResponse::ListCommittedOffsets(ListCommittedOffsetsRes { offsets });
                Ok(self.transport.reply(response, reply_ctx).await?)
            }
            CommandSource::Client => {
                let parts: Partitioned<Vec<_>> =
                    self.ownership.partition(command.keys, String::as_str);
                let peer_list =
                    |keys| Command::ListCommittedOffsets(ListCommittedOffsetsCommand { keys });

                let remote = FanOut::scatter(&self.transport, token, parts.remote, peer_list);
                let mut offsets = self.committed_offsets.list(parts.local);

                let remote_parts = remote.join::<ListCommittedOffsetsRes>().await?;
                for part in remote_parts {
                    match part {
                        Ok(part) => offsets.extend(part.offsets),
                        Err(error) => {
                            return Ok(self.transport.reply_error(error, reply_ctx).await?);
                        }
                    }
                }

                let response =
                    CommandResponse::ListCommittedOffsets(ListCommittedOffsetsRes { offsets });
                Ok(self.transport.reply(response, reply_ctx).await?)
            }
        }
    }

    async fn handle_commit_offsets(
        &self,
        token: &CancellationToken,
        source: CommandSource,
        reply_ctx: ReplyContext,
        command: CommitOffsetsCommand,
    ) -> Result<(), AppErr> {
        match source {
            CommandSource::Peer => {
                self.committed_offsets.commit(command.offsets);
                let response = CommandResponse::CommitOffsets(CommitOffsetsRes);
                Ok(self.transport.reply(response, reply_ctx).await?)
            }
            CommandSource::Client => {
                let parts: Partitioned<HashMap<_, _>> = self
                    .ownership
                    .partition(command.offsets, |(key, _)| key.as_str());
                let peer_commit =
                    |offsets| Command::CommitOffsets(CommitOffsetsCommand { offsets });

                let remote = FanOut::scatter(&self.transport, token, parts.remote, peer_commit);
                let applied_locally = !parts.local.is_empty();
                if applied_locally {
                    self.committed_offsets.commit(parts.local);
                }

                let mut applied_somewhere = applied_locally;
                let mut failures = Vec::new();
                for part in remote.join::<CommitOffsetsRes>().await? {
                    match part {
                        Ok(CommitOffsetsRes) => applied_somewhere = true,
                        Err(error) => failures.push(error),
                    }
                }

                match commit_error(failures, applied_somewhere) {
                    None => {
                        let response = CommandResponse::CommitOffsets(CommitOffsetsRes);
                        Ok(self.transport.reply(response, reply_ctx).await?)
                    }
                    Some(error) => {
                        tracing::warn!(?error, dest = %reply_ctx.dest, "commit failed on some owners");
                        Ok(self.transport.reply_error(error, reply_ctx).await?)
                    }
                }
            }
        }
    }
}

/// A commit split between owners is not atomic. A definite failure ("not applied") is reported
/// only if nothing was applied anywhere; otherwise the outcome of the commit is unknown.
fn commit_error(failures: Vec<CommandError>, applied_somewhere: bool) -> Option<CommandError> {
    if let Some(indefinite) = failures.iter().find(|error| !error.kind.is_definite()) {
        return Some(indefinite.clone());
    }

    let first = failures.into_iter().next()?;
    if applied_somewhere {
        return Some(CommandError::new(
            ErrorKind::Timeout,
            format!("partially applied: {}", first.text),
        ));
    }
    Some(first)
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, time::Duration};

    use tokio_util::bytes::Bytes;

    use crate::{
        app::{AppCommand, ListCommittedOffsetsRes},
        logs::StoreConfig,
        message::Outgoing,
        transport::{PeerRequest, RequestId, RpcConfig},
    };

    use super::*;

    /// App on `n1` in a two-node cluster, with the receiving end of its output.
    struct Harness {
        app: Arc<App>,
        transport: Transport,
        offsets: CommittedOffsets,
        ownership: Ownership,
        write_rx: mpsc::Receiver<Outgoing>,
    }

    fn harness() -> Harness {
        let (write_tx, write_rx) = mpsc::channel(16);
        let rpc = RpcConfig {
            attempt_timeout: Duration::from_millis(20),
            deadline: Duration::from_millis(100),
            min_backoff: Duration::from_millis(5),
            max_backoff: Duration::from_millis(5),
        };
        let transport = Transport::new(write_tx, rpc, 1);
        let offsets = CommittedOffsets::new();
        let app = App::new(
            Store::new(StoreConfig::default(), CancellationToken::new()),
            transport.clone(),
            offsets.clone(),
            Ownership::new("n1".to_owned(), ["n2".to_owned()], 0),
            100,
            8,
        );
        Harness {
            app: Arc::new(app),
            transport,
            offsets,
            ownership: Ownership::new("n1".to_owned(), ["n2".to_owned()], 0),
            write_rx,
        }
    }

    impl Harness {
        /// A key owned by `node`.
        fn key_of(&self, node: &str) -> String {
            (0..)
                .map(|i| format!("key-{i}"))
                .find(|key| self.ownership.owner(key) == node)
                .unwrap_or_default()
        }

        fn run(&self, kind: Command) -> tokio::task::JoinHandle<Result<(), AppErr>> {
            self.run_from(CommandSource::Client, kind)
        }

        fn run_from(
            &self,
            source: CommandSource,
            kind: Command,
        ) -> tokio::task::JoinHandle<Result<(), AppErr>> {
            let app = Arc::clone(&self.app);
            let request = (source == CommandSource::Peer).then_some(PeerRequest {
                id: RequestId {
                    incarnation: 1,
                    seq: 0,
                },
                watermark: 0,
                deduplicate: !kind.is_read_only(),
            });
            let command = AppCommand {
                reply_ctx: ReplyContext {
                    dest: "c1".to_owned(),
                    in_reply_to: 1,
                    request,
                },
                kind,
                source,
            };
            tokio::spawn(async move { app.handle(&CancellationToken::new(), command).await })
        }

        async fn append(&self, key: &str, msg: &'static [u8]) {
            self.app
                .logs
                .append(key, Bytes::from_static(msg))
                .await
                .expect("appended");
        }

        /// The command forwarded to `n2` and its `msg_id`.
        async fn forwarded(&mut self) -> (u64, Command) {
            match self.write_rx.recv().await {
                Some(Outgoing::Forward {
                    msg_id,
                    command,
                    dest,
                    ..
                }) => {
                    assert_eq!(dest, "n2");
                    (msg_id, command)
                }
                _ => panic!("expected a forward"),
            }
        }

        /// The reply to the client, skipping retries of forwards.
        async fn client_reply(&mut self) -> Outgoing {
            loop {
                match self.write_rx.recv().await {
                    Some(Outgoing::Forward { .. }) => continue,
                    Some(reply) => return reply,
                    None => panic!("output closed"),
                }
            }
        }
    }

    #[tokio::test]
    async fn commit_is_applied_on_every_owner() {
        let mut h = harness();
        let (local, remote) = (h.key_of("n1"), h.key_of("n2"));
        let task = h.run(Command::CommitOffsets(CommitOffsetsCommand {
            offsets: HashMap::from([(local.clone(), 5), (remote.clone(), 7)]),
        }));

        let (msg_id, command) = h.forwarded().await;
        assert!(matches!(
            command,
            Command::CommitOffsets(CommitOffsetsCommand { ref offsets })
                if offsets == &HashMap::from([(remote.clone(), 7)])
        ));
        h.transport
            .register_reply(msg_id, Ok(CommandResponse::CommitOffsets(CommitOffsetsRes)));

        assert!(matches!(
            h.client_reply().await,
            Outgoing::CommandReply {
                in_reply_to: 1,
                response: CommandResponse::CommitOffsets(_),
                ..
            }
        ));
        task.await.expect("handler").expect("no app error");

        let listed = h.offsets.list([local.clone(), remote]);
        // only the local part is stored here, the remote one lives on n2
        assert_eq!(listed, HashMap::from([(local, 5)]));
    }

    #[tokio::test]
    async fn commit_with_unresponsive_owner_is_indefinite() {
        let mut h = harness();
        let (local, remote) = (h.key_of("n1"), h.key_of("n2"));
        let task = h.run(Command::CommitOffsets(CommitOffsetsCommand {
            offsets: HashMap::from([(local, 5), (remote, 7)]),
        }));

        assert!(matches!(
            h.client_reply().await,
            Outgoing::ErrorReply {
                in_reply_to: 1,
                error: CommandError {
                    kind: ErrorKind::Timeout,
                    ..
                },
                ..
            }
        ));
        task.await.expect("handler").expect("no app error");
    }

    #[tokio::test]
    async fn list_merges_local_and_remote_offsets() {
        let mut h = harness();
        let (local, remote) = (h.key_of("n1"), h.key_of("n2"));
        h.offsets.commit([(local.clone(), 3)]);
        let task = h.run(Command::ListCommittedOffsets(ListCommittedOffsetsCommand {
            keys: vec![local.clone(), remote.clone()],
        }));

        let (msg_id, command) = h.forwarded().await;
        assert!(matches!(
            command,
            Command::ListCommittedOffsets(ListCommittedOffsetsCommand { ref keys })
                if keys == &vec![remote.clone()]
        ));
        h.transport.register_reply(
            msg_id,
            Ok(CommandResponse::ListCommittedOffsets(
                ListCommittedOffsetsRes {
                    offsets: HashMap::from([(remote.clone(), 9)]),
                },
            )),
        );

        match h.client_reply().await {
            Outgoing::CommandReply {
                response: CommandResponse::ListCommittedOffsets(res),
                ..
            } => assert_eq!(res.offsets, HashMap::from([(local, 3), (remote, 9)])),
            _ => panic!("expected list_committed_offsets_ok"),
        }
        task.await.expect("handler").expect("no app error");
    }

    #[tokio::test]
    async fn list_fails_when_owner_replies_with_error() {
        let mut h = harness();
        let remote = h.key_of("n2");
        let task = h.run(Command::ListCommittedOffsets(ListCommittedOffsetsCommand {
            keys: vec![remote],
        }));

        let (msg_id, _) = h.forwarded().await;
        h.transport.register_reply(
            msg_id,
            Err(CommandError::new(ErrorKind::Protocol, "bad request")),
        );

        assert!(matches!(
            h.client_reply().await,
            Outgoing::ErrorReply {
                error: CommandError {
                    kind: ErrorKind::Protocol,
                    ..
                },
                ..
            }
        ));
        task.await.expect("handler").expect("no app error");
    }

    fn poll(offsets: &[(&str, u64)]) -> Command {
        Command::Poll(PollCommand {
            offsets: offsets
                .iter()
                .map(|&(key, offset)| (key.to_owned(), offset))
                .collect(),
        })
    }

    fn msgs(entries: &[(&str, u64, &'static [u8])]) -> HashMap<String, (u64, Vec<Bytes>)> {
        entries
            .iter()
            .map(|&(key, from, msg)| (key.to_owned(), (from, vec![Bytes::from_static(msg)])))
            .collect()
    }

    #[tokio::test]
    async fn poll_merges_local_and_remote_messages() {
        let mut h = harness();
        let (local, remote) = (h.key_of("n1"), h.key_of("n2"));
        h.append(&local, b"l0").await;
        h.append(&local, b"l1").await;
        let task = h.run(poll(&[(&local, 1), (&remote, 0)]));

        let (msg_id, command) = h.forwarded().await;
        assert!(matches!(
            command,
            Command::Poll(PollCommand { ref offsets })
                if offsets == &HashMap::from([(remote.clone(), 0)])
        ));
        h.transport.register_reply(
            msg_id,
            Ok(CommandResponse::Poll(PollRes {
                msgs: msgs(&[(&remote, 0, b"r0")]),
            })),
        );

        match h.client_reply().await {
            Outgoing::CommandReply {
                response: CommandResponse::Poll(res),
                ..
            } => assert_eq!(res.msgs, msgs(&[(&local, 1, b"l1"), (&remote, 0, b"r0")])),
            _ => panic!("expected poll_ok"),
        }
        task.await.expect("handler").expect("no app error");
    }

    #[tokio::test]
    async fn poll_with_unresponsive_owner_returns_what_was_collected() {
        let mut h = harness();
        let (local, remote) = (h.key_of("n1"), h.key_of("n2"));
        h.append(&local, b"l0").await;
        let task = h.run(poll(&[(&local, 0), (&remote, 0)]));

        match h.client_reply().await {
            Outgoing::CommandReply {
                response: CommandResponse::Poll(res),
                ..
            } => assert_eq!(res.msgs, msgs(&[(&local, 0, b"l0")])),
            _ => panic!("expected poll_ok"),
        }
        task.await.expect("handler").expect("no app error");
    }

    #[tokio::test]
    async fn poll_from_peer_is_served_locally() {
        let mut h = harness();
        let (local, remote) = (h.key_of("n1"), h.key_of("n2"));
        h.append(&local, b"l0").await;
        let task = h.run_from(CommandSource::Peer, poll(&[(&local, 0), (&remote, 0)]));

        match h.write_rx.recv().await {
            Some(Outgoing::ForwardReply {
                response: CommandResponse::Poll(res),
                ..
            }) => assert_eq!(res.msgs, msgs(&[(&local, 0, b"l0")])),
            _ => panic!("expected a local reply to the peer, without forwarding"),
        }
        task.await.expect("handler").expect("no app error");
    }

    fn error(kind: ErrorKind) -> CommandError {
        CommandError::new(kind, "x")
    }

    #[test]
    fn commit_without_failures_succeeds() {
        assert!(commit_error(Vec::new(), true).is_none());
    }

    #[test]
    fn unapplied_commit_keeps_definite_failure() {
        let reply = commit_error(vec![error(ErrorKind::Protocol)], false);
        assert!(matches!(
            reply,
            Some(CommandError {
                kind: ErrorKind::Protocol,
                ..
            })
        ));
    }

    #[test]
    fn partially_applied_commit_is_indefinite() {
        let reply = commit_error(vec![error(ErrorKind::Protocol)], true);
        assert!(matches!(
            reply,
            Some(CommandError {
                kind: ErrorKind::Timeout,
                ..
            })
        ));
    }

    #[test]
    fn indefinite_failure_wins() {
        let reply = commit_error(
            vec![error(ErrorKind::Protocol), error(ErrorKind::Internal)],
            false,
        );
        assert!(matches!(
            reply,
            Some(CommandError {
                kind: ErrorKind::Internal,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn stopping_app_rejects_queued_commands() {
        let (write_tx, mut write_rx) = mpsc::channel(8);
        let transport = Transport::new(write_tx, RpcConfig::default(), 1);
        let app = App::new(
            Store::new(StoreConfig::default(), CancellationToken::new()),
            transport,
            CommittedOffsets::new(),
            Ownership::new("n1".to_owned(), [], 0),
            100,
            1,
        );
        let (command_tx, command_rx) = mpsc::channel(4);
        for in_reply_to in 1..=3 {
            let command = AppCommand {
                reply_ctx: ReplyContext {
                    dest: "c1".to_owned(),
                    in_reply_to,
                    request: None,
                },
                kind: poll(&[("k", 0)]),
                source: CommandSource::Client,
            };
            command_tx.send(command).await.expect("queued");
        }

        // stopped before it took any command
        let token = CancellationToken::new();
        token.cancel();
        app.run(token, command_rx).await.expect("no app error");

        for _ in 1..=3 {
            assert!(matches!(
                write_rx.recv().await,
                Some(Outgoing::ErrorReply {
                    error: CommandError {
                        kind: ErrorKind::Unavailable,
                        ..
                    },
                    ..
                })
            ));
        }
    }
}
