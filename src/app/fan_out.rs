use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::{
    app::{AppErr, Command, CommandError, CommandResponse, ErrorKind},
    transport::{Transport, TransportErr},
};

type PartResult = (String, Result<CommandResponse, TransportErr>);

/// Forwards of the parts of one command to their owners (other nodes e.g. peers), running concurrently.
///
/// The forwards are owned by this value: dropping it aborts them.
pub struct FanOut {
    tasks: JoinSet<PartResult>,
}

impl FanOut {
    /// Starts forwarding every `(owner, part)`, as the command built from the part by `command`.
    pub fn scatter<'a, C>(
        transport: &Transport,
        token: &CancellationToken,
        parts: impl IntoIterator<Item = (&'a str, C)>,
        command: impl Fn(C) -> Command,
    ) -> Self {
        let mut tasks = JoinSet::new();
        for (owner, items) in parts {
            let transport = transport.clone();
            let token = token.clone();
            let owner = owner.to_owned();
            let command = command(items);
            tasks.spawn(async move {
                let result = transport.forward(&token, &owner, command).await;
                (owner, result)
            });
        }
        Self { tasks }
    }

    /// Waits for every part; each one ends within [`RpcConfig::deadline`](crate::transport::RpcConfig::deadline).
    ///
    /// A part that failed for the client (timeout, error from the owner, a response of a wrong
    /// type) is an `Err` in the returned list, see [`owner_reply`].
    ///
    /// # Errors
    ///
    /// The first error that stops the handler: [`AppErr::Transport`] with
    /// [`TransportErr::Cancelled`] when the node is stopping, other [`AppErr`]s when the node is
    /// broken, [`AppErr::Handler`] if a forward task panicked.
    pub async fn join<R>(mut self) -> Result<Vec<Result<R, CommandError>>, AppErr>
    where
        R: TryFrom<CommandResponse, Error = CommandResponse>,
    {
        let mut parts = Vec::with_capacity(self.tasks.len());
        while let Some(joined) = self.tasks.join_next().await {
            let (owner, result) = joined?;
            parts.push(owner_reply(&owner, result)?);
        }
        Ok(parts)
    }
}

/// Turns the outcome of a forward to `owner` into a response of type `R` or the error to
/// reply to the client.
///
/// - A response of another type is a bug: logged, and becomes [`ErrorKind::Internal`].
/// - [`TransportErr::Timeout`] becomes [`ErrorKind::Timeout`].
/// - [`TransportErr::Remote`] is passed on as is.
///
/// # Errors
///
/// [`AppErr::Transport`] for the other transport errors: [`TransportErr::Cancelled`] when the
/// node is stopping, the rest when it is broken.
pub fn owner_reply<R>(
    owner: &str,
    result: Result<CommandResponse, TransportErr>,
) -> Result<Result<R, CommandError>, AppErr>
where
    R: TryFrom<CommandResponse, Error = CommandResponse>,
{
    match result {
        Ok(response) => Ok(R::try_from(response).map_err(|response| {
            tracing::error!(
                owner,
                ?response,
                "owner replied with a response of a wrong type"
            );
            CommandError::new(
                ErrorKind::Internal,
                format!("unexpected reply from owner {owner}"),
            )
        })),
        Err(TransportErr::Timeout(_)) => Ok(Err(CommandError::new(
            ErrorKind::Timeout,
            format!("owner {owner} did not reply in time"),
        ))),
        Err(TransportErr::Remote(error)) => {
            tracing::warn!(owner, ?error, "owner replied with an error");
            Ok(Err(error))
        }
        Err(e) => Err(e.into()),
    }
}

/// Error to reply to the client when forwarding to `owner` failed; `None` when the node is
/// stopping and nobody is to be answered.
///
/// # Errors
///
/// [`AppErr::Transport`] if the transport is broken.
pub fn forward_failure(owner: &str, error: TransportErr) -> Result<Option<CommandError>, AppErr> {
    match error {
        TransportErr::Timeout(_) => {
            tracing::warn!(owner, "owner did not reply in time");
            Ok(Some(CommandError::new(
                ErrorKind::Timeout,
                format!("owner {owner} did not reply in time"),
            )))
        }
        TransportErr::Remote(error) => {
            tracing::warn!(owner, ?error, "owner replied with an error");
            Ok(Some(error))
        }
        TransportErr::Cancelled => Ok(None),
        e => Err(e.into()),
    }
}

pub fn unexpected_response(owner: &str, response: &CommandResponse) -> CommandError {
    tracing::error!(
        owner,
        ?response,
        "owner replied with a response of a wrong type"
    );
    CommandError::new(
        ErrorKind::Internal,
        format!("unexpected reply from owner {owner}"),
    )
}

#[cfg(test)]
mod tests {
    use crate::app::{PollRes, SendRes};

    use super::*;

    fn reply(
        result: Result<CommandResponse, TransportErr>,
    ) -> Result<Result<SendRes, CommandError>, AppErr> {
        owner_reply("n2", result)
    }

    #[test]
    fn owner_reply_of_expected_type_is_accepted() {
        let result = reply(Ok(CommandResponse::Send(SendRes { offset: 3 })));
        assert!(matches!(result, Ok(Ok(SendRes { offset: 3 }))));
    }

    #[test]
    fn owner_reply_of_wrong_type_is_internal_error() {
        let wrong = CommandResponse::Poll(PollRes {
            msgs: Default::default(),
        });
        let result = reply(Ok(wrong));
        assert!(matches!(
            result,
            Ok(Err(CommandError {
                kind: ErrorKind::Internal,
                ..
            }))
        ));
    }

    #[test]
    fn owner_timeout_and_remote_error_go_to_client() {
        let timeout = reply(Err(TransportErr::Timeout("forward reply")));
        assert!(matches!(
            timeout,
            Ok(Err(CommandError {
                kind: ErrorKind::Timeout,
                ..
            }))
        ));

        let remote = reply(Err(TransportErr::Remote(error(ErrorKind::Protocol))));
        assert!(matches!(
            remote,
            Ok(Err(CommandError {
                kind: ErrorKind::Protocol,
                ..
            }))
        ));
    }

    #[test]
    fn cancelled_forward_stops_the_handler() {
        let result = reply(Err(TransportErr::Cancelled));
        assert!(matches!(
            result,
            Err(AppErr::Transport(TransportErr::Cancelled))
        ));
    }

    fn error(kind: ErrorKind) -> CommandError {
        CommandError::new(kind, "x")
    }
}
