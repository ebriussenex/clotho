use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::{
    app::{AppErr, CommandError, ErrorKind, Request},
    transport::{Transport, TransportErr},
};

type PartResult<R> = (String, Result<R, TransportErr>);

/// Forwards of the parts of one command to their owners (other nodes e.g. peers), running concurrently.
///
/// The forwards are owned by this value: dropping it aborts them.
pub struct FanOut<Q: Request> {
    tasks: JoinSet<PartResult<Q::Response>>,
}

impl<Q: Request> FanOut<Q> {
    /// Starts forwarding every `(owner, part)`, as the request built from the part by `request`.
    pub fn scatter<'a, C>(
        transport: &Transport,
        token: &CancellationToken,
        parts: impl IntoIterator<Item = (&'a str, C)>,
        request: impl Fn(C) -> Q,
    ) -> Self {
        let mut tasks = JoinSet::new();
        for (owner, items) in parts {
            let transport = transport.clone();
            let token = token.clone();
            let owner = owner.to_owned();
            let request = request(items);
            tasks.spawn(async move {
                let result = transport.forward(&token, &owner, request).await;
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
    pub async fn join(mut self) -> Result<Vec<Result<Q::Response, CommandError>>, AppErr> {
        let mut parts = Vec::with_capacity(self.tasks.len());
        while let Some(joined) = self.tasks.join_next().await {
            let (owner, result) = joined?;
            parts.push(owner_reply(&owner, result)?);
        }
        Ok(parts)
    }
}

/// Turns the outcome of a forward to `owner` into its response or the error to reply to the
/// client.
///
/// - [`TransportErr::Timeout`] becomes [`ErrorKind::Timeout`].
/// - [`TransportErr::Remote`] is passed on as is.
/// - [`TransportErr::UnexpectedResponse`] becomes [`ErrorKind::Internal`].
///
/// # Errors
///
/// [`AppErr::Transport`] for the other transport errors: [`TransportErr::Cancelled`] when the
/// node is stopping, the rest when it is broken.
pub fn owner_reply<R>(
    owner: &str,
    result: Result<R, TransportErr>,
) -> Result<Result<R, CommandError>, AppErr> {
    match result {
        Ok(response) => Ok(Ok(response)),
        Err(TransportErr::Timeout(_)) => Ok(Err(CommandError::new(
            ErrorKind::Timeout,
            format!("owner {owner} did not reply in time"),
        ))),
        Err(TransportErr::Remote(error)) => {
            tracing::warn!(owner, ?error, "owner replied with an error");
            Ok(Err(error))
        }
        Err(TransportErr::UnexpectedResponse) => Ok(Err(CommandError::new(
            ErrorKind::Internal,
            format!("unexpected reply from owner {owner}"),
        ))),
        Err(e) => Err(e.into()),
    }
}

#[cfg(test)]
mod tests {
    use crate::app::SendRes;

    use super::*;

    fn reply(
        result: Result<SendRes, TransportErr>,
    ) -> Result<Result<SendRes, CommandError>, AppErr> {
        owner_reply("n2", result)
    }

    #[test]
    fn owner_reply_of_expected_type_is_accepted() {
        let result = reply(Ok(SendRes { offset: 3 }));
        assert!(matches!(result, Ok(Ok(SendRes { offset: 3 }))));
    }

    #[test]
    fn owner_reply_of_wrong_type_is_internal_error() {
        let result = reply(Err(TransportErr::UnexpectedResponse));
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
