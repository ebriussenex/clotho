use crate::app::{
    Command, CommandResponse, CommitOffsetsCommand, CommitOffsetsRes, ListCommittedOffsetsCommand,
    ListCommittedOffsetsRes, PollCommand, PollRes, SendCommand, SendRes,
};

/// A command together with the only response it can be answered with.
///
/// Ties a command to its response at compile time: forwarding a [`PollCommand`] yields a
/// [`PollRes`], and a handler of a [`PollCommand`] can reply only a [`PollRes`]. The one place
/// left to check at run time is a reply from another node, see
/// [`Transport::forward`](crate::transport::Transport::forward).
pub trait Request: Into<Command> + Send + 'static {
    type Response: Into<CommandResponse>
        + TryFrom<CommandResponse, Error = CommandResponse>
        + Send
        + 'static;
}

/// Pairs the command and the response of one variant of [`Command`] and [`CommandResponse`].
macro_rules! request {
    ($variant:ident: $command:ty => $response:ty) => {
        impl Request for $command {
            type Response = $response;
        }

        impl From<$command> for Command {
            fn from(command: $command) -> Self {
                Command::$variant(command)
            }
        }

        impl From<$response> for CommandResponse {
            fn from(response: $response) -> Self {
                CommandResponse::$variant(response)
            }
        }

        impl TryFrom<CommandResponse> for $response {
            type Error = CommandResponse;

            fn try_from(response: CommandResponse) -> Result<Self, Self::Error> {
                match response {
                    CommandResponse::$variant(response) => Ok(response),
                    response => Err(response),
                }
            }
        }
    };
}

request!(Send: SendCommand => SendRes);
request!(Poll: PollCommand => PollRes);
request!(CommitOffsets: CommitOffsetsCommand => CommitOffsetsRes);
request!(ListCommittedOffsets: ListCommittedOffsetsCommand => ListCommittedOffsetsRes);
