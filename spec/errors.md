# Errors

Failures are replied with the Maelstrom `error` message, to clients and to nodes alike:

```json
{"type": "error", "msg_id": 9, "in_reply_to": 1, "code": 11, "text": "node is overloaded"}
```

Inside the node an error has a kind ([`ErrorKind`](../src/app/response.rs)); Maelstrom codes
appear only on the wire ([`src/message/error_code.rs`](../src/message/error_code.rs)).

## Kinds

*Definite* means the request was certainly not applied, *indefinite* that it may or may not
have been. A client may safely repeat a request after a definite error; after an indefinite
one, the outcome is unknown.

| Code | Kind | Definite | Retried by nodes | When |
|---|---|---|---|---|
| 0 `timeout` | `Timeout` | no | no | the owner of a key didn't reply before the deadline; a commit applied on some owners only |
| 11 `temporarily-unavailable` | `Unavailable` | yes | yes | the node is overloaded (command queue full) or shutting down |
| 12 `malformed-request` | `Protocol` | yes | no | the message couldn't be decoded |
| 13 `crash` | `Internal` | no | no | a bug, e.g. an owner replied with a response of a wrong type |

Incoming code 10 (`not-supported`) is read as `Protocol`; any other unknown code as
`Internal`.

## Per operation

What the client gets when owners fail (see [client protocol](client-protocol.md)):

| Operation | One of several owners fails |
|---|---|
| `send` | the owner's error, or `timeout` |
| `poll` | `poll_ok` without that owner's keys |
| `list_committed_offsets` | the owner's error: the whole request fails |
| `commit_offsets` | the owner's error if nothing was applied anywhere, otherwise `timeout` ("partially applied"): a commit is not atomic across owners |

Repeating a commit is always safe: offsets never move back.

## Dropped without a reply

| Message | Why |
|---|---|
| undecodable, without readable `src` and `msg_id` | nobody to answer |
| request without `msg_id` | nothing to reply to |
| reply without `in_reply_to` | can't be matched to a request |
| reply nobody waits for | its request already completed or timed out |
| client-protocol reply (`send_ok`, ...) from a node | nodes never send client requests |
| retry of a `fwd` still executing, or no longer awaited | see [deduplication](internode-protocol.md#deduplication-on-the-owner) |
