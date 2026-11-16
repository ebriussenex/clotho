# Client protocol

The protocol of the Maelstrom [`kafka` workload](https://github.com/jepsen-io/maelstrom/blob/main/doc/workloads.md#workload-kafka):
newline-delimited JSON over stdin/stdout. Every message has an envelope with `src`, `dest`
and a `body`; the body carries `type`, the request's `msg_id`, and in replies `in_reply_to`.

```json
{"src": "c1", "dest": "n1", "body": {"type": "send", "msg_id": 1, "key": "k1", "msg": 7}}
```

Any node accepts any request. Keys owned by other nodes are forwarded over the
[internode protocol](internode-protocol.md); the client always gets the reply from the node
it asked. Failures are [error replies](errors.md).

Wire types: [`src/maelstrom/protocol/wire.rs`](../src/maelstrom/protocol/wire.rs).

## `send`

Appends `msg` to the log of `key` and returns its offset. `msg` is any JSON value (Maelstrom
sends integers); the node stores it as opaque bytes.

```json
{"type": "send", "msg_id": 1, "key": "k1", "msg": 7}
{"type": "send_ok", "msg_id": 5, "in_reply_to": 1, "offset": 1000}
```

## `poll`

Reads messages of every key starting at the given offset, at most 100 per key. Every message
comes with its own offset.

```json
{"type": "poll", "msg_id": 2, "offsets": {"k1": 1000, "k2": 2000}}
{"type": "poll_ok", "msg_id": 6, "in_reply_to": 2,
 "msgs": {"k1": [[1000, 7], [1001, 9]], "k2": [[2000, 3]]}}
```

A key is missing from `msgs` when it has no messages at or after the offset, or when its
owner could not be reached: for `poll` both mean "nothing new yet", and the consumer simply
polls again.

## `commit_offsets`

Stores consumer offsets. An offset never moves back: committing an older offset is a no-op,
so retried and reordered commits are harmless.

```json
{"type": "commit_offsets", "msg_id": 3, "offsets": {"k1": 1001, "k2": 2000}}
{"type": "commit_offsets_ok", "msg_id": 7, "in_reply_to": 3}
```

## `list_committed_offsets`

Returns the committed offsets of the keys. Keys without a committed offset are left out.

```json
{"type": "list_committed_offsets", "msg_id": 4, "keys": ["k1", "k2", "k3"]}
{"type": "list_committed_offsets_ok", "msg_id": 8, "in_reply_to": 4,
 "offsets": {"k1": 1001, "k2": 2000}}
```

Unlike `poll`, it fails as a whole if any owner is unreachable: a missing key would read as
"nothing committed" and make the consumer start the key over.
