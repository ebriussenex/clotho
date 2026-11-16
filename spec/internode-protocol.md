# Internode protocol

How a node forwards the part of a [client request](client-protocol.md) that belongs to
another node, the key's *owner* (see [architecture](architecture.md#ownership)). It is a
protocol of its own, separate from the client one, so it can later move to TCP without
touching the client side.

Code: [`src/transport/`](../src/transport/) (retries, deduplication),
[`src/message/`](../src/message/) (conversions),
[`src/maelstrom/protocol/wire.rs`](../src/maelstrom/protocol/wire.rs) (wire types).

## `fwd`

A request wraps the client command, with its keys narrowed to the owner's part:

```json
{"src": "n1", "dest": "n2", "body": {
  "type": "fwd", "msg_id": 42, "in_reply_to": null,
  "request_id": {"incarnation": 1794281168921545, "seq": 17},
  "watermark": 15,
  "payload": {"type": "send", "key": "k1", "msg": "Nw=="}}}
```

| Field | Meaning |
|---|---|
| `msg_id` | new for every attempt; the reply is matched by it |
| `request_id` | the same for all attempts of one request: the deduplication key |
| `request_id.incarnation` | the sender's start time in µs; a restarted sender never reuses ids |
| `request_id.seq` | per-sender counter of requests |
| `watermark` | the lowest `seq` the sender still waits for; the owner may forget replies below it |
| `payload` | `send`, `poll`, `commit_offsets` or `list_committed_offsets`, fields as in the client protocol |

Messages are opaque bytes between nodes, so `msg` in `send` is base64 (`"Nw=="` is the JSON
text `7` the client sent).

## `fwd_ok`

A successful reply nests the response the same way:

```json
{"type": "fwd_ok", "msg_id": 5, "in_reply_to": 42,
 "payload": {"type": "send_ok", "offset": 3}}

{"type": "fwd_ok", "msg_id": 6, "in_reply_to": 43,
 "payload": {"type": "poll_ok", "msgs": {"k1": [3, ["Nw==", "OQ=="]]}}}

{"type": "fwd_ok", "msg_id": 7, "in_reply_to": 44,
 "payload": {"type": "commit_offsets_ok"}}

{"type": "fwd_ok", "msg_id": 8, "in_reply_to": 45,
 "payload": {"type": "list_committed_offsets_ok", "offsets": {"k1": 1001}}}
```

`poll_ok` between nodes is compact: per key the offset of the first message and the list of
messages. Only the client protocol numbers every message.

A failure is the same `error` reply as for clients, see [errors](errors.md).

## Retries

The sender retries until a deadline:

| Setting | Default |
|---|---|
| attempt timeout | 300 ms |
| deadline for all attempts | 2 s |
| pause between attempts | 20–200 ms, exponential with jitter |

- Every attempt has a new `msg_id` and the same `request_id`. A late reply to any earlier
  attempt completes the request.
- `temporarily-unavailable` (11) is retried like a timeout. Any other error is final and
  goes to the client.
- After the deadline the client gets `timeout` (0): the request may or may not have been
  executed.

## Deduplication on the owner

The owner keeps a reply cache per sender, keyed by `request_id`:

| Incoming `fwd` | Action |
|---|---|
| new `seq` | execute, reply, remember the reply |
| `seq` already answered | send the remembered reply again, don't execute |
| `seq` still executing | drop the attempt; the reply to the first one will do |
| `seq` below the cached watermark, or an older `incarnation` | drop: the sender no longer waits |

- A higher `watermark` evicts the remembered replies below it, so the cache holds only
  requests the sender still waits for.
- A newer `incarnation` resets the sender's cache.
- An error reply is not remembered: a retry of a failed request is executed again.
- `poll` and `list_committed_offsets` are read-only and skip the cache entirely: executing a
  retry twice is harmless, and `poll` replies are too big to keep.

## Rules

- A `fwd` is executed locally only and is never forwarded further.
- A node answers a `fwd` with `fwd_ok` or `error`. Plain client replies (`send_ok`, ...) from
  another node are dropped, since a node never sends client requests.
