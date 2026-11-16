# Architecture

## Components

A node is three tasks connected by channels, plus one storage actor per key.

```mermaid
flowchart LR
    stdin([stdin]) --> reader
    subgraph clotho["node"]
        reader["reader task<br/>decode, classify"]
        queue[("command queue<br/>bounded")]
        app["app<br/>handlers, at most max_in_flight"]
        transport["transport<br/>retries, deduplication"]
        writer["writer task"]
        logs[("logs<br/>actor per key")]
        offsets[("committed offsets")]

        reader -- "commands (try_send)" --> queue
        reader -- "full: temporarily-unavailable" --> transport
        reader -- "replies from nodes" --> transport
        queue --> app
        app -- "local part" --> logs
        app -- "local part" --> offsets
        app -- "remote parts, replies" --> transport
        transport -- "outgoing messages" --> writer
    end
    writer --> stdout([stdout])
```

| Module | Responsibility |
|---|---|
| [`src/io`](../src/io/) | message I/O; production uses stdin/stdout |
| [`src/maelstrom/protocol`](../src/maelstrom/protocol/) | wire types, handshake, JSON and base64 value codecs |
| [`src/message`](../src/message/) | wire ⇄ application types, error codes |
| [`src/node.rs`](../src/node.rs) | task lifecycle, channels, dispatch, backpressure |
| [`src/app`](../src/app/) | request handling, ownership, fan-out to owners |
| [`src/transport`](../src/transport/) | outbound requests: retries, deadlines, pending replies, deduplication |
| [`src/logs`](../src/logs/) | segmented in-memory log storage |
| [`src/offsets`](../src/offsets/) | committed offsets |

- **The reader never waits for the app.** Commands go to a bounded queue with `try_send`;
  when it is full the request is rejected as `temporarily-unavailable` and the reader moves
  on. Replies from other nodes therefore always get through, even under overload.
- **Cancellation.** All tasks observe one `CancellationToken`. Forwards of a request are owned
  by its handler and aborted with it; there are no detached tasks.

## Ownership

Every key has exactly one owner, chosen by rendezvous (highest random weight) hashing:

```
owner(key) = argmax over nodes n of wyhash(n, wyhash(key, seed))
```

- Every node computes the same owner from the node list of the Maelstrom `init`, with no
  coordination.
- Adding or removing a node moves only the keys that end up on, or were on, that node.
- The owner stores the key's log and its committed offset.

Code: [`src/app/ownership.rs`](../src/app/ownership.rs).

## Forwarding a request

A request for a key owned by another node is forwarded over the
[internode protocol](internode-protocol.md). Retries carry the same `request_id`, so the
owner executes the request once:

```mermaid
sequenceDiagram
    participant C as client
    participant A as n1 (any node)
    participant B as n2 (owner of k1)

    C->>A: send k1 = 7
    A->>B: fwd msg 42, request (i, 17)
    Note over B: append k1, offset 3,<br/>remember reply for (i, 17)
    Note over A: no reply within 300 ms
    A->>B: fwd msg 43, request (i, 17)
    Note over B: (i, 17) answered:<br/>replay, don't append again
    B-->>A: fwd_ok to msg 42, offset 3 (late)
    B-->>A: fwd_ok to msg 43, offset 3
    A-->>C: send_ok offset 3
    Note over A: the first reply completes the request,<br/>the second is ignored
```

## Multi-key requests

`poll`, `commit_offsets` and `list_committed_offsets` are split by owner. Remote parts are
forwarded concurrently and the local part runs meanwhile; then the results are merged.

```mermaid
sequenceDiagram
    participant C as client
    participant A as n1
    participant B as n2
    participant D as n3

    C->>A: poll a, b, c
    par remote parts
        A->>B: fwd poll b
    and
        A->>D: fwd poll c
    end
    Note over A: read a locally
    B-->>A: fwd_ok poll_ok b
    Note over D: partitioned away:<br/>retries until the deadline
    A-->>C: poll_ok a, b
```

How each operation treats a failed owner is in [errors](errors.md#per-operation).

## Storage

- **Logs.** One actor per key is the single writer: it assigns offsets and appends.
  Readers never lock: sealed segments are immutable and published with `ArcSwap` (RCU), the
  active segment is an append-only lock-free vector.
- **Committed offsets.** A sharded `DashMap`; an update keeps the maximum, so an offset never
  moves back.
- **Messages are opaque bytes.** The client's JSON value is converted only at the protocol
  edge; nodes pass bytes to each other as base64.

Everything is in memory: a node that stops loses its keys.
