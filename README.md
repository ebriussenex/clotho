# clotho

A Kafka-style distributed log in Rust, built as a learning project around the
[Maelstrom](https://github.com/jepsen-io/maelstrom) `kafka` workload from Fly.io's
[distributed systems challenges](https://fly.io/dist-sys/) (formerly *Gossip Glomers*).

The node talks newline-delimited JSON over stdin/stdout. Maelstrom, built on Jepsen, runs a
cluster of nodes, drives clients against it, injects network partitions and checks the
recorded history for consistency anomalies.

## What it does

Clients append messages to named logs (keys), read them from an offset, and commit and read
consumer offsets:

| Operation | Semantics |
|---|---|
| `send` | append a message to a key's log, returns its offset |
| `poll` | read messages of several keys starting at given offsets |
| `commit_offsets` | store consumer offsets; an offset never moves back |
| `list_committed_offsets` | read committed offsets |

```json
{"type": "send", "msg_id": 1, "key": "k1", "msg": 7}
{"type": "send_ok", "msg_id": 5, "in_reply_to": 1, "offset": 1000}
```

Every key is owned by exactly one node. Any node accepts any request and forwards the parts
it doesn't own to their owners.

Specification:

- [Client protocol](spec/client-protocol.md): every message with examples.
- [Internode protocol](spec/internode-protocol.md): forwarding, retries, deduplication.
- [Errors](spec/errors.md): error codes and what each operation reports on failure.
- [Architecture](spec/architecture.md): components, ownership, request flows (diagrams).

## Distributed systems properties

**Partitioning without replication.** Keys are spread over the nodes by rendezvous hashing;
each key has a single copy on its owner. Ownership is static (the node list comes from
Maelstrom's `init`), so all nodes agree on it without coordination and there is never more
than one writer per key.

**Consistency.** The owner is the single writer of a key: it assigns offsets in order, so a
key's log has one total order. A `send` is acknowledged after the append, and every later
read goes to the same owner, so an acknowledged message is visible to all later polls
through any node. Internode requests are idempotent: retries carry a stable request id, and
the owner executes a write at most once. (Retries by the *client* are new requests; there is
no idempotent producer yet.)

**Partitions: consistency over availability (CP), per key.** A node cut off from a key's
owner does not serve that key from a stale or divergent copy. After retrying until a
deadline it answers `timeout`, an *indefinite* error: the write may or may not have
happened. Keys whose owners are reachable stay available. Each operation picks its own
trade-off (see [errors](spec/errors.md#per-operation)):

- `poll` returns what it could collect: a missing key only means "nothing new yet".
- `list_committed_offsets` fails as a whole rather than report a key as never committed.
- A commit applied on some owners only is reported as indefinite, never as "not applied".

**Without partitions: latency.** A request for a remote key costs one internode round trip;
multi-key requests fan out to owners concurrently.

**Fault tolerance.** None yet: everything is in memory and a key lives on one node, so a
stopped node loses its keys. The Maelstrom version used here (0.2.4) can inject network
partitions only, not node crashes.

### Maelstrom results

Every run below passes the `kafka` checker (`:valid? true`): no lost or duplicated writes, no
reordered or skipped offsets.

| Scenario | Ops | Availability | Indefinite sends | `send` p50 / p99 | Internode msgs per op | Worst realtime lag |
|---|---|---|---|---|---|---|
| 1 node, 1000 req/s | 19.9k | 99.97% | 0 | 0.1 / 0.3 ms | 0 | 0 s |
| 2 nodes, 1000 req/s | 19.8k | 99.96% | 0 | 0.2 / 0.5 ms | 1.25 | 0 s |
| 3 nodes, partitions, 200 req/s | 10.3k | 99.67% | 16 | 0.3 / 0.8 ms | 1.72 | 6.1 s |
| 3 nodes, partitions, 1000 req/s | 11.9k | 99.08% | 85 | 0.2 / 0.7 ms | 1.62 | 2.0 s |
| 3 nodes, partitions, 200 ms latency | 1.4k | 94.72% | 48 | 400 / 1479 ms | 1.63 | 0 s |
| 3 nodes, partitions, 50 ms latency, 60 clients, 2000 req/s | 36.2k | 97.84% | 577 | 100 / 132 ms | 1.45 | 3.5 s |

- **Availability**: the share of operations that completed successfully (Maelstrom's
  `ok-fraction`).
- **Indefinite sends**: sends answered with `timeout` while a partition separated the node
  from the key's owner.
- **Latency** of acknowledged sends, computed from the recorded history. With `--latency`,
  Maelstrom also delays client messages, so the client round trip is included.
- **Internode msgs per op**: messages between nodes per client operation, the cost of
  forwarding.
- **Worst realtime lag**: the longest a consumer stayed behind acknowledged sends, as
  reported by Maelstrom.

With 200 ms one-way latency every forward outlives its 300 ms attempt timeout and is retried
(up to 5 attempts). The network log of that run shows 322 writes answered again from the
owner's reply cache and **0 duplicate appends**.

## Design highlights

- **Reliable internode requests**: retries with jittered exponential backoff until a
  deadline, at-most-once execution of writes, a reply cache trimmed by a watermark the
  sender piggybacks on requests ([internode protocol](spec/internode-protocol.md)).
- **Explicit error model**: definite vs indefinite failures, mapped to Maelstrom codes in
  one place ([errors](spec/errors.md)).
- **Backpressure**: the input reader never waits for the application; when the bounded
  command queue is full the request is rejected as `temporarily-unavailable` and nodes retry
  it.
- **Storage**: one actor per key is the single writer of a segmented log, readers never
  lock (RCU through `ArcSwap`); committed offsets in a sharded `DashMap`
  ([architecture](spec/architecture.md#storage)).
- **Opaque messages**: bytes inside the node, JSON only at the client edge, base64 between
  nodes.
- **Cancellation and safety**: every long-running task observes a `CancellationToken`, no
  detached tasks, cleanup by drop guards; no panicking code paths.

## Running

Requirements: Rust (edition 2024) and Maelstrom 0.2.x (needs a JDK, Graphviz and gnuplot).

```sh
cargo build --release

# single node
maelstrom test -w kafka --bin target/release/clotho \
  --node-count 1 --concurrency 2n --time-limit 20 --rate 1000

# three nodes with network partitions
maelstrom test -w kafka --bin target/release/clotho \
  --node-count 3 --concurrency 2n --time-limit 60 --rate 200 \
  --nemesis partition --nemesis-interval 10
```

Maelstrom writes every run to `store/`: the history, node logs, latency and rate plots, and
a timeline. Browse them with:

```sh
maelstrom serve    # http://localhost:8080
```

Unit tests cover retries, timeouts, cancellation, deduplication, out-of-order replies and
backpressure:

```sh
cargo test
```

## API documentation

The code is documented with rustdoc: every fallible function lists its errors, every error
type its variants. Build and open it with:

```sh
cargo doc --no-deps --document-private-items --open
```
