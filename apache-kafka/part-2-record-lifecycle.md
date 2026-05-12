# How One Record Travels Through Apache Kafka

## Where we are in the series

[Part 1](./part-1-kafka-internals.md) covered the cluster: producers, brokers, partitions, leaders, followers, consumer groups. A static picture. You can hold the architecture in your head now.

This article is about motion. We're going to trace a single record from `producer.send()` all the way to a committed offset — through the leader's log, through replication, through the durability checks, through `sendfile()` to the consumer, and into `__consumer_offsets`. Eleven steps, three phases, one diagram.

If anything in here is unfamiliar terminology — LEO, ISR, page cache, `sendfile` — I'll define it the first time it appears. By the end, every guarantee Kafka makes (durability, ordering, exactly-once) will map back to a specific step in this picture.

## The picture, all at once

![Sequence diagram showing Producer, Leader Broker, Follower Broker, Page Cache + Disk, and Consumer lifelines, with 11 numbered messages tracing a write through replication to a consumer commit](./images/kafka-write-sequence.png)
*One record from `producer.send()` to `OffsetCommit`. Five lifelines, eleven messages, three phases.*

Five vertical lifelines, each representing one actor:

- **Producer** — your application, calling `producer.send()`.
- **Leader Broker** — the broker that owns the target partition.
- **Follower Broker** — a broker holding a replica of the same partition.
- **Page Cache + Disk** — the kernel's file-system cache and the underlying storage. It gets its own lifeline because Kafka's performance and correctness both depend on how the leader and follower interact with it.
- **Consumer** — a consumer in some consumer group, polling the leader.

The diagram is divided into three phases — **Produce**, **Replicate**, **Consume** — separated by an explicit "ACK RETURNED" marker between steps 7 and 8. Eleven numbered messages thread through them. The rest of the article walks each one.

## Phase 1: Produce (steps 1–2)

Your application calls `producer.send(record)`. The producer client serializes the record, picks a partition (via `hash(key) mod N`, as covered in Part 1), and sends a **ProduceRequest** to the broker that currently leads that partition.

A ProduceRequest carries:

- The records themselves, packaged in a **record batch**. Producers don't ship individual records; they accumulate records destined for the same partition, batch them, and compress the batch as a single unit. This is one of the largest performance wins in Kafka.
- The producer's `acks` setting (covered in step 7).
- The producer's **producer ID (PID)** and a **monotonic sequence number** per partition — these are what enable idempotent retries. The broker tracks the last few sequence numbers it has seen from each PID; if a retry arrives with a sequence it has already accepted, it silently drops the duplicate and returns the original ACK.

The leader receives the batch, validates it, and **appends it to the active log segment** — not by writing directly to disk, but by writing to the **OS page cache**. The page cache is a kernel-managed region of RAM that mirrors recently-written file pages. The kernel decides when to flush dirty pages to physical disk based on its own write-back policy. From Kafka's perspective, the write is "done" the moment it lands in the page cache.

## Phase 2: Replicate (steps 3–5)

Here's the replication mechanic that surprises most newcomers: **followers don't passively wait. They poll.**

A follower broker periodically (and constantly) sends a **FetchRequest** to the leader for each partition it follows. The FetchRequest carries the follower's current **LEO (Log End Offset)** — the offset of the next record it would write — and asks for everything past that. The leader replies with the batch of new records the follower hasn't yet seen.

The follower receives the records and appends them to its own copy of the partition log, again via the page cache. Now the follower's LEO advances to match.

The leader keeps a running view of each follower's LEO. A follower is considered **in-sync** if its LEO is within `replica.lag.time.max.ms` (default 30 seconds) of the leader's LEO. The set of in-sync followers — plus the leader itself — is called the **ISR (In-Sync Replicas)**. A follower that falls too far behind is evicted from the ISR; when it catches back up, it rejoins.

The ISR is the durability boundary. Only ISR members are eligible to take over as leader, and only data replicated to the full ISR is considered durable.

## The High Watermark moves (step 6)

Two key offsets per partition:

- **LEO (Log End Offset)** — the offset of the next record to be appended on a given replica. Each replica has its own LEO.
- **HW (High Watermark)** — the offset that marks the boundary between "replicated to the full ISR" and "not yet fully replicated." Defined as `min(LEO across all ISR members)`.

The leader's HW advances every time the slowest follower in the ISR catches up. **Records below the HW are visible to consumers; records at or above the HW are not.**

This is the durability guarantee made physical. If a record is below the HW, the leader can crash, any ISR follower can be elected the new leader, and the record will still be there. Records above the HW only exist on the leader's disk and can be lost in a failover — so Kafka hides them.

## The producer gets an ACK — what `acks` actually controls (step 7)

The leader has appended the record. The followers in the ISR have caught up. The HW has advanced. Now the leader sends a **ProduceResponse** back to the producer.

When that ACK is returned is controlled by one setting: **`acks`**.

- **`acks=0`** — the leader doesn't ACK at all. The producer is fire-and-forget. If the leader crashes before the record makes it to disk, the record is gone and the producer never knows. Used only for non-critical telemetry.
- **`acks=1`** — the leader ACKs as soon as its *own* write to the page cache succeeds. Fast. But if the leader crashes before any follower has replicated the record, the record is lost.
- **`acks=all`** (also written `acks=-1`) — the leader waits until the full ISR has replicated the record, then ACKs. This is the durable choice. Combined with `min.insync.replicas ≥ 2`, it guarantees no data loss as long as at least two brokers stay healthy.

`min.insync.replicas` is the second knob. If the ISR shrinks below that threshold (say, two followers go offline), producers using `acks=all` start getting `NotEnoughReplicas` errors — the cluster is failing closed to protect durability, rather than silently accepting writes that aren't actually replicated.

For data that matters: **`acks=all` + `min.insync.replicas=2` + `enable.idempotence=true`**. That's the standard production setting.

## Phase 3: Consume (steps 8–9)

The consumer is in its own consumer group, polling the leader for one or more partitions. Its request is a **FetchRequest** — the *same* protocol followers use — carrying:

- `fetchOffset`: the next offset to read for each subscribed partition.
- `maxWaitMs`: how long to long-poll if there's no new data (default a few hundred ms).
- `isolationLevel`: either `read_uncommitted` (records up to the HW) or `read_committed` (records up to the **Last Stable Offset**, or LSO — the highest offset before any open transaction).

The broker reads records starting at `fetchOffset` and returns everything it can up to the visibility limit. Crucially, **the broker does this via `sendfile()`**.

`sendfile()` is a Linux syscall that copies bytes directly from a file descriptor (in this case, the page cache pages backing the log segment) to a network socket — *without ever copying through user space*. The broker doesn't decompress the batch, doesn't parse it, doesn't re-encode it. The consumer receives the *exact same bytes the producer sent*. Decompression and deserialization happen on the consumer side.

This is why Kafka can saturate a network link with very little CPU. The hot path is "page cache → NIC."

## Storage mechanics — why this is fast

The disk side is worth a closer look, because everything in Phase 2 and Phase 3 depends on it.

![Diagram of one partition directory on a broker, showing the active .log segment with records inside, a sparse .index file, a .timeindex file, a previous rolled segment, and a sendfile() arrow flowing to a consumer box](./images/kafka-storage.png)
*One partition on disk. The active `.log` receives sequential appends through the page cache; `sendfile()` streams bytes directly from the page cache to consumer sockets.*

Each partition is a directory of segment files:

- `.log` — the actual records.
- `.index` — a sparse map of offset → byte position in the `.log`. Sparse means "one entry every ~4 KB," not one per record. Lookups are: binary-search the index → seek to the nearest batch → scan forward.
- `.timeindex` — the same idea for timestamp → offset, used for retention-by-time and consumer seeks by time.

When the active segment gets big enough (`log.segment.bytes`, default 1 GiB) or old enough (`log.roll.ms`, default 7 days), Kafka rolls it: closes the current files, opens a new active segment with a higher base offset, and starts appending there.

Three properties make this fast:

1. **Sequential writes only.** Modern disks — spinning and SSD alike — are dramatically faster on sequential I/O than on random I/O. Kafka never updates a record in place.
2. **Page cache instead of an in-process buffer.** Hot data stays in RAM because the kernel caches it automatically. There's no extra layer of buffering inside the JVM, which keeps GC pressure low.
3. **Zero-copy reads.** `sendfile()` lets the kernel stream bytes from page cache to socket without a user-space round trip.

Retention is handled per-segment, not per-record. Whole segment files age out and get deleted when they cross the retention threshold. There's also **log compaction** — a mode where only the latest record per key is retained — used for things like `__consumer_offsets` and CDC outboxes where you want "current state" semantics on top of a log.

## Commit (steps 10–11)

The consumer has the records. It processes them — charges the card, writes to the database, emits to another topic. When it's done with a batch, it **commits its offset** by sending an `OffsetCommitRequest` to the group coordinator. The coordinator persists it as a record in the internal `__consumer_offsets` topic, keyed by `(group, topic, partition)`.

If the consumer crashes after processing but *before* committing, the offset stays where it was. The replacement consumer (after a rebalance) starts from the last committed offset and reprocesses those records. That's why **idempotent handlers** matter — at-least-once delivery is the default, and your processing code has to tolerate seeing the same record twice.

The trap to avoid is `enable.auto.commit=true`. With auto-commit, the client commits offsets on a timer (every `auto.commit.interval.ms`, default 5 s) regardless of whether your handler has actually finished processing them. The timer can fire after records are handed to your code but before your handler returns — so a crash between those points marks records as committed that were never actually processed. Use `enable.auto.commit=false` and call `commitSync()` after processing for anything that mutates external state. Part 4 covers the rebalance protocol and commit modes in depth.

## Pulling it together

Every Kafka property you care about traces back to a specific step in this picture:

- **Durability** lives in steps 3–6. `acks=all` waits for the full ISR; the HW advances only when everyone has caught up; consumers never see records above the HW.
- **Throughput** lives in step 2 (batching, compression, sequential append) and step 9 (zero-copy reads). The broker is dumb on purpose.
- **Ordering** comes from the partition being a single append-only log, with all writes going through the leader.
- **At-least-once** delivery is the natural default of step 11; **exactly-once** within Kafka adds idempotence (step 1) plus transactions across steps 2, 7, and 11 — the subject of Part 5.

Once you see the eleven-step picture, the rest of Kafka is filling in details.

## What's next

- **Part 3** — Producer write path, deep. Batching internals, the `RecordAccumulator`, compression algorithms compared, the exact `ProduceRequest` wire format byte for byte.
- **Part 4** — Consumer mechanics. Rebalancing, the cooperative-sticky assignor, fetch sessions, commit modes, why your handler timing matters.
- **Part 5** — Exactly-once and transactions. The transactional protocol, zombie fencing, `__transaction_state`, atomic commits across partitions.
- **Part 6 onward** — Flink, then Spark.

If anything in this article was unclear or wrong, tell me — I'm writing this series as I learn, and corrections are gold.

---

*Diagrams built in Excalidraw. Source: [`source/`](./source/). Exported PNGs: [`images/`](./images/).*
