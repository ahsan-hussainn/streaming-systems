# Kafka Internals — A Progressive Walkthrough

A single, end-to-end reference for Apache Kafka's internals, written so each section builds on the previous one. Read top-to-bottom and you go from "what is Kafka" to "what's on the wire byte-for-byte" to "when do I actually use it" without backtracking.

Companion diagram: `kafka.excalidraw` (open at https://excalidraw.com → File → Open).

---

## Table of contents

**Part I — Foundations**
1. [What Kafka actually is (and isn't)](#1-what-kafka-actually-is-and-isnt)
2. [The one-sentence mental model](#2-the-one-sentence-mental-model)
3. [Building blocks — components and the *why* for each](#3-building-blocks--components-and-the-why-for-each)

**Part II — The cluster as a system**
4. [On disk — what actually gets written](#4-on-disk--what-actually-gets-written)
5. [Replication, ISR, controller](#5-replication-isr-controller)
6. [High Watermark (HW) and LEO](#6-high-watermark-hw-and-leo)

**Part III — Reading the architecture diagram (data flow)**
7. [Walking the architecture diagram, zone by zone](#7-walking-the-architecture-diagram-zone-by-zone)
8. [The arrows — the actual data flow](#8-the-arrows--the-actual-data-flow)
9. [End-to-end: tracing one record through the system](#9-end-to-end-tracing-one-record-through-the-system)

**Part IV — The wire and the disk**
10. [The shape of the data at each hop](#10-the-shape-of-the-data-at-each-hop)
11. [What each component actually holds](#11-what-each-component-actually-holds)

**Part V — Producer and consumer mechanics**
12. [The producer write path, in full](#12-the-producer-write-path-in-full)
13. [The consumer read path, in full](#13-the-consumer-read-path-in-full)
14. [Failure scenarios — what actually happens](#14-failure-scenarios--what-actually-happens)

**Part VI — Delivery semantics**
15. [Exactly-once — what it really means](#15-exactly-once--what-it-really-means)
16. [Transactions — how they actually work](#16-transactions--how-they-actually-work)

**Part VII — Schemas and serialization**
17. [Serializers — JSON, Avro, Protobuf](#17-serializers--json-avro-protobuf)
18. [Schema Registry as infrastructure](#18-schema-registry-as-infrastructure)

**Part VIII — Performance profile and ecosystem**
19. [Kafka's real performance profile (and a common misconception)](#19-kafkas-real-performance-profile-and-a-common-misconception)
20. [Real problems Kafka solved](#20-real-problems-kafka-solved)
21. [Industries and use cases where Kafka thrives](#21-industries-and-use-cases-where-kafka-thrives)
22. [Where Kafka is *not* the right tool](#22-where-kafka-is-not-the-right-tool)

**Part IX — Building on Kafka**
23. [Practical engineering decisions](#23-practical-engineering-decisions)
24. [Running Kafka locally for learning](#24-running-kafka-locally-for-learning)
25. [How Kafka fits with Flink and Spark](#25-how-kafka-fits-with-flink-and-spark)

---

# Part I — Foundations

## 1. What Kafka actually is (and isn't)

Kafka is often miscategorised. It is:

- **Not a message queue** in the RabbitMQ sense. RabbitMQ tracks per-consumer state, deletes messages once acknowledged, and supports complex routing. Kafka deletes nothing on consumption — it stores every record for a retention window, and consumers track their own position.
- **Not a database**, although log-compacted topics give you something close to a key-value store with full change history.
- **Not a stream processor**. Kafka moves and stores bytes. Processing happens in clients (Kafka Streams, Flink, Spark) that read from Kafka and write back.

What Kafka **is**: a **distributed, replicated, append-only commit log**. Producers append records; brokers persist them on disk; consumers read at their own pace. Everything else — exactly-once, stream processing, change-data-capture, event sourcing — is built by combining that primitive with discipline.

Why this primitive is so useful:
- **Decoupling**: producers don't know about consumers; consumers don't know about producers. Either side can be redeployed, scaled, or rewritten in a different language without coordination.
- **Replay**: a new consumer can read history from offset 0. Reprocessing a year of events is just `--from-beginning`.
- **Durable buffer**: bursts that would overwhelm a downstream service get absorbed in the log. The downstream simply lags briefly and catches up.

## 2. The one-sentence mental model

> A **topic** is a sharded log. Each shard (a **partition**) lives on a **leader broker**, is replicated to **follower brokers**, and is consumed by exactly one consumer per **consumer group** at a time.

Carry that sentence around in your head. Every Kafka decision (partition count, key choice, replication factor, consumer group membership) is a consequence of it.

## 3. Building blocks — components and the *why* for each

| Component | What it is | Why it exists |
|---|---|---|
| **Broker** | A Kafka server process. A cluster has N brokers, typically 3, 5, or more. | Stores partitions on disk; serves produce and fetch requests. |
| **Topic** | A named logical stream — e.g. `orders`, `clickstream`, `payments.requested`. | Categorisation. A topic is just a name; the data lives in its partitions. |
| **Partition** | An ordered, immutable, append-only log file. The unit of parallelism *and* of ordering. | A single log file can't scale. Sharding the log into N partitions lets N producers write and N consumers read in parallel. |
| **Offset** | A 64-bit integer identifying a record's position within a partition. | Lets a consumer say "give me everything from offset 12345 onwards." Independent of wall-clock time. |
| **Replica** | A copy of a partition on another broker. | Durability + availability. If the broker holding the leader dies, a follower takes over with no data loss. |
| **Leader / Follower** | Among a partition's replicas, one is the leader (handles all reads and writes) and the rest are followers (only replicate). | Single source of truth for ordering. Followers are passive; they don't serve clients. |
| **ISR — In-Sync Replicas** | The dynamic set of replicas that have kept up with the leader within `replica.lag.time.max.ms` (default 30s). | Only ISR members are eligible to be elected leader. Defines "fully replicated." |
| **Producer** | Client library (or app) that appends records to topics. | Source. |
| **Consumer** | Client that reads records and tracks its own offset. | Sink. |
| **Consumer group** | A set of consumers cooperating on the same set of topics. | Horizontal scaling: each partition is read by exactly one member of the group. Multiple groups read independently of each other. |
| **Controller** | One broker, elected as the cluster's metadata coordinator. | Manages leader elections, ISR changes, broker membership. |
| **KRaft / ZooKeeper** | The metadata quorum. ZooKeeper was the original; KRaft (Raft-based, embedded in Kafka itself) replaces it from 3.x onwards. | Stores cluster metadata, elects the controller, maintains topic configs. |

---

# Part II — The cluster as a system

## 4. On disk — what actually gets written

A Kafka broker is, at heart, a process that opens a lot of files and appends to them. Per partition, on disk:

```
/var/kafka-logs/orders-3/
  00000000000000000000.log      ← active segment, append-only records
  00000000000000000000.index    ← sparse offset → byte position lookup
  00000000000000000000.timeindex← timestamp → offset
  00000000000000123456.log      ← previous segment, rolled when log.segment.bytes hit
  00000000000000123456.index
  ...
  leader-epoch-checkpoint        ← tracks leader epoch transitions
```

Each `.log` file is a sequence of **record batches** (header + compressed records). The active segment receives appends; once it crosses `log.segment.bytes` (default 1 GiB) or `log.roll.ms` (default 7 days), it's rolled and a new active segment begins.

Three properties make this fast:

1. **Sequential writes only.** Spinning disks and SSDs both love sequential I/O. A modern disk can sustain hundreds of MB/s sequentially while doing only a few hundred IOPS randomly. Kafka never updates a record in place.
2. **Page cache instead of an in-process cache.** Kafka writes through the OS page cache (`write()` to a `FileChannel`). Recent data stays in memory automatically; cold data is paged out by the kernel. There is no extra layer of caching inside the JVM heap, which keeps GC pressure low.
3. **Zero-copy reads.** When a consumer fetches, the broker uses the `sendfile()` syscall to push bytes directly from page cache to the network socket without copying through user space.

Two retention modes coexist per topic:

- **Time/size based**: `retention.ms` or `retention.bytes`. When a segment is fully older than the retention threshold, it's deleted as a whole file. Cheap.
- **Log compaction**: keep only the latest record per key, plus tombstones (null-value records) for deletes. Used for "current state" topics — Kafka Streams' state store changelogs, the internal `__consumer_offsets` topic, CDC outboxes. Old keyed updates get garbage-collected by a background log cleaner thread.

A topic can be both `cleanup.policy=compact,delete` — log-compacted *and* time-bounded.

## 5. Replication, ISR, controller

A topic is created with a **replication factor** (RF), typically 3. Each partition has RF replicas; one is leader, the rest are followers.

**Replication mechanics**:
- Followers fetch from the leader using the same `FetchRequest` protocol that consumers use. The leader is the single source of truth for ordering and offsets.
- The leader tracks each follower's "fetch position." A follower is in the ISR if its position is within `replica.lag.time.max.ms` of the log end.
- The leader maintains a **High Watermark (HW)** = the highest offset that all ISR members have replicated. Only records below HW are visible to consumers. This is what guarantees readers never see records that could later be lost.

**The controller**:
- One broker in the cluster is the controller. In KRaft mode, it's elected by a Raft quorum across the controller-role brokers; in ZooKeeper mode, by an ephemeral znode race.
- The controller decides who leads each partition. When a broker dies, it picks a new leader from the surviving ISR for every partition that broker was leading, and pushes the new metadata out.
- Failover is fast (sub-second in healthy KRaft clusters) because the controller already knows the state — no quorum reads needed at failover time.

**`unclean.leader.election.enable`** (default `false` in modern Kafka): if the entire ISR dies and only an out-of-date replica remains, do you elect it leader (lose some data, stay available) or refuse (preserve data, go offline)? The safe default is "refuse." Worth knowing because it's a real operational decision.

## 6. High Watermark (HW) and LEO

**Definition**: the High Watermark is the offset that marks the boundary between *replicated-to-the-full-ISR* and *not-yet-fully-replicated*. Records below the HW are visible to consumers; records at or above the HW are not.

Two related markers per partition:

- **LEO (Log End Offset)** — the offset of the *next* record to be appended on this replica. If the leader has written 100 records, its LEO = 100.
- **HW (High Watermark)** — the leader's view of "what every ISR member has caught up to." HW = `min(LEO of all ISR members)`.

Picture a partition log on the leader, with two followers F1 and F2 in the ISR:

```
Leader Broker 1 — partition P0:
offset:    0   1   2   3   4   5   6   7   8
records:  [A] [B] [C] [D] [E] [F] [G] [H] [I]
                                  ↑
                                  HW = 7        (records 0..6 are visible)
                                                  ↑
                                                  LEO = 9  (next write goes here)

Follower F1 has fetched up to LEO=7  → caught up to offset 6
Follower F2 has fetched up to LEO=7  → caught up to offset 6
Leader sees: min ISR LEO = 7  → HW = 7
```

So records 7 and 8 are physically on the leader's disk, but **a consumer fetching from this partition will not see them yet**. Why? Because if the leader crashed right now, F1 or F2 would be elected leader and they don't have records 7 and 8 — those records would vanish. By hiding them until they're replicated, Kafka makes sure consumers never see something that could later disappear.

The HW advances as followers catch up. Once F1 and F2 fetch records 7 and 8, the leader bumps HW to 9, and consumers can now read them.

There's a related concept, **LSO (Last Stable Offset)**, used when transactions are enabled — it's the highest offset before any open (uncommitted) transaction. `read_committed` consumers can only read up to LSO, not HW. For non-transactional topics LSO = HW.

---

# Part III — Reading the architecture diagram (data flow)

## 7. Walking the architecture diagram, zone by zone

Open `kafka.excalidraw` next to this file. Three vertical zones:

```
[ PRODUCERS ]   →   [ KAFKA CLUSTER (3 brokers) ]   →   [ CONSUMER GROUPS ]
   blue boxes        green boxes with red/gray tiles       orange boxes
```

The **blue boxes on the left** are application services that *write* data. The **green boxes in the middle** are Kafka brokers (servers) that *store* data. The **orange boxes on the right** are application services that *read* data. The dashed gray cluster border is just the logical "this is one Kafka cluster" boundary — it's not a network thing.

**Inside the cluster:**

The purple bar at the top — **KRaft Controller Quorum** — is the cluster's metadata brain. It does *not* sit on the data path. It only decides things like "Broker 2 is now the leader for partition P1" and pushes that info to clients. Producers and consumers don't talk to it for actual records.

Below the purple bar, the three green boxes are the brokers. Inside each broker you see three partition tiles:

- **Red-bordered tiles = LEADER replicas.** This broker is the authoritative copy for that partition. All writes and all reads for this partition go through this broker.
- **Gray-bordered tiles = FOLLOWER replicas.** This broker holds a passive copy. It does not serve clients. Its only job is to stay caught up with the leader.

Look at partition **P0** specifically:
- Broker 1 has `P0 LEADER` (red)
- Broker 2 has `P0 follower` (gray)

That's one partition, replicated twice (replication factor = 2). Same story for P1, P2, P3 — each partition appears as a leader on one broker and as a follower on another. This is what gives you durability: if Broker 1 dies, Broker 2 already has P0 and can be promoted to leader.

Notice that **leadership is spread across all three brokers** — Broker 1 leads P0+P3, Broker 2 leads P1, Broker 3 leads P2. That's deliberate: it spreads load. If one broker were leader for everything, all writes for the topic would hit that one machine.

## 8. The arrows — the actual data flow

There are three colours of arrows, each telling a different story.

**1. Blue arrows (producers → brokers) — the write path.**

When `order-service` calls `producer.send(record)`, the producer client:
1. Hashes the record's key to pick a partition (e.g. `hash(orderId) → P0`).
2. Looks up which broker is the *leader* for P0 right now (it asks the cluster for metadata, then caches it).
3. Sends the record directly to that leader broker — Broker 1 in this diagram.

So the blue arrow from `order-service` to Broker 1 represents this: *"the order-service is writing records whose keys map to partitions led by Broker 1."* The other blue arrows show the same story for the other producers writing to other partitions.

Producers do **not** broadcast to all brokers. Each record goes to exactly one leader. The producer client handles the routing transparently.

**2. Dashed gray arrows (broker ↔ broker) — replication.**

Once Broker 1 (the leader of P0) has appended the record to its log, the followers — Broker 2 in this case — *pull* the new records over by issuing a `FetchRequest`, the same protocol consumers use.

A subtle but important point: **followers pull from leaders, leaders don't push to followers.** It's the same fetch protocol the consumers use. Once a follower has caught up to offset N, the leader knows it's "in sync" and counts it toward the ISR (In-Sync Replicas).

The leader only acknowledges the producer's write (with `acks=all`) once the full ISR has caught up. That's the guarantee: when your producer sees "OK," every replica in the ISR has the record.

**3. Orange arrows (brokers → consumers) — the read path.**

Now the right side. Two consumer groups are reading the *same topic* but for different purposes.

**Group `order-fulfillment`** (top orange box):
- consumer-1 has been assigned P0 and P3.
- consumer-2 has been assigned P1 and P2.
- Together, the group covers all four partitions, with no overlap. **Each partition is read by exactly one consumer in the group.** This is how you parallelise — two consumers means roughly half the work each.

So consumer-1 fetches from Broker 1 (the leader of P0) and Broker 3 (the leader of P3). consumer-2 fetches from Broker 2 (leader of P1) and Broker 3 (leader of P2). Each consumer talks to whichever broker happens to lead its assigned partitions.

**Group `analytics`** (bottom orange box):
- consumer-1 owns *all* partitions on its own.
- It's a single consumer in its own group.

Crucially, **the analytics group reads completely independently of the fulfillment group.** They don't share offsets, they don't share work, they don't even know about each other. Each group has its own position in `__consumer_offsets`. Analytics can be lagging by an hour while fulfillment is real-time, and that's fine — both are reading their own copy of the same log.

This is the magic of Kafka's "consumers track their own offset" model: the broker doesn't care who's reading or how many times. It's just a log.

## 9. End-to-end: tracing one record through the system

Imagine an order arrives:

1. **`order-service`** receives an HTTP request, builds an order record, calls `send(record)` keyed by `orderId`.
2. The producer client hashes the key → P0. Looks up: P0 leader = Broker 1. Sends the record there. *(blue arrow)*
3. **Broker 1** appends the record to its local P0 log segment file (page cache + disk).
4. **Broker 2** (which holds the P0 follower) is constantly polling Broker 1; it fetches the new record and writes it to its own P0 log. *(dashed gray arrow)*
5. Broker 1 sees Broker 2 has caught up → advances the High Watermark for P0 → sends ACK back to the producer.
6. **Group `order-fulfillment` consumer-1** is polling Broker 1 for P0. It fetches the new record and processes it (charges card, sends confirmation email, whatever). *(orange arrow, top)*
7. **Group `analytics` consumer-1** is *also* polling Broker 1 for P0 — independently, with its own offset. It fetches the same record and writes it into a data warehouse. *(orange arrow, bottom)*
8. Both consumers eventually commit their respective offsets to `__consumer_offsets`. Their progress is tracked separately.

The producer is done at step 5. The record is durably stored. Whether or not consumers have read it yet is not the producer's problem — that's the entire point of decoupling through a log.

---

# Part IV — The wire and the disk

## 10. The shape of the data at each hop

Following one record from `order-service` through the system. Here's the data structure at each stage.

### Stage 1 — Producer side: the `ProducerRecord` object

This is what your application code creates:

```
ProducerRecord {
  topic:      "orders"
  partition:  null              // unset → producer will compute via hash(key)
  key:        "order-12345"     // bytes (after key serializer)
  value:      {"orderId":"12345","userId":"u789","total":42.50, ...}  // bytes (after value serializer)
  headers:    [("trace-id","abc123"), ("source","web-checkout")]
  timestamp:  1715174400123     // ms epoch, optional
}
```

The serializers (Avro, Protobuf, JSON, plain string) turn the key and value into `byte[]` before anything else happens.

### Stage 2 — Producer accumulator: a *batch* of records

The producer doesn't send one record at a time. It groups records by `(topic, partition)` into a **RecordBatch** in memory:

```
In-memory RecordAccumulator:
  ("orders", partition=0) → [batch in progress]
  ("orders", partition=1) → [batch in progress]
  ("orders", partition=2) → [batch in progress]
```

Each in-progress batch fills until it hits `batch.size` (default 16 KB, often raised to 64–256 KB) or `linger.ms` elapses (default 0, often raised to 5–20 ms to allow batching).

### Stage 3 — Wire format: `ProduceRequest` sent to the leader broker

```
ProduceRequest {
  acks:             -1        // -1 = "all" (full ISR)
  timeoutMs:        30000
  transactionalId:  null      // or "txn-id-123" for transactional producer
  topicData: [
    {
      topic: "orders",
      partitionData: [
        { partition: 0, recordBatchBytes: <serialized batch, see below> }
      ]
    }
  ]
}
```

The serialized batch on the wire (and on disk — same format) is:

```
RecordBatch (v2 format) {
  baseOffset:           12450        // first offset in this batch
  batchLength:          2048
  partitionLeaderEpoch: 7
  magic:                2            // record format version
  crc32c:               0xABCDEF...
  attributes:           compression=lz4, timestampType=create, isTransactional=false
  lastOffsetDelta:      99           // 100 records: offsets 12450..12549
  baseTimestamp:        1715174400000
  maxTimestamp:         1715174400500
  producerId:           7421         // for idempotence
  producerEpoch:        0
  baseSequence:         45000        // dedup key with producerId
  records: [
    Record { offsetDelta:0, timestampDelta:0,   keyLen:11, key:"order-12345", valueLen:120, value:<bytes>, headers:[...] },
    Record { offsetDelta:1, timestampDelta:5,   keyLen:11, key:"order-12346", valueLen:118, value:<bytes>, headers:[...] },
    Record { offsetDelta:2, timestampDelta:12,  ... },
    ...
  ]
}
```

Two things to notice:

- **Records inside a batch are deltas**, not absolute. `offsetDelta=2` means "this record's absolute offset = baseOffset + 2 = 12452." Same for timestamps. This compresses extremely well.
- **The batch is compressed as a single unit** (gzip/snappy/lz4/zstd). All 100 records' bytes go through one compressor. That's what makes Kafka's compression so effective.

### Stage 4 — On disk in the partition

The leader broker takes the batch and appends it (untouched, still compressed) to the active segment file:

```
/var/kafka-logs/orders-0/00000000000012000000.log
   ├── [... earlier batches ...]
   ├── batch @ baseOffset=12350, 80 records, 1850 bytes
   ├── batch @ baseOffset=12430, 20 records, 480 bytes
   └── batch @ baseOffset=12450, 100 records, 2048 bytes  ← just appended
```

Index files are updated sparsely:
```
00000000000012000000.index            (offset → byte position)
   12000 → 0
   12100 → 23456
   12200 → 47890
   ...
   12450 → 290112
```

The index doesn't store every offset — only every Nth (controlled by `index.interval.bytes`, default 4 KB). Lookup of a specific offset = binary search in the index → seek to the nearest batch → scan forward.

### Stage 5 — Consumer side: `FetchRequest`

```
FetchRequest {
  replicaId:       -1            // -1 means "I am a consumer", not a follower
  maxWaitMs:       500           // long-poll up to 500ms if no data
  minBytes:        1
  maxBytes:        52428800      // 50 MB total cap
  isolationLevel:  1             // 1 = read_committed (skip aborted txns)
  sessionId:       4521          // incremental fetch session
  topics: [
    {
      topic: "orders",
      partitions: [
        { partition: 0, fetchOffset: 12450, partitionMaxBytes: 1048576 }
      ]
    }
  ]
}
```

### Stage 6 — `FetchResponse` from broker to consumer

The broker uses `sendfile()` to push raw bytes directly from the page cache to the socket:

```
FetchResponse {
  throttleTimeMs: 0
  responses: [
    {
      topic: "orders",
      partitions: [
        {
          partition:        0,
          errorCode:        0,
          highWatermark:    12550,    // tells consumer how far it can go
          lastStableOffset: 12549,
          logStartOffset:   0,        // earliest available (after retention deletion)
          recordBatchBytes: <raw bytes — same format the producer sent>
        }
      ]
    }
  ]
}
```

**Crucially**: the broker doesn't decompress, doesn't deserialize, doesn't re-frame. The consumer receives the *exact same bytes* the producer sent. The consumer client decompresses, parses the batch into individual records, deserializes key and value, and hands `ConsumerRecord` objects to your application code:

```
ConsumerRecord {
  topic:     "orders"
  partition: 0
  offset:    12450
  timestamp: 1715174400000
  key:       "order-12345"
  value:     {"orderId":"12345","userId":"u789","total":42.50, ...}
  headers:   [("trace-id","abc123"), ("source","web-checkout")]
}
```

### Stage 7 — OffsetCommit

After processing, the consumer commits:

```
OffsetCommitRequest {
  groupId:      "order-fulfillment"
  generationId: 42                    // rebalance epoch
  memberId:     "consumer-1-7f8a9..."
  topics: [
    {
      topic: "orders",
      partitions: [
        { partition: 0, committedOffset: 12550, leaderEpoch: 7, metadata: "" }
      ]
    }
  ]
}
```

The group coordinator stores this as a record in the internal `__consumer_offsets` topic, keyed by `(groupId, topic, partition)`. Log-compacted, so only the latest offset per key is kept.

## 11. What each component actually holds

**Producer** (purely in-memory, no disk):
- Metadata cache: `partition → leaderBrokerId` for every topic the producer touches
- `RecordAccumulator`: a `Map<TopicPartition, Deque<RecordBatch>>` of in-flight batches
- For idempotence: `producerId`, `producerEpoch`, and per-partition sequence number
- For transactions: transaction coordinator address, transaction state machine
- Connection pool to brokers, request inflight-tracking per connection

If the producer process crashes, anything in the accumulator is lost. The application must persist data *before* calling `send()` if it needs to survive a producer crash.

**Broker** (durable on disk, plus in-memory state):

*On disk:*
- For each hosted partition: the directory `<topic>-<partition>/` with `.log`, `.index`, `.timeindex` segment files
- `leader-epoch-checkpoint` — history of which leader epoch covered which offsets (used during truncation after failover)
- `replication-offset-checkpoint` — periodic snapshot of HW per partition
- `recovery-point-checkpoint` — last offset that's been fsynced (used for log recovery on restart)
- For the controller broker (KRaft): the `__cluster_metadata` topic, which is the source of truth for cluster state
- Internal topics it might host partitions of: `__consumer_offsets`, `__transaction_state`

*In memory:*
- Open file handles + memory-mapped index files
- Per-partition state: leader epoch, HW, LEO, ISR set, pending replica fetch state
- Producer ID map for idempotence dedup (which `(producerId, partition, sequence)` triples have been seen)
- Open transactions
- The OS page cache, which is doing most of the work — this is *not* in Kafka's heap, it's the kernel's, but it's where hot reads come from

**Consumer** (mostly in-memory; durable state lives on the broker):

*In memory:*
- Subscribed topics, current partition assignment, group membership (memberId, generationId, groupCoordinator)
- Per partition: `position` = next offset to fetch
- Fetch buffer of records pulled but not yet returned to the application
- Auto-commit timer state (if enabled)

*"Durable" state, but not on the consumer:*
- Committed offsets live in `__consumer_offsets` on the brokers, keyed by `(groupId, topic, partition)`. The consumer has *no* local state file — that's why a replacement consumer can take over a failed one's partitions just by reading the last committed offset.

---

# Part V — Producer and consumer mechanics

## 12. The producer write path, in full

What happens when you call `producer.send(record)`:

1. **Serialization.** The configured key serializer and value serializer turn objects into `byte[]`. Common choices: Avro + Schema Registry, Protobuf, JSON.
2. **Partitioner picks a partition**:
   - **With key**: `murmur2(key) % numPartitions`. Same key always lands on same partition → per-key ordering preserved.
   - **Without key**: the **sticky partitioner** (default since 2.4) batches to one partition until the batch fills, then rotates. This produces fewer, fatter batches than naive round-robin → much higher throughput.
3. **Record is placed in an in-memory accumulator** organised as `(topic, partition) → batch`. The producer doesn't send immediately; it batches.
4. **A background sender thread** drains accumulator batches that are either full (`batch.size`) or aged (`linger.ms`) and sends them as `ProduceRequest` to each partition's leader broker.
5. **Leader broker** validates the request, appends the batch to its active log segment (page cache), and waits according to `acks`.
6. **Followers in the ISR fetch** the new records (they were already polling). Once they've fetched up to offset N, the leader knows they're caught up.
7. **High Watermark advances** to the highest offset replicated to the full ISR.
8. **ProduceResponse** is returned to the producer based on `acks`:
   - `acks=0` — fire and forget. No durability guarantee.
   - `acks=1` — leader has written to its log. Vulnerable to leader failure between write and replication.
   - `acks=all` — all current ISR members have replicated. Combined with `min.insync.replicas=2`, this is the configuration for durable production data.
9. **Producer retries** on retriable errors. With `enable.idempotence=true` (default in modern clients), each `(producer-id, partition, sequence-number)` triple is recorded by the broker and duplicate retries are silently dropped.

## 13. The consumer read path, in full

1. A consumer sets `group.id` and calls `subscribe(["orders"])`.
2. It sends `JoinGroup` to its **group coordinator** — a broker chosen by hashing the group id onto a partition of `__consumer_offsets`.
3. The coordinator runs the **rebalance protocol**:
   - All members send their subscriptions and capabilities.
   - One member is elected **group leader** and runs the configured assignor (`Range`, `RoundRobin`, `Sticky`, or `CooperativeSticky`).
   - The coordinator distributes the assignment.
4. Each consumer fetches its assigned partitions from each partition's leader broker using `FetchRequest(partition, offset, max_bytes, max_wait_ms)`.
5. The broker returns records in order. The consumer processes them.
6. The consumer **commits offsets** to `__consumer_offsets`. Two modes:
   - **Auto-commit** (`enable.auto.commit=true`, every `auto.commit.interval.ms`): convenient, but commits happen on a timer, not after processing. A crash can leave you having "committed" records that weren't yet handled.
   - **Manual commit**: `commitSync()` after processing. The reliable choice for anything that mutates state.
7. On consumer death or new member: a rebalance triggers, the coordinator reassigns partitions, and the new owner resumes from the last committed offset.

**Cooperative rebalancing** (assignor `CooperativeSticky`, default in modern clients) avoids stop-the-world rebalances. Instead of every member dropping all partitions and re-joining, only the partitions actually moving are revoked; the rest keep flowing. For low-latency consumers this is a big deal — the old protocol could pause processing for seconds.

**Delivery semantics in plain language**:

- **At-most-once**: commit offset before processing. If you crash, the record is skipped.
- **At-least-once** (default for most apps): process, then commit. If you crash between, the next consumer reprocesses. You need to make handlers idempotent.
- **Exactly-once within Kafka**: covered fully in §15–16.

## 14. Failure scenarios — what actually happens

| Scenario | What happens |
|---|---|
| Leader broker dies mid-write with `acks=all` | Producer's in-flight request fails with `NotLeaderForPartition`. Producer refreshes metadata, retries against the new leader. With idempotence on, no duplicates. Records that had been replicated to the ISR are preserved by the new leader. |
| Leader broker dies mid-write with `acks=1` | Whatever was written to the leader but not yet replicated is **lost**. This is the price of `acks=1`. |
| Producer crashes after sending, before getting an ACK | The retry was already in flight or will be issued by the next process instance with the same `transactional.id` (if transactional). Idempotence prevents duplicates. |
| Consumer crashes after processing a batch but before committing | Replacement consumer (after rebalance) re-reads from the last committed offset → the batch is processed again. Idempotent handlers make this a non-event. |
| Network partition isolates a broker from the cluster | It's removed from ISR. Its partitions are still served by the leader (if it isn't the leader) or fail over (if it was). When the partition heals, it catches up and rejoins ISR. |
| ISR shrinks to a single replica | If `min.insync.replicas=2`, producers using `acks=all` start getting `NotEnoughReplicas` errors — the cluster is failing closed to protect durability. |
| Disk fills on a broker | That broker stops serving. Other brokers' followers (now ISR members on those partitions) handle traffic. |
| Whole cluster restarts | Each broker replays its log on startup. Controller is re-elected. ISR reconverges. With `acks=all`, no data loss. |
| Controller broker dies (KRaft) | Controller quorum elects a new active controller. Sub-second in healthy clusters. Data plane keeps running on existing leaders during the failover. |

---

# Part VI — Delivery semantics

## 15. Exactly-once — what it really means

First, dispel the common misconception: **exactly-once doesn't mean "the message is delivered to the broker exactly once."** That's mathematically impossible in distributed systems (the Two Generals Problem). What Kafka actually gives you is **exactly-once *processing*** — the *side effects* of processing each record happen exactly once, even with retries, crashes, and rebalances.

Three problems together cause "non-exactly-once" behaviour, and Kafka solves each separately:

**Problem 1 — duplicate sends from producer retries.**
Producer sends record → leader writes it → ACK is lost in transit → producer retries → leader writes the record again. Now the partition has two copies.

*Solution: idempotent producer.* When `enable.idempotence=true`:
- Broker assigns the producer a `producerId` (PID) + `producerEpoch` on first connect.
- Producer attaches a per-partition monotonic sequence number to every record.
- Broker keeps a small in-memory map of "last 5 sequences seen per `(PID, partition)`."
- A duplicate retry has the same `(PID, partition, sequence)` → broker silently drops it and returns the original ACK.

This is **on by default in modern clients** and has essentially no cost.

**Problem 2 — duplicate processing from consumer crashes.**
Consumer reads record at offset 12450, processes it (writes to a database), crashes before committing the offset. Replacement consumer starts from the last committed offset (say 12440), reprocesses 12440..12450, and the database write happens twice.

*Solution depends on what you're doing with the record:*
- If the side effect is "write to another Kafka topic" → solved by transactions (Problem 3).
- If the side effect is "write to an external system" → make the write **idempotent**. Use upsert keyed by record id, or check-and-write, or have the database deduplicate on a unique constraint. Kafka cannot solve this for you because it doesn't control your database.

**Problem 3 — atomic multi-partition writes + offset commits.**
The classic stream-processing pattern is "consume from topic A, transform, produce to topic B, commit consumer offset." Without transactions, three failure points:
- Produce to B succeeds, offset commit fails → reprocess on restart → duplicate write to B.
- Offset commit succeeds, produce to B fails → record lost on restart.
- Producing to *multiple* output partitions: some succeed, some fail.

*Solution: Kafka transactions.* All produces and the offset commit either all happen or all roll back.

The exactly-once stack:

```
   Idempotent producer       → no duplicates from retries
 + Kafka transactions        → atomic multi-partition writes + offset commits
 + read_committed consumers  → never see records from aborted transactions
 = exactly-once within Kafka
```

The phrase "**within Kafka**" is load-bearing. End-to-end exactly-once that includes external sinks (Postgres, S3, Elasticsearch) requires the sink to participate. Three patterns work:

1. **Idempotent sink writes**: upsert by record key. Works for databases and key-value stores. Simplest.
2. **Two-phase commit / transactional sink**: sink prepares the write, Kafka acks, then sink commits. Heavy, only some systems support it (e.g. Flink's Kafka-to-Postgres connectors with 2PC).
3. **Change data capture pattern**: write to Kafka transactionally, then replicate Kafka → external system with idempotent loader. The loader's "last successfully written offset" is its own checkpoint.

## 16. Transactions — how they actually work

The transactional protocol has three pieces:

- **The producer** with a configured `transactional.id` (a stable string per logical producer instance).
- **The transaction coordinator** — one broker, chosen by hashing `transactional.id` onto a partition of the internal `__transaction_state` topic.
- **The `__transaction_state` topic** — the persistent log of every transaction's lifecycle.

### The lifecycle, step by step

```java
producer.initTransactions();        // step 1 — once per process

while (running) {
  ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(500));

  producer.beginTransaction();       // step 2

  for (ConsumerRecord<K, V> r : records) {
    OutputRecord o = transform(r);
    producer.send(new ProducerRecord<>("output-topic", o.key, o.value));   // step 3
  }

  // step 4 — atomically include the consumer offsets in this transaction
  producer.sendOffsetsToTransaction(
      currentOffsetsFromConsumer(records),
      consumer.groupMetadata()
  );

  producer.commitTransaction();      // step 5
}
```

What happens on the wire:

**Step 1 — `initTransactions()`:**
- Producer asks the cluster: "who is the transaction coordinator for `transactional.id=order-processor-3`?"
- Producer connects to that coordinator and registers.
- Coordinator assigns the producer a `producerId` and **bumps the `producerEpoch`**. Any older instance of "order-processor-3" still alive (zombie, stuck in GC, on a partitioned-off network) now has a stale epoch. When it tries to write, the broker rejects it with `ProducerFenced`. This is **zombie fencing**.

**Step 2 — `beginTransaction()`:** pure local state change. No network call yet.

**Step 3 — each `send()`:**
- The producer sends as normal, but now records are stamped with `producerId`, `producerEpoch`, `sequence`, and an `isTransactional` flag in the batch header.
- The first time a transactional record lands on a new partition, the producer first calls `AddPartitionsToTxn` on the coordinator, which writes a record to `__transaction_state` saying "transaction X involves partition `output-topic-7`."
- Records are written to the destination partitions immediately, but they're invisible to `read_committed` consumers because the transaction isn't committed yet.

**Step 4 — `sendOffsetsToTransaction()`:**
- Tells the coordinator: "the consumer offsets for group `order-fulfillment` are part of this transaction too."
- Coordinator writes that fact to `__transaction_state`.
- The actual offset records will be written to `__consumer_offsets` as part of the commit, atomically with everything else.

**Step 5 — `commitTransaction()`:**
- Producer sends `EndTxn(commit=true)` to the coordinator.
- Coordinator writes a `PREPARE_COMMIT` record to `__transaction_state` (durability checkpoint — if the coordinator crashes here, the new coordinator can finish the commit).
- Coordinator writes a **commit marker** (a special control record) to every partition that was part of the transaction, including the relevant partitions of `__consumer_offsets`.
- Coordinator writes `COMPLETE_COMMIT` to `__transaction_state`.

### How `read_committed` consumers see this

A normal consumer reads records and skips any that are part of an *aborted* transaction. To know which transactions are open vs. committed vs. aborted, consumers track the **Last Stable Offset (LSO)** per partition — the offset before any open transaction. They only return records below LSO to the application, so a transaction that's still in progress is invisible.

Throughput cost is real. Transactions add roughly:
- An extra round trip per first-write-to-partition for `AddPartitionsToTxn`.
- Two writes to `__transaction_state` per commit (prepare + complete).
- Commit markers in every partition you wrote to.

In practice this adds 5–20 ms per transaction. You amortize by batching many records per transaction. Kafka Streams tunes this automatically (default ~100 ms commit interval).

### When transactions matter, when they don't

- **Yes, use transactions**: stream processing where you read from Kafka and write to Kafka (Kafka Streams, Flink Kafka-to-Kafka), financial event processing, anything where reprocessing produces visible duplicates.
- **No, skip transactions**: pure ingestion (producer → Kafka → external sink, where the sink handles dedup), at-least-once analytics pipelines, anything where downstream is idempotent anyway.

The default for most teams: **idempotent producer always on, transactions only for consume-process-produce loops.**

---

# Part VII — Schemas and serialization

## 17. Serializers — JSON, Avro, Protobuf

Kafka stores `byte[]`. Producer and consumer must agree on what those bytes mean. The serializer is the contract.

### The naive choice — JSON

```python
producer.send("orders", value=json.dumps({"orderId": "12345", ...}).encode())
```

Why teams reach for it: zero setup, human-readable, every language supports it.

Why it eventually hurts:
- **No schema enforcement.** Producer adds a typo'd field, every consumer breaks silently or noisily.
- **Verbose on the wire.** Field names repeated in every record. Easily 3–5x the size of binary formats.
- **Slow to parse.** JSON deserialization is one of the largest CPU consumers in real Kafka pipelines.
- **No type system.** "Is `userId` a number or a string this week?" Both happen in production.

JSON is fine for prototyping or low-volume topics. For anything serious, you want a schema-first format.

### Avro

Schema is a JSON document describing the record:

```json
{
  "type": "record",
  "namespace": "com.acme.orders",
  "name": "OrderPlaced",
  "fields": [
    {"name": "orderId",      "type": "string"},
    {"name": "userId",       "type": "string"},
    {"name": "total",        "type": "double"},
    {"name": "currency",     "type": "string", "default": "USD"},
    {"name": "items",        "type": {"type": "array", "items": "string"}},
    {"name": "discountCode", "type": ["null", "string"], "default": null}
  ]
}
```

How it serializes: extremely compact binary. **Field names are not in the data** — only values, in declared order. The schema must be available to deserialize.

Where does the schema live? Typically in a **Schema Registry** (Confluent's is the dominant one, also Apicurio, AWS Glue Schema Registry). The wire format on Kafka is:

```
[ 0x00 ][ 4-byte schema id ][ Avro-encoded bytes ]
   ^         ^                 ^
   magic     id from registry  the actual record
```

Producer flow:
1. Application produces an `OrderPlaced` object.
2. Avro serializer asks the registry: "register this schema for subject `orders-value`. Give me an ID."
3. Registry returns `id=147` (or rejects if the schema breaks compatibility rules).
4. Producer writes `[0x00][147][avro bytes]` to Kafka.

Consumer flow:
1. Reads `[0x00][147][avro bytes]`.
2. Looks up schema id 147 in the registry (cached forever — schemas are immutable by id).
3. Decodes the Avro bytes against that schema into a `GenericRecord` or a code-generated class.

Avro's killer feature: **schema evolution rules**. The registry enforces them.

| Compatibility mode | What it allows |
|---|---|
| `BACKWARD` (default) | New schema can read old data. Add optional field, remove field with default. |
| `FORWARD` | Old schema can read new data. Add field, remove optional field. |
| `FULL` | Both. The intersection of the above. |
| `NONE` | Anything goes. Don't. |

In practice, `BACKWARD` is what most teams use: producers can be upgraded freely (they write with the new schema), consumers don't have to be upgraded simultaneously.

### Protobuf

Schema is a `.proto` file:

```protobuf
syntax = "proto3";
package com.acme.orders;

message OrderPlaced {
  string order_id      = 1;
  string user_id       = 2;
  double total         = 3;
  string currency      = 4;
  repeated string items = 5;
  optional string discount_code = 6;
}
```

How it serializes: each field is encoded as `(field_number, wire_type, value)`. Field *numbers* are in the bytes; field *names* are not. Numbers are how evolution works — never reuse a number, never change its type, and you can read old data with new code and vice versa.

Schema location: same options as Avro. Confluent Schema Registry supports Protobuf in the same wire format (`[magic][id][bytes]`). You can also embed `.proto` definitions in your codebase and skip the registry, which many polyglot shops do.

### When to pick which

| You should pick | When |
|---|---|
| **JSON** | Prototyping. Low-volume topics. Topics consumed by external partners with no shared tooling. Observability / logging streams where humans read the data. |
| **Avro** | JVM-heavy stack. Heavy use of Kafka Connect (Avro is best-supported there). Working in Hadoop / Spark / Hive ecosystems (Avro is native). Want explicit, registry-enforced compatibility. |
| **Protobuf** | Polyglot teams (Go, Python, Java, JS all first-class). Already using gRPC (same schemas everywhere). Want best raw encode/decode speed. Want richer type system (oneOf, maps, well-known types). |

Throughput differences in real benchmarks:
- JSON → Avro: typically 3–5x smaller, 2–4x faster to parse.
- JSON → Protobuf: similar size, often faster to parse than Avro.
- Avro vs Protobuf size: roughly equivalent. Not a tiebreaker.

The real tiebreaker is ecosystem fit: which language do most of your producers/consumers run in, and what serialization is the rest of your platform using?

## 18. Schema Registry as infrastructure

Whichever format you pick, a registry is genuinely worth running:

- Single source of truth for "what is the shape of data on topic X."
- Compatibility checks at registration time prevent breaking changes from reaching production.
- Schema IDs are tiny (4 bytes), so wire overhead is negligible.
- Tooling integration: Kafka Connect, ksqlDB, Flink Kafka connector, all natively understand registry-encoded records.

The cost is real (one more service to run, one more thing in your hot path), but the alternative — schema discipline by social contract — fails at scale. Every team that's run Kafka long enough has stories of a producer change silently breaking three downstream consumers.

---

# Part VIII — Performance profile and ecosystem

## 19. Kafka's real performance profile (and a common misconception)

A common confusion is to label Kafka as "low-volume, high-latency, fault-tolerant." The reality is the opposite on two of three.

| Misconception | Reality |
|---|---|
| Low volume | **Very high volume.** Kafka exists *because* high-volume systems needed something better than databases or queues. LinkedIn (where Kafka was built) processes trillions of records/day on it. A single well-tuned cluster handles millions of messages/sec and multiple GB/s of throughput. |
| High latency | **Low latency.** Producer-to-consumer end-to-end is typically 2–10 ms with `acks=all`. Not as low as in-memory pub/sub like Redis (~sub-ms), but very low for *durable, replicated* delivery. |
| Fault tolerant | **Correct.** Replication, ISR, controller failover all exist for this. |

The real profile: **high throughput, low latency, durable, fault tolerant, horizontally scalable.**

Two common sources of the misconception:

1. **Comparison with Flink.** People hear "Flink does sub-millisecond per-event processing" and assume Kafka must be slow. But Flink runs *on top of* Kafka. Kafka delivers events to Flink in single-digit ms; Flink then processes them.
2. **Comparison with Spark Structured Streaming.** Spark's micro-batch model adds 100–500 ms of batching latency on top of Kafka. The latency people complain about there is Spark's batching overhead, not Kafka's transport.

Why Kafka is fast (the "secret sauce"):
1. **Sequential I/O** + **page cache** — disk acts like memory for hot data.
2. **Zero-copy** (`sendfile`) on the broker.
3. **Batching + compression** on the producer.
4. **Pull-based** consumers — no broker bookkeeping per consumer; broker is dumb log server.
5. **Partition = independent unit** — scale by adding partitions and brokers.

## 20. Real problems Kafka solved

To understand *why* Kafka exists, picture LinkedIn's data plumbing in 2010 (where it was built). They had:

- A user database, a profile database, a search index, a recommendation system, a data warehouse, a real-time analytics system, ML training pipelines, an email system, a messaging system.
- Each pair of systems that needed to share data had a **point-to-point integration** — usually a custom batch job, sometimes an HTTP API.
- N systems meant up to N² integrations to build and maintain.
- Most data sync was **batch-oriented** — nightly jobs moving yesterday's data to the warehouse.
- Adding any new consumer (a new ML model, a new analytics tool) meant building yet another extraction from every source.

Kafka introduced one primitive — **the durable, replayable, partitioned log** — and it cleanly solved a stack of problems:

**1. The N² integration problem becomes N+M.**
Every producer publishes to a topic. Every consumer subscribes. New consumer = subscribe, no producer change.

**2. Real-time replaces batch.**
The same data that used to arrive in the warehouse the next morning now flows continuously.

**3. Replayability turned into a superpower.**
With retention measured in days/weeks/forever, a new consumer (new ML model, new dashboard, new derived database) can replay the entire history. **This made experimentation cheap.**

**4. Producers and consumers got decoupled in time.**
Slow consumer? Doesn't matter — the log absorbs the lag. Burst traffic? The log buffers it.

**5. The log became the source of truth.**
Database CDC, event sourcing, the data lakehouse pattern — all built on Kafka as the canonical event stream.

**6. Throughput at internet scale.**
Earlier brokers (ActiveMQ, RabbitMQ at the time) topped out at tens of thousands of messages per second. Kafka was designed for *millions* per second per broker.

**7. Stream processing got a backbone.**
Flink, Spark Structured Streaming, Kafka Streams, ksqlDB — all of modern stream processing assumes a Kafka-shaped substrate.

## 21. Industries and use cases where Kafka thrives

The pattern is consistent: **Kafka thrives anywhere event volume is high, decoupling matters, and you need to feed the same data to many downstream systems in real time.**

### By industry

**Tech / consumer internet** — basically all of FAANG-scale infra:
- LinkedIn (origin), Netflix, Uber, Airbnb, Pinterest, Twitter/X, Spotify, Booking.com.
- Use cases: every user action emitted as an event, monitoring, recommendations, A/B testing, data warehouse loading.

**Financial services** — second-largest adopter category:
- Banks (Goldman Sachs, JPMorgan, Capital One, ING), exchanges, fintech (Stripe, Robinhood).
- Use cases: trade events, order book updates, real-time fraud detection, risk pipelines, regulatory reporting (must capture and replay), payment authorization streams.

**Retail / e-commerce:**
- Walmart (one of the largest non-tech Kafka users), Target, Tesco, Shopify, Etsy.
- Use cases: inventory sync across thousands of stores, order events, real-time pricing, recommendation pipelines.

**Telecom:**
- AT&T, Verizon, Vodafone.
- Use cases: call detail records, network event streams, real-time billing, IoT data from cell towers.

**Logistics / mobility:**
- Uber, Lyft, DoorDash, FedEx, DHL.
- Use cases: GPS pings from millions of devices, route optimization, supply-demand matching, ETAs.

**Manufacturing / industrial IoT:**
- Bosch, Siemens, Tesla.
- Use cases: sensor streams from factory floors, predictive maintenance, vehicle telemetry, supply chain visibility.

**Healthcare:**
- Cerner, Epic-adjacent integrations, large hospital networks.
- Use cases: HL7 / FHIR event streams, patient monitoring, claims processing.

**Gaming:**
- Riot, Epic, Activision.
- Use cases: every player action, real-time leaderboards, anti-cheat, telemetry-driven matchmaking.

### Use case archetypes (the patterns repeat across industries)

**1. Activity tracking / clickstream.** Every user action becomes an event. Origin use case at LinkedIn.

**2. Log aggregation.** App logs across thousands of containers stream into Kafka, then fan out to ELK, Splunk, Datadog, S3.

**3. Metrics pipeline.** Metrics agents emit to Kafka, then a consumer writes to a time-series DB.

**4. Change data capture (CDC).** Database WAL → Kafka topic → downstream systems. **Debezium** is the canonical tool.

**5. Event sourcing.** Business events are the *source of truth*. Application state is derived by replaying the log.

**6. Microservices integration.** Services publish domain events; other services react.

**7. Stream processing pipeline.** Kafka → Flink / Spark / Kafka Streams → Kafka → sink.

**8. Real-time fraud / anomaly detection.** Transactions → Kafka → ML scoring → flagged events.

**9. Data lake / lakehouse ingestion.** Real-time replacement for batch ETL. Kafka → S3 / ADLS / GCS.

**10. Outbox pattern.** App writes to its DB and to an `outbox` table in the same transaction. A separate process tails the outbox and publishes to Kafka. Solves the dual-write problem.

## 22. Where Kafka is *not* the right tool

- **Tiny systems (< few thousand events/day)** — operational overhead outweighs benefit. Use SQS, Pub/Sub, or just a database table with polling.
- **Request/reply RPC** — Kafka isn't an RPC system. Use HTTP/gRPC.
- **Strong total ordering across all data** — Kafka orders within a partition only.
- **Sub-millisecond latency** — Kafka's ~5–10 ms is fast, but for HFT-grade latencies you reach for Aeron, Chronicle, or shared memory.
- **Tiny payloads, massive fan-out, fire-and-forget** — pub/sub systems like Redis Streams or NATS can be cheaper.
- **You only need it for one consumer** — if there's truly one consumer and it doesn't need replay, a queue is simpler.

The honest test: **do you have multiple consumers of the same data, real-time requirements, and volume that hurts in batch?** If yes, Kafka is the right tool, full stop.

---

# Part IX — Building on Kafka

## 23. Practical engineering decisions

### Picking the partition count

Partition count is the single most consequential topic-level decision because it's painful to change for keyed topics — repartitioning breaks the `hash(key) % N` mapping.

Rough sizing:
- **Throughput target / per-partition throughput**. A single partition can usually handle 10–50 MB/s, but per-consumer processing speed is usually the lower bound.
- **Parallelism ceiling**. A consumer group can have at most `numPartitions` active members. If you might want 24 consumers eventually, you need at least 24 partitions.
- **Cost of overshooting**. Each partition costs file handles, memory for indices, and metadata.

A practical rule: pick partitions for **2–3× current peak throughput**, leaving headroom but not enormous overhead.

### Choosing keys

The key determines partition assignment, which determines ordering. Pick a key that groups records you need ordered (`userId`, `accountId`, `orderId`) — never a key with extreme skew (a single hot key sends all traffic to one partition).

Skew is one of the most common operational problems. Symptoms: one consumer in a group is always behind, one broker's disk is filling faster than others.

### Producer config worth knowing

| Setting | Sensible default | Why |
|---|---|---|
| `acks` | `all` | Durability. |
| `enable.idempotence` | `true` | Dedup retries; no downside. |
| `retries` | `Integer.MAX_VALUE` | Idempotence makes infinite retries safe. |
| `max.in.flight.requests.per.connection` | `5` | With idempotence on, ordering is preserved up to 5 in-flight. |
| `compression.type` | `lz4` or `zstd` | Cheap CPU, big network/disk savings. |
| `linger.ms` | `5–20` | Trade a little latency for much better batching. |
| `batch.size` | `64KB`+ | Bigger batches → better throughput. |

### Consumer config worth knowing

| Setting | Sensible default | Why |
|---|---|---|
| `enable.auto.commit` | `false` | You almost always want manual commits after processing. |
| `isolation.level` | `read_committed` (if upstream is transactional) | Skip aborted records. |
| `max.poll.records` | `500` | Cap per-poll work so heartbeats don't time out. |
| `session.timeout.ms` / `heartbeat.interval.ms` | `45000` / `3000` | Long enough to ride through GC; short enough to fail fast. |
| `max.poll.interval.ms` | `5min`+ | Largest gap between `poll()` calls before the member is kicked. |

### What to monitor

- **Consumer lag** per group, per partition. The single most important metric.
- **Under-replicated partitions** (URP) — should be 0 in a healthy cluster.
- **ISR shrink/expand events**. Frequent shrinks indicate flaky brokers or saturated disks/network.
- **Request latency** — `Produce` and `Fetch` p99.
- **Disk usage per broker** + rate of growth.
- **Broker JVM** — GC pause time, heap usage. Brokers should run small heaps (~6 GB); the OS page cache does the heavy lifting.

### Common pitfalls

- **Auto-commit + treating Kafka as exactly-once.** Records can be processed and the auto-committer crashes before committing → reprocess on next run.
- **One giant partition for "ordering."** You've thrown away parallelism. Use a key that groups what must be ordered.
- **Tiny `max.poll.interval.ms` with slow handlers.** The handler takes longer than the timeout → the consumer is kicked → rebalance storm.
- **`acks=1` "for performance."** With modern compression and batching, the throughput cost of `acks=all` is small; the durability cost of `acks=1` is real.
- **Treating the broker as your application's database.** Kafka's retention is a buffer, not a system of record.

## 24. Running Kafka locally for learning

Single-broker KRaft cluster in Docker. Save as `docker-compose.yml`:

```yaml
services:
  kafka:
    image: confluentinc/cp-kafka:7.6.0
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: 'broker,controller'
      KAFKA_LISTENERS: 'PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093'
      KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT://localhost:9092'
      KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT'
      KAFKA_CONTROLLER_QUORUM_VOTERS: '1@localhost:9093'
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      CLUSTER_ID: 'MkU3OEVBNTcwNTJENDM2Qk'
```

Useful CLI commands (run inside the container with `docker compose exec kafka bash`):

```bash
# create a topic
kafka-topics --bootstrap-server localhost:9092 \
  --create --topic orders --partitions 4 --replication-factor 1

# describe it
kafka-topics --bootstrap-server localhost:9092 --describe --topic orders

# produce from stdin
kafka-console-producer --bootstrap-server localhost:9092 --topic orders \
  --property "parse.key=true" --property "key.separator=:"

# consume from beginning, including keys
kafka-console-consumer --bootstrap-server localhost:9092 --topic orders \
  --from-beginning --property "print.key=true" --property "key.separator=: "

# inspect consumer group lag
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group order-fulfillment

# look at log segment contents
kafka-dump-log --files /var/lib/kafka/data/orders-0/00000000000000000000.log --print-data-log
```

That last one (`kafka-dump-log`) is worth running once — seeing the actual record batches on disk demystifies the whole storage layer.

## 25. How Kafka fits with Flink and Spark

Kafka is the **transport and durable buffer** layer. Flink and Spark Structured Streaming both read from Kafka topics, do stateful processing, and typically write back to other Kafka topics (or to a sink).

The clean mental separation:

- Kafka: **moves and stores** records, durably and replayable.
- Flink / Spark: **transforms** records — joins, aggregations, windowing, enrichment.

A typical real pipeline looks like:

```
sources →  Kafka topic "raw"  →  Flink/Spark job  →  Kafka topic "enriched"  →  multiple sinks
                                  (state, joins,
                                   windows)
```

Each arrow is durable. Each stage can fail and recover independently. That is the architectural payoff of using Kafka as the spine.

**Next stops in this series:**
- `../flink/` — JobManager/TaskManager topology, checkpoint barriers, watermarks, RocksDB state backend.
- `../spark/` — micro-batch model, structured streaming, state store, checkpoint dir.

---

*End of Kafka section. Resume here for Flink → Spark.*
