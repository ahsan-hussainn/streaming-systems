# Apache Kafka

The Kafka portion of the Streaming Systems series.

## Articles

| Part | Title | Source | Published |
|---|---|---|---|
| 1 | Apache Kafka Architecture for Beginners | [`part-1-kafka-internals.md`](./part-1-kafka-internals.md) | [Read on Medium →](https://medium.com/@ahsanhussain17999/apache-kafka-architecture-for-beginners-a242e0ac105c) |
| 2 | How One Record Travels Through Apache Kafka | [`part-2-record-lifecycle.md`](./part-2-record-lifecycle.md) | [Read on Medium →](https://medium.com/@ahsanhussain17999/how-one-record-travels-through-apache-kafka-e78aed234114) |

## Companion material

| File | What it is |
|---|---|
| [`kafka-internals-deep.md`](./kafka-internals-deep.md) | Long-form reference walkthrough the articles were distilled from. Goes byte-by-byte through the wire format, exactly-once, transactions, schema choice, and operational decisions. ~1,100 lines. |
| [`source/`](./source/) | Excalidraw source files for all diagrams, plus the Python scripts that generated them. |
| [`images/`](./images/) | PNG exports of the diagrams, referenced by the articles. |

## Reproducing the diagrams

```bash
cd source
python build_main_diagram.py        # → kafka.excalidraw (cluster architecture)
python build_supporting_visuals.py  # → partition_log, key_hashing, leader_follower, consumer_groups
python build_write_sequence.py      # → kafka-write-sequence.excalidraw (Part 2 hero)
python build_storage.py             # → kafka-storage.excalidraw (Part 2 storage detail)
```

To regenerate the PNGs, open each `.excalidraw` at [excalidraw.com](https://excalidraw.com) (File → Open) and export as PNG into `../images/`.

## Roadmap

- ✅ Part 1 — Cluster architecture
- ✅ Part 2 — Record lifecycle (produce → replicate → consume → commit)
- ⏳ Part 3 — Producer write path, deep
- ⏳ Part 4 — Consumer mechanics, deep
- ⏳ Part 5 — Exactly-once and transactions
- ⏳ Then — Apache Flink
- ⏳ Then — Apache Spark Structured Streaming
