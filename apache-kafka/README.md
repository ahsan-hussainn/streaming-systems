# Apache Kafka

Part 1 of the Streaming Systems series.

## Contents

| File | What it is |
|---|---|
| [`part-1-kafka-internals.md`](./part-1-kafka-internals.md) | The published article — a structured, visual tour of Kafka's internals (producers, brokers, partitions, leaders/followers, consumer groups). ~1,750 words. |
| [`kafka-internals-deep.md`](./kafka-internals-deep.md) | Long-form reference walkthrough this article was distilled from. Goes byte-by-byte through the wire format, exactly-once, transactions, schema choice, and operational decisions. ~1,100 lines. |
| [`source/`](./source/) | Excalidraw source files for all five diagrams, plus the Python scripts that generated them. |
| [`images/`](./images/) | PNG exports of the diagrams, referenced by the article. |

## Reproducing the diagrams

```bash
cd source
python build_main_diagram.py        # → kafka.excalidraw (cluster architecture)
python build_supporting_visuals.py  # → partition_log, key_hashing, leader_follower, consumer_groups
```

To regenerate the PNGs, open each `.excalidraw` at [excalidraw.com](https://excalidraw.com) (File → Open) and export as PNG into `../images/`.

## What's next

- Part 2 — The Kafka write path
- Part 3 — The Kafka read path
- Part 4 — Exactly-once and transactions
- Part 5 onward — Apache Flink
- Then — Apache Spark Structured Streaming
