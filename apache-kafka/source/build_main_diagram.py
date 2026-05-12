"""Build kafka.excalidraw — architecture diagram + write-path sequence diagram."""
import json
from pathlib import Path

elements = []
_id = 0
def nid():
    global _id
    _id += 1
    return f"e{_id}"

def rect(x, y, w, h, stroke="#1e1e1e", bg="transparent", sw=2, dashed=False, round_=True):
    return {
        "type": "rectangle", "version": 1, "versionNonce": _id+1, "isDeleted": False,
        "id": nid(), "fillStyle": "solid", "strokeWidth": sw,
        "strokeStyle": "dashed" if dashed else "solid",
        "roughness": 1, "opacity": 100, "angle": 0,
        "x": x, "y": y, "strokeColor": stroke, "backgroundColor": bg,
        "width": w, "height": h, "seed": 1, "groupIds": [], "frameId": None,
        "roundness": {"type": 3} if round_ else None,
        "boundElements": [], "updated": 1, "link": None, "locked": False,
    }

def text(x, y, content, size=16, color="#1e1e1e", w=None, h=None, align="center", bold=False):
    if w is None:
        w = max(80, int(len(content) * size * 0.6))
    if h is None:
        h = int(size * 1.4)
    return {
        "type": "text", "version": 1, "versionNonce": _id+1, "isDeleted": False,
        "id": nid(), "fillStyle": "solid", "strokeWidth": 1, "strokeStyle": "solid",
        "roughness": 1, "opacity": 100, "angle": 0,
        "x": x, "y": y, "strokeColor": color, "backgroundColor": "transparent",
        "width": w, "height": h, "seed": 1, "groupIds": [], "frameId": None,
        "roundness": None, "boundElements": [], "updated": 1, "link": None, "locked": False,
        "fontSize": size, "fontFamily": 5 if bold else 1,
        "text": content, "textAlign": align, "verticalAlign": "middle",
        "containerId": None, "originalText": content,
        "lineHeight": 1.25, "baseline": int(size * 1.1),
    }

def arrow(x1, y1, x2, y2, color="#1e1e1e", sw=2, dashed=False, label=None):
    el = {
        "type": "arrow", "version": 1, "versionNonce": _id+1, "isDeleted": False,
        "id": nid(), "fillStyle": "solid", "strokeWidth": sw,
        "strokeStyle": "dashed" if dashed else "solid",
        "roughness": 1, "opacity": 100, "angle": 0,
        "x": x1, "y": y1, "strokeColor": color, "backgroundColor": "transparent",
        "width": abs(x2 - x1), "height": abs(y2 - y1), "seed": 1,
        "groupIds": [], "frameId": None, "roundness": {"type": 2},
        "boundElements": [], "updated": 1, "link": None, "locked": False,
        "startBinding": None, "endBinding": None, "lastCommittedPoint": None,
        "startArrowhead": None, "endArrowhead": "arrow",
        "points": [[0, 0], [x2 - x1, y2 - y1]],
    }
    return el

def line(x1, y1, x2, y2, color="#868e96", sw=1, dashed=True):
    return {
        "type": "line", "version": 1, "versionNonce": _id+1, "isDeleted": False,
        "id": nid(), "fillStyle": "solid", "strokeWidth": sw,
        "strokeStyle": "dashed" if dashed else "solid",
        "roughness": 1, "opacity": 100, "angle": 0,
        "x": x1, "y": y1, "strokeColor": color, "backgroundColor": "transparent",
        "width": abs(x2 - x1), "height": abs(y2 - y1), "seed": 1,
        "groupIds": [], "frameId": None, "roundness": {"type": 2},
        "boundElements": [], "updated": 1, "link": None, "locked": False,
        "startBinding": None, "endBinding": None, "lastCommittedPoint": None,
        "startArrowhead": None, "endArrowhead": None,
        "points": [[0, 0], [x2 - x1, y2 - y1]],
    }

# Color palette
PRODUCER_BG, PRODUCER_S = "#a5d8ff", "#1971c2"
BROKER_BG, BROKER_S = "#b2f2bb", "#2f9e44"
CLUSTER_S = "#495057"
KRAFT_BG, KRAFT_S = "#eebefa", "#862e9c"
CONSUMER_BG, CONSUMER_S = "#ffd8a8", "#e8590c"
PART_LEAD_BG, PART_LEAD_S = "#ffc9c9", "#c92a2a"
PART_FOL_BG, PART_FOL_S = "#f1f3f5", "#868e96"
LIFELINE_BG, LIFELINE_S = "#dee2e6", "#343a40"

# ============================================================
# ARCHITECTURE DIAGRAM (y = 0..680)
# ============================================================
elements.append(text(360, 10, "Apache Kafka — Cluster Architecture", size=22, bold=True, w=520, h=32))

# Producers (left column)
elements.append(text(40, 70, "PRODUCERS", size=14, bold=True, w=160, h=20, color="#1971c2"))
for i, name in enumerate(["order-service", "payment-service", "inventory-service"]):
    y = 110 + i * 80
    elements.append(rect(40, y, 160, 60, stroke=PRODUCER_S, bg=PRODUCER_BG))
    elements.append(text(40, y + 18, name, size=14, w=160, h=24))

# Cluster outer box
CLUSTER_X, CLUSTER_Y, CLUSTER_W, CLUSTER_H = 250, 70, 580, 540
elements.append(rect(CLUSTER_X, CLUSTER_Y, CLUSTER_W, CLUSTER_H, stroke=CLUSTER_S, sw=2, dashed=True, bg="#f8f9fa"))
elements.append(text(CLUSTER_X + 10, CLUSTER_Y + 6, "KAFKA CLUSTER  (3 brokers, topic 'orders' partitions=4 RF=2)",
                     size=13, bold=True, w=560, h=20, align="left", color=CLUSTER_S))

# KRaft controller bar
elements.append(rect(CLUSTER_X + 20, CLUSTER_Y + 36, CLUSTER_W - 40, 38, stroke=KRAFT_S, bg=KRAFT_BG))
elements.append(text(CLUSTER_X + 20, CLUSTER_Y + 42, "KRaft Controller Quorum  —  metadata, leader election, ISR tracking",
                     size=13, bold=True, w=CLUSTER_W - 40, h=24, color=KRAFT_S))

# Three brokers
broker_data = [
    {"x": CLUSTER_X + 20, "label": "Broker 1",
     "parts": [("P0", "leader"), ("P1", "follower"), ("P3", "leader")]},
    {"x": CLUSTER_X + 210, "label": "Broker 2",
     "parts": [("P1", "leader"), ("P2", "follower"), ("P0", "follower")]},
    {"x": CLUSTER_X + 400, "label": "Broker 3",
     "parts": [("P2", "leader"), ("P3", "follower")]},
]
BROKER_Y = CLUSTER_Y + 90
BROKER_H = 220
for b in broker_data:
    bx = b["x"]
    elements.append(rect(bx, BROKER_Y, 160, BROKER_H, stroke=BROKER_S, bg=BROKER_BG))
    elements.append(text(bx, BROKER_Y + 8, b["label"], size=14, bold=True, w=160, h=22, color=BROKER_S))
    for j, (pid, role) in enumerate(b["parts"]):
        py = BROKER_Y + 38 + j * 56
        if role == "leader":
            elements.append(rect(bx + 12, py, 136, 46, stroke=PART_LEAD_S, bg=PART_LEAD_BG, sw=2))
            elements.append(text(bx + 12, py + 6, f"{pid}  LEADER", size=12, bold=True, w=136, h=16, color=PART_LEAD_S))
            elements.append(text(bx + 12, py + 24, "log + .index + .timeindex", size=10, w=136, h=14, color="#666"))
        else:
            elements.append(rect(bx + 12, py, 136, 46, stroke=PART_FOL_S, bg=PART_FOL_BG, sw=1))
            elements.append(text(bx + 12, py + 6, f"{pid}  follower", size=12, w=136, h=16, color=PART_FOL_S))
            elements.append(text(bx + 12, py + 24, "replica (pulls from leader)", size=10, w=136, h=14, color="#666"))

# Bottom note inside cluster
elements.append(text(CLUSTER_X + 20, CLUSTER_Y + 480, "Followers fetch from leaders → leader advances High Watermark once full ISR is caught up.",
                     size=12, w=CLUSTER_W - 40, h=20, color="#495057"))
elements.append(text(CLUSTER_X + 20, CLUSTER_Y + 504, "On disk: append-only segment files rolled by size/time, served via OS page cache + sendfile().",
                     size=12, w=CLUSTER_W - 40, h=20, color="#495057"))

# Consumer groups (right column)
elements.append(text(870, 70, "CONSUMER GROUPS", size=14, bold=True, w=210, h=20, color=CONSUMER_S))
# Group A
GA_Y = 110
elements.append(rect(870, GA_Y, 210, 200, stroke=CONSUMER_S, bg=CONSUMER_BG))
elements.append(text(870, GA_Y + 8, "group: order-fulfillment", size=13, bold=True, w=210, h=20, color=CONSUMER_S))
elements.append(rect(884, GA_Y + 38, 182, 64, stroke=CONSUMER_S, bg="#fff", sw=1))
elements.append(text(884, GA_Y + 50, "consumer-1", size=12, bold=True, w=182, h=18))
elements.append(text(884, GA_Y + 70, "owns P0, P3", size=11, w=182, h=16, color="#666"))
elements.append(rect(884, GA_Y + 116, 182, 64, stroke=CONSUMER_S, bg="#fff", sw=1))
elements.append(text(884, GA_Y + 128, "consumer-2", size=12, bold=True, w=182, h=18))
elements.append(text(884, GA_Y + 148, "owns P1, P2", size=11, w=182, h=16, color="#666"))

# Group B
GB_Y = 340
elements.append(rect(870, GB_Y, 210, 150, stroke=CONSUMER_S, bg=CONSUMER_BG))
elements.append(text(870, GB_Y + 8, "group: analytics", size=13, bold=True, w=210, h=20, color=CONSUMER_S))
elements.append(rect(884, GB_Y + 38, 182, 90, stroke=CONSUMER_S, bg="#fff", sw=1))
elements.append(text(884, GB_Y + 52, "consumer-1", size=12, bold=True, w=182, h=18))
elements.append(text(884, GB_Y + 76, "owns ALL partitions", size=11, w=182, h=16, color="#666"))
elements.append(text(884, GB_Y + 96, "(reads independently of group A)", size=10, w=182, h=14, color="#888"))

# Arrows: producers → brokers
elements.append(arrow(200, 140, 270, 200, color=PRODUCER_S))    # P1 → Broker1
elements.append(arrow(200, 220, 460, 220, color=PRODUCER_S))    # P2 → Broker2
elements.append(arrow(200, 300, 650, 240, color=PRODUCER_S))    # P3 → Broker3

# Arrows: brokers → consumer group A
elements.append(arrow(430, 240, 880, 180, color=CONSUMER_S))
elements.append(arrow(620, 240, 880, 260, color=CONSUMER_S))

# Arrows: brokers → consumer group B
elements.append(arrow(810, 280, 880, 400, color=CONSUMER_S))

# Replication arrows (between brokers, dashed)
elements.append(arrow(370, 280, 460, 280, color="#888", dashed=True, sw=1))
elements.append(arrow(620, 290, 540, 290, color="#888", dashed=True, sw=1))
elements.append(arrow(370, 350, 660, 350, color="#888", dashed=True, sw=1))

# Legend
LEG_Y = 640
elements.append(text(40, LEG_Y, "Legend:", size=13, bold=True, w=80, h=20))
elements.append(rect(120, LEG_Y, 14, 14, stroke=PART_LEAD_S, bg=PART_LEAD_BG))
elements.append(text(140, LEG_Y - 1, "= partition leader", size=12, w=140, h=18, align="left"))
elements.append(rect(290, LEG_Y, 14, 14, stroke=PART_FOL_S, bg=PART_FOL_BG))
elements.append(text(310, LEG_Y - 1, "= partition follower (replica)", size=12, w=200, h=18, align="left"))
elements.append(line(530, LEG_Y + 7, 580, LEG_Y + 7, color="#888", dashed=True))
elements.append(text(590, LEG_Y - 1, "= replication (follower pulls)", size=12, w=220, h=18, align="left"))

# ============================================================
# SEQUENCE DIAGRAM (y = 740..1500)
# ============================================================
SEQ_TOP = 740
elements.append(text(280, SEQ_TOP, "Write Path Sequence — Producer → Leader → Followers → Consumer  (acks=all, idempotence on)",
                     size=18, bold=True, w=720, h=28))

# Lifelines
lifelines = [
    {"x": 80,  "label": "Producer",         "bg": PRODUCER_BG, "s": PRODUCER_S},
    {"x": 290, "label": "Leader Broker",    "bg": BROKER_BG,   "s": BROKER_S},
    {"x": 510, "label": "Follower Broker",  "bg": BROKER_BG,   "s": BROKER_S},
    {"x": 720, "label": "Page Cache + Disk","bg": "#fff3bf",   "s": "#e67700"},
    {"x": 930, "label": "Consumer",         "bg": CONSUMER_BG, "s": CONSUMER_S},
]
HEADER_Y = SEQ_TOP + 50
HEADER_H = 50
LL_TOP = HEADER_Y + HEADER_H
LL_BOT = LL_TOP + 700

for ll in lifelines:
    elements.append(rect(ll["x"], HEADER_Y, 160, HEADER_H, stroke=ll["s"], bg=ll["bg"]))
    elements.append(text(ll["x"], HEADER_Y + 14, ll["label"], size=14, bold=True, w=160, h=24, color=ll["s"]))
    cx = ll["x"] + 80
    elements.append(line(cx, LL_TOP, cx, LL_BOT, color="#adb5bd", sw=1, dashed=True))

# Helper for sequence message
def seq_msg(y, from_idx, to_idx, label, color="#1e1e1e", dashed=False):
    fx = lifelines[from_idx]["x"] + 80
    tx = lifelines[to_idx]["x"] + 80
    if from_idx == to_idx:
        # self-message — small loop arrow + label
        elements.append(arrow(fx, y, fx + 60, y, color=color, dashed=dashed, sw=2))
        elements.append(arrow(fx + 60, y, fx + 60, y + 18, color=color, dashed=dashed, sw=2))
        elements.append(arrow(fx + 60, y + 18, fx + 4, y + 18, color=color, dashed=dashed, sw=2))
        elements.append(text(fx + 70, y - 4, label, size=11, w=240, h=18, align="left", color=color))
    else:
        elements.append(arrow(fx, y, tx, y, color=color, dashed=dashed, sw=2))
        mid_x = min(fx, tx) + 6
        elements.append(text(mid_x, y - 18, label, size=11,
                             w=abs(tx - fx) - 10, h=18, align="left", color=color))

y = LL_TOP + 30
step = 56
seq_msg(y, 0, 1, "1. ProduceRequest(acks=all, batch, PID+seq)", color=PRODUCER_S); y += step
seq_msg(y, 1, 3, "2. append batch → log segment (page cache)", color=BROKER_S); y += step
seq_msg(y, 2, 1, "3. (follower) FetchRequest", color="#868e96", dashed=True); y += step
seq_msg(y, 1, 2, "4. records up to offset N", color=BROKER_S); y += step
seq_msg(y, 2, 3, "5. follower also persists locally", color=BROKER_S); y += step
seq_msg(y, 1, 1, "6. ISR caught up → advance High Watermark", color=KRAFT_S); y += step + 10
seq_msg(y, 1, 0, "7. ProduceResponse OK (offset, partition)", color=PRODUCER_S); y += step + 14
seq_msg(y, 4, 1, "8. FetchRequest(partition, offset=X)", color=CONSUMER_S); y += step
seq_msg(y, 1, 4, "9. records (only those ≤ HW)", color=CONSUMER_S); y += step
seq_msg(y, 4, 4, "10. process records → side effects", color=CONSUMER_S); y += step + 10
seq_msg(y, 4, 1, "11. OffsetCommit → __consumer_offsets", color=CONSUMER_S); y += step

# Phase brackets
elements.append(text(80, LL_TOP + 4, "── PRODUCE PHASE ──", size=12, bold=True, w=850, h=18, align="left", color="#495057"))
elements.append(text(80, LL_TOP + 30 + step * 6 + 2, "── ACK RETURNED ──", size=12, bold=True, w=850, h=18, align="left", color="#495057"))
elements.append(text(80, LL_TOP + 30 + step * 7 + 14, "── CONSUME PHASE ──", size=12, bold=True, w=850, h=18, align="left", color="#495057"))

# Footer notes
NOTES_Y = LL_BOT + 30
elements.append(text(80, NOTES_Y, "Durability gates:", size=14, bold=True, w=300, h=20, align="left", color="#1e1e1e"))
notes = [
    "• acks=0 → producer doesn't wait. Loses data if leader crashes.",
    "• acks=1 → wait for leader's local write only. Loses data if leader crashes before step 4.",
    "• acks=all + min.insync.replicas≥2 → wait for full ISR. The durable choice.",
    "• Idempotent producer (PID + sequence) silently dedupes retries on the broker.",
    "• read_committed consumers skip records inside aborted transactions.",
]
for i, n in enumerate(notes):
    elements.append(text(80, NOTES_Y + 26 + i * 22, n, size=12, w=900, h=18, align="left"))

# ============================================================
# Write file
# ============================================================
out = {
    "type": "excalidraw",
    "version": 2,
    "source": "https://excalidraw.com",
    "elements": elements,
    "appState": {
        "gridSize": None,
        "viewBackgroundColor": "#ffffff",
    },
    "files": {},
}

Path(__file__).with_name("kafka.excalidraw").write_text(json.dumps(out, indent=2))
print(f"Wrote kafka.excalidraw with {len(elements)} elements.")
