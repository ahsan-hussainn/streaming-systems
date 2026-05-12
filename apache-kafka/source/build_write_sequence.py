"""Build kafka-write-sequence.excalidraw — the producer → broker → consumer
write-path sequence diagram, used as the hero for Part 2."""
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

def arrow(x1, y1, x2, y2, color="#1e1e1e", sw=2, dashed=False):
    return {
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

# Palette — matches the other diagrams
PRODUCER_BG, PRODUCER_S = "#a5d8ff", "#1971c2"
BROKER_BG, BROKER_S = "#b2f2bb", "#2f9e44"
CONSUMER_BG, CONSUMER_S = "#ffd8a8", "#e8590c"
KRAFT_S = "#862e9c"

# Title
elements.append(text(280, 20, "Write Path Sequence — Producer → Leader → Followers → Consumer  (acks=all, idempotence on)",
                     size=18, bold=True, w=720, h=28))

# Lifelines
lifelines = [
    {"x": 80,  "label": "Producer",         "bg": PRODUCER_BG, "s": PRODUCER_S},
    {"x": 290, "label": "Leader Broker",    "bg": BROKER_BG,   "s": BROKER_S},
    {"x": 510, "label": "Follower Broker",  "bg": BROKER_BG,   "s": BROKER_S},
    {"x": 720, "label": "Page Cache + Disk","bg": "#fff3bf",   "s": "#e67700"},
    {"x": 930, "label": "Consumer",         "bg": CONSUMER_BG, "s": CONSUMER_S},
]
HEADER_Y = 70
HEADER_H = 50
LL_TOP = HEADER_Y + HEADER_H
LL_BOT = LL_TOP + 700

for ll in lifelines:
    elements.append(rect(ll["x"], HEADER_Y, 160, HEADER_H, stroke=ll["s"], bg=ll["bg"]))
    elements.append(text(ll["x"], HEADER_Y + 14, ll["label"], size=14, bold=True, w=160, h=24, color=ll["s"]))
    cx = ll["x"] + 80
    elements.append(line(cx, LL_TOP, cx, LL_BOT, color="#adb5bd", sw=1, dashed=True))

def seq_msg(y, from_idx, to_idx, label, color="#1e1e1e", dashed=False):
    fx = lifelines[from_idx]["x"] + 80
    tx = lifelines[to_idx]["x"] + 80
    if from_idx == to_idx:
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

# Footer
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

out = {
    "type": "excalidraw",
    "version": 2,
    "source": "https://excalidraw.com",
    "elements": elements,
    "appState": {"gridSize": None, "viewBackgroundColor": "#ffffff"},
    "files": {},
}
Path(__file__).with_name("kafka-write-sequence.excalidraw").write_text(json.dumps(out, indent=2))
print(f"Wrote kafka-write-sequence.excalidraw with {len(elements)} elements.")
