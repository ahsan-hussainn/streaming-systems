"""Build kafka-storage.excalidraw — partition on disk: segment files, sparse
index, sendfile() to consumer. Supporting visual for Part 2."""
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

# Palette
BROKER_S = "#2f9e44"
PART_FOL_S = "#868e96"
HASH_BG, HASH_S = "#fff3bf", "#e67700"
CONSUMER_BG, CONSUMER_S = "#ffd8a8", "#e8590c"

# Title
elements.append(text(120, 20, "What a partition looks like on disk",
                     size=20, bold=True, w=700, h=30))
elements.append(text(120, 56, "Append-only segment files, sparse index, zero-copy reads.",
                     size=13, w=700, h=20, color="#555"))

# Filesystem path
elements.append(text(40, 95, "/var/kafka-logs/orders-0/", size=14, bold=True,
                     w=400, h=20, color="#666", align="left"))

# --- Active .log segment ---
seg_x, seg_y, seg_w, seg_h = 40, 130, 600, 90
elements.append(rect(seg_x, seg_y, seg_w, seg_h, stroke=BROKER_S, bg="#f4fbf6", sw=2))
elements.append(text(seg_x + 10, seg_y + 8, "00000000000000000000.log   (active segment)",
                     size=13, bold=True, w=400, h=18, color=BROKER_S, align="left"))

# Records inside
rec_y = seg_y + 38
rec_w, rec_gap = 50, 4
rec_x_start = seg_x + 20
labels = ["A", "B", "C", "D", "E", "F", "...", "", "next", ""]
for i in range(9):
    x = rec_x_start + i * (rec_w + rec_gap)
    if i == 8:
        # "next" slot, dashed
        elements.append(rect(x, rec_y, rec_w, 40, stroke="#adb5bd", bg="#f8f9fa", sw=1, dashed=True))
        elements.append(text(x, rec_y + 12, "next", size=11, w=rec_w, h=18, color="#999"))
    elif labels[i] == "...":
        elements.append(text(x, rec_y + 12, "...", size=14, w=rec_w, h=18, color="#888"))
    elif labels[i]:
        elements.append(rect(x, rec_y, rec_w, 40, stroke=PART_FOL_S, bg="#fff", sw=1))
        elements.append(text(x, rec_y + 12, labels[i], size=12, bold=True, w=rec_w, h=18))

# Append arrow at the tail of the active segment
last_x = rec_x_start + 8 * (rec_w + rec_gap)
elements.append(arrow(last_x - 15, rec_y + 20, last_x + 2, rec_y + 20, color="#666", sw=2))

# Callout 1: Sequential append (right of the active .log)
elements.append(text(seg_x + seg_w + 20, seg_y + 22, "Sequential append",
                     size=12, bold=True, w=200, h=18, color=BROKER_S, align="left"))
elements.append(text(seg_x + seg_w + 20, seg_y + 42, "via OS page cache",
                     size=11, w=200, h=16, color="#555", align="left"))
elements.append(text(seg_x + seg_w + 20, seg_y + 58, "(no in-process buffer)",
                     size=10, w=200, h=14, color="#888", align="left"))

# --- .index file ---
idx_x, idx_y, idx_w, idx_h = 40, 250, 600, 65
elements.append(rect(idx_x, idx_y, idx_w, idx_h, stroke=HASH_S, bg=HASH_BG, sw=2))
elements.append(text(idx_x + 10, idx_y + 8, "00000000000000000000.index",
                     size=13, bold=True, w=400, h=18, color=HASH_S, align="left"))
elements.append(text(idx_x + 10, idx_y + 32,
                     "0 → 0     100 → 23456     200 → 47890     300 → 72104     ...",
                     size=12, w=580, h=18, align="left"))

# Callout 2: Sparse index
elements.append(text(idx_x + idx_w + 20, idx_y + 16, "Sparse index",
                     size=12, bold=True, w=200, h=18, color=HASH_S, align="left"))
elements.append(text(idx_x + idx_w + 20, idx_y + 36, "one entry per ~4 KB",
                     size=11, w=200, h=16, color="#555", align="left"))
elements.append(text(idx_x + idx_w + 20, idx_y + 52, "(offset → byte position)",
                     size=10, w=200, h=14, color="#888", align="left"))

# --- .timeindex ---
tix_x, tix_y, tix_w, tix_h = 40, 332, 600, 40
elements.append(rect(tix_x, tix_y, tix_w, tix_h, stroke="#adb5bd", bg="#fff", sw=1))
elements.append(text(tix_x + 10, tix_y + 12,
                     "00000000000000000000.timeindex   (timestamp → offset)",
                     size=12, w=580, h=16, color="#555", align="left"))

# --- Rolled (older) segment ---
old_x, old_y, old_w, old_h = 40, 388, 600, 40
elements.append(rect(old_x, old_y, old_w, old_h, stroke="#adb5bd", bg="#f1f3f5", sw=1))
elements.append(text(old_x + 10, old_y + 12,
                     "00000000000123450000.log   (previous segment — rolled at size or time limit)",
                     size=12, w=580, h=16, color="#666", align="left"))

# --- Consumer box on the right ---
con_x, con_y, con_w, con_h = 850, 145, 130, 60
elements.append(rect(con_x, con_y, con_w, con_h, stroke=CONSUMER_S, bg=CONSUMER_BG, sw=2))
elements.append(text(con_x, con_y + 14, "Consumer", size=14, bold=True, w=con_w, h=22, color=CONSUMER_S))
elements.append(text(con_x, con_y + 36, "(socket)", size=10, w=con_w, h=14, color="#666"))

# sendfile() arrow from active .log to consumer
elements.append(arrow(seg_x + seg_w + 5, seg_y + 70, con_x - 5, con_y + 30,
                      color=CONSUMER_S, sw=2))
elements.append(text(seg_x + seg_w + 30, seg_y + 92, "sendfile()",
                     size=13, bold=True, w=200, h=18, color=CONSUMER_S, align="left"))
elements.append(text(seg_x + seg_w + 30, seg_y + 111, "kernel-mode zero copy:",
                     size=11, w=240, h=16, color="#666", align="left"))
elements.append(text(seg_x + seg_w + 30, seg_y + 127, "page cache → socket,",
                     size=11, w=240, h=16, color="#666", align="left"))
elements.append(text(seg_x + seg_w + 30, seg_y + 143, "never copied to user space.",
                     size=11, w=240, h=16, color="#666", align="left"))

# Footer
elements.append(text(40, 460, "Records are never modified in place. Whole segments age out via retention (time or size).",
                     size=12, w=950, h=18, color="#555", align="left"))
elements.append(text(40, 480, "Log compaction can keep only the latest record per key — used by __consumer_offsets, CDC outboxes, and state-store changelogs.",
                     size=12, w=950, h=18, color="#555", align="left"))

out = {
    "type": "excalidraw",
    "version": 2,
    "source": "https://excalidraw.com",
    "elements": elements,
    "appState": {"gridSize": None, "viewBackgroundColor": "#ffffff"},
    "files": {},
}
Path(__file__).with_name("kafka-storage.excalidraw").write_text(json.dumps(out, indent=2))
print(f"Wrote kafka-storage.excalidraw with {len(elements)} elements.")
