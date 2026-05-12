"""Build the four supporting Excalidraw visuals for the Medium article."""
import json
from pathlib import Path

# Shared palette — matches kafka.excalidraw so the article feels coherent
PRODUCER_BG, PRODUCER_S = "#a5d8ff", "#1971c2"
BROKER_BG, BROKER_S = "#b2f2bb", "#2f9e44"
CONSUMER_BG, CONSUMER_S = "#ffd8a8", "#e8590c"
PART_LEAD_BG, PART_LEAD_S = "#ffc9c9", "#c92a2a"
PART_FOL_BG, PART_FOL_S = "#f1f3f5", "#868e96"
HASH_BG, HASH_S = "#fff3bf", "#e67700"
HW_S = "#862e9c"

_id = [0]
def nid():
    _id[0] += 1
    return f"e{_id[0]}"
def reset():
    _id[0] = 0

def rect(x, y, w, h, stroke="#1e1e1e", bg="transparent", sw=2, dashed=False, round_=True):
    return {
        "type": "rectangle", "version": 1, "versionNonce": 1, "isDeleted": False,
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
        "type": "text", "version": 1, "versionNonce": 1, "isDeleted": False,
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
        "type": "arrow", "version": 1, "versionNonce": 1, "isDeleted": False,
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

def write_file(filename, elements):
    out = {
        "type": "excalidraw",
        "version": 2,
        "source": "https://excalidraw.com",
        "elements": elements,
        "appState": {"gridSize": None, "viewBackgroundColor": "#ffffff"},
        "files": {},
    }
    Path(__file__).with_name(filename).write_text(json.dumps(out, indent=2))
    print(f"Wrote {filename} with {len(elements)} elements.")


# ============================================================
# Diagram A — partition_log.excalidraw
# ============================================================
def build_partition_log():
    reset()
    els = []
    els.append(text(200, 20, "topic: orders  —  4 partitions, each an append-only log",
                    size=20, bold=True, w=800, h=30))
    els.append(text(200, 56, "Records append at the tail. Offsets are monotonic and never change.",
                    size=13, w=800, h=20, color="#555"))

    BOX_W, BOX_H, GAP_X = 70, 50, 4
    LEFT_X = 110
    rows_y = [110, 200, 290, 380]
    records = [
        ["A", "B", "C", "D", "E", "F", "G", "H"],
        ["a", "b", "c", "d", "e", "f", "g"],
        ["x", "y", "z", "w"],
        ["m", "n", "o", "p", "q", "r"],
    ]
    for i, y in enumerate(rows_y):
        els.append(text(30, y + 18, f"P{i}", size=20, bold=True, w=70, h=24, color=PART_LEAD_S))
        recs = records[i]
        for j, rec in enumerate(recs):
            x = LEFT_X + j * (BOX_W + GAP_X)
            els.append(rect(x, y, BOX_W, BOX_H, stroke=PART_FOL_S, bg="#fff", sw=1))
            els.append(text(x, y + 4, f"offset {j}", size=10, w=BOX_W, h=14, color="#888"))
            els.append(text(x, y + 22, rec, size=14, bold=True, w=BOX_W, h=20))
        append_x = LEFT_X + len(recs) * (BOX_W + GAP_X)
        els.append(rect(append_x, y, BOX_W, BOX_H, stroke="#adb5bd", bg="#f8f9fa", sw=1, dashed=True))
        els.append(text(append_x, y + 14, "next", size=11, w=BOX_W, h=18, color="#999"))
        els.append(arrow(append_x - 10, y + 25, append_x + 4, y + 25, color="#666", sw=2))

    els.append(text(110, 470, "← oldest", size=12, w=120, h=18, color="#555", align="left"))
    els.append(text(660, 470, "newest →", size=12, w=120, h=18, color="#555", align="left"))
    write_file("partition_log.excalidraw", els)


# ============================================================
# Diagram B — key_hashing.excalidraw
# ============================================================
def build_key_hashing():
    reset()
    els = []
    els.append(text(200, 20, "How a producer picks a partition",
                    size=20, bold=True, w=600, h=30))
    els.append(text(200, 56, "hash(key) mod N  —  same key always lands on the same partition.",
                    size=13, w=600, h=20, color="#555"))

    keys = ["order-12345", "order-67890", "order-11111"]
    targets = [2, 0, 3]
    rec_y_start, rec_h, rec_gap = 110, 70, 30

    for i, key in enumerate(keys):
        y = rec_y_start + i * (rec_h + rec_gap)
        els.append(rect(40, y, 200, rec_h, stroke=PRODUCER_S, bg=PRODUCER_BG, sw=2))
        els.append(text(40, y + 8, "ProducerRecord", size=11, bold=True, w=200, h=16, color=PRODUCER_S))
        els.append(text(40, y + 28, f'key: "{key}"', size=12, w=200, h=18))
        els.append(text(40, y + 46, "value: { ... }", size=11, w=200, h=16, color="#666"))

    hx, hy, hw, hh = 360, 220, 200, 80
    els.append(rect(hx, hy, hw, hh, stroke=HASH_S, bg=HASH_BG, sw=2))
    els.append(text(hx, hy + 12, "Partitioner", size=13, bold=True, w=hw, h=18, color=HASH_S))
    els.append(text(hx, hy + 38, "hash(key) % N", size=15, bold=True, w=hw, h=22, color=HASH_S))

    px = 680
    py_start, ph, pg = 110, 60, 20
    for i in range(4):
        y = py_start + i * (ph + pg)
        is_target = i in targets
        bg = PART_LEAD_BG if is_target else "#fff"
        stroke = PART_LEAD_S if is_target else "#adb5bd"
        els.append(rect(px, y, 150, ph, stroke=stroke, bg=bg, sw=2))
        els.append(text(px, y + 12, f"Partition P{i}", size=14, bold=True, w=150, h=22, color=stroke))
        els.append(text(px, y + 36, "(ordered log)", size=11, w=150, h=16, color="#666"))

    # Records → hash box
    for i in range(3):
        ry = rec_y_start + i * (rec_h + rec_gap) + rec_h / 2
        els.append(arrow(240, ry, hx, hy + hh / 2, color="#555", sw=1.5))
    # Hash box → target partitions
    for t in sorted(set(targets)):
        ty = py_start + t * (ph + pg) + ph / 2
        els.append(arrow(hx + hw, hy + hh / 2, px, ty, color=HASH_S, sw=2))

    write_file("key_hashing.excalidraw", els)


# ============================================================
# Diagram C — leader_follower.excalidraw
# ============================================================
def build_leader_follower():
    reset()
    els = []
    els.append(text(120, 20, "Replication, up close: leader and follower for partition P0",
                    size=19, bold=True, w=820, h=28))
    els.append(text(120, 54, "Followers pull new records from the leader, the same way consumers do.",
                    size=13, w=820, h=20, color="#555"))

    BOX_W, BOX_H, GAP_X, LEFT_X = 65, 50, 4, 100
    leader_y = 140
    follower_y = 320
    leader_recs = ["A", "B", "C", "D", "E", "F", "G", "H", "I"]   # offsets 0..8, LEO=9
    follower_recs = ["A", "B", "C", "D", "E", "F", "G"]            # offsets 0..6, LEO=7
    HW = 7

    # Leader broker container
    els.append(rect(20, leader_y - 32, 900, BOX_H + 80, stroke=PART_LEAD_S, bg="#fff5f5", sw=2))
    els.append(text(30, leader_y - 24, "Broker 1  —  LEADER for P0", size=14, bold=True,
                    w=300, h=20, color=PART_LEAD_S, align="left"))
    for j, rec in enumerate(leader_recs):
        x = LEFT_X + j * (BOX_W + GAP_X)
        visible = j < HW
        bg = "#fff" if visible else "#f1f3f5"
        stroke = PART_LEAD_S if visible else "#adb5bd"
        els.append(rect(x, leader_y, BOX_W, BOX_H, stroke=stroke, bg=bg, sw=1.5))
        els.append(text(x, leader_y + 4, f"offset {j}", size=10, w=BOX_W, h=14, color="#888"))
        els.append(text(x, leader_y + 22, rec, size=14, bold=True, w=BOX_W, h=20))

    # HW marker on leader
    hw_x = LEFT_X + HW * (BOX_W + GAP_X)
    els.append(arrow(hw_x, leader_y + BOX_H + 4, hw_x, leader_y + BOX_H + 22, color=HW_S, sw=2))
    els.append(text(hw_x - 90, leader_y + BOX_H + 26, "HW = 7", size=12, bold=True, w=180, h=18, color=HW_S))
    els.append(text(hw_x - 90, leader_y + BOX_H + 44, "(visible to consumers)", size=10, w=180, h=14, color=HW_S))

    # LEO marker on leader
    leo_x = LEFT_X + 9 * (BOX_W + GAP_X)
    els.append(text(leo_x - 40, leader_y - 18, "LEO = 9 (next write)", size=11, w=200, h=14, color="#666"))

    # Follower broker container
    els.append(rect(20, follower_y - 32, 900, BOX_H + 60, stroke=PART_FOL_S, bg="#f8f9fa", sw=2))
    els.append(text(30, follower_y - 24, "Broker 2  —  FOLLOWER for P0", size=14, bold=True,
                    w=300, h=20, color=PART_FOL_S, align="left"))
    for j, rec in enumerate(follower_recs):
        x = LEFT_X + j * (BOX_W + GAP_X)
        els.append(rect(x, follower_y, BOX_W, BOX_H, stroke=PART_FOL_S, bg="#fff", sw=1))
        els.append(text(x, follower_y + 4, f"offset {j}", size=10, w=BOX_W, h=14, color="#888"))
        els.append(text(x, follower_y + 22, rec, size=14, bold=True, w=BOX_W, h=20))

    # FetchRequest arrow (follower → leader, upward)
    fx = LEFT_X + 4 * (BOX_W + GAP_X) + BOX_W / 2
    els.append(arrow(fx, follower_y - 5, fx, leader_y + BOX_H + 6, color=PART_FOL_S, sw=2, dashed=True))
    mid_y = (leader_y + BOX_H + follower_y) / 2 - 18
    els.append(text(fx + 10, mid_y, "FetchRequest", size=12, bold=True, w=200, h=18, color=PART_FOL_S, align="left"))
    els.append(text(fx + 10, mid_y + 18, "(follower pulls)", size=11, w=200, h=16, color="#888", align="left"))

    # Bottom captions
    els.append(text(40, 440, "Leader holds records up to offset 8 (LEO = 9), but only 0–6 are below the High Watermark.",
                    size=12, w=900, h=18, color="#555", align="left"))
    els.append(text(40, 460, "Consumers see 0–6. Records 7 and 8 stay hidden until the follower catches up.",
                    size=12, w=900, h=18, color="#555", align="left"))

    write_file("leader_follower.excalidraw", els)


# ============================================================
# Diagram D — consumer_groups.excalidraw
# ============================================================
def build_consumer_groups():
    reset()
    els = []
    els.append(text(150, 20, "Two consumer groups, one log",
                    size=20, bold=True, w=700, h=30))
    els.append(text(150, 56, "Each group tracks its own offset. The broker doesn't know who's reading.",
                    size=13, w=700, h=20, color="#555"))

    BOX_W, BOX_H, GAP_X, LEFT_X = 55, 50, 3, 50
    tape_y = 180
    recs = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O"]

    els.append(text(LEFT_X, tape_y - 26, "Partition P0  (topic: orders)", size=13, bold=True,
                    w=300, h=18, color=PART_LEAD_S, align="left"))
    for j, rec in enumerate(recs):
        x = LEFT_X + j * (BOX_W + GAP_X)
        els.append(rect(x, tape_y, BOX_W, BOX_H, stroke="#adb5bd", bg="#fff", sw=1))
        els.append(text(x, tape_y + 4, f"{j}", size=10, w=BOX_W, h=14, color="#888"))
        els.append(text(x, tape_y + 22, rec, size=14, bold=True, w=BOX_W, h=20))

    # Group analytics — lagging at offset 4 (above)
    g2 = 4
    g2x = LEFT_X + g2 * (BOX_W + GAP_X) + BOX_W / 2
    els.append(rect(g2x - 130, tape_y - 110, 260, 55, stroke="#e67700", bg=HASH_BG, sw=2))
    els.append(text(g2x - 130, tape_y - 102, "group: analytics", size=13, bold=True,
                    w=260, h=18, color="#e67700"))
    els.append(text(g2x - 130, tape_y - 82, "committed offset = 4  (lagging — fine!)",
                    size=11, w=260, h=16, color="#555"))
    els.append(arrow(g2x, tape_y - 50, g2x, tape_y - 6, color="#e67700", sw=2.5))

    # Group order-fulfillment — real-time at offset 12 (below)
    g1 = 12
    g1x = LEFT_X + g1 * (BOX_W + GAP_X) + BOX_W / 2
    els.append(arrow(g1x, tape_y + BOX_H + 30, g1x, tape_y + BOX_H + 6, color=CONSUMER_S, sw=2.5))
    els.append(rect(g1x - 130, tape_y + BOX_H + 35, 260, 55, stroke=CONSUMER_S, bg=CONSUMER_BG, sw=2))
    els.append(text(g1x - 130, tape_y + BOX_H + 42, "group: order-fulfillment", size=13, bold=True,
                    w=260, h=18, color=CONSUMER_S))
    els.append(text(g1x - 130, tape_y + BOX_H + 62, "committed offset = 12  (real-time)",
                    size=11, w=260, h=16, color="#555"))

    # Bottom note
    els.append(text(40, 400, "Both groups read the same records. Offsets are stored separately in the internal __consumer_offsets topic.",
                    size=12, w=950, h=18, color="#555", align="left"))
    els.append(text(40, 420, "A brand-new group reading from offset 0 sees the full history; an old group can pause for a week and pick up where it left off.",
                    size=12, w=950, h=18, color="#555", align="left"))

    write_file("consumer_groups.excalidraw", els)


if __name__ == "__main__":
    build_partition_log()
    build_key_hashing()
    build_leader_follower()
    build_consumer_groups()
