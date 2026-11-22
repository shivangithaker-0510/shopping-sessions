#!/usr/bin/env python3
"""
Straightforward reference implementation of rules.md, written independently of
the Rust pipeline and optimised for obviousness, not speed (use on <= ~300K
events). Writes user_daily.csv and campaign_daily.csv in the dev-answer format
so scripts/compare.py can diff it against a Parquet output folder.

Usage: reference.py DATA_DIR OUT_DIR
"""
import calendar
import csv
import glob
import json
import os
import sys
from collections import defaultdict

DAY = 86_400_000
GAP = 1_800_000
WEEK = 604_800_000


def read_lines(path):
    with open(path, "rb") as f:
        data = f.read()
    lines = data.split(b"\n")
    # Only \n ends a line; a trailing \n does not start another line.
    if lines and lines[-1] == b"":
        lines.pop()
    return lines[1:]  # header


def fields(line, n):
    """The n field values of a line, or None if malformed (rules.md CSV dialect)."""
    if line == b"":
        return None
    raw = line.split(b",")
    if len(raw) != n:
        return None
    out = []
    for f in raw:
        if b'"' in f:
            if len(f) >= 2 and f[0:1] == b'"' and f[-1:] == b'"' and b'"' not in f[1:-1]:
                f = f[1:-1]
            else:
                return None
        out.append(f)
    return out


def rows(folder, n):
    for path in sorted(glob.glob(os.path.join(folder, "*.csv"))):
        for line in read_lines(path):
            r = fields(line, n)
            if r is not None:
                yield r


DIGITS = set(b"0123456789")


def all_digits(b):
    return len(b) > 0 and all(c in DIGITS for c in b)


def civil(s):
    """'YYYY-MM-DDTHH:MM:SS' -> epoch ms or None."""
    if len(s) != 19 or s[4:5] != b"-" or s[7:8] != b"-" or s[10:11] != b"T" or s[13:14] != b":" or s[16:17] != b":":
        return None
    parts = [s[0:4], s[5:7], s[8:10], s[11:13], s[14:16], s[17:19]]
    if not all(all_digits(p) for p in parts):
        return None
    y, mo, d, h, mi, se = (int(p) for p in parts)
    if not (1 <= mo <= 12) or h > 23 or mi > 59 or se > 59 or d < 1:
        return None
    if d > calendar.monthrange(y, mo)[1] if y > 0 else d > [31, 29 if False else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][mo - 1]:
        return None
    if y == 0:
        return None  # never inside a generated window; avoids calendar's year-0 gap
    days = (calendar.timegm((y, mo, d, 0, 0, 0)) // 86400)
    return ((days * 24 + h) * 60 + mi) * 60_000 + se * 1000


def parse_ts(s):
    if s == b"":
        return None
    if all_digits(s):
        v = int(s)
        return v if v <= 2**63 - 1 else None
    if len(s) == 20 and s[19:20] == b"Z":
        return civil(s[:19])
    if len(s) == 25 and s[19:20] in (b"+", b"-") and s[22:23] == b":":
        if not (all_digits(s[20:22]) and all_digits(s[23:25])):
            return None
        oh, om = int(s[20:22]), int(s[23:25])
        if oh > 23 or om > 59:
            return None
        local = civil(s[:19])
        if local is None:
            return None
        sign = 1 if s[19:20] == b"+" else -1
        return local - sign * (oh * 60 + om) * 60_000
    return None


def main():
    data, out = sys.argv[1], sys.argv[2]
    m = json.load(open(os.path.join(data, "manifest.json")))
    ws = parse_ts(m["window_start"].encode())
    we = parse_ts(m["window_end"].encode())

    products = {}
    for r in rows_file(os.path.join(data, "products.csv"), 2):
        products[r[0]] = r[1]
    campaigns = {}
    for r in rows_file(os.path.join(data, "campaigns.csv"), 2):
        campaigns.setdefault(r[0], r[1])

    # Links: union-find on exact byte strings.
    parent = {}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for a, b in rows(os.path.join(data, "links"), 2):
        if a == b"" or b == b"":
            continue
        for x in (a, b):
            parent.setdefault(x, x)
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb
    smallest = {}
    for x in parent:
        r = find(x)
        if r not in smallest or x < smallest[r]:
            smallest[r] = x

    def person(uid):
        if uid in parent:
            return smallest[find(uid)]
        return uid

    # Events: parse, dedup (smallest ingest_ts, before validation), validate, late.
    kept = {}
    for r in rows(os.path.join(data, "events"), 7):
        eid, uid, ts, ingest, etype, pid, price = r
        if eid == b"":
            continue
        ing = parse_ts(ingest)
        ing_key = ing if ing is not None else float("inf")
        if eid not in kept or ing_key < kept[eid][0]:
            kept[eid] = (ing_key, r)
    events = defaultdict(list)  # person -> [(ts, type, price, category)]
    for ing, r in kept.values():
        eid, uid, ts, ingest, etype, pid, price = r
        if uid == b"" or pid == b"" or etype not in (b"view", b"cart", b"purchase"):
            continue
        t = parse_ts(ts)
        if t is None or not (ws <= t < we):
            continue
        p = 0
        if etype == b"purchase":
            if not all_digits(price):
                continue
            p = int(price)
            if not (1 <= p <= 10_000_000):
                continue
        if ing == float("inf") or ing - t > DAY:
            continue
        events[person(uid)].append((t, etype, p, products.get(pid, b"unknown")))

    clicks = defaultdict(list)  # person -> [(ts, click_id, campaign)]
    seen = set()
    for cid, uid, camp, ts in rows(os.path.join(data, "clicks"), 4):
        if cid == b"" or cid in seen:
            continue
        seen.add(cid)
        if uid == b"" or camp not in campaigns:
            continue
        t = parse_ts(ts)
        if t is None or not (ws - WEEK <= t < we):
            continue
        clicks[person(uid)].append((t, cid, camp))

    user_rows = {}
    camp_rows = defaultdict(lambda: [0, 0, 0, 0])
    for who, evs in events.items():
        evs.sort(key=lambda e: e[0])
        sessions = []
        for e in evs:
            if sessions and e[0] - sessions[-1][-1][0] <= GAP:
                sessions[-1].append(e)
            else:
                sessions.append([e])
        cks = clicks.get(who, [])
        for s in sessions:
            start = s[0][0]
            day = start // DAY
            purchases = [e for e in s if e[1] == b"purchase"]
            rev = sum(e[2] for e in purchases)
            u = user_rows.setdefault((who, day), [0, 0, 0, defaultdict(int)])
            u[0] += 1
            u[1] += len(s)
            u[2] += rev
            for e in purchases:
                u[3][e[3]] += e[2]
            window = [c for c in cks if start - WEEK <= c[0] <= start]
            if window:
                best = max(window, key=lambda c: (c[0], [-b for b in c[1]], -len(c[1])))
                # explicit tie-break: latest ts, then byte-wise smallest click_id
                top_ts = max(c[0] for c in window)
                best = min((c for c in window if c[0] == top_ts), key=lambda c: c[1])
                credited = best[2]
            else:
                credited = b"organic"
            c = camp_rows[(credited, day)]
            c[0] += 1
            c[1] += 1 if purchases else 0
            c[2] += rev
            for camp in {c[2] for c in window}:
                camp_rows[(camp, day)][3] += 1

    os.makedirs(out, exist_ok=True)
    import datetime

    def iso(day):
        return (datetime.date(1970, 1, 1) + datetime.timedelta(days=day)).isoformat()

    with open(os.path.join(out, "user_daily.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["user_id", "day", "sessions", "events", "revenue_cents", "top_category"])
        for (who, day), (n, ne, rev, cats) in sorted(user_rows.items()):
            top = ""
            if cats:
                top = min(cats.items(), key=lambda kv: (-kv[1], kv[0]))[0].decode()
            w.writerow([who.decode(), iso(day), n, ne, rev, top])
    with open(os.path.join(out, "campaign_daily.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["campaign_id", "channel", "day", "sessions", "purchasing_sessions", "revenue_cents", "assisted_sessions"])
        for (camp, day), (n, pn, rev, asst) in sorted(camp_rows.items()):
            ch = b"organic" if camp == b"organic" else campaigns[camp]
            w.writerow([camp.decode(), ch.decode(), iso(day), n, pn, rev, asst])


def rows_file(path, n):
    if not os.path.isfile(path):
        return
    for line in read_lines(path):
        r = fields(line, n)
        if r is not None:
            yield r


if __name__ == "__main__":
    main()
