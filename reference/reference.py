#!/usr/bin/env python3
"""
Shopping Sessions: a plain-Python reference implementation of rules.md.

Slow on purpose: every rule is written out step by step, so the fast Rust
pipeline can be cross-checked against something easy to read.

Usage: reference.py DATA_DIR OUT_DIR
Writes OUT_DIR/user_daily.csv and OUT_DIR/campaign_daily.csv in the fixtures'
expected-CSV format (day as YYYY-MM-DD, empty top_category for null).
"""
import glob
import json
import os
import sys
from collections import defaultdict

SECOND = 1000
MINUTE = 60 * SECOND
HOUR = 60 * MINUTE
DAY = 24 * HOUR
SESSION_GAP = 30 * MINUTE
CLICK_REACH = 7 * DAY
LATE = 24 * HOUR
DIGITS = b"0123456789"


# ---------------------------------------------------------------- CSV dialect
def read_lines(path):
    """Data lines of one CSV file (header dropped). Only \\n ends a line."""
    with open(path, "rb") as f:
        data = f.read()
    lines = data.split(b"\n")
    if lines and lines[-1] == b"":
        lines.pop()  # the text after the final \n is not a line
    return lines[1:]  # every file has a header row


def parse_line(line, nfields):
    """Fields of a well-formed line (None for an empty field), or None if malformed."""
    if line == b"":
        return None
    fields = line.split(b",")
    if len(fields) != nfields:
        return None
    out = []
    for f in fields:
        if b'"' in f:
            if len(f) >= 2 and f[0] == 0x22 and f[-1] == 0x22 and b'"' not in f[1:-1]:
                f = f[1:-1]
            else:
                return None
        out.append(f if f != b"" else None)
    return out


def all_digits(s):
    return len(s) > 0 and all(c in DIGITS for c in s)


# ---------------------------------------------------------------- timestamps
def is_leap(y):
    return y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)


def days_in_month(y, m):
    return [31, 29 if is_leap(y) else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][m - 1]


def days_from_civil(y, m, d):
    y = y - 1 if m <= 2 else y
    era = y // 400
    yoe = y - era * 400
    doy = (153 * ((m + 9) % 12) + 2) // 5 + d - 1
    doe = yoe * 365 + yoe // 4 - yoe // 100 + doy
    return era * 146097 + doe - 719468


def parse_ts(s):
    """Milliseconds since the epoch, or None for a bad timestamp."""
    if s is None:
        return None
    n = len(s)
    if all_digits(s):  # F3
        return int(s)
    if n not in (20, 25):
        return None

    def num(a, b):
        part = s[a:b]
        return int(part) if all_digits(part) else None

    if s[4:5] != b"-" or s[7:8] != b"-" or s[10:11] != b"T" or s[13:14] != b":" or s[16:17] != b":":
        return None
    y, mo, d, h, mi, se = num(0, 4), num(5, 7), num(8, 10), num(11, 13), num(14, 16), num(17, 19)
    if None in (y, mo, d, h, mi, se):
        return None
    if not (1 <= mo <= 12) or not (1 <= d <= days_in_month(y, mo)):
        return None
    if h > 23 or mi > 59 or se > 59:
        return None
    t = ((days_from_civil(y, mo, d) * 24 + h) * 60 + mi) * 60 + se
    t *= SECOND
    if n == 20:  # F1
        if s[19:20] != b"Z":
            return None
        return t
    # F2
    sign = s[19:20]
    if sign not in (b"+", b"-") or s[22:23] != b":":
        return None
    oh, om = num(20, 22), num(23, 25)
    if oh is None or om is None or oh > 23 or om > 59:
        return None
    off = (oh * 60 + om) * MINUTE
    return t - off if sign == b"+" else t + off


def utc_date(ms):
    days = ms // DAY
    z = days + 719468
    era = z // 146097
    doe = z - era * 146097
    yoe = (doe - doe // 1460 + doe // 36524 - doe // 146096) // 365
    y = yoe + era * 400
    doy = doe - (365 * yoe + yoe // 4 - yoe // 100)
    mp = (5 * doy + 2) // 153
    d = doy - (153 * mp + 2) // 5 + 1
    m = mp + 3 if mp < 10 else mp - 9
    if m <= 2:
        y += 1
    return f"{y:04d}-{m:02d}-{d:02d}"


# ---------------------------------------------------------------- pipeline
def csv_files(data, folder):
    return sorted(glob.glob(os.path.join(data, folder, "*.csv")))


def main():
    data, out = sys.argv[1], sys.argv[2]
    with open(os.path.join(data, "manifest.json")) as f:
        manifest = json.load(f)
    ws = parse_ts(manifest["window_start"].encode())
    we = parse_ts(manifest["window_end"].encode())
    assert ws is not None and we is not None

    products = {}
    for line in read_lines(os.path.join(data, "products.csv")):
        f = parse_line(line, 2)
        if f is not None and f[0] is not None:
            products[f[0]] = f[1]
    campaigns = {}
    for line in read_lines(os.path.join(data, "campaigns.csv")):
        f = parse_line(line, 2)
        if f is not None and f[0] is not None:
            campaigns[f[0]] = f[1]

    # Linked IDs: union-find, each person named by the byte-wise smallest ID.
    parent = {}

    def find(x):
        root = x
        while parent.get(root, root) != root:
            root = parent[root]
        while parent.get(x, x) != root:
            parent[x], x = root, parent[x]
        return root

    for path in csv_files(data, "links"):
        for line in read_lines(path):
            f = parse_line(line, 2)
            if f is None or f[0] is None or f[1] is None:
                continue
            a, b = find(f[0]), find(f[1])
            if a != b:
                # keep the smaller ID as the root, so the root is the name
                if b < a:
                    a, b = b, a
                parent[b] = a
            parent.setdefault(a, a)

    def person(uid):
        return find(uid)

    # Events: parse, dedup (smallest ingest_ts, before validation).
    kept = {}
    for path in csv_files(data, "events"):
        for line in read_lines(path):
            f = parse_line(line, 7)
            if f is None or f[0] is None:
                continue
            ingest = parse_ts(f[3])
            if ingest is None:
                raise SystemExit(f"unexpected ingest_ts {f[3]!r}")
            prev = kept.get(f[0])
            if prev is None or ingest < prev[0]:
                kept[f[0]] = (ingest, f)

    # Validate, drop late, group by person.
    by_person = defaultdict(list)
    for ingest, f in kept.values():
        _, uid, ts_s, _, etype, pid, price_s = f
        if uid is None or pid is None:
            continue
        ts = parse_ts(ts_s)
        if ts is None or not (ws <= ts < we):
            continue
        if etype not in (b"view", b"cart", b"purchase"):
            continue
        price = 0
        if etype == b"purchase":
            if price_s is None or not all_digits(price_s):
                continue
            price = int(price_s)
            if not (1 <= price <= 10_000_000):
                continue
        if ingest - ts > LATE:
            continue
        cat = products.get(pid, b"unknown")
        by_person[person(uid)].append((ts, etype == b"purchase", price, cat))

    # Clicks: parse, dedup, validate, group by person.
    clicks_seen = {}
    for path in csv_files(data, "clicks"):
        for line in read_lines(path):
            f = parse_line(line, 4)
            if f is None or f[0] is None:
                continue
            clicks_seen.setdefault(f[0], f)
    clicks_by_person = defaultdict(list)
    for cid, f in clicks_seen.items():
        _, uid, camp, ts_s = f
        if uid is None or camp is None or camp not in campaigns:
            continue
        ts = parse_ts(ts_s)
        if ts is None or not (ws - CLICK_REACH <= ts < we):
            continue
        clicks_by_person[person(uid)].append((ts, cid, camp))

    user_rows = {}
    camp_rows = defaultdict(lambda: [0, 0, 0, 0])  # sessions, purchasing, revenue, assisted
    for p, evs in by_person.items():
        evs.sort(key=lambda e: e[0])
        sessions = []
        for e in evs:
            if sessions and e[0] - sessions[-1][-1][0] <= SESSION_GAP:
                sessions[-1].append(e)
            else:
                sessions.append([e])
        clicks = clicks_by_person.get(p, [])
        for s in sessions:
            start = s[0][0]
            day = utc_date(start)
            revenue = sum(e[2] for e in s if e[1])
            purchased = any(e[1] for e in s)
            row = user_rows.setdefault((p, day), [0, 0, 0, defaultdict(int)])
            row[0] += 1
            row[1] += len(s)
            row[2] += revenue
            for e in s:
                if e[1]:
                    row[3][e[3]] += e[2]
            best = None
            assists = set()
            for ts, cid, camp in clicks:
                if ts <= start and start - ts <= CLICK_REACH:
                    assists.add(camp)
                    if best is None or ts > best[0] or (ts == best[0] and cid < best[1]):
                        best = (ts, cid, camp)
            credited = best[2] if best is not None else b"organic"
            c = camp_rows[(credited, day)]
            c[0] += 1
            c[1] += 1 if purchased else 0
            c[2] += revenue
            for camp in assists:
                camp_rows[(camp, day)][3] += 1

    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "user_daily.csv"), "w", encoding="utf-8", newline="") as f:
        f.write("user_id,day,sessions,events,revenue_cents,top_category\n")
        for (p, day), (n, ev, rev, cats) in sorted(user_rows.items()):
            top = ""
            if cats:
                top = min(cats.items(), key=lambda kv: (-kv[1], kv[0]))[0].decode()
            f.write(f"{csv_field(p)},{day},{n},{ev},{rev},{csv_field_s(top)}\n")
    with open(os.path.join(out, "campaign_daily.csv"), "w", encoding="utf-8", newline="") as f:
        f.write("campaign_id,channel,day,sessions,purchasing_sessions,revenue_cents,assisted_sessions\n")
        for (camp, day), (n, purch, rev, assisted) in sorted(camp_rows.items()):
            channel = b"organic" if camp == b"organic" else campaigns[camp]
            f.write(f"{csv_field(camp)},{csv_field(channel)},{day},{n},{purch},{rev},{assisted}\n")


def csv_field_s(s):
    if s == "":
        return ""
    if any(c in s for c in ',"\n\r') or s != s.strip():
        return '"' + s.replace('"', '""') + '"'
    return s


def csv_field(b):
    return csv_field_s(b.decode("utf-8"))


if __name__ == "__main__":
    main()
