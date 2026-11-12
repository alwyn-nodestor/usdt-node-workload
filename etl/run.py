"""The ETL (PLAN.md 2.6): solidified blocks from the local node, in block order, into Tier 2 parquet.

  bin/blockline-usdt etl live                 follow the solidified head (Phase 1). First start: at the current
                                              solidified block, unless --start is given. Resumes from its cursor.
  bin/blockline-usdt etl range FROM TO        one history worker for blocks FROM..TO (Phase 2), resumable
  bin/blockline-usdt etl block N              extract one block and print the rows (no writes)

Cursors: state/etl-live.json, state/etl-range-FROM-TO.json. The live run also writes state/etl.json for the status
page (docs/DASHBOARD.md), folding in every range cursor it finds.
"""
import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from etl.extract import BlockError, extract  # noqa: E402
from etl.store import EPOCH, covered, epoch_of, read_json, remove_from, write_json, write_parts  # noqa: E402

ROOT = Path(os.environ.get("BLOCKLINE_USDT_ROOT", "/srv/blockline-usdt"))
STATE = ROOT / "state"
TIER2 = ROOT / "tier2"
SOLID = "http://127.0.0.1:18091/walletsolidity"
SEAL_MARGIN = 1000
FETCHERS = 6


def log(msg):
    print(time.strftime("%F %T"), msg, flush=True)


def post(path, body, tries=8):
    delay = 2
    for attempt in range(tries):
        try:
            req = urllib.request.Request(f"{SOLID}/{path}", data=json.dumps(body).encode(),
                                         headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.load(r)
        except (urllib.error.URLError, TimeoutError, ConnectionError, json.JSONDecodeError) as err:
            if attempt == tries - 1:
                raise
            log(f"node call {path} {body} failed ({err}); retry in {delay}s")
            time.sleep(delay)
            delay = min(delay * 2, 120)


def solid_head():
    return post("getnowblock", {})["block_header"]["raw_data"]["number"]


def fetch(num):
    block = post("getblockbynum", {"num": num})
    if block.get("block_header", {}).get("raw_data", {}).get("number", 0) != num:
        raise BlockError(f"block {num}: the node returned another block (or none): {str(block)[:200]}")
    infos = post("gettransactioninfobyblocknum", {"num": num})
    return num, block, infos


def now_utc():
    return time.strftime("%F %T", time.gmtime())


class Run:
    """One cursor-driven run over [start, stop] (stop None = follow the solidified head forever)."""

    def __init__(self, cursor_path, start, stop, flush_blocks, flush_seconds, on_flush=None):
        self.cursor_path, self.stop = cursor_path, stop
        self.flush_blocks, self.flush_seconds = flush_blocks, flush_seconds
        self.on_flush = on_flush or (lambda run: None)
        cur = read_json(cursor_path)
        if cur is None:
            prev_id = post("getblockbynum", {"num": start - 1})["blockID"] if start > 0 else None
            cur = {"start_block": start, "stop_block": stop, "next_block": start, "prev_id": prev_id,
                   "rows": {}, "blocks_done": 0, "started": now_utc(), "error": None}
            write_json(cursor_path, cur)
        cur["error"] = None
        self.cur = cur
        removed = remove_from(TIER2, cur["next_block"])
        if removed:
            log(f"removed {removed} part files at or past the cursor (left by an interrupted flush)")
        self.buf, self.buf_first, self.buf_since = None, None, time.time()
        self.rate_mark = (time.time(), cur["next_block"])
        self.rate = None
        self.solid = None

    def _reset_buf(self, first):
        self.buf, self.buf_first, self.buf_since = {"block_headers": []}, first, time.time()

    def flush(self):
        if not self.buf or not self.buf["block_headers"]:
            return
        first, last = self.buf_first, self.buf["block_headers"][-1]["block"]
        counts = write_parts(TIER2, first, last, self.buf)
        for t, n in counts.items():
            self.cur["rows"][t] = self.cur["rows"].get(t, 0) + n
        self.cur["next_block"] = last + 1
        self.cur["prev_id"] = self.buf["block_headers"][-1]["block_id"]
        self.cur["blocks_done"] += last - first + 1
        self.cur["updated"] = now_utc()
        t0, b0 = self.rate_mark
        if time.time() - t0 > 5:
            self.rate = round((last + 1 - b0) / (time.time() - t0), 2)
            self.rate_mark = (time.time(), last + 1)
        self.cur["blocks_per_s"] = self.rate
        write_json(self.cursor_path, self.cur)
        self.buf = None
        self.on_flush(self)

    def add(self, header, rows):
        n = header["block"]
        if self.buf is None:
            self._reset_buf(n)
        self.buf["block_headers"].append(header)
        for t, rs in rows.items():
            if rs:
                self.buf.setdefault(t, []).extend(rs)

    def due(self, next_num):
        if self.buf is None:
            return False
        nblocks = len(self.buf["block_headers"])
        return (nblocks >= self.flush_blocks or epoch_of(next_num) != epoch_of(self.buf_first)
                or time.time() - self.buf_since >= self.flush_seconds)

    def go(self):
        nxt = self.cur["next_block"]
        prev_id = self.cur["prev_id"]
        with ThreadPoolExecutor(FETCHERS) as pool:
            while True:
                if self.stop is not None and nxt > self.stop:
                    self.flush()
                    log(f"range done at block {nxt - 1}")
                    return
                self.solid = solid_head()
                top = self.solid if self.stop is None else min(self.solid, self.stop)
                if nxt > top:
                    if self.due(nxt):
                        self.flush()
                    self.on_flush(self)
                    time.sleep(3)
                    continue
                window = list(range(nxt, min(top, nxt + 4 * FETCHERS - 1) + 1))
                for num, block, infos in pool.map(fetch, window):
                    if self.due(num):
                        self.flush()
                    header, rows = extract(block, infos, prev_id)
                    prev_id = header["block_id"]
                    self.add(header, rows)
                    nxt = num + 1


# ---- status for the page ---------------------------------------------------------------------------------------

def seal(solid):
    """Mark epochs that are fully present in Tier 2 and solidified past the margin. Returns (sealed, total)."""
    sealed_dir = TIER2 / "_sealed"
    sealed_dir.mkdir(parents=True, exist_ok=True)
    have = covered(TIER2)
    total = epoch_of(solid) + 1 if solid is not None else 0
    for k in range(total):
        lo, hi = k * EPOCH, (k + 1) * EPOCH - 1
        marker = sealed_dir / f"epoch={k:04d}.json"
        if marker.exists() or hi > solid - SEAL_MARGIN:
            continue
        if any(a <= lo and hi <= b for a, b in have):
            write_json(marker, {"epoch": k, "first": lo, "last": hi, "sealed": now_utc()})
    return len(list(sealed_dir.glob("epoch=*.json"))), total


def status(run):
    last_seal = getattr(status, "_seal", 0)
    if time.time() - last_seal > 600:
        status._seal_result = seal(run.solid)
        status._seal = time.time()
    sealed, total = getattr(status, "_seal_result", (0, 0))
    cur = run.cur
    workers = []
    for p in sorted(STATE.glob("etl-range-*.json")):
        c = read_json(p) or {}
        workers.append(c)
    hist = None
    if workers:
        done = sum(c.get("next_block", 0) - c.get("start_block", 0) for c in workers)
        size = sum(c.get("stop_block", 0) - c.get("start_block", 0) + 1 for c in workers)
        hist = {"next_block": done, "target_block": size, "workers": len(workers),
                "blocks_per_s": round(sum(c.get("blocks_per_s") or 0 for c in workers if not c.get("finished")), 2),
                "note": "blocks done of blocks assigned, over all range workers"}
    rows = dict(cur["rows"])
    for c in workers:
        for t, n in (c.get("rows") or {}).items():
            rows[t] = rows.get(t, 0) + n
    lag = None if run.solid is None else max(0, run.solid - (cur["next_block"] - 1))
    write_json(STATE / "etl.json", {
        "updated": now_utc(), "phase": "1", "phases": {"0": "run"},
        "state": "following" if lag is not None and lag < 200 else "catching up",
        "live": {"start_block": cur["start_block"], "last_block": cur["next_block"] - 1, "lag_blocks": lag,
                 "solid_head": run.solid, "blocks_per_s": cur.get("blocks_per_s"), "blocks_done": cur["blocks_done"]},
        "history": hist,
        "epochs": {"sealed": sealed, "total": total},
        "rows": rows,
        "checks": {"block ID, parent hash, tx hashes (every block)": "pass" if not cur.get("error") else "fail"},
        "error": cur.get("error"),
    })


def main():
    p = argparse.ArgumentParser(prog="blockline-usdt etl", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    lv = sub.add_parser("live")
    lv.add_argument("--start", type=int, help="first block on the very first start (default: the solidified head)")
    rg = sub.add_parser("range")
    rg.add_argument("first", type=int)
    rg.add_argument("last", type=int)
    one = sub.add_parser("block")
    one.add_argument("num", type=int)
    args = p.parse_args()

    if args.cmd == "block":
        _, block, infos = fetch(args.num)
        prev = post("getblockbynum", {"num": args.num - 1})["blockID"] if args.num > 0 else None
        header, rows = extract(block, infos, prev)
        print(json.dumps({"header": header, **{t: r for t, r in rows.items() if r}}, indent=1, default=lambda b: b.hex()))
        return

    STATE.mkdir(parents=True, exist_ok=True)
    TIER2.mkdir(parents=True, exist_ok=True)
    if args.cmd == "live":
        cursor = STATE / "etl-live.json"
        start = args.start if args.start is not None else (read_json(cursor) or {}).get("start_block") or solid_head()
        run = Run(cursor, start, None, flush_blocks=200, flush_seconds=600, on_flush=status)
        log(f"live: from block {run.cur['next_block']} (started at {run.cur['start_block']})")
    else:
        cursor = STATE / f"etl-range-{args.first}-{args.last}.json"
        run = Run(cursor, args.first, args.last, flush_blocks=20_000, flush_seconds=1800)
        log(f"range {args.first}..{args.last}: from block {run.cur['next_block']}")
    try:
        run.go()
        if args.cmd == "range":
            run.cur["finished"] = now_utc()
            write_json(cursor, run.cur)
    except BlockError as err:
        run.cur["error"] = str(err)
        write_json(cursor, run.cur)
        if args.cmd == "live":
            status(run)
        log(f"STOPPED: {err}")
        sys.exit(2)


if __name__ == "__main__":
    main()
