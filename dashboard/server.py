"""blockline-usdt status page: the node, the snapshot stream and the ETL. Read-only.

  python3 dashboard/server.py [--port 8288] [--bind ADDR ...]   (the blockline-usdt-dashboard unit runs this)
  --bind is repeatable and defaults to 127.0.0.1; each address listed is also accepted as a Host name.
  ssh -L 8288:127.0.0.1:8288 debian@<server>     then open http://localhost:8288 on your own machine

Sources, all local; nothing here writes or calls out:
  - systemd: the blockline-usdt units (state, memory, restarts)
  - /srv/blockline-usdt/state/snapshot.json, written by `blockline-usdt snapshot`
  - the node's HTTP APIs on 127.0.0.1:18090 (head) and 18091 (solidified)
  - /srv/blockline-usdt/state/etl.json, written by the ETL once it exists (contract in docs/DASHBOARD.md)
  - /srv/blockline-shared/bandwidth.json, the host traffic ledger
  - journalctl, when the service may read the journal
Stdlib only, so it runs on the system python3 with no virtualenv.
"""
import argparse
import collections
import json
import os
import shutil
import subprocess
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

ROOT = Path(os.environ.get("BLOCKLINE_USDT_ROOT", "/srv/blockline-usdt"))
STATE = ROOT / "state"
NODE = ROOT / "node"
LEDGER = Path("/srv/blockline-shared/bandwidth.json")
PAGE = Path(__file__).with_name("index.html")
HTTP_PORT, SOLIDITY_PORT = 18090, 18091
UNITS = ["blockline-usdt-snapshot", "blockline-usdt-node", "blockline-usdt-clickhouse", "blockline-usdt-etl",
         "blockline-usdt-history",
         "blockline-usdt-dashboard"]
LOGS = {"snapshot": "blockline-usdt-snapshot", "node": "blockline-usdt-node", "etl": "blockline-usdt-etl"}
LOCAL_HOSTS = frozenset({"127.0.0.1", "localhost", "::1", "[::1]", ""})
SAMPLE_EVERY, SAMPLES = 60, 24 * 60  # one point a minute, a day of history (in memory; restarts start empty)


def host_of(header):
    header = (header or "").strip().lower()
    if header.startswith("["):
        return header.split("]", 1)[0] + "]"
    return header.rsplit(":", 1)[0] if header.count(":") == 1 else header


def read_json(path):
    try:
        return json.loads(Path(path).read_text())
    except FileNotFoundError:
        return None
    except (OSError, ValueError) as err:
        return {"error": f"unreadable: {err}"}


def api(path, port):
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=3) as r:
            return json.load(r)
    except Exception as err:  # noqa: BLE001 - the page must always render
        return {"error": str(err)}


def unit(name):
    props = "LoadState,ActiveState,SubState,ActiveEnterTimestamp,MemoryCurrent,NRestarts,Result,IPIngressBytes,IPEgressBytes"
    try:
        out = subprocess.run(["systemctl", "show", f"{name}.service", "-p", props],
                             capture_output=True, text=True, timeout=5).stdout
    except (OSError, subprocess.TimeoutExpired) as err:
        return {"error": str(err)}
    info = dict(line.split("=", 1) for line in out.splitlines() if "=" in line)
    for key in ("MemoryCurrent", "NRestarts", "IPIngressBytes", "IPEgressBytes"):
        info[key] = int(info[key]) if info.get(key, "").isdigit() else None
    return info


def journal(name, lines=60):
    try:
        out = subprocess.run(["journalctl", "-u", f"{name}.service", "-n", str(lines), "--no-pager", "-o", "short-iso", "-q"],
                             capture_output=True, text=True, timeout=5)
    except (OSError, subprocess.TimeoutExpired) as err:
        return [f"journalctl failed: {err}"]
    text = out.stdout.strip()
    logfile = ROOT / "state" / "logs" / f"{name.removeprefix('blockline-usdt-')}.log"
    if not text and logfile.exists():   # a process started by hand (nohup) logs to a file instead of the journal
        with open(logfile, "rb") as f:
            f.seek(max(0, f.seek(0, 2) - 20000))
            return f.read().decode("utf-8", "replace").splitlines()[-lines:]
    if not text:
        return ["(no entries, or this service may not read the journal: add it to the systemd-journal group)"]
    return text.splitlines()


def block_summary(block):
    if not block or "error" in block:
        return {"error": (block or {}).get("error", "no answer")}
    raw = block.get("block_header", {}).get("raw_data", {})
    ts = raw.get("timestamp")
    return {"number": raw.get("number"), "timestamp": ts,
            "behind_s": None if ts is None else round(time.time() - ts / 1000, 1),
            "txs": len(block.get("transactions", []))}


def node_view():
    head = block_summary(api("/wallet/getnowblock", HTTP_PORT))
    if "error" in head:
        return {"up": False, "error": head["error"]}
    solid = block_summary(api("/walletsolidity/getnowblock", SOLIDITY_PORT))
    info = api("/wallet/getnodeinfo", HTTP_PORT)
    cfg = info.get("configNodeInfo", {}) if isinstance(info, dict) else {}
    machine = info.get("machineInfo", {}) if isinstance(info, dict) else {}
    # the network head is not known without asking outside; estimate it from the clock (one block every 3 s)
    expected = None if head.get("timestamp") is None else head["number"] + max(0, int(head["behind_s"] // 3))
    return {"up": True, "head": head, "solid": solid, "expected_head": expected,
            "peers": info.get("currentConnectCount"), "active_peers": info.get("activeConnectCount"),
            "version": cfg.get("codeVersion"), "p2p_version": cfg.get("p2pVersion"),
            "jvm_used": machine.get("javaHeapUsed") if isinstance(machine, dict) else None,
            "jvm_max": machine.get("jvmTotalMemory") if isinstance(machine, dict) else None}


class Sampler:
    """Keeps a day of one-a-minute points (snapshot bytes, node head, ETL block) for the rate charts."""

    def __init__(self):
        self.points = collections.deque(maxlen=SAMPLES)
        self.lock = threading.Lock()

    def take(self, snapshot, node, etl):
        point = {"t": int(time.time()),
                 "snap": (snapshot or {}).get("bytes"),
                 "head": node.get("head", {}).get("number") if node.get("up") else None,
                 "etl": ((etl or {}).get("live") or {}).get("last_block")}
        with self.lock:
            self.points.append(point)

    def series(self):
        with self.lock:
            return list(self.points)

    def run(self, collector):
        while True:
            try:
                view = collector.collect(fresh=True)
                self.take(view["snapshot"], view["node"], view["etl"])
            except Exception:  # noqa: BLE001 - never let the sampler die
                pass
            time.sleep(SAMPLE_EVERY)


class Collector:
    def __init__(self):
        self.cache, self.cached_at = None, 0.0
        self.lock = threading.Lock()
        self.sampler = Sampler()

    def collect(self, fresh=False):
        with self.lock:
            if not fresh and self.cache and time.time() - self.cached_at < 3:
                return self.cache
        usage = shutil.disk_usage(ROOT)
        mem = {}
        try:
            for line in Path("/proc/meminfo").read_text().splitlines():
                key, val = line.split(":", 1)
                mem[key] = int(val.split()[0]) * 1024
        except OSError:
            pass
        view = {
            "now": int(time.time()),
            "units": {name: unit(name) for name in UNITS},
            "snapshot": read_json(STATE / "snapshot.json"),
            "snapshot_verified": (NODE / ".snapshot-verified").exists(),
            "node": node_view(),
            "etl": read_json(STATE / "etl.json"),
            "disk": {"total": usage.total, "used": usage.used, "free": usage.free},
            "memory": {"total": mem.get("MemTotal"), "available": mem.get("MemAvailable")},
            "load": os.getloadavg(),
            "traffic": read_json(LEDGER),
        }
        with self.lock:
            self.cache, self.cached_at = view, time.time()
        return view


def handler(collector, allowed_hosts):
    class Handler(BaseHTTPRequestHandler):
        def send(self, code, body, kind):
            data = body if isinstance(body, bytes) else body.encode()
            self.send_response(code)
            self.send_header("Content-Type", kind)
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            # only the addresses it listens on (and loopback names); other Host names would be DNS rebinding
            if host_of(self.headers.get("Host")) not in allowed_hosts:
                return self.send(421, "this page answers only on the addresses it listens on", "text/plain")
            url = urlparse(self.path)
            if url.path == "/":
                self.send(200, PAGE.read_bytes(), "text/html; charset=utf-8")
            elif url.path == "/api/status":
                view = dict(collector.collect())
                view["series"] = collector.sampler.series()
                self.send(200, json.dumps(view), "application/json")
            elif url.path == "/api/log":
                name = LOGS.get(parse_qs(url.query).get("unit", [""])[0])
                if not name:
                    return self.send(400, "unknown unit", "text/plain")
                self.send(200, json.dumps(journal(name)), "application/json")
            else:
                self.send(404, "not found", "text/plain")

        def log_message(self, *args):  # quiet: the page polls every few seconds
            pass
    return Handler


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--port", type=int, default=8288)
    parser.add_argument("--bind", action="append", metavar="ADDR",
                        help="address to listen on; repeat for several (default 127.0.0.1)")
    args = parser.parse_args()
    binds = list(dict.fromkeys(args.bind or ["127.0.0.1"]))
    allowed = LOCAL_HOSTS | {b.lower() for b in binds} | {f"[{b.lower()}]" for b in binds if ":" in b}
    collector = Collector()
    threading.Thread(target=collector.sampler.run, args=(collector,), daemon=True).start()
    servers = [ThreadingHTTPServer((b, args.port), handler(collector, allowed)) for b in binds]
    for b in binds:
        print(f"blockline-usdt status on http://{f'[{b}]' if ':' in b else b}:{args.port}", flush=True)
    for s in servers[1:]:
        threading.Thread(target=s.serve_forever, daemon=True).start()
    servers[0].serve_forever()


if __name__ == "__main__":
    main()
