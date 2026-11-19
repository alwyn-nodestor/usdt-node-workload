"""blockline-usdt node tooling: install the pinned runtime, render the node config, stream the snapshot, report status.

Runs as the unprivileged `usdt` user, standard library only. Every download is pinned by sha256 (and the java-tron jars
also by TRON's release GPG key when a keyserver is reachable). Nothing here needs root; the systemd units come from
bootstrap/usdt-bootstrap.sh. No host firewall: the AceCloud security group allows inbound only SSH and TCP 8333, so the
node's APIs and inbound peer port 18888 are unreachable from outside (PLAN.md 2.4).

  blockline-usdt install     JDK 8, FullNode.jar, Toolkit.jar, node/config.conf
  blockline-usdt snapshot    stream the FullNode snapshot (with internal transactions) into the node directory and
                             verify its md5; resumes a broken connection at the byte it stopped (HTTP Range)
  blockline-usdt status      snapshot progress, node head/solidified block, peers, disk
"""
import argparse
import collections
import hashlib
import http.client
import itertools
import json
import os
import shutil
import subprocess
import sys
import tarfile
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

ROOT = Path(os.environ.get("BLOCKLINE_USDT_ROOT", "/srv/blockline-usdt"))
NODE = ROOT / "node"
STATE = ROOT / "state"
UA = {"User-Agent": "blockline-usdt/1"}

# --- Pins (change together, then re-run `install`) ------------------------------------------------------------------
JAVA_TRON_TAG = "GreatVoyage-v4.8.2.4"
JARS = {  # GitHub release asset digests, 2026-10-22
    "FullNode.jar": "709423e8d14ccba391daeef7aa78bb5f41a1b69559cc867346beacbd6aae39c9",
    "Toolkit.jar": "d1da524e974a44e44a99c014fdd44bdc06d0b2c8f00794a6886e62ab15d39971",
}
TRON_GPG_FINGERPRINT = "1254F859D2B1BD9F66E7107DF859BCB44A28290B"  # build@tron.network, from the java-tron README
CONFIG_URL = f"https://raw.githubusercontent.com/tronprotocol/java-tron/{JAVA_TRON_TAG}/framework/src/main/resources/config.conf"
CONFIG_SHA256 = "fb51f5ee508cefb4e0d4b0c1868029e385a997ee9b7468875c9d781d77b0ea2a"
JDK = {  # Eclipse Temurin 8 (java-tron on x86_64 requires JDK 8)
    "release": "jdk8u512-b09",
    "url": "https://github.com/adoptium/temurin8-binaries/releases/download/jdk8u512-b09/OpenJDK8U-jdk_x64_linux_hotspot_8u512b09.tar.gz",
    "sha256": "fb3bfa869012419b562bb4235d0d83ec4f8c1dda62ae432ba20c6c3a7e43f7e4",
}
SNAPSHOT = {  # official TRON mirror; FullNode snapshot WITH internal transactions
    # BLOCKLINE_USDT_SNAPSHOT_BASE exists for testing the stream against a small local archive only.
    "base": os.environ.get("BLOCKLINE_USDT_SNAPSHOT_BASE", "http://35.247.128.170/backup20261106"),
    "file": "FullNode_output-directory.tgz",
}

# Ports (PLAN.md 2.4): 8090 on this host is blockline-btc's hot service, so HTTP 18090 / solidity 18091; gRPC 505x1.
# java-tron binds them to every interface; the AceCloud security group has no inbound rule for any of them.
HTTP_PORT, SOLIDITY_PORT, RPC_PORT, RPC_SOLIDITY_PORT = 18090, 18091, 50551, 50561

# Edits to the official config: (exact text, replacement). Each must match exactly once, so an upstream change to the
# file fails loudly here instead of silently leaving a default in place.
CONFIG_EDITS = [
    # few peers: java-tron has no blocks-only mode, and peers are most of a node's traffic
    ("  maxConnections = 30\n  minConnections = 8\n", "  maxConnections = 10\n  minConnections = 5\n"),
    ("    fullNodePort = 8090\n", f"    fullNodePort = {HTTP_PORT}\n"),
    ("    solidityPort = 8091\n    PBFTEnable = true\n    PBFTPort = 8092\n",
     f"    solidityPort = {SOLIDITY_PORT}\n    PBFTEnable = false\n    PBFTPort = 8092\n"),
    ("    port = 50051\n    solidityEnable = true\n    solidityPort = 50061\n    PBFTEnable = true\n",
     f"    port = {RPC_PORT}\n    solidityEnable = true\n    solidityPort = {RPC_SOLIDITY_PORT}\n    PBFTEnable = false\n"),
    # balanceOf and other constant calls (the per-address balance check needs them)
    ("  supportConstant = false\n", "  supportConstant = true\n"),
    # internal transactions for every block this node processes; featured = stake/delegate internal records
    # replaying blocks: allow the VM 20x the per-transaction time budget (release: 5x). At 5x this node rejected two
    # valid blocks as OUT_OF_TIME on 2026-11-12 (87,909,164 and 87,922,315) and stalled about an hour each time
    ("  maxTimeRatio = 5.0\n", "  maxTimeRatio = 20.0\n"),
    ("  saveInternalTx = false\n",
     "  saveInternalTx = true\n  saveFeaturedInternalTx = true\n  saveCancelAllUnfreezeV2Details = true\n"),
]
CONFIG_REQUIRED = ['transHistory.switch = "on"', 'db.engine = "LEVELDB"']
# Appended after the release file (HOCON merges a later node {} into the earlier one). Not in the release config.
# No effect on a full node; it keeps getblockbynum / gettransactioninfobyblocknum open after go-lite (PLAN.md 2.4).
CONFIG_APPEND = "\n# blockline-usdt (lib/usdt_node.py)\nnode {\n  openHistoryQueryWhenLiteFN = true\n}\n"

CLICKHOUSE = {  # LTS, published 2026-10-21; sha256 of the tarball whose GitHub .sha512 was checked on 2026-11-12
    "version": "26.8.33.10",
    "url": "https://github.com/ClickHouse/ClickHouse/releases/download/v26.8.33.10-lts/clickhouse-common-static-26.8.33.10-amd64.tgz",
    "sha256": "4b11daf6b9abd18a7c14cfd839d837ced74c39c71be0f945338deabbe4ed4224",
}
# CPUs without PCLMULQDQ (e.g. a VM with the generic "QEMU Virtual CPU" model) get SIGILL from the release build;
# they get ClickHouse's amd64compat build instead. It is only published from master, so this pin is the binary
# downloaded on 2026-11-19: on a new server the URL will serve a newer build and the sha256 check will refuse it.
CLICKHOUSE_COMPAT = {
    "version": "26.11.1.2819-compat",
    "url": "https://builds.clickhouse.com/master/amd64compat/clickhouse",
    "sha256": "f54cce9dc95355d5efdaaa0d983d697e264b8c29ca0b1d5a171bc2903b0d827d",
}
REPO = Path(__file__).resolve().parents[1]

DISK_RESERVE = int(os.environ.get("BLOCKLINE_USDT_DISK_RESERVE", 200 * 1024**3))  # the snapshot stream stops if the disk would drop below this
CHUNK = 4 * 1024 * 1024
SNAPSHOT_WORKERS = int(os.environ.get("BLOCKLINE_USDT_SNAPSHOT_WORKERS", 8))  # parallel connections to the mirror
SNAPSHOT_PIECE = 64 * 1024 * 1024  # bytes per range request; up to WORKERS * 3 pieces are held in memory
_state_lock = threading.Lock()


def log(msg):
    print(time.strftime("%F %T"), msg, flush=True)


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(CHUNK), b""):
            h.update(block)
    return h.hexdigest()


def fetch(url, dest, sha256):
    dest = Path(dest)
    if dest.exists() and sha256_file(dest) == sha256:
        log(f"have {dest.name} (sha256 ok)")
        return dest
    tmp = dest.with_suffix(dest.suffix + ".part")
    log(f"download {url}")
    failures = 0
    while True:
        offset = tmp.stat().st_size if tmp.exists() else 0
        headers = dict(UA)
        if offset:
            headers["Range"] = f"bytes={offset}-"
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=120) as r:
                resumed = offset and r.status == 206 and r.headers.get("Content-Range", "").startswith(f"bytes {offset}-")
                if offset and not resumed:
                    log(f"{dest.name}: server ignored the resume (HTTP {r.status}), starting over")
                    offset = 0
                elif offset:
                    log(f"{dest.name}: resuming at {offset / 1e6:.1f} MB")
                total = offset + int(r.headers.get("Content-Length") or 0)
                with open(tmp, "ab" if offset else "wb") as f:
                    while block := r.read(CHUNK):
                        f.write(block)
                        failures = 0
                if r.headers.get("Content-Length") and tmp.stat().st_size < total:
                    raise IOError("connection closed early")
            break
        except urllib.error.HTTPError as err:
            if err.code == 416 and offset:  # .part already complete (or bigger than the file): let sha256 decide
                break
            if err.code < 500:
                raise
            failures += 1
            reason = err
        except (urllib.error.URLError, http.client.HTTPException, OSError) as err:
            failures += 1
            reason = err
        if failures > 10:
            sys.exit(f"download of {url} failed 10 times in a row: {reason}")
        wait = min(60, 5 * failures)
        log(f"{dest.name}: {reason}; retry {failures}/10 in {wait}s")
        time.sleep(wait)
    got = sha256_file(tmp)
    if got != sha256:
        tmp.unlink()
        sys.exit(f"sha256 mismatch for {url}: got {got}, pinned {sha256}")
    tmp.replace(dest)
    log(f"verified {dest.name}")
    return dest


def gpg_verify(jar, sig):
    """Second check on the jars. Skipped with a warning if gpg or the keyserver is unavailable: sha256 is pinned."""
    if not shutil.which("gpg"):
        log("gpg not installed: skipping signature check (sha256 pinned)")
        return
    home = STATE / "gnupg"
    home.mkdir(parents=True, exist_ok=True, mode=0o700)
    env = {**os.environ, "GNUPGHOME": str(home)}
    have = subprocess.run(["gpg", "--batch", "--list-keys", TRON_GPG_FINGERPRINT], env=env, capture_output=True)
    if have.returncode != 0:
        got = subprocess.run(["gpg", "--batch", "--keyserver", "hkps://keys.openpgp.org", "--recv-keys",
                              TRON_GPG_FINGERPRINT], env=env, capture_output=True, text=True)
        if got.returncode != 0:
            log("could not fetch TRON's GPG key: skipping signature check (sha256 pinned)")
            return
    out = subprocess.run(["gpg", "--batch", "--status-fd", "1", "--verify", str(sig), str(jar)],
                         env=env, capture_output=True, text=True)
    if f"VALIDSIG {TRON_GPG_FINGERPRINT}" not in out.stdout:
        sys.exit(f"GPG signature check FAILED for {jar.name}:\n{out.stdout}{out.stderr}")
    log(f"{jar.name}: signed by {TRON_GPG_FINGERPRINT}")


def render_config():
    raw = urllib.request.urlopen(urllib.request.Request(CONFIG_URL, headers=UA), timeout=60).read()
    got = hashlib.sha256(raw).hexdigest()
    if got != CONFIG_SHA256:
        sys.exit(f"official config.conf changed upstream (sha256 {got}); review CONFIG_EDITS before re-pinning")
    text = raw.decode()
    for old, new in CONFIG_EDITS:
        n = text.count(old)
        if n != 1:
            sys.exit(f"config edit matched {n} times, expected 1: {old!r}")
        text = text.replace(old, new)
    for needed in CONFIG_REQUIRED:
        if needed not in text:
            sys.exit(f"config is missing {needed}")
    if "openHistoryQueryWhenLiteFN" in text:
        sys.exit("the release config now sets openHistoryQueryWhenLiteFN itself: turn CONFIG_APPEND into an edit")
    text = text.rstrip("\n") + "\n" + CONFIG_APPEND
    NODE.mkdir(parents=True, exist_ok=True)
    out = NODE / "config.conf"
    out.write_text(f"# Rendered by blockline-usdt install from {JAVA_TRON_TAG} config.conf; edits in lib/usdt_node.py\n" + text)
    log(f"wrote {out}")


def cmd_install(_):
    downloads = ROOT / "downloads"
    app = ROOT / "app" / JAVA_TRON_TAG
    for d in (downloads, app, STATE):
        d.mkdir(parents=True, exist_ok=True)

    tarball = fetch(JDK["url"], downloads / Path(JDK["url"]).name, JDK["sha256"])
    jdk_dir = ROOT / JDK["release"]
    if not (jdk_dir / "bin" / "java").exists():
        with tarfile.open(tarball) as t:
            t.extractall(ROOT, filter="data")
        if not (jdk_dir / "bin" / "java").exists():
            sys.exit(f"JDK did not extract to {jdk_dir}")
    link = ROOT / "jdk"
    if link.is_symlink() or link.exists():
        link.unlink()
    link.symlink_to(jdk_dir.name)
    log(subprocess.run([str(link / "bin" / "java"), "-version"], capture_output=True, text=True).stderr.splitlines()[0])

    base = f"https://github.com/tronprotocol/java-tron/releases/download/{JAVA_TRON_TAG}"
    for name, digest in JARS.items():
        jar = fetch(f"{base}/{name}", app / name, digest)
        sig = app / f"{name}.sig"
        if not sig.exists():
            urllib.request.urlretrieve(f"{base}/{name}.sig", sig)
        gpg_verify(jar, sig)
    current = ROOT / "app" / "current"
    if current.is_symlink() or current.exists():
        current.unlink()
    current.symlink_to(JAVA_TRON_TAG)

    render_config()
    install_clickhouse(None)
    log("install done")


def install_clickhouse(_):
    """ClickHouse server for Tier 2 (docs/CLICKHOUSE.md): pinned binary, data folders, config from the repo."""
    downloads, home = ROOT / "downloads", ROOT / "clickhouse"
    for d in (downloads, home / "bin", home / "data", home / "tmp", home / "log", home / "user_files"):
        d.mkdir(parents=True, exist_ok=True)
    flags = next((l.split(":", 1)[1].split() for l in open("/proc/cpuinfo") if l.startswith("flags")), [])
    if "pclmulqdq" not in flags:
        log("CPU has no PCLMULQDQ: using the ClickHouse amd64compat build")
        binary = home / "bin" / f"clickhouse-{CLICKHOUSE_COMPAT['version']}"
        # the compat build decompresses itself in place on first run, so its sha256 only matches before that:
        # check the pin on download, then keep the binary
        if not binary.exists():
            fetch(CLICKHOUSE_COMPAT["url"], binary, CLICKHOUSE_COMPAT["sha256"])
            binary.chmod(0o755)
    else:
        binary = home / "bin" / f"clickhouse-{CLICKHOUSE['version']}"
    if not binary.exists():
        tarball = fetch(CLICKHOUSE["url"], downloads / Path(CLICKHOUSE["url"]).name, CLICKHOUSE["sha256"])
        member = f"clickhouse-common-static-{CLICKHOUSE['version']}/usr/bin/clickhouse"
        with tarfile.open(tarball) as t:
            src = t.extractfile(member)
            with open(binary.with_suffix(".part"), "wb") as f:
                shutil.copyfileobj(src, f, CHUNK)
        binary.with_suffix(".part").chmod(0o755)
        binary.with_suffix(".part").replace(binary)
    link = home / "clickhouse"
    if link.is_symlink() or link.exists():
        link.unlink()
    link.symlink_to(Path("bin") / binary.name)
    shutil.copyfile(REPO / "clickhouse" / "config.xml", home / "config.xml")
    log(subprocess.run([str(link), "server", "--version"], capture_output=True, text=True).stdout.strip())
    log(f"ClickHouse installed in {home}; start it with the blockline-usdt-clickhouse unit")


# --- Snapshot ---------------------------------------------------------------------------------------------------------

def write_state(**fields):
    with _state_lock:
        _write_state(**fields)


def _write_state(**fields):
    STATE.mkdir(parents=True, exist_ok=True)
    path = STATE / "snapshot.json"
    data = json.loads(path.read_text()) if path.exists() else {}
    data.update(fields, updated=time.strftime("%F %T"))
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=1))
    tmp.replace(path)


def fetch_range(url, start, end):
    """Bytes start..end (inclusive) of url, resuming inside the range after a network error (40 tries in a row)."""
    buf, failures = bytearray(), 0
    while start + len(buf) <= end:
        pos = start + len(buf)
        try:
            req = urllib.request.Request(url, headers={**UA, "Range": f"bytes={pos}-{end}"})
            with urllib.request.urlopen(req, timeout=120) as r:
                if r.status != 206 or not r.headers.get("Content-Range", "").startswith(f"bytes {pos}-"):
                    raise IOError(f"range refused: HTTP {r.status} {r.headers.get('Content-Range')}")
                while block := r.read(CHUNK):
                    buf += block
                    failures = 0
            if start + len(buf) <= end:
                raise IOError("connection closed early")
        except (urllib.error.URLError, http.client.HTTPException, OSError) as err:
            failures += 1
            # the mirror (nginx) answers a share of requests with a bare 404 under parallel load and serves the same
            # range a moment later: retry those quickly instead of backing off as for a real outage
            brief = isinstance(err, urllib.error.HTTPError) and err.code in (404, 429, 502, 503, 504)
            if failures > (200 if brief else 40):
                raise RuntimeError(f"bytes {pos}-{end}: {err}") from err
            wait = 2 if brief else min(300, 10 * failures)
            if not brief or failures % 20 == 0:
                log(f"network error at {pos / 1e9:.1f} GB ({err}, {failures} in a row); retrying that range in {wait}s")
                write_state(last_error=str(err))
            time.sleep(wait)
    if len(buf) != end - start + 1:
        raise RuntimeError(f"bytes {start}-{end}: got {len(buf)} bytes")
    return buf


def cmd_snapshot(_):
    url = f"{SNAPSHOT['base']}/{SNAPSHOT['file']}"
    verified, partial = NODE / ".snapshot-verified", NODE / ".snapshot-partial"
    target = NODE / "output-directory"
    if verified.exists():
        log(f"snapshot already verified ({verified}); nothing to do")
        return
    NODE.mkdir(parents=True, exist_ok=True)
    if target.exists():
        if not partial.exists():
            sys.exit(f"{target} exists but was not written by this command; move it away first")
        log("removing a previous incomplete extraction (a gzip stream cannot restart in the middle)")
        shutil.rmtree(target)

    expected_md5 = urllib.request.urlopen(urllib.request.Request(url + ".md5sum", headers=UA), timeout=60).read().split()[0].decode()
    head = urllib.request.urlopen(urllib.request.Request(url, method="HEAD", headers=UA), timeout=60)
    size = int(head.headers["Content-Length"])
    free = shutil.disk_usage(NODE).free
    log(f"{url}: {size / 1e12:.3f} TB, md5 {expected_md5}; free disk {free / 1e12:.3f} TB")
    if free < size * 1.05 + DISK_RESERVE:
        sys.exit("not enough free disk for the extracted snapshot plus the reserve")

    partial.write_text(time.strftime("%F %T"))
    write_state(status="streaming", url=url, size=size, md5_expected=expected_md5, started=time.strftime("%F %T"),
                bytes=0, error=None)
    tar = subprocess.Popen(["tar", "-xzf", "-", "-C", str(NODE)], stdin=subprocess.PIPE)
    md5 = hashlib.md5()
    offset, last_report, window_start, window_bytes = 0, 0.0, time.time(), 0

    def fatal(msg, **state):
        # os._exit: a plain exit would wait for the download threads still retrying
        write_state(status="failed", **state)
        log(msg)
        tar.kill()
        os._exit(1)

    # The mirror caps each connection (~3.5 MB/s) but not the host (~20 MB/s over 8), so SNAPSHOT_WORKERS threads
    # fetch PIECE-sized ranges ahead while this thread feeds them to tar and md5 strictly in order.
    pieces = iter([(s, min(s + SNAPSHOT_PIECE, size) - 1) for s in range(0, size, SNAPSHOT_PIECE)])
    log(f"{SNAPSHOT_WORKERS} parallel connections, {SNAPSHOT_PIECE // 1024**2} MB pieces")
    pool = ThreadPoolExecutor(SNAPSHOT_WORKERS)
    pending = collections.deque(pool.submit(fetch_range, url, *p) for p in itertools.islice(pieces, SNAPSHOT_WORKERS * 3))
    while pending:
        try:
            data = pending.popleft().result()
        except Exception as err:  # noqa: BLE001 — fetch_range already retried
            fatal(f"gave up at byte {offset}: {err}", error=f"gave up after repeated network errors: {err}")
        nxt = next(pieces, None)
        if nxt:
            pending.append(pool.submit(fetch_range, url, *nxt))
        try:
            tar.stdin.write(data)
        except BrokenPipeError:
            fatal("tar exited while streaming (see journal)", error="tar exited while streaming")
        md5.update(data)
        offset += len(data)
        window_bytes += len(data)
        del data
        now = time.time()
        if now - last_report >= 30:
            rate = window_bytes / max(now - window_start, 1)
            free = shutil.disk_usage(NODE).free
            write_state(bytes=offset, percent=round(100 * offset / size, 2), rate_mb_s=round(rate / 1e6, 1),
                        eta_hours=round((size - offset) / max(rate, 1) / 3600, 1), free_disk_tb=round(free / 1e12, 3))
            if now - window_start > 600:
                window_start, window_bytes = now, 0
            last_report = now
            if free < DISK_RESERVE:
                fatal("disk reserve reached: stopping the stream", error="disk reserve reached")
    pool.shutdown()
    if offset != size:
        fatal(f"stream ended at byte {offset} of {size}", error="short stream")

    tar.stdin.close()
    if tar.wait() != 0:
        write_state(status="failed", error=f"tar exited {tar.returncode}")
        sys.exit(f"tar exited {tar.returncode}")
    got = md5.hexdigest()
    if got != expected_md5:
        write_state(status="failed", md5_got=got, error="md5 mismatch: re-run to download again")
        sys.exit(f"md5 mismatch: got {got}, published {expected_md5}. Re-run `snapshot` to start over.")
    if not (target / "database").is_dir():
        write_state(status="failed", error="archive did not contain output-directory/database")
        sys.exit(f"{target}/database not found after extraction")
    verified.write_text(json.dumps({"url": url, "md5": got, "bytes": offset, "verified": time.strftime("%F %T")}))
    partial.unlink()
    write_state(status="verified", bytes=offset, percent=100, md5_got=got)
    log(f"snapshot verified (md5 {got}); the node unit may start")


# --- Status -----------------------------------------------------------------------------------------------------------

def api(path, port=HTTP_PORT):
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=5) as r:
            return json.load(r)
    except Exception as err:  # noqa: BLE001 — status must always print something
        return {"error": str(err)}


def cmd_status(_):
    snap = STATE / "snapshot.json"
    print("snapshot:", snap.read_text() if snap.exists() else "not started")
    usage = shutil.disk_usage(ROOT)
    print(f"disk: {usage.free / 1e12:.2f} TB free of {usage.total / 1e12:.2f} TB")
    head = api("/wallet/getnowblock")
    solid = api("/walletsolidity/getnowblock", SOLIDITY_PORT)
    info = api("/wallet/getnodeinfo")
    if "error" in head:
        print("node: not answering on", HTTP_PORT, "-", head["error"])
        return
    num = head.get("block_header", {}).get("raw_data", {}).get("number")
    ts = head.get("block_header", {}).get("raw_data", {}).get("timestamp", 0) / 1000
    print(f"node head: block {num}, {(time.time() - ts) / 3600:.1f} h behind the clock")
    print("solidified:", solid.get("block_header", {}).get("raw_data", {}).get("number"))
    print("peers:", info.get("currentConnectCount"), "| version:", info.get("configNodeInfo", {}).get("codeVersion"))


def main():
    p = argparse.ArgumentParser(prog="blockline-usdt")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("install").set_defaults(fn=cmd_install)
    sub.add_parser("install-clickhouse").set_defaults(fn=install_clickhouse)
    sub.add_parser("snapshot").set_defaults(fn=cmd_snapshot)
    sub.add_parser("status").set_defaults(fn=cmd_status)
    args = p.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
