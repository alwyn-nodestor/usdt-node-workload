# Status page

One read-only page on `127.0.0.1:8288` (PLAN.md 2.2, 2.5) that shows the snapshot stream, the node and the ETL.
`dashboard/server.py` (stdlib Python, no virtualenv) serves `dashboard/index.html` and a JSON feed; the page refreshes
every 5 seconds. It writes nothing and calls nothing outside the machine.

Open it from your own machine through an SSH tunnel:

    ssh -L 8288:127.0.0.1:8288 debian@<server>
    # then http://localhost:8288

It runs as the `blockline-usdt-dashboard` unit (user `usdt`, in the `systemd-journal` group so the log panel can read
the snapshot, node and ETL logs). `sudo systemctl restart blockline-usdt-dashboard` after a `git pull`.
Requests with any Host other than 127.0.0.1 / localhost get 421, so a web page cannot read it by DNS rebinding.

## What it reads

| Panel | Source |
|---|---|
| Pipeline | derived from the panels below: snapshot → node catch-up → Phases 0–3 |
| Snapshot | `/srv/blockline-usdt/state/snapshot.json` (written by `blockline-usdt snapshot`) and `node/.snapshot-verified` |
| Node | `/wallet/getnowblock`, `/wallet/getnodeinfo` on 18090, `/walletsolidity/getnowblock` on 18091 |
| ETL | `/srv/blockline-usdt/state/etl.json` (contract below) |
| Host | disk of `/srv`, `/srv/blockline-shared/bandwidth.json`, `/proc/meminfo`, load |
| Services | `systemctl show` for the snapshot, node, etl and dashboard units |
| Log | `journalctl -u <unit> -n 60` |

The rate charts come from one sample a minute kept in memory (a day); they start empty when the page server restarts.
"Blocks to go" for the node is estimated from the clock (one block every 3 s), not asked of the network.

## ETL contract: `state/etl.json`

The ETL (`etl/run.py live`, docs/ETL.md) reports by rewriting this file atomically (write a temp file, then rename) at least once a
minute. Every field is optional; the page shows what is present. The page marks the ETL "stalled?" when `updated` is
more than 5 minutes old.

```json
{
  "updated": "2026-11-20 12:00:00",          // UTC, "YYYY-MM-DD HH:MM:SS"
  "phase": "2",                              // current plan phase: "0", "1", "2" or "3"
  "phases": {"0": "done", "1": "run"},       // optional override per phase: done | run | wait | fail
  "state": "running",                        // short word for the badge
  "history": {"next_block": 30000000, "target_block": 77000000, "blocks_per_s": 52.3, "workers": 4},
  "live":    {"start_block": 87915000, "last_block": 87930000, "lag_blocks": 1, "solid_head": 87930001,
              "blocks_per_s": 0.33, "blocks_done": 15000},
  "epochs":  {"sealed": 30, "total": 77},
  "rows":    {"trc20_transfers": 123456789},
  "checks":  {"continuity": "pass", "no negative balance": "pending"},   // pass | fail | pending
  "error": null                              // a message here turns the badge red
}
```
