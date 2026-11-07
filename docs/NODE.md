# blockline-usdt node (route A: full snapshot on this host)

Decided 2026-11-07: the TRON **full node** runs on this host from the official FullNode snapshot **with internal
transactions**. Management cleared exceeding the 1 TB monthly traffic allowance for the download only. This replaces
the lite-node-plus-cloud-VM route of PLAN.md revision 3 for the node and the backfill source; PLAN.md still needs
updating to match.

## Facts measured on 2026-11-07

| | |
|---|---|
| Snapshot | `http://35.247.128.170/backup20261106/FullNode_output-directory.tgz`, 3,926,337,977,624 bytes, md5 `72a47d2c4e600c37e63d2f1acfc9256e` |
| Download speed | about 29.5 MB/s (20 s sample): about 37 hours for the stream |
| Free disk | 5.5 TB on `/` (shared with BTC and the forks) |
| Traffic guard | counts rx + tx; stops `bitcoind` at 950 GB unless the month is in `limits_off_months` |
| Port 8090 | taken by blockline-btc's hot service, so the node uses 8590/8591 (HTTP) and 50551/50561 (gRPC) |
| Firewall | inbound is filtered upstream (a public port is unreachable); the node's API ports are also dropped locally |

## Pinned runtime

java-tron `GreatVoyage-v4.8.2.4` (FullNode.jar and Toolkit.jar by sha256, and TRON's release GPG key
`1254 F859 D2B1 BD9F 66E7 107D F859 BCB4 4A28 290B`), Eclipse Temurin `jdk8u512-b09` by sha256, and the release's own
`config.conf` by sha256 with the edits listed in `lib/usdt_node.py` (10 peers, internal transactions and featured
internal transactions saved, constant calls on for `balanceOf`, PBFT off, ports above).

## Running it

```sh
sudo bash /home/debian/projects/blockline-usdt/bootstrap/usdt-bootstrap.sh     # once, root
sudo -u usdt /home/usdt/blockline-usdt/bin/blockline-usdt install               # JDK, jars, config
sudo systemctl start blockline-usdt-snapshot                                    # stream + verify, then node starts
sudo -u usdt /home/usdt/blockline-usdt/bin/blockline-usdt status                # progress / node head
journalctl -u blockline-usdt-snapshot -f
```

The stream resumes a dropped connection at the byte it stopped (HTTP Range, one connection at a time; the mirror warns
that parallel or segmented downloads get 404). It stops if free disk would fall below 200 GB. The node unit will not
start until `.snapshot-verified` exists (md5 matched and `output-directory/database` present).

## First checks once the node runs (Phase 0)

1. The earliest block whose `gettransactioninfobyblocknum` carries internal transactions (the mirror does not say).
2. `getblockbynum` and `gettransactioninfobyblocknum` answer for a 2018 and a 2019 block on 8590 and 8591.
3. Catch-up speed to the head (the snapshot is from 2026-11-06, so about a day of blocks).
4. Node traffic per day once synced (budget: 300 GB a month).
