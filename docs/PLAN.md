# blockline-usdt: Implementation Plan

Date: 2026-11-06. Status: **revision 2, draft for review**. Sibling of `/home/debian/blockline-btc` (live, read-only
for this work) and `/home/zcash/blockline-zec`; same layout, runner, guards and verification habits.

## 1. Goals and scope

### Goal

Every USDT movement on TRON (USDT-TRC20, contract `TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t`) since the contract was deployed
in 2019, stored losslessly, checked against the chain, and enriched with enough TRON account context to support:

1. **Transaction investigation**: any transfer by hash, its fee payer, energy source, caller and position in a flow.
2. **Wallet tracking**: per-address history, counterparties, balances at any block, watchlists with alerts on new
   activity within about a minute.
3. **Address clustering**: entities built from TRON-specific evidence (section 6), with a report on every run.

### v1 (Phases 0 to 6)

USDT-TRC20 transfers, failed USDT calls and Tether admin events, plus the TRX-side context the clustering needs:
account activations, TRX transfers (including internal transfers made by contracts), resource delegation and account
permission (multisig) changes. Delivered as parquet under the data root and a Postgres schema `usdt` for the platform.

### Later

USDC-TRC20 and other TRC-20 tokens (the same ETL with a contract list), USDT on Ethereum (needs its own node: separate
plan), cross-chain flows through bridges and exchanges.

### Non-goals

A full or archive TRON node (about 4 TB of SSD and 3.3–4.4 TB of download: neither fits the traffic allowance),
contract state other than USDT balances, TRC-10 tokens, serving other TRON nodes.

## 2. Architecture

### 2.1 Repo and layout

`/home/debian/projects/blockline-usdt` during the draft, then `/home/usdt/blockline-usdt` owned by a `usdt` Linux user
(created by a sudo bootstrap, as for the forks). Data root `/srv/blockline-usdt`.

| Folder | Does | Output (under `data_root`) |
|---|---|---|
| `node/` | java-tron **Lite FullNode** settings (history queries and internal transactions on), systemd unit, loopback-only HTTP API, peer limits, the offline prune script (2.2) | `node/` |
| `etl/` | `live`: polls the node block by block. `backfill`: history from the chosen source (2.4), kept as received. Both write the same Tier 2 schemas | `foundation/tier1`, `foundation/source`, `tier2` |
| `index/` | Address numbering, per-address stats, balances by block, counterparty edges, features | `foundation/tier3` |
| `cluster/` | ut-v1: evidence edges, service detection, union-find with merge forest, report, quality, ablation | `clustering/ut-v1` |
| `labels/` | Tether blacklist (from chain), OFAC / OpenSanctions TRON addresses, ground truth | `labels/` |
| `hot/` | Unsealed recent blocks, watchlists, alerts | `hot/` |
| `pipeline/` | Runner, RAM guard, traffic guard (reads the host ledger), status | `status.json`, `logs/` |
| `dashboard/` | One page on 127.0.0.1 (port 8288), opened through an SSH tunnel | |
| `install/` | Staged, idempotent setup: JDK 8 (Temurin 8 tarball, no root), java-tron, snapshot, services | |
| `verify/` | Balance checks against the node, per-block log counts against a second source, supply reconciliation (section 5) | |
| `bin/` | `blockline`: install, status, test | |

### 2.2 Node

- java-tron Lite FullNode from the official **lite snapshot**: current state plus the last 65,536 blocks. After
  startup it stores everything a FullNode stores, so its database grows at the FullNode rate (rate measured in
  Phase 0). It answers `triggerconstantcontract` (`balanceOf`, `totalSupply`) at the head.
- `node.openHistoryQueryWhenLiteFN = true`. A Lite FullNode closes its history-query APIs by default (blocks by
  height, transaction info), which would stop `getblockbynum` and `gettransactioninfobyblocknum`. With the setting on
  they return only what is still in the local database. Phase 0 checks that both endpoints answer.
- `vm.saveInternalTx = true` and `vm.saveFeaturedInternalTx = true` (Stake 2.0 and voting records) **from the first
  start**. They apply only to blocks processed after they are set, and do not backfill. The 65,536 blocks inside
  the snapshot were processed elsewhere, so their internal transactions come from the backfill source.
- **Disk growth is the risk, not data loss.** History is lost only when we prune or re-bootstrap from a fresh
  snapshot. Pruning is offline: stop the node, confirm it has exited, back up, run `Toolkit.jar db lite`, restart.
  The tool keeps the current state plus the latest 65,536 blocks.
- **Pruning rule**: never prune past the last sealed epoch of the live ETL. The prune script refuses unless every
  block below the prune point (head − 65,536) is in a sealed epoch, so it runs in the window after an epoch seal.
  Schedule: section 10.2 and decision 6.
- JDK 8 on x86_64 (Temurin 8 tarball, no root); JDK 17 only if the host is ARM64. java-tron checks the architecture
  and JDK at startup and exits on a mismatch. This host is x86_64. Heap 12 GB, RAM cap 24 GB for the unit (settings,
  revisited after Phase 0). HTTP API on 127.0.0.1 only; no gRPC exposed.
- Traffic: java-tron has no "blocks only" mode, so peers are kept low (`maxConnections` about 10, no inbound listen
  if it syncs without it). The first seven days are measured, upload and download separately, before the monthly
  figure goes into the guard.

### 2.3 Data root, ports, users

`/srv/blockline-usdt`, owned by `usdt`, ACL for `debian`. Node HTTP 127.0.0.1:8090, dashboard 127.0.0.1:8288. Traffic
is counted once for the whole host in `/srv/blockline-shared/bandwidth.json` (the BTC guard's ledger, shared as for
the forks); the USDT guard stops the TRON node at its own share of the month.

### 2.4 Ingestion

- **Live**: poll the node's HTTP API on loopback, one block at a time: `getblockbynum` (transactions, contract types,
  owners) and `gettransactioninfobyblocknum` (logs, internal transactions, energy, fees, result). Idempotent and
  resumable by block number; no Kafka, no event plugin. The java-tron event plugin is the fallback if polling cannot
  keep up (it should: one block every 3 s).
- **Backfill** (2019-04 to the snapshot height): the source is chosen in Phase 0 by measurement. Candidates:
  1. **BigQuery** `bigquery-public-data.goog_blockchain_tron_mainnet_us` (not `goog-blockchain.tron_mainnet_us`, which
     stops updating after 2026-10-31). Tables: `blocks`, `transactions`, `receipts`, `logs`, `decoded_events`. Good for
     USDT logs. The schema page lists no traces table, so internal transactions are probably missing. Coverage of TRX
     contract types (transfers, delegations, permission updates) in `transactions` is confirmed in Phase 0. Downloads
     count against host traffic.
  2. **SQD Portal** TRON dataset: blocks, transactions with contract type, receipt fields, logs and internal
     transactions, as JSON lines. Bytes per block, access terms and licence are measured or checked in Phase 0.
  3. A commercial export.

  **TronGrid** is not viable as the backfill. The TRX context needs about 80M blocks × 2 calls, and the free tier is
  about 100k requests a day, so it would take years (both figures approximate, to confirm). It stays as a
  cross-check source (section 5).
  The choice is made on bytes per transfer, completeness (checked in section 5), coverage of internal transactions
  and licence. If the chosen source has no internal transactions, the gap is stated in every report. Expected size:
  a few billion USDT transfers (estimate). Traffic is 0.4–2 TB depending on the source (estimate), spread over 2–3
  months under the guard.

## 3. Data model

### 3.1 Epochs and sealing

Epoch = 201,600 blocks (7 days). A block is final when it is at or below the node's **solidified** block
(`walletsolidity/getnowblock`, about 19 blocks / 1 minute behind head; estimate, measured in Phase 0). An epoch is
sealed when its last block is solidified plus a margin of 1,000 blocks. Sealed epochs are immutable; `hot/` holds
everything newer and is rebuilt from the node after any fork.

### 3.2 Tier 1 (lossless)

**Live era only.** Per kept transaction, not per block: the transaction from `getblockbynum` and its
`gettransactioninfobyblocknum` entry, without `raw_data_hex`, zstd-compressed (protobuf considered against JSON in
Phase 0). Kept transactions are those of the types in Tier 2. Lets every Tier 2 table be rebuilt without the network.
Nearly every block has a USDT transaction, so filtering whole blocks would save almost nothing.

Size, estimate to measure in Phase 0: about 7M transactions a day × about 2 KB of JSON is about 14 GB a day raw, about
3 GB a day zstd, about 1 TB a year. Keeping only kept types, without `raw_data_hex`, lowers that by an amount measured
in Phase 0.

For the backfill the lossless layer is the source's raw export as received (`foundation/source/<source>/`), not Tier 1.

### 3.3 Tier 2

| Table | Rows | Key columns |
|---|---|---|
| `usdt_transfers` | every `Transfer` log of the USDT contract (successful transactions only: a reverted transaction emits no logs) | block, ts, tx_hash, log_index, from, to, amount (uint64, 6 decimals; checked never to overflow), method (`transfer` / `transferFrom` / other), caller (tx owner) |
| `usdt_failed_calls` | failed transactions whose call targets the USDT contract directly, decoded from call data | block, tx_hash, method, from/owner, to, amount, result |
| `usdt_tx_costs` | per USDT transaction | energy_used, origin_energy_usage (part paid by the contract deployer), energy_from_stake, energy_fee (TRX burned), net_fee, fee_limit, inferred energy source |
| `usdt_admin` | `AddedBlackList`, `RemovedBlackList`, `DestroyedBlackFunds`, `Issue`, `Redeem`, `Deprecate`, `Params`, `Pause`, ownership changes | block, tx_hash, event, address, amount |
| `usdt_approvals` | `Approval` logs | owner, spender, amount |
| `account_activations` | first appearance of an address: `CreateAccountContract`, or the TRX / TRC-10 transfer that created it, internal transfers included | address, activator, activator_is_contract, block, method |
| `trx_transfers` | `TransferContract` and internal TRX transfers | from, to, amount_sun, internal (bool) |
| `internal_transfers` | every internal transaction (`vm.saveInternalTx`) | tx_hash, caller, to, token (TRX / TRC-10 id), amount, rejected |
| `resource_delegations` | `DelegateResourceContract`, `UnDelegateResourceContract`, legacy freeze with receiver | delegator, receiver, resource, amount, lock |
| `permission_updates` | `AccountPermissionUpdateContract` | account, owner keys, active keys, thresholds |
| `contracts` | addresses that are smart contracts | address, creator, block |

- **Energy source**: the receipt names no delegator. The energy source in `usdt_tx_costs` is inferred from the
  `resource_delegations` state at that block and is labelled as inferred. The USDT contract/user energy split
  (`origin_energy_usage`) is confirmed in Phase 0.
- **Failed calls** made through another contract are not recoverable from logs. An internal transaction with
  `rejected = true` shows only that a nested call happened, not its arguments.
- **Deprecate**: an immediate alert (hot layer and dashboard). After it, calls may be forwarded to an upgraded
  contract that this pipeline does not watch.
- **Amounts**: a single transfer is uint64. Every sum (per-address totals, edge values, entity totals, report
  figures) is int128 / `decimal(38,0)` (DuckDB `HUGEINT`, Postgres `numeric(38,0)`).

Scope of the TRX-side tables: all accounts from the live ETL (cheap, rows are small). For the backfill, all accounts
if the source allows it at acceptable traffic, otherwise only accounts that ever touched USDT (two passes). Decided
in Phase 0.

### 3.4 Tier 3 (index)

Address numbering (`lib/ids.py`, one definition), per-address stats (first/last seen, in/out counts and value,
distinct counterparties, active days), daily balances, a counterparty edge table (address pair, count, value, first,
last), per-transfer features (round amounts, time to forward, share of balance forwarded), lookup indexes by address
and by tx hash. Every value column that is a sum is `decimal(38,0)`.

## 4. Parsing

The USDT `Transfer` event is `Transfer(address indexed from, address indexed to, uint256 value)`; topics are
EVM-style 20-byte addresses, stored as TRON base58 (`41` prefix + checksum) and as 21-byte binary. Reverted
transactions emit no logs, so they never reach `usdt_transfers`. Direct calls to the USDT contract are decoded from
call data into `usdt_failed_calls`.

**Balance rebuild** applies admin events as well as transfers. `DestroyedBlackFunds` sets the address to 0 and lowers
`totalSupply` without a `Transfer`. In the Ethereum-lineage TetherToken code, `Issue` credits and `Redeem` debits the
owner without a `Transfer`. The rebuild applies `Issue` (+owner), `Redeem` (−owner) and `DestroyedBlackFunds`
(address → 0), unless Phase 0 shows that the deployed TRON contract emits a matching `Transfer` (then it does not
count twice). All parsing lives in `lib/tron.py`, with fixtures taken from real blocks.

## 5. Checks, classified by what they can prove

| Check | Proves |
|---|---|
| **Supply**: `Issue − Redeem − DestroyedBlackFunds` = sum of all rebuilt balances = `totalSupply()` at the head | admin events are handled correctly. **Only that**: transfers conserve the sum of balances, so a missing or doubled transfer passes this check |
| **No negative balance**: every address ≥ 0 after every block during the rebuild | catches missing credits or doubled debits wherever they make a balance negative |
| **Balances**: `balanceOf` at the head, through the local node, for **every** address that ever held USDT, against the rebuilt balance (sweep time measured in Phase 0). The 10,000-address sample (all large holders + random) stays as the fast daily check | per-address correctness at the head; the full sweep is the completeness check for current balances |
| **Per-block log count**: USDT `Transfer` logs per block against a second source (TronGrid or another candidate) | no transfer missing or doubled in the blocks checked |
| **Source cross-check**: one week of backfill re-fetched from the live node or a second source, row for row | the backfill source is complete for that week |
| **Continuity**: every block number from contract deployment to the head is either present or listed in `gaps.parquet` | no silent holes |
| **Fingerprints**: per epoch, a hash of each sealed Tier 2 table | two builds, or two machines, agree |

A sealed epoch that fails continuity or the no-negative-balance check does not enter Tier 3.

## 6. Clustering: ut-v1

TRON is account-based: there is no co-spend, so the Bitcoin rules do not apply. The rules below are split into
**merging** edges and **evidence** that is reported but never merges. Every merging rule has a service exclusion,
because on TRON the dangerous case is a service that touches millions of users (exchanges, energy-rental markets,
activation services, custodians).

### Service detection (runs first)

An address is a **service** if it activates more than N distinct accounts, delegates resources to more than N, holds
a key in the permissions of more than N accounts (custody or co-signer), is a contract, activates accounts through
internal transfers (activation by a contract), or is labelled as an exchange. N is set by ablation, starting at 50.
On TRON a third party can pay fees only through delegation, so "delegates resources to" is the fee-payer test.

**A service may receive joins, but may never connect two other entities.** A deposit address joins the exchange it
sweeps to (rule 2). No edge whose evidence passes through a service (activated by it, delegated to by it, sharing a
key it holds) merges the two accounts on either side.

### Merging edges

1. **Shared key** (strongest): accounts whose owner or active permission lists the same key, or the same set of keys
   with a threshold one party can meet alone. The TRON equivalent of "same key" in cl-v1. A key held by more than N
   accounts makes them a service (above) and does not merge them. A permission change shortly before an outflow that
   drains the account is evidence of a hijack, never a merge.
2. **Deposit sweep**: address D receives USDT from several unrelated senders and forwards almost its whole balance,
   repeatedly and within a short delay, to the same hot wallet H, with no other outflows. D joins H's entity, even
   when H is a service. The deposit-address rule for account chains (Victor, 2020), adapted to TRC-20.
3. **Small-scale activation**: **evidence only in the first run** (listed below). Promoted to merging only if
   ground-truth precision supports it in the ablation.
4. **Small-scale resource funding**: a non-service delegates energy or bandwidth to A more than once. Joins A to the
   delegator.

Union-find with a merge forest, as in cl-v1; a circuit breaker refuses any single join of two sides above a size
limit and lists it for review.

### Evidence (reported, never merging)

Small-scale activation (rule 3: a non-service activator that activates account A and then funds it or receives from
it), permission changes before draining outflows, `transferFrom` spenders, amount-and-time matches through an
intermediary (peel chains, pass-through), round trips, time-of-day profiles, shared counterparties.

### Reported with every run (mandatory)

Entity size distribution; merges per rule (count and USDT value); the largest 100 entities with their dominant joins;
joins refused by the circuit breaker; ablation (each rule switched off in turn and rule 3 switched on: merges lost or
gained, ground-truth pairs before and after); entity quality per entity (shape: exchange-like, personal,
contract-bound, suspected collapse); the internal-transaction gap of the backfill source, if any.

## 7. Labels and ground truth

- **On chain, authoritative**: Tether blacklist and frozen-funds events (`usdt_admin`).
- **Sanctions**: OFAC SDN TRON addresses, OpenSanctions; each with its licence zone, as in blockline-btc `labels/`.
- **Exchanges**: published hot wallets with citations; TronScan public tags only if their terms allow it (to check).
- **Ground truth**: `data/groundtruth/*.csv` (`published` rows with citation), used only to score the clustering,
  never as merge input.

## 8. Investigation layer

- **Tracing**: forward and backward from an address or transfer, with selectable methods (all paths, FIFO, haircut),
  stopping at services and listing the service reached.
- **Wallet tracking**: watchlists in Postgres; the hot layer checks every new block against them and records an alert
  (address, transfer, counterparty entity, label) within one block plus the poll interval.
- **Exposure**: per address or entity, the share of inflow and outflow from labelled categories (sanctioned,
  blacklisted, exchange, unknown), direct and up to K hops.
- **Delivery**: parquet for analysis; Postgres schema `usdt` (transfers, entities, labels, alerts) for the platform,
  with every sum column `numeric(38,0)`. An API comes after v1.

## 9. Operations

Same pattern as blockline-btc: `bin/blockline install`, `bin/blockline status`, one runner that starts each step as
soon as its input exists, a commit point per step, RAM guard, traffic guard, dashboard on loopback. No agent runs on
the server. The live ETL has a lag alert, because watchlist alerts are only as fresh as the ETL (section 8): it fires
when the ETL is more than 10 minutes behind the solidified head (setting). A lagging ETL loses no data, because the
node keeps every block until it is pruned. The prune script (2.2) and a disk alert at the ceiling (decision 6) run
from the same runner.

## 10. Capacity and timing

### 10.1 Measured on this server (2026-11-06)

32 vCPU x86_64, 251 GB RAM (about 170 GB free with blockline-btc and wallex running), 5.5 TB free disk. The disk
performs as SSD: 441 MB/s write and 766 MB/s read sequential, about 22,000 random 4K reads/s and 15,000 random 4K
writes/s with 16 parallel requests, under the current load. Traffic used this month: 38.8 of 1,000 GB (BTC is synced
and uses about 40 GB a month).

### 10.2 Budget

All figures in this table are estimates; each is on the Phase 0 measurement list.

| | Disk | Traffic | RAM |
|---|---|---|---|
| Lite node at start | 200–300 GB | 0.1–0.2 TB once | 24 GB |
| Lite node growth | FullNode rate between prunes (rate to measure) | 150–300 GB/month (upload and download to measure) | — |
| Tier 1, live era | about 1 TB/year (3.2) | — | — |
| Backfill source as received | depends on source (to measure) | 0.4–2 TB once, over 2–3 months | — |
| Tier 2 + Tier 3 | 300–500 GB | — | — |
| Postgres `usdt` | 0.6–1 TB | — | 32–64 GB |
| Clustering, peak | 100 GB working space | — | 64–128 GB |
| **Total, first year** | **about 2.2–2.9 TB plus lite node growth and the backfill source** | | |

**Pruning schedule**: monthly to start (the cadence java-tron's docs call common), in the window after an epoch seal
(2.2), and earlier if the lite node passes its share of the disk ceiling (decision 6). After Phase 0 the cadence is
set from the measured growth rate.

The forks plan needs disk too (one fork at a time). USDT is budgeted at 3 TB in the first year, and the dashboard
shows the free disk.

## 11. Phases

| Phase | Work | Done when |
|---|---|---|
| **0. Spike** (1–2 weeks) | Lite node from snapshot with history queries and internal transactions on; the measurements below; live ETL prototype; backfill source chosen by measuring 1 day of data from each candidate; activation semantics confirmed (does a TRC-20 transfer to a new address activate it? expected no) | node synced and steady, every measurement below recorded, backfill source and scope of TRX tables decided |
| **1. Live** | `node/` (including the prune script), `etl live`, Tier 1/2 for new blocks, sealing, guards, status, dashboard | live data from the snapshot height on, no gaps for 14 days |
| **2. Backfill** | `etl backfill` from 2019 to the snapshot height; section 5 checks | full `balanceOf` match at the head for every address that ever held USDT; no negative balance at any block; per-block log counts match on the cross-check week; supply reconciles |
| **3. Index** | Tier 3 | stats and balances match the node for the sample |
| **4. Clustering** | ut-v1, report, quality, ablation | report reviewed; no unexplained join over the breaker limit |
| **5. Labels** | label store, ground truth, scoring | ground-truth precision and recall published in the report |
| **6. Investigation** | tracing, watchlists and alerts, exposure, Postgres `usdt` | an analyst can investigate a hash, track a wallet and see its entity |

Phase 1 starts as soon as the node is up, even before the backfill source is chosen, so that the live era begins as
early as possible and internal transactions are recorded locally from the first block processed.

Phase 0 measures and confirms:

- CPU architecture (x86_64 on 2026-11-06, `uname -m`), so JDK 8.
- `getblockbynum` and `gettransactioninfobyblocknum` answer on the lite node with `openHistoryQueryWhenLiteFN`.
- Lite node disk growth per day.
- Lite node traffic per month, upload and download separately.
- Solidified-block lag behind head.
- Tier 1 bytes per day, JSON against protobuf.
- Time for a full `balanceOf` sweep over every address that ever held USDT.
- Whether `Issue` / `Redeem` emit `Transfer` in the deployed TRON USDT contract.
- The USDT energy split between contract deployer and user (`origin_energy_usage`).
- BigQuery coverage of TRX-side contract types (transfers, delegations, permission updates) and of internal
  transactions; bytes per day downloaded.
- SQD Portal bytes per block, access terms and licence.
- TronGrid request quota (for the cross-check).
- Disk and traffic figures of 10.2.

## 12. Decisions needed from the owner

1. **Backfill source and budget**: BigQuery (USDT logs; TRX context and internal transactions to confirm), SQD Portal
   (TRX context and internal transactions; licence to check), or a commercial export. TronGrid only as a cross-check.
2. **TRX context scope**: all accounts, or only accounts that touched USDT (smaller, but weaker clustering).
3. **Traffic share**: how much of the 1 TB/month USDT may use while the backfill runs, given the forks schedule.
4. **Bootstrap**: create the `usdt` user and `/srv/blockline-usdt` (one sudo script, as for the forks).
5. **Platform**: whether `usdt` lives in the wallex Postgres or its own database.
6. **Lite node pruning schedule and disk ceiling**: how much disk the lite node may reach before an early prune, and
   whether monthly is the default cadence.

## Changes from revision 1

1. Lite node keeps every block after startup and grows at FullNode rate; the "forgets blocks after 2.3 days"
   constraint is removed; pruning is offline, and the rule is never to prune past the last sealed epoch (2.2, 9, 11).
   Source note: the cited page (`tronprotocol.github.io/.../developers/litefullnode`) returns 404. Confirmed instead on
   developers.tron.network "Deploy the FullNode" (updated 2026-11-05) and the java-tron Toolkit page.
2. Lag alert kept, now justified by watchlist freshness, threshold 10 minutes behind the solidified head (9).
3. Lite node growth row, Tier 1 row, backfill-source row and a pruning schedule added; first-year total re-budgeted
   (10.2).
4. `node.openHistoryQueryWhenLiteFN = true` (2.1, 2.2). The docs say history queries (blocks by height, transaction
   info) are closed by default, but do not name `getblockbynum` / `gettransactioninfobyblocknum`; Phase 0 checks both.
5. JDK 8 on x86_64, JDK 17 only on ARM64 (2.1, 2.2, 11). Source note: issue #5976 does not state this rule; the
   developers.tron.network deployment page does, and adds that the node exits on a mismatch. Host confirmed x86_64.
6. Supply check rewritten as proving admin-event handling only; no-negative-balance and per-block log count added;
   full `balanceOf` sweep for every holder, with the sample kept as the daily check (5).
7. Phase 2 "done when" changed to full `balanceOf` match, no negative balance, per-block counts match (11).
8. Balance rebuild applies `Issue`, `Redeem` and `DestroyedBlackFunds` unless Phase 0 shows matching `Transfer` events
   (4). BlockSec confirms that `DestroyedBlackFunds` zeroes the balance without `Transfer`; it does not cover
   `Issue` / `Redeem`, so that stays in Phase 0.
9. Failed transactions removed from `usdt_transfers`; `usdt_failed_calls` added (3.3, 4).
10. `vm.saveInternalTx = true` from the first start; `internal_transfers` added; activations and TRX transfers include
    internal transfers; activation by a contract is a service signal (2.2, 3.3, 6). Added from the source:
    `vm.saveFeaturedInternalTx` for Stake 2.0 records, and the note that the snapshot's own blocks have no local
    internal transactions.
11. Tier 1 now holds kept transactions without `raw_data_hex`, live era only; size estimate added; the backfill's
    lossless layer is the source export as received (2.1, 3.2, 10.2).
12. Backfill candidates now BigQuery, SQD Portal, commercial; TronGrid only as a cross-check (2.4, 12). Source note:
    the BigQuery schema page lists **five** TRON tables (`blocks`, `transactions`, `receipts`, `logs`,
    `decoded_events`), not three, and no traces table. SQD: licence and terms not found on its page.
13. `Deprecate` and `Params` added to `usdt_admin`, with an immediate alert on `Deprecate` (3.3).
14. `origin_energy_usage` added; energy source marked as inferred from delegation state; service detection uses
    "delegates resources to" (3.3, 6).
15. Sums are int128 / `decimal(38,0)` in parquet, DuckDB and Postgres (3.3, 3.4, 8).
16. Services may receive deposit-sweep joins but never connect two other entities (6).
17. A key shared by more than N accounts marks a service; permission change before a draining outflow is hijack
    evidence (6).
18. Small-scale activation moved to evidence for the first run; ablation tests promoting it (6).
19. Decision 1 rewritten, decision 6 (pruning and disk ceiling) added (12).
20. Phase 0 measurement list added under the phase table; estimates in 3.1, 3.2, 2.4 and 10.2 marked (11).
