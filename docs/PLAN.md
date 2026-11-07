# blockline-usdt: Implementation Plan

Date: 2026-11-07. Status: **revision 3, draft for review**. Sibling of `/home/debian/blockline-btc` (live, read-only
for this work) and `/home/zcash/blockline-zec`; same layout, runner, guards and verification habits. Revision 3
applies the verification review of 2026-11-07 (changelog at the end).

## 1. Goals and scope

### Goal

Every USDT movement on TRON (USDT-TRC20, contract `TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t`, deployed 2019-04-16 at about
block 8.4M, estimate) stored losslessly, checked against the chain, and enriched with enough TRON account context
**from genesis (2018-06)** to support:

1. **Transaction investigation**: any transfer by hash, its fee payer, energy source, caller and position in a flow.
2. **Wallet tracking**: per-address history, counterparties, balances at any block, watchlists with alerts on new
   activity within about a minute.
3. **Address clustering**: entities built from TRON-specific evidence (section 6), with a report on every run.

### v1 (Phases 0 to 6)

USDT-TRC20 transfers, failed USDT calls and Tether admin events from contract deployment, plus the TRX-side context
the clustering needs from block 0: account activations, TRX transfers (including TRX sent with contract calls and
internal transfers made by contracts), resource delegation (Stake 1.0 and 2.0), account permission (multisig)
changes and contract creations. Delivered as parquet under the data root and a Postgres schema `usdt` for the
platform.

### Later

USDC-TRC20 and other TRC-20 tokens (the same ETL with a contract list), USDT on Ethereum (needs its own node: separate
plan), cross-chain flows through bridges and exchanges.

### Non-goals

A FullNode on this host (3.31–3.35 TB compressed snapshot, 3.54 TB with internal transactions, Sep 2026: neither the
download nor the disk fits), contract state other than USDT balances, TRC-10 tokens, serving other TRON nodes.

## 2. Architecture

### 2.1 Repo and layout

`/home/debian/projects/blockline-usdt` during the draft, then `/home/usdt/blockline-usdt` owned by a `usdt` Linux user
(created by a sudo bootstrap, as for the forks). Data root `/srv/blockline-usdt`.

| Folder | Does | Output (under `data_root`) |
|---|---|---|
| `node/` | java-tron **Lite FullNode** settings (history queries and internal transactions on), systemd unit, loopback-only HTTP API, peer limits, firewall rule for port 18888, the offline prune script (2.2) | `node/` |
| `etl/` | `live`: polls the node's solidity API block by block. `backfill`: loads the parquet extracted off-host (2.4) and checks it against its manifest. Both write the same Tier 2 schemas | `foundation/tier1`, `foundation/backfill`, `tier2` |
| `extract/` | Scripts for the temporary cloud VM (2.4): restore the snapshot, run a peer-less FullNode, export parquet, per-block aggregates, the frozen-height `balanceOf` sweep | (written on the VM, shipped as parquet) |
| `index/` | Address numbering, per-address stats, balances by block, counterparty edges, features | `foundation/tier3` |
| `cluster/` | ut-v1: evidence edges, service detection, union-find with merge forest, report, quality, ablation | `clustering/ut-v1` |
| `labels/` | Tether blacklist (from chain), OFAC TRON addresses, ground truth | `labels/` |
| `hot/` | Unsolidified and unsealed recent blocks, watchlists, alerts | `hot/` |
| `pipeline/` | Runner, RAM guard, disk guard, traffic guard (reads the host ledger), offsite backup, status | `status.json`, `logs/` |
| `dashboard/` | One page on 127.0.0.1 (port 8288), opened through an SSH tunnel | |
| `install/` | Staged, idempotent setup: JDK 8 (Temurin 8 tarball, no root), java-tron, snapshot, services | |
| `verify/` | Balance checks, per-block aggregates against BigQuery, supply reconciliation, continuity (section 5) | |
| `fixtures/` | One real (or synthetic) case per edge case, written before the parser (section 4.4) | |
| `bin/` | `blockline`: install, status, test | |

### 2.2 Node

- java-tron Lite FullNode from the official **lite snapshot**: current state plus the last 65,536 blocks. On disk at
  start: about 3% of a FullNode, so about 100 GB (docs; Phase 0 measures it). After startup it stores everything a
  FullNode stores, so its database grows at the FullNode rate (measured in Phase 0). It answers
  `triggerconstantcontract` (`balanceOf`, `totalSupply`) at the head.
- `node.openHistoryQueryWhenLiteFN = true` (off by default). The docs do not say which endpoints it opens. Phase 0
  checks that `getblockbynum` and `gettransactioninfobyblocknum` answer, on both the HTTP port (8090) and the
  solidity port (8091), with the setting on.
- `vm.saveInternalTx = true` and `vm.saveFeaturedInternalTx = true` (both off by default) **from the first start**.
  They apply only to blocks processed after they are set, and do not backfill. The 65,536 blocks inside the snapshot
  were processed elsewhere, so their internal transactions come from the cloud extraction (2.4).
- **Disk growth is the risk, not data loss.** History is lost only when we prune or re-bootstrap from a fresh
  snapshot. Pruning is offline: stop the node, confirm it has exited, back up, run `Toolkit.jar db lite` (it splits a
  new lite snapshot out of the node's database, which is then swapped in), restart. The result keeps the current state
  plus the latest 65,536 blocks. Scratch space during the split is about 2× the node database. Watchlist alerts pause
  while the node is down.
- **Pruning rule**: never prune a block whose data is not in a sealed epoch. The prune script computes the prune point
  (head − 65,536) **after** the node has exited, and refuses unless every block below it is in a sealed epoch whose
  Tier 1 and Tier 2 files are written and synced to disk (fsync, checksum recorded) and backed up offsite (9). Because
  sealing lags the head by between about 1,020 blocks and one epoch, the window opens after each epoch seal and lasts
  about 2.2 days in every 7. The runner schedules the prune on the day after a seal.
- Cadence: the docs say "periodically". The cadence is set from the measured growth rate and the disk ceiling
  (decision 6), not a fixed month.
- JDK 8 on x86_64 (Temurin 8 tarball, no root); JDK 17 only on ARM64, which also needs the RocksDB snapshot instead of
  LevelDB (both since v4.8.1). This host is x86_64. Heap 12 GB (docs: 9–12 GB at 16 GB RAM, 40% of RAM at 32 GB+), RAM
  cap 24 GB for the unit, revisited after Phase 0. HTTP and solidity APIs on 127.0.0.1 only; no gRPC exposed.
- Traffic: java-tron has no "blocks only" mode. Peers are kept low with `node.maxConnections = 10` (default 30) and
  `node.minConnections` below it (default 8). There is no switch to disable inbound connections, so inbound port
  18888 is closed in the firewall. The first seven days are measured, upload and download separately, before the
  monthly figure goes into the guard.

### 2.3 Data root, ports, users

`/srv/blockline-usdt`, owned by `usdt`, ACL for `debian`. Node HTTP 127.0.0.1:8090, solidity HTTP 127.0.0.1:8091,
dashboard 127.0.0.1:8288. Traffic is counted once for the whole host in `/srv/blockline-shared/bandwidth.json` (the BTC
guard's ledger, shared as for the forks). The USDT guard sheds load in a fixed order when USDT reaches its share of
the month: backfill download first, then offsite backup uploads, the node last (because stopping the node stops
watchlist alerts).

### 2.4 Ingestion

- **Live, final data**: poll the solidity API on loopback (port 8091), one solidified block at a time:
  `/walletsolidity/getblockbynum` (header, transactions, contract types, owners) and
  `/walletsolidity/gettransactioninfobyblocknum` (logs, internal transactions, energy, fees, result). Tier 1 and Tier 2
  are written only from solidified blocks. Each block is checked before it is written: the block ID matches, the
  transaction IDs in the info call match those in the block, and its parent hash is the previous written block's hash.
  Idempotent and resumable by block number. No Kafka, no event plugin; the java-tron event plugin (Kafka or MongoDB,
  still documented) is the fallback if polling cannot keep up (one block every 3 s).
- **Live, head**: the hot layer reads the head (port 8090) for watchlist alerts, about 19 blocks earlier than the
  solidified data. An alert on a block that is later dropped by a fork is retracted.
- **Backfill (block 0 to the snapshot height S)**: from the official FullNode snapshot **with internal transactions**
  (about 3.54 TB compressed, Sep 2026), restored on a temporary cloud VM (the company already runs GCP; cost in 10.3).
  The VM runs a peer-less FullNode from the snapshot and exports with tronetl or a gRPC reader into the Tier 2 schemas
  as parquet, plus a manifest (file, rows, block range, sha256). Only the parquet and manifest are shipped to the host.
  It is the only free source with every contract type and internal transactions.
  The same VM also supplies:
  1. the internal transactions of the 65,536 blocks in the lite snapshot (the live node never processed them);
  2. a frozen-height `balanceOf` sweep at S for every address that ever held USDT (section 5);
  3. the block headers 0..S for the continuity and parent-hash check across the backfill/live seam.
- **BigQuery** `bigquery-public-data.goog_blockchain_tron_mainnet_us` (preview product; addresses stored as lowercase
  hex) is the **second source**, not the backfill. It has no internal transactions, and its coverage of system
  contract types is doubtful (the docs say only "initiated by EOAs"; Phase 0 checks `INFORMATION_SCHEMA.COLUMNS`). It is
  used for per-block USDT aggregates over every block (section 5) and as a cross-check of USDT logs. Queries run in
  BigQuery; only the small aggregate tables are downloaded.
- **Fallbacks** if the snapshot route fails in Phase 0: SQD Portal on a paid plan (the free tiers forbid commercial
  use), or a quote from Nodit or Allium. TronGrid is not a backfill source: about 87.7M blocks × 2 calls against a free
  quota of about 100k requests a day (third-party figure; the official docs give none).
- Expected size: a few billion USDT transfers since 2019 (estimate; the cumulative count is a Phase 0 query). TRX
  side from genesis adds about 8.4M blocks before the contract existed.

## 3. Data model

### 3.1 Epochs and sealing

Epochs are defined by block number from block 0: epoch k holds blocks [k × 201,600, (k + 1) × 201,600). That is about
7 days, but not exactly, because of missed slots. A block is final when it is at or below the node's **solidified**
block (`/walletsolidity/getnowblock` on port 8091; 70% of 27 SRs = 19 blocks, typically about 1 minute, with no fixed
upper bound). An epoch is sealed when its last block is solidified plus a margin of 1,000 blocks. Sealed epochs are
immutable; `hot/` holds everything newer and is rebuilt from the node after any fork.

Owners: the backfill seals every epoch entirely below S, epoch 0 included. The **seam epoch** (the one containing S) is
sealed by the live sealer, and only once the backfill rows below S in that epoch are loaded and the parent-hash chain
links block S − 1 to block S.

### 3.2 Tier 1 (lossless for decoded fields)

**Live era only.** Per block: one header row (number, block ID, parent hash, timestamp, witness, transaction count), so
an empty block is provable. Per transaction: the transaction from `getblockbynum` and its `gettransactioninfobyblocknum`
entry, zstd-compressed (protobuf considered against JSON in Phase 0). **Kept** means every transaction: the internal
transactions need every `TriggerSmartContract`, which is most of the chain, so filtering by type saves almost nothing.
`raw_data_hex` is dropped, so Tier 1 is lossless for decoded fields, not byte-for-byte. Every Tier 2 table can be
rebuilt from it without the network.

Size, estimate to measure in Phase 0: about 11.8M transactions a day (Q2 2026 average, Messari) × about 2 KB of JSON is
about 24 GB a day raw, about 5 GB a day zstd, **about 1.85 TB a year**.

For the backfill the lossless layer is the extracted parquet plus the manifest's checksums of what was exported, not
Tier 1. The raw snapshot is not kept.

### 3.3 Tier 2

Value types, used everywhere below:

- **uint256 values that a caller controls** (approval amounts, failed-call amounts, `Params` values): the raw 32 bytes
  plus a decimal value. Postgres `numeric(78,0)`; parquet the 32 bytes and a decimal string, because neither
  `decimal(38,0)` nor DuckDB `UHUGEINT` (2^128) can hold them. `approve` accepts `MAX_UINT` = 2^256 − 1 (about
  1.16e77).
- **A single successful transfer**: fits uint64 (supply about 9e16 base units against 1.8e19). Parquet `uint64`,
  Postgres `bigint` with `CHECK (amount >= 0)` (Postgres has no uint64). The ETL fails loudly if a value ever exceeds
  2^63 − 1.
- **Sums of transfers** (per-address totals, edge values, entity totals, report figures): int128 / `decimal(38,0)`
  (DuckDB `HUGEINT`, Postgres `numeric(38,0)`).
- **Fractions** (haircut tracing shares): `numeric(38,18)` scaled decimal, or a rational pair where exactness matters.

Ordering: every row carries `block`, `tx_index` (position in the block) and, for logs, `log_index` **normalised to the
position within the transaction** (the node and BigQuery may scope it differently). Rows are keyed by
(block, tx_index, log_index).

| Table | Rows | Key columns |
|---|---|---|
| `block_headers` | every block from 0 | block, block_id, parent_hash, ts, witness, tx_count |
| `usdt_transfers` | every `Transfer` log of the USDT contract (successful transactions only: a reverted transaction emits no logs), including mint (`from` = 0x0) and burn (`to` = 0x0) | block, ts, tx_hash, tx_index, log_index, from, to, amount (uint64, 6 decimals), method (`transfer` / `transferFrom` / `issue` / `redeem` / other), caller (tx owner), kind (`transfer` / `fee` / `mint` / `burn`), spoofed (bool, 6) |
| `usdt_failed_calls` | failed transactions whose call targets the USDT contract directly, decoded from call data | block, tx_hash, tx_index, method, from/owner, to, amount (uint256), result, call_data_ok (bool) |
| `usdt_tx_costs` | per USDT transaction | call_value (amount in the call data, before any fee), energy_used, origin_energy_usage (part paid by the contract deployer), energy_from_stake, energy_fee (TRX burned), energy_penalty_total, net_fee, fee_limit, inferred energy source |
| `usdt_admin` | `AddedBlackList`, `RemovedBlackList`, `DestroyedBlackFunds`, `Issue`, `Redeem`, `Deprecate`, `Params`, `Pause`, `Unpause`, `OwnershipTransferred`; plus one row for the constructor's initial supply (no event) | block, tx_hash, tx_index, log_index, event, address (from the topic for blacklist and destroy events), amount (uint256) |
| `usdt_approvals` | `Approval` logs | owner, spender, amount (uint256) |
| `account_activations` | first appearance of an address: `AccountCreateContract` (type 0), or the TRX / TRC-10 transfer that created it, internal transfers included | address, activator, activator_is_contract, block, method |
| `trx_transfers` | three origins: `TransferContract`, TRX sent as `call_value` on `TriggerSmartContract`, internal TRX transfers | from, to, amount_sun, origin (`transfer` / `call_value` / `internal`) |
| `internal_transfers` | every internal transaction (`vm.saveInternalTx`) | tx_hash, caller, to, token (TRX / TRC-10 id), amount, rejected |
| `resource_delegations` | Stake 2.0 `DelegateResourceContract` / `UnDelegateResourceContract`; Stake 1.0 freeze and unfreeze with a receiver | delegator, receiver, resource, amount, lock |
| `permission_updates` | `AccountPermissionUpdateContract` | account, block, owner keys and threshold, active permissions (keys, weights, threshold, allowed operations) |
| `contracts` | addresses that are smart contracts | address, creator, block, consume_user_resource_percent, origin_energy_limit |

- **Success** comes from the receipt result only. The TRON USDT `transfer` has no return statement, so it returns
  false even when it succeeds; the return value is never used.
- **Fee transfers**: if `Params` ever sets a non-zero `basisPointsRate`, each transfer emits two `Transfer` logs: the
  amount after the fee to the recipient, and the fee to the owner (`kind = fee`). `usdt_tx_costs.call_value` keeps the
  amount the caller sent. The current `basisPointsRate`, `maximumFee` and `deprecated` flag are read in Phase 0.
- **Energy source**: the receipt names no delegator. USDT has `consume_user_resource_percent = 30` and
  `origin_energy_limit = 10M`, so the deployer pays part of the energy of every call; dynamic energy also applies to
  USDT (factor 4.4×, Nov 2026, in `energy_penalty_total`). The energy source in `usdt_tx_costs` is inferred from the
  `resource_delegations` state at that block and is labelled as inferred.
- **Failed calls** made through another contract are not recoverable from logs. An internal transaction with
  `rejected = true` shows only that a nested call happened, not its arguments. The call-data decoder tolerates short or
  malformed call data (`call_data_ok = false`, raw bytes kept) and fails loudly rather than truncating a value.
- **Deprecate**: an immediate alert (hot layer and dashboard). After it, calls may be forwarded to an upgraded
  contract that this pipeline does not watch.

Scope of the TRX-side tables: all accounts from block 0. Extraction happens on the cloud VM, so the scope costs VM
time, not host traffic beyond the parquet (measured in Phase 0).

### 3.4 Tier 3 (index)

Address numbering (`lib/ids.py`, one definition), per-address stats (first/last seen, in/out counts and value,
distinct counterparties, active days), daily balances, a counterparty edge table (address pair, count, value, first,
last), per-transfer features (round amounts, time to forward, share of balance forwarded), lookup indexes by address
and by tx hash. Every value column that is a sum is `decimal(38,0)`. Spoofed transfers (3.3, 6) are excluded from
edges, stats and features, and counted separately.

## 4. Parsing

### 4.1 Source of truth

Every parsing rule comes from the **deployed** TetherToken source verified on Tronscan, not from the Ethereum
contract or java-tron defaults. Phase 0 checks once that the verified source compiles to the deployed bytecode. The
line references below are to that verified source.

### 4.2 Events

The contract emits 12 events: `Transfer`, `Approval`, `Pause`, `Unpause`, `OwnershipTransferred`, `AddedBlackList`,
`RemovedBlackList`, `DestroyedBlackFunds`, `Issue`, `Redeem`, `Deprecate`, `Params`. `Transfer` is
`Transfer(address indexed from, address indexed to, uint256 value)`. On TRON the blacklist and destroy events index
the address (it is a topic, not data), unlike Ethereum. Topics carry EVM-style 20-byte addresses, stored as TRON
base58 (`41` prefix + checksum) and as 21-byte binary. Reverted transactions emit no logs, so they never reach
`usdt_transfers`. Direct calls to the USDT contract are decoded from call data into `usdt_failed_calls`.

### 4.3 Balance rebuild

- `issue` emits `Transfer(0x0, owner, amount)` and `redeem` emits `Transfer(owner, 0x0, amount)` (L938–955), so mints
  and burns are already in `usdt_transfers`. **`Issue` and `Redeem` are not applied** to balances; each is kept in
  `usdt_admin` and checked to match exactly one `Transfer` in the same transaction.
- `DestroyedBlackFunds` sets the address to 0 and lowers `totalSupply` without a `Transfer` (L957–962). It is applied.
- The constructor gives the initial supply to the owner with no event (L838–846). The rebuild seeds the owner with it,
  read from the constructor arguments of the creation transaction (Phase 0).
- So: balance = initial supply (owner only) + Σ incoming `Transfer` − Σ outgoing `Transfer` − destroyed amounts.

All parsing lives in `lib/tron.py`.

### 4.4 Fixtures before code

One fixture per edge case, from a real transaction where one exists, synthetic otherwise, each with its expected rows:
a mint, a redeem, a destroy, a zero-value `transferFrom` by a third party, a GasFree transfer, a failed call with a
`MAX_UINT` value, a failed call with short call data, an activation made by a contract, a fee transfer (synthetic
unless a real one exists), TRX sent as `call_value`, a Stake 1.0 delegation, a permission update with an operations
mask. The parser is not merged until every fixture passes.

## 5. Checks, classified by what they can prove

| Check | Proves |
|---|---|
| **Supply**: initial supply + Σ`Transfer` from 0x0 − Σ`Transfer` to 0x0 − Σ`DestroyedBlackFunds` = Σ rebuilt balances = `totalSupply()`; and every `Issue` / `Redeem` matches one mint / burn `Transfer` | mints, burns, destroys and the initial supply are handled correctly. **Only that**: transfers conserve the sum of balances, so a missing or doubled transfer passes this check |
| **No negative balance**: every address ≥ 0 after every block during the rebuild | catches missing credits or doubled debits wherever they make a balance negative |
| **Balances at a frozen height**: `balanceOf` at height S, on the cloud VM's peer-less node, for **every** address that ever held USDT (about 75M hold USDT now, Jun 2026; more have ever held it), against the rebuilt balance at S | per-address correctness at S; the completeness check for every balance |
| **Balances at the head**: daily 10,000-address sample (all large holders + random) through the local node. The head is recorded before and after each batch; any address with a transfer between the two is checked again | per-address correctness of the live era |
| **Per-block aggregates**: for every block from deployment, USDT `Transfer` count, sum of amounts and a hash of the sorted (from, to, amount) triples, computed in BigQuery and compared with ours | no transfer missing, doubled or altered in any block, against an independent source |
| **Continuity**: every block number from 0 to the head has a header row, and each header's parent hash is the previous block's ID, across the backfill/live seam included | no holes, no fork mixed in |
| **Fingerprints**: per epoch, a hash of each sealed Tier 2 table, over rows in key order | two builds, or two machines, agree |

A sealed epoch that fails continuity, the no-negative-balance check or the per-block aggregates does not enter Tier 3.
There is no gap list that excuses a hole: Phase 2 requires zero gaps.

## 6. Clustering: ut-v1

TRON is account-based: there is no co-spend, so the Bitcoin rules do not apply. The rules below are split into
**merging** edges and **evidence** that is reported but never merges. Every merging rule has a service exclusion,
because on TRON the dangerous case is a service that touches millions of users (exchanges, energy-rental markets,
activation services, GasFree, custodians).

### Spoofed transfers (removed first)

Anyone can call `transferFrom(victim, to, 0)` and emit `Transfer(victim, to, 0)` (L768–786), because a zero amount
passes the allowance check. This is used at scale for address poisoning on TRON. A transfer is marked
`spoofed` when it is a `transferFrom` with value 0 whose caller is not `from`. Spoofed transfers never create edges,
never count for rule 2 and are never followed by tracing. They are reported (count, addresses targeted) as evidence of
poisoning.

### Service detection (runs first)

An address is a **service** if it activates more than N distinct accounts, delegates resources to more than N, holds
a key in the permissions of more than N accounts (custody or co-signer), is a contract, activates accounts through
internal transfers (activation by a contract), is a GasFree provider or controller, or is labelled as an exchange. N is
set by ablation, starting at 50.

A third party can pay a user's fees in three ways, and none of them is ownership: resource delegation, the contract
deployer's energy share (for USDT, 70% up to 10M energy per call, paid by the deployer), and GasFree relayers (live
since Mar 2025; the provider is the transaction owner, so `caller` is the provider, not the user). "Caller" and
"delegates resources to" are therefore never merging evidence on their own.

**A service may receive joins, but may never connect two other entities.** No edge whose evidence passes through a
service (activated by it, delegated to by it, sharing a key it holds, relayed by it) merges the two accounts on either
side.

### Merging edges

1. **Shared key** (strongest): account A merges with key holder K only when K's key **alone** meets A's owner
   threshold, or meets the threshold of an active permission that allows transfer operations; and only for the block
   range in which that key was in place. Earlier history of A does not join K's entity. A key limited to other
   operations (for example delegation only) is evidence, not control. A key held by more than N accounts makes the
   holder a service and does not merge them. A permission change shortly before an outflow that drains the account is
   evidence of a hijack, never a merge.
2. **Deposit sweep**: address D receives USDT from several unrelated senders and forwards almost its whole balance,
   repeatedly and within a short delay, to the same hot wallet H, with no other outflows. D joins H's entity, even
   when H is a service. **D is then a leaf**: it is frozen from further merges, so no later edge on D can drag another
   entity into the exchange. Adapted from Victor (FC 2020), which publishes no precision against ground truth and no
   TRON adaptation; rule 2 must meet the precision bar below in the first ablation to stay merging.

Union-find with a merge forest, as in cl-v1. Edges are processed in a fixed order (rule, block, tx_index, log_index), so
two builds refuse the same joins. The **circuit breaker** refuses a join when the smaller side has more than L
addresses or the merged entity would exceed L2 (both set before the first run, then by ablation) and lists it for
review.

### Evidence (reported, never merging)

Small-scale activation (a non-service activator that activates account A and then funds it or receives from it);
small-scale resource funding (a non-service delegates energy or bandwidth to A more than once: energy lenders on rental
platforms look like this); permission changes before draining outflows; keys below the threshold or with limited
operations; `transferFrom` spenders; amount-and-time matches through an intermediary (peel chains, pass-through); round
trips; time-of-day profiles; shared counterparties; address-poisoning targets.

### Promotion bar

A rule moves from evidence to merging (or stays merging, for rules 1 and 2) only if its pairwise precision against
ground truth is at least **95%** in the ablation (bar set before the first ablation, owner to confirm in decision 10).

### Reported with every run (mandatory)

Entity size distribution; merges per rule (count and USDT value); the largest 100 entities with their dominant joins;
joins refused by the circuit breaker; ablation (each rule switched off in turn, each evidence rule switched on: merges
lost or gained, pairwise precision and recall before and after); entity quality per entity (shape: exchange-like,
personal, contract-bound, suspected collapse); spoofed-transfer counts.

## 7. Labels and ground truth

- **On chain, authoritative**: Tether blacklist and frozen-funds events (`usdt_admin`).
- **Sanctions**: OFAC `sdn_advanced.xml` (public domain; 345 unique TRON addresses, 266 TRX + 79 USDT, at 2026-10-29).
  OpenSanctions is CC BY-NC, so a commercial company needs a paid licence (decision 7); not used until then.
- **Exchanges**: published hot wallets with citations. **No TronScan tags**: its terms ban scrapers and reuse without
  written permission (decision 7).
- **Ground truth**: `data/groundtruth/*.csv` (`published` rows with citation), used only to score the clustering,
  never as merge input.

## 8. Investigation layer

- **Tracing**: forward and backward from an address or transfer, with selectable methods (all paths, FIFO, haircut),
  stopping at services and listing the service reached. Spoofed transfers are never followed. Haircut shares are
  scaled decimals (3.3).
- **Wallet tracking**: watchlists in Postgres; the hot layer checks every new head block against them and records an
  alert (address, transfer, counterparty entity, label) within one block plus the poll interval. An alert whose block
  is dropped by a fork is marked retracted.
- **Exposure**: per address or entity, the share of inflow and outflow from labelled categories (sanctioned,
  blacklisted, exchange, unknown), direct and up to K hops.
- **Delivery**: parquet for analysis; Postgres schema `usdt` (transfers, entities, labels, alerts) for the platform,
  with the types of 3.3 (`bigint` + check for single transfers, `numeric(38,0)` for sums, `numeric(78,0)` for uint256
  values). An API comes after v1.

## 9. Operations

Same pattern as blockline-btc: `bin/blockline install`, `bin/blockline status`, one runner that starts each step as
soon as its input exists, a commit point per step, guards, dashboard on loopback. No agent runs on the server.

- **Lag alert**: watchlist alerts are only as fresh as the hot layer, so it fires when the hot layer is more than about
  2 minutes behind the node's head (setting). A separate alert fires when the solidified ETL falls an hour behind. A
  lagging ETL loses no data, because the node keeps every block until it is pruned.
- **RAM guard**: node 24 GB + Postgres 32–64 GB + clustering 64–128 GB is 120–216 GB against about 170 GB free, so the
  guard runs clustering and bulk Postgres loads one at a time, never together.
- **Disk guard**: at the ceiling (decision 6) the backfill load throttles first, then an early prune is scheduled for
  the next window.
- **Offsite backups**: live-era internal transactions exist only on this host; a re-bootstrap from a fresh snapshot
  would leave a permanent hole. So sealed Tier 1 epochs, sealed Tier 2 epochs and the backfill parquet (with manifest)
  are copied offsite after each seal (target and cost: decision 8). The last pre-prune backup is kept as a restore
  point. Uploads count against the traffic ledger if the host cap counts both directions (Phase 0).
- The prune script (2.2) runs from the same runner.

## 10. Capacity, traffic and money

### 10.1 Measured on this server (2026-11-06)

32 vCPU x86_64, 251 GB RAM (about 170 GB free with blockline-btc and wallex running), 5.5 TB free disk (shared with BTC
and the forks). The disk performs as SSD: 441 MB/s write and 766 MB/s read sequential, about 22,000 random 4K reads/s
and 15,000 random 4K writes/s with 16 parallel requests, under the current load. Traffic used this month: 38.8 of
1,000 GB (BTC is synced and uses about 40 GB a month). Re-measured at the start of Phase 0.

### 10.2 Disk, first year

All figures are estimates on the Phase 0 measurement list.

| Item | Disk |
|---|---|
| Lite node at start | about 0.1 TB |
| Lite node growth between prunes | FullNode rate × days between prunes (rate to measure) |
| Prune scratch (transient, during `db lite`) | about 2× the node database: 0.2–0.4 TB |
| Tier 1, live era | about 1.85 TB/year |
| Backfill parquet + manifest | below the 0.4–2 TB of a raw export (to measure) |
| Tier 2 + Tier 3, TRX side from genesis | 0.3–0.5 TB |
| Postgres `usdt` | 0.6–1 TB |
| Clustering working space | 0.1 TB |
| **Sum, first-year peak** | **about 3.55–5.95 TB plus node growth** |
| Budget in revision 2 | 3 TB |

**The plan does not fit 3 TB.** Tier 1 alone is about 1.85 TB a year. Options for decision 6: raise the USDT budget to
about 6 TB (it then needs more disk than the 5.5 TB free shared with BTC and the forks), or keep only the last N months
of Tier 1 on the host and the rest offsite (restore on demand), which brings the first-year peak to about 2–4 TB.

### 10.3 Traffic

| | Month 1 | Steady month |
|---|---|---|
| BTC (measured) | 40 GB | 40 GB |
| Lite snapshot download | 100–200 GB | — |
| Lite node (upload + download, to measure) | 150–300 GB | 150–300 GB |
| Backfill parquet download | 133–1,000 GB (if spread over 2–3 months) | 0 after it ends |
| Offsite backup upload (if counted) | backfill parquet + Tier 1 | about 150 GB (Tier 1 growth) + Tier 2 |
| **Sum** | **0.42–1.54 TB plus backups** | **0.34–0.49 TB plus Tier 2 backups** |
| Host cap | 1 TB | 1 TB |

Month 1 can exceed the cap at the high end. The guard sheds in the order of 2.3 (backfill, backups, node last), so
the backfill stretches over more months rather than stopping the node. Extraction off-host keeps the download to
parquet, not raw history. Whether the 1 TB cap counts both directions is checked with the provider in Phase 0.

### 10.4 Money

| Option | Cost |
|---|---|
| Cloud VM for the snapshot route: VM + disk for the 3.54 TB archive and the restored database, for the weeks of extraction | to price in Phase 0 (the largest item) |
| GCP egress of parquet to the host | $0.08–0.12 per GiB, so about $30–250 for 0.4–2 TB |
| BigQuery per-block aggregates and cross-checks | first 1 TiB a month free, then $6.25 per TiB scanned |
| SQD Portal (fallback) | Starter $99/month, Growth $499/month, TRON +$300/month |
| Nodit / Allium (fallback) | quote |
| Offsite backup storage | to price with decision 8 |
| OpenSanctions commercial licence | quote, if decision 7 says yes |

## 11. Phases

| Phase | Work | Done when |
|---|---|---|
| **0. Spike** (1–2 weeks) | Lite node from snapshot with history queries and internal transactions on; open items and measurements below; live ETL prototype on the solidity API; snapshot-route trial on a cloud VM (restore, export one epoch, ship it); fixtures (4.4) | every item below recorded, and the exit criteria below met or the plan revised |
| **1. Live** | `node/` (including the prune script), `etl live`, Tier 1/2 for solidified blocks, sealing, hot layer, guards, offsite backup, status, dashboard | live data from the snapshot height on, no gaps and parent hashes linked for 14 days |
| **2. Backfill** | cloud extraction from block 0 to S, load and manifest checks, section 5 checks | frozen-height `balanceOf` match at S for every address that ever held USDT; no negative balance at any block; per-block aggregates match BigQuery for every block; supply reconciles; zero gaps and parent-hash linkage from block 0 across the seam |
| **3. Index** | Tier 3 | stats and balances match the node for the sample |
| **4. Clustering** | ut-v1, report, quality, ablation | report reviewed; every merging rule meets the precision bar; no unexplained join over the breaker limits |
| **5. Labels** | label store, ground truth, scoring | ground-truth precision and recall published in the report |
| **6. Investigation** | tracing, watchlists and alerts, exposure, Postgres `usdt` | an analyst can investigate a hash, track a wallet and see its entity |

Phase 1 starts as soon as the node is up, even before the cloud extraction runs, so that the live era begins as early
as possible and internal transactions are recorded locally from the first block processed.

### Phase 0: open items (one call or one query each)

- [ ] The Tronscan-verified source matches the deployed bytecode.
- [ ] Initial supply in the constructor arguments of the creation transaction.
- [ ] Current `basisPointsRate`, `maximumFee` and `deprecated` flag.
- [ ] Token flow of one GasFree transaction: `transferFrom` or a wallet-contract transfer, and what `from` shows.
- [ ] `getblockbynum` and `gettransactioninfobyblocknum` answer with `openHistoryQueryWhenLiteFN`, on 8090 and 8091.
- [ ] `getnodeinfo` exposes the solidified block number.
- [ ] BigQuery `transactions` columns for system-contract parameters (`INFORMATION_SCHEMA.COLUMNS`), and the
      `log_index` scope in BigQuery `logs`.
- [ ] Number of addresses that ever held USDT, and cumulative transfers since 2019 (BigQuery).
- [ ] Host figures of 10.1, and whether the 1 TB cap counts both directions.

### Phase 0: measurements

- Lite snapshot size on disk; lite node disk growth per day; prune scratch space and prune duration.
- Lite node traffic per month, upload and download separately.
- Solidified-block lag behind head.
- Tier 1 bytes per day, JSON against protobuf.
- Snapshot route: restore time, export time and parquet bytes per epoch, VM cost per day.
- Frozen-height `balanceOf` sweep rate on the VM.
- Disk, traffic and money figures of 10.2–10.4, re-summed.

### Phase 0: exit criteria (proposed, owner to confirm)

Go to Phase 1 only if all hold:

- lite node traffic ≤ 300 GB/month (upload + download), measured over 7 days and scaled;
- lite node disk growth ≤ the rate that keeps node + Tier 1 + Tier 2 within the disk ceiling of decision 6 with the
  chosen prune cadence (the number is set with decision 6);
- solidified ETL keeps up: ≥ 2 blocks/s sustained catch-up rate (about 6× the chain);
- the snapshot-route trial exported one full epoch with internal transactions and the shipped parquet passed its
  manifest;
- every fixture of 4.4 passes;
- the re-summed disk, traffic and money budgets fit the caps the owner set.

## 12. Decisions needed from the owner

1. **Backfill route and money**: the official snapshot with internal transactions on a temporary GCP VM
   (recommended: free data, full coverage; cost is the VM and disk for some weeks plus egress), or SQD Portal on a
   paid plan ($99–$799/month), or a Nodit / Allium quote. BigQuery stays as the second source in every case.
2. **TRX context scope**: all accounts from block 0 (recommended now that extraction is off-host) or only accounts that
   touched USDT.
3. **Traffic share**: how much of the 1 TB/month USDT may use in month 1 and after, given the forks schedule. The
   shed order (backfill, backups, node) is fixed.
4. **Bootstrap**: create the `usdt` user and `/srv/blockline-usdt` (one sudo script, as for the forks).
5. **Platform**: whether `usdt` lives in the wallex Postgres or its own database.
6. **Disk budget and ceiling**: raise the USDT budget to about 6 TB, or keep the last N months of Tier 1 on the host
   and the rest offsite (10.2); the ceiling at which the backfill throttles and an early prune is scheduled.
7. **Labels licences**: buy an OpenSanctions commercial licence or use OFAC only; ask TronScan for written permission
   for tags or do without them.
8. **Offsite backups**: target (for example a GCP bucket) and budget for storage and upload.
9. **Phase 0 exit criteria**: confirm or change the numbers in section 11.
10. **Clustering precision bar**: 95% pairwise precision against ground truth, or another figure.

## Changes from revision 2

All from the verification review of 2026-11-07, which checked 34 claims against primary sources (16 held, 9 partly,
7 wrong, 1 unverified, 1 not checkable without the server). Revision 1's changes are in git history (`dadf144`).

Critical:

1. Balance rebuild no longer applies `Issue` / `Redeem`: the deployed TRON contract emits `Transfer(0x0 → owner)` on
   issue and `Transfer(owner → 0x0)` on redeem, so applying both counted every mint twice. The rebuild applies
   `Transfer`, `DestroyedBlackFunds` and the constructor's initial supply (no event); `Issue` / `Redeem` are a
   cross-check (4.3). Supply check rewritten with the initial supply (5).
2. uint256 values (approvals up to `MAX_UINT`, failed-call amounts, `Params`) stored as raw 32 bytes plus
   `numeric(78,0)`; single transfers as `bigint` with a check; sums stay `decimal(38,0)`; haircut shares as scaled
   decimals (3.3, 3.4, 8). Decoder tolerates bad call data and fails loudly instead of truncating.
3. Disk budget re-summed with the prune scratch space, node growth and Tier 1 at 11.8M tx/day (1.85 TB/year): first-year
   peak about 3.55–5.95 TB against the 3 TB budget; options in decision 6 (10.2). Disk guard throttles the backfill
   first (9).
4. Traffic re-summed: month 1 is 0.42–1.54 TB against the 1 TB cap; fixed shed order backfill → backups → node (2.3,
   10.3).

High:

5. Backfill from the official FullNode snapshot with internal transactions on a temporary cloud VM, shipping parquet
   only; BigQuery becomes the second source (no internal transactions, doubtful system-contract coverage); SQD free
   tiers ban commercial use, so SQD is a paid fallback (2.4, 12).
6. TRX-side tables from genesis (block 0), not 2019-04 (1, 2.4, 3.3).
7. Tier 1 gets one header row per block, "kept" is every transaction, and it is called lossless for decoded fields
   (3.2); `block_headers` added (3.3).
8. Tier 1/2 written only from solidified blocks through the solidity API (port 8091), with block ID, tx ID and
   parent-hash checks; the hot layer reads the head and retracts alerts after a fork (2.4, 8).
9. Full `balanceOf` check moved to a frozen height S on the VM's peer-less node; head samples recheck addresses that
   moved during a batch (5).
10. Cross-check week replaced by per-block count, sum and triple hash from BigQuery for every block (5, 11).
11. Zero-value third-party `transferFrom` marked spoofed and excluded from edges, rule 2, Tier 3 and tracing (3.3, 3.4,
    6, 8).
12. Clustering: resource funding (old rule 4) is evidence; rule 1 needs one key alone meeting the owner threshold or a
    transfer-capable active threshold, and only for the period the key was in place; deposit addresses are frozen
    leaves; GasFree providers and controllers are services; fee-payer paths corrected (deployer energy share, GasFree)
    (6).
13. Offsite backups of sealed Tier 1, sealed Tier 2 and the backfill parquet; last pre-prune backup kept (9, 12).

Medium and low:

14. RAM guard runs clustering and bulk Postgres loads one at a time (9).
15. Lag alert at about 2 minutes behind the head, plus a separate solidified-ETL alert (9).
16. Money added as a budget axis and beside each option in decision 1 (10.4, 12).
17. `trx_transfers` gets the `call_value` origin (3.3).
18. `tx_index` added; `log_index` normalised to per-transaction (3.3).
19. Success from the receipt only, since `transfer` returns false even on success (3.3).
20. Fee transfers: `kind = fee`, rows keyed by `log_index`, `call_value` in `usdt_tx_costs` (3.3).
21. Continuity requires zero gaps and parent-hash linkage across the seam; `gaps.parquet` removed (5, 11).
22. Circuit breaker defined (min side > L or merged size > L2) with a fixed edge order (6).
23. Prune point computed after the node exits; Tier 1/2 must be sealed, synced and backed up; the window (about 2.2
    days in 7) and the alert pause are stated (2.2).
24. Epochs defined by block number from block 0; the backfill owns epoch 0, the live sealer owns the seam epoch (3.1).
25. Postgres `bigint` with `CHECK ≥ 0` for single transfers (3.3).
26. Promotion bar: 95% pairwise precision, proposed (6, 12).

Factual corrections:

27. `AccountCreateContract` (type 0), not `CreateAccountContract` (3.3).
28. `/walletsolidity/*` is on the solidity port 8091, not 8090 (2.3, 2.4, 3.1).
29. TronScan tags dropped (terms ban reuse without written permission); OpenSanctions needs a paid licence; OFAC
    `sdn_advanced.xml` is the sanctions source (7, 12).
30. Lite node at start about 100 GB (about 3% of a FullNode), not 200–300 GB (2.2, 10.2).
31. FullNode snapshot sizes: 3.31–3.35 TB, 3.54 TB with internal transactions, 4.38 TB with balance history (none is
    an archive) (1, 2.4).
32. 11.8M tx/day (Q2 2026), not 7M (3.2).
33. Block counts: head about 87.7M; about 79M from USDT deployment, 87.7M from genesis (2.4).
34. Pruning cadence: the docs say "periodically", so "monthly" is removed; the procedure (split a new snapshot and swap
    it in) is described (2.2).
35. "Node exits on a JDK mismatch" removed (not found in the docs); ARM64 needs RocksDB (2.2).
36. `maxConnections` default 30, `minConnections` 8; no switch to disable inbound, so port 18888 is firewalled (2.2).
37. TronGrid quota marked as a third-party figure (2.4).
38. `energy_penalty_total` added; USDT dynamic energy factor 4.4× (3.3).
39. Blacklist and destroy events index the address as a topic; the full list of 12 events (4.2); `Unpause` and
    `OwnershipTransferred` added to `usdt_admin` (3.3).
40. Stake 1.0 delegations and contract energy settings added (3.3); TRC-20 activation question closed (a TRC-20
    transfer does not activate an account).

Process, from the review:

41. Parsing rules come from the deployed verified source, checked against the bytecode once (4.1).
42. Fixtures for every edge case before code (4.4).
43. Every resource (disk, traffic, RAM, money) is summed against its cap, one-off and scratch space included, and
    re-summed each revision (9, 10).
44. Phase 0 exit criteria with numbers (11).
45. Each revision gets an independent verification pass against primary sources.
