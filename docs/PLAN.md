# blockline-usdt: Implementation Plan

Date: 2026-11-07. Status: **revision 4, draft for review**. Sibling of `/home/debian/blockline-btc` (live, read-only
for this work) and `/home/zcash/blockline-zec`; same layout, runner, guards and verification habits. Revision 4 applies
the owner's decisions of 2026-11-07 on revision 3: **extract once from a full node on this server, then go lite**
(changelog at the end).

## 1. Goals and scope

### Goal

Every USDT movement on TRON (USDT-TRC20, contract `TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t`, deployed 2019-04-16 at about
block 8.4M, estimate), extracted from a node on this server, checked, and enriched with enough TRON account context
**from genesis (2018-06)** to support:

1. **Transaction investigation**: any transfer by hash, its fee payer, energy source, caller and position in a flow.
2. **Wallet tracking**: per-address history, counterparties, balances at any block, watchlists with alerts on new
   activity within about a minute.
3. **Address clustering**: entities built from TRON-specific evidence (section 6), with a report on every run.

### v1 (Phases 0 to 7)

USDT-TRC20 transfers, failed USDT calls and Tether admin events from contract deployment, plus the TRX-side context
the clustering needs from block 0, for all accounts: account activations, TRX transfers (including TRX sent with
contract calls and internal transfers made by contracts), resource delegation (Stake 1.0 and 2.0), account permission
(multisig) changes and contract creations. Delivered as parquet under the data root and a Postgres database for the
platform.

### Later

USDC-TRC20 and other TRC-20 tokens (the same ETL with a contract list; history needs a new snapshot download, 3.3),
USDT on Ethereum (needs its own node: separate plan), cross-chain flows through bridges and exchanges.

### Non-goals

Keeping a FullNode after the extraction, contract state other than USDT balances, TRC-10 tokens, serving other TRON
nodes, paid data sources (BigQuery, SQD, commercial exports, TronGrid).

## 2. Architecture

### 2.1 How the data gets here

1. **Download once**: the official FullNode snapshot with internal transactions (about 3.54 TB compressed, Sep 2026),
   streamed straight into the node directory (2.3). Free data; the cost is one-off traffic (10.3).
2. **Full node, temporarily**: java-tron starts on the extracted database and catches up to the head (about 1.7M blocks
   from 18 Sep, estimate). The ETL reads every block from 0 to the head from this local node.
3. **Extract once**: Tier 2 for every block (3.3), checked (section 5).
4. **Go lite**: once the extraction is complete and checked, the node is converted to a Lite FullNode with
   `Toolkit.jar db lite`, freeing about 3.5 TB. The ETL keeps following the head from the lite node.

From then on, the extracted tables are the history. Anything not extracted (a new token, a new field) needs a new
snapshot download of 3.5 TB or more. That is the accepted cost of this route.

### 2.2 Repo and layout

`/home/debian/projects/blockline-usdt` during the draft, then `/home/usdt/blockline-usdt` owned by a `usdt` Linux user
(created by the sudo bootstrap, decision 4). Data root `/srv/blockline-usdt`.

| Folder | Does | Output (under `data_root`) |
|---|---|---|
| `node/` | java-tron settings (full, then lite; history queries and internal transactions on), systemd unit, loopback-only HTTP and solidity APIs, peer limits, firewall rule for port 18888, the snapshot download (2.3), the go-lite conversion and the prune script (2.4) | `node/` |
| `etl/` | One ETL: reads solidified blocks from the local node in block order, from 0 to the head and then live. Writes Tier 2 | `tier2` |
| `index/` | Address numbering, per-address stats, balances by block, counterparty edges, features | `foundation/tier3` |
| `cluster/` | ut-v1: evidence edges, service detection, union-find with merge forest, report, quality, ablation | `clustering/ut-v1` |
| `labels/` | Tether blacklist (from chain), OFAC TRON addresses, exchange hot wallets, ground truth | `labels/` |
| `hot/` | Unsolidified and unsealed recent blocks, watchlists, alerts | `hot/` |
| `pipeline/` | Runner, RAM guard, disk guard, traffic guard (reads the host ledger), offsite backup, status | `status.json`, `logs/` |
| `db/` | The `usdt` Postgres instance: config, memory limit, roles (read-only role for wallex) | `postgres/` |
| `dashboard/` | One page on 127.0.0.1 (port 8288), opened through an SSH tunnel | |
| `install/` | Staged, idempotent setup: JDK 8 (Temurin 8 tarball, no root), java-tron, services | |
| `verify/` | Balance sweep, supply reconciliation, no-negative, continuity, fingerprints (section 5) | |
| `fixtures/` | One real (or synthetic) case per edge case, written before the parser (section 4.4) | |
| `bin/` | `blockline`: install, status, test | |

### 2.3 Snapshot download

- One connection only: the mirror warns that parallel or segmented downloads get 404s, and its page does not mention
  resuming. A broken download restarts from zero.
- Stream into the extractor, computing the checksum on the way, because storing the archive and then extracting it
  needs about 7 TB:

  ```
  wget -q -O - http://35.247.128.170/<path>/FullNode_output-directory.tgz \
    | tee >(md5sum > got.md5) | tar -xzf - -C /srv/blockline-usdt/node
  ```

- The published `.md5sum` file is fetched **before** the download starts and again after it ends. If the two differ,
  the mirror replaced the file mid-download, and the result is discarded. If `got.md5` does not match, everything is
  deleted and downloaded again. The script refuses to start the node until the checksum matches.
- First test once the node starts: the earliest block with internal transactions (2.4). The snapshot page does not
  say where they begin.

### 2.4 Node

**Full phase** (from download to go-lite):

- JDK 8 on x86_64 (Temurin 8 tarball, no root) with the LevelDB snapshot. Heap 12 GB, RAM cap 24 GB for the unit
  (revisited after the catch-up measurement).
- `vm.saveInternalTx = true` and `vm.saveFeaturedInternalTx = true` from the first start, so blocks processed locally
  during the catch-up keep their internal transactions.
- History queries are on by default on a FullNode; Phase 0 confirms that `getblockbynum` and
  `gettransactioninfobyblocknum` answer for a 2018 block and a 2019 block, on 8090 and 8091.
- **Earliest block with internal transactions** is recorded. If they start after block 0, the blocks before that
  have none locally. Producing them would mean replaying the chain from genesis, a much larger job. The gap is then
  stated in every report and the owner decides whether to replay (decision 11).
- **No pruning** while the node is the history source.

**Go-lite** (once, Phase 3): stop the node, confirm it has exited, run `Toolkit.jar db lite` to split a lite
database out of the full one, swap it in, restart with `node.openHistoryQueryWhenLiteFN = true`, delete the full
database. Allowed only when the gate in Phase 3 holds. This is irreversible without a new download.

**Lite phase** (from then on):

- The lite node holds the current state plus the latest 65,536 blocks, then grows at the FullNode rate, about 1 TB a
  year (estimate, measured during the full phase). It is pruned offline with the same `db lite` procedure. Scratch
  space during a prune is about 2× the node database. Watchlist alerts pause while the node is down.
- **Pruning rule**: the prune script computes the prune point (head − 65,536) **after** the node has exited, and
  refuses unless every block below it is in a sealed epoch whose Tier 2 files are written, synced to disk and backed
  up offsite (9). The window opens after each epoch seal and lasts about 2.2 days in every 7; the runner schedules the
  prune on the day after a seal. Cadence follows the measured growth and the disk thresholds (10.2).

**Both phases**: HTTP (8090) and solidity (8091) APIs on 127.0.0.1 only; no gRPC exposed. `node.maxConnections = 10`
(default 30), `node.minConnections` below it (default 8). There is no switch to disable inbound connections, so inbound
port 18888 is closed in the firewall. Node traffic is measured over 7 days, upload and download separately.

### 2.5 Data root, ports, users

`/srv/blockline-usdt`, owned by `usdt`, ACL for `debian`. If a dedicated disk is added later, the bootstrap mounts it
there before creating the user, folders and rights. Node HTTP 127.0.0.1:8090, solidity HTTP 127.0.0.1:8091, dashboard
127.0.0.1:8288, Postgres on a Unix socket and 127.0.0.1 (port set at install). Traffic is counted once for the whole
host in `/srv/blockline-shared/bandwidth.json` (the BTC guard's ledger, shared as for the forks). When USDT reaches its
share of the month, the guard stops offsite backup uploads first and the node last (because stopping the node stops
watchlist alerts). The ETL itself uses no traffic.

### 2.6 Ingestion

- **One ETL, block order**: poll the solidity API on loopback (port 8091), one solidified block at a time:
  `/walletsolidity/getblockbynum` (header, transactions, contract types, owners) and
  `/walletsolidity/gettransactioninfobyblocknum` (logs, internal transactions, energy, fees, result). It starts at
  block 0, and when it reaches the solidified head it simply keeps following it. There is no separate backfill and no
  seam. Several workers may read disjoint epochs in parallel during the history run, while one follows the head. Each
  epoch is sealed on its own once complete; the rebuild and Tier 3 run in block order and wait until every epoch below
  them is sealed.
- Each block is checked before it is written: the block ID matches, the transaction IDs in the info call match those
  in the block, and its parent hash is the previous block's ID. Idempotent and resumable by block number. The java-tron
  event plugin (Kafka or MongoDB) is the fallback if polling cannot keep up with the head.
- **Speed target**: the full extraction must project to 21 days or less (Phase 0 gate). About 87.7M blocks in 21 days is
  about 48 blocks/s sustained, about 100 local HTTP calls/s, while the node is also catching up. This is measured, not
  assumed.
- **Head**: the hot layer reads the head (port 8090) for watchlist alerts, about 19 blocks earlier than the solidified
  data. An alert on a block that is later dropped by a fork is retracted.

## 3. Data model

### 3.1 Epochs and sealing

Epochs are defined by block number from block 0: epoch k holds blocks [k × 201,600, (k + 1) × 201,600). That is about
7 days, but not exactly, because of missed slots. A block is final when it is at or below the node's **solidified**
block (`/walletsolidity/getnowblock` on port 8091; 70% of 27 SRs = 19 blocks, typically about 1 minute, with no fixed
upper bound). An epoch is sealed by the ETL when its last block is solidified plus a margin of 1,000 blocks. Sealed
epochs are immutable; `hot/` holds everything newer and is rebuilt from the node after any fork.

### 3.2 No separate lossless copy

Revision 3's Tier 1 (a copy of every live transaction) is dropped, saving about 1.85 TB a year. Tier 2, with the block
header table, is the record. If a Tier 2 table has to be rebuilt, or a field added, the source is a new snapshot
download (2.1). Backups (9) protect Tier 2 itself.

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

Ordering: every row carries `block`, `tx_index` (position in the block) and, for logs, `log_index` normalised to the
position within the transaction. Rows are keyed by (block, tx_index, log_index).

Because the history is extracted only once, the field lists below are reviewed against the investigation and
clustering needs before the history run starts (Phase 0), and include every field of the transaction and receipt that
a later feature might plausibly need for these contract types.

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
| `internal_transfers` | every internal transaction | tx_hash, caller, to, token (TRX / TRC-10 id), amount, rejected |
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

### 3.4 Tier 3 (index)

Address numbering (`lib/ids.py`, one definition), per-address stats (first/last seen, in/out counts and value,
distinct counterparties, active days), daily balances, a counterparty edge table (address pair, count, value, first,
last), per-transfer features (round amounts, time to forward, share of balance forwarded), lookup indexes by address
and by tx hash. Every value column that is a sum is `decimal(38,0)`. Spoofed transfers (6) are excluded from edges,
stats and features, and counted separately.

## 4. Parsing

### 4.1 Source of truth

Every parsing rule comes from the **deployed** TetherToken source verified on Tronscan, not from the Ethereum
contract or java-tron defaults. Phase 0 checks once that the verified source compiles to the deployed bytecode (read
from the local node). The line references below are to that verified source.

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
mask. Real fixtures are taken from the local node once it is up. The parser is not merged until every fixture passes.

## 5. Checks, classified by what they can prove

There is no second data source. The node is the chain itself, so the checks below target the real risk: bugs in the
ETL and the rebuild.

| Check | Proves |
|---|---|
| **Supply**: initial supply + Σ`Transfer` from 0x0 − Σ`Transfer` to 0x0 − Σ`DestroyedBlackFunds` = Σ rebuilt balances = `totalSupply()`; and every `Issue` / `Redeem` matches one mint / burn `Transfer` | mints, burns, destroys and the initial supply are handled correctly. **Only that**: transfers conserve the sum of balances, so a missing or doubled transfer passes this check |
| **No negative balance**: every address ≥ 0 after every block during the rebuild | catches missing credits or doubled debits wherever they make a balance negative |
| **Full `balanceOf` sweep** for every address that ever held USDT (about 75M hold USDT now, Jun 2026; more have ever held it), against the rebuilt balance, run once before go-lite at a **frozen height**: outbound port 18888 is blocked so the node stops syncing, the height H is recorded, the sweep runs, the port is reopened | per-address correctness at H for every balance; the main completeness check |
| **Daily head sample**: 10,000 addresses (all large holders + random) through the local node. The head is recorded before and after each batch; any address with a transfer between the two is checked again | per-address correctness of the live era |
| **Continuity**: every block number from 0 to the head has a header row, and each header's parent hash is the previous block's ID | no holes, no fork mixed in |
| **Fingerprints**: per epoch, a hash of each sealed Tier 2 table, over rows in key order | two builds, or two machines, agree |
| **Manual spot checks** (optional): a few blocks compared by hand against Tronscan's website (viewing only; its terms ban scraping) | a sanity check that the decoded rows look like the explorer |

A sealed epoch that fails continuity or the no-negative-balance check does not enter Tier 3. Phase 2 requires zero
gaps.

## 6. Clustering: ut-v1

TRON is account-based: there is no co-spend, so the Bitcoin rules do not apply. The rules below are split into
**merging** edges and **evidence** that is reported but never merges. Every merging rule has a service exclusion,
because on TRON the dangerous case is a service that touches millions of users (exchanges, energy-rental markets,
activation services, GasFree, custodians).

### Spoofed transfers (removed first)

Anyone can call `transferFrom(victim, to, 0)` and emit `Transfer(victim, to, 0)` (L768–786), because a zero amount
passes the allowance check. This is used at scale for address poisoning on TRON. A transfer is marked `spoofed` when it
is a `transferFrom` with value 0 whose caller is not `from`. Spoofed transfers never create edges, never count for
rule 2 and are never followed by tracing. They are reported (count, addresses targeted) as evidence of poisoning.

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
   TRON adaptation. The same detection feeds the exchange labels (7).

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

A rule merges (rules 1 and 2) or moves from evidence to merging only if, in the ablation:

- its pairwise precision against ground truth is at least **95%**;
- it is scored on **at least 200 known address pairs**, held out of any tuning (of N, L, L2 or the rule's own
  thresholds);
- the **100 largest entities** it produces have been reviewed by hand and none is an unexplained collapse.

Until a rule meets all three, it runs as evidence. With only OFAC and public exchange wallets as known examples,
building 200 held-out pairs per rule takes real time (7), so the first clustering run may be evidence-only.

### Reported with every run (mandatory)

Entity size distribution; merges per rule (count and USDT value); the largest 100 entities with their dominant joins
and review status; joins refused by the circuit breaker; ablation (each rule switched off in turn, each evidence rule
switched on: merges lost or gained, pairwise precision and recall before and after, number of held-out pairs);
entity quality per entity (shape: exchange-like, personal, contract-bound, suspected collapse); spoofed-transfer
counts; the internal-transaction gap, if the earliest block with internal transactions is after block 0 (2.4).

## 7. Labels and ground truth

- **On chain, authoritative**: Tether blacklist and frozen-funds events (`usdt_admin`).
- **Sanctions**: OFAC only, parsed from OFAC's own `sdn_advanced.xml` (public domain, free; 345 unique TRON addresses,
  266 TRX + 79 USDT, at 2026-10-29).
- **Exchanges**: published hot-wallet lists with citations, plus our own deposit-address detection (rule 2).
- **Not used until a customer needs them**: OpenSanctions (CC BY-NC, commercial licence), TronScan tags (terms ban
  reuse without written permission).
- **Ground truth**: `data/groundtruth/*.csv` (`published` rows with citation), used only to score the clustering,
  never as merge input. Pairs are split into a tuning set and a held-out set per rule (6).

## 8. Investigation layer

- **Tracing**: forward and backward from an address or transfer, with selectable methods (all paths, FIFO, haircut),
  stopping at services and listing the service reached. Spoofed transfers are never followed. Haircut shares are
  scaled decimals (3.3).
- **Wallet tracking**: watchlists in Postgres; the hot layer checks every new head block against them and records an
  alert (address, transfer, counterparty entity, label) within one block plus the poll interval. An alert whose block
  is dropped by a fork is marked retracted.
- **Exposure**: per address or entity, the share of inflow and outflow from labelled categories (sanctioned,
  blacklisted, exchange, unknown), direct and up to K hops.
- **Delivery**: parquet for analysis; the **`usdt` Postgres instance** (its own instance, not the wallex one) with
  transfers, entities, labels and alerts, using the types of 3.3. It runs under the data root with its own memory
  limit, so its bulk loads and vacuums do not compete with wallex. Wallex reads it through a read-only role, or
  `postgres_fdw` if it needs joins. An API comes after v1.

## 9. Operations

Same pattern as blockline-btc: `bin/blockline install`, `bin/blockline status`, one runner that starts each step as
soon as its input exists, a commit point per step, guards, dashboard on loopback. No agent runs on the server.

- **Lag alert**: fires when the hot layer is more than about 2 minutes behind the node's head (setting). A separate
  alert fires when the solidified ETL falls an hour behind.
- **RAM guard**: node 24 GB + Postgres 32–64 GB + clustering 64–128 GB is 120–216 GB against about 170 GB free, so the
  guard runs clustering and bulk Postgres loads one at a time, never together.
- **Disk guard** on the data root's filesystem: alert at 80% full; at 90% pause clustering and Postgres loads.
- **Offsite backups** of what cannot be downloaded again: the Tier 2 tables (0.3–0.8 TB once, then about 10–30 GB a
  month), Postgres dumps of watchlists, alerts and labels, ground truth, clustering outputs and configs. Not the node:
  a fresh snapshot can be downloaded again. Target: AceCloud object storage if offered, otherwise any S3-compatible
  bucket in another location (decision 8). Uploads stop first when the traffic share runs out (2.5).
- The go-lite conversion and the prune script (2.4) run from the same runner and refuse unless their gates hold.

## 10. Capacity, traffic and money

### 10.1 Measured on this server (2026-11-06)

32 vCPU x86_64, 251 GB RAM (about 170 GB free with blockline-btc and wallex running), 5.5 TB free disk (shared with BTC
and the forks). The disk performs as SSD: 441 MB/s write and 766 MB/s read sequential, about 22,000 random 4K reads/s
and 15,000 random 4K writes/s with 16 parallel requests, under the current load. Traffic used this month: 38.8 of
1,000 GB (BTC is synced and uses about 40 GB a month). Re-measured at the start of Phase 0.

### 10.2 Disk

All figures are estimates, measured in Phase 0 and the full phase.

| Item | Full phase (peak) | After go-lite |
|---|---|---|
| Node | about 3.6–4.0 TB extracted, plus catch-up growth | about 0.1 TB, then about 1 TB/year between prunes |
| Prune scratch (transient) | — (no pruning) | about 2× the node database |
| Tier 2 + Tier 3, history and live | 0.4–0.8 TB | 0.4–0.8 TB, growing |
| Postgres `usdt` | — (loaded after go-lite) | 0.6–1.0 TB |
| Clustering working space | — | about 0.1 TB |
| **Sum** | **about 4.0–4.8 TB (owner estimate 4.6 TB)** | **about 1.2–2.2 TB plus node growth and prune scratch** |

No new disk is needed. The full phase uses most of the 5.5 TB free, so **the forks pause** from the start of the
download until go-lite (10.5). Postgres and clustering wait until after go-lite so that the peak stays at the node plus
Tier 2.

### 10.3 Traffic

| | Month 1 (download and catch-up) | Steady month |
|---|---|---|
| BTC (measured) | 40 GB | 40 GB |
| Snapshot download | 3.54 TB once (more if a download breaks and restarts) | — |
| Node catch-up from 18 Sep / node in steady state | 0.2–0.6 TB (owner estimate) | up to 300 GB |
| Offsite backups | — | up to 50 GB |
| **Sum** | **about 3.8–4.2 TB, about four times the 1 TB cap** | **up to 390 GB; about 610 GB left for the forks** |

- First question to AceCloud: whether incoming traffic counts against the 1 TB cap (many providers count only
  outgoing). If it counts: a one-off top-up or a temporary unmetered month.
- The download is not spread over months under the cap: a streamed download cannot pause for months, the
  store-then-extract route needs 7 TB of disk, and the mirror may replace the September file before it finishes.
- After month 1, USDT's share is 350 GB a month: node up to 300 GB, backups up to 50 GB.

### 10.4 Money

| Item | Cost |
|---|---|
| Snapshot data | free |
| One-off traffic for month 1 (if incoming counts) | AceCloud top-up or unmetered month: quote |
| Offsite backup storage | AceCloud object storage or another S3-compatible bucket: quote |
| Extra disk | none on this route |

### 10.5 Timeline of the full phase (estimate)

Download (3.54 TB on one connection: days, measured) + catch-up (about 1.7M blocks at 2 blocks/s is about 10 days) +
full extraction (21 days or less, gate) + checks and the frozen-height sweep + go-lite. About 5–7 weeks in total, during
which the forks are paused and the host disk is near its peak.

## 11. Phases

| Phase | Work | Done when |
|---|---|---|
| **0. Prepare and download** | AceCloud answers (decision 12); bootstrap (decision 4); fixtures (4.4) and parser against them; Tier 2 field review (3.3); forks paused; snapshot streamed with checksum (2.3); full node started and caught up; the gate below | every gate item holds |
| **1. Live** | `etl` following the solidified head, sealing, hot layer, guards, status, dashboard; history run started in parallel | live data from the head on, no gaps and parent hashes linked for 14 days |
| **2. Extract history** | `etl` from block 0 to the head; section 5 checks including the frozen-height sweep | full `balanceOf` match at H for every address that ever held USDT; no negative balance at any block; supply reconciles; zero gaps and parent-hash linkage from block 0; every epoch below the head sealed |
| **3. Go lite** | the conversion of 2.4, under the gate below; forks resume | lite node at the head, ETL following it, disk back to the after-go-lite figure |
| **4. Index** | Tier 3; Postgres `usdt` loaded | stats and balances match the node for the sample |
| **5. Clustering** | ut-v1, report, quality, ablation | report reviewed; every merging rule meets the promotion bar (6); no unexplained join over the breaker limits |
| **6. Labels** | label store, ground truth (tuning and held-out sets), scoring | ground-truth precision and recall published in the report |
| **7. Investigation** | tracing, watchlists and alerts, exposure | an analyst can investigate a hash, track a wallet and see its entity |

### Phase 0 gate (before the full extraction)

- The snapshot checksum matches, and the `.md5sum` file did not change during the download.
- The node starts on JDK 8 with LevelDB and catches up to the head at 2 or more blocks a second.
- History queries answer locally for a 2018 block and a 2019 block.
- The earliest block with internal transactions is recorded (decision 11 if it is after block 0).
- ETL read speed from the local node is measured, and the projected full extraction takes 21 days or less.
- One full epoch is extracted and passes the checks that apply to a single epoch: continuity and parent-hash links,
  `Issue` / `Redeem` matching, and a manual spot check of a few of its blocks against Tronscan. Balance checks need the
  whole history (the node answers `balanceOf` only at the head), so they wait for Phase 2.
- Every edge-case fixture passes.
- Node traffic is at most 300 GB a month, measured over 7 days.
- The disk, traffic and money totals fit the caps.

### Phase 3 gate (go-lite, irreversible)

- Phase 2 is done: every check passed, every epoch below head − 65,536 sealed.
- The Tier 2 field review was signed off: nothing known to be missing.
- The decision on backups before go-lite is applied (decision 13).
- The node has exited before the conversion starts.

### Phase 0 open items (one call or one query each)

- [ ] The Tronscan-verified source matches the deployed bytecode (read from the local node).
- [ ] Initial supply in the constructor arguments of the creation transaction.
- [ ] Current `basisPointsRate`, `maximumFee` and `deprecated` flag.
- [ ] Token flow of one GasFree transaction: `transferFrom` or a wallet-contract transfer, and what `from` shows.
- [ ] `getnodeinfo` exposes the solidified block number.
- [ ] Number of addresses that ever held USDT, and cumulative transfers since 2019 (from the extraction).
- [ ] Host figures of 10.1.

## 12. Decisions

### Settled by the owner (2026-11-07)

1. **Backfill source**: the official snapshot with internal transactions, downloaded to this server and streamed into
   the extractor with a checksum. No BigQuery, no APIs, no second source.
2. **TRX-side accounts**: all accounts, from block 0.
3. **Traffic**: month 1 about 4 TB, handled with AceCloud (10.3); after month 1, 350 GB a month for USDT (node 300,
   backups 50); backups stop first, the node last.
4. **Bootstrap**: one sudo script as for the forks; it mounts a dedicated disk at `/srv/blockline-usdt` first if one
   is ever added.
5. **Platform database**: its own Postgres instance, not the wallex one; read-only role or `postgres_fdw` for wallex.
6. **Disk**: extract once, then go lite. No dedicated disk; the forks pause during the full phase. Alert at 80%,
   pause clustering and Postgres loads at 90%. No pruning while the node is the history source. Tier 1 dropped.
7. **Labels**: OFAC `sdn_advanced.xml` only, published exchange hot wallets with citations and our own deposit-address
   detection. No OpenSanctions licence, no TronScan tags until a customer needs them.
8. **Offsite backups**: only what cannot be downloaded again (9); AceCloud object storage or another S3-compatible
   bucket elsewhere; quote needed.
9. **Phase 0 gate**: as in section 11, adjusted for this route.
10. **Clustering bar**: 95% pairwise precision, at least 200 held-out pairs per rule, the 100 largest entities reviewed
    before a rule merges.

### Still open

11. **Internal-transaction gap**: if the snapshot's internal transactions start after block 0, replay the chain from
    genesis to produce the missing ones, or accept and report the gap. Decided once the first test (2.4) has run.
12. **AceCloud answers**, needed before anything else: does incoming traffic count against the 1 TB cap; the price of
    a one-off top-up or an unmetered month; an object-storage quote for backups.
13. **Backups before go-lite**: at 50 GB a month, the first 0.3–0.8 TB of Tier 2 takes 6–16 months to upload, and the
    owner's "spread over 2–3 months" needs 100–400 GB a month. After go-lite, a disk failure before the upload ends
    means downloading the snapshot again and re-running the extraction. Choose: a temporary larger backup share (or a
    one-off top-up) so the upload finishes before go-lite; or go lite first and accept that risk for the months the
    upload takes.
14. **Download retries**: a broken download restarts from zero and costs another 3.54 TB of traffic if incoming
    counts. How many attempts the month-1 traffic budget covers.
15. **Forks pause**: confirm the forks can pause for the 5–7 weeks of 10.5.

## Changes from revision 3

All from the owner's decisions of 2026-11-07. Revision 3's changes are in git history (`cea373e`).

1. Route changed to **extract once, then go lite**: full snapshot with internal transactions downloaded to this server,
   full node caught up, history extracted from it, then converted to a lite node (1, 2.1, 2.4, 11).
2. Cloud VM, BigQuery, SQD, commercial exports and TronGrid removed (1, 2.6, 10.4). There is no second source; the
   checks target ETL bugs (5).
3. Streamed download with a checksum computed on the way, the `.md5sum` file checked before and after, one connection,
   restart from zero on failure (2.3).
4. First node test: the earliest block with internal transactions; a gap becomes decision 11 (2.4).
5. One ETL from block 0 through the head and onwards; no separate backfill, no seam, no manifest (2.6, 3.1).
6. Tier 1 dropped; Tier 2 is the record, a new snapshot download is the rebuild source; field review before the history
   run, because history is extracted only once (3.2, 3.3).
7. Checks: per-block aggregates removed; full `balanceOf` sweep at a frozen height before go-lite (peer port blocked);
   optional manual Tronscan spot checks (5).
8. Traffic re-budgeted: month 1 about 3.8–4.2 TB, handled with AceCloud; after that 350 GB a month for USDT; shed order
   backups first, node last (2.5, 10.3).
9. Disk re-budgeted: peak about 4.0–4.8 TB in the full phase with the forks paused; about 1.2–2.2 TB after go-lite;
   thresholds 80% and 90%; Postgres and clustering wait until after go-lite (9, 10.2).
10. Own Postgres instance for `usdt` with a read-only role or `postgres_fdw` for wallex (2.2, 8).
11. Labels: OFAC only plus exchange hot wallets and deposit-address detection; OpenSanctions and TronScan deferred (7).
12. Backups only of what cannot be downloaded again; the node is not backed up (9).
13. Phase 0 gate replaced by the owner's list; a go-lite gate added (11).
14. Clustering promotion bar: 95% precision, 200 held-out pairs per rule, top 100 entities reviewed (6).
15. Phases renumbered 0–7, with history extraction (2) and go-lite (3) as their own phases (11).
16. Open decisions 11–15 added: internal-transaction gap, AceCloud answers, backups before go-lite, download retries,
    forks pause (12).
