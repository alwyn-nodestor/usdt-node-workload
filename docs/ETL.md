# ETL

`etl/` reads solidified blocks from the local node (solidity API, 127.0.0.1:18091) in block order and writes the
Tier 2 tables of PLAN.md 3.3 into ClickHouse (`docs/CLICKHOUSE.md`). Parquet was used until 2026-11-12 and imported
with `etl import-parquet`.

    bin/blockline-usdt etl live              # Phase 1: follow the solidified head (the blockline-usdt-etl unit)
    bin/blockline-usdt etl range FROM TO     # Phase 2: one history worker over FROM..TO, resumable
    bin/blockline-usdt etl history           # Phase 2: block 0 up to the live start, epoch by epoch (blockline-usdt-history unit)
    bin/blockline-usdt etl block N           # print one block's rows, write nothing

## Setup (once, as usdt)

The system python has no `venv` module and there is no root, so the venv is made with uv:

    ~/.local/bin/uv venv --python /usr/bin/python3 .venv
    ~/.local/bin/uv pip install --python .venv/bin/python pyarrow==25.0.1 pycryptodome==3.23.0

pyarrow is still used: batches travel to ClickHouse as parquet.

## Per block

1. `getblockbynum` and `gettransactioninfobyblocknum` (hex addresses; stored as base58 `T…`).
2. Checks, before anything is written: the block ID starts with the block number; the parent hash is the previous
   block's ID (carried across restarts in the cursor); every txID is sha256 of its `raw_data_hex`; the info call lists
   exactly the block's transactions, in order. A failure stops the run (exit 2) and shows on the status page.
3. Rows for every table, inserted into ClickHouse every 20 blocks or 30 s (live) or 1,000 blocks (range), never
   across an epoch (201,600 blocks). The cursor moves only after a batch is in; rows past the cursor are deleted on
   start.
4. An epoch is sealed (`state/sealed.json`) when `block_headers` has all of its blocks and its last block is 1,000
   below the solidified head.

## Tables written

| Table | Notes |
|---|---|
| `block_headers` | every block, with the producer's `witness_signature` (the block is signed by its SR) |
| `usdt_legs` (view `transfers`) | every `Transfer` log of USDT, direct or through another contract (`method = via_contract`), stored once per side; `kind` mint / burn / transfer; `spoofed` set at extraction (below). Plus `address_summary` |
| `usdt_approvals`, `usdt_admin` | uint256 values as 32 raw bytes plus decimal string; unknown USDT topics kept as `unknown:<topic>`. Approvals carry the signer and the contract called (approval phishing) |
| `usdt_tx_costs` | every transaction that calls USDT directly or emits a USDT log; `energy_source` null until inferred. Also the contract the transfer went through (`entry_contract`, `call_selector`, `call_value_sun`) and the signer's fingerprint (below) |
| `usdt_failed_calls` | direct USDT calls whose receipt is not SUCCESS, decoded from call data; raw data kept when it does not decode; `res_message` is the node's revert / out-of-energy text |
| `trx_transfers` | `TransferContract`, `call_value`, and internal TRX transfers (not rejected, transaction succeeded). Plain transfers carry `fee`, `net_fee`, `memo`, `permission_id`, `new_account` |
| `trc10_transfers` | `TransferAssetContract` that may have created an account (`new_account` not false); all of them with `BLOCKLINE_USDT_TRC10_ALL=1` (about 1.5 billion rows, mostly airdrop spam) |
| `internal_transfers` | every internal transaction, one row per call-value entry |
| `account_activations` | `AccountCreateContract`, and TRX / TRC-10 transfers whose fee shows they created the recipient |
| `resource_delegations` | Stake 2.0 delegate / undelegate and self-stake (`stake` / `unstake`); Stake 1.0 freeze / unfreeze (receiver = owner when none). `permission_id` shows a rental platform signing for the account |
| `permission_updates`, `contracts` | permissions as JSON text; contract creation parameters plus `code_hash` (sha256 of the bytecode), `code_len`, `abi_names` |
| `account_names` | `AccountUpdateContract` / `SetAccountIdContract`: names owners give their own accounts. A claim, not a label |
| `tx_signers` | signatures of value-moving and control-changing transactions signed with a non-owner permission or several keys (lib/tron.py `SIGNER_TYPES`) |
| `token_transfers` | `Transfer` logs of the watched tokens (lib/tron.py `WATCHED_TOKENS`: USDD x2, USDC, TUSD, BUSD, A7A5, WTRX), amount in base units as UInt256 |
| `watched_logs`, `watched_calls` | every log and direct call of lib/tron.py `WATCHED_CONTRACTS` (the USDT owner multisig and the GasFree controller), raw |

Views, computed on read: `blacklist_current` (Tether blacklist status from the events), `address_balance` (transfers
minus `DestroyedBlackFunds`, which burns a balance without a `Transfer` log), `first_trx_in` (first TRX received, and
from whom).

## Investigation fields (field review of 2026-11-23)

The history is extracted once and the node then goes lite, so the review took everything an AML or criminal
investigation can later need from the raw transaction, even where only later tiers use it. Research notes and the
report behind it: `/home/blocklineadm/usdt-research/` (outside the repo).

- **Account creation from the fee.** A plain TRX / TRC-10 transfer uses no energy, so a fee beyond the bandwidth fee
  is the account-creation fee: `new_account` = `fee - net_fee > 0`; null when a memo fee or the multi-sign fee could
  also be in it, or the transaction failed. Checked on 50 transfers against the account's `create_time` on the node
  (50/50). Early blocks charged no creation fee in TRX, so `false` is not proof there: the first-seen pass
  (`first_trx_in`) remains the complete method.
- **Spoofed transfers** (`spoofed`, PLAN.md 6): a zero-value `Transfer` whose `from` did not sign. Directly, a
  `transferFrom` by someone other than `from`; through a contract, `from` is neither the signer nor the contract
  called (batch poisoning contracts). Mint and burn are never spoofed.
- **Who signed.** `permission_id` wherever an owner acts; `tx_signers` keeps the signatures (65 bytes each) where the
  signer can differ from the owner, so the signing key can be recovered later (ecrecover over the txID). A hijacked
  multisig or a custodian shows here.
- **Wallet fingerprint** (`usdt_tx_costs`): `tx_ts_delta` and `expiration_delta`, the transaction's own timestamp and
  expiry as offsets from the block time in ms, with `fee_limit` and `n_sigs`. Wallet software sets these its own way
  (in the sample, a 57 s expiry against about 10 h). Evidence for clustering, never a merge (PLAN.md 6).
- **Path of a transfer.** `entry_contract` and `call_selector` tell a DEX router, GasFree, a multisend or a bridge
  apart. GasFree: the `from` of the USDT transfer is the user's GasFree contract, the user is in the controller's
  events (`watched_logs`), and the provider's fee is a second `Transfer` in the same transaction.
- **A freeze before it lands.** Tether blacklists through its owner multisig: `submitTransaction` (`c6427474`)
  carrying `addBlackList` (`0ecb93c0`) and the target, confirmations, then `AddedBlackList`. On block 87,707,271 the
  submission came 19 blocks before the event. Funds moved in that window are the "drained before the freeze" signal.
- **Layering into other stablecoins**: `token_transfers` (USDT to USDD after Tether freezes, A7A5 for Garantex /
  Grinex). A token added to `WATCHED_TOKENS` later needs its range extracted again, which is impossible after go-lite.
- **Contract families**: `contracts.code_hash` groups clones (multisends, drainers, fake-USDT tokens).

Not taken, by decision (each costs disk; see the report): `Transfer` logs of other tokens (fake-USDT poisoning
tokens remain reachable only through `contracts`); the full signed transaction (`raw_data_hex`: the txID and any
node re-verify it, and signatures are kept where the signer matters); signatures of resource delegations.

Rows over the whole history, from 300 blocks sampled across the chain: trx_transfers 7.6 B, internal_transfers 5.5 B,
usdt_tx_costs 3.9 B, usdt_transfers 3.9 B, resource_delegations 2.6 B, account_activations 0.35 B, tx_signers 0.09 B,
token_transfers 0.03 B, watched logs and calls 0.025 B; trc10_transfers 1.5 B if all are kept.

## Not done yet (Phase 0 items)

- Activations made by an internal TRX transfer to a new address, and those in early blocks without a creation fee:
  the first-seen pass over the history (view `first_trx_in`).
- Fee transfers (`kind = fee`): `basisPointsRate` and `maximumFee` are 0 and the contract is neither paused nor
  deprecated at the head (read 2026-11-23); a `Params` event in the history would show an earlier fee.
- GasFree provider fees: tagged in a later pass (second `Transfer` in a transaction whose `entry_contract` is the
  GasFree controller).
- `address_summary.first_block` / `last_block` are not corrected when `delete_from` removes an interrupted batch
  (a min / max cannot be subtracted). The same blocks are re-inserted right after, so it matters only if a range is
  deleted for good.
- One read-speed measurement so far: 300 recent blocks in 4.9 s (about 61 blocks/s, 6 fetchers) while the node was
  still catching up.

## History backfill (Phase 2)

Started by `bin/usdt-up` (step 9) right after the live ETL, i.e. once the node is synced; owner's decision of
2026-11-19, ahead of the Phase 0 field review. The `blockline-usdt-history` unit runs `etl history`:

- The range is block 0 to the live run's `start_block - 1`, fixed in `state/etl-history.json` on the first run, so
  history and live meet with no gap and no overlap.
- One `etl range` worker per epoch, oldest first, 16 at a time (`BLOCKLINE_USDT_HISTORY_WORKERS` in the unit; 4 when run by hand), each with
  its own cursor `state/etl-range-FROM-TO.json`. A restart skips finished epochs and resumes the rest.
- A block failing its checks stops new epochs from starting; the running ones finish, the unit exits 2 and systemd
  retries every 5 minutes. The error shows in `etl-history.json` and as Phase 2 "fail" on the status page.
- The unit is skipped (ConditionPathExists) until the live cursor exists, so a reboot before sync starts nothing.
