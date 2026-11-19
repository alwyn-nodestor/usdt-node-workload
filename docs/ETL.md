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
| `block_headers` | every block |
| `usdt_legs` (view `transfers`) | every `Transfer` log of USDT, direct or through another contract (`method = via_contract`), stored once per side; `kind` mint / burn / transfer; `spoofed` left null (filled by clustering, PLAN.md 6). Plus `address_summary` |
| `usdt_approvals`, `usdt_admin` | uint256 values as 32 raw bytes plus decimal string; unknown USDT topics kept as `unknown:<topic>` |
| `usdt_tx_costs` | every transaction that calls USDT directly or emits a USDT log; `energy_source` null until inferred |
| `usdt_failed_calls` | direct USDT calls whose receipt is not SUCCESS, decoded from call data; raw data kept when it does not decode |
| `trx_transfers` | `TransferContract`, `call_value`, and internal TRX transfers (not rejected, transaction succeeded) |
| `internal_transfers` | every internal transaction, one row per call-value entry |
| `account_activations` | `AccountCreateContract` only so far |
| `resource_delegations` | Stake 2.0 delegate / undelegate, Stake 1.0 freeze / unfreeze with a receiver |
| `permission_updates`, `contracts` | permissions as JSON text; contract creation parameters |

## Not done yet (Phase 0 items)

- Activations made by a TRX / TRC-10 transfer to a new address: the block data does not flag them; they need the
  first-seen pass over the history.
- Fee transfers (`kind = fee`): only possible if `Params` sets a non-zero `basisPointsRate`; read it in Phase 0.
- The field review against investigation and clustering needs before the history run (PLAN.md 3.3).
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
