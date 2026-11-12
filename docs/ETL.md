# ETL

`etl/` reads solidified blocks from the local node (solidity API, 127.0.0.1:18091) in block order and writes the
Tier 2 tables of PLAN.md 3.3 as parquet under `/srv/blockline-usdt/tier2/<table>/epoch=<k>/<first>-<last>.parquet`.

    bin/blockline-usdt etl live              # Phase 1: follow the solidified head (the blockline-usdt-etl unit)
    bin/blockline-usdt etl range FROM TO     # Phase 2: one history worker over FROM..TO, resumable
    bin/blockline-usdt etl block N           # print one block's rows, write nothing

## Setup (once, as usdt)

The system python has no `venv` module and there is no root, so the venv is made with uv:

    ~/.local/bin/uv venv --python /usr/bin/python3 .venv
    ~/.local/bin/uv pip install --python .venv/bin/python pyarrow==25.0.1 pycryptodome==3.23.0

## Per block

1. `getblockbynum` and `gettransactioninfobyblocknum` (hex addresses; stored as base58 `T…`).
2. Checks, before anything is written: the block ID starts with the block number; the parent hash is the previous
   block's ID (carried across restarts in the cursor); every txID is sha256 of its `raw_data_hex`; the info call lists
   exactly the block's transactions, in order. A failure stops the run (exit 2) and shows on the status page.
3. Rows for every table. Parts are flushed every 200 blocks or 10 minutes (live), 20,000 blocks (range), never across
   an epoch (201,600 blocks). The cursor moves only after a flush; parts past the cursor are removed on start.
4. An epoch is sealed (`tier2/_sealed/epoch=<k>.json`) when `block_headers` covers all of it and its last block is
   1,000 below the solidified head.

## Tables written

| Table | Notes |
|---|---|
| `block_headers` | every block |
| `usdt_transfers` | every `Transfer` log of USDT, direct or through another contract (`method = via_contract`); `kind` mint / burn / transfer; `spoofed` left null (filled by clustering, PLAN.md 6) |
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
- History workers are not started: they wait for the Phase 0 gate. One measurement: 300 recent blocks
  in 4.9 s (about 61 blocks/s, 6 fetchers) while the node was still catching up.
