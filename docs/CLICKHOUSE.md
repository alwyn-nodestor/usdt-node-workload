# ClickHouse: the Tier 2 store

Tier 2 (PLAN.md 3.3) lives in one ClickHouse server on this host, written directly by the ETL. There is no parquet
copy and no Postgres copy of chain data (PLAN.md change 23). Owner's decision, 2026-11-12: single copy, no backup;
losing it after go-lite means a new 4 TB snapshot download and re-extraction.

| | |
|---|---|
| Version | 26.8.33.10 LTS, pinned by sha256 in `lib/usdt_node.py` (`bin/blockline-usdt install-clickhouse`) |
| Home | `/srv/blockline-usdt/clickhouse` (`clickhouse` binary, `config.xml` copied from `clickhouse/config.xml`, `data/`, `log/`) |
| Ports | 127.0.0.1 only: HTTP 18123, native 19000. No other listeners |
| Memory | capped at 16 GB in the config; unit hard stop 20 GB |
| Compression | zstd level 3 for every column (LZ4, the default, was 1.6-1.9x larger in the benchmark) |
| Unit | `blockline-usdt-clickhouse` (user usdt); the ETL unit requires it |
| Schema | `etl/schema.sql`, applied by the ETL at every start (idempotent) |

Query from the shell:

    /srv/blockline-usdt/clickhouse/clickhouse client --port 19000
    SELECT tron(counterparty), dir, amount / 1e6, ts FROM usdt.usdt_legs
      WHERE address = tron_addr('TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t') ORDER BY block DESC LIMIT 20;

## Layout

- Addresses are the 21 raw bytes (`FixedString(21)`), hashes 32 bytes. `tron(a)` gives back `T…`, `tron_addr('T…')`
  the bytes; `lower(hex(tx_hash))` gives the usual hash text.
- `usdt_legs`: each USDT `Transfer` twice, `dir = 'out'` under the sender and `dir = 'in'` under the receiver, sorted
  by `(address, block, tx_index, log_index, dir)`. A wallet's history is one range read (two 8,192-row granules,
  whatever the table size). `usdt.transfers` is the view of the `out` rows: each transfer once.
- `address_summary`: per address `n_in`, `n_out`, `sum_in`, `sum_out`, `first_block`, `last_block`, kept by a
  materialized view on every insert into `usdt_legs`. Read it aggregated:
  `SELECT sum(sum_in) - sum(sum_out) AS balance FROM usdt.address_summary WHERE address = … GROUP BY address`.
- Every other table is in block order, one row per item, with the columns of `etl/extract.py`.

## Writes and recovery

The ETL sends each batch as parquet over HTTP (`INSERT … SELECT … FROM input() FORMAT Parquet`) and ClickHouse
converts addresses and hashes on the way in (`etl/ch.py`). Batches: 20 blocks or 30 s live, 1,000 blocks for range
workers. The cursor file moves only after every table of a batch is in. On start, rows at or past the cursor are
deleted (a batch interrupted mid-way), after subtracting them from `address_summary`, then extraction resumes.
Tested 2026-11-12: delete 100 blocks and resume, all totals identical.

## Benchmark (2026-11-12)

6,453 real blocks (87,915,843-87,922,295): ClickHouse with zstd 144.9 MB against parquet 149.9 MB (0.97x); Postgres
was 5.2x parquet. Legs (each transfer twice) 1.6x the single transfer table. Lookups read 16,384 rows for any wallet;
on 200M synthetic rows 22 ms median with uncached disk reads (O_DIRECT), 3.4 ms warm; the same lookup without the sort
key is a 4.5 s full scan. A 10-hop trace over real data (frontier capped at 200, stop at addresses with more than
1,000 counterparties) took 146 ms.
