-- blockline-usdt Tier 2 in ClickHouse (docs/CLICKHOUSE.md). Applied by the ETL at every start; idempotent.
-- Addresses are the 21 raw bytes of a TRON address (0x41 + 20), hashes 32 bytes. The server-wide functions tron(x)
-- turn an address back into "T…" and tron_addr('T…') the other way. Every table carries `block`, so a range can be deleted and re-inserted.
-- The ETL converts from its rows; column names match etl/extract.py.
CREATE DATABASE IF NOT EXISTS usdt;

CREATE FUNCTION IF NOT EXISTS tron AS (a) -> base58Encode(concat(a, substring(SHA256(SHA256(a)), 1, 4)));
CREATE FUNCTION IF NOT EXISTS tron_addr AS (s) -> CAST(substring(base58Decode(s), 1, 21) AS FixedString(21));

CREATE TABLE IF NOT EXISTS usdt.block_headers (
  block UInt32, block_id FixedString(32), parent_hash FixedString(32), ts DateTime('UTC'),
  witness Nullable(FixedString(21)), tx_count UInt16, version UInt16, tx_trie_root FixedString(32)
) ENGINE = MergeTree ORDER BY block;

-- USDT transfers, stored once per side: dir 'out' is the sender's row, 'in' the receiver's. Sorted by address, so a
-- wallet's history is a range read. The transfers themselves are the 'out' rows (view usdt.transfers).
CREATE TABLE IF NOT EXISTS usdt.usdt_legs (
  address FixedString(21), block UInt32, tx_index UInt16, log_index UInt16, dir Enum8('out' = 1, 'in' = 2),
  counterparty FixedString(21), amount UInt64 CODEC(T64, ZSTD(3)), ts DateTime('UTC') CODEC(DoubleDelta, ZSTD(3)),
  tx_hash FixedString(32), method LowCardinality(String), caller Nullable(FixedString(21)), kind LowCardinality(String),
  spoofed Nullable(Bool),
  INDEX blk block TYPE minmax GRANULARITY 1
) ENGINE = MergeTree ORDER BY (address, block, tx_index, log_index, dir);

CREATE VIEW IF NOT EXISTS usdt.transfers AS
  SELECT block, ts, tx_hash, tx_index, log_index, address AS from_addr, counterparty AS to_addr, amount, method, caller, kind, spoofed
  FROM usdt.usdt_legs WHERE dir = 'out';

-- One row per address, kept by the materialized view below as legs arrive. Signed, so the ETL can subtract a range
-- it deletes before re-inserting it (etl/ch.py delete_from). Read with sum()/min()/max() GROUP BY address.
CREATE TABLE IF NOT EXISTS usdt.address_summary (
  address FixedString(21),
  n_in SimpleAggregateFunction(sum, Int64), n_out SimpleAggregateFunction(sum, Int64),
  sum_in SimpleAggregateFunction(sum, Int128), sum_out SimpleAggregateFunction(sum, Int128),
  first_block SimpleAggregateFunction(min, UInt32), last_block SimpleAggregateFunction(max, UInt32)
) ENGINE = AggregatingMergeTree ORDER BY address;

CREATE MATERIALIZED VIEW IF NOT EXISTS usdt.address_summary_mv TO usdt.address_summary AS
  SELECT address, toInt64(countIf(dir = 'in')) AS n_in, toInt64(countIf(dir = 'out')) AS n_out,
         toInt128(sumIf(amount, dir = 'in')) AS sum_in, toInt128(sumIf(amount, dir = 'out')) AS sum_out,
         min(block) AS first_block, max(block) AS last_block
  FROM usdt.usdt_legs GROUP BY address;

CREATE TABLE IF NOT EXISTS usdt.usdt_approvals (
  block UInt32, ts DateTime('UTC'), tx_hash FixedString(32), tx_index UInt16, log_index UInt16,
  owner Nullable(FixedString(21)), spender Nullable(FixedString(21)), amount_raw Nullable(FixedString(32)), amount_dec Nullable(String)
) ENGINE = MergeTree ORDER BY (block, tx_index, log_index);

CREATE TABLE IF NOT EXISTS usdt.usdt_admin (
  block UInt32, ts DateTime('UTC'), tx_hash FixedString(32), tx_index UInt16, log_index UInt16, event LowCardinality(String),
  address Nullable(FixedString(21)), amount_raw Nullable(FixedString(32)), amount_dec Nullable(String), topics String, data Nullable(String)
) ENGINE = MergeTree ORDER BY (block, tx_index, log_index);

CREATE TABLE IF NOT EXISTS usdt.usdt_tx_costs (
  block UInt32, ts DateTime('UTC'), tx_hash FixedString(32), tx_index UInt16, direct Bool, method LowCardinality(String),
  caller Nullable(FixedString(21)), call_value_dec Nullable(String), result LowCardinality(String),
  energy_usage Int64, origin_energy_usage Int64, energy_usage_total Int64, energy_fee Int64, energy_penalty_total Int64,
  net_usage Int64, net_fee Int64, fee Int64, fee_limit Nullable(Int64), energy_source LowCardinality(Nullable(String))
) ENGINE = MergeTree ORDER BY (block, tx_index);

CREATE TABLE IF NOT EXISTS usdt.usdt_failed_calls (
  block UInt32, ts DateTime('UTC'), tx_hash FixedString(32), tx_index UInt16, method LowCardinality(String),
  owner Nullable(FixedString(21)), from_addr Nullable(FixedString(21)), to_addr Nullable(FixedString(21)),
  amount_raw Nullable(FixedString(32)), amount_dec Nullable(String), result LowCardinality(String), call_data_ok Bool,
  call_data Nullable(String)
) ENGINE = MergeTree ORDER BY (block, tx_index);

CREATE TABLE IF NOT EXISTS usdt.trx_transfers (
  block UInt32 CODEC(Delta, ZSTD(3)), ts DateTime('UTC') CODEC(DoubleDelta, ZSTD(3)), tx_hash FixedString(32), tx_index UInt16,
  from_addr Nullable(FixedString(21)), to_addr Nullable(FixedString(21)), amount_sun Int64 CODEC(T64, ZSTD(3)),
  origin LowCardinality(String), success Bool
) ENGINE = MergeTree ORDER BY (block, tx_index);

CREATE TABLE IF NOT EXISTS usdt.internal_transfers (
  block UInt32, ts DateTime('UTC'), tx_hash FixedString(32), tx_index UInt16, internal_index UInt32,
  internal_hash Nullable(FixedString(32)), caller Nullable(FixedString(21)), to_addr Nullable(FixedString(21)),
  token LowCardinality(String), amount Int64, rejected Bool, note LowCardinality(Nullable(String))
) ENGINE = MergeTree ORDER BY (block, tx_index, internal_index);

CREATE TABLE IF NOT EXISTS usdt.account_activations (
  block UInt32, ts DateTime('UTC'), tx_hash FixedString(32), tx_index UInt16, address Nullable(FixedString(21)),
  activator Nullable(FixedString(21)), method LowCardinality(String), success Bool
) ENGINE = MergeTree ORDER BY (block, tx_index);

CREATE TABLE IF NOT EXISTS usdt.resource_delegations (
  block UInt32, ts DateTime('UTC'), tx_hash FixedString(32), tx_index UInt16, action LowCardinality(String),
  delegator Nullable(FixedString(21)), receiver Nullable(FixedString(21)), resource LowCardinality(String),
  amount_sun Int64, lock Nullable(Bool), lock_period Nullable(Int64), stake UInt8, success Bool
) ENGINE = MergeTree ORDER BY (block, tx_index);

CREATE TABLE IF NOT EXISTS usdt.permission_updates (
  block UInt32, ts DateTime('UTC'), tx_hash FixedString(32), tx_index UInt16, account Nullable(FixedString(21)),
  owner_perm Nullable(String), witness_perm Nullable(String), active_perms Nullable(String), success Bool
) ENGINE = MergeTree ORDER BY (block, tx_index);

CREATE TABLE IF NOT EXISTS usdt.contracts (
  block UInt32, ts DateTime('UTC'), tx_hash FixedString(32), tx_index UInt16, address Nullable(FixedString(21)),
  creator Nullable(FixedString(21)), name Nullable(String), consume_user_resource_percent Nullable(Int64),
  origin_energy_limit Nullable(Int64), success Bool
) ENGINE = MergeTree ORDER BY (block, tx_index);
