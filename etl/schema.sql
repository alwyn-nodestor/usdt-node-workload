-- blockline-usdt Tier 2 in ClickHouse (docs/CLICKHOUSE.md). Applied by the ETL at every start; idempotent.
-- Addresses are the 21 raw bytes of a TRON address (0x41 + 20), hashes 32 bytes. The server-wide functions tron(x)
-- turn an address back into "T…" and tron_addr('T…') the other way. Every table carries `block`, so a range can be deleted and re-inserted.
-- The ETL converts from its rows; column names match etl/extract.py.
CREATE DATABASE IF NOT EXISTS usdt;

CREATE FUNCTION IF NOT EXISTS tron AS (a) -> base58Encode(concat(a, substring(SHA256(SHA256(a)), 1, 4)));
CREATE FUNCTION IF NOT EXISTS tron_addr AS (s) -> CAST(substring(base58Decode(s), 1, 21) AS FixedString(21));

CREATE TABLE IF NOT EXISTS usdt.block_headers (
  block UInt32, block_id FixedString(32), parent_hash FixedString(32), ts DateTime('UTC'),
  witness Nullable(FixedString(21)), tx_count UInt16, version UInt16, tx_trie_root FixedString(32),
  witness_signature Nullable(FixedString(65)) CODEC(ZSTD(1))
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
  owner Nullable(FixedString(21)), spender Nullable(FixedString(21)), amount_raw Nullable(FixedString(32)), amount_dec Nullable(String),
  caller Nullable(FixedString(21)), entry_contract Nullable(FixedString(21))
) ENGINE = MergeTree ORDER BY (block, tx_index, log_index);

CREATE TABLE IF NOT EXISTS usdt.usdt_admin (
  block UInt32, ts DateTime('UTC'), tx_hash FixedString(32), tx_index UInt16, log_index UInt16, event LowCardinality(String),
  address Nullable(FixedString(21)), amount_raw Nullable(FixedString(32)), amount_dec Nullable(String), topics String, data Nullable(String)
) ENGINE = MergeTree ORDER BY (block, tx_index, log_index);

CREATE TABLE IF NOT EXISTS usdt.usdt_tx_costs (
  block UInt32, ts DateTime('UTC'), tx_hash FixedString(32), tx_index UInt16, direct Bool, method LowCardinality(String),
  caller Nullable(FixedString(21)), call_value_dec Nullable(String), result LowCardinality(String),
  energy_usage Int64, origin_energy_usage Int64, energy_usage_total Int64, energy_fee Int64, energy_penalty_total Int64,
  net_usage Int64, net_fee Int64, fee Int64, fee_limit Nullable(Int64), energy_source LowCardinality(Nullable(String)),
  entry_contract Nullable(FixedString(21)), call_selector LowCardinality(Nullable(String)), call_value_sun Int64,
  permission_id UInt8, n_sigs UInt8, tx_ts_delta Nullable(Int64) CODEC(T64, ZSTD(3)), expiration_delta Nullable(Int64) CODEC(T64, ZSTD(3)), memo Nullable(String)
) ENGINE = MergeTree ORDER BY (block, tx_index);

CREATE TABLE IF NOT EXISTS usdt.usdt_failed_calls (
  block UInt32, ts DateTime('UTC'), tx_hash FixedString(32), tx_index UInt16, method LowCardinality(String),
  owner Nullable(FixedString(21)), from_addr Nullable(FixedString(21)), to_addr Nullable(FixedString(21)),
  amount_raw Nullable(FixedString(32)), amount_dec Nullable(String), result LowCardinality(String), call_data_ok Bool,
  call_data Nullable(String), res_message Nullable(String), permission_id UInt8
) ENGINE = MergeTree ORDER BY (block, tx_index);

CREATE TABLE IF NOT EXISTS usdt.trx_transfers (
  block UInt32 CODEC(Delta, ZSTD(3)), ts DateTime('UTC') CODEC(DoubleDelta, ZSTD(3)), tx_hash FixedString(32), tx_index UInt16,
  from_addr Nullable(FixedString(21)), to_addr Nullable(FixedString(21)), amount_sun Int64 CODEC(T64, ZSTD(3)),
  origin LowCardinality(String), success Bool, fee Nullable(Int64), net_fee Nullable(Int64), permission_id Nullable(UInt8),
  memo Nullable(String), new_account Nullable(Bool)
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
  amount_sun Int64, lock Nullable(Bool), lock_period Nullable(Int64), stake UInt8, success Bool, permission_id UInt8
) ENGINE = MergeTree ORDER BY (block, tx_index);

CREATE TABLE IF NOT EXISTS usdt.permission_updates (
  block UInt32, ts DateTime('UTC'), tx_hash FixedString(32), tx_index UInt16, account Nullable(FixedString(21)),
  owner_perm Nullable(String), witness_perm Nullable(String), active_perms Nullable(String), success Bool, permission_id UInt8
) ENGINE = MergeTree ORDER BY (block, tx_index);

CREATE TABLE IF NOT EXISTS usdt.contracts (
  block UInt32, ts DateTime('UTC'), tx_hash FixedString(32), tx_index UInt16, address Nullable(FixedString(21)),
  creator Nullable(FixedString(21)), name Nullable(String), consume_user_resource_percent Nullable(Int64),
  origin_energy_limit Nullable(Int64), success Bool, code_hash Nullable(FixedString(32)), code_len Int64,
  abi_names Nullable(String)
) ENGINE = MergeTree ORDER BY (block, tx_index);

-- ---- Investigation fields (2026-11-23 field review, docs/ETL.md "Investigation fields") ------------------------------
-- Tables created before the review get the new columns here; on a fresh database the CREATEs above already have them.
ALTER TABLE usdt.block_headers ADD COLUMN IF NOT EXISTS witness_signature Nullable(FixedString(65)) CODEC(ZSTD(1));
ALTER TABLE usdt.usdt_approvals ADD COLUMN IF NOT EXISTS caller Nullable(FixedString(21)),
  ADD COLUMN IF NOT EXISTS entry_contract Nullable(FixedString(21));
ALTER TABLE usdt.usdt_tx_costs ADD COLUMN IF NOT EXISTS entry_contract Nullable(FixedString(21)),
  ADD COLUMN IF NOT EXISTS call_selector LowCardinality(Nullable(String)), ADD COLUMN IF NOT EXISTS call_value_sun Int64,
  ADD COLUMN IF NOT EXISTS permission_id UInt8, ADD COLUMN IF NOT EXISTS n_sigs UInt8,
  ADD COLUMN IF NOT EXISTS tx_ts_delta Nullable(Int64) CODEC(T64, ZSTD(3)),
  ADD COLUMN IF NOT EXISTS expiration_delta Nullable(Int64) CODEC(T64, ZSTD(3)),
  ADD COLUMN IF NOT EXISTS memo Nullable(String);
ALTER TABLE usdt.usdt_failed_calls ADD COLUMN IF NOT EXISTS res_message Nullable(String),
  ADD COLUMN IF NOT EXISTS permission_id UInt8;
ALTER TABLE usdt.trx_transfers ADD COLUMN IF NOT EXISTS fee Nullable(Int64), ADD COLUMN IF NOT EXISTS net_fee Nullable(Int64),
  ADD COLUMN IF NOT EXISTS permission_id Nullable(UInt8), ADD COLUMN IF NOT EXISTS memo Nullable(String),
  ADD COLUMN IF NOT EXISTS new_account Nullable(Bool);
ALTER TABLE usdt.resource_delegations ADD COLUMN IF NOT EXISTS permission_id UInt8;
ALTER TABLE usdt.permission_updates ADD COLUMN IF NOT EXISTS permission_id UInt8;
ALTER TABLE usdt.contracts ADD COLUMN IF NOT EXISTS code_hash Nullable(FixedString(32)),
  ADD COLUMN IF NOT EXISTS code_len Int64, ADD COLUMN IF NOT EXISTS abi_names Nullable(String);

-- TRC-10 transfers that may have created an account (new_account not false); all of them with
-- BLOCKLINE_USDT_TRC10_ALL=1 (about 1.5 billion rows over the history, mostly airdrop spam).
CREATE TABLE IF NOT EXISTS usdt.trc10_transfers (
  block UInt32, ts DateTime('UTC'), tx_hash FixedString(32), tx_index UInt16, from_addr Nullable(FixedString(21)),
  to_addr Nullable(FixedString(21)), asset LowCardinality(Nullable(String)), amount Int64, success Bool,
  fee Nullable(Int64), net_fee Nullable(Int64), permission_id Nullable(UInt8), memo Nullable(String), new_account Nullable(Bool)
) ENGINE = MergeTree ORDER BY (block, tx_index);

-- Self-chosen account names (AccountUpdateContract) and account ids. The owner's claim, never a label by itself.
CREATE TABLE IF NOT EXISTS usdt.account_names (
  block UInt32, ts DateTime('UTC'), tx_hash FixedString(32), tx_index UInt16, address Nullable(FixedString(21)),
  kind LowCardinality(String), name Nullable(String), success Bool
) ENGINE = MergeTree ORDER BY (block, tx_index);

-- Signatures of value-moving and control-changing transactions signed by a non-owner permission or several keys:
-- the signing key (ecrecover of txID) shows who controlled the account, e.g. a hijacked multisig. Comma-separated hex.
CREATE TABLE IF NOT EXISTS usdt.tx_signers (
  block UInt32, ts DateTime('UTC'), tx_hash FixedString(32), tx_index UInt16, owner Nullable(FixedString(21)),
  contract_type LowCardinality(String), permission_id UInt8, n_sigs UInt8, signatures String CODEC(ZSTD(1)), success Bool
) ENGINE = MergeTree ORDER BY (block, tx_index);

-- Transfer logs of the watched tokens (lib/tron.py WATCHED_TOKENS): USDT swapped into USDD / A7A5 / USDC, DEX legs.
-- amount in the token's base units (decimals differ per token: 6 or 18).
CREATE TABLE IF NOT EXISTS usdt.token_transfers (
  block UInt32, ts DateTime('UTC'), tx_hash FixedString(32), tx_index UInt16, log_index UInt16,
  token LowCardinality(String), token_addr FixedString(21), from_addr FixedString(21), to_addr FixedString(21),
  amount UInt256, decimals UInt8, caller Nullable(FixedString(21)), entry_contract Nullable(FixedString(21)),
  INDEX from_bf from_addr TYPE bloom_filter GRANULARITY 4, INDEX to_bf to_addr TYPE bloom_filter GRANULARITY 4
) ENGINE = MergeTree ORDER BY (block, tx_index, log_index);

-- Every log and every direct call of the watched contracts (lib/tron.py WATCHED_CONTRACTS), raw: the USDT owner
-- multisig (a freeze is submitted and confirmed there before AddedBlackList) and the GasFree controller.
CREATE TABLE IF NOT EXISTS usdt.watched_logs (
  block UInt32, ts DateTime('UTC'), tx_hash FixedString(32), tx_index UInt16, log_index UInt16,
  contract_addr FixedString(21), contract_name LowCardinality(String), caller Nullable(FixedString(21)),
  entry_contract Nullable(FixedString(21)), topic0 Nullable(String), topics String, data Nullable(String)
) ENGINE = MergeTree ORDER BY (block, tx_index, log_index);

CREATE TABLE IF NOT EXISTS usdt.watched_calls (
  block UInt32, ts DateTime('UTC'), tx_hash FixedString(32), tx_index UInt16, contract_addr FixedString(21),
  contract_name LowCardinality(String), caller Nullable(FixedString(21)), selector Nullable(String),
  call_data Nullable(String), result LowCardinality(String)
) ENGINE = MergeTree ORDER BY (block, tx_index);

-- ---- Views for investigators (computed on read; nothing stored) ---------------------------------------------------
-- Tether blacklist status per address, from the on-chain events only. listed = the last Added/Removed was Added.
CREATE VIEW IF NOT EXISTS usdt.blacklist_current AS
  SELECT assumeNotNull(address) AS address,
         argMaxIf(event, (block, tx_index, log_index), event IN ('AddedBlackList', 'RemovedBlackList')) = 'AddedBlackList' AS listed,
         nullIf(minIf(ts, event = 'AddedBlackList'), toDateTime(0, 'UTC')) AS first_listed,
         nullIf(maxIf(ts, event = 'RemovedBlackList'), toDateTime(0, 'UTC')) AS last_removed,
         countIf(event = 'AddedBlackList') AS times_listed,
         sumIf(toInt128OrZero(assumeNotNull(amount_dec)), event = 'DestroyedBlackFunds') AS destroyed
  FROM usdt.usdt_admin
  WHERE event IN ('AddedBlackList', 'RemovedBlackList', 'DestroyedBlackFunds') AND address IS NOT NULL
  GROUP BY address;

-- USDT balance from transfers. DestroyedBlackFunds burns a frozen balance without a Transfer log, so it is
-- subtracted here; without it every destroyed address would show its frozen balance forever.
CREATE VIEW IF NOT EXISTS usdt.address_balance AS
  SELECT s.address AS address, s.sum_in - s.sum_out - d.destroyed AS balance, s.sum_in AS sum_in, s.sum_out AS sum_out,
         d.destroyed AS destroyed, s.n_in AS n_in, s.n_out AS n_out, s.first_block AS first_block, s.last_block AS last_block
  FROM (SELECT address, sum(sum_in) AS sum_in, sum(sum_out) AS sum_out, sum(n_in) AS n_in, sum(n_out) AS n_out,
               min(first_block) AS first_block, max(last_block) AS last_block
        FROM usdt.address_summary GROUP BY address) AS s
  LEFT JOIN (SELECT assumeNotNull(address) AS address, sum(toInt128OrZero(assumeNotNull(amount_dec))) AS destroyed
             FROM usdt.usdt_admin WHERE event = 'DestroyedBlackFunds' AND address IS NOT NULL GROUP BY address) AS d
  ON s.address = d.address;

-- First TRX an address ever received (any origin, successful): the activator for accounts created by a plain
-- transfer in blocks where the fee does not show it, and the "who funded this wallet first" lead.
CREATE VIEW IF NOT EXISTS usdt.first_trx_in AS
  SELECT assumeNotNull(to_addr) AS address, argMin(from_addr, (block, tx_index)) AS funder,
         argMin(origin, (block, tx_index)) AS first_origin, argMin(amount_sun, (block, tx_index)) AS first_amount_sun,
         min(block) AS first_block, argMin(tx_hash, (block, tx_index)) AS first_tx_hash
  FROM usdt.trx_transfers WHERE success AND amount_sun > 0 AND to_addr IS NOT NULL
  GROUP BY to_addr;
