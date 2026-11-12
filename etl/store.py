"""Row schemas of the Tier 2 tables as the extractor produces them (arrow types; the ETL ships them to ClickHouse
as parquet, etl/ch.py), the epoch length, and small JSON state helpers."""
import json
import os
from pathlib import Path

import pyarrow as pa

EPOCH = 201_600
TS = pa.timestamp("ms", tz="UTC")
S, I64, I32, U64, B, BIN = pa.string(), pa.int64(), pa.int32(), pa.uint64(), pa.bool_(), pa.binary(32)
BASE = [("block", I64), ("ts", TS), ("tx_hash", S), ("tx_index", I32)]

SCHEMAS = {
    "block_headers": [("block", I64), ("block_id", S), ("parent_hash", S), ("ts", TS), ("witness", S),
                      ("tx_count", I32), ("version", I32), ("tx_trie_root", S)],
    "usdt_transfers": BASE + [("log_index", I32), ("from_addr", S), ("to_addr", S), ("amount", U64), ("method", S),
                              ("caller", S), ("kind", S), ("spoofed", B)],
    "usdt_approvals": BASE + [("log_index", I32), ("owner", S), ("spender", S), ("amount_raw", BIN), ("amount_dec", S)],
    "usdt_admin": BASE + [("log_index", I32), ("event", S), ("address", S), ("amount_raw", BIN), ("amount_dec", S),
                          ("topics", S), ("data", S)],
    "usdt_tx_costs": BASE + [("direct", B), ("method", S), ("caller", S), ("call_value_dec", S), ("result", S),
                             ("energy_usage", I64), ("origin_energy_usage", I64), ("energy_usage_total", I64),
                             ("energy_fee", I64), ("energy_penalty_total", I64), ("net_usage", I64), ("net_fee", I64),
                             ("fee", I64), ("fee_limit", I64), ("energy_source", S)],
    "usdt_failed_calls": BASE + [("method", S), ("owner", S), ("from_addr", S), ("to_addr", S), ("amount_raw", BIN),
                                 ("amount_dec", S), ("result", S), ("call_data_ok", B), ("call_data", S)],
    "trx_transfers": BASE + [("from_addr", S), ("to_addr", S), ("amount_sun", I64), ("origin", S), ("success", B)],
    "internal_transfers": BASE + [("internal_index", I32), ("internal_hash", S), ("caller", S), ("to_addr", S),
                                  ("token", S), ("amount", I64), ("rejected", B), ("note", S)],
    "account_activations": BASE + [("address", S), ("activator", S), ("method", S), ("success", B)],
    "resource_delegations": BASE + [("action", S), ("delegator", S), ("receiver", S), ("resource", S),
                                    ("amount_sun", I64), ("lock", B), ("lock_period", I64), ("stake", I32), ("success", B)],
    "permission_updates": BASE + [("account", S), ("owner_perm", S), ("witness_perm", S), ("active_perms", S),
                                  ("success", B)],
    "contracts": BASE + [("address", S), ("creator", S), ("name", S), ("consume_user_resource_percent", I64),
                         ("origin_energy_limit", I64), ("success", B)],
}
SCHEMAS = {t: pa.schema(cols) for t, cols in SCHEMAS.items()}


def epoch_of(block):
    return block // EPOCH


def read_json(path, default=None):
    try:
        return json.loads(Path(path).read_text())
    except FileNotFoundError:
        return default


def write_json(path, obj):
    path = Path(path)
    tmp = path.with_name("." + path.name + ".tmp")
    tmp.write_text(json.dumps(obj, indent=1))
    os.replace(tmp, path)
