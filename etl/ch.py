"""ClickHouse access for the ETL: schema, inserts, range deletes, table sizes (docs/CLICKHOUSE.md).

Rows from etl/extract.py (addresses as "T…", hashes as hex) are sent as parquet over the local HTTP port and
converted inside ClickHouse to the compact column types of etl/schema.sql: addresses to 21 raw bytes, hashes to 32.
USDT transfers become two rows each in usdt_legs (the sender's 'out' row and the receiver's 'in' row).
"""
import io
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from etl.store import SCHEMAS

URL = os.environ.get("BLOCKLINE_USDT_CH", "http://127.0.0.1:18123/")
DB = os.environ.get("BLOCKLINE_USDT_CH_DB", "usdt")
SCHEMA_SQL = Path(__file__).with_name("schema.sql")
ADDR = {"from_addr", "to_addr", "caller", "owner", "spender", "address", "activator", "delegator", "receiver",
        "account", "creator", "witness", "entry_contract", "token_addr", "contract_addr"}
HASH = {"tx_hash", "block_id", "parent_hash", "tx_trie_root", "internal_hash", "code_hash", "witness_signature"}  # hex -> bytes
TABLES = [t for t in SCHEMAS if t != "usdt_transfers"] + ["usdt_legs"]   # what lives in ClickHouse


class CHError(Exception):
    pass


def query(sql, body=None, tries=5, **settings):
    """Run one statement (names always carry the database); returns the response text. A body (insert data) moves
    the statement into the URL. Retries only when the server is unreachable, never on a ClickHouse error."""
    params = dict(settings)
    if body is not None:
        params["query"] = sql
    url = URL + "?" + urllib.parse.urlencode(params)
    data = body if body is not None else sql.encode()
    delay = 2
    for attempt in range(tries):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, data=data), timeout=600) as r:
                return r.read().decode()
        except urllib.error.HTTPError as err:
            raise CHError(f"{err.read().decode(errors='replace')[:2000]}\n-- in: {sql[:500]}") from None
        except (urllib.error.URLError, ConnectionError, TimeoutError) as err:
            if attempt == tries - 1:
                raise CHError(f"ClickHouse unreachable at {URL}: {err}") from None
            time.sleep(delay)
            delay = min(delay * 2, 60)


def apply_schema():
    """Create the database, functions and tables if missing (schema.sql, with the database name substituted)."""
    text = SCHEMA_SQL.read_text().replace("usdt.", f"{DB}.").replace("DATABASE IF NOT EXISTS usdt", f"DATABASE IF NOT EXISTS {DB}")
    text = re.sub(r"--[^\n]*", "", text)
    for stmt in (s.strip() for s in text.split(";")):
        if stmt:
            query(stmt)
    _target_types.clear()


_target_types = {}


def target_types(table):
    if table not in _target_types:
        rows = query(f"SELECT name, type FROM system.columns WHERE database = '{DB}' AND table = '{table}' "
                     "ORDER BY position FORMAT TSVRaw")
        _target_types[table] = [tuple(line.split("\t")) for line in rows.splitlines()]
    return _target_types[table]


ARROW_TO_CH = {pa.int64(): "Int64", pa.int32(): "Int32", pa.uint64(): "UInt64", pa.bool_(): "Bool",
               pa.string(): "String", pa.binary(32): "FixedString(32)"}


def _input_structure(schema):
    cols = []
    for f in schema:
        t = "DateTime64(3)" if pa.types.is_timestamp(f.type) else ARROW_TO_CH[f.type]
        cols.append(f"{f.name} Nullable({t})")
    return ", ".join(cols)


def _expr(name, target):
    """Input column -> target column expression."""
    if name in ADDR:
        e = f"if(isNull({name}) OR {name} = '', NULL, substring(base58Decode({name}), 1, 21))"
    elif name in HASH:
        e = f"if(isNull({name}), NULL, unhex({name}))"
    elif name == "ts":
        e = f"toDateTime({name}, 'UTC')"
    else:
        e = name
    if not target.startswith("Nullable") and not target.startswith("LowCardinality(Nullable"):
        e = f"assumeNotNull({e})"
    return f"CAST({e} AS {target})"


def _parquet_bytes(table, rows):
    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pylist(rows, schema=SCHEMAS[table]), buf, compression="none")
    return buf.getvalue()


def insert(table, rows):
    """Insert extract.py rows of one Tier 2 table. usdt_transfers goes to usdt_legs (two rows per transfer)."""
    if not rows:
        return 0
    schema = SCHEMAS[table]
    structure = _input_structure(schema)
    if table == "usdt_transfers":
        side = {"address": "if(d = 1, from_addr, to_addr)", "counterparty": "if(d = 1, to_addr, from_addr)"}
        sel = []
        for name, t in target_types("usdt_legs"):
            if name in side:
                sel.append(f"CAST(substring(base58Decode(assumeNotNull({side[name]})), 1, 21) AS {t}) AS {name}")
            elif name == "dir":
                sel.append(f"CAST(d AS {t}) AS dir")
            else:
                sel.append(f"{_expr(name, t)} AS {name}")
        sql = (f"INSERT INTO {DB}.usdt_legs SELECT {', '.join(sel)} FROM input('{structure}') "
               "ARRAY JOIN [1, 2] AS d FORMAT Parquet")
        query(sql, body=_parquet_bytes(table, rows))
        return len(rows)
    sel = [f"{_expr(name, t)} AS {name}" for name, t in target_types(table)]
    sql = f"INSERT INTO {DB}.{table} SELECT {', '.join(sel)} FROM input('{structure}') FORMAT Parquet"
    query(sql, body=_parquet_bytes(table, rows))
    return len(rows)


def scalar(sql):
    out = query(sql + " FORMAT TSV").strip()
    return out


def delete_from(first, last=None):
    """Remove rows with first <= block (<= last), left by a batch that was written but not committed to a cursor.
    The address summary is corrected first by subtracting the legs being removed. Returns rows removed."""
    cond = f"block >= {first}" + (f" AND block <= {last}" if last is not None else "")
    removed = 0
    if int(scalar(f"SELECT count() FROM {DB}.usdt_legs WHERE {cond}") or 0):
        query(f"INSERT INTO {DB}.address_summary SELECT address, -toInt64(countIf(dir = 'in')), -toInt64(countIf(dir = 'out')), "
              f"-toInt128(sumIf(amount, dir = 'in')), -toInt128(sumIf(amount, dir = 'out')), min(block), max(block) "
              f"FROM {DB}.usdt_legs WHERE {cond} GROUP BY address")
    for table in TABLES:
        n = int(scalar(f"SELECT count() FROM {DB}.{table} WHERE {cond}") or 0)
        if n:
            query(f"DELETE FROM {DB}.{table} WHERE {cond}", lightweight_deletes_sync=2)
            removed += n
    return removed


def sizes():
    """{table: {rows, bytes}} from the active parts (deleted-but-not-merged rows included)."""
    out = query(f"SELECT table, sum(rows), sum(bytes_on_disk) FROM system.parts WHERE database = '{DB}' AND active "
                "GROUP BY table FORMAT TSV")
    res = {}
    for line in out.splitlines():
        t, r, b = line.split("\t")
        res[t] = {"rows": int(r), "bytes": int(b)}
    return res


def epoch_counts(epoch_len):
    out = query(f"SELECT intDiv(block, {epoch_len}) e, count(), uniqExact(block) FROM {DB}.block_headers GROUP BY e FORMAT TSV")
    return {int(e): int(u) for e, _, u in (line.split("\t") for line in out.splitlines())}
