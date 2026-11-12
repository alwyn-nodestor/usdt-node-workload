"""One solidified block (getblockbynum + gettransactioninfobyblocknum, hex addresses) -> Tier 2 rows (PLAN.md 3.3).

Checked before anything is returned (PLAN.md 2.6): the block ID carries the block number, the parent hash is the
previous block's ID, every transaction ID is sha256 of its raw data, and the info call lists exactly the block's
transactions in the same order. Any mismatch raises BlockError; the caller stops rather than write a bad block.
"""
import hashlib
import json

from lib.tron import (ADMIN_EVENTS, APPROVAL, INT63, SELECTORS, TRANSFER, USDT_HEX, ZERO20, addr, uint256,
                      word_addr, words)

USDT_LOG = USDT_HEX[2:]   # log addresses come without the 41 prefix


class BlockError(Exception):
    pass


def _call_amount(method, data_words):
    """The amount argument of a direct USDT call, as (raw32, int, ok)."""
    idx = {"transfer": 1, "approve": 1, "transferFrom": 2, "issue": 0, "redeem": 0}.get(method)
    if idx is None or len(data_words) <= idx:
        return None, None, False
    raw, val = uint256(data_words[idx])
    return raw, val, True


def decode_call(data_hex):
    """Call data of a direct USDT call -> method, to-address, from-address, amount (raw, int), call_data_ok."""
    data_hex = (data_hex or "").lower()
    sel = data_hex[:8]
    method = SELECTORS.get(sel, f"other:{sel}" if sel else "none")
    args = data_hex[8:]
    ws = words(args)
    need = {"transfer": 2, "approve": 2, "transferFrom": 3, "issue": 1, "redeem": 1}.get(method)
    ok = need is not None and len(ws) >= need and len(args) == 64 * need
    frm = to = None
    if method == "transferFrom" and len(ws) >= 2:
        frm, to = word_addr(ws[0]), word_addr(ws[1])
    elif method in ("transfer", "approve") and ws:
        to = word_addr(ws[0])
    raw, val, have = _call_amount(method, ws)
    return method, frm, to, raw, val, ok and have


def extract(block, infos, prev_id=None):
    """Return (header_row, {table: [rows]})."""
    raw = block.get("block_header", {}).get("raw_data", {})
    num = raw.get("number", 0)
    bid = block.get("blockID", "")
    if not bid or int(bid[:16], 16) != num:
        raise BlockError(f"block {num}: block ID {bid[:20]}… does not carry the block number")
    parent = raw.get("parentHash")
    if prev_id is not None and parent != prev_id:
        raise BlockError(f"block {num}: parent hash {parent} is not the previous block's ID {prev_id}")
    ts = raw.get("timestamp", 0)
    txs = block.get("transactions", []) or []
    if isinstance(infos, dict):           # an empty block answers {} instead of []
        infos = [] if not infos else [infos]
    for t in txs:
        if hashlib.sha256(bytes.fromhex(t["raw_data_hex"])).hexdigest() != t["txID"]:
            raise BlockError(f"block {num}: transaction {t['txID']} does not hash to its ID")
    ids = [t["txID"] for t in txs]
    if ids != [i.get("id") for i in infos]:
        raise BlockError(f"block {num}: the info call lists {len(infos)} transactions, the block {len(ids)}, "
                         "or in another order")

    header = {"block": num, "block_id": bid, "parent_hash": parent, "ts": ts, "witness": addr(raw.get("witness_address")),
              "tx_count": len(txs), "version": raw.get("version"), "tx_trie_root": raw.get("txTrieRoot")}
    out = {k: [] for k in ("usdt_transfers", "usdt_approvals", "usdt_admin", "usdt_tx_costs", "usdt_failed_calls",
                           "trx_transfers", "internal_transfers", "account_activations", "resource_delegations",
                           "permission_updates", "contracts")}

    for ti, (t, info) in enumerate(zip(txs, infos)):
        h = t["txID"]
        rd = t["raw_data"]
        contract = (rd.get("contract") or [{}])[0]
        ctype = contract.get("type")
        v = contract.get("parameter", {}).get("value", {})
        owner = addr(v.get("owner_address"))
        receipt = info.get("receipt", {})
        # success comes from the receipt only (PLAN.md 3.3); system contracts carry no receipt result
        result = receipt.get("result") or (t.get("ret") or [{}])[0].get("contractRet") or "SUCCESS"
        ok = result == "SUCCESS" and info.get("result") != "FAILED"
        base = {"block": num, "ts": ts, "tx_hash": h, "tx_index": ti}

        # ---- TRX, accounts, resources, permissions, contracts
        if ctype == "TransferContract":
            out["trx_transfers"].append({**base, "from_addr": owner, "to_addr": addr(v.get("to_address")),
                                         "amount_sun": v.get("amount", 0), "origin": "transfer", "success": ok})
        elif ctype == "AccountCreateContract":
            out["account_activations"].append({**base, "address": addr(v.get("account_address")), "activator": owner,
                                               "method": "AccountCreateContract", "success": ok})
        elif ctype in ("DelegateResourceContract", "UnDelegateResourceContract"):
            out["resource_delegations"].append({**base, "action": "delegate" if ctype.startswith("Delegate") else "undelegate",
                                                "delegator": owner, "receiver": addr(v.get("receiver_address")),
                                                "resource": v.get("resource", "BANDWIDTH"), "amount_sun": v.get("balance", 0),
                                                "lock": bool(v.get("lock", False)), "lock_period": v.get("lock_period"),
                                                "stake": 2, "success": ok})
        elif ctype in ("FreezeBalanceContract", "UnfreezeBalanceContract") and v.get("receiver_address"):
            out["resource_delegations"].append({**base, "action": "freeze" if ctype.startswith("Freeze") else "unfreeze",
                                                "delegator": owner, "receiver": addr(v.get("receiver_address")),
                                                "resource": v.get("resource", "BANDWIDTH"),
                                                "amount_sun": v.get("frozen_balance", 0), "lock": None,
                                                "lock_period": v.get("frozen_duration"), "stake": 1, "success": ok})
        elif ctype == "AccountPermissionUpdateContract":
            out["permission_updates"].append({**base, "account": owner,
                                              "owner_perm": json.dumps(v.get("owner"), sort_keys=True),
                                              "witness_perm": json.dumps(v.get("witness"), sort_keys=True) if v.get("witness") else None,
                                              "active_perms": json.dumps(v.get("actives"), sort_keys=True),
                                              "success": ok})
        elif ctype == "CreateSmartContract":
            nc = v.get("new_contract", {})
            out["contracts"].append({**base, "address": addr(info.get("contract_address")), "creator": owner,
                                     "name": nc.get("name"),
                                     "consume_user_resource_percent": nc.get("consume_user_resource_percent"),
                                     "origin_energy_limit": nc.get("origin_energy_limit"), "success": ok})
        elif ctype == "TriggerSmartContract" and v.get("call_value"):
            out["trx_transfers"].append({**base, "from_addr": owner, "to_addr": addr(v.get("contract_address")),
                                         "amount_sun": v["call_value"], "origin": "call_value", "success": ok})

        # ---- internal transactions
        for ii, it in enumerate(info.get("internal_transactions", []) or []):
            rejected = bool(it.get("rejected", False))
            for cv in it.get("callValueInfo") or [{}]:
                token = cv.get("tokenId") or "TRX"
                amount = cv.get("callValue", 0)
                row = {**base, "internal_index": ii, "internal_hash": it.get("hash"),
                       "caller": addr(it.get("caller_address")), "to_addr": addr(it.get("transferTo_address")),
                       "token": token, "amount": amount, "rejected": rejected,
                       "note": bytes.fromhex(it.get("note", "")).decode("utf-8", "replace") if it.get("note") else None}
                out["internal_transfers"].append(row)
                if token == "TRX" and amount and not rejected and ok:
                    out["trx_transfers"].append({**base, "from_addr": row["caller"], "to_addr": row["to_addr"],
                                                 "amount_sun": amount, "origin": "internal", "success": True})

        # ---- USDT
        direct = ctype == "TriggerSmartContract" and (v.get("contract_address") or "").lower() == USDT_HEX
        method = call_raw = call_val = None
        if direct:
            method, frm, to, call_raw, call_val, data_ok = decode_call(v.get("data"))
            if not ok:
                out["usdt_failed_calls"].append({**base, "method": method, "owner": owner, "from_addr": frm,
                                                 "to_addr": to, "amount_raw": call_raw,
                                                 "amount_dec": None if call_val is None else str(call_val),
                                                 "result": result, "call_data_ok": data_ok,
                                                 "call_data": None if data_ok else v.get("data")})
        usdt_logs = [lg for lg in info.get("log", []) or [] if (lg.get("address") or "").lower() == USDT_LOG]
        for li, lg in enumerate(info.get("log", []) or []):
            if (lg.get("address") or "").lower() != USDT_LOG:
                continue
            tps = lg.get("topics") or []
            t0 = tps[0] if tps else ""
            data_w = words(lg.get("data"))
            row = {**base, "log_index": li}
            if t0 == TRANSFER:
                if len(tps) != 3 or len(data_w) != 1:
                    raise BlockError(f"block {num} tx {h}: Transfer log with {len(tps)} topics / {len(data_w)} data words")
                frm_hex, to_hex = tps[1][-40:], tps[2][-40:]
                amount = int(data_w[0], 16)
                if amount > INT63:
                    raise BlockError(f"block {num} tx {h}: transfer amount {amount} exceeds 2^63-1")
                kind = "mint" if frm_hex == ZERO20 else "burn" if to_hex == ZERO20 else "transfer"
                out["usdt_transfers"].append({**row, "from_addr": addr(frm_hex), "to_addr": addr(to_hex),
                                              "amount": amount, "method": method if direct else "via_contract",
                                              "caller": owner, "kind": kind, "spoofed": None})
            elif t0 == APPROVAL:
                raw32, val = uint256(data_w[0]) if data_w else (None, None)
                out["usdt_approvals"].append({**row, "owner": word_addr(tps[1]) if len(tps) > 1 else None,
                                              "spender": word_addr(tps[2]) if len(tps) > 2 else None,
                                              "amount_raw": raw32, "amount_dec": None if val is None else str(val)})
            else:
                event = ADMIN_EVENTS.get(t0, f"unknown:{t0}")
                args = [x for x in tps[1:]] + data_w     # indexed or not, the arguments in declaration order
                address = amount_raw = amount_dec = None
                if event in ("AddedBlackList", "RemovedBlackList", "Deprecate", "DestroyedBlackFunds",
                             "OwnershipTransferred") and args:
                    address = word_addr(args[0])
                if event in ("DestroyedBlackFunds",) and len(args) > 1:
                    amount_raw, v2 = uint256(args[1]); amount_dec = str(v2)
                if event in ("Issue", "Redeem") and args:
                    amount_raw, v2 = uint256(args[0]); amount_dec = str(v2)
                out["usdt_admin"].append({**row, "event": event, "address": address, "amount_raw": amount_raw,
                                          "amount_dec": amount_dec, "topics": ",".join(tps), "data": lg.get("data")})
        if direct or usdt_logs:
            out["usdt_tx_costs"].append({
                **base, "direct": direct, "method": method if direct else "via_contract", "caller": owner,
                "call_value_dec": None if call_val is None else str(call_val), "result": result,
                "energy_usage": receipt.get("energy_usage", 0), "origin_energy_usage": receipt.get("origin_energy_usage", 0),
                "energy_usage_total": receipt.get("energy_usage_total", 0), "energy_fee": receipt.get("energy_fee", 0),
                "energy_penalty_total": receipt.get("energy_penalty_total", 0), "net_usage": receipt.get("net_usage", 0),
                "net_fee": receipt.get("net_fee", 0), "fee": info.get("fee", 0), "fee_limit": rd.get("fee_limit"),
                "energy_source": None})  # inferred later from resource_delegations state (PLAN.md 3.3)
    return header, out
