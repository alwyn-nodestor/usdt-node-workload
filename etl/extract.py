"""One solidified block (getblockbynum + gettransactioninfobyblocknum, hex addresses) -> Tier 2 rows (PLAN.md 3.3).

Checked before anything is returned (PLAN.md 2.6): the block ID carries the block number, the parent hash is the
previous block's ID, every transaction ID is sha256 of its raw data, and the info call lists exactly the block's
transactions in the same order. Any mismatch raises BlockError; the caller stops rather than write a bad block.

The history is extracted once and the node then goes lite (PLAN.md 3.1), so everything an investigation may need
from the raw transaction is taken here even when it is only used later: permission id and signatures of non-owner
signers, wallet fingerprints (tx timestamp, expiration), memos, fees that reveal account creation, the contract a
USDT transfer went through, watched stablecoins and the USDT owner multisig (docs/ETL.md, "Investigation fields").
"""
import hashlib
import json
import os

from lib.tron import (ADMIN_EVENTS, APPROVAL, INT63, SELECTORS, SIGNER_TYPES, TRANSFER, USDT_HEX, WATCHED_CONTRACTS,
                      WATCHED_TOKENS, ZERO20, addr, uint256, word_addr, words)

USDT_LOG = USDT_HEX[2:]   # log addresses come without the 41 prefix
# TRC-10 transfers: about 1.5 billion over the history, mostly 2019-20 airdrop spam. By default only those that
# may have created an account are kept (new_account not False: the activation path and its evidence).
TRC10_ALL = os.environ.get("BLOCKLINE_USDT_TRC10_ALL") == "1"

TABLES = ("usdt_transfers", "usdt_approvals", "usdt_admin", "usdt_tx_costs", "usdt_failed_calls", "trx_transfers",
          "trc10_transfers", "internal_transfers", "account_activations", "resource_delegations", "permission_updates",
          "contracts", "account_names", "tx_signers", "token_transfers", "watched_logs", "watched_calls")


class BlockError(Exception):
    pass


def _text(hex_str):
    """Hex bytes from the node (memo, account name, note, revert message) -> text, undecodable bytes replaced."""
    if not hex_str:
        return None
    try:
        return bytes.fromhex(hex_str).decode("utf-8", "replace")
    except ValueError:
        return hex_str


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


def new_account(ok, fee, net_fee, memo, n_sigs):
    """Whether a TRX / TRC-10 transfer created its recipient's account, from the fee alone. A plain transfer uses no
    energy, so any fee beyond the bandwidth fee is the account-creation fee. Unknown (None) when a memo fee or a
    multi-signature fee could also be in it, or the transaction failed. Early blocks charged no creation fee in TRX,
    so False there is not proof: the first-seen pass over trx_transfers stays the complete method."""
    if not ok or memo or n_sigs > 1:
        return None
    return (fee or 0) - (net_fee or 0) > 0


def is_spoofed(method, amount, frm, owner, entry):
    """A zero-value Transfer whose `from` did not sign: transferFrom(victim, to, 0) passes the allowance check, so
    anyone can emit it (address poisoning, PLAN.md 6). Directly: transferFrom by someone other than `from`.
    Through a contract (batch poisoners): `from` is neither the signer nor the contract that was called."""
    if amount != 0 or frm is None:
        return False
    if method == "via_contract":
        return frm not in (owner, entry)
    return method == "transferFrom" and frm != owner


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
              "tx_count": len(txs), "version": raw.get("version"), "tx_trie_root": raw.get("txTrieRoot"),
              "witness_signature": block.get("block_header", {}).get("witness_signature")}
    out = {k: [] for k in TABLES}

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
        # signing and wallet-software fingerprints (who signed, with which permission, how the tx was built)
        pid = contract.get("Permission_id", 0) or 0
        sigs = t.get("signature") or []
        memo = rd.get("data") or None
        fee, net_fee = info.get("fee", 0), receipt.get("net_fee", 0)
        entry = addr(v.get("contract_address")) if ctype == "TriggerSmartContract" else None

        if ctype in SIGNER_TYPES and (pid != 0 or len(sigs) > 1):
            out["tx_signers"].append({**base, "owner": owner, "contract_type": ctype, "permission_id": pid,
                                      "n_sigs": len(sigs), "signatures": ",".join(sigs), "success": ok})

        # ---- TRX, accounts, resources, permissions, contracts
        if ctype == "TransferContract":
            created = new_account(ok, fee, net_fee, memo, len(sigs))
            to = addr(v.get("to_address"))
            out["trx_transfers"].append({**base, "from_addr": owner, "to_addr": to, "amount_sun": v.get("amount", 0),
                                         "origin": "transfer", "success": ok, "fee": fee, "net_fee": net_fee,
                                         "permission_id": pid, "memo": _text(memo), "new_account": created})
            if created:
                out["account_activations"].append({**base, "address": to, "activator": owner,
                                                   "method": "TransferContract", "success": ok})
        elif ctype == "TransferAssetContract":
            created = new_account(ok, fee, net_fee, memo, len(sigs))
            to = addr(v.get("to_address"))
            if TRC10_ALL or created is not False:
                out["trc10_transfers"].append({**base, "from_addr": owner, "to_addr": to,
                                               "asset": _text(v.get("asset_name")), "amount": v.get("amount", 0),
                                               "success": ok, "fee": fee, "net_fee": net_fee, "permission_id": pid,
                                               "memo": _text(memo), "new_account": created})
            if created:
                out["account_activations"].append({**base, "address": to, "activator": owner,
                                                   "method": "TransferAssetContract", "success": ok})
        elif ctype == "AccountCreateContract":
            out["account_activations"].append({**base, "address": addr(v.get("account_address")), "activator": owner,
                                               "method": "AccountCreateContract", "success": ok})
        elif ctype in ("DelegateResourceContract", "UnDelegateResourceContract"):
            out["resource_delegations"].append({**base, "action": "delegate" if ctype.startswith("Delegate") else "undelegate",
                                                "delegator": owner, "receiver": addr(v.get("receiver_address")),
                                                "resource": v.get("resource", "BANDWIDTH"), "amount_sun": v.get("balance", 0),
                                                "lock": bool(v.get("lock", False)), "lock_period": v.get("lock_period"),
                                                "stake": 2, "success": ok, "permission_id": pid})
        elif ctype in ("FreezeBalanceV2Contract", "UnfreezeBalanceV2Contract"):
            # staking for oneself: the other energy source a USDT sender can have (usdt_tx_costs.energy_source)
            stake = ctype.startswith("Freeze")
            out["resource_delegations"].append({**base, "action": "stake" if stake else "unstake",
                                                "delegator": owner, "receiver": owner,
                                                "resource": v.get("resource", "BANDWIDTH"),
                                                "amount_sun": v.get("frozen_balance" if stake else "unfreeze_balance", 0),
                                                "lock": None, "lock_period": None, "stake": 2, "success": ok,
                                                "permission_id": pid})
        elif ctype in ("FreezeBalanceContract", "UnfreezeBalanceContract"):
            # Stake 1.0; without a receiver the stake is the owner's own. Unfreeze carries no amount.
            out["resource_delegations"].append({**base, "action": "freeze" if ctype.startswith("Freeze") else "unfreeze",
                                                "delegator": owner, "receiver": addr(v.get("receiver_address")) or owner,
                                                "resource": v.get("resource", "BANDWIDTH"),
                                                "amount_sun": v.get("frozen_balance", 0), "lock": None,
                                                "lock_period": v.get("frozen_duration"), "stake": 1, "success": ok,
                                                "permission_id": pid})
        elif ctype == "AccountPermissionUpdateContract":
            out["permission_updates"].append({**base, "account": owner,
                                              "owner_perm": json.dumps(v.get("owner"), sort_keys=True),
                                              "witness_perm": json.dumps(v.get("witness"), sort_keys=True) if v.get("witness") else None,
                                              "active_perms": json.dumps(v.get("actives"), sort_keys=True),
                                              "success": ok, "permission_id": pid})
        elif ctype in ("AccountUpdateContract", "SetAccountIdContract"):
            # self-chosen names: exchanges and services name their wallets; the name is the owner's claim, not a label
            name = v.get("account_name") if ctype == "AccountUpdateContract" else v.get("account_id")
            out["account_names"].append({**base, "address": owner,
                                         "kind": "name" if ctype == "AccountUpdateContract" else "account_id",
                                         "name": _text(name), "success": ok})
        elif ctype == "CreateSmartContract":
            nc = v.get("new_contract", {})
            code = nc.get("bytecode") or ""
            abi = (nc.get("abi") or {}).get("entrys") or []
            out["contracts"].append({**base, "address": addr(info.get("contract_address")), "creator": owner,
                                     "name": nc.get("name"),
                                     "consume_user_resource_percent": nc.get("consume_user_resource_percent"),
                                     "origin_energy_limit": nc.get("origin_energy_limit"), "success": ok,
                                     # same code = same contract family (multisend, drainer, fake-USDT clones)
                                     "code_hash": hashlib.sha256(bytes.fromhex(code)).hexdigest() if code else None,
                                     "code_len": len(code) // 2,
                                     "abi_names": ",".join(sorted({f"{e.get('type', 'Function')}:{e['name']}"
                                                                   for e in abi if e.get("name")})) or None})
        elif ctype == "TriggerSmartContract" and v.get("call_value"):
            out["trx_transfers"].append({**base, "from_addr": owner, "to_addr": entry,
                                         "amount_sun": v["call_value"], "origin": "call_value", "success": ok,
                                         "fee": None, "net_fee": None, "permission_id": pid, "memo": None,
                                         "new_account": None})

        if ctype == "TriggerSmartContract" and (v.get("contract_address") or "").lower() in WATCHED_CONTRACTS:
            cdata = (v.get("data") or "").lower()
            out["watched_calls"].append({**base, "contract_addr": entry,
                                         "contract_name": WATCHED_CONTRACTS[v["contract_address"].lower()],
                                         "caller": owner, "selector": cdata[:8] or None, "call_data": cdata or None,
                                         "result": result})

        # ---- internal transactions
        for ii, it in enumerate(info.get("internal_transactions", []) or []):
            rejected = bool(it.get("rejected", False))
            for cv in it.get("callValueInfo") or [{}]:
                token = cv.get("tokenId") or "TRX"
                amount = cv.get("callValue", 0)
                row = {**base, "internal_index": ii, "internal_hash": it.get("hash"),
                       "caller": addr(it.get("caller_address")), "to_addr": addr(it.get("transferTo_address")),
                       "token": token, "amount": amount, "rejected": rejected, "note": _text(it.get("note"))}
                out["internal_transfers"].append(row)
                if token == "TRX" and amount and not rejected and ok:
                    out["trx_transfers"].append({**base, "from_addr": row["caller"], "to_addr": row["to_addr"],
                                                 "amount_sun": amount, "origin": "internal", "success": True,
                                                 "fee": None, "net_fee": None, "permission_id": None, "memo": None,
                                                 "new_account": None})

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
                                                 "call_data": None if data_ok else v.get("data"),
                                                 # e.g. a blacklisted sender trying to move frozen funds
                                                 "res_message": _text(info.get("resMessage")),
                                                 "permission_id": pid})
        usdt_logs = 0
        for li, lg in enumerate(info.get("log", []) or []):
            la = (lg.get("address") or "").lower()
            tps = lg.get("topics") or []
            t0 = tps[0] if tps else ""
            row = {**base, "log_index": li}
            if ("41" + la) in WATCHED_CONTRACTS:
                out["watched_logs"].append({**row, "contract_addr": addr(la), "contract_name": WATCHED_CONTRACTS["41" + la],
                                            "caller": owner, "entry_contract": entry, "topic0": t0 or None,
                                            "topics": ",".join(tps), "data": lg.get("data")})
                continue
            if ("41" + la) in WATCHED_TOKENS:
                if t0 == TRANSFER and len(tps) == 3:
                    data_w = words(lg.get("data"))
                    sym, dec = WATCHED_TOKENS["41" + la]
                    out["token_transfers"].append({**row, "token": sym, "token_addr": addr(la),
                                                   "from_addr": word_addr(tps[1]), "to_addr": word_addr(tps[2]),
                                                   "amount": str(int(data_w[0], 16)) if data_w else None,
                                                   "decimals": dec, "caller": owner, "entry_contract": entry})
                continue
            if la != USDT_LOG:
                continue
            usdt_logs += 1
            data_w = words(lg.get("data"))
            if t0 == TRANSFER:
                if len(tps) != 3 or len(data_w) != 1:
                    raise BlockError(f"block {num} tx {h}: Transfer log with {len(tps)} topics / {len(data_w)} data words")
                frm_hex, to_hex = tps[1][-40:], tps[2][-40:]
                amount = int(data_w[0], 16)
                if amount > INT63:
                    raise BlockError(f"block {num} tx {h}: transfer amount {amount} exceeds 2^63-1")
                kind = "mint" if frm_hex == ZERO20 else "burn" if to_hex == ZERO20 else "transfer"
                m = method if direct else "via_contract"
                frm = addr(frm_hex)
                out["usdt_transfers"].append({**row, "from_addr": frm, "to_addr": addr(to_hex),
                                              "amount": amount, "method": m, "caller": owner, "kind": kind,
                                              "spoofed": kind == "transfer" and is_spoofed(m, amount, frm, owner, entry)})
            elif t0 == APPROVAL:
                raw32, val = uint256(data_w[0]) if data_w else (None, None)
                out["usdt_approvals"].append({**row, "owner": word_addr(tps[1]) if len(tps) > 1 else None,
                                              "spender": word_addr(tps[2]) if len(tps) > 2 else None,
                                              "amount_raw": raw32, "amount_dec": None if val is None else str(val),
                                              "caller": owner, "entry_contract": entry})
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
                "net_fee": net_fee, "fee": fee, "fee_limit": rd.get("fee_limit"),
                "energy_source": None,  # inferred later from resource_delegations state (PLAN.md 3.3)
                # the contract the transfer went through (DEX router, GasFree, multisend, bridge) and its function
                "entry_contract": entry, "call_selector": (v.get("data") or "")[:8].lower() or None,
                "call_value_sun": v.get("call_value", 0) if ctype == "TriggerSmartContract" else 0,
                # wallet-software fingerprint: how the signer's software builds transactions
                # (as offsets from the block time, ms: same information, and they compress)
                "permission_id": pid, "n_sigs": len(sigs),
                "tx_ts_delta": None if rd.get("timestamp") is None else rd["timestamp"] - ts,
                "expiration_delta": None if rd.get("expiration") is None else rd["expiration"] - ts,
                "memo": _text(memo)})
    return header, out
