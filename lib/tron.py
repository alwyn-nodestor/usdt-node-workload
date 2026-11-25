"""TRON encoding helpers shared by the ETL: addresses, event topics, ABI words. No network, no state."""
import hashlib

from Crypto.Hash import keccak

B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
USDT_HEX = "41a614f803b6fd780986a42c78ec9c7f77e6ded13c"   # TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t
ZERO20 = "00" * 20
INT63 = 2**63 - 1


def keccak_hex(text):
    return keccak.new(data=text.encode(), digest_bits=256).hexdigest()


def b58check(raw):
    body = raw + hashlib.sha256(hashlib.sha256(raw).digest()).digest()[:4]
    n = int.from_bytes(body, "big")
    out = ""
    while n:
        n, r = divmod(n, 58)
        out = B58[r] + out
    pad = len(body) - len(body.lstrip(b"\0"))
    return "1" * pad + out


_cache = {}


def addr(hex41):
    """'41…' (21 bytes hex) or a bare 20-byte hex -> base58check 'T…'. None stays None."""
    if not hex41:
        return None
    h = hex41.lower()
    if len(h) == 40:
        h = "41" + h
    got = _cache.get(h)
    if got is None:
        if len(h) != 42:
            raise ValueError(f"not a TRON address: {hex41!r}")
        got = b58check(bytes.fromhex(h))
        if len(_cache) > 2_000_000:
            _cache.clear()
        _cache[h] = got
    return got


def word_addr(word_hex):
    """A 32-byte ABI word (topic or data) holding an address -> 'T…'."""
    return addr(word_hex[-40:])


def words(data_hex):
    data_hex = data_hex or ""
    return [data_hex[i:i + 64] for i in range(0, len(data_hex) - len(data_hex) % 64, 64)]


def uint256(word_hex):
    """A 32-byte word -> (raw bytes, int)."""
    raw = bytes.fromhex(word_hex)
    return raw, int.from_bytes(raw, "big")


def topic(signature):
    return keccak_hex(signature)


# USDT (TetherToken on TRON) events. Unknown topics are kept as "unknown:<topic0>", never dropped.
TRANSFER = topic("Transfer(address,address,uint256)")
APPROVAL = topic("Approval(address,address,uint256)")
ADMIN_EVENTS = {topic(sig): sig.split("(")[0] for sig in [
    "AddedBlackList(address)", "RemovedBlackList(address)", "DestroyedBlackFunds(address,uint256)",
    "Issue(uint256)", "Redeem(uint256)", "Deprecate(address)", "Params(uint256,uint256)",
    "Pause()", "Unpause()", "OwnershipTransferred(address,address)",
]}

def hex_of(b58):
    """'T…' -> '41…' hex, checksum verified. Used to write the constant tables below in their readable form."""
    n = 0
    for c in b58:
        n = n * 58 + B58.index(c)
    raw = n.to_bytes(25, "big")
    if hashlib.sha256(hashlib.sha256(raw[:21]).digest()).digest()[:4] != raw[21:]:
        raise ValueError(f"bad checksum: {b58}")
    return raw[:21].hex()


# Other TRC-20 tokens whose Transfer logs are kept (usdt.token_transfers): the stablecoins USDT is swapped into or
# out of when layering (USDD after Tether freezes, A7A5 for sanctions evasion), and WTRX (DEX legs). Symbol and
# decimals read from each contract on the local node, 2026-11-23. Adding one later needs a re-extraction of its range.
WATCHED_TOKENS = {hex_of(a): (sym, dec) for a, sym, dec in [
    ("TPYmHEhy5n8TCEfYGqW2rPxsghSfzghPDn", "USDD", 18),     # USDD (original contract)
    ("TXDk8mbtRbXeYuMNS83CfKPaYYT8XWv9Hz", "USDD", 18),     # USDD (2025 contract)
    ("TEkxiTehnzSmSe2XqrBj4w32RUN966rdz8", "USDC", 6),
    ("TUpMhErZL2fhh4sVNULAbNKLokS4GjC1F4", "TUSD", 18),
    ("TMz2SWatiAtZVVcH2ebpsbVtYwUPT9EdjH", "BUSD", 18),
    ("TLeVfrdym8RoJreJ23dAGyfJDygRtiWKBZ", "A7A5", 6),      # ruble stablecoin, Garantex / Grinex (OFAC 2025-08-14)
    ("TNUC9Qb1rRpS5CbWLmNMxXBjyFoydXjWFR", "WTRX", 6),
]}

# Contracts whose every log and every direct call is kept raw (usdt.watched_logs / usdt.watched_calls).
WATCHED_CONTRACTS = {hex_of(a): name for a, name in [
    # USDT's owner() (verified on the node): every blacklist / destroy / issue goes through it as submit -> confirm
    # -> execute, so a pending freeze, and its target in the submit call data, is visible before AddedBlackList.
    ("TBPxhVAsuzoFnKyXtc1o2UySEydPHgATto", "usdt_owner_multisig"),
    # GasFree controller (proxy): its events name the real user behind a GasFree transfer, whose USDT Transfer
    # shows only the per-user GasFree contract as `from`.
    ("TFFAMQLZybALaLb4uxHA9RBE7pxhUAjF3U", "gasfree_controller"),
]}

# Transaction types whose signatures are kept when signed by a non-owner permission or by several keys
# (usdt.tx_signers), so the key that actually signed can be recovered later: value moves and control changes.
# Resource delegation is left out on purpose: rental platforms sign millions of those with active permissions,
# and they say nothing about who owns the funds (resource_delegations still carries permission_id).
SIGNER_TYPES = {"TransferContract", "TransferAssetContract", "TriggerSmartContract", "AccountPermissionUpdateContract",
                "AccountUpdateContract", "CreateSmartContract", "AccountCreateContract"}

# 4-byte selectors of the calls the USDT contract takes directly
SELECTORS = {
    "a9059cbb": "transfer",       # transfer(address,uint256)
    "23b872dd": "transferFrom",   # transferFrom(address,address,uint256)
    "095ea7b3": "approve",        # approve(address,uint256)
    "cc872b66": "issue",          # issue(uint256)
    "db006a75": "redeem",         # redeem(uint256)
}
assert TRANSFER == "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
assert APPROVAL == "8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925"
assert addr(USDT_HEX) == "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
