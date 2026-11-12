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
