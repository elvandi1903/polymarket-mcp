"""Polymarket MCP server (read + order) built on the official unified SDK.

Design notes (do not "fix" without source):
- Python SDK uses get_* method names (not fetch_* seen in TS docs).
- The SDK derives signature_type by CREATE2-reclassifying (signer, wallet).
  A Proxy Wallet must classify as POLY_PROXY; otherwise the SDK silently
  falls back to SESSION_KEY/DEPOSIT_WALLET (signature_type 3) and the
  order is signed for the wrong maker. Preflight aborts on that.
- Trading only ever runs through a token + server-flag gate; default is
  dry-run, so a stray agent/tool call cannot move real funds.
"""

__all__ = ["client", "config", "safety", "server"]
__version__ = "0.1.0"
