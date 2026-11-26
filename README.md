# Polymarket Trading MCP Server

A standalone [Model Context Protocol](https://modelcontextprotocol.io) server that
exposes **Polymarket CLOB trading** (market reads + order placement) through the
*official* unified `polymarket-client` Python SDK. It is built for a **legacy
Proxy Wallet** (Magic / email) account and for connecting to **Hermes Agent**
(or any MCP client) over **stdio**.

> **Read first:** keep the agent in **DRY-RUN** (`POLY_ALLOW_LIVE=0`).
> Live BUY is blocked by the server. The existing SELL path still requires
> both live switches and the Proxy-Wallet preflight; the switches are not
> a substitute for operator approval.

---

## What was verified against the official docs/SDK (no assumptions)

- Polymarket's old `@polymarket/clob-client` (TS) and `py-clob-client` (Python)
  are **archived and non-functional**. The supported path is the unified SDK.
- The Python SDK (`polymarket-client==0.20.1`) uses `get_*` method names
  (`get_market`, `get_order_book`, …), *not* the `fetch_*` names shown in the
  TypeScript docs.
- The SDK does **not** accept `signature_type` directly. It infers it by
  re-deriving CREATE2 addresses from (signer, wallet). For a Proxy Wallet it
  must resolve `POLY_PROXY` (`signature_type=1`). If the wallet does not match
  any derived address it **silently falls back** to `SESSION_KEY/DEPOSIT_WALLET`
  (`signature_type=3`) — signing orders for the wrong maker. This codebase
  guards against that with a hard preflight.

Docs read: `docs.polymarket.com` trading / wallets-auth / place-orders /
manage-orders / fees / error-codes / migrate-to-unified-sdk / SDKs & APIs,
cross-checked against the installed `polymarket-client` source.

---

## Tools

**Read (unauthenticated, no keys needed)**
`search_markets`, `get_market`, `get_order_book`, `get_midpoint`, `get_price`,
`get_last_trade_price`, `estimate_market_price`, `preview_buy_depth`

**BTC 5m settlement reads (public, no keys)**
`get_btc5m_snapshot`, `get_btc5m_resolution`, `get_recent_btc5m_resolutions` — on-demand reads only;
the autonomous poller is documented in the settlement section below.
`get_btc5m_snapshot` returns current/next slug, seconds to boundary, PTB, both
live books, market constraints, 5-share fill estimates, and the last N
Binance/Coinbase tape points in **one call**. BUY estimates preserve the
5-share target by converting it to notional from the current best ask and
rounding up to the market tick; if no ask is available, that side reports
`fill.<SIDE>:NoAsk` without discarding the rest of the snapshot. Tape points
include trade size and the feed-reported side when available.

**Live book (WebSocket, no keys needed for public book)**
`subscribe_book`, `unsubscribe_book`, `get_live_book`, `get_live_top_of_book`, `stream_status`,
`stream_events`, `stream_health`, `watch_asset` — see the live-book section
below for durability, freshness, and rollover behavior.

**Credentials (CLOB L2 API keys)**
`derive_api_credentials`, `validate_api_credentials`, `test_connection`,
`list_saved_credentials`, `save_api_credentials`, `revoke_api_credentials`
— see the credentials section below.

**Account (requires keys, read-only)**
`account_preflight`, `get_balance_allowance`, `list_positions`,
`list_open_orders`, `get_trading_approvals_state`

**Order — MARKET ONLY (no limit orders)**
`place_market_order`, `preview_order`, `place_batch_order`, `track_order`,
`cancel_order`, `cancel_all_orders`, `setup_trading_approvals`

> Limit orders are intentionally **not supported** in this deployment.
> Per operator, all order paths use market orders only. Post-only/GTC/GTD
> heartbeat mechanics do not apply because market orders never rest on the
> book; their remainder is canceled immediately.

---

## Install

```bash
cd polymarket-mcp
python3 -m venv .venv
.venv/bin/pip install -e .          # runtime only
.venv/bin/pip install -e '.[dev]'   # runtime + pytest + ruff
.venv/bin/ruff check .              # lint
.venv/bin/python -m pytest -q
```

No hard-coded developer paths are required. The stdio test harness derives the
project root from `__file__` and stubs HTTP in the child process, so the suite is
path-independent and offline.

## Configure (secrets live only on your machine)

```bash
cp .env.example .env   # then fill on YOUR machine; never commit it
```

| Variable | Meaning |
|----------|---------|
| `POLY_PRIVATE_KEY` | Signer EOA private key (still required for a Proxy Wallet) |
| `POLY_WALLET` | Your **proxy/funder** address (polymarket.com → Profile → Deposit address) |
| `POLY_API_KEY/SECRET/PASSPHRASE` | Optional existing CLOB creds; omit to derive |
| `POLY_BUILDER_CODE` | Optional attribution code |
| `POLY_ALLOW_LIVE` | `1` arms live mode; keep `0` for DRY-RUN |
| `POLY_MAX_NOTIONAL` | Per-order USD cap (0 = unset) — set this |
| `POLY_MAX_OPEN_ORDERS` | Open-order cap (0 = unset) |
| `POLY_GUARD_STATE_DIR` | Process environment only. Private local state directory; defaults to `~/.local/state/polymarket-mcp`. Example: `POLY_GUARD_STATE_DIR="$HOME/.local/state/polymarket-agent"`. |
| `POLY_NONCE` | Derivation nonce if your API key used one |

Preflight before anything else: call `account_preflight` and confirm
`"proxy_ok": true`, `wallet_type: "POLY_PROXY"`, `signature_type: 1`.

## Run

```bash
.venv/bin/polymarket-mcp           # stdio MCP server (for Hermes / any client)
```

## Register in Hermes (NOT done automatically)

Hermes reads MCP servers from `~/.hermes/config.yaml` → `mcp_servers`. Add this
**manually** when you are ready (Hermes then needs `hermes restart` or a new
gateway session to pick it up):

```yaml
mcp_servers:
  polymarket:
    command: /home/trubs/polymarket-mcp/.venv/bin/polymarket-mcp
    args: []
    env:
      POLY_PRIVATE_KEY: "${POLY_PRIVATE_KEY}"
      POLY_WALLET: "${POLY_WALLET}"
      POLY_ALLOW_LIVE: "0"
      POLY_MAX_NOTIONAL: "100"
      POLY_MAX_OPEN_ORDERS: "5"
```

Hmm: `${...}` interpolation in Hermes' YAML — verify it resolves your OS env
(or paste absolute values, but then never commit the YAML). The command above
is an example starting point, **not** an instruction to edit Hermes config;
this repo does not touch `~/.hermes/config.yaml`.

## Autonomous mode (server-side)

Set `POLY_AUTONOMOUS=1` in the agent's MCP process environment (default `0`).
Restart the process to apply changes. The server registers only a built-in
set of data/preview tools and forces both order tools into DRY-RUN, even if
`POLY_ALLOW_LIVE=1` and the caller sends `confirm=true`. Cancellation,
approvals, credential tools, and authenticated account tools are never
registered in autonomous mode. Keep `POLY_ALLOW_LIVE=0` as well.

`POLY_AUTONOMOUS_TOOLS` can narrow the built-in set to comma-separated exact
names. Unset means the full built-in set; an explicitly empty list registers
no tools. Example: `POLY_AUTONOMOUS_TOOLS=guard_status,get_btc5m_snapshot,preview_buy_depth`.
Listing a privileged tool here does not make it available.

## BUY depth preview and live-order gate

Keep `POLY_ALLOW_LIVE=0` (the default) for agent use. The order tools
otherwise require both `POLY_ALLOW_LIVE=1` and `confirm=true` to submit.
Even with both set, the server rejects live BUYs, including batches that
contain a BUY, before signing or submitting any order. No configuration
switch overrides this BUY block.

Use `preview_buy_depth(market, outcome)` to estimate a BUY from the public
order book. It consumes the cheapest eligible asks first, up to **5 shares**
at **0.72 USD/share** or less. For example, two shares at 0.50 and three at
0.55 cost **2.65 USD before fees**. Less eligible depth means a smaller
estimate. At the price and quantity ceilings, the book notional is at most
3.60 USD before fees. The separate **5 USD including fees** ceiling is
reported but not verified by this preview. The book timestamp is provided
when available; this is not a freshness check, fill guarantee, or a test of
minimum order size, tick precision, or order eligibility.

The installed SDK (`polymarket-client==0.20.1`) takes USD notional for a
market BUY, and its `max_spend` is an estimate based on recent fee rates.
This deployment does not have a verified execution path that enforces both
the share cap and the all-in cap. Existing DRY-RUN order previews and BTC
snapshot estimates are unchanged; use `preview_buy_depth` for the capped
book-depth calculation.

## BUY budget and round reconciliation

`reconcile_round(window_start)` is an operator-only evidence view. It reads the
exact BTC 5m round's open orders and positions using existing CLOB credentials;
it never creates credentials, submits, retries, or releases a claim. Absence
from those two lists does not establish that an order did not fill.

The SQLite BUY ledger is tested independently of submission. Its day boundary
is midnight UTC. A reservation requires a positive daily cap and atomically
claims the round. SDK-typed `unmatched` and `not_enough_balance` release the
reservation, not the claim. A match keeps the reservation charged; unknown or
interrupted outcomes also stay charged. SDK 0.20.1 does not expose `INVALID`
as a typed rejection, so ambiguous errors are not treated as no-fill. The BUY
execution path is disabled; these tools do not create live reservations.

## Guard audit and watchdog

The server appends guard decisions and SDK submission outcomes to
`guard-audit.jsonl` in `POLY_GUARD_STATE_DIR`. Each record contains a Unix
timestamp, server-generated request ID, tool name, event and outcome; batch
outcomes also include a zero-based response index. Tool arguments, wallet
and order IDs, credentials, signatures, raw SDK responses and exception
messages are not recorded. Covered tools are `preview_buy_depth`,
`preview_order`, `place_market_order`, `place_batch_order`, `cancel_order`,
`cancel_all_orders`, and `setup_trading_approvals`.

`guard_status` returns the last 10 decisions from the current process
(oldest first) plus audit and heartbeat health and failure counts. The JSONL
file persists across restarts; the status history does not. Audit failure
leaves DRY-RUN responses unchanged but refuses audited live operations before
SDK submission. After invoking the SDK, an audit failure does not replace
its response or trigger another submission. `unknown` is not proof of no
fill, and `submit_started` without a recorded result requires investigation,
not a retry.

The CLI atomically replaces `guard-heartbeat.json` at startup and every 10
seconds while its event loop runs. The file contains `pid` and Unix
`timestamp`. Use a dedicated state directory per server process. A watchdog
should check the expected PID and treat a missing or stale heartbeat as
unhealthy (for example, older than 30 seconds). The last file stays in place
when the process stops; a fresh heartbeat does not attest to exchange or
upstream feed health. New state directories and files use modes `0700` and
`0600`; use a private directory on a local filesystem. The server does not
rotate or truncate the audit log; monitor disk use and archive it externally
when the server is stopped.

## Testing (offline, no funds)

```bash
.venv/bin/ruff check .
.venv/bin/python -m pytest -q
```

The suite covers the BUY depth preview and live BUY block, Proxy preflight
(including the misclassification trap), the two-factor gate, live-book durability
(book snapshot authority, out-of-order/drop invalidation, freshness thresholds,
rollover isolation), and the secret-leak regression scan (sentinel secrets).

Live read tools were also exercised against real CLOB/Gamma endpoints with
**no keys** (public client only) and the live BTC 5m book streamed at
~4.000 events/6s with `age≈0–10ms`. No test places a real order.

## Live order book (WebSocket, durable)

`polymarket_mcp/live.py` provides a resilient live book on top of the SDK's
`subscribe(MarketSpec)` (docs: `wss://ws-subscriptions-clob.polymarket.com/ws/market` +
`docs.polymarket.com/market-data/realtime-data.md`).

Key durability facts, verified in source (not assumed):

- SDK owns socket lifecycle: app-level `PING` every 10s, 30s stale watchdog,
  jittered exponential backoff reconnect (base 0.25s cap 30s). Multiplexes
  multiple `MarketSpec` handles onto ONE socket; a second `subscribe()` on
  the same socket only sends a diff frame — it does NOT reconnect.
- Market events carry **no** sequence number (that field exists only on the
  PolyBolt reference-price channel). We never invent ordering: we track
  `payload.timestamp` only, and event timestamps are monotonically increasing
  — a violation is dropped and counted (`out_of_order_drops`).
- Staleness is data-level: every asset has a watchdog with explicit
  `availability` (`live`/`aging`/`stale`), `age_ms`, `last_event_at`,
  `reconnections`, `dropped_events`. Freshness thresholds default to
  `fresh_ms=2000`, `stale_ms=15000`, `no_event_ms=8000` (all env-overridable).
- BTC 5m rollover: slug `btc-updown-5m-{epoch//300*300}`. `RolloverWatcher`
  prefetches the next window's asset_id long before the boundary and adds it
  to the SAME socket, so the WS is never torn down at the boundary.
  Condition IDs differ per window, so current/next books are isolated — no
  cross-contamination.

Tools: `subscribe_book`, `unsubscribe_book`, `get_live_book` (with REST
fallback when cold/stale), `get_live_top_of_book` (compact timestamped quote),
`stream_status`, `stream_events`,
`stream_health`, `watch_asset`.

`start_market_data` takes explicit `binance_symbol` and `coinbase_symbol`
arguments. The signal runner supplies its configured policy symbols so the
MCP feed cannot silently substitute another quote pair.

**Round subscription (auto-rollover)**
`subscribe_round(slug="", outcome="yes")` resolves the current (or given)
BTC 5m round, subscribes its asset to the live CLOB socket and starts the
`RolloverWatcher`, which prefetches the next round before the 300-second
boundary on the same socket. `round_status()` reports current/next slugs,
seconds to boundary and prefetched slugs; `stop_round_watch()` stops the
watcher. At subscription and at every prefetch the next round is **REST-bootstrapped first**
(`get_order_book` → `clob_rest_snapshot`) and only then attached to the WS reader, so the
book is already fresh (`availability=live`) before the first stream event; the watchdog
also refreshes stale assets from REST. The live feed itself remains `clob_ws`.
Read-only — no order is placed.

**Authenticated user stream (official `user` topic)**
`start_user_stream(markets=None)`, `stop_user_stream()`,
`user_stream_status()`, `user_stream_events(limit=50)`. Subscribes via
`AsyncSecureClient.subscribe(UserSpec(...))` to Polymarket's documented
`user` stream and buffers `order` / `trade` events (newest last). Requires
configured CLOB credentials; subscribing never places or cancels orders.
Stream errors are surfaced in `user_stream_status().error`.

**Price-to-Beat feed authentication**
`start_ptb_feed`, `stop_ptb_feed`, and `ptb_status` manage the authenticated
PolyBolt TWAP feed. `auth_failed`, `auth_invalid`, and `auth_required` are
terminal for the current feed instance: it stops instead of entering reconnect
backoff. `ptb_status` exposes `auth_failed` and `last_error`; after correcting
credentials, call `stop_ptb_feed` before starting a fresh feed instance.

## Credentials (CLOB L2, automatic lifecycle)

`polymarket_mcp/credentials.py` implements the full credential ladder,
read from the SDK's own code:

- **Derive**: `sign_api_key_auth` (ClobAuth EIP-712, chain 137, nonce) →
  `POST /auth/api-key`, falling back to `GET /auth/derive-api-key` only on
  400 (`create_or_derive_api_key` in `_internal/actions/auth.py`). Opt-in
  across deployments is a separate, per-operator decision — e.g. migrating
  to Deposit Wallet Flow can re-derive credentials under the new signer/type.
- **Validate**: L2-signed `GET /auth/api-keys` probe (`_credentials_are_active`).
  401 = invalid. `fetch_api_keys` is also callable directly.
- **Persist**: 0600 JSON file (`~/.config/polymarket-mcp/credentials.json`) via
  atomic temp-file + rename. `ApiKeyCreds` has `repr=False` on every field,
  so logging it is safe by construction; this module additionally never
  returns `key`/`secret`/`passphrase` in any dict.
- **Revoke**: `delete_api_key` (DELETE `/auth/api-key`) with optional
  rotate-before-revoke (derive a fresh credential first, e.g. with a
  different nonce) so the account is never left keyless.
- **Builder/Relayer**: exposed as scaffold only — they need Builder/Relayer
  API keys from polymarket.com and this deployment does not mint or store them.

Tools: `derive_api_credentials`, `validate_api_credentials`,
`test_connection`, `list_saved_credentials`, `save_api_credentials`,
`revoke_api_credentials`.

## Market orders only

This deployment intentionally does **not** expose limit orders. Reasons and
docs:

- Market orders trade against available liquidity immediately; they never
  rest on the book (`docs.polymarket.com/trading/place-orders.md` — "A
  market order never rests on the book").
- SDK supports market orders with `FAK` (default) and `FOK` modes only.
  Kill. This server only exposes `place_market_order` (plus `preview_order`
  and `place_batch_order` for market orders), and all of them route through
  the two-factor gate.
- Heartbeat/GTD/GTD-cooldown mechanics are limit-order mechanics; they do
  not apply here.

## Settlement: auto-polling mandiri (tanpa agent)

### Masalah yang diperbaiki

`get_btc5m_resolution(window_start, wait_seconds=...)` hanya melakukan polling
**di dalam satu panggilan tool**. `wait_seconds` dibatasi maksimal 60 detik,
jadi kalau settlement belum keluar atau agent tidak memanggil lagi, hasil
tetap `pending`. Itu bukan mekanisme settlement yang otonom.

Solusinya adalah proses terpisah:

```bash
polymarket-settlement-watcher \
  --interval 15 \
  --lookback 12 \
  --output "$HOME/.config/polymarket-mcp/settlements.jsonl"
```

`polymarket-settlement-watcher` dipasang bersama paket ini (entry point di
`pyproject.toml`). Dia **tidak** memakai MCP, tidak butuh agent, tidak butuh API
key, dan tidak mengirim order. Dia hanya membaca Gamma + Data API v2 lewat
`polymarket_mcp/resolution.py`.

### Perilaku

- Setiap `--interval` detik (default `15`) dia memeriksa `--lookback` window
  5 menit terakhir (default `12` = 1 jam). Window di masa depan otomatis
  dilewati sampai `window_end`.
- State `pending`, `market_not_found`, atau `error` **di-polling ulang**; tidak
  dianggap final. Hanya `resolved` yang ditulis.
- Penulisan **idempoten**: saat start dia membaca file JSONL yang sudah ada dan
  menandai `window_start` yang pernah ditulis, sehingga restart tidak
  menghasilkan duplikat.
- Suatu window hanya ditulis sekali, dengan `observed_at` (unix epoch) plus
  payload lengkap dari `get_btc5m_resolution`.
- `SIGINT`/`SIGTERM` dihentikan dengan anggun. `--once` menjalankan satu cycle
  lalu keluar (cocok untuk smoke test / cron).

### Format output (JSONL, satu baris per window resolved)

```json
{"observed_at":1795426500,"slug":"btc-updown-5m-1795426200","window_start":1795426200,"window_end":1795426500,"state":"resolved","winner":"Up","condition_id":"0x...","market_version":"v1","gamma_resolution_status":"resolved","resolution_source":"...","data_api_resolution":{"status":"resolved","payouts":[1000000,0]},"outcomes":[{"outcome":"Up","resolved":true,"final_price":1.0,"token_id":"...","outcome_index":0}]}
```

Field penting:

| Field | Arti |
|---|---|
| `slug` | Event Gamma `btc-updown-5m-{epoch}` (pola ini **EMPIRIS**, bukan kontrak resmi docs). |
| `window_start` / `window_end` | Batas jendela 300 detik (UTC epoch). |
| `state` | `resolved`, `pending`, `window_not_ended`, `market_not_found`, atau `error`. |
| `winner` | `Up`/`Down` bila token `resolved=true` dan `final_price >= 0.999` (toleransi EMPIRIS untuk float). |
| `condition_id` | Dipakai untuk query `/v2/resolutions` dan `/v2/tokens`. |
| `market_version` | `v1` untuk market CTF saat ini; tool tetap menangani cabang `v2`. |
| `gamma_resolution_status` | `umaResolutionStatus` untuk v1, `resolutionStatus` untuk v2. |
| `data_api_resolution` | Baris mentah `/v2/resolutions` (`status`, `payouts`, `resolved_at`, dll). |
| `outcomes` | Tiap token: `outcome`, `resolved`, `final_price`, `token_id`, `outcome_index`. |

### Sumber data resmi

- Gamma: `GET /events/slug/{slug}` → https://docs.polymarket.com/api-spec/gamma-openapi.yaml
- Data API v2: `GET /v2/resolutions?condition=...` → https://docs.polymarket.com/api-reference/data-api/overview
- Data API v2: `GET /v2/tokens?condition=...` → https://docs.polymarket.com/market-data/public-analytics.md

Semua response memakai envelope `data`; miss = `data: null`/`[]`, bukan error.
Tidak ada Data API v1 yang dipakai.

### Menjalankan sebagai service (disarankan)

Server MCP tetap **on-demand via stdio** — dia menyala saat client memanggil,
bukan daemon standby. Settlement watcher adalah **proses terpisah** yang memang
harus hidup terus. Contoh systemd user unit:

```ini
[Unit]
Description=Polymarket BTC5m settlement watcher
After=network-online.target

[Service]
Type=simple
WorkingDirectory=/absolute/path/to/polymarket-mcp
ExecStart=/absolute/path/to/polymarket-mcp/.venv/bin/polymarket-settlement-watcher --interval 15 --lookback 12
Restart=always
RestartSec=10
StandardOutput=append:/absolute/path/to/polymarket-mcp/settlement-watcher.log
StandardError=append:/absolute/path/to/polymarket-mcp/settlement-watcher.log

[Install]
WantedBy=default.target
```

```bash
systemctl --user daemon-reload
systemctl --user enable --now polymarket-settlement-watcher.service
systemctl --user status polymarket-settlement-watcher.service
journalctl --user -u polymarket-settlement-watcher.service -f
```

Smoke test tanpa daemon:

```bash
polymarket-settlement-watcher --once --lookback 3 --output /tmp/settlements.jsonl
cat /tmp/settlements.jsonl
```

### Catatan operasional

- --output bisa besar: satu baris per window resolved (±288/hari). Rotasi
  dengan `logrotate` bila dipakai lama.
- Jika `winner` null tetapi `state=resolved`, cek `outcomes` dan
  `data_api_resolution.payouts` — itu berarti label outcome tidak `Up`/`Down`.
- `error` biasanya sementara (429/503/jaringan); watcher akan mencoba lagi.
  `-error` di stderr tidak menghentikan proses.
- Clock harus sinkron (UTC). `window_start` harus kelipatan 300.
- Ini pembacaan publik, bukan jaminan pembayaran. DYOR.

## Safety notes

- Secrets are read from env only, never logged, and never return their values.
- Neg-Risk Adapter approvals are **not** covered by `setup_trading_approvals`;
  check `state.neg_risk` and `get_trading_approvals_state` per market.
- Rate limits / error codes (400/401/425/429/503) are surfaced by the SDK; this
  server does not yet add its own retry/backoff — treat a surfaced error as
  "retry later," never "assume it filled." Check `list_open_orders`.
- DYOR. This is tooling, not financial advice.

## Transport: HTTP (streamable) vs stdio

MCP mendukung dua transport (`mcp==2.3.0` MCPServer, dengan fallback FastMCP 1.x):

| Transport | Cara jalan | Kapan dipakai |
|---|---|---|
| `stdio` (default) | `polymarket-mcp --transport stdio` | Hermes `mcp_servers` dengan `command`/`args`. Dikonfigurasi di `~/.hermes/config.yaml` (manual). |
| `streamable-http` | `polymarket-mcp --transport streamable-http --host 127.0.0.1 --port 8000 --path /mcp` | Server sebagai web service (mis. InstaBox VM dengan port forwarding). Client HTTP hubungi `/mcp`. |
| `sse` | `polymarket-mcp --transport sse --host 127.0.0.1 --port 8000` | Legacy SSE transport. |

**Host/port tidak dibaca dari environment oleh SDK** — mereka adalah constructor
args `FastMCP(host=..., port=...)`. Module ini mem-parsing CLI flags `--host`,
`--port`, `--path` dan men-set `mcp.settings` sebelum `run()`.

**Keamanan HTTP:** default bind `127.0.0.1`. Non-loopback wajib menggunakan
`POLY_HTTP_TOKEN` dari environment proses. Host dan Origin diperiksa untuk
HTTP dan SSE; Origin browser ditolak secara default. Stdio tidak memakai
bearer HTTP.

**Test yang sudah dijalankan:**

- stdio: 39 tools terdaftar, semua tools merespons.
- HTTP: server bind di `127.0.0.1:18000`, `POST /mcp` initialize mengembalikan
  `serverInfo.polymarket-trading@1.30.0` dan `tools/list` mengembalikan daftar
  tool lengkap (search_markets, get_market, dsb.) via session header
  `Mcp-Session-Id`.

MCP URL untuk client HTTP: `http://127.0.0.1:8000/mcp` (atau host/port yang
dipilih).

## HTTP security (for VM / external access)

Non-loopback binds require `POLY_HTTP_TOKEN` exported before startup. When
configured, the token is required on loopback HTTP too. Send it as a Bearer
token over TLS. The verifier uses `hmac.compare_digest`.

`POLY_HTTP_ALLOWED_HOSTS` accepts comma-separated exact Host values. By default
only the bind host, with or without the configured port, is accepted. Wildcard
binds (`0.0.0.0` or `::`) require an explicit list. Browser Origins are denied
unless explicitly listed in `POLY_HTTP_ALLOWED_ORIGINS` as exact HTTP(S) origins.
Native clients may omit Origin. These settings come from the process environment.

After exporting a token from your secret manager, for example:

```bash
POLY_HTTP_ALLOWED_HOSTS=mcp.example.test \
polymarket-mcp --transport streamable-http --host 0.0.0.0 --port 8000 --path /mcp
```
