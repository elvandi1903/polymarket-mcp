# Polymarket Trading MCP Server

A standalone [Model Context Protocol](https://modelcontextprotocol.io) server that
exposes **Polymarket CLOB trading** (market reads + order placement) through the
*official* unified `polymarket-client` Python SDK. It is built for a **legacy
Proxy Wallet** (Magic / email) account and for connecting to **Hermes Agent**
(or any MCP client) over **stdio**.

> **Read first:** this is a money-moving tool. It ships **read-only by default**
> (DRY-RUN). A live order requires two independent switches AND passes a
> Proxy-Wallet preflight. No order is ever placed silently.

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

## Tools (55)

**Read (unauthenticated, no keys needed)**
`search_markets`, `get_market`, `get_order_book`, `get_midpoint`, `get_price`,
`get_last_trade_price`, `estimate_market_price`

**BTC 5m settlement reads (public, no keys)**
`get_btc5m_snapshot`, `get_btc5m_resolution`, `get_recent_btc5m_resolutions` — on-demand reads only;
the autonomous poller is documented in the settlement section below.
`get_btc5m_snapshot` returns current/next slug, seconds to boundary, PTB, both
live books, 5-share fill estimates and the last N Binance/Coinbase tape points in
**one call** — use it for fast round entry instead of a multi-call chain.

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
.venv/bin/python -m pytest -q       # 75 tests
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

## The two-factor gate

A state-changing call is **LIVE only if both** `POLY_ALLOW_LIVE=1` **and**
`confirm=true`. Otherwise it returns a full DRY-RUN preview (market
constraints, resolved `asset_id`, tick/min-size) and sends nothing.

## Testing (offline, no funds)

```bash
.venv/bin/python -m pytest tests -q   # 36 passed
```

The suite covers the Proxy preflight (including the misclassification trap),
the two-factor gate, live-book durability (book snapshot authority,
out-of-order/drop invalidation, freshness thresholds, rollover isolation),
and the secret-leak regression scan (all tools × sentinel secrets).

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

**Keamanan HTTP:** secara default tidak ada auth pada server HTTP ini
(`transport_security=None`, `auth=None` di FastMCP). Binding default adalah
`127.0.0.1` saja — aman untuk single-machine. Jika Anda mengekspos ke publik/
InstaBox, gunakan auth di reverse proxy atau jangan expose ke `0.0.0.0` tanpa
perlindungan.

**Test yang sudah dijalankan:**

- stdio: 39 tools terdaftar, semua tools merespons.
- HTTP: server bind di `127.0.0.1:18000`, `POST /mcp` initialize mengembalikan
  `serverInfo.polymarket-trading@1.30.0` dan `tools/list` mengembalikan daftar
  tool lengkap (search_markets, get_market, dsb.) via session header
  `Mcp-Session-Id`.

MCP URL untuk client HTTP: `http://127.0.0.1:8000/mcp` (atau host/port yang
dipilih).

## HTTP security (for VM / external access)

Muse di luar VM (VM luar) akan akses MCP HTTP sebagai client. Untuk itu:

1. **Bind loopback hanya** kalau MCP dan client di mesin yang sama:
   `--host 127.0.0.1`. Kalau beda mesin, bind ke IP/0.0.0.0 agar port forwarding
   InstaBox bisa diarahkan, tetapi **wajib** pakai token (poin 2).

2. **Bearer token wajib** (`POLY_HTTP_TOKEN`): server akan menolak request
   tanpa token (`401 invalid_token`) atau dengan token salah. Token dibuat
   sebagai 64-char base64 (`secrets.token_urlsafe(48)`) dan disimpan di
   `~/.config/polymarket-mcp/http_token.txt` (0600) dan `.env` (0600).
   Client mengirim header:
   ```
   Authorization: Bearer <token>
   ```

3. **Token tidak pernah di-log atau dikembalikan di response.** Verifier
   hanya membandingkan string; tidak ada token yang dicetak ke stdout/stderr.

4. **TLS/reverse-proxy disarankan** kalau expose ke publik (InstaBox public
   port). HTTPS terminasi di InstaBox edge; tunnel tetap memerlukan token.

5. **stdio tidak terkena auth HTTP** — token hanya berlaku untuk
   `streamable-http`/`sse`. Mode stdio tetap untuk Hermes lokal.

Contoh run aman untuk VM luar:

```bash
POLY_HTTP_TOKEN=$(cat ~/.config/polymarket-mcp/http_token.txt) \
polymarket-mcp --transport streamable-http --host 0.0.0.0 --port 8000 --path /mcp
# client VM luar: POST http://<vm-ip>:8000/mcp  dengan header
# Authorization: Bearer <token>
```

Tes yang sudah dijalankan:
- Unauthenticated POST → 401 invalid_token.
- Wrong token → 401.
- Valid token → 200 + `serverInfo.polymarket-trading@1.30.0` + tools/list.
