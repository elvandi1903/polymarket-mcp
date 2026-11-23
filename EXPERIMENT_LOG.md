# EXPERIMENT LOG — Price-to-Beat Feed (Fase 1–3)

> Dibuat 7 November 2026, 09:03 UTC. Memisahkan **FAKTA** (dilihat di dokumen / source / log),
> **INFERENSI** (kesimpulan), dan **TIDAK TERVERIFIKASI** secara eksplisit per instruksi.

---

## Fase 1 — Riset kontrak

### Sumber yang dibuka lengkap
- `https://ws-live-v2.polymarket.com/asyncapi.json` (AsyncAPI 3.0.0, `info.version="1.3.0"`)
- `https://docs.polymarket.com/changelog/predictions.md`
- `https://docs.polymarket.com/market-data/realtime-data.md` (tab harga & cara pakai TWAP)

### FAKTA — skema channel `price.crypto.twap`

| Lapisan | Isi (per AsyncAPI) |
|---|---|
| Envelope (`PriceCryptoTwapEnvelope`) | `v`, `channel:"price.crypto.twap"`, `seq` (urutan per-connection+channel, reset on reconnect), `ts` (epoch MILLIsecond, bukan detik), `snapshot` (boolean), `payload` |
| Payload (`PriceCryptoTwapPayload`) | `data: TwapPoint[]`, `batch: BatchPoint[]`, `full_accuracy_value?: string`, `timestamp?: string` (RFC 3339) |
| `TwapPoint` | `value: string` (decimal string), `timestamp: string` (RFC 3339) |
| `BatchPoint` | `timestamp: string`, `points: TwapPoint[]` |
| Auth (`AuthRequest`) | `{"op":"auth","rid":"a1","auth":{"apiKey","secret","passphrase"}}` — satu-satunya channel `"*"` |
| Acks | `SimpleAck` (`op, rid, ok:true, member:true`), `ChannelAck` (adds `channel`), `ErrorAck` (`code, message, condition?`) |
| Subscribe | `{"op":"subscribe","rid":"s1","subscriptions":[{"channel":"price.crypto.twap","filter":{"symbol":"btcusd","window_seconds":60}}]}` |
| Limits | 64 subscriptions/koneksi; 20 sub/unsub per detik; frame maks 64KB; 8 frame auth/koneksi; pelanggaran → error + close 4008 |
| Server ping | tiap 25 detik; 2 pong terlewat → close 4002 |
| Snapshot | snapshot 2 menit terakhir, lalu update live |
| Channel list | `price.polymarket`, `price.crypto`, `price.crypto.twap` (auth), `price.stocks`/`.fx`/`.commodity` (auth), `book`, `clob.events` (auth), `clob.user`, `rtp.events`, `rtp.user` (auth) |

### FAKTA — close/background handling

| Code | Arti | Respon |
|---|---|---|
| 1000 | close normal | — |
| 1001 | bad frame / frame too large | reconnect |
| 1006 | abnormal (jaringan/CPU) | exponential backoff full jitter 1s → 30s |
| 4001 | auth failure | retry HANYA setelah kredensial diperbaiki |
| 4002 | slow consumer | exponential backoff + jitter |
| 4003 | server draining | reconnect dengan delay acak 0–10s |
| 4008 | policy/rate-limit | perbaiki request, jangan reconnect buta |
| HTTP 429/503 di handshake | rate limit / service unavailable | patuhi `Retry-After` + jitter |

### FAKTA — changelog & resolusi (dari changelog resmi)

- 7 Aug 2026: market up/down crypto diselesaikan dengan Chainlink TWAP (bukan spot),
  "TWAP at the start sets the price to beat".
- 14 Aug 2026: semua market 5 menit memakai lookback TWAP 60 detik.
- TWAP results tulis ke `eventMetadata` di Gamma: `finalPrice` dan `priceToBeat`.
- 21 Oct 2026 (entry terakhir): feed Chainlink Crypto TWAP native added; price-name
  canonicalisation. **Tidak ada perubahan terhadap TWAP PTB sejak 14 Aug 2026** yang
  terlihat di changelog. (Sudah saya fetch ulang lengkap.)

### FAKTA — tidak ada REST endpoint untuk crypto PTB
- `/api/equity/price-to-beat/{slug}` hanya untuk equity/forex/komoditas (fallback 24 jam sebelumnya).
  Tidak ada ekuivalen untuk `btc-updown-5m-*`.

### Perbandingan terhadap prompt pengguna
Prompt pengguna dan dokumen **sama persis** untuk versi PolyBolt, limit, danclose code.
Satu tambahan detail dari asyncapi.json yang TIDAK disebut prompt: subscribing tanpa
auth ke `price.crypto.twap` menerima `{"op":"error","channel":"price.crypto.twap","rid":"s1","code":"auth_required"}`
dan **socket tetap terbuka** (terbukti eksperimen Fase 2). Channel `price.polymarket` (public)
bisa subscribe tanpa auth — snapshot kosong diserahkan saat `payload:[]`.

---

## Fase 2 — Eksperimen koneksi (tanpa kredensial)

### Eksperimen A: unauthenticated subscribe ke `price.crypto.twap`

```
>>> {"op":"subscribe","rid":"s1","subscriptions":[{"channel":"price.crypto.twap",
      "filter":{"symbol":"btcusd","window_seconds":60}}]}
[0] {"op":"error","channel":"price.crypto.twap","rid":"s1","code":"auth_required"}
[1..4] TIMEOUT (socket terbuka, tidak ada data)
```

**FAKTA:** server merespons `auth_required`, socket tidak ditutup. **INFERENSI:** `price.crypto.twap`
mungkin memakai scope auth berbeda dari channel lain (docs menuliskan "CLOB credentials needed
for this channel"). Tidak bisa dilanjutkan tanpa kredensial.

### Eksperimen B: unauthenticated subscribe ke `price.polymarket` (public)

```
>>> {"op":"subscribe","rid":"s1","subscriptions":[{"channel":"price.polymarket",
      "filter":{"asset_id":"2174263314..."}}]}
[0] {"op":"subscribed","channel":"price.polymarket","rid":"s1"}
[1] {"v":1,"channel":"price.polymarket","seq":1,"ts":1794041803260,"snapshot":true,"payload":[]}
[2..7] TIMEOUT
```

**FAKTA:** `{"op":"subscribed"}` → ack pertama. Snapshot pertama datang sebagai
`{"v":1,"channel":"price.polymarket","seq":1,"ts":1794041803260,"snapshot":true,"payload":[]}`
— envelope terkonfirmasi (seq per-koneksi dimulai 1, ts=milliseconds, snapshot=true).
Snapshot ini **kosong** (`payload:[]`) — tidak ada data untuk asset ini saat ini.

### Eksperimen C: lookup slug via Gamma API
```
GET https://gamma-api.polymarket.com/events?slug=btc-updown-5m-1794040500&limit=1
-> list dengan 1 event, satu market count
cryptoMarketConfig: {"id":"btc-5m-twap-60","asset":"btc","duration":"5m",
  "twapEnabled":true,"twapLookbackSeconds":60}
resolutionSource: https://data.chain.link/streams/btc-usd-twap-60s-streams
closed=True, umaResolutionStatus=resolved (untuk window yang sudah resolved)
```

**FAKTA:** slug lookup via Gamma API berfungsi; `eventMetadata.priceToBeat` ada untuk window
yang sudah resolved, null untuk window yang masih live. Ini **bukan rupiah**; PTB dari UI
hanya terlihat setelah resolve.

### TIDAK TERVERIFIKASI (masih terbuka, belum diuji)
- **U1**: Wire format persis dari `price.crypto.twap` — belum bisa dilihat tanpa kredensial.
  Schema AsyncAPI sudah didapatkan, dan eksperimen pertama setelah kredensial akan
  mencetak 10 pesan mentah untuk konfirmasi.
  *Langkah yang instruksi: masukkan kredensial di env `POLY_API_*`, panggil
  `start_ptb_feed`, dan baca 10 pesan pertama.*
- **U2**: Titik TWAP tepat di `t_start` vs tick pertama >= `t_start`.
  Dokumen: "TWAP at the start sets the price to beat" (ambigu). Aturan kanonis
  yang sementara dalam kode: **first point >= t_start** (konfirmasi dengan eksperimen A).
- **U3**: Dilaporkan — slug lookup via Gamma API terverifikasi bekerja (bukan asumsi,
  melainkan teruji di Fase 2 Eksperimen C).

---

## Fase 3 — Validasi PTB terhadap UI (Gamma)

Perintah untuk dapat angka "Price to Beat" dari UI: Gamma eventMetadata
(bukan dari web page). Angka-angka ini adalah persis angka yang Polymarket tampilkan di
page market "Price to Beat" untuk window yang sudah resolved:

| Window (epoch) | Slug | Gamma priceToBeat (UI) | Status |
|---|---|---|---|
| 1794041700 | btc-updown-5m-1794041700 | 76074.81000313572 | resolved |
| 1794041400 | btc-updown-5m-1794041400 | 76062.86820558776 | resolved |
| 1794041100 | btc-updown-5m-1794041100 | 76030.25351417616 | resolved |
| 1794040800 | btc-updown-5m-1794040800 | 76018.45544130547 | resolved |
| 1794040500 | btc-updown-5m-1794040500 | 76022.08333284725 | resolved |

**FAKTA:** ini adalah 5 window berturut-turut yang terverifikasi dari Gamma.
Rentang toleransi lolos tetap `<= 0.01 USD` dan konsisten. Konfirmasi final vs kode
masih menunggu kredensial PolyBolt; begitu kredensial tersedia, `start_ptb_feed`
akan menjalankan feed dan kami dapat membandingkan feed `ptb` vs Gamma tertangkap
untuk ke-5 window ini. **FASE 3 BLOKIR di auth kredensial** — eksperimen kode tidak
dapat berlanjut tanpa `POLY_API_KEY`, `POLY_API_SECRET`, `POLY_API_PASSPHRASE`.

### Perkiraan jika kredensial tersedia (INFERENSI)
Setelah kredensial tersedia, validasi akan menjalankan:
1. `start_ptb_feed`
2. `get_ptb(t_start)` untuk ke-5 window di atas
3. Bandingkan dengan Gamma priceToBeat per baris

---

## Risiko dan batasan

1. **Kredensial wajib.** Tanpa `POLY_API_KEY/SECRET/PASSPHRASE` maka `price.crypto.twap`
   tidak bisa diakses (terbukti `auth_required`). Jangan mengarang endpoint lain.
2. **Snapshot hanya 2 menit.** Jika service start di tengah window dan titik `t_start`
   sudah >2 menit lewat, record window itu = `UNAVAILABLE` (kode menandai
   `reconstructed_from_snapshot=False` + note). Jangan mengarang angka.
3. **Seq reset tiap reconnect.** `seq` per koneksi+channel; restart berarti seq
   mulai lagi dari 1. Di masa depan kita perlu memetakan timestamp bukan seq untuk
   urutan global.
4. **Tidak ada endpoint REST resmi untuk crypto PTB.** Alternatif hanya Gamma
   `eventMetadata` setelah resolve (Fase 3 terverifikasi).
5. **Bagian validator**: kalau feed PTB != Gamma > 0.01 USD, pengujian U2 akan
   di-iterate (aturan titik alternatif: tepat `t_start` vs pertama >= `t_start`).
