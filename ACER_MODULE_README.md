[ACER_MODULE_README.md](https://github.com/user-attachments/files/26647755/ACER_MODULE_README.md)
# ACER Historical Physical Price — Module Addition

**Added in v127** · April 2026

---

## What this adds

A new tab in **Physical Trading → HISTORICAL ACER** providing:

1. **Historical chart** — NWE / SE / EU physical DES price ($/MMBtu) with period/window selector
2. **Seasonal chart** — Monthly distribution vs historical band (10–90th pct, 2023–2025 baseline) with period overlay
3. **EU LNG Send-out** — Seasonal EU aggregate send-out in MCM/day (GIE AGGI+), shifted M+1 to reflect the cargo trading → delivery lag
4. **Gate LNG breakeven** — Reference line at TTF −$0.15/MMBtu (NWE max variable regas cost, Spark Commodities data)
5. **Daily update** — PDF parser for YYMMDD_LNGPA.pdf uploaded to Google Drive

---

## Files

| File | Description |
|---|---|
| `acer_physical.html` | Standalone version (self-contained, no dependencies) |
| `api/acer.js` | Vercel edge proxy for ACER TERMINAL (CORS bypass) |
| `lngtradeos_v127.html` | Main platform with ACER tab integrated |

---

## Methodology

### Physical price derivation

```
TTF_implied  = EU_DES − ACER_Benchmark          (EUR/MWh)
Physical_NWE = NWE_DES − TTF_implied            (EUR/MWh)
Physical_SE  = SE_DES  − TTF_implied            (EUR/MWh)
Physical_EU  = ACER_Benchmark                   (EUR/MWh, direct)

$/MMBtu      = EUR/MWh × EUR/USD(1.09) ÷ 3.41214
```

### ACER Benchmark

`ACER_Benchmark` = EU DES LNG Spot − ICE TTF Gas Futures Front-Month Settlement (ICE Endex daily)

**Rolling window:** Up to 10 working days. Transactions normalised to a spread vs TTF at transaction time, then geometrically time-weighted (recent transactions carry higher weight). Minimum 5 transactions to publish.

**Zones:**
- **NWE** — Belgium, Netherlands, France (Atlantic), Germany, UK
- **SE** — Spain, France (Mediterranean), Italy, Croatia (KrK), Greece (Revithoussa)
- **EU** — Aggregate across all zones
- Klaipeda (Lithuania) feeds EU aggregate only — not NWE or SE

### Regas holder constraint

```
TTF(M+1) − variable_cost − DES > 0

DES = TTF(M) − x   [x = ACER physical, negative when DES < TTF]

→  TTF(M+1) − TTF(M) − variable_cost > −x
   curve_carry − variable_cost > ACER_physical
```

The ACER physical reading alone is insufficient — the full signal requires curve carry (M1-M2 spread).

### Terminal variable regas costs (Spark Commodities, Jan 2025)

| Terminal | EUR/MWh | $/MMBtu | Category |
|---|---|---|---|
| Dunkerque | ~0.05 | ~0.016 | NWE cheapest |
| Fos Cavaou / Spain TVB | ~0.08 | ~0.026 | SE cheapest |
| **Gate (Rotterdam)** | **~0.10** | **~0.15** | **NWE max — reference line** |
| Montoir | ~0.38 | ~0.121 | NWE |
| Brunsbuttel (FSRU) | ~0.50 | ~0.160 | NWE FSRU |
| Adriatic LNG | ~0.53 | ~0.169 | SE Italian |
| KRK Croatia | ~0.60 | ~0.191 | SE Adriatic |
| OLT Toscana | ~0.65 | ~0.207 | SE Italian FSRU |
| Zeebrugge | ~0.70 | ~0.223 | NWE |
| Revithoussa | ~0.75 | ~0.239 | SE Greek |
| Klaipeda | ~1.05 | ~0.335 | Baltic FSRU |
| Wilhelmshaven 1 (FSRU) | ~1.50 | ~0.479 | NWE German FSRU |

**Key implication:** Italian (Adriatic/OLT/Piombino) and Greek terminals price against PSV/local hub premiums above TTF — positive SE ACER readings are not necessarily distress bids, they reflect regional market economics.

### Send-out M+1 shift

Cargoes are traded in month M for delivery in month M+1. The send-out chart X-axis is shifted accordingly:

```
Physical chart:   Jan Feb Mar Apr ... Dec   [trading month]
Send-out chart:   Feb Mar Apr May ... Jan   [delivery month]
```

Columns align vertically: ACER physical in Jan corresponds to send-out in Feb.

### Unit conversion for send-out

```
MCM/day = GWh/day ÷ 10.55    (EU standard calorific value: 10.55 kWh/m³)
```

---

## Data sources

| Data | Source | Frequency |
|---|---|---|
| ACER NWE/SE/EU DES + Benchmark | ACER TERMINAL (`aegis.acer.europa.eu`) | Daily (weekdays) |
| Embedded history | CSV export from ACER TERMINAL | Mar 2023 → Apr 2026 (730 sessions) |
| Daily update | `YYMMDD_LNGPA.pdf` → Google Drive → UPDATE button | Daily |
| EU LNG send-out | GIE AGGI+ API | Daily |

---

## Daily update routine

1. Download `YYMMDD_LNGPA.pdf` from ACER TERMINAL (publicly available, no login required)
2. Upload to Google Drive folder (same folder as EOD curve files: `18CJsgeFbLzmW3fV4I5XEz8nGsRHd7WQq`)
3. Click **↺ UPDATE PDF** in the HISTORICAL ACER tab
4. PDF.js parses the report, extracts NWE/SE/EU DES prices + Benchmark + transaction count
5. New row merged into dataset; both charts refresh

**Transaction count warning:** Sessions with `< 5` transactions are flagged (ACER methodology marks these as potentially unreliable). The reading is still stored and shown.

---

## Vercel proxy (`api/acer.js`)

Required because `aegis.acer.europa.eu` does not serve CORS headers.

| Endpoint | Description |
|---|---|
| `GET /api/acer?action=list` | Scrapes TERMINAL index page, returns all file IDs |
| `GET /api/acer?action=pdf&id=N` | Proxies PDF binary for file ID N |
| `GET /api/acer?action=csv` | Proxies historical CSV download |

To load full history (all ~800 PDFs since Jan 2023): call `list` to get all IDs, then iterate `pdf&id=N` for each, parse with PDF.js, build the dataset. This replaces the embedded dataset in `acer_physical.html`.

Environment variables required: none (ACER TERMINAL is public).

---

## Integration into lngtradeos_v127.html

Changes from v126:

1. **CSS** — ACER module styles added to main `<style>` block, scoped to `#acer-wrap`
2. **HTML** — New tab button `ptab-acer` in section-physical subnav
3. **JS** — `PH_T` and `PH_D` dictionaries updated with `acer` key
4. **JS** — New `else if(tab==='acer')` case in `phTab()` function
5. **HTML** — Hidden `<div id="acer-tpl">` containing ACER module HTML (after main script close, before `</html>`)
6. **JS** — Namespaced ACER script block (all functions/vars prefixed `acer_` / `ACER_`) appended after v126 code

All ACER identifiers are namespaced to avoid collisions with existing v126 globals.
