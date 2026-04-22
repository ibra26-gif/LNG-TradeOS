// api/argentina-enargas.js — Vercel Serverless Function
// Serves Argentina gas balance from data/argentina_gas_balance.csv (long
// format, scraped monthly by scripts/argentina_enargas_fetch.py from the
// three ENARGAS XLSX files: GRT.xlsx, GETD.xlsx, Exportaciones.xlsx).
//
// CSV schema:
//   month,kind,id,thousand_m3_month
//   2026-02,supply,tgn_neuquina,924329
//   2026-02,demand,dist_residencial,282894
//   2026-02,export,chile_gasandes,190051
//
// Maps to the platform's AR_D row shape consumed by samLoadArgentina():
//   { rows: [ {id, lbl, s:'sup'|'dem'|'dem-exp'|'dem-fut', v:[12 MCM/D] }, ... ] }
//
// Bolivia pipeline imports (YABOG) and Escobar FSRU LNG imports are NOT
// published in these ENARGAS sheets; those rows are returned as null and
// the front-end renders them as em-dashes. The Net Trade KPI treats them
// as 0 via a front-end guard.

import { readFileSync } from 'fs';
import { join } from 'path';

const CSV_PATH = join(process.cwd(), 'data', 'argentina_gas_balance.csv');

// Display row → { lbl, section, csv kind, csv id list }. Section codes
// mirror what AR_D expects:
//   'sup'     → supply rows
//   'dem'     → non-export demand
//   'dem-exp' → pipeline exports (Chile · Brazil · Uruguay)
//   'dem-fut' → placeholder (Southern Energy FLNG, from 2027)
const ROW_MAP = [
  { id:'prod', lbl:'Domestic production (TGN + TGS + Dist own/other)', s:'sup', kind:'supply',
    ids:['tgn_neuquina','tgn_noroeste','tgn_otros',
         'tgs_neuquina','tgs_san_jorge','tgs_austral','tgs_otros',
         'dist_propios','dist_otros'] },
  // Bolivia and LNG imports aren't in the ENARGAS sheet — null means "not sourced".
  { id:'bol',  lbl:'Bolivia pipeline imports (YABOG)',               s:'sup', kind:null, ids:[] },
  { id:'lng',  lbl:'LNG imports (Escobar FSRU)',                     s:'sup', kind:null, ids:[] },

  { id:'res',  lbl:'Residential',                                    s:'dem', kind:'demand',
    ids:['dist_residencial'] },
  { id:'com',  lbl:'Commercial (incl. sub-distribution, public sector)', s:'dem', kind:'demand',
    ids:['dist_comercial','dist_entes_oficiales','dist_subdistribuidor'] },
  { id:'ind',  lbl:'Industrial (GEGU + distribution + RTP)',         s:'dem', kind:'demand',
    ids:['dist_industria','trans_industria','trans_rtp','trans_subdistribuidor'] },
  { id:'pow',  lbl:'Power generation (CEPU)',                        s:'dem', kind:'demand',
    ids:['dist_centrales','trans_centrales'] },
  { id:'gnc',  lbl:'Automotive (CNG / GNC)',                         s:'dem', kind:'demand',
    ids:['dist_gnc'] },
  { id:'oth',  lbl:'Other',                                          s:'dem', kind:null, ids:[] },

  { id:'excl', lbl:'Exports to Chile (pipeline)',                    s:'dem-exp', kind:'export',
    ids:['chile_gasandes','chile_norandino','chile_egs','chile_methanex_ypf'] },
  { id:'exot', lbl:'Exports · other (Brazil + Uruguay)',             s:'dem-exp', kind:'export',
    ids:['brasil_ypf_uruguayana','uruguay_petrouruguay','uruguay_gas_link'] },

  { id:'flng', lbl:'Future LNG Exports (Southern Energy, YPF-Shell)', s:'dem-fut', kind:null, ids:[] },
];

function parseCSV(text) {
  const lines = text.trim().split(/\r?\n/);
  const months = new Set();
  const byKey = new Map();
  for (let i = 1; i < lines.length; i++) {
    const parts = lines[i].split(',');
    if (parts.length < 4) continue;
    const [m, k, id, vRaw] = parts;
    const v = parseFloat(vRaw);
    if (!isFinite(v)) continue;
    months.add(m);
    byKey.set(`${m}|${k}|${id}`, v);
  }
  return { months, byKey };
}

function lastNMonths(months, n) {
  return [...months].sort().slice(-n);
}

function daysInMonth(ym) {
  const [y, m] = ym.split('-').map(Number);
  return new Date(y, m, 0).getDate();
}

// Sum thousand_m3/month across CSV ids, then convert to MCM/day.
function aggMcmDay(byKey, month, kind, ids) {
  let sum = 0, any = false;
  for (const id of ids) {
    const v = byKey.get(`${month}|${kind}|${id}`);
    if (v != null) { sum += v; any = true; }
  }
  if (!any) return null;
  return +(sum / daysInMonth(month) / 1000).toFixed(2);
}

export default function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Cache-Control', 's-maxage=3600, stale-while-revalidate=86400');

  try {
    const text = readFileSync(CSV_PATH, 'utf-8');
    const { months, byKey } = parseCSV(text);
    const last12 = lastNMonths(months, 12);

    const rows = ROW_MAP.map(def => {
      const v = last12.map(m => {
        if (!def.kind || !def.ids.length) return null;
        return aggMcmDay(byKey, m, def.kind, def.ids);
      });
      return { id: def.id, lbl: def.lbl, s: def.s, v };
    });

    return res.status(200).json({
      ok: true,
      rows,
      months: last12,
      meta: {
        source: 'ENARGAS — GRT.xlsx (supply by basin) · GETD.xlsx (demand by user type) · Exportaciones.xlsx',
        rowCount: rows.length,
        coverageMonths: last12.length,
        latest: last12[last12.length - 1] || null,
        warnings: [
          'Bolivia pipeline (YABOG) and Escobar FSRU LNG imports are not published in these ENARGAS sheets; those rows return null.',
        ],
      },
    });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e.message });
  }
}
