#!/usr/bin/env python3
"""Compare data/korea.json against the prior committed version (HEAD~)
and print 'changed' or 'unchanged' so the workflow can skip noisy commits.

Material change criteria:
  - Any reactor's status flips (Operational <-> Maintenance/Offline)
  - A reactor's output_mwe changes by > 50 MWe
  - khnp totals (totalOnlineGW / onlineCount) change
  - khnpTrips byYear changes (annual trip counts updated)
  - khnpAnnual byYear changes (annual util updated)
  - fx.krwUsd changes by > 0.5 KRW

Volatile fields (scrapedAt, asOf, updated) are ignored — those tick every
minute regardless of underlying data.
"""
import json
import subprocess
import sys


CURRENT_PATH = 'data/korea.json'


def load_prior():
    """Read the previous korea.json from git HEAD (the last committed version)."""
    try:
        out = subprocess.run(
            ['git', 'show', f'HEAD:{CURRENT_PATH}'],
            check=True, capture_output=True, text=True,
        )
        return json.loads(out.stdout)
    except (subprocess.CalledProcessError, json.JSONDecodeError):
        return None


def reactor_map(d):
    """Map: reactor name -> (status, rounded MWe)"""
    k = (d or {}).get('khnp') or {}
    out = {}
    for r in (k.get('reactors') or []):
        out[r.get('name')] = (
            r.get('status'),
            round(r.get('output_mwe') or 0),
        )
    return out


def has_material_change(old, new):
    if old is None:
        return True, 'no prior version (first commit)'

    # FX
    old_fx = ((old.get('fx') or {}).get('krwUsd')) or 0
    new_fx = ((new.get('fx') or {}).get('krwUsd')) or 0
    if abs(new_fx - old_fx) > 0.5:
        return True, f'FX changed {old_fx} → {new_fx}'

    # Khnp totals
    old_k = old.get('khnp') or {}
    new_k = new.get('khnp') or {}
    if abs((new_k.get('totalOnlineGW') or 0) - (old_k.get('totalOnlineGW') or 0)) > 0.05:
        return True, f"online GW changed {old_k.get('totalOnlineGW')} → {new_k.get('totalOnlineGW')}"
    if (new_k.get('onlineCount') or 0) != (old_k.get('onlineCount') or 0):
        return True, f"online count changed {old_k.get('onlineCount')} → {new_k.get('onlineCount')}"

    # Per-reactor status flips or material output change
    old_m, new_m = reactor_map(old), reactor_map(new)
    for name, (new_st, new_mwe) in new_m.items():
        old_st, old_mwe = old_m.get(name, (None, 0))
        if new_st != old_st:
            return True, f'{name} status {old_st} → {new_st}'
        if abs(new_mwe - old_mwe) > 50:
            return True, f'{name} output {old_mwe} → {new_mwe} MWe'

    # Annual util / trips updated (rare — yearly)
    if (old.get('khnpAnnual') or {}).get('byYear') != (new.get('khnpAnnual') or {}).get('byYear'):
        return True, 'annual util byYear updated'
    if (old.get('khnpTrips') or {}).get('byYear') != (new.get('khnpTrips') or {}).get('byYear'):
        return True, 'annual trips byYear updated'

    return False, 'no material change'


def main():
    try:
        with open(CURRENT_PATH, 'r', encoding='utf-8') as f:
            new = json.load(f)
    except Exception as e:
        print(f'changed (read error: {e})')
        sys.exit(0)
    old = load_prior()
    changed, reason = has_material_change(old, new)
    print('changed' if changed else 'unchanged')
    print(reason, file=sys.stderr)
    sys.exit(0)


if __name__ == '__main__':
    main()
