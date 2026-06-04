#!/usr/bin/env python3
"""Build EthiopiaRHS panel_ids.json / updated_ids.json (GH #271).

ERHS panel linkage (recon 2026-05-19):

- 1994a/1994b/1995 all read the SAME pooled person-panel demo123.dta
  with one household key (q1c, hhid) -> (q1c,hhid) is invariant across
  R1-R3 by construction.  Links among them are the identity on i.
- 1997 (age_sex_r4.dta) keys (q1c, q1d); q1d == the R1-3 hhid, same
  q1c code space -> 1997 <-> 1995 is the identity on i (99.4%).
- demo123.q2 ("HH no. in 1989/90 survey") back-links R1-3 -> 1989.
  q2 is the SAME packed 5-digit key as demog89_1.hhid but carries a
  sign flag on ~29 HH (use abs) and 4 junk non-5-digit values (drop).
  Only ~31% of R1-3 HH have q2; ~62% of those resolve into the
  *partial* 1989 demog89_1 roster -- the documented pre-expansion
  subset gap, not attrition error.

Canonical baseline 1994a; chain 1989 -> 1994a -> 1994b -> 1995 ->
1997.  Composite i + the abs/junk q2 transform are not YAML-path
expressible, so this is the script path (data_scheme: panel_ids:
!make).  IDs are built with the SHARED ``ethiopiarhs.i`` formatter so
panel_ids.json keys match Country.household_roster() index exactly
(the diagnostics ``panel_ids_targets_exist`` check enforces this).
  Reads use convert_categoricals=False so q1c is the numeric
  code -- IDENTICAL to the roster's code-based extraction
  (a decoded-name i would silently mismatch the roster index).

No household-split variable exists in ERHS (attrition is person-level
in the roster, not an id change) -> updated_ids.json is empty.
"""
import json

import pandas as pd

from lsms_library.local_tools import get_dataframe
from ethiopiarhs import i as _i


def _cid(village, hh):
    """Composite household i (1994+), via the shared formatter."""
    return _i(pd.Series([village, hh]))


def _sid(hh):
    """Scalar household i (1989 packed hhid), via the shared formatter."""
    return _i(hh)


# --- 1989 roster id set (resolve target existence) ---------------------
d89 = get_dataframe('../1989/Data/demog89_1.dta', convert_categoricals=False)
ids_1989 = {_sid(h) for h in d89['hhid'].dropna().unique()}

# --- R1-3 pooled file: (q1c,hhid) + q2 backlink to 1989 ----------------
d123 = get_dataframe('../1994a/Data/demo123.dta', convert_categoricals=False)
hh123 = d123[['q1c', 'hhid', 'q2']].dropna(subset=['q1c', 'hhid'])
hh123 = hh123.drop_duplicates(subset=['q1c', 'hhid'])

D = {}            # "cur_wave,cur_i" -> "prev_wave,prev_i"
for _, r in hh123.iterrows():
    cur = _cid(r['q1c'], r['hhid'])
    # 1994a -> 1989 via q2 (abs; drop non-5-digit junk; target must
    # exist in the partial 1989 roster).
    q2 = r['q2']
    if pd.notna(q2):
        n = int(abs(q2))
        if 10000 <= n <= 99999:
            prev = _sid(float(n))
            if prev in ids_1989:
                D[f'1994a,{cur}'] = f'1989,{prev}'
    # R1-3 identity chain (same demo123 (q1c,hhid)).
    D[f'1994b,{cur}'] = f'1994a,{cur}'
    D[f'1995,{cur}'] = f'1994b,{cur}'

# --- 1997 <-> 1995 identity on (q1c,q1d)==(q1c,hhid) -------------------
r1_3_ids = {_cid(v, h) for v, h in
            zip(hh123['q1c'], hh123['hhid'])}
asr4 = get_dataframe('../1997/Data/age_sex_r4.dta', convert_categoricals=False)
hh97 = (asr4[['q1c', 'q1d']].dropna()
        .drop_duplicates(subset=['q1c', 'q1d']))
for _, r in hh97.iterrows():
    cur = _cid(r['q1c'], r['q1d'])
    if cur in r1_3_ids:                # household carried from 1995
        D[f'1997,{cur}'] = f'1995,{cur}'

with open('panel_ids.json', 'w') as f:
    json.dump(D, f)

# No household-split tracking in ERHS.
with open('updated_ids.json', 'w') as f:
    json.dump({}, f)

print(f'panel_ids.json: {len(D)} edges '
      f'(1994a->1989: {sum(k.startswith("1994a,") for k in D)}; '
      f'1994b->1994a: {sum(k.startswith("1994b,") for k in D)}; '
      f'1995->1994b: {sum(k.startswith("1995,") for k in D)}; '
      f'1997->1995: {sum(k.startswith("1997,") for k in D)})')
