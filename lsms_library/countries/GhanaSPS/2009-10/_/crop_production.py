#!/usr/bin/env python
"""GhanaSPS 2009-10 crop_production -> canonical (t, i, plot_id, j, u, season).

TWO SEASONS, TWO WIDE FILES, MELTED.  Section 4 Part A(v) records, per plot,
up to five harvest records for the last MAJOR season (S4AV1.dta, A79-A119)
and up to five for the last MINOR season (S4AV2.dta, A120b-A160).  Each slot
is a block of one crop id, one part id, quantity + unit, market value and
"total revenue from this crop harvest (sold)", the money as cedis + pesewas.

`season` IS ASSIGNED FROM THE FILE, NOT THE LABELS.  Every quantity label in
S4AV2 reads "... harvested in the last major season" and is wrong; the
questionnaire (Documentation/Household_questionnaire_Part_A.pdf) heads that
block "LAST MINOR SEASON: CROP (HARVESTS) 1" and asks at A122 "What is the
quantity harvested in the last minor season?", and S4AVI2's own label agrees
("In the past minor season, have you used chemical 1").  Trusting the .dta
labels would stamp both blocks major and double-count the season.

Slot map (major / minor), per crop slot k = 1..5:
    crop id   a80i  a88i  a96i  a104i a112i  /  a121i a129i a137i a145i a153i
    part      a80ii a88ii a96ii a104ii a112ii / a121ii ...
    quantity  a81i  a89i  a97i  a105i a113i  /  a122i a130i a138i a146i a154i
    unit code a81ii a89ii a97ii a105ii a113ii / a122ii a130ii a138ii a146ii a154ii
    revenue   a83   a91   a99   a107  a115   /  a124  a132  a140  a148  a156  (i = cedis, ii = pesewas)

Decoding, all through helpers in ../../_/ghanasps.py so the YAML waves and
this script agree:
  - crop id -> label via the codebook list (CODE_BOOK.pdf "CROP CODES" 01-43;
    the harvest-slot id has no value labels) -> `harmonize_crop` Preferred
    Label -> `product_label` qualifies it by the harvested PART where that is
    a different product ("e.g. Cocoa, Cocoa Leaves", A80.2).  The part is not
    the canonical `condition` and is not force-fitted into it.
  - unit code -> units.org `unit09` -> `harmonizedunit` Preferred Label, the
    same chain 2009-10/_/food_acquired.py uses.  Codes 0 / -1 and a missing
    unit under a quantity -> `Unknown`; a positive code the codebook does not
    define (1, 46, 50, 51, 53, 57, 58, 61, 80, 94) stays as the code string,
    an accepted residual per the add-feature/food-acquired/units skill.
  - Value_sold = A83/A124 cedis + pesewas/100 (revenue from THIS harvest,
    "(sold)"), NaN where neither is recorded.  S4BI's B71 is NOT used: Part B
    is a season-less 12-month sales roster ("B62. Were there any crops ...
    sold to other people last 12 months?") whose crop rows match a major
    harvest slot on (plot, crop) for only 2,976 of 4,752 rows.
  - Quantity_sold is not asked at this grain in 2009-10 (NaN by design).
  - harvest_month is a PROXY: the ending month of the major / minor season of
    the plot's DOMINANT crop (S4AIX1 A289.2 / S4AIX5 A326.2), stamped on every
    harvest record of that plot-season; the later waves record the month(s)
    per crop.

Rows dropped, deliberately and counted on stdout: slots with no crop id but
a datum; crop codes the instrument does not define (0, 44-47, 90); a null plot
number (measured 0); slots with a crop id but no quantity, unit or revenue.  Duplicate (t, i, plot_id, j, u, season) lines -- two
harvest events of one product in one unit -- are SUMMED (min_count=1), a
bounded reducer whose group count the test pins.
"""
import sys

sys.path.append('../../_/')

import numpy as np
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet, format_id
from ghanasps import (_CROP_COLUMNS, _CROP_INDEX, _CROP_UNIT_UNKNOWN,
                      _W1_CROP_CODES, _W1_PART_CODES, crop_label_map,
                      month_string, product_label,
                      sum_duplicate_harvest_records, w1_unit_labels)

t = '2009-10'

SEASONS = {
    'major': {
        'file': '../Data/S4AV1.dta', 'plot': 's4av1_plotno',
        'slots': [('s4v_a80i', 's4v_a80ii', 's4v_a81i', 's4v_a81ii', 's4v_a83i', 's4v_a83ii'),
                  ('s4v_a88i', 's4v_a88ii', 's4v_a89i', 's4v_a89ii', 's4v_a91i', 's4v_a91ii'),
                  ('s4v_a96i', 's4v_a96ii', 's4v_a97i', 's4v_a97ii', 's4v_a99i', 's4v_a99ii'),
                  ('s4v_a104i', 's4v_a104ii', 's4v_a105i', 's4v_a105ii', 's4v_a107i', 's4v_a107ii'),
                  ('s4v_a112i', 's4v_a112ii', 's4v_a113i', 's4v_a113ii', 's4v_a115i', 's4v_a115ii')],
        'months': ('../Data/S4AIX1.dta', 's4aix1_plotno', 's4aix_289ii'),
    },
    'minor': {
        'file': '../Data/S4AV2.dta', 'plot': 's4av2_plotno',
        'slots': [('s4v_a121i', 's4v_a121ii', 's4v_a122i', 's4v_a122ii', 's4v_a124i', 's4v_a124ii'),
                  ('s4v_a129i', 's4v_a129ii', 's4v_a130i', 's4v_a130ii', 's4v_a132i', 's4v_a132ii'),
                  ('s4v_a137i', 's4v_a137ii', 's4v_a138i', 's4v_a138ii', 's4v_a140i', 's4v_a140ii'),
                  ('s4v_a145i', 's4v_a145ii', 's4v_a146i', 's4v_a146ii', 's4v_a148i', 's4v_a148ii'),
                  ('s4v_a153i', 's4v_a153ii', 's4v_a154i', 's4v_a154ii', 's4v_a156i', 's4v_a156ii')],
        'months': ('../Data/S4AIX5.dta', 's4aix5_plotno', 's4aix_326ii'),
    },
}

label_map = crop_label_map()
unit_map = w1_unit_labels()


def melt_season(season, spec):
    df = get_dataframe(spec['file'], convert_categoricals=False)
    pieces = []
    for k, (cid, part, qty, unit, cedis, pesewas) in enumerate(spec['slots'], start=1):
        piece = pd.DataFrame({
            'hhno': df['hhno'].to_numpy(),
            'plotno': df[spec['plot']].to_numpy(),
            'crop_code': pd.to_numeric(df[cid], errors='coerce').to_numpy(),
            'part_code': pd.to_numeric(df[part], errors='coerce').to_numpy(),
            'Quantity': pd.to_numeric(df[qty], errors='coerce').to_numpy(),
            'unit_code': pd.to_numeric(df[unit], errors='coerce').to_numpy(),
            'cedis': pd.to_numeric(df[cedis], errors='coerce').to_numpy(),
            'pesewas': pd.to_numeric(df[pesewas], errors='coerce').to_numpy(),
            'slot': k,
        })
        pieces.append(piece)
    long = pd.concat(pieces, ignore_index=True)
    # a harvest record is a slot carrying any datum
    datum = long[['crop_code', 'Quantity', 'unit_code', 'cedis', 'pesewas']].notna().any(axis=1)
    long = long[datum].copy()
    n_records = len(long)

    # drops, counted
    no_crop = long['crop_code'].isna()
    undefined = ~long['crop_code'].isin(list(_W1_CROP_CODES)) & ~no_crop
    no_plot = long['plotno'].isna()
    # a slot with a crop id but no quantity, no unit and no revenue is not a
    # harvest record (the plot-level month proxy alone would otherwise carry
    # it through _finalize_result's dropna(how='all'))
    no_datum = long[['Quantity', 'unit_code', 'cedis', 'pesewas']].isna().all(axis=1)
    print(f'{t} {season}: {n_records:,} harvest records; dropped {int(no_crop.sum())} with no crop id, '
          f'{int(undefined.sum())} with an undefined crop code '
          f'{sorted(long.loc[undefined, "crop_code"].astype(int).unique().tolist())}, '
          f'{int(no_plot.sum())} with a null plot number, '
          f'{int((no_datum & ~no_crop & ~undefined).sum())} with a crop id but no quantity, unit or revenue')
    long = long[~no_crop & ~undefined & ~no_plot & ~no_datum].copy()

    # j: code -> instrument label -> harmonize_crop -> qualified by part
    crop_raw = long['crop_code'].astype(int).map(_W1_CROP_CODES)
    crop = crop_raw.map(lambda s: label_map.get(s, s))
    unmapped = sorted(set(crop_raw[crop == crop_raw]) - set(label_map.values()))
    assert not unmapped, f'{t}: crop labels missing from harmonize_crop: {unmapped}'
    part = long['part_code'].map(_W1_PART_CODES)
    long['j'] = [product_label(c, p) for c, p in zip(crop, part)]

    # u: code -> unit09 -> harmonizedunit; 0 / -1 / missing -> Unknown;
    # undefined positive codes stay as the code string (accepted residual)
    code = long['unit_code']
    u = code.map(unit_map)
    residual = u.isna() & code.notna() & (code > 0)
    u = u.where(~residual, code.where(residual).map(lambda x: str(int(x)) if pd.notna(x) else x))
    u = u.where(u.notna(), _CROP_UNIT_UNKNOWN)
    long['u'] = u.astype(str)
    print(f'{t} {season}: unit residual codes (kept as code strings): '
          f'{long.loc[residual, "u"].value_counts().to_dict()}; Unknown: {int((long["u"] == _CROP_UNIT_UNKNOWN).sum())}')

    # Value_sold = cedis + pesewas/100, NaN where neither is recorded
    reported = long['cedis'].notna() | long['pesewas'].notna()
    long['Value_sold'] = (long['cedis'].fillna(0) + long['pesewas'].fillna(0) / 100).where(reported)
    long['Quantity_sold'] = np.nan

    # harvest_month proxy: ending month of the season of the plot's dominant crop
    mfile, mplot, mcol = spec['months']
    m = get_dataframe(mfile, convert_categoricals=False)[['hhno', mplot, mcol]].rename(
        columns={mplot: 'plotno', mcol: 'end_month'})
    m = m[m['plotno'].notna()].drop_duplicates(['hhno', 'plotno'])
    long = long.merge(m, on=['hhno', 'plotno'], how='left')
    long['harvest_month'] = [month_string([v]) for v in long['end_month']]

    long['t'] = t
    long['i'] = long['hhno'].map(format_id)
    long['plot_id'] = long['plotno'].map(format_id)
    long['season'] = season
    return long[list(_CROP_INDEX) + list(_CROP_COLUMNS)]


parts = [melt_season(s, spec) for s, spec in SEASONS.items()]
flat = pd.concat(parts, ignore_index=True)
flat, n_groups = sum_duplicate_harvest_records(flat)
print(f'{t}: {len(flat):,} rows after summing {n_groups} duplicate (t, i, plot_id, j, u, season) groups; '
      f'per season {flat.groupby("season").size().to_dict()}')

flat['harvest_month'] = flat['harvest_month'].astype('string')
out = flat.set_index(list(_CROP_INDEX))[list(_CROP_COLUMNS)].sort_index()
assert len(out) > 0, f'crop_production produced no rows for {t}'
assert out.index.is_unique, f'Non-unique crop_production index for {t}'
assert set(out.index.get_level_values('season')) == {'major', 'minor'}
to_parquet(out, 'crop_production.parquet')
