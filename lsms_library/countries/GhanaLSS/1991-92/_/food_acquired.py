#!/usr/bin/env python
"""GhanaLSS 1991-92 canonical ``food_acquired``.

Emits the canonical long form with index ``(t, i, j, u, s, visit)`` and
columns ``[Quantity, Expenditure, Price]`` (Uganda ``food_acquired_to_canonical``
contract PLUS the GhanaLSS-local ``visit`` level -- DESIGN doc D1: keep the
repeated-visit recall structure; the DERIVED tables sum it out).

``s`` in {purchased, produced}:
  * purchased (S9B / Code_9b) -- value-only: u='Value', Expenditure=value,
    Quantity=Expenditure (no fabricated/imputed qty; Phase-3 out of scope).
  * produced  (S8H / Code_8h) -- real Quantity + decoded unit ``u``
    (GH #348: s8hq13 decoded via the wave ``units`` table) + farmgate Price;
    Expenditure stays NaN (no produced value recorded this wave).

``i`` is built via this wave's canonical ``mapping.i()`` (clust + zero-padded
nh, no separator) so ``food.i`` matches ``sample.i`` / ``roster.i`` -- fixes the
100% NaN-v bug (GH #256): the old script used format_id(clust)+format_id(nh)
WITHOUT the 2-digit zero-pad ("30022" vs the canonical "300202").

``v`` is intentionally absent -- the framework joins it from ``sample()`` at API
time (``_join_v_from_sample``; see CLAUDE.md "sample() and Cluster Identity").

FIELD DESIGN -- GLSS3 IS STRATIFIED, and no other GhanaLSS wave is.
``Documentation/pdf/G3Intman.pdf`` section 1.9: "There will be 8 interviewer
visits at 2-day intervals to each rural household and 11 interviewer visits at
3-day intervals to each urban household in a cycle.  Thus a workload for the 16
days in a rural cluster is 10 households, while a workload for the 33 days in an
urban cluster is 15 households."  Both modules are asked at every visit after
the first (p.42: "Part H is to be administered on every visit except the
first").  So the number of recall occasions AND the length of each differ by
stratum: urban 10 asks x 3 days = 30 days, rural 7 asks x 2 days = 14 days.

That shows in the PURCHASED rows exactly as it should -- ~45,000 rows at each of
visits 2..8, then ~20,300 at 9..11 once the rural households have run out.  It
does NOT show in the produced rows, and that is a PRE-EXISTING defect this
change does not fix: ``Price`` comes from ``s8hq14``, a single column per
(i, j, u), so ``wide_to_long`` replicates it across all ten visit rows and the
closing ``dropna(how='all')`` keeps a row whose only content is that price.
Measured 2026-09-09: 28,678-28,710 produced rows at EVERY visit, of which
``Quantity`` is non-null in 13,810 at visit 2 falling to 598 / 519 / 493 at
visits 9 / 10 / 11 -- so 206,498 produced rows carry a price and no quantity,
and a rural household that was visited eight times has price rows at visits it
was never asked.  Do not read a produced ROW COUNT here as an observation
count; count ``Quantity.notna()``.  See GH #851.

The consequence is real and is NOT handled here: summing ``visit`` away in
``food_expenditures`` returns a 30-day figure for an urban household and a
14-day figure for a rural one, so the urban/rural ratio is off by ~2.14x unless
the analyst rescales.  GSS did rescale -- its published ``POV_GH.expfoodc`` is
~11.4x the raw 9B sum for urban and ~21.7x for rural, against 365/30 = 12.2 and
365/14 = 26.1.  See GH #851 and ``GhanaLSS/_/CONTENTS.org``.
"""
import sys
import numpy as np
import pandas as pd
sys.path.append('.')
sys.path.append('../../_')
sys.path.append('../../../_/')
import mapping
from lsms_library.local_tools import (get_categorical_mapping, format_id,
                                      df_data_grabber, _to_numeric,
                                      to_parquet, df_from_orgfile)
from lsms_library.paths import countries_root
import warnings

t = '1991-92'

####################
# Purchased (S9B)
####################
# value-only this wave: u='Value', Expenditure=value, Quantity=Expenditure.
labelsd = get_categorical_mapping(tablename='harmonize_food',
                                  idxvars={'Code':('Code_9b',format_id)},
                                  **{'Label':'Preferred Label'})

idxvars = dict(i=(['clust','nh'], lambda x: mapping.i(pd.Series([x.clust, x.nh]))),
               t=('nh', lambda x: t),
               j=('fdexpcd', lambda x: labelsd[format_id(x)]))

# Keep visits separate.  Source question numbers are NOT visit numbers: 9B
# q1..q10 are asked at the 2nd..11th visits, so the value written to `visit`
# is k+1.  G3QPartB.pdf p.20 prints q1 as "How much was spent on ... since my
# FIRST visit?" and q2..q10 as "since my last visit"; the form's eleven date
# boxes are labelled 1st..11th, and "1st" has no question column of its own.
#
# q1 WAS NOT READ before 2026-09-09 (`range(2,11)` started at the second
# stem).  It is the largest column in the module -- 58,132 positive values,
# C12,428,840, 16.22% of all recorded GLSS3 food purchase expenditure -- and
# it was silently dropped from every GLSS3 number this library has served.
myvars = {f"Purchased_v{i + 1}":(f"s9bq{i}",_to_numeric) for i in range(1,11)}

x = df_data_grabber('../Data/S9B.DTA',idxvars,**myvars)

x = x.groupby(['i','t','j']).sum()  # collapse multiple records per (i,t,j)
x = x.replace(0,np.nan)

x = pd.wide_to_long(x.reset_index(),['Purchased'],['i','t','j'],'visit',sep='_v')

# Purchases: u='Value'; Expenditure=value; Quantity mirrors Expenditure.
x['u'] = 'Value'
x['s'] = 'purchased'
x = x.rename(columns={'Purchased':'Expenditure'})
x['Quantity'] = x['Expenditure']
x['Price'] = np.nan
x = x.reset_index().set_index(['t','i','j','u','s','visit'])
x = x[['Quantity','Expenditure','Price']]

####################
# Home produced (S8H)
####################
labelsd = get_categorical_mapping(tablename='harmonize_food',
                                  idxvars={'Code':('Code_8h',format_id)},
                                  **{'Label':'Preferred Label'})
# GH #348: pass the value column ('Label') so the Code->Label dict is built.
# A bare get_categorical_mapping(tablename='units') yields an EMPTY dict (no
# value column requested -> df_data_grabber returns a column-less frame), so
# the wave-level unit decode was silently dead and raw s8hq13 codes
# (1/7/8/9/12/14/16/17/18/19/22/23/25) leaked into food_acquired's u.
unitsd = get_categorical_mapping(tablename='units', Label='Label')

# food quantities.  s8hq13 reads as float (1.0); the units table is keyed on
# string codes ('1'), so normalize via format_id before lookup (same pattern
# as j above) -- otherwise every code misses the dict.  Unknown / missing
# sentinel codes (Stata's large-number NA) map to <NA>.
idxvars = dict(i=(['clust','nh'], lambda x: mapping.i(pd.Series([x.clust, x.nh]))),
               t=('nh', lambda x: t),
               j=('homagrcd', lambda x: labelsd[format_id(x)]),
               u=('s8hq13', lambda x: unitsd.get(format_id(x), pd.NA)))

# Keep visits separate.  8H q3..q12 are the SAME 2nd..11th visits, so the
# value written to `visit` is k-1.  q1/q2 are screeners ("Did the household
# consume any home produced ... in the last 12 months?", "how many months?"),
# which is why the visit columns start at q3; q13 is the unit and q14 the
# selling price.  G3QPartB.pdf p.9.
myvars = {'Price':('s8hq14',_to_numeric)}
myvars.update({f"Produced_v{i - 1}":(f"s8hq{i}",_to_numeric) for i in range(3,13)})

prod = df_data_grabber('../Data/S8H.DTA',idxvars,**myvars)

# Oddity with large number for missing code
na = prod.select_dtypes(exclude=['object', 'category']).max().max()

if na>1e99:  # Missing values?
    warnings.warn(f"Large number used for missing?  Replacing {na} with NaN.")
    prod = prod.replace(na,np.nan)

prod = prod.groupby(['i','t','j','u']).sum()  # collapse multiple records

# Unstack by visits
prod = prod.replace(0,np.nan)
prod = pd.wide_to_long(prod.reset_index(),['Produced'],['i','t','j','u'],'visit',sep='_v')

# Produced: real Quantity + decoded u + farmgate Price; Expenditure NaN.
prod = prod.rename(columns={'Produced':'Quantity'})
prod['s'] = 'produced'
prod['Expenditure'] = np.nan
prod = prod.reset_index().set_index(['t','i','j','u','s','visit'])
prod = prod[['Quantity','Expenditure','Price']]

####################
# Stack purchased + produced
####################
fa = pd.concat([x, prod])

fa = fa.dropna(how='all')

# Drop rows whose food item failed to harmonize (j == '').  Purchases are
# 100% food-mapped this wave per the audit, so this is near-zero.
fa = fa.reset_index()
fa = fa[fa['j'] != '']
fa = fa.set_index(['t','i','j','u','s','visit'])

# --- canonical `u`: the country's `_/unit_labels.org` Preferred-Label axis ----
# RESTORED 2026-09-08.  The country-level `_/food_acquired.py` applied
# `df1['u'].replace(ulabelsd['u']['Preferred Label'])` until c345d317; GH #109
# Phase 2 (6de0ce37) rewrote that script without it and only 2016-17's wave
# script re-implemented it, so this wave has shipped the raw survey spellings
# ('american tin', 'bowl', 'litre', 'Maxi bag') as `u` ever since -- off the
# axis `community_prices` and every other consumer of `u` share.
#
# Two disciplines this country's CONTENTS.org requires:
#   * resolve the table through countries_root(), never a package-relative
#     path, so LSMS_COUNTRIES_ROOT is honoured (Trap 6 / GH #753);
#   * ASSERT the index stays unique.  `food_acquired` is in
#     `_ADDITIVE_MEASURE_COLUMNS`, so a duplicate makes core SUM
#     Quantity/Expenditure and re-derive `Price = Expenditure/Quantity` on the
#     WHOLE frame -- destroying every recorded farmgate price (Trap 9).
#     Measured before this landed: 0 rows in duplicate groups on the delivered
#     5,259,320-row table under this exact map.
_ul = df_from_orgfile(countries_root() / 'GhanaLSS' / '_' / 'unit_labels.org',
                      name='unit_label').dropna()
_umap = dict(zip(_ul['u'].astype(str).str.strip(),
                 _ul['Preferred Label'].astype(str).str.strip()))
assert _umap, 'unit_labels.org: unit_label decoded to an EMPTY dict'
fa = fa.rename(index=lambda x: _umap.get(x, x), level='u')
assert not fa.index.duplicated().any(), (
    'unit canonicalisation collided on (t, i, j, u, s, visit) -- two raw unit '
    'spellings share a Preferred Label for one (household, item, visit); '
    'resolve it here, not in core (CONTENTS.org Trap 9)')

if __name__=='__main__':
    to_parquet(fa,'food_acquired.parquet')
