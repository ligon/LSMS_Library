#!/usr/bin/env python
"""GhanaLSS 2005-06 canonical food_acquired.

Emits the canonical long form with a GhanaLSS-local ``visit`` index level:

    index  : (t, i, j, u, s, visit)      -- NO v (joined from sample() at API time)
    columns: [Quantity, Expenditure, Price]
    s      : {purchased, produced}

Purchases (Section 9B / Code_9b) are value-only: Expenditure = recorded value,
u = 'Value', Quantity = Expenditure (no fabricated physical quantity; price-based
imputation is a later, out-of-scope phase).  Production (Section 8H / Code_8h)
carries a real Quantity in a native unit ``u`` plus a farmgate Price; its
Expenditure is left NaN (no produced value is recorded).

Food codes map to canonical ``j`` via the harmonize_food table, decoded on the
NUMERIC CODE axis for both modules and asserted to be exhaustive (GH #782); rows
whose label is empty ('') are the non-food Section-9B block (codes ~165-277) and
are dropped.

Household id ``i`` is built EXACTLY as sample()/household_roster() build it -- by
running this wave's mapping.i() over the pre-composed source ``hhid`` column --
so food.i matches sample.i and v joins cleanly (fixes the GH #256 NaN-v bug).
"""
import sys
sys.path.append('../../_')
sys.path.append('../../../_/')

import numpy as np
import pandas as pd

from lsms_library.local_tools import to_parquet, df_from_orgfile, get_dataframe
from lsms_library.paths import countries_root
import mapping

t = '2005-06'

# --- food-item harmonization (purchases: Code_9b; production: Code_8h) ----------
# BOTH modules decode on the NUMERIC CODE axis.  The org table's Label_9b /
# Label_8h columns transcribe the survey's own label text and are documentation
# only -- decoding on them is fragile, because a transcription is one well-meant
# typo fix away from silently matching nothing (GH #782): the 8h labels for codes
# 91/92 read 'alcoholic beverages' / 'non-alcoholic beverages' in the org table
# but 'alchoholic beverages' / 'non-alchoholic beverages' (sic) in the .dta.
# The code axis has no such failure mode: Code_8h agrees with the data's foodcd
# codes for all 63 items, and Code_9b with freqcd for all 164.
labels = df_from_orgfile('./categorical_mapping.org', name='harmonize_food',
                         encoding='ISO-8859-1')
labelsd = {}
for column in ['Code_9b', 'Code_8h']:
    labelsd[column] = (labels[['Preferred Label', column]]
                       .dropna(subset=[column])
                       .astype({column: int})
                       .set_index(column)['Preferred Label'].to_dict())


def _drop_nonfood(s):
    """Keep only rows whose harmonized j is a non-empty string label."""
    return s.apply(lambda x: isinstance(x, str) and x.strip() != '')


def _assert_decoded(codes, j, column):
    """Fail loudly if any source code fell through the harmonize_food map.

    ``.map()`` yields NaN for an unmatched code, which ``_drop_nonfood`` would
    then discard silently; ``.replace()`` (used here before GH #782) was worse
    still -- it passed the unmatched value through unchanged, so 149,047 rows of
    2005-06 own production shipped the survey's raw labels as ``j`` for years.
    Neither failure is acceptable without a noise, so assert coverage instead.
    """
    missing = sorted(set(codes[j.isna()].unique()))
    assert not missing, (
        f'GhanaLSS 2005-06: {len(missing)} source code(s) absent from '
        f'harmonize_food[{column}] -- {missing[:20]}')


# ================================ PURCHASES (s9b) ==============================
# Value-only.  s9bq1..s9bq10 carry the values for the 2nd..11th visits.
df = get_dataframe('../Data/partb/sec9b.dta', convert_categoricals=False)

# i exactly as sample()/roster() build it: mapping.i() over the *pre-composed*
# source 'hhid' column (sample reads idxvars i: hhid -> format_id(hhid)).
df['i'] = df['hhid'].apply(mapping.i)
df['j'] = df['freqcd'].astype('int64').map(labelsd['Code_9b'])
_assert_decoded(df['freqcd'], df['j'], 'Code_9b')
df = df[_drop_nonfood(df['j'])]          # drop non-food Section-9B block (j == '')

# Source question numbers are NOT visit numbers.  This wave's own Stata
# variable labels give the mapping:
#   s9bq1 'amount spent by second visit' .. s9bq10 '.. by eleventh visit'
pur_visit_cols = {f's9bq{v}': f'Expenditure_v{v + 1}' for v in range(1, 11)}
x = df.rename(columns=pur_visit_cols)[['i', 'j'] + list(pur_visit_cols.values())]
x = x.replace({r'': pd.NA, 0: np.nan})
# Several distinct freqcd codes harmonize to one j (e.g. 'Other Cereal'); sum
# their per-visit values so (i, j) uniquely identifies a row for the melt.
x = x.groupby(['i', 'j']).sum(min_count=1).reset_index()
x = pd.wide_to_long(x, ['Expenditure'], ['i', 'j'], 'visit', sep='_v')
x = x.dropna(subset=['Expenditure'])     # keep only visits with a recorded value
x['s'] = 'purchased'
x['u'] = 'Value'
x['Quantity'] = x['Expenditure']         # value-only: Quantity carries the value
x['Price'] = np.nan
x = x.reset_index()

# ================================ PRODUCED (s8h) ==============================
# Real quantity (s8hq3..s8hq12 = the 2nd..11th visits) in a native unit
# (s8hq13), with a farmgate Price
# (s8hq14).  Expenditure left NaN -- no produced value is recorded.
#
# The file is read TWICE, and both reads are load-bearing (GH #782).  The
# ``convert_categoricals=True`` read is required for ``s8hq1`` ('yes'/'no', not
# 1/2) and for ``s8hq13`` (the native unit as text, which this wave decodes
# nowhere else).  But that same conversion turns ``foodcd`` into the survey's
# label text, and the harmonize_food map is keyed by the numeric code -- so j
# must be taken from a ``convert_categoricals=False`` read.  The second read
# costs 0.3s and the two frames are row-aligned (asserted below).
prod = get_dataframe('../Data/partb/sec8h.dta', convert_categoricals=True)
prod_codes = get_dataframe('../Data/partb/sec8h.dta', convert_categoricals=False)
assert len(prod) == len(prod_codes), 'sec8h.dta read twice, row counts differ'
prod['foodcd_code'] = prod_codes['foodcd'].to_numpy()

prod = prod[prod['s8hq1'] == 'yes']      # only HH that consumed own produce
prod['i'] = prod['hhid'].apply(mapping.i)
prod['j'] = prod['foodcd_code'].astype('int64').map(labelsd['Code_8h'])
_assert_decoded(prod['foodcd_code'], prod['j'], 'Code_8h')
prod = prod[_drop_nonfood(prod['j'])]

# Native unit label (decoded text, e.g. 'basket', 'kilogram') and farmgate price.
prod['u'] = prod['s8hq13'].astype(str)
prod['Price'] = prod['s8hq14']

#   s8hq3..s8hq12 -> calendar visits 2..11.  The Stata label on s8hq3 reads
#   'number of units consumed', which was read as a TOTAL until 2026-09-11
#   and the column was dropped -- 21,493 positive records, the wave's whole
#   2nd-visit own-production ask.  It is not a total: the questionnaire
#   (G5QPartB.pdf p.17, printed 8.11) heads columns 3..12 '2nd 3rd .. 11th',
#   each asking 'how much of own produced .. was consumed .. since my last
#   visit?', and in the data s8hq3 < sum(s8hq4..s8hq12) in 27,314 rows, which
#   no total can be.  Both modules run 2..11 in this wave (the purchases'
#   s9bq1 label is 'amount spent by second visit'); the 8H stems start at q3
#   because q1/q2 are screeners, exactly as in 1998-99 / 2012-13 / 2016-17.
pro_visit_cols = {f's8hq{v}': f'Quantity_v{v - 1}' for v in range(3, 13)}
keep = ['i', 'j', 'u', 'Price'] + list(pro_visit_cols.values())
y = prod.rename(columns=pro_visit_cols)[keep]
y = y.replace({r'': pd.NA, 0: np.nan})
# As with purchases, distinct foodcd may share a j; sum per-visit quantities so
# (i, j, u, Price) uniquely identifies a row for the melt.
y = y.groupby(['i', 'j', 'u', 'Price']).sum(min_count=1).reset_index()
y = pd.wide_to_long(y, ['Quantity'], ['i', 'j', 'u', 'Price'], 'visit', sep='_v')
y = y.dropna(subset=['Quantity'])        # keep only visits with a recorded quantity
y = y.reset_index()

# `Price` is in the groupby key above (two farmgate prices for one commodity are
# two real observations), but it is NOT an index level of the canonical schema
# -- so once several foodcd harmonize to one j, a household can hold two rows on
# the same (i, j, u, visit) differing only in Price.  Resolve that HERE, on the
# canonical grain, rather than leaving it to the framework: `_normalize_dataframe_index`
# would reduce it with `groupby().first()`, i.e. keep one farmgate price and
# silently discard the other (GH #323's hazard, and measured to be exactly this
# -- 24 rows in 12 groups, all 12 disagreeing on Price).  Core is right not to
# aggregate; the wave script is where this belongs.
#
# Quantity SUMS (the household consumed both) and Price becomes the
# QUANTITY-WEIGHTED MEAN, which is the price of the harmonized commodity and is
# what makes Expenditure = Quantity * Price hold across the merge.  Affects only
# the Goat (mutton+goat) and Other Meat (game birds + other domestic meat + wild
# game) merges; every other row is already unique on this grain.
y['_qp'] = y['Quantity'] * y['Price']
g = y.groupby(['i', 'j', 'u', 'visit'], sort=False)
y = g.agg(Quantity=('Quantity', 'sum'), _qp=('_qp', 'sum'),
          _pf=('Price', 'first')).reset_index()
y['Price'] = (y['_qp'] / y['Quantity']).where(y['Quantity'] != 0, y['_pf'])
y = y.drop(columns=['_qp', '_pf'])

y['s'] = 'produced'
y['Expenditure'] = np.nan

# =============================== COMBINE & WRITE ==============================
idx = ['t', 'i', 'j', 'u', 's', 'visit']
fa = pd.concat([x, y], ignore_index=True)
fa['t'] = t
fa = fa.set_index(idx)[['Quantity', 'Expenditure', 'Price']]
fa = fa.dropna(how='all')

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

to_parquet(fa, 'food_acquired.parquet')
