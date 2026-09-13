#!/usr/bin/env python
"""GhanaLSS 2016-17 nonfood_expenditures -- Section 9A, item-level, long.

SELF-CONTAINED (matching Togo/2018/_/nonfood_expenditures.py, the canonical
instance): the decode helper is inlined, so this does NOT import ghanalss.

WHAT SECTION 9A IS, from the instrument.  GLSS Section 9 is titled
"HOUSEHOLD EXPENDITURE (FOOD AND NON-FOOD EXPENSES)" and splits on PURCHASE
FREQUENCY, not on food.  Part A is

    PART A:  NON-FOOD EXPENSES (LESS FREQUENTLY PURCHASED ITEMS)

and the GLSS7 Interviewer Manual says of it: "Responses for these items will be
solicited only once during the last visit to the household ...  the reference
period for the amount spent on any of these items will be '... in the last 12
months'."  Hence RecallWindow = '12 months' for every row.

    Q1  Was anything spent by the household on ...... in the past 12 months?
    Q2  How much was spent on ...... in the past 12 months altogether?
    Q3  Has the household used, consumed out of its own output or has received
        as gift ...... in the past 12 months?   IF NONE PUT '00' & >> NEXT ITEM
    Q4  How much of ...... has the household used or consumed out of own
        production, or has received as gift in the past 12 months

`Expenditure` = Q2 + Q4, summed with min_count=1.  That composition is
`uganda.nonfood_expenditures`'s (purchased + away + produced + given) and
Nigeria follows it, so GhanaLSS matches the two countries `Feature()` pools it
with.  Togo is not a counter-example -- EHCVM s09 records only a montant
depense, so it has nothing to sum.

*** DO NOT GATE ON Q1. ***  Q1 is "was anything SPENT", and its `No` branch
skips to Q3, so own-production and gifts are recorded for households that spent
nothing.  Measured on 2016-17: a `q1 == 1` gate drops 14,703 rows carrying
2.274e6 cedi of own-produce/gift consumption -- 7.6% of the wave's non-food
total of 2.99e7.  (This read "1.163e8 ... 28%" until 2026-09-12: the row count
was right, the share was computed against a sentinel-inflated denominator.)  The gate here is on the VALUE, which is what the canonical sparsity
rule actually asks for ("rows exist only where the household reported the
item", lsms_library/data_info.yml).

*** THE MISSING-VALUE SENTINELS ARE A REPDIGIT FAMILY, AND THEY ARE
MEASURED, NOT GUESSED. ***  s9aq2 and s9aq4 carry 999999 (6 + 5 rows),
9999999 (27 + 13), 99999999 (43 + 20) and 9999996 (0 + 1).  Each is spread
across many DISTINCT items (99999999 over 30 items, 9999999 over 26), which
is what identifies them as a field-width missing code rather than an item
price; the genuine large values sit on one item each and stop at 725,700.

Unguarded they take this wave's annual non-food total to 4.60e9 cedi.  A
`>= 99999999` THRESHOLD -- which is what this script shipped first -- catches
only the widest of them and still serves 4.51e8, of which 93% is the 9999999
tier.  The measured total is *** 2.99e7 ***.  Cross-checked against GSS's own
aggregate (`15_GHA_2017_E_final.dta`): 9A is then 39.7% of `TOTNFD`, median
1,011 against a GSS non-food median of 3,437 and a household total of 7,863 --
the right share for a less-frequently-purchased module.

A threshold is also actively WRONG for the sibling waves, which is why the set
is per-wave: 2005-06 is in OLD cedi, where 1e8 is an ordinary large purchase.

GRAIN (t, i, j).  No `v`: the framework joins it from sample() at API time
(CLAUDE.md, "sample() and Cluster Identity"), so emitting one here is wrong.

THE ITEM AXIS IS PER-WAVE.  `Code_2016-17` in the country's
categorical_mapping.org `nonfood_items` table -- never a shared code column:
GLSS6 shifts every GLSS5 code from 4 on.  See that table's preamble.

SCOPE, AND THE OMISSION IS LARGE.  This is the LESS-frequently-purchased
module only.  The frequently purchased non-food (soap, fuel, toiletries,
transport, stationery) is inside Section 9B on the per-visit grid, mixed with
food, and is NOT in this table.

*** THE OMITTED BASKET IS ~44% OF NON-FOOD, NOT ~7%. ***  Re-measured
2026-09-12 on 2016-17, both on the 30-day exposure (6 asks x 5 days): 9B
non-food is 1.931e6 against 9A's 2.457e6, i.e. 44.0% of 30-day non-food
spending; 2012-13 gives 38.7%.  The "~7%" this file shipped with divided by a
denominator that was 90% missing-value sentinel -- see the sentinel paragraph
above.  Annualised, 9A (2.99e7) + 9B non-food (2.35e7) is 71% of GSS's own
TOTNFD aggregate, the remaining 29% being rent, utilities, education and health
drawn from other sections -- which is what makes both numbers credible.

The 9B non-food vocabulary is now recorded: `nonfood_items_9b` in
../../_/categorical_mapping.org (204 items, per-wave codes).  It is NOT wired
to anything; serving 9B needs a `visit`-grain decision the canonical shape does
not cover.  See _/CONTENTS.org, "nonfood_expenditures is Section 9A only".
"""
import numpy as np
import pandas as pd

from lsms_library.local_tools import (df_from_orgfile, format_id,
                                      get_dataframe, to_parquet)

t = '2016-17'
FN = '../Data/g7sec9a.dta'
ID = 'hid'
SENTINELS = frozenset({999999, 9999999, 9999996, 99999999})   # MEASURED; see the module docstring
WINDOW = '12 months'


def _item_map(wave):
    """{numeric 9A code -> Preferred Label} for THIS wave only."""
    tbl = df_from_orgfile('../../_/categorical_mapping.org', name='nonfood_items')
    col = f'Code_{wave}'
    sub = tbl[['Preferred Label', col]].dropna(subset=[col])
    out = {}
    for label, code in zip(sub['Preferred Label'].astype(str).str.strip(), sub[col]):
        out[int(float(code))] = label
    assert out, f'nonfood_items has no {col} entries -- empty decode (CONTENTS Trap 1)'
    labels = list(out.values())
    assert len(labels) == len(set(labels)), (
        'nonfood_items Preferred Labels are not injective for '
        f'{wave}: {sorted({x for x in labels if labels.count(x) > 1})}')
    return out


item_map = _item_map(t)

# convert_categoricals=False: we decode on the NUMERIC code (CONTENTS Trap 8 --
# True would hand back the label and the .map() would silently produce NaN).
df = get_dataframe(FN, convert_categoricals=False)

codes = pd.to_numeric(df['lfreqcd'], errors='coerce').astype('Int64')
unknown = sorted(set(codes.dropna().unique()) - set(item_map))
assert not unknown, f'{FN}: 9A item codes with no nonfood_items row: {unknown}'


#: The repdigit-9 family at every plausible field width -- 999999, 9999999,
#: ... and the reserved ...96/97/98 codes beside them.  Used ONLY as a
#: TRIPWIRE.  What is actually masked is SENTINELS, measured from this file;
#: a threshold would be a guess, and in old cedi a wrong one.
_REPDIGIT = {float(int('9' * (w - 1) + d))
             for w in range(6, 11) for d in '6789'}


def _amount(col):
    """The reported amount, with this wave's MEASURED sentinels set to NA.

    Refuses to build on an unlisted repdigit: the set is a measurement of THIS
    distribution, so a re-release that adds a field width -- or a wave that
    acquires the convention -- must fail loudly rather than be served as a
    purchase.  A false alarm here costs one re-measurement; a miss shipped
    2016-17 at 15x its true total.
    """
    s = pd.to_numeric(df[col], errors='coerce')
    stray = (_REPDIGIT & set(s.dropna().unique())) - {float(x) for x in SENTINELS}
    assert not stray, (
        f'{FN}:{col}: unlisted repdigit value(s) {sorted(stray)}. The sentinel '
        'set is a MEASUREMENT of this file -- re-measure it (are these spread '
        'across distinct items? then they are missing codes) before serving '
        'them as purchases.  See the module docstring.')
    return s.mask(s.isin(SENTINELS))


spent = _amount('s9aq2')
own = _amount('s9aq4')
expenditure = pd.concat([spent, own], axis=1).sum(axis=1, min_count=1)

out = pd.DataFrame({
    't': t,
    'i': df[ID].map(format_id),
    'j': codes.map(item_map).astype('string'),
    'RecallWindow': WINDOW,
    'Expenditure': expenditure.astype('Float64'),
})

# Sparsity (lsms_library/data_info.yml, `nonfood_expenditures`): a household
# that did not report an item has NO ROW, never a fabricated 0.
out = out[out['i'].notna() & out['j'].notna() & (out['Expenditure'] > 0)]

out['RecallWindow'] = out['RecallWindow'].astype('string')
dups = out.duplicated(subset=['t', 'i', 'j']).sum()
assert dups == 0, f'{dups} duplicate (t, i, j) rows in nonfood_expenditures {t}'

out = out.set_index(['t', 'i', 'j'])[['Expenditure', 'RecallWindow']]
assert len(out) > 0, f'nonfood_expenditures {t} produced no rows'

to_parquet(out, 'nonfood_expenditures.parquet')
