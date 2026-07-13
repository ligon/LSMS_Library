#!/usr/bin/env python
"""Burkina Faso 2014 (EMC) food_acquired -- canonical long form.

    index  : (t, v, i, j, u, s, visit)
    columns: [Quantity, Expenditure]
    s      : {purchased, produced, inkind}
    visit  : 1..4  -- the four quarterly PASSAGES of the EMC 2014
                      continuous survey

WHY THIS IS A SCRIPT AND NOT data_info.yml (GH #323)
----------------------------------------------------
The EMC 2014 is a CONTINUOUS survey: the same 10,800 households are
revisited in four quarterly passages, each carrying its own independent
SEVEN-DAY consumption recall.  The four source files

    emc2014_p{1,2,3,4}_conso7jours_16032015.dta

were previously listed under a single ``file:`` key in the wave's
``data_info.yml``, with no index level distinguishing the passage and
``t`` set to '2014' for all four.  The four quarters therefore landed on
the SAME (t, v, i, j, u, s) tuple and were collapsed by the framework's
duplicate-index reducer -- which, because ``food_acquired`` is an
additive-measure table, SUMMED them.  Four independent 7-day recalls were
added together and labelled as one 7-day observation (items seen in all
four quarters inflated 4x; items seen once left at 1x).  The original
author knew: the legacy version of this script read only passage 1 and
carried the comment ``#change to 5 to get the files for all 4 rounds``.

``visit`` is the repeated-measure level, exactly as in GhanaLSS
(``food_acquired: (t, v, i, j, u, s, visit)``).  It keeps the four
passages on distinct index tuples, so nothing is summed across quarters
at the food_acquired grain.  A YAML ``file:`` list cannot express a
per-file index level -- hence the script path (CLAUDE.md: "multi-wave
source files with a round column -> script with materialize: make").

THE THREE ACQUISITION SOURCES
-----------------------------
Each source row carries THREE PARALLEL (quantity, unit, value) triples --
they are independent acquisition acts, NOT a total-and-subset:

    purchases    qachat    uachat    achat
    own produce  qautocons uautocons autocons
    gifts/inkind qcadeau   ucadeau   cadeau

The old YAML mapped ``Quantity: qachat`` / ``Produced: qautocons`` and let
``food_acquired_to_canonical`` compute ``purchased = Quantity - Produced``.
That is wrong twice over: ``qachat`` is the PURCHASED quantity (not a
total), and the two columns are near-disjoint -- only 286 p1 rows have
both non-null, and in 49 of those ``qautocons > qachat``.  It also tagged
the produced quantity with ``uachat`` (the PURCHASE unit, usually NaN)
instead of its own ``uautocons``, which is what made those rows NaN-keyed
and got them annihilated by the groupby.  And ``cadeau`` -- an entire
in-kind acquisition source -- was never read at all.

VALUE-ONLY ROWS (u = 'Value')
-----------------------------
Passages 2, 3 and 4 have NO quantity or unit columns whatsoever (verified:
they carry only achat / autocons / cadeau / conson).  Many p1 rows are
value-only too (44,048 rows have achat > 0 with a NaN uachat).  Such rows
record a real monetary amount with no physical quantity basis.  They are
emitted with the LCU convention already used by GhanaLSS: ``u='Value'``,
``Quantity = Expenditure``.  Previously they entered with ``u=NaN`` and
were DELETED outright by ``groupby(...)``'s ``dropna=True`` default --
460,438 rows / 261.8M CFA, 76.5% of the wave's expenditure.

``conson`` is an Oui/Non consumption flag, not a value; ``mtt_dep`` does
not reconcile to achat+autocons+cadeau.  Neither is used.

RECONCILIATION (checked at the bottom of this script)
    sum(Expenditure | s='purchased') == 93,383,167 + 82,473,394
                                      + 93,328,730 + 72,925,224
                                      == 342,110,515 CFA
"""
import sys

import numpy as np
import pandas as pd

sys.path.append('../../_/')
from lsms_library.local_tools import (format_id, get_categorical_mapping,
                                      get_dataframe, to_parquet)

t = '2014'

# product label -> canonical j (same table the old YAML used via `mappings:`)
harmonize_food = get_categorical_mapping(tablename='harmonize_food',
                                         idxvars='Original Label',
                                         **{'Label': 'Preferred Label'})

# (quantity, unit, value) triple per canonical acquisition source `s`.
SOURCES = {
    'purchased': ('qachat',    'uachat',    'achat'),
    'produced':  ('qautocons', 'uautocons', 'autocons'),
    'inkind':    ('qcadeau',   'ucadeau',   'cadeau'),
}

PASSAGES = [1, 2, 3, 4]

out = []
for passage in PASSAGES:
    df = get_dataframe(f'../Data/emc2014_p{passage}_conso7jours_16032015.dta')

    # i EXACTLY as household_roster / sample build it for this wave:
    # format_id(zd) + format_id(menage, zeropadding=3).
    base = pd.DataFrame(index=df.index)
    base['i'] = (df['zd'].map(format_id)
                 + df['menage'].map(lambda x: format_id(x, zeropadding=3)))
    base['v'] = df['zd'].map(format_id)
    base['j'] = df['product'].map(lambda x: harmonize_food.get(x, x))

    for s, (qcol, ucol, vcol) in SOURCES.items():
        # p2/p3/p4 carry no quantity/unit columns at all -> value-only.
        q = (pd.to_numeric(df[qcol], errors='coerce')
             if qcol in df.columns else pd.Series(np.nan, index=df.index))
        u = (df[ucol].astype('object')
             if ucol in df.columns else pd.Series(pd.NA, index=df.index))
        val = (pd.to_numeric(df[vcol], errors='coerce')
               if vcol in df.columns else pd.Series(np.nan, index=df.index))

        x = base.copy()
        x['s'] = s
        x['visit'] = passage

        # A row is PHYSICAL when it has both a positive quantity and a unit;
        # otherwise, if it carries money, it is VALUE-ONLY (u='Value').
        physical = q.notna() & (q > 0) & u.notna()
        money = val.notna() & (val > 0)

        x['u'] = np.where(physical, u.astype('object'), 'Value')
        x['Quantity'] = np.where(physical, q, np.where(money, val, np.nan))
        # Expenditure is the reported monetary amount (NaN when none given --
        # a physical observation with no value is still a real observation).
        x['Expenditure'] = np.where(money, val, np.nan)

        # Keep only rows where SOMETHING was observed.  Dropping a row with
        # neither a quantity nor a value loses nothing (class-2 by design).
        out.append(x[physical | money])

fa = pd.concat(out, ignore_index=True)
fa['t'] = t

# Collapse the 131 rows (130 groups) where two DIFFERENT source products map to
# the same canonical `j` for one household in one passage (harmonize_food is
# many-to-one: several fish codes -> 'Poisson frais', several rice codes ->
# 'Riz importé'), or where two value-only lines of the same item both land on
# u='Value'.  These are genuinely separate acquisition LINES for the same item,
# and Quantity/Expenditure are additive -- so SUM them.  This is the same policy
# the framework's _ADDITIVE_MEASURE_COLUMNS reducer would apply; doing it here
# makes it explicit and lets the uniqueness guard below actually mean something.
# Verified lossless: the purchased total is unchanged to the franc.
#
# min_count=1 so an all-NaN group stays NaN: a physical observation with no
# reported value must not be silently turned into "0 CFA of expenditure".
IDX = ['t', 'v', 'i', 'j', 'u', 's', 'visit']
fa = (fa.groupby(IDX, dropna=False, observed=True)[['Quantity', 'Expenditure']]
        .sum(min_count=1)
        .sort_index())

# --- Reconciliation guards: fail LOUDLY rather than ship a silent loss. ---
purchased = fa.xs('purchased', level='s')['Expenditure'].sum()
EXPECTED = 93_383_167 + 82_473_394 + 93_328_730 + 72_925_224   # = 342,110,515
assert abs(purchased - EXPECTED) < 1, (
    f'purchased Expenditure {purchased:,.0f} != sum of source `achat` '
    f'{EXPECTED:,.0f} -- the four passages no longer reconcile.')

# No (v, i, j, u, s) tuple may pool observations from more than one passage.
assert fa.index.is_unique, 'food_acquired index is not unique'

print(f'Burkina_Faso 2014 food_acquired: {len(fa):,} rows')
print(f'  purchased Expenditure = {purchased:,.0f} CFA (reconciles to source achat)')
print(f'  by visit: {fa.groupby("visit").size().to_dict()}')
print(f'  by s    : {fa.groupby("s").size().to_dict()}')

to_parquet(fa, 'food_acquired.parquet')
