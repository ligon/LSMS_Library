#!/usr/bin/env python
"""Burkina Faso 2014 (EMC) months_food_inadequate -- 12-month provisioning.

Source: ``emc2014_p3_securitealimentaire_27022015.dta`` (Part 3 "Sécurité
alimentaire").  HOUSEHOLD-level (one row per HH, index i = each wave's roster i
= format_id(zd) + format_id(menage, zeropadding=3), e.g. zd=1, menage=1 -> '1001').

    SA4        "Au cours des 12 derniers mois, avez-vous fait face à une
               situation où vous n'aviez pas assez de nourriture [...]" (Oui/Non).
               The 12-month gate.
    SA5A..SA5L which of the 12 months the household met this problem
               (Juillet 2013 .. Juillet 2014), each Oui/Non.  Only asked when
               SA4 == 'Oui'; NaN for the 'Non' households (skipped out).

Derivation (the which-months battery is present, so MonthsInadequate is the
count of "Oui" months per the brief):
    MonthsInadequate = number of SA5A..SA5L == 'Oui', where SA4 == 'Oui';
                       0 where SA4 == 'Non' (skipped out precisely because they
                       had no inadequate months); NaN where SA4 is NaN
                       (question not asked, 10 HH).
    AnyInadequate    = MonthsInadequate > 0.

``t`` = '2014'.  ``v`` is NOT baked in -- the framework joins it from
``sample()`` at API time.
"""
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet, format_id

df = get_dataframe('../Data/emc2014_p3_securitealimentaire_27022015.dta')

sa5_cols = ['SA5' + c for c in 'ABCDEFGHIJKL']

# Gate: faced a not-enough-food situation in the last 12 months?
gate = df['SA4'].astype(str).str.strip().str.lower()
faced = gate.map({'oui': True, 'non': False})  # NaN where not asked

# Count "Oui" month-flags (only populated for the 'Oui' households).
oui_months = df[sa5_cols].apply(
    lambda col: col.astype(str).str.strip().str.lower().eq('oui')).sum(axis=1)

# Non households were skipped out of SA5 -> 0 inadequate months; NaN gate -> NaN.
months = oui_months.where(faced != False, other=0)
months = months.where(faced.notna(), other=pd.NA)

out = pd.DataFrame(index=df.index)
out['i'] = (df['zd'].apply(format_id)
            + df['menage'].apply(lambda m: format_id(m, zeropadding=3)))
out['t'] = '2014'
out['MonthsInadequate'] = pd.to_numeric(months, errors='coerce').astype('Int64')
out['AnyInadequate'] = (out['MonthsInadequate'] > 0).astype('boolean')
# Preserve NaN gate as <NA> AnyInadequate rather than False.
out.loc[out['MonthsInadequate'].isna(), 'AnyInadequate'] = pd.NA

out = out.set_index(['t', 'i'])

if not out.index.is_unique:
    out = out.groupby(level=out.index.names).first()

to_parquet(out, 'months_food_inadequate.parquet')
