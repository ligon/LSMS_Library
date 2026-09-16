#!/usr/bin/env python
"""GhanaLSS 1987-88 plot_inputs -- Section 9 Part D (FARM INPUTS).

Seven separate source files, one per expense block, melted into one long
table at (t, i, input, j).  A YAML `data_info.yml` cannot express this: the
blocks live in different files under different variable names and each
contributes a CONSTANT `input` label, which is the documented
`materialize: make` case (CLAUDE.md, "Two Build Paths").

There is NO plot level.  Section 9 has no farm/plot roster at all -- the
GLSS1/GLSS2 `plot_features` cells carry a C4-validated `not-asked` verdict --
so the grain is household-crop, which is also what the seven EHCVM countries
declare for this table: `(t, i, input, crop, u)`.  Here `u` is a COLUMN rather
than an index level, following Uganda / Ethiopia / Nigeria, because four of the
seven blocks record no quantity and hence no unit: a NaN on a declared index
level is deleted by the first groupby (CLAUDE.md, "Site I").

The blocks, all verified against the questionnaire (image-only CCITT scan,
rendered with slurm_logs/ghana_audit/glss2_questionnaire/render_pages.py):

  p.47  Q1-6    seeds or young plants -- spend only
        Q7-13   fertilizer            -- quantity + unit + spend
        Q14-19  organic manure        -- quantity + unit + spend
  p.48  Q20-26  insecticides OR herbicides -- quantity + unit + spend
        Q27-30  transporting crops    -- spend only
        Q31-36  sacks, twine or containers -- spend only
  p.49  Q37-40  storage               -- spend only

TWO THINGS A READER MUST NOT MISREAD.

1.  `Transport`, `Containers` and `Storage` are farm OPERATING expenses, not
    inputs applied to a crop.  They are carried here because the survey asks
    them per crop and because summing `Cost` over `input` is exactly the
    farm-expense aggregate this table exists to support -- but they are not
    comparable to the agronomic rows, and they carry no `Quantity`.

2.  The `input` labels are CATEGORIES, not products.  Every other country in
    the corpus names the chemical (`Urea`, `NPK`, `DAP`, `Pesticide`,
    `Herbicide`); GLSS1/GLSS2 asks only "fertilizer" and "insecticides or
    herbicides".  `Fertilizer` here is therefore a SUPERSET of the corpus's
    product labels and must not be pooled with them, and
    `Pesticide or Herbicide` is deliberately not called `Pesticide`.

`Purchased` is NOT served.  The form asks "how did you obtain" as a source
code (private individual, agency, co-operative, ...) and separately whether it
was obtained on credit; neither is the canonical boolean, and a spend > 0 is
not the same question (Q10 says "IF NOTHING, WRITE ZERO", so a scheme-supplied
input is a used-but-unbought row).  An honest gap rather than a guess.
"""
import sys
sys.path.append('../../_')
from mapping import i as i_helper
import numpy as np
import pandas as pd
sys.path.append('../../../_/')
from lsms_library.local_tools import (df_from_orgfile, format_id, get_dataframe,
                                      to_parquet)

t = '1987-88'

#: (file, input label, crop column, quantity column, unit column, spend column)
BLOCKS = [
    ('Y09D1A.DAT', 'Seed',                   'SEEDCR', None,      None,       'SEEDAMT'),
    ('Y09D1B.DAT', 'Fertilizer',             'FERTCR', 'FERTQNT', 'FERTQNTU', 'FERTAMT'),
    ('Y09D1C.DAT', 'Manure',                 'MANUCR', 'MANUQNT', 'MANUQNTU', 'MANUAMT'),
    ('Y09D2A.DAT', 'Pesticide or Herbicide', 'INSCCR', 'INSCQNT', 'INSCQNTU', 'INSCAMT'),
    ('Y09D2B.DAT', 'Transport',              'TRANCR', None,      None,       'TRANAMT'),
    ('Y09D2C.DAT', 'Containers',             'CONTCR', None,      None,       'CONTAMT'),
    ('Y09D3A.DAT', 'Storage',                'STORCR', None,      None,       'STORAMT'),
]

_root = '../../_/categorical_mapping.org'
_crops = df_from_orgfile(_root, name='agric_crop', encoding='ISO-8859-1')
CROP = (_crops.assign(Code=_crops['Code'].astype('Int64').astype('string'))
              .set_index('Code')['Label'].to_dict())
_units = df_from_orgfile(_root, name='agric_unit', encoding='ISO-8859-1')
UNIT = (_units.assign(Code=_units['Code'].astype('Int64').astype('string'))
              .set_index('Code')['Label'].to_dict())


def _decode(s, table):
    return s.apply(format_id).astype('string').replace(table)


frames = []
for fn, label, crop_col, qty_col, unit_col, amt_col in BLOCKS:
    df = get_dataframe(f'../Data/{fn}')
    out = pd.DataFrame({
        't': t,
        'i': df['HID'].apply(i_helper),
        'input': label,
        'j': _decode(df[crop_col], CROP),
        'Cost': pd.to_numeric(df[amt_col], errors='coerce'),
    })
    out['Quantity'] = (pd.to_numeric(df[qty_col], errors='coerce')
                       if qty_col else np.nan)
    # An unmapped unit code survives the decode as a numeric string; every
    # real label is alphabetic, so anything still numeric is not a unit.
    if unit_col:
        u = _decode(df[unit_col], UNIT)
        out['u'] = u.mask(pd.to_numeric(u, errors='coerce').notna(), pd.NA)
    else:
        out['u'] = pd.Series(pd.NA, index=out.index, dtype='string')
    frames.append(out)

f = pd.concat(frames, ignore_index=True)

# A crop that was never named in a block is a skip, not a zero-cost row.
f = f[f['j'].notna() & (f['j'].astype('string') != '<NA>')]

# A household may name the same crop on more than one line of a block -- two
# separate purchases for the same crop.  Measured: 0 duplicate keys in
# 1987-88, 17 of 5,666 rows (0.30%) in 1988-89, all in Seed (4), Pesticide or
# Herbicide (12) and Containers (1).  Collapse them HERE rather than let the
# API-layer canonical-shape guard do it, which would keep one row and drop the
# other's spend (the reason `food_acquired.py` collapses in the same place).
#
# `Cost` is additive and is summed.  `Quantity` is additive only when the
# lines share a unit, and they often do not -- only 4 of the 12 Pesticide
# groups do.  Where the units disagree the quantity is NOT commensurable, so
# it is dropped to NA with its unit rather than summed into a number that
# means nothing; the spend, which is in one currency, survives either way.
key = ['t', 'i', 'input', 'j']
_before = len(f)
_g = f.groupby(key, dropna=False)
out = pd.DataFrame({
    'Cost': _g['Cost'].sum(min_count=1),
    'Quantity': _g['Quantity'].sum(min_count=1),
    'u': _g['u'].first(),
})
_mixed = (_g['u'].nunique(dropna=True) > 1).reindex(out.index, fill_value=False)
out.loc[_mixed, 'Quantity'] = np.nan
out.loc[_mixed, 'u'] = pd.NA
if _before != len(out):
    print(f'{t}: collapsed {_before - len(out)} duplicate (t, i, input, j) '
          f'line(s); {int(_mixed.sum())} of them had disagreeing units, so '
          f'their Quantity is served NA (Cost is summed regardless)')

f = out.sort_index()
assert f.index.is_unique, 'collapse left a non-unique index'

to_parquet(f[['Quantity', 'u', 'Cost']], 'plot_inputs.parquet')
