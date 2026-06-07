#!/usr/bin/env python
"""Build plot_features for Tajikistan 2007 (TLSS round 1, module 12a).

The land module is a genuine per-plot roster split across three subsections,
each keyed by (hhid, plotcode):

  r1m12a1 -- plots OWNED by the household        (3685 plots; Tenure=owned)
  r1m12a2 -- plots RENTED/BORROWED IN            ( 269 plots; Tenure=rented_in)
  r1m12a3 -- plots RENTED/LENT OUT               (  20 plots; Tenure=rented_out)

`plotcode` restarts at 1 within each subsection per household, so the same
(hhid, plotcode) appears in more than one subsection (231 collisions between
a1 and a2 alone).  We therefore tag plot_id with the tenure flavour
("owned-1", "rented_in-1", ...) to keep (t, i, plot_id) unique while
preserving the per-plot identity.

Per-subsection canonical columns:
  Area       q3  (numeric; unit not in the data -- TLSS records land in
                  sotka = 1/100 ha = 100 m^2, the standard Tajik unit ->
                  AreaUnit hardcoded to 'sotka')
  SoilType   q4  (land kind: ANNUAL CROP LAND / TREE CROP LAND / PASTURE ...)
  Irrigated  q6 (a1, a2) / q5 (a3)   Yes/No -> bool
  Tenure         the subsection itself
  TenureSystem   a1 q9 only (legal title: CERTIFICATE / ACT / ...); a2/a3
                 have no title question -> NaN there.

Single round -> single t = 2007.  `i = format_id(hhid)` matches the 2007
household_roster `i`.
"""
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet, format_id

t = '2007'
AREA_UNIT = 'sotka'  # TLSS land area is recorded in sotka (1/100 ha)


def _subsection(fn, tenure, area, soil, irrig, title=None):
    """Extract one land subsection into the canonical plot_features shape."""
    df = get_dataframe(fn)

    out = pd.DataFrame(index=df.index)
    out['i'] = df['hhid'].apply(format_id)
    # tenure-tagged plot identity (plotcode restarts per subsection)
    out['plot_id'] = tenure + '-' + df['plotcode'].astype('Int64').astype(str)

    out['Area'] = pd.to_numeric(df[area], errors='coerce').astype(float)
    out['AreaUnit'] = AREA_UNIT
    out['Tenure'] = tenure
    out['TenureSystem'] = (
        df[title].astype(str).replace('nan', pd.NA) if title else pd.NA
    )
    out['SoilType'] = df[soil].astype(str).replace('nan', pd.NA)
    out['Irrigated'] = df[irrig].astype(str).str.strip().map(
        {'Yes': True, 'No': False}
    ).astype('boolean')
    return out


pieces = [
    _subsection('../Data/r1m12a1.dta', 'owned',
                area='m12a1q3', soil='m12a1q4', irrig='m12a1q6',
                title='m12a1q9'),
    _subsection('../Data/r1m12a2.dta', 'rented_in',
                area='m12a2q3', soil='m12a2q4', irrig='m12a2q6'),
    _subsection('../Data/r1m12a3.dta', 'rented_out',
                area='m12a3q3', soil='m12a3q4', irrig='m12a3q5'),
]

df = pd.concat(pieces, axis=0, ignore_index=True)
df['t'] = t
df = df.set_index(['t', 'i', 'plot_id'])

assert df.index.is_unique, "Non-unique (t, i, plot_id) in plot_features 2007"
assert len(df) > 0, "plot_features 2007 produced no rows"

to_parquet(df, 'plot_features.parquet')
