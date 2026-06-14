"""Build plot_labor (item-level plot labor by source) for Mali EACI 2014-15.

GAP 3a (parity loop).  One row per (t, i, plot, source), source in
{family, hired, other}.

Sources (both at the plot grain):
  - EACIMAINOUVRE_p1.dta  post-planting (PP) plot-labor roster, s2b vars.
  - EACIS2F_p2.dta        post-harvest (PH) plot-labor roster, s2f vars.

plot = "{field}_{parcel}" (s2bq01_s2bq02 for PP; s2fq01_s2fq02 for PH) —
the SAME plot id as crop_production / plot_inputs / plot_features.

REPORTED person-days per source only:
  PersonDays = persons * days-each, summed over the man/woman/child
               demographic splits and the two passages (PP + PH).
  Wage       = reported cash paid to hired labor (FCFA); NA for family/other.
NO total_labor_days / total_*_labor_days / hired_labor_value — those are
transformations over these rows.

Variable map traced from MLI_EACI1.do (WB harmonised plot-labor section):
  PP family (gate s2bq04): man s2bq05a*s2bq05b, woman s2bq05d*s2bq05e,
                           child s2bq05g*s2bq05h.
  PP hired  (gate s2bq06): man s2bq07a*s2bq07b (wage s2bq07c),
                           woman s2bq07d*s2bq07e (wage s2bq07f),
                           child s2bq07g*s2bq07h (wage s2bq07i).
  PP other  (gate s2bq08): man s2bq09a*s2bq09b, woman s2bq09d*s2bq09e,
                           child s2bq09g*s2bq09h.
  PH family (gates s2fq03/s2fq09): two activity blocks
                           q04 (a/b,c/d,e/f) and q10 (a/b,c/d,e/f).
  PH hired  (gates s2fq05/s2fq11): q06 (a/b wage c; d/e wage f; g/h wage i)
                           and q12 (a/b wage c; d/e wage f; g/h wage i).
  PH other  (gates s2fq07/s2fq13): q08 (a/b,d/e,g/h) and q14 (a/b,d/e,g/h).
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet
from mali import i as mali_i, plot_labor_finalize

WAVE = '2014-15'


def _hhid(df):
    return df.apply(lambda r: mali_i(pd.Series([r['grappe'], r['menage']])), axis=1)


def _plot(df, fcol, pcol):
    f = df[fcol].astype('Int64').astype('string')
    p = df[pcol].astype('Int64').astype('string')
    return (f + '_' + p).where(f.notna() & p.notna(), pd.NA)


# EACI "Manquant" / refusal sentinels on the persons / days / wage
# components (cf. MLI_EACI1.do:720-722, 756-758 which `replace = . if
# ==99|999|99999999` BEFORE forming persons*days).  Cleared at the COMPONENT
# level so a sentinel never multiplies into a spurious huge person-day total.
_SENTINELS = [99, 999, 9999, 99999, 999999, 9999999, 99999999]


def _num(df, col):
    if col in df.columns:
        s = pd.to_numeric(df[col], errors='coerce')
        return s.mask(s.isin(_SENTINELS))
    return pd.Series(pd.NA, index=df.index, dtype='Float64')


def _prod(df, persons_col, days_col):
    """persons * days-each -> reported person-days for one demographic split."""
    return _num(df, persons_col) * _num(df, days_col)


pieces = []

# ---------------- post-planting (PP) roster: s2b -------------------------
pp = get_dataframe('../Data/EACIMAINOUVRE_p1.dta').copy()
pp['i'] = _hhid(pp)
pp['plot'] = _plot(pp, 's2bq01', 's2bq02')

# family: sum man/woman/child person-days
fam_days = (_prod(pp, 's2bq05a', 's2bq05b')
            .add(_prod(pp, 's2bq05d', 's2bq05e'), fill_value=0)
            .add(_prod(pp, 's2bq05g', 's2bq05h'), fill_value=0))
pieces.append(pd.DataFrame({
    't': WAVE, 'i': pp['i'], 'plot': pp['plot'], 'source': 'family',
    'PersonDays': fam_days, 'Wage': pd.NA,
}))

# hired: person-days + reported cash wage
hir_days = (_prod(pp, 's2bq07a', 's2bq07b')
            .add(_prod(pp, 's2bq07d', 's2bq07e'), fill_value=0)
            .add(_prod(pp, 's2bq07g', 's2bq07h'), fill_value=0))
hir_wage = (_num(pp, 's2bq07c')
            .add(_num(pp, 's2bq07f'), fill_value=0)
            .add(_num(pp, 's2bq07i'), fill_value=0))
pieces.append(pd.DataFrame({
    't': WAVE, 'i': pp['i'], 'plot': pp['plot'], 'source': 'hired',
    'PersonDays': hir_days, 'Wage': hir_wage,
}))

# other (free / exchange) labor
oth_days = (_prod(pp, 's2bq09a', 's2bq09b')
            .add(_prod(pp, 's2bq09d', 's2bq09e'), fill_value=0)
            .add(_prod(pp, 's2bq09g', 's2bq09h'), fill_value=0))
pieces.append(pd.DataFrame({
    't': WAVE, 'i': pp['i'], 'plot': pp['plot'], 'source': 'other',
    'PersonDays': oth_days, 'Wage': pd.NA,
}))

# ---------------- post-harvest (PH) roster: s2f --------------------------
ph = get_dataframe('../Data/EACIS2F_p2.dta').copy()
ph['i'] = _hhid(ph)
ph['plot'] = _plot(ph, 's2fq01', 's2fq02')

# family: two activity blocks (q04, q10)
ph_fam = (_prod(ph, 's2fq04a', 's2fq04b')
          .add(_prod(ph, 's2fq04c', 's2fq04d'), fill_value=0)
          .add(_prod(ph, 's2fq04e', 's2fq04f'), fill_value=0)
          .add(_prod(ph, 's2fq10a', 's2fq10b'), fill_value=0)
          .add(_prod(ph, 's2fq10c', 's2fq10d'), fill_value=0)
          .add(_prod(ph, 's2fq10e', 's2fq10f'), fill_value=0))
pieces.append(pd.DataFrame({
    't': WAVE, 'i': ph['i'], 'plot': ph['plot'], 'source': 'family',
    'PersonDays': ph_fam, 'Wage': pd.NA,
}))

# hired: two activity blocks (q06, q12) + wages (c/f/i)
ph_hir = (_prod(ph, 's2fq06a', 's2fq06b')
          .add(_prod(ph, 's2fq06d', 's2fq06e'), fill_value=0)
          .add(_prod(ph, 's2fq06g', 's2fq06h'), fill_value=0)
          .add(_prod(ph, 's2fq12a', 's2fq12b'), fill_value=0)
          .add(_prod(ph, 's2fq12d', 's2fq12e'), fill_value=0)
          .add(_prod(ph, 's2fq12g', 's2fq12h'), fill_value=0))
ph_hir_wage = (_num(ph, 's2fq06c').add(_num(ph, 's2fq06f'), fill_value=0)
               .add(_num(ph, 's2fq06i'), fill_value=0)
               .add(_num(ph, 's2fq12c'), fill_value=0)
               .add(_num(ph, 's2fq12f'), fill_value=0)
               .add(_num(ph, 's2fq12i'), fill_value=0))
pieces.append(pd.DataFrame({
    't': WAVE, 'i': ph['i'], 'plot': ph['plot'], 'source': 'hired',
    'PersonDays': ph_hir, 'Wage': ph_hir_wage,
}))

# other: two activity blocks (q08, q14)
ph_oth = (_prod(ph, 's2fq08a', 's2fq08b')
          .add(_prod(ph, 's2fq08d', 's2fq08e'), fill_value=0)
          .add(_prod(ph, 's2fq08g', 's2fq08h'), fill_value=0)
          .add(_prod(ph, 's2fq14a', 's2fq14b'), fill_value=0)
          .add(_prod(ph, 's2fq14d', 's2fq14e'), fill_value=0)
          .add(_prod(ph, 's2fq14g', 's2fq14h'), fill_value=0))
pieces.append(pd.DataFrame({
    't': WAVE, 'i': ph['i'], 'plot': ph['plot'], 'source': 'other',
    'PersonDays': ph_oth, 'Wage': pd.NA,
}))

df = pd.concat(pieces, ignore_index=True)
df = plot_labor_finalize(df)

assert len(df) > 0, "plot_labor 2014-15 produced no rows"
assert df.index.is_unique, "Non-unique (t,i,plot,source) in plot_labor 2014-15"

to_parquet(df, 'plot_labor.parquet')
