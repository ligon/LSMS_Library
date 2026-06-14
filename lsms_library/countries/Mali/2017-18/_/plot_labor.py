"""Build plot_labor (item-level plot labor by source) for Mali EACI 2017-18.

GAP 3a (parity loop).  One row per (t, i, plot, source), source in
{family, hired, other}.

Sources (both at the plot grain):
  - eaci17_s11ep1.dta  post-planting (PP) plot-labor roster, s11e vars.
  - eaci17_s7ep2.dta   post-harvest (PH) plot-labor roster, s7e vars.

plot = "{field}_{parcel}" (s11eq01_s11eq02 for PP; s7eq01_s7eq02 for PH) —
the SAME plot id as crop_production / plot_inputs / plot_features.  i is
built from (grappe, exploitation) (the 2017-18 EACI household key).

REPORTED person-days per source only:
  PersonDays = persons * days-each, summed over the man/woman/child
               demographic splits and the two passages (PP + PH).
  Wage       = reported cash paid to hired labor (FCFA); NA for family/other.
NO total_labor_days / total_*_labor_days / hired_labor_value — those are
transformations over these rows.

Variable map traced from MLI_EACI2.do (WB harmonised plot-labor section):
  PP family (gate s11eq04): man s11eq05a1*s11eq05a2, woman s11eq05b1*05b2,
                            child s11eq05c1*05c2.
  PP hired  (gate s11eq06): man s11eq07a1*07a2 (wage s11eq07a3),
                            woman s11eq07b1*07b2 (wage s11eq07b3),
                            child s11eq07c1*07c2 (wage s11eq07c3).
  PP other  (gate s11eq08): man s11eq09a1*09a2, woman s11eq09b1*09b2,
                            child s11eq09c1*09c2.
  PH family: blocks q05 (a1/a2,b1/b2,c1/c2) and q11 (a1/a2,b1/b2,c1/c2).
  PH hired:  blocks q07 (a1/a2 wage a3; b...; c...) and q13 (a1/a2 wage a3;
             ...).  NB: the WB .do uses `s7eq13c1*s7eq13c3` for child days
             (c3 is the WAGE column) — a copy-paste slip; we use the
             consistent persons*days `s7eq13c1*s7eq13c2` so a wage value
             never leaks into PersonDays.
  PH other:  blocks q09 (a1/a2,b1/b2,c1/c2) and q15 (a1/a2,b1/b2,c1/c2).
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet
from mali import i as mali_i, plot_labor_finalize

WAVE = '2017-18'


def _hhid(df):
    return df.apply(lambda r: mali_i(pd.Series([r['grappe'], r['exploitation']])), axis=1)


def _plot(df, fcol, pcol):
    f = df[fcol].astype('Int64').astype('string')
    p = df[pcol].astype('Int64').astype('string')
    return (f + '_' + p).where(f.notna() & p.notna(), pd.NA)


# EACI "Manquant" / refusal sentinels on the persons / days / wage
# components (cf. MLI_EACI2.do which clears 99/999/99999999 before forming
# persons*days).  Cleared at the COMPONENT level so a sentinel never
# multiplies into a spurious huge person-day total.
_SENTINELS = [99, 999, 9999, 99999, 999999, 9999999, 99999999]


def _num(df, col):
    if col in df.columns:
        s = pd.to_numeric(df[col], errors='coerce')
        return s.mask(s.isin(_SENTINELS))
    return pd.Series(pd.NA, index=df.index, dtype='Float64')


def _prod(df, persons_col, days_col):
    return _num(df, persons_col) * _num(df, days_col)


def _sumcols(*series):
    out = series[0]
    for s in series[1:]:
        out = out.add(s, fill_value=0)
    return out


pieces = []

# ---------------- post-planting (PP) roster: s11e ------------------------
pp = get_dataframe('../Data/eaci17_s11ep1.dta').copy()
pp['i'] = _hhid(pp)
pp['plot'] = _plot(pp, 's11eq01', 's11eq02')

fam = _sumcols(_prod(pp, 's11eq05a1', 's11eq05a2'),
               _prod(pp, 's11eq05b1', 's11eq05b2'),
               _prod(pp, 's11eq05c1', 's11eq05c2'))
pieces.append(pd.DataFrame({'t': WAVE, 'i': pp['i'], 'plot': pp['plot'],
                            'source': 'family', 'PersonDays': fam, 'Wage': pd.NA}))

hir = _sumcols(_prod(pp, 's11eq07a1', 's11eq07a2'),
               _prod(pp, 's11eq07b1', 's11eq07b2'),
               _prod(pp, 's11eq07c1', 's11eq07c2'))
hir_wage = _sumcols(_num(pp, 's11eq07a3'), _num(pp, 's11eq07b3'),
                    _num(pp, 's11eq07c3'))
pieces.append(pd.DataFrame({'t': WAVE, 'i': pp['i'], 'plot': pp['plot'],
                            'source': 'hired', 'PersonDays': hir, 'Wage': hir_wage}))

oth = _sumcols(_prod(pp, 's11eq09a1', 's11eq09a2'),
               _prod(pp, 's11eq09b1', 's11eq09b2'),
               _prod(pp, 's11eq09c1', 's11eq09c2'))
pieces.append(pd.DataFrame({'t': WAVE, 'i': pp['i'], 'plot': pp['plot'],
                            'source': 'other', 'PersonDays': oth, 'Wage': pd.NA}))

# ---------------- post-harvest (PH) roster: s7e --------------------------
ph = get_dataframe('../Data/eaci17_s7ep2.dta').copy()
ph['i'] = _hhid(ph)
ph['plot'] = _plot(ph, 's7eq01', 's7eq02')

ph_fam = _sumcols(_prod(ph, 's7eq05a1', 's7eq05a2'),
                  _prod(ph, 's7eq05b1', 's7eq05b2'),
                  _prod(ph, 's7eq05c1', 's7eq05c2'),
                  _prod(ph, 's7eq11a1', 's7eq11a2'),
                  _prod(ph, 's7eq11b1', 's7eq11b2'),
                  _prod(ph, 's7eq11c1', 's7eq11c2'))
pieces.append(pd.DataFrame({'t': WAVE, 'i': ph['i'], 'plot': ph['plot'],
                            'source': 'family', 'PersonDays': ph_fam, 'Wage': pd.NA}))

ph_hir = _sumcols(_prod(ph, 's7eq07a1', 's7eq07a2'),
                  _prod(ph, 's7eq07b1', 's7eq07b2'),
                  _prod(ph, 's7eq07c1', 's7eq07c2'),
                  _prod(ph, 's7eq13a1', 's7eq13a2'),
                  _prod(ph, 's7eq13b1', 's7eq13b2'),
                  _prod(ph, 's7eq13c1', 's7eq13c2'))
ph_hir_wage = _sumcols(_num(ph, 's7eq07a3'), _num(ph, 's7eq07b3'),
                       _num(ph, 's7eq07c3'), _num(ph, 's7eq13a3'),
                       _num(ph, 's7eq13b3'), _num(ph, 's7eq13c3'))
pieces.append(pd.DataFrame({'t': WAVE, 'i': ph['i'], 'plot': ph['plot'],
                            'source': 'hired', 'PersonDays': ph_hir, 'Wage': ph_hir_wage}))

ph_oth = _sumcols(_prod(ph, 's7eq09a1', 's7eq09a2'),
                  _prod(ph, 's7eq09b1', 's7eq09b2'),
                  _prod(ph, 's7eq09c1', 's7eq09c2'),
                  _prod(ph, 's7eq15a1', 's7eq15a2'),
                  _prod(ph, 's7eq15b1', 's7eq15b2'),
                  _prod(ph, 's7eq15c1', 's7eq15c2'))
pieces.append(pd.DataFrame({'t': WAVE, 'i': ph['i'], 'plot': ph['plot'],
                            'source': 'other', 'PersonDays': ph_oth, 'Wage': pd.NA}))

df = pd.concat(pieces, ignore_index=True)
df = plot_labor_finalize(df)

assert len(df) > 0, "plot_labor 2017-18 produced no rows"
assert df.index.is_unique, "Non-unique (t,i,plot,source) in plot_labor 2017-18"

to_parquet(df, 'plot_labor.parquet')
