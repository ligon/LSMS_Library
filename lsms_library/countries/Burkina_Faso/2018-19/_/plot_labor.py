"""Build plot_labor for Burkina Faso EHCVM 2018-19 (GAP 3, item-level).

Self-contained clone of Niger/2018-19/_/plot_labor.py (no ``import niger``):
the labor-grid reducer and the (t, i, plot, source) finisher are inlined.
No categorical maps are needed (labor cells are numeric; the `source`
vocabulary is a tiny fixed in-code set).

Single source file: s16a_me_bfa2018.dta (agriculture-parcel module — the
same file plot_features reads, so plot ids align by construction).  EHCVM
records plot labor at the parcel level, family vs non-family (hired) only:

  Family labor (per family member, suffix _1.._N, three phases):
    s16aq33b_*  days each member worked, prep/sowing phase
    s16aq35b_*  days each member worked, maintenance phase
    s16aq37b_*  days each member worked, harvest phase
  -> family PersonDays = Σ over members & phases.

  Non-family / hired labor (per worker gender category, suffix _1.._N, three
  phases; a = #workers, b = days each, c = wage paid):
    s16aq39{a,b,c}_*  prep/sowing phase
    s16aq41{a,b,c}_*  maintenance phase
    s16aq43{a,b,c}_*  harvest phase
  -> hired PersonDays = Σ (a workers × b days); hired Wage = Σ c.

One row per (plot, source); plot = "{s16aq02}_{s16aq03}" aligns with
crop_production / plot_features plot_id.  i = EHCVM composite via
burkina_faso.ehcvm_i (reconciles 100% with sample()).  Grain
(t, i, plot, source); v joined from sample() by the framework.
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet, format_id
from burkina_faso import ehcvm_i


LABOR_SOURCE_FAMILY = 'family'
LABOR_SOURCE_HIRED = 'hired'


def plot_labor_ehcvm(src, t):
    """Reduce the s16a labor grids to one (i, plot, source) row per labor
    source, returning the long frame [i, plot, source, PersonDays, Wage]."""
    cols = list(src.columns)

    def _num(col):
        return pd.to_numeric(src[col], errors='coerce').astype('Float64')

    hh = src.apply(lambda r: ehcvm_i(r['grappe'], r['menage']), axis=1)
    field = src['s16aq02'].apply(format_id)
    parcel = src['s16aq03'].apply(format_id)
    plot = field.astype('string') + '_' + parcel.astype('string')

    # family: sum member-day cells across the three phase grids
    fam_cols = [c for c in cols
                if c.startswith('s16aq33b_')
                or c.startswith('s16aq35b_')
                or c.startswith('s16aq37b_')]
    fam_days = sum(_num(c).fillna(0) for c in fam_cols)
    fam_any = pd.concat([_num(c).notna() for c in fam_cols], axis=1).any(axis=1)
    fam_days = fam_days.where(fam_any.values, pd.NA)
    fam = pd.DataFrame({
        'i': hh.values, 'plot': plot.values,
        'source': LABOR_SOURCE_FAMILY,
        'PersonDays': fam_days.values, 'Wage': pd.NA,
    })

    # hired: Σ(#workers × days) person-days, Σ wage, over phase × category
    hired_days = pd.Series(0.0, index=src.index, dtype='Float64')
    hired_wage = pd.Series(0.0, index=src.index, dtype='Float64')
    any_days = pd.Series(False, index=src.index)
    any_wage = pd.Series(False, index=src.index)
    for base in ['s16aq39', 's16aq41', 's16aq43']:
        for ac in [c for c in cols if c.startswith(base + 'a_')]:
            suf = ac[len(base + 'a'):]   # e.g. '_1' (keeps the underscore)
            bc, cc = base + 'b' + suf, base + 'c' + suf
            workers = _num(ac)
            days = _num(bc) if bc in cols else pd.Series(pd.NA, index=src.index, dtype='Float64')
            wage = _num(cc) if cc in cols else pd.Series(pd.NA, index=src.index, dtype='Float64')
            pd_cell = workers * days
            hired_days = hired_days.add(pd_cell.fillna(0))
            any_days = any_days | pd_cell.notna().values
            hired_wage = hired_wage.add(wage.fillna(0))
            any_wage = any_wage | wage.notna().values
    hired_days = hired_days.where(any_days.values, pd.NA)
    hired_wage = hired_wage.where(any_wage.values, pd.NA)
    hired = pd.DataFrame({
        'i': hh.values, 'plot': plot.values,
        'source': LABOR_SOURCE_HIRED,
        'PersonDays': hired_days.values, 'Wage': hired_wage.values,
    })

    return pd.concat([fam, hired], ignore_index=True)


def _finish_plot_labor(df, t):
    """Sum PersonDays / Wage within (t, i, plot, source) (a within-source
    collapse of the member / category / phase strata), drop rows with no
    plot / source / reported labor, build (t, i, plot, source)."""
    df = df.copy()
    df['t'] = t
    df['source'] = df['source'].astype('string')
    df['plot'] = df['plot'].astype('string')
    df['PersonDays'] = pd.to_numeric(df.get('PersonDays'), errors='coerce').astype('Float64')
    if 'Wage' not in df.columns:
        df['Wage'] = pd.NA
    df['Wage'] = pd.to_numeric(df['Wage'], errors='coerce').astype('Float64')
    df = df[df['i'].notna() & df['plot'].notna() & df['source'].notna()]
    df = (df.groupby(['t', 'i', 'plot', 'source'], dropna=False)[['PersonDays', 'Wage']]
            .sum(min_count=1)
            .reset_index())
    df = df[df['PersonDays'].notna() | df['Wage'].notna()]
    df = df.set_index(['t', 'i', 'plot', 'source'])
    return df


src = get_dataframe('../Data/s16a_me_bfa2018.dta', convert_categoricals=False)
df = plot_labor_ehcvm(src, '2018-19')
df = _finish_plot_labor(df, '2018-19')

assert len(df) > 0, 'plot_labor 2018-19 produced no rows'
to_parquet(df, 'plot_labor.parquet')
