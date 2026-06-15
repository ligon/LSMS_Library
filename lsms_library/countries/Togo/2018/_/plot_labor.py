"""Build plot_labor for Togo EHCVM 2018 (item-level), cloned from the Niger
2018-19 EHCVM template.  SELF-CONTAINED: the build/finish helpers are
inlined here, so this script does NOT import togo or niger.

Single source file: ../Data1/s16a_me_tgo2018.dta (agriculture-parcel module
— the same file plot_features reads, so plot ids align by construction).
EHCVM records plot labor at the parcel level, family vs non-family (hired)
only (NO free/exchange "other" labor):

  Family labor (per family member, member-grid suffix _1.._N, three phases):
    s16aq33b_*  days each member worked, prep/sowing phase
    s16aq35b_*  days each member worked, maintenance phase
    s16aq37b_*  days each member worked, harvest phase
  -> family PersonDays = Σ over members & phases of those member-day cells.

  Non-family / hired labor (per worker gender category, grid suffix _1.._4,
  three phases; a = #workers, b = days each, c = wage paid):
    s16aq39{a,b,c}_*  prep/sowing phase
    s16aq41{a,b,c}_*  maintenance phase
    s16aq43{a,b,c}_*  harvest phase
  -> hired PersonDays = Σ over categories & phases of (a workers × b days);
     hired Wage       = Σ over categories & phases of c (cash paid).

*** TOGO PATH / WAVE QUIRKS: source in 2018/Data1/; wave dir 2018; code
suffix tgo2018; t='2018'; standardized module (NOT Togo_survey2018_*). ***

One row per (plot, source); plot = "{s16aq02}_{s16aq03}" aligns with
crop_production / plot_features plot_id.  `i` is Togo's composite household
id (grappe + '0' + zero-padded menage; NO 'E_' prefix), matching sample()
(t='2018').  Grain (t, i, plot, source).
"""
import sys

sys.path.append('../../_/')
import pandas as pd

import lsms_library.local_tools as tools
from lsms_library.local_tools import get_dataframe, to_parquet


LABOR_SOURCE_FAMILY = 'family'
LABOR_SOURCE_HIRED = 'hired'


def i(value):
    """Composite household id from (grappe, menage), matching Togo's
    sample().  Inlined VERBATIM from togo.i() / 2018/_/livestock.py: grappe
    + '0' separator + zero-padded (2-digit) menage.  NO 'E_' prefix."""
    return tools.format_id(value.iloc[0]) + '0' + tools.format_id(value.iloc[1], zeropadding=2)


def plot_labor_ehcvm(src, t):
    """Build plot_labor from the s16a parcel module (inlined from
    niger.plot_labor_ehcvm, but uses Togo's i()).  EHCVM records family vs
    non-family (hired) plot labor only.  Returns the long
    (i, plot, source, PersonDays, Wage) frame ready for _finish_plot_labor."""
    cols = list(src.columns)

    def _num(col):
        return pd.to_numeric(src[col], errors='coerce').astype('Float64')

    hh = src.apply(lambda r: i(pd.Series([r['grappe'], r['menage']],
                                         index=['grappe', 'menage'])), axis=1)
    field = src['s16aq02'].apply(tools.format_id)
    parcel = src['s16aq03'].apply(tools.format_id)
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
    """Common tail (inlined from niger._finish_plot_labor): SUM PersonDays /
    Wage within (t, i, plot, source) so the member / category / phase strata
    collapse onto the (plot, source) item grain.  Drops rows with no plot, no
    source, or no reported labor at all."""
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


src = get_dataframe('../Data1/s16a_me_tgo2018.dta', convert_categoricals=False)
df = plot_labor_ehcvm(src, '2018')
df = _finish_plot_labor(df, '2018')

assert len(df) > 0, 'plot_labor 2018 produced no rows'
to_parquet(df, 'plot_labor.parquet')
