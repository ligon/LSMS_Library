"""Build plot_labor for Niger ECVMA 2011-12 (GAP 3, item-level).

Two source files (the WB ${lab_roster} / ${lab_roster2}):
  ecvmaas1_p1.dta — post-planting (PP) plot labor.  Family days
    as02aq20b..as02aq25b; hired as02aq27{a gate / b,c,d days / e wage};
    other (free) as02aq26{a gate / b,c,d days}.  plot = (as01q03, as01q05).
  ecvmaas1_p2.dta — post-harvest (PH) plot labor.  Family days
    as02aq28b..33b, 36b..41b; hired as02aq35{a/b-d/e} + as02aq43{a/b-d/e};
    other as02aq34{a/b-d} + as02aq42{a/b-d}.  plot = (as01q03, as01q05).

One row per (plot, source); PersonDays = reported person-days of that
source (man + woman + child or per-family-member cells, summed within the
source — NOT a cross-source rollup; see niger.py module note).  Wage = cash
paid to hired labor (NaN for family/other).  PP and PH rows for the same
(plot, source) are concatenated and summed by _finish_plot_labor.

``hid`` already equals grappe*100+menage (the canonical 2011-12 household
id).  plot = "{as01q03}_{as01q05}" aligns with crop_production's plot key.
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet, format_id
from niger import (i as niger_i, _coerce_days, _coerce_wage,
                   _finish_plot_labor, LABOR_SOURCE_FAMILY,
                   LABOR_SOURCE_HIRED, LABOR_SOURCE_OTHER)


def _plot_key(df):
    hid = df['hid'].apply(lambda x: niger_i(x) if pd.notna(x) else pd.NA)
    field = df['as01q03'].apply(format_id)
    parcel = df['as01q05'].apply(format_id)
    plot = field.astype('string') + '_' + parcel.astype('string')
    return hid, plot


def _rows(df, source, day_cols, gate_col=None, wage_col=None):
    """One reported (plot, source) row per source-line: PersonDays = sum of
    the day cells (man/woman/child or per-family-member), gated to NA where
    the survey's "did you use this source?" gate says No (==2)."""
    hid, plot = _plot_key(df)
    days = sum(_coerce_days(df[c]).fillna(0) for c in day_cols)
    # Restore NA where every cell was NA (fillna(0) above masked that).
    any_reported = pd.concat([_coerce_days(df[c]).notna() for c in day_cols],
                             axis=1).any(axis=1)
    days = days.where(any_reported, pd.NA)
    if gate_col is not None:
        gate = pd.to_numeric(df[gate_col], errors='coerce')
        days = days.where(gate != 2, 0)  # gate==No -> zero days, not NA
    wage = _coerce_wage(df[wage_col]) if wage_col is not None else pd.NA
    return pd.DataFrame({
        'i': hid.values,
        'plot': plot.values,
        'source': source,
        'PersonDays': days.values,
        'Wage': wage.values if wage_col is not None else pd.NA,
    })


pp = get_dataframe('../Data/NER_2011_ECVMA_v01_M_Stata8/ecvmaas1_p1.dta',
                   convert_categoricals=False)
ph = get_dataframe('../Data/NER_2011_ECVMA_v01_M_Stata8/ecvmaas1_p2.dta',
                   convert_categoricals=False)

frames = []
# --- post-planting (p1) ---------------------------------------------------
frames.append(_rows(pp, LABOR_SOURCE_FAMILY,
                    ['as02aq20b', 'as02aq21b', 'as02aq22b',
                     'as02aq23b', 'as02aq24b', 'as02aq25b']))
frames.append(_rows(pp, LABOR_SOURCE_HIRED,
                    ['as02aq27b', 'as02aq27c', 'as02aq27d'],
                    gate_col='as02aq27a', wage_col='as02aq27e'))
frames.append(_rows(pp, LABOR_SOURCE_OTHER,
                    ['as02aq26b', 'as02aq26c', 'as02aq26d'],
                    gate_col='as02aq26a'))
# --- post-harvest (p2) ----------------------------------------------------
frames.append(_rows(ph, LABOR_SOURCE_FAMILY,
                    ['as02aq28b', 'as02aq29b', 'as02aq30b', 'as02aq31b',
                     'as02aq32b', 'as02aq33b', 'as02aq36b', 'as02aq37b',
                     'as02aq38b', 'as02aq39b', 'as02aq40b', 'as02aq41b']))
frames.append(_rows(ph, LABOR_SOURCE_HIRED,
                    ['as02aq35b', 'as02aq35c', 'as02aq35d'],
                    gate_col='as02aq35a', wage_col='as02aq35e'))
frames.append(_rows(ph, LABOR_SOURCE_HIRED,
                    ['as02aq43b', 'as02aq43c', 'as02aq43d'],
                    gate_col='as02aq43a', wage_col='as02aq43e'))
frames.append(_rows(ph, LABOR_SOURCE_OTHER,
                    ['as02aq34b', 'as02aq34c', 'as02aq34d'],
                    gate_col='as02aq34a'))
frames.append(_rows(ph, LABOR_SOURCE_OTHER,
                    ['as02aq42b', 'as02aq42c', 'as02aq42d'],
                    gate_col='as02aq42a'))

df = pd.concat(frames, ignore_index=True)
df = _finish_plot_labor(df, '2011-12')

assert len(df) > 0, 'plot_labor 2011-12 produced no rows'
to_parquet(df, 'plot_labor.parquet')
