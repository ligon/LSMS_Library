"""Build plot_labor for Niger ECVMA 2014-15 (GAP 3, item-level).

Two source files (the WB ${lab_roster} / ${lab_roster2}):
  ECVMA2_AS2AP1.dta — post-planting (PP) plot labor.  Family days
    AS02AQ17B..AS02AQ22B; hired AS02AQ24{a gate / b,c,d days / e wage};
    other AS02AQ23{a gate / b,c,d days}.  plot = (AS01Q01, AS01Q03).
  ECVMA2_AS2AP2.dta — post-harvest (PH) plot labor.  Family days
    AS02AQ28B..33B, 36B..41B; hired AS02AQ35{a/b-d/e}; other
    AS02AQ34{a/b-d}.  plot = (AS02AQ01, AS02AQ03).

One row per (plot, source); PersonDays summed within source (man/woman/child
or per-member cells); Wage = cash paid to hired labor.  i from
(GRAPPE, MENAGE) via niger.i (matching crop_production / sample, which omit
EXTENSION).  plot = "{field}_{parcel}" aligns with crop_production's plot key.
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet, format_id
from niger import (i as niger_i, _coerce_days, _coerce_wage,
                   _finish_plot_labor, LABOR_SOURCE_FAMILY,
                   LABOR_SOURCE_HIRED, LABOR_SOURCE_OTHER)


def _hh(df):
    return df.apply(lambda r: niger_i(pd.Series([r['GRAPPE'], r['MENAGE']],
                                                index=['GRAPPE', 'MENAGE'])),
                    axis=1)


def _rows(df, source, field_col, parcel_col, day_cols,
          gate_col=None, wage_col=None):
    hid = _hh(df)
    field = df[field_col].apply(format_id)
    parcel = df[parcel_col].apply(format_id)
    plot = field.astype('string') + '_' + parcel.astype('string')
    days = sum(_coerce_days(df[c]).fillna(0) for c in day_cols)
    any_reported = pd.concat([_coerce_days(df[c]).notna() for c in day_cols],
                             axis=1).any(axis=1)
    days = days.where(any_reported, pd.NA)
    if gate_col is not None:
        gate = pd.to_numeric(df[gate_col], errors='coerce')
        days = days.where(gate != 2, 0)
    wage = _coerce_wage(df[wage_col]) if wage_col is not None else pd.NA
    return pd.DataFrame({
        'i': hid.values,
        'plot': plot.values,
        'source': source,
        'PersonDays': days.values,
        'Wage': wage.values if wage_col is not None else pd.NA,
    })


base = '../Data/NER_2014_ECVMA-II_v02_M_STATA8/'
pp = get_dataframe(base + 'ECVMA2_AS2AP1.dta', convert_categoricals=False)
ph = get_dataframe(base + 'ECVMA2_AS2AP2.dta', convert_categoricals=False)

frames = []
# --- post-planting (AS2AP1; plot = AS01Q01_AS01Q03) -----------------------
frames.append(_rows(pp, LABOR_SOURCE_FAMILY, 'AS01Q01', 'AS01Q03',
                    ['AS02AQ17B', 'AS02AQ18B', 'AS02AQ19B',
                     'AS02AQ20B', 'AS02AQ21B', 'AS02AQ22B']))
frames.append(_rows(pp, LABOR_SOURCE_HIRED, 'AS01Q01', 'AS01Q03',
                    ['AS02AQ24B', 'AS02AQ24C', 'AS02AQ24D'],
                    gate_col='AS02AQ24A', wage_col='AS02AQ24E'))
frames.append(_rows(pp, LABOR_SOURCE_OTHER, 'AS01Q01', 'AS01Q03',
                    ['AS02AQ23B', 'AS02AQ23C', 'AS02AQ23D'],
                    gate_col='AS02AQ23A'))
# --- post-harvest (AS2AP2; plot = AS02AQ01_AS02AQ03) ----------------------
frames.append(_rows(ph, LABOR_SOURCE_FAMILY, 'AS02AQ01', 'AS02AQ03',
                    ['AS02AQ28B', 'AS02AQ29B', 'AS02AQ30B', 'AS02AQ31B',
                     'AS02AQ32B', 'AS02AQ33B', 'AS02AQ36B', 'AS02AQ37B',
                     'AS02AQ38B', 'AS02AQ39B', 'AS02AQ40B', 'AS02AQ41B']))
frames.append(_rows(ph, LABOR_SOURCE_HIRED, 'AS02AQ01', 'AS02AQ03',
                    ['AS02AQ35B', 'AS02AQ35C', 'AS02AQ35D'],
                    gate_col='AS02AQ35A', wage_col='AS02AQ35E'))
frames.append(_rows(ph, LABOR_SOURCE_OTHER, 'AS02AQ01', 'AS02AQ03',
                    ['AS02AQ34B', 'AS02AQ34C', 'AS02AQ34D'],
                    gate_col='AS02AQ34A'))

df = pd.concat(frames, ignore_index=True)
df = _finish_plot_labor(df, '2014-15')

assert len(df) > 0, 'plot_labor 2014-15 produced no rows'
to_parquet(df, 'plot_labor.parquet')
