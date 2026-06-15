"""Build livestock for Togo EHCVM 2018 (item-level), cloned from the
Niger 2018-19 EHCVM template.  SELF-CONTAINED: the map/finish helpers
are inlined here, so this script does NOT import togo or niger.

Single source file: ../Data1/s17_me_tgo2018.dta — the EHCVM section-17
livestock ('Élevage') roster, one row per (household, species).  The
roster is already restricted to owned species (s17q03 == 1 for every
row); the gate is kept for parity / safety.

*** SOURCE LOCATION: the EHCVM livestock module lives in 2018/Data1/
(NOT 2018/Data/, which holds only _forEthan extracts) and the wave dir
is named 2018 (NOT 2018-19).  Togo's repo also ships bespoke
Togo_survey2018_* files — DO NOT use those; this uses the standardized
EHCVM module s17_me_tgo2018.dta. ***

Columns:
  s17q02  species code (1-11, elevage__id; harmonize_species_ehcvm -> animal)
  s17q03  owned/raised this species? (1=Oui / 2=Non) — gate (all ==1 here)
  s17q06  number belonging to the household (HeadCount owned now)
  s17q08  number bought in the last 12 months (HeadAcquired)
  s17q10  number sold on the hoof in the last 12 months (HeadSold)

There is NO current herd-value question in EHCVM s17, so Value is not
emitted.  i is Togo's composite id (grappe + '0' + zero-padded menage,
inlined verbatim from togo.i() — NO 'E_' prefix), matching Togo's
sample() and plot_features (which use t='2018').  Grain (t, i, animal);
no v level (livestock is in the framework's _no_v_join set).
"""
import sys

sys.path.append('../../_/')
import pandas as pd

import lsms_library.local_tools as tools
from lsms_library.local_tools import get_dataframe, to_parquet


def i(value):
    """Composite household id from (grappe, menage), matching Togo's
    sample().  Inlined VERBATIM from togo.i() (Togo/_/togo.py): grappe +
    '0' separator + zero-padded (2-digit) menage.  Togo's sample() and
    plot_features() use this exact form (NO 'E_' prefix), so livestock i
    matches sample() 1:1 (verified: 100% intersection)."""
    return tools.format_id(value.iloc[0]) + '0' + tools.format_id(value.iloc[1], zeropadding=2)


def _harmonized_codes(tablename, key='Code', value='Preferred Label'):
    """Load a {int code -> Preferred Label} dict from categorical_mapping.org.
    Blank / '---' Preferred Labels map to NA.  (Inlined from togo._/.)"""
    raw = tools.get_categorical_mapping(tablename=tablename, idxvars=key,
                                        **{value: value})
    out = {}
    for k, v in raw.items():
        try:
            int_k = int(k)
        except (TypeError, ValueError):
            int_k = k
        if pd.isna(v) or str(v).strip() in ('---', ''):
            out[int_k] = pd.NA
        else:
            out[int_k] = str(v).strip()
    return out


def _map_codes(series, code_map):
    """Map a numeric raw Stata integer-code Series through code_map, returning
    a string Series with NA where unmapped.  Source loaded with
    convert_categoricals=False so codes arrive as integers."""
    out = series.astype('Int64').map(code_map)
    return out.astype('string').where(out.notna(), pd.NA)


def _finish_livestock(df, t):
    """Coerce numeric columns, drop unresolved-species placeholder rows, SUM
    head counts within (t, i, animal), build the (t, i, animal) index.
    HeadCount / HeadAcquired / HeadSold are Float64 (nullable); min_count=1
    keeps an all-NaN group NaN.  No Value column (EHCVM records no herd
    value)."""
    cols = ['HeadCount', 'HeadAcquired', 'HeadSold']
    for col in cols:
        if col not in df.columns:
            df[col] = pd.NA
        df[col] = pd.to_numeric(df[col], errors='coerce').astype('Float64')
    df['t'] = t
    df['animal'] = df['animal'].astype('string')
    df = df[df['animal'].notna() & df['i'].notna()]
    df = (df.groupby(['t', 'i', 'animal'], dropna=False)[cols]
            .sum(min_count=1)
            .reset_index())
    keep = ['t', 'i', 'animal'] + cols
    df = df[keep]
    df = df.set_index(['t', 'i', 'animal'])
    return df


srcn = get_dataframe('../Data1/s17_me_tgo2018.dta', convert_categoricals=False)

ehcvm_map = _harmonized_codes('harmonize_species_ehcvm')

# The roster only carries owned species, but keep the gate for parity / safety.
owned = srcn['s17q03'] == 1
srcn = srcn[owned.values]

hh = srcn.apply(lambda r: i(pd.Series([r['grappe'], r['menage']],
                                      index=['grappe', 'menage'])), axis=1)

df = pd.DataFrame({
    'i':            hh.values,
    'animal':       _map_codes(srcn['s17q02'], ehcvm_map).values,
    'HeadCount':    pd.to_numeric(srcn['s17q06'], errors='coerce').values,
    'HeadAcquired': pd.to_numeric(srcn['s17q08'], errors='coerce').values,
    'HeadSold':     pd.to_numeric(srcn['s17q10'], errors='coerce').values,
})

df = _finish_livestock(df, '2018')

assert len(df) > 0, 'livestock 2018 produced no rows'
to_parquet(df, 'livestock.parquet')
