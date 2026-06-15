"""Build livestock for Senegal EHCVM 2018-19 (GAP 4, item-level).

Single source file: s17_me_sen2018.dta — the EHCVM section-17 livestock
('Élevage') roster, one row per (household, species).  The roster is
already restricted to owned species (s17q03 == 1 for every row).

Columns:
  s17q02  species code (1-11, elevage__id; harmonize_species_ehcvm -> animal)
  s17q03  owned/raised this species? (1=Oui / 2=Non) — gate (all ==1 here)
  s17q06  number belonging to the household (HeadCount owned now)
  s17q08  number bought in the last 12 months (HeadAcquired)
  s17q10  number sold on the hoof in the last 12 months (HeadSold)

There is NO current herd-value question in EHCVM s17 (s17q05/q06 are head
counts; s17q09/q13 are purchase/sale FLOW values), so Value is not emitted.
``i`` is the Senegal EHCVM composite id (grappe + '0' + zero-padded menage)
matching sample().  Grain (t, i, animal); no v level (livestock is in the
framework _no_v_join set).

This script is SELF-CONTAINED — it inlines the Senegal household-id
formatter, the integer-code mapper, and the finishing tail rather than
importing them, so it has no cross-country / wave-module import coupling.
"""
import pandas as pd

import lsms_library.local_tools as tools
from lsms_library.local_tools import get_dataframe, to_parquet


def i(value):
    """Senegal EHCVM household id from (grappe, menage): grappe + '0' +
    zero-padded (2-digit) menage.  Matches sample().i / mapping.i.  Built
    positionally (``.iloc``) so a bare ``pd.Series([grappe, menage])``
    works, mirroring senegal.py's formatter."""
    g = tools.format_id(value.iloc[0])
    m = tools.format_id(value.iloc[1], zeropadding=2)
    if g is None or m is None:
        return None
    return g + '0' + m


def _harmonized_codes(tablename, key='Code', value='Preferred Label'):
    """Load a ``{int code -> Preferred Label}`` dict from
    ``categorical_mapping.org``.  Codes whose Preferred Label is blank /
    '---' map to NA so the corresponding row stays NaN."""
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
    """Map a numeric (raw Stata integer-code) Series through ``code_map``,
    returning a string Series with NA where the code is unmapped.  Source
    files must be loaded with ``convert_categoricals=False``."""
    out = series.astype('Int64').map(code_map)
    return out.astype('string').where(out.notna(), pd.NA)


def _finish_livestock(df, t):
    """Coerce numeric columns, drop unresolved-species placeholder rows, SUM
    the head counts within (t, i, animal) so each (household, canonical
    species) is one row, and build the (t, i, animal) index.  The EHCVM
    roster already reports one row per species (codes 1-11), so the sum is a
    no-op here, but it keeps the (t, i, animal) index unique for safety.
    HeadCount / HeadAcquired / HeadSold are Float64 (nullable head counts);
    min_count=1 keeps an all-NaN group NaN rather than 0.  Value is NOT a
    column: EHCVM s17 records no current herd value."""
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


# convert_categoricals=False keeps the integer s17 codes that the
# harmonize_species_ehcvm table keys on.
srcn = get_dataframe('../Data/s17_me_sen2018.dta', convert_categoricals=False)

ehcvm_map = _harmonized_codes('harmonize_species_ehcvm')

# The roster only carries owned species, but keep the gate for parity / safety.
owned = srcn['s17q03'] == 1
srcn = srcn[owned.values]

hh = srcn.apply(lambda r: i(pd.Series([r['grappe'], r['menage']])), axis=1)

df = pd.DataFrame({
    'i':            hh.values,
    'animal':       _map_codes(srcn['s17q02'], ehcvm_map).values,
    'HeadCount':    pd.to_numeric(srcn['s17q06'], errors='coerce').values,
    'HeadAcquired': pd.to_numeric(srcn['s17q08'], errors='coerce').values,
    'HeadSold':     pd.to_numeric(srcn['s17q10'], errors='coerce').values,
})

df = _finish_livestock(df, '2018-19')

assert len(df) > 0, 'livestock 2018-19 produced no rows'
to_parquet(df, 'livestock.parquet')
