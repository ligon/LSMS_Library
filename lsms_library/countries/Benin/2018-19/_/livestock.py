"""Build livestock for Benin EHCVM 2018-19 (GAP 4, item-level).

Single source file: s17_me_ben2018.dta — the EHCVM section-17 livestock
('Élevage') roster, one row per (household, species).  The roster is
already restricted to owned species (s17q03 == 1 for every row).

Columns:
  s17q02  species code (1-11; harmonize_species_ehcvm -> animal)
  s17q03  owned/raised this species? (1=Oui / 2=Non) — gate (all ==1 here)
  s17q06  number belonging to the household (HeadCount owned now)
  s17q08  number bought in the last 12 months (HeadAcquired)
  s17q10  number sold on the hoof in the last 12 months (HeadSold)

There is NO current herd-value question in EHCVM s17, so Value is not
emitted.  ``i`` is the Benin EHCVM composite household id built with
``benin.i()`` from a (grappe, menage) Series, so it matches ``sample().i``
natively (cf. the GhanaLSS GH #256 key-match).  Grain (t, i, animal); no v
level (livestock is in the framework's _no_v_join set).

This script is SELF-CONTAINED: the generic ``_map_codes`` /
``_finish_livestock`` logic (cloned from the Niger EHCVM reference) is
inlined below; only the load-bearing ``i()`` household-id constructor is
imported from Benin's own ``benin`` module so it cannot drift from
``sample()``.
"""
import sys

sys.path.append('../../_/')
import pandas as pd

import lsms_library.local_tools as tools
from lsms_library.local_tools import get_dataframe, to_parquet
from benin import i as benin_i


def _harmonized_codes(tablename, key='Code', value='Preferred Label'):
    """Load a ``{int code -> Preferred Label}`` dict from
    ``categorical_mapping.org`` for a harmonize_* table.  Codes whose
    Preferred Label is blank / '---' map to NA so the corresponding column
    stays NaN.  (Inlined from the Niger / Benin EHCVM helper.)"""
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
    files must be loaded with ``convert_categoricals=False`` so the codes
    arrive as integers.  (Inlined from the Niger EHCVM reference.)"""
    out = series.astype('Int64').map(code_map)
    return out.astype('string').where(out.notna(), pd.NA)


def _finish_livestock(df, t):
    """Common tail (inlined from the Niger EHCVM reference): coerce numeric
    columns, drop unresolved-species placeholder rows, SUM the head counts
    within (t, i, animal) so each (household, canonical species) is one row,
    and build the (t, i, animal) index.  HeadCount / HeadAcquired / HeadSold
    are Float64 (head counts, nullable); the sum uses min_count=1 so an
    all-NaN group stays NaN rather than becoming 0.  The EHCVM roster already
    reports one row per species (codes 1-11), so the sum is effectively a
    no-op; it is kept for parity with the ECVMA sub-type collapse and to
    guarantee a unique (t, i, animal) index ahead of the framework's
    canonical-index de-dup collapse."""
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


srcn = get_dataframe('../Data/s17_me_ben2018.dta', convert_categoricals=False)

ehcvm_map = _harmonized_codes('harmonize_species_ehcvm')

# The roster only carries owned species, but keep the gate for parity / safety.
owned = srcn['s17q03'] == 1
srcn = srcn[owned.values]

# Household id: EHCVM composite (grappe, menage) via benin.i(), matching
# sample().i.  benin.i() reads the Series positionally (grappe, menage).
hh = srcn.apply(lambda r: benin_i(pd.Series([r['grappe'], r['menage']])),
                axis=1)

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
