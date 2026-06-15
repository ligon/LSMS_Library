"""Build livestock for Burkina Faso EHCVM 2018-19 (GAP 4, item-level).

Self-contained clone of Niger/2018-19/_/livestock.py (no ``import niger``):
the helper logic is inlined and the species code map is read from Burkina's
own categorical_mapping.org (harmonize_species_ehcvm, copied from Niger).

Single source file: s17_me_bfa2018.dta — the EHCVM section-17 livestock
('Élevage') roster, one row per (household, species).  The roster is already
restricted to owned species (s17q03 == 1 for every row).

Columns:
  s17q02  species code (1-11; harmonize_species_ehcvm -> animal)
  s17q03  owned/raised this species? (1=Oui / 2=Non) — gate (all ==1 here)
  s17q06  number belonging to the household (HeadCount owned now)
  s17q08  number bought in the last 12 months (HeadAcquired)
  s17q10  number sold on the hoof in the last 12 months (HeadSold)

There is NO current herd-value question in EHCVM s17, so Value is not emitted.

i is the EHCVM composite id reconciling with ``sample()``:
``format_id(grappe) + '0' + format_id(menage, zeropadding=2)`` via
``burkina_faso.ehcvm_i`` (NOT the older 3-digit ``i()``, which strands the
~17% of households with a 3-digit menage off sample — the GAP-4 i-key bug
this script fixes; verified 100% i-key ∩ sample).  Grain (t, i, animal); no
v level (livestock is in the framework's _no_v_join set).
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet, get_categorical_mapping
from burkina_faso import ehcvm_i


def _harmonized_codes(tablename, key='Code', value='Preferred Label'):
    """Load an ``{int code -> Preferred Label}`` dict from Burkina's
    categorical_mapping.org.  Blank / '---' labels map to NA."""
    raw = get_categorical_mapping(tablename=tablename, idxvars=key,
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
    """Map a raw integer-code Series through ``code_map`` -> string Series,
    NA where unmapped.  Source loaded convert_categoricals=False."""
    out = series.astype('Int64').map(code_map)
    return out.astype('string').where(out.notna(), pd.NA)


def _finish_livestock(df, t):
    """Coerce numeric columns, drop unresolved-species rows, SUM head counts
    within (t, i, animal) (a within-species sub-type collapse, a no-op for
    EHCVM which already reports one row per species), build (t, i, animal)."""
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


srcn = get_dataframe('../Data/s17_me_bfa2018.dta', convert_categoricals=False)

ehcvm_map = _harmonized_codes('harmonize_species_ehcvm')

# The roster only carries owned species, but keep the gate for parity / safety.
owned = srcn['s17q03'] == 1
srcn = srcn[owned.values]

hh = srcn.apply(lambda r: ehcvm_i(r['grappe'], r['menage']), axis=1)

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
