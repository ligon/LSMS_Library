"""Build livestock for Guinea-Bissau EHCVM 2018-19 (GAP 4, item-level).

Cloned from Niger/2018-19/_/livestock.py but SELF-CONTAINED: the map /
finish helpers and the household-id builder are inlined here so this
script does not import the niger module.

Single source file: s17_me_gnb2018.dta — the EHCVM section-17 livestock
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

Guinea-Bissau note: the s17 species value LABELS are French/Portuguese,
but s17q02 carries the same integer 1-11 code scheme as the other EHCVM
countries, so keying harmonize_species_ehcvm on the integer code is
language-proof (the file is loaded convert_categoricals=False).

`i` is the EHCVM composite id built with Guinea-Bissau's own formatter
(format_id(grappe) + '0' + zero-padded format_id(menage)) — NOTE: unlike
Niger, Guinea-Bissau does NOT prepend the 'E_' panel-namespace prefix, so
this matches sample().i natively.  Grain (t, i, animal); no v level
(livestock is in the framework _no_v_join set).
"""
import pandas as pd

import lsms_library.local_tools as tools
from lsms_library.local_tools import get_dataframe, to_parquet


def i(value):
    """Guinea-Bissau EHCVM household id from (grappe, menage).

    Inlined from Guinea-Bissau/_/guinea_bissau.py so this script is
    self-contained.  No 'E_' prefix (Guinea-Bissau has a single EHCVM
    wave, no panel namespace collision to guard against)."""
    if isinstance(value, pd.Series):
        grappe = tools.format_id(value.iloc[0])
        menage = tools.format_id(value.iloc[1], zeropadding=2)
        if grappe is None or menage is None:
            return None
        return grappe + '0' + menage
    return tools.format_id(value)


def _map_codes(series, code_map):
    """Map a numeric (raw Stata integer-code) Series through ``code_map``,
    returning a string Series with NA where the code is unmapped.  The
    source file is loaded convert_categoricals=False so the codes arrive
    as integers."""
    out = series.astype('Int64').map(code_map)
    return out.astype('string').where(out.notna(), pd.NA)


def _species_map():
    """Load the EHCVM species code->Preferred Label dict (codes 1-11) from
    categorical_mapping.org.  Keyed on the integer Code."""
    raw = tools.get_categorical_mapping(tablename='harmonize_species_ehcvm',
                                        idxvars='Code',
                                        **{'Preferred Label': 'Preferred Label'})
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


def _finish_livestock(df, t):
    """Tag t, coerce numeric columns, drop unresolved-species rows, sum head
    counts within (t, i, animal) so each (household, canonical species) is
    one row, and build the (t, i, animal) index.  The EHCVM roster already
    reports one row per species (codes 1-11), so the sum is a no-op here;
    it is kept for parity with the Niger template.  min_count=1 keeps an
    all-NaN group NaN rather than 0."""
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


srcn = get_dataframe('../Data/s17_me_gnb2018.dta', convert_categoricals=False)

ehcvm_map = _species_map()

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

df = _finish_livestock(df, '2018-19')

assert len(df) > 0, 'livestock 2018-19 produced no rows'
to_parquet(df, 'livestock.parquet')
