#!/usr/bin/env python
"""Malawi-specific helpers for wave-level food_acquired.py scripts.

The live surface is three functions used by the four IHS3+ wave scripts
(2010-11, 2013-14, 2016-17, 2019-20) to apply Malawi's region-keyed
unit-conversion CSV and to handle "300 grams"-style free-text units.
Other helpers (roster decomposition, get_other_features, etc.) were
removed in 2026-05-05 alongside the shadowed
food_prices_quantities_and_expenditures.py — see GH #218.
"""

import pandas as pd
import numpy as np
import sys
sys.path.append('../../../_/')
from lsms_library.local_tools import conversion_table_matching_global


def _extract_kg_conversion(series):
    """Extract kilogram conversion factors from a unit-detail string series.

    Parses patterns like '300 grams', '1kg', '2 kilo' and returns
    a Series of conversion factors in kilograms.
    """
    grams = r'(\d+)\s*g(?:\s+|r)'
    kgs = r'(\d+)\s*k(?:g|ilo)'

    lower = series.str.lower()
    conv = pd.concat([lower.str.extract(grams).astype(float) * 0.01,
                      lower.str.extract(kgs).astype(float)], axis=0).dropna()
    return conv


def handling_unusual_units(df, suffixes=None):
    """Convert unusual unit descriptions to kg-based quantities.

    Parameters
    ----------
    df : DataFrame
    suffixes : list[str], optional
        Column suffixes to process (e.g. ``['consumed', 'bought']``).
        For each suffix, expects columns ``unitsdetail_{suffix}``,
        ``cfactor_{suffix}``, ``quantity_{suffix}``, and ``units_{suffix}``.
        Defaults to ``['consumed', 'bought']`` for backward compatibility.
    """
    if suffixes is None:
        suffixes = ['consumed', 'bought']

    for suffix in suffixes:
        detail_col = f'unitsdetail_{suffix}'
        cfactor_col = f'cfactor_{suffix}'
        quantity_col = f'quantity_{suffix}'
        units_col = f'units_{suffix}'
        u_col = f'u_{suffix}'

        if detail_col not in df.columns:
            continue

        conv_kg = _extract_kg_conversion(df[detail_col])

        df[cfactor_col] = df.apply(lambda x, c=cfactor_col: x[c] or conv_kg, axis=1)
        df[quantity_col] = df[quantity_col].mul(df[cfactor_col].fillna(1))
        df[u_col] = np.where(~df[cfactor_col].isna(), 'kg', df[detail_col])
        df[u_col] = df[u_col].replace('nan', pd.NA).fillna(df[units_col])

    return df


def Sex(value):
    if isinstance(value, str) and value.strip():
        return value.strip().upper()[0]
    else:
        return np.nan


def harmonize_food_labels(df, level='i'):
    """Apply the cross-wave union of Malawi's harmonize_food map to ``df``.

    The wave-level food_acquired.py scripts apply
    ``df['i'].astype(str).str.capitalize()`` before renaming, which produces
    sentence-cased labels (e.g. ``'Sugar cane'``).  The per-wave columns of
    ``harmonize_food`` in ``categorical_mapping.org`` mix Title-case and
    sentence-case entries, so the per-wave rename via
    ``get_categorical_mapping(idxvars={'j': wave})`` silently misses any
    label whose harmonize_food entry is in a different case than the
    post-``.capitalize()`` data — see GH #216.

    This helper sidesteps the drift by building a single label map from
    *all* wave columns of ``harmonize_food`` (including each value's
    ``.capitalize()`` variant) and applying it once.  A label that's
    documented in *any* wave column gets resolved to its Preferred Label
    regardless of which wave's data we're processing.

    The Preferred Label column is honoured as-is; any truncation (e.g.
    ``'Maize Ufa Mgaiwa (Normal F'``) carries through to the output.
    Truncation cleanup is a separate concern (GH #169 / #216 follow-up).

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame whose index includes the food-item level.
    level : str, default 'i'
        Index level name carrying the food labels.  In Malawi's wave-level
        builds the item lives on ``'i'`` (the framework's ``map_index``
        swaps it to canonical ``'j'`` downstream).

    Returns
    -------
    pd.DataFrame
        ``df`` with food labels remapped to Preferred Labels where the
        union map covers them.  Labels not in the map pass through
        unchanged.
    """
    import os
    from lsms_library.local_tools import all_dfs_from_orgfile

    org_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            'categorical_mapping.org')
    hf = all_dfs_from_orgfile(org_path)['harmonize_food']

    unify = {}
    skip_cols = {'Preferred Label', 'GD Category'}
    for col in hf.columns:
        if col in skip_cols:
            continue
        for _, row in hf.iterrows():
            v = row.get(col)
            p = row.get('Preferred Label')
            if pd.isna(v) or pd.isna(p):
                continue
            v_str = str(v).strip()
            if v_str in ('', '---'):
                continue
            # Map both the literal harmonize_food entry and its
            # .capitalize() form (since wave scripts apply .capitalize()
            # to the data before this rename runs).
            unify.setdefault(v_str, p)
            unify.setdefault(v_str.capitalize(), p)

    return df.rename(index=unify, level=level)

def conversion_table_matching(df, conversions, conversion_label_name, num_matches=3, cutoff=0.6):
    return conversion_table_matching_global(df, conversions, conversion_label_name,
                                            num_matches=num_matches, cutoff=cutoff)


# ---- Food-label normalization & harmonize_food application ----------------
#
# Three flavors of mangled en-dash show up in the raw food-item .dta values
# across waves, depending on the source encoding and pyreadstat decode path:
#   - '\x96'  : cp1252 byte for en-dash, preserved when the file is read as
#               latin1 (seen in 2010-11, 2013-14).
#   - '�': Unicode replacement char from a failed UTF-8 decode (2016-17).
#   - 'ï¿½'   : UTF-8 mojibake of '�' (the bytes 0xef 0xbf 0xbd
#               re-decoded as latin1 and re-encoded as UTF-8) (2016-17).
# All three should become a proper en-dash before the harmonize_food rename,
# otherwise rows like 'Citrus – naartje, orange, etc.' fail to match.

_ENDASH_MOJIBAKE = [('\x96', '–'), ('ï¿½', '–'), ('�', '–')]


def normalize_food_label(s):
    """Replace mangled en-dashes in a food-label Series.

    Apply *after* ``.str.capitalize()`` in wave scripts so that the data
    side matches the dict keys produced by :func:`apply_harmonize_food`.
    """
    out = s
    for bad, good in _ENDASH_MOJIBAKE:
        out = out.str.replace(bad, good, regex=False)
    return out


def _normalize_label_key(k):
    """Normalize a single dict key to mirror the wave-script data path.

    Applies ``str.capitalize()`` (single-word title-case as in every wave
    script's ``df['i'] = ... .str.capitalize()`` line) followed by the same
    en-dash repair as :func:`normalize_food_label`.  2004-05's wave script
    skips ``capitalize()`` but its column entries in categorical_mapping.org
    are already in capitalize-form, so this is a no-op there.
    """
    if not isinstance(k, str):
        return k
    out = k.capitalize()
    for bad, good in _ENDASH_MOJIBAKE:
        out = out.replace(bad, good)
    return out


def apply_harmonize_food(df, wave, level='i'):
    """Rename *level* of *df*'s index via Malawi's harmonize_food table.

    Builds the dict from the *wave* column of
    ``../../_/categorical_mapping.org``, then normalizes each dict key with
    :func:`_normalize_label_key` so that case drift and encoding mojibake
    between the .dta source and the org table never silently break the
    mapping.

    .. note:: This intentionally calls :func:`df_from_orgfile` directly
        instead of :func:`get_categorical_mapping`, because the latter
        runs every ``idxvars`` value through
        :func:`local_tools.format_id`, which does ``.split('.')[0]`` to
        strip Stata's ``"123.0"`` → ``"123"``.  For food labels ending in
        ``"etc."`` (and anything else with an internal period) that
        truncates the key and silently breaks the mapping.
    """
    from lsms_library.local_tools import df_from_orgfile
    import os
    for d in ('./', '../../_/', '../../../_/'):
        p = os.path.join(d, 'categorical_mapping.org')
        if os.path.exists(p):
            tab = df_from_orgfile(p, name='harmonize_food')
            break
    else:
        raise FileNotFoundError(
            "categorical_mapping.org not found in any of "
            "'./', '../../_/', '../../../_/'"
        )
    if wave not in tab.columns:
        raise KeyError(f"Wave column {wave!r} not in harmonize_food table; "
                       f"available: {[c for c in tab.columns if c not in ('Preferred Label','GD Category')]}")
    pairs = tab[[wave, 'Preferred Label']].dropna()
    raw = dict(zip(pairs[wave], pairs['Preferred Label']))
    labelsd = {_normalize_label_key(k): v for k, v in raw.items()}
    return df.rename(index=labelsd, level=level)

