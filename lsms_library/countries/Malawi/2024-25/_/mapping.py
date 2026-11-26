"""Malawi 2024-25 sample/ID hooks (PR #1604).

The weights these hooks carry are served as wired (maintainer ruling
2026-11-12, "Lift hold, rebase, merge"); that ruling covers this wave's
content only and does not settle decision 48's joint-inclusion question.
"""
import sys
from pathlib import Path
import pandas as pd
from lsms_library.local_tools import format_id
sys.path.append(str(Path(__file__).resolve().parents[2] / '_'))
from malawi import CS_24, ihps24_refreshed

def cs_i(value):
    # Bare IHS6 case_ids coincide with 3 IHS3, 202 IHS4 and 83 IHS5 case_ids,
    # so a shared or absent prefix would make false panel links (GH #1278).
    return CS_24 + format_id(value.iloc[0])

def sample(df):
    """Move the 815's IHPS id and weight to their IHS6 rows, drop the copies.

    Fails closed: a refreshment household with no IHS6 row in this wave's
    cover would otherwise lose its weight and its link without a trace.
    """
    refreshed = ihps24_refreshed()
    is_copy = df.index.get_level_values('i').isin(list(refreshed))
    copies, kept = df[is_copy], df[~is_copy].drop(columns='_ihs6_case_id')
    target = CS_24 + copies['_ihs6_case_id'].map(format_id)
    kept_i = kept.index.get_level_values('i')
    missing = set(target) - set(kept_i)
    if missing:
        raise ValueError(f'{len(missing)} IHPS 2024 refreshment households '
                         'have no IHS6 row in the cover file')
    for col in ('ihps_hhid', 'ihps_weight'):
        carried = pd.Series(copies[col].to_numpy(), index=target.to_numpy())
        kept[col] = kept[col].where(kept[col].notna(), kept_i.map(carried))
    return kept

def panel_ids(df):
    """Drop rows with no previous-round id: the 815 refreshment households.

    `y4_hhid` is blank on them.  `_/panel_ids.py` reads it through `format_id`,
    which makes a blank missing, but `previous_i` is a `myvars` column, and those
    are not formatted, so a route that reads this block directly saw it as ''
    and minted 815 links to an id '' (review of GH #1275: 8,449 entries against
    7,634 committed).  That route is the wave-level `Country['2024-25']
    .panel_ids()`, or `Country._compute_panel_ids` if the committed JSON were
    absent; with the JSON present both backends serve it (PR #1604 review).
    """
    return df[df['previous_i'].map(format_id).notna().to_numpy()]


def housing(df):
    """Serve each Module F interview once, after verifying the refresh copies."""
    helpers = ['_housing_module_key', '_housing_cover_key',
               '_housing_previous_key', '_housing_ihs6_key']
    if not set(helpers).issubset(df):
        raise ValueError('Malawi 2024-25 housing requires Module F and the IHPS panel cover')
    if df.index.to_frame(index=False).isna().any(axis=None) or df.index.has_duplicates:
        raise ValueError('Malawi 2024-25 housing has missing or duplicate household keys')

    i = df.index.get_level_values('i')
    panel = ~i.str.startswith(CS_24)
    in_module = df[helpers[0]].map(format_id).notna().to_numpy()
    in_cover = df[helpers[1]].map(format_id).notna().to_numpy()
    if not in_module.all() or (in_cover != panel).any():
        raise ValueError('Malawi 2024-25 housing and IHPS cover disagree on household membership')

    copy = panel & df[helpers[2]].map(format_id).isna().to_numpy()
    target = CS_24 + df.loc[copy, helpers[3]].map(format_id)
    keys = pd.MultiIndex.from_arrays([df.index.get_level_values('t')[copy], target],
                                    names=df.index.names)
    if target.isna().any() or keys.has_duplicates or not keys.isin(df.index[~panel]).all():
        raise ValueError('Malawi 2024-25 housing has missing or duplicate IHS6 refresh counterparts')

    # The two releases use the same native F08/F09 codebooks. Compare the
    # decoded answers before finalization; preserve ordinary missingness.
    materials = ['Roof', 'Floor']
    ihps = df.loc[copy, materials].set_axis(keys)
    ihs6 = df.loc[~panel, materials].reindex(keys)
    equal = ihps.eq(ihs6) | (ihps.isna() & ihs6.isna())
    if not equal.fillna(False).to_numpy().all():
        raise ValueError('Malawi 2024-25 IHS6 and IHPS refresh housing answers disagree')
    return df.loc[~copy].drop(columns=helpers)
