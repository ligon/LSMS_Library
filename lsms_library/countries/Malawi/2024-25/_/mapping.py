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
    are not formatted, so under `LSMS_BUILD_BACKEND=make` (which reads this block
    directly) it arrived as '' and minted 815 links to an id '' (review of GH
    #1275: 8,449 entries against 7,634 committed).
    """
    return df[df['previous_i'].map(format_id).notna().to_numpy()]
