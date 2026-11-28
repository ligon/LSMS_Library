"""Malawi 2024-25 sample/ID hooks (PR #1604).

The weights these hooks carry are served as wired (maintainer ruling
2026-11-12, "Lift hold, rebase, merge"); that ruling covers this wave's
content only and does not settle decision 48's joint-inclusion question.
"""
import sys
from pathlib import Path
import pandas as pd
from lsms_library.local_tools import format_id


def shocks(df):
    """Serve IHS6 and retained IHPS Module U, with IHS6 taking precedence."""
    cover = ['_shocks_cover_key', '_shocks_previous_key', '_shocks_ihs6_key']
    if not set(cover).issubset(df):
        raise ValueError('Malawi 2024-25 shocks require the IHPS panel cover')
    if df.index.to_frame(index=False).isna().any(axis=None) or df.index.has_duplicates:
        raise ValueError('Malawi 2024-25 shocks have missing or duplicate household-shock keys')

    i = df.index.get_level_values('i')
    panel = ~i.str.startswith(CS_24)
    matched_cover = df[cover[0]].map(format_id).notna().to_numpy()
    if (matched_cover != panel).any():
        raise ValueError('Malawi 2024-25 shocks and the IHPS cover disagree on household identity')

    # A blank previous-round ID identifies a refresh copy. On the retained
    # rows, the cover case_id is an older baseline key, not an IHS6 key.
    copy = panel & df[cover[1]].map(format_id).isna().to_numpy()
    target = CS_24 + df.loc[copy, cover[2]].map(format_id)
    matches = pd.DataFrame({'panel': i[copy], 'ihs6': target.to_numpy()}).drop_duplicates()
    if target.isna().any() or matches['ihs6'].duplicated().any():
        raise ValueError('Malawi 2024-25 shocks have missing or duplicate IHS6 refresh counterparts')

    keys = pd.MultiIndex.from_arrays(
        [df.index.get_level_values('t')[copy], target,
         df.index.get_level_values('Shock')[copy]],
        names=df.index.names,
    )
    ihs6 = df.loc[~panel & i.isin(target)].index
    if len(keys) != len(ihs6) or not keys.isin(ihs6).all():
        raise ValueError('Malawi 2024-25 refresh and IHS6 disagree on shared shock keys')

    # The released coping answers can differ. Keep the whole IHS6 row
    # without reconciling or replacing any of its ranked responses.
    return df.loc[~copy].drop(columns=cover)


sys.path.append(str(Path(__file__).resolve().parents[2] / '_'))
from malawi import CS_24, ihps24_refreshed

from malawi_sample_component import ihs_component, ihps_component

def cs_i(value):
    # Bare IHS6 case_ids coincide with 3 IHS3, 202 IHS4 and 83 IHS5 case_ids,
    # so a shared or absent prefix would make false panel links (GH #1278).
    return CS_24 + format_id(value.iloc[0])
# Education uses the interview-local Module C person key.

def individual_education(df):
    """Compare the native qualification before dropping its refresh copy."""
    cover = ['_education_cover_key', '_education_previous_key', '_education_ihs6_key']
    native = '_education_native_c09'
    if not {*cover, native}.issubset(df):
        raise ValueError('Malawi 2024-25 education requires Module C and the IHPS panel cover')
    if df.index.to_frame(index=False).isna().any(axis=None) or df.index.has_duplicates:
        raise ValueError('Malawi 2024-25 education has missing or duplicate person keys')

    i = df.index.get_level_values('i')
    panel = ~i.str.startswith(CS_24)
    matched_cover = df[cover[0]].map(format_id).notna().to_numpy()
    if (matched_cover != panel).any():
        raise ValueError('Malawi 2024-25 education and IHPS cover disagree on household identity')

    copy = panel & df[cover[1]].map(format_id).isna().to_numpy()
    target = CS_24 + df.loc[copy, cover[2]].map(format_id)
    # More than one IHPS household may not split the same IHS6 roster.
    households = pd.DataFrame({'panel': i[copy], 'ihs6': target.to_numpy()}).drop_duplicates()
    if target.isna().any() or households['ihs6'].duplicated().any():
        raise ValueError('Malawi 2024-25 education has missing or duplicate IHS6 refresh counterparts')

    keys = pd.MultiIndex.from_arrays(
        [df.index.get_level_values('t')[copy], target,
         df.index.get_level_values('pid')[copy]],
        names=df.index.names,
    )
    ihs6 = df.loc[~panel & i.isin(target), native]
    if len(ihs6) != len(keys) or not keys.isin(ihs6.index).all():
        raise ValueError('Malawi 2024-25 refresh and IHS6 education disagree on person keys')

    ihps = df.loc[copy, native].set_axis(keys)
    ihs6 = ihs6.reindex(keys)
    equal = ihps.eq(ihs6) | (ihps.isna() & ihs6.isna())
    if not equal.fillna(False).all():
        raise ValueError('Malawi 2024-25 refresh and IHS6 qualifications disagree')
    return df.loc[~copy].drop(columns=[native, *cover])


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

def interview_date(df):
    """Verify the IHPS refresh copies before melting the unique interview dates."""
    from malawi import interview_date as melt_interview_dates

    ids = df.index.get_level_values('i')
    if ids.hasnans or not ids.is_unique:
        raise ValueError('Malawi 2024-25 interview_date: missing or duplicate cover IDs')

    refreshed = ihps24_refreshed()
    if any(copy is None or case is None for copy, case in refreshed.items()):
        raise ValueError('Malawi 2024-25 interview_date: missing refresh or IHS6 case ID')
    targets = pd.Index([CS_24 + case for case in refreshed.values()])
    if not targets.is_unique or not targets.isin(ids).all() or not set(refreshed) <= set(ids):
        raise ValueError('Malawi 2024-25 interview_date: missing or non-unique refresh counterparts')
    dates = pd.Series(df['int_t'].array, index=ids)
    if not dates.reindex(list(refreshed)).reset_index(drop=True).equals(
            dates.reindex(targets).reset_index(drop=True)):
        raise ValueError('Malawi 2024-25 interview_date: IHS6 and IHPS refresh dates disagree')

    return melt_interview_dates(df[~ids.isin(refreshed)])


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


def household_roster(df):
    """Serve each Module B interview once (ledger 1275-malawi-roster, §§3-5).

    The panel cover's blank previous-round ID distinguishes refresh copies.
    Within those interviews the IHS6 PID corresponds to the IHPS local
    id_code, not to the IHPS longitudinal serial PID.
    """
    cover = ['_roster_y5_hhid', '_roster_y4_hhid', '_roster_ihs6_case_id']
    if not set(cover).issubset(df):
        raise ValueError('Malawi 2024-25 roster requires the IHPS panel cover')
    if df.index.to_frame(index=False).isna().any(axis=None) or df.index.has_duplicates:
        raise ValueError('Malawi 2024-25 roster has missing or duplicate person keys')

    i = df.index.get_level_values('i')
    panel = ~i.str.startswith(CS_24)
    matched_cover = df[cover[0]].map(format_id).notna().to_numpy()
    if (matched_cover != panel).any():
        raise ValueError('Malawi 2024-25 roster and the IHPS panel cover disagree '
                         'on household identity')

    copy = panel & df[cover[1]].map(format_id).isna().to_numpy()
    target = CS_24 + df.loc[copy, cover[2]].map(format_id)
    if target.isna().any():
        raise ValueError('Malawi 2024-25 refresh roster has no IHS6 household key')
    keys = pd.MultiIndex.from_arrays(
        [df.index.get_level_values('t')[copy], target,
         df.index.get_level_values('pid')[copy]],
        names=df.index.names,
    )
    payload = ['Sex', 'Relationship', 'Age', 'MonthsAway']
    ihs6 = df.loc[~panel & i.isin(target), payload]
    if keys.has_duplicates or not keys.sort_values().equals(ihs6.index.sort_values()):
        raise ValueError('Malawi 2024-25 refresh and IHS6 rosters disagree on '
                         'person keys within their shared interviews')
    refresh = df.loc[copy, payload].set_axis(keys)
    ihs6 = ihs6.reindex(keys)
    same = refresh.eq(ihs6) | (refresh.isna() & ihs6.isna())
    if not same.fillna(False).to_numpy().all():
        raise ValueError('Malawi 2024-25 refresh and IHS6 rosters disagree on '
                         'roster answers within their shared interviews')
    return df.loc[~copy].drop(columns=cover)
