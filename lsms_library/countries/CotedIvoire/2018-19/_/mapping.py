# Formatting Functions for CotedIvoire 2018-19
import pandas as pd
import numpy as np
import lsms_library.local_tools as tools
from lsms_library.transformations import food_acquired_to_canonical as _food_acquired_canonical


# Lossy-substitution prefixes the source data carries upstream of
# pyreadstat: the export pipeline sometimes replaced the second byte
# of certain UTF-8 codepoints with literal ``__`` (0x5f 0x5f).  No
# byte-level decoding can recover those (the original byte is gone),
# so we substitute the canonical French form directly when the
# prefix appears at the start of a string.
#
# CotedIvoire 2018-19 ``food_acquired`` does NOT exhibit any
# lost-prefix entries in either ``u`` or ``j`` (verified 2026-05-07
# against the cleaned output), so this dict is currently empty.  It
# is kept so the post-processor mirrors the structure used by other
# EHCVM countries (Guinea-Bissau, Niger) and to make extending it
# trivial if a future cache rebuild surfaces new lost-prefix labels.
_LOST_PREFIX_REPLACEMENTS = {}


def _decode_mojibake(s):
    """Reverse the UTF-8-as-Latin-1-as-UTF-8 double-encoding that pyreadstat
    produces for CotedIvoire's 2018-19 .dta value labels.

    The .dta file's value-labels block stores French strings as raw
    UTF-8 bytes (e.g. ``UnitÃ©`` should be ``Unité``: the ``é`` is
    UTF-8 ``b'\\xc3\\xa9'``), but the file metadata declares Latin-1
    encoding.  pyreadstat respects the metadata, decoding the UTF-8
    bytes as Latin-1 and re-encoding as Python ``str`` -- producing
    ``UnitÃ©`` instead of ``Unité``.  None of pyreadstat's
    ``encoding=`` options correct this (verified for the analogous
    Guinea-Bissau case 2026-05-07).  The fix is post-decode: encode
    the mojibake string back to Latin-1 bytes (recovers the original
    UTF-8 byte sequence), then decode as UTF-8.

    Strings without non-ASCII characters round-trip cleanly through
    ``encode('latin-1').decode('utf-8')`` (every byte 0x00-0x7F is
    valid in both encodings and identical), so this is safe to apply
    to all string values; only the mojibake-bearing ones change.

    Returns the input unchanged on any error or when ``s`` isn't a
    string -- best-effort, never raises.
    """
    if not isinstance(s, str):
        return s
    for bad, good in _LOST_PREFIX_REPLACEMENTS.items():
        if s.startswith(bad):
            return good + s[len(bad):]
    try:
        return s.encode('latin-1').decode('utf-8')
    except (UnicodeEncodeError, UnicodeDecodeError):
        return s


def _decode_mojibake_in_df(df):
    """Apply ``_decode_mojibake`` to every string column and string
    index level.  Used by per-feature ``df_edit`` hooks to fix
    value-label mojibake that affects French strings in multiple
    columns at once (food items, units, etc.) -- 5 of 52 ``u`` values
    and 46 of 136 ``j`` values carry it before this fix.
    """
    new_levels = []
    new_names = list(df.index.names)
    changed = False
    for i, name in enumerate(df.index.names):
        level = df.index.get_level_values(i)
        if level.dtype == object or pd.api.types.is_string_dtype(level):
            mapped = level.map(_decode_mojibake)
            if not mapped.equals(level):
                changed = True
            new_levels.append(mapped)
        else:
            new_levels.append(level)
    if changed:
        df = df.copy()
        df.index = pd.MultiIndex.from_arrays(new_levels, names=new_names) \
            if len(new_levels) > 1 else new_levels[0]
    for col in df.columns:
        if df[col].dtype == object or pd.api.types.is_string_dtype(df[col]):
            df[col] = df[col].map(_decode_mojibake)
    return df


def food_acquired(df):
    """``food_acquired`` post-processor: canonical reshape + mojibake fix.

    Wraps ``transformations.food_acquired_to_canonical`` (the Phase 3
    s-axis reshape) and then sweeps mojibake out of every string
    column / index level.  See ``_decode_mojibake`` for the encoding
    rationale.
    """
    df = _food_acquired_canonical(df)
    df = _decode_mojibake_in_df(df)
    return df


_FIES_ITEMS = ['Worried', 'HealthyDiet', 'FewFoods', 'SkippedMeal',
               'AteLess', 'RanOut', 'Hungry', 'WholeDay']


def food_security(df):
    '''Compute FIES_score from the 8 FAO FIES experience items.

    EHCVM §8A items s08aq01..s08aq08 have already been mapped to the 8
    canonical bool columns by the YAML ``mapping:`` blocks (Oui->True,
    Non->False, everything else->NaN).  FIES_score is the row-wise count
    of True across those 8 items.  Per the canonical contract it is NaN
    only when ALL 8 items are NaN; otherwise NaN items count as 0 (not
    affirmative).
    '''
    items = df[_FIES_ITEMS].apply(lambda c: c.map({True: 1, False: 0}))
    score = items.sum(axis=1, skipna=True)
    all_na = items.isna().all(axis=1)
    score = score.where(~all_na, np.nan)
    df = df.copy()
    df['FIES_score'] = score.astype('Int64')
    return df


def pid(value):
    '''Formatting person id from (grappe, menage, individual).'''
    return (tools.format_id(value.iloc[0]) + '0'
            + tools.format_id(value.iloc[1], zeropadding=2) + '0'
            + tools.format_id(value.iloc[2], zeropadding=2))


def Age(value):
    '''
    Pass Age columns through as a list for age_handler.

    CotedIvoire s01q03b (month) is already an integer (1-12); no
    month_map conversion needed.  The list is returned unchanged so
    household_roster() can unpack [age_raw, day, month, year].
    '''
    return list(value)


def household_roster(df):
    '''
    Recover Age from date-of-birth components when s01q04a is null.

    Age list in data_info.yml: [s01q04a, s01q03a(day), s01q03b(month), s01q03c(year)].
    CotedIvoire s01q03b is an integer month (1-12) — no month_map needed.
    DOB columns use true NaN (no sentinel), so no additional sentinel handling required.
    '''
    def _age_from_row(x):
        age_raw = x["Age"][0]
        # Pass None for negative sentinel values so age_handler falls through to DOB columns
        age_val = None if (pd.notna(age_raw) and int(float(age_raw)) < 0) else age_raw
        result = tools.age_handler(age=age_val, d=x["Age"][1], m=x["Age"][2], y=x["Age"][3],
                                   interview_date=x["interview_date"], interview_year=2018)
        return np.nan if (pd.notna(result) and result < 0) else result

    df["Age"] = df.apply(_age_from_row, axis=1)
    df = df.drop('interview_date', axis='columns')
    return df

