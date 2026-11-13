# Formatting Functions for CotedIvoire 2018-19
import pandas as pd
import numpy as np
import lsms_library.local_tools as tools
from lsms_library.transformations import food_acquired_to_canonical as _food_acquired_canonical

from lsms_library.ehcvm import coping_set_from_grid as _coping_set_from_grid
from lsms_library.ehcvm import (
    derivation_key as _derivation_key,
    food_acquired_ehcvm as _food_acquired_ehcvm,
    inputs_last_purchase as _inputs_last_purchase,
    strata_label,
    inputs_region_milieu_stratum as _inputs_region_milieu_stratum,
    stamp_region_milieu_stratum as _stamp_region_milieu_stratum,
)


# GH #1575; reuse GH #812 unchanged (task ledger §§2–5).
def inputs_strata(wave):
    """Own cover-page region and milieu labels at (t, i)."""
    return _inputs_region_milieu_stratum('CotedIvoire', wave)


def sample(df):
    """Disclose the region/milieu construction on rows with a stratum."""
    return _stamp_region_milieu_stratum(df, 'CotedIvoire')


# GH #876.  The wide frame's `Expenditure` used to be `s07bq08`, the value of
# ONE purchase made up to 30 days ago, sitting beside a 7-day consumption
# `Quantity`.  `_food_acquired_ehcvm` derives it instead -- purchased Quantity
# x (s07bq08 / s07bq07a), NA where the last purchase used a different unit --
# and stamps the registry key on every purchased row.  See lsms_library/ehcvm.py
# and CotedIvoire/_/derivations.yml.
FOOD_ACQUIRED_DERIVATION = _derivation_key('CotedIvoire')


def inputs_food_acquired(wave):
    """The registry `inputs` callable: raw section-7B answers at (t, i, j).

    A wave-level wrapper rather than a `functools.partial` in the country
    module, so that binding the country name does not put this code in
    CotedIvoire/_/cotedivoire.py -- which is in EVERY CotedIvoire table's cache
    fingerprint.
    """
    return _inputs_last_purchase('CotedIvoire', wave)


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
    # GH #1224: `decode` -- the served base unit of a sized row is read
    # through the same repair the sweep below applies to every other row.
    df = _food_acquired_ehcvm(df, FOOD_ACQUIRED_DERIVATION,
                              decode=_decode_mojibake)
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

    The function name ``Age`` collides with the ``assets`` myvar
    ``Age: s12q07`` (item age in years), which the framework binds to
    this formatter by name (column_mapping in country.py).  That path
    passes a *scalar*, not the roster's DOB Series; pass scalars
    through unchanged so assets extraction does not crash on
    ``list(<float>)`` (closes #321).
    '''
    if not isinstance(value, pd.Series):
        return value
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



# ---------------------------------------------------------------------------
# shocks: section 14.05 coping strategies (GH #1562)
# ---------------------------------------------------------------------------
# Moved here from CotedIvoire/_/cotedivoire.py so that an edit re-warms this
# wave alone, not every CotedIvoire table.  The EHCVM standard option list.
COPING_LABELS = {
    1: "Utilisation de son épargne",
    2: "Aide de parents ou d'amis",
    3: "Aide du gouvernement/l'Etat",
    4: "Aide d'organisations religieuses ou d'ONG",
    5: "Marier les enfants",
    6: "Changement des habitudes de consommation",
    7: "Achat d'aliments moins chers",
    8: "Membres actifs ont pris des emplois supplémentaires",
    9: "Membres adultes inactifs/chômeurs ont pris des emplois",
    10: "Enfants de moins de 15 ans amenés à travailler",
    11: "Les enfants ont été déscolarisés",
    12: "Migration de membres du ménage",
    13: "Réduction des dépenses de santé/d'éducation",
    14: "Obtention d'un crédit",
    15: "Vente des actifs agricoles",
    16: "Vente des biens durables du ménage",
    17: "Vente de terrain/immeubles/Maisons",
    18: "Louer/mettre ses terres en gages",
    19: "Vente du stock de vivres",
    20: "Pratique plus importante des activités de pêche",
    21: "Vente de bétail",
    22: "Confiage des enfants à d'autres ménages",
    23: "Engagé dans des activités spirituelles",
    24: "Pratique de la culture de contre saison",
    25: "Autre stratégie",
    26: "Aucune stratégie",
}


def shocks(df):
    """The coping SET from the s14q05__N grid; HowCoped0..2 NA (GH #1562).

    The CAPI form calls 14.05 "CHOIX MULTIPLE: ORDONNE", but the release holds
    {0, 1, 3} per option and never a 2, so the order is not established (ruling
    2026-11-12): the set only, ehcvm.coping_set_from_grid, which says which
    cell values count as chosen.  The hook this replaces listed options in code
    order and dropped HowCoped2 when it was all-null.
    """
    return _coping_set_from_grid(df, COPING_LABELS, country="CotedIvoire")
