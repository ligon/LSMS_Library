# Formatting Functions for Guinea-Bissau 2018-19
import pandas as pd
import numpy as np
import lsms_library.local_tools as tools
from lsms_library.transformations import food_acquired_to_canonical as _food_acquired_canonical


# Lossy-substitution prefixes the source data carries for two specific
# Portuguese words.  Upstream of pyreadstat, the export pipeline
# replaced the second byte of certain UTF-8 codepoints with literal
# ``__`` (0x5f 0x5f) -- so ``Óleo`` (UTF-8: 0xc3 0x93 + 'leo') becomes
# ``Ã__leo`` and ``Água`` (UTF-8: 0xc3 0x81 + 'gua') becomes
# ``Ã__gua``.  No byte-level decoding can recover these (the original
# byte is gone), so we substitute the canonical Portuguese form
# directly when we see the prefix.  Two known cases drive this:
# food items ``Óleo de mancarra``, ``Óleo de soja``, ``Óleo de palma``,
# and ``Água filtrada`` (4 of 135 distinct ``j`` values in
# food_acquired 2018-19).
_LOST_PREFIX_REPLACEMENTS = {
    'Ã__leo': 'Óleo',
    'Ã__gua': 'Água',
}


def _decode_mojibake(s):
    """Reverse the UTF-8-as-Latin-1-as-UTF-8 double-encoding that pyreadstat
    produces for Guinea-Bissau's 2018-19 .dta value labels.

    The .dta file's value-labels block stores Portuguese strings as raw
    UTF-8 bytes (e.g. ``Pedaço`` = ``b'\\xc3\\xa7'`` for the ``ç``), but
    the file metadata declares Latin-1 encoding.  pyreadstat respects
    the metadata, decoding the UTF-8 bytes as Latin-1 and re-encoding
    them as Python ``str`` -- producing ``PedaÃ§o`` instead of ``Pedaço``.
    None of pyreadstat's ``encoding=`` options correct this (verified
    2026-05-07: ``utf-8`` raises, ``cp1252`` / ``iso-8859-1`` give the
    same mojibake).  The fix is post-decode: encode the mojibake string
    back to Latin-1 bytes (recovers the original UTF-8 byte sequence),
    then decode as UTF-8.

    Two j-values in food_acquired 2018-19 carry an additional lossy
    substitution upstream of pyreadstat where the second byte of
    certain UTF-8 codepoints was replaced with literal ``__`` (i.e.
    ``Óleo`` -> ``Ã__leo``, ``Água`` -> ``Ã__gua``).  Those can't be
    recovered byte-wise, so we handle them via
    ``_LOST_PREFIX_REPLACEMENTS`` above.

    Strings without non-ASCII characters round-trip cleanly through
    ``encode('latin-1').decode('utf-8')`` (every byte 0x00-0x7F is
    valid in both encodings and identical).  So this is safe to apply
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
    index level in-place.  Used by per-feature ``df_edit`` hooks below
    to fix value-label mojibake that affects Portuguese strings in
    multiple columns at once (food items, units, etc.) -- 40 of 135
    food items and 4 of 29 units carry it before this fix.
    """
    # Index levels
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
    # String columns
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

def Sex(value):
    '''
    Map Portuguese sex labels from s01_me_gnb2018.dta to canonical M/F.
    Stata labels: 1=Masculino, 2=Feminino.
    '''
    if value in ('Masculino', '1'):
        return 'M'
    if value in ('Feminino', '2'):
        return 'F'
    return pd.NA


# Portuguese month names used in s01q03b Stata labels.
_MONTH_MAP_PT = {
    'Janeiro': 1, 'Fevereiro': 2, 'Março': 3, 'Marco': 3, 'Abril': 4,
    'Maio': 5, 'Junho': 6, 'Julho': 7, 'Agosto': 8, 'Setembro': 9,
    'Outubro': 10, 'Novembro': 11, 'Dezembro': 12,
}


def Age(value):
    '''
    Convert DOB list [s01q04a, s01q03a, s01q03b, s01q03c] so that month
    (index 2) is a plain integer 1-12.

    s01q03b Stata labels are Portuguese month names (Janeiro…Dezembro).
    get_dataframe() returns the label strings as categoricals.
    The sentinel 9999 is left as-is; household_roster() handles it.
    '''
    result = list(value)
    raw_month = value.iloc[2]
    if isinstance(raw_month, str):
        result[2] = _MONTH_MAP_PT.get(raw_month)
    # if already numeric (int/float), keep as-is; household_roster will
    # null it out if it equals 9999
    return result


def household_roster(df):
    '''
    Recover Age from date-of-birth components (s01q03a/b/c) when s01q04a
    is missing or 9999.

    Age list in data_info.yml: [s01q04a, s01q03a(day), s01q03b(month), s01q03c(year)].
    The Age() formatter above has already converted month to int 1-12 (or None).
    The 9999 sentinel marks unknown age / DOB components.
    '''
    def _age_from_row(x):
        age_raw = x["Age"][0]
        # 9999 is the sentinel for unknown age; treat as missing
        age_val = None if (pd.notna(age_raw) and int(float(age_raw)) == 9999) else age_raw
        # day sentinel
        raw_day = x["Age"][1]
        day = None if (pd.notna(raw_day) and int(float(raw_day)) == 9999) else raw_day
        # month — Age() may have returned None for unrecognised strings
        month = x["Age"][2]
        result = tools.age_handler(age=age_val, d=day, m=month, y=x["Age"][3],
                                   interview_date=x["interview_date"], interview_year=2018)
        return np.nan if (pd.notna(result) and result < 0) else result

    df["Age"] = df.apply(_age_from_row, axis=1)
    df = df.drop('interview_date', axis='columns')
    return df


# FAO FIES 8 experience items, in canonical order (s08aq01..s08aq08).
_FIES_ITEMS = ['Worried', 'HealthyDiet', 'FewFoods', 'SkippedMeal',
               'AteLess', 'RanOut', 'Hungry', 'WholeDay']

# Yes/No labels for the FIES items.  Guinea-Bissau's s08a value labels are
# Portuguese (Sim/Não); French Oui/Non included defensively for any
# differently-localized export.  Recusa (refused) / Não sabe (don't know)
# and anything else fall through to pd.NA.
_FIES_YES = {'Sim', 'Oui', 'sim', 'oui'}
_FIES_NO = {'Não', 'Nao', 'Non', 'não', 'nao', 'non'}


def _fies_bool(value):
    if value in _FIES_YES:
        return True
    if value in _FIES_NO:
        return False
    return pd.NA


def food_security(df):
    '''FIES post-processor: map the 8 Portuguese Yes/No items to bool and
    add FIES_score (count of True; NaN only when all 8 items are NaN).

    s08aq01..s08aq08 arrive renamed to the canonical FAO names via
    data_info.yml.  Sim->True, Não->False, Recusa/Não sabe/NaN->NA.
    '''
    for col in _FIES_ITEMS:
        df[col] = df[col].map(_fies_bool).astype('boolean')

    items = df[_FIES_ITEMS]
    score = items.sum(axis=1, skipna=True)          # True counts as 1
    all_na = items.isna().all(axis=1)               # no item answered
    score = score.where(~all_na, other=pd.NA).astype('Int64')
    df['FIES_score'] = score
    return df


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
}


def shocks(df):
    cope_cols = [c for c in df.columns if c.startswith('Cope')]

    how_coped = {0: [], 1: [], 2: []}
    for _, row in df[cope_cols].iterrows():
        found = []
        for c in cope_cols:
            num = int(c.replace('Cope', ''))
            val = row[c]
            try:
                val = float(val)
            except (ValueError, TypeError):
                continue
            if val >= 1:
                found.append(COPING_LABELS.get(num, f'Strategy {num}'))
            if len(found) == 3:
                break
        for k in range(3):
            how_coped[k].append(found[k] if k < len(found) else np.nan)

    df['HowCoped0'] = how_coped[0]
    df['HowCoped1'] = how_coped[1]
    df['HowCoped2'] = how_coped[2]
    df = df.drop(columns=cope_cols)
    return df

