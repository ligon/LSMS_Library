# Formatting Functions for Niger 2018-19
import pandas as pd
import numpy as np
import lsms_library.local_tools as tools
from lsms_library.transformations import food_acquired_to_canonical as _food_acquired_canonical

from lsms_library.ehcvm import (
    derivation_key as _derivation_key,
    food_acquired_ehcvm as _food_acquired_ehcvm,
    inputs_last_purchase as _inputs_last_purchase,
)

# GH #876.  The wide frame's `Expenditure` used to be `s07bq08`, the value of
# ONE purchase made up to 30 days ago, sitting beside a 7-day consumption
# `Quantity`.  `_food_acquired_ehcvm` derives it instead -- purchased Quantity
# x (s07bq08 / s07bq07a), NA where the last purchase used a different unit --
# and stamps the registry key on every purchased row.  See lsms_library/ehcvm.py
# and Niger/_/derivations.yml.
FOOD_ACQUIRED_DERIVATION = _derivation_key('Niger')


def inputs_food_acquired(wave):
    """The registry `inputs` callable: raw section-7B answers at (t, i, j).

    A wave-level wrapper rather than a `functools.partial` in the country
    module, so that binding the country name does not put this code in
    Niger/_/niger.py -- which is in EVERY Niger table's cache
    fingerprint.
    """
    return _inputs_last_purchase('Niger', wave)

# The country module holds the shared `u` sentinel (GH #842).
import sys as _sys
from pathlib import Path as _Path
_sys.path.append(str(_Path(__file__).resolve().parents[2] / '_'))
from niger import fill_missing_u as _fill_missing_u  # noqa: E402


# Pre-decode byte fixups.  The export pipeline upstream of pyreadstat
# replaced the second byte of certain UTF-8 codepoints with literal
# ``__`` (0x5f 0x5f).  For Niger 2018-19, two food items use the
# OE/oe ligature (Œ / œ); their UTF-8 second bytes (``\\x92``,
# ``\\x93``) were lost and replaced with ``__``, so pyreadstat
# returns mojibake fragments like ``Å__ufs`` (= Œufs, UTF-8
# ``\\xc5\\x92...``) and ``bÅ__uf`` (= bœuf, UTF-8 ``b\\xc5\\x93uf``,
# appearing in ``Viande de bœuf``).  We restore the lost UTF-8
# continuation byte BEFORE the byte-level decode by mapping ``Å__``
# (mojibake ``\\xc5\\x5f\\x5f``) back to ``Å\\x92`` or ``Å\\x93`` --
# Latin-1-encodable strings whose round-trip through
# ``encode('latin-1').decode('utf-8')`` yields the canonical Œ / œ.
# Keys are mojibake substrings; values are mojibake-form replacements
# that contain the recovered UTF-8 continuation byte.
_LOST_PREFIX_REPLACEMENTS = {
    'Å__ufs': 'Å\x92ufs',  # Œufs (UTF-8: \xc5\x92)
    'bÅ__uf': 'bÅ\x93uf',  # bœuf (UTF-8: \xc5\x93)
}


def _decode_mojibake(s):
    """Reverse the UTF-8-as-Latin-1-as-UTF-8 double-encoding that pyreadstat
    produces for Niger's 2018-19 .dta value labels.

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

    Two j-values carry an additional lossy substitution upstream of
    pyreadstat where the second byte of certain UTF-8 codepoints was
    replaced with literal ``__`` (i.e. ``Œufs`` -> ``Å__ufs``,
    ``bœuf`` -> ``bÅ__uf``).  Those can't be recovered byte-wise, so
    we handle them via ``_LOST_PREFIX_REPLACEMENTS`` above.

    Strings without non-ASCII characters round-trip cleanly through
    ``encode('latin-1').decode('utf-8')`` (every byte 0x00-0x7F is
    valid in both encodings and identical), so this is safe to apply
    to all string values; only the mojibake-bearing ones change.

    Returns the input unchanged on any error or when ``s`` isn't a
    string -- best-effort, never raises.
    """
    if not isinstance(s, str):
        return s
    # Restore lost UTF-8 continuation bytes BEFORE the byte-level
    # decode.  The replacement values are still in mojibake form
    # (Latin-1-encodable codepoints that map back to the original
    # UTF-8 byte sequence after ``encode('latin-1')``).  Substring
    # match -- the fragments may appear at any position (e.g.
    # ``bÅ__uf`` inside ``Viande de bÅ__uf``).
    for bad, good in _LOST_PREFIX_REPLACEMENTS.items():
        if bad in s:
            s = s.replace(bad, good)
    try:
        return s.encode('latin-1').decode('utf-8')
    except (UnicodeEncodeError, UnicodeDecodeError):
        return s


def _decode_mojibake_in_df(df):
    """Apply ``_decode_mojibake`` to every string column and string
    index level.  Used by per-feature ``df_edit`` hooks to fix
    value-label mojibake that affects French strings in multiple
    columns at once (food items, units, etc.) -- 7 of 44 ``u`` values
    and 49 of 135 ``j`` values carry it before this fix.
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


def _decode_cp1252(value):
    """Re-decode a value label that the reader took as Latin-1 but the file
    wrote as cp1252 (GH #801).

    ``s01_me_ner2018.dta`` is Stata format 115 with no declared encoding, and its
    value-label bytes are Windows-1252: ``Fr\\xe8re, s\\x9cur`` for
    ``Frere, soeur`` (0x9C is the oe ligature in cp1252).  ``get_dataframe``
    reads it through pandas ``StataReader``, which decodes every pre-118
    file as Latin-1, so 0x9C comes back as the C1 control U+009C and the
    served label was ``Frere, s<U+009C>ur``.  (pyreadstat's default, or
    ``encoding='cp1252'``, reads the same file correctly -- the defect is
    the reader's fallback, not the file.)  Latin-1 and cp1252 agree on
    every byte outside 0x80-0x9F, so re-encoding as Latin-1 and decoding
    as cp1252 changes only strings that carry a C1 control and leaves
    every other label byte-identical.  Returns the input unchanged for
    non-strings, strings without a C1 control, and the five cp1252 holes
    (0x81, 0x8D, 0x8F, 0x90, 0x9D) that cannot be decoded.
    """
    if not isinstance(value, str) or not any('\x80' <= ch <= '\x9f' for ch in value):
        return value
    try:
        return value.encode('latin-1').decode('cp1252')
    except (UnicodeEncodeError, UnicodeDecodeError):
        return value


def _fix_relationship(value):
    """``_decode_cp1252`` for the Relationship column, re-title-cased.

    The ``Relationship`` formatter above has already ``.title()``-cased
    the label, and ``str.title`` treats the control byte as a word break,
    so the artefact reaches the hook as ``Frere, S<U+009C>Ur``; decoding
    alone would give ``Frere, SoeUr``.  Re-titling a decoded string yields
    ``Frere, Soeur``, the label the sibling EHCVM-1 waves serve.  Values
    the decode leaves alone are returned untouched.
    """
    fixed = _decode_cp1252(value)
    if isinstance(fixed, str) and fixed != value:
        return fixed.title()
    return value


def food_acquired(df):
    """``food_acquired`` post-processor: canonical reshape + mojibake fix.

    Wraps ``transformations.food_acquired_to_canonical`` (the Phase 3
    s-axis reshape), sweeps mojibake out of every string column / index
    level, then fills a missing ``u``.  See ``_decode_mojibake`` for the
    encoding rationale.

    The reshape drops every row left with no measurement, so the rows that
    reach the fill are exactly the rows the table serves; ``fill_missing_u``
    changes a value, never a row count.  A row that reported an amount with no
    unit gets the ``Unknown`` sentinel instead of a NaN on the declared ``u``
    index level, which the next ``groupby`` would delete silently (GH #842 --
    the rationale is in ``niger.py`` at ``U_NA``).

    This wave serves 0 NaN-``u`` rows today (the 12 #842 counted are in
    2021-22); the fill is here so the two EHCVM waves cannot drift apart.
    """
    df = _food_acquired_ehcvm(df, FOOD_ACQUIRED_DERIVATION)
    df = _decode_mojibake_in_df(df)
    # Fill a missing unit with the `Unknown` sentinel, AFTER the mojibake
    # sweep so the two never fight over the same value.
    return _fill_missing_u(df)

_FIES_ITEMS = ['Worried', 'HealthyDiet', 'FewFoods', 'SkippedMeal',
               'AteLess', 'RanOut', 'Hungry', 'WholeDay']


def _to_fies_bool(s):
    '''Coerce a FIES item column to nullable boolean.  The YAML
    ``mapping: {Oui: True, Non: False}`` runs through df_data_grabber,
    which leaves UNMAPPED values ("Ne sait pas" / "Refus" / NaN)
    UNCHANGED rather than turning them into NaN.  So we keep only genuine
    True/False and send everything else (residual strings, NaN) to
    pd.NA.'''
    return s.map(lambda x: True if x is True else (False if x is False else pd.NA)).astype('boolean')


def food_security(df):
    '''food_security post-processor: coerce the 8 FIES items to nullable
    boolean and compute FIES_score.

    FIES_score is the count of True across the 8 items, and is NaN only
    when ALL 8 items are NaN (the question was not asked at all).
    Partial NaNs count as not-True (FAO scoring).
    '''
    for c in _FIES_ITEMS:
        df[c] = _to_fies_bool(df[c])
    items = df[_FIES_ITEMS]
    all_na = items.isna().all(axis=1)
    score = items.eq(True).sum(axis=1)
    df['FIES_score'] = score.where(~all_na).astype('Int64')
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
    26: "Aucune stratégie",
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


def v(value):
    '''
    Formatting cluster id
    '''
    return tools.format_id(value)


def i(value):
    '''
    Formatting household id from (grappe, menage).
    Uses '0' separator + zero-padded menage to prevent collisions
    (e.g., grappe=1,menage=23 vs grappe=12,menage=3).
    Adds 'E_' prefix for EHCVM waves to prevent panel collision with ECVMA waves.
    '''
    grappe = tools.format_id(value.iloc[0])
    menage = tools.format_id(value.iloc[1], zeropadding=2)
    if grappe is None or menage is None:
        return None
    return 'E_' + grappe + '0' + menage


def Sex(value):
    '''
    Formatting sex variable (Stata label: 1=Masculin, 2=Féminin)
    '''
    if value in ('Masculin', 'Masculin'):
        return 'M'
    if value in ('Féminin', 'Feminin'):
        return 'F'
    s = str(value).strip()
    if s == '1':
        return 'M'
    if s == '2':
        return 'F'
    return pd.NA


def Age(value):
    '''
    Pre-process Age list: convert French month name (s01q03b) to integer.
    Sentinel 9999 for any component is left as-is; age_handler's is_valid()
    rejects values >= 2100 (covers 9999).

    The function name ``Age`` collides with the ``assets`` myvar
    ``Age: s12q07`` (item age in years), which the framework binds to
    this formatter by name (column_mapping in country.py).  That path
    passes a *scalar*, not the roster's DOB Series; pass scalars
    through unchanged so assets extraction does not crash on
    ``list(<float>)`` (closes #321).
    '''
    if not isinstance(value, pd.Series):
        return value
    month_map = {
        'Janvier': 1, 'Fevrier': 2, 'Février': 2, 'Mars': 3, 'Avril': 4,
        'Mai': 5, 'Juin': 6, 'Juillet': 7, 'Aout': 8, 'Août': 8,
        'Septembre': 9, 'Octobre': 10, 'Novembre': 11, 'Decembre': 12,
        'Décembre': 12,
    }
    result = list(value)
    # s01q03b (index 2) may be a French month name or numeric/sentinel
    raw_month = value.iloc[2]
    if isinstance(raw_month, str):
        result[2] = month_map.get(raw_month, None)
    return result


def Relationship(value):
    '''
    Formatting relationship variable
    '''
    if value:
        return str(value).title()


def household_roster(df):
    '''
    Compute Age from date-of-birth components using canonical age_handler.
    s01q04a = age in years (direct)
    s01q03a = day of birth
    s01q03b = month of birth (French text, already converted to int by Age())
    s01q03c = year of birth
    Sentinel 9999 in any component is handled by age_handler's is_valid() check.

    Also restores the ``Frere, soeur`` Relationship label that the
    Latin-1 read of this cp1252 file turns into ``Frere, S<U+009C>Ur``
    (``_fix_relationship``; GH #801).  Note the two roster/food files of
    this wave carry DIFFERENT defects: s07b's labels are raw UTF-8 bytes
    read as Latin-1 (``_decode_mojibake`` above, a latin-1 -> utf-8
    round trip); s01's are cp1252 bytes read as Latin-1 (a latin-1 ->
    cp1252 round trip).  Neither helper fixes the other's file.
    '''

    def _age_from_row(x):
        age_raw = x['Age'][0]
        # 9999 is not a valid age (> 130); treat as missing
        age_val = None if (pd.notna(age_raw) and int(float(age_raw)) >= 9999) else age_raw
        result = tools.age_handler(
            age=age_val,
            d=x['Age'][1],
            m=x['Age'][2],
            y=x['Age'][3],
            interview_date=x['interview_date'],
            interview_year=2018,
        )
        return np.nan if (pd.notna(result) and result < 0) else result

    df['Age'] = df.apply(_age_from_row, axis=1)
    df = df.drop('interview_date', axis='columns')
    if 'Relationship' in df.columns:
        df['Relationship'] = df['Relationship'].map(_fix_relationship)
    return df
