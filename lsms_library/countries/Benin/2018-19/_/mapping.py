import pandas as pd
import numpy as np
import lsms_library.local_tools as tools
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
# and Benin/_/derivations.yml.
FOOD_ACQUIRED_DERIVATION = _derivation_key('Benin')


def inputs_food_acquired(wave):
    """The registry `inputs` callable: raw section-7B answers at (t, i, j).

    A wave-level wrapper rather than a `functools.partial` in the country
    module, so that binding the country name does not put this code in
    Benin/_/benin.py -- which is in EVERY Benin table's cache
    fingerprint.
    """
    return _inputs_last_purchase('Benin', wave)


def food_acquired(df):
    return _food_acquired_ehcvm(df, FOOD_ACQUIRED_DERIVATION)

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


def Age(value):
    '''
    Convert French month names to integers for the DOB components.

    Benin s01q03b (month) uses French month names (Janvier, Février, ...)
    identical to Senegal 2018-19.  Convert month to int so tools.age_handler()
    can compute age from DOB.

    The list [s01q04a, s01q03a(day), s01q03b(month), s01q03c(year)] is
    passed to household_roster() via tools.age_handler().

    The function name ``Age`` collides with the ``assets`` myvar
    ``Age: s12q07`` (item age in years), which the framework binds to
    this formatter by name (column_mapping in country.py).  That path
    passes a *scalar*, not the roster's DOB Series; pass scalars
    through unchanged so assets extraction does not crash on
    ``list(<float>)`` (closes #321).
    '''
    if not isinstance(value, pd.Series):
        return value
    month_map = {'Janvier': 1, 'Février': 2, 'Mars': 3, 'Avril': 4, 'Mai': 5,
                 'Juin': 6, 'Juillet': 7, 'Août': 8, 'Septembre': 9,
                 'Octobre': 10, 'Novembre': 11, 'Décembre': 12}
    result = list(value)
    result[2] = month_map.get(value.iloc[2])
    return result


def cluster_features(df):
    '''Reduce the broadcast household-level cover page to CLUSTER grain (GH #323).

    ``df_main`` extracts Region/Rural from ``s00_me_ben2018.dta``, which is
    HOUSEHOLD-level (8,012 rows = 8,012 distinct (grappe, menage) pairs over
    670 grappes), but declares only ``v: grappe`` as its index var.  Each
    grappe's attributes are therefore broadcast across all its households,
    handing the framework 8,012 rows on a (t, v) index with 7,342 duplicate
    tuples -- which ``_normalize_dataframe_index`` (GH #323 Site 1) then
    reduces with ``groupby().first()``.

    That reduction is VALUE-LOSSLESS today, so no data has ever been lost here
    and the framework's #323 audit is correctly silent about it: measured
    against the raw source under ``LSMS_NO_CACHE=1``, 0 of the 670 grappes
    carry more than one distinct ``s00q01`` (Region) or ``s00q04`` (Rural),
    and ``grappe_gps_ben2018.dta`` is already grappe-level at 670/670.

    But "lossless today" is a property of the 2018-19 data, not of the
    extraction, and relying on it MASKS the defect class: if a grappe ever
    straddled two regions, ``first()`` would silently pick one -- and, because
    it skips NA per COLUMN, it would in general hand back a composite row
    assembled from different households, a cluster that exists nowhere in the
    survey.  So fix the grain at the source and ENFORCE the invariant rather
    than describing it: assert every cluster carries exactly one distinct
    value per attribute, and RAISE if that ever stops being true, instead of
    quietly choosing a winner.

    This changes no returned value (670 rows before and after); it makes the
    670 correct BY CONSTRUCTION rather than by luck.
    '''
    levels = list(df.index.names)
    spread = df.groupby(level=levels, observed=True).nunique(dropna=False)
    straddling = spread[(spread > 1).any(axis=1)]
    if len(straddling):
        raise ValueError(
            f'Benin cluster_features: {len(straddling)} cluster(s) carry more '
            f'than one distinct value for a cluster attribute, so reducing the '
            f'household-level cover page to cluster grain would have to GUESS. '
            f'Offending clusters (nunique per column):\n{straddling.head(10)}'
        )
    # Exact-duplicate rows by construction (the assertion above proves it), so
    # keeping one row per cluster discards only redundancy.
    return df[~df.index.duplicated(keep='first')]


def _decode_cp1252(value):
    """Re-decode a value label that the reader took as Latin-1 but the file
    wrote as cp1252 (GH #801).

    ``s01_me_ben2018.dta`` is Stata format 115 with no declared encoding, and its
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


def household_roster(df):
    '''
    Recover Age from date-of-birth components when s01q04a is null.

    Age list in data_info.yml: [s01q04a, s01q03a(day), s01q03b(month), s01q03c(year)].
    Benin s01q03b uses French month names converted to int by the Age() formatter above.
    DOB columns use true NaN (no sentinel other than -1 for age), so no extra sentinel mask needed.

    Also restores the ``Frere, soeur`` Relationship label that the
    Latin-1 read of this cp1252 file turns into ``Frere, s<U+009C>ur``
    (``_decode_cp1252``; GH #801).  No ``Relationship`` formatter runs
    for Benin, so the decoded label keeps the file's own casing, as the
    CotedIvoire / Burkina Faso 2018-19 siblings do.
    '''
    def _age_from_row(x):
        age_raw = x["Age"][0]
        # -1 is a sentinel for missing age; pass None so age_handler falls through to DOB columns
        age_val = None if (pd.notna(age_raw) and int(float(age_raw)) < 0) else age_raw
        result = tools.age_handler(age=age_val, d=x["Age"][1], m=x["Age"][2], y=x["Age"][3],
                                   interview_date=x["interview_date"], interview_year=2018)
        # Null out any negative result (dob slightly after interview date)
        return np.nan if (pd.notna(result) and result < 0) else result

    df["Age"] = df.apply(_age_from_row, axis=1)
    df = df.drop('interview_date', axis='columns')
    if 'Relationship' in df.columns:
        df['Relationship'] = df['Relationship'].map(_decode_cp1252)
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


def shocks(df):
    # Benin 2018-19 records at most 2 coping strategies per household.
    # (A probe over the full survey shows no household with a third
    # strategy flagged; HowCoped2 would be all-NaN and is therefore
    # not declared in data_scheme.yml.)
    cope_cols = [c for c in df.columns if c.startswith('Cope')]

    how_coped = {0: [], 1: []}
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
            if len(found) == 2:
                break
        for k in range(2):
            how_coped[k].append(found[k] if k < len(found) else np.nan)

    df['HowCoped0'] = how_coped[0]
    df['HowCoped1'] = how_coped[1]
    df = df.drop(columns=cope_cols)
    return df

