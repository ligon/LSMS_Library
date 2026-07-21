# Formatting Functions for Togo 2018
import pandas as pd
import numpy as np
import lsms_library.local_tools as tools
from lsms_library.transformations import food_acquired_to_canonical as food_acquired

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

    Togo s01q03b (month) uses French month names (Janvier, Février, ...)
    identical to Senegal 2018-19 — the audit incorrectly described them as
    plain integers.  Convert month to int so tools.age_handler() can
    compute age from DOB.

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
    '''Reduce the household-level EHCVM cover page to one row per CLUSTER.

    ``cluster_features`` is declared ``(t, v)`` -- one row per grappe -- but its
    df_main source ``s00_me_tgo2018.dta`` is the EHCVM *household* cover page:
    6,171 rows = 6,171 households across 540 grappes.  Feeding it straight
    through emitted 6,171 rows for a 540-row table, leaving 5,631 duplicate
    ``(t, v)`` tuples for the framework to collapse with ``groupby().first()``.

    That is an EXTRACTION bug, not an aggregation one, so it is fixed HERE
    rather than by declaring a reducer: the table should never have had
    duplicates.  It also fired a spurious GH #323 "possible silent data loss"
    RuntimeWarning on every cold build -- noise that was camouflaging the REAL
    #323 warning raised by ``plot_inputs`` on the same country.

    The reduction is exactly VALUE-LOSSLESS, verified on the source: all 540
    grappes carry exactly one distinct Region (s00q01) and one distinct Rural
    (s00q04) value, and ``grappe_gps_tgo2018.dta`` is already one row per
    grappe.  We do not take that on faith: if a column ever varies WITHIN a
    grappe, the guard below raises instead of silently keeping an arbitrary row
    (GH #323 -- a loud failure beats a quiet wrong answer).
    '''
    dup = df.index.duplicated(keep=False)
    if dup.any():
        levels = list(df.index.names)
        varying = [
            c for c in df.columns
            if df[dup].groupby(level=levels, observed=True)[c]
                      .nunique(dropna=False).gt(1).any()
        ]
        if varying:
            raise ValueError(
                f'Togo/2018 cluster_features: column(s) {varying} vary WITHIN a '
                f'cluster, so collapsing the household cover page to one row per '
                f'{levels} would silently discard real variation. Fix the '
                f'extraction (or declare an aggregation policy) rather than '
                f'dropping rows.'
            )
    return df[~df.index.duplicated(keep='first')]


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


FIES_ITEMS = ['Worried', 'HealthyDiet', 'FewFoods', 'SkippedMeal',
              'AteLess', 'RanOut', 'Hungry', 'WholeDay']


def food_security(df):
    '''Compute the harmonized FIES_score from the 8 FAO FIES items.

    The 8 experience items (Worried..WholeDay) arrive as booleans
    (Oui->True, Non->False, Ne sait pas / Refus -> NaN) from the YAML
    `mapping:` blocks.  FIES_score is the row-wise count of True items
    (0-8); it is NaN only when ALL 8 items are NaN (the FIES module was
    not administered to that household).
    '''
    items = df[FIES_ITEMS]
    score = items.sum(axis=1, skipna=True)
    all_na = items.isna().all(axis=1)
    score = score.where(~all_na, other=pd.NA)
    df['FIES_score'] = score.astype('Int64')
    return df


def household_roster(df):
    '''
    Recover Age from date-of-birth components when s01q04a is null.

    Age list in data_info.yml: [s01q04a, s01q03a(day), s01q03b(month), s01q03c(year)].
    Togo s01q03b is a numeric integer month (1-12), not French text — no month_map needed.
    DOB columns use true NaN (no 9999 sentinel), so no additional sentinel handling required.
    '''
    def _age_from_row(x):
        age_raw = x["Age"][0]
        # -1 is a sentinel for missing age; pass None so age_handler falls through to DOB columns
        age_val = None if (pd.notna(age_raw) and int(float(age_raw)) < 0) else age_raw
        return tools.age_handler(age=age_val, d=x["Age"][1], m=x["Age"][2], y=x["Age"][3],
                                 interview_date=x["interview_date"], interview_year=2018)

    df["Age"] = df.apply(_age_from_row, axis=1)
    df = df.drop('interview_date', axis='columns')
    return df

