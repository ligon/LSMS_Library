# Formatting functions for CotedIvoire 2021-22 (EHCVM-2).  See _/CONTENTS.org "2021-22 wired".
import numpy as np
import pandas as pd

import lsms_library.local_tools as tools

# The same projection as Togo 2021-22: Region, Rural and the GPS point are constant
# within a grappe, and this raises, naming the grappes, on the day one is not.
from lsms_library.transformations import (
    collapse_to_cluster_grain as cluster_features,
)

#: `i` of this wave.  11,589 of the 12,965 (grappe, menage) keys also occur in 2018-19,
#: and the head of the household under a recurring key agrees with the head under another
#: key of the same grappe as often as with its own (GH #552): the keys do not name the same
#: household.  No link is served, so the ids must not be able to meet.
ID_PREFIX = 'E2_'


def i(value):
    '''Household id from (grappe, menage): 2018-19's form, with this wave's prefix.  None
    where the file has no key: s08a has 728 such rows, s14b 1,408 with no menage.'''
    grappe, menage = value.iloc[0], value.iloc[1]
    if pd.isna(grappe) or pd.isna(menage):
        return None
    return ID_PREFIX + tools.format_id(grappe) + tools.format_id(menage, zeropadding=3)


def pid(value):
    '''Person id, membres__id or numind: the file's member number.'''
    return tools.format_id(value)


def _upper(value):
    return value.upper() if isinstance(value, str) else value


def strata(value):
    '''s00q01, on 2018-19's spelling.  This file labels 22 of the 33 regions in lower case
    (`poro`, `gÔh`), on the same codes; upper-casing gives 2018-19's label on all 33.'''
    return _upper(value)


def Region(value):
    '''s00q01, as `strata`.'''
    return _upper(value)


_MONTH = {'Janvier': 1, 'Février': 2, 'Mars': 3, 'Avril': 4, 'Mai': 5, 'Juin': 6,
          'Juillet': 7, 'Août': 8, 'Septembre': 9, 'Octobre': 10, 'Novembre': 11,
          'Décembre': 12}


def Age(value):
    '''[s01q04a, s01q03a, s01q03b, s01q03c], with the month label as a number.

    The name collides with the `assets` myvar `Age: s12q07`, which the framework
    binds to this by name with a scalar; a scalar passes through (GH #321).
    '''
    if not isinstance(value, pd.Series):
        return value
    result = list(value)
    result[2] = _MONTH.get(value.iloc[2])
    return result


def household_roster(df):
    '''Age from the stated years, else from the birth date and the interview date.

    s01q04a is filled on 3,918 of 64,491 members and s01q03c on all of them.  One member
    has the year 1900 and no stated age; age_handler judges it.
    '''
    def age(row):
        stated, d, m, y = row['Age']
        return tools.age_handler(age=None if pd.isna(stated) else stated, d=d, m=m, y=y,
                                 interview_date=row['interview_date'], interview_year=2021)

    df['Age'] = df.apply(age, axis=1)
    return df.drop('interview_date', axis='columns')


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

AFFECTED = ['AffectedIncome', 'AffectedAssets', 'AffectedProduction', 'AffectedConsumption']

_RANK = {'Première stratégie': 0, 'Deuxième strategie': 1, 'Troisième stratégie': 2}


def shocks(df):
    '''HowCoped0..2 by the survey's own rank: s14bq05__N holds a rank label or Non,
    where 2018-19's s14q05__N held an indicator.

    Column 25, "Autre strategie", is the exception: 25 of the 26 columns carry the
    `strategie` label set and this one `nonoui`, so it says Oui with no rank.  It takes
    the rank no other strategy holds: all 15 of its rows name no other strategy.
    '''
    # Only a row with follow-ups is served: the framework would drop the rest anyway, and
    # with them gone the grain audit counts this file's real collisions, not empty rows.
    df = df[df[AFFECTED].notna().any(axis=1)].copy()
    cope = [c for c in df.columns if c.startswith('Cope')]
    how = {0: [], 1: [], 2: []}
    for _, row in df[cope].iterrows():
        ranked = [np.nan, np.nan, np.nan]
        for c in cope:
            rank = _RANK.get(row[c])
            if rank is not None:
                ranked[rank] = COPING_LABELS[int(c.replace('Cope', ''))]
        if row['Cope25'] == 'Oui':
            empty = next((k for k in range(3) if pd.isna(ranked[k])), None)
            if empty is not None:
                ranked[empty] = COPING_LABELS[25]
        for k in range(3):
            how[k].append(ranked[k])
    for k in range(3):
        df[f'HowCoped{k}'] = how[k]
    return df.drop(columns=cope)


FIES_ITEMS = ['Worried', 'HealthyDiet', 'FewFoods', 'SkippedMeal',
              'AteLess', 'RanOut', 'Hungry', 'WholeDay']


def food_security(df):
    '''FIES_score: the count of True among the 8 items, NA where all 8 are missing.

    The file's 728 rows with no grappe and no menage hold nothing and are not households.
    '''
    df = df[df.index.get_level_values('i').notna()].copy()
    items = df[FIES_ITEMS]
    score = items.sum(axis=1, skipna=True).where(~items.isna().all(axis=1), other=pd.NA)
    df['FIES_score'] = score.astype('Int64')
    return df


def interview_date(df):
    '''Visits as the country's hook melts them, for households interviewed: a Non rempli
    household records no start, and 167 of the 728 record an end.'''
    df = df[df['Result'].astype('string').str.strip().ne('Non rempli')].drop(columns='Result')
    return tools.melt_visit_intervals(df)
