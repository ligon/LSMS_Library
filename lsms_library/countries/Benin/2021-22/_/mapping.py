# Formatting functions for Benin 2021-22 (EHCVM-2).  See _/CONTENTS.org "2021-22 wired".
import numpy as np
import pandas as pd

import lsms_library.local_tools as tools

#: `i` of this wave.  6,994 of the 8,032 (grappe, menage) keys also occur in 2018-19.
#: No link is served (GH #552), so the ids must not be able to meet.
ID_PREFIX = 'E2_'


def i(value):
    '''Household id from (grappe, menage): 2018-19's form, with this wave's prefix.'''
    return ID_PREFIX + tools.format_id(value.iloc[0]) + tools.format_id(value.iloc[1], zeropadding=3)


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
    '''Members only, with Age from the stated years, else the birth date.

    s01 lists the 2018-19 roster again and asks of each person whether they are still a
    member (s01q00aa).  The 6,138 who are not have every other answer blank, and
    ehcvm_individu leaves them out, so they are not served.

    s01q04a is filled on 42 of the 42,660 members and s01q03c on the other 42,618.  A
    birth date just after the interview gives a negative age, served NA, as 2018-19.
    '''
    gone = df['StillMember'].astype('string').str.strip().str.lower().eq('non').fillna(False)
    df = df[~gone.astype(bool)].copy()

    def age(row):
        stated, d, m, y = row['Age']
        result = tools.age_handler(age=None if pd.isna(stated) else stated, d=d, m=m, y=y,
                                   interview_date=row['interview_date'], interview_year=2021)
        return np.nan if (pd.notna(result) and result < 0) else result

    df['Age'] = df.apply(age, axis=1)
    return df.drop(columns=['interview_date', 'StillMember'])


def cluster_features(df):
    '''The cover to (t, v), and a refusal if a grappe straddles two values.

    The same projection as 2018-19/_/mapping.py.  Region, Rural and the GPS point are
    constant within each of the 670 grappes.
    '''
    levels = list(df.index.names)
    spread = df.groupby(level=levels, observed=True).nunique(dropna=False)
    straddling = spread[(spread > 1).any(axis=1)]
    if len(straddling):
        raise ValueError(f'Benin cluster_features 2021-22: {len(straddling)} grappes carry '
                         f'more than one value:\n{straddling.head(10)}')
    return df[~df.index.duplicated(keep='first')]


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

_STRATEGY_RANK = {
    'Première stratégie': 1,
    'Deuxième strategie': 2,
    'Troisième stratégie': 3,
}


def shocks(df):
    '''HowCoped0..2 by the survey's own rank: s14bq05__N holds a rank label or Non.

    Column 25, `Autre stratégie`, carries the Oui/Non label set, so its rank is not in
    the file.  It takes the rank no other strategy holds, as Niger's hook after GH #1556:
    ranks are otherwise gapless, and Burkina, which keeps the raw rank, shows 25 always
    filling the next one.  34 rows choose it here, 13 of them beside another strategy.
    '''
    cope_cols = [c for c in df.columns if c.startswith('Cope')]

    how_coped = {0: [], 1: [], 2: []}
    for _, row in df[cope_cols].iterrows():
        ranked = []
        unranked = []
        for c in cope_cols:
            num = int(c.replace('Cope', ''))
            val = row[c]
            if pd.isna(val):
                continue
            rank = _STRATEGY_RANK.get(str(val))
            if rank is not None:
                ranked.append((rank, COPING_LABELS.get(num, f'Strategy {num}')))
            elif str(val) == 'Oui':
                unranked.append(COPING_LABELS.get(num, f'Strategy {num}'))

        held = {rank for rank, _ in ranked}
        for label in unranked:
            rank = min(set(range(1, len(held) + 2)) - held)
            held.add(rank)
            ranked.append((rank, label))

        ranked.sort(key=lambda x: x[0])
        found = [label for _, label in ranked]

        for k in range(3):
            how_coped[k].append(found[k] if k < len(found) else np.nan)

    df['HowCoped0'] = how_coped[0]
    df['HowCoped1'] = how_coped[1]
    df['HowCoped2'] = how_coped[2]
    return df.drop(columns=cope_cols)


FIES_ITEMS = ['Worried', 'HealthyDiet', 'FewFoods', 'SkippedMeal',
              'AteLess', 'RanOut', 'Hungry', 'WholeDay']


def food_security(df):
    '''FIES_score: the count of True among the 8 items, NA where all 8 are missing.'''
    items = df[FIES_ITEMS].apply(lambda c: c.map({True: 1, False: 0}))
    score = items.sum(axis=1, skipna=True).where(~items.isna().all(axis=1), other=np.nan)
    df = df.copy()
    df['FIES_score'] = score.astype('Int64')
    return df

