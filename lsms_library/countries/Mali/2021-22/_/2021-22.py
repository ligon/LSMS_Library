# Formatting Functions for Mali 2021-22
import pandas as pd
from lsms_library.local_tools import format_id
import numpy as np

COPING_LABELS = {
    1: "Utilisation de son épargne",
    2: "Aide de parents ou d'amis",
    3: "Aide du gouvernement/l'Etat",
    4: "Aide d'organisations religieuses ou d'ONG",
    5: "Marier au moins une de ses filles",
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

    # For 2021-22: coping columns have ranked string values
    # "Première stratégie", "Deuxième strategie", "Troisième stratégie", "Non"
    rank_map = {
        'Première stratégie': 0,
        'Deuxième strategie': 1,
        'Troisième stratégie': 2,
    }

    how_coped = {0: [], 1: [], 2: []}
    for _, row in df[cope_cols].iterrows():
        ranked = [np.nan, np.nan, np.nan]
        for c in cope_cols:
            num = int(c.replace('Cope', ''))
            val = row[c]
            if val in rank_map:
                idx = rank_map[val]
                ranked[idx] = COPING_LABELS.get(num, f'Strategy {num}')
        for k in range(3):
            how_coped[k].append(ranked[k])

    df['HowCoped0'] = how_coped[0]
    df['HowCoped1'] = how_coped[1]
    df['HowCoped2'] = how_coped[2]
    df = df.drop(columns=cope_cols)
    return df


def panel_ids(df):
    '''
    filter the dataframe to only include the second visit
    '''
    df = df[(df.index.get_level_values('visit') == '2') & (df['in_previous_wave'] == 1)]
    def previous_i(value):

        return (format_id(value.iloc[0]) or '') + '0' + (format_id(value.iloc[1], zeropadding=2) or '')
    df['previous_i'] = df[['previous_v', 'previous_hid']].apply(previous_i, axis=1)
    df = df.reset_index().loc[:, ['i', 'previous_i']].drop_duplicates().set_index('i')
    return df