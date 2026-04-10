# Formatting  Functions for Senegal 2018-19
import pandas as pd
import numpy as np
import lsms_library.local_tools as tools
from collections import defaultdict

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
    '''
    return tools.format_id(value.iloc[0]) + '0' + tools.format_id(value.iloc[1], zeropadding=2)

def Sex(value):
    '''
    Formatting sex variable
    '''
    if value == 'Féminin':
        return 'f'
    if value == 'Masculin':
        return 'm'

def Age(value):
    '''
    Formatting age from date components
    '''
    month_map = {'Janvier': 1, 'Février': 2, 'Mars': 3, 'Avril': 4, 'Mai': 5,
                 'Juin': 6, 'Juillet': 7, 'Août': 8, 'Septembre': 9,
                 'Octobre': 10, 'Novembre': 11, 'Décembre': 12}
    result = list(value)
    result[2] = month_map.get(value.iloc[2])
    return result

def Birthplace(value):
    '''
    Formatting birthplace variable
    '''
    if pd.isna(value):
        return value
    else:
        return value.title()
    
def Relationship(value):
    '''
    Formatting relationship variable
    '''
    if value:
        return str(value).title()

def Region(value):
    '''
    Formatting region variable
    '''
    return value



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


def household_roster(df):
    '''
    Formatting dataframe to calculate ages
    '''

    df["Age"] = df.apply(lambda x: tools.age_handler(age = x["Age"][0], d = x["Age"][1], m = x["Age"][2], y = x["Age"][3], interview_date=x["interview_date"], interview_year=2018), axis = 1)
    df = df.drop('interview_date', axis = 'columns')
    
    return df