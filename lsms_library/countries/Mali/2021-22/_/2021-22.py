# Formatting Functions for Mali 2021-22
import pandas as pd
from lsms_library.local_tools import format_id
import numpy as np
import lsms_library.local_tools as tools

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


def pid(value):
    '''
    Formatting person id from single membres__id column.
    Overrides the country-level pid() which expects 3 components.
    '''
    return tools.format_id(value)


def interview_date(value):
    '''
    Pass through interview_date as a scalar.
    Overrides the country-level interview_date(df) which expects a DataFrame.
    '''
    return value


def Sex(value):
    '''
    Formatting sex variable (numeric codes: 1=Masculin, 2=Féminin)
    '''
    if value == 1 or value == 'Masculin':
        return 'm'
    if value == 2 or value == 'Féminin':
        return 'f'


_MONTH_MAP = {
    'Janvier': 1, 'Février': 2, 'Mars': 3, 'Avril': 4, 'Mai': 5,
    'Juin': 6, 'Juillet': 7, 'Août': 8, 'Septembre': 9,
    'Octobre': 10, 'Novembre': 11, 'Décembre': 12,
}


def Age(value):
    '''
    Convert 4-element Age list [s01q04a, s01q03a, s01q03b, s01q03c].
    s01q03b is a French month string (or 'Ne sait pas'); convert to integer.
    Numeric sentinel masking (9999) and age_handler call happen in household_roster(df).
    '''
    result = list(value)
    m_raw = value.iloc[2]
    if pd.isna(m_raw):
        result[2] = None
    else:
        result[2] = _MONTH_MAP.get(str(m_raw))
    return result


def household_roster(df):
    '''
    Formatting dataframe to calculate ages.
    Sentinel 9999 masks missing day/year components before passing to age_handler.
    s01q03b was converted to int by Age(); 'Ne sait pas' becomes None via _MONTH_MAP miss.
    '''

    def _age_from_row(x):
        age_raw = x["Age"][0]
        # 9999 is the sentinel for unknown age; pass None so age_handler falls through to dob columns
        age_val = None if (pd.notna(age_raw) and float(age_raw) >= 9999) else age_raw
        d_raw = x["Age"][1]
        d_val = None if (pd.notna(d_raw) and float(d_raw) >= 9999) else d_raw
        m_val = x["Age"][2]  # already None if 'Ne sait pas' or missing
        y_raw = x["Age"][3]
        y_val = None if (pd.notna(y_raw) and float(y_raw) >= 9999) else y_raw
        result = tools.age_handler(age=age_val, d=d_val, m=m_val, y=y_val,
                                   interview_date=x["interview_date"], interview_year=2021)
        # Null out any negative result (e.g. dob slightly after interview date)
        return np.nan if (pd.notna(result) and result < 0) else result

    df["Age"] = df.apply(_age_from_row, axis=1)
    df = df.drop('interview_date', axis='columns')

    return df


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