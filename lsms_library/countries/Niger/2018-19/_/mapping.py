# Formatting Functions for Niger 2018-19
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
    '''
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
    return df
