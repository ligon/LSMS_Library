import pandas as pd
import numpy as np
import lsms_library.local_tools as tools

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


def Sex(value):
    '''
    Map numeric sex codes to canonical M/F.
    Mali s01q01: 1=Masculin, 2=Féminin
    '''
    if value == 1 or value == 'Masculin':
        return 'M'
    if value == 2 or value == 'Féminin':
        return 'F'


def Age(value):
    '''
    Pass Age list [s01q04a, s01q03a, s01q03b, s01q03c] through unchanged.
    s01q03b is integer month (1-12); 9999 sentinels handled by age_handler.
    Override country-level mali.py Age() which expects a scalar.
    '''
    return list(value)


def interview_date(value):
    '''
    Column-level pass-through for interview_date.
    Overrides mali.py's df-level interview_date(df) which expects a DataFrame.
    The household_roster(df) function handles date parsing via age_handler.
    '''
    return value


_MONTH_MAP = {
    'Janvier': 1, 'F\xe9vrier': 2, 'f': 2, 'Mars': 3, 'Avril': 4, 'Mai': 5,
    'Juin': 6, 'Juillet': 7, 'Ao\xfbt': 8, 'Ao': 8, 'Septembre': 9,
    'Octobre': 10, 'Novembre': 11, 'D\xe9cembre': 12, 'd': 12,
}


def household_roster(df):
    '''
    Compute Age from DOB components when s01q04a is absent.

    Age list in data_info.yml: [s01q04a, s01q03a(day), s01q03b(month), s01q03c(year)].
    s01q03b is a French month string (sometimes truncated due to latin-1 encoding);
    converted via _MONTH_MAP. 9999 sentinels in numeric DOB columns are passed through
    (age_handler treats values >= 2100 as invalid).
    No negative age sentinel in s01q04a — it is simply NaN when absent.
    '''
    def _age_from_row(x):
        age_raw = x["Age"][0]
        d_raw = x["Age"][1]
        d_val = None if (pd.notna(d_raw) and float(d_raw) >= 9999) else d_raw
        m_raw = x["Age"][2]
        m_val = _MONTH_MAP.get(str(m_raw)) if pd.notna(m_raw) else None
        y_raw = x["Age"][3]
        y_val = None if (pd.notna(y_raw) and float(y_raw) >= 9999) else y_raw
        try:
            result = tools.age_handler(
                age=age_raw,
                d=d_val, m=m_val, y=y_val,
                interview_date=x["interview_date"],
                interview_year=2018,
            )
        except (ValueError, OverflowError):
            # Invalid DOB combination (e.g. day > days-in-month); fall back to year only
            result = tools.age_handler(
                age=age_raw, y=y_val,
                interview_date=x["interview_date"],
                interview_year=2018,
            )
        return np.nan if (pd.notna(result) and result < 0) else result

    df["Age"] = df.apply(_age_from_row, axis=1)
    df = df.drop('interview_date', axis='columns')
    return df


def shocks(df):
    cope_cols = [c for c in df.columns if c.startswith('Cope')]

    # For 2018-19 binary coping: values are numeric (0=No, 1=Yes)
    # Convert to float for comparison; pick the first 3 strategies with value >= 1
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
