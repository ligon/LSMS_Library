# Formatting Functions for CotedIvoire
import numpy as np
import pandas as pd
import lsms_library.local_tools as tools


def i(value):
    '''
    Formatting household id: concatenate grappe + zero-padded menage.
    Compound key [grappe, menage] -> string like "1001001".
    '''
    return tools.format_id(value.iloc[0]) + tools.format_id(value.iloc[1], zeropadding=3)


# Coping strategy labels (EHCVM standard for CotedIvoire)
_COPING_LABELS = {
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
    '''
    Post-process shocks DataFrame: collapse Cope1..Cope26 binary columns into
    HowCoped0, HowCoped1, HowCoped2 (top-3 coping strategies per shock-HH row).

    HowCoped2 is omitted from the output when no household used 3+ strategies
    (all-null column), since it would fail structural sanity checks and provides
    no information.  It is declared optional in data_scheme.yml.
    '''
    cope_cols = [c for c in df.columns if c.startswith('Cope')]

    how_coped: dict[int, list] = {0: [], 1: [], 2: []}
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
                found.append(_COPING_LABELS.get(num, f'Strategy {num}'))
            if len(found) == 3:
                break
        for k in range(3):
            how_coped[k].append(found[k] if k < len(found) else pd.NA)

    df['HowCoped0'] = pd.array(how_coped[0], dtype='string')
    df['HowCoped1'] = pd.array(how_coped[1], dtype='string')
    hc2 = pd.array(how_coped[2], dtype='string')
    if hc2.isna().all():
        # No household used 3+ strategies — omit rather than write an all-null column.
        pass
    else:
        df['HowCoped2'] = hc2
    df = df.drop(columns=cope_cols)
    return df
