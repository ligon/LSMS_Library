# Formatting functions for Guinea-Bissau 2021-22 (EHCVM-2).  See _/CONTENTS.org
# "2021-22 wired".  Copied from the Togo / Benin 2021-22 wiring (PR #1711), with
# every label-keyed map re-keyed from THIS file's own Portuguese value labels.
import numpy as np
import pandas as pd

import lsms_library.local_tools as tools

# GH #1575's construction, extended to this wave as the 2026-11-12 ruling extends it
# to Togo / Benin / Cote d'Ivoire ("Same rule as #1575"): `strata` is the cover's
# region label and milieu label, by the shared helper, and `sample` stamps the
# registry key on every row with a stratum.  The registry's `inputs` callable is
# 2018-19's, which reads each wave's own cover.
from lsms_library.ehcvm import (
    stamp_region_milieu_stratum as _stamp_region_milieu_stratum,
    strata_label,
)


def sample(df):
    return _stamp_region_milieu_stratum(df, 'Guinea-Bissau')


#: `i` of this wave.  4,893 of the 5,351 (grappe, menage) keys also occur in 2018-19
#: (s00q07f1 / s00q07f2 record the 2018 grappe and household, and PanelHH a panel
#: flag), and whether a recurring key is the same household is not checked here.
#: No link is served (ruling 2026-11-12: no panel IDs), so the ids must not meet.
ID_PREFIX = 'E2_'


def i(value):
    '''Household id from (grappe, menage): 2018-19's form, with this wave's prefix.'''
    return ID_PREFIX + tools.format_id(value.iloc[0]) + '0' + tools.format_id(value.iloc[1], zeropadding=2)


def Sex(value):
    '''s01q01: Masculino / Feminino, as 2018-19's labels.'''
    return {'Masculino': 'M', 'Feminino': 'F'}.get(str(value).strip(), pd.NA) \
        if pd.notna(value) else pd.NA


#: s01q03b, Portuguese month names; `Não sabe` (5,819 rows) is no month.
_MONTH = {'Janeiro': 1, 'Fevereiro': 2, 'Março': 3, 'Abril': 4, 'Maio': 5, 'Junho': 6,
          'Julho': 7, 'Agosto': 8, 'Setembro': 9, 'Outubro': 10, 'Novembro': 11,
          'Dezembro': 12}


def Age(value):
    '''[s01q04a, s01q03a, s01q03b, s01q03c], with the month label as a number.

    The name collides with the `assets` myvar `Age: s12q07`, which the framework
    binds to this by name with a scalar; a scalar passes through (GH #321).
    '''
    if not isinstance(value, pd.Series):
        return value
    result = list(value)
    result[2] = _MONTH.get(str(value.iloc[2]).strip()) if pd.notna(value.iloc[2]) else None
    return result


#: s01q00aa, "[NOM] est-il toujours un membre de ce ménage?".  The file spells `não`
#: double-encoded with a byte lost (`nÃo`), whose lower case is `não`.
_STILL = {'sim': True, 'não': False, 'nao': False}


def household_roster(df):
    '''Members only, with Age from the stated years, else the birth date.

    The 341 people s01q00aa says have left are not served (GH #1565 is open; this
    is Benin 2021-22's and ehcvm_individu's choice).  Ages as 2018-19's hook: 9999
    is no answer, and a birth date after the interview gives NA.
    '''
    still = df['StillMember'].astype('string').str.strip().str.lower()
    unknown = sorted(set(still.dropna()) - set(_STILL))
    if unknown:
        raise ValueError(f'Guinea-Bissau 2021-22 s01q00aa: unknown answers {unknown}')
    df = df[still.map(_STILL).fillna(True).astype(bool).to_numpy()].copy()

    def _known(x):
        return None if (pd.isna(x) or int(float(x)) == 9999) else x

    def age(row):
        stated, d, m, y = row['Age']
        result = tools.age_handler(age=_known(stated), d=_known(d), m=m, y=_known(y),
                                   interview_date=row['interview_date'], interview_year=2021)
        return np.nan if (pd.notna(result) and result < 0) else result

    df['Age'] = df.apply(age, axis=1)
    return df.drop(columns=['interview_date', 'StillMember'])


def cluster_features(df):
    '''The cover to (t, v), and a refusal if a grappe straddles two values.

    Benin 2021-22's hook.  Region, Rural and the GPS point are constant within each
    of the 450 grappes (measured), so dropping the repeats loses nothing.
    '''
    levels = list(df.index.names)
    spread = df.groupby(level=levels, observed=True).nunique(dropna=False)
    straddling = spread[(spread > 1).any(axis=1)]
    if len(straddling):
        raise ValueError(f'Guinea-Bissau cluster_features 2021-22: {len(straddling)} grappes '
                         f'carry more than one value:\n{straddling.head(10)}')
    return df[~df.index.duplicated(keep='first')]


#: The coping strategies, s14bq05__1..26, by their variable labels: 2018-19's 25 list
#: (French, as Togo's and Benin's) and 26 `Aucune stratégie`.
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

#: The `strategie` value labels of this file (French, as Benin's and Togo's).
_STRATEGY_RANK = {
    'Première stratégie': 1,
    'Deuxième strategie': 2,
    'Troisième stratégie': 3,
}


def shocks(df):
    '''HowCoped0..2 by the survey's own rank: s14bq05__N holds a rank label or Non.

    Benin 2021-22's hook (Niger's after GH #1556).  Column 25, `Autre stratégie`,
    carries the Oui/Non label set (`nonoui`), so its rank is not in the file: it takes
    the rank no other strategy holds.  34 rows choose it here.  A label outside the
    two sets is refused.
    '''
    cope_cols = [c for c in df.columns if c.startswith('Cope')]
    seen = {str(v) for c in cope_cols for v in df[c].dropna().unique()}
    unknown = sorted(seen - set(_STRATEGY_RANK) - {'Oui', 'Non'})
    if unknown:
        raise ValueError(f'Guinea-Bissau 2021-22 s14bq05: unknown answers {unknown}')

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
