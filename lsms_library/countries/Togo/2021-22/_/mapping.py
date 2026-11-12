# Formatting functions for Togo 2021-22 (EHCVM-2).  See _/CONTENTS.org "2021-22 wired".
import numpy as np
import pandas as pd

import lsms_library.local_tools as tools

from lsms_library.ehcvm import (
    derivation_key as _derivation_key,
    food_acquired_ehcvm as _food_acquired_ehcvm,
    inputs_last_purchase as _inputs_last_purchase,
)

# The same projection as 2018: Region, Rural and the GPS point are constant within
# a grappe, and this raises, naming the grappes, on the day one is not.
from lsms_library.transformations import (
    collapse_to_cluster_grain as cluster_features,
)

# GH #1575's construction, extended to this wave (ruling 2026-11-12, "Same rule as
# #1575"): `strata` is the cover's region label and milieu label, by the shared
# helper, and `sample` stamps the registry key on every row with a stratum.  The
# registry's `inputs` callable is 2018's, which reads each wave's own cover.
from lsms_library.ehcvm import (  # noqa: E402
    stamp_region_milieu_stratum as _stamp_region_milieu_stratum,
    strata_label,
)


def sample(df):
    return _stamp_region_milieu_stratum(df, 'Togo')


#: `i` of this wave.  5,441 of the 6,462 (grappe, menage) keys also occur in 2018,
#: and 1,397 of those are at the wrong-household baseline (GH #1697; Mali's #1494 is
#: the same trap).  No link is served, so the ids must not be able to meet.
ID_PREFIX = 'E2_'


def i(value):
    '''Household id from (grappe, menage): 2018's form, with this wave's prefix.'''
    return ID_PREFIX + tools.format_id(value.iloc[0]) + '0' + tools.format_id(value.iloc[1], zeropadding=2)


def pid(value):
    '''Person id, membres__id or numind: one number, the same in every table of this
    wave (numind matches membres__id on all 28,815 rows).  Overrides togo.pid, which
    expects 2018's three components.'''
    return tools.format_id(value)


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

    s01q04a is filled on 2,719 of 28,815 members and s01q03c on the other 26,096,
    with no sentinel in either in this wave.
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

_RANK = {'Première stratégie': 0, 'Deuxième strategie': 1, 'Troisième stratégie': 2}


def shocks(df):
    '''HowCoped0..2 by the survey's own rank: s14bq05__N holds a rank label or Non,
    where 2018's s14q05__N held an indicator.

    Column 25, "Autre strategie", is the exception: 25 of the 26 columns carry the
    `strategie` label set and this one `nonoui`, so it says Oui with no rank.  It goes
    in the first empty slot; all 26 of its rows name no other strategy.
    '''
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
    '''FIES_score: the count of True among the 8 items, NA where all 8 are missing.'''
    items = df[FIES_ITEMS]
    score = items.sum(axis=1, skipna=True).where(~items.isna().all(axis=1), other=pd.NA)
    df['FIES_score'] = score.astype('Int64')
    return df


FOOD_ACQUIRED_DERIVATION = _derivation_key('Togo')

#: s07bq03b and s07bq07b label one code book twice.  `_unit_key` folds case and
#: accents, and these two codes differ by more: 139 is `sachet` and `sachet
#: (industrielle)`, 141 is `Sans os au tas` and `Sans os en tas`.  This file's labels
#: carry no code prefix to settle it, so unit_match would read every sachet purchase
#: as another unit: 55,169 rows match on the raw labels, 70,581 on the codes, and
#: 70,581 with this map, row for row.
_PURCHASE_UNIT_AS_CONSUMPTION_UNIT = {'sachet (industrielle)': 'sachet',
                                      'Sans os en tas': 'Sans os au tas'}


def LastPurchaseUnitCode(value):
    '''s07bq07b, on s07bq03b's spelling of the same code.'''
    return _PURCHASE_UNIT_AS_CONSUMPTION_UNIT.get(value, value)


def food_acquired(df):
    '''GH #876: purchased Quantity x s07bq08 / s07bq07a, keyed; size in u (GH #1224).'''
    return _food_acquired_ehcvm(df, FOOD_ACQUIRED_DERIVATION)


def inputs_food_acquired(wave):
    """The registry `inputs` callable: raw section-7B answers at (t, i, j)."""
    return _inputs_last_purchase('Togo', wave)


# --- the wave's scripts, and the registry's raw inputs ------------------------------

KEY_HH_CROP_SALE = 'Togo::crop_production::hh-crop-sale-single-row'
KEY_HARVEST_TO_DATE = 'Togo::crop_production::harvest-to-date'


def household_ids(frame):
    '''The served `i` of each row of a (grappe, menage) file.'''
    return frame.apply(lambda r: i(pd.Series([r['grappe'], r['menage']])), axis=1)


def plot_ids(frame, field, parcel):
    '''`{field}_{parcel}`: the spelling of plot_features' plot_id.'''
    return (frame[field].apply(tools.format_id).astype('string') + '_'
            + frame[parcel].apply(tools.format_id).astype('string'))


def _food_table():
    from pathlib import Path

    from lsms_library.paths import countries_root
    root = Path(countries_root()) / 'Togo'
    return tools.get_categorical_mapping(
        tablename='harmonize_food', idxvars='Original Label',
        dirs=[f'{root / "2021-22" / "_"}/', f'{root / "_"}/'],
        **{'Preferred Label': 'Preferred Label'})


def crop_labels(series):
    '''A crop label through harmonize_food; one the table does not name passes as
    recorded, as in 2018 (Soja, Anacarde, Cacaoo).'''
    table = _food_table()
    raw = series.astype('string').str.strip()
    return raw.map(lambda x: table.get(x, x) if pd.notna(x) else pd.NA).astype('string')


def _raw_crop_file(wave, name, crop):
    '''One crop file as recorded, with the served t, i and crop.'''
    from pathlib import Path

    from lsms_library.paths import countries_root
    src = tools.get_dataframe(str(Path(countries_root()) / 'Togo' / wave / 'Data' / name),
                              convert_categoricals=True)
    src = src[src[crop].notna()].copy()
    src['t'] = wave
    src['i'] = household_ids(src).astype('string').values
    src['crop'] = crop_labels(src[crop]).values
    return src


def inputs_hh_crop_sale(wave):
    '''Registry `inputs`: the s16d sale as recorded, with the field and parcel of the
    one s16c row it went on, one row per keyed served row, at (t, i, crop).'''
    from lsms_library.ehcvm import hh_crop_records, is_crop_sale
    harvest = _raw_crop_file(wave, 's16c_me_tgo2021.dta', 's16cq04')
    sold = hh_crop_records(_raw_crop_file(wave, 's16d_me_tgo2021.dta', 's16dq01'))
    sold = sold[is_crop_sale(sold['s16dq05a'], sold['s16dq06'])]
    lines = harvest.groupby(['i', 'crop']).size()
    single = lines[lines == 1].index
    harvest = harvest[harvest.set_index(['i', 'crop']).index.isin(single)]
    out = sold[['t', 'i', 'crop', 's16dq01', 's16dq04', 's16dq05a', 's16dq05b', 's16dq06']].merge(
        harvest[['i', 'crop', 's16cq02', 's16cq03']], on=['i', 'crop'], how='inner',
        validate='one_to_one')
    return out.set_index(['t', 'i', 'crop'])


def inputs_harvest_to_date(wave):
    '''Registry `inputs`: the unfinished rows with a recorded quantity, at (t, i, crop).'''
    src = _raw_crop_file(wave, 's16c_me_tgo2021.dta', 's16cq04')
    src = src[src['s16cq11'].astype('string').str.strip().eq('Non') & src['s16cq16a'].notna()]
    return src[['t', 'i', 'crop', 's16cq02', 's16cq03', 's16cq11', 's16cq12', 's16cq16a',
                's16cq16b']].set_index(['t', 'i', 'crop'])
