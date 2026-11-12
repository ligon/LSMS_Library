# Formatting functions for CotedIvoire 2021-22 (EHCVM-2).  See _/CONTENTS.org "2021-22 wired".
import re

import numpy as np
import pandas as pd

import lsms_library.local_tools as tools

from lsms_library.ehcvm import (
    derivation_key as _derivation_key,
    food_acquired_ehcvm as _food_acquired_ehcvm,
    inputs_last_purchase as _inputs_last_purchase,
)

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
    '''s00q01, on 2018-19's spelling.  This file labels 23 of the 33 regions in lower case
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


# --- food_acquired ------------------------------------------------------------------

FOOD_ACQUIRED_DERIVATION = _derivation_key('CotedIvoire')

_UNIT_CODE_PREFIX = re.compile(r'^\d+\.\s*')

#: A label of this file that names what 2018-19 spells otherwise.  The rest keep their
#: words, including the units 2018-19 kept apart and this file does not (`Bol/Cope`,
#: `Papier/feuille d'emballage`).
_UNIT_SPELLING = {'Cueillère': 'Cuillère'}


def _unit_label(value):
    '''s07bq03b, without the code: `139. Sachet` is 2018-19's `Sachet`.  The country's u
    table then serves it, as it serves 2018-19's.'''
    if not isinstance(value, str):
        return value
    label = _UNIT_CODE_PREFIX.sub('', value).strip()
    return _UNIT_SPELLING.get(label, label)


def food_acquired(df):
    '''GH #876: purchased Quantity x s07bq08 / s07bq07a, keyed; size in u (GH #1224).

    `u` is respelled here because a one-column idxvar binds format_id, not a formatter;
    ConsumptionUnitCode and LastPurchaseUnitCode keep the code, which unit_match reads.

    A row where s07bq04 + s07bq05 = s07bq03a exactly purchased nothing.  The shared hook
    subtracts in two orders, so 12 such rows reach the reshape at 1.1e-16 instead of 0; the
    file's own residual has no value between 0 and 0.001.  They go the way a true zero goes.
    '''
    df = df.rename(index=_unit_label, level='u')
    out = _food_acquired_ehcvm(df, FOOD_ACQUIRED_DERIVATION)
    float_residue = (out.index.get_level_values('s') == 'purchased') & (out['Quantity'] < 1e-9).to_numpy()
    return out[~float_residue]


def inputs_food_acquired(wave):
    """The registry `inputs` callable: raw section-7B answers at (t, i, j)."""
    return _inputs_last_purchase('CotedIvoire', wave)


# --- the wave's scripts, and the registry's raw inputs ------------------------------

KEY_HH_CROP_SALE = 'CotedIvoire::crop_production::hh-crop-sale-single-row'
KEY_HARVEST_TO_DATE = 'CotedIvoire::crop_production::harvest-to-date'
KEY_HARVEST_DISPOSITIONS = 'CotedIvoire::crop_production::harvest-from-dispositions'

#: The four dispositions of s16d: consumed, given, sold, in stock.
DISPOSITIONS = ('s16dq02', 's16dq03', 's16dq05', 's16dq13')


def household_ids(frame):
    '''The served `i` of each row of a (grappe, menage) file.'''
    return frame.apply(lambda r: i(pd.Series([r['grappe'], r['menage']])), axis=1)


def plot_ids(frame, field, parcel):
    '''`{field}_{parcel}`: the spelling of plot_features' plot_id.'''
    return (frame[field].apply(tools.format_id).astype('string') + '_'
            + frame[parcel].apply(tools.format_id).astype('string'))


def _levels():
    from pathlib import Path

    from lsms_library.paths import countries_root
    root = Path(countries_root()) / 'CotedIvoire'
    return [f'{root / "2021-22" / "_"}/', f'{root / "_"}/']


def crop_labels(series):
    '''A crop label through harmonize_food.  One the table does not name passes as
    recorded, and a code with no value label as its number: 56 to 60, 3,946 rows, whose
    names no document gives (_/CONTENTS.org, "2021-22 wired").'''
    table = tools.get_categorical_mapping(
        tablename='harmonize_food', idxvars='Original Label', dirs=_levels(),
        **{'Preferred Label': 'Preferred Label'})

    def label(value):
        if pd.isna(value):
            return pd.NA
        if isinstance(value, (int, float, np.integer, np.floating)):
            return tools.format_id(value)
        text = str(value).strip()
        return table.get(text, text)

    return pd.Series([label(v) for v in series], index=series.index, dtype='string')


def s16d_units(codes):
    '''Each s16d unit by code, on the served label.  The file labels 1 to 7 here and the
    rest are the codes of section 7B's list; harmonize_unit_s16d has them, read off this
    file.  A code on neither stays as its number.'''
    from lsms_library.ehcvm import served_unit_labels
    table = tools.get_categorical_mapping(
        tablename='harmonize_unit_s16d', idxvars='Code', dirs=_levels(),
        **{'Preferred Label': 'Preferred Label'})
    table = {int(code): label for code, label in table.items()}
    numbers = pd.to_numeric(codes, errors='coerce')
    raw = pd.Series([pd.NA if pd.isna(n) else table.get(int(n), tools.format_id(n)) for n in numbers],
                    index=codes.index, dtype='string')
    known = raw.dropna().unique()
    served = dict(zip(known, served_unit_labels('CotedIvoire', known)))
    return raw.map(served).astype('string')


def _raw_crop_file(wave, name, crop):
    '''One crop file as recorded, with the served t, i and crop.'''
    from pathlib import Path

    from lsms_library.paths import countries_root
    src = tools.get_dataframe(str(Path(countries_root()) / 'CotedIvoire' / wave / 'Data' / name),
                              convert_categoricals=True)
    src = src[src[crop].notna()].copy()
    src['t'] = wave
    src['i'] = household_ids(src).astype('string').values
    src['crop'] = crop_labels(src[crop]).values
    return src


def inputs_hh_crop_sale(wave):
    '''Registry `inputs`: the s16d sale as recorded, with the field and parcel of the
    one s16c row it went on, one row per keyed served row, at (t, i, crop).'''
    from lsms_library.ehcvm import is_crop_sale
    harvest = _raw_crop_file(wave, 's16c_me_civ2021.dta', 's16cq04')
    sold = _raw_crop_file(wave, 's16d_me_civ2021.dta', 's16dq01')
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
    src = _raw_crop_file(wave, 's16c_me_civ2021.dta', 's16cq04')
    src = src[src['s16cq11'].astype('string').str.strip().eq('Non') & src['s16cq16a'].notna()]
    return src[['t', 'i', 'crop', 's16cq02', 's16cq03', 's16cq11', 's16cq12', 's16cq16a',
                's16cq16b']].set_index(['t', 'i', 'crop'])


def inputs_harvest_from_dispositions(wave):
    '''Registry `inputs`: every s16d record with its dispositions, at (t, i, crop).'''
    src = _raw_crop_file(wave, 's16d_me_civ2021.dta', 's16dq01')
    columns = [f'{var}{part}' for var in DISPOSITIONS for part in 'ab']
    return src[['t', 'i', 'crop', 's16dq01'] + columns].set_index(['t', 'i', 'crop'])
