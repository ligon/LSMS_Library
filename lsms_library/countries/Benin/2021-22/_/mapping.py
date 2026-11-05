# Formatting functions for Benin 2021-22 (EHCVM-2).  See _/CONTENTS.org "2021-22 wired".
import numpy as np
import pandas as pd

import lsms_library.local_tools as tools

from lsms_library.ehcvm import (
    derivation_key as _derivation_key,
    food_acquired_ehcvm as _food_acquired_ehcvm,
    inputs_last_purchase as _inputs_last_purchase,
)

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


FOOD_ACQUIRED_DERIVATION = _derivation_key('Benin')

#: s07bq03b and s07bq07b label one code book twice, and each lacks a code the other has:
#: 102 has no label in s07bq03b (47 rows) and is `Alvéole` in s07bq07b (126); 207 is
#: `Chaîne en bois` in s07bq03b (41) and has none in s07bq07b (13).  The questionnaire's
#: unit list prints both (PDF pp. 113-115).  A code with no label arrives as a number, so
#: unit_match would read it as another unit and the row would serve Expenditure NA.  The
#: other five differences fold under `_unit_key`: Kg/kg, Boite/Boîte, Bol/bol,
#: Morceau/morceau, Tongolo/tongolo.
_CONSUMPTION_UNIT_WITHOUT_LABEL = {102: 'Alvéole'}
_PURCHASE_UNIT_WITHOUT_LABEL = {207: 'Chaîne en bois'}


def _named_code(value, names):
    if isinstance(value, (int, float, np.integer, np.floating)) and pd.notna(value):
        return names.get(int(value), value)
    return value


def ConsumptionUnitCode(value):
    '''s07bq03b, with the label this variable lacks.'''
    return _named_code(value, _CONSUMPTION_UNIT_WITHOUT_LABEL)


def LastPurchaseUnitCode(value):
    '''s07bq07b, with the label this variable lacks.'''
    return _named_code(value, _PURCHASE_UNIT_WITHOUT_LABEL)


def food_acquired(df):
    '''GH #876: purchased Quantity x s07bq08 / s07bq07a, keyed; size in u (GH #1224).'''
    return _food_acquired_ehcvm(df, FOOD_ACQUIRED_DERIVATION)


def inputs_food_acquired(wave):
    """The registry `inputs` callable: raw section-7B answers at (t, i, j)."""
    return _inputs_last_purchase('Benin', wave)


# --- the wave's scripts, and the registry's raw inputs ------------------------------

KEY_HH_CROP_SALE = 'Benin::crop_production::hh-crop-sale-single-row'
KEY_HARVEST_TO_DATE = 'Benin::crop_production::harvest-to-date'
KEY_HARVEST_DISPOSITIONS = 'Benin::crop_production::harvest-from-dispositions'


def household_ids(frame):
    '''The served `i` of each row of a (grappe, menage) file.'''
    return frame.apply(lambda r: i(pd.Series([r['grappe'], r['menage']])), axis=1)


def plot_ids(frame, field, parcel):
    '''`{field}_{parcel}`: the spelling of plot_features' plot_id.'''
    return (frame[field].apply(tools.format_id).astype('string') + '_'
            + frame[parcel].apply(tools.format_id).astype('string'))


def members(frame):
    '''A (grappe, menage, membres__id) file without the people who have left: s01 and
    s04a list them with every answer blank (s01q00aa = non), as `household_roster` drops
    them.'''
    from pathlib import Path

    from lsms_library.paths import countries_root
    roster = tools.get_dataframe(str(Path(countries_root()) / 'Benin' / '2021-22' / 'Data'
                                     / 's01_me_ben2021.dta'), convert_categoricals=True)
    gone = roster[roster['s01q00aa'].astype('string').str.strip().str.lower().eq('non').fillna(False)]
    key = ['grappe', 'menage', 'membres__id']
    out = frame.merge(gone[key].assign(_gone=True), on=key, how='left', validate='one_to_one')
    return out[out['_gone'].isna()].drop(columns='_gone')


def _food_table():
    from pathlib import Path

    from lsms_library.paths import countries_root
    root = Path(countries_root()) / 'Benin'
    return tools.get_categorical_mapping(
        tablename='harmonize_food', idxvars='Original Label',
        dirs=[f'{root / "2021-22" / "_"}/', f'{root / "_"}/'],
        **{'Preferred Label': 'Preferred Label'})


def crop_labels(codes):
    '''The served crop of each crop code, read BY CODE off s16cq04's value labels.

    s16dq01 labels only codes 1-47 and 65: its 1,089 rows of codes 51-64, 919 of them
    Soja, carry no label at all, and it spells 42 and 65 otherwise than s16cq04
    (`Amarante (Tchapata)` and `Amarante(Tchapata)`).  So a join of s16d to s16c on the
    labels misses 1,098 of the 7,701 pairs that join on the codes.  A label the table does
    not name passes as recorded, as in 2018-19 (Soja, Anacarde).
    '''
    from pathlib import Path

    from lsms_library.paths import countries_root
    names = tools.get_dataframe(str(Path(countries_root()) / 'Benin' / '2021-22' / 'Data'
                                    / 's16c_me_ben2021.dta'), categories_only=True)['s16cq04']
    table = _food_table()
    served = {int(code): table.get(str(label).strip(), str(label).strip())
              for code, label in names.items()}
    codes = pd.to_numeric(codes, errors='coerce').astype('Int64')
    unknown = sorted(set(codes.dropna().astype(int)) - set(served))
    if unknown:
        raise ValueError(f'crop codes with no s16cq04 label: {unknown}')
    return codes.map(served).astype('string')


def _raw_crop_file(wave, name, crop):
    '''One crop file as recorded, with the served t, i and crop.'''
    from pathlib import Path

    from lsms_library.paths import countries_root
    path = str(Path(countries_root()) / 'Benin' / wave / 'Data' / name)
    src = tools.get_dataframe(path, convert_categoricals=True)
    codes = tools.get_dataframe(path, convert_categoricals=False)
    keep = codes[crop].notna().values
    src, codes = src[keep].copy(), codes[keep]
    src['t'] = wave
    src['i'] = household_ids(codes).astype('string').values
    src['crop'] = crop_labels(codes[crop]).values
    src['_crop_code'] = pd.to_numeric(codes[crop], errors='coerce').astype('Int64').values
    return src


def sale_records(wave):
    '''s16d, one row per (household, crop), as recorded.  The file repeats a crop's record
    once per harvest line (627 exact repeats, compared on the codes).'''
    from pathlib import Path

    from lsms_library.ehcvm import hh_crop_records
    from lsms_library.paths import countries_root
    path = str(Path(countries_root()) / 'Benin' / wave / 'Data' / 's16d_me_ben2021.dta')
    codes = tools.get_dataframe(path, convert_categoricals=False)
    labels = tools.get_dataframe(path, convert_categoricals=True)
    kept = hh_crop_records(codes).index
    out = labels.loc[kept].copy()
    out['s16dq01'] = codes.loc[kept, 's16dq01']
    out['t'] = wave
    out['i'] = household_ids(codes.loc[kept]).astype('string').values
    out['crop'] = crop_labels(codes.loc[kept, 's16dq01']).values
    return out


def unit_labels(series):
    '''Each unit label as the API serves it; NA stays NA.'''
    from lsms_library.ehcvm import served_unit_labels
    raw = series.astype('string').str.strip()
    known = raw.dropna().unique()
    served = dict(zip(known, served_unit_labels('Benin', known)))
    return raw.map(served).astype('string')


def dispositions(records):
    '''s16d, one row per (i, crop), with its dispositions on the served unit labels.'''
    from lsms_library.ehcvm import DISPOSITIONS
    out = pd.DataFrame({'i': records['i'].values, 'crop': records['crop'].values})
    for var in DISPOSITIONS:
        out[f'{var}a'] = pd.to_numeric(records[f'{var}a'], errors='coerce').values
        out[f'{var}b'] = unit_labels(records[f'{var}b']).values
    return out


def inputs_hh_crop_sale(wave):
    '''Registry `inputs`: the s16d sale as recorded, with the field and parcel of the
    one s16c row it went on, one row per keyed served row, at (t, i, crop).'''
    from lsms_library.ehcvm import is_crop_sale
    harvest = _raw_crop_file(wave, 's16c_me_ben2021.dta', 's16cq04')
    sold = sale_records(wave)
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
    src = _raw_crop_file(wave, 's16c_me_ben2021.dta', 's16cq04')
    src = src[src['s16cq11'].astype('string').str.strip().eq('Non') & src['s16cq16a'].notna()]
    return src[['t', 'i', 'crop', 's16cq02', 's16cq03', 's16cq11', 's16cq12', 's16cq16a',
                's16cq16b']].set_index(['t', 'i', 'crop'])


def inputs_harvest_from_dispositions(wave):
    '''Registry `inputs`: the s16d answers behind the derived totals, at (t, i, crop), the
    four quantities and their units as recorded, one row per keyed served row.'''
    from lsms_library.ehcvm import DISPOSITIONS, harvest_totals
    harvest = _raw_crop_file(wave, 's16c_me_ben2021.dta', 's16cq04')
    records = sale_records(wave)
    lines = harvest.assign(Quantity=harvest['s16cq16a'], finished=harvest['s16cq11'])
    totals = harvest_totals(lines, dispositions(records))
    raw = [f'{var}{part}' for var in DISPOSITIONS for part in 'ab']
    out = records[records.set_index(['i', 'crop']).index.isin(totals.index)]
    return out[['t', 'i', 'crop', 's16dq01'] + raw].set_index(['t', 'i', 'crop'])
