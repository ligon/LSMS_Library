# Formatting functions for CotedIvoire 2021-22 (EHCVM-2).  See _/CONTENTS.org "2021-22 wired".
import re

import numpy as np
import pandas as pd

import lsms_library.local_tools as tools

from lsms_library.ehcvm import (
    derivation_key as _derivation_key,
    derive_region_milieu_stratum as _derive_region_milieu_stratum,
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
#: key of the same grappe as often as with its own (GH #1697): the keys do not name the same
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


#: This wave's own strata key (F1 of the GH #1697 red team; the maintainer accepted the
#: case fold on 2026-11-12, "the registry must reproduce it (own key)").  2018-19 keeps
#: `CotedIvoire::sample::region-milieu-stratum`, whose function serves the labels as filed.
KEY_STRATUM = 'CotedIvoire::sample::region-milieu-stratum-upper-region'


def derive_upper_region_milieu_stratum(region, milieu):
    '''GH #1575's region x milieu label with the region upper-cased first: the registered
    function of `KEY_STRATUM`, which reproduces the served value from the raw s00q01 and
    s00q04.  The shared ``ehcvm.derive_region_milieu_stratum`` does the rest, with its
    rules unchanged: NA where either label is missing or blank, a code refused.  This file
    labels 23 of the 33 regions in lower case (`poro`, `gÔh`) on 2018-19's codes, and
    upper case is 2018-19's label on all 33.'''
    return _derive_region_milieu_stratum(_upper(region), milieu)


def strata_label_upper_region(row):
    '''[s00q01, s00q04]: `derive_upper_region_milieu_stratum`, as a cell formatter.  The
    region part of `strata` is therefore `cluster_features.Region` (mapping.Region).'''
    return derive_upper_region_milieu_stratum(row.iloc[0], row.iloc[1])


def sample(df):
    '''Stamp this wave's strata key on every row with a stratum, and on no other.'''
    out = df.copy()
    key = pd.Series(KEY_STRATUM, index=out.index, dtype='string')
    out['Derivation'] = key.where(out['strata'].notna())
    return out


def Region(value):
    '''s00q01, on 2018-19's spelling: this file labels 23 of the 33 regions in lower case
    (`poro`, `gÔh`), on the same codes; upper-casing gives 2018-19's label on all 33.'''
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


# --- s07bq03a: the producer's H and L rows (GH #1697) --------------------------------
#
# Ruling @ligon 2026-11-12, "H to NA, flag L (Recommended)", on the evidence of
# burst/isa-sm-20261112/reports/ehcvm2-s07bq03a-docsearch/REPORT.md §3 and _/CONTENTS.org
# "s07bq03a: H withheld, L flagged".  A positive s07bq03a off the quarter grid with four or
# more decimals (H) is read as the household's answer replaced by the producer with a
# region x milieu median per adult equivalent, re-expressed in the reported unit (UEMOA /
# World Bank, EHCVM Document de travail no 4, "Mesure de la pauvreté", 2019, §2.2: "la
# valeur aberrante sera remplacée par la médiane dans la région et le milieu de
# résidence"), which no document says was written into this file.  The row's s07bq04 and
# s07bq05 are off the grid with it (2,721 of 2,722 and 2,047 of 2,047 rows), so every
# quantity the row serves is withheld.  Off the grid with at most three decimals (L) is
# flagged and served: partly a conversion between unit triplets, low-to-moderate
# confidence.  2018-19, the control, has no H row and 2,868 L-shaped ones.

KEY_REPLACED = 'CotedIvoire::food_acquired::s07bq03a-outlier-replaced-withheld'
KEY_CONVERTED = 'CotedIvoire::food_acquired::s07bq03a-partial-triplet-conversion'

#: The recording grid of the instrument (2018-19: 99.0% of positive s07bq03a on it).
GRID = 0.25
#: The fewest decimals that mark a replaced value.
REPLACED_DECIMALS = 4
#: Relative tolerance of "is an integer" when counting decimals: the file holds some values
#: at single precision.  Between 1e-7 and 1e-5 the H count moves by 244 of 20,507 rows.
DECIMAL_TOLERANCE = 1e-6
CLASS_REPLACED = 'replaced'
CLASS_CONVERTED = 'converted'


def _decimals(x, most=8):
    x = np.asarray(x, dtype=float)
    out = np.full(x.shape, most + 1)
    for d in range(most, -1, -1):
        y = x * 10 ** d
        ok = np.abs(y - np.round(y)) <= DECIMAL_TOLERANCE * np.maximum(1.0, np.abs(y))
        out = np.where(ok, d, out)
    return out


def classify_s07bq03a(s07bq03a):
    ''''replaced' (H), 'converted' (L) or None for each s07bq03a.  The registered function
    of both keys.

    H: positive, off the quarter grid, and four or more decimals; L: positive, off the
    quarter grid, three decimals or fewer.  "On the grid" and "d decimals" are judged at
    the relative tolerance `DECIMAL_TOLERANCE`.
    '''
    q3 = pd.to_numeric(pd.Series(np.ravel(s07bq03a)), errors='coerce').to_numpy(dtype=float)
    k = q3 / GRID
    off = (q3 > 0) & ~(np.abs(k - np.round(k)) <= DECIMAL_TOLERANCE * np.maximum(1.0, np.abs(k)))
    d = _decimals(np.where(np.isnan(q3), 0.0, q3))
    out = np.full(q3.shape, None, dtype=object)
    out[off & (d >= REPLACED_DECIMALS)] = CLASS_REPLACED
    out[off & (d < REPLACED_DECIMALS)] = CLASS_CONVERTED
    return out


def _shared(df):
    '''GH #876: purchased Quantity x s07bq08 / s07bq07a, keyed; size in u (GH #1224).

    A row where s07bq04 + s07bq05 = s07bq03a exactly purchased nothing.  The shared hook
    subtracts in two orders, so 12 such rows reach the reshape at 1.1e-16 instead of 0; the
    file's own residual has no value between 0 and 0.001.  They go the way a true zero goes.
    '''
    out = _food_acquired_ehcvm(df, FOOD_ACQUIRED_DERIVATION)
    float_residue = (out.index.get_level_values('s') == 'purchased') & (out['Quantity'] < 1e-9).to_numpy()
    return out[~float_residue]


def _add_key(out, key):
    from lsms_library.derivations import union_keys
    current = out['Derivation'].astype(object).to_numpy()
    out['Derivation'] = pd.Series([union_keys([c, key]) for c in current], index=out.index,
                                  dtype='string')
    return out


def food_acquired(df):
    '''GH #876's construction, with the H rows withheld and the L rows flagged (GH #1697).

    `u` is respelled here because a one-column idxvar binds format_id, not a formatter;
    ConsumptionUnitCode and LastPurchaseUnitCode keep the code, which unit_match reads.

    Every served row of an H source row (purchased, produced, in-kind, as the filed
    quantities make them) carries Quantity NA and Expenditure NA under `KEY_REPLACED`: no
    NA quantity leaves a valued purchase.  Every served row of an L source row is served
    as filed under `KEY_CONVERTED`.
    '''
    df = df.rename(index=_unit_label, level='u')
    kind = classify_s07bq03a(df['Quantity'])
    parts = []
    rest = pd.isna(kind)
    if rest.any():
        parts.append(_shared(df[rest]))
    converted = kind == CLASS_CONVERTED
    if converted.any():
        parts.append(_add_key(_shared(df[converted]), KEY_CONVERTED))
    replaced = kind == CLASS_REPLACED
    if replaced.any():
        out = _shared(df[replaced])
        out['Quantity'] = np.nan
        out['Expenditure'] = np.nan
        parts.append(_add_key(out, KEY_REPLACED))
    return pd.concat(parts)


S07BQ03A_RAW_VARIABLES = ['s07bq03a', 's07bq03b', 's07bq03c', 's07bq04', 's07bq05']


def _inputs_s07bq03a(wave, kind):
    '''The raw rows of one class, at (t, i, j), that are served at least one row.'''
    raw = _inputs_last_purchase('CotedIvoire', wave)
    raw = raw[classify_s07bq03a(raw['s07bq03a']) == kind]
    return raw[S07BQ03A_RAW_VARIABLES].copy()


def inputs_outlier_replaced(wave):
    '''Registry `inputs`: the H rows, s07bq03a, s07bq04 and s07bq05 as filed.'''
    return _inputs_s07bq03a(wave, CLASS_REPLACED)


def inputs_partial_triplet_conversion(wave):
    '''Registry `inputs`: the L rows, as filed.'''
    return _inputs_s07bq03a(wave, CLASS_CONVERTED)


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
