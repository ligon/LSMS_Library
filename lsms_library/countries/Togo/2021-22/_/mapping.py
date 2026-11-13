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


def inputs_food_acquired(wave):
    """The registry `inputs` callable: raw section-7B answers at (t, i, j)."""
    return _inputs_last_purchase('Togo', wave)


# --- s07bq03a: the producer's regional factor, divided out (GH #1697) ---------------
#
# Ruling @ligon 2026-11-12, "Invert, registered (Recommended)", on the evidence of
# burst/isa-sm-20261112/reports/ehcvm2-s07bq03a-docsearch/REPORT.md §2 and
# _/CONTENTS.org "s07bq03a: the regional factor is divided out".  This file's s07bq03a,
# and no other column, is the household's answer times 1.115 x a regional multiplier
# that no document explains.  It is divided out here, before the purchased residual
# s07bq03a - s07bq04 - s07bq05 is formed, so the residual no longer leaves phantom
# purchases.  A count (`unité`) was scaled and then floored to an integer: where the
# floor is injective (f >= 1) the count is recovered exactly; where it is not (Grand
# Lomé, f < 1), or where it recovers to 0, the quantity is withheld, NA, and never
# guessed.  A row whose filed residual was positive and whose recovered one is not loses
# its purchased row; that deletion is registered too (`KEY_PURCHASE_REMOVED`).
#
# The cover s00_me_tgo2021.dta is read by a constructed path, so it is not a declared
# source of `food_acquired` and its sidecar is not in the table's cache hash (CLAUDE.md,
# cache limitation (a)): a re-released cover alone does not invalidate the table.

KEY_DIVIDED = 'Togo::food_acquired::s07bq03a-regional-factor-divided-out'
KEY_COUNT_RECOVERED = 'Togo::food_acquired::s07bq03a-floored-count-recovered'
KEY_COUNT_WITHHELD = 'Togo::food_acquired::s07bq03a-floored-count-withheld'
#: A DELETION (`rows: 0`, no served row carries it): the purchased rows the filed value
#: served and the recovered one does not; `inputs_purchase_removed_by_division`.
KEY_PURCHASE_REMOVED = 'Togo::food_acquired::s07bq03a-purchase-removed-by-division'

#: The common scalar and the round regional multipliers (doc search T-TGO-4/-6A).
COMMON_FACTOR = 1.115
REGIONAL_MULTIPLIER = {'Centrale': 1.5, 'Savanes': 1.2, 'Plateaux': 1.1, 'Kara': 0.9,
                       'Maritime': 0.9, 'Grand Lomé': 0.82}
#: The products, typed to four decimals as the file carries them: 1.6725, 1.338,
#: 1.2265, 1.0035, 1.0035, 0.9143.
REGIONAL_FACTOR = {r: round(COMMON_FACTOR * m, 4) for r, m in REGIONAL_MULTIPLIER.items()}
LOME = 'Grand Lomé'
#: Grand Lomé's other factor, on a row reporting own production or a gift.
LOME_OWN_OR_GIFT_FACTOR = 1.2
#: The one unit the producer floored after scaling: s07bq03b code 147, labelled `unité`.
#: Measured on own-production-only rows of Centrale, Savanes and Plateaux: 730 of its 767
#: are floor(s07bq04 x f) and none is s07bq04 x f, while every other unit is exact.
COUNT_UNIT = 'unité'
COUNT_CODE = 147
#: Two numbers within this relative distance are the same number: the file carries
#: some values at single precision, so q3/f and q4 + q5 differ by up to 5e-8 where the
#: household reported no purchase.  No purchase is a millionth of a consumption.
SAME_NUMBER = 1e-6

BRANCH_DIVIDED = 'divided'
BRANCH_COUNT_RECOVERED = 'count-recovered'
BRANCH_WITHHELD = 'withheld'


def regional_factor(s00q01, s07bq04, s07bq05):
    '''The factor on each row's s07bq03a: 1.115 x the region's multiplier, except a Grand
    Lomé row reporting own production or a gift, which carries 1.2.  A region the table
    does not name is refused: its mechanics are unknown.'''
    region = pd.Series(np.ravel(s00q01), dtype='string').str.strip()
    produced = pd.to_numeric(pd.Series(np.ravel(s07bq04)), errors='coerce').fillna(0)
    gift = pd.to_numeric(pd.Series(np.ravel(s07bq05)), errors='coerce').fillna(0)
    unknown = sorted(set(region.dropna()) - set(REGIONAL_FACTOR)) + (
        ['<missing>'] if region.isna().any() else [])
    if unknown:
        raise LookupError(f'Togo 2021-22 s07bq03a: no regional factor for {unknown}; the '
                          f'known regions are {sorted(REGIONAL_FACTOR)} (GH #1697)')
    factor = region.map(REGIONAL_FACTOR).astype(float).to_numpy()
    lome_own_or_gift = (region.eq(LOME) & ((produced > 0) | (gift > 0))).to_numpy(dtype=bool)
    return np.where(lome_own_or_gift, LOME_OWN_OR_GIFT_FACTOR, factor)


def _on_grid(x, step):
    k = np.asarray(x, dtype=float) / step
    return np.abs(k - np.round(k)) <= SAME_NUMBER * np.maximum(1.0, np.abs(k))


def _recover(s07bq03a, s07bq03b, s00q01, s07bq04, s07bq05):
    '''(recovered s07bq03a, branch, recovered-to-0) for each row; see `recover_s07bq03a`.'''
    q3 = pd.to_numeric(pd.Series(np.ravel(s07bq03a)), errors='coerce').to_numpy(dtype=float)
    q4 = pd.to_numeric(pd.Series(np.ravel(s07bq04)), errors='coerce').fillna(0).to_numpy()
    q5 = pd.to_numeric(pd.Series(np.ravel(s07bq05)), errors='coerce').fillna(0).to_numpy()
    f = regional_factor(s00q01, s07bq04, s07bq05)
    region = pd.Series(np.ravel(s00q01), dtype='string').str.strip().to_numpy()
    unit = pd.Series(np.ravel(s07bq03b), dtype=object)
    count = (unit.astype('string').str.strip().str.lower().eq(COUNT_UNIT).fillna(False)
             | pd.to_numeric(unit, errors='coerce').eq(COUNT_CODE)).to_numpy(dtype=bool)
    lome = region == LOME
    own_or_gift = (q4 > 0) | (q5 > 0)
    has = ~np.isnan(q3)
    with np.errstate(divide='ignore', invalid='ignore'):
        divided = q3 / f

    value = np.full(q3.shape, np.nan)
    branch = np.full(q3.shape, None, dtype=object)

    # Every unit but the count: q3 = answer x f exactly.  So is a Grand Lomé count with
    # own production or a gift (x 1.2, not floored); there the answer must land on the
    # quarter grid, or the row did not follow that rule.
    plain = has & ~count
    value[plain], branch[plain] = divided[plain], BRANCH_DIVIDED
    lome_og_count = has & count & lome & own_or_gift
    ok = lome_og_count & (q3 > 0) & _on_grid(divided, 0.25)
    value[ok], branch[ok] = divided[ok], BRANCH_DIVIDED
    branch[lome_og_count & ~ok] = BRANCH_WITHHELD

    # A count elsewhere: q3 = floor(k x f) with f >= 1.0035, injective on integers k.
    # The integer k whose floor reproduces q3 is the answer; if none does, or q3 is not
    # an integer, the row did not follow the rule and is withheld.  A count that
    # recovers to 0 is withheld too (ruling 2026-11-12, "Recover; a 0 becomes NA"): q3
    # was filed as 0, the floor of a count below 1 / f, which is a fraction, so the
    # count is not known, and a recovered 0 would serve no row for an item the
    # household reported.  Its purchase, below one unit, is not known either, so a
    # purchased row NA is served for it whatever its own production or gift
    # (`_purchase_implied`).
    floored = has & count & ~lome
    k0 = np.ceil(np.where(floored, q3, 0) / np.where(floored, f, 1) - 1e-9)
    found = np.full(q3.shape, np.nan)
    hits = np.zeros(q3.shape, dtype=int)
    for k in (k0 - 1, k0, k0 + 1):
        trip = floored & (k >= 0) & (np.abs(np.floor(k * f + 1e-9) - q3) <= 1e-9)
        hits += trip
        found = np.where(trip & np.isnan(found), k, found)
    unique = floored & _on_grid(q3, 1) & (hits == 1)
    zero = unique & (found == 0)
    rec = unique & (found > 0)
    value[rec], branch[rec] = found[rec], BRANCH_COUNT_RECOVERED
    branch[floored & ~rec] = BRANCH_WITHHELD

    # A Grand Lomé count with neither: floor(k x 0.9143) sends 1 to 0 and 11 and 12 to 10.
    # Withheld, as ruled; never guessed.
    branch[has & count & lome & ~own_or_gift] = BRANCH_WITHHELD

    # Where the answer and q4 + q5 are the same number, no purchase was reported: make
    # the residual exactly zero, not the 1e-8 of single-precision storage.
    total = q4 + q5
    same = ~np.isnan(value) & (np.abs(value - total) <= SAME_NUMBER * np.maximum(1.0, total))
    value = np.where(same, total, value)
    return value, branch, zero


def recover_s07bq03a(s07bq03a, s07bq03b, s00q01, s07bq04, s07bq05):
    '''The household's own 7-day consumption quantity, with the producer's factor
    divided out.  The registered function of the `...-divided-out` and
    `...-floored-count-recovered` keys.

    * a unit other than `unité`, or a Grand Lomé `unité` reporting own production or a
      gift: s07bq03a / f, f from `regional_factor`;
    * an `unité` outside Grand Lomé: the positive integer k with floor(k x f) = s07bq03a;
    * NaN where it cannot be recovered: a Grand Lomé `unité` with neither own production
      nor a gift, an `unité` no integer reproduces, an `unité` outside Grand Lomé filed
      as 0 (it recovers to 0, the floor of a fraction), or a Grand Lomé `unité` with own
      production or a gift whose quotient is off the quarter grid;
    * a value within `SAME_NUMBER` of s07bq04 + s07bq05 is that sum exactly.

    No options, by design (``SkunkWorks/derived_values.org``).  The producer's filed
    value is s07bq03a in ``Country('Togo').derivation_inputs(key)``.
    '''
    return _recover(s07bq03a, s07bq03b, s00q01, s07bq04, s07bq05)[0]


def classify_s07bq03a(s07bq03a, s07bq03b, s00q01, s07bq04, s07bq05):
    '''Which rule each row's s07bq03a takes: 'divided', 'count-recovered', 'withheld',
    or None where it is missing.  The registered function of the `...-withheld` key.'''
    return _recover(s07bq03a, s07bq03b, s00q01, s07bq04, s07bq05)[1]


def _wave_data():
    from pathlib import Path

    from lsms_library.paths import countries_root
    return Path(countries_root()) / 'Togo' / '2021-22' / 'Data'


def _cover_regions():
    '''s00q01 per grappe, as the cover labels it; one region per grappe, or refused.'''
    cover = tools.get_dataframe(str(_wave_data() / 's00_me_tgo2021.dta'), convert_categoricals=True)
    grappe = cover['grappe'].map(tools.format_id)
    region = cover['s00q01'].astype('string').str.strip()
    per = region.groupby(grappe.to_numpy()).nunique()
    if (per > 1).any():
        raise LookupError(f'Togo 2021-22: grappes {list(per[per > 1].index[:5])} hold two regions')
    return region.groupby(grappe.to_numpy()).first()


def _shared(df):
    '''GH #876's hook on one part of the wave: Expenditure, keyed; size in u (GH #1224).'''
    return _food_acquired_ehcvm(df, FOOD_ACQUIRED_DERIVATION)


def _add_key(out, key, rows):
    from lsms_library.derivations import union_keys
    current = out['Derivation'].astype(object).to_numpy()
    stamped = [union_keys([c, key]) if r else c for c, r in zip(current, rows)]
    out['Derivation'] = pd.Series(stamped, index=out.index, dtype='string')
    return out


def _purchased(out):
    return out.index.get_level_values('s').to_numpy() == 'purchased'


def _purchase_implied(q4, q5, filed_residual, zero):
    '''Where a withheld row is served one purchased row, NA: it reports neither own
    production nor a gift, or its filed residual s07bq03a - s07bq04 - s07bq05 is positive,
    or it is a count that recovered to 0 (`zero`).  For the last, a filed 0 makes the
    filed residual silent, and the purchase cannot be excluded: the count is below 1 / f.'''
    return ((q4 <= 0) & (q5 <= 0)) | (filed_residual > SAME_NUMBER) | zero


def food_acquired(df):
    '''GH #876's construction on s07bq03a with the regional factor divided out (GH #1697).

    Each row's s07bq03a is first recovered (`recover_s07bq03a`); the shared hook then
    forms the purchased residual and values it at the last purchase's unit value.  The
    purchased row carries the 7-day key and the key of the rule its s07bq03a took.  A
    withheld row keeps its produced and in-kind rows, which read s07bq04 and s07bq05 as
    filed, and is served one purchased row with Quantity and Expenditure NA where a
    purchase is implied: the row reports neither own production nor a gift, or its filed
    residual is positive, or it is a count that recovered to 0 (`_purchase_implied`).
    '''
    regions = _cover_regions()
    region = pd.Series(df.index.get_level_values('v')).map(regions).to_numpy()
    filed = pd.to_numeric(df['Quantity'], errors='coerce').to_numpy(dtype=float)
    value, branch, zero = _recover(filed, df['ConsumptionUnitCode'], region,
                                   df['Produced'], df['GiftQuantity'])
    df = df.assign(Quantity=value)
    parts = []
    for name, key in ((BRANCH_DIVIDED, KEY_DIVIDED), (BRANCH_COUNT_RECOVERED, KEY_COUNT_RECOVERED)):
        part = df[branch == name]
        if len(part):
            out = _shared(part)
            parts.append(_add_key(out, key, _purchased(out)))
    if pd.isna(branch).any():
        parts.append(_shared(df[pd.isna(branch)]))
    held = branch == BRANCH_WITHHELD
    if held.any():
        # Quantity is NA here, so the shared hook serves the produced and in-kind rows only.
        parts.append(_shared(df[held]))
        q4 = df['Produced'].fillna(0).to_numpy()
        q5 = df['GiftQuantity'].fillna(0).to_numpy()
        implied = held & _purchase_implied(q4, q5, filed - q4 - q5, zero)
        if implied.any():
            # The shared reshape builds the purchased row's index (sized u included), from
            # a placeholder quantity it never serves.
            out = _shared(df[implied].assign(Quantity=1.0, Produced=0.0, GiftQuantity=0.0))
            out = out[_purchased(out)].copy()
            out['Quantity'] = np.nan
            out['Expenditure'] = np.nan
            parts.append(_add_key(out, KEY_COUNT_WITHHELD, np.ones(len(out), dtype=bool)))
    return _withhold_collisions(pd.concat(parts), KEY_COUNT_WITHHELD)


def _withhold_collisions(out, key):
    '''A withheld row that shares its served key (t, v, i, j, u, s) with another row -- two
    items of the file under one served j -- would be summed with it by the core collapse,
    which skips an NA, and the sum would be served as if whole.  So every row of such a
    group is withheld and keyed: no NA quantity leaves a valued row.'''
    keyed = out['Derivation'].fillna('').astype(str).str.split('+').apply(lambda ks: key in ks).to_numpy()
    dup = out.index.duplicated(keep=False)
    hit = keyed & dup
    if not hit.any():
        return out
    group = out.index.isin(out.index[hit])
    out = out.copy()
    out.loc[group, 'Quantity'] = np.nan
    out.loc[group, 'Expenditure'] = np.nan
    return _add_key(out, key, group)


S07BQ03A_RAW_VARIABLES = ['s07bq03a', 's07bq03b', 's07bq03c', 's07bq04', 's07bq05', 's00q01']


def _inputs_s07bq03a(wave):
    '''The raw answers behind the three s07bq03a keys at (t, i, j): the 7-day key's
    inputs, with the cover's region beside them, and the rule each row takes.'''
    raw = _inputs_last_purchase('Togo', wave)
    cover = tools.get_dataframe(str(_wave_data() / 's00_me_tgo2021.dta'), convert_categoricals=True)
    region = pd.Series(cover['s00q01'].astype('string').str.strip().to_numpy(),
                       index=household_ids(cover).astype('string').to_numpy())
    raw = raw.assign(s00q01=region.reindex(raw.index.get_level_values('i')).to_numpy())
    raw = raw[S07BQ03A_RAW_VARIABLES].copy()
    args = [raw[c] for c in ('s07bq03a', 's07bq03b', 's00q01', 's07bq04', 's07bq05')]
    value, branch, zero = _recover(*args)
    q4 = pd.to_numeric(raw['s07bq04'], errors='coerce').fillna(0).to_numpy()
    q5 = pd.to_numeric(raw['s07bq05'], errors='coerce').fillna(0).to_numpy()
    filed = pd.to_numeric(raw['s07bq03a'], errors='coerce').to_numpy(dtype=float)
    return raw, branch, value - q4 - q5, filed - q4 - q5, q4, q5, zero


def inputs_regional_factor_divided_out(wave):
    '''Registry `inputs`: the rows divided by the factor that are served a purchased row,
    one per keyed row, at (t, i, j).  s07bq03a is the producer's filed value.'''
    raw, branch, residual = _inputs_s07bq03a(wave)[:3]
    return raw[(branch == BRANCH_DIVIDED) & (residual > 0)]


def inputs_floored_count_recovered(wave):
    '''Registry `inputs`: the `unité` rows whose count was recovered from its floor and
    are served a purchased row, at (t, i, j).'''
    raw, branch, residual = _inputs_s07bq03a(wave)[:3]
    return raw[(branch == BRANCH_COUNT_RECOVERED) & (residual > 0)]


def inputs_floored_count_withheld(wave):
    '''Registry `inputs`: the withheld rows that are served a purchased row with Quantity
    and Expenditure NA, at (t, i, j): those reporting neither own production nor a gift,
    those whose filed residual is positive, and the counts that recovered to 0.'''
    raw, branch, _, filed_residual, q4, q5, zero = _inputs_s07bq03a(wave)
    return raw[(branch == BRANCH_WITHHELD) & _purchase_implied(q4, q5, filed_residual, zero)]


def inputs_purchase_removed_by_division(wave):
    '''Registry `inputs` of the DELETION entry (`rows: 0`): the source rows that the filed
    s07bq03a served a purchased row and the recovered one serves none, at (t, i, j).

    A filed residual s07bq03a - s07bq04 - s07bq05 above 0 is the shared reshape's own test
    for serving a purchased row; after the factor is divided out (or the count recovered)
    the residual is 0 or below, so the row has no purchased row to carry a key.  A
    withheld row is not here: where a purchase is implied it is served one, NA.  The
    deleted row's produced and in-kind rows are served as filed.
    '''
    raw, branch, residual, filed_residual = _inputs_s07bq03a(wave)[:4]
    recovered = (branch == BRANCH_DIVIDED) | (branch == BRANCH_COUNT_RECOVERED)
    return raw[recovered & (filed_residual > 0) & ~(residual > 0)]


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
