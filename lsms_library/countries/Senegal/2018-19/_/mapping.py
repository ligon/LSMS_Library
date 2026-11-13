# Formatting  Functions for Senegal 2018-19
import pandas as pd
import numpy as np
import lsms_library.local_tools as tools
from lsms_library.transformations import U_UNKNOWN
from lsms_library.ehcvm import coping_set_from_grid as _coping_set_from_grid
from lsms_library.ehcvm import (
    UnitSizeLabelError,
    derivation_key as _derivation_key,
    food_acquired_with_unit_weights as _shared_food_acquired_with_unit_weights,
    inputs_last_purchase as _inputs_last_purchase,
    served_unit_labels as _served_unit_labels,
    sized_unit as _sized_unit,
)

# GH #876.  The wide frame's `Expenditure` used to be `s07bq08`, the value of
# ONE purchase made up to 30 days ago, sitting beside a 7-day consumption
# `Quantity`.  `_food_acquired_ehcvm` derives it instead -- purchased Quantity
# x (s07bq08 / s07bq07a), NA where the last purchase used a different unit --
# and stamps the registry key on every purchased row.  See lsms_library/ehcvm.py
# and Senegal/_/derivations.yml.
FOOD_ACQUIRED_DERIVATION = _derivation_key('Senegal')

# GH #812.  `strata` is the documented stratum, region x milieu, as a label.  It
# is a construction, so `sample` carries its registry key: see
# lsms_library/ehcvm.py and Senegal/_/derivations.yml.
from lsms_library.ehcvm import (  # noqa: E402
    inputs_region_milieu_stratum as _inputs_region_milieu_stratum,
    stamp_region_milieu_stratum as _stamp_region_milieu_stratum,
    strata_label,
)


def inputs_strata(wave):
    """The registry `inputs` callable: the cover's region and milieu at (t, i)."""
    return _inputs_region_milieu_stratum('Senegal', wave)


def sample(df):
    return _stamp_region_milieu_stratum(df, 'Senegal')


def inputs_food_acquired(wave):
    """The registry `inputs` callable: raw section-7B answers at (t, i, j).

    A wave-level wrapper rather than a `functools.partial` in the country
    module, so that binding the country name does not put this code in
    Senegal/_/senegal.py -- which is in EVERY Senegal table's cache
    fingerprint.
    """
    return _inputs_last_purchase('Senegal', wave)


def _food_acquired_with_unit_weights(df, key, *, wave):
    """The shared EHCVM hook, then the GH #1519 exact-value rule on its output.

    Named for the shared function (imported as ``_shared_food_acquired_with_unit_weights``)
    so that :func:`food_acquired` reads as it did before GH #1519; the GH #1500 tests run
    that function with this name stubbed.  The near-zero records are found BEFORE the
    shared hook, on the wide frame, which is still the file row for row.
    """
    near_zero = _near_zero_positions(df)
    out = _shared_food_acquired_with_unit_weights(df, key, wave=wave)
    return _withhold_near_zero_quantities(out, df, near_zero)


def food_acquired(df):
    out = _food_acquired_with_unit_weights(
        df, FOOD_ACQUIRED_DERIVATION, wave='2018-19')
    # GH #1500; ledger §4.  Fill only the final label, after size/NSU/value
    # processing: a size or last purchase cannot supply a missing unit answer.
    index = out.index
    unit_level = index.names.index('u')
    codes = index.codes[unit_level]
    missing = codes == -1
    if missing.any():
        units = index.levels[unit_level]
        if U_UNKNOWN not in units:
            if isinstance(units, pd.CategoricalIndex) and U_UNKNOWN not in units.categories:
                units = units.add_categories([U_UNKNOWN])
            units = units.insert(len(units), U_UNKNOWN)
        filled = index.set_levels(units, level='u').set_codes(
            np.where(missing, units.get_loc(U_UNKNOWN), codes), level='u')
        out.index = filled
    return out


# ---------------------------------------------------------------------------
# GH #1519: two released quantities below 1e-3 are served NA (ruling @ligon
# 2026-11-12, "Exact-value NA rule")
# ---------------------------------------------------------------------------
#
# Three records of s07b_me_sen2018.dta, all product 132 (served `Autres tisanes
# et infusions`), hold s07bq03a near 4e-19 / 9e-19 and s07bq07a near 1e-20 /
# 2e-20, each exactly 40 times the other.  No other positive s07bq03a or s07bq07a
# of this file, or of any other EHCVM wave's s07b, is below 1e-3.  Served as
# recorded, the quantity put food_prices at 1.45e22 and 1.79e22 FCFA/kg.  The two
# values are treated as missing, which is the precedent of GH #1594 (Mali 2014-15,
# an exact s13q03c of 99,999,999 masked in the wave mapper): the rows stay, with
# Quantity NA and so, by the registered #876 rule that multiplies it, Expenditure
# and Quantity_kg NA.  Disclosed by `Senegal::food_acquired::near-zero-quantity-withheld`
# (`rows: 0`, like Burkina Faso's withheld GPS); the rows keep their #876 key.
NEAR_ZERO_KEY = 'Senegal::food_acquired::near-zero-quantity-withheld'
#: Below this, a released s07bq03a / s07bq07a is not a quantity (GH #1519).
NEAR_ZERO_BOUND = 1e-3
_NEAR_ZERO_SOURCE = 's07b_me_sen2018.dta'
_NEAR_ZERO_COLUMNS = ('Quantity', 'Expenditure', 'Quantity_kg')


class NearZeroQuantityError(Exception):
    """The GH #1519 rule cannot be applied as ruled.

    The wide frame is not the file row for row, or a record the rule selects
    shares its served key with another record, so NA on that key would delete the
    other record's answer.  Subclasses ``Exception``: a derived food table must
    not read it as a failed build (GH #1546)."""


def _near_zero_raw(wave='2018-19'):
    """``s07b_me_sen2018.dta`` in its codes, every record, in file order."""
    if wave != '2018-19':
        raise ValueError(f'{NEAR_ZERO_KEY} covers 2018-19 only, not {wave!r}')
    from lsms_library.paths import countries_root
    path = countries_root() / 'Senegal' / wave / 'Data' / _NEAR_ZERO_SOURCE
    return tools.get_dataframe(str(path), convert_categoricals=False)


def near_zero_quantity_rows(quantity, last_quantity):
    """Boolean ndarray: ``s07bq03a`` AND ``s07bq07a`` both in (0, 1e-3)."""
    q = pd.to_numeric(pd.Series(np.ravel(quantity)), errors='coerce').to_numpy(dtype=float)
    q7 = pd.to_numeric(pd.Series(np.ravel(last_quantity)), errors='coerce').to_numpy(dtype=float)
    with np.errstate(invalid='ignore'):
        return ((q > 0) & (q < NEAR_ZERO_BOUND)
                & (q7 > 0) & (q7 < NEAR_ZERO_BOUND))


def _near_zero_positions(df):
    """:func:`near_zero_quantity_rows` on the wide frame, the file row for row."""
    raw = _near_zero_raw()
    served = pd.to_numeric(df['Quantity'], errors='coerce').to_numpy(dtype=float)
    held = pd.to_numeric(raw['s07bq03a'], errors='coerce').to_numpy(dtype=float)
    if len(raw) != len(df) or not np.array_equal(served, held, equal_nan=True):
        raise NearZeroQuantityError(
            f'the wide food_acquired frame is not {_NEAR_ZERO_SOURCE} row for row')
    return near_zero_quantity_rows(raw['s07bq03a'], raw['s07bq07a'])


def _withhold_near_zero_quantities(out, df, near_zero):
    """NA ``Quantity``, ``Expenditure`` and ``Quantity_kg`` on the rows the selected
    records become; drop the unit-weight key where it no longer describes one.

    The selected records are reshaped on their own to learn their served keys.
    Every such key must belong to exactly one row of ``out``.
    """
    if not near_zero.any():
        return out
    from lsms_library.ehcvm import food_acquired_ehcvm, nsu_derivation_key
    from lsms_library.derivations import MULTI_SEP
    sub = food_acquired_ehcvm(df[near_zero], FOOD_ACQUIRED_DERIVATION)
    hit = out.index.isin(sub.index)
    if int(hit.sum()) != len(sub) or out.index[hit].duplicated().any():
        raise NearZeroQuantityError(
            f'{int(near_zero.sum())} record(s) under {NEAR_ZERO_KEY} become '
            f'{len(sub)} served row(s), and {int(hit.sum())} rows of the table carry '
            f'their keys: a key shared with another record would lose that answer')
    out = out.copy()
    for col in _NEAR_ZERO_COLUMNS:
        if col in out.columns:
            out.loc[hit, col] = np.nan
    weight_key = nsu_derivation_key('Senegal')

    def without_weight(value):
        if value is None or (not isinstance(value, str) and pd.isna(value)):
            return value
        parts = [p for p in str(value).split(MULTI_SEP) if p and p != weight_key]
        return MULTI_SEP.join(parts) if parts else pd.NA

    if 'Derivation' in out.columns:
        current = out['Derivation'].astype(object)
        out['Derivation'] = pd.Series(
            [without_weight(c) if h else c for c, h in zip(current, hit)],
            index=out.index, dtype='string')
    return out


def inputs_near_zero_quantity(wave='2018-19'):
    """The registry ``inputs`` callable: the selected records at ``(t, i, j)``,
    with the file's own names and codes, indexed as the served table is."""
    raw = _near_zero_raw(wave)
    frame = _inputs_last_purchase('Senegal', wave)
    held = pd.to_numeric(raw['s07bq03a'], errors='coerce').to_numpy(dtype=float)
    keyed = pd.to_numeric(frame['s07bq03a'], errors='coerce').to_numpy(dtype=float)
    if len(raw) != len(frame) or not np.array_equal(held, keyed, equal_nan=True):
        raise NearZeroQuantityError(f'the input frame is not {_NEAR_ZERO_SOURCE} row for row')
    hit = near_zero_quantity_rows(raw['s07bq03a'], raw['s07bq07a'])
    columns = ['vague', 's07bq01', 's07bq03a', 's07bq03b', 's07bq03c', 's07bq04',
               's07bq05', 's07bq06', 's07bq07a', 's07bq07b', 's07bq07c', 's07bq08']
    out = raw.loc[hit, columns].copy()
    out.index = frame.index[hit]
    return out
from collections import defaultdict

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
    '''
    return tools.format_id(value.iloc[0]) + '0' + tools.format_id(value.iloc[1], zeropadding=2)

def Sex(value):
    '''
    Formatting sex variable
    '''
    if value == 'Féminin':
        return 'f'
    if value == 'Masculin':
        return 'm'

def Age(value):
    '''
    Formatting age from date components
    '''
    month_map = {'Janvier': 1, 'Février': 2, 'Mars': 3, 'Avril': 4, 'Mai': 5,
                 'Juin': 6, 'Juillet': 7, 'Août': 8, 'Septembre': 9,
                 'Octobre': 10, 'Novembre': 11, 'Décembre': 12}
    result = list(value)
    result[2] = month_map.get(value.iloc[2])
    return result

def Birthplace(value):
    '''
    Formatting birthplace variable
    '''
    if pd.isna(value):
        return value
    else:
        return value.title()
    
def Relationship(value):
    '''Title-case the s01q02 label, and keep a missing answer MISSING.

    GH #813.  The former guard was ``if value:``, and a float ``nan`` is
    TRUTHY -- so an unanswered ``s01q02`` went through ``str(nan).title()``
    and was served as the literal string ``'Nan'``.  3 of this wave's
    66,119 person-rows (and 15,201 of 2021-22's 78,732).  A missing answer
    is ``pd.NA``; non-null values are title-cased exactly as before.  The
    cp1252 ``Frère, sœur`` label reaches this formatter already decoded
    (GH #805, at the reader), so ``.title()`` yields ``Frère, Sœur``
    directly; the GH #801 ``_fix_relationship`` re-titling is retired.
    '''
    if pd.isna(value):
        return pd.NA
    return str(value).title()


def Region(value):
    '''
    Formatting region variable
    '''
    return value



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


class ShockLayoutError(Exception):
    """Section 14.04 of this file does not have the layout :func:`shocks` serves.

    Subclasses ``Exception``, not ``ValueError`` / ``KeyError`` / ``RuntimeError``:
    the derived-table paths catch those and would fall back to another build.
    """


def _food_stock_by_vague(df):
    """Serve food stock and purchases from each vague's own columns (GH #1556).

    Vague 1 asked five consequences (d = stock, e = achats, f blank) and vague 2
    six (d = effectif du cheptel, e = stock, f = achats).  ``AffectedFoodStock``
    arrives as d, ``StockVague2`` as e, and ``AffectedConsumption`` as f,
    through the same Boolean mapping. ``PurchasesVague2`` retains raw f for the
    guard, independent of whether a label maps to a Boolean. A blank f is what
    tells the two apart in the file, so a release where it stops being one raises:
    d is a different question on those rows and nothing else would say so.
    """
    vague = pd.to_numeric(df.pop('vague'))
    stock_vague2 = df.pop('StockVague2')
    purchases = df.pop('PurchasesVague2')
    five = vague.eq(1)
    asked = stock_vague2.notna() | df['AffectedFoodStock'].notna()
    if not vague.isin([1, 2]).all() or purchases[five].notna().any() \
            or purchases[~five & asked].isna().any():
        raise ShockLayoutError(
            f"s14q04f should be blank in vague 1 and filled in vague 2: "
            f"{int(purchases[five].notna().sum())} filled in vague 1, "
            f"{int(purchases[~five & asked].isna().sum())} blank in vague 2, "
            f"vague values {sorted(vague.dropna().unique().tolist())}")
    df['AffectedFoodStock'] = df['AffectedFoodStock'].where(five, stock_vague2)
    df['AffectedConsumption'] = df['AffectedConsumption'].where(~five, stock_vague2)
    return df


def shocks(df):
    """The coping SET from the s14q05__N grid; no HowCoped<n> position (GH #1562).

    The grid records which strategies were chosen, not their order (ruling
    2026-11-12), so the set only: ehcvm.coping_set_from_grid, which says which
    cell values count as chosen.
    """
    df = _food_stock_by_vague(df)
    return _coping_set_from_grid(df, COPING_LABELS, country="Senegal")


FIES_ITEMS = ['Worried', 'HealthyDiet', 'FewFoods', 'SkippedMeal',
              'AteLess', 'RanOut', 'Hungry', 'WholeDay']


def food_security(df):
    '''
    Compute FIES_score: count of True across the 8 FAO FIES items.
    NaN only when ALL eight items are NaN (item not asked / refused).
    '''
    items = df[FIES_ITEMS].apply(lambda c: c.map({True: True, False: False}))
    score = items.sum(axis=1, min_count=1)
    all_na = items.isna().all(axis=1)
    score = score.where(~all_na, other=np.nan)
    df['FIES_score'] = score.astype('Int64')
    return df


def household_roster(df):
    '''
    Formatting dataframe to calculate ages.

    The previous -1 pre-strip and negative-result post-null in this
    function were subsumed by the 2026-04-25 age_handler hardening
    (commit 63866d8e): age_handler now rejects sentinel and
    out-of-biological-range inputs at the source via _is_plausible_age
    and [0, 120] clamps on the DOB and year-math fallback paths.

    The ``Frère, Sœur`` Relationship label of this cp1252 file is decoded
    by the reader since GH #805; the GH #801 ``_fix_relationship`` hook
    that used to repair ``Frère, S<U+009C>Ur`` here is retired.
    '''

    def _age_from_row(x):
        return tools.age_handler(
            age=x["Age"][0], d=x["Age"][1], m=x["Age"][2], y=x["Age"][3],
            interview_date=x["interview_date"], interview_year=2018,
        )

    df["Age"] = df.apply(_age_from_row, axis=1)
    df = df.drop('interview_date', axis='columns')

    return df


# ---------------------------------------------------------------------------
# community_prices: EHCVM section 5, s05_co_sen2018.dta (GH #1226)
# ---------------------------------------------------------------------------
#
# The Niger template (rulings D1-D6, 2026-10-22; Niger/_/niger.py _s05_*), with
# the household's sized unit form and no folding (#1453 ruling, 2026-11-04).
# One copy per country, so that each country's wiring can be taken alone.  #1226
# asks for them in lsms_library/ehcvm.py; that move is the residual.
COMMUNITY_PRICES_DERIVATION = 'Senegal::community_prices::one-item-per-price'
_S05_SOURCE = 's05_co_sen2018.dta'
_S05_HOUSEHOLD = 's07b_me_sen2018.dta'
_S05_INDEX = ['t', 'v', 'j', 'u', 'variety', 'visit', 'obs']


def _s05_labels(categories, name):
    return {int(code): str(label).strip() for code, label in categories.get(name, {}).items()
            if pd.notna(label) and str(label).strip()}


def _s05_unit_size(code, hh_units):
    """``(unit, size)`` of one ``s05q02`` unit-size code.

    ``code = 10 * unit + size``, including the four-digit units 1000-1003
    (10010 is unit 1001, size 0).  A two-digit size (98, `Autre taille 1`) makes
    it ``100 * unit + size``; the household labels list it and no price uses it.
    """
    code = int(code)
    unit, size = divmod(code, 10)
    if unit not in hh_units:
        unit, size = divmod(code, 100)
    if unit not in hh_units:
        raise UnitSizeLabelError(f"Senegal s05: unit-size code {code} names no household unit")
    return unit, size


def _s05_u_by_code(unit_codes, community, household):
    """``{code: u}``: the household's served label for the code's unit and size.

    The same composition ``ehcvm.carry_unit_size`` gives ``food_acquired.u``
    (the read-time ``u`` table, keyed on the household's own mojibake, then
    ``ehcvm.sized_unit``), so a price joins the household row in the same unit
    and size.  By CODE, because the s05 labels are not the household's strings,
    and eight used codes have no s05 label at all.  So 10000 is
    `Bouteille de 19 L` although the form prints `Bouteille de 20l`: 19 and 20
    are label variants of unit 1000 (CONTENTS.org).  No two units or sizes may
    share a label: that would be a fold, and it raises.
    """
    hh_units = _s05_labels(household, 's07bq03b')
    hh_sizes = _s05_labels(household, 's07bq03c')
    parts = {int(c): _s05_unit_size(c, hh_units) for c in unit_codes}
    raw = sorted({hh_units[unit] for unit, _ in parts.values()})
    served = dict(zip(raw, _served_unit_labels('Senegal', raw)))
    out, source = {}, {}
    for code, (unit, size) in parts.items():
        label = _sized_unit(served[hh_units[unit]], hh_sizes[size])
        if label in source:
            raise UnitSizeLabelError(f"Senegal s05: codes {source[label]} and {code} "
                                     f"would both be served {label!r}")
        source[label] = code
        out[code] = label
    moved = [(a, b) for a, b in zip(out.values(), _served_unit_labels('Senegal', list(out.values())))
             if a != b]
    if moved:
        raise UnitSizeLabelError(f"Senegal s05: the read-time u table would relabel {moved[:5]}")
    return out


def _s05_base(codes, community, household):
    """One row per source record: its served keys and ``Description``, by CODE.

    ``j`` is the household's ``s07bq01`` label of the same product code through
    ``harmonize_food``, whose keys are the household's mojibake (CONTENTS.org:
    the crosswalk is the repair, so nothing is decoded here).  ``Description``
    keeps the s05 label as recorded.  The form names nothing finer than ``j``,
    so ``variety`` is the canonical ``unknown_variety``, unless
    ``harmonize_food`` lumps two priced codes into one ``j`` (none today), where
    the code is the ``variety`` (``data_info.yml``, GH #840).
    """
    from lsms_library.paths import countries_root
    path = countries_root() / 'Senegal'
    item_map = tools.get_categorical_mapping(
        tablename='harmonize_food', idxvars='Original Label',
        dirs=[str(path / '2018-19' / '_'), str(path / '_')],
        **{'Preferred Label': 'Preferred Label'})
    hh_items = _s05_labels(household, 's07bq01')
    s05_items = _s05_labels(community, 's05q01')
    product = codes['s05q01'].astype(int)
    unit = codes['s05q02'].astype(int)
    j_by_code = {c: item_map.get(hh_items[c], hh_items[c]) for c in product.unique()}
    lumped = pd.Series(j_by_code).value_counts()
    u_by_code = _s05_u_by_code(unit.unique(), community, household)
    return pd.DataFrame({
        't': '2018-19',
        'v': codes['grappe'].map(tools.format_id).astype('string'),
        'j': product.map(j_by_code).astype('string'),
        'u': unit.map(u_by_code).astype('string'),
        'variety': product.map(lambda c: str(c) if lumped[j_by_code[c]] > 1
                               else 'unknown_variety').astype('string'),
        'visit': pd.to_numeric(codes['vague'], errors='raise').astype('int64'),
        'Description': product.map(s05_items).astype('string'),
    })


def community_prices_s05(codes, community, household):
    """Both recorded readings of every source record, unaltered.

    ``s05q03`` -> obs 1, ``s05q04`` -> obs 2; a repeated (grappe, product,
    unit-size) key's later record -> obs 3 / 4 (D1, D5; none in the held file).
    Nothing is dropped, averaged or converted.  ``Quantity`` is not a source
    field: see :func:`derive_s05_one_item_per_price`.
    """
    from lsms_library.ehcvm import community_price_slots
    codes = codes.reset_index(drop=True)
    base = _s05_base(codes, community, household)
    rank = codes.groupby(['grappe', 's05q01', 's05q02'], sort=False).cumcount()
    out = community_price_slots(base, codes, ('s05q03', 's05q04'), rank)
    out = out.set_index(_S05_INDEX).sort_index()
    if not out.index.is_unique:
        raise ValueError('Senegal s05: two recorded prices share a served key')
    return out


def derive_s05_one_item_per_price(df):
    """``Quantity = 1`` on every s05 price row, stamped with the registry key.

    The instrument prices ONE item of the selected unit-size and asks no
    quantity (D2), so the 1 is a construction of the form, not an answer; see
    ``Senegal/_/derivations.yml``.  Takes the finished frame and returns a copy.
    """
    out = df.copy()
    out['Quantity'] = pd.Series(1.0, index=out.index, dtype='Float64')
    out['Derivation'] = pd.Series(COMMUNITY_PRICES_DERIVATION, index=out.index, dtype='string')
    return out


def inputs_community_prices(wave='2018-19'):
    """The raw s05 answers under their own names, one row per source record,
    keyed by the served ``(t, v, j, u, variety, visit)``.  One record is two
    served rows, obs 1 and 2.  No quantity: the file records none."""
    if wave != '2018-19':
        raise ValueError('Senegal community prices exist only for 2018-19')
    from lsms_library.paths import countries_root
    root = countries_root() / 'Senegal' / wave / 'Data'
    codes = tools.get_dataframe(root / _S05_SOURCE, convert_categoricals=False).reset_index(drop=True)
    community = tools.get_dataframe(root / _S05_SOURCE, categories_only=True)
    household = tools.get_dataframe(root / _S05_HOUSEHOLD, categories_only=True)
    base = _s05_base(codes, community, household).drop(columns='Description')
    out = pd.concat([base, codes], axis=1)
    return out.set_index(list(base.columns))
