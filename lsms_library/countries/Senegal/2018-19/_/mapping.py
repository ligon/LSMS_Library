# Formatting  Functions for Senegal 2018-19
import pandas as pd
import numpy as np
import lsms_library.local_tools as tools
from lsms_library.ehcvm import (
    UnitSizeLabelError,
    derivation_key as _derivation_key,
    food_acquired_ehcvm as _food_acquired_ehcvm,
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


def food_acquired(df):
    return _food_acquired_ehcvm(df, FOOD_ACQUIRED_DERIVATION)
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
    """Serve the food stock from the column each vague put it in (GH #1556).

    Vague 1 asked five consequences (d = stock, e = achats, f blank) and vague 2
    six (d = effectif du cheptel, e = stock, f = achats).  ``AffectedConsumption``
    arrives as d and ``StockVague2`` as e, through one mapping.  A blank f is what
    tells the two apart in the file, so a release where it stops being one raises:
    d is a different question on those rows and nothing else would say so.
    """
    vague = pd.to_numeric(df.pop('vague'))
    stock_vague2 = df.pop('StockVague2')
    purchases = df.pop('PurchasesVague2')
    five = vague.eq(1)
    asked = stock_vague2.notna() | df['AffectedConsumption'].notna()
    if not vague.isin([1, 2]).all() or purchases[five].notna().any() \
            or purchases[~five & asked].isna().any():
        raise ShockLayoutError(
            f"s14q04f should be blank in vague 1 and filled in vague 2: "
            f"{int(purchases[five].notna().sum())} filled in vague 1, "
            f"{int(purchases[~five & asked].isna().sum())} blank in vague 2, "
            f"vague values {sorted(vague.dropna().unique().tolist())}")
    df['AffectedConsumption'] = df['AffectedConsumption'].where(five, stock_vague2)
    return df


def shocks(df):
    df = _food_stock_by_vague(df)
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
