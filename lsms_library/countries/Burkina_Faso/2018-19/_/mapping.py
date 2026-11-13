# Formatting Functions for Burkina Faso 2018-19
import pandas as pd
import numpy as np
import lsms_library.local_tools as tools
from lsms_library.build_transforms import add_visit_level
from lsms_library.transformations import (
    food_acquired_to_canonical as _food_acquired_canonical)

from lsms_library.ehcvm import coping_set_from_grid as _coping_set_from_grid
from lsms_library.ehcvm import (
    UnitSizeLabelError,
    derivation_key as _derivation_key,
    food_acquired_with_unit_weights as _food_acquired_with_unit_weights,
    inputs_last_purchase as _inputs_last_purchase,
    served_unit_labels as _served_unit_labels,
    sized_unit as _sized_unit,
)

# GH #876.  The wide frame's `Expenditure` used to be `s07bq08`, the value of
# ONE purchase made up to 30 days ago, sitting beside a 7-day consumption
# `Quantity`.  `_food_acquired_ehcvm` derives it instead -- purchased Quantity
# x (s07bq08 / s07bq07a), NA where the last purchase used a different unit --
# and stamps the registry key on every purchased row.  See lsms_library/ehcvm.py
# and Burkina_Faso/_/derivations.yml.
FOOD_ACQUIRED_DERIVATION = _derivation_key('Burkina_Faso')

# GH #812.  `strata` is the documented stratum, region x milieu, as a label.  It
# is a construction, so `sample` carries its registry key: see
# lsms_library/ehcvm.py and Burkina_Faso/_/derivations.yml.
from lsms_library.ehcvm import (  # noqa: E402
    inputs_region_milieu_stratum as _inputs_region_milieu_stratum,
    stamp_region_milieu_stratum as _stamp_region_milieu_stratum,
    strata_label,
)


def inputs_strata(wave):
    """The registry `inputs` callable: the cover's region and milieu at (t, i)."""
    return _inputs_region_milieu_stratum('Burkina_Faso', wave)


def sample(df):
    return _stamp_region_milieu_stratum(df, 'Burkina_Faso')


def inputs_food_acquired(wave):
    """The registry `inputs` callable: raw section-7B answers at (t, i, j).

    A wave-level wrapper rather than a `functools.partial` in the country
    module, so that binding the country name does not put this code in
    Burkina_Faso/_/burkina_faso.py -- which is in EVERY Burkina_Faso table's cache
    fingerprint.
    """
    return _inputs_last_purchase('Burkina_Faso', wave)


def food_acquired(df):
    """``food_acquired`` post-processor: canonical reshape + ``visit = 1``.

    EHCVM fields the consumption module ONCE (its ``vague`` is a sample split
    — which households are surveyed when — not a repeated measure, and
    ``food_acquired_to_canonical`` drops it upstream).  The country's 2014 EMC
    wave DOES repeat it, four times, so ``food_acquired`` carries a ``visit``
    level; this wave gets the constant ``1`` so all three waves share one index
    shape and the country-level concat aligns.  That is exactly what
    ``build_transforms.add_visit_level`` is for.

    No number changes here: one constant level is appended, nothing is summed,
    reduced or dropped.
    """
    return add_visit_level(_food_acquired_with_unit_weights(
        df, FOOD_ACQUIRED_DERIVATION, wave='2018-19'), visit=1)

def v(value):
    '''
    Formatting cluster id
    '''
    return tools.format_id(value)


def i(value):
    '''
    Formatting household id from (grappe, menage).
    Uses '0' separator + zero-padded menage to prevent collisions.
    '''
    return tools.format_id(value.iloc[0]) + '0' + tools.format_id(value.iloc[1], zeropadding=2)


def pid(value):
    '''
    Formatting person id from (grappe, menage, s01q00a).
    '''
    return (tools.format_id(value.iloc[0]) + '0'
            + tools.format_id(value.iloc[1], zeropadding=2) + '0'
            + tools.format_id(value.iloc[2], zeropadding=2))


def Sex(value):
    '''
    Formatting sex variable from Stata labels.
    '''
    if value == 'Féminin':
        return 'F'
    if value == 'Masculin':
        return 'M'


def Age(value):
    '''
    Formatting age from date components.
    s01q03b (month) uses French month names in this file.
    Mask 9999 sentinels on day and month before passing to age_handler.

    The function name ``Age`` collides with the ``assets`` myvar
    ``Age: s12q07`` (item age in years), which the framework binds to
    this formatter by name (column_mapping in country.py).  That path
    passes a *scalar*, not the roster's DOB Series; pass scalars
    through unchanged so assets extraction does not crash on
    ``list(<float>)`` (closes #321).
    '''
    if not isinstance(value, pd.Series):
        return value
    month_map = {'Janvier': 1, 'Février': 2, 'Mars': 3, 'Avril': 4, 'Mai': 5,
                 'Juin': 6, 'Juillet': 7, 'Août': 8, 'Septembre': 9,
                 'Octobre': 10, 'Novembre': 11, 'Décembre': 12}
    result = list(value)
    # day sentinel: 9999 → None
    if pd.notna(result[1]) and result[1] == 9999:
        result[1] = None
    # month: French text → int; 'Ne sait pas' / unknown → None
    result[2] = month_map.get(value.iloc[2])
    return result


def household_roster(df):
    '''
    Recover Age from date-of-birth components when s01q04a (declared age) is null.

    Age list from data_info.yml: [s01q04a, s01q03a(day), s01q03b(month), s01q03c(year)].
    s01q03a carries 9999 sentinel (masked in Age() above).
    s01q03b is French text months; 'Ne sait pas' maps to None via month_map.
    '''
    def _age_from_row(x):
        age_raw = x["Age"][0]
        # -1 is a sentinel for missing age; pass None so age_handler falls through to DOB columns
        age_val = None if (pd.notna(age_raw) and int(float(age_raw)) < 0) else age_raw
        result = tools.age_handler(age=age_val, d=x["Age"][1], m=x["Age"][2], y=x["Age"][3],
                                   interview_date=x["interview_date"], interview_year=2018)
        return np.nan if (pd.notna(result) and result < 0) else result

    df["Age"] = df.apply(_age_from_row, axis=1)
    df = df.drop('interview_date', axis='columns')
    return df


# ---------------------------------------------------------------------------
# cluster_features: fixes that are not a reading of their own grappe (GH #1057)
# ---------------------------------------------------------------------------
#
# Every fix in grappe_gps_bfa2018.dta at one or two decimals is a rounded copy of
# another grappe's reading: the same Timestamp to the second, the same Altitude, an
# Accuracy differing only by rounding, a position that rounds to it, the same
# province.  All seven are in vague 2.  The value is the grappe whose reading it
# copies.  Ruling of 2026-11-05: a copied reading is not a reading of that grappe.
# The lists are literal; tests/test_gh1057_burkina_copied_gps.py recomputes them from
# the file.
_GPS_COPIED_READINGS = {'216': '217', '218': '219', '226': '223', '237': '232',
                        '240': '232', '350': '351', '365': '369'}

# 6 m apart, both Namentenga and 109 km from that province's median fix, within 30 m
# of Kadiogo grappes 89 and 136, and stamped the same day as them.  Withheld by the
# same ruling.
_GPS_KADIOGO_POINT_GRAPPES = ('103', '104')

_GPS_WITHHELD_GRAPPES = (*_GPS_COPIED_READINGS, *_GPS_KADIOGO_POINT_GRAPPES)

_GPS_WITHHELD_KEY = 'Burkina_Faso::cluster_features::copied-grappe-gps-withheld'

_GPS_RAW_COLUMNS = ['grappe', 'vague', 'coordonnes_gps__Latitude', 'coordonnes_gps__Longitude',
                    'coordonnes_gps__Accuracy', 'coordonnes_gps__Altitude',
                    'coordonnes_gps__Timestamp']


def withhold_copied_grappe_gps(df):
    """Blank ``Latitude`` / ``Longitude`` on the withheld grappes.

    Takes the merged frame: the ``v`` index level, and the coordinates from ``df_geo``.
    Every other column and every row stay as they are, in order, and no row gets a key:
    a withheld value is not a served one, so the registered entry is ``rows: 0``.  With
    neither column present (``df_geo`` could not be fetched, which the GH #675 drop stamp
    reports) there is nothing to withhold.
    """
    out = df.copy()
    present = [c for c in ('Latitude', 'Longitude') if c in out.columns]
    if not present:
        return out
    withheld = out.index.get_level_values('v').isin(_GPS_WITHHELD_GRAPPES)
    out.loc[withheld, present] = np.nan
    return out


def cluster_features(df):
    """``cluster_features`` post-processor: see :func:`withhold_copied_grappe_gps`."""
    return withhold_copied_grappe_gps(df)


def inputs_cluster_features(wave='2018-19'):
    """The registry `inputs` callable: the file's own row for each withheld fix, at
    ``(t, v)``, with ``copy_of`` naming the grappe whose reading it copies (NA for 103
    and 104, which share a point rather than copy a reading).
    """
    if wave != '2018-19':
        raise ValueError(f'{_GPS_WITHHELD_KEY} covers 2018-19 only, not {wave!r}')
    from lsms_library.paths import countries_root
    gps = tools.get_dataframe(countries_root() / 'Burkina_Faso' / wave / 'Data'
                              / 'grappe_gps_bfa2018.dta')
    v = gps['grappe'].map(tools.format_id)
    keep = v.isin(_GPS_WITHHELD_GRAPPES)
    out = gps.loc[keep, _GPS_RAW_COLUMNS].assign(t=wave, v=v[keep])
    out['copy_of'] = out['v'].map(_GPS_COPIED_READINGS).astype('string')
    return out.set_index(['t', 'v'])


FIES_ITEMS = ['Worried', 'HealthyDiet', 'FewFoods', 'SkippedMeal',
              'AteLess', 'RanOut', 'Hungry', 'WholeDay']


def food_security(df):
    '''
    FAO 8-item FIES post-processor (EHCVM §8A).

    The YAML maps s08aq01..08 (Oui/Non) to True/False, with
    'Ne sait pas'/'Refus' -> None.  Here we coerce each item to a
    nullable boolean and compute FIES_score = count of True across the
    8 items.  FIES_score is NaN only when ALL 8 items are NaN; otherwise
    a NaN item contributes 0 to the count (standard for the raw FIES
    affirmative-count screener).
    '''
    for c in FIES_ITEMS:
        df[c] = df[c].astype('boolean')

    items = df[FIES_ITEMS]
    score = items.sum(axis=1, skipna=True)              # True counts as 1
    all_na = items.isna().all(axis=1)
    score = score.astype('Int64')
    score[all_na] = pd.NA
    df['FIES_score'] = score
    return df


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
    """The coping SET from the s14q05__N grid; HowCoped0..2 NA (GH #1562).

    The grid records which strategies were chosen, not their order (ruling
    2026-11-12), so the set only: ehcvm.coping_set_from_grid, which says which
    cell values count as chosen.
    """
    df = _food_stock_by_vague(df)
    return _coping_set_from_grid(df, COPING_LABELS, country="Burkina_Faso")


# Own PRICE instrument, not the separate FOCUS community workbook (#1226).
# Exact code-label attestations: UEMOA catalogue43/download/104 and /105,
# Qx_EHCVM_PRIX_BF / Qx_EHCVM_PRIX_BF_V2. Dictionary labels take precedence.
_PRICE_PRODUCT_LABELS = {
    '140': 'Sorgho rouge',       # v2 p6 / v1 p7
    '141': 'Macoroni',          # printed spelling, v2 p6 / v1 p7
    '143': 'Sucre en carreaux', # v2 p15 / v1 p16
}
_PRICE_UNIT_LABELS = {'1161': 'Canari-petit'}  # v2 p17, BOISSONS
#: Priced units the read-time `u` table folds onto another priced unit.  The
#: community side keeps them apart (#1453 ruling, 2026-11-04): a tomato tin is
#: not a tin, whatever the household table does with it.
_PRICE_UNFOLDED_UNITS = frozenset({103, 108, 140, 141})
#: Codes served in the form's own word: 1040's label is not unit 104's (`Avec os
#: en tas`), and 1400 keeps the spelling the read-time `u` table does not fold.
_PRICE_FORM_BASE = {1040: 'Avec os au kg', 1400: 'Sans os au kg'}
COMMUNITY_PRICES_DERIVATION = 'Burkina_Faso::community_prices::quoted-native-unit-count'
_PRICE_INDEX = ['t', 'v', 'j', 'u', 'variety', 'visit', 'obs']
_PRICE_SOURCE = 's05_co_bfa2018.dta'


def _price_labels(categories, name):
    return {tools.format_id(code): str(label)
            for code, label in categories.get(name, {}).items()
            if pd.notna(label) and str(label).strip()}


def _price_u_by_code(unit_codes, community_categories, household_categories):
    """{code: u}, the household's sized form by code, with nothing folded.

    A size-free unit is served bare and a sized one `<unit> (<Size>)`, composed
    as ehcvm.carry_unit_size composes food_acquired.u: the household's label for
    the unit (its value label, else the ehcvm_units codebook data_info.yml
    decodes it with), the read-time u table, ehcvm.sized_unit. So a price joins
    the household row in the same unit and size (#1453 ruling, 2026-11-04).
    Three kinds of code stay out of that composition:
    - a unit the table folds onto another priced unit keeps its own label
      (_PRICE_UNFOLDED_UNITS);
    - a code no label names stays opaque: 1001 is NOT inferred to be Kg (Petit);
    - a labelled code whose unit nobody labels (2540 Demi, 2550 Entier) keeps
      its label, which assumes no physical mass.
    No two units or sizes may share a label: that would be a fold, and it raises.
    Identity survives regardless in variety and SourceUnitCode.
    """
    from lsms_library.paths import countries_root
    path = countries_root() / 'Burkina_Faso'
    codebook = tools.get_categorical_mapping(
        tablename='ehcvm_units', idxvars='Code',
        dirs=[str(path / '2018-19' / '_'), str(path / '_')],
        **{'Preferred Label': 'Preferred Label'})
    s05_units = _price_labels(community_categories, 's05q02')
    hh_units = _price_labels(household_categories, 's07bq03b')
    hh_sizes = _price_labels(household_categories, 's07bq03c')

    def raw_unit(unit):
        return str(hh_units.get(str(unit), codebook.get(str(unit), ''))).strip() or None

    codes = [str(code) for code in unit_codes]
    raw = sorted({raw_unit(int(code) // 10) for code in codes} - {None})
    served = dict(zip(raw, _served_unit_labels('Burkina_Faso', raw)))
    out, source = {}, {}
    for code in codes:
        label = s05_units.get(code, _PRICE_UNIT_LABELS.get(code))
        unit, size = divmod(int(code), 10)
        base = raw_unit(unit)
        if label is None:
            out[code] = f'Unknown [Burkina_Faso 2018-19 s05q02={code}]'
        elif base is None:
            out[code] = label.strip()
        else:
            if int(code) in _PRICE_FORM_BASE:
                base = _PRICE_FORM_BASE[int(code)]
            elif unit not in _PRICE_UNFOLDED_UNITS:
                base = served[base]
            word = hh_sizes.get(str(size)) or (label.rsplit('-', 1)[-1] if size else None)
            out[code] = _sized_unit(base, word)
            if source.setdefault(out[code], (unit, size)) != (unit, size):
                raise UnitSizeLabelError(f"Burkina_Faso s05: {source[out[code]]} and "
                                         f"{(unit, size)} would both be served {out[code]!r}")
    return out


def _price_base(codes, community_categories, household_categories):
    """Native priced-line identity before slot expansion (ledger1226 §§3–5).

    j follows this country's household Original Label crosswalk by product
    code. u is the household's sized form by code (_price_u_by_code); no kg
    factor is guessed. variety is a constructed source coordinate, NOT an
    answer about physical variety. Raw codes remain available too.
    """
    from lsms_library.paths import countries_root
    path = countries_root() / 'Burkina_Faso'
    item_map = tools.get_categorical_mapping(
        tablename='harmonize_food', idxvars='Original Label',
        dirs=[str(path / '2018-19' / '_'), str(path / '_')],
        **{'Preferred Label': 'Preferred Label'})
    codes = codes.reset_index(drop=True)
    product = codes['s05q01'].map(tools.format_id).astype('string')
    unit = codes['s05q02'].map(tools.format_id).astype('string')
    v = codes['grappe'].map(tools.format_id).astype('string')
    visit = pd.to_numeric(codes['vague'], errors='raise').astype('Int64')
    if product.isna().any() or unit.isna().any() or v.isna().any() or visit.isna().any():
        raise ValueError('Burkina s05 has missing native observation keys; cannot discard them')
    community_items = _price_labels(community_categories, 's05q01')
    household_items = _price_labels(household_categories, 's07bq01')
    u_by_code = _price_u_by_code(unit.unique(), community_categories, household_categories)

    def description(code):
        return community_items.get(code, _PRICE_PRODUCT_LABELS.get(
            code, f'Unknown [Burkina_Faso 2018-19 s05q01={code}]')).strip()

    def item(code):
        raw = household_items.get(code, description(code))
        return str(item_map.get(raw, raw)).strip()

    return pd.DataFrame({
        't': '2018-19', 'v': v, 'j': product.map(item).astype('string'),
        'u': unit.map(u_by_code).astype('string'),
        'variety': ('s05q01=' + product + ';s05q02=' + unit).astype('string'),
        'visit': visit, 'Description': product.map(description).astype('string'),
        'SourceProductCode': product, 'SourceUnitCode': unit,
        'SourceRow': pd.Series(range(len(codes)), dtype='Int64'),
    })


def community_prices_s05(codes, community_categories, household_categories):
    """Both recorded slots of every Burkina PRICE source row, unaltered.

    No Existe/price filter, repeat removal, averaging or unit conversion.
    Repeated native keys, including equal-valued rows, get obs3/4,5/6 in
    file order. Complete product+unit codes protect identity if public j/u
    mappings coalesce. Quantity is not a source field; see the separate
    registered quoted-native-unit-count construction below.
    """
    from lsms_library.ehcvm import community_price_slots
    codes = codes.reset_index(drop=True)
    base = _price_base(codes, community_categories, household_categories)
    rank = base.groupby(['t', 'v', 'variety', 'visit'], sort=False,
                        dropna=False).cumcount()
    out = community_price_slots(base, codes, ('s05q03', 's05q04'), rank)
    out = out.set_index(_PRICE_INDEX).sort_index()
    if not out.index.is_unique:
        raise ValueError('Burkina s05 has colliding full observation keys')
    return out


def derive_quoted_native_unit_count(df):
    """One QUOTED native unit, not measured quantity or kg (registry1226)."""
    out = df.copy()
    out['Quantity'] = pd.Series(1.0, index=out.index, dtype='Float64')
    out['Derivation'] = pd.Series(COMMUNITY_PRICES_DERIVATION,
                                  index=out.index, dtype='string')
    return out


def inputs_community_prices(wave='2018-19'):
    """Original seven source fields and matching native join keys; no quantity.

    One source record yields two served price slots. SourceRow retains the
    original position even when a native key repeats. No household ID is
    created; #1357's household exclusion policy remains unchanged.
    """
    if wave != '2018-19':
        raise ValueError('Burkina community price inputs support only2018-19')
    from lsms_library.paths import countries_root
    root = countries_root() / 'Burkina_Faso' / wave / 'Data'
    source = root / _PRICE_SOURCE
    codes = tools.get_dataframe(source, convert_categoricals=False)
    community_categories = tools.get_dataframe(source, categories_only=True)
    household_categories = tools.get_dataframe(root / 's07b_me_bfa2018.dta',
                                                categories_only=True)
    base = _price_base(codes, community_categories, household_categories)
    out = codes.reset_index(drop=True).copy()
    for column in base:
        out[column] = base[column]
    return out.set_index(_PRICE_INDEX[:-1])
