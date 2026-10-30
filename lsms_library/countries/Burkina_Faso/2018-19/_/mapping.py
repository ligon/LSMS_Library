# Formatting Functions for Burkina Faso 2018-19
import pandas as pd
import numpy as np
import lsms_library.local_tools as tools
from lsms_library.build_transforms import add_visit_level
from lsms_library.transformations import (
    food_acquired_to_canonical as _food_acquired_canonical)

from lsms_library.ehcvm import (
    derivation_key as _derivation_key,
    food_acquired_ehcvm as _food_acquired_ehcvm,
    inputs_last_purchase as _inputs_last_purchase,
)

# GH #876.  The wide frame's `Expenditure` used to be `s07bq08`, the value of
# ONE purchase made up to 30 days ago, sitting beside a 7-day consumption
# `Quantity`.  `_food_acquired_ehcvm` derives it instead -- purchased Quantity
# x (s07bq08 / s07bq07a), NA where the last purchase used a different unit --
# and stamps the registry key on every purchased row.  See lsms_library/ehcvm.py
# and Burkina_Faso/_/derivations.yml.
FOOD_ACQUIRED_DERIVATION = _derivation_key('Burkina_Faso')


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
    return add_visit_level(_food_acquired_ehcvm(df, FOOD_ACQUIRED_DERIVATION), visit=1)

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


def shocks(df):
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


# Own PRICE instrument, not the separate FOCUS community workbook (#1226).
# Exact code-label attestations: UEMOA catalogue43/download/104 and /105,
# Qx_EHCVM_PRIX_BF / Qx_EHCVM_PRIX_BF_V2. Dictionary labels take precedence.
_PRICE_PRODUCT_LABELS = {
    '140': 'Sorgho rouge',       # v2 p6 / v1 p7
    '141': 'Macoroni',          # printed spelling, v2 p6 / v1 p7
    '143': 'Sucre en carreaux', # v2 p15 / v1 p16
}
_PRICE_UNIT_LABELS = {'1161': 'Canari-petit'}  # v2 p17, BOISSONS
COMMUNITY_PRICES_DERIVATION = 'Burkina_Faso::community_prices::quoted-native-unit-count'
_PRICE_INDEX = ['t', 'v', 'j', 'u', 'variety', 'visit', 'obs']
_PRICE_SOURCE = 's05_co_bfa2018.dta'


def _price_labels(categories, name):
    return {tools.format_id(code): str(label)
            for code, label in categories.get(name, {}).items()
            if pd.notna(label) and str(label).strip()}


def _price_base(codes, community_categories, household_categories):
    """Native priced-line identity before slot expansion (ledger1226 §§3–5).

    j follows this country's household Original Label crosswalk by product
    code. Full community unit-size labels supply u; no digit decomposition
    or guessed kg factor is used. variety is a constructed source coordinate,
    NOT an answer about physical variety. Raw codes remain available too.
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
    community_units = _price_labels(community_categories, 's05q02')

    def description(code):
        return community_items.get(code, _PRICE_PRODUCT_LABELS.get(
            code, f'Unknown [Burkina_Faso 2018-19 s05q01={code}]')).strip()

    def item(code):
        raw = household_items.get(code, description(code))
        return str(item_map.get(raw, raw)).strip()

    def sized_unit(code):
        # In particular1001 is NOT inferred to be Kg(Petit). Codes2540/2550
        # keep their own Demi/Entier labels, not an assumed physical mass.
        return community_units.get(code, _PRICE_UNIT_LABELS.get(
            code, f'Unknown [Burkina_Faso 2018-19 s05q02={code}]')).strip()

    return pd.DataFrame({
        't': '2018-19', 'v': v, 'j': product.map(item).astype('string'),
        'u': unit.map(sized_unit).astype('string'),
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
