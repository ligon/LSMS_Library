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
# and Benin/_/derivations.yml.
FOOD_ACQUIRED_DERIVATION = _derivation_key('Benin')


def inputs_food_acquired(wave):
    """The registry `inputs` callable: raw section-7B answers at (t, i, j).

    A wave-level wrapper rather than a `functools.partial` in the country
    module, so that binding the country name does not put this code in
    Benin/_/benin.py -- which is in EVERY Benin table's cache
    fingerprint.
    """
    return _inputs_last_purchase('Benin', wave)


def food_acquired(df):
    return _food_acquired_ehcvm(df, FOOD_ACQUIRED_DERIVATION)

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


def Age(value):
    '''
    Convert French month names to integers for the DOB components.

    Benin s01q03b (month) uses French month names (Janvier, Février, ...)
    identical to Senegal 2018-19.  Convert month to int so tools.age_handler()
    can compute age from DOB.

    The list [s01q04a, s01q03a(day), s01q03b(month), s01q03c(year)] is
    passed to household_roster() via tools.age_handler().

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
    result[2] = month_map.get(value.iloc[2])
    return result


def cluster_features(df):
    '''Reduce the broadcast household-level cover page to CLUSTER grain (GH #323).

    ``df_main`` extracts Region/Rural from ``s00_me_ben2018.dta``, which is
    HOUSEHOLD-level (8,012 rows = 8,012 distinct (grappe, menage) pairs over
    670 grappes), but declares only ``v: grappe`` as its index var.  Each
    grappe's attributes are therefore broadcast across all its households,
    handing the framework 8,012 rows on a (t, v) index with 7,342 duplicate
    tuples -- which ``_normalize_dataframe_index`` (GH #323 Site 1) then
    reduces with ``groupby().first()``.

    That reduction is VALUE-LOSSLESS today, so no data has ever been lost here
    and the framework's #323 audit is correctly silent about it: measured
    against the raw source under ``LSMS_NO_CACHE=1``, 0 of the 670 grappes
    carry more than one distinct ``s00q01`` (Region) or ``s00q04`` (Rural),
    and ``grappe_gps_ben2018.dta`` is already grappe-level at 670/670.

    But "lossless today" is a property of the 2018-19 data, not of the
    extraction, and relying on it MASKS the defect class: if a grappe ever
    straddled two regions, ``first()`` would silently pick one -- and, because
    it skips NA per COLUMN, it would in general hand back a composite row
    assembled from different households, a cluster that exists nowhere in the
    survey.  So fix the grain at the source and ENFORCE the invariant rather
    than describing it: assert every cluster carries exactly one distinct
    value per attribute, and RAISE if that ever stops being true, instead of
    quietly choosing a winner.

    This changes no returned value (670 rows before and after); it makes the
    670 correct BY CONSTRUCTION rather than by luck.
    '''
    levels = list(df.index.names)
    spread = df.groupby(level=levels, observed=True).nunique(dropna=False)
    straddling = spread[(spread > 1).any(axis=1)]
    if len(straddling):
        raise ValueError(
            f'Benin cluster_features: {len(straddling)} cluster(s) carry more '
            f'than one distinct value for a cluster attribute, so reducing the '
            f'household-level cover page to cluster grain would have to GUESS. '
            f'Offending clusters (nunique per column):\n{straddling.head(10)}'
        )
    # Exact-duplicate rows by construction (the assertion above proves it), so
    # keeping one row per cluster discards only redundancy.
    return df[~df.index.duplicated(keep='first')]


def household_roster(df):
    '''
    Recover Age from date-of-birth components when s01q04a is null.

    Age list in data_info.yml: [s01q04a, s01q03a(day), s01q03b(month), s01q03c(year)].
    Benin s01q03b uses French month names converted to int by the Age() formatter above.
    DOB columns use true NaN (no sentinel other than -1 for age), so no extra sentinel mask needed.

    The ``Frère, sœur`` Relationship label this cp1252 file writes is
    decoded by the reader since GH #805 (``get_dataframe`` re-decodes a
    pre-118 file's cp1252 strings); the wave-level ``_decode_cp1252`` that
    GH #801 put here is retired.
    '''
    def _age_from_row(x):
        age_raw = x["Age"][0]
        # -1 is a sentinel for missing age; pass None so age_handler falls through to DOB columns
        age_val = None if (pd.notna(age_raw) and int(float(age_raw)) < 0) else age_raw
        result = tools.age_handler(age=age_val, d=x["Age"][1], m=x["Age"][2], y=x["Age"][3],
                                   interview_date=x["interview_date"], interview_year=2018)
        # Null out any negative result (dob slightly after interview date)
        return np.nan if (pd.notna(result) and result < 0) else result

    df["Age"] = df.apply(_age_from_row, axis=1)
    df = df.drop('interview_date', axis='columns')
    return df


_FIES_ITEMS = ['Worried', 'HealthyDiet', 'FewFoods', 'SkippedMeal',
               'AteLess', 'RanOut', 'Hungry', 'WholeDay']


def food_security(df):
    '''Compute FIES_score from the 8 FAO FIES experience items.

    EHCVM §8A items s08aq01..s08aq08 have already been mapped to the 8
    canonical bool columns by the YAML ``mapping:`` blocks (Oui->True,
    Non->False, everything else->NaN).  FIES_score is the row-wise count
    of True across those 8 items.  Per the canonical contract it is NaN
    only when ALL 8 items are NaN; otherwise NaN items count as 0 (not
    affirmative).
    '''
    items = df[_FIES_ITEMS].apply(lambda c: c.map({True: 1, False: 0}))
    score = items.sum(axis=1, skipna=True)
    all_na = items.isna().all(axis=1)
    score = score.where(~all_na, np.nan)
    df = df.copy()
    df['FIES_score'] = score.astype('Int64')
    return df


class ShockLayoutError(Exception):
    """Section 14.04 differs from the documented five/six-column layout.

    Not a ValueError/KeyError/RuntimeError: build fallbacks must not hide it.
    """


def _food_effects_by_vague(df):
    """Select stock and purchases per row, preserving mapped missing answers.

    Vague 1: d = stock, e = purchases, f blank.  Vague 2: d = livestock,
    e = stock, f = purchases (GH #1561).  The raw f sentinel checks this
    layout independently of the answer-to-boolean mapping, as in the core
    EHCVM 2018-19 hooks.  The same e is purchases in vague 1 and stock in 2.
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
    df = _food_effects_by_vague(df)
    # Benin 2018-19 records at most 2 coping strategies per household.
    # (A probe over the full survey shows no household with a third
    # strategy flagged; HowCoped2 would be all-NaN and is therefore
    # not declared in data_scheme.yml.)
    cope_cols = [c for c in df.columns if c.startswith('Cope')]

    how_coped = {0: [], 1: []}
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
            if len(found) == 2:
                break
        for k in range(2):
            how_coped[k].append(found[k] if k < len(found) else np.nan)

    df['HowCoped0'] = how_coped[0]
    df['HowCoped1'] = how_coped[1]
    df = df.drop(columns=cope_cols)
    return df



# ---------------------------------------------------------------------------
# community_prices: EHCVM section 5, s05_co_ben2018.dta (GH #1226)
# ---------------------------------------------------------------------------
#
# The Niger template (rulings D1-D6, 2026-10-22; Niger/_/niger.py _s05_*), with
# the household's sized unit form and no folding (#1453 ruling, 2026-11-04).
# One copy per country, so that each country's wiring can be taken alone.  #1226
# asks for them in lsms_library/ehcvm.py; that move is the residual.
COMMUNITY_PRICES_DERIVATION = 'Benin::community_prices::one-item-per-price'
_S05_SOURCE = 's05_co_ben2018.dta'
_S05_HOUSEHOLD = 's07b_me_ben2018.dta'
_S05_INDEX = ['t', 'v', 'j', 'u', 'variety', 'visit', 'obs']

#: Unit-size codes whose digits name a different household unit from the one
#: the instrument prints for them, mapped to the unit the instrument names.
#: Benin's CAPI price form (UEMOA catalogue 44, /download/123 and /124) prints
#: `1050: Bidon-Unité de taille unique` although household unit 105 is Bassine,
#: and all 44 rows of 1050 are vinegar; it prints `1330: Sac (10 Kg)-...` (and
#: 1340 under both spellings) although household unit 133 is a bare Sac.  What
#: the enumerator saw on the screen is the unit that was priced.
_S05_FORM_UNIT = {1050: 106, 1330: 134}


def _s05_labels(categories, name):
    return {int(code): str(label).strip() for code, label in categories.get(name, {}).items()
            if pd.notna(label) and str(label).strip()}


def _s05_unit_size(code, hh_units):
    """``(unit, size)`` of one ``s05q02`` unit-size code.

    ``code = 10 * unit + size``, except that the two-digit sizes 98 and 99
    (`Autre taille`) make it ``100 * unit + size``: 13198 is Pot, size 98.
    """
    code = int(code)
    unit, size = divmod(code, 10)
    if unit not in hh_units:
        unit, size = divmod(code, 100)
    if unit not in hh_units:
        raise UnitSizeLabelError(f"Benin s05: unit-size code {code} names no household unit")
    return _S05_FORM_UNIT.get(code, unit), size


def _s05_u_by_code(unit_codes, community, household):
    """``{code: u}``: the household's served label for the code's unit and size.

    The same composition ``ehcvm.carry_unit_size`` gives ``food_acquired.u``
    (the read-time ``u`` table, then ``ehcvm.sized_unit``), so a price joins the
    household row in the same unit and size.  A size the household labels do not
    list (99, `Autre taille 2`) takes the form's own word.  Unlike the household
    side an exact metric unit keeps its size (1014 `Litre (Quart)`): a quarter
    litre is not a litre.  No two units or sizes may share a label: that would
    be a fold, and it raises.
    """
    hh_units = _s05_labels(household, 's07bq03b')
    hh_sizes = _s05_labels(household, 's07bq03c')
    s05_units = _s05_labels(community, 's05q02')
    parts = {int(c): _s05_unit_size(c, hh_units) for c in unit_codes}
    raw = sorted({hh_units[unit] for unit, _ in parts.values()})
    served = dict(zip(raw, _served_unit_labels('Benin', raw)))
    out, source = {}, {}
    for code, (unit, size) in parts.items():
        word = hh_sizes.get(size) or s05_units[code].rsplit('-', 1)[-1]
        label = _sized_unit(served[hh_units[unit]], word)
        if source.setdefault(label, (unit, size)) != (unit, size):
            raise UnitSizeLabelError(f"Benin s05: {source[label]} and {(unit, size)} "
                                     f"would both be served {label!r}")
        out[code] = label
    moved = [(a, b) for a, b in zip(out.values(), _served_unit_labels('Benin', list(out.values())))
             if a != b]
    if moved:
        raise UnitSizeLabelError(f"Benin s05: the read-time u table would relabel {moved[:5]}")
    return out


def _s05_base(codes, community, household):
    """One row per source record: its served keys and ``Description``, by CODE.

    ``j`` is the household's ``s07bq01`` label of the same product code through
    ``harmonize_food``; ``Description`` keeps the s05 label as recorded.  Where
    ``harmonize_food`` lumps several codes into one ``j`` (Riz local parfumé and
    Riz local Nérica into Autre Riz local) the code is the ``variety``, the
    canonical definition of that level (``data_info.yml``, GH #840; Tanzania's
    item codes).  Elsewhere the form names nothing finer than ``j``:
    ``unknown_variety``, as Niger.
    """
    from lsms_library.paths import countries_root
    path = countries_root() / 'Benin'
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
        raise ValueError('Benin s05: two recorded prices share a served key')
    return out


def derive_s05_one_item_per_price(df):
    """``Quantity = 1`` on every s05 price row, stamped with the registry key.

    The instrument prices ONE item of the selected unit-size and asks no
    quantity (D2), so the 1 is a construction of the form, not an answer; see
    ``Benin/_/derivations.yml``.  Takes the finished frame and returns a copy.
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
        raise ValueError('Benin community prices exist only for 2018-19')
    from lsms_library.paths import countries_root
    root = countries_root() / 'Benin' / wave / 'Data'
    codes = tools.get_dataframe(root / _S05_SOURCE, convert_categoricals=False).reset_index(drop=True)
    community = tools.get_dataframe(root / _S05_SOURCE, categories_only=True)
    household = tools.get_dataframe(root / _S05_HOUSEHOLD, categories_only=True)
    base = _s05_base(codes, community, household).drop(columns='Description')
    out = pd.concat([base, codes], axis=1)
    return out.set_index(list(base.columns))
