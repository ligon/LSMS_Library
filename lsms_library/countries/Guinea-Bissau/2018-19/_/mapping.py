# Formatting Functions for Guinea-Bissau 2018-19
import pandas as pd
import numpy as np
import lsms_library.local_tools as tools
from lsms_library.transformations import food_acquired_to_canonical as _food_acquired_canonical

from lsms_library.ehcvm import (
    UnitSizeLabelError,
    derivation_key as _derivation_key,
    food_acquired_ehcvm as _food_acquired_ehcvm,
    inputs_last_purchase as _inputs_last_purchase,
    inputs_region_milieu_stratum as _inputs_region_milieu_stratum,
    stamp_region_milieu_stratum as _stamp_region_milieu_stratum,
    strata_label,
    served_unit_labels as _served_unit_labels,
    sized_unit as _sized_unit,
)

# GH #876.  The wide frame's `Expenditure` used to be `s07bq08`, the value of
# ONE purchase made up to 30 days ago, sitting beside a 7-day consumption
# `Quantity`.  `_food_acquired_ehcvm` derives it instead -- purchased Quantity
# x (s07bq08 / s07bq07a), NA where the last purchase used a different unit --
# and stamps the registry key on every purchased row.  See lsms_library/ehcvm.py
# and Guinea-Bissau/_/derivations.yml.
FOOD_ACQUIRED_DERIVATION = _derivation_key('Guinea-Bissau')


def inputs_food_acquired(wave):
    """The registry `inputs` callable: raw section-7B answers at (t, i, j).

    A wave-level wrapper rather than a `functools.partial` in the country
    module, so that binding the country name does not put this code in
    Guinea-Bissau/_/guinea-bissau.py -- which is in EVERY Guinea-Bissau table's cache
    fingerprint.
    """
    return _inputs_last_purchase('Guinea-Bissau', wave)


# GH #1575; shared construction and provenance (task ledger §§2–5).
def inputs_strata(wave):
    """The original cover-page region/milieu labels at (t, i)."""
    return _inputs_region_milieu_stratum('Guinea-Bissau', wave)


def sample(df):
    return _stamp_region_milieu_stratum(df, 'Guinea-Bissau')


def df_geo(df):
    """``cluster_features.df_geo`` sub-frame hook — GH #323 site 4 / GH #627.

    **This is a SUB-FRAME hook, not a table hook.**  ``Wave.grab_data``
    resolves a ``dfs:`` sub-frame's ``df_edit`` by calling
    ``column_mapping(<sub-frame name>, ...)``, so a module-level function
    named exactly after the sub-frame key (``df_geo``) runs on that
    sub-frame **after extraction and before the merge**.  That "before the
    merge" is the whole point; see below.

    ``grappe_gps_gnb2018.dta`` ships **450 rows for 445 grappes**: five
    grappes (155325, 155426, 199719, 199816, 1771231) appear TWICE, as
    VERBATIM duplicate records — same latitude, same longitude, same
    accuracy, same altitude, same GPS timestamp.  The companion community
    cover ``s00a_co_gnb2018.dta`` carries the identical 450/445 split, so
    the duplication happened once, on the community side of the export.

    That alone made the ``cluster_features`` merge a CARTESIAN PRODUCT.
    The other sub-frame (``s00_me_gnb2018.dta``, the household cover page)
    is HOUSEHOLD grain — ~12 rows per grappe — so for those five grappes
    the merge on ``v`` paired 11-12 household rows with 2 GPS rows and
    emitted 22-24 where an honest join emits 11-12: **5,351 -> 5,410 rows,
    59 of them phantom** (PR #627's 40-country census).

    The cure is the second remedy the #627 guard itself names — *"reduce a
    sub-frame to the merge-key grain BEFORE it is merged"*.  It is
    deliberately NOT the first one (*"merge on a key unique in at least one
    sub-frame"*): Guinea-Bissau has no such key.  The GPS file is at grappe
    grain and carries nothing finer (only ``grappe`` and ``vague``, and both
    copies of every duplicate agree on ``vague``), while the cover page is
    at household grain, so ``v`` is non-unique on *both* sides and no
    declarable key separates the two copies.

    ``drop_duplicates()`` over EVERY column — key and payload alike — is
    used on purpose, and it is de-duplication, not aggregation (#323 D1
    forbids the latter).  It removes a row only when an identical row
    already exists, so it cannot choose between disagreeing records: if a
    future export ever ships two GPS fixes for one grappe that actually
    DIFFER, both survive, the merge is cartesian again, and #627's guard
    fires.  A ``groupby().first()`` or ``keep='first'`` would silently pick
    one and hide exactly that.

    Unrelated to the cardinality and NOT fixed here: the household cover
    page names 450 grappes but the GPS file only 445, so five grappes carry
    NaN coordinates.  Missing data, not manufactured data — and note the
    two counts coincide only by arithmetic accident: the five duplicated
    records are byte-identical, so they are not five mislabelled fixes that
    could be reassigned to the five grappes that lack one.
    """
    names = list(df.index.names)
    flat = df.reset_index()
    deduped = flat.drop_duplicates()
    return deduped.set_index(names)


# Lossy-substitution prefixes the source data carries for two specific
# Portuguese words.  Upstream of pyreadstat, the export pipeline
# replaced the second byte of certain UTF-8 codepoints with literal
# ``__`` (0x5f 0x5f) -- so ``Óleo`` (UTF-8: 0xc3 0x93 + 'leo') becomes
# ``Ã__leo`` and ``Água`` (UTF-8: 0xc3 0x81 + 'gua') becomes
# ``Ã__gua``.  No byte-level decoding can recover these (the original
# byte is gone), so we substitute the canonical Portuguese form
# directly when we see the prefix.  Two known cases drive this:
# food items ``Óleo de mancarra``, ``Óleo de soja``, ``Óleo de palma``,
# and ``Água filtrada`` (4 of 135 distinct ``j`` values in
# food_acquired 2018-19).
_LOST_PREFIX_REPLACEMENTS = {
    'Ã__leo': 'Óleo',
    'Ã__gua': 'Água',
}


def _decode_mojibake(s):
    """Reverse the UTF-8-as-Latin-1-as-UTF-8 double-encoding that pyreadstat
    produces for Guinea-Bissau's 2018-19 .dta value labels.

    The .dta file's value-labels block stores Portuguese strings as raw
    UTF-8 bytes (e.g. ``Pedaço`` = ``b'\\xc3\\xa7'`` for the ``ç``), but
    the file metadata declares Latin-1 encoding.  pyreadstat respects
    the metadata, decoding the UTF-8 bytes as Latin-1 and re-encoding
    them as Python ``str`` -- producing ``PedaÃ§o`` instead of ``Pedaço``.
    None of pyreadstat's ``encoding=`` options correct this (verified
    2026-05-07: ``utf-8`` raises, ``cp1252`` / ``iso-8859-1`` give the
    same mojibake).  The fix is post-decode: encode the mojibake string
    back to Latin-1 bytes (recovers the original UTF-8 byte sequence),
    then decode as UTF-8.

    Two j-values in food_acquired 2018-19 carry an additional lossy
    substitution upstream of pyreadstat where the second byte of
    certain UTF-8 codepoints was replaced with literal ``__`` (i.e.
    ``Óleo`` -> ``Ã__leo``, ``Água`` -> ``Ã__gua``).  Those can't be
    recovered byte-wise, so we handle them via
    ``_LOST_PREFIX_REPLACEMENTS`` above.

    Strings without non-ASCII characters round-trip cleanly through
    ``encode('latin-1').decode('utf-8')`` (every byte 0x00-0x7F is
    valid in both encodings and identical).  So this is safe to apply
    to all string values; only the mojibake-bearing ones change.

    Returns the input unchanged on any error or when ``s`` isn't a
    string -- best-effort, never raises.
    """
    if not isinstance(s, str):
        return s
    for bad, good in _LOST_PREFIX_REPLACEMENTS.items():
        if s.startswith(bad):
            return good + s[len(bad):]
    try:
        return s.encode('latin-1').decode('utf-8')
    except (UnicodeEncodeError, UnicodeDecodeError):
        return s


def _decode_mojibake_in_df(df):
    """Apply ``_decode_mojibake`` to every string column and string
    index level in-place.  Used by per-feature ``df_edit`` hooks below
    to fix value-label mojibake that affects Portuguese strings in
    multiple columns at once (food items, units, etc.) -- 40 of 135
    food items and 4 of 29 units carry it before this fix.
    """
    # Index levels
    new_levels = []
    new_names = list(df.index.names)
    changed = False
    for i, name in enumerate(df.index.names):
        level = df.index.get_level_values(i)
        if level.dtype == object or pd.api.types.is_string_dtype(level):
            mapped = level.map(_decode_mojibake)
            if not mapped.equals(level):
                changed = True
            new_levels.append(mapped)
        else:
            new_levels.append(level)
    if changed:
        df = df.copy()
        df.index = pd.MultiIndex.from_arrays(new_levels, names=new_names) \
            if len(new_levels) > 1 else new_levels[0]
    # String columns
    for col in df.columns:
        if df[col].dtype == object or pd.api.types.is_string_dtype(df[col]):
            df[col] = df[col].map(_decode_mojibake)
    return df


def food_acquired(df):
    """``food_acquired`` post-processor: canonical reshape + mojibake fix.

    Wraps ``transformations.food_acquired_to_canonical`` (the Phase 3
    s-axis reshape) and then sweeps mojibake out of every string
    column / index level.  See ``_decode_mojibake`` for the encoding
    rationale.
    """
    # GH #1224: `decode` -- the served base unit of a sized row is read
    # through the same repair the sweep below applies to every other row.
    df = _food_acquired_ehcvm(df, FOOD_ACQUIRED_DERIVATION,
                              decode=_decode_mojibake)
    df = _decode_mojibake_in_df(df)
    return df

def Sex(value):
    '''
    Map Portuguese sex labels from s01_me_gnb2018.dta to canonical M/F.
    Stata labels: 1=Masculino, 2=Feminino.
    '''
    if value in ('Masculino', '1'):
        return 'M'
    if value in ('Feminino', '2'):
        return 'F'
    return pd.NA


# Portuguese month names used in s01q03b Stata labels.
_MONTH_MAP_PT = {
    'Janeiro': 1, 'Fevereiro': 2, 'Março': 3, 'Marco': 3, 'Abril': 4,
    'Maio': 5, 'Junho': 6, 'Julho': 7, 'Agosto': 8, 'Setembro': 9,
    'Outubro': 10, 'Novembro': 11, 'Dezembro': 12,
}


def Age(value):
    '''
    Convert DOB list [s01q04a, s01q03a, s01q03b, s01q03c] so that month
    (index 2) is a plain integer 1-12.

    s01q03b Stata labels are Portuguese month names (Janeiro…Dezembro).
    get_dataframe() returns the label strings as categoricals.
    The sentinel 9999 is left as-is; household_roster() handles it.

    The function name ``Age`` collides with the ``assets`` myvar
    ``Age: s12q07`` (item age in years), which the framework binds to
    this formatter by name (column_mapping in country.py).  That path
    passes a *scalar*, not the roster's DOB Series; pass scalars
    through unchanged so assets extraction does not crash on
    ``list(<float>)`` (closes #321).
    '''
    if not isinstance(value, pd.Series):
        return value
    result = list(value)
    raw_month = value.iloc[2]
    if isinstance(raw_month, str):
        result[2] = _MONTH_MAP_PT.get(raw_month)
    # if already numeric (int/float), keep as-is; household_roster will
    # null it out if it equals 9999
    return result


def household_roster(df):
    '''
    Recover Age from date-of-birth components (s01q03a/b/c) when s01q04a
    is missing or 9999.

    Age list in data_info.yml: [s01q04a, s01q03a(day), s01q03b(month), s01q03c(year)].
    The Age() formatter above has already converted month to int 1-12 (or None).
    The 9999 sentinel marks unknown age / DOB components.
    '''
    def _age_from_row(x):
        age_raw = x["Age"][0]
        # 9999 is the sentinel for unknown age; treat as missing
        age_val = None if (pd.notna(age_raw) and int(float(age_raw)) == 9999) else age_raw
        # day sentinel
        raw_day = x["Age"][1]
        day = None if (pd.notna(raw_day) and int(float(raw_day)) == 9999) else raw_day
        # month — Age() may have returned None for unrecognised strings
        month = x["Age"][2]
        result = tools.age_handler(age=age_val, d=day, m=month, y=x["Age"][3],
                                   interview_date=x["interview_date"], interview_year=2018)
        return np.nan if (pd.notna(result) and result < 0) else result

    df["Age"] = df.apply(_age_from_row, axis=1)
    df = df.drop('interview_date', axis='columns')
    return df


# FAO FIES 8 experience items, in canonical order (s08aq01..s08aq08).
_FIES_ITEMS = ['Worried', 'HealthyDiet', 'FewFoods', 'SkippedMeal',
               'AteLess', 'RanOut', 'Hungry', 'WholeDay']

# Yes/No labels for the FIES items.  Guinea-Bissau's s08a value labels are
# Portuguese (Sim/Não); French Oui/Non included defensively for any
# differently-localized export.  Recusa (refused) / Não sabe (don't know)
# and anything else fall through to pd.NA.
_FIES_YES = {'Sim', 'Oui', 'sim', 'oui'}
_FIES_NO = {'Não', 'Nao', 'Non', 'não', 'nao', 'non'}


def _fies_bool(value):
    if value in _FIES_YES:
        return True
    if value in _FIES_NO:
        return False
    return pd.NA


def food_security(df):
    '''FIES post-processor: map the 8 Portuguese Yes/No items to bool and
    add FIES_score (count of True; NaN only when all 8 items are NaN).

    s08aq01..s08aq08 arrive renamed to the canonical FAO names via
    data_info.yml.  Sim->True, Não->False, Recusa/Não sabe/NaN->NA.
    '''
    for col in _FIES_ITEMS:
        df[col] = df[col].map(_fies_bool).astype('boolean')

    items = df[_FIES_ITEMS]
    score = items.sum(axis=1, skipna=True)          # True counts as 1
    all_na = items.isna().all(axis=1)               # no item answered
    score = score.where(~all_na, other=pd.NA).astype('Int64')
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



# ---------------------------------------------------------------------------
# community_prices: EHCVM section 5, s05_co_gnb2018.dta (GH #1226)
# ---------------------------------------------------------------------------
#
# The Niger template (rulings D1-D6, 2026-10-22; Niger/_/niger.py _s05_*), with
# the household's sized unit form and no folding (#1453 ruling, 2026-11-04).
# One copy per country, so that each country's wiring can be taken alone.  #1226
# asks for them in lsms_library/ehcvm.py; that move is the residual.
COMMUNITY_PRICES_DERIVATION = 'Guinea-Bissau::community_prices::one-item-per-price'
_S05_SOURCE = 's05_co_gnb2018.dta'
_S05_HOUSEHOLD = 's07b_me_gnb2018.dta'
_S05_INDEX = ['t', 'v', 'j', 'u', 'variety', 'visit', 'obs']

#: s05 product code -> the household s07bq01 code whose label it is served under.
#: GH #1226, ruled by the maintainer 2026-11-04: "Bridge Guinea-Bissau product 94
#: (`Feijão seco`) to the household's 95 (`Feijão-frade, feijão seco`)".  94 is on the
#: price form only, 95 in the household file only (s05 holds 348 records of 94 and
#: none of 95), so the bridge lumps no two priced codes and `variety` stays
#: `unknown_variety`.  `Description` keeps the form's own `Feijão seco`.
_S05_PRODUCT_BRIDGE = {94: 95}


def _s05_labels(categories, name):
    return {int(code): str(label).strip() for code, label in categories.get(name, {}).items()
            if pd.notna(label) and str(label).strip()}


def _s05_unit_size(code, hh_units):
    """``(unit, size)`` of one ``s05q02`` unit-size code.

    ``code = 10 * unit + size``, except that the two-digit sizes 98 and 99
    (`Outro tamanho`) make it ``100 * unit + size``: 41398 is Monte, size 98.
    """
    code = int(code)
    unit, size = divmod(code, 10)
    if unit not in hh_units:
        unit, size = divmod(code, 100)
    if unit not in hh_units:
        raise UnitSizeLabelError(f"Guinea-Bissau s05: unit-size code {code} names no household unit")
    return unit, size


def _s05_u_by_code(unit_codes, community, household):
    """``{code: u}``: the household's served label for the code's unit and size.

    The same composition ``ehcvm.carry_unit_size`` gives ``food_acquired.u``
    (the mojibake repair, the read-time ``u`` table, then ``ehcvm.sized_unit``),
    so a price joins the household row in the same unit and size, in the
    instrument's Portuguese: ``Monte (Medio)``.  A size the household labels do
    not list (98 / 99, `Outro tamanho 1 / 2`) takes the form's own word.  No two
    units or sizes may share a label: that would be a fold, and it raises.
    """
    hh_units = _s05_labels(household, 's07bq03b')
    hh_sizes = _s05_labels(household, 's07bq03c')
    s05_units = _s05_labels(community, 's05q02')
    parts = {int(c): _s05_unit_size(c, hh_units) for c in unit_codes}
    raw = sorted({_decode_mojibake(hh_units[unit]).strip() for unit, _ in parts.values()})
    served = dict(zip(raw, _served_unit_labels('Guinea-Bissau', raw)))
    out, source = {}, {}
    for code, (unit, size) in parts.items():
        word = hh_sizes.get(size) or s05_units[code].rsplit('-', 1)[-1]
        label = _sized_unit(served[_decode_mojibake(hh_units[unit]).strip()],
                            _decode_mojibake(word).strip())
        if source.setdefault(label, (unit, size)) != (unit, size):
            raise UnitSizeLabelError(f"Guinea-Bissau s05: {source[label]} and {(unit, size)} "
                                     f"would both be served {label!r}")
        out[code] = label
    moved = [(a, b) for a, b in zip(out.values(),
                                    _served_unit_labels('Guinea-Bissau', list(out.values())))
             if a != b]
    if moved:
        raise UnitSizeLabelError(f"Guinea-Bissau s05: the read-time u table would relabel {moved[:5]}")
    return out


def _s05_base(codes, community, household):
    """One row per source record: its served keys and ``Description``, by CODE.

    ``j`` is the household's ``s07bq01`` label of the same product code through
    ``harmonize_food`` and the mojibake repair.  Product 94 (`Feijão seco`) is
    priced but has no household label; it takes household code 95's
    (:data:`_S05_PRODUCT_BRIDGE`, GH #1226).  A code with no household label
    and no bridge would take its own.  ``Description`` is
    the s05 label with the mojibake repaired, as every other table of this wave
    serves its labels.  The form names nothing finer than ``j``, so ``variety``
    is the canonical ``unknown_variety``, unless ``harmonize_food`` lumps two
    priced codes into one ``j`` (none today), where the code is the ``variety``
    (``data_info.yml``, GH #840).
    """
    from lsms_library.paths import countries_root
    path = countries_root() / 'Guinea-Bissau'
    item_map = tools.get_categorical_mapping(
        tablename='harmonize_food', idxvars='Original Label',
        dirs=[str(path / '2018-19' / '_'), str(path / '_')],
        **{'Preferred Label': 'Preferred Label'})
    hh_items = _s05_labels(household, 's07bq01')
    s05_items = _s05_labels(community, 's05q01')
    product = codes['s05q01'].astype(int)
    unit = codes['s05q02'].astype(int)

    def item(code):
        raw = hh_items.get(_S05_PRODUCT_BRIDGE.get(code, code), s05_items[code])
        return _decode_mojibake(item_map.get(raw, raw)).strip()
    j_by_code = {c: item(c) for c in product.unique()}
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
        'Description': product.map(lambda c: _decode_mojibake(s05_items[c])).astype('string'),
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
        raise ValueError('Guinea-Bissau s05: two recorded prices share a served key')
    return out


def derive_s05_one_item_per_price(df):
    """``Quantity = 1`` on every s05 price row, stamped with the registry key.

    The instrument prices ONE item of the selected unit-size and asks no
    quantity (D2), so the 1 is a construction of the form, not an answer; see
    ``Guinea-Bissau/_/derivations.yml``.  Takes the finished frame and returns a
    copy.
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
        raise ValueError('Guinea-Bissau community prices exist only for 2018-19')
    from lsms_library.paths import countries_root
    root = countries_root() / 'Guinea-Bissau' / wave / 'Data'
    codes = tools.get_dataframe(root / _S05_SOURCE, convert_categoricals=False).reset_index(drop=True)
    community = tools.get_dataframe(root / _S05_SOURCE, categories_only=True)
    household = tools.get_dataframe(root / _S05_HOUSEHOLD, categories_only=True)
    base = _s05_base(codes, community, household).drop(columns='Description')
    out = pd.concat([base, codes], axis=1)
    return out.set_index(list(base.columns))
