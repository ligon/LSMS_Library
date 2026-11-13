import re

import pandas as pd
import numpy as np
import lsms_library.local_tools as tools
from lsms_library.ehcvm import coping_set_from_grid as _coping_set_from_grid
from lsms_library.ehcvm import (
    derivation_key as _derivation_key,
    food_acquired_with_unit_weights as _food_acquired_with_unit_weights,
    inputs_last_purchase as _inputs_last_purchase,
)

# GH #876.  The wide frame's `Expenditure` used to be `s07bq08`, the value of
# ONE purchase made up to 30 days ago, sitting beside a 7-day consumption
# `Quantity`.  `_food_acquired_ehcvm` derives it instead -- purchased Quantity
# x (s07bq08 / s07bq07a), NA where the last purchase used a different unit --
# and stamps the registry key on every purchased row.  See lsms_library/ehcvm.py
# and Mali/_/derivations.yml.
FOOD_ACQUIRED_DERIVATION = _derivation_key('Mali')

# GH #812.  `strata` is the documented stratum, region x milieu, as a label.  It
# is a construction, so `sample` carries its registry key: see
# lsms_library/ehcvm.py and Mali/_/derivations.yml.
from lsms_library.ehcvm import (  # noqa: E402
    inputs_region_milieu_stratum as _inputs_region_milieu_stratum,
    stamp_region_milieu_stratum as _stamp_region_milieu_stratum,
    strata_label,
)


def j(value):
    '''The food label with its padding gone, so the ``mappings:`` lookup can find it.

    Every ``s07bq01`` label of this wave carries seven leading spaces, and the lookup in
    ``data_info.yml`` is exact, so 2018-19 ``j`` never received ``harmonize_food`` (GH
    #1513).  Runs before that lookup, as ``column_mapping`` chains a function named for
    the level; no other wave's file is padded.  ``value`` is a one-element row there,
    and a scalar on a level with no ``mappings:`` (``assets``).
    '''
    label = value.iloc[0] if isinstance(value, pd.Series) else value
    return label.strip() if isinstance(label, str) else label


def inputs_strata(wave):
    """The registry `inputs` callable: the cover's region and milieu at (t, i)."""
    return _inputs_region_milieu_stratum('Mali', wave)


def sample(df):
    return _stamp_region_milieu_stratum(df, 'Mali')


def inputs_food_acquired(wave):
    """The registry `inputs` callable: raw section-7B answers at (t, i, j).

    A wave-level wrapper rather than a `functools.partial` in the country
    module, so that binding the country name does not put this code in
    Mali/_/mali.py -- which is in EVERY Mali table's cache
    fingerprint.
    """
    return _inputs_last_purchase('Mali', wave)


def food_acquired(df):
    df = _withhold_rice_ration_block(df)   # marks the withheld records; see below
    out = _food_acquired_with_unit_weights(
        df, FOOD_ACQUIRED_DERIVATION, wave='2018-19')
    return out[out.index.get_level_values('j') != _WITHHELD_J]


# ---------------------------------------------------------------------------
# GH #1707: the rice-ration block is not served (ruling @ligon 2026-11-12,
# "Remove, registered")
# ---------------------------------------------------------------------------
#
# s07b_me_mli2018.dta holds 3,357 records with no `vague`.  Every other record
# of the file has one.  All 3,357 are `Riz local Gambiaka` in kg (size-free),
# one per household, answered `Plus de 30 jours` with nothing produced, given,
# or bought (`s07bq04`, `s07bq05`, `s07bq07a`, `s07bq08` all empty), and with
# `s07bq03a` a whole multiple of 1.4 kg: 1.4 x `hhsize` (0.2 kg a person a day
# for 7 days) on 3,348 of them.  No document explains them; the World Bank's
# DDI counts the 3,357 missing `vague` and says nothing.  They are not a
# household's answer, so they are not served.  The rows go; that is a deletion,
# disclosed by the registry entry `Mali::food_acquired::rice-ration-block-withheld`
# (`rows: 0`), whose `inputs` returns them.  See Mali/_/CONTENTS.org.
RICE_RATION_KEY = 'Mali::food_acquired::rice-ration-block-withheld'
#: The block's fingerprint in s07b's own codes: product, consumption unit, size.
_RICE_RATION_CELL = {'s07bq01': 1, 's07bq03b': 100, 's07bq03c': 0}
#: 0.2 kg per person per day, 7 days.
_RICE_RATION_KG_PER_PERSON = 1.4
#: The source file, read in its codes (its labels are padded, GH #1513).
_RICE_RATION_SOURCE = 's07b_me_mli2018.dta'
#: The rice-ration rows' `j` while the shared reshape runs; never served.
_WITHHELD_J = '\x00withheld: GH #1707 rice ration'


class RiceRationBlockError(Exception):
    """s07b no longer looks as the GH #1707 ruling describes it.

    Raised when a record with no ``vague`` does not carry the block's
    fingerprint (see :func:`rice_ration_rows`), so withholding the no-``vague``
    records would drop something the ruling did not see, or when the wide frame
    is not the file row for row.  Subclasses ``Exception``, not ``ValueError``
    or ``RuntimeError``: a derived food table must not read it as a failed build.
    """


def _rice_ration_raw(wave='2018-19'):
    """``s07b_me_mli2018.dta`` in its codes, every record, in file order."""
    if wave != '2018-19':
        raise ValueError(f'{RICE_RATION_KEY} covers 2018-19 only, not {wave!r}')
    from lsms_library.paths import countries_root
    path = countries_root() / 'Mali' / wave / 'Data' / _RICE_RATION_SOURCE
    return tools.get_dataframe(str(path), convert_categoricals=False)


def rice_ration_rows(raw):
    """Boolean ndarray: the records of the rice-ration block (GH #1707).

    ``raw`` is ``s07b_me_mli2018.dta`` read in its codes.  A record is in the
    block when it has no ``vague``.  Every such record must also carry the
    fingerprint the ruling saw -- product 1 (``Riz local Gambiaka``) in kg,
    size-free; nothing produced, given or bought (``s07bq04``, ``s07bq05``,
    ``s07bq07a``, ``s07bq08`` empty or 0); ``s07bq03a`` a whole multiple of
    1.4 kg -- or :class:`RiceRationBlockError` is raised.  The fingerprint
    is checked rather than selected on, so a release that changes the
    block stops the build instead of serving half of it.

    ``hhsize`` is deliberately not read.  1.4 x the welfare file's ``hhsize``
    holds for 3,348 of the 3,357; for the other 9 ``s07bq03a`` is still a whole
    multiple of 1.4, of another integer, and 3 of those households are on no
    cover at all.  On rows WITH a ``vague`` the same 1.4 x ``hhsize`` holds by
    coincidence for 284 rice-kg answers, so it could not select the block on
    its own; the missing ``vague`` is what does.
    """
    def col(name):
        return pd.to_numeric(raw[name], errors='coerce').to_numpy(dtype=float)

    block = np.isnan(col('vague'))
    q = col('s07bq03a')
    with np.errstate(invalid='ignore'):
        multiple = q / _RICE_RATION_KG_PER_PERSON
        fits = ((q > 0) & (np.abs(multiple - np.round(multiple)) < 1e-9)
                & (np.round(multiple) >= 1))
        for name, code in _RICE_RATION_CELL.items():
            fits &= col(name) == code
        for name in ('s07bq04', 's07bq05', 's07bq07a', 's07bq08'):
            fits &= np.nan_to_num(col(name), nan=0.0) == 0
    if (block & ~fits).any():
        raise RiceRationBlockError(
            f'{int((block & ~fits).sum())} of {int(block.sum())} records of '
            f'{_RICE_RATION_SOURCE} with no vague do not carry the GH #1707 '
            f'fingerprint; the ruling does not cover them')
    return block


def _withhold_rice_ration_block(df):
    """The wide frame with the block's ``j`` set to :data:`_WITHHELD_J`.

    The rows cannot leave before the shared hook: its unit-weight join reads the
    source by position and refuses a frame that is not the file row for row
    (``ehcvm._nsu_factor``).  So they are marked here, every row they become
    after the reshape carries the mark, and :func:`food_acquired` drops those.
    A marked row shares no canonical key with a served row, so it moves no
    other row's ``Quantity_kg``.
    """
    raw = _rice_ration_raw()
    served = pd.to_numeric(df['Quantity'], errors='coerce').to_numpy(dtype=float)
    held = pd.to_numeric(raw['s07bq03a'], errors='coerce').to_numpy(dtype=float)
    if len(raw) != len(df) or not np.array_equal(served, held, equal_nan=True):
        raise RiceRationBlockError(
            f'the wide food_acquired frame is not {_RICE_RATION_SOURCE} row for row')
    block = rice_ration_rows(raw)
    if not block.any():
        return df
    idx = df.index.to_frame(index=False)
    j = idx['j'].astype(object).to_numpy(copy=True)
    j[block] = _WITHHELD_J
    try:
        idx['j'] = pd.array(j, dtype=idx['j'].dtype)
    except (TypeError, ValueError):
        idx['j'] = j
    out = df.copy()
    out.index = pd.MultiIndex.from_frame(idx)
    return out


def inputs_rice_ration_block(wave='2018-19'):
    """The registry ``inputs`` callable: the withheld records, at ``(t, i, j)``.

    One row per withheld record, with the file's own variable names and codes
    (``vague`` is NA on every row: that is the block), indexed as the served
    table is (:func:`inputs_food_acquired` supplies the index).  Nothing is
    served from these rows, so they join to no served row; they are what the
    table served before GH #1707, as purchases.
    """
    raw = _rice_ration_raw(wave)
    frame = _inputs_last_purchase('Mali', wave)
    held = pd.to_numeric(raw['s07bq03a'], errors='coerce').to_numpy(dtype=float)
    keyed = pd.to_numeric(frame['s07bq03a'], errors='coerce').to_numpy(dtype=float)
    if len(raw) != len(frame) or not np.array_equal(held, keyed, equal_nan=True):
        raise RiceRationBlockError(
            f'the input frame is not {_RICE_RATION_SOURCE} row for row')
    block = rice_ration_rows(raw)
    columns = ['vague', 's07bq01', 's07bq03a', 's07bq03b', 's07bq03c', 's07bq04',
               's07bq05', 's07bq06', 's07bq07a', 's07bq07b', 's07bq07c', 's07bq08']
    out = raw.loc[block, columns].copy()
    out.index = frame.index[block]
    return out

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


def Sex(value):
    '''
    Map numeric sex codes to canonical M/F.
    Mali s01q01: 1=Masculin, 2=Féminin
    '''
    if value == 1 or value == 'Masculin':
        return 'M'
    if value == 2 or value == 'Féminin':
        return 'F'


def Age(value):
    '''
    Pass Age list [s01q04a, s01q03a, s01q03b, s01q03c] through unchanged.
    s01q03b is integer month (1-12); 9999 sentinels handled by age_handler.
    Override country-level mali.py Age() which expects a scalar.

    The function name ``Age`` also collides with the ``assets`` myvar
    ``Age: s12q07`` (item age in years), which the framework binds to
    this formatter by name (column_mapping in country.py).  That path
    passes a *scalar*, not the roster's DOB Series; pass scalars
    through unchanged so assets extraction does not crash on
    ``list(<float>)`` (closes #321).
    '''
    if not isinstance(value, pd.Series):
        return value
    return list(value)


def interview_date(value):
    '''
    Column-level pass-through for interview_date.
    Overrides mali.py's df-level interview_date(df) which expects a DataFrame.
    The household_roster(df) function handles date parsing via age_handler.
    '''
    return value


_MONTH_MAP = {
    'Janvier': 1, 'F\xe9vrier': 2, 'f': 2, 'Mars': 3, 'Avril': 4, 'Mai': 5,
    'Juin': 6, 'Juillet': 7, 'Ao\xfbt': 8, 'Ao': 8, 'Septembre': 9,
    'Octobre': 10, 'Novembre': 11, 'D\xe9cembre': 12, 'd': 12,
}


# Relationship (s01q02) value labels are TRUNCATED IN THE FILE (GH #801).
# s01_me_mli2018.dta stores ``Chef de m``, ``p``, ``Fr`` and ``Personne
# non apparent`` -- each label cut at its first non-ASCII character, with
# zero non-ASCII bytes left in the label set -- so no ``encoding=`` choice
# can recover them (utf-8, latin-1 and cp1252 all return the same
# strings; measured 2026-09-07).  It is the same producer-side truncation
# that puts ``'f'``, ``'Ao'`` and ``'d'`` in ``_MONTH_MAP`` above.  The
# full labels are fixed by code position in the EHCVM-1 questionnaire and
# identical in every sibling 2018-19 ``s01_me_*`` file (Benin, Burkina
# Faso, CotedIvoire, Niger, Senegal, Togo): 1 = Chef de menage, 4 = Pere,
# Mere, 7 = Frere, soeur, 9 = Personne non apparentee au CM/Conjoint.
# Keys are the strings as they reach the ``household_roster`` hook (after
# mali.py's ``Relationship`` formatter has ``.title()``-cased them) plus
# the raw file spellings, for robustness; values are the title-cased
# clean labels that Mali 2021-22 and Niger / Senegal 2018-19 already
# serve, which resolve in ``kinship.yml`` through existing keys.  Before
# this hook the four artefacts were keys in ``kinship.yml`` (9,890 rows,
# 21.5% of the wave); they are not any more.
_RELATIONSHIP_FIX = {
    'Chef De M': 'Chef De M\u00e9nage',
    'Chef de m': 'Chef De M\u00e9nage',
    'P': 'P\u00e8re, M\u00e8re',
    'p': 'P\u00e8re, M\u00e8re',
    'Fr': 'Fr\u00e8re, S\u0153ur',
    'Personne Non Apparent': 'Personne Non Apparent\u00e9e Au Cm/Conjoint',
    'Personne non apparent': 'Personne Non Apparent\u00e9e Au Cm/Conjoint',
}


def household_roster(df):
    '''
    Compute Age from DOB components when s01q04a is absent, and restore
    the four Relationship labels the source file ships truncated (see
    ``_RELATIONSHIP_FIX``; GH #801).

    Age list in data_info.yml: [s01q04a, s01q03a(day), s01q03b(month), s01q03c(year)].
    s01q03b is a French month string (sometimes truncated IN THE FILE at its first
    accented letter, the same defect as ``_RELATIONSHIP_FIX`` -- not a read-encoding issue);
    converted via _MONTH_MAP. 9999 sentinels in numeric DOB columns are passed through
    (age_handler treats values >= 2100 as invalid).
    No negative age sentinel in s01q04a — it is simply NaN when absent.
    '''
    def _age_from_row(x):
        age_raw = x["Age"][0]
        d_raw = x["Age"][1]
        d_val = None if (pd.notna(d_raw) and float(d_raw) >= 9999) else d_raw
        m_raw = x["Age"][2]
        m_val = _MONTH_MAP.get(str(m_raw)) if pd.notna(m_raw) else None
        y_raw = x["Age"][3]
        y_val = None if (pd.notna(y_raw) and float(y_raw) >= 9999) else y_raw
        try:
            result = tools.age_handler(
                age=age_raw,
                d=d_val, m=m_val, y=y_val,
                interview_date=x["interview_date"],
                interview_year=2018,
            )
        except (ValueError, OverflowError):
            # Invalid DOB combination (e.g. day > days-in-month); fall back to year only
            result = tools.age_handler(
                age=age_raw, y=y_val,
                interview_date=x["interview_date"],
                interview_year=2018,
            )
        return np.nan if (pd.notna(result) and result < 0) else result

    df["Age"] = df.apply(_age_from_row, axis=1)
    df = df.drop('interview_date', axis='columns')
    if 'Relationship' in df.columns:
        # String-only fix: whole-value replacement, never a substring match.
        df['Relationship'] = df['Relationship'].replace(_RELATIONSHIP_FIX)
    return df


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
    return _coping_set_from_grid(df, COPING_LABELS, country="Mali")


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


#: s05q02 = 10 * s07bq03b + s07bq03c: the last digit is the household's size code.
#: Quart / Demi / Entier are defined and unused here, so another digit raises.
_S05_SIZES = {1: 'Petit', 2: 'Moyen', 3: 'Grand'}
_S05_SIZED_LABEL = re.compile(r'^(?P<base>.*\S)\s*-\s*(?P<size>petit|moyen|grand)$',
                              re.IGNORECASE)
#: The community spells four units unlike the household's served base for the
#: same unit code.  One source unit on each side, so these are respellings; the
#: u table's many-to-one rows (Kg, Tas, Boite) are folds and are not applied.
_S05_BASE_RESPELL = {'Alvéoole': 'Alvéole', 'Motte(boule)': 'Boule',
                     'Moude (Moudé)': 'Moude', 'Seau (5 Kg )': 'Seau (5 Kg)'}


def _s05_unit_label(code, label):
    """Served ``u`` of one s05q02 code: the household's sized form (GH #1453).

    ``Tas - Petit`` -> ``Tas (Petit)``, the ``<base> (<Size>)`` of
    ``ehcvm.sized_unit``, so a sized community price joins a household's
    ``food_acquired.u``.  The size comes from the code's DIGIT (Niger's rule:
    ``_s05_unit_label`` in niger.py), and a label whose word disagrees raises.
    A code with no size digit keeps its label verbatim.

    NOT folded: ``Avec os en tas - Petit`` stays ``Avec os en tas (Petit)``, where
    the household's ``u`` table serves ``Tas (Petit)``.  Folding would put two
    source units under one label on 418 (grappe, product, vague) keys.
    """
    if pd.isna(code) or pd.isna(label):
        return pd.NA         # community_prices_s05 refuses a missing key
    code, label = int(code), str(label).strip()
    digit = code % 10
    if digit == 0:
        return label
    if digit not in _S05_SIZES:
        raise ValueError(f's05 unit code {code}: no size for digit {digit}')
    match = _S05_SIZED_LABEL.match(label)
    if match is None or match['size'].title() != _S05_SIZES[digit]:
        raise ValueError(f's05 unit code {code}: {label!r} does not name '
                         f'{_S05_SIZES[digit]}')
    base = match['base'].strip()
    return f'{_S05_BASE_RESPELL.get(base, base)} ({_S05_SIZES[digit]})'


def community_prices_s05(codes, labels):
    """Both reported EHCVM price slots, without selection (GH #821).

    Unlike EAC-I, this file records NO quantity or weight. Quantity is NA,
    never a count of one and never inferred from the unit label. See ledger
    821 §2-5 and CONTENTS.org. No per-kg price is computed.

    The s05 product code retains the source's finer product/variety identity
    when harmonize_food maps several labels to one j. Each unit-size code keeps
    a label of its own, spelled in the household's sized form (GH #1453,
    :func:`_s05_unit_label`). A repeated public source key gets obs 3/4, then
    5/6, in file order (Niger's slot + 2*rank convention), including identical
    repeats. Missing or nonpositive prices remain observations; no EAC-I
    eligibility filter or weight-sentinel rule applies to this different
    instrument.
    """
    if not codes.index.equals(labels.index) or len(codes) != len(labels):
        raise ValueError('s05 codes and labels must have the same row index')
    codes, labels = codes.reset_index(drop=True), labels.reset_index(drop=True)
    description = labels['s05q01'].astype('string').str.strip()
    served_units = pd.Series(
        [_s05_unit_label(code, label) for code, label in zip(codes['s05q02'], labels['s05q02'])],
        index=codes.index)
    pairs = pd.DataFrame({'code': codes['s05q02'], 'u': served_units}).dropna().drop_duplicates()
    if pairs['code'].duplicated().any() or pairs['u'].duplicated().any():
        raise ValueError('s05 unit codes and served labels must be one to one')
    item_map = tools.get_categorical_mapping(
        tablename='harmonize_food', idxvars='Code',
        **{'Preferred Label': 'Preferred Label'})
    base = pd.DataFrame({
        't': '2018-19',
        'v': codes['grappe'].map(tools.format_id).astype('string'),
        'j': description.map(item_map).fillna(description),
        'u': served_units.astype('string'),
        'variety': codes['s05q01'].map(tools.format_id).astype('string'),
        'visit': pd.to_numeric(codes['vague']).astype('Int64'),
        'Description': description,
    })
    keys = ['t', 'v', 'j', 'u', 'variety', 'visit']
    if base[keys].isna().any().any():
        raise ValueError('s05 has a missing observation key; cannot silently discard it')
    rank = base.groupby(keys, sort=False, dropna=False).cumcount()
    pieces = []
    for slot, column in enumerate(('s05q03', 's05q04'), 1):
        piece = base.copy()
        piece['obs'] = slot + 2 * rank
        piece['Price'] = pd.to_numeric(codes[column], errors='raise').astype('Float64')
        piece['Quantity'] = pd.Series(pd.NA, index=piece.index, dtype='Float64')
        pieces.append(piece)
    out = pd.concat(pieces, ignore_index=True)
    # Current map_index recognizes cluster tables (v and j without i).
    # No household identifier or household-ID walk belongs to this source.
    return out.set_index(['t', 'v', 'j', 'u', 'variety', 'visit', 'obs'])[
        ['Price', 'Quantity', 'Description']].sort_index()
