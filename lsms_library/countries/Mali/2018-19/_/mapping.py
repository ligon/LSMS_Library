import re

import pandas as pd
import numpy as np
import lsms_library.local_tools as tools
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
    df = _food_stock_by_vague(df)
    cope_cols = [c for c in df.columns if c.startswith('Cope')]

    # For 2018-19 binary coping: values are numeric (0=No, 1=Yes)
    # Convert to float for comparison; pick the first 3 strategies with value >= 1
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
