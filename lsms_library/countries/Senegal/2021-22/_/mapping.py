# Formatting  Functions for Senegal 2021-22
import pandas as pd
import numpy as np
import lsms_library.local_tools as tools
from lsms_library.ehcvm import (
    derivation_key as _derivation_key,
    food_acquired_with_unit_weights as _food_acquired_with_unit_weights,
    inputs_unit_weight as _inputs_unit_weight,
    inputs_last_purchase as _inputs_last_purchase,
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
    stamp_region_milieu_stratum as _stamp_region_milieu_stratum,
    strata_label,
)


def sample(df):
    return _stamp_region_milieu_stratum(df, 'Senegal')

# GH #1452: `Quantity_kg` from the survey's own unit-weight table, national median;
# which file and column, and why: lsms_library.ehcvm.NSU_TABLES.


def inputs_food_acquired(wave):
    """The registry `inputs` callable: raw section-7B answers at (t, i, j).

    A wave-level wrapper rather than a `functools.partial` in the country
    module, so that binding the country name does not put this code in
    Senegal/_/senegal.py -- which is in EVERY Senegal table's cache
    fingerprint.
    """
    return _inputs_last_purchase('Senegal', wave)


def inputs_food_acquired_unit_weight(wave):
    """The registry `inputs` callable of the unit-weight derivation (GH #1452)."""
    return _inputs_unit_weight('Senegal', wave)


def food_acquired(df):
    marked = _withhold_salt_per_capita_rows(df)
    out = _food_acquired_with_unit_weights(
        marked, FOOD_ACQUIRED_DERIVATION, wave='2021-22')
    return out[out.index.get_level_values('j') != _WITHHELD_J]


# ---------------------------------------------------------------------------
# Salt in kg at a fixed amount per person is not served (ruling @ligon
# 2026-11-12, "Remove, registered")
# ---------------------------------------------------------------------------
#
# 100 records of s07b_me_sen2021.dta, all product 139 (`Sel`) in kg, size-free,
# have s07bq03a = 0.0199997 kg x the household's `hhsize` (ehcvm_welfare_sen2021),
# to within 1e-12 on every one; 34% of the wave's 294 salt-in-kg records.  No other
# record of any product or unit meets that constant.  It reads as "0.02 kg a person"
# rounded, which is how the 2026-11-12 data check reported it; no record equals
# 0.02 x hhsize.  No document explains them.  The source records go, every row they
# become with them; a deletion, disclosed by
# `Senegal::food_acquired::salt-per-capita-withheld` (`rows: 0`).
SALT_PER_CAPITA_KEY = 'Senegal::food_acquired::salt-per-capita-withheld'
#: The fixed amount, kg per household member, as the file holds it.
SALT_KG_PER_PERSON = 0.0199997
#: The records' cell in s07b's own codes: product, consumption unit, size.
_SALT_CELL = {'s07bq01': 139, 's07bq03b': 100, 's07bq03c': 0}
_SALT_TOLERANCE = 1e-9
_SALT_SOURCE = 's07b_me_sen2021.dta'
#: Declared in data_info.yml (`household_size:`), so its sidecar is hashed.
_SALT_HOUSEHOLD_SIZE = 'ehcvm_welfare_sen2021.dta'
#: The withheld rows' `j` while the shared reshape runs; never served.
_WITHHELD_J = '\x00withheld: salt per capita'


class SaltPerCapitaJoinError(Exception):
    """The wide frame or the household-size file is not what the rule reads.

    Subclasses ``Exception``: a derived food table must not read it as a failed
    build (GH #1546)."""


def _salt_raw(wave='2021-22'):
    """``s07b`` in its codes, with each record's welfare ``hhsize`` beside it."""
    if wave != '2021-22':
        raise ValueError(f'{SALT_PER_CAPITA_KEY} covers 2021-22 only, not {wave!r}')
    from lsms_library.paths import countries_root
    data = countries_root() / 'Senegal' / wave / 'Data'
    raw = tools.get_dataframe(str(data / _SALT_SOURCE), convert_categoricals=False)
    welfare = tools.get_dataframe(str(data / _SALT_HOUSEHOLD_SIZE), convert_categoricals=False)

    def key(frame):
        return pd.MultiIndex.from_arrays(
            [pd.to_numeric(frame[c], errors='coerce') for c in ('grappe', 'menage')])

    size = pd.Series(pd.to_numeric(welfare['hhsize'], errors='coerce').to_numpy(),
                     index=key(welfare))
    if size.index.duplicated().any():
        raise SaltPerCapitaJoinError(f'{_SALT_HOUSEHOLD_SIZE} repeats a (grappe, menage)')
    raw = raw.copy()
    raw['hhsize'] = size.reindex(key(raw)).to_numpy()
    return raw


def salt_per_capita_rows(raw):
    """Boolean ndarray: salt in kg at 0.0199997 kg x ``hhsize`` (to 1e-9).

    ``raw`` is s07b in its codes with the welfare ``hhsize`` per record
    (:func:`_salt_raw`).  A record with no ``hhsize`` is never selected.
    """
    def col(name):
        return pd.to_numeric(raw[name], errors='coerce').to_numpy(dtype=float)

    q = col('s07bq03a')
    with np.errstate(invalid='ignore'):
        hit = (q > 0) & (np.abs(q - SALT_KG_PER_PERSON * col('hhsize')) < _SALT_TOLERANCE)
        for name, code in _SALT_CELL.items():
            hit &= col(name) == code
    return hit


def _withhold_salt_per_capita_rows(df):
    """The wide frame with the withheld records' ``j`` set to :data:`_WITHHELD_J`.

    Marked, not dropped: the shared hook's unit-weight join reads the source by
    position and refuses a frame that is not the file row for row.  Every row a
    marked record becomes carries the mark, and :func:`food_acquired` drops them.
    """
    raw = _salt_raw()
    served = pd.to_numeric(df['Quantity'], errors='coerce').to_numpy(dtype=float)
    held = pd.to_numeric(raw['s07bq03a'], errors='coerce').to_numpy(dtype=float)
    if len(raw) != len(df) or not np.array_equal(served, held, equal_nan=True):
        raise SaltPerCapitaJoinError(
            f'the wide food_acquired frame is not {_SALT_SOURCE} row for row')
    hit = salt_per_capita_rows(raw)
    if not hit.any():
        return df
    idx = df.index.to_frame(index=False)
    j = idx['j'].astype(object).to_numpy(copy=True)
    j[hit] = _WITHHELD_J
    try:
        idx['j'] = pd.array(j, dtype=idx['j'].dtype)
    except (TypeError, ValueError):
        idx['j'] = j
    out = df.copy()
    out.index = pd.MultiIndex.from_frame(idx)
    return out


def inputs_salt_per_capita(wave='2021-22'):
    """The registry ``inputs`` callable: the withheld records at ``(t, i, j)``.

    The file's own names and codes, with ``hhsize`` from the welfare file, indexed
    as the served table is (:func:`inputs_food_acquired` supplies the index).
    """
    raw = _salt_raw(wave)
    frame = _inputs_last_purchase('Senegal', wave)
    held = pd.to_numeric(raw['s07bq03a'], errors='coerce').to_numpy(dtype=float)
    keyed = pd.to_numeric(frame['s07bq03a'], errors='coerce').to_numpy(dtype=float)
    if len(raw) != len(frame) or not np.array_equal(held, keyed, equal_nan=True):
        raise SaltPerCapitaJoinError(f'the input frame is not {_SALT_SOURCE} row for row')
    hit = salt_per_capita_rows(raw)
    columns = ['vague', 's07bq01', 's07bq03a', 's07bq03b', 's07bq03c', 's07bq04',
               's07bq05', 's07bq06', 's07bq07a', 's07bq07b', 's07bq07c', 's07bq08',
               'hhsize']
    out = raw.loc[hit, columns].copy()
    out.index = frame.index[hit]
    return out
from collections import defaultdict

def v(value):
    '''
    Formatting cluster id (grappe is now a single column)
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
    TRUTHY -- so every unanswered ``s01q02`` went through
    ``str(nan).title()`` and was served as the literal string ``'Nan'``.
    That is 15,201 of this wave's 78,732 person-rows (and 3 in 2018-19):
    Survey Solutions pre-populates the roster grid from the 2018-19
    enumeration (``s01qpreload_*``, populated in 15,200 of the 15,201) and
    the interviewer left every live Section-1 column blank -- ``s01q01``,
    ``s01q03a``, ``s01q04a``, ``s01q00`` and ``s01q12`` are ALL null on
    exactly those rows.  They are CAPI ghost rows, which is why
    ``household_roster`` below drops rows whose Sex, Age and Relationship
    are all null -- a filter that could never fire while this function
    guaranteed a non-null string.

    A missing answer is ``pd.NA``.  Non-null values are title-cased exactly
    as before, and ``pd.isna`` leaves nothing else changed.
    '''
    if pd.isna(value):
        return pd.NA
    return str(value).title()

def Region(value):
    '''
    Formatting region variable
    '''
    return value



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


def previous_i(value):
    '''
    Formatting previous household id from (grappe, menage).
    Must match i() format: format_id(grappe) + '0' + format_id(menage, zp=2).
    '''
    return tools.format_id(value.iloc[0]) + '0' + tools.format_id(value.iloc[1], zeropadding=2)


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

# Mapping from 2021-22 string values to strategy rank (1-indexed)
_STRATEGY_RANK = {
    'Première stratégie': 1,
    'Deuxième strategie': 2,
    'Troisième stratégie': 3,
}


def shocks(df):
    cope_cols = [c for c in df.columns if c.startswith('Cope')]

    how_coped = {0: [], 1: [], 2: []}
    for _, row in df[cope_cols].iterrows():
        # In 2021-22, cope columns contain rank strings instead of binary indicators.
        # Collect (rank, label) pairs and sort by rank to get ordered coping strategies.
        ranked = []
        unranked = []
        for c in cope_cols:
            num = int(c.replace('Cope', ''))
            val = row[c]
            if pd.isna(val):
                continue
            rank = _STRATEGY_RANK.get(str(val))
            if rank is not None:
                ranked.append((rank, COPING_LABELS.get(num, f'Strategy {num}')))
            elif str(val) == 'Oui':
                unranked.append(COPING_LABELS.get(num, f'Strategy {num}'))

        # Code 25 (Autre strategie) alone carries the Oui/Non label set, so its
        # rank is not in the file (GH #1556).  It takes the rank no other
        # strategy holds: ranks are otherwise gapless, and Burkina, which keeps
        # the raw rank, shows 25 always filling the next one.  The instrument
        # allows three strategies, so a fourth falls off the three served slots
        # below and is dropped (0 held rows).
        held = {rank for rank, _ in ranked}
        for label in unranked:
            rank = min(set(range(1, len(held) + 2)) - held)
            held.add(rank)
            ranked.append((rank, label))

        ranked.sort(key=lambda x: x[0])
        found = [label for _, label in ranked]

        for k in range(3):
            how_coped[k].append(found[k] if k < len(found) else np.nan)

    df['HowCoped0'] = how_coped[0]
    df['HowCoped1'] = how_coped[1]
    df['HowCoped2'] = how_coped[2]
    df = df.drop(columns=cope_cols)
    return df


def household_roster(df):
    '''
    Formatting dataframe to calculate ages
    '''

    # The previous -1 pre-strip and negative-result post-null in this
    # function were subsumed by the 2026-04-25 age_handler hardening
    # (commit 63866d8e): age_handler now rejects sentinel and
    # out-of-biological-range inputs at the source via _is_plausible_age
    # and [0, 120] clamps on the DOB and year-math fallback paths.
    def _age_from_row(x):
        return tools.age_handler(
            age=x["Age"][0], d=x["Age"][1], m=x["Age"][2], y=x["Age"][3],
            interview_date=x["interview_date"], interview_year=2021,
        )

    df["Age"] = df.apply(_age_from_row, axis=1)
    df = df.drop('interview_date', axis='columns')

    # Drop CAPI preload ghost rows: Survey Solutions pre-populates a fixed
    # roster grid from a prior enumeration round (s01qpreload_* columns).
    # Rows where the interviewer did not fill in Section-1 live columns
    # (s01q01-s01q04a) remain as blank entries.  Drop any row where all
    # three of Sex, Age, and Relationship are null.
    df = df.dropna(subset=['Sex', 'Age', 'Relationship'], how='all')

    return df
