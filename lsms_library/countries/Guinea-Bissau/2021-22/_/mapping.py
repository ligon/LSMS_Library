# Formatting functions for Guinea-Bissau 2021-22 (EHCVM-2).  See _/CONTENTS.org
# "2021-22 wired".  Copied from the Togo / Benin 2021-22 wiring (PR #1711), with
# every label-keyed map re-keyed from THIS file's own Portuguese value labels.
import numpy as np
import pandas as pd

import lsms_library.local_tools as tools

# GH #1575's construction, extended to this wave as the 2026-11-12 ruling extends it
# to Togo / Benin / Cote d'Ivoire ("Same rule as #1575"): `strata` is the cover's
# region label and milieu label, by the shared helper, and `sample` stamps the
# registry key on every row with a stratum.  The registry's `inputs` callable is
# 2018-19's, which reads each wave's own cover.
from lsms_library.ehcvm import (
    stamp_region_milieu_stratum as _stamp_region_milieu_stratum,
    strata_label,
)


def sample(df):
    return _stamp_region_milieu_stratum(df, 'Guinea-Bissau')


#: `i` of this wave.  4,893 of the 5,351 (grappe, menage) keys also occur in 2018-19
#: (s00q07f1 / s00q07f2 record the 2018 grappe and household, and PanelHH a panel
#: flag), and whether a recurring key is the same household is not checked here.
#: No link is served (ruling 2026-11-12: no panel IDs), so the ids must not meet.
ID_PREFIX = 'E2_'


def i(value):
    '''Household id from (grappe, menage): 2018-19's form, with this wave's prefix.'''
    return ID_PREFIX + tools.format_id(value.iloc[0]) + '0' + tools.format_id(value.iloc[1], zeropadding=2)


def Sex(value):
    '''s01q01: Masculino / Feminino, as 2018-19's labels.'''
    return {'Masculino': 'M', 'Feminino': 'F'}.get(str(value).strip(), pd.NA) \
        if pd.notna(value) else pd.NA


#: s01q03b, Portuguese month names; `Não sabe` (5,819 rows) is no month.
_MONTH = {'Janeiro': 1, 'Fevereiro': 2, 'Março': 3, 'Abril': 4, 'Maio': 5, 'Junho': 6,
          'Julho': 7, 'Agosto': 8, 'Setembro': 9, 'Outubro': 10, 'Novembro': 11,
          'Dezembro': 12}


def Age(value):
    '''[s01q04a, s01q03a, s01q03b, s01q03c], with the month label as a number.

    The name collides with the `assets` myvar `Age: s12q07`, which the framework
    binds to this by name with a scalar; a scalar passes through (GH #321).
    '''
    if not isinstance(value, pd.Series):
        return value
    result = list(value)
    result[2] = _MONTH.get(str(value.iloc[2]).strip()) if pd.notna(value.iloc[2]) else None
    return result


#: s01q00aa, "[NOM] est-il toujours un membre de ce ménage?".  The file spells `não`
#: double-encoded with a byte lost (`nÃo`), whose lower case is `não`.
_STILL = {'sim': True, 'não': False, 'nao': False}


def household_roster(df):
    '''Every person s01 lists, the departed flagged; Age from the stated years, else the
    birth date.

    Ruling 2026-11-13, "Serve leavers": the 341 people s01q00aa ("est-il toujours un
    membre de ce ménage?") says have left are SERVED, with `Departed` True; everyone else
    has `Departed` False, including the 9,727 rows the question was not put to (new
    members, all of them in ehcvm_individu).  No canonical flag existed, so `Departed` is
    a country-level optional column (data_scheme.yml).  Benin 2021-22 still drops its
    leavers, so the two waves differ until GH #1565 settles the family.  A leaver's
    MonthsSpent is NA (ruling 2026-11-13, "Exclude from HH size"), so
    household_characteristics counts the survey's household; the 12 rows whose s01q12 is
    withheld carry KEY_DEPARTED_MONTHS_WITHHELD in `Derivation`.  Ages as 2018-19's hook:
    9999 is no answer, and a birth date after the interview gives NA.
    '''
    still = df['StillMember'].astype('string').str.strip().str.lower()
    unknown = sorted(set(still.dropna()) - set(_STILL))
    if unknown:
        raise ValueError(f'Guinea-Bissau 2021-22 s01q00aa: unknown answers {unknown}')
    df = df.copy()
    df['Departed'] = (~still.map(_STILL).fillna(True).astype(bool)).astype('boolean')
    # s01q12 is blank on most leavers, so MonthsSpent holds NA: nullable whole numbers
    # (2018-19 has none, and a two-wave read serves its column as Int64, values unchanged).
    # A leaver's answer is withheld (ruling 2026-11-13, "Exclude from HH size"), and the
    # 12 rows where one was recorded carry KEY_DEPARTED_MONTHS_WITHHELD.
    months = pd.to_numeric(df['MonthsSpent'], errors='coerce').astype('Int64')
    departed = df['Departed'].fillna(False).to_numpy(dtype=bool)
    df['MonthsSpent'] = withhold_departed_months(months, departed)
    df['Derivation'] = pd.Series(np.where(departed & months.notna().to_numpy(dtype=bool),
                                          KEY_DEPARTED_MONTHS_WITHHELD, None),
                                 index=df.index, dtype='string')

    def _known(x):
        return None if (pd.isna(x) or int(float(x)) == 9999) else x

    def age(row):
        stated, d, m, y = row['Age']
        result = tools.age_handler(age=_known(stated), d=_known(d), m=m, y=_known(y),
                                   interview_date=row['interview_date'], interview_year=2021)
        return np.nan if (pd.notna(result) and result < 0) else result

    df['Age'] = df.apply(age, axis=1)
    return df.drop(columns=['interview_date', 'StillMember'])


#: Ruling @ligon 2026-11-13, "Exclude from HH size (Recommended)": the leavers are served
#: in the roster, flagged `Departed`, but their s01q12 answer is not, so
#: household_characteristics counts the household the survey counts (ehcvm_individu,
#: ehcvm_welfare.hhsize).  12 leavers answered s01q12 (all Sim, 12); their answer is
#: withheld and the rows carry this key.  The other 329 leavers have no answer to withhold.
KEY_DEPARTED_MONTHS_WITHHELD = 'Guinea-Bissau::household_roster::departed-monthsspent-withheld'


def withhold_departed_months(months, departed):
    '''The registered function of KEY_DEPARTED_MONTHS_WITHHELD: MonthsSpent, NA wherever
    the person has left the household (`Departed` True).  No options, by design.'''
    m = pd.to_numeric(pd.Series(months), errors='coerce').astype('Int64')
    gone = pd.Series(departed, index=m.index).fillna(False).astype(bool).to_numpy()
    return m.mask(gone)


def inputs_departed_monthsspent_withheld(wave):
    '''Registry `inputs` of KEY_DEPARTED_MONTHS_WITHHELD: the s01 rows of the leavers
    whose s01q12 is answered, with the raw s01q00aa and s01q12, at (t, i, pid).'''
    from pathlib import Path

    from lsms_library.paths import countries_root
    s01 = tools.get_dataframe(str(Path(countries_root()) / 'Guinea-Bissau' / wave / 'Data'
                                  / 's01_me_gnb2021.dta'), convert_categoricals=True)
    still = s01['s01q00aa'].astype('string').str.strip().str.lower()
    departed = (~still.map(_STILL).fillna(True).astype(bool)).to_numpy(dtype=bool)
    raw = s01[departed & s01['s01q12'].notna().to_numpy(dtype=bool)]
    ids = [ID_PREFIX + tools.format_id(g) + '0' + tools.format_id(m, zeropadding=2)
           for g, m in zip(raw['grappe'], raw['menage'])]
    pid = [tools.format_id(p) for p in raw['membres__id']]
    index = pd.MultiIndex.from_arrays([[wave] * len(raw), ids, pid], names=['t', 'i', 'pid'])
    return raw[['s01q00aa', 's01q12']].set_axis(index)


def cluster_features(df):
    '''The cover to (t, v), and a refusal if a grappe straddles two values.

    Benin 2021-22's hook.  Region, Rural and the GPS point are constant within each
    of the 450 grappes (measured), so dropping the repeats loses nothing.
    '''
    levels = list(df.index.names)
    spread = df.groupby(level=levels, observed=True).nunique(dropna=False)
    straddling = spread[(spread > 1).any(axis=1)]
    if len(straddling):
        raise ValueError(f'Guinea-Bissau cluster_features 2021-22: {len(straddling)} grappes '
                         f'carry more than one value:\n{straddling.head(10)}')
    return df[~df.index.duplicated(keep='first')]


#: The coping strategies, s14bq05__1..26, by their variable labels: 2018-19's 25 list
#: (French, as Togo's and Benin's) and 26 `Aucune stratégie`.
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

#: The `strategie` value labels of this file (French, as Benin's and Togo's).
_STRATEGY_RANK = {
    'Première stratégie': 1,
    'Deuxième strategie': 2,
    'Troisième stratégie': 3,
}


def shocks(df):
    '''HowCoped0..2 by the survey's own rank: s14bq05__N holds a rank label or Non.

    Benin 2021-22's hook (Niger's after GH #1556).  Column 25, `Autre stratégie`,
    carries the Oui/Non label set (`nonoui`), so its rank is not in the file: it takes
    the rank no other strategy holds.  34 rows choose it here.  A label outside the
    two sets is refused.
    '''
    cope_cols = [c for c in df.columns if c.startswith('Cope')]
    seen = {str(v) for c in cope_cols for v in df[c].dropna().unique()}
    unknown = sorted(seen - set(_STRATEGY_RANK) - {'Oui', 'Non'})
    if unknown:
        raise ValueError(f'Guinea-Bissau 2021-22 s14bq05: unknown answers {unknown}')

    how_coped = {0: [], 1: [], 2: []}
    for _, row in df[cope_cols].iterrows():
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
    return df.drop(columns=cope_cols)


# --- food_acquired: served as released, the rounding registered (GH #1697) ------------
#
# Ruling @ligon 2026-11-13, "Serve, registered key" (not the recommended hold).  The
# release's s07b quantities are rounded to even integers by a step no document records:
# every positive s07bq03a (128,229) and s07bq07a (96,262) is even (s07bq04 / s07bq05: 23
# odd each, on the 23 rows where q3 = q4 + q5), 3,945 consumed rows carry s07bq03a = 0,
# and 2 x round-half-up(x / 2) applied to 2018-19's answers reproduces 2021-22's
# distribution.  The rule maps 1, 2 and 2.5 all to 2, so it cannot be inverted: nothing is
# recovered: every row with a positive quantity is served as released and carries
# KEY_ROUNDED.  The 3,945 consumed rows whose s07bq03a the rounding sent to 0 are served
# one purchased row each with Quantity and Expenditure NA, under KEY_ZERO_WITHHELD
# (ruling 2026-11-13, "Serve NA, as Togo"; Togo 2021-22's floored-count-withheld rows).
from lsms_library.ehcvm import (  # noqa: E402
    derivation_key as _derivation_key,
    food_acquired_ehcvm as _food_acquired_ehcvm,
    inputs_last_purchase as _inputs_last_purchase,
)

FOOD_ACQUIRED_DERIVATION = _derivation_key('Guinea-Bissau')
KEY_ROUNDED = 'Guinea-Bissau::food_acquired::s07b-quantities-rounded-to-even'
#: A WITHHELD quantity (GH #1519's rule): a consumed row the release rounds to 0 is served
#: one purchased row, Quantity NA and Expenditure NA, carrying this key.
KEY_ZERO_WITHHELD = 'Guinea-Bissau::food_acquired::s07bq03a-rounded-to-zero-withheld'

#: s07bq03b and s07bq07b label one code book twice: 107 is `Lata/caneca` and `Lata`, 122
#: `Rede de batata` and `Rede de cebola, alho`, 126 `Pedaço` and `Pedaço (25 F)`.  These
#: labels carry no code prefix, so unit_match would read every such purchase as another
#: unit.  Put on s07bq03b's spelling of the same code, as Togo 2021-22 does.
_PURCHASE_UNIT_AS_CONSUMPTION_UNIT = {'Lata': 'Lata/caneca',
                                      'Rede de cebola, alho': 'Rede de batata',
                                      'Pedaço (25 F)': 'Pedaço'}


def LastPurchaseUnitCode(value):
    '''s07bq07b, on s07bq03b's spelling of the same code.'''
    if isinstance(value, str):
        return _PURCHASE_UNIT_AS_CONSUMPTION_UNIT.get(value.strip(), value)
    return value


def quantity_as_released(quantity):
    '''The registered function of KEY_ROUNDED: the identity.  The release's quantity is
    served unchanged, because the rounding cannot be inverted; the key says what the
    number is.  The fitted rounding is `round_half_up_to_even`, a hypothesis.'''
    return quantity


def round_half_up_to_even(x):
    '''2 x floor(x / 2 + 0.5): the rule that reproduces the release's distribution from
    2018-19's answers (Kg and Litro shares of 0 / 2 / 4; it is also Stata's round(x, 2)).
    A fit, not a proof: ceiling to even fits Unidade and Monte as well (red team F3), and
    only the Kg / Litro zeros tell the two apart.  Not applied to anything served.'''
    return 2 * np.floor(np.asarray(x, dtype=float) / 2 + 0.5)


def _add_key(out, key, rows):
    from lsms_library.derivations import union_keys
    current = out['Derivation'].astype(object).to_numpy()
    stamped = [union_keys([c, key]) if r else c for c, r in zip(current, rows)]
    out['Derivation'] = pd.Series(stamped, index=out.index, dtype='string')
    return out


def _purchased(out):
    return out.index.get_level_values('s').to_numpy() == 'purchased'


def zero_quantity_withheld(quantity):
    '''The registered function of KEY_ZERO_WITHHELD: the release's s07bq03a, NA where it
    is 0.  A consumed row filed as 0 is a small quantity the rounding sent to 0, which
    cannot be inverted, so it is withheld rather than served as 0.  No options, by design.'''
    q = pd.to_numeric(pd.Series(np.ravel(quantity)), errors='coerce')
    return q.mask(q == 0)


#: s07bq06 ("when did you last buy it?") answer of a household that says it never bought
#: the item.  Ruling 2026-11-13, "GNB 'Nunca' rows": "s='other'".
NEVER_BOUGHT = 'Nunca'


def _withheld_rows(df, s, stamp_7day):
    '''One served row per wide row of `df`, Quantity and Expenditure NA, at source `s`,
    keyed KEY_ZERO_WITHHELD.  The shared reshape builds the row's index (sized u
    included) from a placeholder quantity it never serves.'''
    held = _food_acquired_ehcvm(df.assign(Quantity=1.0, Produced=0.0, GiftQuantity=0.0),
                                FOOD_ACQUIRED_DERIVATION)
    held = held[_purchased(held)].copy()
    held['Quantity'] = np.nan
    held['Expenditure'] = np.nan
    if not stamp_7day:
        # Not a purchase, so not the 7-day key's row (it is on every purchased row and
        # nowhere else).
        held['Derivation'] = pd.Series(pd.NA, index=held.index, dtype='string')
    if s != 'purchased':
        names = list(held.index.names)
        flat = held.reset_index()
        flat['s'] = s
        held = flat.set_index(names)
    return _add_key(held, KEY_ZERO_WITHHELD, np.ones(len(held), dtype=bool))


def food_acquired(df):
    '''GH #876's construction (Expenditure at the last purchase's unit value, keyed; size
    in u, GH #1224) on the release's rounded quantities, KEY_ROUNDED on every row with a
    quantity, and one row NA, under KEY_ZERO_WITHHELD, for each consumed row the release
    rounds to 0.

    Ruling 2026-11-13, "Serve NA, as Togo": Togo 2021-22's withheld rows are the template.
    A purchased row is served where a purchase is implied, which is Togo's test, the row
    reporting neither own production nor a gift; every one of the 3,945 passes it
    (s07bq04 is 0 or NA, s07bq05 0).  Its Quantity is NA and so is its Expenditure: the
    purchase value goes with the quantity it would be derived from (GH #1519).  Ruling
    2026-11-13, "GNB 'Nunca' rows": a row whose household says it never bought the item
    (s07bq06 = Nunca, 685) is served at s='other' instead, under the same key and without
    the 7-day key; the other 3,260 stay purchased.
    '''
    when = df['LastPurchaseWhen'].astype('string').str.strip()
    df = df.drop(columns=['LastPurchaseWhen'])
    out = _food_acquired_ehcvm(df, FOOD_ACQUIRED_DERIVATION)
    out = _add_key(out, KEY_ROUNDED, np.ones(len(out), dtype=bool))
    q3 = pd.to_numeric(df['Quantity'], errors='coerce').to_numpy(dtype=float)
    q4 = pd.to_numeric(df['Produced'], errors='coerce').fillna(0).to_numpy(dtype=float)
    q5 = pd.to_numeric(df['GiftQuantity'], errors='coerce').fillna(0).to_numpy(dtype=float)
    zero = (q3 == 0) & (q4 <= 0) & (q5 <= 0)
    never = when.eq(NEVER_BOUGHT).fillna(False).to_numpy(dtype=bool)
    parts = [out]
    if (zero & ~never).any():
        parts.append(_withheld_rows(df[zero & ~never], 'purchased', stamp_7day=True))
    if (zero & never).any():
        parts.append(_withheld_rows(df[zero & never], 'other', stamp_7day=False))
    return _withhold_collisions(pd.concat(parts), KEY_ZERO_WITHHELD)


def _withhold_collisions(out, key):
    '''Togo 2021-22's rule: a withheld row that shares its served key (t, v, i, j, u, s)
    with another row -- two items of the file under one served j (sugar's codes 134 and
    135) -- would be summed with it by the core collapse, which skips an NA, and the sum
    would be served as if whole.  So every row of such a group is withheld and keyed.
    Measured: no household holds both sugar codes in one unit and size, so none fires.'''
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


def inputs_food_acquired(wave):
    """The registry `inputs` callable: raw section-7B answers at (t, i, j)."""
    return _inputs_last_purchase('Guinea-Bissau', wave)


def _s07b_with_consumed(wave):
    '''The 7-day key's raw inputs with s07bq02 (consumed in the last 7 days?) and s07bq06
    (when last bought) beside them, row for row: both read the file in its own order.'''
    from pathlib import Path

    from lsms_library.paths import countries_root
    raw = _inputs_last_purchase('Guinea-Bissau', wave)
    s07 = tools.get_dataframe(str(Path(countries_root()) / 'Guinea-Bissau' / wave / 'Data'
                                  / 's07b_me_gnb2021.dta'), convert_categoricals=True)
    if len(s07) != len(raw):
        raise ValueError(f'Guinea-Bissau {wave} s07b: {len(s07)} rows against {len(raw)} inputs')
    return raw.assign(s07bq02=s07['s07bq02'].astype('string').str.strip().to_numpy(),
                      s07bq06=s07['s07bq06'].astype('string').str.strip().to_numpy())


def inputs_quantities_rounded(wave):
    '''Registry `inputs` of KEY_ROUNDED: the released answers of every consumed row with a
    positive s07bq03a, the rows every served row of the wave comes from, at (t, i, j).
    The quantities are the release's, rounded.'''
    raw = _inputs_last_purchase('Guinea-Bissau', wave)
    return raw[pd.to_numeric(raw['s07bq03a'], errors='coerce') > 0]


def inputs_zero_quantity_withheld(wave):
    '''Registry `inputs` of KEY_ZERO_WITHHELD: the consumed rows (s07bq02 = Oui) whose
    released s07bq03a is 0, each served one purchased row NA, at (t, i, j), with s07bq06.'''
    raw = _s07b_with_consumed(wave)
    q3 = pd.to_numeric(raw['s07bq03a'], errors='coerce')
    return raw[(raw['s07bq02'] == 'Oui') & (q3 == 0)]
