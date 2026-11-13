# Formatting Functions for Burkina Faso 2021-22
import pandas as pd
import numpy as np
import lsms_library.local_tools as tools
from lsms_library.build_transforms import add_visit_level
from lsms_library.transformations import (
    food_acquired_to_canonical as _food_acquired_canonical)

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
# and Burkina_Faso/_/derivations.yml.
FOOD_ACQUIRED_DERIVATION = _derivation_key('Burkina_Faso')

# GH #812.  `strata` is the documented stratum, region x milieu, as a label.  It
# is a construction, so `sample` carries its registry key: see
# lsms_library/ehcvm.py and Burkina_Faso/_/derivations.yml.
from lsms_library.ehcvm import (  # noqa: E402
    stamp_region_milieu_stratum as _stamp_region_milieu_stratum,
    strata_label,
)


def sample(df):
    return _stamp_region_milieu_stratum(df, 'Burkina_Faso')

# GH #1452: `Quantity_kg` from the survey's own unit-weight table, national median;
# which file and column, and why: lsms_library.ehcvm.NSU_TABLES.


def inputs_food_acquired(wave):
    """The registry `inputs` callable: raw section-7B answers at (t, i, j).

    A wave-level wrapper rather than a `functools.partial` in the country
    module, so that binding the country name does not put this code in
    Burkina_Faso/_/burkina_faso.py -- which is in EVERY Burkina_Faso table's cache
    fingerprint.
    """
    return _inputs_last_purchase('Burkina_Faso', wave)


def inputs_food_acquired_unit_weight(wave):
    """The registry `inputs` callable of the unit-weight derivation (GH #1452)."""
    return _inputs_unit_weight('Burkina_Faso', wave)


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
        df, FOOD_ACQUIRED_DERIVATION, wave='2021-22'), visit=1)

# ---------------------------------------------------------------------------
# cluster_features: a panel grappe's 2018-19 fix (GH #1057)
# ---------------------------------------------------------------------------
#
# 2021-22 ships no cluster GPS: no grappe file in either catalog entry, and the
# panel cover's GPS__* are 0 of 3,227 populated.  The panel re-interviews
# households "tracked within the EA" (bid_bfa_ehcvm2.pdf p. 4) and grappe ==
# grappe_EHCVM1 for all 3,227, so a panel grappe IS a 2018-19 EA and the fix
# carries.  Three grappes name a different province in the two waves, which puts
# that identity in doubt for them (GH #1059), six carry a fix far from their own
# parcels, and three carry a copy of another grappe's reading, so all twelve stay NA
# by ruling.  The cross-section has no 2018-19 antecedent; its household GPS is not a
# cluster fix.
PANEL_GPS_DERIVATION = 'Burkina_Faso::cluster_features::panel-grappe-gps-from-2018-19'

# Seno/Yagha, Poni/Noumbiel, Kompienga/Gourma.  The first two have a 2018-19 fix;
# the third has none, so its exclusion changes nothing served.
_GPS_PROVINCE_CHANGED_GRAPPES = ('19', '365', '549')

# The fix lies more than 25 km from the median of the grappe's own 2021-22 parcels
# (s16aq46; km 104.7, 103.7, 51.4, 44.5, 29.3, 28.8), where the median carried grappe
# is 1.6 km away.  Ruling of 2026-11-04.  The list is literal so that a build never
# reads s16a; tests/test_gh1060_1057_burkina_cluster_features.py recomputes it from
# the parcels.
_GPS_FAR_FROM_PARCELS_GRAPPES = ('103', '104', '240', '237', '229', '226')

# Rounded copies of another grappe's reading, so not a reading of this grappe in either
# wave: 2018-19/_/mapping.py withholds them there.  Ruling of 2026-11-05.  226, 237, 240
# and 365 are copies too, and are listed above.
_GPS_COPIED_READING_GRAPPES = ('216', '218', '350')

_GPS_NOT_CARRIED_GRAPPES = (_GPS_PROVINCE_CHANGED_GRAPPES + _GPS_FAR_FROM_PARCELS_GRAPPES
                            + _GPS_COPIED_READING_GRAPPES)

_GPS_COLUMNS = {'Latitude': 'coordonnes_gps__Latitude',
                'Longitude': 'coordonnes_gps__Longitude'}


def derive_panel_grappe_gps(df):
    """Keep a panel grappe's 2018-19 fix, and stamp the rows that carry one.

    Takes the merged frame: the ``v`` index level, and ``Latitude`` / ``Longitude``
    from ``df_geo``.  Blanks the excluded grappes, then stamps ``Derivation`` on
    every row still carrying a coordinate.  Row order is preserved.  With neither
    column present (``df_geo`` could not be fetched, which the GH #675 drop stamp
    reports) there is nothing carried and nothing to stamp.
    """
    out = df.copy()
    present = [c for c in _GPS_COLUMNS if c in out.columns]
    if not present:
        return out
    excluded = out.index.get_level_values('v').isin(_GPS_NOT_CARRIED_GRAPPES)
    out.loc[excluded, present] = np.nan
    carried = out[present].notna().any(axis=1).to_numpy()
    out['Derivation'] = pd.Series(pd.NA, index=out.index, dtype='string')
    out.loc[carried, 'Derivation'] = PANEL_GPS_DERIVATION
    return out


def cluster_features(df):
    """``cluster_features`` post-processor: see :func:`derive_panel_grappe_gps`."""
    return derive_panel_grappe_gps(df)


def inputs_cluster_features(wave='2021-22'):
    """The registry `inputs` callable: the 2018-19 fix behind each row that carries
    one, one row per grappe, at ``(t, v)`` with ``t`` the wave it is served in.

    ``grappe_EHCVM1`` is the panel cover's own pointer to the 2018-19 grappe, the
    link the construction rests on.  An excluded grappe's fix is not returned,
    because no served row was computed from it.
    """
    if wave != '2021-22':
        raise ValueError(f'{PANEL_GPS_DERIVATION} covers 2021-22 only, not {wave!r}')
    from lsms_library.paths import countries_root
    root = countries_root() / 'Burkina_Faso'
    gps = tools.get_dataframe(root / '2018-19' / 'Data' / 'grappe_gps_bfa2018.dta')
    cover = tools.get_dataframe(root / wave / 'Data' / 's00_me_bfa2021.dta')
    out = gps[['grappe', *_GPS_COLUMNS.values()]].merge(
        cover[['grappe', 'grappe_EHCVM1']].drop_duplicates(), on='grappe')
    v = out['grappe'].map(tools.format_id)
    keep = out[list(_GPS_COLUMNS.values())].notna().any(axis=1) & ~v.isin(_GPS_NOT_CARRIED_GRAPPES)
    return out[keep].assign(t=wave, v=v[keep]).set_index(['t', 'v'])


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


def Age(value):
    '''
    Convert French month names to integers for the DOB components.

    Burkina Faso s01q03b (month) uses French month names when loaded via DVC
    with convert_categoricals=True (e.g. 'Janvier', 'Février', ..., 'Décembre').
    'Ne sait pas' ('don't know') is the sentinel for unknown month; it maps to
    None so age_handler falls back to year-only age computation.
    Returns a list [s01q04a, s01q03a(day), s01q03b(month int), s01q03c(year)].

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


def household_roster(df):
    '''
    Recover Age from date-of-birth components when s01q04a is null.

    Age list in data_info.yml: [s01q04a, s01q03a(day), s01q03b(month), s01q03c(year)].
    s01q03b uses French month names (Janvier…Décembre) after categorical conversion;
    the Age() function above maps them to integers.  'Ne sait pas' maps to None, so
    age_handler falls back to year-only computation.
    Negative results (birth year recorded after interview) are nulled out.
    '''
    def _age_from_row(x):
        age_raw = x["Age"][0]
        # Pass None for missing/negative direct-age so age_handler falls through to DOB columns
        age_val = None if (pd.notna(age_raw) and int(float(age_raw)) < 0) else age_raw
        result = tools.age_handler(age=age_val, d=x["Age"][1], m=x["Age"][2], y=x["Age"][3],
                                   interview_date=x["interview_date"], interview_year=2021)
        # Null out any negative result (e.g. DOB slightly after interview date)
        return np.nan if (pd.notna(result) and result < 0) else result

    df["Age"] = df.apply(_age_from_row, axis=1)
    df = df.drop('interview_date', axis='columns')
    return df


def shocks(df):
    cope_cols = [c for c in df.columns if c.startswith('Cope')]

    how_coped = {0: [], 1: [], 2: []}
    for _, row in df[cope_cols].iterrows():
        # Unlabelled in this file, but the values are the instrument's ranks:
        # every row holds 1, 1-2 or 1-2-3, and code 25 keeps its rank (GH #1556).
        ranked = []
        for c in cope_cols:
            num = int(c.replace('Cope', ''))
            val = row[c]
            try:
                val = float(val)
            except (ValueError, TypeError):
                continue
            if val >= 1:
                ranked.append((val, COPING_LABELS.get(num, f'Strategy {num}')))
        ranked.sort(key=lambda x: x[0])
        found = [label for _, label in ranked]
        for k in range(3):
            how_coped[k].append(found[k] if k < len(found) else np.nan)

    df['HowCoped0'] = how_coped[0]
    df['HowCoped1'] = how_coped[1]
    df['HowCoped2'] = how_coped[2]
    df = df.drop(columns=cope_cols)
    return df


# --- crop_production (GH #1436 second residual, #824, #1437) -------------------------
#
# The 2021-22 builder is crop_production.py beside this file; these helpers are shared
# with the registry `inputs` callables below, so the decoding is written once.  Names
# carry a leading underscore or an `inputs_` prefix: every callable here is also a
# formatting function keyed by its name, and none may shadow a column or a table.
#
# THE IDENTITY OF A CROP IS ITS CODE.  s16cq04 ("Id in cultures") and s16dq01 ("Id in
# culture_utilisation_annuel") are Survey Solutions roster ids on one crop list, but the
# two held files carry DIFFERENT value-label sets for it (measured 2026-11-13):
# s16c says 3 'Riz', 8 'Niébé/Haricot', 9 'Voandzou (pois de terre)', 27 'Laitue
# (salade)', 53 'Agrume', 54 'Manguier', 55 'Autre (à préciser)' and leaves 58, 64, 65
# bare; s16d says 3 'Riz Paddy', 8 'Niébé', 9 'Voandzou', 27 'Laitue', 65 'Autre (à
# préciser)' and leaves 53, 54, 55, 58, 64 bare.  A sale is therefore matched to its
# harvest lines by CODE: by label, 332 of the 1,548 sales find no line, by code none.
# The served label is the harvest file's own, through harmonize_food as in 2018-19; a
# code it leaves bare, or labels with a word s16d gives to a DIFFERENT code (55's
# 'Autre (à préciser)', which s16d puts on 65), keeps its source-qualified identity.

KEY_HH_CROP_SALE = 'Burkina_Faso::crop_production::hh-crop-sale-single-row'
KEY_HARVEST_TO_DATE = 'Burkina_Faso::crop_production::harvest-to-date'
_CROP_WAVE = '2021-22'


def _crop_household_ids(frame):
    """The native 2021-22 panel `i`: burkina_faso.i's rule, grappe + menage.zfill(3).
    The framework walks it to the 2018-19 id exactly once (updated_ids.json)."""
    return pd.Series([tools.format_id(g) + tools.format_id(m, zeropadding=3)
                      for g, m in zip(frame['grappe'], frame['menage'])],
                     index=frame.index, dtype='string')


def _crop_plot_ids(frame):
    """`{s16cq02}_{s16cq03}`: the spelling of plot_features' plot_id."""
    return (frame['s16cq02'].apply(tools.format_id).astype('string') + '_'
            + frame['s16cq03'].apply(tools.format_id).astype('string'))


def _code_labels(labelled, codes, field):
    """``{code: label or None}`` of one variable, from its labelled and numeric reads.
    A code the file leaves bare (the labelled read returns the number) maps to None.
    Refuses a code read with two different labels."""
    num = pd.to_numeric(codes, errors='coerce')
    text = labelled.astype('string').str.strip()
    out = {}
    for code, label in zip(num, text):
        if pd.isna(code):
            continue
        code = int(code)
        bare = pd.isna(label) or pd.to_numeric(pd.Series([label]), errors='coerce').eq(code).iloc[0]
        out.setdefault(code, set()).add(None if bare else str(label))
    bad = sorted(c for c, labels in out.items() if len(labels) > 1)
    if bad:
        raise ValueError(f'{field}: codes read with two labels: {bad}')
    return {c: next(iter(labels)) for c, labels in sorted(out.items())}


def _qualified(field, code):
    return f'Unknown [Burkina_Faso {_CROP_WAVE} {field}={int(code)}]'


def _crop_label_table(harvest_labels, record_labels, food_map):
    """``{code: served crop label}`` for the wave (see the block comment above)."""
    claimed = {}
    for code, label in record_labels.items():
        if label is not None:
            claimed.setdefault(label, set()).add(code)
    table = {}
    for code in sorted(set(harvest_labels) | set(record_labels)):
        label = harvest_labels.get(code)
        if label is not None and claimed.get(label, {code}) - {code}:
            label = None
        table[code] = food_map.get(label, label) if label is not None else _qualified('s16cq04', code)
    return table


def _unit_labels(labelled, codes, field, unit_map):
    """One unit variable, by code: its own value label through the `u` table, or its
    source-qualified identity where the file leaves the code bare.  NA stays NA."""
    pairs = _code_labels(labelled, codes, field)
    num = pd.to_numeric(codes, errors='coerce')
    def one(code):
        if pd.isna(code):
            return pd.NA
        label = pairs[int(code)]
        return unit_map.get(label, label) if label is not None else _qualified(field, code)
    return pd.Series([one(c) for c in num], index=codes.index, dtype='string')


def _crop_maps():
    """harmonize_food and the `u` table, as the 2018-19 builder reads them."""
    food = tools.get_categorical_mapping(
        tablename='harmonize_food', idxvars='Original Label', dirs=[str(_country_dir()) + '/'],
        **{'Preferred Label': 'Preferred Label'})
    units = tools.get_categorical_mapping(
        tablename='u', idxvars='Original Label', dirs=[str(_country_dir()) + '/'],
        **{'Preferred Label': 'Preferred Label'})
    return food, units


def _country_dir():
    from lsms_library.paths import countries_root
    return countries_root() / 'Burkina_Faso' / '_'


def _raw_crop_file(name):
    from lsms_library.paths import countries_root
    path = countries_root() / 'Burkina_Faso' / _CROP_WAVE / 'Data' / name
    return (tools.get_dataframe(str(path), convert_categoricals=True),
            tools.get_dataframe(str(path), convert_categoricals=False))


def _raw_crop_frames():
    """s16c (crop rows only) and s16d (exact repeats removed), each with the served
    t, i and crop beside the answers as recorded."""
    from lsms_library.ehcvm import hh_crop_records
    c_lab, c_num = _raw_crop_file('s16c_me_bfa2021.dta')
    d_lab, d_num = _raw_crop_file('s16d_me_bfa2021.dta')
    keep = c_num['s16cq04'].notna().to_numpy()
    c_lab, c_num = c_lab[keep], c_num[keep]
    food, _ = _crop_maps()
    table = _crop_label_table(_code_labels(c_lab['s16cq04'], c_num['s16cq04'], 's16cq04'),
                              _code_labels(d_lab['s16dq01'], d_num['s16dq01'], 's16dq01'), food)
    d_num = hh_crop_records(d_num)
    # A record with no crop code is the placeholder of a household with no crop (1,164).
    d_num = d_num[d_num['s16dq01'].notna()]
    d_lab = d_lab.loc[d_num.index]
    for lab, num, field in ((c_lab, c_num, 's16cq04'), (d_lab, d_num, 's16dq01')):
        lab['t'] = _CROP_WAVE
        lab['i'] = _crop_household_ids(num).to_numpy()
        lab['crop_code'] = pd.to_numeric(num[field], errors='coerce').astype('Int64').to_numpy()
        lab['crop'] = pd.array([table[int(c)] for c in lab['crop_code']], dtype='string')
    c_lab['plot_id'] = _crop_plot_ids(c_num).astype(str).to_numpy()
    return c_lab, d_lab


def inputs_hh_crop_sale(wave):
    """Registry `inputs`: the s16d sale as recorded, with the field and parcel of the one
    s16c line of the same crop CODE it went on, one row per keyed served row, at
    (t, i, plot_id, crop), the served line's grain."""
    if wave != _CROP_WAVE:
        raise ValueError(f'{KEY_HH_CROP_SALE} covers {_CROP_WAVE} only, not {wave!r}')
    from lsms_library.ehcvm import is_crop_sale
    harvest, records = _raw_crop_frames()
    sold = records[is_crop_sale(records['s16dq05a'], records['s16dq06']).to_numpy()]
    lines = harvest.groupby(['i', 'crop_code']).size()
    single = lines[lines == 1].index
    harvest = harvest[harvest.set_index(['i', 'crop_code']).index.isin(single)]
    out = sold[['t', 'i', 'crop', 'crop_code', 's16dq01', 's16dq04', 's16dq05a', 's16dq05b',
                's16dq06']].merge(harvest[['i', 'crop_code', 'plot_id', 's16cq02', 's16cq03']],
                                  on=['i', 'crop_code'], how='inner', validate='one_to_one')
    return out.set_index(['t', 'i', 'plot_id', 'crop'])


def inputs_harvest_to_date(wave):
    """Registry `inputs`: the unfinished s16c rows with a recorded quantity, at
    (t, i, plot_id, crop), the served line's grain."""
    if wave != _CROP_WAVE:
        raise ValueError(f'{KEY_HARVEST_TO_DATE} covers {_CROP_WAVE} only, not {wave!r}')
    harvest, _ = _raw_crop_frames()
    keep = (harvest['s16cq11'].astype('string').str.strip().eq('Non').fillna(False)
            & pd.to_numeric(harvest['s16cq16a'], errors='coerce').notna())
    return harvest.loc[keep.to_numpy(), ['t', 'i', 'plot_id', 'crop', 's16cq02', 's16cq03',
                                         's16cq11', 's16cq12', 's16cq16a', 's16cq16b']].set_index(
                                             ['t', 'i', 'plot_id', 'crop'])
