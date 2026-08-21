#!/usr/bin/env python

import warnings

import pandas as pd
import pyreadstat
import numpy as np
import json


# ---------------------------------------------------------------------------
# Cluster identity (GH #323)
#
# ENCOVI 2000's primary sampling unit.  The survey DID release a PSU: the
# household-level file CONSUMO5.DTA carries `upm` ("Unidad Primaria de
# Muestreo") alongside the full geographic hierarchy depto/mupio/sector/
# segmento.  Both CLAUDE.md's "Guatemala | No PSU/cluster variable in data" and
# the old `v: region` wiring predate finding it -- the ECV*/HOGARES/PERSONAS
# files carry only region+area, so the PSU was never looked for in the one
# household-level file that actually has it.
#
# **Do NOT use the raw `upm` column.**  Stata stored it as a `float` (IEEE
# single precision) and every value lies in ~1.0e8-2.2e9, far above float32's
# exact-integer limit of 2**24 = 16,777,216.  The float32 ULP at those
# magnitudes is 8-256, which destroys the low-order digits encoding `segmento`:
# the 1,065 real PSUs collapse into only 847 distinct stored `upm` values, and
# 201 of those values conflate two or more genuinely different PSUs (2,128
# households).  Reading `upm` back therefore silently MERGES clusters -- a
# smaller instance of the very bug this fix exists to remove.
#
# The faithful, uncorrupted reconstruction is the geographic composite built by
# v() below.  Its components are small integers (mupio<=35, sector<=119,
# segmento<=49) that float32 represents exactly.  Verified against the
# 7,276-household frame:
#   * 1,065 clusters; strictly refines the corrupt `upm` (no composite cell
#     spans two upm values), so it is at least as fine and is clean.
#   * 0 clusters contain both urban and rural households -> Rural is a genuine
#     function of v, so cluster_features is well-posed.
#   * 0 clusters span more than one region -> Region likewise.
#   * the design weight `factor` is EXACTLY constant within every one of the
#     1,065 clusters (0 with >1 distinct weight), as a two-stage design
#     requires -- independent confirmation this is the true PSU.  The corrupt
#     `upm` fails that test (17 of its 847 groups carry >1 weight).
#   * median 6, max 16 households per cluster.
# ---------------------------------------------------------------------------
def v(value):
    """Build the ENCOVI 2000 cluster (PSU) key from its geographic components.

    Bound by the ``[depto, mupio, sector, segmento]`` list in data_info.yml
    (df_data_grabber's "Trickier" form, as with Benin's composite ``i()``), so
    ``value`` is the row's Series of those four columns.

    ``depto`` is a Stata value-label (a department name, e.g. 'alta verapaz');
    the others are small numeric codes, zero-padded here so keys sort sensibly
    and cannot collide by digit-run ambiguity ('1-2-3' vs '12-3').
    """
    depto, mupio, sector, segmento = (value.iloc[k] for k in range(4))

    # Missing any component => no identifiable cluster.  Return NA and let the
    # row be handled explicitly downstream rather than silently bucketed into
    # some wrong PSU.
    if any(pd.isna(x) for x in (depto, mupio, sector, segmento)):
        return pd.NA

    return '{}-{:02d}-{:03d}-{:02d}'.format(
        str(depto).strip(), int(mupio), int(sector), int(segmento))


def cluster_features(df):
    """Collapse the household-level extraction to exactly one row per PSU.

    df_edit hook for ``cluster_features`` (GH #323).  The source file is
    household-level (7,276 rows) but cluster_features' canonical index is
    (t, v) -- one row per cluster.  The old wiring declared ``v: region`` (only
    8 distinct values) *and* leaked ``i: hogar`` into idxvars, so the framework
    received 7,276 rows on a declared (t, v) index and
    ``_normalize_dataframe_index`` collapsed them with ``groupby().first()``.
    Since every one of the 8 regions contains both urban and rural households,
    that ``.first()`` was not a dedup but an ARBITRARY pick: it stamped 7
    regions "Urban" and one "Rural" purely by row order, leaving 3,591 of 7,276
    households (49.4%) in a cluster whose Rural flag contradicted their own.
    That is silently-WRONG data, not merely missing.

    With v = the true PSU, Rural and Region are genuine functions of v, so the
    collapse is exact.  We do not *assume* that -- we ENFORCE it: if any
    payload column ever varies within a cluster we raise, rather than silently
    picking one value.  A comment is documentation; this is enforcement.
    """
    flat = df.reset_index()
    if 'v' not in flat.columns:
        raise ValueError(
            "Guatemala cluster_features: no `v` level/column in the extracted "
            f"frame (got {list(flat.columns)}).  Check idxvars in "
            "2000/_/data_info.yml."
        )

    # Rows with no identifiable cluster cannot be placed.  Drop them LOUDLY:
    # silently-missing beats silently-wrong, but neither should be silent.
    unplaced = int(flat['v'].isna().sum())
    if unplaced:
        warnings.warn(
            f"Guatemala cluster_features: dropping {unplaced} row(s) with no "
            "identifiable PSU (missing depto/mupio/sector/segmento).",
            RuntimeWarning,
        )
        flat = flat[flat['v'].notna()]

    keys = [c for c in ('t', 'v') if c in flat.columns]
    payload = [c for c in flat.columns if c not in keys and c != 'i']

    # ENFORCE that every payload column is constant within a cluster, so the
    # dedup below is provably lossless instead of an arbitrary pick.
    if payload:
        varying = (flat.groupby(keys, observed=True)[payload]
                       .nunique(dropna=False)
                       .max())
        bad = [c for c in payload if int(varying.get(c, 0)) > 1]
        if bad:
            raise ValueError(
                f"Guatemala cluster_features: column(s) {bad} are not constant "
                "within a cluster (t, v).  Collapsing would silently discard "
                "real variation (GH #323).  The PSU key or the column set is "
                "wrong -- fix the wiring rather than letting groupby().first() "
                "pick a value arbitrarily."
            )

    out = flat.drop_duplicates(subset=keys).set_index(keys)
    return out.drop(columns=[c for c in ('i',) if c in out.columns])


def _household_roster_from_df(df, sex, age, HHID, sex_converter=None, age_converter=None,
                               months_spent='months_spent', Age_ints=None):
    """Inline replacement for lsms.tools.get_household_roster(fn_type=None)."""
    cols = [c for c in [HHID, sex, age, months_spent] if c in df.columns]
    df = df.loc[:, cols].rename(columns={HHID: 'HHID', sex: 'sex', age: 'age',
                                          months_spent: 'months_spent'})
    if sex_converter is not None:
        df['sex'] = df['sex'].apply(sex_converter)
    df = df.dropna(how='any')
    df['sex'] = df['sex'].apply(lambda s: str(s[0]).lower())
    if age_converter is not None:
        df['age'] = df['age'].apply(age_converter)
    df['boys']  = (df['sex'] == 'm') & (df['age'] < 18)
    df['girls'] = (df['sex'] == 'f') & (df['age'] < 18)
    df['men']   = (df['sex'] == 'm') & (df['age'] >= 18)
    df['women'] = (df['sex'] == 'f') & (df['age'] >= 18)
    if Age_ints is None:
        Age_ints = ((0,1),(1,5),(5,10),(10,15),(15,20),(20,30),(30,50),(50,60),(60,100))
    valvars = list({'HHID','girls','boys','men','women'}.intersection(df.columns))
    for lo, hi in Age_ints:
        s, e = lo, hi - 1
        df['Males %02d-%02d' % (s, e)]   = (df['sex'] == 'm') & (df['age'] >= lo) & (df['age'] < hi)
        df['Females %02d-%02d' % (s, e)] = (df['sex'] == 'f') & (df['age'] >= lo) & (df['age'] < hi)
        valvars += ['Males %02d-%02d' % (s, e), 'Females %02d-%02d' % (s, e)]
    try:
        if df['HHID'].iloc[0].split('.')[-1] == '0':
            df['HHID'] = df['HHID'].apply(lambda x: '%d' % int(float(x)))
    except (ValueError, AttributeError):
        pass
    if 'months_spent' in df.columns and df['months_spent'].count() > 0:
        g = df.loc[df['months_spent'] > 0, valvars].groupby('HHID')
    else:
        g = df[valvars].groupby('HHID')
    return g.sum()


def age_sex_composition(df, sex, sex_converter, age, age_converter, hhid):
    Age_ints = ((0,4),(4,9),(9,14),(14,19),(19,31),(31,51),(51,100))
    testdf = _household_roster_from_df(df, sex=sex, age=age, HHID=hhid,
                                       sex_converter=sex_converter,
                                       age_converter=age_converter,
                                       Age_ints=Age_ints)
    testdf['log HSize'] = np.log(testdf[['girls', 'boys', 'men', 'women']].sum(axis=1))
    testdf.index.name = 'j'
    return testdf


def harmonized_food_labels(fn='../../_/food_items.csv',key='Code',value='Preferred Label'):
    # Harmonized food labels
    food_items = pd.read_csv(fn,delimiter='|',skipinitialspace=True,converters={1:lambda s: s.strip(),2:lambda s: s.strip()})
    food_items.columns = [s.strip() for s in food_items.columns]
    food_items = food_items.loc[:,food_items.count()>0]
    food_items = food_items.apply(lambda x: x.str.strip())

    if type(key) is not str:  # Assume a series of foods
        myfoods = set(key.values)
        for key in food_items.columns:
            if len(myfoods.difference(set(food_items[key].values)))==0: # my foods all in key
                break

    food_items = food_items[[key,value]].dropna()
    food_items = food_items.set_index(key)

    return food_items.squeeze().str.strip().to_dict()


def individual_education(df):
    """Refine Guatemala ENCOVI 2000 Educational Attainment with the grade year.

    df_edit hook for ``individual_education`` (GH #493).  By the time this
    runs, the YAML ``mappings: harmonize_education`` table has already mapped
    the bare ``p07b27a`` nivel labels onto *coarse* canonical levels:

        ninguno -> None,  preparatoria -> Pre-primary,
        primaria -> "Primary complete",  educacion media -> "Lower secondary",
        educacion superior -> Bachelor,  post-grado -> Postgraduate,
        educacion adultos -> Informal,  nivel 9 -> Unknown.

    Two levels need the grade year ``p07b27b`` (carried in ``edu_grade``) to
    place them on the ordinal scale; this hook supplies that refinement and
    drops the ``edu_grade`` helper column:

      * Primary  (6-year cycle): grado 1-5 -> "Primary incomplete",
        grado >=6 -> "Primary complete" (the coarse default, unchanged).
      * Secondary "educacion media" = ciclo basico (years 1-3, lower
        secondary) + ciclo diversificado (years 4-6, upper secondary):
            grado 1-2 -> "Lower secondary"          (some basico)
            grado 3   -> "Lower secondary complete" (basico finished)
            grado 4-5 -> "Upper secondary"          (some diversificado)
            grado >=6 -> "Upper secondary complete" (diversificado finished)

    Rows with a missing/zero grade keep the coarse entry-tier label.
    """
    grade = pd.to_numeric(df.get('edu_grade'), errors='coerce')

    # nivel code 9 is unlabelled in Stata, so convert_categoricals leaves it as
    # the numeric value 9.0 -- the extraction-time `mappings:` table (string
    # keys) can't reach it before it is stringified downstream.  Map it to
    # Unknown here, where it is still numeric.  ``replace`` matches 9 / 9.0 /
    # "9" / "9.0" so it is robust to either dtype.
    att = df['Educational Attainment']
    df['Educational Attainment'] = att = att.replace(
        {9: 'Unknown', 9.0: 'Unknown', '9': 'Unknown', '9.0': 'Unknown'})

    prim = att == 'Primary complete'
    df.loc[prim & (grade < 6), 'Educational Attainment'] = 'Primary incomplete'

    media = att == 'Lower secondary'
    df.loc[media & (grade == 3), 'Educational Attainment'] = 'Lower secondary complete'
    df.loc[media & (grade >= 4) & (grade <= 5), 'Educational Attainment'] = 'Upper secondary'
    df.loc[media & (grade >= 6), 'Educational Attainment'] = 'Upper secondary complete'

    if 'edu_grade' in df.columns:
        df = df.drop(columns='edu_grade')
    return df
