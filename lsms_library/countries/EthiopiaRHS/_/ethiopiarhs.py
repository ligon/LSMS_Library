# Formatting functions for EthiopiaRHS (ERHS).
#
# Formatting helpers ONLY -- intentionally NO `waves`/`Waves` dict, so
# Country.waves falls through to SOURCE.org auto-discovery
# (country.py:1054-1073). See _/CONTENTS.org and GH #271.
#
# ERHS `hhid` (HH no. for this survey) is unique only WITHIN a peasant
# association, so household identity is the composite (village, hhid)
# -- the EHCVM (grappe, menage) pattern. Pure YAML cannot express a
# composite key; the framework needs a named `i` formatter for the
# list-valued idxvar (see Mali/_/mali.py for the analogous helper).

import numpy as np
import pandas as pd

import lsms_library.local_tools as tools


def _norm_village(v):
    """Canonicalize a peasant-association name across source files.

    Village appears as 'Haresaw' in the demographics file (demo123)
    but 'HARESAW' in the food file (food1).  Title-case both so the
    composite household id joins across modules.  '.title()' leaves
    the already-title 'Haresaw' unchanged.
    """
    return str(v).strip().title()


def i(value):
    """Household id formatter, dual-path.

    Composite path (1994+ waves): ``value`` is a row Series
    ``[village, hh_no]`` -- ERHS ``hhid``/``q5``/``q2`` is unique only
    within a village, so identity is the pair, joined as
    ``<village>_<hh_no>``.

    Scalar path (1989): ``value`` is a single already-unique household
    key (the packed 5-digit ``hhid`` = region+PA+HH; one column, fed
    per-cell by ``df[col].apply``).  Just format it.

    A row missing a required component cannot be assigned to a
    household (observed: a food3.dta row with NaN HH number that still
    carries food data).  Return ``pd.NA`` rather than crashing; such
    rows are dropped downstream (the food melt filters them; roster
    NA-index rows fail the sanity no-null-index check, surfacing
    genuinely bad data).
    """
    # Composite: a 2+-element row Series from df[[village, hh]].apply.
    if hasattr(value, 'iloc') and getattr(value, 'size', 1) >= 2:
        v0, v1 = value.iloc[0], value.iloc[1]
        if pd.isna(v0) or pd.isna(v1):
            return pd.NA
        return tools.format_id(_norm_village(v0)) + '_' + tools.format_id(v1)
    # Scalar: single packed household key (1989 hhid).
    if pd.isna(value):
        return pd.NA
    return tools.format_id(value)


def food_acquired(df):
    """Melt ERHS wide per-source food columns into canonical long form.

    ERHS records, per (household, item), separate quantities + units
    for three acquisition sources.  This is a new pattern: the
    framework helper (transformations.food_acquired_to_canonical)
    assumes a single unit with purchased = Total - Produced and no
    in-kind source, which ERHS does not satisfy.

    Input (df_edit hook, pre-`t`): index (i, j); columns the source
    triplets named q_/u_/e_ for purchased, produced, inkind.

    Output: index (i, j, u, s), columns [Quantity, Expenditure];
    s in {purchased, produced, inkind}.  The framework prepends `t`
    (check_adding_t) and joins `v` from sample() at API time.

    Pass-through path (1989): food89.dta is already long -- one row
    per (hh, item) with a single `source` code, mapped to `s` in the
    wave's data_info.yml, so the frame arrives canonical (no q_/u_/e_
    triplet columns).  Detect that (no 'q_purch'), keep only canonical
    s values (drops the NaN/5/6/7 source rows that have no clean
    purchased/produced/inkind meaning), drop unidentifiable rows, and
    return as-is.
    """
    CANON_S = ('purchased', 'produced', 'inkind')
    flat = df.reset_index()
    if 'q_purch' not in flat.columns:
        idx = [c for c in ('i', 'j', 'u', 's') if c in flat.columns]
        flat = flat[flat['s'].isin(CANON_S)]
        for k in ('i', 'j', 'u'):
            if k in flat.columns:
                flat = flat[flat[k].notna() & (flat[k].astype('string')
                                               .str.strip() != '')]
        return flat.set_index(idx)

    w = df.reset_index()
    specs = [('purchased', 'q_purch', 'u_purch', 'e_purch'),
             ('produced',  'q_prod',  'u_prod',  None),
             ('inkind',    'q_inkind', 'u_inkind', None)]
    frames = []
    for s, q, u, e in specs:
        sub = pd.DataFrame({
            'i': w['i'].values,
            'j': w['j'].values,
            'u': w[u].astype('string').str.strip(),
            's': s,
            'Quantity': pd.to_numeric(w[q], errors='coerce'),
            'Expenditure': (pd.to_numeric(w[e], errors='coerce')
                            if e else np.nan),
        })
        # Keep a row only if it carries a measurement.
        sub = sub[(sub['Quantity'].fillna(0) > 0)
                  | (sub['Expenditure'].fillna(0) > 0)]
        frames.append(sub)
    out = pd.concat(frames, ignore_index=True)
    # Drop rows with no household id (i is pd.NA when the HH-number
    # key was missing in the source -- see `i` above) and rows with
    # no unit (a unit is required to place the quantity on the u axis).
    out = out[out['i'].notna()]
    out = out[out['u'].notna() & (out['u'] != '')]
    out = out.set_index(['i', 'j', 'u', 's'])
    return out


def sample(df):
    """Dedup the per-person extract to one row per household.

    ERHS has no household-level cover file, so `sample` is extracted
    from the per-person roster (i + v=village).  Collapse to HH grain
    (first row per i).  ERHS is a PURPOSIVE 15-village panel with no
    survey *design* weights; rather than NaN we use UNIFORM weights of
    1.0 (self-weighting -- each household counts once, giving
    unweighted means and not breaking downstream weighted-aggregation
    code).  These are explicitly NOT expansion factors; sourcing any
    constructed ERHS weights remains a documented follow-up.  strata =
    village (the panel's PSU); Rural = 'Rural' (it is the *Rural*
    Household Survey -- all rural).
    """
    flat = df.reset_index()
    flat = flat[flat['i'].notna()].drop_duplicates(subset='i', keep='first')
    # v is a myvar here (no auto format_id) but is an idxvar in
    # cluster_features (auto format_id'd) -- format it the same way so
    # the market join (_add_market_index) matches ('1' == '1', not
    # '1.0' != '1').
    flat['v'] = flat['v'].map(tools.format_id)
    flat['weight'] = 1.0
    flat['panel_weight'] = 1.0
    flat['strata'] = flat['v'].astype('string')
    flat['Rural'] = 'Rural'
    return flat.set_index(['i'])[['v', 'weight', 'panel_weight',
                                  'strata', 'Rural']]


def cluster_features(df):
    """Dedup the per-person extract to one row per village (v).

    Region comes from the erhs_village_region map applied in the wave
    data_info (per-person rosters lack q1a).  Collapse to one row per
    v and stamp Rural='Rural'.
    """
    flat = df.reset_index()
    flat = flat[flat['v'].notna() & (flat['v'].astype('string').str.strip()
                                     != '')]
    flat = flat.drop_duplicates(subset='v', keep='first')
    flat['Rural'] = 'Rural'
    return flat.set_index(['v'])[['Region', 'Rural']]
