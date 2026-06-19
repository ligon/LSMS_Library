# Formatting functions for EthiopiaRHS (ERHS).
#
# ERHS `hhid` (HH no. for this survey) is unique only WITHIN a peasant
# association, so household identity is the composite (village, hhid)
# -- the EHCVM (grappe, menage) pattern. Pure YAML cannot express a
# composite key; the framework needs a named `i` formatter for the
# list-valued idxvar (see Mali/_/mali.py for the analogous helper).

import numpy as np
import pandas as pd

import lsms_library.local_tools as tools


# Explicit `waves` list (consumed by Country.waves, country.py:1054-1073).
# All 8 ERHS rounds are wired:
#  - 1989/1994a/1994b/1995/1997 (R1-R4): person roster + item-level food.
#  - 1999 (R5): person roster ("11 3.3.99 roster.tab" -> 1999/Data/roster.tab,
#    10,788 person-rows) wired for household_roster + sample/cluster_features;
#    bespoke (t,i) livestock + income from the R5 lvs5/inc5 modules.
#  - 2004 (R6) / 2009 (R7): the IFPRI archive has NO person roster and NO
#    item-level food for R6/R7 -- only pre-aggregated HH-level scalars -- so
#    they are wired as bespoke HH-level (t,i) aggregate waves (consumption,
#    livestock, area_output, and 2009-only hhsize), with sample +
#    cluster_features derived from the consumption-aggregate file's
#    paid/pa (= peasant-association code 1..20) + the erhs_village_* Code
#    tables.  They do NOT fake a roster/food_acquired the data can't support.
# Item-level food_acquired stays 1994-1997 only (R1-R4).
# See _/CONTENTS.org and GH #271 / #277.
waves = ['1989', '1994a', '1994b', '1995', '1997', '1999', '2004', '2009']


# Unit handling (GH #347).  ``u`` carries the harmonized unit label
# ALONE -- the ``harmonize_unit`` Preferred Label for the 1994+ waves
# (1->kg, 2->"100 kg", 3->Chinet, 19->litre, 30->Birr, ...) and the
# raw in-data unit *code* for 1989 (an unlabelled scheme; still a unit
# token, not a food label).  An EARLIER design appended the food label
# to non-metric units ("Chinet [Butter]") to coax the framework's
# price-ratio kg inference into a per-good factor; that polluted the
# unit axis with ITEM NAMES (e.g. '0 [Butter]', '5 [Teff]') and is the
# bug reported in #347.  Removed: ``u`` is now a clean unit token, as
# in every other country (Uganda/Malawi/EHCVM).  The framework's
# price-ratio inference (transformations.conversion_to_kgs) already
# groups by item ``i`` within each unit, so a single unit label per
# good still recovers sensible factors with no per-good tagging.


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
        # 1989 unitcode is an in-data-unlabelled scheme (harmonize_unit
        # not applied), so ``u`` carries the raw unit *code* here -- a
        # unit token on the unit axis, NOT a food label (#347).  1989
        # metric units are therefore not distinguished (documented;
        # would need Codeunit1.SPS).
        if 'u' in flat.columns:
            flat['u'] = flat['u'].astype('string').str.strip()
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
    # ``u`` is the harmonize_unit Preferred Label (e.g. 'kg', 'Chinet',
    # 'Birr'); the source missing-unit sentinel 0 maps to '' via
    # harmonize_unit (#347) and is dropped here.  Each row's ``u`` is a
    # clean unit token -- no food label appended (the framework's
    # price-ratio inference recovers per-good factors on its own).
    out['u'] = out['u'].astype('string').str.strip()
    out = out[out['i'].notna()]
    out = out[out['u'].notna() & (out['u'] != '')]
    out = out.set_index(['i', 'j', 'u', 's'])
    return out


def sample(df):
    """Dedup the per-person / per-HH extract to one row per household.

    ERHS has no household-level cover file, so `sample` is extracted from
    the per-person roster (1989-1999) or the HH-level consumption
    aggregate file (2004/2009): i + v=village.  Collapse to HH grain
    (first row per i).  ERHS is a PURPOSIVE 15-village panel with no
    survey *design* weights; rather than NaN we use UNIFORM weights of
    1.0 (self-weighting -- each household counts once, giving
    unweighted means and not breaking downstream weighted-aggregation
    code).  These are explicitly NOT expansion factors; sourcing any
    constructed ERHS weights remains a documented follow-up.  strata =
    village (the panel's PSU); Rural = 'Rural' (it is the *Rural*
    Household Survey -- all rural).

    R6-POPULATION FILTER (2004).  consumptionaggregates_123456.tab is a
    pooled R1-R6 file (1598 rows); only the rows with non-NaN cons6 are
    the R6 population (1368 HH).  A wave may pass an optional `_present`
    helper myvar (e.g. _present: cons6) -- rows where it is NaN are
    dropped so the 230 out-of-round HH are not carried into the 2004
    sample (matches the consumption block's R6 filter).  Waves that do
    not provide `_present` (1989-1999, 2009) are unaffected.
    """
    flat = df.reset_index()
    if '_present' in flat.columns:
        flat = flat[flat['_present'].notna()]
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
    """Dedup the per-person/per-HH extract to one row per village (v).

    Region (named, via erhs_village_region) and Woreda (real in-data
    name, via erhs_village_woreda) are applied in the wave data_info.
    1989 supplies neither (demog89_1 has no q1b/q1a) -- keep whichever
    of Region/Woreda the wave provided so the country-level concat
    fills the rest with NaN.  Collapse to one row per v; Rural='Rural'.

    For 2004/2009 the source is the HH-level aggregate file (one row per
    HH); v = paid/pa (the same numeric peasant-association code 1..20 as
    q1c), so the dedup-to-village + erhs_village_* Code-table mapping
    yields the SAME canonical Region/Woreda spellings as every other wave
    (geography is consistent across all 8 waves; 2009's slightly-variant
    in-data woreda spellings -- 'Tsibi Wonberat' vs 'Atsbi' -- are NOT
    used, to avoid splitting a village's Woreda label across waves).
    """
    flat = df.reset_index()
    flat = flat[flat['v'].notna() & (flat['v'].astype('string').str.strip()
                                     != '')]
    flat = flat.drop_duplicates(subset='v', keep='first')
    flat['Rural'] = 'Rural'
    keep = [c for c in ('Region', 'Woreda') if c in flat.columns] + ['Rural']
    return flat.set_index(['v'])[keep]


def _drop_all_nan_value_rows(df):
    """Drop rows whose every value column (non-index) is NaN.

    Shared helper for the 2004/2009 HH-level aggregate tables.  The
    pooled R1-R6 aggregate files (e.g. consumptionaggregates_123456.tab,
    1598 rows) carry HH not present in the round being sliced: only the
    rows with a non-NaN value in the round-suffixed column (cons6, 1368
    rows for R6) are the round's population.  After myvar extraction the
    out-of-round rows are all-NaN, so dropping rows that are all-NaN
    across the value columns recovers exactly the round population (the
    documented R6-population filter).  For single-round files (the 2009
    *_r7 files) this drops nothing.
    """
    flat = df.reset_index()
    idx = list(df.index.names)
    valcols = [c for c in flat.columns if c not in idx]
    flat = flat.dropna(subset=valcols, how='all')
    return flat.set_index(idx)


def consumption(df):
    """R6/R7 HH-level consumption aggregates: drop out-of-round rows.

    See _drop_all_nan_value_rows.  For 2004 this filters the 1598-row
    pooled file down to the 1368 R6-population HH (cons6 non-null); for
    2009 (consumptionAggrgates_r7.tab, already single-round) it is a
    no-op.
    """
    return _drop_all_nan_value_rows(df)


def hhsize(df):
    """2009 HH-size summary: drop any all-NaN value rows (no-op in practice)."""
    return _drop_all_nan_value_rows(df)


def anthropometry(df):
    """Individual (t, i, pid) body measures, one round-slice per wave (#438).

    Source Anthropometry_R1-R4.tab is a POOLED wide file carrying every
    measured person across rounds 1-4 in round-suffixed columns; each wave's
    data_info extracts THAT round's height/weight/age (+ roster sex), so the
    framework hands this hook a frame indexed (i, pid) with columns
    [Height, Weight, Age_months, Sex] in which the out-of-round persons (not
    measured in this round) are all-NaN on the measures.  Two fixes:

    1. Drop the out-of-round rows -- those with NO height/weight/age at all
       (Sex is roster-sourced and present for everyone, so an all-value-cols
       drop would keep them; restrict the all-NaN test to the MEASURES).
    2. ``Age_months`` arrives as raw AGE IN YEARS (the source ``age{r}``
       column); multiply by 12 to match the cross-country anthropometry
       convention (Ethiopia ESS / Malawi / Uganda).  ERHS-shipped WHO
       z-scores (haz/waz/whz) are query-time transforms and are NOT carried.

    NA-index rows (a person with no q1c/q1d/pid) cannot be placed in a
    household -> dropped (they would fail the sanity no-null-index check).
    """
    idx = list(df.index.names)
    flat = df.reset_index()

    measures = [c for c in ('Height', 'Weight', 'Age_months') if c in flat.columns]
    flat = flat.dropna(subset=measures, how='all')

    # Age years -> months (cross-country convention).
    if 'Age_months' in flat.columns:
        flat['Age_months'] = pd.to_numeric(flat['Age_months'], errors='coerce') * 12

    # Drop rows that cannot be assigned to a (i, pid) cell.
    for k in idx:
        flat = flat[flat[k].notna()
                    & (flat[k].astype('string').str.strip() != '')]

    return flat.set_index(idx)


def plot_features(df):
    """Resolve (i, plot_id) collisions in ERHS land1all.tab (GH #513).

    land1all.tab is non-unique on (q1c, q5, q21_1).  Of the 15 collision
    groups carrying a non-null plot_id, 12 pair a placeholder "land TOTAL"
    row (zero/NaN Area, all plot attributes NaN) with a real plot that
    reuses its q21_1; under the downstream ``groupby().first()`` the
    placeholder can sort first and SHADOW the genuine plot, leaking Area=0
    into the API.  The remaining ~2 groups are genuinely-distinct plots
    that happen to reuse a plot_id (e.g. HH Shumsha_301 plots 1 & 2, areas
    1.00 vs 0.25 ha).

    Fix: drop the zero-area attribute-less placeholders (the documented
    "land TOTAL" residual), then cumcount-suffix any residual
    genuinely-distinct collisions -- Albania precedent (albania.py:287-295)
    -- so each physical plot keeps its own row instead of being dropped.
    Single-country, single-wave; no canonical-index change (suffixes are
    just ``_2`` on the few real collisions).
    """
    flat = df.reset_index()
    area = pd.to_numeric(flat['Area'], errors='coerce')
    attr_cols = [c for c in ('Tenure', 'SoilType', 'Irrigated')
                 if c in flat.columns]
    attrs_all_nan = (flat[attr_cols].isna().all(axis=1)
                     if attr_cols else pd.Series(True, index=flat.index))
    placeholder = (area.isna() | (area == 0)) & attrs_all_nan
    flat = flat[~placeholder].copy()

    key = [c for c in ('i', 'plot_id') if c in flat.columns]
    if key and flat.duplicated(key, keep=False).any():
        # Larger plot keeps the bare plot_id; smaller distinct plot -> _2.
        flat = flat.sort_values('Area', ascending=False, na_position='last')
        n = flat.groupby(key, dropna=False).cumcount()
        extra = n > 0
        flat.loc[extra, 'plot_id'] = (
            flat.loc[extra, 'plot_id'].astype('string')
            + '_' + (n[extra] + 1).astype('string'))

    idx = [c for c in ('t', 'i', 'plot_id') if c in flat.columns]
    return flat.set_index(idx)


_CROP_LABELS = {
    'wtef': 'White Teff', 'btef': 'Black Teff', 'barl': 'Barley',
    'wht': 'Wheat', 'maiz': 'Maize', 'sorg': 'Sorghum',
    'coff': 'Coffee', 'chat': 'Chat', 'enset': 'Enset',
}


def crop_production(df):
    """Melt the wide ERHS HH x crop area_output_* slice to long (t,i,j) (#438).

    Each wave's data_info extracts that round's per-crop production (kg) and
    area (ha) as {stub}_kg / {stub}_ha myvars over an (i,) index.  Reshape to
    one row per (i, j) crop: Quantity (kg), Area_ha, u='Kg'.  ERHS aggregates
    are HH x crop (already summed over plots) -> reduced (t,i,j) grain, no
    plot dimension.  Framework prepends t and joins v from sample().
    """
    idx = list(df.index.names)            # ('i',)
    flat = df.reset_index()
    parts = []
    for stub, label in _CROP_LABELS.items():
        kg, ha = f'{stub}_kg', f'{stub}_ha'
        if kg not in flat.columns and ha not in flat.columns:
            continue
        part = flat[idx].copy()
        part['j'] = label
        part['Quantity'] = (pd.to_numeric(flat[kg], errors='coerce')
                            if kg in flat.columns
                            else pd.Series(np.nan, index=flat.index))
        part['Area_ha'] = (pd.to_numeric(flat[ha], errors='coerce')
                           if ha in flat.columns
                           else pd.Series(np.nan, index=flat.index))
        parts.append(part)
    out = pd.concat(parts, ignore_index=True)
    out = out[out['Quantity'].notna() | out['Area_ha'].notna()]
    out['u'] = 'Kg'
    out = out[out['j'].notna()]
    for k in idx:
        out = out[out[k].notna() & (out[k].astype('string').str.strip() != '')]
    out = out.set_index(idx + ['j', 'u'])   # u is a grouping key, not a column
    out = out.groupby(level=out.index.names).first()   # defensive de-dupe
    return out
