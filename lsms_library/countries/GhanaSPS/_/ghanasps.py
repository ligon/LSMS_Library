import pandas as pd
import numpy as np
from collections import defaultdict
from lsms_library.local_tools import get_dataframe, DVCFS

# Data to link household ids across waves
Waves = {'2009-10':(),
         '2013-14':(),
         '2017-18':('00_hh_info.dta', 'FPrimary', 'FPrimary_original')
         }

def harmonized_food_labels2(fn='../../_/food_items.org'):
    # Harmonized food labels
    food_items = pd.read_csv(fn,delimiter='|',skipinitialspace=True,converters={1:int,2:lambda s: s.strip()})
    food_items.columns = [s.strip() for s in food_items.columns]
    food_items = food_items[['Code','Preferred Label']].dropna()
    food_items = food_items.set_index('Code')

    return food_items.to_dict()['Preferred Label']

def harmonized_food_labels(fn='../../_/food_items.org',key=list(Waves.keys()),value='Preferred Label'):
    # Harmonized food labels
    food_items = pd.read_csv(fn,delimiter='|',skipinitialspace=True,converters={1:lambda s: s.strip(),2:lambda s: s.strip()})
    food_items.columns = [s.strip() for s in food_items.columns]
    food_items = food_items.loc[:,food_items.count()>0]
    food_items = food_items.drop(columns = ['Food Codes','FCT Label']).apply(lambda x: x.str.strip())

    if type(key) == list :
        for k in key:
            if type(k) is not str:  # Assume a series of foods
                myfoods = set(k.values)
                for k in food_items.columns:
                    if len(myfoods.difference(set(food_items[k].values)))==0: # my foods all in key
                        break

        food_items = food_items[key + [value]].replace('---', pd.NA).dropna(how = 'all')
    else:
        food_items = food_items[[key] + [value]].replace('---', pd.NA).dropna(how = 'all')
        
    food_items = food_items.set_index(key)

    return food_items.squeeze().str.strip().to_dict()

def _sum_expenditures_from_file(fn, purchased, away, produced, given, itmcd, HHID,
                                 units=None, itemlabels=None, convert_categoricals=False):
    """Inline replacement for lsms.tools.get_food_expenditures (file-opening path)."""
    df = get_dataframe(fn, convert_categoricals=convert_categoricals)
    sources = {'purchased': purchased, 'away': away, 'produced': produced, 'given': given}
    varnames = {v: k for k, v in sources.items() if v is not None}
    varnames[HHID] = 'HHID'
    varnames[itmcd] = 'itmcd'
    if units is not None:
        varnames[units] = 'units'
    df = df.rename(columns=varnames)
    value_cols = [k for k, v in sources.items() if v is not None]
    for col in value_cols:
        df[col] = df[col].astype(np.float64)
    try:
        df['itmcd'] = df['itmcd'].astype(float)
        df = df.loc[~np.isnan(df['itmcd'])]
        df['itmcd'] = df['itmcd'].astype(int)
    except (ValueError, TypeError):
        pass
    if itemlabels is not None:
        df = df.replace({'itmcd': itemlabels})
    valvars = ['HHID', 'itmcd'] + value_cols
    if units is not None:
        df['units'] = df['units'].fillna(0).astype(int)
        g = df.loc[:, valvars + ['units']].groupby(['HHID', 'units', 'itmcd'])
        x = g.sum().sum(axis=1).unstack('itmcd')
    else:
        g = df.loc[:, valvars].groupby(['HHID', 'itmcd'])
        x = g.sum().sum(axis=1).unstack('itmcd')
    x = x.fillna(0)
    if itemlabels is not None:
        x = x.loc[:, x.columns.isin(itemlabels.values())]
    return x


def _household_roster_from_file(fn, sex='sex', age='age', HHID='HHID',
                                  months_spent='months_spent', sex_converter=None,
                                  months_converter=None, Age_ints=None,
                                  convert_categoricals=True):
    """Inline replacement for lsms.tools.get_household_roster (file-opening path)."""
    df = get_dataframe(fn, convert_categoricals=convert_categoricals)
    cols = [c for c in [HHID, sex, age, months_spent] if c in df.columns]
    df = df.loc[:, cols].rename(columns={HHID: 'HHID', sex: 'sex', age: 'age',
                                          months_spent: 'months_spent'})
    if months_converter is not None:
        df['months_spent'] = df['months_spent'].apply(months_converter)
    if sex_converter is not None:
        df['sex'] = df['sex'].apply(sex_converter)
    df = df.dropna(how='any')
    df['sex'] = df['sex'].apply(lambda s: str(s[0]).lower())
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


def prices_and_units(fn='',units='units',item='item',HHID='HHID',market='market',farmgate='farmgate'):

    df = get_dataframe(fn, convert_categoricals=True)

    # Unit labels from Stata value labels (need a stream, not a DataFrame)
    with DVCFS.open(fn) as dta:
        sr = pd.io.stata.StataReader(dta)
        try:
            unitlabels = sr.value_labels()[units]
        except KeyError:
            foo = sr.value_labels()
            key = [k for k,v in foo.items() if 'Kilogram' in [u[:8] for l,u in v.items()]][0]
            unitlabels = sr.value_labels()[key]

    df = df.rename(columns={HHID: 'HHID', item: 'itmcd', farmgate: 'farmgate',
                             market: 'market', units: 'units'})
    if df['itmcd'].dtype in (float, int):
        try:
            df['itmcd'] = df['itmcd'].astype(float)
            df = df.loc[~np.isnan(df['itmcd'])]
            df['itmcd'] = df['itmcd'].astype(int)
        except (ValueError, TypeError):
            pass
    prices = df.loc[:, ['HHID', 'itmcd', 'farmgate', 'market', 'units']].set_index(['HHID', 'itmcd'])
    prices = prices.replace({'units': unitlabels})
    prices.units = prices.units.astype(str)

    pd.Series(unitlabels).to_csv('unitlabels.csv')

    return prices

def food_expenditures(fn='',purchased=None,away=None,produced=None,given=None,item='item',HHID='HHID'):

    expenditures = _sum_expenditures_from_file(fn, purchased, away, produced, given,
                                                itmcd=item, HHID=HHID)
    return expenditures

def food_quantities(fn='',item='item',HHID='HHID',
                    purchased=None,away=None,produced=None,given=None,units=None):

    quantities = _sum_expenditures_from_file(fn, purchased, away, produced, given,
                                              itmcd=item, HHID=HHID, units=units,
                                              convert_categoricals=True)
    return quantities

def age_sex_composition(fn,sex='sex',sex_converter=None,
                        age='age',months_spent='months_spent',HHID='HHID',months_converter=None, convert_categoricals=True,Age_ints=None,fn_type='stata'):

    df = _household_roster_from_file(fn, sex=sex, age=age, HHID=HHID,
                                      months_spent=months_spent,
                                      sex_converter=sex_converter,
                                      months_converter=months_converter,
                                      convert_categoricals=convert_categoricals)
    df.index.name = 'j'
    df.columns.name = 'k'

    return df

def household_characteristics(fn='',sex='',age='',HHID='HHID',months_spent='months_spent'):

    if type(sex) in [list,tuple]:
        sex,sex_converter = sex
    else:
        sex_converter = None

    df = _household_roster_from_file(fn, sex=sex, age=age, HHID=HHID,
                                      months_spent=months_spent,
                                      sex_converter=sex_converter)
    df.index.name = 'j'
    df.columns.name = 'k'
    df['log HSize'] = np.log(df[['girls', 'boys', 'men', 'women']].sum(axis=1))

    return df


def change_id(x,fn=None,id0=None,id1=None,transform_id1=None):
    """Replace instances of id0 with id1.

    The identifier id0 is assumed to be unique.

    If mapping id0->id1 is not one-to-one, then id1 modified with
    suffixes of the form _%d, with %d replaced by a sequence of
    integers.
    """
    idx = x.index.names

    if fn is None:
        x = x.reset_index()
        if x['j'].dtype==float:
            x['j'] = x['j'].astype(str).apply(lambda s: s.split('.')[0]).replace('nan',pd.NA)
        elif x['j'].dtype==int:
            x['j'] = x['j'].astype(str)
        elif x['j'].dtype==str:
            x['j'] = x['j'].replace('',pd.NA)

        x = x.set_index(idx)

        return x

    id = get_dataframe(fn)

    id = id[[id0,id1]]
    id[id1] = id[id1].replace('', pd.NA).fillna(id[id0])

    for column in id:
        if id[column].dtype==float:
            id[column] = id[column].astype(str).apply(lambda s: s.split('.')[0]).replace('nan',pd.NA)
        elif id[column].dtype==int:
            id[column] = id[column].astype(str).replace('nan',pd.NA)
        elif id[column].dtype==object:
            id[column] = id[column].replace('nan',pd.NA)
            id[column] = id[column].replace('',pd.NA)

    ids = dict(id[[id0,id1]].values.tolist())

    if transform_id1 is not None:
        ids = {k:transform_id1(v) for k,v in ids.items()}

    d = defaultdict(list)

    for k,v in ids.items():
        d[v] += [k]

    try:
        d.pop(np.nan, None)  # Get rid of nan key, if any
        d.pop(pd.NA, None)
    except KeyError: pass

    updated_id = {}
    for k,v in d.items():
        if len(v)==1: updated_id[v[0]] = k
        else:
            for it,v_element in enumerate(v):
                updated_id[v_element] = '%s_%d' % (k,it)

    x = x.reset_index()
    x['j'] = x['j'].map(updated_id).fillna(x['j'])
    x = x.set_index(idx)

    assert x.index.is_unique, "Non-unique index."

    return x


# ---------------------------------------------------------------------------
# individual_education (GH #171)
# ---------------------------------------------------------------------------
#
# `df_edit` hook for the `individual_education` table, shared by ALL THREE
# waves: `Wave.formatting_functions` starts from `Country.formatting_functions`
# (which loads this module, `ghanasps.py`) and only then overlays the wave's own
# `mapping.py`, so defining the hook once here binds it everywhere.  Each wave's
# `data_info.yml` supplies the same three columns under the same names, which is
# what makes one implementation legitimate rather than merely convenient.
#
# WHY A HOOK AT ALL.  The YAML `mapping:` machinery maps ONE source column onto
# one target.  GSPS splits the attainment answer across three:
#
#   attended     ever attended school (Yes/No)
#   highestgrade highest grade successfully completed  -- the primary ladder
#   highestqual  highest educational qualification     -- the only ladder that
#                reaches tertiary in waves 2 and 3
#
# Measured cost of wiring `highestgrade` alone (full numbers in CONTENTS.org):
# 12,827 people whom the survey POSITIVELY RECORDS as never having attended
# school would have a null grade and be deleted by `_finalize_result`'s
# `dropna(how='all')` -- in wave 2 that would report `No education` for 9.6% of
# the table when the survey says 24.7% -- and a further 501 (W2) + 775 (W3)
# people would be reported `Unknown` when the survey knows they hold a degree,
# diploma or HND.  Both are "an empty or wrong answer wearing the shape of a
# right one".
#
# The two ladders are COMPLEMENTARY, not redundant, and that is why the
# fallback is narrow rather than a max().  Waves 2 and 3 have NO tertiary rung
# in `highestgrade` at all -- every tertiary person is dumped into the
# 'Other - Specify' / 'Other (please specify)' bucket, whose qualification is
# non-null for 501/501 (W2) and 774/775 (W3) and overwhelmingly tertiary.
# Below that bucket the qualification is essentially a function of the grade
# (W2: 7,470 people with qual 'None' against 7,488 whose grade is at or below
# JSS2/M3), so substituting it there would add nothing and would risk
# overriding an explicit grade with an exam result.

# Harmonized qualification values that carry no information the grade ladder
# did not already have.  'No education' is excluded because `highestqual`
# 'None' means "holds no certificate", NOT "never went to school" -- a P6
# leaver has qual 'None'.  Substituting it would silently demote them.
_EDU_UNINFORMATIVE_QUAL = frozenset({'Unknown', 'No education'})


def individual_education(df):
    """Resolve `Educational Attainment` from grade + qualification + attended.

    Receives the frame `Wave.grab_data` has already extracted and indexed as
    `(t, i, pid)`, carrying:

      Educational Attainment  -- harmonized `highestgrade`
      _qual                   -- harmonized `highestqual`
      _attended               -- raw Yes/No screener

    Resolution order, and why:

    1. Qualification fallback, applied where the grade is absent or resolved to
       `Unknown` (i.e. it was the survey's own 'Other' bucket).  A qualification
       that is itself `Unknown` or `No education` is not informative here (see
       `_EDU_UNINFORMATIVE_QUAL`) and is skipped, leaving the row for step 2.
    2. `attended == 'No'` -> `No education`, for rows still unresolved.  This is
       the never-schooled case the canonical vocabulary explicitly says to KEEP
       (canonical_education_labels.org, "Never-schooled handling"), and it is
       ~exactly complementary to a null grade: in waves 2 and 3 every
       `attended == 'No'` row has a null grade, and in wave 1 4,849 of 4,878 do.
       Applied AFTER step 1 so that wave 1's rows which answer 'No' yet carry a
       real qualification keep the qualification rather than being flattened.

    Rows still unresolved keep NA and are dropped downstream by
    `_finalize_result`'s `dropna(how='all')` -- the survey recorded nothing for
    them.  In waves 2 and 3 those are overwhelmingly children under the
    module's age floor of 3.

    Finally drops the five wave-1 rows whose person id is null.  `format_id`
    turns a NaN `s1fi_hhmid` into None, which would (a) fail the framework's
    `no_null_index_levels` check and (b) make the index non-unique -- three of
    the five share `hhno` 108294010.  Four carry no education data at all; the
    fifth (hhno 105159025) records 'Yes'/P6/'None' and IS a real loss, recorded
    in CONTENTS.org rather than hidden.  There is no way to attach it to a
    person: S1D.dta has no member with that id.
    """
    if 'Educational Attainment' not in df.columns:
        return df

    df = df.copy()
    ea = df['Educational Attainment'].astype(object)
    ea = ea.where(ea.notna(), pd.NA)

    # 1. qualification fallback
    if '_qual' in df.columns:
        qual = df['_qual'].astype(object)
        qual = qual.where(qual.notna(), pd.NA)
        usable = qual.notna() & ~qual.isin(_EDU_UNINFORMATIVE_QUAL)
        unresolved = ea.isna() | ea.eq('Unknown')
        ea = ea.mask(unresolved & usable, qual)

    # 2. never-schooled
    if '_attended' in df.columns:
        att = df['_attended'].astype(str).str.strip().str.casefold()
        ea = ea.mask(ea.isna() & att.eq('no'), 'No education')

    df['Educational Attainment'] = ea
    df = df.drop(columns=[c for c in ('_attended', '_qual') if c in df.columns])

    # A null person id cannot be a row of a (t, i, pid) table.
    if df.index.names is not None and 'pid' in list(df.index.names):
        keep = pd.notna(df.index.get_level_values('pid'))
        if not keep.all():
            df = df[keep]

    return df


# ---------------------------------------------------------------------------
# plot_features (GH #732, #729)
# ---------------------------------------------------------------------------
#
# `df_edit` hook for the `plot_features` table, shared by ALL THREE waves --
# the same binding rule as `individual_education` above: a callable named after
# a declared table in this module is that table's hook, applied by
# `Wave.grab_data` to the merged and indexed (t, i, plot_id) frame after the
# per-wave `dfs:` merge.  Each wave's data_info.yml supplies the same temporary
# columns under the same names, which is what makes one implementation
# legitimate rather than merely convenient.
#
# WHY A HOOK AT ALL.  Two things the YAML path cannot do:
#
#   1. MULTIPLY.  `Area` (hectares -- the one column the canonical schema marks
#      REQUIRED anywhere in the agriculture set) is the reported native size
#      times a per-unit factor.  A `derived:` block dispatches to exactly one
#      registered transformer (`coalesce_coord_bin`), and a column formatting
#      function maps one row of one column at a time; neither multiplies two
#      columns.  Adding a transformer kind is a core (`build_transforms.py`)
#      change, not a country change.
#   2. DROP ROWS.  2009-10's S4AII.dta has 12 rows with a null plot number
#      (S4AIV.dta has 16; none of the 28 carries any datum).  `format_id`
#      turns a null plot_no into a null `plot_id` index level, which the
#      framework's (t, i, plot_id) collapse deletes with a
#      GrainCollapseWarning -- fatal under LSMS_GRAIN_STRICT=1 -- and, because
#      pd.merge matches null keys, the left merge first fans one household's
#      null row out 1 -> 15.  Dropping them HERE is the deliberate drop
#      slurm_logs/ghanasps/FINDINGS_agriculture.org asks for ("must be dropped
#      deliberately, not silently").  2013-14 and 2017-18 have no null keys.
#
# HECTARES PER NATIVE UNIT.  2009-10 ships a producer-computed `area_ha`;
# dividing it by the reported size recovers these constants to float precision
# (within-unit standard deviation ~1e-8 across 4,538 / 717 / 238 / 60 plots).
# 2013-14 and 2017-18 ship no area_ha, so THEIR sizes are converted with wave
# 1's own factors -- internal consistency with the shipped wave-1 numbers was
# preferred to the standard acre (0.404686 ha; a 2e-5 relative difference,
# seen and not used).  Keys are the canonical `AreaUnit` labels, because the
# per-wave YAML maps the raw unit spellings (Acre/Acres, Rope/Ropes/Robes,
# Plot/Plots, ...) through the `AreaUnit` table in _/categorical_mapping.org
# at extraction, i.e. BEFORE this hook runs.  `Other` has no factor and
# yields a NaN Area, as it should.
_PLOT_HECTARES_PER_UNIT = {
    'Acres': 0.404694,
    'Poles': 0.409551,
    'Ropes': 0.236342,
    'Plot':  0.102388,
}


def plot_features(df):
    """Compute `Area` in hectares and drop the null-plot rows.

    Receives the frame `Wave.grab_data` has merged and indexed as
    `(t, i, plot_id)`, carrying:

      AreaUnit  -- canonical unit label (Acres / Poles / Ropes / Plot / Other)
      _size     -- the reported size in that unit            (all waves)
      _area_ha  -- the producer-computed hectares            (2009-10 only)
      Tenure    -- already canonical

    Rules, and why:

    1. `Area = _area_ha` where the producer supplied it, else
       `_size x _PLOT_HECTARES_PER_UNIT[AreaUnit]`.  The shipped number is
       preferred because it is the producer's own; measured on 2009-10 the
       fallback fires on 0 rows (area_ha is null exactly where the size or
       the unit is null, or the unit is 'Other'), so the two paths never
       disagree in practice.  In 2013-14 / 2017-18 there is no `_area_ha` and
       every Area is derived.  No value is filled, clipped or aggregated:
       a unit without a factor gives NaN, a reported size of 0 gives 0, and
       2017-18's two 70,000-pole plots come back as 28,669 ha each.
    2. The temporary `_size` / `_area_ha` columns are dropped, so the built
       table has exactly the declared columns.
    3. Rows whose `plot_id` index level is null are dropped (2009-10 only:
       12 source rows plus the 14-row merge fan-out, none carrying a datum).
       See the module comment for why this must happen here.
    """
    df = df.copy()

    if '_size' in df.columns:
        size = pd.to_numeric(df['_size'], errors='coerce')
        if 'AreaUnit' in df.columns:
            unit = df['AreaUnit'].astype(object)
        else:
            unit = pd.Series(pd.NA, index=df.index, dtype=object)
        factor = pd.to_numeric(unit.map(_PLOT_HECTARES_PER_UNIT), errors='coerce')
        area = size * factor
        if '_area_ha' in df.columns:
            shipped = pd.to_numeric(df['_area_ha'], errors='coerce')
            area = shipped.where(shipped.notna(), area)
        df['Area'] = area.astype(float)

    df = df.drop(columns=[c for c in ('_size', '_area_ha') if c in df.columns])

    # Declared order: Area first, then the rest as extracted.
    if 'Area' in df.columns:
        df = df[['Area'] + [c for c in df.columns if c != 'Area']]

    if df.index.names is not None and 'plot_id' in list(df.index.names):
        keep = pd.notna(df.index.get_level_values('plot_id'))
        if not keep.all():
            df = df[keep]

    return df


# ---------------------------------------------------------------------------
# livestock (GH #729, #736)
# ---------------------------------------------------------------------------
#
# `df_edit` hook for the `livestock` table, shared by ALL THREE waves -- the
# same binding rule as `individual_education` and `plot_features` above: a
# callable named after a declared table in this module is that table's hook,
# applied by `Wave.grab_data` to the extracted and indexed (t, i, animal)
# frame (after the wave-3 two-file concat).  Each wave's data_info.yml
# supplies the same column names, which is what makes one implementation
# legitimate rather than merely convenient.
#
# WHY A HOOK AT ALL.  Four things the YAML path cannot do, each measured on
# the shipped files and each pinned by tests/test_ghanasps_livestock.py:
#
#   1. ADD TWO COLUMNS.  2009-10 records the herd value as a cedis /
#      pesewas PAIR (s3ai_3i / s3ai_3ii); HerdValue = cedis + pesewas/100.
#      A YAML `mapping:` is one column to one column and `derived:` has one
#      registered kind (adding one edits build_transforms.py).  Reading the
#      cedis member alone truncates 25 rows silently.
#   2. DROP NULL-ANIMAL ROWS.  2009-10 has 8 rows with a null animal_id
#      (181 head).  format_id turns them into a null `animal` index level,
#      which the (t, i, animal) collapse deletes with a GrainCollapseWarning
#      -- fatal under LSMS_GRAIN_STRICT=1.  Dropped deliberately and counted
#      in that wave's data_info.yml; they are a real loss.
#   3. DROP THE ROSTER-GRID FILLER.  2013-14 asks a fixed seven-species grid
#      of every household, so 10,884 of its 14,860 rows have quantity == 0
#      and nothing else.  Every precedent country (Uganda, Nigeria, Malawi)
#      drops the never-owned filler; the rule here is Nigeria's INCLUSIVE
#      form -- keep a row iff ANY of HeadCount / HeadSold / HerdValue is
#      non-null and > 0.  A positive value alone keeps the row because
#      HerdValue is a stock valuation of the herd; Uganda excludes
#      value-alone rows because ITS value is a per-head price, which is not
#      evidence of ownership, and that reasoning does not transfer.
#      Measured: the two forms differ on 9 wave-1 and 11 wave-3 rows (null
#      head, no sale, positive value) and on 0 wave-2 rows.
#   4. RESOLVE DUPLICATE (t, i, animal) LINES.  2009-10: 20 households list
#      `Other Farm Animals` on 2-3 lines each (42 rows) -- different unnamed
#      species the instrument never asks to name.  2017-18: one household
#      (106183008) has `Chickens/roosters` in the main file and `Chickens`
#      in `_osp`.  Left alone, the framework's collapse would `.first()`
#      them and file a GrainCollapseWarning.  Here they are SUMMED, which is
#      legal because every declared column is additive at this grain --
#      HeadCount / HeadSold are counts of the same label and HerdValue is a
#      herd total by definition (data_info.yml `Columns: livestock`).  This
#      is the Malawi assemble_livestock / Uganda livestock_for_wave
#      precedent, with `min_count=1` so an all-NaN group stays NaN rather
#      than becoming 0.  It is a REDUCER, and it is kept BOUNDED: the
#      verify script prints the groups collapsed per wave and the test pins
#      their number, so a new duplicate turns red instead of being summed.
#
# Nothing else is edited: no fill, no clip, no sentinel handling.  The
# 0.01-GHS goat (1 pesewa, null cedis) and the 40,000-chicken sale are
# delivered as reported.
_LIVESTOCK_MEASURES = ('HeadCount', 'HeadSold', 'HerdValue')


def livestock(df):
    """Recombine W1 cedis + pesewas, drop null-animal and filler rows, sum
    duplicate (t, i, animal) lines.

    Receives the frame `Wave.grab_data` has extracted and indexed as
    `(t, i, animal)`, carrying (per wave):

      HeadCount  -- head possessed now                      (all waves)
      HeadSold   -- head sold in the last 12 months         (2017-18 only)
      HerdValue  -- "if you sold all of them", decimal GHS  (2013-14, 2017-18)
      _cedis, _pesewas -- the 2009-10 value pair, TEMPORARY

    Rules, in order:

    1. `HerdValue = _cedis + _pesewas / 100` where the temporaries are
       present; NaN only where cedis is null AND pesewas is 0 (a null
       cedis with 1 pesewa is 0.01, as reported).  The temporaries are
       dropped.
    2. Every measure is coerced to float.
    3. Rows whose `animal` index level is null are dropped.
    4. Rows with no positive measure are dropped (the keep-rule above).
    5. Duplicate `(t, i, animal)` keys are summed with `min_count=1`.
    6. Columns come back in declared order: HeadCount, HeadSold, HerdValue.
    """
    df = df.copy()

    # 1. wave-1 cedis + pesewas -> HerdValue
    if '_cedis' in df.columns or '_pesewas' in df.columns:
        cedis = (pd.to_numeric(df['_cedis'], errors='coerce')
                 if '_cedis' in df.columns
                 else pd.Series(np.nan, index=df.index, dtype=float))
        pesewas = (pd.to_numeric(df['_pesewas'], errors='coerce')
                   if '_pesewas' in df.columns
                   else pd.Series(np.nan, index=df.index, dtype=float))
        reported = cedis.notna() | (pesewas.fillna(0) > 0)
        value = cedis.fillna(0) + pesewas.fillna(0) / 100
        df['HerdValue'] = value.where(reported)
    df = df.drop(columns=[c for c in ('_cedis', '_pesewas') if c in df.columns])

    # 2. numeric measures
    present = [c for c in _LIVESTOCK_MEASURES if c in df.columns]
    for c in present:
        df[c] = pd.to_numeric(df[c], errors='coerce').astype(float)

    # 3. a null species cannot be a row of a (t, i, animal) table
    if df.index.names is not None and 'animal' in list(df.index.names):
        keep = pd.notna(df.index.get_level_values('animal'))
        if not keep.all():
            df = df[keep]

    # 4. keep-rule: any measure non-null and > 0 (numpy, not Series, so a
    #    not-yet-unique index cannot trigger an alignment)
    holds = np.zeros(len(df), dtype=bool)
    for c in present:
        holds |= (df[c].fillna(0) > 0).to_numpy()
    df = df[holds]

    # 5. duplicate lines: sum (all measures additive at this grain)
    if df.index.has_duplicates:
        levels = list(df.index.names)
        df = df.groupby(level=levels, sort=False, dropna=False)[present].sum(min_count=1)

    return df[present]
