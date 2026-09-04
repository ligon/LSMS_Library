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


# ---------------------------------------------------------------------------
# crop_production (GH #729, #140)
# ---------------------------------------------------------------------------
#
# Grain (t, i, plot_id, j, u, season); columns Quantity, Quantity_sold,
# Value_sold, harvest_month.  A MIXED build path, and the reasons:
#
#   2009-10  WAVE SCRIPT 2009-10/_/crop_production.py.  The instrument is a
#            plot-by-crop MATRIX: S4AV1 (last MAJOR season) and S4AV2 (last
#            MINOR season), each with five harvest slots per plot.  YAML
#            cannot melt ten wide blocks into long rows, so the script does,
#            with the helpers below.  `season` is assigned from the FILE --
#            every S4AV2 quantity label says "major season" and is WRONG
#            (questionnaire Part A, A120-A122: "LAST MINOR SEASON ... A122.
#            What is the quantity harvested in the last minor season?").
#   2013-14  YAML (04n_harvestquestions, long plot-crop rows) + the
#   2017-18  `crop_production` df_edit hook below (2017-18 also `dfs:`-merges
#            04o_cropsalesstoresquestions, the plot-crop sales file).
#
# The hook is the same binding rule as `individual_education`, `plot_features`
# and `livestock`: a callable named after the table in this module is applied
# by `Wave.grab_data` to the extracted, (t, i, plot_id, j, u)-indexed frame
# of every YAML wave.  What it does that YAML cannot (each measured on the
# shipped files and pinned by tests/test_ghanasps_crop_production.py):
#
#   1. DROP THE NO-HARVEST ROWS.  Both later waves keep a row for every crop
#      planted, and a row for every uncultivated plot: 2013-14 has 1,341 rows
#      with neither a quantity nor a unit (1,322 "No, all yet to be
#      harvested"); 2017-18 has 1,926 blank-crop placeholders plus 963
#      quantity-and-unit-null rows.  None carries a harvest month.  A null
#      `u` is a null index level, which the (t, i, plot_id, j, u, season)
#      collapse would DELETE with a GrainCollapseWarning (fatal under
#      LSMS_GRAIN_STRICT=1); dropping them here is the deliberate drop.
#   2. FOLD THE "OTHER -- SPECIFY" UNIT.  2013-14 offers only a 20-code unit
#      subset, so 226 rows are `Other - specify` with free text -- 51 of them
#      `tubers` / `TUBERS`, a unit the other two waves offer as code 26.  A
#      specify text is folded ONLY on an exact case-insensitive match to a
#      vocabulary label or its plural; `ropes`, `pan`, `bashing rubber`,
#      `Motor King` ... stay `Other`.
#   3. BUILD harvest_month.  2013-14 stores up to six month columns
#      (harvestmonths1..6), 2017-18 one space-separated string ('10 11');
#      both become a sorted, zero-padded, space-separated token string
#      ('10 11') -- Nigeria's `_month_str` zero-padding, extended to a
#      multi-select.
#   4. AGGREGATE THE NINE SALE CHANNELS (2017-18 only).  04o records, per
#      plot-crop, a quantity / unit / price for each of nine buyer channels,
#      and the price is per unit ("What was the price for which [Name] sold
#      one [unit] of [crop type] to ...").  Value_sold = sum over channels of
#      quantity x price -- money is additive whatever the units.
#      Quantity_sold = sum of the channel quantities ONLY when every selling
#      channel used one unit and that unit is the row's harvest unit `u`
#      (3,062 rows); a sale in a different unit (432), in more than one unit
#      (7) or in `Other` gives NaN -- a quantity in a foreign unit on a row
#      keyed by `u` would be wrong, not merely imprecise.  A channel with a
#      quantity but no price (12 rows) makes Value_sold NaN rather than a
#      silently partial sum.  2013-14's 04o has NO plotid, so both columns
#      are NaN there by design (Nigeria W3-W5 do the same).
#   5. ADD THE CONSTANT `season` LEVEL, `annual`.  Both instruments say "If
#      there has been more than one harvest from the plot, give the total
#      quantity which has been harvested over the last 12 months" -- a
#      12-month recall, NOT a major or minor season.  The level is kept so
#      the three waves share one shape.
#   6. SUM DUPLICATE (t, i, plot_id, j, u, season) LINES.  2013-14 has two:
#      107250012 plot 1 cassava, 10 + 20 baskets; 109315002 plot 1 millet,
#      0.5 + 0.5 maxi bags -- two harvest events of one product in one unit,
#      additive at this grain (the food_acquired `_ADDITIVE_MEASURE_COLUMNS`
#      semantics), months unioned.  Bounded and pinned exactly as the
#      livestock reducer is: the test fixes the per-wave group count.
#
# Nothing else is edited: no fill, no clip.  One 2017-18 sale computes to
# 42.8M GHS (a quantity x unit-price keying error) and is delivered as is.
from pathlib import Path

from lsms_library.local_tools import all_dfs_from_orgfile, df_from_orgfile, format_id

_CROP_MEASURES = ('Quantity', 'Quantity_sold', 'Value_sold')
_CROP_COLUMNS = ('Quantity', 'Quantity_sold', 'Value_sold', 'harvest_month')
_CROP_INDEX = ('t', 'i', 'plot_id', 'j', 'u', 'season')
_CROP_UNIT_UNKNOWN = 'Unknown'
_CROP_UNIT_OTHER = 'Other'
_CROP_SEASON_RECALL = 'annual'
_CROP_SALE_CHANNELS = ('samecomm', 'othercomm', 'traders', 'contractors',
                       'orgs', 'agg', 'outgrow', 'coop', 'other')

# 2009-10 crop codes: CODE_BOOK.pdf "CROP CODES" 01-43, identical to the
# value labels of the A78 crop-grown columns (s4v_a78i..x) in S4AV1/S4AV2.
# The harvest-slot crop id (s4v_a80i, a88i, ... / a121i, ...) carries the
# same codes but NO value labels, so the decode is baked here; a data-gated
# test pins it to the file's own labels.  Codes 0 / 44-47 / 90 occur on 12
# harvest records and are defined nowhere in the 2009-10 instrument (44 is
# `Soya bean(s)` only in the 2013-14 / 2017-18 lists) -- those records have
# no crop identity and the script drops them, counted.
_W1_CROP_CODES = {
    1: 'Avocado pear', 2: 'Banana', 3: 'Beans/Peas', 4: 'Cashew nut',
    5: 'Cassava', 6: 'Cocoa', 7: 'Coconut', 8: 'Cocoyam', 9: 'Coffee',
    10: 'Colanut', 11: 'Cotton', 12: 'Garden Egg/Egg plant', 13: 'Ginger',
    14: 'Groundnut/Peanut', 15: 'Guinea corn/Sorghum', 16: 'Kenef',
    17: 'Leafy Vegetable', 18: 'Lime/Lemon', 19: 'Maize', 20: 'Mango',
    21: 'Millet', 22: 'Oil Palm', 23: 'Okro', 24: 'Onion',
    25: 'Oranges/Tangerine', 26: 'Pawpaw', 27: 'Pepper', 28: 'Pineapple',
    29: 'Plantain', 30: 'Potatoes/Sweet potatoes', 31: 'Rice', 32: 'Rubber',
    33: 'Sheanut', 34: 'Sugarcane', 35: 'Tiger nut', 36: 'Tobacco',
    37: 'Tomatoes', 38: 'Water melon', 39: 'Woodlot', 40: 'Yam',
    41: 'Other food crops', 42: 'Other fruits', 43: 'Other vegetables',
}

# 2009-10 "ID of part of crop harvested" (A80.2 / A121.2), the value labels
# of s4v_a80ii etc.  NOT the canonical `condition` level (that is the
# physical state -- green / fresh / dry -- of ONE product; this is WHICH
# product of the plant was taken) and it is not force-fitted into it.  The
# instrument's own example, "e.g. Cocoa, Cocoa Leaves", says what it is: a
# part other than the crop's principal one is a different PRODUCT, so
# `product_label` qualifies `j` for those records.
_W1_PART_CODES = {1: 'Leaves', 2: 'Branches', 3: 'Bark', 4: 'Sap',
                  5: 'Stem/stick', 6: 'Roots/tuber', 7: 'Fruit/Seeds/Nuts',
                  8: 'Bulb'}
# Parts that make a record a different product from the crop's principal
# harvest.  Roots/tuber, Fruit/Seeds/Nuts and Bulb are never qualifying: a
# respondent who reports maize "roots" or yam "seeds" is reporting the main
# harvest with an odd part code (the crosstab has 88 cassava and 43 yam
# records under Fruit/Seeds/Nuts), and relabelling those would fabricate
# products.  Leaves / Branches / Bark / Sap / Stem are unambiguous.
_W1_QUALIFYING_PARTS = frozenset({'Leaves', 'Branches', 'Bark', 'Sap',
                                  'Stem/stick'})
# ... except where that part IS the crop's principal product.
_W1_PRINCIPAL_PARTS = {
    'Leafy Vegetables': frozenset({'Leaves'}),
    'Other Vegetables': frozenset({'Leaves'}),
    'Sugarcane': frozenset({'Stem/stick'}),
    'Woodlot': frozenset({'Stem/stick'}),
}
# A qualified product that already has a label in food_items.org reuses it,
# so it joins food_acquired.j like any other crop label.
_W1_PART_PRODUCT_LABELS = {('Cocoyam', 'Leaves'): 'Cocoyam Leaves'}


def _country_dir() -> Path:
    return Path(__file__).resolve().parent


def crop_label_map() -> dict:
    """`harmonize_crop` (Alternate Spelling -> Preferred Label) from
    _/categorical_mapping.org -- the same table the YAML waves reference."""
    tbl = all_dfs_from_orgfile(_country_dir() / 'categorical_mapping.org')['harmonize_crop']
    return dict(zip(tbl['Alternate Spelling'].astype(str).str.strip(),
                    tbl['Preferred Label'].astype(str).str.strip()))


def harvest_unit_vocabulary() -> list:
    """The Preferred Labels of `harmonizedunit` in _/units.org -- the `u`
    vocabulary food_acquired delivers, and therefore the one crop_production
    must deliver."""
    tbl = df_from_orgfile(str(_country_dir() / 'units.org'), name='harmonizedunit',
                          encoding='ISO-8859-1')
    labels = tbl['Preferred Label'].astype(str).str.strip()
    return sorted(l for l in labels.unique() if l and l not in ('nan', '---'))


def w1_unit_labels() -> dict:
    """2009-10 unit CODE (int) -> harmonizedunit Preferred Label, through the
    `unit09` code list and the `harmonizedunit` 2009-10 column -- exactly the
    chain 2009-10/_/food_acquired.py uses, so the two tables agree on `u`."""
    unit09 = df_from_orgfile(str(_country_dir() / 'units.org'), name='unit09',
                             encoding='ISO-8859-1')
    codes = unit09['Code'].astype(str).str.replace(r'[^0-9]', '', regex=True)
    code2label = {int(c): str(l).strip() for c, l in zip(codes, unit09['Preferred Label']) if c}
    harmonized = df_from_orgfile(str(_country_dir() / 'units.org'), name='harmonizedunit',
                                 encoding='ISO-8859-1')
    h = harmonized[['2009-10', 'Preferred Label']].astype(str).apply(lambda s: s.str.strip())
    h = h[~h['2009-10'].isin(['', '---', 'nan'])]
    label2pref = dict(zip(h['2009-10'], h['Preferred Label']))
    return {c: label2pref.get(l, l) for c, l in code2label.items()}


def product_label(crop: str, part) -> str:
    """The delivered `j` for a 2009-10 harvest record: the crop's Preferred
    Label, qualified by the harvested part where that part is a different
    product (see `_W1_QUALIFYING_PARTS`).  A missing or off-scheme part code
    leaves the crop label unqualified."""
    if pd.isna(part):
        return crop
    part = str(part)
    if part not in _W1_QUALIFYING_PARTS:
        return crop
    if part in _W1_PRINCIPAL_PARTS.get(crop, ()):
        return crop
    return _W1_PART_PRODUCT_LABELS.get((crop, part), f'{crop} ({part.lower()})')


def _unit_fold_dict(vocabulary) -> dict:
    d = {}
    for label in vocabulary:
        if label in (_CROP_UNIT_OTHER, _CROP_UNIT_UNKNOWN):
            continue
        d[label.lower()] = label
        d[label.lower() + 's'] = label
        d[label.lower() + 'es'] = label
    return d


def fold_other_unit(u: pd.Series, specify: pd.Series) -> pd.Series:
    """Where `u` is `Other` and the specify text names a vocabulary unit
    (exact, case-insensitive, singular or plural), use that unit."""
    u = u.astype(object).copy()
    fold = _unit_fold_dict(harvest_unit_vocabulary())
    text = specify.astype(object).where(specify.notna(), '').astype(str).str.strip().str.lower()
    is_other = u.astype(str).eq(_CROP_UNIT_OTHER)
    folded = text.map(fold)
    hit = is_other & folded.notna()
    u[hit] = folded[hit]
    return u


_MONTH_NAMES = {name: n for n, name in enumerate(
    ('january', 'february', 'march', 'april', 'may', 'june', 'july',
     'august', 'september', 'october', 'november', 'december'), start=1)}
_MONTH_NAMES.update({k[:3]: v for k, v in list(_MONTH_NAMES.items())})


def month_string(tokens) -> object:
    """Sorted, de-duplicated, zero-padded month tokens ('08 11'); pd.NA when
    none is a valid month.  `tokens` is any iterable of month numbers,
    month NAMES (2013-14's harvestmonths1..6 decode to 'January' ...) or
    strings (2017-18's '10 11' is split on whitespace)."""
    months = set()
    for tok in tokens:
        if pd.isna(tok):
            continue
        for piece in str(tok).split():
            try:
                m = int(float(piece))
            except ValueError:
                m = _MONTH_NAMES.get(piece.strip().lower())
                if m is None:
                    continue
            if 1 <= m <= 12:
                months.add(m)
    if not months:
        return pd.NA
    return ' '.join(f'{m:02d}' for m in sorted(months))


def sum_duplicate_harvest_records(flat: pd.DataFrame, levels=_CROP_INDEX):
    """Bounded reducer: sum the measures of rows sharing an index key
    (`min_count=1`), union their harvest months.  Returns (frame, n_groups)
    so the caller can print / pin how many keys it collapsed."""
    levels = list(levels)
    dup = flat.duplicated(levels, keep=False)
    n_groups = int(flat.loc[dup, levels].drop_duplicates().shape[0])
    if n_groups == 0:
        return flat, 0
    measures = [c for c in _CROP_MEASURES if c in flat.columns]
    agg = {c: (c, lambda s: s.sum(min_count=1)) for c in measures}
    if 'harvest_month' in flat.columns:
        agg['harvest_month'] = ('harvest_month', lambda s: month_string(s.tolist()))
    out = flat.groupby(levels, dropna=False, sort=False).agg(**agg).reset_index()
    return out, n_groups


def _sales_from_channels(flat: pd.DataFrame) -> pd.DataFrame:
    """2017-18: Value_sold / Quantity_sold from the nine per-channel
    quantity / unit / price columns (see the module comment, item 4)."""
    qcols = [f'_sq_{c}' for c in _CROP_SALE_CHANNELS if f'_sq_{c}' in flat.columns]
    if not qcols:
        return flat
    ucols = [f'_su_{c[4:]}' for c in qcols]
    pcols = [f'_sp_{c[4:]}' for c in qcols]
    q = flat[qcols].apply(pd.to_numeric, errors='coerce')
    p = flat[pcols].apply(pd.to_numeric, errors='coerce')
    p.columns = q.columns
    used = q.notna()
    value = (q * p).sum(axis=1, min_count=1)
    value = value.where(~(used & p.isna()).any(axis=1))
    units = flat[ucols].astype(object)
    units.columns = q.columns
    units = units.where(used)
    n_units = units.apply(lambda r: len({str(x) for x in r.dropna()}), axis=1)
    sale_unit = units.bfill(axis=1).iloc[:, 0].astype(object)
    same = (n_units.eq(1)
            & sale_unit.notna()
            & sale_unit.astype(str).eq(flat['u'].astype(str))
            & ~sale_unit.astype(str).isin([_CROP_UNIT_OTHER, _CROP_UNIT_UNKNOWN]))
    qty = q.sum(axis=1, min_count=1).where(same)
    flat = flat.drop(columns=qcols + ucols + pcols)
    flat['Quantity_sold'] = qty.astype(float)
    flat['Value_sold'] = value.astype(float)
    return flat


def crop_production(df):
    """df_edit hook for the 2013-14 / 2017-18 YAML waves.  See the module
    comment for each step and its measured cost."""
    idx = [n for n in (df.index.names or []) if n is not None]
    flat = df.reset_index() if idx else df.copy()

    # 1. rows without a crop, or with neither a quantity nor a unit
    j = flat['j'].astype(object)
    has_crop = j.notna() & (j.astype(str).str.strip() != '')
    flat = flat[has_crop].copy()
    flat['Quantity'] = pd.to_numeric(flat['Quantity'], errors='coerce').astype(float)
    u = flat['u'].astype(object)
    u = u.where(u.notna() & (u.astype(str).str.strip() != ''), pd.NA)
    flat = flat[~(flat['Quantity'].isna() & u.isna())].copy()
    u = u.loc[flat.index]

    # 2. unit: missing with a quantity -> Unknown; Other + specify -> folded
    u = u.where(u.notna(), _CROP_UNIT_UNKNOWN)
    if '_u_other' in flat.columns:
        u = fold_other_unit(u, flat['_u_other'])
    flat['u'] = u.astype(str)

    # 3. harvest_month
    mcols = [c for c in flat.columns if c.startswith('_m')]
    if mcols:
        flat['harvest_month'] = [month_string(r) for r in flat[mcols].itertuples(index=False)]
    elif 'harvest_month' in flat.columns:
        flat['harvest_month'] = [month_string([v]) for v in flat['harvest_month']]
    else:
        flat['harvest_month'] = pd.NA

    # 4. sales (2017-18 only)
    flat = _sales_from_channels(flat)
    for c in ('Quantity_sold', 'Value_sold'):
        if c not in flat.columns:
            flat[c] = np.nan
        flat[c] = pd.to_numeric(flat[c], errors='coerce').astype(float)
    flat = flat.drop(columns=[c for c in flat.columns if c.startswith('_')])

    # 5. the constant recall level
    flat['season'] = _CROP_SEASON_RECALL

    # 6. duplicate lines
    flat, n_groups = sum_duplicate_harvest_records(flat)
    flat.attrs['crop_production_dedup_groups'] = n_groups

    flat['harvest_month'] = flat['harvest_month'].astype('string')
    out = flat.set_index(list(_CROP_INDEX))[list(_CROP_COLUMNS)]
    return out


# ---------------------------------------------------------------------------
# plot_labor (GH #729, #140) -- shared vocabulary + arithmetic for the three
# wave scripts.  There is NO country-level df_edit hook: every wave is a
# script (the `source` axis is a melt across column GROUPS, which
# df_data_grabber cannot express), so the shared code lives here as plain
# helpers rather than as a hook the framework calls.
# ---------------------------------------------------------------------------

_LABOR_INDEX = ('t', 'i', 'plot_id', 'season', 'stage', 'source')
_LABOR_COLUMNS = ('PersonDays', 'Hours', 'WageRateMen', 'WageRateWomen',
                  'WageRateChildren', 'WageUnit')

#: The 2013-14 / 2017-18 recall period, verbatim from Part M: "how much time
#: you and others spent working on your farms in the last farming season"
#: (2013-14) / "in the last farming season (2017)" (2017-18).  Deliberately
#: NOT `major` -- neither instrument names the season -- and deliberately not
#: crop_production's `annual`, which is that table's own explicit 12-month
#: recall (N5) and a different question in the same wave.
LABOR_SEASON_LATER = 'last'

#: 2009-10 stage -> the (major, minor) S4AIX file numbers and the A-number
#: range each file's labour cells must carry.  The stage names come from the
#: questionnaire's eight section headers (quoted in full in
#: _/categorical_mapping.org); the A-numbers come from the .dta variable
#: labels, so the mapping is ASSERTED against the data rather than assumed.
_W1_STAGE_BLOCKS = (
    ('land_preparation', 1, 5, (290, 298), (327, 335)),
    ('field_management', 2, 6, (299, 307), (336, 344)),
    ('harvesting',       3, 7, (308, 316), (345, 353)),
    ('post_harvest',     4, 8, (317, 325), (354, 362)),
)

#: 2009-10 source -> the offset of its (men, women, children) question triple
#: from the block's first A-number.  A290/A291/A292 casual, A293/A294/A295
#: permanent, A296/A297/A298 family -- and the same 0/3/6 offsets in every
#: one of the eight blocks.
_W1_SOURCE_OFFSETS = (('casual', 0), ('permanent', 3), ('family', 6))

#: The three per-question cells: .1 number of days, .2 average hours per day,
#: .3 average number of workers.
_W1_CELL_SUFFIXES = ('i', 'ii', 'iii')

#: 2013-14 / 2017-18 source -> (worker-count column, days-per-worker column)
#: pairs by sex.  `self` is special: `personaldays` is the respondent alone,
#: so it is already person-days and has no worker count.
_LATER_SOURCE_COLUMNS = {
    'family':   (('familywomen', 'familywomendays'), ('familymen', 'familymendays')),
    'communal': (('communalwomen', 'communalwomendays'), ('communalmen', 'communalmendays')),
    'hired':    (('hiredwomen', 'hiredwomendays'), ('hiredmen', 'hiredmendays')),
    'other':    (('otherwomen', 'otherwomendays'), ('othermen', 'othermendays')),
}

#: The source that carries the hired-labour pay block.
LABOR_HIRED_SOURCE = 'hired'

#: 2017-18's `hiredpayunit` ships as a bare numeric code with NO value label
#: (that wave's .dta carries value labels for `cultivated` and `interviewedid`
#: only), so the closed answers have to come from the instrument itself --
#: M196 "Does [Name] pay those amounts per day or per acre?  1 Per day
#: 2 Per week  3 Per month  4 Per plot  5 Per acre  6 Per pole  7 Per rope
#: -666 Other (specify)".  2013-14 ships the same list AS a value label
#: (`thiredpayunit`), which is why only this wave needs the table.  Reading
#: the codes as strings would silently leave WageUnit 100% null.
_WAGE_UNIT_CODES = {1: 'Per day', 2: 'Per week', 3: 'Per month',
                    4: 'Per plot', 5: 'Per acre', 6: 'Per pole',
                    7: 'Per rope', -666: 'Other (specify)'}


def wage_unit_labels(s, unit_map):
    """`hiredpayunit` -> a WageUnit Preferred Label, labelled or coded.

    2013-14 delivers the value label ('Per day'); 2017-18 delivers the bare
    code (1.0), which is decoded through _WAGE_UNIT_CODES first.
    """
    if pd.api.types.is_numeric_dtype(s):
        codes = pd.to_numeric(s, errors='coerce')
        raw = codes.map(lambda c: _WAGE_UNIT_CODES.get(int(c)) if pd.notna(c) else pd.NA)
        unknown = sorted({int(c) for c in codes.dropna().unique()
                          if int(c) not in _WAGE_UNIT_CODES})
        assert not unknown, f'hiredpayunit codes not in the instrument list: {unknown}'
    else:
        raw = s.astype(str).str.strip().replace({'': pd.NA, 'nan': pd.NA})
    return raw.map(lambda x: unit_map.get(x, pd.NA) if pd.notna(x) else pd.NA)


def labor_stage_map() -> dict:
    """`harmonize_stage` (Alternate Spelling -> Preferred Label)."""
    tbl = all_dfs_from_orgfile(_country_dir() / 'categorical_mapping.org')['harmonize_stage']
    return dict(zip(tbl['Alternate Spelling'].astype(str).str.strip(),
                    tbl['Preferred Label'].astype(str).str.strip()))


def labor_source_map() -> dict:
    """`harmonize_labor_source` (Alternate Spelling -> Preferred Label)."""
    tbl = all_dfs_from_orgfile(_country_dir() / 'categorical_mapping.org')['harmonize_labor_source']
    return dict(zip(tbl['Alternate Spelling'].astype(str).str.strip(),
                    tbl['Preferred Label'].astype(str).str.strip()))


def wage_unit_map() -> dict:
    """`WageUnit` (Alternate Spelling -> Preferred Label)."""
    tbl = all_dfs_from_orgfile(_country_dir() / 'categorical_mapping.org')['WageUnit']
    return dict(zip(tbl['Alternate Spelling'].astype(str).str.strip(),
                    tbl['Preferred Label'].astype(str).str.strip()))


def drop_labor_sentinels(s):
    """A negative day / hour / worker / rate cell is a sentinel, not a datum.

    2009-10 uses -10 (90 cells) and -1 (34 cells) across the eight S4AIX
    files; 2013-14 uses -1 in its three pay cells (24 / 591 / 832).  The
    2009-10 CODE_BOOK documents neither, so they are read as missing and
    counted rather than guessed at.
    """
    s = pd.to_numeric(s, errors='coerce')
    return s.where(s >= 0)


def finish_labor_frame(flat, t):
    """Common tail for the three wave scripts: keep rule, dtypes, index.

    The keep rule is ``PersonDays > 0``.  It is forced by the two later waves
    having INCOMPATIBLE fill patterns for a stage nobody worked: 2013-14 ships
    a full 7-stages-x-every-plot grid with an `any` Yes/No screener, while
    2017-18 ships only the stages the plot actually did (~3.85 rows per plot).
    Keeping zero / null rows would emit 2013-14's "No" rows and no 2017-18
    equivalent -- a pure artefact of how the two extracts were exported.
    """
    n_in = len(flat)
    kept = flat['PersonDays'] > 0
    n_zero = int((flat['PersonDays'] == 0).sum())
    n_null = int(flat['PersonDays'].isna().sum())
    flat = flat[kept].copy()
    print(f'{t}: {n_in:,} candidate (plot, season, stage, source) cells -> '
          f'{len(flat):,} kept; dropped {n_zero:,} with PersonDays == 0 and '
          f'{n_null:,} with no reported person-days')
    for c in ('PersonDays', 'Hours', 'WageRateMen', 'WageRateWomen', 'WageRateChildren'):
        if c not in flat.columns:
            flat[c] = np.nan
        flat[c] = pd.to_numeric(flat[c], errors='coerce').astype(float)
    if 'WageUnit' not in flat.columns:
        flat['WageUnit'] = pd.NA
    flat['WageUnit'] = flat['WageUnit'].astype('string')
    out = flat.set_index(list(_LABOR_INDEX))[list(_LABOR_COLUMNS)].sort_index()
    assert len(out) > 0, f'plot_labor produced no rows for {t}'
    assert out.index.is_unique, f'Non-unique plot_labor index for {t}'
    assert not out.index.to_frame().isna().any().any(), f'Null index level in plot_labor for {t}'
    return out


def drop_nonpositive_rate(s):
    """A reported hired-labour pay rate of <= 0 is not a rate.

    2013-14 fills the three "How much on average did you pay each man / woman
    / child" cells for every plot that paid in cash, using -1 and 0 where the
    category does not apply: 591 of 1,811 woman cells are -1 and 701 are 0,
    but only 449 plots hired any woman at all.  2017-18 has no -1 but 38 man
    and 4 woman zeros.  Zero is not an answer to "how much did you pay", so
    both are read as missing; the count is printed by each wave script.
    """
    s = pd.to_numeric(s, errors='coerce')
    return s.where(s > 0)


def later_wave_labor(stage_df, stage_col, pay_df, pay_per_stage, t,
                     source_map, stage_map, unit_map):
    """2013-14 / 2017-18 plot_labor: melt the stage grid onto the source axis.

    *stage_df* is the plot-x-stage file keyed (FPrimary, plotid, *stage_col*);
    *pay_df* carries the hired-pay block, keyed on the PLOT when
    *pay_per_stage* is False (2013-14's plot-level 04m_aglabour) and on the
    plot-stage row when it is True (2017-18, where the block is folded into
    the stage file and genuinely varies within a plot).

    PERSON-DAYS ARE NOT THE ``*days`` COLUMNS.  Part M asks the worker count
    and the per-worker duration as separate questions -- "Approximately how
    many hired laborers WHO ARE MEN worked on this plot ...?  Number of
    people" then "Approximately how many days ON AVERAGE did EACH OF the
    hired laborers WHO ARE MEN work on this plot ...?  Number of days" -- so
    PersonDays = workers x days, summed over the two sexes of ONE question.
    Reading ``hiredmendays`` as person-days undercounts by the mean worker
    count (~3.4 hired men per plot-stage in 2013-14).  ``personaldays`` is the
    respondent alone and IS already person-days.
    """
    def _norm_stage(s):
        s = s.astype(str).str.strip()
        return s.map(lambda x: stage_map.get(x, x if x else pd.NA))

    pieces = []
    for source, sexes in _LATER_SOURCE_COLUMNS.items():
        person_days = None
        for wcol, dcol in sexes:
            pd_g = drop_labor_sentinels(stage_df[wcol]) * drop_labor_sentinels(stage_df[dcol])
            person_days = pd_g if person_days is None else person_days.add(pd_g, fill_value=0)
        pieces.append(pd.DataFrame({
            'i': stage_df['FPrimary'].to_numpy(),
            'plot_id': stage_df['plotid'].to_numpy(),
            'stage': _norm_stage(stage_df[stage_col]).to_numpy(),
            'source': source_map.get(source, source),
            'PersonDays': person_days.to_numpy(),
        }))
    pieces.append(pd.DataFrame({
        'i': stage_df['FPrimary'].to_numpy(),
        'plot_id': stage_df['plotid'].to_numpy(),
        'stage': _norm_stage(stage_df[stage_col]).to_numpy(),
        'source': 'self',
        'PersonDays': drop_labor_sentinels(stage_df['personaldays']).to_numpy(),
    }))
    flat = pd.concat(pieces, ignore_index=True)

    n_nullstage = int(flat['stage'].isna().sum())
    if n_nullstage:
        print(f'{t}: dropped {n_nullstage:,} candidate cells whose stage is blank '
              f'(the uncultivated-plot placeholder rows)')
        flat = flat[flat['stage'].notna()].copy()
    unmapped = sorted(set(flat['stage']) - set(stage_map.values()))
    assert not unmapped, f'{t}: stage labels missing from harmonize_stage: {unmapped}'

    # --- the hired-pay block ------------------------------------------------
    rate_cols = {'WageRateMen': 'hiredavgpayman',
                 'WageRateWomen': 'hiredavgpaywoman',
                 'WageRateChildren': 'hiredavepaychild'}
    pay = pd.DataFrame({
        'i': pay_df['FPrimary'].to_numpy(),
        'plot_id': pay_df['plotid'].to_numpy(),
    })
    if pay_per_stage:
        pay['stage'] = _norm_stage(pay_df[stage_col]).to_numpy()
    dropped = {}
    for target, src in rate_cols.items():
        if src in pay_df.columns:
            raw = pd.to_numeric(pay_df[src], errors='coerce')
            pay[target] = drop_nonpositive_rate(raw).to_numpy()
            dropped[target] = int((raw.notna() & pay[target].isna().to_numpy()).sum())
        else:
            pay[target] = np.nan
            dropped[target] = 'not asked in this wave'
    pay['WageUnit'] = wage_unit_labels(pay_df['hiredpayunit'], unit_map).to_numpy()
    n_unit_lost = int((pay_df['hiredpayunit'].notna().to_numpy()
                       & pd.isna(pay['WageUnit']).to_numpy()).sum())
    assert n_unit_lost == 0, (
        f'{t}: {n_unit_lost} reported hiredpayunit values did not map to a '
        f'WageUnit Preferred Label')
    print(f'{t}: non-positive pay cells read as missing: {dropped}')

    keys = ['i', 'plot_id'] + (['stage'] if pay_per_stage else [])
    pay = pay.dropna(subset=keys)
    assert not pay.duplicated(keys).any(), (
        f'{t}: the hired-pay block is not unique on {keys}')
    pay = pay[keys + list(rate_cols) + ['WageUnit']]

    hired = flat['source'] == LABOR_HIRED_SOURCE
    merged = flat.loc[hired, keys].merge(pay, on=keys, how='left')
    assert len(merged) == int(hired.sum()), f'{t}: the pay join changed the row count'
    for c in list(rate_cols) + ['WageUnit']:
        flat[c] = pd.NA
        flat.loc[hired, c] = merged[c].to_numpy()

    flat['t'] = t
    flat['season'] = LABOR_SEASON_LATER
    flat['i'] = flat['i'].astype(str)
    flat['plot_id'] = flat['plot_id'].map(format_id)
    return flat
