import pandas as pd
import numpy as np
from collections import defaultdict
from importlib.resources import files
from lsms_library.local_tools import get_dataframe, DVCFS

# Formatting  Functions for Ghana 2016-17
import pandas as pd
import numpy as np
import lsms_library.local_tools as tools
from collections import defaultdict


def interview_date(df):
    """Expose reported dates with calendar-visit assignments.

    Existing intake sources and parsers are retained. GLSS4/GLSS5 also use
    the registered, fixed anchored-chronology construction: only the visit
    assignment is derived, and those rows carry its Derivation key.
    Undated slots are omitted; all raw inference inputs remain retrievable
    through Country.derivation_inputs, including unresolved/invalid records.
    Early food visit=1 denotes a consumption occasion at the second contact,
    so its visit number is not a date-table join key (task ledger, sections 3/4).
    """
    waves = df.index.get_level_values('t').unique()
    if len(waves) == 1 and str(waves[0]) in ('1998-99', '2005-06'):
        from lsms_library.derivations import resolve_callable
        build = resolve_callable(
            'lsms_library.countries.GhanaLSS._.visit_dates:build_visit_dates')
        # The builder attaches row provenance after reshaping; the generic
        # melt intentionally retains date columns only.
        return build(str(waves[0]))
    return tools.melt_visit_intervals(df, start_base='Int_t', out_start='Int_t')


def i(value):
    '''
    Formatting household id as "clust/nh".

    Returns pd.NA when either component is missing (e.g. blank/trailing
    rows in some source files such as g7sec9c), so such rows drop out of
    the index rather than raising.
    '''
    if type(value) == pd.Series:
        clust = tools.format_id(value.iloc[0])
        nh = tools.format_id(value.iloc[1], zeropadding=2)
        if clust is None or nh is None:
            return pd.NA
        return clust + '/' + nh
    else:
        return tools.format_id(value)


def Sex(value):
    '''
    Formatting sex veriable
    '''
    if pd.isna(value):
        return pd.NA
    else:
        return str(value).upper()[0]

def Age(value):
    '''
    Formatting age variable
    '''
    if pd.isna(value):
        return np.nan
    else:
        return int(value)
    
def Birthplace(value):
    '''
    Formatting birthplace variable
    '''
    if pd.isna(value):
        return pd.NA
    else:
        return value.title() if isinstance(value,str) else pd.NA

def Relationship(value):
    '''
    Formatting relationship variable
    '''
    if pd.isna(value):
        return pd.NA
    else:
        return value.title()



# Data to link household ids across waves
# PANELC.DAT in 1988-89 maps HID1 (GLSS1) -> HID2 (GLSS2) at person level;
# panel_ids() deduplicates to household level.  714 panel households.
#
# Waves 1991-92 through 2016-17: cluster numbering changes between rounds
# (3xxx, 4xxx, 5xxx, 6xxxx, 7xxxx) and no explicit prior-wave HH ID column
# exists in the available data files, so cross-wave linkage cannot be
# established.  The 2005-06 and 2012-13 hints ('rhhno') referred to
# within-wave replacement households, not cross-wave panel linkage.
Waves = {'1987-88':(),
         '1988-89':('PANELC.DAT', 'HID2', 'HID1'),
         '1991-92':(),  # No linkage file; cluster scheme changed (2xxxx -> 3xxx)
         '1998-99':(),  # No linkage file; cluster scheme changed (3xxx -> 4xxx)
         '2005-06':(),  # No linkage file; cluster scheme changed (4xxx -> 5xxx)
         '2012-13':(),  # reint marks 1476 panel HHs but no prior-wave ID column
         '2016-17':()   # No linkage fields
         }

def yearly_expenditure(row, cost = 'CFOODB', freq = 'TFOODB', freq_unit = 'UTFOODB', months = 'MFOODBLY'):
    row = row.replace({'.':0})
    val = float(row[cost]) * float(row[freq]) #cost each time * freq per time unit
    if row[freq_unit] == '3': #if bought daily
        val *= 30 * float(row[months]) # * days in a month * num of months item bought
    elif row[freq_unit] == '4': #if bought weekly
        val *= 4 * float(row[months]) # * weeks in a month * num of months item bought
    elif row[freq_unit] == '5': #if bought monthly
        val *= float(row[months]) # * num of months item bought
    elif row[freq_unit] == '6': #if bought quarterly
        val *= 4 # * quarters in a year 
    elif row[freq_unit] == '7': #if bought semiannually
        val *= 2 # * half years in a year
    elif row[freq_unit] == '8': #if bought yearly
        val *= 1
    else:
        val = 0
    return val

def load_large_dta(fn, convert_categoricals = False):
    import sys

    reader = pd.read_stata(fn, iterator=True, convert_categoricals = convert_categoricals)
    df = pd.DataFrame()

    try:
        chunk = reader.get_chunk(100*1000)
        while len(chunk) > 0:
            df = pd.concat([df, chunk], ignore_index=True)
            chunk = reader.get_chunk(100*1000)
            sys.stdout.flush()
    except (StopIteration, KeyboardInterrupt):
        pass
    print('\nloaded {} rows'.format(len(df)))
    return df

def split_by_visit(df, first_visit, last_visit, t, ind = ('j','t','i'), unit_col = None, aggregate_amount = False):
    ind = list(ind)
    df = df.set_index([ind[0]] + ind[2:])
    df_by_visit = []
    for i in range(first_visit, last_visit+1):
        tem = df[df.columns[df.columns.str.contains(str(i))]]
        temp = tem.dropna(how='all').copy()
        temp['t']= t + ', ' + str(i)
        temp = temp.reset_index().set_index(ind)
        temp.columns = ['_'.join(c.split('_')[:-1]) for c in temp.columns]
        #temp = temp.set_index('t', append = True)
        df_by_visit.append(temp)
    result = pd.concat(df_by_visit)
    result = result.rename(columns={unit_col: 'u'})
    if aggregate_amount:
        try:
            return result.groupby(ind + ['u']).agg("sum")
        except KeyError:
            return result.groupby(ind).agg("sum")
    else:
        try:
            return result.reset_index().set_index(ind + ['u'])
        except KeyError:
            return result.reset_index().set_index(ind)

# GH #783: harmonized_food_labels() / harmonized_food_labels2() were
# REMOVED here.  They read the retired standalone ../../_/food_items.org
# with pd.read_csv(delimiter="|") -- a parse that no longer matches any
# file we ship -- and had ZERO callers in the repo (measured with
# `git grep harmonized_food_labels`), so they would have raised if
# called.  The vocabulary now lives in _/categorical_mapping.org as
# `harmonize_food`; read it with
#     df_from_orgfile(fn, name="harmonize_food")
# (cf. tanzania.harmonized_food_labels, ethiopia.harmonized_food_labels).

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

def household_characteristics(fn='',sex='',age='',HHID='HHID',months_spent='months_spent', fn_type = 'stata'):

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


# GH #808: the `_%d` (0-first) suffix convention below is RETIRED for
# food_acquired -- the framework's updated_ids (panel_ids.py, bare-first) is
# the single source of panel ids there.  Still used by _/household_roster.py.
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
    #generalize to ids being a list of columns needing to be joined        
    if type(id0) == list:
        id['id0'] = concate_id(id, id0[0], id0[1],True, 2)
        id0 = 'id0'
    if type(id1) == list:
        id['id1'] = concate_id(id, id1[0], id1[1],True, 2)
        id1 = 'id1'

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
        d.pop(pd.NA)  # Get rid of nan key, if any
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

def concate_id(df, parta, partb, leading_zero = False, digit = None):
    df = df.replace('', pd.NA)
    df.loc[df[parta].isna(), partb] = pd.NA
    df.loc[df[partb].isna(), parta] = pd.NA
    if leading_zero and digit != None:
        df['newid'] = df[parta].astype('Int64').astype(str) + df[partb].astype('Int64').astype(str).str.zfill(digit)
    else:
        df['newid'] = df[parta].astype('Int64').astype(str) + df[partb].astype('Int64').astype(str)
    na_id = df.loc[df[parta].isna(), 'newid'].iloc[0]
    df['newid'] = df['newid'].replace(na_id, pd.NA)
    return df['newid']


# ---------------------------------------------------------------------------
# BID Appendix I -- the authoritative GLSS1/GLSS2 cluster attribute table.
# ---------------------------------------------------------------------------

def appendix_i_cluster_attributes(year_column, offset):
    """Cluster attributes for a GLSS1/GLSS2 wave, from BID Appendix I.

    GLSS1 and GLSS2 ship NO cluster-location variable -- a sweep of all 170
    dictionaries finds none.  `cluster_features.Region` used to be INFERRED as
    the modal birth region of a cluster's under-12s, which was ~98% right and
    wrong for five clusters.  Appendix I of the joint Basic Information
    Document is the survey's own cluster list and is authoritative for these
    waves (decision, 2026-08-21); see `_/appendix_i_clusters.org`.

    Parameters
    ----------
    year_column : {'yr1', 'yr2'}
        Appendix column holding this wave's cluster number.
    offset : int
        Added to that number to reach the microdata's ``CLUST``
        (1000 for 1987-88, 2000 for 1988-89).  Verified: 176/176 and 170/170.

    Returns
    -------
    DataFrame indexed by ``v`` (zero-free string ``CLUST``) with columns
    ``Region``, ``Rural`` and ``Ecological_zone``.

    ``Rural`` IS returned, folded to the canonical binary {Urban, Rural} by
    the ``urbrur_abbrev`` table in the same org file.  The Appendix classifies
    clusters THREE ways -- U / R / SU (semi-urban, 52 of 263 clusters, 20%) --
    and ``SU`` is delivered as ``Rural`` **by decision** (@ligon, 2026-08-21).

    That is a judgement call and **the survey offers no evidence either way**.
    The GLSS2 7-way ``rural`` table's ``Classification`` column was cited on
    GH #690 as precedent for folding a middle tier to Rural; it is *editorial,
    not on the questionnaire* (GH #692), and it decodes ``Y01C:NRCPL`` -- a
    *non-resident child's* place of residence, not the household's settlement
    class.  Do not cite it.

    The U/R/SU distinction IS lost in the delivered column.  The raw three-way
    survives in the ``UrbRur`` column of ``_/appendix_i_clusters.org`` for
    anyone who needs it.
    """
    # countries_root() honours LSMS_COUNTRIES_ROOT; a hardcoded
    # files("lsms_library")/"countries" would silently read the installed
    # package's config tree instead of a worktree under development (GH #436).
    from lsms_library.paths import countries_root
    path = countries_root()/'GhanaLSS'/'_'/'appendix_i_clusters.org'
    tbl = tools.df_from_orgfile(path, name='appendix_i_clusters', to_numeric=False)
    tbl.columns = [str(c).strip() for c in tbl.columns]
    tbl = tbl.map(lambda x: str(x).strip() if pd.notna(x) else x)

    regions = tools.df_from_orgfile(path, name='region_abbrev', to_numeric=False)
    regions.columns = [str(c).strip() for c in regions.columns]
    region_map = dict(zip(regions['Reg'].str.strip(), regions['Region'].str.strip()))

    urbrur = tools.df_from_orgfile(path, name='urbrur_abbrev', to_numeric=False)
    urbrur.columns = [str(c).strip() for c in urbrur.columns]
    urbrur_map = dict(zip(urbrur['UrbRur'].str.strip(), urbrur['Rural'].str.strip()))

    zones = tools.df_from_orgfile(path, name='ecozone_abbrev', to_numeric=False)
    zones.columns = [str(c).strip() for c in zones.columns]
    zone_map = dict(zip(zones['EcoZone'].str.strip(), zones['Ecological_zone'].str.strip()))

    # A decode that silently yields {} is this country's most expensive
    # recurring defect (GH #372/#377, #348, and the 2026-08-19 revival).
    # Assert rather than discover it as a 100%-null column downstream.
    assert region_map, 'region_abbrev decoded to an EMPTY dict'
    assert zone_map, 'ecozone_abbrev decoded to an EMPTY dict'
    assert urbrur_map, 'urbrur_abbrev decoded to an EMPTY dict'

    tbl = tbl[tbl[year_column].astype(str).str.strip().ne('.')].copy()
    tbl['v'] = (tbl[year_column].astype(int) + offset).astype(str)

    out = pd.DataFrame({
        'Region': tbl['Reg'].map(region_map).values,
        'Rural': tbl['UrbRur'].map(urbrur_map).values,
        'Ecological_zone': tbl['EcoZone'].map(zone_map).values,
    }, index=pd.Index(tbl['v'].values, name='v'))

    unmapped = tbl.loc[out['Region'].isna().values, 'Reg'].unique()
    assert len(unmapped) == 0, f'unmapped region abbreviations: {list(unmapped)}'
    bad_ur = tbl.loc[out['Rural'].isna().values, 'UrbRur'].unique()
    assert len(bad_ur) == 0, f'unmapped urb/rur abbreviations: {list(bad_ur)}'
    return out


# ---------------------------------------------------------------------------
# GLSS1 / GLSS2 section 12B -- own-production value, DERIVED (2026-09-11)
#
# Registry: _/derivations.yml, key GhanaLSS::food_acquired::12b-fortnight.
# Design: SkunkWorks/derived_values.org, "First instance".
# ---------------------------------------------------------------------------

#: Days per 12B time-unit code (question 4's legend: DAY 3, WEEK 4, MONTH 5,
#: QUARTER 6, HALF YEAR 7, YEAR 8).  30.4 / 91.3 / 182.6 are GSS's own month,
#: quarter and half-year lengths -- their annual EXPEND.HPFOOD reproduces to
#: within 2% only with them (30 days per month gives 1.03-1.04).
DAYS_PER_UNIT = {3: 1.0, 4: 7.0, 5: 30.4, 6: 91.3, 7: 182.6, 8: 365.0}

#: The purchase side's nominal window: section 12A is "since my last visit",
#: and the 1988-89 Interviewer Manual (printed p.70) says of that interval
#: "in theory this period is two weeks".  A module constant, NOT an argument:
#: the cached parquet must be ONE identifiable construction.
WINDOW_DAYS = 14


def _as_float(x):
    return pd.to_numeric(pd.Series(np.ravel(x)).replace({'.': np.nan}),
                         errors='coerce').to_numpy(dtype=float)


def derive_12b_fortnight_value(months, times, unit_code, value_each_time):
    """Value of own-produced food eaten in a FORTNIGHT drawn at random from the year.

    Section 12B of GLSS1/GLSS2 never asks a "since my last visit" question.
    For each home-produced food it asks:

      q2  MFOODCLY  -- in how many of the last 12 months was it eaten;
      q3  TFOODC    -- how many times per (q4 unit) it was eaten;
      q4  UTFOODC   -- the unit of q3 (DAY 3 .. YEAR 8, see DAYS_PER_UNIT);
      q5  VFOODCPD  -- "How much would it cost to buy the amount they ate
                       each time?" -- the value of ONE EATING OCCASION.

    The served ``Expenditure`` for a produced row is therefore a CONSTRUCTION,
    not an answer.  This is the one construction the library serves::

        value_each_time * times * (WINDOW_DAYS / DAYS_PER_UNIT[unit_code])
                        * months / 12

    i.e. the year-average fortnight: GSS's own annual construction
    ``EXPEND.HPFOOD`` (= value x times x (30.4 / days) x months) divided by
    26.  Worked example (agreed with @ligon, 2026-09-11): eaten in 6 months
    of the 12, 12 times a month, 10 cedis each time -> 120 a month in season,
    720 a year (``HPFOOD``), 55.3 for an in-season fortnight, and **27.6** for
    a fortnight drawn at random from the year -- the 6/12 being the chance
    the interview fortnight lands in season.

    Why the year average and not the in-season figure: the purchase side
    (12A) is a REALISED fortnight, but 12B does not record WHICH months were
    cited, so the in-season number is right for some households and zero is
    right for the rest, unknowably.  The year average is unbiased over the
    sample and defers to the construction the people who collected the data
    used.  The alternatives are computable from
    ``Country('GhanaLSS').derivation_inputs(key)``:  the in-season fortnight
    drops the ``months / 12`` factor; GSS's annual ``HPFOOD`` is this x 26
    (up to 30.4 x 12 = 364.8 vs 365 days).

    No options, by design: a cached parquet is one identifiable construction.
    Vectorised; accepts scalars, arrays or Series (elementwise, positional).
    Returns a float for scalar inputs, else a float ndarray.  An unknown unit
    code yields NaN.  Where ``months`` is 0 (three 1987-88 rows) the value is
    0 -- the row is served, labelled, and contributes nothing.
    """
    months_f = _as_float(months)
    times_f = _as_float(times)
    value_f = _as_float(value_each_time)
    codes = pd.to_numeric(pd.Series(np.ravel(unit_code)).replace({'.': np.nan}),
                          errors='coerce').astype('Int64')
    days = codes.map(DAYS_PER_UNIT).astype(float).to_numpy()
    out = value_f * times_f * (WINDOW_DAYS / days) * months_f / 12.0
    scalar = all(np.ndim(x) == 0 for x in (months, times, unit_code, value_each_time))
    return float(out[0]) if scalar else out


def _load_module_by_path(path, name):
    import importlib.util as _ilu
    spec = _ilu.spec_from_file_location(name, path)
    mod = _ilu.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def inputs_12b(wave):
    """The raw section-12B answers behind the derived produced rows of one wave.

    Re-reads ``Y12B.DAT`` through ``get_dataframe`` and returns a frame indexed
    ``(t, i, j)`` -- ``i`` via the wave's own ``mapping.i()``, ``j`` via the
    same ``harmonize_food`` decode the wave script uses -- whose columns are
    the ORIGINAL variable names: ``FOODCD``, ``MFOODCLY``, ``TFOODC``,
    ``UTFOODC``, ``VFOODCPD``.  Every source row is returned, unfiltered
    (the wave script drops zero / missing ``VFOODCPD``), at the INPUT grain:
    several ``FOODCD`` harmonise to one ``j``, so ``(t, i, j)`` is not unique
    here and ``FOODCD`` is kept so the row can be read against the
    questionnaire.  Nothing is cached.
    """
    from lsms_library.paths import countries_root
    from lsms_library.local_tools import df_from_orgfile, format_id
    if wave not in ('1987-88', '1988-89'):
        raise ValueError(f'section 12B exists only in 1987-88 and 1988-89, not {wave!r}')
    root = countries_root() / 'GhanaLSS' / wave
    df = get_dataframe(str(root / 'Data' / 'Y12B.DAT'))
    mapping = _load_module_by_path(root / '_' / 'mapping.py', f'_ghanalss_mapping_{wave}')
    labels = df_from_orgfile(str(root / '_' / 'categorical_mapping.org'),
                             name='harmonize_food', encoding='ISO-8859-1')
    codes = labels['Code_12B'].astype('Int64').astype('string')
    lab = labels[['Preferred Label']].set_index(codes)['Preferred Label'].to_dict()
    out = pd.DataFrame({
        't': wave,
        'i': df['HID'].apply(mapping.i),
        'j': df['FOODCD'].apply(format_id).astype('string').replace(lab),
        'FOODCD': df['FOODCD'],
        'MFOODCLY': df['MFOODCLY'], 'TFOODC': df['TFOODC'],
        'UTFOODC': df['UTFOODC'], 'VFOODCPD': df['VFOODCPD'],
    })
    return out.set_index(['t', 'i', 'j'])


# Hectares per native plot-area unit (GH #879).  The canonical schema
# (lsms_library/data_info.yml) declares plot_features.Area in HECTARES,
# with AreaUnit the provenance label of the original unit.  GLSS Section
# 8B reports farm size (s8bq4a/s8bq4) with a unit code decoded to
# Acres/Poles/Ropes/Hectare (older waves) or the GLSS7 labels (Acres /
# Poles / Ropes / Plot / Hectare / Other).
#
# The factors are the Ghana STATISTICAL SERVICE's own: GhanaSPS 2009-10
# ships a producer-computed area_ha alongside the native size, and
# dividing the two recovers these factors to float precision (measured
# there; see GhanaSPS/_/ghanasps.py::_PLOT_HECTARES_PER_UNIT, from which
# they are copied verbatim).  The acre factor is the GSS's 0.404694, not
# the standard 0.404686 -- applying the producer's own number keeps
# GhanaLSS consistent with the same service's published hectares.
# 'Other' names nothing -> NaN Area with the native unit still recorded
# in AreaUnit (honest, not fabricated); a null unit likewise.
_PLOT_HECTARES_PER_UNIT = {
    'Acres': 0.404694,
    'Poles': 0.409551,
    'Ropes': 0.236342,
    'Plot': 0.102388,
    'Hectare': 1.0,
}


def plot_features(df):
    """df_edit hook: convert ``Area`` to hectares per-row on ``AreaUnit``.

    Rows whose unit has no factor in :data:`_PLOT_HECTARES_PER_UNIT`
    ('Other', missing) get a NaN Area -- the schema's required column is
    in hectares and a guessed factor is worse than an honest gap
    (GH #879).  ``AreaUnit`` is left untouched: it is the provenance
    label of the unit the SURVEY recorded, not of the unit served.
    """
    df = df.copy()
    area = pd.to_numeric(df['Area'], errors='coerce')
    factor = df['AreaUnit'].astype(object).map(_PLOT_HECTARES_PER_UNIT)
    df['Area'] = area * pd.to_numeric(factor, errors='coerce')
    return df


# ---------------------------------------------------------------------------
# GLSS4 / GLSS5 / GLSS6 / GLSS7 section 8H -- own-production value, DERIVED
# (2026-09-13)
#
# Registry: _/derivations.yml, key GhanaLSS::food_acquired::8h-farmgate.
# Ledger:   .coder/ledger/ghanalss-produced-qp.md
# ---------------------------------------------------------------------------

#: Waves whose section-8H produced rows carry BOTH a native-unit quantity and a
#: reported farmgate price.  1991-92 is deliberately absent: only 80,282 of its
#: 286,922 produced rows (28%) carry both, and its dominant unit is ``All``
#: (93,349 rows), which reads as "the whole harvest" rather than a countable
#: quantity.  See the ledger, and `_/CONTENTS.org`.
FARMGATE_WAVES = ('1998-99', '2005-06', '2012-13', '2016-17')

#: The section-8H price variable, per wave.  2016-17 asks it once per VISIT
#: (``s8hq{v}p``), the others once per (household, item, unit).
FARMGATE_PRICE_VARIABLE = {
    '1998-99': 's8hq10',
    '2005-06': 's8hq14',
    '2012-13': 's8hq10',
    '2016-17': 's8hq{v}p',
}


def derive_produced_farmgate_value(quantity, price):
    """Value of own-produced food at the household's own reported farmgate price.

    Section 8H of GLSS4-GLSS7 asks, for each home-produced food the household
    consumed, *how much* was consumed in the recall window (a real quantity in
    a native unit ``u``) and *what it would fetch* -- the farmgate price of one
    such unit.  It never asks what the food was worth in total, so the served
    ``Expenditure`` on a produced row is a CONSTRUCTION, not an answer::

        Expenditure = Quantity * Price

    **This reverses a prior decision, deliberately.** Each wave script used to
    say "Expenditure is left NaN -- no produced value is recorded", and that
    sentence is true about the *survey*; it was never a statement that the
    value could not be constructed.  The 2005-06 script already multiplies the
    same two columns internally (``y['_qp']``) to resolve households that
    report two farmgate prices for one commodity.  What changed is that the
    library now has somewhere honest to put a construction: it is served with
    the registry key on the row, so a reader can tell it from an answer and can
    recover the factors through ``Country.derivation_inputs``.

    The valuation is at the **producer** price, which is what section 8H asks.
    It is therefore NOT comparable with the GLSS1/GLSS2 ``12b-fortnight``
    figure, which values own consumption at what it would **cost to buy**
    (``VFOODCPD``); a farmgate valuation should sit below a consumer-price one,
    and does.  An analyst who wants own production at consumer prices should
    re-value it from ``food_prices()``; the factors are in the table.

    No options, by design: a cached parquet must be one identifiable
    construction.  Vectorised; accepts scalars, arrays or Series (elementwise,
    positional).  Returns a float ndarray, NaN wherever either factor is
    missing -- such a row is served with ``Expenditure`` NaN and carries **no**
    registry key, so the coverage this derivation claims is exactly the set of
    rows it actually computed.
    """
    q = _as_float(quantity)
    p = _as_float(price)
    return q * p


def _farmgate_hhid(wave, df):
    """Rebuild a wave's household id EXACTLY as its own ``food_acquired.py`` does.

    Each of the four waves composes ``i`` differently, and a generic guess at
    the id column does not join (``CLAUDE.md`` §"Derived Values", step 4:
    reusing the YAML's ``idxvars`` is not enough where ``mapping.py``
    post-processes the index).  Verified against the served rows by
    ``tests/test_ghanalss_8h_farmgate.py``.
    """
    from lsms_library.local_tools import format_id

    if wave == '1998-99':          # format_id(clust) + format_id(nh, pad 2), no sep
        return df.apply(
            lambda r: (format_id(r['clust'], zeropadding=0) or '')
            + (format_id(r['nh'], zeropadding=2) or ''), axis=1)
    if wave == '2005-06':          # mapping.i() over the pre-composed 'hhid'
        from lsms_library.paths import countries_root
        mapping = _load_module_by_path(
            countries_root() / 'GhanaLSS' / wave / '_' / 'mapping.py',
            f'_ghanalss_mapping_{wave}')
        return df['hhid'].apply(mapping.i)
    if wave == '2012-13':          # pre-composed 'hid', carried through as-is
        return df['hid']
    if wave == '2016-17':          # format_id(clust) + '/' + format_id(nh, pad 2)
        def _one(c, n):
            c, n = format_id(c), format_id(n, zeropadding=2)
            return pd.NA if (c is None or n is None) else f'{c}/{n}'
        return pd.Series([_one(c, n) for c, n in zip(df['clust'], df['nh'])],
                         index=df.index)
    raise ValueError(f'no household-id construction recorded for {wave!r}')


def inputs_produced_farmgate(wave):
    """The raw section-8H answers behind the derived produced rows of one wave.

    Returns a frame at the INPUT grain -- one row per source record, before the
    wave script's visit melt and unit canonicalisation -- indexed ``(t, i)``
    with the ORIGINAL variable names kept, so a row can be read against the
    questionnaire.  ``i`` goes through the wave's own ``mapping.i`` so the frame
    joins to the served rows (``CLAUDE.md`` §"Derived Values", step 4: reusing
    the YAML's ``idxvars`` is not enough where ``mapping.py`` rewrites the
    index).  Nothing is cached.
    """
    from lsms_library.paths import countries_root

    if wave not in FARMGATE_WAVES:
        raise ValueError(
            f'section 8H farmgate valuation covers {FARMGATE_WAVES}, not {wave!r} '
            f'(1991-92 is excluded by coverage -- see _/CONTENTS.org)')

    sources = {
        '1998-99': 'Data/SEC8H.DTA',
        '2005-06': 'Data/partb/sec8h.dta',
        '2012-13': 'Data/PARTB/sec8h.dta',
        '2016-17': 'Data/g7sec8h.dta',
    }
    root = countries_root() / 'GhanaLSS' / wave
    df = get_dataframe(str(root / sources[wave]), convert_categoricals=False)

    price_var = FARMGATE_PRICE_VARIABLE[wave]
    keep = [c for c in df.columns
            if c.lower().startswith('s8hq') or c.lower() in {'homagrcd', 'itemcd'}]
    out = df[keep].copy()
    out.insert(0, 't', wave)
    out.insert(1, 'i', _farmgate_hhid(wave, df))
    out.attrs['price_variable'] = price_var
    return out.set_index(['t', 'i'])


def to_country_food_labels(df, wave, level='j'):
    """Map a wave's own ``harmonize_food`` label onto the COUNTRY one (Lcp).

    Each GLSS round names foods in its own vocabulary; the country-level
    ``_/categorical_mapping.org`` ``harmonize_food`` table records exactly that
    round's spelling in its ``<wave>`` column, against the canonical
    ``Preferred Label``.  That crosswalk is what makes ``j`` comparable across
    rounds -- and until 2026-09-16 *nothing applied it*, so the served ``j`` was
    the wave vocabulary and 45 of 225 labels never reached the country axis
    (``slurm_logs/ghanalss_aggregate_labels/FINDINGS.org``).  This function is
    that step.

    It is a **pure rename**: nothing is summed, dropped or reindexed.  The
    country table is a bijection onto each wave's column (0 ambiguous cells in
    all seven waves, asserted here), so two wave labels cannot collide on one
    country label within a wave, and the index stays as unique as it was.

    An unmapped label is a **hard error**, not a pass-through: a silent
    pass-through is exactly the defect this replaces, and ``pandas.rename``
    would do it by default.
    """
    from lsms_library.paths import countries_root
    from lsms_library.local_tools import df_from_orgfile

    tbl = df_from_orgfile(
        str(countries_root() / 'GhanaLSS' / '_' / 'categorical_mapping.org'),
        name='harmonize_food')
    tbl.columns = [str(c).strip() for c in tbl.columns]
    if wave not in tbl.columns:
        raise KeyError(f'harmonize_food has no column for wave {wave!r}')
    native = tbl[wave].astype(str).str.strip()
    canon = tbl['Preferred Label'].astype(str).str.strip()
    pairs = [(n, c) for n, c in zip(native, canon) if n and n != 'nan']
    dup = {n for n, _ in pairs if sum(1 for m, _ in pairs if m == n) > 1}
    if dup:
        raise ValueError(
            f'harmonize_food column {wave!r} is not injective -- {sorted(dup)} '
            f'each name more than one Preferred Label; the crosswalk is ambiguous')
    m = dict(pairs)

    if level in (df.index.names or []):
        seen = set(df.index.get_level_values(level).dropna().astype(str))
    else:
        seen = set(df[level].dropna().astype(str))
    missing = sorted(seen - set(m))
    if missing:
        raise KeyError(
            f'GhanaLSS {wave}: {len(missing)} label(s) on {level!r} are absent from the '
            f'{wave!r} column of the country harmonize_food, so they cannot be mapped '
            f'onto the country axis: {missing[:12]}.  Add them to '
            f'countries/GhanaLSS/_/categorical_mapping.org rather than letting them '
            f'pass through -- a pass-through is GH #782/#925 all over again.')

    if level in (df.index.names or []):
        return df.rename(index=m, level=level)
    out = df.copy()
    out[level] = out[level].map(lambda v: m.get(str(v), v))
    return out


_ADDITIVE_MEASURES = ('Expenditure',)


def reduce_duplicate_food_rows(df, keys, quantity='Quantity', price='Price'):
    """Collapse rows sharing ``keys``: ``quantity`` SUMS, ``price`` becomes the
    QUANTITY-WEIGHTED MEAN.

    The named reducer this country's CONTENTS.org requires wherever several
    survey lines harmonise onto one ``j``.  It exists because the alternatives
    are both wrong:

    * leaving the duplicate for the framework -- ``food_acquired`` is in
      ``_ADDITIVE_MEASURE_COLUMNS``, so core SUMs Quantity/Expenditure and then
      re-derives ``Price = Expenditure / Quantity`` across the WHOLE frame,
      destroying every recorded farmgate price (CONTENTS.org Trap 9);
    * ``groupby().first()`` -- keeps one price and silently discards the other,
      which is GH #323's hazard.

    The weighted mean is the price OF THE HARMONISED COMMODITY, and it is the
    choice that makes ``Quantity * Price`` add up across the merge: with
    ``Q = sum(q)`` and ``P = sum(q*p)/sum(q)``, ``Q*P == sum(q*p)``, so a wave
    that derives Expenditure from Quantity x Price (1998-99) conserves it
    exactly.  Where the summed quantity is 0 or missing the first price is kept
    rather than dividing by zero.

    Same rule as the inline reduction in ``2005-06/_/food_acquired.py`` (the
    mutton+goat merge); that one predates this helper and is left as it is.
    """
    d = df.copy()
    d['_qp'] = (pd.to_numeric(d[quantity], errors='coerce')
                * pd.to_numeric(d[price], errors='coerce'))
    # min_count=1 throughout: a plain 'sum' returns 0.0 for an ALL-NA group,
    # which would turn "this visit recorded nothing" into a recorded zero and
    # smuggle the row past the caller's dropna(how='all').  Measured when this
    # was wrong: +32,040 phantom rows on the delivered table.
    _sum = lambda s: s.sum(min_count=1)
    spec = {quantity: (quantity, _sum), '_qp': ('_qp', _sum), '_pf': (price, 'first')}
    # Every OTHER column is carried, not dropped: the additive measures SUM
    # (they are `_ADDITIVE_MEASURE_COLUMNS` and a sum of all-NA stays NA), and
    # anything else -- `s`, a wave tag -- is constant within a key by
    # construction, so `first` is exact rather than a choice.
    carried = [c for c in d.columns if c not in keys and c not in (quantity, price, '_qp')]
    for c in carried:
        spec[c] = (c, _sum) if c in _ADDITIVE_MEASURES else (c, 'first')
    out = d.groupby(keys, sort=False, dropna=False).agg(**spec).reset_index()
    q = pd.to_numeric(out[quantity], errors='coerce')
    out[price] = (out['_qp'] / q).where(q.notna() & (q != 0), out['_pf'])
    return out.drop(columns=['_qp', '_pf'])


def reconcile_after_crosswalk(df):
    """Reduce the duplicates that ``to_country_food_labels`` can create.

    The crosswalk is many-to-one by design: 1991-92 and 1998-99 field guinea
    corn and sorghum as two separate own-production lines (``Code_8h`` 4 and 7)
    and they are the same crop, so both map to ``Guinea Corn/Sorghum``.  A
    household that filed both then holds two rows on one
    ``(t, i, j, u, s, visit)``.

    Reducing HERE -- after Lcp, on the canonical grain -- is the point.  Doing
    it earlier would mean rewriting the wave's own vocabulary, which is not
    ours to rewrite: those really are two lines on that questionnaire, and the
    per-wave columns exist precisely so the harmonisation can happen at the
    country level instead.  Doing it later means core does it, and core SUMs
    the additive measures and then re-derives ``Price`` across the whole frame
    (CONTENTS.org Trap 9).

    No-op where the crosswalk introduced no duplicate, and asserts it left the
    index unique.
    """
    names = list(df.index.names)
    cols = list(df.columns)
    if not df.index.duplicated().any():
        return df
    out = reduce_duplicate_food_rows(df.reset_index(), names)
    out = out.set_index(names)[cols]
    assert not out.index.duplicated().any(), (
        'reconcile_after_crosswalk left duplicates on ' + repr(names))
    return out
