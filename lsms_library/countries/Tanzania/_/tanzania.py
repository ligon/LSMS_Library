import pandas as pd
import numpy as np
import warnings
import json
import sys
sys.path.append('../../_')
sys.path.append('../../../_')
from lsms_library.local_tools import format_id, id_walk, RecursiveDict, get_dataframe, update_id, DVCFS
from collections import defaultdict

country = 'Tanzania'

def _is_primary_hhid(rid):
    """Check whether an r_hhid represents a primary (non-split-off) household.

    Round 2 (16-digit): suffix '01' is primary, '02'+ is split-off.
    Rounds 3-4 (NNNN-NNN): suffix '001' is primary, others are split-offs.
    Round 1 (14-digit): all primary (no splits yet).
    """
    rid = str(rid)
    if len(rid) == 16:           # round 2
        return rid.endswith('01')
    elif '-' in rid:             # rounds 3-4
        return rid.split('-')[1] == '001'
    return True                  # round 1 (all primary)


def map_08_15(df, col):
    """Build panel linkage for the 2008-15 multi-round file.

    Uses UPHI (Universal Panel Household Identifier) to link households
    across the 4 rounds.  Multiple UPHIs can share the same r_hhid in
    early rounds and diverge later (household splits).

    To avoid many-to-one collisions (GitHub #114), this function builds
    composite R1-based IDs.  When multiple primary r_hhid values in rounds
    3-4 trace back to the same R1 r_hhid (because the original household
    split), the continuation household (lowest UPHI) keeps the bare R1
    r_hhid; split-offs get a suffix (e.g., ``01010140020171-s02``).

    Returns a DataFrame indexed by ``(t, i)`` with column ``previous_i``
    mapping each round's r_hhid to its R1-based composite ID.
    """
    hhid = df[col].copy()
    map_round = {1: '2008-09', 2: '2010-11', 3: '2012-13', 4: '2014-15'}

    # --- Step 1: canonical UPHI per (r_hhid, round) -----------------------
    # Food data is at (r_hhid, round) level, not UPHI level.  Pick the
    # minimum UPHI per (r_hhid, round) as the representative for lineage
    # tracing.
    canonical = hhid.groupby(['round', 'r_hhid'])['UPHI'].min().reset_index()
    canonical.columns = ['round', 'r_hhid', 'min_UPHI']

    # --- Step 2: map each canonical UPHI to its R1 r_hhid -----------------
    r1_hhid_map = hhid[hhid['round'] == 1].drop_duplicates('UPHI').set_index('UPHI')['r_hhid'].to_dict()
    canonical['r1_hhid'] = canonical['min_UPHI'].map(r1_hhid_map)

    # Entries without R1 mapping (e.g., households first appearing in R4
    # refresh panel) cannot be chained; they keep their own r_hhid.
    has_r1 = canonical.dropna(subset=['r1_hhid']).copy()

    # --- Step 3: assign composite IDs within each (round, r1_hhid) group --
    # Sort by min_UPHI so the lowest UPHI (continuation household) gets
    # rank 0 and keeps the bare R1 r_hhid; higher ranks are split-offs.
    has_r1 = has_r1.sort_values(['round', 'r1_hhid', 'min_UPHI'])
    has_r1['rank'] = has_r1.groupby(['round', 'r1_hhid']).cumcount()

    def _make_composite(row):
        r1 = str(row['r1_hhid'])
        if row['rank'] == 0:
            return r1
        return f'{r1}-s{int(row["rank"]) + 1:02d}'

    has_r1['composite_id'] = has_r1.apply(_make_composite, axis=1)

    # --- Step 4: filter to primary households only -------------------------
    has_r1 = has_r1[has_r1['r_hhid'].apply(_is_primary_hhid)]

    # --- Step 5: build (t, i) -> previous_i linkage -----------------------
    # For rounds > 1, map r_hhid -> composite_id (R1-based).
    rows = []
    for rnd in [2, 3, 4]:
        sub = has_r1[has_r1['round'] == rnd]
        for _, row in sub.iterrows():
            rows.append({
                't': map_round[rnd],
                'i': str(row['r_hhid']),
                'previous_i': row['composite_id'],
            })

    result = pd.DataFrame(rows)
    result = result.set_index(['t', 'i'])[['previous_i']]
    # Safety: drop any residual duplicate linkage entries
    result = result.loc[~result.index.duplicated(keep='first')]
    return result


def _map_with_head_tracking(cover_df, roster_df, current_id, previous_id,
                            head_var='hh_b05', prev_member_var='hh_b06'):
    """Map current-wave IDs to previous-wave IDs, using head-tracking for splits.

    When multiple current-wave households link to the same previous-wave
    household (a split), the household whose head was the head in the
    previous round (prev_member_var == 1) inherits the canonical ID.
    Split-offs get their own new IDs.

    Returns a DataFrame indexed by (t=None, i=current_id) with column
    'previous_i' = previous_id for the continuation household only.
    Split-offs map to themselves (new canonical IDs, no backward link).
    """
    # Identify which current HH contains the previous-round head
    heads = roster_df[roster_df[head_var] == 'HEAD'][[current_id, prev_member_var]].copy()
    heads = heads.dropna(subset=[prev_member_var])

    # Merge cover with head info
    merged = pd.merge(cover_df[[current_id, previous_id]].dropna(),
                      heads, on=current_id, how='left')

    # For splits: previous_id appears multiple times
    dup_mask = merged.duplicated(subset=[previous_id], keep=False)
    non_splits = merged[~dup_mask]
    splits = merged[dup_mask]

    result_rows = []

    # Non-split households: simple linkage
    for _, row in non_splits.iterrows():
        result_rows.append({'i': format_id(row[current_id]),
                           'previous_i': format_id(row[previous_id])})

    # Split households: head with prev_member_var == 1 inherits canonical ID
    if not splits.empty:
        for prev_val, group in splits.groupby(previous_id):
            # The continuation household: head was member #1 in prev round
            continuation = group[group[prev_member_var] == 1.0]
            if continuation.empty:
                # Fallback: if no member #1, pick the first one
                continuation = group.head(1)

            # Continuation inherits the backward link
            for _, row in continuation.iterrows():
                result_rows.append({'i': format_id(row[current_id]),
                                   'previous_i': format_id(prev_val)})

            # Split-offs are deliberately excluded from the linkage —
            # they will appear as new households starting from this wave.

    return pd.DataFrame(result_rows)


Waves = {'2008-15': ('upd4_hh_a.dta', ['r_hhid', 'round', 'UPHI'], map_08_15),
         '2019-20': ('HH_SEC_A.dta', 'sdd_hhid', 'y4_hhid'),
         '2020-21': ('hh_sec_a.dta', 'y5_hhid', 'y4_hhid')}

waves = ['2008-09', '2010-11', '2012-13', '2014-15', '2019-20', '2020-21']
wave_folder_map = {'2008-09':'2008-15', '2010-11':'2008-15', '2012-13':'2008-15', '2014-15':'2008-15', '2019-20':'2019-20', '2020-21':'2020-21'}

def harmonized_food_labels(fn='../../_/categorical_mapping.org', name='harmonize_food'):
    # Harmonized food labels.  Reads the canonical harmonize_food table
    # (migrated from the retired food_items.org -- Unit #0 shared label
    # foundation).  Returns a {raw per-wave survey label -> Preferred Label}
    # dict by stacking every per-wave code column, so a raw food_acquired
    # (j) label resolves to its canonical Preferred Label regardless of wave.
    from lsms_library.local_tools import df_from_orgfile
    hf = df_from_orgfile(fn, name=name)
    wave_cols = [c for c in hf.columns if c not in ('Code', 'Preferred Label', 'FTC Label')]
    out = {}
    for col in wave_cols:
        for raw, pref in zip(hf[col], hf['Preferred Label']):
            if isinstance(raw, str) and raw.strip():
                out[raw.strip()] = pref
    return out
    

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


def _household_identification_from_file(fn, HHID='HHID', urban='urban', region='region',
                                         urban_converter=None, region_converter=None,
                                         convert_categoricals=True, wave=None, **kwargs):
    """Inline replacement for lsms.tools.get_household_identification_particulars."""
    df = get_dataframe(fn, convert_categoricals=convert_categoricals)
    df = df.rename(columns={HHID: 'HHID', urban: 'urban', region: 'region'})
    if kwargs:
        df = df.rename(columns={v: k for k, v in kwargs.items()})
    if urban_converter is not None:
        df['urban'] = df['urban'].apply(urban_converter)
    if region_converter is not None:
        df['region'] = df['region'].apply(region_converter)
    df['region'] = df['region'].apply(lambda s: str(s).lower())
    df['urban'] = df['urban'].apply(lambda x: x == 1)
    try:
        if df['HHID'].iloc[0].split('.')[-1] == '0':
            df['HHID'] = df['HHID'].apply(lambda x: '%d' % int(float(x)))
    except (ValueError, AttributeError):
        pass
    columns = ['urban', 'region']
    if wave is not None:
        columns += ['wave']
        df['wave'] = df[wave]
    columns += list(kwargs.keys())
    df = df.loc[:, ['HHID'] + columns]
    df = df.set_index('HHID')
    return df.loc[:, columns]


def _household_roster_from_file(fn, sex='sex', age='age', HHID='HHID',
                                  months_spent='months_spent', sex_converter=None,
                                  months_converter=None, Age_ints=None, wave=None,
                                  convert_categoricals=True):
    """Inline replacement for lsms.tools.get_household_roster (file-opening path)."""
    df = get_dataframe(fn, convert_categoricals=convert_categoricals)
    cols = [c for c in [HHID, sex, age, months_spent, wave] if c is not None and c in df.columns]
    df = df.loc[:, cols].rename(columns={HHID: 'HHID', sex: 'sex', age: 'age',
                                          months_spent: 'months_spent'})
    if wave is not None and wave in df.columns:
        df = df.rename(columns={wave: 'wave'})
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
    idxs = ['HHID']
    if wave is not None:
        idxs += ['wave']
    valvars = list({'HHID','girls','boys','men','women'}.intersection(df.columns))
    if 'wave' in df.columns:
        valvars += ['wave']
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
        g = df.loc[df['months_spent'] > 0, valvars].groupby(idxs)
    else:
        g = df[valvars].groupby(idxs)
    return g.sum()


def prices_and_units(fn='',units='units',item='item',HHID='HHID',market='market',farmgate='farmgate'):

    food_items = harmonized_food_labels()

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

    if food_items is not None:
        df = df.replace({item: food_items})
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
    food_items = harmonized_food_labels()

    expenditures = _sum_expenditures_from_file(fn, purchased, away, produced, given,
                                                itmcd=item, HHID=HHID, itemlabels=food_items)

    expenditures.index.name = 'j'
    expenditures.columns.name = 'i'

    return expenditures

def food_quantities(fn='',item='item',HHID='HHID',
                    purchased=None,away=None,produced=None,given=None,units=None):
    food_items = harmonized_food_labels()

    quantities = _sum_expenditures_from_file(fn, purchased, away, produced, given,
                                              itmcd=item, HHID=HHID, units=units,
                                              itemlabels=food_items)

    quantities.index.name = 'j'
    quantities.columns.name = 'i'

    return quantities

def age_sex_composition(fn,sex='sex',sex_converter=None,age='age',
                        months_spent='months_spent',HHID='HHID',months_converter=None,
                        wave=None,convert_categoricals=True,Age_ints=None,fn_type='stata'):

    if Age_ints is None:
        # Match Uganda FCT categories
        Age_ints = ((0,4),(4,9),(9,14),(14,19),(19,31),(31,51),(51,100))

    df = _household_roster_from_file(fn, sex=sex, age=age, HHID=HHID,
                                      months_spent=months_spent,
                                      sex_converter=sex_converter,
                                      months_converter=months_converter,
                                      Age_ints=Age_ints, wave=wave,
                                      convert_categoricals=convert_categoricals)
    df.index.name = 'j'
    df.columns.name = 'k'

    return df

def harmonized_unit_labels(fn='../../_/unitlabels.csv',key='Label',value='Preferred Label'):
    unitlabels = pd.read_csv(fn)
    unitlabels.columns = [s.strip() for s in unitlabels.columns]
    unitlabels = unitlabels[[key,value]].dropna()
    unitlabels = unitlabels.set_index(key)
    return unitlabels.squeeze().str.strip().to_dict()

    
def food_acquired(fn,myvars):
    df = get_dataframe(fn)
    df = df.loc[:,list(myvars.values())].rename(columns={v:k for k,v in myvars.items()})

    if 'year' in myvars:
        #map round code to actual years
        dict = {1:'2008-09', 2:'2010-11', 3:'2012-13', 4:'2014-15'}
        df = df.replace({"year": dict})
        df = df.set_index(['HHID','item','year']).dropna(how='all')
        df.index.names = ['j','i','t']
        if not df.index.is_unique:
            # Same HH recorded multiple times due to tracking of complete HH
            # lineage in the UPHI system.  If dedup-by-index and dedup-by-row
            # produce the same row count, the duplicates are harmless
            # exact-duplicate rows; drop them by index.  Otherwise fail loud.
            #
            # NB: an earlier version ran ``pd.testing.assert_frame_equal`` as
            # a paranoia check between the two dedupes, but under pandas 2.x
            # a StringDtype(na_value=nan) index level vs. plain ``object``
            # dtype on the same string values trips ``assert_index_equal`` on
            # dtype.  The trailing ``is_unique`` check already provides the
            # safety net.
            if df[~df.index.duplicated()].shape[0] == df.reset_index().drop_duplicates().shape[0]:
                df = df[~df.index.duplicated()]
                if not df.index.is_unique:
                    raise ValueError("Non-unique index! Even after attempted fix.")
            else:
                raise AssertionError("Non-unique index with non-trivial duplicates!  Fix me!")
    else:
        df = df.set_index(['HHID','item']).dropna(how='all')
        df.index.names = ['j','i']

    # Fix type of hhids if need be
    if df.index.get_level_values('j').dtype ==float:
        fix = {k: v for k, v in zip(df.index.levels[0],df.index.levels[0].astype(int).astype(str))}
        df = df.rename(index=fix,level=0)

    #harmonize food labels 
    #df = df.rename(index=harmonized_food_labels(),level='i')
    unitlabels = {0: float("nan"), 'KILOGRAMS':'Kg', 'GRAMS':'Gram', 'LITRE':'Litre', 'MILLILITRE':'Millilitre', 'PIECES':'Piece'}
    unitcolumn = {'unit_ttl_consume': unitlabels, 'unit_purchase': unitlabels, 'unit_own': unitlabels, 'unit_inkind': unitlabels}
    df = df.replace(unitcolumn)

    #fix quantities that are read as categorical vars
    df = df.replace(['none', 'NONE', 'hakuna'], 0)
    df = df.astype({"quant_purchase": 'float64',
                    "quant_own" : 'float64',
                    "quant_inkind" : 'float64'})

    df['unitvalue_purchase'] = df['value_purchase']/df['quant_purchase']
    df['unitvalue_purchase'] = df['unitvalue_purchase'].where(np.isfinite(df['unitvalue_purchase']))


    #with open('../../_/conversion_to_kgs.json','r') as f:
        #conversion_to_kgs = pd.Series(json.load(f))
    #conversion_to_kgs.name='unit_ttl_consume_Kgs'
    #conversion_to_kgs.index.name='unit_ttl_consume'
    #df = df.join(conversion_to_kgs,on='unit_ttl_consume')
    #df = df.astype(float)
    return df

def other_features(fn,urban=None,region=None,HHID='HHID',urban_converter=None,wave=None,**kwargs):
    """
    Pass a dictionary othervars to grab other variables.
    """
    df = _household_identification_from_file(fn, HHID=HHID, urban=urban, region=region,
                                              urban_converter=urban_converter,
                                              wave=wave, **kwargs)
    # Fix any floats in j
    df.index.name = 'j'
    k = df.index.get_level_values('j')
    f2s = {i:str(i).split('.')[0] for i in k}

    df.columns.name = 'k'

    df = df.rename(index=f2s,level='j')

    return df


def id_match(df, wave, waves_dict):
    df = df.reset_index()
    if len(waves_dict[wave]) == 3:
        if 'y4_hhid' and 'UPHI' not in df.columns:
            h = get_dataframe('../%s/Data/%s' % (wave,waves_dict[wave][0]))
            h = h[[waves_dict[wave][1], waves_dict[wave][2]]]
            m = df.merge(h, how = 'left', left_on ='j', right_on =waves_dict[wave][2])

            uphi = get_dataframe('../2008-15/Data/upd4_hh_a.dta')[['UPHI','r_hhid','round']]
            uphi['UPHI'] = uphi['UPHI'].astype(int).astype(str)
            y4 = uphi.loc[uphi['round']==4, 'r_hhid'].to_frame().rename(columns ={'r_hhid':'y4_hhid'})
            uphi = uphi.join(y4)    
            uphi = uphi[['UPHI', 'y4_hhid']].dropna()
            m = m.merge(uphi, how= 'left', on = 'y4_hhid')

            m['UPHI'] = m['UPHI'].replace('', pd.NA)
            m['UPHI'] = m['UPHI'].fillna(m.pop(waves_dict[wave][2]))
            m.j = m.UPHI
            m = m.drop(columns=['UPHI', 'y4_hhid'])
            if 't' not in m.columns:
                m.insert(1, 't', wave) 

    if len(waves_dict[wave]) == 4:
        if 'UPHI'  in df.columns: 
            m = df.rename(columns={'UPHI': 'j'})
        else:
            h = get_dataframe('../%s/Data/%s' % (wave,waves_dict[wave][0]))
            h = h[[waves_dict[wave][1], waves_dict[wave][2], waves_dict[wave][3]]]
            h[waves_dict[wave][1]] = h[waves_dict[wave][1]].astype(int).astype(str)
            dict = {1:'2008-09', 2:'2010-11', 3:'2012-13', 4:'2014-15'}
            h = h.replace({"round": dict})
            m = df.merge(h.drop_duplicates(), how = 'left', left_on =['j','t'], right_on =[waves_dict[wave][2], waves_dict[wave][3]])
            m['UPHI'] = m['UPHI'].fillna(m.pop('j'))
            m = m.rename(columns={'UPHI': 'j'})
            m = m.drop(columns=[waves_dict[wave][2], waves_dict[wave][3]])
    return m

def food_acquired_to_canonical(df):
    '''
    Reshape Tanzania wide-form food_acquired output to the canonical
    (t, i, j, u, s) long form.  Phase 3 of GH #169 / DESIGN_food_acquired_
    canonical_2026-05-05.org.

    Input
    -----
    DataFrame produced by ``food_acquired()`` then ``new_harmonize_units()``,
    indexed by ``(j, t, i)`` where (per the legacy Tanzania convention)
    ``j`` is the HHID and ``i`` is the food item code.  Required columns:
    ``quant_purchase, unit_purchase, value_purchase, quant_own, unit_own,
    quant_inkind, unit_inkind`` plus the redundant ``quant_ttl_consume,
    unit_ttl_consume`` (dropped here — they are the sum of the per-source
    quants).

    Output
    ------
    DataFrame indexed by ``(t, i, j, u, s)`` with columns
    ``[Quantity, Expenditure]``, where the canonical convention is
    ``i`` = household, ``j`` = item, ``u`` = unit, ``s`` ∈
    ``{'purchased', 'produced', 'inkind'}``.  The wave-level legacy
    ``j ↔ i`` swap is handled here so downstream code sees canonical names.

    Reshape rules
    -------------
    Each input row becomes up to 3 long-form rows:

    * ``s = 'purchased'`` -- ``Quantity = quant_purchase``,
      ``u = unit_purchase``, ``Expenditure = value_purchase``
    * ``s = 'produced'``  -- ``Quantity = quant_own``,
      ``u = unit_own``, ``Expenditure = NaN``
    * ``s = 'inkind'``    -- ``Quantity = quant_inkind``,
      ``u = unit_inkind``, ``Expenditure = NaN``

    Rows are kept where EITHER ``Quantity > 0`` OR ``Expenditure > 0``
    (matches the shared
    :func:`lsms_library.transformations.food_acquired_to_canonical` rule).
    A purchased row with a recorded value but no quantity is legitimate
    data and is carried through with NaN ``Quantity``.  The redundant
    ``quant_ttl_consume`` / ``unit_ttl_consume`` columns are discarded.
    '''
    work = df.reset_index()
    # Legacy Tanzania: j=HHID, i=item.  Canonical: i=HHID, j=item.
    work = work.rename(columns={'j': 'i_canon', 'i': 'j_canon'})
    work = work.rename(columns={'i_canon': 'i', 'j_canon': 'j'})

    # Route the canonical item axis (j) through harmonize_food so it carries
    # the shared Preferred Label rather than the raw UPPERCASE survey label
    # (GH #443).  This is the SAME {raw label -> Preferred Label} resolver
    # used by prices_and_units / food_expenditures / food_quantities and by
    # crop_production, so food_acquired.j now joins crop_production.j.  Items
    # with no harmonize_food entry keep their raw label (no fabrication); the
    # row count is unchanged -- only the j *labels* change.
    _food_labels = harmonized_food_labels()
    _jcol = work['j'].astype('object')
    work['j'] = _jcol.map(lambda s: _food_labels.get(s.strip(), s)
                          if isinstance(s, str) else s)

    def _make(source_label, quant_col, unit_col, value_col=None):
        out = pd.DataFrame({
            't': work['t'].values,
            'i': work['i'].values,
            'j': work['j'].values,
            'u': work[unit_col].values,
            's': source_label,
            'Quantity': pd.to_numeric(work[quant_col], errors='coerce').values,
        })
        if value_col is not None:
            out['Expenditure'] = pd.to_numeric(work[value_col], errors='coerce').values
        else:
            out['Expenditure'] = pd.NA
        return out

    purchased = _make('purchased', 'quant_purchase', 'unit_purchase',
                      value_col='value_purchase')
    produced  = _make('produced',  'quant_own',      'unit_own')
    inkind    = _make('inkind',    'quant_inkind',   'unit_inkind')

    from lsms_library.transformations import _finalize_canonical_food_acquired

    out = pd.concat([purchased, produced, inkind], ignore_index=True)
    # Filter (qty>0 | exp>0; expenditure-only rows kept with NaN Quantity)
    # via the shared tail (GH #251).  dedupe=False: Tanzania's per-source
    # _make already yields unique canonical keys, so no groupby is needed.
    out = _finalize_canonical_food_acquired(out, dedupe=False)
    return out


def new_harmonize_units(df, unit_conversion):
    pair = {'quant': ['quant_ttl_consume', 'quant_purchase', 'quant_own', 'quant_inkind'] ,
        'unit': ['unit_ttl_consume', 'unit_purchase', 'unit_own', 'unit_inkind']}
    
    #convert categorical columns to object columns for fillna to work
    df[pair['unit']] = df[pair['unit']].astype('object') 

    df = df.fillna(0).replace(unit_conversion).replace(['none', 'NONE', 'hakuna'], 0)
    pattern = r"[p+]"
    for i in range(4):
        df[pair['quant'][i]] = df[pair['quant'][i]].astype(np.int64) * df[pair['unit'][i]]
        df[pair['quant'][i]] = df[pair['quant'][i]].replace('', 0)
        if df[pair['quant'][i]].dtype != 'O':
            df[pair['unit'][i]] = 'kg'
        else: 
            # NB: dropped a vestigial ``.to_frame()`` here — under pandas 2.x
            # it produced a 2-D mask that ``np.where`` returned as a 2-D
            # ndarray, which then failed to assign back into a 1-D column
            # (``ArrowInvalid: only handle 1-dimensional arrays``).  Series
            # comparison stays 1-D, so the rest of the expression is fine.
            df[pair['unit'][i]] = np.where(df[pair['quant'][i]].str.contains(pattern) == True, 'piece', 'kg')
            df[pair['quant'][i]] = df[pair['quant'][i]].apply(lambda x: x if str(x).count('p') == 0 else str(x).count('p'))

    df['agg_u'] = df[pair['unit']].apply(lambda x: max(x) if min(x) == max(x) else min(x) + '+' + max(x), axis = 1)

    df['unitvalue_purchase'] = df['value_purchase']/df['quant_purchase']
    df = df.replace([np.inf, -np.inf, 0], np.nan)
    return df


def id_walk(df, updated_ids, hh_index='j'):
    '''
    Updates household IDs in panel data across different waves separately.

    Parameters:
        df (DataFrame): Panel data with a MultiIndex, including 't' for wave and 'i' (default) for household ID.
        updated_ids (dict): A dictionary mapping each wave to another dictionary that maps original household IDs to updated IDs.
            Format:
                {wave_1: {original_id: new_id, ...},
                 wave_2: {original_id: new_id, ...}, ...}
        hh_index (str): Index name for the household ID level (default is 'i').

    Example:
        updated_ids = {
            '2013-14': {'0001-001': '101012150028', '0009-001': '101015620053', '0005-001': '101012150022'},
            '2016-17': {'0001-002': '0001-001', '0003-001': '0005-001', '0005-001': '0009-001'}
        }

        In this example, IDs are updated independently for each wave.
        Because the same original household ID across different waves may not represent the same household.
        Specifically, household '0005-001' in wave '2016-17' corresponds to household '0009-001' from wave '2013-14', not '0005-001' from '2013-14'.

    The function handles these wave-specific mappings separately, ensuring accurate household identification over time.
    '''
    #seperate df into different waves:
    dfs = {}
    waves = df.index.get_level_values('t').unique()
    for wave in waves:
        dfs[wave] = df[df.index.get_level_values('t') == wave].copy()
    #update ids for each wave
    for wave, df_wave in dfs.items():
        #update ids
        if wave in updated_ids:
            df_wave = df_wave.rename(index=updated_ids[wave], level=hh_index)
            #update the dataframe with the new ids
            dfs[wave] = df_wave
        else:
            continue
    #combine the updated dataframes
    df = pd.concat(dfs.values(), axis=0)

    # df= df.rename(index=updated_ids,level=['t', hh_index])
    df.attrs['id_converted'] = True
    return df


def harmonize_education_labels(series):
    '''Map raw NPS "highest grade completed" text codes onto the canonical
    ordinal Educational-Attainment vocabulary (GH #171).

    The mapping lives in ``_/categorical_mapping.org`` under
    ``#+name: harmonize_education`` (Tanzania-specific NPS grade ladder:
    PP, ADULT, D1..D8, F1..F6, 'O'+COURSE, DIPLOMA, U1..U5&+, plus the
    2020-21 lowercase / spaced variants).  Any label not in the table folds
    to 'Unknown' (none expected -- the table covers every value label across
    all six NPS waves).  Called from each wave-level individual_education.py
    so the framework's per-wave concatenation already carries canonical
    labels (the country-level _/individual_education.py aggregator is the
    no-per-wave-parquet fallback, so harmonization cannot live only there).
    '''
    from lsms_library.local_tools import all_dfs_from_orgfile
    edu_map = all_dfs_from_orgfile('../../_/categorical_mapping.org')['harmonize_education']
    rdict = (edu_map.assign(**{'Original Label': edu_map['Original Label'].str.strip()})
             .set_index('Original Label')['Preferred Label'].to_dict())
    raw = series.astype(str).str.strip()
    mapped = raw.map(rdict)
    return mapped.fillna('Unknown')


def panel_ids(Waves):
    '''
    Input: DataFrame with a MultiIndex that includes a level named 't' representing the wave and 'i' current househod ID'
            And single 'previous_i' column as the previous household ID.
    Output: Wave-specific panel id mapping dictionaires and a recursive dictionary of tuple of (wave, household identifiers)
    '''
    if isinstance(Waves, dict):
        dfs = []
        for wave_year, wave_info in Waves.items():
            if not wave_info:
                continue  # skip empty entries

            file_path = f"../{wave_year}/Data/{wave_info[0]}"
            if isinstance(wave_info[1], list):
                columns = wave_info[1]
            else:
                columns = [wave_info[1], wave_info[2]]

            df = get_dataframe(file_path)[columns]

            # Process mapping when recent_id is a list (list-based mapping)
            if isinstance(wave_info[1], list): #tanzania
                df = wave_info[2](df, wave_info[1])
            else:
                df[wave_info[1]] = df[wave_info[1]].apply(format_id)
                df[wave_info[2]] = df[wave_info[2]].apply(format_id)
                # If a transformation function is provided (tuple length 4), apply it to the old_id column
                if len(wave_info) == 4:
                    df[wave_info[2]] = df[wave_info[2]].apply(wave_info[3])
                df['t'] = wave_year
                df = df.rename(columns={wave_info[1]: 'i', wave_info[2]: 'previous_i'})
                df = df.set_index(['t', 'i'])[['previous_i']]
            dfs.append(df)
        panel_ids_df = pd.concat(dfs, axis=0)
    else:
        # If Waves is not a dictionary, assume it's a DataFrame
        panel_ids_df = Waves.copy()

    updated_wave = {}
    check_id_split = {}
    sorted_waves = sorted(panel_ids_df.index.get_level_values('t').unique())
    recursive_D = RecursiveDict()
    for wave_year in sorted_waves:
        df = panel_ids_df[panel_ids_df.index.get_level_values('t') == wave_year].copy().reset_index()
        wave_matches = df[['i', 'previous_i']].dropna().set_index('i')['previous_i'].to_dict()
        previous_wave = sorted_waves[sorted_waves.index(wave_year) - 1] if sorted_waves.index(wave_year) > 0 else None
        if wave_year == '2020-21':
            previous_wave = '2014-15'
        if previous_wave:
            previous_wave_matches = updated_wave[previous_wave]
            # update the current wave matches dictionary values to the previous wave matches
            wave_matches = {k: previous_wave_matches.get(v, v)for k, v in wave_matches.items()}
            recursive_D.update({(wave_year, k): (previous_wave, v) for k, v in wave_matches.items()})
        wave_matches, check_id_split = update_id(wave_matches,  check_id_split)
        updated_wave[wave_year] = wave_matches
    return recursive_D, updated_wave


# ---------------------------------------------------------------------
# plot_features (GH #167)
# ---------------------------------------------------------------------

ACRES_PER_HECTARE = 2.471053814671653  # 1 ha = 2.471... acres
HECTARES_PER_ACRE = 1.0 / ACRES_PER_HECTARE  # 0.404686 ha / acre


def _plot_harmonized_codes(tablename, key='Code', value='Preferred Label'):
    """Load a {int code -> Preferred Label} dict from
    Tanzania/_/categorical_mapping.org.  Codes whose Preferred Label is
    blank / '---' map to pd.NA (so the column is left NaN)."""
    from lsms_library.local_tools import get_categorical_mapping

    raw = get_categorical_mapping(tablename=tablename, idxvars=key,
                                  **{value: value})
    out = {}
    for k, v in raw.items():
        try:
            int_k = int(k)
        except (TypeError, ValueError):
            int_k = k
        if pd.isna(v) or str(v).strip() in ('---', ''):
            out[int_k] = pd.NA
        else:
            out[int_k] = str(v).strip()
    return out


def _map_plot_codes(series, code_map):
    """Map a numeric (raw Stata) Series through ``code_map`` ({int: str}).
    Returns a nullable string Series, NaN where the code is unmapped."""
    if series is None:
        return None
    out = pd.to_numeric(series, errors='coerce').astype('Int64').map(code_map)
    return out.astype('string').where(out.notna(), pd.NA)


def plot_features_for_wave(t, sec02, sec3a, colmap):
    """Build canonical ``plot_features`` for one Tanzania NPS wave.

    The two source modules are merged on (hhid, plot):
      AG_SEC_02 — plot area: farmer estimate (acres) + GPS-measured (acres).
      AG_SEC_3A — plot detail: use status, soil type, irrigation, how
                  acquired, and certificate of occupancy.

    Parameters
    ----------
    t : str
        Wave id ('2019-20' or '2020-21'); used as the ``t`` index value.
    sec02, sec3a : pd.DataFrame
        Raw AG_SEC_02 / AG_SEC_3A frames, loaded via
        ``get_dataframe(..., convert_categoricals=False)`` so categorical
        columns carry integer codes.
    colmap : dict with keys
        hhid       — household id column (sdd_hhid / y5_hhid)
        plot       — within-HH plot column (plotnum / plot_id)
        area_est   — farmer-estimated area, acres (ag2a_04)
        area_gps   — GPS-measured area, acres   (ag2a_09)
        use        — plot use status            (ag3a_03)
        soil_type  — soil type                  (ag3a_10)
        irrigated  — irrigated y/n              (ag3a_18)
        erosion    — erosion-protection y/n     (ag3a_15)  [optional]
        acquire    — how acquired               (ag3a_25)
        legal_cert — certificate of occupancy   (ag3a_28a)
        cert_other — other ownership document   (ag3a_28d) [optional]

    Returns
    -------
    pd.DataFrame indexed by ``(t, i, plot_id)`` with columns
        Area (hectares, float), AreaUnit (str, 'acres'),
        SelfReportedArea (hectares, float — farmer estimate before the
        GPS-preference merge), Tenure (str), TenureSystem (str),
        SoilType (str), Irrigated (bool nullable), Owned (bool nullable),
        Certificate (bool nullable), ErosionProtection (bool nullable),
        Fallow (bool nullable).

    The five reported item attributes beyond the original area/tenure/soil
    set (SelfReportedArea, Owned, Certificate, ErosionProtection, Fallow)
    mirror the WB LSMS-ISA Plot_dataset item fields (TZA_NPS*.do plot
    block); each is a value the questionnaire records PER PLOT.  Per-HH
    rollups (farm_size, nb_plots, nb_fallow_plots, soil_fertility_index)
    are transformations over these item columns, NOT stored here.

    GPS coordinates (ag2a_07__Latitude/Longitude) are CONFIDENTIAL /
    redacted in the source and are deliberately NOT emitted.  plot_slope /
    plot_elevation are ABSENT from the NPS instrument (the WB cleaning code
    notes "plot slope (absent)" / "plot elevation (absent)"; they come from
    external geospatial layers, not the survey) and are likewise omitted.
    """
    c = colmap
    tenure_map = _plot_harmonized_codes('harmonize_tenure')
    tenure_system_map = _plot_harmonized_codes('harmonize_tenure_system')
    soil_map = _plot_harmonized_codes('harmonize_soil')

    # --- AG_SEC_02: area ------------------------------------------------
    a = sec02[[c['hhid'], c['plot']]].copy()
    a['_hh'] = sec02[c['hhid']].apply(format_id)
    a['_plot'] = sec02[c['plot']].apply(format_id)
    area_gps = pd.to_numeric(sec02[c['area_gps']], errors='coerce').astype('Float64')
    area_est = pd.to_numeric(sec02[c['area_est']], errors='coerce').astype('Float64')
    # Plausibility clamp: parcels > 2500 acres (~1000 ha) are data-entry
    # errors for smallholder plots (the 2020-21 GPS column has a 28710-acre
    # outlier); drop to NaN so AreaUnit follows rather than poisoning
    # area-weighted aggregates downstream (GH #167).
    area_gps = area_gps.where((area_gps <= 2500) & (area_gps > 0) | area_gps.isna(), pd.NA)
    area_est = area_est.where((area_est <= 2500) & (area_est > 0) | area_est.isna(), pd.NA)
    # Prefer GPS-measured, fall back to farmer estimate.
    area_acres = area_gps.where(area_gps.notna(), area_est)
    a['Area'] = (area_acres * HECTARES_PER_ACRE).values
    area_unit = pd.Series(['acres'] * len(sec02), index=sec02.index, dtype='string')
    a['AreaUnit'] = area_unit.where(area_acres.notna(), pd.NA).values
    # SelfReportedArea: the farmer estimate alone (ha), kept distinct from
    # the GPS-preferred Area (WB Plot_dataset carries area_self_reported as
    # its own item field).  Same plausibility clamp already applied above.
    a['SelfReportedArea'] = (area_est * HECTARES_PER_ACRE).values
    area = a[['_hh', '_plot', 'Area', 'AreaUnit', 'SelfReportedArea']]

    # --- AG_SEC_3A: detail ---------------------------------------------
    d = pd.DataFrame(index=sec3a.index)
    d['_hh'] = sec3a[c['hhid']].apply(format_id)
    d['_plot'] = sec3a[c['plot']].apply(format_id)

    # Tenure from how-acquired, with a use-status override.
    acq = pd.to_numeric(sec3a[c['acquire']], errors='coerce').astype('Int64')
    tenure = acq.map(tenure_map).astype('string')
    use = pd.to_numeric(sec3a[c['use']], errors='coerce').astype('Int64')
    # ag3a_03 code 2 = RENTED OUT, 3 = GIVEN OUT -> Tenure rented_out.
    tenure = tenure.where(~use.isin([2, 3]), 'rented_out')
    d['Tenure'] = tenure.values

    d['TenureSystem'] = _map_plot_codes(sec3a[c['legal_cert']], tenure_system_map).values
    d['SoilType'] = _map_plot_codes(sec3a[c['soil_type']], soil_map).values

    # Irrigated: ag3a_18 1=YES->True, 2=NO->False, else NaN.
    irr = pd.to_numeric(sec3a[c['irrigated']], errors='coerce').astype('Int64')
    irrigated = irr.map({1: True, 2: False}).astype('boolean')
    d['Irrigated'] = irrigated.values

    # --- reported item attributes (WB Plot_dataset parity, GAP 6) -------
    # Owned: WB plot_owned recode of how-acquired ag3a_25 -- codes
    # {1,2,5,9} (purchased / granted-titled / inherited / allocated as
    # owner) -> True, any other acquisition mode -> False.  NaN where
    # acquisition is unrecorded.  (TZA_NPS*.do plot block.)
    owned = acq.map(lambda v: pd.NA if pd.isna(v)
                    else (True if v in (1, 2, 5, 9) else False)).astype('boolean')
    d['Owned'] = owned.values

    # Certificate: WB plot_certificate.  Base from ag3a_28a (1,2 -> Yes;
    # 3 -> No), promoted to True when any other ownership document
    # ag3a_28d is in 1..5, and forced False when the plot is not Owned.
    cert_a = pd.to_numeric(sec3a[c['legal_cert']], errors='coerce').astype('Int64')
    cert = cert_a.map({1: True, 2: True, 3: False}).astype('boolean')
    if c.get('cert_other') is not None and c['cert_other'] in sec3a.columns:
        cert_d = pd.to_numeric(sec3a[c['cert_other']], errors='coerce').astype('Int64')
        cert = cert.where(~cert_d.isin([1, 2, 3, 4, 5]), True)
    cert = cert.where(~(owned == False), False)
    d['Certificate'] = cert.values

    # ErosionProtection: ag3a_15 1=YES->True, 2=NO->False, else NaN.
    if c.get('erosion') is not None and c['erosion'] in sec3a.columns:
        ero = pd.to_numeric(sec3a[c['erosion']], errors='coerce').astype('Int64')
        d['ErosionProtection'] = ero.map({1: True, 2: False}).astype('boolean').values
    else:
        d['ErosionProtection'] = pd.array([pd.NA] * len(sec3a), dtype='boolean')

    # Fallow: WB fallow_plot from use status ag3a_03 == 4 (left fallow);
    # any other recorded use -> False; NaN where use is unrecorded.
    fallow = use.map(lambda v: pd.NA if pd.isna(v)
                     else (True if v == 4 else False)).astype('boolean')
    d['Fallow'] = fallow.values

    # --- merge (1:1 on hhid, plot) -------------------------------------
    df = area.merge(d, on=['_hh', '_plot'], how='outer', indicator=True)
    n_unmatched = int((df['_merge'] != 'both').sum())
    if n_unmatched:
        warnings.warn(
            f"plot_features {t}: {n_unmatched} of {len(df)} (hhid, plot) "
            f"rows did not match across AG_SEC_02 and AG_SEC_3A.")
    df = df.drop(columns='_merge')

    df['t'] = t
    df = df.rename(columns={'_hh': 'i', '_plot': 'plot_id'})
    df = df.set_index(['t', 'i', 'plot_id'])
    df = df[['Area', 'AreaUnit', 'SelfReportedArea', 'Tenure', 'TenureSystem',
             'SoilType', 'Irrigated', 'Owned', 'Certificate',
             'ErosionProtection', 'Fallow']]
    return df


# Crop-production feature (parity-loop GAP 1).  Item-level harvest at the
# natural grain (t, i, plot_id, j) -- one row per crop grown on a plot --
# carrying ONLY reported survey fields.  No harvest_kg sum, no yield, no
# main_crop, no value-share: those are transformations over these rows.
CROP_PRODUCTION_COLUMNS = [
    'Quantity', 'u', 'Quantity_sold', 'Value_sold',
    'harvest_month', 'intercropped', 'perennial',
]
# NB: planting_month is part of the GAP-1 schema but is NOT recorded by the
# Tanzania NPS instrument (the WB cleaning code notes "planting month
# (absent)").  Both buildable waves lack it, so the column is omitted here
# rather than carried all-null (which fails the no_all_null_columns sanity
# check); it would be added back for a wave/country that records it.


def _crop_labels(tablename='harmonize_crop'):
    """{int crop code -> canonical Preferred Label} from
    Tanzania/_/categorical_mapping.org.  Reuses ``_plot_harmonized_codes``
    (codes with a blank Preferred Label collapse to pd.NA)."""
    return _plot_harmonized_codes(tablename)


def _months_to_int(series):
    """Coerce a survey month column to nullable Int64 in 1..12; 0/blank -> NaN."""
    m = pd.to_numeric(series, errors='coerce').astype('Int64')
    return m.where((m >= 1) & (m <= 12), pd.NA)


def crop_production_for_wave(t, seas, fruit, peren, sales, colmaps):
    """Build canonical ``crop_production`` for one Tanzania NPS wave.

    Three harvest modules are stacked at grain (hhid, plot, crop):
      seas  -- AG_SEC_4A  seasonal/annual crops (harvest qty ``ag4a_27``,
               zeroed where ``ag4a_19`` == 2 "did not harvest"; intercrop
               ``ag4a_04``; harvest-end month ``ag4a_24_2``).
      fruit -- AG_SEC_6A  perennial FRUIT trees (``ag6a_09``; intercrop
               ``ag6a_05``; end month ``ag6a_07_4``).  perennial=True.
      peren -- AG_SEC_6B  perennial NON-fruit/cash trees (``ag6b_09`` ...).
               perennial=True.

    Reported harvest quantity is recorded in KILOGRAMS by the questionnaire
    ((KG) on every harvest variable), so ``u`` = 'kg' and the native
    ``Quantity`` is the reported kg -- NO unit conversion is performed here.

    Sales are recorded at the (hhid, crop) grain (no plot): seasonal in
    AG_SEC_5A (``ag5a_02`` qty kg, ``ag5a_03`` value), perennial in 7A/7B.
    We attach ``Quantity_sold`` / ``Value_sold`` to a harvest row ONLY when
    the (hhid, crop) maps to exactly ONE plot in that wave -- so a reported
    household-crop sale is carried without being split or duplicated across
    plots (splitting would fabricate an allocation, which is a
    transformation, not a reported value).  Where a crop spans >1 plot the
    sold columns stay NaN.

    planting_month is ABSENT in NPS (the WB cleaning code notes
    "planting month (absent)") -> NaN.

    Parameters
    ----------
    t : str            wave id ('2019-20' or '2020-21'); the ``t`` value.
    seas, fruit, peren : pd.DataFrame
        raw AG_SEC_4A / 6A / 6B frames (convert_categoricals=False).
    sales : dict with optional keys 'seasonal' (5A), 'fruit' (7A),
        'perennial' (7B), each a raw DataFrame or None.
    colmaps : dict 'seasonal'/'fruit'/'perennial'/'sales_*' -> column-name
        dicts (so 2019-20 'plotnum'/'sdd_hhid' vs 2020-21 'plot_id'/'y5_hhid'
        differences live in the wave script, not here).

    Returns
    -------
    pd.DataFrame indexed by (t, i, plot_id, j) with columns
    CROP_PRODUCTION_COLUMNS.
    """
    crop_map = _crop_labels()

    def _block(df, cm, perennial):
        """One harvest module -> tidy (i, plot_id, code, fields) frame."""
        if df is None or len(df) == 0:
            return None
        out = pd.DataFrame(index=df.index)
        out['i'] = df[cm['hhid']].apply(format_id)
        out['plot_id'] = df[cm['plot']].apply(format_id)
        code = pd.to_numeric(df[cm['crop']], errors='coerce').astype('Int64')
        out['_code'] = code
        out['j'] = code.map(crop_map).astype('string')
        qty = pd.to_numeric(df[cm['qty']], errors='coerce').astype('Float64')
        # Seasonal: harvest is 0 where respondent did not harvest (ag4a_19==2).
        if cm.get('harvested') is not None:
            harvested = pd.to_numeric(df[cm['harvested']], errors='coerce')
            qty = qty.where(harvested != 2, 0.0)
        out['Quantity'] = qty
        out['u'] = pd.Series(['kg'] * len(df), index=df.index, dtype='string')
        out['u'] = out['u'].where(qty.notna(), pd.NA)
        out['harvest_month'] = _months_to_int(df[cm['harvest_month']]).values
        ic = pd.to_numeric(df[cm['intercrop']], errors='coerce').astype('Int64')
        out['intercropped'] = ic.map({1: True, 2: False}).astype('boolean').values
        out['perennial'] = perennial
        return out

    pieces = [
        _block(seas, colmaps['seasonal'], False),
        _block(fruit, colmaps['fruit'], True),
        _block(peren, colmaps['perennial'], True),
    ]
    pieces = [p for p in pieces if p is not None]
    if not pieces:
        raise ValueError(f"crop_production {t}: no harvest source rows")
    df = pd.concat(pieces, ignore_index=True)

    # Drop rows with no resolvable crop label AND no quantity (pure noise).
    df = df[~(df['j'].isna() & df['Quantity'].isna())].copy()

    # --- reported sales, attached at the unambiguous single-plot grain ----
    def _sales_block(raw, cm):
        if raw is None or len(raw) == 0:
            return None
        s = pd.DataFrame(index=raw.index)
        s['i'] = raw[cm['hhid']].apply(format_id)
        s['_code'] = pd.to_numeric(raw[cm['crop']], errors='coerce').astype('Int64')
        s['Quantity_sold'] = pd.to_numeric(raw[cm['qty']], errors='coerce').astype('Float64')
        s['Value_sold'] = pd.to_numeric(raw[cm['value']], errors='coerce').astype('Float64')
        s = s.dropna(subset=['_code'])
        # Multiple sale rows per (i, crop) (e.g. fruit + non-fruit listed under
        # the same code) -> sum reported quantities/values.
        s = (s.groupby(['i', '_code'], dropna=False)[['Quantity_sold', 'Value_sold']]
               .sum(min_count=1).reset_index())
        return s

    sales_pieces = [
        _sales_block(sales.get('seasonal'), colmaps['sales_seasonal']),
        _sales_block(sales.get('fruit'), colmaps['sales_fruit']),
        _sales_block(sales.get('perennial'), colmaps['sales_perennial']),
    ]
    sales_pieces = [s for s in sales_pieces if s is not None]
    if sales_pieces:
        all_sales = pd.concat(sales_pieces, ignore_index=True)
        all_sales = (all_sales.groupby(['i', '_code'], dropna=False)
                     [['Quantity_sold', 'Value_sold']].sum(min_count=1)
                     .reset_index())
        # Only attach where (i, crop) lives on exactly one plot (no fabricated
        # split across plots).
        plot_count = df.groupby(['i', '_code'])['plot_id'].transform('nunique')
        single = df[['i', '_code']].copy()
        single['_single'] = (plot_count == 1).values
        df = df.merge(all_sales, on=['i', '_code'], how='left')
        df.loc[~single['_single'].values, ['Quantity_sold', 'Value_sold']] = pd.NA
    else:
        df['Quantity_sold'] = pd.Series([pd.NA] * len(df), dtype='Float64')
        df['Value_sold'] = pd.Series([pd.NA] * len(df), dtype='Float64')

    # j unresolved (rare 'Other' / unmapped) -> keep raw code as a string so
    # the row is not silently dropped, but flag via the sentinel label.
    df['j'] = df['j'].where(df['j'].notna(),
                            df['_code'].astype('string').radd('crop_'))

    df['t'] = t
    df = df.drop(columns=['_code'])
    df = df.set_index(['t', 'i', 'plot_id', 'j'])
    # Collapse exact-duplicate (t,i,plot,j) rows (same crop listed twice on a
    # plot across the seasonal+perennial stack is not expected, but guard):
    if not df.index.is_unique:
        df = df.groupby(level=['t', 'i', 'plot_id', 'j']).agg({
            'Quantity': 'sum', 'u': 'first',
            'Quantity_sold': 'first', 'Value_sold': 'first',
            'harvest_month': 'max',
            'intercropped': 'max', 'perennial': 'max',
        })
    df = df[CROP_PRODUCTION_COLUMNS]
    return df


# Plot-inputs feature (parity-loop GAP 2).  Item-level inputs applied to a
# plot at the natural grain (t, i, plot_id, input, crop) -- one row per input
# applied to a plot -- carrying ONLY reported survey fields.  No seed_kg sum,
# no nitrogen_kg, no any-use flags, no fertilizer totals: those are
# transformations over these rows.
#   * input  : the input identity, on the shared harmonize_input labels --
#              seed / organic fertilizer / a named inorganic-fertilizer product
#              (Urea / DAP / NPK / CAN / SA / TSP / MRP / Other Fertilizer,
#              from ag3a_48 / ag3a_55 codes) / herbicide / pesticide.
#   * crop   : ONLY for seed rows -- the canonical harmonize_food/harmonize_crop
#              Preferred Label of the crop the seed was sown for (NPS seed is
#              reported per plot-crop in AG_SEC_4A).  pd.NA for plot-level
#              inputs (fertilizer / herbicide / pesticide), which are recorded
#              once per plot with no crop split.
#   * Quantity + u : reported native quantity and its native unit.  Fertilizer
#              quantities are in KG; seed/herbicide/pesticide units are the
#              reported native unit code resolved to a tidy label (kg / litre /
#              millilitre / 20 liter bucket / small/large cup), harmonized at
#              API time by the country u table.
#   * Purchased (bool) + Quantity_purchased : whether any of the input was
#              purchased and (where the survey records it as a SEPARATE
#              quantity) how much.  Organic fertilizer and seed record a
#              purchased quantity (ag3a_44 / ag4a_10c_1); inorganic fert /
#              herbicide / pesticide record only a purchased VALUE, so Purchased
#              there is value>0 and Quantity_purchased is NaN (not fabricated).
#   * Improved (bool) : seed rows only -- ag4a_08 improved/recycled-improved.
PLOT_INPUTS_COLUMNS = ['Quantity', 'u', 'Purchased', 'Quantity_purchased',
                       'Improved']

# Native seed-unit code (ag4a_10_2 / ag4a_10c_2) -> tidy label.  kg/litre/etc.
# are harmonized to the canonical u Preferred Label at API time by the country
# u table; the bucket/cup rows were added to that table by this feature.
_SEED_UNIT_LABELS = {
    1: 'kg', 2: '20 liter bucket', 3: 'small cup', 4: 'large cup',
    5: pd.NA,  # OTHER (SPECIFY) -- unit unknown
}
# Native herbicide/pesticide-unit code (ag3a_62_2 / ag3a_65b_2) -> tidy label.
_LIQUID_UNIT_LABELS = {1: 'kg', 2: 'litre', 3: 'millilitre', 4: pd.NA}


def _yn_true(series):
    """Coerce a Stata 1=YES/2=NO column to a nullable boolean (1->True,
    2->False, else NaN)."""
    v = pd.to_numeric(series, errors='coerce').astype('Int64')
    return v.map({1: True, 2: False}).astype('boolean')


def plot_inputs_for_wave(t, sec3a, sec4a, colmap):
    """Build canonical ``plot_inputs`` for one Tanzania NPS wave.

    Item-level inputs at grain (t, i, plot_id, input, crop) from two modules:
      sec3a -- AG_SEC_3A plot detail: organic fertilizer (ag3a_41/42/43/44),
               inorganic fertilizer type 1 (ag3a_47/48/49/51) and type 2
               (ag3a_54/55/56/58), herbicide (ag3a_60/62_1/62_2/63), pesticide
               (ag3a_65a/65b_1/65b_2/65c).  One row per (plot, input).
      sec4a -- AG_SEC_4A seasonal-crop seed: ag4a_08 improved flag, ag4a_10_1/2
               total seed qty+unit, ag4a_10c_1/2 purchased seed qty+unit,
               ag4a_12 amount paid.  One SEED row per (plot, crop).

    STORES REPORTED VALUES ONLY.  seed_kg / nitrogen_kg / inorganic_fertilizer
    any-use flags are transformations over these rows, NOT columns here.

    Parameters
    ----------
    t : str            wave id ('2019-20' or '2020-21'); the ``t`` value.
    sec3a, sec4a : pd.DataFrame
        raw AG_SEC_3A / AG_SEC_4A frames (convert_categoricals=False).
    colmap : dict with keys hhid, plot, crop (the id columns, which differ
        across waves: sdd_hhid/plotnum/cropid vs y5_hhid/plot_id/cropid).

    Returns
    -------
    pd.DataFrame indexed by (t, i, plot_id, input, crop) with columns
    PLOT_INPUTS_COLUMNS.
    """
    c = colmap
    fert_type_map = _plot_harmonized_codes('harmonize_input')
    crop_map = _crop_labels()

    def _ids(df):
        return (df[c['hhid']].apply(format_id), df[c['plot']].apply(format_id))

    rows = []

    # --- AG_SEC_4A: SEED (one row per plot-crop) ------------------------
    if sec4a is not None and len(sec4a):
        hh, plot = _ids(sec4a)
        code = pd.to_numeric(sec4a[c['crop']], errors='coerce').astype('Int64')
        qty = pd.to_numeric(sec4a['ag4a_10_1'], errors='coerce').astype('Float64')
        unit = pd.to_numeric(sec4a['ag4a_10_2'], errors='coerce').astype('Int64')
        pqty = pd.to_numeric(sec4a['ag4a_10c_1'], errors='coerce').astype('Float64')
        paid = pd.to_numeric(sec4a['ag4a_12'], errors='coerce').astype('Float64')
        seedtype = pd.to_numeric(sec4a['ag4a_08'], errors='coerce').astype('Int64')
        seed = pd.DataFrame({
            'i': hh.values, 'plot_id': plot.values,
            'input': 'Seed',
            'crop': code.map(crop_map).astype('string').values,
            'Quantity': qty.values,
            'u': unit.map(_SEED_UNIT_LABELS).astype('string').values,
            # Purchased if a purchased quantity OR an amount paid is recorded.
            'Purchased': ((pqty.fillna(0) > 0) | (paid.fillna(0) > 0)).values,
            'Quantity_purchased': pqty.values,
            'Improved': seedtype.map({1: True, 2: False, 3: True})
                        .astype('boolean').values,
        })
        # A seed row is real only if a quantity OR a crop label is present.
        seed = seed[seed['Quantity'].notna() | seed['crop'].notna()]
        # crop unresolved -> keep raw code as crop_<code> (do not drop).
        seed['crop'] = seed['crop'].where(
            seed['crop'].notna(),
            code.map(lambda x: f'crop_{int(x)}' if pd.notna(x) else pd.NA)
                .astype('string').reindex(seed.index).values)
        rows.append(seed)

    # --- AG_SEC_3A: plot-level inputs (crop = NA) -----------------------
    hh3, plot3 = _ids(sec3a)

    def _liquid_unit(col):
        return (pd.to_numeric(sec3a[col], errors='coerce').astype('Int64')
                .map(_LIQUID_UNIT_LABELS).astype('string'))

    # organic fertilizer
    used = _yn_true(sec3a['ag3a_41'])
    org = pd.DataFrame({
        'i': hh3.values, 'plot_id': plot3.values, 'input': 'Organic Fertilizer',
        'crop': pd.array([pd.NA] * len(sec3a), dtype='string'),
        'Quantity': pd.to_numeric(sec3a['ag3a_42'], errors='coerce').astype('Float64').values,
        'u': 'kg',
        'Purchased': _yn_true(sec3a['ag3a_43']).values,
        'Quantity_purchased': pd.to_numeric(sec3a['ag3a_44'], errors='coerce').astype('Float64').values,
        'Improved': pd.array([pd.NA] * len(sec3a), dtype='boolean'),
    })
    org = org[used.fillna(False).values]

    # inorganic fertilizer, first and second type
    def _inorg(used_col, type_col, qty_col, val_col):
        u = _yn_true(sec3a[used_col])
        typ = pd.to_numeric(sec3a[type_col], errors='coerce').astype('Int64')
        label = typ.map(fert_type_map).astype('string')
        val = pd.to_numeric(sec3a[val_col], errors='coerce').astype('Float64')
        d = pd.DataFrame({
            'i': hh3.values, 'plot_id': plot3.values,
            'input': label.where(label.notna(), 'Other Fertilizer').values,
            'crop': pd.array([pd.NA] * len(sec3a), dtype='string'),
            'Quantity': pd.to_numeric(sec3a[qty_col], errors='coerce').astype('Float64').values,
            'u': 'kg',
            # Inorganic fert records a purchased VALUE (TSH), not a separate
            # purchased qty -> Purchased = value>0; Quantity_purchased NaN.
            'Purchased': (val.fillna(0) > 0).astype('boolean').values,
            'Quantity_purchased': pd.array([pd.NA] * len(sec3a), dtype='Float64'),
            'Improved': pd.array([pd.NA] * len(sec3a), dtype='boolean'),
        })
        return d[u.fillna(False).values]

    inorg1 = _inorg('ag3a_47', 'ag3a_48', 'ag3a_49', 'ag3a_51')
    inorg2 = _inorg('ag3a_54', 'ag3a_55', 'ag3a_56', 'ag3a_58')

    # herbicide
    uh = _yn_true(sec3a['ag3a_60'])
    valh = pd.to_numeric(sec3a['ag3a_63'], errors='coerce').astype('Float64')
    herb = pd.DataFrame({
        'i': hh3.values, 'plot_id': plot3.values, 'input': 'Herbicide',
        'crop': pd.array([pd.NA] * len(sec3a), dtype='string'),
        'Quantity': pd.to_numeric(sec3a['ag3a_62_1'], errors='coerce').astype('Float64').values,
        'u': _liquid_unit('ag3a_62_2').values,
        'Purchased': (valh.fillna(0) > 0).astype('boolean').values,
        'Quantity_purchased': pd.array([pd.NA] * len(sec3a), dtype='Float64'),
        'Improved': pd.array([pd.NA] * len(sec3a), dtype='boolean'),
    })
    herb = herb[uh.fillna(False).values]

    # pesticide
    up = _yn_true(sec3a['ag3a_65a'])
    valp = pd.to_numeric(sec3a['ag3a_65c'], errors='coerce').astype('Float64')
    pest = pd.DataFrame({
        'i': hh3.values, 'plot_id': plot3.values, 'input': 'Pesticide',
        'crop': pd.array([pd.NA] * len(sec3a), dtype='string'),
        'Quantity': pd.to_numeric(sec3a['ag3a_65b_1'], errors='coerce').astype('Float64').values,
        'u': _liquid_unit('ag3a_65b_2').values,
        'Purchased': (valp.fillna(0) > 0).astype('boolean').values,
        'Quantity_purchased': pd.array([pd.NA] * len(sec3a), dtype='Float64'),
        'Improved': pd.array([pd.NA] * len(sec3a), dtype='boolean'),
    })
    pest = pest[up.fillna(False).values]

    rows += [org, inorg1, inorg2, herb, pest]
    df = pd.concat([r for r in rows if r is not None and len(r)], ignore_index=True)

    # u is meaningless where there is no quantity.
    df['u'] = df['u'].where(df['Quantity'].notna(), pd.NA)

    df['t'] = t
    df = df.set_index(['t', 'i', 'plot_id', 'input', 'crop'])
    # Collapse the rare exact-duplicate (t,i,plot,input,crop) -- a plot listing
    # the same inorganic-fertilizer product as both type 1 and type 2, or two
    # raw crop codes that share a canonical Seed crop label (e.g. Beans +
    # Cowpeas -> Pulses): sum the reported quantities, OR the purchased flag,
    # keep the first unit.  ``min_count=1`` so an all-NA group stays NA (a plain
    # ``sum`` would zero the all-NA Quantity_purchased of fertilizer rows, which
    # record only a purchased VALUE, not a purchased quantity).  ``dropna=False``
    # keeps the NA-crop (plot-level input) rows.
    if not df.index.is_unique:
        def _sum1(s):
            return s.sum(min_count=1)
        df = df.groupby(level=['t', 'i', 'plot_id', 'input', 'crop'],
                        dropna=False).agg({
            'Quantity': _sum1, 'u': 'first',
            'Purchased': 'max', 'Quantity_purchased': _sum1,
            'Improved': 'max',
        })
    df = df[PLOT_INPUTS_COLUMNS]
    return df


# Livestock feature (parity-loop GAP 4).  Item-level herd at the natural grain
# (t, i, animal) -- one row per species/animal-type the household OWNS --
# carrying ONLY reported survey fields.  No TLU, no herd-value total, no
# engaged-in-livestock binary: those are transformations over these rows (the
# WB binary = our ``groupby(['t','i']).size() > 0``).
#
# 'livestock' is in the framework ``_no_v_join`` set, so the grain is (t, i,
# animal) with NO ``v`` cluster level joined.
LIVESTOCK_COLUMNS = ['HeadCount', 'HeadAcquired', 'HeadSold']
# NB: a herd VALUE column is part of the GAP-4 schema "where the source reports
# it", but the Tanzania NPS livestock module (LF_SEC_02) records no current-
# herd valuation -- only purchase value (lf02_08), sale value (lf02_26) and
# loss values, each a transaction value, not a stock value.  So Value is
# omitted here rather than carried all-null (which fails the
# no_all_null_columns sanity check) -- the same discipline crop_production uses
# for the absent planting_month.  A purchase/sale value is recoverable as a
# transformation over HeadAcquired / HeadSold should a valuation be wanted.


# Plausibility ceiling for a 12-month head-count FLOW (bought / sold).  The
# largest legitimate flow observed is ~2500 head (a poultry operation); two
# rows carry an obvious data-entry error where the TSH value was typed into the
# count field (2019-20: HH 0525-001-001 "bought 324000" == its 324000-TSH
# purchase value; 2020-21: HH 3722-001-01 "sold 100000" of its 10 owned
# chickens for 10 TSH).  Clamp flows above 50000 to NaN -- well above any
# plausible smallholder flow, so only the corrupt entries are dropped.  This is
# the same plausibility-clamp discipline plot_features uses for >2500-acre
# parcels (GH #167).
_LIVESTOCK_FLOW_CEILING = 50000


def _livestock_count(series, ceiling=None):
    """Coerce a reported head-count column to nullable Float64 (>=0; negatives
    -> NaN).  When ``ceiling`` is given, values above it (data-entry errors
    where a TSH value landed in the count field) are also set to NaN."""
    v = pd.to_numeric(series, errors='coerce').astype('Float64')
    v = v.where(v >= 0, pd.NA)
    if ceiling is not None:
        v = v.where(v <= ceiling, pd.NA)
    return v


def livestock_for_wave(t, lf, colmap):
    """Build canonical ``livestock`` for one Tanzania NPS wave.

    The livestock roster (LF_SEC_02 in 2019-20 / 2020-21) is at the
    (household, lvstckid) grain -- one row per animal TYPE the household was
    asked about.  We keep ONLY the rows the household actually OWNS
    (``lf02_01`` == 1), so the emitted grain is one row per (household, owned
    species).  Each raw ``lvstckid`` is resolved to a canonical species
    Preferred Label via this wave's column of the ``harmonize_species``
    categorical table; pets (Dogs / Cats / Other -> blank label -> NaN) are
    dropped, mirroring the WB ``drop if inlist(animal, 15, 16)``.

    Reported item-level fields carried (NO aggregation):
      HeadCount    -- currently owned = lf02_04_1 (indigenous) + lf02_04_2
                      (improved/exotic).  Reported current stock.
      HeadAcquired -- bought alive in the past 12 months (lf02_07).  The
                      directly-reported acquisition flow that parallels
                      HeadSold; births (lf02_05) and gifts received (lf02_10)
                      are separate reported flows, NOT summed in here.
      HeadSold     -- sold alive in the past 12 months (lf02_25).

    Parameters
    ----------
    t : str          wave id ('2019-20' or '2020-21'); the ``t`` value.
    lf : pd.DataFrame
        raw LF_SEC_02 frame (convert_categoricals=False, so lvstckid carries
        the integer code).
    colmap : dict with keys
        hhid    -- household id column (sdd_hhid / y5_hhid)
        animal  -- animal-type code column (lvstckid)
        own     -- own-y/n flag           (lf02_01; 1=yes, 2=no)
        owned   -- [indigenous, improved] current-stock columns
                   (['lf02_04_1', 'lf02_04_2'])
        bought  -- head bought alive      (lf02_07)
        sold    -- head sold alive        (lf02_25)
        species_col -- this wave's column name in harmonize_species
                       ('2019-20' / '2020-21')

    Returns
    -------
    pd.DataFrame indexed by (t, i, animal) with columns LIVESTOCK_COLUMNS.
    Emits raw hhid as ``i``; the country-level concatenator applies id_walk.
    """
    c = colmap
    species_map = _plot_harmonized_codes('harmonize_species',
                                         value='Preferred Label')
    # _plot_harmonized_codes keys on the canonical 'Code' column, but the raw
    # lvstckid uses this wave's own code column; for Tanzania the two coincide
    # for every species we keep (1..14) and the divergent pet/other tail maps
    # to NA either way -- but resolve through the wave column to be exact.
    from lsms_library.local_tools import get_categorical_mapping
    raw = get_categorical_mapping(tablename='harmonize_species',
                                  idxvars=c['species_col'],
                                  **{'Preferred Label': 'Preferred Label'})
    species_map = {}
    for k, v in raw.items():
        try:
            k = int(k)
        except (TypeError, ValueError):
            pass
        species_map[k] = (pd.NA if (pd.isna(v) or str(v).strip() in ('', '---'))
                          else str(v).strip())

    out = pd.DataFrame(index=lf.index)
    out['i'] = lf[c['hhid']].apply(format_id)
    out['animal'] = _map_plot_codes(lf[c['animal']], species_map)

    owned = lf[c['owned']].apply(pd.to_numeric, errors='coerce')
    head = owned.sum(axis=1, min_count=1).astype('Float64')
    out['HeadCount'] = head.where((head >= 0) & (head <= _LIVESTOCK_FLOW_CEILING),
                                  pd.NA)
    out['HeadAcquired'] = _livestock_count(lf[c['bought']],
                                           ceiling=_LIVESTOCK_FLOW_CEILING)
    out['HeadSold'] = _livestock_count(lf[c['sold']],
                                       ceiling=_LIVESTOCK_FLOW_CEILING)

    own = pd.to_numeric(lf[c['own']], errors='coerce')

    # Keep only OWNED rows (own-flag == yes) with a resolvable species label.
    out = out[(own == 1) & out['animal'].notna()].copy()

    out['t'] = t
    out = out.set_index(['t', 'i', 'animal'])
    # Guard against a duplicated (t,i,animal): sum reported head counts.
    if not out.index.is_unique:
        out = out.groupby(level=['t', 'i', 'animal']).agg({
            'HeadCount': lambda s: s.sum(min_count=1),
            'HeadAcquired': lambda s: s.sum(min_count=1),
            'HeadSold': lambda s: s.sum(min_count=1),
        })
    out = out[LIVESTOCK_COLUMNS]
    return out


# ---------------------------------------------------------------------------
# Labor features (parity-loop GAP 3).  TWO distinct natural grains:
#
#   plot_labor        (t, i, plot_id, source)  -- plot-level person-days of
#                     labor by source {family, hired}, from AG_SEC_3A, joins
#                     crop_production / plot_inputs on (t, i, plot_id).
#   people_last7days  (t, i, pid)              -- per-INDIVIDUAL 7-day activity
#                     dummies / hours / wage-work industry, from HH_SEC_E1.
#
# Both store REPORTED person-level/plot-level fields ONLY.  The WB aggregate
# constructs (total_labor_days / total_family_labor_days /
# total_hired_labor_days = sums over plot_labor by source; hired_labor_value =
# median-wage valuation over the hired rows) are TRANSFORMATIONS over these
# rows, NEVER stored here.
# ---------------------------------------------------------------------------

# plot_labor at the natural grain (t, i, plot_id, source) -- one row per
# (plot, source) -- carrying REPORTED person-days and (hired only) reported
# cash wage.  source in {family, hired}; the GAP-3 schema also allows
# {other/exchange} cross-country, but the NPS-SDD / Y5 AG_SEC_3A instrument
# records no free / exchange labor block, so no 'other' rows arise for Tanzania
# (we do not fabricate them).
PLOT_LABOR_COLUMNS = ['PersonDays', 'Wage']

# Plausibility ceiling for a per-(plot, gender, task) person-day cell.  A
# single farm task in one season cannot legitimately absorb hundreds of
# person-days from one gender group; values this large are data-entry errors
# where a TSH wage amount was typed into a day field (2020-21 ag3a_74_1c reaches
# 450000, ~0.7% of hired-labor plots carry such a corrupt cell).  Clamp cells
# above 730 (two person-years) to NaN -- well above any plausible single-task
# smallholder figure, so only the corrupt entries drop.  Same plausibility-clamp
# discipline livestock / plot_features already use (GH #167).
_PLOT_LABOR_DAY_CEILING = 730


def _clamp_days(series):
    """Coerce a reported person-day column to nullable Float64 (>=0; negatives
    and values above the plausibility ceiling -> NaN)."""
    v = pd.to_numeric(series, errors='coerce').astype('Float64')
    return v.where((v >= 0) & (v <= _PLOT_LABOR_DAY_CEILING), pd.NA)


def plot_labor_for_wave(t, sec3a, colmap):
    """Build canonical ``plot_labor`` for one Tanzania NPS wave from AG_SEC_3A.

    Item-level plot labor at grain (t, i, plot_id, source), source in
    {family, hired}:

      HIRED -- gated by ag3a_73 (did you hire labor? 1=yes/2=no).  PersonDays =
        Sum over the three farm tasks (Land Prep / Weeding / Harvest, blocks
        1/2/3) of the per-gender day cells ag3a_74_{1,2,3}{a,b,c} (woman / man /
        child).  Wage = Sum over tasks of the cash paid ag3a_74_{1,2,3}d [TSH].
        When ag3a_73==2 (no hired labor) PersonDays and Wage are 0.

      FAMILY -- the household members' own reported days on the plot.  In
        2020-21 (Y5) the questionnaire records per-member DAYS for each of the
        three tasks (ag3a_72c_* prep / ag3a_72g_* weeding / ag3a_72k_*
        harvest); PersonDays = the rowtotal over all member slots and tasks.
        In 2019-20 (NPS-SDD) the family block records only the WORKER ROSTER IDs
        per task (ag3a_72b/f/j_*) and NO day columns, so family person-days are
        NOT reported that wave -- we emit NO family rows for 2019-20 rather than
        fabricate a count (the WB NPS5.do rowtotals the ID columns as "days",
        which we deliberately do NOT reproduce).  Wage is NaN for family rows.

    Parameters
    ----------
    t : str            wave id ('2019-20' or '2020-21').
    sec3a : pd.DataFrame   raw AG_SEC_3A (convert_categoricals=False).
    colmap : dict with keys hhid, plot (the id columns: sdd_hhid/plotnum for
        2019-20, y5_hhid/plot_id for 2020-21).

    Returns
    -------
    pd.DataFrame indexed by (t, i, plot_id, source) with columns
    PLOT_LABOR_COLUMNS (PersonDays, Wage).
    """
    c = colmap
    hh = sec3a[c['hhid']].apply(format_id)
    plot = sec3a[c['plot']].apply(format_id)

    rows = []

    # --- HIRED labor (ag3a_73 / ag3a_74_*) ------------------------------
    hire_yn = pd.to_numeric(sec3a['ag3a_73'], errors='coerce')
    day_cols = [f'ag3a_74_{task}{g}'
                for task in (1, 2, 3) for g in ('a', 'b', 'c')]
    wage_cols = [f'ag3a_74_{task}d' for task in (1, 2, 3)]
    hired_days = pd.concat([_clamp_days(sec3a[col]) for col in day_cols],
                           axis=1).sum(axis=1, min_count=1)
    hired_wage = pd.concat(
        [pd.to_numeric(sec3a[col], errors='coerce').astype('Float64')
         for col in wage_cols],
        axis=1).sum(axis=1, min_count=1)
    # ag3a_73==2 -> the household hired no labor: 0 days, 0 wage.
    hired_days = hired_days.where(hire_yn != 2, 0)
    hired_wage = hired_wage.where(hire_yn != 2, 0)
    hired = pd.DataFrame({
        'i': hh.values, 'plot_id': plot.values, 'source': 'hired',
        'PersonDays': hired_days.values, 'Wage': hired_wage.values,
    })
    # Keep a hired row only where the hire question was answered.
    hired = hired[hire_yn.notna().values]
    rows.append(hired)

    # --- FAMILY labor -- DAYS only where the wave records them -----------
    # 2020-21: ag3a_72c_* (prep) + ag3a_72g_* (weeding) + ag3a_72k_* (harvest).
    fam_day_prefixes = ['ag3a_72c_', 'ag3a_72g_', 'ag3a_72k_']
    fam_day_cols = [col for col in sec3a.columns
                    if any(col.startswith(p) and col[len(p):].isdigit()
                           for p in fam_day_prefixes)]
    if fam_day_cols:
        fam_days = pd.concat([_clamp_days(sec3a[col]) for col in fam_day_cols],
                             axis=1).sum(axis=1, min_count=1)
        family = pd.DataFrame({
            'i': hh.values, 'plot_id': plot.values, 'source': 'family',
            'PersonDays': fam_days.values,
            'Wage': pd.array([pd.NA] * len(sec3a), dtype='Float64'),
        })
        # Keep a family row only where a day total is reported (>=0).
        family = family[fam_days.notna().values]
        rows.append(family)
    # else (2019-20): family block is worker-IDs only, no days -> no family
    # rows for this wave.

    df = pd.concat([r for r in rows if r is not None and len(r)],
                   ignore_index=True)
    df['t'] = t
    df = df.set_index(['t', 'i', 'plot_id', 'source'])
    # Collapse the rare duplicate (t,i,plot_id,source): sum reported days,
    # sum reported wage (min_count=1 keeps an all-NA group NA).
    if not df.index.is_unique:
        def _sum1(s):
            return s.sum(min_count=1)
        df = df.groupby(level=['t', 'i', 'plot_id', 'source']).agg({
            'PersonDays': _sum1, 'Wage': _sum1,
        })
    df = df[PLOT_LABOR_COLUMNS]
    return df


# people_last7days at the natural grain (t, i, pid) -- one row per INDIVIDUAL,
# carrying the REPORTED 7-day activity dummies / hours / wage-work industry,
# mirroring Uganda's (the one country that already has this feature) construct.
# Source HH_SEC_E1, following the NPS5.do labor block (lines 1026-1058) but
# using the LABEL-CORRECT dummy<->hours pairing (the .do swaps farm_hrs/wage_hrs
# relative to its own dummies; we pair each hours item with the activity it
# actually times, per the questionnaire labels).
PEOPLE_LAST7DAYS_COLUMNS = [
    'farm_work', 'SOB_work', 'wage_work',
    'farm_hrs', 'SB_hrs', 'wage_hrs',
    'industry', 'working_age',
]


def _industry_from_isic(sec, isic_col, in_wage_emp):
    """Classify the ISIC-division sector code (hh_e31b_2) into the canonical
    coarse industry label, following the NPS .do ranges.  Only meaningful for
    individuals in wage employment (``in_wage_emp`` True); others -> NA."""
    s = pd.to_numeric(sec[isic_col], errors='coerce')
    ind = pd.Series(pd.NA, index=sec.index, dtype='object')
    ind = ind.mask((s == 1) | (s == 2), 'Agriculture')
    ind = ind.mask(s == 3, 'Fishing')
    ind = ind.mask((s >= 5) & (s <= 9), 'Mining')
    ind = ind.mask((s >= 10) & (s <= 37), 'Manufacturing')
    ind = ind.mask((s >= 41) & (s <= 43), 'Construction')
    ind = ind.mask((s >= 45) & (s <= 4000), 'Services')
    ind = ind.where(in_wage_emp.fillna(False), pd.NA)
    return ind.astype('string')


def people_last7days_for_wave(t, sec, colmap):
    """Build canonical ``people_last7days`` for one Tanzania NPS wave.

    Per-individual 7-day activity at grain (t, i, pid) from HH_SEC_E1:
      farm_work  = hh_e07 (worked in HH agriculture, last 7 days)  1->1 / 2->0
      SOB_work   = hh_e05 (ran an own non-farm business, last 7 days)
      wage_work  = hh_e03 (worked as a wage employee, last 7 days)
      farm_hrs   = hh_e08 (hours in HH ag),  0 if farm_work==0
      SB_hrs     = hh_e06 (hours in own business), 0 if SOB_work==0
      wage_hrs   = hh_e04 (hours in wage work), 0 if wage_work==0
      industry   = coarse ISIC class of the wage job (hh_e31b_2), wage workers
                   only (gated by hh_e28, "is the answer to wage-work Q yes?")
      working_age= hh_e01_1==1 (member is 5 years or above)
    All dummies / hours are set to 0 for members below working age, mirroring
    the WB .do (every activity is zeroed when working_age==0).

    Parameters
    ----------
    t : str            wave id ('2019-20' or '2020-21').
    sec : pd.DataFrame    raw HH_SEC_E1 (convert_categoricals=False).
    colmap : dict with keys hhid, pid (sdd_hhid/sdd_indid for 2019-20,
        y5_hhid/indidy5 for 2020-21).

    Returns
    -------
    pd.DataFrame indexed by (t, i, pid) with columns PEOPLE_LAST7DAYS_COLUMNS.
    """
    c = colmap
    working_age = (pd.to_numeric(sec['hh_e01_1'], errors='coerce') == 1)

    def _dummy(col):
        v = pd.to_numeric(sec[col], errors='coerce').astype('Int64')
        return v.map({1: True, 2: False}).astype('boolean')

    def _hrs(col, work_dummy):
        v = pd.to_numeric(sec[col], errors='coerce').astype('Float64')
        # hours are meaningful only for those who did the activity; set 0 where
        # the activity dummy is False (matching the .do gating).
        return v.where(work_dummy.fillna(False), 0)

    farm_work = _dummy('hh_e07')
    sob_work = _dummy('hh_e05')
    wage_work = _dummy('hh_e03')
    in_wage_emp = (pd.to_numeric(sec['hh_e28'], errors='coerce') == 1)

    out = pd.DataFrame({
        'i': sec[c['hhid']].apply(format_id).values,
        'pid': sec[c['pid']].apply(format_id).values,
        'farm_work': farm_work.values,
        'SOB_work': sob_work.values,
        'wage_work': wage_work.values,
        'farm_hrs': _hrs('hh_e08', farm_work).values,
        'SB_hrs': _hrs('hh_e06', sob_work).values,
        'wage_hrs': _hrs('hh_e04', wage_work).values,
        'industry': _industry_from_isic(sec, 'hh_e31b_2', in_wage_emp).values,
        'working_age': working_age.values,
    })

    # Below working age: zero every activity dummy / hours (the .do convention),
    # but keep the row -- working_age itself stays the truthful flag.
    not_wa = ~out['working_age']
    for col in ('farm_work', 'SOB_work', 'wage_work'):
        out.loc[not_wa, col] = False
    for col in ('farm_hrs', 'SB_hrs', 'wage_hrs'):
        out.loc[not_wa, col] = 0
    out.loc[not_wa, 'industry'] = pd.NA

    out['working_age'] = out['working_age'].astype('boolean')
    out['t'] = t
    out = out.set_index(['t', 'i', 'pid'])
    # GH #637 key-soundness review -- key SOUND, collapse is dead code.  Both
    # callers' HH_SEC_E1 is (hhid, pid)-unique -- 2019-20 (sdd_hhid,
    # sdd_indid): 5,587 rows / 5,587 groups / 0 duplicates; 2020-21 (y5_hhid,
    # indidy5): 23,592 / 23,592 / 0 -- and format_id is injective over each
    # wave's households (1,184 -> 1,184; 4,709 -> 4,709), so .first() is never
    # reached on a cold build of either wave.
    if not out.index.is_unique:
        out = out.groupby(level=['t', 'i', 'pid']).first()
    out = out[PEOPLE_LAST7DAYS_COLUMNS]
    return out


# ===========================================================================
# community_prices (parity-loop GAP C -- OURS-ONLY; maintainer priority)
# ===========================================================================
#
# Item-level community/market prices at the natural grain (t, v, j, u) -- one
# row per (community cluster v, harmonized food item j, native unit u) -- from
# the COMMUNITY price questionnaire (CM_SEC_F / CM_SEC_F_ID), carrying ONLY the
# REPORTED surveyed price.  This is the community-price arm of label
# unification: j is the canonical Preferred Label from harmonize_community_price
# (which REUSES harmonize_food / harmonize_crop Preferred Labels for every
# priced food), so a community_prices row for "Maize (grain)" lines up with the
# food_acquired (consumption) and crop_production (harvest) "Maize (grain)"
# rows.  NO median/mean across clusters, NO community->household imputation:
# those are transformations over these rows, never stored here.
#
# The price observation in CM_SEC_F_ID is "price cm_f063 for a quantity cm_f062
# of native unit cm_f061" at the VILLAGE market (the cluster's local market);
# the district-capital triple (cm_f064/65/66) is a DIFFERENT geographic level
# (the district town), so it is NOT folded into the cluster grain -- the
# village price is the cluster price the feature is about.  Price is normalized
# to the REPORTED unit price = cm_f063 / cm_f062 (currency per ONE native unit
# u): this is the reported price divided through by its own reported quantity
# basis -- NOT a cross-row aggregation.  Rows with Price <= 0 or Quantity <= 0
# are the questionnaire's "item not available / not priced" sentinels (price
# and quantity both 0) and are dropped as non-observations.
#
# v == the community questionnaire's own cluster id (interview__key).  IMPORTANT
# DATA LIMITATION (issue #113; full diagnosis + match rates in CONTENTS.org,
# "Why the community-price <-> household link cannot be made cleanly"): the NPS
# community instrument and the household instrument use INCOMPATIBLE cluster
# coding -- v == interview__key is a per-INTERVIEW token (NOT a cluster id) with
# ZERO overlap with sample().v (clusterid / y5_cluster).  The only shared
# geography is the national admin tuple (community id_01..id_05 vs household
# t0_region/district/ward); but it is many-to-many with the survey cluster even
# within the household file (tracking design), so the best-case match is 38/99
# communities -> a unique cluster in 2019-20, and for 2020-21 the district
# numbering is irreconcilable (region+district+ward match == 0/488).  So
# community_prices.v is the community cluster's NATIVE id and does NOT intersect
# sample().v; a community->household price join is at best a REGION-level
# fallback TRANSFORMATION (the documented #113 use), deliberately NOT baked into
# this item-level feature.  Because the grain carries no
# household i, the framework's _join_v_from_sample never fires (it requires an
# i level), so v is left exactly as emitted -- no _no_v_join entry is needed.

# Native community-price unit CODE (cm_f061, raw Stata integer; the decoded
# label is KILOGRAMS/GRAMS/LITRES/MILLILITRES/PIECES, upper- or lower-cased by
# wave) -> canonical Preferred Label, matching the country `u` categorical table
# (Kg / Gram / Litre / Millilitre / Piece).  The 1..5 code space is identical
# across both buildable waves.  Resolved in-script so the emitted u is already
# canonical; the `u` table additionally carries the decoded-text variants so
# the label axis is documented and the framework auto-map is an idempotent
# no-op.
_COMMUNITY_PRICE_UNIT_LABELS = {
    1: 'Kg', 2: 'Gram', 3: 'Litre', 4: 'Millilitre', 5: 'Piece',
}

COMMUNITY_PRICES_COLUMNS = ['Price']


def _community_price_codes(tablename='harmonize_community_price'):
    """{int community-price item code -> canonical Preferred Label} from
    Tanzania/_/categorical_mapping.org (Code -> Preferred Label).  Reuses the
    harmonize_food Preferred Labels for every priced food so community_prices.j
    joins food_acquired.j / crop_production.j; codes with a blank Preferred
    Label collapse to pd.NA (none today -- all 52 codes carry a label)."""
    return _plot_harmonized_codes(tablename)


def community_prices_for_wave(t, idf, colmap):
    """Build canonical ``community_prices`` for one Tanzania NPS wave.

    Parameters
    ----------
    t : str          wave id ('2019-20' or '2020-21'); the ``t`` value.
    idf : pd.DataFrame
        raw CM_SEC_F_ID frame (convert_categoricals=False, so item_id carries
        the integer community-price item code 1..52).
    colmap : dict with keys
        cluster -- community cluster id column (interview__key) -> v
        item    -- community-price item code column (item_id)
        unit    -- native unit column          (cm_f061)
        qty     -- reported quantity basis      (cm_f062)
        price   -- reported price for that qty  (cm_f063)

    Returns
    -------
    pd.DataFrame indexed by (t, v, j, u) with column COMMUNITY_PRICES_COLUMNS
    (['Price'] -- reported unit price, currency per one native unit u).
    """
    c = colmap
    code_map = _community_price_codes()

    price = pd.to_numeric(idf[c['price']], errors='coerce').astype('Float64')
    qty = pd.to_numeric(idf[c['qty']], errors='coerce').astype('Float64')
    code = pd.to_numeric(idf[c['item']], errors='coerce').astype('Int64')

    unit_code = pd.to_numeric(idf[c['unit']], errors='coerce').astype('Int64')

    out = pd.DataFrame({
        'v': idf[c['cluster']].astype('string').str.strip(),
        'j': code.map(code_map).astype('string'),
        # Native unit CODE -> canonical Preferred Label.
        'u': unit_code.map(_COMMUNITY_PRICE_UNIT_LABELS).astype('string'),
        'Price': price,
        '_qty': qty,
    })

    # Keep only genuine reported prices: a positive price for a positive
    # quantity basis with a resolved item and unit.  Price<=0 & qty<=0 are the
    # "item not available / not priced" questionnaire sentinels.
    out = out[(out['Price'] > 0) & (out['_qty'] > 0)
              & out['v'].notna() & out['j'].notna() & out['u'].notna()].copy()

    # REPORTED unit price = price / quantity basis (currency per one native
    # unit u).  This divides the reported price through by its own reported
    # quantity -- not a cross-row aggregation.
    out['Price'] = (out['Price'] / out['_qty']).astype('Float64')
    out = out.drop(columns=['_qty'])

    out['t'] = t

    # ~10% of (t, v, j, u) tuples carry >1 source item code that the shared
    # label deliberately MERGES (e.g. MILLET (GRAIN) + SORGHUM (GRAIN) -> the
    # one harmonize_food label "Millet & Sorghum (grain)", exactly as
    # food_acquired lumps them).  Keep-first: a genuine REPORTED price per cell
    # (consistent with the sibling community_prices features), not a computed
    # mean over the label-lumped rows.  [The paragraph that used to stand here
    # said the collision was resolved by the MEAN; the code has always taken
    # the first.  Corrected under GH #637 -- doc only, no behaviour change.]
    #
    # GH #637 key-soundness review.  (t, v, j, u) IS the declared grain of this
    # feature, and the collisions are EXACTLY the intended label lump, checked
    # rather than assumed: of 261 colliding groups in 2019-20 and 1,281 in
    # 2020-21, 261 / 1,281 span more than one SOURCE ITEM CODE and ZERO repeat
    # a code.  So the survey never prices the same item twice in a cluster, and
    # no additional index level is available that would not break the shared
    # `j` vocabulary this feature exists to join on.
    #
    # What the collapse DOES do is resolve genuine disagreement arbitrarily:
    # 243 of the 261 (2019-20) and 1,149 of the 1,281 (2020-21) groups hold
    # more than one distinct observed price, e.g. cluster 01-06-47-85 in
    # 2020-21 reports "Millet & Sorghum (grain)" per Kg at both 2,000 and
    # 16,000 TSh -- .first() keeps 2,000 and the other observation disappears.
    # That is the GH #323 "arbitrary resolution of two OBSERVED values"
    # discussion, explicitly NOT the GH #637 completion question, and per #323
    # D1 the answer may not be a reducer.  Left for a maintainer decision;
    # recorded here rather than half-fixed.
    out = (out.groupby(['t', 'v', 'j', 'u'], as_index=False, dropna=False)
              [['Price']].first())

    # community_prices is a CLUSTER table -- there is no household i, the
    # natural grain is (t, v, j, u).  But the framework's
    # local_tools.map_index() (run on EVERY read path) unconditionally swaps
    # j -> i whenever a `j` index level is present and an `i` level is NOT.
    # That swap would rename our item level `j` to `i`, drop `j`, and collapse
    # the table to (t, v, u).  To keep `j` intact WITHOUT touching the
    # framework, carry a redundant `i` level (set equal to the cluster v)
    # positioned BEFORE `j`: map_index then sees i present and j after i, so it
    # does NOT swap, and the framework's _normalize_dataframe_index drops the
    # undeclared `i` level (data_scheme declares (t, v, j, u)), leaving the
    # canonical (t, v, j, u) grain.  v already in the index means
    # _join_v_from_sample is skipped; the spurious i==v never reaches the API.
    # (Same framework-compatibility shim Malawi/Ethiopia/Mali community_prices
    # use -- see Malawi assemble_community_prices.)
    out['i'] = out['v']
    out = out.set_index(['t', 'v', 'i', 'j', 'u'])

    out = out[COMMUNITY_PRICES_COLUMNS]
    assert out.reset_index().duplicated(['t', 'v', 'j', 'u']).sum() == 0, \
        f"community_prices {t}: (t,v,j,u) not unique"
    return out


COMMUNITY_CLUSTER_XWALK_COLUMNS = ['cluster', 'region', 'match', 'n_candidates']

# Date half-window (days) for disambiguating match keys that hold >1 survey
# cluster.  2019-20 leans on this (its key is only (region, ward)): the sparse
# Extended Panel tolerates a wider window (positive control 59%->82%).  2020-21
# resolves mostly deterministically from the baked-in (region, ward, EA, seq)
# codes, so the window only breaks the rare residual collision -- kept narrow
# since the dense Refresh Panel re-ambiguates if widened.  See the window sweep
# in Tanzania/_/CONTENTS.org (#113).
_XWALK_WINDOW_DAYS = {'2019-20': 14, '2020-21': 7}


def _int_code(series):
    """Admin code as a NaN-safe integer (drops the leading-zero / float-string
    ambiguity that makes ``id_01==2`` fail to match ``y5_cluster`` RR ``'02'``)."""
    return pd.to_numeric(series, errors='coerce').astype('Int64')


def link_community_to_cluster(t, cm_a, hh_a, window_days=None):
    """Crosswalk a Tanzania NPS community-price cluster to the household survey
    cluster (== ``sample().v``), issue #113.

    The community price instrument carries no survey-cluster id, its GPS is
    redacted, and the Survey-Solutions interviewer/team id is stripped, so the
    only geography shared with the household frame is the national admin tuple.
    The match is wave-specific:

    * **2020-21** -- the survey cluster id is BAKED INTO the community admin
      codes.  With ``y5_cluster = RR-DD-WWW-EE-CCC``, the tuple
      ``(RR, WWW, EE, CCC)`` equals the community ``(id_01, id_03, id_04,
      id_05)`` (region, ward, EA, cluster-seq) on 99-100% of the geo-unique
      wards, so we reconstruct the cluster *deterministically* from those four
      codes (455/488 unique, all real ``sample().v``), dropping only the
      within-region district ``DD`` -- the one field whose community (national
      code) and household (within-region index) encodings are incompatible
      (``(region, district, ward)`` matches 0/488).

    * **2019-20** -- ``clusterid`` uses an internal numbering that does not line
      up with the national admin codes below the ward, so the finest reliable
      key is ``(region, ward)``; wards holding >1 cluster are split by
      interview-date overlap (the community ``cm_start`` falls inside the
      candidate cluster's household ``hh_a18`` window, because one field team
      runs the community interview and that cluster's households in one visit
      -- validated 99% (2020-21) / 79% (2019-20) on geo-unique wards).

    The date lever is also the residual disambiguator for any wave whose match
    key still leaves >1 candidate.

    Parameters
    ----------
    t : str
        Wave id, ``'2019-20'`` or ``'2020-21'`` (the only waves with a
        community questionnaire).  Selects the household cluster encoding:
        2019-20 = ``clusterid`` + ``t0_region`` / ``t0_ward_code``; 2020-21 =
        ``y5_cluster`` (``RR-DD-WWW-EE-CCC``, admin embedded).
    cm_a, hh_a : pd.DataFrame
        Raw ``CM_SEC_A`` (community cover) and ``HH_SEC_A`` (household cover),
        loaded with ``convert_categoricals=False``.
    window_days : int, optional
        Date half-window in days; defaults to ``_XWALK_WINDOW_DAYS[t]``.

    Returns
    -------
    pd.DataFrame indexed by ``(t, v)`` (``v`` = community ``interview__key``,
    matching ``community_prices.v``) with columns
    ``COMMUNITY_CLUSTER_XWALK_COLUMNS``:
      ``cluster``      -- resolved survey cluster id (== ``sample().v``) where a
                          unique match exists, else <NA>;
      ``region``       -- region code (always present; the region-level
                          fallback key for the #113 quantity/price fallback);
      ``match``        -- ``'cluster'`` (unique) or ``'region'`` (ambiguous or
                          no surviving household in the ward -> region fallback);
      ``n_candidates`` -- # survey clusters sharing the community's match key
                          (2020-21: region/ward/EA/seq; 2019-20: region/ward).
                          0 = no household at that key, 1 = uniquely resolved,
                          >1 = date-disambiguated or fell back to region.

    This is a deliberately lossy, confidence-tagged crosswalk, NOT a clean key
    join (see Tanzania/_/CONTENTS.org #113); it is materialised separately so
    ``community_prices.v`` stays the native ``interview__key`` and the residual
    falls back to region.
    """
    if t not in ('2019-20', '2020-21'):
        raise ValueError(f"community_cluster_xwalk: unsupported wave {t!r}")
    if window_days is None:
        window_days = _XWALK_WINDOW_DAYS.get(t, 10)

    # --- household side: survey cluster id + match key + interview date ---
    if t == '2019-20':
        # 2019-20's clusterid uses an internal numbering that does NOT line up
        # with the national admin codes below the ward, so the finest reliable
        # key is (region, ward); wards holding >1 cluster fall to the date lever.
        h = hh_a[hh_a['clusterid'].notna()].copy()
        h['k_region'] = _int_code(h['t0_region'])
        h['k_ward'] = _int_code(h['t0_ward_code'])
        h['clu'] = h['clusterid'].apply(format_id)           # int -> '11014002'
        keycols = ['k_region', 'k_ward']
        cm_src = {'k_region': 'id_01', 'k_ward': 'id_03'}
    else:  # 2020-21
        # The survey cluster id is BAKED INTO the community admin codes: with
        # y5_cluster = RR-DD-WWW-EE-CCC, the tuple (RR, WWW, EE, CCC) equals the
        # community (id_01, id_03, id_04, id_05) on 99-100% of the geo-unique
        # wards (verified, issue #113).  So we reconstruct the cluster directly
        # from (region, ward, EA, cluster-seq) -- deterministic, no date needed
        # -- dropping only the within-region district DD, the one field whose
        # community (national code) and household (within-region index)
        # encodings are incompatible.  Date only breaks the rare residual
        # (RR, WWW, EE, CCC) collision across DD.
        y5 = hh_a['y5_cluster'].astype('string').str.strip()
        keep = y5.str.match(r'^\d\d-\d\d-\d\d\d-\d\d-\d\d\d$').fillna(False)
        h = hh_a[keep].copy()
        parts = y5[keep].str.split('-', expand=True)
        h['k_region'] = _int_code(parts[0])
        h['k_ward'] = _int_code(parts[2])
        h['k_ea'] = _int_code(parts[3])
        h['k_seq'] = _int_code(parts[4])
        h['clu'] = y5[keep]                                  # full y5_cluster == sample().v
        keycols = ['k_region', 'k_ward', 'k_ea', 'k_seq']
        cm_src = {'k_region': 'id_01', 'k_ward': 'id_03',
                  'k_ea': 'id_04', 'k_seq': 'id_05'}
    h['date'] = pd.to_datetime(h['hh_a18'], errors='coerce')

    hk = h.dropna(subset=keycols + ['clu']).copy()
    for k in keycols:
        hk[k] = hk[k].astype('int64')                        # plain-int keys
    cand = hk.groupby(keycols)['clu'].agg(lambda s: sorted(set(s))).to_dict()
    win = h.dropna(subset=['date', 'clu']).groupby('clu')['date'].agg(['min', 'max'])

    # --- community side ---
    cm = cm_a.copy()
    for k, src in cm_src.items():
        cm[k] = _int_code(cm[src])
    cm['date'] = pd.to_datetime(cm['cm_start'], errors='coerce')
    cm['v'] = cm['interview__key'].astype('string').str.strip()

    span = pd.Timedelta(days=window_days)
    cols = keycols + ['v', 'date']
    rows = []
    for vals in zip(*[cm[c] for c in cols]):
        rec = dict(zip(cols, vals))
        v = rec['v']
        if pd.isna(v):
            continue
        key = tuple(rec[k] for k in keycols)
        clusters = (cand.get(tuple(int(x) for x in key), [])
                    if all(pd.notna(x) for x in key) else [])
        d = rec['date']
        resolved = pd.NA
        if len(clusters) == 1:
            resolved = clusters[0]
        elif len(clusters) > 1 and pd.notna(d):
            hits = [c for c in clusters
                    if c in win.index
                    and win.loc[c, 'min'] - span <= d <= win.loc[c, 'max'] + span]
            if len(hits) == 1:
                resolved = hits[0]
        r = rec['k_region']
        rows.append({
            't': t,
            'v': v,
            'cluster': resolved,
            'region': str(int(r)) if pd.notna(r) else pd.NA,
            'match': 'cluster' if pd.notna(resolved) else 'region',
            'n_candidates': len(clusters),
        })

    out = pd.DataFrame(rows)
    out['cluster'] = out['cluster'].astype('string')
    out['region'] = out['region'].astype('string')
    out['match'] = out['match'].astype('string')
    out['n_candidates'] = out['n_candidates'].astype('Int64')
    out = out.drop_duplicates(['t', 'v']).set_index(['t', 'v'])
    return out[COMMUNITY_CLUSTER_XWALK_COLUMNS]