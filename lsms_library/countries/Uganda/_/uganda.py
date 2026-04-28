import numpy as np
import pandas as pd
from collections import defaultdict
from cfe.df_utils import use_indices
import warnings
import json

if __name__=='__main__':
    import sys
    sys.path.append('../../_')
    sys.path.append('../../../_')
    from local_tools import format_id, get_dataframe
else:
    from lsms_library.local_tools import format_id, get_dataframe

def District(x):
    """Canonical District form across all Uganda waves.

    Pre-2018 waves source ``District`` from numeric Stata columns
    (``h1aq1``, ``h1aq1a``) which df_data_grabber stringifies to
    ``'101.0'``-style float-strings (CLAUDE.md: ``format_id`` is
    auto-applied to ``idxvars`` but NOT to ``myvars``).  Post-2018
    waves source from string columns (``district_name`` / ``district``)
    which need no normalisation but ``format_id`` is a no-op on them.

    Defining a country-level ``District`` formatter routes every
    Uganda ``District`` myvar through the canonical normalizer so
    cross-wave District values share an int-string encoding.
    GH #161.
    """
    return format_id(x)


def v(x):
    """Canonical cluster-id form for Uganda's ``v`` myvar across all tables.

    Uganda declares ``v`` as a ``myvar`` in ``sample`` (and elsewhere)
    rather than an ``idxvar``, so the auto-applied ``format_id`` does
    not fire and numeric cluster codes like ``10120402`` get
    float-stringified to ``'10120402.0'`` -- which silently fails to
    join against ``cluster_features.v`` and ``household_characteristics.v``
    where ``v`` is in ``idxvars`` and *does* go through ``format_id``.

    Defining a country-level ``v`` formatter routes every Uganda
    ``v`` myvar through ``format_id``, restoring the cross-table
    invariant.  ``format_id`` is a no-op on already-canonical strings
    (e.g., the ``parish_name`` strings in 2018-19, 2019-20) and maps
    empty strings to ``None`` (cleaning up the 565-row ``v == ''``
    leak in 2009-10's sample).

    GH #196.
    """
    return format_id(x)


# Data to link household ids across waves
Waves = {'2005-06':(),
         '2009-10':(), # ID of parent household  in ('GSEC1.dta',"HHID",'HHID_parent'), but not clear how to use
         '2010-11':(),
         '2011-12':(),
         '2013-14':('GSEC1.dta','HHID','HHID_old'),
         '2015-16':('gsec1.dta','HHID','hh',lambda s: s.replace('-05-','-04-')),
         '2018-19':('GSEC1.dta','hhid','t0_hhid'),
         '2019-20':('HH/gsec1.dta','hhid','hhidold')}

def harmonized_unit_labels(fn='../../_/unitlabels.csv',key='Code',value='Preferred Label'):
    unitlabels = pd.read_csv(fn)
    unitlabels.columns = [s.strip() for s in unitlabels.columns]
    unitlabels = unitlabels[[key,value]].dropna()
    unitlabels = unitlabels.set_index(key)

    unitlabels = unitlabels.squeeze().str.strip().to_dict()

    return unitlabels

def harmonized_food_labels(fn='../../_/food_items.org',key='Code',value='Preferred Label'):
    # Harmonized food labels
    food_items = pd.read_csv(fn,delimiter='|',skipinitialspace=True,converters={1:int,2:lambda s: s.strip()})
    food_items.columns = [s.strip() for s in food_items.columns]
    food_items = food_items[[key,value]].dropna()
    food_items = food_items.set_index(key)

    return food_items.squeeze().str.strip().to_dict()


def food_acquired(fn,myvars):

    df = get_dataframe(fn,convert_categoricals=False)

    df = df.loc[:,[v for v in myvars.values()]].rename(columns={v:k for k,v in myvars.items()})

    # Replace missing unit values
    df['units'] = df['units'].fillna('---')

    df = df.set_index(['HHID','item','units']).dropna(how='all')

    df.index.names = ['i','j','u']  # HHID='i', item='j', units='u'


    # Fix type of hhids if need be
    if df.index.get_level_values('i').dtype ==float:
        fix = dict(zip(df.index.levels[0],df.index.levels[0].astype(int).astype(str)))
        df = df.rename(index=fix,level=0)

    df = df.rename(index=harmonized_food_labels(),level='j')
    unitlabels = harmonized_unit_labels()
    df = df.rename(index=unitlabels,level='u')

    if not 'market' in df.columns:
        df['market'] = df.filter(regex='^market').median(axis=1)

    # Compute unit values
    df['unitvalue_home'] = df['value_home']/df['quantity_home']
    df['unitvalue_away'] = df['value_away']/df['quantity_away']
    df['unitvalue_own'] = df['value_own']/df['quantity_own']
    df['unitvalue_inkind'] = df['value_inkind']/df['quantity_inkind']

    # Get list of units used in current survey
    units = list(set(df.index.get_level_values('u').tolist()))

    unknown_units = set(units).difference(unitlabels.values())
    if len(unknown_units):
        warnings.warn("Dropping some unknown unit codes!")
        print(unknown_units)
        df = df.loc[df.index.isin(unitlabels.values(),level='u')]

    with open('../../_/conversion_to_kgs.json','r') as f:
        conversion_to_kgs = pd.Series(json.load(f))

    conversion_to_kgs.name='Kgs'
    conversion_to_kgs.index.name='u'

    df = df.join(conversion_to_kgs,on='u')
    df = df.astype(float)

    return df


def nonfood_expenditures(fn='', purchased=None, away=None, produced=None,
                         given=None, item='item', HHID='HHID'):
    """Uganda non-food expenditures from a single .dta file.

    Aggregates across three or four source columns (purchased, away,
    produced, given) at the (HHID, item) level and returns a wide
    matrix (HHID rows x item columns) of total expenditures.

    Replaces the prior lsms.tools.get_food_expenditures-based
    implementation with an inline pandas groupby+sum; the upstream
    lsms dependency is being retired.
    """
    if __name__ == '__main__':
        from local_tools import get_dataframe
    else:
        from lsms_library.local_tools import get_dataframe

    nonfood_items = harmonized_food_labels(
        fn='../../_/nonfood_items.org', key='Code', value='Preferred Label')

    # Read source file via the repo's standard entry point.
    df = get_dataframe(fn, convert_categoricals=False)

    # Gather source columns (skip None entries).
    source_cols = {
        'purchased': purchased,
        'away':      away,
        'produced':  produced,
        'given':     given,
    }
    source_cols = {k: v for k, v in source_cols.items() if v is not None}

    # Project down to the columns we need and rename.
    keep = [HHID, item] + list(source_cols.values())
    df = df[keep].copy()
    rename_map = {HHID: 'HHID', item: 'itmcd'}
    rename_map.update({v: k for k, v in source_cols.items()})
    df = df.rename(columns=rename_map)

    # Coerce itmcd to numeric, drop missing item codes, cast to int.
    df['itmcd'] = pd.to_numeric(df['itmcd'], errors='coerce')
    df = df.dropna(subset=['itmcd'])
    df['itmcd'] = df['itmcd'].astype(int)

    # Handle HHID-as-float-string (see upstream lsms.tools lines 101-108).
    try:
        first = df['HHID'].iloc[0]
        if isinstance(first, str) and first.split('.')[-1] == '0':
            df['HHID'] = df['HHID'].apply(lambda x: '%d' % int(float(x)))
    except (ValueError, AttributeError, IndexError):
        pass

    # Replace itmcd codes with preferred labels BEFORE groupby so that
    # items sharing a label are merged naturally by the groupby.
    # Keep only rows with a recognized item code.
    df['itmcd'] = df['itmcd'].replace(nonfood_items)
    df = df[df['itmcd'].isin(nonfood_items.values())]

    # Sum source columns, groupby HHID+itmcd (now label names).
    active_sources = list(source_cols.keys())
    df['total'] = df[active_sources].sum(axis=1, min_count=1)
    wide = df.groupby(['HHID', 'itmcd'])['total'].sum().unstack('itmcd')
    wide = wide.fillna(0)

    # Match the old output's index/column names.
    wide.index.name = 'j'
    wide.columns.name = 'i'
    return wide


def id_walk(df, updated_ids, hh_index='i'):
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
    index_names = list(df.index.names or [])
    if not index_names:
        raise ValueError("Dataframe must have a named MultiIndex for id_walk.")
    if 't' not in index_names:
        raise KeyError("Index must contain a 't' level for wave identifiers.")

    household_level = hh_index
    fallback_used = False
    if household_level not in index_names:
        for candidate in ('i', 'j'):
            if candidate in index_names:
                household_level = candidate
                fallback_used = True
                break
        else:
            # fallback to the first non-'t' level (or level 0)
            non_wave_levels = [name for name in index_names if name != 't']
            if not non_wave_levels:
                raise KeyError("Cannot determine household index level for id_walk.")
            household_level = non_wave_levels[0]
            fallback_used = True

    household_level_pos = index_names.index(household_level)

    if fallback_used and household_level != hh_index:
        warnings.warn(
            f"id_walk expected index level '{hh_index}' but found '{household_level}'. "
            "Proceeding with the detected household index."
        )

    #seperate df into different waves:
    dfs = {}
    waves = df.index.get_level_values('t').unique()
    for wave in waves:
        dfs[wave] = df[df.index.get_level_values('t') == wave].copy()
    #update ids for each wave
    for wave, df_wave in dfs.items():
        #update ids
        if wave in updated_ids:
            df_wave = df_wave.rename(index=updated_ids[wave], level=household_level)
            #update the dataframe with the new ids
            dfs[wave] = df_wave
        else:
            continue
    #combine the updated dataframes
    df = pd.concat(dfs.values(), axis=0)

    if 'i' not in df.index.names or household_level != 'i':
        df.index = df.index.set_names('i', level=household_level_pos)

    # df= df.rename(index=updated_ids,level=['t', household_level])
    df.attrs['id_converted'] = True
    return df  
