import numpy as np
import pandas as pd
from collections import defaultdict
from cfe.df_utils import use_indices
import warnings
import json
from lsms_library.local_tools import get_dataframe, DVCFS, format_id, df_from_orgfile
import os

# Wave list used by harmonized_food_labels() and other helpers.
# NB: panel_ids no longer uses this dict -- see panel_ids.py for the
# bespoke chain that goes W3->W2->W1 via household_id2 identity mapping.
Waves = {'2011-12':(),
         '2013-14':('sect_cover_hh_w2.dta','household_id2','household_id'),
         '2015-16':('sect_cover_hh_w3.dta','household_id2','household_id'),
         '2018-19':(),  # Entirely new sample drawn
         '2021-22':(),
         }


def harmonized_unit_labels(fn='../../_/unitlabels.csv',key='Code',value='Preferred Label'):
    unitlabels = pd.read_csv(fn)
    unitlabels.columns = [s.strip() for s in unitlabels.columns]
    unitlabels = unitlabels[[key,value]].dropna()
    unitlabels = unitlabels.set_index(key)

    return unitlabels.squeeze().str.strip().to_dict()


def _read_food_label_table(fn):
    """Load the harmonized-food label table as a wide DataFrame.

    Unit #0 migration (2026-06-14): the canonical food-item label table now
    lives as ``#+name: harmonize_food`` inside ``categorical_mapping.org``
    (was the standalone ``food_items.org``).  Read it via the org-table
    reader.  A bare pipe-table file (e.g. the legacy ``nonfood_items.org``,
    which has no ``#+name`` header) still parses -- ``df_from_orgfile`` with
    ``name=None`` grabs the first table in the file.  Pass the
    ``categorical_mapping.org`` path to pick up ``harmonize_food`` explicitly.
    """
    import os
    base = os.path.basename(str(fn))
    if base == 'categorical_mapping.org':
        from lsms_library.local_tools import all_dfs_from_orgfile
        return all_dfs_from_orgfile(fn)['harmonize_food']
    # Legacy / code-keyed tables (nonfood_items.org): bare pipe table.
    from lsms_library.local_tools import df_from_orgfile
    return df_from_orgfile(fn, name=None, to_numeric=False)


def harmonized_food_labels(fn='../../_/categorical_mapping.org',key=list(Waves.keys()),value='Preferred Label'):
    # Harmonized food labels.  Reads the canonical ``harmonize_food`` table
    # (Unit #0); ``df_from_orgfile`` already nulls '---' cells, so the
    # replace('---', ...) calls below are now defensive no-ops kept for the
    # rare legacy bare-pipe path.
    food_items = _read_food_label_table(fn)
    food_items.columns = [str(s).strip() for s in food_items.columns]
    food_items = food_items.astype(object).where(food_items.notna(), pd.NA)
    food_items = food_items.loc[:,food_items.count()>0]
    drop = [c for c in ['FTC Code', 'FDC ID'] if c in food_items.columns]
    if drop:
        food_items = food_items.drop(columns=drop)
    food_items = food_items.apply(lambda x: x.astype(object).map(
        lambda s: s.strip() if isinstance(s, str) else s))

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


def harmonize_food_union_map(fn='../../_/categorical_mapping.org', value='Preferred Label'):
    """Union of every per-wave column of ``harmonize_food`` -> Preferred Label.

    Mirrors Malawi's ``harmonize_food_labels`` cross-wave union (GH #216): a
    raw food string documented in *any* wave column resolves to its Preferred
    Label regardless of which wave is being processed.  Applied at the
    WAVE-script level (in :func:`food_acquired`) so the resolved ``j`` label
    persists into each wave parquet -- the country-API concat path
    (``load_from_waves``) then sees clean labels with no further rename.
    """
    food_items = _read_food_label_table(fn)
    food_items.columns = [str(s).strip() for s in food_items.columns]
    wave_cols = [c for c in food_items.columns
                 if c not in (value, 'FTC Code', 'FDC ID')]
    unify = {}
    for col in wave_cols:
        for _, row in food_items.iterrows():
            v = row.get(col)
            p = row.get(value)
            if pd.isna(v) or pd.isna(p):
                continue
            v_str = str(v).strip()
            p_str = str(p).strip()
            if v_str in ('', '---') or p_str in ('', '---'):
                continue
            unify.setdefault(v_str, p_str)
    return unify

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
                                         convert_categoricals=True, **kwargs):
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
    columns = ['urban', 'region'] + list(kwargs.keys())
    df = df.loc[:, ['HHID'] + columns]
    df = df.set_index('HHID')
    return df.loc[:, columns]


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

    food_items = harmonized_food_labels(fn='../../_/categorical_mapping.org')

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
    if food_items is not None:
        df = df.replace({'itmcd': food_items})
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

def food_acquired(fn,myvars):
    """Reshape Ethiopia's food_acquired into the canonical (t, i, j, u, s) form.

    Phase 3 of GH #169 / DESIGN_food_acquired_canonical_2026-05-05.org.
    Per the design row for Ethiopia:
        "derive purchased + produced (= total - purchased)"

    Ethiopia's source records, per (household, item):
      - quantity / units              : TOTAL acquired in `units`
      - value_purchased               : monetary value of the purchased subset
      - quantity_purchased / units_purchased : amount and unit of the
        purchased subset

    The wave script supplies `myvars` mapping these to the source columns;
    `t` is appended from the wave folder name (parent of cwd).

    Unit handling (decision documented 2026-05-06):
      In wave 1 (2011-12) only 64/19,231 (0.3%) of rows where both quantity
      and quantity_purchased are positive have `units != units_purchased`.
      The asymmetry is rare enough that we treat the row as "single-unit":
      we use `units` as the canonical `u` axis and silently drop
      `units_purchased`.  Produced is then derived in the same unit:
          Produced = (quantity - quantity_purchased).clip(lower=0)
      For the rare unit-mismatch rows the subtraction is approximate, but
      preserving the framework helper's one-unit-per-row contract is more
      important than the ~0.3% accuracy loss.  No kg conversion is done at
      this layer — that lives downstream in the framework's
      food_quantities_from_acquired path.
    """
    from lsms_library.transformations import food_acquired_to_canonical

    df = get_dataframe(fn,convert_categoricals=True)

    df = df.loc[:,list(myvars.values())].rename(columns={v:k for k,v in myvars.items()})

    # Correct unit labels (title-case + a few historical typos).  Preserved
    # from the legacy implementation -- keeps `u` values consistent with the
    # countries/Ethiopia/_/conversion_to_kgs.json keys that downstream code
    # still references for kg conversion.
    df['units_purchased'] = df['units_purchased'].str.title()
    df['units'] = df['units'].str.title()
    # Strip the survey's "NNN. " code prefix that ESS embeds in the unit
    # value labels (e.g. "1. Kilogram", "171. Sini Small") so `u` carries a
    # clean label, not a code-prefixed string (GH #223 Layer 2).  Applied
    # only to the unit columns -- item names / IDs may legitimately start
    # with a digit.  "1. Kilogram" -> "Kilogram" then resolves via KNOWN_METRIC
    # / the global u.org (-> "Kg"); container labels stay native.
    for _col in ('units', 'units_purchased'):
        df[_col] = (df[_col].str.replace(r'^\s*\d+\.\s*', '', regex=True)
                            .str.strip())
    rep = {r'\s+':' ',
           'Meduim': 'Medium',
           'Kubaya ':'Kubaya/Cup ',
           'Milliliter' : 'Mili Liter'}
    df = df.replace(rep, regex=True)

    # Coerce HHID to canonical integer-string form when it lands as float
    # (some Ethiopia waves' household_id columns import as float64).
    if df['HHID'].dtype == float:
        df['HHID'] = df['HHID'].astype(str).str.split('.').str[0].replace('nan', pd.NA)

    # Compute Produced = total - purchased, clipped at 0.  In ~0.3% of
    # populated rows units != units_purchased; we accept the approximate
    # subtraction and use `units` as the canonical unit u.
    quantity = pd.to_numeric(df['quantity'], errors='coerce')
    quantity_purchased = pd.to_numeric(df['quantity_purchased'], errors='coerce')
    df['Quantity'] = quantity
    df['Produced'] = (quantity.fillna(0) - quantity_purchased.fillna(0)).clip(lower=0)
    df['Expenditure'] = pd.to_numeric(df['value_purchased'], errors='coerce')

    # Derive `t` from the wave folder name.  Wave scripts cd into
    # countries/Ethiopia/<wave>/_/ before importing this helper, so the
    # parent directory of cwd is the wave label.  Mirrors the established
    # script-path pattern in Tanzania / Malawi.
    import os
    wave = os.path.basename(os.path.dirname(os.getcwd()))
    df['t'] = wave

    # Resolve raw food strings to canonical Preferred Labels at the WAVE
    # level (Unit #0 / Malawi pattern).  The raw `item` is the Stata value
    # label the categorical read surfaced -- per-wave name strings that vary
    # in casing, code prefixes ("101. Teff"), typos ("PuUrchased Injera")
    # and Stata truncation ("Greens (kale, cabbage, e").  The
    # harmonize_food union map (every per-wave column of categorical_mapping
    # .org's harmonize_food table -> Preferred Label) covers all of these,
    # so the resolved label persists into this wave's parquet and the
    # country-API concat path sees clean `j` labels.  Strings absent from
    # the map pass through unchanged.
    food_map = harmonize_food_union_map(fn='../../_/categorical_mapping.org')
    df['item'] = df['item'].astype(str).str.strip().replace(food_map)

    # Build the wide-form frame the framework helper expects:
    # index (t, i, j, u), columns (Quantity, Expenditure, Produced).
    df = (df.rename(columns={'HHID': 'i', 'item': 'j', 'units': 'u'})
            .set_index(['t', 'i', 'j', 'u'])
            [['Quantity', 'Expenditure', 'Produced']]
            .dropna(how='all'))

    # Ditch the now-unused `units_purchased` (already dropped via the
    # column projection above).  Helper produces (t, i, j, u, s) with
    # s in {'purchased', 'produced'}.
    out = food_acquired_to_canonical(df, drop_columns=())
    return out

def food_expenditures(fn='',purchased=None,away=None,produced=None,given=None,item='item',HHID='HHID'):
    food_items = harmonized_food_labels(fn='../../_/categorical_mapping.org')

    expenditures = _sum_expenditures_from_file(fn, purchased, away, produced, given,
                                                itmcd=item, HHID=HHID, itemlabels=food_items)

    expenditures.index.name = 'j'
    expenditures.columns.name = 'i'

    expenditures = expenditures[expenditures.columns.intersection(food_items.values())]

    return expenditures


def nonfood_expenditures(fn='',purchased=None,away=None,produced=None,given=None,item='item',HHID='HHID'):
    nonfood_items = harmonized_food_labels(fn='../../_/nonfood_items.org',key='Code',value='Preferred Label')

    expenditures = _sum_expenditures_from_file(fn, purchased, away, produced, given,
                                                itmcd=item, HHID=HHID, itemlabels=nonfood_items)

    expenditures.index.name = 'j'
    expenditures.columns.name = 'i'
    expenditures = expenditures[expenditures.columns.intersection(nonfood_items.values())]

    return expenditures

def food_quantities(fn='',item='item',HHID='HHID',
                    purchased=None,away=None,produced=None,given=None,units=None):
    food_items = harmonized_food_labels(fn='../../_/categorical_mapping.org')

    quantities = _sum_expenditures_from_file(fn, purchased, away, produced, given,
                                              itmcd=item, HHID=HHID, units=units,
                                              itemlabels=food_items)

    quantities.index.names = ['j','u']
    quantities.columns.name = 'i'

    return quantities

def age_sex_composition(fn,sex='sex',sex_converter=None,age='age',months_spent='months_spent',HHID='HHID',months_converter=None, convert_categoricals=True,Age_ints=None,fn_type='stata'):

    if Age_ints is None:
        # Match Uganda FCT categories
        Age_ints = ((0,4),(4,9),(9,14),(14,19),(19,31),(31,51),(51,100))

    df = _household_roster_from_file(fn, sex=sex, age=age, HHID=HHID,
                                      months_spent=months_spent,
                                      sex_converter=sex_converter,
                                      months_converter=months_converter,
                                      Age_ints=Age_ints,
                                      convert_categoricals=convert_categoricals)
    df.index.name = 'j'
    df.columns.name = 'k'

    return df


def other_features(fn,urban=None,region=None,HHID='HHID',urban_converter=None):

    df = _household_identification_from_file(fn, HHID=HHID, urban=urban, region=region,
                                              urban_converter=urban_converter)
    df.index.name = 'j'
    df.columns.name = 'k'

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

    d.pop(np.nan, None)  # Get rid of nan key, if any
    d.pop(pd.NA, None)

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

def panel_attrition(df,return_ids=False,waves=None):
    """
    Produce an upper-triangular) matrix showing the number of households (j) that
    transition between rounds (t) of df.
    """
    idxs = df.reset_index().groupby('t')['j'].apply(list).to_dict()

    if waves is None:
        waves = list(Waves.keys())

    foo = pd.DataFrame(index=waves,columns=waves)
    IDs = {}
    for m,s in enumerate(waves):
        for t in waves[m:]:
            IDs[(s,t)] = set(idxs[s]).intersection(idxs[t])
            foo.loc[s,t] = len(IDs[(s,t)])

    if return_ids:
        return foo,IDs
    else:
        return foo


# ---------------------------------------------------------------------
# plot_features (GH #167) — ESS post-planting parcel/field join
# ---------------------------------------------------------------------
#
# ESS records farm land at two granularities in the post-planting (pp)
# round:
#   - sect2_pp  : one row per PARCEL (holder_id, household_id, parcel_id)
#                 with the lasting parcel attributes Tenure / SoilType.
#   - sect3_pp  : one row per FIELD  (holder_id, household_id, parcel_id,
#                 field_id) with field-level Area / Irrigated.
# plot_features emits ONE ROW PER FIELD, broadcasting the parcel-level
# attributes onto each of the parcel's fields via a LEFT JOIN on the
# composite key (holder_id, household_id, parcel_id).
#
# CRITICAL: the join key MUST include holder_id.  A single household can
# host several "holders" (decision-makers) who each number their parcels
# from 1; joining on (household_id, parcel_id) alone collides those and
# inflates the result by +265..+1567 rows/wave.  Verified empirically:
# with holder_id in the key the merged row count equals the sect3 row
# count exactly in every wave (0 inflation).
#
# plot_id = format_id(holder_id)_format_id(parcel_id)_format_id(field_id)
# — globally unique within (t, i).
#
# GPS: the public ESS GPS coordinates are 100% redacted to the literal
# string "**CONFIDENTIAL**" in every wave, so Latitude / Longitude are
# DEFERRED (not emitted).  The GPS-*measured field area* (sq metres) is
# NOT redacted and is the primary Area source.
#
# Area: GPS sq-metres / 10000 -> hectares where present (AreaUnit =
# 'hectares').  Where only a farmer estimate in a non-metric local unit
# (Timad/Kert/Boy/...) exists, we cannot convert (local-unit->ha factors
# are not in the repo), so Area = NaN and AreaUnit = the native unit name.

PLOT_AREA_UNIT_HA = 'hectares'


def _eth_orgfile_path():
    """Resolve Ethiopia/_/categorical_mapping.org from any wave-script CWD."""
    here = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        os.path.join(here, 'categorical_mapping.org'),
        os.path.abspath(os.path.join('..', '..', '_', 'categorical_mapping.org')),
        'categorical_mapping.org',
    ]
    return next((c for c in candidates if os.path.exists(c)), candidates[0])


def _harmonize_wave_keyed(name):
    """Load a (Wave, Code) -> Preferred Label dict from categorical_mapping.org."""
    df = df_from_orgfile(_eth_orgfile_path(), name=name,
                         set_columns=True, to_numeric=True)
    out = {}
    for _, row in df.iterrows():
        wv = str(row['Wave']).strip()
        c = row['Code']
        try:
            c = int(c)
        except (TypeError, ValueError):
            pass
        lab = row.get('Preferred Label')
        if pd.isna(lab):
            continue
        out[(wv, c)] = str(lab).strip()
    return out


def _harmonize_code_label(name):
    """Load a Code -> Preferred Label dict (not wave-keyed)."""
    df = df_from_orgfile(_eth_orgfile_path(), name=name,
                         set_columns=True, to_numeric=True)
    out = {}
    for _, row in df.iterrows():
        c = row['Code']
        try:
            c = int(c)
        except (TypeError, ValueError):
            pass
        lab = row.get('Preferred Label')
        if pd.isna(lab):
            continue
        out[c] = str(lab).strip()
    return out


def _map_int_codes(series, code_map):
    """Map a numeric (raw Stata code) Series through ``code_map``.

    Returns a 'string' Series with pd.NA where the code is absent."""
    if series is None:
        return None
    out = pd.to_numeric(series, errors='coerce').astype('Int64').map(code_map)
    return out.astype('string').where(out.notna(), pd.NA)


def plot_features_for_wave(t, sect2, sect3, colmap):
    """Build canonical ``plot_features`` for one Ethiopia ESS wave.

    Parameters
    ----------
    t : str
        Wave id (e.g. ``"2013-14"``), used as the ``t`` index value AND
        as the wave key for the wave-keyed harmonize tables.
    sect2 : pd.DataFrame
        Raw ``sect2_pp_w{N}.dta`` (parcel roster), loaded with
        ``convert_categoricals=False`` so categorical columns carry the
        integer codes the harmonize tables key on.
    sect3 : pd.DataFrame
        Raw ``sect3_pp_w{N}.dta`` (field roster), same loading.
    colmap : dict
        Per-wave column-name map.  Required keys:
            hhid       — household id column EMITTED as ``i`` (must match
                         sample().i: household_id2 for W2/W3, else
                         household_id).  Carried from sect3.
            join_hhid  — household id column used in the parcel JOIN key
                         (always 'household_id', present in both files).
            holder_id, parcel_id, field_id — join-key + plot_id columns.
            area_gps   — sect3 GPS field-area column (square metres).
            area_unit  — sect3 farmer-estimate area-unit code column.
            acquire    — sect2 "how acquired" code column (-> Tenure).
        Optional keys (NaN where omitted / not asked):
            soil_type  — sect2 soil-type code column (absent in W1).

    Returns
    -------
    pd.DataFrame indexed by ``(t, i, plot_id)`` with columns
        ``Area`` (hectares, float), ``AreaUnit`` (str), ``Tenure`` (str),
        ``TenureSystem`` (str, always NaN — ESS has no analogue),
        ``SoilType`` (str), ``Irrigated`` (nullable bool).  GPS
        Latitude / Longitude are NOT emitted (source 100% redacted).
    """
    c = colmap
    acquire_map = _harmonize_wave_keyed('harmonize_acquire')
    area_unit_map = _harmonize_wave_keyed('harmonize_area_unit')
    soil_map = _harmonize_code_label('harmonize_soil')

    # --- Parcel-level attributes (sect2), keyed for the join ---
    key = [c['holder_id'], c['join_hhid'], c['parcel_id']]
    parcel = sect2.copy()
    # Tenure: wave-keyed acquire code -> canonical tenure.
    parcel['_Tenure'] = parcel[c['acquire']].pipe(
        lambda s: pd.to_numeric(s, errors='coerce').astype('Int64').map(
            lambda code: acquire_map.get((t, int(code))) if pd.notna(code) else pd.NA
        )).astype('string')
    if c.get('soil_type') and c['soil_type'] in parcel.columns:
        parcel['_SoilType'] = _map_int_codes(parcel[c['soil_type']], soil_map)
    else:
        parcel['_SoilType'] = pd.Series(pd.NA, index=parcel.index, dtype='string')

    parcel_attrs = (parcel[key + ['_Tenure', '_SoilType']]
                    .drop_duplicates(subset=key))

    # --- Field-level rows (sect3) ---
    field = sect3.copy()

    # Area: GPS square-metres -> hectares where present.
    area_sqm = pd.to_numeric(field[c['area_gps']], errors='coerce')
    area_ha = (area_sqm / 10000.0).astype('Float64')
    area_ha = area_ha.where(area_sqm > 0, pd.NA)

    # AreaUnit: 'hectares' where GPS area is present; otherwise the
    # native farmer-estimate unit name (Area stays NaN there).
    native_unit = _map_int_codes(field[c['area_unit']], area_unit_map) \
        if c.get('area_unit') and c['area_unit'] in field.columns \
        else pd.Series(pd.NA, index=field.index, dtype='string')
    area_unit = pd.Series(pd.NA, index=field.index, dtype='string')
    area_unit = area_unit.where(area_ha.isna(), PLOT_AREA_UNIT_HA)
    area_unit = area_unit.where(area_ha.notna(), native_unit)

    # Irrigated: 1=Yes, 2=No.
    irr = pd.to_numeric(field[c['irrigated']], errors='coerce').astype('Int64')
    irrigated = pd.Series(pd.NA, index=field.index, dtype='boolean')
    irrigated = irrigated.mask(irr == 1, True)
    irrigated = irrigated.mask(irr == 2, False)

    # Join parcel attributes onto field rows (LEFT, on the holder-aware key).
    field = field.merge(parcel_attrs, on=key, how='left')

    hh = field[c['hhid']].apply(format_id)
    plot_id = (field[c['holder_id']].apply(format_id).astype(str) + '_'
               + field[c['parcel_id']].apply(format_id).astype(str) + '_'
               + field[c['field_id']].apply(format_id).astype(str))

    out = pd.DataFrame({
        't':            t,
        'i':            hh.values,
        'plot_id':      plot_id.values,
        'Area':         area_ha.values,
        'AreaUnit':     area_unit.values,
        'Tenure':       field['_Tenure'].astype('string').values,
        'TenureSystem': pd.Series(pd.NA, index=field.index, dtype='string').values,
        'SoilType':     field['_SoilType'].astype('string').values,
        'Irrigated':    irrigated.values,
    })
    out = out.dropna(subset=['i', 'plot_id'])
    out = out.set_index(['t', 'i', 'plot_id'])
    return out


# food_coping (CSI / rCSI coping-strategies battery; GH #332, Family B).
#
# ESS Section 7 ("Food Security"), household-level file sect7_hh_w{1,2,3}.
# Question hh_s7q02_{a..h} = "In the past 7 days, how many days have you or
# someone in your HH had to: [strategy]?" (integer 0-7, "IF NO DAYS, RECORD
# ZERO").  This is the 8-item Coping Strategies Index, which embeds the 5
# reduced-CSI (rCSI) strategies (LessPreferred, BorrowFood, LimitPortion,
# RestrictAdults, ReduceMeals) plus three survey-specific items.  Item
# wording was confirmed verbatim from the ESS3 HH Questionnaire (Section 7,
# Q2, columns A-H); the W1/W2 .dta labels are byte-identical (Stata
# truncates them at 80 chars but the prefixes match exactly).
#
# Wave wiring: i is the wave-native household id matching household_roster /
# sample (household_id for W1; household_id2 for W2/W3).  The cross-wave
# id_walk to panel-canonical ids is applied once at the country level.
FOOD_COPING_STRATEGIES = {
    'hh_s7q02_a': 'LessPreferred',   # Rely on less preferred foods? (rCSI)
    'hh_s7q02_b': 'LimitVariety',    # Limit the variety of foods eaten?
    'hh_s7q02_c': 'LimitPortion',    # Limit portion size at mealtimes? (rCSI)
    'hh_s7q02_d': 'ReduceMeals',     # Reduce number of meals eaten in a day? (rCSI)
    'hh_s7q02_e': 'RestrictAdults',  # Restrict consumption by adults for small children to eat? (rCSI)
    'hh_s7q02_f': 'BorrowFood',      # Borrow food, or rely on help from a friend/relative? (rCSI)
    'hh_s7q02_g': 'NoFood',          # Have no food of any kind in your household?
    'hh_s7q02_h': 'WholeDay',        # Go a whole day and night without eating anything?
}


def food_coping_for_wave(t, df, hhid):
    """Reshape the ESS Section 7 CSI day-count battery to long form.

    Parameters
    ----------
    t : str
        Wave label (e.g. '2011-12').
    df : pd.DataFrame
        Raw sect7_hh_w*.dta frame.
    hhid : str
        Source household-id column matching the wave's roster/sample i
        ('household_id' for W1, 'household_id2' for W2/W3).

    Returns
    -------
    pd.DataFrame
        Index (t, i, Strategy); column ``Days`` (Int64, 0-7).  One row per
        (household, coping strategy).  Rows with a missing Days value are
        dropped (the strategy was not answered for that household).
    """
    cols = list(FOOD_COPING_STRATEGIES)
    out = df[[hhid] + cols].copy()
    out['i'] = out[hhid].apply(format_id)
    out = out.drop(columns=[hhid])

    long = out.melt(id_vars='i', value_vars=cols,
                    var_name='_var', value_name='Days')
    long['Strategy'] = long['_var'].map(FOOD_COPING_STRATEGIES)
    long = long.drop(columns='_var')
    long['t'] = t

    long['Days'] = pd.to_numeric(long['Days'], errors='coerce')
    # Valid domain is 0-7 days within the 7-day recall window.  ESS W3 has
    # one data-entry outlier (LimitPortion = 8); drop out-of-domain values.
    long.loc[(long['Days'] < 0) | (long['Days'] > 7), 'Days'] = pd.NA
    long['Days'] = long['Days'].astype('Int64')
    long = long.dropna(subset=['i', 'Days'])

    long = long.set_index(['t', 'i', 'Strategy']).sort_index()
    return long


# crop_production (GAP 1 — item-level harvest at (t, i, plot, crop))
# ---------------------------------------------------------------------
#
# ESS post-harvest §9 (sect9_ph) records ONE ROW PER (plot, crop): the
# crop the farmer grew on a field, the REPORTED harvest quantity, and the
# native reporting unit.  This is the clean item-level source.  We emit
# exactly those reported fields — NO unit->kg conversion, NO yield, NO
# main-crop / value-share rollups (those are transformations, per the
# parity-loop hard rule).
#
# Grain / IDs: plot_id == format_id(holder_id)_format_id(parcel_id)_
# format_id(field_id) and i == format_id(household_id[2]), IDENTICAL to
# plot_features (so crop_production joins plot_features on (t, i, plot)).
#
# Columns (all REPORTED, item-level):
#   Quantity        reported harvest quantity (native unit)
#   u               native harvest unit (Preferred Label via the `u` table)
#   Quantity_sold   reported quantity sold (sect11/§11 sale module)
#   Value_sold      reported sale value (Birr)
#   planting_month  Gregorian planting month 1-12 (sect4_pp / §4)
#   harvest_month   Gregorian harvest-END month 1-12 (§9)
#   intercropped    field was a mixed stand (bool)
#   perennial       crop is a perennial/tree crop (bool, crop-code property)
#
# crop labels: code-keyed `harmonize_crop` table whose Preferred Labels
# REUSE the food labels (Maize, Sorghum, Teff, Wheat, Barley, Rice, ...)
# so crop_production.j joins food_acquired.j; non-food crops (Cotton,
# Enset, Gesho, Chat, Coffee, ...) get their own Preferred Labels.
#
# SALES GRAIN CAVEAT: the §11 sale module is keyed (household, holder,
# crop) — NOT (plot, crop).  A holder who grows one crop on several plots
# reports a single sale figure that cannot be split across plots without
# fabricating an allocation.  We therefore attach Quantity_sold /
# Value_sold ONLY where the (holder, crop) maps to exactly ONE plot-crop
# harvest row (unambiguous).  Where the crop is grown on multiple plots
# the sale columns stay NaN — we never duplicate or arbitrarily split a
# reported value.  (Cross-wave aggregate harvest_sold lives in
# transformations, not here.)

# WB perennial/tree crop-code list (ETH_ESS1.do:415 PERENNIAL/FRUIT set).
PERENNIAL_CROP_CODES = frozenset({
    19, 20, 22, 34, 35, 37, 38, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50,
    55, 65, 66, 71, 72, 74, 75, 76, 81, 82, 84, 85, 86, 98, 99, 108, 111,
    112, 113, 114, 115, 116, 117, 122,
})


def _eth_crop_label_map():
    """Load the code-keyed harmonize_crop table -> {crop_code: Preferred Label}."""
    return _harmonize_code_label('harmonize_crop')


def _clean_unit_label(series):
    """Normalize a §9/§11 harvest unit label to a canonical `u` value.

    Strips the survey 'NNN. ' code prefix, title-cases, and canonicalizes
    the harvest module's parenthesized size suffix ``(Small)/(Medium)/
    (Large)`` to the `` Small/ Medium/ Large`` form the `u` table (and the
    food_acquired units) already use -- so harvest units resolve through
    the SAME `u` table the food units use rather than fragmenting the unit
    axis with a parallel parenthesized scheme.  Also folds a handful of
    known synonyms / Stata truncations onto their `u`-table label."""
    s = series.astype('string')
    s = s.str.replace(r'^\s*\d+\.\s*', '', regex=True).str.strip()
    # Drop the U+FFFD replacement-char mojibake ESS W5 embeds in a few
    # labels (e.g. "Zorba/Akara � Large").
    s = s.str.replace('�', '', regex=False)
    s = s.str.title()
    s = s.str.replace(r'\s+', ' ', regex=True)
    # Normalize the recurring "Meduim" misspelling BEFORE size matching.
    s = s.str.replace('Meduim', 'Medium', regex=False)
    # Parenthesized size suffix "(Small)" -> " Small" (tolerating the
    # Stata-truncated "(Smal"/"(Medi"/"(Larg" and bare "(Mediu" forms).
    s = s.str.replace(r'\s*\(\s*Smal[l]?\s*\)?\s*$', ' Small', regex=True)
    s = s.str.replace(r'\s*\(\s*Med(?:i|iu|ium)?\s*\)?\s*$', ' Medium', regex=True)
    s = s.str.replace(r'\s*\(\s*Larg[e]?\s*\)?\s*$', ' Large', regex=True)
    s = s.str.replace(r'\s+', ' ', regex=True).str.strip()
    s = s.str.replace('Joniya Kysha', 'Joniya/Kasha', regex=False)
    syn = {
        'Kilogram': 'Kg', 'Kuintal': 'Quintal', 'Quntal': 'Quintal',
        'Kurbet': 'Quintal',  # ESS W2 alias of Kuintal/Quintal
        'Meduim': 'Medium',
        'Kubaya Small': 'Kubaya/Cup Small',
        'Kubaya Medium': 'Kubaya/Cup Medium',
        'Kubaya Large': 'Kubaya/Cup Large',
        'Akumada(Dawela) Lekota Small': 'Akumada/Dawla/Lekota Small',
        'Akumada(Dawela) Lekota Large': 'Akumada/Dawla/Lekota Large',
        'Madaberiya/Nuse/Shera/Chiret Small': 'Madaberia/Nuse/Shera/Cheret Small',
        'Madaberiya/Nuse/Shera/Chiret Medium': 'Madaberia/Nuse/Shera/Cheret Medium',
        'Madaberiya/Nuse/Shera/Chiret Large': 'Madaberia/Nuse/Shera/Cheret Large',
        'Others(Specify)': 'Other (Specify)',
        'Other(Specify)': 'Other (Specify)',
    }
    s = s.replace(syn)
    return s.replace({'': pd.NA, '0': pd.NA})


# Ethiopian-calendar month code -> Gregorian month number (1-12).  ESS
# records the local-calendar month; this deterministic relabel (the same
# recode the WB do-files apply) is a label normalization, not an
# aggregate.  Codes 1/13 -> September (9), 2 -> October, ... 12 -> August.
_ETH_MONTH_TO_GREG = {1: 9, 13: 9, 2: 10, 3: 11, 4: 12, 5: 1, 6: 2,
                      7: 3, 8: 4, 9: 5, 10: 6, 11: 7, 12: 8}


def _greg_month(series):
    """Recode the ESS local-calendar month code -> Gregorian month (1-12)."""
    n = pd.to_numeric(series, errors='coerce').astype('Int64')
    return n.map(_ETH_MONTH_TO_GREG).astype('Int64')


def crop_production_for_wave(t, harvest, planting, sale, colmap,
                             intercrop=None, unit_labels=None,
                             sale_unit_labels=None):
    """Build canonical ``crop_production`` for one Ethiopia ESS wave.

    Parameters
    ----------
    t : str
        Wave id (e.g. ``"2011-12"``), the ``t`` index value.
    harvest : pd.DataFrame
        Raw §9 post-harvest crop file (sect9_ph_w{N}.dta), loaded with
        ``convert_categoricals=False`` (codes; unit/crop label decode is
        done in-script against the harmonize tables).
    planting : pd.DataFrame or None
        Raw §4 post-planting crop file (sect4_pp_w{N}.dta) for the
        planting-month join, or None if unavailable.
    sale : pd.DataFrame or None
        Raw §11 post-harvest sale file (sect11_ph_w{N}.dta) for the
        sold-quantity / sold-value join, or None.
    colmap : dict
        Per-wave column map.  Required keys:
            hhid, holder_id, parcel_id, field_id  — id / plot_id columns
            crop_code                              — §9 crop code column
            quantity                               — §9 reported harvest qty
        Optional keys (NaN where omitted):
            unit            — §9 native harvest-unit code col (None => Kg,
                              W1 reports kilos directly)
            harvest_month   — §9 harvest-END month code col
            intercrop       — §9 pure/mixed-stand col (1=pure, 2=mixed)
            pl_crop_code, pl_month  — §4 crop-code + planting-month cols
            s_crop_code, s_sold_flag, s_qty, s_value, s_unit  — §11 cols
                              (s_unit None => Kg, W1 sales report kilos)
    intercrop : pd.DataFrame or None
        Optional plot-roster (sect3_pp) carrying a plot-level mixed-stand
        flag, used for W1 (which has no §9 stand variable).  When given,
        colmap must carry 'ic_holder_id'/'ic_parcel_id'/'ic_field_id'/
        'ic_flag' (flag: 1=No, 2=Yes per the WB recode).

    Returns
    -------
    pd.DataFrame indexed by ``(t, i, plot_id, j, u)`` with columns
        Quantity (float), Quantity_sold (float), Value_sold (float),
        planting_month (Int64), harvest_month (Int64),
        intercropped (boolean), perennial (boolean).
    """
    c = colmap
    crop_map = _eth_crop_label_map()

    h = harvest.copy()
    # plot_id identical to plot_features.
    plot_id = (h[c['holder_id']].apply(format_id).astype(str) + '_'
               + h[c['parcel_id']].apply(format_id).astype(str) + '_'
               + h[c['field_id']].apply(format_id).astype(str))
    code = pd.to_numeric(h[c['crop_code']], errors='coerce').astype('Int64')

    out = pd.DataFrame({
        't':       t,
        'i':       h[c['hhid']].apply(format_id).values,
        'plot_id': plot_id.values,
        '_holder': h[c['holder_id']].apply(format_id).values,
        '_code':   code.values,
        'j':       code.map(crop_map).astype('string').values,
        'Quantity': pd.to_numeric(h[c['quantity']], errors='coerce').astype('Float64').values,
    })

    # Native harvest unit (Preferred Label).  W1 reports kilos directly.
    if unit_labels is not None:
        # ``unit_labels`` is the §9 unit column DECODED to its survey label
        # (caller loads that one column with convert_categoricals=True),
        # aligned row-for-row with ``harvest``.  Normalize through the same
        # pipeline food_acquired uses so it resolves via the shared `u`
        # table.
        out['u'] = _clean_unit_label(
            pd.Series(unit_labels.values, index=h.index)).values
    else:
        # W1 reports kilograms directly (no unit code).
        out['u'] = 'Kg'

    out['harvest_month'] = (_greg_month(h[c['harvest_month']]).values
                            if c.get('harvest_month') and c['harvest_month'] in h.columns
                            else pd.NA)

    # intercropped: §9 mixed-stand (1=pure -> False, 2=mixed -> True), or
    # plot-roster fallback for W1.
    if c.get('intercrop') and c['intercrop'] in h.columns:
        ic = pd.to_numeric(h[c['intercrop']], errors='coerce').astype('Int64')
        icbool = pd.Series(pd.NA, index=h.index, dtype='boolean')
        icbool = icbool.mask(ic == 1, False).mask(ic == 2, True)
        out['intercropped'] = icbool.values
    elif intercrop is not None and c.get('ic_flag'):
        ic_pid = (intercrop[c['ic_holder_id']].apply(format_id).astype(str) + '_'
                  + intercrop[c['ic_parcel_id']].apply(format_id).astype(str) + '_'
                  + intercrop[c['ic_field_id']].apply(format_id).astype(str))
        flag = pd.to_numeric(intercrop[c['ic_flag']], errors='coerce').astype('Int64')
        icb = pd.Series(pd.NA, index=intercrop.index, dtype='boolean')
        icb = icb.mask(flag == 1, False).mask(flag == 2, True)
        ic_df = pd.DataFrame({'plot_id': ic_pid.values, 'intercropped': icb.values}) \
            .dropna(subset=['plot_id']).drop_duplicates('plot_id')
        out = out.merge(ic_df, on='plot_id', how='left')
    else:
        out['intercropped'] = pd.Series(pd.NA, index=out.index, dtype='boolean')

    # perennial: crop-code property.
    out['perennial'] = out['_code'].map(
        lambda x: (int(x) in PERENNIAL_CROP_CODES) if pd.notna(x) else pd.NA
    ).astype('boolean')

    # planting_month from §4, joined on (plot_id, crop_code).
    if planting is not None and c.get('pl_month'):
        p_pid = (planting[c['holder_id']].apply(format_id).astype(str) + '_'
                 + planting[c['parcel_id']].apply(format_id).astype(str) + '_'
                 + planting[c['field_id']].apply(format_id).astype(str))
        p_code = pd.to_numeric(planting[c['pl_crop_code']], errors='coerce').astype('Int64')
        pm = pd.DataFrame({
            'plot_id': p_pid.values,
            '_code':   p_code.values,
            'planting_month': _greg_month(planting[c['pl_month']]).values,
        }).dropna(subset=['plot_id', '_code']).drop_duplicates(['plot_id', '_code'])
        out = out.merge(pm, on=['plot_id', '_code'], how='left')
    else:
        out['planting_month'] = pd.Series(pd.NA, index=out.index, dtype='Int64')

    # Sales: §11 is (household, holder, crop) grain.  Attach reported
    # sold-qty / sold-value ONLY where (holder, crop) maps to exactly one
    # plot-crop harvest row (see SALES GRAIN CAVEAT above).
    #
    # Value_sold (Birr) is unit-free and always attachable.  Quantity_sold
    # carries the SALE unit, which need not equal the harvest `u`; since we
    # never convert units, Quantity_sold is attached only on rows whose
    # harvest `u` equals the (single) sale unit reported for that
    # (holder, crop).  Where they differ it stays NaN (no cross-unit fib).
    out['Quantity_sold'] = pd.Series(pd.NA, index=out.index, dtype='Float64')
    out['Value_sold'] = pd.Series(pd.NA, index=out.index, dtype='Float64')
    if sale is not None and c.get('s_qty'):
        s = sale.copy()
        s_code = pd.to_numeric(s[c['s_crop_code']], errors='coerce').astype('Int64')
        s_holder = s[c['holder_id']].apply(format_id)
        sold_flag = (pd.to_numeric(s[c['s_sold_flag']], errors='coerce').astype('Int64')
                     if c.get('s_sold_flag') and c['s_sold_flag'] in s.columns else None)
        qty = pd.to_numeric(s[c['s_qty']], errors='coerce')
        val = (pd.to_numeric(s[c['s_value']], errors='coerce')
               if c.get('s_value') and c['s_value'] in s.columns else pd.NA)
        # sale unit label: pre-decoded labels (s_unit_labels) where the
        # sale unit is a coded column, else 'Kg' (W1/W2 sale qty is kilos).
        if sale_unit_labels is not None:
            s_unit = _clean_unit_label(
                pd.Series(sale_unit_labels.values, index=s.index)).fillna('Kg')
        else:
            s_unit = pd.Series('Kg', index=s.index)
        sdf = pd.DataFrame({
            '_holder': s_holder.values,
            '_code':   s_code.values,
            '_sunit':  s_unit.values,
            'Quantity_sold': qty.values,
            'Value_sold': val if np.isscalar(val) else val.values,
        })
        if sold_flag is not None:
            # sold flag 2 == No -> zero the reported sale.
            no = (sold_flag == 2).values
            sdf.loc[no, ['Quantity_sold', 'Value_sold']] = 0.0
        sdf = sdf.dropna(subset=['_holder', '_code'])
        # Aggregate to (holder, crop): sum Value_sold (unit-free); for
        # Quantity_sold sum WITHIN each sale unit and keep the per-unit
        # breakdown so we can match it against the harvest `u`.
        vagg = sdf.groupby(['_holder', '_code'], as_index=False)['Value_sold'].sum()
        qagg = sdf.groupby(['_holder', '_code', '_sunit'], as_index=False)['Quantity_sold'].sum()
        # Count plot-crop harvest rows per (holder, crop); attach only when 1.
        nplots = (out.dropna(subset=['_code'])
                     .groupby(['_holder', '_code'])['plot_id'].nunique()
                     .rename('_nplots').reset_index())
        keep = nplots[nplots['_nplots'] == 1][['_holder', '_code']]
        vagg = vagg.merge(keep, on=['_holder', '_code'], how='inner')
        qagg = qagg.merge(keep, on=['_holder', '_code'], how='inner')
        out = out.drop(columns=['Quantity_sold', 'Value_sold'])
        out = out.merge(vagg, on=['_holder', '_code'], how='left')
        # Quantity_sold joins additionally on the unit (harvest u == sale unit).
        out = out.merge(
            qagg.rename(columns={'_sunit': 'u'}),
            on=['_holder', '_code', 'u'], how='left')

    out = out.drop(columns=['_holder', '_code'])
    out = out.dropna(subset=['i', 'plot_id', 'j'])
    # `u` is an index level (cannot be null).  A row may legitimately
    # report a quantity with an unrecorded unit -> tag it 'Other (Specify)'
    # (a value already in the `u` table); rows with neither a quantity nor
    # a recorded unit are empty placeholders -> drop.
    empty_u = out['u'].isna()
    out = out[~(empty_u & out['Quantity'].isna()
                & out['Quantity_sold'].isna())]
    out['u'] = out['u'].fillna('Other (Specify)')
    out = out.set_index(['t', 'i', 'plot_id', 'j', 'u'])
    # Stable column order.
    out = out[['Quantity', 'Quantity_sold', 'Value_sold',
               'planting_month', 'harvest_month', 'intercropped', 'perennial']]
    return out


# plot_inputs (GAP 2 — item-level ag inputs at (t, i, plot, input, j))
# ---------------------------------------------------------------------
#
# ESS records the inputs applied to a plot in the post-planting (pp) round
# across three sections, at THREE DIFFERENT GRAINS:
#
#   §3 plot roster (sect3_pp)  : ONE ROW PER FIELD.  Inorganic-fertilizer
#       use + reported kg per type (Urea / DAP / NPS), and organic-fertilizer
#       use dummies (Manure / Compost / Other Organic).  Fertilizer is a
#       PLOT-LEVEL input (no crop) -> these rows carry j = WHOLE_PLOT_SENTINEL.
#   §4 planting (sect4_pp)     : ONE ROW PER (FIELD, CROP).  Seed quantity
#       (kg) + the improved-seed flag (W2-W5; for W1 the seed quantity lives
#       in §5).  Pesticide use (a plot-crop dummy).  These are CROP-specific
#       -> j = the crop Preferred Label.
#   §5 seed (sect5_pp)         : seed PURCHASE detail (purchased y/n, purchased
#       qty kg, and, for W1, the seed quantity itself).  W1's §5 is at
#       (field, crop) grain; W2-W5's §5 is at (HOLDER, crop) grain (no
#       field_id), so its purchased columns attach to the §4 plot-crop seed
#       rows ONLY where the (holder, crop) maps to exactly ONE plot
#       (the same SALES GRAIN CAVEAT crop_production uses for §11 sales).
#
# We emit ONE ROW PER (plot, input[, crop]) carrying only REPORTED,
# item-level fields:
#   input               input identity (Preferred Label via harmonize_input)
#   Quantity            reported applied quantity (native unit u; kg here)
#   u                   native unit ('Kg' for every ESS input quantity)
#   Purchased           was the input purchased (nullable bool; seed rows)
#   Quantity_purchased  reported purchased quantity (kg; seed rows)
#   Improved            improved-seed flag (nullable bool; seed rows only)
#
# NO unit->kg re-conversion (the source already reports kg), NO nitrogen_kg
# / seed_kg sums, NO any-use roll-up flags, NO fertilizer totals -- those are
# transformations over these item rows (GAP 2 / GAP 7), not stored columns.
#
# Grain / IDs: plot_id == format_id(holder_id)_format_id(parcel_id)_
# format_id(field_id) and i == format_id(household_id[2]), IDENTICAL to
# plot_features / crop_production (so plot_inputs joins them on (t, i, plot)).
# crop labels reuse harmonize_crop (== the food Preferred Labels) so seed/
# pesticide rows' j joins crop_production.j and food_acquired.j.
#
# WHOLE_PLOT_SENTINEL: a plot-level (non-crop) input cannot carry a real
# crop label, but `j` is an index level (no nulls allowed; crop_production
# fills its `u` level the same way).  We tag those rows with a reserved,
# clearly-non-crop label so the index stays clean and the crop axis is never
# silently corrupted; downstream code selects crop-specific inputs with
# `j != 'Whole Plot'`.
WHOLE_PLOT_SENTINEL = 'Whole Plot'

# Reported nitrogen-fertilizer types whose kg is recorded per plot in §3.
# (N-content factors live in transformations, NEVER here.)
PLOT_INPUT_FERTILIZERS = ('Urea', 'DAP', 'NPS')
PLOT_INPUT_ORGANICS = ('Manure', 'Compost', 'Other Organic')


def _eth_input_label_map():
    """Load the code-keyed harmonize_input table -> {code: Preferred Label}.

    The table documents the shared ESS input taxonomy (Seed / Urea / DAP /
    NPS / Manure / Compost / Other Organic / Pesticide).  Labels are emitted
    directly in-script (as crop_production does for crops), so the table is
    the authoritative label source and a cross-country reference -- not a
    runtime auto-mapping (the index level is named ``input``, the table
    ``harmonize_input``, so the name-match auto-mapper does not fire)."""
    return _harmonize_code_label('harmonize_input')


def _yesno_bool(series, yes=1, no=2):
    """Recode an ESS 1=Yes / 2=No code Series to nullable boolean."""
    n = pd.to_numeric(series, errors='coerce').astype('Int64')
    out = pd.Series(pd.NA, index=n.index, dtype='boolean')
    return out.mask(n == yes, True).mask(n == no, False)


def _plot_id_from(df, c, holder='holder_id', parcel='parcel_id', field='field_id'):
    """holder_parcel_field plot_id, identical to plot_features/crop_production."""
    return (df[c[holder]].apply(format_id).astype(str) + '_'
            + df[c[parcel]].apply(format_id).astype(str) + '_'
            + df[c[field]].apply(format_id).astype(str))


def plot_inputs_for_wave(t, plot_roster, planting, seeds, colmap, labels=None):
    """Build canonical ``plot_inputs`` for one Ethiopia ESS wave.

    Parameters
    ----------
    t : str
        Wave id (e.g. ``"2011-12"``), the ``t`` index value.
    plot_roster : pd.DataFrame or None
        Raw §3 plot roster (sect3_pp_w{N}.dta), loaded with
        ``convert_categoricals=False``.  Source of the per-plot
        fertilizer (Urea/DAP/NPS kg) and organic-fertilizer dummies.
    planting : pd.DataFrame or None
        Raw §4 planting crop file (sect4_pp_w{N}.dta), source of the
        plot-crop seed quantity (W2-W5), the improved-seed flag, and the
        pesticide-use dummy.
    seeds : pd.DataFrame or None
        Raw §5 seed file (sect5_pp_w{N}.dta), source of seed PURCHASE
        detail (and, for W1, the seed quantity itself).
    colmap : dict
        Per-wave column map.  Recognised keys (all optional; a section is
        skipped when its keys are absent / the frame is None):
            hhid, holder_id, parcel_id, field_id   — id / plot_id columns

          § seed (the file carrying the plot-crop seed quantity):
            seed_src ∈ {'seeds', 'planting'}        — which frame holds qty
            seed_crop_code                          — crop code in seed_src
            seed_qty / seed_qty_a / seed_qty_b      — qty col, or kg+gram cols
            improved_code / improved_yes            — improved flag + yes-codes

          § seed purchase (always from §5):
            p_holder_id, p_crop_code                — §5 grain keys
            p_field_id, p_parcel_id                 — present only for W1 §5
            p_purch_flag                            — purchased y/n (2=No)
            p_purch_a / p_purch_b / p_purch_qty     — purchased kg (+gram)

          § fertilizer (§3 plot roster):
            urea_used, urea_kg, dap_used, dap_kg,
            nps_used, nps_kg                         — per-type use + kg
            urea_purch_kg, dap_purch_kg, nps_purch_kg — purchased kg (W2+)

          § organic (§3 plot roster):
            manure_used, compost_used, other_organic_used

          § pesticide (§4 planting):
            pest_crop_code, pest_used, pest_gate     — used + gate (2 zeros it)
    labels : dict or None
        Optional {input_code: Preferred Label} override; defaults to the
        harmonize_input table.

    Returns
    -------
    pd.DataFrame indexed by ``(t, i, plot_id, input, j)`` with columns
        Quantity (float), u (str), Purchased (boolean),
        Quantity_purchased (float), Improved (boolean).
    """
    c = colmap
    crop_map = _eth_crop_label_map()
    pieces = []

    # ---- SEED (plot-crop) -------------------------------------------
    seed_src = c.get('seed_src')
    src = {'seeds': seeds, 'planting': planting}.get(seed_src)
    if src is not None and c.get('seed_crop_code'):
        s = src.copy()
        # Optional seed-block gate (W4/W5 drop s4q15==1 immature/cut rows).
        if c.get('seed_drop_col') and c['seed_drop_col'] in s.columns:
            gate = pd.to_numeric(s[c['seed_drop_col']], errors='coerce')
            s = s[gate != c.get('seed_drop_val', 1)].copy()
        pid = _plot_id_from(s, c)
        code = pd.to_numeric(s[c['seed_crop_code']], errors='coerce').astype('Int64')
        # seed quantity (kg): either a single col or kg + gram pair.
        if c.get('seed_qty') and c['seed_qty'] in s.columns:
            qty = pd.to_numeric(s[c['seed_qty']], errors='coerce').astype('Float64')
        elif c.get('seed_qty_a') and c['seed_qty_a'] in s.columns:
            a = pd.to_numeric(s[c['seed_qty_a']], errors='coerce')
            g = (pd.to_numeric(s[c['seed_qty_b']], errors='coerce') * 0.001
                 if c.get('seed_qty_b') and c['seed_qty_b'] in s.columns
                 else pd.Series(0.0, index=s.index))
            qty = a.add(g, fill_value=0).astype('Float64').where(
                a.notna() | (g != 0), pd.NA)
        else:
            qty = pd.Series(pd.NA, index=s.index, dtype='Float64')
        qty = qty.mask(qty < 0, pd.NA)
        # improved flag.
        if c.get('improved_code') and c['improved_code'] in s.columns:
            ic = pd.to_numeric(s[c['improved_code']], errors='coerce').astype('Int64')
            yes = set(c.get('improved_yes', (2,)))
            improved = pd.Series(pd.NA, index=s.index, dtype='boolean')
            improved = improved.mask(ic.isin(list(yes)), True)
            improved = improved.mask(ic.notna() & ~ic.isin(list(yes)), False)
        else:
            improved = pd.Series(pd.NA, index=s.index, dtype='boolean')

        seed_df = pd.DataFrame({
            'i':        s[c['hhid']].apply(format_id).values,
            'plot_id':  pid.values,
            '_holder':  s[c['holder_id']].apply(format_id).values,
            '_code':    code.values,
            'j':        code.map(crop_map).astype('string').values,
            'input':    'Seed',
            'Quantity': qty.values,
            'u':        'Kg',
            'Improved': improved.values,
            'Applied':  pd.Series(pd.NA, index=s.index, dtype='boolean').values,
        })

        # Improved flag may instead live in the §4 planting file (W1: §5 has
        # no improved column).  Join it on (plot_id, crop) where requested.
        if (c.get('improved_join') and planting is not None
                and c.get('improved_code') and c['improved_code'] in planting.columns):
            ipl_pid = _plot_id_from(planting, c)
            ipl_code = pd.to_numeric(planting[c['pl_crop_code']], errors='coerce').astype('Int64') \
                if c.get('pl_crop_code') and c['pl_crop_code'] in planting.columns \
                else pd.to_numeric(planting.get(c['seed_crop_code']), errors='coerce').astype('Int64')
            iic = pd.to_numeric(planting[c['improved_code']], errors='coerce').astype('Int64')
            yes = set(c.get('improved_yes', (2,)))
            ibool = pd.Series(pd.NA, index=planting.index, dtype='boolean')
            ibool = ibool.mask(iic.isin(list(yes)), True)
            ibool = ibool.mask(iic.notna() & ~iic.isin(list(yes)), False)
            idf = pd.DataFrame({'plot_id': ipl_pid.values, '_code': ipl_code.values,
                                '_imp': ibool.values}) \
                .dropna(subset=['plot_id', '_code']).drop_duplicates(['plot_id', '_code'])
            seed_df = seed_df.merge(idf, on=['plot_id', '_code'], how='left')
            seed_df['Improved'] = seed_df.pop('_imp').astype('boolean')

        # Seed PURCHASE detail from §5 (purchased y/n, purchased kg).
        seed_df['Purchased'] = pd.Series(pd.NA, index=seed_df.index, dtype='boolean')
        seed_df['Quantity_purchased'] = pd.Series(pd.NA, index=seed_df.index, dtype='Float64')
        if seeds is not None and c.get('p_purch_flag'):
            ps = seeds.copy()
            p_holder = ps[c['p_holder_id']].apply(format_id)
            p_code = pd.to_numeric(ps[c['p_crop_code']], errors='coerce').astype('Int64')
            purch = _yesno_bool(ps[c['p_purch_flag']])
            if c.get('p_purch_qty') and c['p_purch_qty'] in ps.columns:
                pqty = pd.to_numeric(ps[c['p_purch_qty']], errors='coerce').astype('Float64')
            elif c.get('p_purch_a') and c['p_purch_a'] in ps.columns:
                pa = pd.to_numeric(ps[c['p_purch_a']], errors='coerce')
                pg = (pd.to_numeric(ps[c['p_purch_b']], errors='coerce') * 0.001
                      if c.get('p_purch_b') and c['p_purch_b'] in ps.columns
                      else pd.Series(0.0, index=ps.index))
                pqty = pa.add(pg, fill_value=0).astype('Float64').where(
                    pa.notna() | (pg != 0), pd.NA)
            else:
                pqty = pd.Series(pd.NA, index=ps.index, dtype='Float64')
            # purchased flag No -> 0 purchased quantity (mirrors the .do).
            pqty = pqty.mask(purch == False, 0.0)

            if c.get('p_field_id') and c['p_field_id'] in ps.columns:
                # W1: §5 carries field_id -> direct plot-crop join.
                p_pid = _plot_id_from(ps, c, parcel=c.get('p_parcel_id', 'parcel_id'),
                                      field=c['p_field_id'])
                pdf = pd.DataFrame({
                    'plot_id': p_pid.values, '_code': p_code.values,
                    'Purchased': purch.values, 'Quantity_purchased': pqty.values,
                }).dropna(subset=['plot_id', '_code']).drop_duplicates(['plot_id', '_code'])
                seed_df = seed_df.drop(columns=['Purchased', 'Quantity_purchased'])
                seed_df = seed_df.merge(pdf, on=['plot_id', '_code'], how='left')
            else:
                # W2-W5: §5 is (holder, crop) grain -> attach only where the
                # (holder, crop) maps to exactly ONE plot (the SALES GRAIN
                # CAVEAT; never split a holder figure across plots).
                pagg = pd.DataFrame({
                    '_holder': p_holder.values, '_code': p_code.values,
                    '_purch_any': (purch == True).values,
                    'Quantity_purchased': pqty.values,
                })
                pagg = pagg.dropna(subset=['_holder', '_code'])
                pa2 = pagg.groupby(['_holder', '_code'], as_index=False).agg(
                    _purch_any=('_purch_any', 'max'),
                    Quantity_purchased=('Quantity_purchased', 'sum'),
                )
                nplots = (seed_df.dropna(subset=['_code'])
                          .groupby(['_holder', '_code'])['plot_id'].nunique()
                          .rename('_n').reset_index())
                keep = nplots[nplots['_n'] == 1][['_holder', '_code']]
                pa2 = pa2.merge(keep, on=['_holder', '_code'], how='inner')
                seed_df = seed_df.drop(columns=['Purchased', 'Quantity_purchased'])
                seed_df = seed_df.merge(pa2, on=['_holder', '_code'], how='left')
                seed_df['Purchased'] = seed_df.pop('_purch_any').astype('boolean')

        seed_df = seed_df.drop(columns=['_holder', '_code'])
        # Keep a seed row only if it carries a crop and SOME reported value.
        seed_df = seed_df.dropna(subset=['j'])
        has_val = (seed_df['Quantity'].notna() | seed_df['Improved'].notna()
                   | seed_df['Purchased'].notna() | seed_df['Quantity_purchased'].notna())
        seed_df = seed_df[has_val]
        pieces.append(seed_df)

    # ---- FERTILIZER + ORGANIC (plot-level, §3) ----------------------
    if plot_roster is not None:
        pr = plot_roster.copy()
        pid = _plot_id_from(pr, c)
        hh = pr[c['hhid']].apply(format_id)
        fert_specs = [
            ('Urea',          c.get('urea_used'), c.get('urea_kg'), c.get('urea_purch_kg')),
            ('DAP',           c.get('dap_used'),  c.get('dap_kg'),  c.get('dap_purch_kg')),
            ('NPS',           c.get('nps_used'),  c.get('nps_kg'),  c.get('nps_purch_kg')),
        ]
        for name, used_col, kg_col, purch_col in fert_specs:
            if not (kg_col and kg_col in pr.columns):
                continue
            used = _yesno_bool(pr[used_col]) if (used_col and used_col in pr.columns) else None
            kg = pd.to_numeric(pr[kg_col], errors='coerce').astype('Float64')
            pqty = (pd.to_numeric(pr[purch_col], errors='coerce').astype('Float64')
                    if purch_col and purch_col in pr.columns
                    else pd.Series(pd.NA, index=pr.index, dtype='Float64'))
            fert = pd.DataFrame({
                'i': hh.values, 'plot_id': pid.values,
                'j': WHOLE_PLOT_SENTINEL, 'input': name,
                'Quantity': kg.values, 'u': 'Kg',
                'Purchased': pd.Series(pd.NA, index=pr.index, dtype='boolean').values,
                'Quantity_purchased': pqty.values,
                'Improved': pd.Series(pd.NA, index=pr.index, dtype='boolean').values,
                'Applied': (used.values if used is not None
                            else pd.Series(pd.NA, index=pr.index, dtype='boolean').values),
                '_used': (used.values if used is not None
                          else pd.Series(pd.NA, index=pr.index, dtype='boolean').values),
            })
            # ONE ROW PER FERTILIZER ACTUALLY APPLIED to a plot.  Keep a row
            # only where the type was used (used == True) OR a kg / purchased
            # quantity is reported; a plot that answered "did not use" is NOT
            # an applied-input row (no zero-quantity placeholder rows -- the
            # any-use roll-up is a transformation, not stored here).
            keep = ((fert['_used'] == True) | fert['Quantity'].notna()
                    | fert['Quantity_purchased'].notna())
            fert = fert[keep].drop(columns='_used')
            # Drop a reported 0 kg that is really "not applied".
            fert['Quantity'] = fert['Quantity'].mask(fert['Quantity'] == 0, pd.NA)
            pieces.append(fert)

        org_specs = [('Manure', c.get('manure_used')),
                     ('Compost', c.get('compost_used')),
                     ('Other Organic', c.get('other_organic_used'))]
        for name, used_col in org_specs:
            if not (used_col and used_col in pr.columns):
                continue
            used = _yesno_bool(pr[used_col])
            org = pd.DataFrame({
                'i': hh.values, 'plot_id': pid.values,
                'j': WHOLE_PLOT_SENTINEL, 'input': name,
                'Quantity': pd.Series(pd.NA, index=pr.index, dtype='Float64').values,
                'u': pd.Series(pd.NA, index=pr.index, dtype='string').values,
                'Purchased': pd.Series(pd.NA, index=pr.index, dtype='boolean').values,
                'Quantity_purchased': pd.Series(pd.NA, index=pr.index, dtype='Float64').values,
                'Improved': pd.Series(pd.NA, index=pr.index, dtype='boolean').values,
                'Applied': used.values,
            })
            # Organic fertilizer carries no reported quantity in ESS -- the
            # reported datum is the applied dummy, stored in ``Applied``.  Keep
            # only the plots where it was applied (Applied == True); the
            # Quantity / Purchased / Quantity_purchased / Improved fields stay
            # NaN (they are seed-only / fertilizer-purchase fields).
            org = org[org['Applied'] == True]
            org['u'] = pd.NA
            pieces.append(org)

    # ---- PESTICIDE (plot-crop, §4) ----------------------------------
    if planting is not None and c.get('pest_used') and c['pest_used'] in planting.columns:
        pl = planting.copy()
        pid = _plot_id_from(pl, c)
        code = pd.to_numeric(pl[c['pest_crop_code']], errors='coerce').astype('Int64')
        used = _yesno_bool(pl[c['pest_used']])
        if c.get('pest_gate') and c['pest_gate'] in pl.columns:
            gate = pd.to_numeric(pl[c['pest_gate']], errors='coerce').astype('Int64')
            used = used.mask(gate == 2, False)
        pest = pd.DataFrame({
            'i': pl[c['hhid']].apply(format_id).values,
            'plot_id': pid.values,
            'j': code.map(crop_map).astype('string').values,
            'input': 'Pesticide',
            'Quantity': pd.Series(pd.NA, index=pl.index, dtype='Float64').values,
            'u': pd.Series(pd.NA, index=pl.index, dtype='string').values,
            'Purchased': pd.Series(pd.NA, index=pl.index, dtype='boolean').values,
            'Quantity_purchased': pd.Series(pd.NA, index=pl.index, dtype='Float64').values,
            'Improved': pd.Series(pd.NA, index=pl.index, dtype='boolean').values,
            'Applied': used.values,
        })
        pest['u'] = pd.NA
        pest = pest.dropna(subset=['j'])
        # Pesticide carries no reported quantity in ESS -- the reported datum
        # is the applied dummy, stored in ``Applied``.  Keep only the applied
        # plot-crops; collapse duplicate (plot, crop) rows to plot-crop.
        pest = pest[pest['Applied'] == True]
        pest = (pest.groupby(['i', 'plot_id', 'j', 'input'], as_index=False)
                .agg({'Quantity': 'first', 'u': 'first', 'Purchased': 'first',
                      'Quantity_purchased': 'first', 'Improved': 'first',
                      'Applied': 'max'}))
        pieces.append(pest)

    assert pieces, f"plot_inputs {t}: no input sections produced rows"
    out = pd.concat(pieces, ignore_index=True)
    out['t'] = t
    out = out.dropna(subset=['i', 'plot_id', 'input', 'j'])
    out['Quantity'] = out['Quantity'].astype('Float64')
    out['Quantity_purchased'] = out['Quantity_purchased'].astype('Float64')
    out['Purchased'] = out['Purchased'].astype('boolean')
    out['Improved'] = out['Improved'].astype('boolean')
    out['Applied'] = out['Applied'].astype('boolean')
    out['u'] = out['u'].astype('string')
    out = out.set_index(['t', 'i', 'plot_id', 'input', 'j'])
    out = out[['Quantity', 'u', 'Purchased', 'Quantity_purchased',
               'Improved', 'Applied']]
    # Collapse any exact-duplicate index rows (defensive; e.g. a plot that
    # reports the same fertilizer type twice) by taking the first.
    out = out[~out.index.duplicated(keep='first')]
    return out.sort_index()


def _eth_species_label_map(t):
    """Load the wave-keyed harmonize_species -> {code: Preferred Label} for t."""
    wave_keyed = _harmonize_wave_keyed('harmonize_species')
    return {code: lab for (wv, code), lab in wave_keyed.items() if wv == t}


def livestock_for_wave(t, count, txn, colmap):
    """Build canonical ``livestock`` for one Ethiopia ESS §8 wave (GAP 4).

    Item-level livestock at ``(t, i, animal)`` -- one row per
    (household, canonical species).  Carries only the REPORTED fields the
    §8 roster records; anything a wave does not ask is NaN.  This is the
    *pre-collapse* roster the WB code throws away down to a single
    engaged-y/n binary (their =recode pp_saq13 ... collapse(max)=); we keep
    the head counts / transactions instead.

    Parameters
    ----------
    t : str
        Wave id (e.g. ``"2011-12"``), the ``t`` index value AND the wave
        key for the wave-keyed harmonize_species table.
    count : pd.DataFrame
        Raw §8 livestock-roster file carrying the current head count
        (W1/W2 ``sect8a_ls`` -- which also carries the transactions; W3-W5
        ``sect8_1_ls``), loaded with ``convert_categoricals=False`` so the
        animal code column carries the integer codes harmonize_species
        keys on.
    txn : pd.DataFrame or None
        Raw §8 transaction file (W3-W5 ``sect8_2_ls``).  None for W1/W2,
        where the transaction columns live in the ``count`` frame itself.
    colmap : dict
        Per-wave column map.  Required keys:
            hhid        — household id column EMITTED as ``i`` (household_id2
                          for W2/W3, else household_id -- matches sample().i).
            holder_id   — holder id (join key for the count<->txn merge).
            animal_code — animal/species code column in ``count``.
            head_count  — current-head-count column in ``count``.
        Optional keys (NaN where the wave does not ask):
            t_animal_code   — animal code column in ``txn`` (W3-W5; defaults
                              to animal_code).
            t_holder_id     — holder id in ``txn`` (defaults to holder_id).
            head_acquired   — head purchased in last 12 months.
            head_sold       — head sold alive in last 12 months.
            sale_value      — total income from livestock sales (Birr).

    Returns
    -------
    pd.DataFrame indexed by ``(t, i, animal)`` with columns
        HeadCount (float), HeadAcquired (float), HeadSold (float),
        Value (float).  Summed over the raw animal sub-codes that share a
        canonical species within a household (e.g. Bulls+Oxen+Cows ->
        Cattle), which is the within-household roll-up to the (t, i, animal)
        grain -- NOT a WB-style collapse-to-binary.  Rows where the
        household reports neither any head nor any transaction for a
        species are dropped.
    """
    c = colmap
    species_map = _eth_species_label_map(t)
    assert species_map, f'livestock {t}: harmonize_species has no rows for this wave'

    def _num(df, key):
        col = c.get(key)
        if col is None or col not in df.columns:
            return None
        return pd.to_numeric(df[col], errors='coerce').astype('Float64')

    # ---- current head count (from `count`) --------------------------
    cnt = count.copy()
    code = pd.to_numeric(cnt[c['animal_code']], errors='coerce').astype('Int64')
    base = pd.DataFrame({
        'i':        cnt[c['hhid']].apply(format_id).values,
        '_holder':  cnt[c['holder_id']].apply(format_id).values,
        '_code':    code.values,
        'animal':   code.map(species_map).astype('string').values,
        'HeadCount': (_num(cnt, 'head_count') if c.get('head_count') else
                      pd.Series(pd.NA, index=cnt.index, dtype='Float64')).values,
    })

    # ---- transactions (same frame for W1/W2; `txn` for W3-W5) -------
    src = txn if txn is not None else count
    tx = src.copy()
    t_code = pd.to_numeric(tx[c.get('t_animal_code', c['animal_code'])],
                           errors='coerce').astype('Int64')
    txdf = pd.DataFrame({
        '_holder': tx[c.get('t_holder_id', c['holder_id'])].apply(format_id).values,
        '_code':   t_code.values,
        'HeadAcquired': (_num(tx, 'head_acquired') if c.get('head_acquired') else
                         pd.Series(pd.NA, index=tx.index, dtype='Float64')).values,
        'HeadSold': (_num(tx, 'head_sold') if c.get('head_sold') else
                     pd.Series(pd.NA, index=tx.index, dtype='Float64')).values,
        'Value':   (_num(tx, 'sale_value') if c.get('sale_value') else
                    pd.Series(pd.NA, index=tx.index, dtype='Float64')).values,
    })

    if txn is None:
        # W1/W2: count and txn are the same rows -> concatenate columns.
        merged = pd.concat([base, txdf.drop(columns=['_holder', '_code'])], axis=1)
    else:
        # W3-W5: join txn onto the count rows by (holder, raw code).
        merged = base.merge(txdf, on=['_holder', '_code'], how='left')

    merged = merged[merged['animal'].notna()].copy()

    # Within-household roll-up to the (t, i, animal) grain: sum the head
    # counts / transactions over the raw codes that share a canonical
    # species (and over multiple holders in one household).
    agg = merged.groupby(['i', 'animal'], dropna=True)[
        ['HeadCount', 'HeadAcquired', 'HeadSold', 'Value']].sum(min_count=1)

    # Drop species the household neither owns nor transacts (all-NaN or
    # all-zero across every reported column).
    nonzero = agg.fillna(0).abs().sum(axis=1) > 0
    agg = agg[nonzero]

    agg = agg.reset_index()
    agg['t'] = t
    agg['i'] = agg['i'].astype('string')
    agg['animal'] = agg['animal'].astype('string')
    for col in ['HeadCount', 'HeadAcquired', 'HeadSold', 'Value']:
        agg[col] = agg[col].astype('Float64')
    agg = agg.set_index(['t', 'i', 'animal'])
    agg = agg[['HeadCount', 'HeadAcquired', 'HeadSold', 'Value']]
    return agg.sort_index()
