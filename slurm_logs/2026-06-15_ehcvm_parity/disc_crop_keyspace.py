#!/usr/bin/env python
"""crop_production keyspace discovery: for each ERHS wave, find which
area_output_* key columns map to the wave's sample() i-keyspace."""
import pyreadstat, pandas as pd
import lsms_library as ll
from lsms_library.countries.EthiopiaRHS._ import ethiopiarhs as E

ARCH = 'lsms_library/countries/EthiopiaRHS/_dataverse_archive'
D2009 = 'lsms_library/countries/EthiopiaRHS/2009/Data'

# wave -> (file, [candidate key-col pairs], a production-column to gate "has crop data")
WAVES = {
    '1994a': (f'{ARCH}/area_output_94.tab',       [['q1c','hhid'],['q1c','q5'],['paid','hhid']], 'wtefprd94'),
    '1994b': (f'{ARCH}/area_output_r2r3rev.tab',  [['paid','hhid'],['q1b','hhid']],              'wtefprd94_2'),
    '1995' : (f'{ARCH}/area_output_r2r3rev.tab',  [['paid','hhid'],['q1b','hhid']],              'wtefprd95'),
    '1997' : (f'{ARCH}/area_output_97.tab',       [['q1c','hhid'],['q1c','q5'],['paid','hhid']], 'wtefprd_97'),
    '1999' : (f'{ARCH}/area_output_99.tab',       [['paid','hhid']],                             'wtefprd99'),
    '2004' : (f'{ARCH}/area_output_04rev.tab',    [['paid','hhid']],                             'wtefprd04'),
    '2009' : (f'{D2009}/erhs7_meher_area_output_cereals_2009.tab', None,                          None),
}

# sample() i per wave (the keyspace v joins on)
samp = ll.Country('EthiopiaRHS', preload_panel_ids=False).sample()
samp = samp.reset_index()
for w, (fn, cands, prodcol) in WAVES.items():
    print(f"\n=== {w} : {fn.split('/')[-1]} ===")
    try:
        df,_ = pyreadstat.read_dta(fn)
    except Exception as e:
        print("  read ERR:", e); continue
    if w == '2009':
        print("  cols:", list(df.columns)[:30]); continue
    wave_i = set(samp.loc[samp['t']==w, 'i'].astype(str))
    print(f"  sample() i for {w}: {len(wave_i)} households")
    # restrict to rows with crop data
    if prodcol in df.columns:
        has = df[df[prodcol].notna()]
    else:
        has = df
    print(f"  rows with {prodcol}: {len(has)}")
    for keys in cands:
        if not all(k in df.columns for k in keys):
            print(f"    keys {keys}: (missing cols)"); continue
        ikeys = has[keys].dropna().apply(lambda r: E.i(pd.Series([r[keys[0]], r[keys[1]]])), axis=1)
        ikeys = set(ikeys.dropna().astype(str))
        inter = ikeys & wave_i
        print(f"    keys {keys}: {len(ikeys)} HH, overlap {len(inter)} = {len(inter)/max(1,len(ikeys)):.3f}")
