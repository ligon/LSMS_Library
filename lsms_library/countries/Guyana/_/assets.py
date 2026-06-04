#!/usr/bin/env python
"""Compile household durable-goods (assets) for Guyana 1992.

Source: ``1992/Data/DRBLS.dta`` — a WIDE table with one block per durable
item.  Each block ``NN`` carries three columns:

    itemNN  -> number owned        (Quantity)
    valiNN  -> purchase value      (Value)
    yriNN   -> year of purchase    (read but not carried in the canonical
                                    schema, which declares only Quantity/Value)

The item *name* (``j``) is NOT in the data — it lives in the Stata *variable
label* of each ``itemNN`` column (e.g. ``"NO. OWNED - AIR CONDITIONER"``).
Because ``get_dataframe`` does not surface variable labels, and because
``DRBLS.dta`` is a fixed historical artifact, the suffix -> item-name map is
captured below verbatim from the column labels (the ``"NO. OWNED - "`` prefix
stripped).  Blocks 31/31a/31b all share the label ``"OTHER AUDIO-VISUAL"``.

i reconciliation: ``sample()`` keys households on ``i = "ED-HH"`` (from
COVERN.dta).  DRBLS has no separate ED/HH columns, only ``newid``
(= ED*100000 + ED_SMPL*100 + SMPL_H), whose tail does NOT encode HH.  We
therefore join DRBLS.newid <-> COVERN.NEWID to recover (ED, HH) and build the
same ``i`` as the sample table.  Verified 0 orphan households vs.
``Country('Guyana').sample()``; ~5% of DRBLS rows have a newid absent from
COVERN (genuinely out-of-sample) and are dropped.

DRBLS lists multiple per-acquisition detail rows for the same (household,
item).  Because the canonical (t, i, j) index must be unique, these are
collapsed by SUMMING Quantity and Value — see the inline note in ``main`` for
why summing (deterministic, value-preserving) beats the framework's fallback
``groupby().first()`` (arbitrary, lossy).
"""
import sys
import numpy as np
import pandas as pd
from lsms_library.local_tools import to_parquet, get_dataframe, format_id

WAVE = '1992'

# suffix -> item name, taken verbatim from the DRBLS.dta variable labels
# ("NO. OWNED - <name>").  31/31a/31b share "OTHER AUDIO-VISUAL".
ITEMS = {
    '01': 'AIR CONDITIONER',          '02': 'REFRIGERATOR',
    '03': 'COOKING RANGE',            '04': 'MICRO OVEN',
    '05': 'ELECTRIC STOVE',           '06': 'GAS STOVE',
    '07': 'KEROSENE OIL STOVE',       '08': 'FOOD PROC,MIXER,BLE',
    '09': 'DISHWASHER',               '10': 'WASHING MACHINE',
    '11': 'DRYING MACHINE',           '12': 'SEWING MACHINE',
    '13': 'VACUUM CLEANER',           '14': 'IRON',
    '15': 'FAN',                      '16': 'BEDS',
    '17': 'COTS',                     '18': 'SOFA SET',
    '19': 'MOTOR CAR',                '20': 'MOTOR CYCLE/SCOOTER',
    '21': 'BICYCLE',                  '22': 'TELEPHONE',
    '23': 'TELEVISION',               '24': 'VIDEO REC/PLAYER',
    '25': 'TAPE REC/PLAYER',          '26': 'PHONOGRAPH,DISC PLA',
    '27': 'RADIO TURNER',             '28': 'MUSIC SYSTEM',
    '29': 'PIANO,HARMONIUM',          '30': 'STRING INSTRUMENT',
    '31': 'OTHER AUDIO-VISUAL',       '31a': 'OTHER AUDIO-VISUAL',
    '31b': 'OTHER AUDIO-VISUAL',      '32': 'VIDEO CAMERA',
    '33': 'MOVIE CAMERA',             '34': 'STILL CAMERA',
    '35': 'CLOCK',                    '36': 'WATCH',
    '37': 'HORSE',                    '38': 'BULLOCK',
    '39': 'SHEEP/GOAT',               '40': 'CHICKEN/DUCK',
    '41': 'DONKEY/MULES',             '42': 'COWS',
    '43': 'PIGS',                     '44': 'OTHER LIVESTOCK',
}


def main():
    drb = get_dataframe(f'../{WAVE}/Data/DRBLS.dta')
    cov = get_dataframe(f'../{WAVE}/Data/COVERN.dta')

    # Recover (ED, HH) for each DRBLS row by joining newid <-> COVERN.NEWID.
    cov = cov.dropna(subset=['NEWID']).copy()
    cov['NEWID'] = cov['NEWID'].astype('int64')
    id_map = cov.drop_duplicates('NEWID').set_index('NEWID')[['ED', 'HH']]

    drb = drb.dropna(subset=['newid']).copy()
    drb['newid'] = drb['newid'].astype('int64')
    drb = drb.join(id_map, on='newid')

    n_total = len(drb)
    drb = drb[drb['ED'].notna()].copy()      # drop rows with no COVERN match
    n_drop = n_total - len(drb)
    print(f"Guyana 1992 assets: {n_drop}/{n_total} DRBLS rows dropped "
          f"(newid absent from COVERN).", file=sys.stderr)

    drb['i'] = [f"{format_id(ed)}-{format_id(hh)}"
                for ed, hh in zip(drb['ED'], drb['HH'])]

    # WIDE -> LONG: one record per (household, item block).
    frames = []
    for suf, name in ITEMS.items():
        if f'item{suf}' not in drb.columns or f'vali{suf}' not in drb.columns:
            print(f"  skip block {suf}: column(s) missing", file=sys.stderr)
            continue
        block = drb[['i', f'item{suf}', f'vali{suf}']].copy()
        block.columns = ['i', 'Quantity', 'Value']
        block['j'] = name
        frames.append(block)

    long = pd.concat(frames, ignore_index=True)
    long['Quantity'] = pd.to_numeric(long['Quantity'], errors='coerce')
    long['Value'] = pd.to_numeric(long['Value'], errors='coerce')

    # Drop blocks the household does not own (Quantity==0 and Value==0 / NaN).
    owned = ~((long['Quantity'].fillna(0) == 0) & (long['Value'].fillna(0) == 0))
    long = long[owned].copy()

    long['t'] = WAVE

    # DRBLS records SEVERAL detail rows for the same (household, item): e.g.
    # household 1-1 lists five "BEDS" purchases made in different years at
    # different prices.  The canonical assets index is (t, i, j) and the
    # framework REQUIRES it to be unique — ``_normalize_dataframe_index``
    # collapses any remaining duplicates with ``groupby().first()``, which
    # would silently drop all but one detail row (an arbitrary pick that
    # discards real holdings/value).  We instead collapse these same-item
    # detail rows by SUMMING: total ``Quantity`` owned and total ``Value`` of
    # that item type for the household.  This is the household's item-level
    # holding, NOT a cross-item household aggregate — the per-item granularity
    # the assets schema asks for is preserved; only the redundant
    # per-acquisition split (which the framework cannot represent in a unique
    # index anyway) is summed.  Determinstic, and loses no quantity/value
    # vs. the framework's arbitrary first().
    long = (long.groupby(['t', 'i', 'j'], as_index=True, observed=True)[['Quantity', 'Value']]
                .sum(min_count=1)
                .sort_index())

    to_parquet(long, '../var/assets.parquet')
    print(f"Guyana 1992 assets: wrote {len(long)} rows, "
          f"{long.index.get_level_values('i').nunique()} households, "
          f"{long.index.get_level_values('j').nunique()} item types.",
          file=sys.stderr)


if __name__ == '__main__':
    main()
