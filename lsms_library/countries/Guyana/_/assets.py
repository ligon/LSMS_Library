#!/usr/bin/env python
"""Compile household durable-goods (assets) for Guyana 1992.

Source: ``1992/Data/DRBLS.dta`` — a WIDE table, ONE ROW PER HOUSEHOLD, with one
block per durable item.  Each block ``NN`` carries three columns:

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

Household identity (GH #503)
----------------------------
The household is ``(ED, SN, HH)``, not ``(ED, HH)`` — COVERN's 1807 rows hold
1807 unique triples but only 1502 unique pairs, and the survey's own key says so
(``COVERN.NEWID == ED*100000 + SN*100 + HH`` for all 1807 rows).

DRBLS carries NO ED/SN/HH columns — only ``newid`` and ``id_nmbr``.  Neither can
key it:

* ``newid`` in the second-questionnaire file family (DRBLS, HHCHAR) is CORRUPT.
  Both files hold only 1616 DISTINCT ``newid`` across ~1818 rows, and 240 HHCHAR
  rows violate the NEWID identity that COVERN satisfies exactly.  317 DRBLS rows
  carry an ambiguous (duplicated) ``newid``; joining ``DRBLS.newid`` to
  ``COVERN.NEWID`` — what this script used to do — recovers only 1529 distinct
  households from 1730 rows, i.e. it still merges ~200 households into others.
* ``id_nmbr`` is not a household key at all (160 distinct values / 1818 rows).

HHCHAR is the crosswalk.  It carries BOTH the corrupt ``newid`` AND the clean
``(ed_dvsn, ed_smpl, smpl_hh)`` triple (== COVERN's ``(ED, SN, HH)``; verified
independently — ``HHCHAR.hhsize`` matches the ROSTERN member count for 98.1% of
households under the triple vs 81.1% under the pair).  After dropping HHCHAR's
single exact-duplicate record, HHCHAR is **row-for-row parallel to DRBLS**:
``newid`` matches 1818/1818 *and* ``id_nmbr`` matches 1818/1818.  So DRBLS row
``k`` is HHCHAR row ``k``'s household.

That crosswalk is validated where it can be: on the 1415 DRBLS rows whose
``newid`` is unambiguous, it agrees with the old newid join for 1415/1415
(100%).  It resolves the 317 rows the newid join cannot, and yields 1818 rows ->
1818 DISTINCT households (1:1, as a wide one-row-per-household table must be).

The alignment is ASSERTED below.  If a future re-extract of either file breaks
row parity, this script raises — a crash is a gift; silently misattributing a
household's durables is not.

No cross-household summation
----------------------------
Because DRBLS is wide and each row is exactly one household, ``(t, i, j)`` is
unique BY CONSTRUCTION.  The previous version collapsed duplicate ``(t, i, j)``
by SUMMING, justified in its docstring as "several per-acquisition detail rows
for the same (household, item)".  That premise was itself an artifact of the
conflation: those "detail rows" were DIFFERENT HOUSEHOLDS colliding on the
``ED-HH`` key.  209 of 1278 asset households were the arithmetic SUM of two real
households' durables — an invented number, not a dropped row.  The summation is
gone; uniqueness is asserted instead.
"""
import sys
import pandas as pd
from lsms_library.local_tools import to_parquet, get_dataframe, format_id

WAVE = '1992'

# HHCHAR's household key == COVERN's (ED, SN, HH).
HHCHAR_KEY = ['ed_dvsn', 'ed_smpl', 'smpl_hh']

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


def household_key_for_drbls(drb, hhc):
    """Attach the true (ED, SN, HH) to every DRBLS row via the HHCHAR crosswalk.

    Asserts row parity on BOTH shared columns (``newid``, ``id_nmbr``) so a
    source re-extract that breaks the alignment fails loudly rather than
    misattributing durables.  See the module docstring for why neither shared
    column can be used as a join key on its own.
    """
    hhc = (hhc.drop_duplicates(subset=HHCHAR_KEY, keep='first')
              .reset_index(drop=True))
    drb = drb.reset_index(drop=True)

    if len(hhc) != len(drb):
        raise AssertionError(
            f"Guyana assets: HHCHAR ({len(hhc)} deduped rows) and DRBLS "
            f"({len(drb)} rows) are no longer row-parallel; the positional "
            f"crosswalk that recovers DRBLS's household identity is invalid. "
            f"See GH #503 and this module's docstring."
        )
    for col in ('newid', 'id_nmbr'):
        left = pd.to_numeric(hhc[col], errors='coerce').astype('Int64')
        right = pd.to_numeric(drb[col], errors='coerce').astype('Int64')
        n_match = int((left == right).sum())
        if n_match != len(drb):
            raise AssertionError(
                f"Guyana assets: HHCHAR/DRBLS row parity broken on '{col}' "
                f"({n_match}/{len(drb)} rows agree); the positional crosswalk "
                f"is invalid.  See GH #503."
            )

    out = drb.copy()
    for src, dst in zip(HHCHAR_KEY, ['ED', 'SN', 'HH']):
        out[dst] = hhc[src].values
    return out


def main():
    drb = get_dataframe(f'../{WAVE}/Data/DRBLS.dta')
    hhc = get_dataframe(f'../{WAVE}/Data/HHCHAR.dta')
    cov = get_dataframe(f'../{WAVE}/Data/COVERN.dta')

    drb = household_key_for_drbls(drb, hhc)

    # Keep only households that are actually in the sample (COVERN).
    cov_trip = set(
        map(tuple, cov[['ED', 'SN', 'HH']].dropna().astype('int64').values)
    )
    keys = drb[['ED', 'SN', 'HH']].astype('int64')
    in_sample = [tuple(k) in cov_trip for k in keys.values]
    n_total = len(drb)
    drb = drb[in_sample].copy()
    print(f"Guyana 1992 assets: {n_total - len(drb)}/{n_total} DRBLS rows "
          f"dropped (household absent from COVERN, i.e. out of sample).",
          file=sys.stderr)

    drb['i'] = [f"{format_id(ed)}-{format_id(sn)}-{format_id(hh)}"
                for ed, sn, hh in zip(drb['ED'], drb['SN'], drb['HH'])]

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

    # Blocks 31/31a/31b share the item name "OTHER AUDIO-VISUAL", so a household
    # can legitimately contribute up to three rows for that ONE j.  Sum them:
    # this is a within-household, within-item roll-up of the questionnaire's
    # three "other audio-visual" write-in slots -- NOT the cross-household
    # summation this script used to do (see module docstring).
    dupes = long.duplicated(subset=['t', 'i', 'j'], keep=False)
    offenders = set(long.loc[dupes, 'j'].unique()) - {'OTHER AUDIO-VISUAL'}
    if offenders:
        raise AssertionError(
            f"Guyana 1992 assets: duplicate (t, i, j) for item(s) {offenders}, "
            f"which do not share a questionnaire block.  With the correct "
            f"household identity (ED, SN, HH), DRBLS is one row per household, "
            f"so (t, i, j) must be unique outside the 31/31a/31b write-in "
            f"slots.  A duplicate here means the household key is wrong again "
            f"(GH #503) -- do NOT paper over it by summing."
        )
    long = (long.groupby(['t', 'i', 'j'], as_index=True, observed=True)
                [['Quantity', 'Value']]
                .sum(min_count=1)
                .sort_index())

    to_parquet(long, '../var/assets.parquet')
    print(f"Guyana 1992 assets: wrote {len(long)} rows, "
          f"{long.index.get_level_values('i').nunique()} households, "
          f"{long.index.get_level_values('j').nunique()} item types.",
          file=sys.stderr)


if __name__ == '__main__':
    main()
