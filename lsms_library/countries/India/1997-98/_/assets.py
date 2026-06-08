import pandas as pd
import sys
sys.path.append('../../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet

# India 1997-98 §7 Part D 'Consumer durables owned' (SECT07D.DTA).
#
# The module is already stored long: one row per (household, item) with
#   itemcode  = consumer-durable item code (value-labelled in the .dta)
#   v07d02    = "Number of items owned"  -> Quantity
# There is no reported value/price column in this part (§7D records counts
# only; the value of durables is not collected in the 1997-98 instrument),
# so only Quantity is emitted.  This mirrors the canonical assets schema
# (t, i, j) with whichever of Quantity/Value the survey records.
#
# The item label is recovered from the .dta value-label set on `itemcode`.
# The old Stata-105 format truncates those labels to 8 characters
# ('Radio/ca', 'Motorcyc', ...) and -- worse -- gives codes 512 and 515 the
# *same* truncated label 'Pressure', which would silently merge two distinct
# items if used as j.  We therefore map by code to the full, disambiguated
# item names (codes are the authoritative identity; the expansions follow the
# standard SLC consumer-durables roster).
#
# i = hhcode (matches sample()/household_roster 100%; see the
# months_food_inadequate.py note in this directory).  v is NOT baked in; it is
# joined from sample() at API time by _join_v_from_sample.

# itemcode -> canonical item label (j).  Codes are the source of truth; the
# .dta value labels are 8-char-truncated and collide on 512/515.
ITEM_LABELS = {
    501: 'Radio/cassette player',
    502: 'Camera/camcorder',
    503: 'Bicycle',
    504: 'Motorcycle/scooter',
    505: 'Motor car',
    506: 'Refrigerator',
    507: 'Washing machine',
    508: 'Fans',
    509: 'Heaters',
    510: 'B/W television',
    511: 'Colour television',
    512: 'Pressure cooker',
    513: 'Telephone',
    514: 'Sewing machine',
    515: 'Pressure lamp/stove',
    516: 'Watches/clocks',
}

# convert_categoricals=False: keep itemcode as the numeric code (the .dta
# value labels are 8-char-truncated and collide on 512/515; we map codes
# ourselves via ITEM_LABELS).  With the default decode, itemcode comes back
# as the label string ('Watches'...) and astype('Int64') raises.
df = get_dataframe('../Data/SECT07D.DTA', convert_categoricals=False)

out = pd.DataFrame(index=df.index)
out['i'] = df['hhcode'].astype(int).astype(str)
out['t'] = '1997-98'
out['j'] = df['itemcode'].astype('Int64').map(ITEM_LABELS).astype('string')
out['Quantity'] = pd.to_numeric(df['v07d02'], errors='coerce')

# Drop rows whose item code is unknown or whose count is missing/zero.
out = out.dropna(subset=['j'])
out = out[out['Quantity'].fillna(0) > 0]

out = out.set_index(['t', 'i', 'j']).sort_index()

to_parquet(out, 'assets.parquet')
