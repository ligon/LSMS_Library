#!/usr/bin/env python3
"""Iraq 2012 assets — sum physical instances per (i, j).

The source ``2012ihses18_durables.dta`` records one row per *physical
instance* of a durable, distinguished by ``durable_serial`` (a household
owning two TVs yields two rows with the same ``durable_code``).  The
canonical assets index ``(t, i, j)`` requires uniqueness, so the
framework's downstream dedup would otherwise collapse instances with
``.first()`` — silently dropping the 2nd+ instances (25,248 rows on this
wave).

To preserve every instance we AGGREGATE per ``(i, j)`` here: Quantity,
Value, and Purchase Price are SUMMED across serials (``min_count=1`` so an
all-NaN group stays NaN rather than collapsing to 0).  This is the YAML
path's only shortfall — it cannot groupby-sum — so this wave is built via
the script (``materialize: make``) path instead.  ``t`` is added by the
framework when it concatenates waves; this script writes the ``(i, j)``
frame only.
"""
from lsms_library.local_tools import to_parquet, get_dataframe

fn = '../Data/2012ihses18_durables.dta'

df = get_dataframe(fn)

# questid -> i (household), durable_code -> j (item); durable_serial is the
# within-(i, j) physical-instance discriminator we collapse over.
df = df.rename(columns={
    'questid': 'i',
    'durable_code': 'j',
    'q1801': 'Quantity',
    'q1805': 'Value',
    'q1803': 'Purchase Price',
})

# IDs to strings for stable merge keys (matches df_data_grabber/format_id).
df['i'] = df['i'].astype(str)
df['j'] = df['j'].astype(str)

assets = (
    df[['i', 'j', 'Quantity', 'Value', 'Purchase Price']]
    .groupby(['i', 'j'])
    .agg({
        'Quantity': lambda s: s.sum(min_count=1),
        'Value': lambda s: s.sum(min_count=1),
        'Purchase Price': lambda s: s.sum(min_count=1),
    })
)

to_parquet(assets, 'assets.parquet')
