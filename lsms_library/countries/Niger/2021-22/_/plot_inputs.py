"""Build plot_inputs for Niger EHCVM 2021-22 (GAP 2, item-level).

Single source file: s16b_me_ner2021.dta — the household agricultural-input
roster, one row per input-type the household was asked about.  Same layout
as 2018-19:
  s16bq01   input type (organic / inorganic fert / phyto / seeds-by-crop)
  s16bq02   USED this input? (Oui / Non) — application gate
  s16bq03a / s16bq03b   quantity used + native unit (`u`)
  s16bq05 / s16bq07a / s16bq07b   GIFT: "16B.05. Avez-vous obtenu [INTRANT]
            sous forme de cadeau ou de don?", the quantity received, its unit
  s16bq08 / s16bq09a / s16bq09b   PURCHASE: "16B.08. Avez-vous acheté
            [INTRANT]?", the quantity bought, its unit

THE PURCHASE IS s16bq08 (ruling @ligon 2026-11-13, "Extend to all four now
(Recommended)": Niger 2018-19 and 2021-22 join the "Fix all three now" fix;
Togo's 408d02a24 is the template).  Until 2026-11-13 this script served the
GIFT answers (s16bq05 / s16bq07a) under Purchased / Quantity_purchased; they
now ride in Received_as_gift / Quantity_received_as_gift, and each
acquisition unit, decoded from its OWN value labels, in Unit_purchased /
Unit_received_as_gift, served in the vocabulary of `u`
(ehcvm.served_unit_labels).  `u` still describes the quantity USED.
s16bq09c (purchase value) is not read.  See _/CONTENTS.org.

The 2021-22 roster lists EVERY input-type per household (most not used), so
the s16bq02==1 gate is essential: of 73006 raw rows only ~12439 are applied
inputs.  As in 2018-19 the seed's crop is embedded in the input-type label
and resolved via harmonize_seed_crop (2021-22 adds 'Semences de coton',
'Plants/boutures de tubercules' etc.).  Non-seed rows carry crop NaN.
i is the EHCVM composite id via niger.i.  Grain (t, i, input, crop, u).
"""
import sys

sys.path.append('../../_/')
import pandas as pd

from lsms_library.ehcvm import served_unit_labels
from lsms_library.local_tools import get_dataframe, to_parquet
from niger import (i as niger_i, _input_maps, _input_labels,
                   _seed_crop_labels, _unit_labels, _finish_plot_inputs,
                   _na_exact)

# "Don't know" code of s16bq03a, unlabelled in the .dta (GH #865): 9999 on
# exactly one applied-input row (grappe 315, menage 10, an inorganic
# fertiliser with no unit).  Exact value only; s16bq07a carries none.
QTY_SENTINELS = (9999,)


#: The acquisition columns niger._finish_plot_inputs does not keep.  They are
#: attached here, row for row, so the shared tail in niger.py -- and with it
#: the hash of every ECVMA wave -- is unchanged.
ACQUISITION = {'Unit_purchased': 'string', 'Received_as_gift': 'boolean',
               'Quantity_received_as_gift': 'Float64', 'Unit_received_as_gift': 'string'}


def _served_units(series):
    """Each acquisition-unit label as the API serves `u` (Kilogramme -> Kg via
    the global u table); NA stays NA.  Same helper as Togo / Senegal /
    Cote d'Ivoire."""
    raw = series.astype('string').str.strip()
    known = raw.dropna().unique()
    served = dict(zip(known, served_unit_labels('Niger', known)))
    return raw.map(served).astype('string')


def _finish_with_acquisition(df, t):
    """niger._finish_plot_inputs, then the acquisition columns in line order.

    The shared tail drops lines with no input identity and keeps row order, so
    the same filter is applied first and the columns are re-attached by
    position (asserted).  ``Quantity_received_as_gift`` is coerced as the tail
    coerces ``Quantity_purchased``."""
    df = df[df['input'].astype('string').notna()].reset_index(drop=True)
    acq = df[list(ACQUISITION)].copy()
    acq['Quantity_received_as_gift'] = pd.to_numeric(acq['Quantity_received_as_gift'], errors='coerce')
    out = _finish_plot_inputs(df.drop(columns=list(ACQUISITION)), t)
    assert len(out) == len(acq), f'plot_inputs {t}: shared tail changed the line count'
    for col, dtype in ACQUISITION.items():
        out[col] = acq[col].astype(dtype).array
    return out[['Quantity', 'Purchased', 'Quantity_purchased', 'Unit_purchased',
                'Received_as_gift', 'Quantity_received_as_gift', 'Unit_received_as_gift']]


src = get_dataframe('../Data/s16b_me_ner2021.dta', convert_categoricals=True)
srcn = get_dataframe('../Data/s16b_me_ner2021.dta', convert_categoricals=False)

input_map, unit_map, seed_crop_map = _input_maps()

# Keep only inputs the household actually applied (s16bq02 == 1 = Oui).
applied = srcn['s16bq02'] == 1
src = src[applied.values]
srcn = srcn[applied.values]

hh = src.apply(lambda r: niger_i(pd.Series([r['grappe'], r['menage']],
                                           index=['grappe', 'menage'])), axis=1)

# The purchase is s16bq08 / s16bq09a / s16bq09b; the gift is s16bq05 /
# s16bq07a / s16bq07b (labels via convert_categoricals; see the docstring).
yes_no = {'Oui': True, 'Non': False}
purchased = src['s16bq08'].astype('string').map(yes_no)
received_as_gift = src['s16bq05'].astype('string').map(yes_no)

df = pd.DataFrame({
    'i':                  hh.values,
    'input':              _input_labels(src['s16bq01'], input_map).values,
    'crop':               _seed_crop_labels(src['s16bq01'], seed_crop_map).values,
    'u':                  _unit_labels(src['s16bq03b'], unit_map).values,
    'Quantity':           _na_exact(srcn['s16bq03a'], QTY_SENTINELS).values,
    'Purchased':          purchased.values,
    'Quantity_purchased': pd.to_numeric(srcn['s16bq09a'], errors='coerce').values,
    'Unit_purchased':     _served_units(src['s16bq09b']).values,
    'Received_as_gift':   received_as_gift.values,
    'Quantity_received_as_gift': pd.to_numeric(srcn['s16bq07a'], errors='coerce').values,
    'Unit_received_as_gift': _served_units(src['s16bq07b']).values,
})

df = _finish_with_acquisition(df, '2021-22')

assert len(df) > 0, 'plot_inputs 2021-22 produced no rows'
# GH #1123: distinct native input slots must keep distinct declared keys,
# even when their payloads happen to be equal.  Until GH #1123 a
# non-injective harmonize_seed_crop pooled
#   slots 15+20 and 19+20 (the three catch-all seed slots)
# onto one 'Autre crop' key and core silently kept one row.  Same guard as
# Burkina_Faso/Benin/Togo; it reports a count only, never a source key.
dups = df.index[df.index.duplicated()]
assert dups.empty, (
    f"Niger plot_inputs 2021-22: {len(dups)} duplicate tuple(s) on "
    "(t, i, input, crop, u); distinct reported input identities collide"
)
to_parquet(df, 'plot_inputs.parquet')
