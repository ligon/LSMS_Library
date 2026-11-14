#!/usr/bin/env python
"""community_prices for Nigeria GHS-Panel 2015-16 (W3) -- GAP C.

Item-level reported community food prices at grain (t, v, j, u, obs) from
the post-harvest COMMUNITY questionnaire's food-price module (Section C8).  W3
fields C8 at TWO locations per community, one file each, over the SAME item
list: sectc8a_harvestw3.dta is "C8a: FOOD PRICES - 1ST LOCATION" (obs = 1) and
sectc8b_harvestw3.dta "C8b: FOOD PRICES - 2ND LOCATION" (obs = 2)
(PH_W3_Community_Quest.pdf, SECTIONS list "8 Food Prices / a. First location
18 / b. Second location 24").  NOT "different item ranges", as this docstring
used to say: all 110 sectc8b item codes also occur in sectc8a.  How the
second page was used differs by EA (measured 2026-10-23, GH #637): in 78 of
the 487 EAs it re-prices most of the first page's list (4,323 shared (ea,
item) cells); in the other 409 it prices a short tail (median 10 items;
87% of those 4,032 rows are codes 70-77, the fresh vegetables, or 160-163)
that the first location did not price.  Both
files are (ea, item_cd)-unique and cover all 487 EAs.  Until GH #637 the
builder collapsed the two locations on (t, v, j, u) and kept location 1.
CLUSTER-level (no household i); v = cluster_id(state, lga, ea).
W3 item_cd is the consumption-module scheme already in harmonize_food
(resolved via Code); c8q2 is the per-row unit label, c8q3 the reported price,
c8q2b the free-text SIZE (GH #834: blank on 13,767 of the 13,768 g/cl rows
AND on all 3,433 Kg/l rows; the handful stating '300gram' / 'one 75cl bottle'
are normalised to per-g / per-cl and the blank g/cl package prices are dropped).
The blank-size Kg / l rows are SERVED as recorded since GH #587's "Measure PH,
then align" (2026-11-13): they price per kilogram / per litre, as PP's do.
t = 2016Q1 (post-harvest quarter).

Post-planting round (GH #587, rulings 2026-11-12): the PP COMMUNITY
questionnaire prices food too, in Section C2 (sectc2a_plantingw3.dta (1st location, obs 1) + sectc2b_plantingw3.dta (2nd, obs 2)), so
this script also serves t = 2015Q3 through the PP adapter
(../../_/pp_community_prices.py): j / u on Nigeria's shared labels, the native
size in `Size`, Price on the form's own basis -- quantity of one (c2q2 unit, c2q2b other-unit text, c2q3 Naira).
Like the PH rows above (since 2026-11-13), the PP Kg / l rows with blank c2q2b
are SERVED as recorded (ruling 2026-11-12: they measure as per-kg / per-litre
prices); PP g / cl with blank c2q2b stay held (pp-unrecorded-size-hold).
Distinct t per round (pp-ph skill).
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from nigeria import PH_QUARTER, community_prices_for_wave, _crop_labels
from pp_community_prices import combine_rounds, pp_community_prices_for_folder

crop_labels = _crop_labels()

t = PH_QUARTER['2015-16']
frames = []
# obs = the C8 reading slot: 1st location (C8a), 2nd location (C8b).
for obs, f in ((1, '../Data/sectc8a_harvestw3.dta'),
               (2, '../Data/sectc8b_harvestw3.dta')):
    raw = get_dataframe(f, convert_categoricals=False)
    dec = get_dataframe(f, convert_categoricals=True)
    frames.append(dict(df=raw, dec=dec, ea='ea', item='item_cd',
                       price='c8q3', unit='c8q2', size_text='c8q2b',
                       obs=obs))

df = community_prices_for_wave(t, frames, mode='codes',
                              crop_labels=crop_labels)

# The post-planting round's Section C2 (GH #587), its own t (2015Q3).
# Literal paths: the content hash folds the sidecar of every data file a
# wave script spells literally (the adapter checks they match its sources).
PP_SOURCES = (
    '../Data/sectc2a_plantingw3.dta',
    '../Data/sectc2b_plantingw3.dta',
)
df = combine_rounds(df, pp_community_prices_for_folder(
    '2015-16', lambda f: get_dataframe(f, convert_categoricals=False), PP_SOURCES))

assert df.index.is_unique, "community_prices W3: (t,v,j,u,obs) not unique"
assert len(df) > 0
to_parquet(df, 'community_prices.parquet')
