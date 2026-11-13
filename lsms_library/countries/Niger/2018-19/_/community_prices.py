"""Build community_prices for Niger EHCVM 2018-19 (GH #823, item-level).

Source: s05_co_ner2018.dta -- section 5 of the EHCVM community questionnaire,
"Relevé des prix à la consommation".  One row per (grappe, s05q01 product,
s05q02 unit-size) with TWO price readings and no quantity:
  grappe    -> v       (format_id; the sample() keyspace, 504/504 grappes)
  vague     -> visit   (each grappe is fielded in exactly one vague)
  s05q01    -> j       BY CODE: the household s07bq01 label of the same
                       produitID code, through harmonize_food -- the j Niger
                       food_acquired serves; the s05 label is `Description`
  s05q02    -> u       BY CODE: releveID = 10 * household unit code + size
                       code; size 0 -> the household unit label (`Kg`,
                       `Tiya`), else `<unit> (<Size>)` (`Tas (Petit)`)
  s05q03    -> obs 1   "5.03. Relevé 1-Prix (en FCFA)"
  s05q04    -> obs 2   "5.04. Relevé 2-Prix (en FCFA)"
  (none)    -> variety the canonical 'unknown_variety' (no variety field)
A (grappe, product, unit-size) key carried by a second, disagreeing record
(140 keys) serves that record as obs 3 / 4; byte-identical repeats (20 rows)
and a second record's slot that repeats the first record's same slot (10) are
copies and are dropped.  Nothing is averaged or selected.

Quantity = 1 is NOT read from the file (it records none): the Niger CAPI price
questionnaire prices ONE item of each selected unit-size, so it is the
registered derivation Niger::community_prices::one-item-per-price (function
and inputs in this wave's mapping.py), stamped on every row AFTER the last
drop.  Decisions D1-D7 and the measurements: Niger/_/CONTENTS.org and
.coder/ledger/823-niger-community-prices.md.

CLUSTER-level feature (no household i): `v` is declared in the index and is
NOT framework-joined.  Index = (t, v, j, u, variety, visit, obs).
"""
import sys

sys.path.append('../../_/')

from lsms_library.local_tools import get_dataframe, to_parquet
from niger import (_community_prices_maps, _finish_community_prices,
                   _s05_code_maps, _s05_community_prices)
from mapping import _decode_mojibake, derive_s05_one_item_per_price

WAVE = '2018-19'

item_map, unit_map = _community_prices_maps()

src = '../Data/s05_co_ner2018.dta'
labels = get_dataframe(src)
codes = get_dataframe(src, convert_categoricals=False)
# The household food module's value labels: the code bridge for j and u.
hh_categories = get_dataframe('../Data/s07b_me_ner2018.dta', categories_only=True)

j_by_code, u_by_code = _s05_code_maps(codes['s05q01'].unique(),
                                      codes['s05q02'].unique(), hh_categories,
                                      item_map, unit_map, _decode_mojibake)
df = _s05_community_prices(codes, labels, j_by_code, u_by_code)
df = _finish_community_prices(df, WAVE)
df = derive_s05_one_item_per_price(df)

assert len(df) > 0, 'community_prices 2018-19 produced no rows'
assert df.index.is_unique, 'Non-unique index in community_prices 2018-19'
to_parquet(df, 'community_prices.parquet')
