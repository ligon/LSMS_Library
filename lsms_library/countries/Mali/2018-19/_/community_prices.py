"""Mali EHCVM section 5: both FCFA quotes per product and unit-size (#821).

No quantity/weight is shipped. Keep Quantity missing; the EAC-I Poids(KG)
policy and Niger's instrument-specific count derivation do not apply here.
Source products survive harmonization in variety; vague is visit, and grappe
is the native sample().v key. No source row or price slot is dropped.
"""
from lsms_library.local_tools import get_dataframe, to_parquet
from mapping import community_prices_s05

SOURCE = '../Data/s05_co_mli2018.dta'
labels = get_dataframe(SOURCE)
codes = get_dataframe(SOURCE, convert_categoricals=False)
df = community_prices_s05(codes, labels)
assert len(df) == 2 * len(codes), 's05 must retain both slots of every source row'
assert df.index.is_unique, 's05 observation keys must retain all quotes'
to_parquet(df, 'community_prices.parquet')
