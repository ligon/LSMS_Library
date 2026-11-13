"""Burkina2018 PRICE quotes: two reported FCFA slots per native unit-size.

The separate FOCUS workbook's quantity/third-slot fields are not exported by
this source. Quantity1 is an explicit registered quoted-unit construction,
not a reported weight. Complete source coordinates survive in variety.
"""
from lsms_library.local_tools import get_dataframe, to_parquet
from mapping import community_prices_s05, derive_quoted_native_unit_count

SOURCE = '../Data/s05_co_bfa2018.dta'
codes = get_dataframe(SOURCE, convert_categoricals=False)
community_categories = get_dataframe(SOURCE, categories_only=True)
household_categories = get_dataframe('../Data/s07b_me_bfa2018.dta', categories_only=True)
df = community_prices_s05(codes, community_categories, household_categories)
df = derive_quoted_native_unit_count(df)
assert len(df) == 2 * len(codes), 'Burkina s05 must retain every source row and slot'
assert df.index.is_unique, 'Burkina s05 full observation key collision'
to_parquet(df, 'community_prices.parquet')
