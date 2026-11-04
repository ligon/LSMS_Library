"""Build community_prices for Guinea-Bissau EHCVM 2018-19 (GH #1226, item-level).

Source: s05_co_gnb2018.dta, section 5 of the community questionnaire.  One row
per (grappe, s05q01 product, s05q02 unit-size) with TWO price readings and no
quantity: the form prices one item of each unit-size selected.

Complication: every label in the file is double-encoded and in Portuguese, and
`s05q02` is a code that embeds the household's unit and size, so `j` and `u` go
through the household's labels by code and this wave's `_decode_mojibake`.
Rules in mapping.py, measurements in Guinea-Bissau/_/CONTENTS.org.
"""
from lsms_library.local_tools import get_dataframe, to_parquet
from mapping import (_S05_HOUSEHOLD, _S05_SOURCE, community_prices_s05,
                     derive_s05_one_item_per_price)

SOURCE = f'../Data/{_S05_SOURCE}'
codes = get_dataframe(SOURCE, convert_categoricals=False)
community = get_dataframe(SOURCE, categories_only=True)
household = get_dataframe(f'../Data/{_S05_HOUSEHOLD}', categories_only=True)
df = derive_s05_one_item_per_price(community_prices_s05(codes, community, household))
assert len(df) == 2 * len(codes), 'Guinea-Bissau s05 must serve every source record and reading'
assert df.index.is_unique, 'Non-unique index in community_prices 2018-19'
to_parquet(df, 'community_prices.parquet')
