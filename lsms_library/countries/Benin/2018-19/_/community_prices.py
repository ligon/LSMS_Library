"""Build community_prices for Benin EHCVM 2018-19 (GH #1226, item-level).

Source: s05_co_ben2018.dta, section 5 of the community questionnaire.  One row
per (grappe, s05q01 product, s05q02 unit-size) with TWO price readings and no
quantity: the form prices one item of each unit-size selected.

Complication: the household's `harmonize_food` lumps ten of the 103 priced
products into five `j`, so the product code is the `variety` of those ten
(5,858 records would otherwise share a served key), and `s05q02` is a code that
embeds the household's unit and size, with exceptions.  Rules in mapping.py,
measurements in Benin/_/CONTENTS.org.
"""
from lsms_library.local_tools import get_dataframe, to_parquet
from mapping import (_S05_HOUSEHOLD, _S05_SOURCE, community_prices_s05,
                     derive_s05_one_item_per_price)

SOURCE = f'../Data/{_S05_SOURCE}'
codes = get_dataframe(SOURCE, convert_categoricals=False)
community = get_dataframe(SOURCE, categories_only=True)
household = get_dataframe(f'../Data/{_S05_HOUSEHOLD}', categories_only=True)
df = derive_s05_one_item_per_price(community_prices_s05(codes, community, household))
assert len(df) == 2 * len(codes), 'Benin s05 must serve every source record and reading'
assert df.index.is_unique, 'Non-unique index in community_prices 2018-19'
to_parquet(df, 'community_prices.parquet')
