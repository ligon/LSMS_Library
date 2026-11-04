"""Build community_prices for Senegal EHCVM 2018-19 (GH #1226, item-level).

Source: s05_co_sen2018.dta, section 5 of the community questionnaire.  One row
per (grappe, s05q01 product, s05q02 unit-size) with TWO price readings and no
quantity: the form prices one item of each unit-size selected.

Complication: Senegal's `harmonize_food` and `u` tables are keyed on the
household file's MOJIBAKE, and that is load-bearing (CONTENTS.org).  So `j` and
`u` take the household's raw label by code and let those tables repair it;
nothing here decodes, and the s05 labels, which are mostly clean UTF-8, are
never used as keys.  2021-22 has no community price file: s05_me_sen2021 is a
household module and ehcvm_prix_sen2021 has no grappe.
Rules in mapping.py, measurements in Senegal/_/CONTENTS.org.
"""
from lsms_library.local_tools import get_dataframe, to_parquet
from mapping import (_S05_HOUSEHOLD, _S05_SOURCE, community_prices_s05,
                     derive_s05_one_item_per_price)

SOURCE = f'../Data/{_S05_SOURCE}'
codes = get_dataframe(SOURCE, convert_categoricals=False)
community = get_dataframe(SOURCE, categories_only=True)
household = get_dataframe(f'../Data/{_S05_HOUSEHOLD}', categories_only=True)
df = derive_s05_one_item_per_price(community_prices_s05(codes, community, household))
assert len(df) == 2 * len(codes), 'Senegal s05 must serve every source record and reading'
assert df.index.is_unique, 'Non-unique index in community_prices 2018-19'
to_parquet(df, 'community_prices.parquet')
