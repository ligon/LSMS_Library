"""Own2021 household s16b serving candidate (GH #1483; verification UNRUN).

The own q02 card and closed source diagnosis bind the applied-input gate.
q03a/b describe use, q05/07a/b gifts, q08/09a/b purchases; own q09c is
explicitly Montant. Keep the combined q09a wording and raw answers. Native
panel i uses country i, before exactly one framework walk. No CS/parcel
allocation, nutrient-grade, unit-factor or2018 change.
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import (get_dataframe, to_parquet,
                                      get_categorical_mapping, format_id)
from burkina_faso import i
from s16b_2021 import build_s16b_candidate

build_s16b_candidate(read=get_dataframe, write=to_parquet,
                     get_map=get_categorical_mapping, id_fn=i, format_id=format_id,
                     source_path='../Data/s16b_me_bfa2021.dta')
