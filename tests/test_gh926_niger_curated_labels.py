"""Protect the current Niger curated-label lookup and Original display (#926)."""
import pandas as pd

from lsms_library import Country
from lsms_library.local_tools import df_data_grabber


def test_niger_raw_and_clean_keys_converge_before_hook_but_original_is_a_display(tmp_path):
    country = Country('Niger')
    wave = country['2018-19']
    spec = wave.resources['food_acquired']
    (src, transform), = [(name, args['idxvars']['j'])
                         for name, args in wave.column_mapping('food_acquired', spec).items()
                         if name != 'df_edit']
    assert src == 's07b_me_ner2018.dta'
    clean, raw = 'Arachide grillée', 'Arachide grillÃ©e'
    source_columns, lookup = transform
    assert source_columns == ['s07bq01'] and lookup[raw] == clean
    assert lookup.get(clean, clean) == clean
    p = tmp_path / 'invented.dta'
    pd.DataFrame({'hh': [1, 1], 's07bq01': [901, 902], 'q': [2.0, 3.0]}).to_stata(
        p, version=118, write_index=False, value_labels={'s07bq01': {901: raw, 902: clean}})
    wide = df_data_grabber(p, {'i': 'hh', 'j': transform}, Quantity='q')
    assert wide.index.duplicated(keep=False).tolist() == [True, True]
    assert wide.index.get_level_values('j').tolist() == [clean, clean]
    assert wide['Quantity'].tolist() == [2.0, 3.0]
    original = country._relabel_j(wide, 'Original', reaggregate=False)
    assert original.index.get_level_values('j').tolist() == [raw, raw]
    assert original['Quantity'].tolist() == [2.0, 3.0]
