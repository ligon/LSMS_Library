"""Concatenation oracle: each make-backend var/<table>.parquet (the
concatenator's output) must equal the plain concatenation of the four wave
parquets read DIRECTLY with pyarrow (no library reader), wave by wave.
Usage: python oracle.py <data_root> <out.json>.  Aggregates only."""
import json, sys
from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq
root, out = Path(sys.argv[1]) / 'Niger', Path(sys.argv[2])
WAVES = ['2011-12', '2014-15', '2018-19', '2021-22']
res = {}
for tbl in ('plot_inputs', 'crop_production', 'livestock', 'plot_labor', 'people_last7days'):
    waves = {w: pq.read_table(root / w / '_' / f'{tbl}.parquet').to_pandas() for w in WAVES}
    var = pq.read_table(root / 'var' / f'{tbl}.parquet').to_pandas()
    cat = pd.concat([waves[w] for w in WAVES])
    try:
        pd.testing.assert_frame_equal(var, cat, check_exact=True)
        eq = True
    except AssertionError as e:
        eq = str(e).splitlines()[0][:150]
    tv = var.index.get_level_values('t').value_counts().to_dict()
    res[tbl] = {'var_equals_concat_of_wave_parquets': eq,
                'wave_rows': {w: int(len(waves[w])) for w in WAVES},
                'var_rows_by_wave': {w: int(tv.get(w, 0)) for w in WAVES},
                'each_wave_parquet_has_only_its_t': all(
                    set(waves[w].index.get_level_values('t')) == {w} for w in WAVES)}
out.write_text(json.dumps(res, indent=1, sort_keys=True))
print(json.dumps(res, indent=1, sort_keys=True))
