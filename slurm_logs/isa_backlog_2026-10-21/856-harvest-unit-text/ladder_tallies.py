"""GH #856 -- replay the Malawi sale-basis ladder per wave from the raw
modules (malawi._crop_pieces, as derivation_inputs does) and print the
AGGREGATE tallies the pinned test constants describe: unresolved sales and
MWK per wave, and attrs['sale_basis_ladder'][unresolved]['grounds'] summed
over the four waves.  No row-level output.

    python ladder_tallies.py <checkout> <out.json>
"""
import importlib.util
import json
import os
import sys
import warnings
from pathlib import Path

checkout = os.path.realpath(sys.argv[1])
import lsms_library  # noqa: E402
assert os.path.realpath(lsms_library.__file__).startswith(checkout + os.sep)
import pandas as pd  # noqa: E402
from lsms_library.paths import countries_root  # noqa: E402

M = countries_root() / 'Malawi' / '_'
sys.path.insert(0, str(M))
spec = importlib.util.spec_from_file_location('malawi_ladder', M / 'malawi.py')
mw = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mw)

out = {'unresolved': {}, 'grounds_by_wave': {}, 'grounds_sum': {}}
for wave in ('2010-11', '2013-14', '2016-17', '2019-20'):
    harvest, sale, _ = mw._crop_pieces(wave)
    harv = mw._consolidate_harvest(harvest)
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        _, _, tallies = mw._attach_sales(wave, harv, pd.concat(sale, ignore_index=True))
    mm = tallies['sale_basis_mismatch']
    out['unresolved'][wave] = [int(mm['sales']), float(mm['value'])]
    g = tallies['sale_basis_ladder'][mw.SALE_BASIS_UNRESOLVED]['grounds']
    out['grounds_by_wave'][wave] = {k: int(v) for k, v in g.items()}
    for k, v in g.items():
        out['grounds_sum'][k] = out['grounds_sum'].get(k, 0) + int(v)
Path(sys.argv[2]).write_text(json.dumps(out, indent=1) + '\n')
print(json.dumps(out, indent=1))
