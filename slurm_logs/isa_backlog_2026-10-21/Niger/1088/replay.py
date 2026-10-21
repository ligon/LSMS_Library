"""Replay GH #1088 from copied held L1 blobs, without acquiring any source.

Run from the checkout with PYTHONPATH and LSMS_COUNTRIES_ROOT set to it;
see REPORT.md. Output contains aggregate counts only.
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess

import pandas as pd
import yaml


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--out', required=True, type=Path)
parser.add_argument('--held-cache', type=Path,
                    default=Path.home() / '.local/share/lsms_library/dvc-cache')
args = parser.parse_args()
root = Path(subprocess.check_output(
    ['git', 'rev-parse', '--show-toplevel'], text=True).strip()).resolve()
assert Path.cwd().resolve() == root
assert os.environ['PYTHONPATH'] == str(root)
assert os.environ['LSMS_COUNTRIES_ROOT'] == str(root / 'lsms_library/countries')
assert os.environ.get('LSMS_SKIP_AUTH') == '1'
data = args.out.resolve() / 'data'
assert not data.exists(), 'Use a new private output directory'
os.environ['LSMS_DATA_DIR'] = str(data)
sources = [
    'Niger/2011-12/Data/NER_2011_ECVMA_v01_M_Stata8/ecvmacoms07_p1.dta',
    'Niger/2011-12/Data/NER_2011_ECVMA_v01_M_Stata8/ecvmacoms07_p2.dta',
    'Niger/2014-15/Data/NER_2014_ECVMA-II_v02_M_STATA8/comprixcs07.dta',
]
for rel in sources:
    sidecar = root / 'lsms_library/countries' / (rel + '.dvc')
    md5 = yaml.safe_load(sidecar.read_text())['outs'][0]['md5']
    candidates = [args.held_cache / md5[:2] / md5[2:],
                  args.held_cache / 'files/md5' / md5[:2] / md5[2:]]
    held = next((f for f in candidates if f.is_file()), None)
    assert held is not None, 'Missing held source; acquisition is forbidden'
    assert hashlib.md5(held.read_bytes()).hexdigest() == md5
    target = data / 'dvc-cache' / md5[:2] / md5[2:]
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(held, target)
    assert hashlib.md5(target.read_bytes()).hexdigest() == md5


def no_network(self, address):
    raise AssertionError('Held-only replay forbids network connections')


socket.socket.connect = no_network
import lsms_library as ll  # noqa: E402
from lsms_library import local_tools as tools  # noqa: E402

assert Path(ll.__file__).resolve().is_relative_to(root)
country = ll.Country('Niger')
baseline = args.out / 'baseline_niger.py'
baseline.write_bytes(subprocess.check_output([
    'git', 'show', 'f8d236694:lsms_library/countries/Niger/_/niger.py']))
modules = []
for name, path in [('baseline', baseline),
                   ('current', root / 'lsms_library/countries/Niger/_/niger.py')]:
    spec = importlib.util.spec_from_file_location('niger1088_' + name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    modules.append(module)
maps = [tools.get_categorical_mapping(
    tablename=name, idxvars='Original Label',
    dirs=[str(root / 'lsms_library/countries/Niger/_')],
    **{'Preferred Label': 'Preferred Label'}) for name in ('harmonize_food', 'u')]
pieces = [[], []]
for i, rel in enumerate(sources):
    codes = tools.get_dataframe(rel, convert_categoricals=False)
    labels = tools.get_dataframe(rel, convert_categoricals=True)
    triples = ([('cs07q03', 'cs07q04', 'cs07q05'),
                ('cs07q06', 'cs07q07', 'cs07q08'),
                ('cs07q09', 'cs07q10', 'cs07q11')] if i < 2 else
               [('cs07q04', 'cs07q05', 'cs07q03'),
                ('cs07q06', 'cs07q07', 'cs07q03'),
                ('cs07q08', 'cs07q09', 'cs07q03')])
    for module, frames in zip(modules, pieces):
        long = module._community_price_triples(
            labels, *maps, triples, passage=i + 1 if i < 2 else 1, codes=codes)
        frames.append(module._finish_community_prices(long, rel.split('/')[1]))
before, after = [country._finalize_result(
    pd.concat(frames), country._materialization_entry('community_prices'),
    'community_prices') for frames in pieces]
assert before.index.equals(after.index)
pd.testing.assert_frame_equal(before.drop(columns='Quantity'),
                              after.drop(columns='Quantity'))
changed = before.Quantity.notna() & after.Quantity.isna()
assert int(changed.sum()) == 76
assert before.Quantity[changed].eq(9999.900390625).all()
pd.testing.assert_series_equal(before.Quantity[~changed], after.Quantity[~changed])
assert not after.Quantity.eq(9999.900390625).any()
print(json.dumps({'rows': len(after), 'rows_by_wave': after.groupby(level='t').size().to_dict(),
                  'quantity_to_na': int(changed.sum()), 'keys_and_other_columns_equal': True,
                  'later_records': after[after.index.get_level_values('obs') > 3]
                  .groupby(level='t').size().to_dict()}, indent=2))
