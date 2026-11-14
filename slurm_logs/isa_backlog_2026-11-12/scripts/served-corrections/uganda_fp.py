"""Fingerprint of the L2-country Uganda sample parquet, as tests/generate_baseline.py takes it.

usage: python uganda_fp.py <clone> <out.json>   (run with the arm's PRIVATE_ROOT / LSMS_DATA_DIR)
"""
import importlib.util
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import assert_env  # noqa: E402

clone, out = sys.argv[1], Path(sys.argv[2])
head = assert_env(clone)
import pandas as pd  # noqa: E402
from lsms_library.paths import data_root  # noqa: E402

spec = importlib.util.spec_from_file_location('gb', Path(clone) / 'tests' / 'generate_baseline.py')
gb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gb)
p = Path(data_root()) / 'Uganda' / 'var' / 'sample.parquet'
fp = gb.fingerprint(pd.read_parquet(p))
fixture = json.load(open(Path(clone) / 'tests' / 'fixtures' / 'uganda_baseline.json'))
R = {'head': head, 'fingerprint': fp, 'fixture': fixture.get('var/sample.parquet'),
     'matches_fixture': fp == fixture.get('var/sample.parquet')}
out.write_text(json.dumps(R, indent=1, default=str))
print(json.dumps({k: v for k, v in R.items() if k != 'fixture'}, indent=1, default=str))
