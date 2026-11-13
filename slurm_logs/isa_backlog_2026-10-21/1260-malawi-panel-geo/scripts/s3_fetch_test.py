"""Fetch the new blobs through get_dataframe in a FRESH data root; prove they came via the L1 S3 pull."""
import os, sys, hashlib, subprocess
import lsms_library
top = subprocess.check_output(['git', '-C', '/local/job40390255/worktrees/1260', 'rev-parse', '--show-toplevel'], text=True).strip()
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(top) + os.sep), lsms_library.__file__
from lsms_library.local_tools import get_dataframe
from lsms_library.paths import data_root
dr = data_root()
print('data_root', dr)
cases = [('2016-17', 'HouseholdGeovariablesIHPSY3.dta', 'cd3970675b30de62f619b23a8f99b876'),
         ('2019-20', 'householdgeovariables_y4.dta', '13ffd98972e697a2fbb0ab62463b0ba3')]
base = '/local/job40390255/worktrees/1260/lsms_library/countries/Malawi'
for wave, fn, md5 in cases:
    intree = f'{base}/{wave}/Data/Panel/{fn}'
    assert not os.path.exists(intree), intree
    blob_paths = [dr / 'dvc-cache' / md5[:2] / md5[2:], dr / 'dvc-cache' / 'files' / 'md5' / md5[:2] / md5[2:]]
    before = [p.exists() for p in blob_paths]
    os.chdir(f'{base}/{wave}/_')
    df = get_dataframe(f'../Data/Panel/{fn}')
    after = [p for p in blob_paths if p.exists()]
    got = hashlib.md5(after[0].read_bytes()).hexdigest() if after else None
    print(wave, fn, 'shape', df.shape, '| L1 blob before', before, '| L1 blob after', [str(p.relative_to(dr)) for p in after],
          '| blob md5 matches sidecar', got == md5, '| in-tree file still absent', not os.path.exists(intree))
