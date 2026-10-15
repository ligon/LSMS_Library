import sys; sys.path.insert(0,'/local/job39002769/mirrors/LSMS_Library')
from lsms_library.local_tools import all_dfs_from_orgfile
from pathlib import Path
R=Path('/local/job39002769/mirrors/LSMS_Library')
fs=list((R/'lsms_library/categorical_mapping').glob('*.org'))+list((R/'lsms_library/countries').glob('*/_/categorical_mapping.org'))+list((R/'lsms_library/countries').glob('*/*/_/categorical_mapping.org'))
empty=[f for f in fs if not all_dfs_from_orgfile(f)]
print("files:",len(fs),"empty:",[str(f.relative_to(R)) for f in empty])
for f in (R/'lsms_library/categorical_mapping').glob('*.org'):
    print(f.name, list(all_dfs_from_orgfile(f)))
