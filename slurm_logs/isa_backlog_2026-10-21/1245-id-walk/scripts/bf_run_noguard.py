"""Regenerate Burkina's crosswalk in SCRATCH with the chain guard removed.
Run with cwd = <worktree>/lsms_library/countries/Burkina_Faso/_ ."""
import sys, os
from pathlib import Path
here = Path(os.getcwd())
src = (here / 'panel_ids.py').read_text()
old_start = src.index("cur_set = set(df['i'].dropna())")
old_end = src.index("updated_ids['2021-22'] = filtered_21")
new = ("cur_set = set(df['i'].dropna())\n"
       "filtered_21 = dict(updated_ids.get('2021-22', {}))\n"
       "skipped_chains = 0\n")
src = src[:old_start] + new + src[old_end:]
scratch = Path(sys.argv[1])
fake = scratch / 'panel_ids.py'
fake.write_text(src)
sys.path.insert(0, str(here))
g = {'__file__': str(fake), '__name__': '__main__'}
exec(compile(src, str(fake), 'exec'), g)
