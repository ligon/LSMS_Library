"""Site R firing rate on the REAL cold build path, with the guard live.

The file-level sweep could not see reads of intermediate wave parquets (they do
not exist until a build creates them).  This closes that gap: build every table
of a few representative countries from a genuinely cold L2, with the guard
active, and count what Site R actually emits.

Usage: python siteR_live.py <country> [<country> ...]
"""
import sys, warnings, json, time

import lsms_library as ll
assert 'worktrees' in ll.__file__, ll.__file__
from lsms_library.country import Country
from lsms_library.null_read_audit import NullReadWarning

SKIP = {'panel_ids', 'updated_ids'}

site_r, site_b, unrelated = [], [], 0
for cname in sys.argv[1:]:
    C = Country(cname)
    for table in [t for t in C.data_scheme if t not in SKIP]:
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            try:
                getattr(C, table)()
            except Exception as e:
                print(f'  {cname}/{table}: ERR {type(e).__name__}: {str(e)[:90]}')
        for rec in w:
            if not issubclass(rec.category, NullReadWarning):
                unrelated += 1
                continue
            msg = str(rec.message)
            if msg.startswith('read of ') or ': read of ' in msg:
                site_r.append((cname, table, msg[:170]))
            else:
                site_b.append((cname, table, msg[:110]))
    print(f'{cname}: done | cumulative SiteR={len(site_r)} SiteB={len(site_b)}', flush=True)

print()
print(f'SITE R firings on the live cold build path: {len(site_r)}')
for c, t, m in site_r[:30]:
    print(f'   {c}/{t}: {m}')
print(f'SITE B firings: {len(site_b)} (distinct: '
      f'{len(set((c, m) for c, _, m in site_b))})')
print(f'warnings from everything else (PerformanceWarning, grain, ...): {unrelated}')
