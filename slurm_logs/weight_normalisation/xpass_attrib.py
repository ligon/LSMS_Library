"""Is the GhanaLSS panel-consistency xpass caused by weight normalisation?

Runs check_panel_consistency twice: once with the transform live, once with it
neutralised to the identity (i.e. pre-PR behaviour on the read path).
Same verdict both ways => the xpass is unrelated to this change.
"""
import warnings; warnings.filterwarnings('ignore')
import lsms_library as ll
assert 'worktrees' in ll.__file__, ll.__file__
import lsms_library.country as C
from lsms_library.diagnostics import check_panel_consistency

def verdict(label):
    r = check_panel_consistency(ll.Country("GhanaLSS"))
    names = sorted(c.name for c in r.errors)
    print(f'{label:<22} ok={r.ok}  errors={names}')
    return r.ok, tuple(names)

live = verdict('normalisation LIVE')

orig = C._normalise_sample_weights
C._normalise_sample_weights = lambda df, country=None: df   # pre-PR read path
try:
    ll.Country.__dict__  # no-op
    off = verdict('normalisation OFF')
finally:
    C._normalise_sample_weights = orig

print()
print('SAME VERDICT BOTH WAYS:', live == off)
print('=> xpass is', 'UNRELATED to this PR' if live == off else 'CAUSED BY this PR')
