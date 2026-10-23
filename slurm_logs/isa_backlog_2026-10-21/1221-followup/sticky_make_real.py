"""Make backend, warm var/ from a default-backend build: first make -n probe faults.
Same instance asked again, then a populated request on the same instance, then a fresh instance."""
import os, sys, warnings
ck, country, table, ew, pw = sys.argv[1:6]
import lsms_library as ll
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(ck) + os.sep), ll.__file__
import lsms_library.country as C
warnings.simplefilter('ignore')
assert os.environ.get('LSMS_BUILD_BACKEND') == 'make'
def tc(df):
    return df.index.get_level_values('t').astype(str).value_counts().to_dict() if len(df) else {}
real_run = C.subprocess.run; probes = []
def run(command, *a, **k):
    if isinstance(command, list) and command[:2] == ['make', '-n']:
        probes.append(1)
        if len(probes) == 1:
            raise OSError(11, 'Resource temporarily unavailable (injected)')
    return real_run(command, *a, **k)
C.subprocess.run = run
c = ll.Country(country)
for label, f in [(f'1 {ew} faulted', lambda: getattr(c, table)(waves=[ew])),
                 (f'2 {ew} same instance', lambda: getattr(c, table)(waves=[ew])),
                 (f'3 {pw} same instance', lambda: getattr(c, table)(waves=[pw])),
                 (f'4 {ew} fresh instance', lambda: getattr(ll.Country(country), table)(waves=[ew]))]:
    try:
        df = f(); print(label, 'rows', len(df), 'by t', tc(df))
    except Exception as e:
        print(label, 'RAISED', type(e).__name__, str(e)[:100])
print('make -n probes', len(probes))
