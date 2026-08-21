import pandas as pd
from lsms_library.local_tools import get_dataframe

fn = 'lsms_library/countries/Senegal/2018-19/Data/s00_me_sen2018.dta'
df = get_dataframe(fn)
cols = [c for c in df.columns if 's00q23' in c.lower()]
print('s00q23* columns:', cols)
for c in cols:
    s = df[c]
    nn = s.dropna()
    print(f'\n=== {c} === dtype={s.dtype} nonnull={len(nn)}/{len(s)}')
    print('  sample:', list(nn.unique()[:8]))

# Stata variable labels via pyreadstat on the materialized blob
try:
    import pyreadstat
    from lsms_library.local_tools import _ensure_dvc_pulled
    path = _ensure_dvc_pulled(fn)
    _, meta = pyreadstat.read_dta(str(path), metadataonly=True)
    print('\n=== variable labels (s00q23*) ===')
    for c in cols:
        print(f'  {c}: {meta.column_names_to_labels.get(c)!r}')
    # also any date-ish labels
    print('\n=== any var whose LABEL mentions date/visite/passage/enquêt ===')
    for var, lab in meta.column_names_to_labels.items():
        if lab and any(k in lab.lower() for k in ['date','visite','passage','enquêt','enquet','interview']):
            print(f'  {var}: {lab!r}')
except Exception as e:
    print('label introspection failed:', repr(e))
