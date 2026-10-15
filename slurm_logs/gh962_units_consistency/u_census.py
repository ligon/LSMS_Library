import glob, os, sys, collections
import pyarrow.parquet as pq, pandas as pd
D='/global/scratch/fsa/fc_jevons/ligon/cache/lsms_library'
SENT={'---','unknown','manquant','value','','nan','none','other'}
rows=[]
for tbl in ('food_acquired','crop_production'):
    for f in sorted(glob.glob(f'{D}/*/var/{tbl}.parquet')):
        c=f.split('/')[-3]
        try:
            sch=pq.read_schema(f)
            if 'u' not in sch.names: rows.append((tbl,c,'(no u)',0,0)); continue
            t=pq.read_table(f,columns=['u']).column('u').to_pandas()
        except Exception as e:
            rows.append((tbl,c,f'ERR {type(e).__name__}',0,0)); continue
        n=len(t); nan=int(t.isna().sum())
        s=t.dropna().astype(str).str.strip()
        low=s.str.lower()
        vc=collections.Counter()
        for k in SENT:
            m=int((low==k).sum())
            if m: vc[k]=m
        digit=int(s.str.match(r'^\s*\d').sum())
        rows.append((tbl,c,n,nan,dict(vc),digit))
for r in rows: print(*r, sep='\t')
