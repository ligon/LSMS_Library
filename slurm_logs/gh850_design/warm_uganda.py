"""Build Uganda food_acquired into the SCRATCH LSMS_DATA_DIR (never the shared cache)."""
import os, time, lsms_library as ll
assert 'wt-850-design' in ll.__file__, ll.__file__
assert 'gh850_data' in os.environ.get('LSMS_DATA_DIR', ''), os.environ.get('LSMS_DATA_DIR')
t0 = time.time()
df = ll.Country('Uganda').food_acquired()
print('rows', len(df), 'idx', df.index.names, 'cols', list(df.columns), f'{time.time()-t0:.0f}s')
