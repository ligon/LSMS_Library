import pickle, sys, hashlib
import pandas as pd
a = pickle.load(open(sys.argv[1], 'rb')); b = pickle.load(open(sys.argv[2], 'rb'))
print('unit_map identical:', a['unit_map'] == b['unit_map'], len(a['unit_map']))
fa, fb = a['food_acquired'], b['food_acquired']
pd.testing.assert_frame_equal(fa, fb, check_exact=True)
print('food_acquired frame-identical (assert_frame_equal check_exact):', fa.shape)
for name, f in (('before', fa), ('after', fb)):
    t = f.index.get_level_values('t'); u = f.index.get_level_values('u')
    s = f[t == '2014-15']
    vc = s.index.get_level_values('u').value_counts(dropna=False)
    h = hashlib.sha256(pd.util.hash_pandas_object(s, index=True).values.tobytes()).hexdigest()[:16]
    print(name, '2014-15 rows', len(s), 'u NaN', int(pd.isna(s.index.get_level_values('u')).sum()), 'sha', h)
print('2014-15 u value_counts (after):')
f = fb; t = f.index.get_level_values('t')
print(f[t == '2014-15'].index.get_level_values('u').value_counts(dropna=False).to_string())
print('attrs equal:', fa.attrs == fb.attrs, '| dtypes equal:', (fa.dtypes == fb.dtypes).all(), '| index dtypes:', fa.index.dtypes.equals(fb.index.dtypes))
print('warnings equal:', a['warnings'] == b['warnings'], len(a['warnings']))
