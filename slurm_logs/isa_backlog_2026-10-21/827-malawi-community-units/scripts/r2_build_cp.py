"""Cold-build Malawi community_prices; write frame (private dir) + attrs json.
argv: checkout, out_parquet, out_attrs_json"""
import os, sys, json, time
checkout, out, out_attrs = sys.argv[1:4]
import lsms_library
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(checkout) + os.sep), lsms_library.__file__
print('lsms_library from', lsms_library.__file__, 'DATA', os.environ.get('LSMS_DATA_DIR'), flush=True)
import lsms_library as ll
t0 = time.time()
df = ll.Country('Malawi').community_prices()
print('built in', round(time.time() - t0, 1), 's', df.shape, df.index.names, list(df.columns), flush=True)
attrs = {k: v for k, v in df.attrs.items()}
json.dump(attrs, open(out_attrs, 'w'), indent=1, default=str)
print('attrs keys', list(attrs), flush=True)
print('derivations attrs', json.dumps(attrs.get('derivations'), default=str), flush=True)
df.to_parquet(out)
print('wrote', out, flush=True)
