import sys, json, warnings
warnings.filterwarnings('ignore')
import lsms_library
print('lsms_library:', lsms_library.__file__)
from lsms_library.country import Country
from lsms_library._build_registry import build_transforms_fingerprint
PAIRS = [('Tanzania','crop_production'), ('Tanzania','household_roster'), ('Tanzania','sample'),
         ('Uganda','crop_production'), ('Uganda','food_acquired'), ('Uganda','household_roster'),
         ('Malawi','crop_production'), ('Nigeria','crop_production'), ('Ethiopia','crop_production'),
         ('Niger','food_acquired'), ('GhanaLSS','sample'), ('Albania','household_roster')]
out = {}
for c, t in PAIRS:
    try:
        C = Country(c)
        out[f'{c}/{t}'] = C._table_cache_hash(t, list(C.waves))
    except Exception as e:
        out[f'{c}/{t}'] = f'ERR {type(e).__name__}: {e}'
for t in ['crop_production','household_roster','food_acquired','sample','cluster_features']:
    try:
        out[f'btf:{t}'] = build_transforms_fingerprint(t)
    except Exception as e:
        out[f'btf:{t}'] = f'ERR {type(e).__name__}: {e}'
print(json.dumps(out, indent=1, sort_keys=True))
