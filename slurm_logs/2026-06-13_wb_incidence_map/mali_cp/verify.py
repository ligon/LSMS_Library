import warnings; warnings.filterwarnings('ignore')
import pandas as pd
from lsms_library import Country, diagnostics
from lsms_library.diagnostics import is_this_feature_sane
pd.set_option('display.width', 200)

c = Country('Mali')

cp = diagnostics.load_feature(c, 'community_prices')
print("community_prices:", cp.shape, cp.index.names)
san = is_this_feature_sane(cp, 'Mali', 'community_prices')
print("is_this_feature_sane.ok =", san.ok)
if not san.ok:
    print("  FAIL details:", san)
else:
    print("  (sane)")

# Regression: food_acquired + crop_production still build
for feat in ['food_acquired', 'crop_production', 'food_prices']:
    try:
        d = getattr(c, feat)()
        print(f"{feat}: OK shape={d.shape} index={d.index.names}")
    except Exception as e:
        print(f"{feat}: FAIL {type(e).__name__}: {str(e)[:120]}")
