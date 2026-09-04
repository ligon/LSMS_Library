import sys, os, warnings
sys.path.insert(0, os.environ['WT'])
import lsms_library
assert 'wt-gsps-inputs' in lsms_library.__file__, lsms_library.__file__
from lsms_library.paths import countries_root, data_root
assert str(countries_root()).startswith(os.environ['WT'])
assert str(data_root()).startswith(os.environ['SCRATCH'])
from lsms_library.feature import Feature
import pandas as pd
CS = ['Ethiopia','EthiopiaRHS','GhanaLSS','Malawi','Mali','Niger','Nigeria','Tanzania']
for subset in ([c for c in CS if c!='GhanaLSS'], ['GhanaLSS','Malawi'], CS):
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter('always')
        try:
            df = Feature('community_prices')(subset)
            msgs = [str(x.message)[:220] for x in w if 'community' in str(x.message).lower() or 'exclud' in str(x.message).lower() or 'index' in str(x.message).lower()]
            print(f"subset={subset}\n  shape={df.shape} idx={list(df.index.names)}")
            print(f"  countries kept: {sorted(set(df.index.get_level_values('country')))}")
            for m in msgs[:4]: print("  WARN:", m)
        except Exception as e:
            print(f"subset={subset} -> {type(e).__name__}: {e}")
    print()
