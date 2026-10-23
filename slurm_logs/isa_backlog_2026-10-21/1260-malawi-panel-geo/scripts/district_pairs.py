import sys, collections
import pandas as pd
wf = pd.read_parquet(sys.argv[1]).reset_index(); s = wf.i.astype(str)
st = wf[s.str.contains('-') & ~s.str.startswith('cs-') & wf.Latitude.notna()]
g = [tuple(sorted(set(map(str, x)))) for _, x in st.groupby('v')['District']]
pairs = collections.Counter(t for t in g if len(t) > 1)
print(len(pairs), 'distinct disagreement patterns; top:', pairs.most_common(6))
