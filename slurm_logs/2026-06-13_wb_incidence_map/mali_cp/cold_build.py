import warnings; warnings.filterwarnings('ignore')
import pandas as pd
from lsms_library import Country, diagnostics
pd.set_option('display.max_columns', 30); pd.set_option('display.width', 220)

c = Country('Mali')
df = diagnostics.load_feature(c, 'community_prices')
print("=== community_prices (cold framework build) ===")
print("shape:", df.shape)
print("index names:", df.index.names)
print("columns:", list(df.columns))
print("unique index:", df.index.is_unique)
r = df.reset_index()
print("v count:", r['v'].nunique(), "| j count:", r['j'].nunique(), "| u count:", r['u'].nunique())
print("t values:", sorted(r['t'].astype(str).unique()))
print(df.head(8).to_string())

# v intersect sample()
s = c.sample().reset_index()
sv = set(s[s['t'].astype(str) == '2014-15']['v'].dropna().astype(str))
cpv = set(r['v'].astype(str))
print(f"\nv intersect sample: {len(cpv & sv)}/{len(cpv)} community-price clusters join sample()")

# j sample matches food_acquired Preferred Labels?
fa = c.food_acquired().reset_index()
fa_j = set(fa['j'].dropna().astype(str))
cp_j = set(r['j'].dropna().astype(str))
print("community_prices j count:", len(cp_j),
      "| of which also in food_acquired.j:", len(cp_j & fa_j))
print("sample shared j:", sorted(cp_j & fa_j)[:12])
print("community-only j (not in food_acquired):", sorted(cp_j - fa_j))

# sanity
try:
    san = diagnostics.is_this_feature_sane(c, 'community_prices')
except AttributeError:
    from lsms_library.diagnostics import is_this_feature_sane
    san = is_this_feature_sane(c, 'community_prices')
print("\n=== is_this_feature_sane ===")
print("ok:", getattr(san, 'ok', san))
print(san)
