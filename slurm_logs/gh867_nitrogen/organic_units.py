import re, warnings, json, sys
import pandas as pd
import lsms_library as ll
from lsms_library import transformations as T
warnings.simplefilter('ignore')
PAT = re.compile(r'\b(?:org[aâ]ni|manure|compost|estrume|fumier)', re.I)
for c in ['Benin','Burkina_Faso','CotedIvoire','Ethiopia','GhanaLSS','Guinea-Bissau','Malawi','Mali','Nigeria','Niger','Senegal','Tanzania','Togo','Uganda']:
    pi = ll.Country(c).plot_inputs()
    inp = pi.index.get_level_values('input').astype(str)
    m = pd.Series(inp).str.contains(PAT).values
    sub = pi[m]
    u = sub['u'] if 'u' in sub.columns else sub.index.get_level_values('u').to_series(index=sub.index)
    q = pd.to_numeric(sub['Quantity'], errors='coerce')
    if 'u' in sub.index.names:
        kg = T._kg_factor_series(sub)
    else:
        kg = T._kg_factor_series(sub.set_index('u', append=True)); kg.index = sub.index
    conv = (q.notna() & kg.notna()).sum()
    print(f"{c}: organic rows={m.sum():,} qty_notna={int(q.notna().sum()):,} kg_convertible={int(conv):,} "
          f"qty==0:{int((q==0).sum()):,} labels={sorted(set(inp[m]))}")
    print('   units:', dict(pd.Series(u.astype(str).values)[q.notna().values].value_counts().head(8)))
    print('   kg (convertible rows):', round(float((q*kg).sum()),1))
