"""Coverage + comparability analysis over the extracted frames."""
import sys, os, json, itertools
import pandas as pd, numpy as np
SCRATCH = os.environ['SCRATCH']; EX = os.path.join(SCRATCH,'extracts')
pd.set_option('display.width', 200)

# per-unit price recipe: how to get currency-per-ONE-native-unit from the stored columns
RECIPE = {
 'Malawi':      ('Price','NumberOfUnits'),
 'Nigeria':     ('Price', None),
 'Ethiopia':    ('Price','Quantity'),
 'EthiopiaRHS': ('Price', None),
 'Mali':        ('Price','Quantity'),
 'Niger':       ('Price','Quantity'),
 'Tanzania':    ('Price', None),
 'GhanaLSS':    ('Price','NumberOfUnits'),
}
COUNTRIES = list(RECIPE)

def load(c, t):
    p = os.path.join(EX, f"{c}__{t}.parquet")
    return pd.read_parquet(p) if os.path.exists(p) else None

def cp_unit_price(c, cp):
    num, den = RECIPE[c]
    p = cp[num].astype(float)
    if den is not None and den in cp.columns:
        d = cp[den].astype(float)
        p = p / d.where(d > 0)
    out = p.rename('cp_price').to_frame()
    # collapse any extra levels (e.g. GhanaLSS obs) to (t,v,j,u) by MEDIAN,
    # a transformation done HERE for comparison, never stored.
    keys = [k for k in ('t','v','j','u') if k in out.index.names]
    g = out.groupby(level=keys, observed=True)['cp_price']
    return pd.DataFrame({'cp_price': g.median(), 'cp_n': g.size()})

def hh_unit_value(fa):
    """Household unit value = Expenditure/Quantity, purchased rows, at (t,v,j,u)."""
    df = fa
    if 's' in df.index.names:
        s = df.index.get_level_values('s').astype(str)
        df = df[s == 'purchased']
    if not {'Expenditure','Quantity'} <= set(df.columns):
        return None
    q = df['Quantity'].astype(float); e = df['Expenditure'].astype(float)
    uv = (e / q.where(q > 0)).replace([np.inf,-np.inf], np.nan)
    uv = uv[uv > 0].rename('hh_uv').to_frame()
    keys = [k for k in ('t','v','j','u') if k in uv.index.names]
    if 'v' not in keys: return None, keys
    g = uv.groupby(level=keys, observed=True)['hh_uv']
    return pd.DataFrame({'hh_uv': g.median(), 'hh_n': g.size()}), keys

def qsum(x):
    x = x.dropna()
    if len(x)==0: return {}
    lr = np.log(x)
    return dict(n=len(x), p10=x.quantile(.10), p25=x.quantile(.25),
                med=x.median(), p75=x.quantile(.75), p90=x.quantile(.90),
                within25=float(((x>=0.8)&(x<=1.25)).mean()),
                logdev1=float((lr.abs()>1).mean()))

rows_cov, rows_cmp, rows_cmp_tju, notes = [], [], [], {}
for c in COUNTRIES:
    cp = load(c,'community_prices')
    if cp is None:
        notes[c] = 'no community_prices extract'; continue
    cpu = cp_unit_price(c, cp)
    # coverage rows
    for t, sub in cp.groupby(level='t', observed=True):
        rows_cov.append(dict(country=c, wave=t, rows=len(sub),
            n_v=sub.index.get_level_values('v').nunique(),
            n_j=sub.index.get_level_values('j').nunique(),
            n_u=sub.index.get_level_values('u').nunique(),
            extra_levels=','.join([l for l in sub.index.names if l not in ('t','v','j','u')]) or '-'))
    fa = load(c,'food_acquired')
    if fa is None:
        notes[c] = notes.get(c,'') + ' no food_acquired'; continue
    res = hh_unit_value(fa)
    if res is None or (isinstance(res,tuple) and res[0] is None):
        notes[c] = notes.get(c,'') + f' hh unit value unavailable (idx={list(fa.index.names)}, cols={list(fa.columns)})'
        continue
    hh, keys = res
    # --- join on (t, v, j, u)
    j4 = hh.join(cpu, how='inner')
    j4 = j4[(j4['hh_uv']>0) & (j4['cp_price']>0)]
    j4['ratio'] = j4['hh_uv']/j4['cp_price']
    for t, sub in j4.groupby(level='t', observed=True):
        d = dict(country=c, wave=t, join='t,v,j,u', **qsum(sub['ratio']))
        rows_cmp.append(d)
    # --- join on (t, j, u)  (Nigeria precedent; wider overlap)
    hh3 = hh.groupby(level=[k for k in ('t','j','u') if k in hh.index.names], observed=True)['hh_uv'].median()
    cp3 = cpu.groupby(level=[k for k in ('t','j','u') if k in cpu.index.names], observed=True)['cp_price'].median()
    j3 = pd.concat([hh3, cp3], axis=1, join='inner').dropna()
    j3 = j3[(j3['hh_uv']>0)&(j3['cp_price']>0)]
    j3['ratio'] = j3['hh_uv']/j3['cp_price']
    for t, sub in j3.groupby(level='t', observed=True):
        rows_cmp_tju.append(dict(country=c, wave=t, join='t,j,u', **qsum(sub['ratio'])))
    # overlap accounting
    notes[c] = (notes.get(c,'') +
        f" | cp cells={len(cpu)} hh cells={len(hh)} matched(t,v,j,u)={len(j4)}"
        f" cp_matched_frac={len(j4)/max(len(cpu),1):.3f} hh_matched_frac={len(j4)/max(len(hh),1):.3f}"
        f" | (t,j,u) cp={len(cp3)} hh={len(hh3)} matched={len(j3)}")

cov = pd.DataFrame(rows_cov)
cmp4 = pd.DataFrame(rows_cmp)
cmp3 = pd.DataFrame(rows_cmp_tju)
cov.to_csv(os.path.join(SCRATCH,'coverage.csv'), index=False)
cmp4.to_csv(os.path.join(SCRATCH,'ratio_tvju.csv'), index=False)
cmp3.to_csv(os.path.join(SCRATCH,'ratio_tju.csv'), index=False)
json.dump(notes, open(os.path.join(SCRATCH,'notes.json'),'w'), indent=1)
print("=== COVERAGE ==="); print(cov.to_string(index=False))
print("\n=== RATIO at (t,v,j,u) ==="); print(cmp4.to_string(index=False))
print("\n=== RATIO at (t,j,u) ==="); print(cmp3.to_string(index=False))
print("\n=== NOTES ===")
for k,v in notes.items(): print(f"{k}: {v}")

# ---------------------------------------------------------------- axis + join
rows_axis=[]
for c in COUNTRIES:
    cp = load(c,'community_prices'); fa = load(c,'food_acquired'); sm = load(c,'sample')
    if cp is None: continue
    cpj = set(map(str, cp.index.get_level_values('j').unique()))
    cpu = set(map(str, cp.index.get_level_values('u').unique()))
    cpv = set(map(str, cp.index.get_level_values('v').unique()))
    faj = set(map(str, fa.index.get_level_values('j').unique())) if fa is not None and 'j' in fa.index.names else set()
    fau = set(map(str, fa.index.get_level_values('u').unique())) if fa is not None and 'u' in fa.index.names else set()
    if sm is not None and 'v' in sm.columns:
        smv = set(map(str, sm['v'].dropna().unique()))
        hh_in_priced = float(sm['v'].astype(str).isin(cpv).mean())
    else:
        smv, hh_in_priced = set(), float('nan')
    rows_axis.append(dict(country=c,
        cp_j=len(cpj), j_in_fa=len(cpj & faj), j_share=len(cpj & faj)/max(len(cpj),1),
        cp_u=len(cpu), u_in_fa=len(cpu & fau), u_share=len(cpu & fau)/max(len(cpu),1),
        cp_v=len(cpv), v_in_sample=len(cpv & smv), v_share=len(cpv & smv)/max(len(cpv),1),
        hh_rows_in_priced_cluster=hh_in_priced))
ax = pd.DataFrame(rows_axis)
ax.to_csv(os.path.join(SCRATCH,'axes.csv'), index=False)
print("\n=== AXES / JOIN ===");
print(ax.to_string(index=False))

# ---------------------------------------------------------------- u='Value' (#770)
rows_val=[]
for c in COUNTRIES:
    fa = load(c,'food_acquired')
    if fa is None or 'u' not in fa.index.names: continue
    u = pd.Series(fa.index.get_level_values('u').astype(str), index=range(len(fa)))
    t = pd.Series(fa.index.get_level_values('t').astype(str), index=range(len(fa)))
    isval = u.str.lower().eq('value')
    cp = load(c,'community_prices')
    cpw = set(map(str, cp.index.get_level_values('t').unique())) if cp is not None else set()
    for w, grp in isval.groupby(t):
        rows_val.append(dict(country=c, wave=w, rows=int(len(grp)),
                             value_rows=int(grp.sum()), value_share=float(grp.mean()),
                             has_community_price=(w in cpw)))
val = pd.DataFrame(rows_val)
val.to_csv(os.path.join(SCRATCH,'value_rows.csv'), index=False)
print("\n=== u='Value' rows per (country, wave) ===")
print(val[val.value_rows>0].to_string(index=False))

# ------------------------------------------------- reported hh Price coverage
rows_pr=[]
for c in COUNTRIES:
    fa = load(c,'food_acquired')
    if fa is None: continue
    has = 'Price' in fa.columns
    frac = float(fa['Price'].notna().mean()) if has else 0.0
    rows_pr.append(dict(country=c, fa_cols=','.join(fa.columns),
                        has_reported_Price=has, Price_nonnull_frac=round(frac,4),
                        rows=len(fa)))
pr = pd.DataFrame(rows_pr); pr.to_csv(os.path.join(SCRATCH,'hh_price_col.csv'), index=False)
print("\n=== food_acquired reported Price column ==="); print(pr.to_string(index=False))
