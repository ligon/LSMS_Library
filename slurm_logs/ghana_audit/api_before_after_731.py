"""GH #731 / #705 verification: per-wave `individual_education` attainment counts,
`sample().Rural` counts + cluster-invariance, and the live U6/u6 mapping entries.

Usage: python api_before_after_731.py <label>   (writes <label>.out next to it)
Run with LSMS_COUNTRIES_ROOT pinned to the worktree and a private LSMS_DATA_DIR.
"""
import os, sys, warnings
import pandas as pd

def main():
    label = sys.argv[1] if len(sys.argv) > 1 else 'run'
    from lsms_library.paths import countries_root
    root = countries_root(); assert 'wt-glss-731-705' in str(root), root
    import lsms_library as ll
    warnings.simplefilter('ignore')
    c = ll.Country('GhanaLSS')
    out = []
    # 1. effective mapping dict entries (route evidence): Wave.categorical_mapping
    out.append("== effective harmonize_education entries for U6/u6/L6/l6, per wave (Wave.categorical_mapping) ==")
    for w in c.waves:
        wv = c[w] if hasattr(c, '__getitem__') else None
        try:
            wave = ll.country.Wave(w, w, c)
            tbl = wave.categorical_mapping['harmonize_education']
            d = tbl.set_index('Original Label')['Preferred Label'].to_dict()
            dup = tbl['Original Label'].duplicated(keep=False)
            out.append(f"{w}: U6->{d.get('U6')!r}  u6->{d.get('u6')!r}  L6->{d.get('L6')!r}  rows={len(tbl)}  dup_keys={sorted(tbl.loc[dup,'Original Label'].unique().tolist())}")
        except Exception as e:
            out.append(f"{w}: ERROR {type(e).__name__}: {e}")
    # 2. individual_education per wave
    out.append("\n== individual_education: Educational Attainment value_counts per wave ==")
    ie = c.individual_education()
    out.append(f"shape={ie.shape} index={ie.index.names}")
    vc = ie.groupby(level='t')['Educational Attainment'].value_counts(dropna=False).unstack(0).fillna(0).astype(int)
    out.append(vc.to_string())
    out.append("\nnulls per wave: " + ie['Educational Attainment'].isna().groupby(level='t').sum().to_dict().__str__())
    # 3. sample().Rural per wave + cluster invariance
    out.append("\n== sample(): Rural per wave ==")
    s = c.sample()
    out.append(f"shape={s.shape} index={s.index.names} cols={list(s.columns)}")
    r = s.groupby(level='t')['Rural'].value_counts(dropna=False).unstack(1).fillna(0).astype(int)
    out.append(r.to_string())
    share = s.groupby(level='t')['Rural'].apply(lambda x: round(100*(x=='Urban').sum()/x.notna().sum(),1) if x.notna().sum() else float('nan'))
    out.append("urban share % (of non-null): " + share.to_dict().__str__())
    out.append("\n== Rural constant within cluster v? (n clusters with >1 distinct Rural value, per wave) ==")
    flat = s.reset_index()
    vcol = 'v' if 'v' in flat.columns else None
    if vcol:
        g = flat.dropna(subset=['Rural']).groupby(['t', 'v'])['Rural'].nunique()
        bad = g[g > 1].groupby(level='t').size()
        tot = g.groupby(level='t').size()
        out.append(pd.DataFrame({'clusters': tot, 'mixed': bad}).fillna(0).astype(int).to_string())
    else:
        out.append("no v column in sample()")
    text = '\n'.join(out)
    print(text)
    here = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(here, f'api_{label}_731.out'), 'w') as f:
        f.write(text + '\n')

if __name__ == '__main__':
    main()
