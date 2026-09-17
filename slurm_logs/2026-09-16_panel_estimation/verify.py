import warnings; warnings.filterwarnings("ignore")
import lsms_library as ll
for c in ('Nigeria','Tanzania'):
    print("="*60); print(c)
    C=ll.Country(c)
    for t in ("crop_production","plot_features","plot_inputs","plot_labor"):
        d=getattr(C,t)()
        f=d.index.to_frame(index=False)
        print(f"  {t:<16} t = {sorted(f['t'].unique())}", flush=True)
    cp=getattr(C,'crop_production')()
    f=cp.index.to_frame(index=False)
    print("  crop_production hh per t:", f.groupby('t')['i'].nunique().to_dict(), flush=True)
    # do the same households recur across t?
    s=f[['i','t']].drop_duplicates()
    print("  hh appearing in >1 t:", int((s.groupby('i')['t'].nunique()>1).sum()), flush=True)
