"""Raw-source oracle for GH #1250: classify every AGSEC3A/3B row by its use-flag
and its day cells, from the RAW float codes (independent of uganda._to_int_code
and of _labor_persondays).  Key construction mirrors plot_labor_for_wave.
Writes aggregate counts only."""
import json, os, sys
import numpy as np, pandas as pd
import lsms_library
top = os.environ['CHECKOUT']
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(top) + os.sep), lsms_library.__file__
from lsms_library.local_tools import get_dataframe, format_id
root = os.path.join(top, 'lsms_library/countries/Uganda')
sys.path.insert(0, os.path.join(root, '_'))
import uganda
FILES = {'2019-20': ('Data/Agric/agsec3a.dta', 'Data/Agric/agsec3b.dta')}
out = {}
key_rows = []
for t, cmap in uganda.LABOR_COLMAPS.items():
    fa, fb = FILES.get(t, ('Data/AGSEC3A.dta', 'Data/AGSEC3B.dta'))
    for season, f in (('A', fa), ('B', fb)):
        cm = cmap.get(season)
        if not cm:
            continue
        df = get_dataframe(os.path.join(root, t, f), convert_categoricals=False)
        hh = uganda._format_agsec_hhid(df[cm['hhid']], t)
        parcel = df[cm['parcel']].apply(format_id)
        plot = df[cm['plot']].apply(format_id)
        pid = hh.astype(str) + '-' + parcel.astype(str) + '-' + plot.astype(str)
        pid_na = pid.isna()
        wage = pd.to_numeric(df[cm['wage']], errors='coerce') if cm.get('wage') else None
        for source, cells_key, flag_key, code in (('family', 'family', 'family_flag', 0),
                                                  ('hired', 'hired', 'hired_flag', 2),
                                                  ('other', 'other', None, None)):
            if not cm.get(cells_key):
                continue
            cols = [c for c in cm[cells_key] if c in df.columns]
            block = df[cols].apply(lambda s: pd.to_numeric(s, errors='coerce'))
            any_cell = block.notna().any(axis=1)
            ssum = block.sum(axis=1, min_count=1)
            fk = cm.get(flag_key) if flag_key else None
            rec = {'rows': int(len(df)), 'flag_col': fk, 'flag_code_no': code,
                   'plot_id_nan_rows': int(pid_na.sum())}
            if fk:
                flag = pd.to_numeric(df[fk], errors='coerce')  # raw float codes
                rec['flag_value_counts'] = {str(k): int(v) for k, v in flag.value_counts(dropna=False).sort_index().items()}
                no, na = (flag == code), flag.isna()
                oth = flag.notna() & ~no
                for name, m in (('no', no), ('na', na), ('other', oth)):
                    for cname, cm2 in (('cells_empty', ~any_cell), ('cells_present', any_cell)):
                        sel = m & cm2
                        rec[f'flag_{name}&{cname}'] = {
                            'n': int(sel.sum()),
                            'plot_nan': int((sel & pid_na).sum()),
                            'plot_valid': int((sel & ~pid_na).sum())}
                        if cname == 'cells_present':
                            rec[f'flag_{name}&{cname}']['positive_sum'] = int((sel & (ssum > 0)).sum())
                            rec[f'flag_{name}&{cname}']['days_sum'] = float(ssum[sel].sum())
                if source == 'hired' and wage is not None:
                    rec['flag_na&cells_empty&wage_reported'] = int((na & ~any_cell & wage.notna()).sum())
                    rec['flag_no&cells_empty&wage_reported'] = int((no & ~any_cell & wage.notna()).sum())
                # what each version would put into the builder (pre-groupby)
                old = ssum.where(~(flag.notna() & (flag == code)) & ~na, 0.0)  # NA flag -> 0 (dev semantics)
                old = old.where(~no, 0.0)
                new = ssum.mask(no, 0.0)
            else:
                old = new = ssum
            for ver, v in (('old', old), ('new', new)):
                kr = pd.DataFrame({'t': t, 'i': hh.values, 'plot_id': pid.values, 'source': source,
                                   'season': season, 'PersonDays': v.values, 'ver': ver})
                key_rows.append(kr[kr['PersonDays'].notna()])
            out[f'{t}/{season}/{source}'] = rec
kr = pd.concat(key_rows, ignore_index=True)
summ = {}
for ver in ('old', 'new'):
    k = kr[kr['ver'] == ver]
    g = k.groupby(['t', 'i', 'plot_id', 'source', 'season'], dropna=False)['PersonDays']
    served = g.max().reset_index()
    nrows = g.size().reset_index(name='n')
    served['plot_nan'] = served['plot_id'].isna()
    nrows['plot_nan'] = nrows['plot_id'].isna()
    by = served.groupby(['t', 'season', 'source', 'plot_nan']).agg(
        rows=('PersonDays', 'size'), zeros=('PersonDays', lambda s: int((s == 0).sum())),
        positive=('PersonDays', lambda s: int((s > 0).sum())), total=('PersonDays', 'sum'))
    # fold: source rows vs served keys among NaN-plot positive candidates
    pos = k[k['PersonDays'] > 0]
    pos_nan = pos[pos['plot_id'].isna()]
    fold = {}
    for (t, s), sub in pos_nan.groupby(['t', 'season']):
        fold[f'{t}/{s}'] = {'positive_source_rows': int(len(sub)),
                            'served_keys': int(sub.groupby(['i', 'source']).ngroups)}
    pos_valid = pos[pos['plot_id'].notna()]
    dup_valid = pos_valid.groupby(['t', 'i', 'plot_id', 'source', 'season']).size()
    summ[ver] = {'by_cell': {'/'.join(map(str, ix)): {kk: (float(vv) if kk == 'total' else int(vv)) for kk, vv in r.items()}
                             for ix, r in by.iterrows()},
                 'served_rows': int(len(served)), 'served_total': float(served['PersonDays'].sum()),
                 'nan_plot_positive_fold': fold,
                 'valid_plot_positive_dup_keys': int((dup_valid > 1).sum()),
                 'valid_plot_positive_rows_in_dup_keys': int(dup_valid[dup_valid > 1].sum()),
                 'negative_rows': int((k['PersonDays'] < 0).sum())}
json.dump({'raw': out, 'oracle': summ}, open(sys.argv[1], 'w'), indent=1, sort_keys=True)
print('ok', summ['old']['served_rows'], summ['new']['served_rows'])
