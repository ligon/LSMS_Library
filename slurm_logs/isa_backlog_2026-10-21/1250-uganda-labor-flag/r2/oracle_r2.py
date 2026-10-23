"""Raw-source oracle for the GH #1250 follow-up (wage-only hired rows).

Reads AGSEC3A/3B through get_dataframe and, from the RAW float codes (no
_to_int_code, no _labor_persondays), classifies every hired row by
  flag  : yes (1) / no (2) / missing / other (any other code)
  days  : empty (every day cell NaN) / present
  wage  : nan / zero / positive
  plot  : NaN hhid-parcel-plot key or not
It then emulates the builder twice -- OLD = serve a row only on non-NaN
PersonDays (90fc1e18d); NEW = also serve a hired row on a non-NaN Wage
(Malawi's rule) -- including the final groupby(dropna=False).max(), and
reports which served keys the new rule adds and which existing keys it changes
by folding.  Aggregate counts only are written.

Usage: CHECKOUT=<checkout> python oracle_r2.py <out.json> [<keys.parquet>]
(the optional second argument writes the row-level wage-only rows to SCRATCH;
never commit it).
"""
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
KEY = ['t', 'i', 'plot_id', 'source', 'season']


def flag_class(flag, no_code, yes_code=None):
    c = pd.Series('other', index=flag.index, dtype=object)
    c[flag.isna()] = 'missing'
    c[flag == no_code] = 'no'
    if yes_code is not None:
        c[flag == yes_code] = 'yes'
    return c


raw_classes = []
pieces = []
for t, cmap in uganda.LABOR_COLMAPS.items():
    fa, fb = FILES.get(t, ('Data/AGSEC3A.dta', 'Data/AGSEC3B.dta'))
    for season, f in (('A', fa), ('B', fb)):
        cm = cmap.get(season)
        if not cm:
            continue
        df = get_dataframe(os.path.join(root, t, f), convert_categoricals=False)
        hh = uganda._format_agsec_hhid(df[cm['hhid']], t)
        pid = (hh.astype(str) + '-' + df[cm['parcel']].apply(format_id).astype(str)
               + '-' + df[cm['plot']].apply(format_id).astype(str))
        wage = pd.to_numeric(df[cm['wage']], errors='coerce')
        for source, flag_key, code in (('family', 'family_flag', 0),
                                       ('hired', 'hired_flag', 2),
                                       ('other', None, None)):
            if not cm.get(source):
                continue
            cols = [c for c in cm[source] if c in df.columns]
            block = df[cols].apply(lambda s: pd.to_numeric(s, errors='coerce'))
            ssum = block.sum(axis=1, min_count=1)
            fk = cm.get(flag_key) if flag_key else None
            if fk:
                flag = pd.to_numeric(df[fk], errors='coerce')   # raw float code
                pdays = ssum.mask(flag == code, 0.0)             # Stata: replace = 0 if flag==code
                # development 9a0ed5f3c: a MISSING flag was also zeroed (GH #1250)
                pdays_dev = ssum.mask((flag == code) | flag.isna(), 0.0)
                fcls = flag_class(flag, code, 1 if source == 'hired' else None)
            else:
                pdays = pdays_dev = ssum
                fcls = pd.Series('none', index=df.index, dtype=object)
            w = wage if source == 'hired' else pd.Series(np.nan, index=df.index)
            piece = pd.DataFrame({'t': t, 'i': hh.values, 'plot_id': pid.values,
                                  'source': source, 'season': season,
                                  'PersonDays': pdays.values, 'Wage': w.values,
                                  'PersonDays_dev': pdays_dev.values,
                                  'flag': fcls.values,
                                  'days': np.where(block.notna().any(axis=1), 'present', 'empty'),
                                  'wage': np.select([w.isna(), w == 0], ['nan', 'zero'], 'positive')})
            pieces.append(piece)
rows = pd.concat(pieces, ignore_index=True)
rows['plot_nan'] = rows['plot_id'].isna()

old_keep = rows['PersonDays'].notna()
new_keep = old_keep | ((rows['source'] == 'hired') & rows['Wage'].notna())
rows['old'] = old_keep
rows['new'] = new_keep
wage_only = rows[new_keep & ~old_keep]


def serve(sel):
    return (rows[sel].groupby(KEY, dropna=False)
            .agg(PersonDays=('PersonDays', 'max'), Wage=('Wage', 'max'),
                 n_src=('PersonDays', 'size')))


old = serve(old_keep)
new = serve(new_keep)
added_keys = new.index.difference(old.index)
common = new.index.intersection(old.index)
ov, nv = old.loc[common], new.loc[common]


def neq(a, b):
    return (a != b) & ~(a.isna() & b.isna())


changed = common[neq(ov['PersonDays'], nv['PersonDays']).to_numpy()
                 | neq(ov['Wage'], nv['Wage']).to_numpy()]
# wage-only source rows that land on an EXISTING served key (fold)
wo_keys = pd.MultiIndex.from_frame(wage_only[KEY])
wo_on_existing = wo_keys.isin(old.index)

res = {
    'served_rows': {'old': int(len(old)), 'new': int(len(new))},
    'added_keys': int(len(added_keys)),
    'wage_only_source_rows': int(len(wage_only)),
    'wage_only_source_rows_on_existing_key': int(wo_on_existing.sum()),
    'existing_keys_changed': int(len(changed)),
    'wage_only_wage_sum': float(wage_only['Wage'].sum()),
    'added_wage_sum': float(new.loc[added_keys, 'Wage'].sum()),
    'added_plot_nan': int(pd.isna(added_keys.get_level_values('plot_id')).sum()),
    'added_source_counts': {k: int(v) for k, v in pd.Series(added_keys.get_level_values('source')).value_counts().items()},
}
by = (wage_only.groupby(['t', 'season', 'flag', 'wage', 'plot_nan'])
      .agg(rows=('Wage', 'size'), wage_sum=('Wage', 'sum'),
           wage_min=('Wage', 'min'), wage_max=('Wage', 'max')).reset_index())
res['wage_only_by_wave_season_class'] = by.to_dict('records')
# was each wage-only row served on development (as a fabricated 0)?
res['wage_only_served_on_development'] = {
    'served_as_zero': int(wage_only['PersonDays_dev'].notna().sum()),
    'never_served': int(wage_only['PersonDays_dev'].isna().sum()),
    'served_as_zero_by_flag': {k: int(v) for k, v in wage_only[wage_only['PersonDays_dev'].notna()]['flag'].value_counts().items()},
    'never_served_by_flag': {k: int(v) for k, v in wage_only[wage_only['PersonDays_dev'].isna()]['flag'].value_counts().items()},
}
res['wage_only_by_flag'] = {k: int(v) for k, v in wage_only['flag'].value_counts().items()}
res['wage_only_by_wage'] = {k: int(v) for k, v in wage_only['wage'].value_counts().items()}
res['wage_only_by_wave_season'] = {f'{t}/{s}': int(n) for (t, s), n in wage_only.groupby(['t', 'season']).size().items()}
# "No" + wage: served today (PersonDays 0) with its wage; same under NEW
h = rows[rows['source'] == 'hired']
nw = h[(h['flag'] == 'no') & h['Wage'].notna()]
res['no_flag_with_wage'] = {
    'source_rows': int(len(nw)),
    'days_present': int((nw['days'] == 'present').sum()),
    'wage_zero': int((nw['wage'] == 'zero').sum()),
    'wage_positive': int((nw['wage'] == 'positive').sum()),
    'wage_sum': float(nw['Wage'].sum()),
    'served_old': int(nw['old'].sum()), 'served_new': int(nw['new'].sum()),
    'by_wave_season': {f'{t}/{s}': int(n) for (t, s), n in nw.groupby(['t', 'season']).size().items()},
}
# hired rows whose flag says yes/missing/other, with days present, but whose
# wage is reported -- context (already served under both rules)
res['hired_rows_by_flag_days_wage'] = {
    '/'.join(k): int(v) for k, v in h.groupby(['flag', 'days', 'wage']).size().items()}
json.dump(res, open(sys.argv[1], 'w'), indent=1, default=str)
if len(sys.argv) > 2:
    wage_only[KEY + ['PersonDays', 'Wage', 'flag', 'wage', 'days']].to_parquet(sys.argv[2])
print(json.dumps({k: res[k] for k in ('served_rows', 'added_keys', 'wage_only_source_rows',
                                      'wage_only_source_rows_on_existing_key', 'existing_keys_changed',
                                      'wage_only_by_flag', 'wage_only_by_wage', 'wage_only_by_wave_season',
                                      'added_plot_nan', 'no_flag_with_wage',
                                      'wage_only_served_on_development')}, indent=1, default=str))
