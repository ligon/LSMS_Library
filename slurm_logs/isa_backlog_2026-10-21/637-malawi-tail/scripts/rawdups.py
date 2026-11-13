"""Raw-source oracle: exact duplicate RECORDS (every column equal) in each Malawi
ag module file read by crop_production / plot_inputs.  Aggregates only."""
import json, sys, warnings
import pandas as pd, pyreadstat
from lsms_library.data_access import get_data_file

W = {
 '2010-11': ('Data/Full_Sample/Agriculture/', ['ag_mod_g.dta', 'ag_mod_p.dta', 'ag_mod_i.dta', 'ag_mod_q.dta', 'ag_mod_d.dta', 'ag_mod_h.dta']),
 '2013-14': ('Data/', ['AG_MOD_G_13.dta', 'AG_MOD_P_13.dta', 'AG_MOD_I_13.dta', 'AG_MOD_Q_13.dta', 'AG_MOD_D_13.dta', 'AG_MOD_H_13.dta']),
 '2016-17': ('Data/', ['Cross_Sectional/ag_mod_g.dta', 'Cross_Sectional/ag_mod_p.dta', 'Cross_Sectional/ag_mod_i.dta', 'Cross_Sectional/ag_mod_q.dta', 'Cross_Sectional/ag_mod_d.dta', 'Cross_Sectional/ag_mod_h.dta',
                   'Panel/ag_mod_g_16.dta', 'Panel/ag_mod_p_16.dta', 'Panel/ag_mod_i_16.dta', 'Panel/ag_mod_q_16.dta', 'Panel/ag_mod_d_16.dta', 'Panel/ag_mod_h_16.dta']),
 '2019-20': ('Data/', ['Cross_Sectional/ag_mod_g.dta', 'Cross_Sectional/ag_mod_p.dta', 'Cross_Sectional/ag_mod_i.dta', 'Cross_Sectional/ag_mod_q.dta', 'Cross_Sectional/ag_mod_d.dta', 'Cross_Sectional/ag_mod_h.dta',
                   'Panel/ag_mod_g_19.dta', 'Panel/ag_mod_p_19.dta', 'Panel/ag_mod_i_19.dta', 'Panel/ag_mod_q_19.dta', 'Panel/ag_mod_d_19.dta', 'Panel/ag_mod_h_19.dta']),
}
QTY = {'g': ['ag_g13a', 'ag_g04a'], 'p': ['ag_p09a'], 'i': ['ag_i02a', 'ag_i03'], 'q': ['ag_q02a', 'ag_q03'],
       'd': ['ag_d39d', 'ag_d39i', 'ag_d39j', 'ag_d37a'], 'h': ['ag_h16a', 'ag_h26a', 'ag_h07a', 'ag_h38a']}
res = {}
for w, (base, files) in W.items():
    for f in files:
        p = get_data_file(f'Malawi/{w}/{base}{f}')
        excluded = []
        try:
            df, _ = pyreadstat.read_dta(str(p), apply_value_formats=False)
        except Exception:
            # an undecodable free-text column: read every other column.  Zero
            # repeats on a column SUBSET implies zero on the full record.
            _, meta = pyreadstat.read_dta(str(p), metadataonly=True)
            for c in meta.column_names:
                try:
                    pyreadstat.read_dta(str(p), usecols=[c], apply_value_formats=False)
                except Exception:
                    excluded.append(c)
            df, _ = pyreadstat.read_dta(str(p), apply_value_formats=False,
                                        usecols=[c for c in meta.column_names if c not in excluded])
        enc = 'excluded:' + ','.join(excluded) if excluded else 'full'
        dup = df.duplicated(keep='first')           # every column
        anydup = df.duplicated(keep=False)
        mod = f.split('/')[-1].lower().replace('ag_mod_', '')[0]
        r = dict(rows=int(len(df)), cols=int(df.shape[1]), encoding=enc,
                 repeat_records=int(dup.sum()), records_in_repeat_groups=int(anydup.sum()))
        if dup.any():
            r['repeat_qty'] = {c: float(pd.to_numeric(df.loc[dup, c], errors='coerce').fillna(0).sum())
                               for c in QTY[mod] if c in df.columns}
            r['repeat_qty_nonnull'] = {c: int(pd.to_numeric(df.loc[dup, c], errors='coerce').notna().sum())
                                       for c in QTY[mod] if c in df.columns}
        res[f'{w}/{f}'] = r
        print(w, f, r, flush=True)
json.dump(res, open(sys.argv[1], 'w'), indent=1)
