"""Variable + value labels of the Malawi G / P harvest modules, all waves (metadata only)."""
import os, sys, json
import pyreadstat
from lsms_library.data_access import get_data_file
FILES = {
 '2010-11': ['Data/Full_Sample/Agriculture/ag_mod_g.dta', 'Data/Full_Sample/Agriculture/ag_mod_p.dta'],
 '2013-14': ['Data/AG_MOD_G_13.dta', 'Data/AG_MOD_P_13.dta'],
 '2016-17': ['Data/Cross_Sectional/ag_mod_g.dta', 'Data/Cross_Sectional/ag_mod_p.dta', 'Data/Panel/ag_mod_g_16.dta', 'Data/Panel/ag_mod_p_16.dta'],
 '2019-20': ['Data/Cross_Sectional/ag_mod_g.dta', 'Data/Cross_Sectional/ag_mod_p.dta', 'Data/Panel/ag_mod_g_19.dta', 'Data/Panel/ag_mod_p_19.dta'],
}
for w, fs in FILES.items():
    for f in fs:
        p = get_data_file(f'Malawi/{w}/{f}')
        _, m = pyreadstat.read_dta(str(p), metadataonly=True)
        print('=====', w, f, m.number_rows)
        for c, l in zip(m.column_names, m.column_labels):
            vl = m.variable_to_label.get(c)
            labs = m.value_labels.get(vl, {}) if vl else {}
            s = '' if not labs else ' | ' + '; '.join(f'{k}={v}' for k, v in list(labs.items())[:12])
            print(f'  {c}: {l}{s}')
