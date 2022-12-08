# -*- coding: utf-8 -*-
from cfe.df_utils import orgtbl_to_df

units = orgtbl_to_df(tab).set_index('Code')['Preferred Label']

units.to_csv('unitlabels.csv')
