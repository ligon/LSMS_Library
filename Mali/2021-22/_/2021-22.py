# Formatting  Functions for Ghana 2016-17
import pandas as pd
from lsms_library.local_tools import format_id
import numpy as np


def in_previous_wave(value):
    '''
    Formatting in_previous_wave variable
    '''
    mapping = {
        'Non': 0,
        'Oui': 1,
    }
    return mapping.get(value)


def panel_ids(df):
    '''
    filter the dataframe to only include the second visit
    '''
    df = df[(df.index.get_level_values('visit') == '2') & (df['in_previous_wave'] == 1)]
    def previous_i(value):

        return (format_id(value[0]) or '') + '0' + (format_id(value[1], zeropadding=2) or '')
    df['previous_i'] = df[['previous_v', 'previous_hid']].apply(previous_i, axis=1)
    return df