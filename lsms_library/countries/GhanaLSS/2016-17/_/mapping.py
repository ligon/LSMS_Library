# Formatting Functions for Ghana 2016-17
import pandas as pd
import lsms_library.local_tools as tools


def v(value):
    '''
    Formatting cluster variable
    '''
    return tools.format_id(value)
