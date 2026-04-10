# Formatting Functions for Ghana 2012-13
import pandas as pd
import lsms_library.local_tools as tools


def v(value):
    '''
    Formatting cluster variable
    '''
    return tools.format_id(value)
