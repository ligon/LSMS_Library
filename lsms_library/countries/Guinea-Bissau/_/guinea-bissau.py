import pandas as pd
import numpy as np
import lsms_library.local_tools as tools


def i(value):
    '''
    Formatting household id from composite (grappe, menage).
    '''
    if isinstance(value, (pd.Series, np.ndarray, list, tuple)):
        return tools.format_id(value.iloc[0]) + tools.format_id(value.iloc[1]) if isinstance(value, pd.Series) else tools.format_id(value[0]) + tools.format_id(value[1])
    return tools.format_id(value)
