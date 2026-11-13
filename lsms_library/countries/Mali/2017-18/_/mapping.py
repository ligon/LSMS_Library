# Formatting Functions for Mali 2017-18
from pathlib import Path as _Path

import pandas as pd
from lsms_library.local_tools import format_id, get_formatting_functions as _get_formatting_functions

# Bind the helper to this config tree by its exact path, as Burkina Faso 2014 does.
_eaci17_i = _get_formatting_functions(
    _Path(__file__).resolve().parents[2] / '_' / 'mali.py',
    '_mali_eaci_namespace')['eaci17_i']


def i(value):
    '''2017-18 household id, NAMESPACED: ``'EACI17_' + grappe + '0' + exploitation``.

    GH #1494.  Overrides the country ``i`` for every YAML table of this wave.
    See ``mali.EACI17_PREFIX``.
    '''
    return _eaci17_i(value)


def strata(value):
    return format_id(value)
