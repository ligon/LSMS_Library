"""Formatting functions for Burkina Faso 2014 (EMC).

NOTE (2026-05-05, GH #169 Phase 2B): the food_acquired canonical
reshape is NOT applied here.  Burkina_Faso's 2014 wave runs through
the legacy script-path build at lsms_library/countries/Burkina_Faso/_/
food_acquired.py, which bypasses mapping.py post-processors entirely.
The script emits t='2013_Q4' and the legacy column set
(quantity, units, total expenses, ...).  Pending a Phase 3 rewrite
of either the country-level or wave-level food_acquired.py, this
wave continues to emit non-canonical shape.
"""

from lsms_library.local_tools import format_id


def strata(x):
    return format_id(x)
