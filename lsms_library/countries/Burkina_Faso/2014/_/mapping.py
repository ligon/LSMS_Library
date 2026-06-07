"""Formatting functions for Burkina Faso 2014 (EMC).

food_acquired canonical reshape (GH #169 / #107): the wave's
``data_info.yml`` extracts the wide form (Quantity-total, Produced-subset,
Expenditure); importing ``food_acquired_to_canonical as food_acquired``
below registers it as the ``mapping.py`` post-processor, so grab_data
reshapes the wide form into the canonical long form on the ``s``
(acquisition-source) axis — matching the 2018-19 / 2021-22 waves.  Before
this, 2014 emitted the legacy wide shape (a ``Produced`` column, no ``s``),
which leaked into ``Country('Burkina_Faso').food_acquired()`` and broke the
``food_quantities`` kg derivation (0% resolved).
"""

from lsms_library.local_tools import format_id
from lsms_library.transformations import food_acquired_to_canonical as food_acquired


def strata(x):
    return format_id(x)
