"""``ll.recall()`` -- the corpus recall record as one table.

The per-country ``_/recall.yml`` files are the packaged canonical store (see
:mod:`lsms_library.recall`); this assembles them into the frame a human wants to
look at, the way ``ll.coverage()`` does for the coverage snapshot.

Assembled from the *packaged* YAML rather than read from
``.coder/coverage/food_recall.csv``, so it works from an installed wheel.  The
CSV is the evidence base the YAML is generated from and lives only in a source
checkout.
"""
from __future__ import annotations

import pandas as pd

from .catalog import countries as _countries
from .recall import recall_records

__all__ = ["recall"]

COLUMNS = ["country", "wave", "recall_days", "n_asks", "total_exposure_days",
           "ask_count_basis", "basis", "within_wave_variation",
           "n_items_delivered", "recall_verbatim", "evidence", "notes"]


def recall(country: str | list[str] | None = None,
           recorded_only: bool = False) -> pd.DataFrame:
    """What reference period each ``(country, wave)`` food number covers.

    Parameters
    ----------
    country : str or list of str, optional
        Restrict to these countries.  Default: every country with a record.
    recorded_only : bool, default False
        Drop cells nobody has looked at.  **Off by default on purpose** -- the
        blank rows are the work queue, and a table that hides them reads as
        though the corpus were fully documented.  Roughly half of it is not.

    Notes
    -----
    ``total_exposure_days`` is ``recall_days * n_asks`` and is null wherever
    either is unknown.  ``n_asks`` counts ASKS, not interviewer visits.  A cell
    with ``within_wave_variation`` set carries no numbers at all -- its window
    genuinely differs inside the wave, and one value would misreport it.

    Read ``basis`` before using a number: only a minority of stated windows are
    questionnaire-grade, and ``config-comment`` means a maintainer asserted it.
    """
    if country is None:
        names = list(_countries())
    elif isinstance(country, str):
        names = [country]
    else:
        names = list(country)

    rows = []
    for name in names:
        for wave, r in sorted(recall_records(name).items()):
            if recorded_only and not r.is_recorded:
                continue
            rows.append({
                "country": name, "wave": wave,
                "recall_days": r.recall_days, "n_asks": r.n_asks,
                "total_exposure_days": r.total_exposure_days,
                "ask_count_basis": r.ask_count_basis, "basis": r.basis,
                "within_wave_variation": r.within_wave_variation,
                "n_items_delivered": r.n_items_delivered,
                "recall_verbatim": r.recall_verbatim,
                "evidence": r.evidence, "notes": r.notes,
            })
    df = pd.DataFrame(rows, columns=COLUMNS)
    return df.set_index(["country", "wave"]).sort_index()
