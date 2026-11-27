"""Counts for the six reviewed EHCVM expenditure routes (GH #828).

This is post-read disclosure, not a source reader or a valuation rule.
"""
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path
import warnings

import numpy as np
import pandas as pd

from .errors import FoodExpenditureCoverageWarning
from .transformations import VALUATION_RUNGS


ATTRS_KEY = "food_expenditure_coverage"

# Primary food_acquired bindings in the shipped wave /_/data_info.yml.
# Only these releases and their mapping.py::food_acquired hooks were
# reviewed. The 2021-22 Burkina file is the panel, not the cross-section.
_SOURCE_FILES = {
    "Burkina_Faso": {
        "2018-19": "s07b_me_bfa2018.dta",
        "2021-22": "s07b_me_bfa2021.dta",
    },
    "Mali": {
        "2018-19": "s07b_me_mli2018.dta",
        "2021-22": "s07b_me_mli2021.dta",
    },
    "Niger": {
        "2018-19": "s07b_me_ner2018.dta",
        "2021-22": "s07b_me_ner2021.dta",
    },
}
# EHCVM section 7B; see each country's CONTENTS.org #876 and ehcvm.py.
# These are instrument declarations, not inferences from an absent row.
_QUANTITY_FIELDS = {"produced": "s07bq04", "inkind": "s07bq05"}


def _reviewed_sources(country, waves):
    reviewed = _SOURCE_FILES.get(country.name)
    if not reviewed or country.assume_cache_fresh:
        return {}
    # Alternate instrument/builder configs and unchecked parquets cannot
    # inherit this provenance merely by using the same country/wave name.
    shipped = Path(__file__).resolve().parent / "countries" / country.name
    if country.file_path.resolve() != shipped:
        return {}
    return reviewed if waves is None else {
        wave: file for wave, file in reviewed.items() if wave in waves
    }


def _count_coverage(original, valued, sources):
    """Tally the actual transform inputs at acquisition grain, positionally."""
    grain = {"t", "i", "j", "u", "s"}
    if not grain.issubset(original.index.names) or "Quantity" not in original:
        return {}
    t = original.index.get_level_values("t")
    s = original.index.get_level_values("s")
    seen = set(t[s.isin(("purchased", *_QUANTITY_FIELDS))].unique())
    counts = {}
    for wave, file in sources.items():
        if wave not in seen:
            continue
        in_wave = t.isin([wave])
        roles = {}
        for role, question in _QUANTITY_FIELDS.items():
            rows = in_wave & s.isin([role])
            value = original["Expenditure"].iloc[rows]
            zero = value.eq(0).fillna(False).to_numpy(dtype=bool)
            missing = value.isna().to_numpy()
            quantity = pd.to_numeric(original["Quantity"].iloc[rows], errors="coerce").to_numpy(
                dtype=float, na_value=np.nan)
            physical = (np.isfinite(quantity) & (quantity > 0)
                        & ~value.index.get_level_values("u").isin(["Value"]))
            observed_missing = physical & missing
            remaining = observed_missing
            imputed = np.zeros(len(value), dtype=bool)
            if valued is not None:
                priced = valued.iloc[rows]
                remaining = observed_missing & priced["Expenditure"].isna().to_numpy()
                imputed = (priced["Expenditure"].notna()
                           & priced["ValuationSource"].isin(VALUATION_RUNGS)).to_numpy()
            roles[role] = {
                "source": {
                    "file": f"{wave}/Data/{file}",
                    "section": "7B",
                    "quantity_question": "asked",
                    "quantity_field": question,
                    "value_question": "not_asked",
                    "producer": f"{wave}/_/mapping.py::food_acquired",
                    "producer_value": "unvalued",
                },
                "acquisition_rows": len(value),
                "input_value_present": int((~missing).sum()),
                "input_zero": int(zero.sum()),
                "observed_missing_input": int(observed_missing.sum()),
                "imputed": int(imputed.sum()),
                "imputed_from_input_zero": int((imputed & zero).sum()),
                "remaining_unvalued": int(remaining.sum()),
            }
        counts[wave] = {"roles": roles}
    return counts


# Use a private, one-shot, owner-bound slot. In Feature the counts must not
# enter per-country attrs before concat: their disagreement could destroy
# unrelated attrs that otherwise agree. No row data crosses this handoff.
_FEATURE_COVERAGE = ContextVar("_FEATURE_COVERAGE", default=None)


@contextmanager
def _coverage_handoff(owner):
    if owner is None:
        yield None
        return
    slot = {"owner": owner, "claimed": False}
    token = _FEATURE_COVERAGE.set(slot)
    try:
        yield slot
    finally:
        slot.pop("owner")  # A copied context cannot claim a retired slot.
        _FEATURE_COVERAGE.reset(token)


def _claim_coverage_handoff(owner):
    slot = _FEATURE_COVERAGE.get()
    if slot is not None and slot.get("owner") is owner and not slot["claimed"]:
        slot["claimed"] = True
        return slot
    return None


def _result_coverage(result, counts, *, excluded=()):
    """Describe finished output, without interpreting absent rows as rejected."""
    if not counts or "t" not in result.index.names:
        return {}
    represented = set(result.index.get_level_values("t").unique())
    return {
        wave: {**record, "result": "served" if wave in represented else "no_expenditure_rows"}
        for wave, record in counts.items() if wave not in excluded
    }


def _attach_coverage(result, coverage, *, stacklevel=3):
    if coverage:
        result.attrs[ATTRS_KEY] = coverage
        gaps = [
            f"{country} {wave} {role}: {record['remaining_unvalued']}"
            for country, waves in coverage.items()
            for wave, cell in waves.items()
            for role, record in cell["roles"].items()
            if record["remaining_unvalued"]
        ]
        if gaps:
            warnings.warn(
                "food_expenditures(basis='total') has observed acquisitions still "
                "without expenditure value (" + ", ".join(gaps) + "). "
                f"Available values are unchanged. See df.attrs[{ATTRS_KEY!r}].",
                FoodExpenditureCoverageWarning, stacklevel=stacklevel,
            )
    return result
