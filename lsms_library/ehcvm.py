#!/usr/bin/env python3
"""Shared build-path code for the EHCVM 2018+ (WAEMU/UEMOA) waves.

Eight countries field the *same* instrument -- Togo, Benin, Guinea-Bissau,
Senegal, Mali, Niger, Burkina Faso and Cote d'Ivoire -- across twelve waves,
and their ``food_acquired`` blocks in ``data_info.yml`` are byte-for-byte the
same shape.  Code that is genuinely shared between them lives here rather than
being copied twelve times into ``<wave>/_/mapping.py``.

Deliberately NOT in :mod:`lsms_library.build_transforms`: nothing here is
``@build_transform``-tagged, so it costs no corpus-wide re-warm.  A wave's
``mapping.py`` imports from here, and ``_build_registry.framework_imports_
fingerprint`` folds this module's closure into *those waves'* input hashes --
which is exactly the blast radius the change has.

section 7B: one row, two windows (GH #876)
------------------------------------------
The EHCVM household questionnaire heads section 7B *"B Consommation
alimentaire des 7 derniers jours et achat des 30 derniers jours"*, and that
"et" is the whole problem.  One row of ``s07b_me_*.dta`` carries answers on
two different clocks:

===============  ==========================================================
``s07bq03a/b/c`` total quantity of the item CONSUMED by the household in the
                 last 7 days, its unit, and that unit's size
``s07bq04``      of which own-produced
``s07bq05``      of which received as a gift
``s07bq06``      when the item was LAST BOUGHT (yesterday / 7 days / 30 days
                 / ...)
``s07bq07a/b/c`` the quantity of that ONE LAST PURCHASE, its unit, its size
``s07bq08``      *"Valeur (en FCFA) du [PRODUIT] achete la derniere fois"* --
                 the value of that ONE LAST PURCHASE
===============  ==========================================================

Every one of the twelve waves used to wire ``Expenditure: s07bq08``, pairing
the value of a single purchase of unrecorded size, made up to 30 days ago,
with a 7-day consumption quantity on the same row.  Nothing disclosed it.

What the survey itself does with these answers is not a guess: every wave
ships ``ehcvm_conso_<ccc><year>.dta``, the UEMOA consumption aggregate, one
row per (household, product, ``modep`` in {Achat, Autoconso, Don, ...}) with
an annual value ``depan``.  Measured on Togo 2018 over the 472 unique
(household, product) rows where the candidate constructions actually differ
(i.e. ``s07bq04`` or ``s07bq05`` is positive and an ``Achat`` row exists):

=========================================================  ================
construction, annualised x 365/7, vs ``depan(Achat)``       within 1%
=========================================================  ================
``s07bq03a                     * s07bq08/s07bq07a``          0.0%
``(s07bq03a - s07bq04)         * s07bq08/s07bq07a``         12.1%
``(s07bq03a - s07bq04 - s07bq05) * s07bq08/s07bq07a``       91.5%
=========================================================  ================

and ``s07bq04 * uv`` reproduces ``depan(Autoconso)``, ``s07bq05 * uv``
reproduces ``depan(Don)``, both at median ratio 0.99931 -- which is
52.142857/52.178571, i.e. UEMOA annualises with 365/7 and we do not annualise
at all.  So the survey's own unit of account for every acquisition mode is the
**unit value of the last purchase**, and the library serving ``s07bq08`` raw
was serving neither the 7-day spend nor UEMOA's construction.

What this module serves
-----------------------
:func:`derive_ehcvm_purchase_value` returns, for the ``s='purchased'`` row,

    (7-day quantity purchased) x (``s07bq08`` / ``s07bq07a``)

where the 7-day purchased quantity is the row's OWN served ``Quantity`` --
``(s07bq03a - s07bq04).clip(lower=0)``, what
:func:`build_transforms.food_acquired_to_canonical` puts on the row -- so that
``Expenditure / Quantity`` is exactly the unit value and the two numbers on the
row finally refer to the same thing.  ``s07bq05`` (gifts) is NOT subtracted:
that would change the served ``Quantity`` in every EHCVM wave, which is a
separate defect with its own blast radius (the library has never read
``s07bq05``).  The consequence is stated rather than hidden: the purchased
row's quantity, and therefore its value, still includes gifts, so this runs
about 1-3% above UEMOA's ``Achat``.

NaN, never a guess, in two cases:

* ``s07bq07a`` is 0, negative or missing -- there is no unit value to compute;
* the last purchase was made in a DIFFERENT unit from the one the consumption
  was reported in (``s07bq07b/c`` vs ``s07bq03b/c``).  No EHCVM wave ships a
  unit-conversion table -- ``lsms_library/categorical_mapping/ehcvm_units.org``
  is a code->label codebook with no factors, and the only conversion files in
  these countries are the 2021 West-African *calorie* tables -- so there is no
  exact conversion to apply and the honest answer is NA.

Unit identity is compared on :func:`_unit_key`, not on the raw label, because
the two variables carry DIFFERENT value-label sets inside the same ``.dta``:
Senegal 2021-22 spells the same unit ``'139. Sachet'`` in ``s07bq03b`` and
``'139. sachet industriel'`` in ``s07bq07b`` (83% spurious mismatch on a raw
string compare), Mali 2018-19 pads ``s07bq03b`` with seven leading spaces, and
Mali's size labels read ``'Unite de taille unique'`` against ``'Taille
unique'``.  Where both spellings carry the EHCVM numeric prefix the key IS
that code, which is exact.
"""
from __future__ import annotations

import re
import unicodedata

import numpy as np
import pandas as pd

from .build_transforms import food_acquired_to_canonical
from .derivations import COLUMN as DERIVATION_COLUMN

__all__ = [
    "EHCVM_COUNTRIES", "DERIVATION_NAME", "RAW_VARIABLES",
    "MYVARS", "derivation_key", "derive_ehcvm_purchase_value",
    "food_acquired_ehcvm", "inputs_last_purchase",
]

#: The eight countries whose ``food_acquired`` comes from EHCVM section 7B.
EHCVM_COUNTRIES = ("Benin", "Burkina_Faso", "CotedIvoire", "Guinea-Bissau",
                   "Mali", "Niger", "Senegal", "Togo")

#: Name slot of the registry key; the country slot differs per country, so the
#: eight entries share one ``function`` but are eight separate records.  A
#: framework-level (empty country slot) entry would claim all 40 countries.
DERIVATION_NAME = "7day-purchase-at-last-purchase-unit-value"

#: The raw section-7B variables the derivation reads, in questionnaire order.
RAW_VARIABLES = ["s07bq03a", "s07bq03b", "s07bq03c", "s07bq04",
                 "s07bq07a", "s07bq07b", "s07bq07c", "s07bq08"]

#: ``myvars`` names the twelve ``data_info.yml`` blocks bind the raw
#: last-purchase variables to.  Kept here so the eight YAML files and this
#: module cannot drift apart silently; :func:`food_acquired_ehcvm` reads
#: exactly these column names off the extracted frame.
MYVARS = {
    "ConsumptionUnitCode": "s07bq03b",
    "ConsumptionUnitSize": "s07bq03c",
    "LastPurchaseQuantity": "s07bq07a",
    "LastPurchaseUnitCode": "s07bq07b",
    "LastPurchaseUnitSize": "s07bq07c",
    "LastPurchaseValue": "s07bq08",
}

#: Label variants that name the SAME size in the two variables' value-label
#: sets.  ``s07bq03c`` code 0 is spelled "Unite de taille unique" and
#: ``s07bq07c`` code 0 "Taille unique" in Mali 2018-19 (31,305 purchase rows);
#: basis ``variable-label``.  Nothing else is synonymised -- ``Entier`` vs
#: ``Taille unique`` (Niger 2018-19, 866 rows) is left a mismatch because the
#: instrument does not say they are the same.
_SIZE_SYNONYMS = {
    "unite de taille unique": "taille unique",
}


def derivation_key(country: str) -> str:
    """``'<country>::food_acquired::7day-purchase-at-last-purchase-unit-value'``."""
    if country not in EHCVM_COUNTRIES:
        raise ValueError(f"{country!r} is not an EHCVM country: {EHCVM_COUNTRIES}")
    return f"{country}::food_acquired::{DERIVATION_NAME}"


# ---------------------------------------------------------------------------
# unit identity
# ---------------------------------------------------------------------------

_CODE_PREFIX = re.compile(r"^(\d+)\s*[.\-]")


def _unit_key(value):
    """A comparable identity for one EHCVM unit / size label.

    The EHCVM numeric code when the label carries it (``'139. Sachet'`` -> and
    a bare numeric code -> ``'139'``), else the label stripped of accents,
    punctuation, case and padding.  ``None`` for a missing value, which never
    compares equal to anything -- an unrecorded unit is not a matching unit.
    """
    if value is None:
        return None
    if isinstance(value, (bool, np.bool_)):
        return None
    if isinstance(value, (int, float, np.integer, np.floating)):
        if pd.isna(value):
            return None
        return str(int(value))
    if not isinstance(value, str):
        return None
    s = value.strip()
    m = _CODE_PREFIX.match(s)
    if m:
        return str(int(m.group(1)))
    # Several of these .dta are Stata 115 with cp1252/utf-8 label bytes the
    # reader took as Latin-1 ('UnitÃ©'); folding accents makes the two
    # spellings agree without having to know which side is mojibake.
    try:
        s = s.encode("latin-1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        pass
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^a-z0-9]+", " ", s.lower()).strip()
    if not s:
        return None
    return _SIZE_SYNONYMS.get(s, s)


def _keys(series) -> pd.Series:
    return pd.Series(series).map(_unit_key)


def unit_match(quantity_unit, quantity_size, last_unit, last_size) -> pd.Series:
    """Boolean: does the last purchase use the unit the consumption is in?

    Both the unit CODE and the unit SIZE must agree -- a "Petit Bol" and a
    "Grand Bol" are not the same container, and the value per container is what
    ``s07bq08 / s07bq07a`` measures.  A missing key on either side is a
    mismatch, so a row with no recorded purchase unit is never valued.
    """
    a, b = _keys(quantity_unit), _keys(last_unit)
    sa, sb = _keys(quantity_size), _keys(last_size)
    code_ok = a.notna() & b.notna() & (a.to_numpy() == b.to_numpy())
    size_ok = sa.notna() & sb.notna() & (sa.to_numpy() == sb.to_numpy())
    return pd.Series(np.asarray(code_ok) & np.asarray(size_ok))


# ---------------------------------------------------------------------------
# the derivation
# ---------------------------------------------------------------------------

def derive_ehcvm_purchase_value(quantity, quantity_unit, quantity_size,
                                last_quantity, last_unit, last_size,
                                last_value):
    """7-day value of the purchased food: quantity x last-purchase unit value.

    ``quantity`` is the row's served purchased quantity -- EHCVM's 7-day
    consumption ``s07bq03a`` net of own production ``s07bq04``, clipped at
    zero, exactly as :func:`build_transforms.food_acquired_to_canonical`
    computes it -- in unit ``(quantity_unit, quantity_size)``
    (``s07bq03b``/``s07bq03c``).  The unit value is ``last_value /
    last_quantity`` (``s07bq08 / s07bq07a``), in unit ``(last_unit,
    last_size)`` (``s07bq07b``/``s07bq07c``).

    Returns NaN -- never a guess -- where the unit value cannot be formed
    (``last_quantity`` missing or <= 0) or where the two units differ: no
    EHCVM wave ships a unit-conversion table, so there is no exact conversion
    and the survey does not support one.

    No options, by design (``SkunkWorks/derived_values.org``): a cached
    parquet must be one identifiable construction.  A user who wants the raw
    answers back calls ``Country(c).derivation_inputs(key)``; the alternatives
    are computable from them -- UEMOA's own annual ``Achat`` figure is
    ``(s07bq03a - s07bq04 - s07bq05) * uv * 365/7``, and the pre-#876 served
    value was ``s07bq08`` itself.

    Vectorised; accepts scalars, arrays or Series (elementwise, positional).
    Returns a float ndarray, or a float for all-scalar input.
    """
    qty = pd.to_numeric(pd.Series(np.ravel(quantity)), errors="coerce")
    last_q = pd.to_numeric(pd.Series(np.ravel(last_quantity)), errors="coerce")
    value = pd.to_numeric(pd.Series(np.ravel(last_value)), errors="coerce")

    with np.errstate(divide="ignore", invalid="ignore"):
        unit_value = value.to_numpy() / last_q.where(last_q > 0).to_numpy()
    ok = unit_match(np.ravel(quantity_unit), np.ravel(quantity_size),
                    np.ravel(last_unit), np.ravel(last_size)).to_numpy()
    out = np.where(ok, qty.to_numpy() * unit_value, np.nan)

    scalar = all(np.ndim(x) == 0 for x in
                 (quantity, quantity_unit, quantity_size, last_quantity,
                  last_unit, last_size, last_value))
    return float(out[0]) if scalar else out


# ---------------------------------------------------------------------------
# the wave-level df_edit hook
# ---------------------------------------------------------------------------

def food_acquired_ehcvm(df: pd.DataFrame, key: str,
                        drop_columns=("visit",)) -> pd.DataFrame:
    """``food_acquired`` df_edit hook for an EHCVM 2018+ wave.

    Replaces the wide frame's ``Expenditure`` -- which the YAML used to wire
    straight to ``s07bq08``, the value of ONE purchase up to 30 days old --
    with :func:`derive_ehcvm_purchase_value`, then runs the ordinary canonical
    reshape and stamps ``key`` in the ``Derivation`` column on every
    ``s='purchased'`` row, NA-valued ones included: the function produced that
    NA and the row is entitled to say so.  ``s='produced'`` rows are untouched
    (their ``Expenditure`` is NaN by construction) and carry no key.

    A wave's ``mapping.py`` binds this as::

        from lsms_library.ehcvm import derivation_key, food_acquired_ehcvm
        FOOD_ACQUIRED_DERIVATION = derivation_key('Togo')

        def food_acquired(df):
            return food_acquired_ehcvm(df, FOOD_ACQUIRED_DERIVATION)

    The purchased quantity is recomputed here with the same expression
    ``food_acquired_to_canonical`` uses, so ``Expenditure / Quantity`` is the
    unit value exactly; ``tests/test_ehcvm_7day.py`` pins that identity rather
    than trusting the two expressions to stay in step.
    """
    missing = [c for c in MYVARS if c not in df.columns]
    if missing:
        raise KeyError(
            f"food_acquired_ehcvm: the wave's data_info.yml must bind "
            f"{missing} (see lsms_library.ehcvm.MYVARS); got {list(df.columns)}")

    purchased_qty = (df["Quantity"].fillna(0)
                     - df["Produced"].fillna(0)).clip(lower=0)
    expenditure = derive_ehcvm_purchase_value(
        purchased_qty,
        df["ConsumptionUnitCode"], df["ConsumptionUnitSize"],
        df["LastPurchaseQuantity"],
        df["LastPurchaseUnitCode"], df["LastPurchaseUnitSize"],
        df["LastPurchaseValue"])

    work = df.drop(columns=list(MYVARS))
    work = work.assign(Expenditure=expenditure)

    out = food_acquired_to_canonical(work, drop_columns=drop_columns)
    is_purchased = out.index.get_level_values("s").to_numpy() == "purchased"
    out[DERIVATION_COLUMN] = pd.Series(
        np.where(is_purchased, key, None), index=out.index, dtype="string")
    return out


# ---------------------------------------------------------------------------
# raw-input retrieval
# ---------------------------------------------------------------------------

def inputs_last_purchase(country: str, wave: str) -> pd.DataFrame:
    """The raw section-7B answers behind one wave's derived purchased rows.

    Re-reads ``s07b_me_*.dta`` through ``get_dataframe`` and returns a frame
    indexed ``(t, i, j)`` -- the same ``i`` and ``j`` formatting the served
    table uses, taken from the wave's own ``data_info.yml`` so the two cannot
    drift -- whose columns are the ORIGINAL variable names:
    ``s07bq03a``, ``s07bq03b``, ``s07bq03c``, ``s07bq04``, ``s07bq07a``,
    ``s07bq07b``, ``s07bq07c``, ``s07bq08``.

    Every source row is returned, unfiltered and at the INPUT grain: the
    served table splits one row into up to two (``s``) and keys it by unit, so
    ``(t, i, j)`` is not unique here.  Nothing is cached.  ``Country.
    derivation_inputs`` re-keys ``i`` through ``updated_ids`` afterwards, as
    it does for every entry.
    """
    from .country import Country
    from .local_tools import df_data_grabber

    wave_obj = Country(country)[wave]
    info = wave_obj.resources.get("food_acquired")
    if not info:
        raise ValueError(f"{country}/{wave} declares no food_acquired in data_info.yml")
    spec = dict(info)
    spec["idxvars"] = {k: v for k, v in spec["idxvars"].items() if k in ("i", "j")}
    spec["myvars"] = {name: name for name in RAW_VARIABLES}
    mapping = wave_obj.column_mapping("food_acquired", spec)
    mapping.pop("df_edit", None)
    (filename, cols), = mapping.items()

    out = df_data_grabber(str(wave_obj.file_path / "Data" / filename),
                          cols["idxvars"], **cols["myvars"])
    out = out.reset_index()
    out["t"] = wave
    return out.set_index(["t", "i", "j"])[RAW_VARIABLES]
