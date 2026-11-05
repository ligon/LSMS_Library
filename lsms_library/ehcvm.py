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

where the 7-day purchased quantity is the row's OWN served ``Quantity``, and
that quantity is now the survey's own accounting identity

    ``(s07bq03a - s07bq04 - s07bq05).clip(lower=0)``

-- 7-day consumption net of own production AND of gifts received -- so that
``Expenditure / Quantity`` is exactly the unit value and the two numbers on the
row refer to the same thing.  Decided by @ligon, 2026-09-12 (construction "C"),
on the evidence above: it is the only one of the three candidates the survey's
own file reproduces.  Between 2026-09-12's first landing and this one the
served quantity was ``(s07bq03a - s07bq04)`` ("B"), which left gifts inside the
purchased row; before GH #876 it was that same B quantity beside a raw
``s07bq08``.

``s07bq05`` is no longer dropped on the floor: the gift quantity is served as
its own ``s = 'inkind'`` row, in the consumption unit ``s07bq03b``, with
``Expenditure`` **NaN**.  The survey asks no value for a gift.  UEMOA's
``depan(Don)`` is ``s07bq05`` priced at the last purchase's unit value -- an
IMPUTATION by the people who built the aggregate, not an answer anybody gave --
so the library does not serve it.  Own production keeps the treatment it
already had: ``s = 'produced'`` rows carrying ``s07bq04`` with ``Expenditure``
NaN, which is the same rule.

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

The unit SIZE in the served ``u`` (GH #1224)
--------------------------------------------
``s07bq03b`` is a unit and ``s07bq03c`` that unit's size, and a small and a
large heap are different quantities, so ``food_acquired.u`` carries both:
``Tas (Petit)``, ``Tiya (Entier)``, ``Bol (Grand)`` -- Uganda's ``Heap (Small)``
form, decided by @ligon 2026-11-03.  :func:`carry_unit_size` rewrites the wide
frame's ``u`` before the reshape, so the purchased, produced and in-kind rows
all carry it, and it maps the base through the country's own read-time ``u``
table FIRST, because every read-time mapper is keyed on the bare label and
would pass a sized one through unmapped.  Stripping the suffix therefore gives
exactly the ``u`` the row is served without it.  The size-free code and a
missing size keep the bare unit byte for byte; so do a row with no recorded
unit and a row whose served unit is an exact metric label (``Kg``, ``Litre``),
because the suffix would cost the kg ladder its reading of that label and
``transformations.py`` is not this module's to change.  No quantity is
rescaled and no weight is attached to a size.  ``Expenditure`` is untouched:
:func:`unit_match` reads the unit and size columns, not ``u``.
"""
from __future__ import annotations

import re
import unicodedata

import numpy as np
import pandas as pd


def community_price_slots(base, codes, columns, rank):
    """Expand named price slots with ``obs = slot + len(columns) * rank``.

    Only shared EHCVM mechanics (#1226): callers supply their own source-row
    rank and key/label frame. No eligibility, duplicate removal, quantity,
    unit decoding or aggregation policy lives here. Every supplied row and
    slot survives, including missing prices and equal-valued repeats.
    """
    if not (base.index.equals(codes.index) and base.index.equals(rank.index)):
        raise ValueError('community price base, codes and rank must align')
    if not columns or len(set(columns)) != len(columns):
        raise ValueError('community price slots must be distinct named columns')
    pieces = []
    for slot, column in enumerate(columns, 1):
        piece = base.copy()
        piece['obs'] = slot + len(columns) * rank
        piece['Price'] = pd.to_numeric(codes[column], errors='raise').astype('Float64')
        pieces.append(piece)
    return pd.concat(pieces, ignore_index=True)

from .build_transforms import food_acquired_to_canonical
from .derivations import COLUMN as DERIVATION_COLUMN, union_keys

__all__ = [
    "EHCVM_COUNTRIES", "DERIVATION_NAME", "RAW_VARIABLES",
    "FARM_LEVEL", "MYVARS", "UnitSizeLabelError", "carry_unit_size",
    "derivation_key", "derive_ehcvm_purchase_value", "derive_harvest_to_date",
    "derive_hh_crop_sale_attachment", "food_acquired_ehcvm", "hh_crop_records",
    "inputs_last_purchase", "is_crop_sale", "served_unit_labels", "size_label",
    "sized_unit", "stamp_harvest_to_date",
]

#: The eight countries whose ``food_acquired`` comes from EHCVM section 7B.
EHCVM_COUNTRIES = ("Benin", "Burkina_Faso", "CotedIvoire", "Guinea-Bissau",
                   "Mali", "Niger", "Senegal", "Togo")

#: Name slot of the registry key; the country slot differs per country, so the
#: eight entries share one ``function`` but are eight separate records.  A
#: framework-level (empty country slot) entry would claim all 40 countries.
DERIVATION_NAME = "7day-purchase-at-last-purchase-unit-value"

#: The raw section-7B variables the derivation reads, in questionnaire order.
RAW_VARIABLES = ["s07bq03a", "s07bq03b", "s07bq03c", "s07bq04", "s07bq05",
                 "s07bq07a", "s07bq07b", "s07bq07c", "s07bq08"]

#: ``myvars`` names the twelve ``data_info.yml`` blocks bind the raw
#: last-purchase variables to.  Kept here so the eight YAML files and this
#: module cannot drift apart silently; :func:`food_acquired_ehcvm` reads
#: exactly these column names off the extracted frame.
MYVARS = {
    "ConsumptionUnitCode": "s07bq03b",
    "GiftQuantity": "s07bq05",
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


def _country_of(key: str) -> str:
    """The country slot of a :func:`derivation_key` (``'Togo::...'`` -> ``'Togo'``)."""
    country = str(key).split("::", 1)[0]
    if country not in EHCVM_COUNTRIES:
        raise ValueError(f"{key!r} does not name an EHCVM country: {EHCVM_COUNTRIES}")
    return country


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


# ---------------------------------------------------------------------------
# the unit SIZE in the served unit label (GH #1224)
# ---------------------------------------------------------------------------

#: :func:`_unit_key` values that name the SIZE-FREE code 0 of ``s07bq03c``,
#: as the twelve waves' own value labels spell it (measured, all twelve):
#: ``Taille unique`` -- with ``Unite de taille unique`` (Benin, Mali 2018-19,
#: Togo) already folded onto it by :data:`_SIZE_SYNONYMS` --, Guinea-Bissau's
#: Portuguese ``Tamanho único``, and ``'0'`` for the code-prefixed
#: ``'0. Taille unique'`` of Niger and Senegal 2021-22.  Code 0 carries one of
#: these spellings in every wave and no other code does.  This is the size-free
#: CODE spelled three ways, not a synonym: ``Entier`` is a size and stays one.
_SIZE_FREE_KEYS = frozenset({"taille unique", "tamanho unico", "0"})

#: Lower-cased labels the food kg ladder reads as metric by EXACT match
#: (``transformations.KNOWN_METRIC``'s keys).  A size suffix would cost such a
#: label its metric reading -- ``Kg (Grand)`` is neither an exact key nor a
#: label with a number in it -- so a row whose served base is one of these
#: keeps its unit bare (see :func:`carry_unit_size`).  Copied rather than
#: imported so that no kg-inference symbol reaches a build fingerprint
#: (``tests/test_build_transform_hash.py``); ``tests/test_ehcvm_unit_size.py``
#: fails if the two sets drift apart.  A label with a stated quantity
#: (``Sac (50 Kg)``, ``Bidon de 1,5 L``) keeps its size: the parser still
#: reads the number with the suffix after it.
_EXACT_METRIC_LABELS = frozenset({
    "kg", "kilogram", "kilogramme", "kgs", "kilograms", "kilogrammes",
    "kilo", "kilos", "g", "gram", "gramm", "grams", "gramme", "grammes",
    "gm", "gms", "milligram", "milligramme", "milligrammes", "tonne",
    "tonnes", "l", "litre", "liter", "litres", "liters", "ml", "cl",
    "millilitre", "milliliter", "millilitres", "milliliters", "mili liter",
    "centilitre", "centilitres", "pound", "lbs",
})

#: Lower-cased missing-unit sentinels (``transformations._U_SENTINELS``).  A
#: size on a unit nobody recorded is not a sized unit, and ``Unknown (Petit)``
#: would escape that closed vocabulary into the kg inference.
_MISSING_UNIT_LABELS = frozenset({"unknown", "manquant"})

_SIZE_CODE_PREFIX = re.compile(r"^\d+\s*[.\-]\s*")


class UnitSizeLabelError(Exception):
    """A sized ``u`` label could not be built so that it is served as written.

    Subclasses ``Exception``, not ``RuntimeError`` / ``ValueError`` /
    ``KeyError``: the derived-table paths in ``Country.__getattr__`` catch those
    and fall back to another build, so ``food_prices()`` would report "Could not
    materialize" and lose this instruction (the ``IdWalkError`` /
    ``ClusterIdentityError`` rule).  Raised inside a ``make`` subprocess it still
    reaches the parent as ``CalledProcessError``; the message is then in the
    subprocess's stderr, which ``run_make_target`` passes through.
    """


def size_label(value):
    """The served spelling of one ``s07bq03c`` size, or ``None``.

    ``None`` for a missing size and for the size-free code
    (:data:`_SIZE_FREE_KEYS`).  Otherwise the instrument's own value label
    with only its case normalised to an initial capital (``'Très Petite'`` ->
    ``'Très petite'``, ``'Sans Brassée'`` -> ``'Sans brassée'``).  What is
    removed is not label text: padding (Mali 2018-19), the Stata code prefix
    of a numbered value label (``'1. Petit'``, Niger / Senegal 2021-22), and
    UTF-8-read-as-Latin-1 mojibake (``'TrÃ¨s petit'``, Senegal 2018-19), so
    the accents the instrument wrote are the ones served.  A code with no
    value label in the file is served as its code (Togo 2018 code ``98``, 208
    rows): the same convention as an undecodable unit code -- leave the code,
    invent no label.
    """
    key = _unit_key(value)
    if key is None or key in _SIZE_FREE_KEYS:
        return None
    if isinstance(value, str):
        s = value.strip()
        try:
            s = s.encode("latin-1").decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            pass
        s = _SIZE_CODE_PREFIX.sub("", s).strip() or s
    else:                                   # a bare numeric code
        s = key
    s = re.sub(r"\s+", " ", s)
    return s[:1].upper() + s[1:].lower()


def sized_unit(base, size):
    """``'<base> (<Size>)'`` where ``size`` names a size, else ``base``.

    ``base`` must already be the unit label the country SERVES (the read-time
    ``u`` table applied); ``size`` is a raw ``s07bq03c`` label and goes
    through :func:`size_label`.  The Uganda form (``Heap (Small)``), and the
    form Niger 2018-19 ``community_prices`` already serves (GH #823).
    """
    label = size_label(size)
    if label is None or base is None or (not isinstance(base, str) and pd.isna(base)):
        return base
    return f"{base} ({label})"


def served_unit_labels(country: str, values) -> np.ndarray:
    """The ``u`` the Country API serves for each raw label in ``values``.

    Applies ``Country._apply_categorical_mappings`` -- the read-time
    auto-apply of the merged ``u`` table (country rows over the additive
    global ``categorical_mapping/u.org``) -- to a one-level frame of the
    values, so the label is exactly the one ``_finalize_result`` would serve
    rather than a second reading of the same tables that could drift.  Labels
    no table names pass through unchanged, as they do on the served path.
    """
    from .country import Country
    vals = np.asarray(list(values), dtype=object)
    if not len(vals):
        return vals
    probe = pd.DataFrame(index=pd.MultiIndex.from_arrays(
        [pd.Index(vals, dtype=object), np.arange(len(vals))],
        names=["u", "_row"]))
    out = Country(country)._apply_categorical_mappings(probe)
    return np.asarray(out.index.get_level_values("u"), dtype=object)


def carry_unit_size(frame: pd.DataFrame, sizes, country: str,
                    decode=None) -> pd.DataFrame:
    """Rewrite ``frame``'s ``u`` level to carry the consumption unit SIZE.

    ``frame`` is the wide ``food_acquired`` frame (one row per household x
    item), ``sizes`` its ``s07bq03c`` labels, aligned by position.  A row whose
    size names a size (:func:`size_label`) gets ``u = '<served unit>
    (<Size>)'`` -- ``Tas (Petit)``, ``Tiya (Entier)`` -- where ``<served
    unit>`` is the label the Country API serves for that row today
    (:func:`served_unit_labels`, after ``decode``: the wave's own mojibake
    repair, which its hook applies to the index AFTER this runs).  Composing
    BEFORE the suffix is what keeps the label honest: the read-time ``u``
    table is keyed on bare labels and passes a sized label through untouched,
    so a raw ``139. Sachet (Petit)`` would be served as is.

    Every other row keeps its ``u`` byte for byte: a size-free row, a row with
    no recorded size, a row with no unit or the missing-unit sentinel, and a
    row whose served unit is an exact metric label (:data:`
    _EXACT_METRIC_LABELS`, e.g. ``Kg`` or ``Litre`` with a size: 185 rows in
    the twelve waves), which a suffix would hide from the kg ladder.

    Invariant, checked here and raising :class:`UnitSizeLabelError` if broken: on every
    sized row, stripping the trailing `` (<Size>)`` gives exactly the ``u``
    the row was served before -- the read-time table maps no sized label, and
    ``decode`` leaves every sized label unchanged.  Quantities, values and
    row counts are not touched here; two items whose sizes differ simply no
    longer share a key in the later additive collapse.

    Because the mapped base is written into the parquet, a sized row does not
    follow a later change to ``Country._apply_categorical_mappings`` the way a
    bare row does.  The ``u`` tables are hashed; that function is outside the
    fingerprint, so a semantic edit to it needs an ``LSMS_CACHE_SCHEMA`` bump.
    """
    if "u" not in (frame.index.names or []):
        raise UnitSizeLabelError("carry_unit_size: frame has no `u` index level")
    u = pd.Series(np.asarray(frame.index.get_level_values("u"), dtype=object))
    labels = pd.Series(np.asarray(sizes, dtype=object)).map(size_label)
    has_unit = u.map(lambda x: isinstance(x, str) and bool(x.strip()))
    sized = labels.notna().to_numpy() & has_unit.to_numpy()
    if not sized.any():
        return frame
    base = u[sized]
    if decode is not None:
        base = base.map(decode)
    uniq = pd.unique(base.to_numpy())
    served = dict(zip(uniq, served_unit_labels(country, uniq)))
    bad = [b for b, s in served.items() if not isinstance(s, str) or not s.strip()]
    if bad:
        raise UnitSizeLabelError(f"{country}: the u table serves no label for {bad[:5]}")
    served_base = base.map(served)
    keep_bare = served_base.map(lambda s: s.strip().lower() in _EXACT_METRIC_LABELS
                                or s.strip().lower() in _MISSING_UNIT_LABELS)
    new = served_base + " (" + labels[sized] + ")"
    new = new.where(~keep_bare, u[sized])

    minted = pd.unique(new[~keep_bare].to_numpy())
    remapped = served_unit_labels(country, minted)
    moved = [(a, b) for a, b in zip(minted, remapped) if a != b]
    if moved:
        raise UnitSizeLabelError(
            f"{country}: the read-time u table would relabel sized units "
            f"{moved[:5]}; map them in the hook instead")
    if decode is not None:
        changed = [m for m in minted if decode(m) != m]
        if changed:
            raise UnitSizeLabelError(
                f"{country}: the wave's decode would rewrite sized units "
                f"{changed[:5]}")

    out_u = u.copy()
    out_u[sized] = new.to_numpy()
    names = list(frame.index.names)
    arrays = [out_u.to_numpy() if n == "u" else frame.index.get_level_values(n)
              for n in names]
    out = frame.copy()
    out.index = pd.MultiIndex.from_arrays(arrays, names=names)
    return out


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
    consumption ``s07bq03a`` net of own production ``s07bq04`` AND of gifts
    received ``s07bq05``, clipped at zero (the survey's own accounting
    identity; @ligon, 2026-09-12) -- in unit ``(quantity_unit, quantity_size)``
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
    are computable from them -- UEMOA's own annual ``Achat`` figure is this
    times ``365/7``, and the pre-#876 served value was ``s07bq08`` itself.

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
                        drop_columns=("visit",), decode=None) -> pd.DataFrame:
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

    The consumption unit's SIZE ``s07bq03c`` rides in ``u`` (GH #1224):
    :func:`carry_unit_size` rewrites the wide frame's ``u`` before the
    reshape, so the purchased, produced and in-kind rows -- all three in the
    consumption unit -- carry the same ``Tas (Petit)``.  ``decode`` is the
    wave's own scalar mojibake repair, for the three waves whose hook sweeps
    the index after this returns (CotedIvoire, Guinea-Bissau, Niger
    2018-19); the served base unit is read through it.
    """
    missing = [c for c in MYVARS if c not in df.columns]
    if missing:
        raise KeyError(
            f"food_acquired_ehcvm: the wave's data_info.yml must bind "
            f"{missing} (see lsms_library.ehcvm.MYVARS); got {list(df.columns)}")

    # Before anything else, so every series below aligns on the one index.  The
    # unit match behind Expenditure reads the unit and size COLUMNS, not `u`.
    df = carry_unit_size(df, df["ConsumptionUnitSize"].to_numpy(),
                         _country_of(key), decode=decode)

    gifts = df["GiftQuantity"].fillna(0)
    purchased_qty = (df["Quantity"].fillna(0)
                     - df["Produced"].fillna(0)
                     - gifts).clip(lower=0)
    expenditure = derive_ehcvm_purchase_value(
        purchased_qty,
        df["ConsumptionUnitCode"], df["ConsumptionUnitSize"],
        df["LastPurchaseQuantity"],
        df["LastPurchaseUnitCode"], df["LastPurchaseUnitSize"],
        df["LastPurchaseValue"])

    work = df.drop(columns=list(MYVARS))

    # `food_acquired_to_canonical` computes the purchased side as
    # (Quantity - Produced).clip(0) and the produced side as Produced.  Handing
    # it the GIFT-NET total makes its purchased side exactly `purchased_qty`
    # above -- the survey's own identity -- and leaves its produced side, which
    # reads `Produced` directly, untouched.  Doing it this way rather than
    # subtracting gifts inside the shipped reshape is deliberate: that function
    # is every country's, and this identity is EHCVM's.
    net_total = work["Quantity"] - gifts
    out = food_acquired_to_canonical(
        work.assign(Quantity=net_total, Expenditure=expenditure),
        drop_columns=drop_columns)

    inkind = _inkind_rows(work, gifts, drop_columns)
    if len(inkind):
        out = pd.concat([out, inkind])

    is_purchased = out.index.get_level_values("s").to_numpy() == "purchased"
    out[DERIVATION_COLUMN] = pd.Series(
        np.where(is_purchased, key, None), index=out.index, dtype="string")
    return out


def _inkind_rows(work: pd.DataFrame, gifts: pd.Series, drop_columns) -> pd.DataFrame:
    """``s = 'inkind'`` rows carrying ``s07bq05`` at the consumption unit.

    Built by running the SHIPPED reshape a second time with ``Quantity`` and
    ``Produced`` both set to the gift quantity -- its purchased side is then
    identically zero with a NaN value and is filtered out, and its produced
    side is the gift quantity under the same index construction as every other
    row -- and relabelling that side ``inkind``.  Reusing the reshape rather
    than assembling the index by hand is what keeps the two sets of rows on the
    same key for certain.

    ``Expenditure`` is NaN by construction: EHCVM asks no value for a gift.
    """
    gift_frame = work.assign(Quantity=gifts, Produced=gifts,
                             Expenditure=np.nan)
    got = food_acquired_to_canonical(gift_frame, drop_columns=drop_columns)
    got = got[got.index.get_level_values("s").to_numpy() == "produced"]
    if not len(got):
        return got
    levels = list(got.index.names)
    flat = got.reset_index()
    flat["s"] = "inkind"
    return flat.set_index(levels)


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
    served table splits one row into up to three (``s``) and keys it by unit,
    so ``(t, i, j)`` is not unique here.  Nothing is cached.  ``Country.
    derivation_inputs`` re-keys ``i`` through ``updated_ids`` afterwards, as
    it does for every entry.

    "Joins to the served table" is a PROMISE, and keeping it takes more than
    reusing the YAML's ``idxvars``.  Three waves' ``mapping.py`` post-process
    the reshaped frame's INDEX -- Guinea-Bissau, Niger 2018-19 and CotedIvoire
    2018-19 sweep value-label mojibake out of every string level
    (``_decode_mojibake_in_df``) -- so the served ``j`` is ``'Arachide
    grillee'`` with a real accent where an unswept extraction says ``'Arachide
    grillÃ©e'``.  Skipping that step cost 80,563 of CotedIvoire's 218,224
    purchased keys (37%) and 164 of Guinea-Bissau's before it was measured.
    Those normalisers are applied here too, to the INDEX ONLY: the raw columns
    keep the bytes the file holds, because they are the point of this frame.
    ``tests/test_ehcvm_7day.py::test_inputs_join_to_the_served_rows`` runs over
    all twelve cells, so a wave that adds a fourth kind of index rewrite fails
    rather than silently unjoins.
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
    # The table's own `df_edit` is `food_acquired_ehcvm` -- the whole reshape --
    # so it cannot run here; its index normalisers are applied below instead.
    mapping.pop("df_edit", None)
    (filename, cols), = mapping.items()

    out = df_data_grabber(str(wave_obj.file_path / "Data" / filename),
                          cols["idxvars"], **cols["myvars"])
    out = out.reset_index()
    out["t"] = wave
    out = out.set_index(["t", "i", "j"])[RAW_VARIABLES]
    return _normalise_index_as_served(out, wave_obj)


#: Module-level callables in a wave's ``mapping.py`` that rewrite the reshaped
#: frame's INDEX on the served path, and must therefore be applied to the
#: raw-input frame's index too or it will not join.  Named rather than
#: discovered, so adding one is a deliberate act; the twelve-cell join test is
#: what catches an unnamed one.
_SERVED_INDEX_NORMALISERS = ("_decode_mojibake_in_df",)


def _normalise_index_as_served(frame: pd.DataFrame, wave_obj) -> pd.DataFrame:
    """Apply the wave's served-path index rewrites to ``frame``'s index only."""
    fns = wave_obj.formatting_functions or {}
    for name in _SERVED_INDEX_NORMALISERS:
        fn = fns.get(name)
        if fn is None:
            continue
        # An index-only frame, so the normaliser cannot touch a raw column.
        frame.index = fn(pd.DataFrame(index=frame.index)).index
    return frame


# ---------------------------------------------------------------------------
# crop_production of an EHCVM-2 wave (GH #552; rulings @ligon 2026-11-03)
# ---------------------------------------------------------------------------
#
# EHCVM-2 splits the module EHCVM-1 kept on one line: s16c holds the plot-by-crop
# harvest, and s16d, with no plot, what became of each crop.  Benin, Cote d'Ivoire and
# Guinea-Bissau read as Togo does.  Mali has its own copy of the two functions below,
# in mali.py, and Niger its older rule; neither imports these, so this section is in
# neither's fingerprint.  Check that s16cq16a is filled before assuming a wave is
# like Togo (10,325 of 10,877 rows): Mali's is on 309 of 7,247, which is GH #1437.

#: The canonical structural sentinel for ``plot_id``: the record has no plot by
#: design (lsms_library/data_info.yml, top of file).
FARM_LEVEL = "Farm-level"

_SALE_KEYS = ["i", "crop"]
_SALE_COLUMNS = ["Quantity_sold", "Unit_sold", "Value_sold"]


def hh_crop_records(frame: pd.DataFrame, crop: str = "s16dq01") -> pd.DataFrame:
    """``s16d`` with one record per (grappe, menage, crop), or a refusal.

    The export repeats a crop's record once per harvest line, byte for byte
    (Niger 2021-22, GH #824; Togo 2021-22: 1,111 of 10,325 rows).  A line
    identical to another on every column is the same record, not a second sale.
    A pair that still has two different records is not an artefact of that kind,
    so it raises rather than picking one.
    """
    out = frame[~frame.duplicated(keep="first")]
    if out.duplicated(["grappe", "menage", crop]).any():
        raise ValueError("s16d: two different records for one (household, crop)")
    return out


def is_crop_sale(quantity, value) -> pd.Series:
    """A sale is a record with a positive sold quantity or a positive value."""
    return (pd.to_numeric(quantity, errors="coerce").gt(0)
            | pd.to_numeric(value, errors="coerce").gt(0))


def derive_hh_crop_sale_attachment(harvest: pd.DataFrame, sales: pd.DataFrame,
                                   key: str) -> pd.DataFrame:
    """Place each household-crop sale on the served rows of a wave (option (d)).

    ``harvest`` holds one row per plot-crop harvest record, with ``i``, ``crop`` and
    ``plot_id``.  ``sales`` holds one row per sale, with ``i``, ``crop``,
    ``Quantity_sold``, ``Unit_sold`` and ``Value_sold``; a second sale of one
    (i, crop) raises.

    A sale of an (i, crop) with EXACTLY ONE harvest row goes on that row, in its own
    ``Unit_sold``, and that row alone carries ``key`` in ``Derivation``.  Any other
    sale becomes ONE new row of that (i, crop) with ``plot_id = FARM_LEVEL``, its
    other columns NA; the index value discloses it, so it carries no key.

    No sold quantity or value is split, summed or scaled.  Registered per country as
    ``<country>::crop_production::hh-crop-sale-single-row``.
    """
    if sales.duplicated(_SALE_KEYS).any():
        raise ValueError("crop_production: more than one sale for a (i, crop)")
    lines = harvest.groupby(_SALE_KEYS).size().rename("_lines").reset_index()
    n = sales[_SALE_KEYS].merge(lines, on=_SALE_KEYS, how="left")["_lines"]
    single = n.fillna(0).eq(1).to_numpy()
    served = harvest.merge(sales.loc[single, _SALE_KEYS + _SALE_COLUMNS],
                           on=_SALE_KEYS, how="left", validate="many_to_one",
                           indicator="_attached")
    derivation = pd.Series(pd.NA, index=served.index, dtype="string")
    derivation[served.pop("_attached").eq("both")] = key
    served[DERIVATION_COLUMN] = derivation
    farm = sales.loc[~single, _SALE_KEYS + _SALE_COLUMNS].copy()
    farm["plot_id"] = FARM_LEVEL
    return pd.concat([served, farm], ignore_index=True)


def derive_harvest_to_date(quantity):
    """The identity: a recorded harvest of an unfinished crop, served as recorded.

    The registry is the point.  s16cq16a asks the quantity ALREADY harvested, so on a
    row whose harvest is not finished the served ``Quantity`` covers a part of the
    season under a name that implies all of it.
    """
    return quantity


def stamp_harvest_to_date(frame: pd.DataFrame, key: str) -> pd.DataFrame:
    """Key the rows with ``finished == 'Non'`` and a recorded ``Quantity``."""
    out = frame.copy()
    out["Quantity"] = derive_harvest_to_date(pd.to_numeric(out["Quantity"], errors="coerce"))
    mask = (out["finished"].astype("string").str.strip().eq("Non").fillna(False).astype(bool)
            & out["Quantity"].notna())
    derivation = out[DERIVATION_COLUMN].astype("string").copy()
    derivation[mask] = pd.array([union_keys((value, key)) for value in derivation[mask]],
                                dtype="string")
    out[DERIVATION_COLUMN] = derivation
    return out
