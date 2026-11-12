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
  was reported in (``s07bq07b/c`` vs ``s07bq03b/c``).  This valuation converts
  nothing.  The 2018-19 waves ship no unit-conversion table
  (``lsms_library/categorical_mapping/ehcvm_units.org`` is a code->label codebook
  with no factors), and four 2021-22 waves' ``ehcvm_nsu`` table (below; this
  sentence used to say no wave ships one) weighs a unit, which is not used to
  move a price between units.  So the honest answer is NA.

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

The survey's own unit weights (GH #1452)
----------------------------------------
Burkina Faso, Mali, Niger and Senegal 2021-22 ship ``ehcvm_nsu_*2021.dta``:
product x unit x size -> grams, from a market survey that weighs the product
(Mali's BID s.3.4; Cote d'Ivoire's NSU questionnaire).  Mali's BID s.2.3 says the
factors were collected in 2018/19, "remain valid", and were supplemented in 2021;
it is Mali's document, and Niger's and Senegal's folders hold none.
:func:`food_acquired_with_unit_weights` wraps :func:`food_acquired_ehcvm` for both
2018-19 and 2021-22 in those four countries and serves a per-row ``Quantity_kg``
from it, which the food kg ladder reads first (``survey_kg``).  The other EHCVM
countries retain their existing :func:`food_acquired_ehcvm` wiring.  The 2018-19
product join uses labels rather than assuming product codes carry across waves.

*National, not by stratum: a modelling choice.*  :func:`nsu_unit_weights` takes the
median of a key's rows across the table.  Against a household's last-purchase unit
value over the same stratum's median ``Kg`` price, cells with 5+ reports each, the
national weight's median absolute log error is 0.27 against the stratum cell's 0.44
in Burkina Faso, 0.49 against 0.51 in Mali, 0.22 against 0.24 in Niger; Senegal's
stratum file repeats its national table 28 times.  The same test row by row narrows
in Burkina and Mali and REVERSES in Niger, 0.31 against 0.24, though the national
weight stays ahead within 2x (87.8% against 81.8%).  Against an outside reference,
Niger's market weighings of ``Tiya``, national beats stratum, 0.170 against 0.186,
and a stratum's departure from the national weight predicts nothing (slope 0.014).
The national weight also serves more rows: stratum cells cover 75% / 87% / 77% of
Burkina's / Mali's / Niger's consumed rows, the national weights 90% / 100% / 99.9%.
Stratum weights would give 1.6-7.3% more kilograms on the rows both rules serve.

*Kg and Litre keep their labels.*  Their table rows are not used
(:data:`_EXACT_METRIC_LABELS`): a kilogram is a kilogram, and a table litre is
not a density (oils at exactly 1000 g, Niger's ``Lait caille`` at 4371 g).
Those rows are the check on the join instead: the national table weight of a
``Kg`` row is 1000 g at the median in all four countries.  A container that
states a size (``Sac (25 Kg)``, ``Paquet (250 g)``) does read the table, because
the table is what such a container held.  It agrees with the label on every
stated size but one, Senegal's ``Paquet (250 g)``, where it reads a median 25 g.
The label is the defect: the market file weighs "paquet de 25g" tea at 25 g on 477
observations, which owes nothing to household prices, and the same unit is sized
Petit / Moyen / Grand.  Row by row, households put the table within 2x on 75-85% of
those rows and the label on 12-13% (methods in ``Senegal/_/CONTENTS.org``).
:class:`StatedLabelOverruledWarning` reports such a disagreement when the wave builds.

*2018-19 reads the same table, by label (ruling @ligon 2026-11-04).*  The factors were
mostly collected in that round, and one label should not carry two weights across the
boundary.  The wave reads the same weight per key that 2021-22 reads.  But a product CODE
does not carry over: the tables use 2021-22's numbering, in both of Niger's years, and most
product codes the two waves' household files share carry different labels (Burkina Faso 124
of 135, Mali 123 of 134, Niger 126 of 138, Senegal 128 of 140; Niger's 118 is ``Sel`` in
2018-19 and ``Pate d'arachide`` in 2021-22).  So
:func:`nsu_product_crosswalk` joins the PRODUCT by label, in tiers, and never falls back on
the code.  UNIT and SIZE codes do mean the same in both waves (tested), so they are read as
codes.  A row whose product finds no label stays on the inferred ladder.

*Earlier waves do not read it (option B, ruling @ligon 2026-11-05).*  Burkina Faso 2014 and
Mali 2014-15 keep the estimate they had: :func:`lsms_library.transformations.food_kg_factors`
estimates their rows with this key's ``Quantity_kg`` taken out, because a survey kilogram
otherwise leaves the per-unit estimate, and with both newer waves on the table an older wave
would estimate from itself alone.  The name it scopes on is :data:`NSU_DERIVATION_NAME`.

No row is rescaled and no ``u`` changes.  A row the table has no weight for, and
a row whose key is duplicated at the hook, keep ``Quantity_kg`` NA and stay on
the inferred ladder: ``country._normalize_dataframe_index`` SUMS ``Quantity``
and ``Expenditure`` on a duplicated key and SELECTS any other column, so a
kilogram on such a row would describe one row of a summed group.  That loses a
table kilogram on 1,218 rows (Mali 891, Niger 8, Senegal 262, Burkina Faso 57), at
most 0.54% of a country's table kilograms.
"""
from __future__ import annotations

import re
import unicodedata
import warnings

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
from .derivations import COLUMN as DERIVATION_COLUMN


def crop_condition_codes(codes, table):
    """Map numeric condition codes using the caller's own instrument dictionary.

    Return nullable strings; the harvest caller supplies its index sentinel.
    Dictionary acquisition, validation and answer domains stay with the caller.
    Fractional numeric codes retain the nullable-integer conversion error.
    """
    mapped = codes.astype('Int64').map(table)
    return mapped.astype('string')


def crop_condition_labels(labels):
    """Keep literal recorded answers, stripping whitespace and blanking empties.

    Instrument-specific replacements for unlabelled codes belong to the caller.
    """
    out = pd.Series(labels).astype('string').str.strip()
    return out.where(out != '', pd.NA)


__all__ = [
    "crop_condition_codes", "crop_condition_labels",
    "EHCVM_COUNTRIES", "DERIVATION_NAME", "RAW_VARIABLES",
    "MYVARS", "UnitSizeLabelError", "carry_unit_size", "derivation_key",
    "derive_ehcvm_purchase_value", "food_acquired_ehcvm",
    "inputs_last_purchase", "served_unit_labels", "size_label", "sized_unit",
    "NSU_COUNTRIES", "NSU_DERIVATION_NAME", "NSU_JOIN_TIERS", "NSU_RAW_VARIABLES",
    "NSU_TABLES", "NSU_TABLE_WAVE", "nsu_product_crosswalk",
    "StatedLabelOverruledWarning", "UnitWeightJoinError", "derive_nsu_kg_factor",
    "food_acquired_with_unit_weights", "inputs_unit_weight",
    "nsu_derivation_key", "nsu_unit_weights",
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
    from .country import _apply_build_categorical_mappings
    vals = np.asarray(list(values), dtype=object)
    if not len(vals):
        return vals
    probe = pd.DataFrame(index=pd.MultiIndex.from_arrays(
        [pd.Index(vals, dtype=object), np.arange(len(vals))],
        names=["u", "_row"]))
    out = _apply_build_categorical_mappings(country, probe)
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
    bare row does. The ``u`` tables and the mapper's callable closure are
    hashed through this build import path (GH1454), so semantic mapper edits
    invalidate the affected build outputs automatically.
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
    (``last_quantity`` missing or <= 0) or where the two units differ: this
    valuation converts nothing between units (see the module docstring).

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


# ---------------------------------------------------------------------------
# sample.strata: the documented stratum, region x milieu (GH #812)
# ---------------------------------------------------------------------------
#
# Used by Senegal, Mali, Niger and Burkina Faso.  Benin, Togo, Guinea-Bissau and
# Cote d'Ivoire serve the region alone (`strata: s00q01`) and do not import this.
# No EHCVM household file records a stratum; the BIDs say it is the combination
# of region and area of residence.  Labels and never codes: `s00q01` 1 is Boucle
# du Mouhoun in Burkina's 2018-19 cover and Hauts-Bassins in its 2021-22 cover.

#: The cover-page variables a stratum is built from: region, then milieu.
STRATUM_RAW_VARIABLES = ("s00q01", "s00q04")


class StratumError(Exception):
    """A wave's ``strata`` cannot be served: a code where a label belongs, or a
    cluster holding two strata.

    Subclasses ``Exception``, not ``ValueError``.  ``Country._join_v_from_sample``
    catches ``ValueError`` and logs it at INFO, so a refusal in ``sample()`` would
    be loud only on a direct call, and every household table would come back with
    no ``v`` (measured by injection: Niger ``household_roster`` 121,956 rows, Mali
    220,732).  Defined here rather than in a by-path ``mapping.py`` so it pickles
    across parallel wave workers (GH #1524).
    """


def stratum_derivation_key(country: str) -> str:
    """The registry key for one country's region x milieu ``strata``."""
    return f"{country}::sample::region-milieu-stratum"


def derive_region_milieu_stratum(region, milieu):
    """``'<region label> <milieu label>'``, as the cover page writes them.

    NA where either is missing or blank.  A number is REFUSED: a code that lost
    its value label would otherwise be served as a label, and codes do not carry
    across waves.  Region spellings are left as the file has them here; the
    country's ``#+name: strata`` table folds them at read time, row for row its
    ``Region`` table, so the region part of ``strata`` is ``cluster_features.Region``.

    No options, by design (``SkunkWorks/derived_values.org``).  A user who wants
    another stratification re-derives it from ``Country(c).derivation_inputs(key)``.
    """
    parts = []
    for name, value in (("region", region), ("milieu", milieu)):
        if pd.isna(value):
            return pd.NA
        if not isinstance(value, str):
            raise StratumError(f"{name} {value!r} is not a label: a stratum is built "
                               f"from value labels, never from codes (GH #812)")
        value = value.strip()
        if not value:
            return pd.NA
        parts.append(value)
    return " ".join(parts)


def strata_label(row):
    """Cell formatter: ``strata: [s00q01, s00q04, {mapping: strata_label}]``."""
    return derive_region_milieu_stratum(row.iloc[0], row.iloc[1])


def stamp_region_milieu_stratum(df: pd.DataFrame, country: str) -> pd.DataFrame:
    """The ``sample`` hook: stamp the registry key on every row with a stratum.

    A row the merge brought in with no cover page has no ``strata`` and no key.
    """
    out = df.copy()
    key = pd.Series(stratum_derivation_key(country), index=out.index, dtype="string")
    out["Derivation"] = key.where(out["strata"].notna())
    return out


def inputs_region_milieu_stratum(country: str, wave: str) -> pd.DataFrame:
    """The raw cover-page answers behind one wave's ``strata``, at ``(t, i)``.

    ``i`` and the file list come from the wave's own ``sample.df_cover`` block so
    the two cannot drift (Burkina 2021-22 reads two covers, each with its own
    ``i``).  The columns are the ORIGINAL names with the labels the file holds;
    ``Country.derivation_inputs`` re-keys ``i`` through ``updated_ids``.
    """
    from .country import Country
    from .local_tools import df_data_grabber

    wave_obj = Country(country)[wave]
    cover = wave_obj.resources["sample"]["df_cover"]
    spec = dict(cover)
    spec["idxvars"] = {"i": cover["idxvars"]["i"]}
    spec["myvars"] = {name: name for name in STRATUM_RAW_VARIABLES}
    mapping = wave_obj.column_mapping("sample", spec)
    mapping.pop("df_edit", None)
    frames = [df_data_grabber(str(wave_obj.file_path / "Data" / filename),
                              cols["idxvars"], **cols["myvars"])
              for filename, cols in mapping.items()]
    out = pd.concat(frames).reset_index()
    out["t"] = wave
    return out.set_index(["t", "i"])[list(STRATUM_RAW_VARIABLES)]


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
# the survey's own unit weights, 2021-22 (GH #1452)
# ---------------------------------------------------------------------------

#: The countries whose 2021-22 wave ships ``ehcvm_nsu_*2021.dta``.
NSU_COUNTRIES = ("Burkina_Faso", "Mali", "Niger", "Senegal")

#: Name slot of the registry key: one record per country, one shared function.
NSU_DERIVATION_NAME = "nsu-national-unit-weight"

#: The section-7B variables the unit weight reads, in questionnaire order.
NSU_RAW_VARIABLES = ["s07bq01", "s07bq03a", "s07bq03b", "s07bq03c", "s07bq04",
                     "s07bq05"]


#: The wave whose product codes the tables use, and whose source they align with.
NSU_TABLE_WAVE = "2021-22"

#: What each country's table is.  Why each column, CONTENTS.org and the registry:
#: Burkina Faso's ``poids_moyen`` because its two labels are swapped; Niger's ``mu``
#: because ``p50`` is scaled, and ``annee`` because the file is cumulative; Senegal's
#: ``_nat`` because its three files are one table.
NSU_TABLES = {
    "Burkina_Faso": dict(table="ehcvm_nsu_bfa2021.dta", product="codpr", unit="uniteID",
                         size="tailleID", weight="poids_moyen", year=None, names=None,
                         sources={"2018-19": "s07b_me_bfa2018.dta",
                                  "2021-22": "s07b_me_bfa2021.dta"}),
    "Mali": dict(table="ehcvm_nsu_mli2021.dta", product="produitID", unit="uniteID",
                 size="tailleID", weight="poids", year=None, names=None,
                 sources={"2018-19": "s07b_me_mli2018.dta",
                          "2021-22": "s07b_me_mli2021.dta"}),
    "Niger": dict(table="ehcvm_nsu_ner2021.dta", product="produit", unit="unite",
                  size="taille", weight="mu", year="annee", names=None,
                  sources={"2018-19": "s07b_me_ner2018.dta",
                           "2021-22": "s07b_me_ner2021.dta"}),
    "Senegal": dict(table="ehcvm_nsu_sen2021_nat.dta", product="produitID", unit="uniteID",
                    size="tailleID", weight="poids", year=None, names="produitNom",
                    sources={"2018-19": "s07b_me_sen2018.dta",
                             "2021-22": "s07b_me_sen2021.dta"}),
}

#: How a row's product found its table weight.  ``code``: 2021-22, whose codes are the
#: table's.  The rest are 2018-19's label tiers, tried in this order: ``label``, the
#: wave's label equals the table's or 2021-22's label for a table code, up to spelling;
#: ``harmonized``, the two meet through the country's ``harmonize_food``; ``parts``, the
#: label lists goods the instrument later split (``Aubergine, Courge/Courgette``), and
#: reads all of them; ``stem``, they agree before the first ``(``, ``,`` or ``/``, which
#: is what truncated labels need (Mali's ``Cube alimentaire (Maggi, Ju``).
NSU_JOIN_TIERS = ("code", "label", "harmonized", "parts", "stem")

#: A label whose parts are not separated by punctuation.  One entry, large enough to
#: name: sugar is 5,865 of Burkina Faso's and 6,547 of Mali's 2018-19 consumed rows.
_NSU_BUNDLES = {"sucre poudre ou morceaux": ("sucre poudre", "sucre morceaux",
                                              "sucre en poudre", "sucre en morceaux")}


class UnitWeightJoinError(Exception):
    """The wide ``food_acquired`` frame is not the source file, row for row.

    Subclasses ``Exception`` for the reason :class:`UnitSizeLabelError` does:
    the derived-table paths catch ``RuntimeError`` / ``ValueError`` /
    ``KeyError`` and would quietly retry another build.
    """


class StatedLabelOverruledWarning(UserWarning):
    """A unit-weight table disagrees with the mass a unit's own label states.

    ``survey_kg`` outranks ``metric`` in the food ladder, so nothing else reports
    this: ``transformations.StatedVsInferredKgWarning`` compares a label with an
    INFERRED factor.  Raised when the wave builds, so a warm read does not repeat
    it; the same comparison stays available as ``stated_kg`` beside
    ``nsu_kg_per_unit`` in ``Country(c).derivation_inputs(key)``.
    """


#: How far a table weight may sit from a label's stated mass before the build says
#: so.  Senegal's ``Paquet (250 g)`` sits at 10x; every other stated size in the
#: four tables sits within 2x at the median.
STATED_LABEL_TOLERANCE = 2.0

#: The stated masses and volumes in the four countries' unit labels.  A copy of the
#: part of ``transformations._EXPLICIT_METRIC_PATTERNS`` they need, for the reason
#: :data:`_EXACT_METRIC_LABELS` is a copy; ``tests/test_ehcvm_unit_weights.py`` fails
#: if it reads any of those labels differently from ``_parse_explicit_metric``.
_STATED_QUANTITY = re.compile(
    r"(?<![\d.,])(\d+(?:,\d+)?)\s*"
    r"(kilogrammes?|kilograms?|kgs?|grammes?|grams?|gms?|grs?|g|centilitres?|cl|"
    r"millilitres?|ml|litres?|liters?|ltrs?|lts?|l)\b", re.IGNORECASE)
_STATED_SCALE = (("k", 1.0), ("gr", 1e-3), ("gm", 1e-3), ("g", 1e-3), ("c", 1e-2),
                 ("m", 1e-3), ("l", 1.0))


def _stated_kg(label) -> float:
    """The kilograms (or litres) a served unit label states, else NaN."""
    if not isinstance(label, str):
        return np.nan
    m = _STATED_QUANTITY.search(label)
    if m is None:
        return np.nan
    unit = m.group(2).lower()
    scale = next(v for k, v in _STATED_SCALE if unit.startswith(k))
    return float(m.group(1).replace(",", ".")) * scale


def _report_overruled_labels(country: str, wave: str, labels, factor) -> None:
    """Warn where a table weight sits far from the mass the row's label states."""
    frame = pd.DataFrame({"u": np.asarray(labels, dtype=object),
                          "factor": np.asarray(factor, dtype=float)}).dropna()
    frame["stated"] = frame["u"].map(_stated_kg)
    frame = frame[frame["stated"] > 0]
    if frame.empty:
        return
    ratio = frame["factor"] / frame["stated"]
    wide = (ratio > STATED_LABEL_TOLERANCE) | (ratio < 1 / STATED_LABEL_TOLERANCE)
    if not wide.any():
        return
    by_unit = pd.DataFrame({"u": frame["u"], "wide": wide, "ratio": ratio}).groupby("u").agg(
        rows=("wide", "size"), wide=("wide", "sum"), median=("ratio", "median"))
    by_unit = by_unit[by_unit["wide"] > 0].sort_values("wide", ascending=False)
    text = "; ".join(f"{u!r} {int(r.wide):,} of {int(r.rows):,} rows, median table / label "
                     f"{r['median']:.2f}" for u, r in by_unit.iterrows())
    warnings.warn(StatedLabelOverruledWarning(
        f"{country} {wave}: the unit-weight table overrules a stated label by more than "
        f"{STATED_LABEL_TOLERANCE:g}x: {text}.  The table is served; compare stated_kg "
        f"with nsu_kg_per_unit in derivation_inputs (GH #1452)."), stacklevel=3)


def nsu_derivation_key(country: str) -> str:
    """``'<country>::food_acquired::nsu-national-unit-weight'``."""
    if country not in NSU_COUNTRIES:
        raise ValueError(f"{country!r} ships no EHCVM unit-weight table: {NSU_COUNTRIES}")
    return f"{country}::food_acquired::{NSU_DERIVATION_NAME}"


def nsu_unit_weights(table: pd.DataFrame, *, product: str, unit: str, size: str,
                     weight: str, year: str | None = None) -> pd.Series:
    """Kilograms per unit, indexed ``(product, unit, size)`` by numeric code.

    ``table`` is the file read with ``convert_categoricals=False``.  A key's
    weight is the MEDIAN of its positive rows across the table, in grams over
    1000: one number per key, from however many strata the table has.  A
    duplicated (key, stratum) cell (34 in Burkina Faso, 10 in Mali, and the two
    rows of each disagree) simply contributes both rows.

    ``year`` names a column for a table that holds several rounds' weighings
    (Niger's ``annee``): a key's weight then comes from its latest year only.
    """
    if year is not None:
        tab = table[[product, unit, size, weight, year]].copy()
    else:
        tab = table[[product, unit, size, weight]].copy()
    w = pd.to_numeric(tab[weight], errors="coerce")
    keep = (w > 0) & tab[[product, unit, size]].notna().all(axis=1)
    tab = tab[keep].assign(**{weight: w[keep].astype(float)})
    for col in (product, unit, size):
        tab[col] = tab[col].astype(float).astype("int64")
    keys = [product, unit, size]
    if year is not None:
        latest = tab.groupby(keys)[year].transform("max")
        tab = tab[tab[year] == latest]
    out = tab.groupby(keys)[weight].median() / 1000.0
    out.index.names = ["product", "unit", "size"]
    return out.rename("kg_per_unit")


def derive_nsu_kg_factor(product, unit, size, served_unit, weights: pd.Series) -> np.ndarray:
    """Kilograms per unit for each row, from the survey's own table; NaN otherwise.

    ``product``, ``unit`` and ``size`` are the row's numeric ``s07bq01``,
    ``s07bq03b`` and ``s07bq03c``; ``served_unit`` the bare ``u`` label the
    Country API serves for its unit; ``weights`` is :func:`nsu_unit_weights`.

    NaN -- and so the inferred ladder -- where the table has no row for the key
    or a code is missing, and where the served unit is an exact metric label
    (``Kg``, ``Litre``), whose kilograms the label states.  The served
    ``Quantity_kg`` is this factor times the row's own quantity
    (``food_acquired_to_canonical``'s ``kg_factor``), for each acquisition
    source separately, so it sums like the quantity it was built from.

    No options, by design (``SkunkWorks/derived_values.org``).  A user who
    wants a stratum's cell or a different column calls
    ``Country(c).derivation_inputs(key)`` and re-derives.
    """
    codes = [pd.to_numeric(pd.Series(np.ravel(x)), errors="coerce").fillna(-1)
             .astype("int64").to_numpy() for x in (product, unit, size)]
    found = weights.reindex(pd.MultiIndex.from_arrays(codes)).to_numpy(dtype=float)
    labels = pd.Series(np.ravel(served_unit), dtype=object)
    metric = labels.map(lambda s: isinstance(s, str)
                        and s.strip().lower() in _EXACT_METRIC_LABELS).to_numpy(dtype=bool)
    return np.where(metric, np.nan, found)


def _label_text(value):
    """A label with its mojibake and its code prefix repaired, or ``None``."""
    if not isinstance(value, str):
        return None
    text = value.replace("&nbsp;", " ").replace("\u00c5__", "oe").strip()
    try:
        text = text.encode("latin-1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        pass
    for mark in ("\u0153", "\u0152", "\x9c", "\x8c"):
        text = text.replace(mark, "oe")
    return _SIZE_CODE_PREFIX.sub("", text).strip() or None


def _label_key(value):
    """:func:`_label_text` without accents, case or punctuation, or ``None``."""
    text = _label_text(value)
    if text is None:
        return None
    text = "".join(ch for ch in unicodedata.normalize("NFKD", text)
                   if not unicodedata.combining(ch))
    return re.sub(r"[^a-z0-9]+", " ", text.lower()).strip() or None


def _label_stem(value):
    """The key of a label's text before its first ``(``, ``,`` or ``/``."""
    text = _label_text(value)
    return None if text is None else _label_key(re.split(r"[(,/]", text, maxsplit=1)[0])


def _label_parts(value) -> list:
    """The keys of the goods a label lists: its text outside parentheses, split on
    ``,`` and ``/``.  Empty for a label that lists one good."""
    text = _label_text(value)
    if text is None:
        return []
    bundled = _NSU_BUNDLES.get(_label_key(text))
    if bundled:
        return list(bundled)
    parts = re.split(r"[,/]", re.sub(r"\(.*?(\)|$)", " ", text))
    keys = [_label_key(part) for part in parts]
    keys = [key for key in keys if key]
    return keys if len(keys) > 1 else []


def nsu_product_crosswalk(wave_labels: dict, table_labels: dict, harmonize=None) -> pd.DataFrame:
    """Which table products each of a wave's product codes reads, joined by LABEL.

    ``wave_labels`` is ``{code: label}`` for the wave's ``s07bq01``; ``table_labels``
    ``{table code: labels}``, every spelling known for that code; ``harmonize``
    ``{label key: preferred label}``, the country's ``harmonize_food``.  Returns one row
    per (``wave_code``, ``table_code``) pair with the ``tier`` it was found on
    (:data:`NSU_JOIN_TIERS`).  A code's first tier with any match wins, and several
    matching table codes are all kept: 2018-19's ``Sucre (poudre ou morceaux)`` reads both
    of 2021-22's sugars.

    The codes themselves are never compared.  That is the point: 118 is ``Sel`` in Niger's
    2018-19 file and ``Pate d'arachide`` in the table.
    """
    harmonize = harmonize or {}

    def preferred(label):
        return harmonize.get(_label_key(label), label)

    def by_label(label):
        return [_label_key(label)]

    def by_harmonized(label):
        return [_label_key(preferred(label))]

    def by_stem(label):
        return [_label_stem(label), _label_stem(preferred(label))]

    rows = []
    for tier, keys_of in (("label", by_label), ("harmonized", by_harmonized),
                          ("parts", by_label), ("stem", by_stem)):
        index: dict = {}
        for code, labels in table_labels.items():
            for label in labels:
                # A part names a good as the table does, or its stem: 2021-22's own
                # label for aubergine is the truncated ``Aubergine,``.
                for key in keys_of(label) + ([_label_stem(label)] if tier == "parts" else []):
                    if key:
                        index.setdefault(key, set()).add(code)
        done = {r[0] for r in rows}
        for code, label in wave_labels.items():
            if code in done:
                continue
            if tier == "parts":
                hits = set().union(*[index.get(key, set()) for key in _label_parts(label)] or [set()])
                rows.extend((code, hit, tier) for hit in sorted(hits))
                continue
            for key in keys_of(label):
                hits = index.get(key or "", ())
                if hits:
                    rows.extend((code, hit, tier) for hit in sorted(hits))
                    break
    return pd.DataFrame(rows, columns=["wave_code", "table_code", "tier"])


def _nsu_walk(country: str, wave: str, tab: pd.DataFrame) -> pd.DataFrame:
    """:func:`nsu_product_crosswalk` for one older wave, from the files' own labels.

    ``tab`` is the country's table as read.  ``tests/test_ehcvm_unit_weights.py`` pins
    what this returns outside the ``label`` tier: those rows depend on the country's
    ``harmonize_food``, so a later fold there moves kilograms here.
    """
    from .country import Country, _categorical_key_column
    from .local_tools import get_categorical_mapping
    from .paths import countries_root

    spec = NSU_TABLES[country]
    root = countries_root() / country
    table_path = root / NSU_TABLE_WAVE / "Data" / spec["table"]
    wave_labels = get_categorical_mapping(str(root / wave / "Data" / spec["sources"][wave]))["s07bq01"]
    wave_labels = {int(c): lab for c, lab in wave_labels.items()}
    own = get_categorical_mapping(str(root / NSU_TABLE_WAVE / "Data" / spec["sources"][NSU_TABLE_WAVE]))["s07bq01"]
    table_labels: dict = {}
    for code in tab[spec["product"]].dropna().astype(float).astype("int64").unique():
        table_labels[int(code)] = [own.get(int(code))]
    if spec["names"] is not None:
        for code, name in zip(tab[spec["product"]].astype(float).astype("int64"), tab[spec["names"]]):
            table_labels[int(code)].append(name)
    else:
        named = get_categorical_mapping(str(table_path)).get(spec["product"], {})
        for code, name in named.items():
            if int(code) in table_labels:
                table_labels[int(code)].append(name)
    food = Country(country).categorical_mapping.get("harmonize_food")
    harmonize = {}
    if food is not None:
        key = _categorical_key_column(food)
        harmonize = {_label_key(a): b for a, b in zip(food[key], food["Preferred Label"])
                     if _label_key(a) and isinstance(b, str) and b.strip()}
    return nsu_product_crosswalk(wave_labels, table_labels, harmonize)


def _nsu_read(country: str, wave: str):
    """The source with its codes, and the weights keyed on THIS wave's product codes.

    Returns ``(raw, weights, years, tiers)``.  ``weights`` is :func:`nsu_unit_weights`,
    with the product level translated by :func:`nsu_product_crosswalk` where the wave is
    not the table's own; a code reading several table products takes the median of their
    weights.  ``years`` is the same shape, the year of the latest weighing behind it, or
    ``None``.  ``tiers`` is ``{wave code: tier}``, empty on the table's own wave.
    """
    from .local_tools import get_dataframe
    from .paths import countries_root

    spec = NSU_TABLES[country]
    root = countries_root() / country
    raw = get_dataframe(root / wave / "Data" / spec["sources"][wave], convert_categoricals=False)
    table_path = root / NSU_TABLE_WAVE / "Data" / spec["table"]
    tab = get_dataframe(table_path, convert_categoricals=False)
    keyed = dict(product=spec["product"], unit=spec["unit"], size=spec["size"],
                 weight=spec["weight"], year=spec["year"])
    weights = nsu_unit_weights(tab, **keyed)
    years = None
    if spec["year"] is not None:
        good = tab[pd.to_numeric(tab[spec["weight"]], errors="coerce") > 0].dropna(
            subset=[spec["product"], spec["unit"], spec["size"]])
        years = good.assign(**{c: good[c].astype(float).astype("int64")
                               for c in (spec["product"], spec["unit"], spec["size"])}
                            ).groupby([spec["product"], spec["unit"], spec["size"]])[spec["year"]].max()
        years.index.names = weights.index.names
    if wave == NSU_TABLE_WAVE:
        return raw, weights, years, {}

    walk = _nsu_walk(country, wave, tab)

    def translate(series, how):
        frame = series.rename("value").reset_index().merge(
            walk, left_on="product", right_on="table_code")
        out = getattr(frame.groupby(["wave_code", "unit", "size"])["value"], how)()
        out.index.names = ["product", "unit", "size"]
        return out

    return (raw, translate(weights, "median").rename("kg_per_unit"),
            None if years is None else translate(years, "max"),
            dict(zip(walk["wave_code"], walk["tier"])))


def _served_labels(country: str, df_u, decode=None) -> pd.Series:
    """The bare ``u`` the Country API serves for each row's raw label."""
    u = pd.Series(np.asarray(df_u, dtype=object))
    if decode is not None:
        u = u.map(lambda v: decode(v) if isinstance(v, str) else v)
    uniq = pd.unique(u.dropna().to_numpy())
    return u.map(dict(zip(uniq, served_unit_labels(country, uniq))))


def _nsu_factor(df: pd.DataFrame, country: str, wave: str, decode=None) -> np.ndarray:
    """:func:`derive_nsu_kg_factor` for each row of the wide frame ``df``.

    The wide frame carries value LABELS and they are not what the table is keyed on, so
    the unit and size come as CODES, and on the table's own wave the product too, read
    from the source itself.  That read is matched to ``df`` by position, which holds only
    if ``df`` is still the file row for row, so it is checked on two of the file's
    columns and refused otherwise.
    """
    raw, weights, _, _ = _nsu_read(country, wave)
    source = NSU_TABLES[country]["sources"][wave]
    for mine, theirs in (("Quantity", "s07bq03a"), ("LastPurchaseValue", "s07bq08")):
        a = pd.to_numeric(df[mine], errors="coerce").to_numpy(dtype=float)
        b = pd.to_numeric(raw[theirs], errors="coerce").to_numpy(dtype=float)
        if not np.array_equal(a, b, equal_nan=True):
            raise UnitWeightJoinError(
                f"{country} {wave}: the wide food_acquired frame is not {source} "
                f"row for row ({mine} != {theirs}), so its codes cannot be read "
                f"by position")
    labels = _served_labels(country, df.index.get_level_values("u"), decode)
    factor = derive_nsu_kg_factor(raw["s07bq01"], raw["s07bq03b"], raw["s07bq03c"],
                                  labels, weights)
    _report_overruled_labels(country, wave, labels, factor)
    return factor


def food_acquired_with_unit_weights(df: pd.DataFrame, key: str, *, wave: str,
                                    drop_columns=("visit",), decode=None) -> pd.DataFrame:
    """:func:`food_acquired_ehcvm` with a per-row ``Quantity_kg`` from the table.

    ``key`` is the wave's #876 key.  Rows that get kilograms also carry
    :func:`nsu_derivation_key` in ``Derivation``, joined to any key already
    there (``+``), on all three acquisition sources.  Everything else about the
    frame is what :func:`food_acquired_ehcvm` returns.

    A row whose canonical key is duplicated here keeps ``Quantity_kg`` NA and no
    new key: see the module docstring.  It needs no reducer, and it means no
    served kilogram is touched by the core collapse that follows.
    """
    from .derivations import union_keys

    country = _country_of(key)
    factor = _nsu_factor(df, country, wave, decode=decode)
    out = food_acquired_ehcvm(df.assign(kg_factor=factor), key,
                              drop_columns=drop_columns, decode=decode)
    kg = pd.to_numeric(out["Quantity_kg"], errors="coerce").to_numpy(dtype=float)
    kg = np.where(out.index.duplicated(keep=False), np.nan, kg)
    out["Quantity_kg"] = kg

    weight_key = nsu_derivation_key(country)
    current = out[DERIVATION_COLUMN].astype(object)
    joined = {v: union_keys([v, weight_key]) for v in current.dropna().unique()}
    stamped = current.map(joined).where(current.notna(), weight_key)
    out[DERIVATION_COLUMN] = pd.Series(
        np.where(np.isfinite(kg), stamped.to_numpy(dtype=object),
                 current.to_numpy(dtype=object)),
        index=out.index, dtype="string")
    return out


def _extracted_units(country: str, wave: str, quantity) -> np.ndarray:
    """``u`` for each source row, as the wave's own extraction hands it to the hook.

    The wave's declared ``u`` (its ``mappings:`` included), read by the same grabber
    as the table, with the served index rewrites (Niger's mojibake) applied.  Not
    :func:`inputs_last_purchase`, which every EHCVM country reads and which holds the
    unit as a raw answer.  ``quantity`` is the source's ``s07bq03a``, and a read that
    is not the file row for row is refused, as the hook's is.
    """
    from .country import Country
    from .local_tools import df_data_grabber

    wave_obj = Country(country)[wave]
    spec = dict(wave_obj.resources["food_acquired"])
    spec["idxvars"] = {k: v for k, v in spec["idxvars"].items() if k == "u"}
    spec["myvars"] = {"s07bq03a": "s07bq03a"}
    mapping = wave_obj.column_mapping("food_acquired", spec)
    mapping.pop("df_edit", None)
    (filename, cols), = mapping.items()
    frame = df_data_grabber(str(wave_obj.file_path / "Data" / filename),
                            cols["idxvars"], **cols["myvars"])
    read = pd.to_numeric(frame["s07bq03a"], errors="coerce").to_numpy(dtype=float)
    if not np.array_equal(read, quantity, equal_nan=True):
        raise UnitWeightJoinError(
            f"{country} {wave}: the extraction of u is not {filename} row for row")
    return np.asarray(_normalise_index_as_served(frame, wave_obj).index.get_level_values("u"),
                      dtype=object)


def inputs_unit_weight(country: str, wave: str) -> pd.DataFrame:
    """The raw answers and table weight behind one wave's ``Quantity_kg``.

    Indexed ``(t, i, j)`` as the served table is (:func:`inputs_last_purchase`
    supplies the index and the labelled answers), every source row at the INPUT
    grain, with the ORIGINAL section-7B variable names (``s07bq01`` as its numeric
    code, which the labelled ``j`` has already harmonized away), the numeric
    ``unit_code`` and ``size_code`` the join reads, ``nsu_kg_per_unit``: the
    national weight, NaN where the table has none or the served unit is ``Kg`` /
    ``Litre``, ``stated_kg``: the mass the served unit's label states, NaN where
    it states none, and ``nsu_join``: how the product found its weight
    (:data:`NSU_JOIN_TIERS`, NA where it did not).  Where the table holds several
    years (Niger), ``nsu_year`` is the latest behind that weight.  The table's own
    stratum rows are in the file the registry entry names; a user who wants a
    stratum's weight reads it from there.

    ``nsu_kg_per_unit`` is what :func:`food_acquired_with_unit_weights` read for the
    row, so the served unit comes from the wave's own extraction
    (:func:`_extracted_units`), as the hook's does: a raw label is padded in Mali's
    2018-19 and a bare number in Burkina's, and neither says the unit is ``Kg``.  The
    one row it does not describe is a source row whose canonical key repeats (two
    product codes folded into one ``j``): that row reports its weight here and the
    hook declines to serve it, for the reason in the module docstring.
    """
    out = inputs_last_purchase(country, wave)
    out = out[[c for c in NSU_RAW_VARIABLES if c != "s07bq01"]].copy()
    raw, weights, years, tiers = _nsu_read(country, wave)
    source = NSU_TABLES[country]["sources"][wave]
    if len(raw) != len(out):
        raise UnitWeightJoinError(f"{country} {wave}: {len(out)} input rows, {len(raw)} in {source}")
    raw_q = pd.to_numeric(raw["s07bq03a"], errors="coerce").to_numpy(dtype=float)
    if not np.array_equal(pd.to_numeric(out["s07bq03a"], errors="coerce").to_numpy(dtype=float),
                          raw_q, equal_nan=True):
        raise UnitWeightJoinError(f"{country} {wave}: the input frame is not {source} row for row")
    labels = _served_labels(country, _extracted_units(country, wave, raw_q))
    out.insert(0, "s07bq01", raw["s07bq01"].to_numpy())
    out["unit_code"] = raw["s07bq03b"].to_numpy()
    out["size_code"] = raw["s07bq03c"].to_numpy()
    out["nsu_kg_per_unit"] = derive_nsu_kg_factor(
        raw["s07bq01"], raw["s07bq03b"], raw["s07bq03c"], labels.to_numpy(), weights)
    out["stated_kg"] = labels.map(_stated_kg).to_numpy(dtype=float)
    found = out["nsu_kg_per_unit"].notna().to_numpy()
    product = pd.to_numeric(raw["s07bq01"], errors="coerce").fillna(-1).astype("int64")
    join = "code" if wave == NSU_TABLE_WAVE else None
    tier = product.map(tiers).to_numpy(dtype=object) if tiers else np.full(len(out), join, dtype=object)
    out["nsu_join"] = pd.Series(np.where(found, tier, None), index=out.index, dtype="string")
    if years is not None:
        codes = pd.MultiIndex.from_arrays(
            [pd.to_numeric(raw[c], errors="coerce").fillna(-1).astype("int64")
             for c in ("s07bq01", "s07bq03b", "s07bq03c")])
        out["nsu_year"] = np.where(found, years.reindex(codes).to_numpy(dtype=float), np.nan)
    return out
