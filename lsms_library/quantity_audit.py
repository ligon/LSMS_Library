"""SITE Q -- a value that is PRESENT, NON-NULL and IMPOSSIBLE.

The library's content guards, in the order they were built, each check one
thing and stop:

* ``Country._assert_built_required_columns`` / the ``dfs:`` sub-df check --
  is the declared column **present**?
* ``country._audit_index_collapse`` (GH #323) -- is the declared index
  **unique**?
* ``null_read_audit`` Sites R and B (GH #699) -- does the column hold
  **anything at all**?

None of them asks whether what it holds is **possible**.  Tanzania's
``crop_production`` 2020-21 reports a single household harvesting
**7,500,000 kg of coconuts** from one plot -- 7,500 tonnes, on a smallholding.
The value is present, non-null, correctly typed, on a unique index, in a table
that grades ``sane``, and nothing anywhere says a word about it (GH #857).

WHAT THIS DOES NOT DO -- and it is the first thing to say.  It **counts and
NAMES**.  It never clips, never rescales, never NaNs, never drops.  That is
the same contract ``transformations._screen_reported_factors`` states for the
reported ``KgFactor`` ("A rejected value is NEVER clipped or rescaled --
inventing a weight is the failure this whole design exists to avoid"), and it
is not a stylistic echo: an unclipped implausible value is a *defect you can
still see*, while a clipped one is a defect that has been given a plausible
disguise and a signature.  :func:`check_quantities` returns its input frame
unchanged, always and by contract.  The EPAR do-files' remedy for this exact
Tanzania wave -- ``replace kg_harvest = . if strmatch(hhid, "0015-001-001") &
plot_id==4`` (``EPAR_UW_Tanzania_NPS_W5.do:578``) -- is doubly out of bounds
here: it deletes, and it deletes from a hand-written list of household ids.

THE RULE, AND WHY EVERY NUMBER IN IT.  A row is reported when

    Quantity > QUANTITY_OUTLIER_K x reference(row)

where ``reference`` is the :data:`QUANTITY_REFERENCE_QUANTILE` (0.90) of the
**strictly positive** values in the row's **comparison cell**, lifted to a
floor of one native unit (:data:`QUANTITY_REFERENCE_FLOOR`).  The cell is the
finest of ``(t, u, j)`` -> ``(t, u)`` -> ``(t)`` that holds at least
:data:`QUANTITY_MIN_CELL` strictly positive quantities -- a zero neither
defines the scale nor counts toward the floor (GH #1259) -- so its quantile is
positive by construction.  The ladder is
``transformations._survey_median_factors``' fine-to-coarse shape, and like it
the ladder **never pools across ``t``**: a thin module does not borrow a thick
one's licence.

*Why the 90th percentile and not the 99th.*  MEASURED, and the binding case is
Tanzania's own second-worst row.  In the cell ``(2020-21, kg, Timber)``, n=37::

    p99 = 2,632,000   <- 66% of the very row under test: eaten by the outlier
    p95 =    70,800   <- dragged up by the SECOND outlier (200,000)
    p90 =    23,088   <- clean; the 4,000,000 row is 173x it

A percentile at rank ``q*(n-1)`` is immune to the top ``(1-q)*(n-1)`` values,
so at the 30-row floor p99 is immune to nothing, p95 to 1.45 values and p90 to
2.9.  Under a p99 reference the 4,000,000 kg Timber row GH #857 names does not
fire at any threshold that leaves the rest of the corpus quiet.  The 37 are the
cell's POSITIVE rows, and the immunity is counted in measurements: once PR
#1263 serves Tanzania's recorded "not harvested" answers as ``0 kg``, the same
cell holds 346 rows, 309 of them zero, and the argument above survives only
because zeros no longer enter the quantile (next paragraph).

*Why only strictly positive values count -- in the quantile AND in the 30-row
floor (GH #1259).*  A recorded zero ("did not harvest") is a legitimate served
value, but it says WHETHER, not HOW MUCH: it carries no information about the
size of a harvest in its cell, so it may not define the scale others are
judged against.  Counted, zeros collapse the reference wherever they dominate
a cell.  Measured on PR #1263's Tanzania build: Timber/kg 2020-21 is 309 zeros
of 346 rows and its p90 falls from 23,088 kg to 17 (2019-20: 104 of 117, to
56), so a 2,000 kg harvest fires at 117x and sixteen ordinary harvests are
named -- 13 Timber, one each of Citrus, Coconuts and Ripe Bananas.  With zeros
excluded the offender set is exactly development's 14.  And whether a zero
reaches a cell at all depends on a unit CONVENTION that differs by country
(Tanzania and Ethiopia key a zero harvest ``kg``, Malawi leaves ``u`` missing,
Uganda writes ``Unknown`` / ``Others specify``, Nigeria and Niger ``Unknown``);
a plausibility screen must not depend on that.

The floor counts positives too, for the reason the floor exists: a thin cell
cannot manufacture a reference, and zeros do not thicken a cell -- 300 zeros
and 5 harvests are 5 measurements of scale, not 305.  The corpus has the case
that separates the two readings exactly once (2026-10-23: no
``crop_production`` row on the cold build, one row across the 2026-10-19
``food_acquired`` and ``plot_features`` builds): Malawi ``food_acquired``
2016-17, ``Maize Ufa Mgaiwa`` with no unit, is 45 zeros and a single 3,200.
Counting zeros toward the floor but not the quantile makes that row its own
reference (ratio 1, silent); counting measurements drops it to the wave rung,
reference 10, where it fires at 320x.  A zero row is still compared against
its cell's reference -- it can never exceed a positive one -- but it is never
part of the scale.  ``_cell_reference`` masks it to ``NaN`` before
:func:`_group_stats`, which stays a faithful finite-value ``Series.quantile``.

Before #1259 the quantile itself had to be strictly positive (a cell whose
90th percentile is zero carries no scale; cost of omitting that, measured: six
Malawi rows of 2-30 units fired at ratio ``inf``).  A quantile of positive
values is positive by construction, so that condition is now implied, and is
kept in the code only as a guard against the mask being loosened.

*Why the reference is still floored at one unit -- and what #1259 did to the
case that motivated it.*  The red team found Uganda 2010-11 pooling every crop
sharing ``u='Others specify'`` into a ``(t, u)`` cell whose p90 was **0.6**,
and eight ordinary harvests named at 66x to 500x it; the floor removed four of
them.  That p90 was itself a zero artefact: the cell is 697 zeros of 775 rows,
its positive p90 is 92.7, and under #1259 the remaining four (Rice 300, Ground
Nuts 150, Sugarcane 150, Pumpkins 105) stop firing too.  On
``crop_production`` the floor now changes no verdict at any K (the sensitivity
sweep, re-run 2026-10-23); on the 2026-10-19 builds it still binds on
``food_acquired`` (2 rows) and ``plot_features.Area`` (8 rows), where a
cell's positive p90 can sit below one native unit, so it stays.

*Why K = 100 -- and the honest part: THERE IS NO GAP.*  ``null_read_audit``
could set its trigger "in the gap" because one existed (corpus max 28.3% of a
frame's columns all-null, every known-bad parse >= 40%).  Here there is none.
Re-measured under #1259 on a cold build of development (``9a0ed5f3c``,
2026-10-23, all sixteen ``crop_production`` countries): over the 604,269 rows
with a reference -- every row with a finite quantity; the other 68,192,
GhanaLSS's 37,280 among them, have none and are never judged -- the ratio
``Quantity / reference`` runs::

    p90 = 1.0    p99 = 3.6    p99.9 = 20.0    p99.99 = 188.5   max = 11,029

-- smooth, with no discontinuity near the top.  (The 2026-09-09 figures, on
fifteen warm countries and the pre-#1259 rule: p99.9 = 21.0, p99.99 = 233.7,
max = 14,286 -- the max was Niger's ``999999`` sentinel, since stripped.)  So
100 is a **stated tolerance, not a discovered boundary**, and it is stated as:
two orders of magnitude above what nine in ten comparable rows report; five
times the corpus's own 99.9th percentile of that ratio; and below 173.3, where
the 4,000,000 kg Timber row sits, so the rows the issue names actually fire.
The sensitivity is smooth and is recorded so the choice stays re-checkable --
K=30 -> 380 rows, K=50 -> 228, K=100 -> 120, K=150 -> 75, K=300 -> 37 (the
pre-#1259 rule on the same build: 401, 240, 129, 81, 39; 2026-09-09: 417,
261, 151, 90, 49).

WHAT IT FIRES ON TODAY -- **every** country declaring ``crop_production``,
cold-built at development ``9a0ed5f3c`` on 2026-10-23 and read with
``pd.read_parquet`` exactly as the #857 sweep reads it (the shared warm cache
graded ``stale`` against that commit for all sixteen).  (The first version of
this table listed five and was read as the corpus; the measuring script had a
hardcoded country list while its own README said it globbed ``data_root()``.
It globs now.)  "pre-#1259" is the previous rule on the same frames:

=============  =========  =========  ======  ==========
country             rows  pre-#1259   fired  audit cost
=============  =========  =========  ======  ==========
Benin              9,056          2       2        6 ms
Burkina_Faso      15,593          1       1        9 ms
CotedIvoire       22,223         16      16       14 ms
Ethiopia          85,519         10      10       59 ms
EthiopiaRHS       17,523          2       2        4 ms
GhanaLSS          37,280          0       0        0 ms
GhanaSPS          25,109          7       7       15 ms
Guinea-Bissau     12,173          1       1        7 ms
Malawi           135,078         20      19       84 ms
Mali              35,060         18      18       27 ms
Niger             47,123          1       1       25 ms
Nigeria           62,844          9       5       44 ms
Senegal            8,428          1       1        6 ms
Tanzania          14,126         14      14       11 ms
Togo              11,565          1       1        8 ms
Uganda           133,761         26      22       96 ms
**total**    **672,461**    **129** **120**
=============  =========  =========  ======  ==========

GhanaLSS carries no finite ``Quantity`` at all (a Site B matter, not this
one's).  #1259 removes nine rows and adds none.  All nine sat in cells their
zeros had collapsed: Uganda 2010-11's four ``Others specify`` rows (above);
Nigeria 2011Q1's four ``Maize``/``Unknown`` rows (1,800, 500, 200, 200), judged
against a reference of 1.2 from a cell of 60 zeros and 7 harvests, which now
falls to the wave's ``Unknown`` cell (reference 890); and Malawi 2016-17
Pigeon Pea 500 in ``Other (Specify)``, whose 5 zeros put it at 106.4x and
which now sits at exactly 100x.  The ladder moves 3,394 rows (0.5%) and every
move is toward a cell that has a scale: 943 from ``(t,u,j)`` to ``(t,u)``
(their crop cell now holds fewer than 30 harvests) and 2,451 Uganda rows from
``(t)`` to ``(t,u)`` (their unit cell was more than nine-tenths zeros, so its
p90 was 0; its harvests now give it one).  No finite row loses its
reference.

120 of 672,461 rows (0.018%), appearing only on the country being read, is a
work queue rather than a firehose -- the same reading ``null_read_audit`` gives
its own 86 Site-B findings.  Two rows larger than GH #857's headline case turn
up in countries nobody had looked at: **Benin 2018-19 Coton 12,500,000 kg**
(``i=220065``, plot 1_2, x1,785.7) and **CotedIvoire 2018-19 Cocoa 1,500,000**
(``i=600009``, x750).

THE SECOND TABLE -- ``food_acquired.Quantity`` (GH #884), added once the #874
Ethiopia per-source read un-suppressed 1000x data-entry unit slips that only a
warm-time screen can name (2011-12 Onion ``Q2 = 1,400 Gram`` beside
``Q3 = 1,400,000 Tuba``).  Same rule, same constants, measured the same way on
every country holding a warm L2-country ``food_acquired`` parquet.  Re-measured
under #1259 (2026-10-23) on the shared warm cache of 2026-10-19 -- which grades
``stale`` against development ``9a0ed5f3c``, so these describe those builds,
not a fresh one; "pre-#1259" is the previous rule on the same frames, and the
#884 census of 2,005 rows predates both:

=============  ===========  =========  ======  ==========
country               rows  pre-#1259   fired  audit cost
=============  ===========  =========  ======  ==========
Benin              187,930          0       0      129 ms
Burkina_Faso       765,816          7       7      321 ms
Cambodia            38,201          0       0       14 ms
CotedIvoire        296,945          0       0      160 ms
Ethiopia           326,456        132     126      173 ms
EthiopiaRHS         54,905          0       0       23 ms
GhanaLSS         5,340,606        294     294    2,941 ms
GhanaSPS           407,929         18      18      265 ms
Guatemala          215,982         35      35       86 ms
Guinea-Bissau      126,925          0       0       75 ms
Malawi             971,531        216     216      573 ms
Mali               431,046      1,105   1,105      216 ms
Niger              239,877          0       0      102 ms
Nigeria            595,725         82      82      298 ms
Panama             570,942         32      32      285 ms
Senegal            436,576          0       0      234 ms
Serbia             200,046          0       0       81 ms
Tanzania           251,730         70      70       82 ms
Togo               112,445          1       1       49 ms
Uganda             355,571         12      12      186 ms
**total**     **11,927,184**  **2,004** **1,998**
=============  ===========  =========  ======  ==========

1,998 of 11.93M rows (0.017%).  Zeros are rare here (3,911 of 11.24M finite
quantities, against 24,203 of 604,269 in ``crop_production``), and #1259 moves
one cell (below).  The headline is dominated by two known artifact families
itemised below -- Mali's single repeated-sentinel family (1,105) and
sentinel-unit rows (78) -- so the DISPERSED residual is ~815 findings; read the
rate as a work-queue size, not a calibration.  Three features of the census a
reader should know before treating any of these counts as calibration:

* **Ethiopia's 132 (65/25/32/8/2 by wave)** was the reviewer's own figure,
  reproduced exactly -- the cell the issue was written around.  Under #1259 it
  is **126 (59/25/32/8/2)**, and the six that leave are a real cost, stated
  rather than hidden: 2011-12 ``u='Unknown'`` rows of 500,000 to 8,120,000
  (Chick Pea, Chat/Kat x2, Barley, Maize, Sorghum).  Their ``(t, u)`` cell is
  220 zeros and 32 positive values, and SIX of those 32 are these slips --
  more than the 3.1 values a p90 over 32 is immune to -- so the positive p90 is
  698,000 and they fall to x0.7-x11.6.  Before #1259 the 220 zeros dragged the
  p90 to 1,000 and the slips fired at x500-x8,120: caught, but by the same
  collapse that names a 2,000 kg Timber harvest, not by a scale.  It is the
  repeated-sentinel blind spot described below, no longer masked.
* **Mali's 1,105 is 1,104 rows of a single wave** (2014-15), and the wave is a
  REPEATED SENTINEL, not 1,104 outliers: 1,149 rows of that wave are keyed at
  or above 9,000 -- ``9999``, ``9999.9``, ``10000``, ``10001``, ... -- an
  unstripped missing-value code of exactly the kind this docstring's sentinel
  section describes for Uganda's ``99999`` and Niger's ``999999``.  At ~0.9%
  of the wave the sentinel cannot saturate its cells, so the screen sees it;
  that is the stated accident of keying frequency, and stripping it is a
  separate job (the Burkina Faso ``u='Unknown'`` passage-2..4 fix of #842 is
  the pattern).
* **Sentinel-unit rows are judged, deliberately.**  ``u='Unknown'`` /
  ``u='Value'`` rows contributed 96 of the #884 census's 2,005 (18 Unknown --
  twelve of them Burkina Faso 2014 rows escaping a zero-scale ``(t,u,j)`` cell
  onto the pooled ``t`` rung; 77 Value, all GhanaLSS, mostly 1998-99/2005-06
  pre-redenomination currency slips like Sugar 1,350,000,000 at x270,000; one
  Panama ``7777.7`` sentinel).  Re-measured under #1259 they are 78 of 1,998:
  the same 77 GhanaLSS Value rows and one Uganda Unknown.  The Burkina Faso
  twelve and the Panama sentinel no longer fire on the 2026-10-19 builds (a
  data change, not #1259); Ethiopia's six Unknown rows are #1259's (above).
  A sentinel unit has no scale, so a ratio to a
  p90 is meaningless as a MAGNITUDE -- but the value in the ``Quantity``
  column of such a row is not a quantity at all, and the ratio is a
  serviceable detector of a value that was keyed with the wrong number of
  zeros.  Excluding them would silence exactly the GhanaLSS 2005-06 currency
  slips.  They stay in; the recommendation for anyone *acting* on the list is
  to read a sentinel-unit firing as "wrong zeros or unstripped sentinel", not
  "harvest too large".

COST on this table, same spy method (#884, before #1259): 151 ms inside the
Ethiopia warm read (4% of 3.9 s), 3,013 ms inside the 85 s GhanaLSS read of
the corpus's largest frame (3.5%).  Not re-measured inside a read for #1259;
timed in isolation, base and branch interleaved in one process, the mask costs
nothing measurable: Ethiopia 187 vs 184 ms, GhanaLSS 2,886 vs 2,894 ms.

COST, measured INSIDE the read rather than by differencing two timed reads on
a loaded machine (a spy on the hook, best of five, warm cache): Uganda 83 ms of
a 2,157 ms warm ``crop_production`` read (4%), Malawi 65 ms of 1,391 ms (5%),
Tanzania 9 ms of 643 ms (1%).  The first implementation cost 196 ms on Uganda
and was rewritten (:func:`_group_stats`, :func:`_axis`) to 65-83 ms after the
red team measured the read-path delta.  Re-measured at #1259 (2026-10-23, same
spy, warm reads of a development build): Uganda 72-77 ms of a 1,033-1,046 ms
read (7%), Tanzania 9-13 ms of 123-164 ms (8%) -- the same under either rule,
alternating base and branch runs; the read got twice as fast, the audit did
not.  The ``np.where`` that implements #1259 runs once per call, not per rung.

DELIBERATELY NO ALLOWLIST, for the reason ``country._grain_strict`` gives: an
allowlist is the same disease with a registry.  A cell that is loud and
legitimate stays loud until the data or the config can say why.

WHAT A SENTINEL DOES TO A DISTRIBUTIONAL SCREEN -- the useful case, because
the corpus supplies it twice with opposite outcomes.

Uganda used to carry 3,097 rows whose ``Quantity`` was exactly ``99999``, the
2009-10 missing-value sentinel.  Before GH #861 stripped them at the source,
**seven** of them fired here, across **two** cells (2009-10 Millet /
``u='Unknown'``, p90 2.8, and Sorghum / ``Plastic Basin (15 lts)``, p90 12) --
the two cells the sentinel did NOT saturate.  In every other cell it appeared
in, 99999 *was* the 90th percentile, so the rows were not distributional
outliers by any honest reading and the screen said nothing.  (An earlier draft
of this file said "nine, in one cell"; two of those nine were genuine
Sugarcane rows.  Corrected by the red team, 2026-09-09.)  Since #861 landed,
Uganda fired 25 rather than 36 and none of them is a sentinel (22 under
#1259 on the 2026-10-23 cold build; still none a sentinel).

Niger was the same signature, unfixed when this screen landed: **eleven**
rows of exactly ``999999`` in 2011-12, ``u='Unknown'``, Mil / Sorgho / Niebe /
Riz Paddy, which DID fire (x14,285.7 on the ``t`` rung) because eleven rows
cannot saturate a wave.  They were 11 of Niger's 28 firings and the corpus's
largest ratio.  They have since been stripped at source (none in the
2026-10-23 cold build, where Niger fires once).  A REPEATED SENTINEL IS NOT A
LONE EXTREME: whether this screen sees one is an accident of how often it was
keyed, which is exactly why stripping it is a separate job with a separate
issue.  The same accident governs CONTAMINATION in a thin cell: a p90 over
``n`` measurements is immune to only ``0.1*(n-1)`` bad ones, and Ethiopia's
2011-12 ``Unknown`` food cell (six slips in 32) is past it.

Separately, Malawi's ``50 kg Bag`` rows reporting 2015 / 2016 / 2018 / 2019
bags are the calendar-year-keyed-into-a-numeric-field signature that
``transformations._KG_FACTOR_YEAR_BAND`` documents for reported factors; four
were caught here by the ratio alone when this screen landed (six on the
2026-10-23 cold build), and no second rule is added for them -- that would be
scope creep, and it is recorded in the ledger instead.

CACHE.  Like ``null_read_audit`` this is pure reporting -- it provably returns
its input unchanged -- so its entry points are listed in
``_build_registry._EXCLUDED_CALLABLES`` and its ledger in
``_EXCLUDED_CONSTANTS``.  Measured both ways: with the exclusions, adding this
module and its call site moved **0 of 12** probed ``Country._table_cache_hash``
values and **0 of 5** ``build_transforms_fingerprint`` values.  The #1259 rule
change, measured the same two ways: a full census, development against the
branch, moved **0 of 549** L2-country hashes, **0 of 40** build fingerprints
and **0 of 2,109** L2-wave input hashes; a stash probe of the #857 pairs moved
**0 of 17**.
"""

from __future__ import annotations

import os
import warnings
from typing import Any

import numpy as np
import pandas as pd

__all__ = [
    "QuantityImplausibleError",
    "QuantityImplausibleWarning",
    "QUANTITY_OUTLIER_K",
    "QUANTITY_MIN_CELL",
    "QUANTITY_REFERENCE_QUANTILE",
    "QUANTITY_REFERENCE_FLOOR",
    "audit_quantities",
    "check_quantities",
    "quantity_reports",
]


_QUANTITY_STRICT_ENV = "LSMS_QUANTITY_STRICT"

#: How many times its comparison cell's :data:`QUANTITY_REFERENCE_QUANTILE` a
#: value must exceed before it is reported.  See the module docstring for the
#: measurement -- and for the statement that the ratio distribution has no gap,
#: so this is a stated tolerance rather than a discovered boundary.
QUANTITY_OUTLIER_K = 100.0

#: Smallest comparison cell that may supply a reference, counted in STRICTLY
#: POSITIVE values (GH #1259: zeros do not thicken a cell).  A thinner cell
#: falls to the next rung of the ladder; if no rung qualifies the row is not
#: judged.
QUANTITY_MIN_CELL = 30

#: The cell statistic.  0.90 rather than 0.99/0.95 because at the 30-row floor
#: only the 90th percentile is immune to the two or three contaminating values
#: a defective cell actually carries -- measured on Tanzania's Timber cell.
QUANTITY_REFERENCE_QUANTILE = 0.90

#: Smallest reference the ladder may hand back, in the row's OWN unit.  A cell
#: whose 90th percentile is below one unit is not describing a scale -- it is
#: describing a junk unit label under which nine rows in ten record less than
#: one of whatever it is.  Uganda 2010-11 pooled every crop sharing
#: ``u='Others specify'`` into a cell with p90 = 0.6, and eight ordinary
#: harvests (Rice 300, Ground Nuts 150, ... Sugarcane 66) were named at 66x to
#: 500x it.  Strict positivity did not catch them: 0.6 is positive.  The floor
#: is stated in native units deliberately -- there is no cross-unit conversion
#: here and inventing one would be the very thing ``harvest_kg`` exists to do
#: carefully.  GH #1259 found that 0.6 was a ZERO artefact (697 zeros of 775
#: rows; the positive p90 is 92.7), so on ``crop_production`` the floor no
#: longer changes any verdict; it still binds on ``food_acquired`` and
#: ``plot_features.Area`` (module docstring), and stays.
QUANTITY_REFERENCE_FLOOR = 1.0

#: WHICH columns of WHICH tables are screened.  A registry of *what is looked
#: at*, NOT an allowlist of cells excused from looking -- the distinction the
#: no-allowlist rule turns on, and the same kind of mapping as
#: ``country._ADDITIVE_MEASURE_COLUMNS``.  ``food_acquired.Quantity`` was the
#: stated extension (#884) and is measured in the module docstring's second
#: census: 2,005 of 11.57M rows (0.017%) at #884, 1,998 of 11.93M under #1259;
#: 3,030 ms on a 5.34M-row GhanaLSS read (3.5% of the 85 s warm read).
_SCREENED_COLUMNS: dict[str, tuple[str, ...]] = {
    "crop_production": ("Quantity",),
    "food_acquired": ("Quantity",),
    # GH #879: the canonical schema declares plot_features.Area in
    # HECTARES with AreaUnit a PROVENANCE label, but six countries served
    # the native unit instead, and no framework code reads AreaUnit, so
    # the violation was invisible to every shape/null/duplicate guard.
    # Screening Area here is what catches a recurrence: a country serving
    # e.g. square metres (Timor-Leste) or sotka (Tajikistan) as "Area"
    # lands orders of magnitude above the per-wave hectare cell and is
    # NAMED.  The ladder degenerates to the `(t)` rung because
    # plot_features has no `u`/`j` axis -- that rung is exactly the right
    # comparison (plot size against its own wave).  Warn-only, never
    # drops: measured on the 17 correct countries pre-fix, the rule flags
    # 317 of 397,182 rows (0.08%), overwhelmingly genuinely-large hectare
    # self-reports (Togo 2018: 244 rows of 200-1000 ha labelled
    # 'hectares'), which is the same order of false-positive Site Q
    # already accepts for crop_production.  Re-measured under GH #1259
    # (2026-10-23, the 23 warm parquets of 2026-10-19): 347 of 520,849
    # rows (348 under the previous rule; the one that leaves is a Uganda
    # 2005-06 plot at 100.05x whose wave cell held 22 zero areas).
    "plot_features": ("Area",),
}

#: Alternate spellings of the identifying axes, in preference order.  All of
#: these are live: ``j`` (Tanzania, Uganda, Ethiopia) vs ``crop`` (Malawi,
#: Nigeria); ``plot`` vs ``plot_id``; and ``u`` is an index LEVEL in
#: Uganda/Malawi/Ethiopia but a COLUMN in Tanzania/Nigeria.
_AXES: dict[str, tuple[str, ...]] = {
    "t": ("t",),
    "u": ("u",),
    "j": ("j", "crop", "item"),
    "i": ("i",),
    "plot": ("plot", "plot_id", "parcel"),
}

#: How many offending rows a single warning names before it says "+N more".
_MAX_NAMED = 12

#: Guard on the folded group code.  Only reached by a frame with absurdly many
#: distinct labels; the widest product measured in the corpus is 4,590.
_MAX_COMBINED_WIDTH = 1 << 40


class QuantityImplausibleError(RuntimeError):
    """A screened quantity is present, non-null and impossible.

    Raised instead of the default warning when ``LSMS_QUANTITY_STRICT`` is set.
    """


class QuantityImplausibleWarning(RuntimeWarning):
    """A screened quantity is present, non-null and impossible.

    Its own class -- not a bare ``RuntimeWarning`` -- so callers, tests and CI
    can target it precisely:
    ``warnings.simplefilter("error", QuantityImplausibleWarning)``.
    """


def _quantity_strict() -> bool:
    """Whether an implausible value should RAISE rather than warn.

    Warn by default, for the reason ``country._grain_strict`` and
    ``null_read_audit._read_strict`` both give: a guard that breaks a working
    corpus on the day it lands gets reverted, and a revert is how the bug class
    survives.

    ITS OWN LEVER, deliberately -- not ``LSMS_READ_STRICT``.  ``CLAUDE.md``'s
    rule is that "a destroyed row and an empty column are different concerns
    and must ratchet separately"; a value that is present, non-null and
    impossible is a THIRD concern.  Folding it into ``LSMS_READ_STRICT`` would
    gate that lever's adoption in CI -- already blocked on the wave-vs-country
    granularity of ``optional:`` -- on every Malawi bag-of-maize outlier too.
    Identical ``{1,true,yes}`` spelling, so knowing one is knowing all three.
    """
    return os.environ.get(_QUANTITY_STRICT_ENV, "").lower() in {"1", "true", "yes"}


# ---------------------------------------------------------------------------
# Reading the frame.  Index level or column, under any of its live spellings.
# ---------------------------------------------------------------------------

def _axis(df: pd.DataFrame, axis: str) -> tuple[np.ndarray, np.ndarray] | None:
    """``(codes, categories)`` for *axis*, or ``None`` if the frame has none.

    The alternate-aware twin of ``transformations._level_or_column``: it looks
    for each spelling in :data:`_AXES` as an index level first, then as a
    column.  It hands back INTEGER CODES rather than the labels themselves,
    because both things this module does with an axis -- grouping, and naming
    the handful of rows that fire -- want codes, and materialising 130k object
    labels to do either is pure waste.  A ``MultiIndex`` and a categorical
    column already carry codes; everything else is factorised once.  A code of
    ``-1`` means the label is missing, and missing is ITS OWN group -- the rows
    with no unit label are among the likeliest to be defective, and exempting
    them silently would be the opposite of the point.

    Measured on Uganda (130,606 rows): reading the three grouping axes cost
    21.6 ms as object arrays plus 33 ms to factorise them; off the MultiIndex
    codes both numbers are ~0.
    """
    names = list(df.index.names or [])
    for name in _AXES.get(axis, (axis,)):
        if name in names:
            position = names.index(name)
            if isinstance(df.index, pd.MultiIndex):
                return (np.asarray(df.index.codes[position], dtype=np.int64),
                        np.asarray(df.index.levels[position], dtype=object))
            return _factorized(df.index)
        if name in df.columns:
            column = df[name]
            if isinstance(column.dtype, pd.CategoricalDtype):
                return (np.asarray(column.cat.codes, dtype=np.int64),
                        np.asarray(column.cat.categories, dtype=object))
            return _factorized(column)
    return None


def _factorized(values: Any) -> tuple[np.ndarray, np.ndarray]:
    """``(codes, categories)`` for anything without codes of its own."""
    codes, categories = pd.factorize(np.asarray(values, dtype=object),
                                     use_na_sentinel=False)
    return codes.astype(np.int64), np.asarray(categories, dtype=object)


def _label(axis: tuple[np.ndarray, np.ndarray] | None, position: int
           ) -> str | None:
    """The *position*-th label of *axis*, or ``None`` for missing / absent."""
    if axis is None:
        return None
    code = int(axis[0][position])
    if code < 0:
        return None
    return str(axis[1][code])


def _numeric(series: pd.Series) -> np.ndarray:
    """*series* as a positional ``float64`` array, ``pd.NA`` and junk -> ``nan``.

    Not decoration: ``Quantity`` is frequently a nullable masked dtype, and a
    bare ``>`` between a masked array and a float array raises or yields
    ``pd.NA`` rather than ``False``.
    """
    values = pd.to_numeric(series, errors="coerce")
    return values.to_numpy(dtype="float64", na_value=np.nan)


def _group_stats(values: np.ndarray, codes: np.ndarray,
                 quantile: float) -> tuple[np.ndarray, np.ndarray]:
    """Per-ROW ``(quantile of its group, size of its group)``.

    A hand-rolled group-quantile rather than ``groupby(...).transform``, and
    the reason is measured: this audit runs on the READ path of every
    ``crop_production`` call, and the pandas spelling cost 184-228 ms on Uganda
    (130,606 rows, three rungs), a 1.4x-1.6x slowdown of the whole warm read.
    One ``lexsort`` per rung plus flat indexing does the same arithmetic --
    linear interpolation between order statistics, exactly ``Series.quantile``'s
    default -- and is pinned against pandas by
    ``tests/test_quantity_screen.py::test_group_quantile_matches_pandas``.

    Only FINITE values enter a group's quantile or its count: ``NaN`` is
    missing and an infinity is not a measurement, so neither may define the
    scale other rows are judged against.  Both still get a reference from their
    own group, and are still tested against it.  Which finite values count is
    the CALLER's choice: :func:`_cell_reference` hands zeros in as ``NaN``
    (GH #1259), so this function stays a plain finite-value quantile.
    """
    n = len(values)
    stat = np.full(n, np.nan)
    size = np.zeros(n, dtype=np.int64)
    finite = np.isfinite(values)
    if not finite.any():
        return stat, size

    keep = np.flatnonzero(finite)
    order = keep[np.lexsort((values[keep], codes[keep]))]
    sorted_values = values[order]
    sorted_codes = codes[order]

    first = np.flatnonzero(np.concatenate(
        ([True], sorted_codes[1:] != sorted_codes[:-1])))
    counts = np.diff(np.concatenate((first, [len(order)])))
    position = quantile * (counts - 1)
    low = np.floor(position).astype(np.int64)
    high = np.ceil(position).astype(np.int64)
    frac = position - low
    group_stat = (sorted_values[first + low] * (1.0 - frac)
                  + sorted_values[first + high] * frac)
    group_codes = sorted_codes[first]

    slot = np.clip(np.searchsorted(group_codes, codes), 0,
                   max(len(group_codes) - 1, 0))
    hit = group_codes[slot] == codes
    stat[hit] = group_stat[slot[hit]]
    size[hit] = counts[slot[hit]]
    return stat, size


def _cell_reference(values: np.ndarray,
                    keys: dict[str, tuple[np.ndarray, np.ndarray]]
                    ) -> tuple[np.ndarray, np.ndarray]:
    """``(reference, rung)`` for every row.

    Walks the ladder ``(t, u, j)`` -> ``(t, u)`` -> ``(t)``, taking the first
    rung that offers a cell with at least :data:`QUANTITY_MIN_CELL` finite,
    STRICTLY POSITIVE values, and takes the quantile over those values alone
    (GH #1259: a zero neither defines the scale nor counts toward the floor),
    then lifts it to :data:`QUANTITY_REFERENCE_FLOOR`.  A row no rung can
    serve keeps a ``NaN`` reference and is never judged -- silence, not a
    guess.  A zero row still receives its cell's reference.

    *keys* maps an axis to the ``(codes, categories)`` pair :func:`_axis`
    returns; the rungs are folded out of those codes arithmetically, so no axis
    is grouped or factorised more than once however many rungs use it.
    """
    n = len(values)
    # Shift by one so a missing label (code -1) folds like any other group.
    axis_codes = {a: keys[a][0] + 1 for a in keys}
    axis_width = {a: int(len(keys[a][1])) + 1 for a in keys}

    base = [k for k in ("t",) if k in keys]
    rungs: list[tuple[str, list[str]]] = []
    for cols in (base + [k for k in ("u", "j") if k in keys],
                 base + [k for k in ("u",) if k in keys],
                 base):
        name = ",".join(cols) if cols else "whole-frame"
        if name not in [seen for seen, _ in rungs]:
            rungs.append((name, cols))

    # GH #1259: only a STRICTLY POSITIVE value may define a cell's scale or
    # count toward its size.  A recorded "did not harvest" zero is a legitimate
    # served value, but it is not a measurement of how large a harvest in that
    # cell is: where zeros dominate a cell they drag its p90 toward nothing
    # (Tanzania 2020-21 Timber/kg: 309 zeros of 346 rows took the reference
    # from 23,088 kg to 17) and ordinary harvests fire.  Masking them to NaN
    # here -- rather than inside :func:`_group_stats`, which stays a faithful
    # finite-value ``Series.quantile`` -- excludes them from the quantile AND
    # from the QUANTITY_MIN_CELL count, so zeros can neither collapse a
    # reference nor pad a thin cell over the floor.  A zero row still receives
    # its cell's reference (the match is by group code, not by value) and is
    # still tested against it; it can never exceed a positive reference.
    scale = np.where(values > 0, values, np.nan)

    reference = np.full(n, np.nan)
    rung = np.empty(n, dtype=object)
    for name, cols in rungs:
        todo = np.isnan(reference)
        if not todo.any():
            break
        codes = np.zeros(n, dtype=np.int64)
        width = 1
        for col in cols:
            codes = codes * axis_width[col] + axis_codes[col]
            width *= axis_width[col]
            if width > _MAX_COMBINED_WIDTH:
                # Re-compress rather than risk an int64 overflow on a frame
                # with pathologically many labels.  Never reached in the
                # corpus (widest product measured: Malawi, 4,590).
                codes = np.unique(codes, return_inverse=True)[1].astype(np.int64)
                width = int(codes.max()) + 1 if n else 1
        stat, size = _group_stats(scale, codes, QUANTITY_REFERENCE_QUANTILE)
        # ``stat > 0`` is implied by construction now (a quantile of strictly
        # positive values is positive); kept so the rule cannot silently
        # regress if the mask above is ever loosened.
        take = todo & (size >= QUANTITY_MIN_CELL) & (stat > 0)
        reference[take] = np.maximum(stat[take], QUANTITY_REFERENCE_FLOOR)
        rung[take] = name
    return reference, rung


# ---------------------------------------------------------------------------
# Site Q -- the delivered table.  Is a screened value impossible?
# ---------------------------------------------------------------------------

def audit_quantities(df: Any, column: str, *, country: str, table: str
                     ) -> list[dict[str, Any]]:
    """Measure which values of *column* are implausible against their cell.

    Returns one report per wave (per ``t``; one report for the whole frame when
    the frame carries no ``t``), each NAMING up to every offending row it
    found -- household, plot, crop, unit, value, reference and ratio.  Returns
    ``[]`` when nothing fires, which is the case for 99.98% of the corpus's
    ``crop_production`` rows.

    Deliberately silent on: a frame without *column*; an empty frame; a row
    whose value is null or non-finite; and a row for which no rung of the
    ladder offers a cell (see :func:`_cell_reference`).
    """
    if not isinstance(df, pd.DataFrame) or df.empty or column not in df.columns:
        return []
    q = _numeric(df[column])
    if not np.isfinite(q).any():
        return []

    named = {axis: _axis(df, axis) for axis in ("t", "i", "plot", "j", "u")}
    keys = {axis: named[axis] for axis in ("t", "u", "j")
            if named[axis] is not None}
    reference, rung = _cell_reference(q, keys)

    with np.errstate(invalid="ignore", divide="ignore"):
        ratio = q / reference
        flagged = ratio > QUANTITY_OUTLIER_K
    if not flagged.any():
        return []

    positions = np.flatnonzero(flagged)

    by_wave: dict[Any, list[dict[str, Any]]] = {}
    for pos in positions:
        offender = {
            "i": _label(named["i"], pos),
            "plot": _label(named["plot"], pos),
            "j": _label(named["j"], pos),
            "u": _label(named["u"], pos),
            "value": float(q[pos]),
            "reference": float(reference[pos]),
            "ratio": round(float(ratio[pos]), 1),
            "rung": None if rung[pos] is None else str(rung[pos]),
        }
        wave = _label(named["t"], pos)
        by_wave.setdefault(wave, []).append(offender)

    wave_sizes: dict[Any, int] = {}
    if named["t"] is not None:
        codes, categories = named["t"]
        sizes = np.bincount(codes + 1, minlength=len(categories) + 1)
        wave_sizes = {str(categories[k]): int(sizes[k + 1])
                      for k in range(len(categories))}

    reports: list[dict[str, Any]] = []
    for wave in sorted(by_wave, key=lambda w: (w is None, w)):
        offenders = sorted(by_wave[wave], key=lambda o: -o["ratio"])
        reports.append({
            "site": "built-table-quantity",
            "country": country,
            "table": table,
            "column": column,
            "wave": wave,
            "rows": int(wave_sizes.get(wave, len(df))),
            "n_implausible": len(offenders),
            "max_ratio": offenders[0]["ratio"],
            "k": QUANTITY_OUTLIER_K,
            "reference_quantile": QUANTITY_REFERENCE_QUANTILE,
            "min_cell": QUANTITY_MIN_CELL,
            "reference_floor": QUANTITY_REFERENCE_FLOOR,
            "offenders": offenders,
        })
    return reports


# ---------------------------------------------------------------------------
# Reporting -- one choke point, mirroring null_read_audit._emit_null_read_report
# ---------------------------------------------------------------------------

def _format_offender(offender: dict[str, Any]) -> str:
    parts = [f"i={offender['i']}"]
    if offender["plot"] is not None:
        parts.append(f"plot={offender['plot']}")
    if offender["j"] is not None:
        parts.append(offender["j"])
    if offender["u"] is not None:
        parts.append(f"u={offender['u']}")
    parts.append(f"{offender['value']:,.10g} (x{offender['ratio']:,.1f} its "
                 f"cell's p{QUANTITY_REFERENCE_QUANTILE * 100:.0f} of "
                 f"{offender['reference']:,.10g}, cell keyed on "
                 f"{offender['rung']})")
    return " ".join(parts)


def _format_quantity_report(report: dict[str, Any]) -> str:
    where = f"{report['country']}/{report['table']}"
    if report.get("wave"):
        where = f"{where} {report['wave']}"
    offenders = report["offenders"]
    shown = offenders[:_MAX_NAMED]
    more = len(offenders) - len(shown)
    listed = "; ".join(_format_offender(o) for o in shown)
    return (
        f"{where}: {report['n_implausible']:,} of {report['rows']:,} row(s) "
        f"report a '{report['column']}' more than {report['k']:,.0f}x the "
        f"{report['reference_quantile'] * 100:.0f}th percentile of comparable rows "
        f"with a positive value (same wave, unit and crop where at least "
        f"{report['min_cell']} such rows exist; zeros never set the scale): "
        f"{listed}{f' (+{more} more)' if more else ''}. "
        f"NOTHING HAS BEEN CHANGED -- these rows are returned exactly as they "
        f"are stored, because clipping an implausible harvest would hide the "
        f"defect rather than fix it. Read this as a work queue: check the "
        f"wave's source column and its unit label (a quantity keyed into a "
        f"container field, a calendar year keyed into a quantity field, and a "
        f"missing-value sentinel that was never stripped have each been the "
        f"answer in this corpus). Use quantity_reports() to get the rows "
        f"programmatically. Set {_QUANTITY_STRICT_ENV}=1 to make this fatal."
    )


# Reports filed during this process, keyed by (country, table).  A module-level
# ledger and not ``df.attrs``, for the reason GH #323 documents: pandas drops
# ``attrs`` whenever its inputs disagree, so routing a finding through attrs
# would silently lose exactly the findings this exists to surface.
_QUANTITY_LEDGER: dict[tuple[str, str], list[dict[str, Any]]] = {}


def _record_quantity_report(report: dict[str, Any]) -> None:
    """File a report for :func:`quantity_reports`, then emit it."""
    key = (str(report.get("country") or "?"), str(report.get("table") or "?"))
    existing = _QUANTITY_LEDGER.setdefault(key, [])
    if report not in existing:
        existing.append(report)
    _emit_quantity_report(report)


def _emit_quantity_report(report: dict[str, Any]) -> None:
    """Raise (strict) or warn (default).  The single choke point."""
    msg = _format_quantity_report(report)
    if _quantity_strict():
        raise QuantityImplausibleError(msg)
    warnings.warn(msg, QuantityImplausibleWarning, stacklevel=2)


def quantity_reports(country: str | None = None,
                     table: str | None = None) -> list[dict]:
    """Implausible-value reports filed during this process.

    Public read-only accessor -- the twin of ``null_read_audit`` 's
    :func:`~lsms_library.null_read_audit.null_read_reports` and of
    ``country.grain_reports`` -- for tests, for ``bench/feature_audit``, and
    for an analyst who wants the offending rows as data rather than as prose.
    Each report carries an ``offenders`` list naming every flagged row, even
    though the warning text shows only the first few.

    Findings are filed against the SOURCE table, not the one that triggered
    the read: a warning surfaced by ``Country.food_quantities()`` or
    ``Country.food_prices()`` -- both derived from ``food_acquired`` -- is
    filed under ``table='food_acquired'``, and
    ``quantity_reports(table='food_quantities')`` is therefore empty even
    when that call warned.
    """
    out: list[dict[str, Any]] = []
    for (c, t), reports in _QUANTITY_LEDGER.items():
        if country is not None and c != country:
            continue
        if table is not None and t != table:
            continue
        out.extend(reports)
    return out


# ---------------------------------------------------------------------------
# The one entry point the framework calls.  See the CACHE note in the module
# docstring: this and :func:`audit_quantities` are excluded from the build
# fingerprint because they provably cannot change a returned value.
# ---------------------------------------------------------------------------

def check_quantities(df: Any, *, country: str, table: str) -> Any:
    """Site Q.  Audit a built table's screened quantity columns.

    Returns *df* unchanged -- always, and by contract.  Call it for its effect,
    not its value.  Tables not named in :data:`_SCREENED_COLUMNS` cost one
    dictionary lookup.

    The framework calls this keyed on the framework's own method name, so a
    table DERIVED from a screened one (``food_quantities`` and
    ``food_prices``, both derived from ``food_acquired``) emits the source
    table's warnings and files its findings under the SOURCE table's name --
    a finding reached through ``Country.food_quantities()`` is reported under
    ``table='food_acquired'``, which is also where
    :func:`quantity_reports(table=...)` must look for it.
    """
    columns = _SCREENED_COLUMNS.get(str(table), ())
    if not columns:
        return df
    try:
        reports: list[dict[str, Any]] = []
        for column in columns:
            reports.extend(audit_quantities(df, column, country=country,
                                            table=table))
    except QuantityImplausibleError:
        raise
    except Exception as exc:                                   # noqa: BLE001
        # An instrument that fails silently and reports clean is the disease
        # this module exists to cure, so say so rather than swallow it.  A
        # broken audit must never break a read, though: the read is the job.
        warnings.warn(
            f"{country}/{table}: quantity plausibility audit could not run "
            f"({type(exc).__name__}: {exc}). The table itself is unaffected, "
            f"but its quantities are NOT known to be plausible.",
            QuantityImplausibleWarning, stacklevel=2)
        return df
    for report in reports:
        _record_quantity_report(report)
    return df


def _clear_quantity_reports() -> None:
    """Drop every filed report.  For tests only -- the ledger is process-wide
    and a test that asserts on it must not inherit another test's findings."""
    _QUANTITY_LEDGER.clear()
