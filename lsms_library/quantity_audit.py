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
part of the scale.  ``_cell_references`` masks it to ``NaN`` before
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

HISTORICAL FIRING BENCHMARK -- **every** country declaring ``crop_production``,
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

These row counts, firing counts and timings describe that dated build, not
the current corpus.  In particular, Nigeria's 62,844-row frame predates
GH #843's Farm-level sales and GH #1433's retained harvest lines (2026-11-04);
see ``countries/Nigeria/_/CONTENTS.org`` for the subsequent measurements.

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

NEW TABLES, NOT NEW CORRECTIONS (GH #1482 / #1485). ``plot_inputs.Quantity``
now takes the upper screen in its reported unit; purchased/gift quantities
can have their own units and are not included. Livestock takes the head-count
columns declared across all sixteen schemes, each stock/flow independently.
``animal`` identifies species; a thin species NEVER falls to an all-species
reference. Household herd totals without that axis are not species counts.
Both new screens keep any supplied country, wave, period, season, visit and
unit at every rung. Inputs try crop within input, input, then that fixed
boundary cell. Existing screened tables keep their original ladder unchanged.
The constants and arithmetic are reused, but no firing-rate or source-error
claim for these extensions has been established by the source-free schema and
invented-input checks. Thin cells can go unjudged, and a repeated suspect value
can still set its own p90. Absence of a warning is not validation. See ledger
``1482-1485-quantity-diagnostics.md`` for the census and source-validation gates.

SOLD AMOUNTS AND NATIVE SALE PRICES (GH #1569). ``Quantity_sold`` uses its
explicit ``Unit_sold``, never harvest ``u``. ``Sale_price`` is a diagnostic
name for ``Value_sold / Quantity_sold`` per that sold unit, computed only for
finite positive amounts; it is never added to the served frame. The existing
p90/support/tolerance arithmetic is reused, trying crop then pooling crops
within the same supplied country, wave, season, sold unit and sold condition.
Sold unit/state are never dropped or borrowed from harvest labels. Farm-level
rows participate. Missing/unknown sold units, unusable amounts and cells below
the positive-support floor remain unassessed and silent, under the existing
no-reference contract. A disagreement between harvest and sold units remains
unresolved; an outlier names both labels and uses the sold label AS REPORTED,
not as established physical truth. Only the upper tail is added by default.
No real-data firing rate or runtime cost has been measured for this extension;
see ledger ``1569-sold-diagnostics-remote-task.md``.

THE SURVEY KILOGRAM AGAINST ITS SALE PRICES (GH #1559, ruling @ligon 2026-11-12,
"GH1559 price-implied kg per unit for crops", "Also build a rung", part 1).  Site Q
judges a total against other totals, and ``kg_survey_off_label`` judges a survey
kilogram per unit against the mass its unit's label STATES; neither can see a
survey kilogram per ``Tiya`` or ``Botte``, which is most of Niger's survey kilograms.
:func:`audit_survey_kg_prices` names a row whose ``Quantity_kg / Quantity`` lies
outside 0.1x-10x (:data:`SURVEY_KG_PRICE_BAND`) of the PRICE-IMPLIED kilograms per
unit: the median price per unit of the same wave's and crop's sales in that unit,
over the median price per kilogram of its sales in ``Kg`` or in a unit stating its
mass, read at that mass (the ruling's baseline, "kg sales plus sacks at stated
mass").  Both sides need :data:`SURVEY_KG_PRICE_MIN_REPORTS` /
:data:`SURVEY_KG_PRICE_MIN_BASELINE` sales and a spread within
:data:`SURVEY_KG_PRICE_MAX_SPREAD`; the product state is matched where both the sale
and the harvest record one.  Only a unit stating NO mass is judged, so this screen
and the label screen never name the same row.  It is Site Q's concern (a value
present, non-null and implausible), so it rides Site Q's channels: the ledger,
:func:`quantity_reports` (``site='survey-kg-price'``, ``column='kg_survey'``, a
diagnostic name as ``Sale_price`` is), ``LSMS_QUANTITY_STRICT``, and a warning class
of its own that subclasses :class:`QuantityImplausibleWarning`.  It is the item
#1559's body names as missing ("not on Site Q's channels").  A flag and not a
verdict: the reference has measured biases (survey kilograms 1.1-1.4x it at the
median; state; sacks at their stated mass; sellers' prices), set out in the #1559
rung design memo.

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
    "SurveyKgPriceWarning",
    "QUANTITY_OUTLIER_K",
    "QUANTITY_MIN_CELL",
    "QUANTITY_REFERENCE_QUANTILE",
    "QUANTITY_REFERENCE_FLOOR",
    "QUANTITY_LOW_REFERENCE_QUANTILE",
    "SURVEY_KG_PRICE_BAND",
    "SURVEY_KG_PRICE_MIN_REPORTS",
    "SURVEY_KG_PRICE_MIN_BASELINE",
    "SURVEY_KG_PRICE_MAX_SPREAD",
    "audit_near_zero",
    "audit_quantities",
    "audit_survey_kg_prices",
    "check_quantities",
    "quantity_reports",
    "survey_kg_price_factors",
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

#: The lower tail's cell statistic (GH #1493): the upper rule's p90 mirrored, so
#: it is just as immune to a few contaminated rows.  Only a positive value is
#: judged and a zero never sets the scale, so a recorded "none" is never near zero.
QUANTITY_LOW_REFERENCE_QUANTILE = 0.10

#: GH #1559 (ruling @ligon 2026-11-12, "GH1559 price-implied kg per unit for
#: crops", "Also build a rung", part 1).  A SURVEY kilogram per unit
#: (``Quantity_kg / Quantity``) outside this multiple of the PRICE-IMPLIED
#: kilograms per unit is named.  It is the label screen's band
#: (``transformations.SURVEY_KG_LABEL_BAND``, ruling 2026-11-05), so the two
#: halves of #1559 judge on one footing; a test pins them equal.  Ten and not
#: two because the survey runs 1.1-1.4x the price-implied mass at the median:
#: a 2x band would name 11-12% of the rows it can judge (#1559 measurement,
#: reports/sold-kg-1560, Part 2).
SURVEY_KG_PRICE_BAND = (0.1, 10.0)

#: Support floors of the price-implied mass, per cell: sales in the UNIT, and
#: sales stating a MASS (the per-kilogram baseline).  The food ladder's numbers
#: (``FOOD_KG_MIN_REPORTS``, ``FOOD_KG_MIN_BASELINE``), and the ones the #1559
#: measurement the ruling was made on used.  Own constants, not imports: the
#: ruling keeps the crop design apart from the food ladder, so a food-ladder
#: change must not move this screen in silence.
SURVEY_KG_PRICE_MIN_REPORTS = 5
SURVEY_KG_PRICE_MIN_BASELINE = 5

#: Dispersion gate on BOTH sides of the ratio: ``p90/p10`` where a cell has 10
#: or more sales, ``max/min`` below that (``FOOD_KG_BASELINE_MAX_SPREAD``'s
#: statistic and value).  The food ladder gates only its baseline; the #1559
#: measurement gated both sides, and a screen that names rows should not judge
#: them against a unit price whose own sales disagree by more than the band.
SURVEY_KG_PRICE_MAX_SPREAD = 10.0

_SURVEY_KG_PRICE_SITE = "survey-kg-price"
_SURVEY_KG_PRICE_COLUMN = "kg_survey"

#: ``{tail: (quantile, floor)}``.  The lower tail has no floor: lifting a small
#: reference would make the rule fire more often, not less.
_TAILS: dict[str, tuple[float, float]] = {
    "high": (QUANTITY_REFERENCE_QUANTILE, QUANTITY_REFERENCE_FLOOR),
    "low": (QUANTITY_LOW_REFERENCE_QUANTILE, 0.0),
}

#: WHICH columns of WHICH tables are screened.  A registry of *what is looked
#: at*, NOT an allowlist of cells excused from looking -- the distinction the
#: no-allowlist rule turns on, and the same kind of mapping as
#: ``country._ADDITIVE_MEASURE_COLUMNS``.  ``food_acquired.Quantity`` was the
#: stated extension (#884) and is measured in the module docstring's second
#: census: 2,005 of 11.57M rows (0.017%) at #884, 1,998 of 11.93M under #1259;
#: 3,030 ms on a 5.34M-row GhanaLSS read (3.5% of the 85 s warm read).
_SCREENED_COLUMNS: dict[str, tuple[str, ...]] = {
    # GH #1482/#1485: native amounts only, not acquisition amounts in their
    # own unit columns, monetary values, ownership flags or TLU.  The head
    # columns are the union of all 16 declared livestock schemes (ledger
    # 1482-1485 §2); absent columns cost no audit and are never manufactured.
    "plot_inputs": ("Quantity",),
    "livestock": ("HeadCount", "HeadAcquired", "HeadSold",
                  "HeadCount12MonthsAgo", "HeadBornOrReceived", "HeadEaten",
                  "HeadLostOrDied"),
    # `Quantity_kg` (GH #1436) is the survey's own total, read first by the crop
    # ladder and served as recorded, so this is the only place an impossible one
    # is named.  `Quantity_sold_kg` (GH #1560, ruling @ligon 2026-11-12) is its
    # sold-side twin, a total served as recorded, screened the same way on the
    # generic (t,u,j) ladder: report-only, as everywhere here.
    "crop_production": ("Quantity", "Quantity_kg", "Quantity_sold", "Quantity_sold_kg",
                        "Sale_price"),
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
    # GH #1493: a price is a ratio, so a near-zero quantity puts its error here.
    # Senegal 2018-19 serves two prices near 1e22.  941 of 5,347,847 rows
    # (0.018%) on 2026-11-04, Site Q's rate on food_acquired, and only the
    # country being read warns.
    "food_prices": ("Price",),
}

#: WHICH columns get the lower tail as well (GH #1493): a positive value more than
#: :data:`QUANTITY_OUTLIER_K` times below its cell's
#: :data:`QUANTITY_LOW_REFERENCE_QUANTILE`.  On 2026-11-04, 1,201 of 11,238,869
#: positive ``food_acquired`` rows (0.011%) and 24 of 590,407 ``crop_production``
#: (0.004%).  ``plot_features.Area`` is not here, because a small plot is a plot.
#: Nor is ``food_prices.Price``: a low price is a high quantity, which the upper
#: rule already names (1,965 of 5,347,847 rows would fire, 946 of them in Mali).
_NEAR_ZERO_COLUMNS: dict[str, tuple[str, ...]] = {
    # `Quantity_sold_kg` takes the lower tail as `Quantity_kg` does (GH #1560).
    "crop_production": ("Quantity", "Quantity_kg", "Quantity_sold_kg"),
    "food_acquired": ("Quantity",),
}

#: Alternate spellings of the identifying axes, in preference order.  All of
#: these are live: ``j`` (Tanzania, Uganda, Ethiopia) vs ``crop`` (Malawi,
#: Nigeria); ``plot`` vs ``plot_id``; and ``u`` is an index LEVEL in
#: Uganda/Malawi/Ethiopia but a COLUMN in Tanzania/Nigeria.
_AXES: dict[str, tuple[str, ...]] = {
    "t": ("t",),
    "u": ("u",),
    "j": ("j", "crop", "item", "animal"),
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


class SurveyKgPriceWarning(QuantityImplausibleWarning):
    """A survey kilogram per unit far from what the unit's sale prices imply (GH #1559).

    A subclass, so a filter on :class:`QuantityImplausibleWarning` still sees it
    (it is Site Q's concern and Site Q's lever), while a caller who wants only
    this screen, or only the distributional one, can target it.
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
    the CALLER's choice: :func:`_cell_references` hands zeros in as ``NaN``
    (GH #1259), so this function stays a plain finite-value quantile.
    """
    stats, size = _group_quantiles(values, codes, (quantile,))
    return stats[0], size


def _group_quantiles(values: np.ndarray, codes: np.ndarray,
                     quantiles: tuple[float, ...]
                     ) -> tuple[list[np.ndarray], np.ndarray]:
    """:func:`_group_stats` for several quantiles from one sort.

    The ``lexsort`` is the cost, so the two tails of a column share it (GH #1493).
    """
    n = len(values)
    stats = [np.full(n, np.nan) for _ in quantiles]
    size = np.zeros(n, dtype=np.int64)
    finite = np.isfinite(values)
    if not finite.any():
        return stats, size

    keep = np.flatnonzero(finite)
    order = keep[np.lexsort((values[keep], codes[keep]))]
    sorted_values = values[order]
    sorted_codes = codes[order]

    first = np.flatnonzero(np.concatenate(
        ([True], sorted_codes[1:] != sorted_codes[:-1])))
    counts = np.diff(np.concatenate((first, [len(order)])))
    group_codes = sorted_codes[first]
    slot = np.clip(np.searchsorted(group_codes, codes), 0,
                   max(len(group_codes) - 1, 0))
    hit = group_codes[slot] == codes
    size[hit] = counts[slot[hit]]
    for stat, quantile in zip(stats, quantiles):
        position = quantile * (counts - 1)
        low = np.floor(position).astype(np.int64)
        high = np.ceil(position).astype(np.int64)
        frac = position - low
        group_stat = (sorted_values[first + low] * (1.0 - frac)
                      + sorted_values[first + high] * frac)
        stat[hit] = group_stat[slot[hit]]
    return stats, size


def _cell_reference(values: np.ndarray,
                    keys: dict[str, tuple[np.ndarray, np.ndarray]]
                    ) -> tuple[np.ndarray, np.ndarray]:
    """The upper tail's ``(reference, rung)``: see :func:`_cell_references`."""
    references, rung = _cell_references(values, keys, (_TAILS["high"],))
    return references[0], rung


def _cell_references(values: np.ndarray,
                     keys: dict[str, tuple[np.ndarray, np.ndarray]],
                     tails: tuple[tuple[float, float], ...],
                     *, rungs: list[tuple[str, list[str]]] | None = None,
                     ) -> tuple[list[np.ndarray], np.ndarray]:
    """``(references, rung)`` for every row, one reference per ``(quantile, floor)``.

    Walks the ladder ``(t, u, j)`` -> ``(t, u)`` -> ``(t)``, taking the first
    rung that offers a cell with at least :data:`QUANTITY_MIN_CELL` finite,
    STRICTLY POSITIVE values, and takes each quantile over those values alone
    (GH #1259: a zero neither defines the scale nor counts toward the floor),
    then lifts it to that tail's floor.  A row no rung can
    serve keeps a ``NaN`` reference and is never judged -- silence, not a
    guess.  A zero row still receives its cell's reference.

    *keys* maps an axis to the ``(codes, categories)`` pair :func:`_axis`
    returns; the rungs are folded out of those codes arithmetically, so no axis
    is grouped or factorised more than once however many rungs use it.
    Explicit *rungs* preserve the comparison boundaries of newly screened
    tables (GH #1482/#1485); the original ladder remains the default.
    """
    n = len(values)
    # Shift by one so a missing label (code -1) folds like any other group.
    axis_codes = {a: keys[a][0] + 1 for a in keys}
    axis_width = {a: int(len(keys[a][1])) + 1 for a in keys}

    if rungs is None:
        base = [k for k in ("t",) if k in keys]
        rungs = []
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

    references = [np.full(n, np.nan) for _ in tails]
    rung = np.empty(n, dtype=object)
    for name, cols in rungs:
        todo = np.isnan(references[0])
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
        stats, size = _group_quantiles(scale, codes, tuple(q for q, _ in tails))
        # ``stat > 0`` is implied by construction now (a quantile of strictly
        # positive values is positive); kept so the rule cannot silently
        # regress if the mask above is ever loosened.
        take = todo & (size >= QUANTITY_MIN_CELL) & (stats[0] > 0)
        for reference, stat, (_, floor) in zip(references, stats, tails):
            reference[take] = np.maximum(stat[take], floor)
        rung[take] = name
    return references, rung


# ---------------------------------------------------------------------------
# Site Q -- the delivered table.  Is a screened value impossible?
# ---------------------------------------------------------------------------

def _audit_crop_sales(df: Any, column: str, *, country: str,
                      tails: tuple[str, ...] = ("high",),
                      ) -> list[dict[str, Any]]:
    """Screen native sold amounts with explicit sold-unit denominators.

    Reuses Site Q arithmetic (ledger 1569 §§2–5), with the crop fine-to-coarse
    shape but no cross-unit/state/country/wave/season reference. Plot, cluster
    and source coordinates name the original row; they are not new support
    gates. No conversion, factor inference or harvest-label fallback occurs.
    The open unit vocabulary is retained; the shared missing-unit predicate
    identifies absent/sentinel labels without making physical assumptions.
    Unassessable values keep the existing silent/no-reference behavior.
    """
    if not isinstance(df, pd.DataFrame) or len(df) == 0:
        return []
    if "Quantity_sold" not in df.columns:
        return []
    if column == "Sale_price" and "Value_sold" not in df.columns:
        return []
    from .transformations import _unit_sentinel_mask

    named = {a: _axis(df, a) for a in (
        "country", "t", "v", "i", "plot", "j", "u", "source", "s",
        "period", "season", "visit", "production_obs", "Unit_sold",
        "Condition_sold", "condition_sold_native")}
    named["harvest_u"] = named["u"]
    named["u"] = named["Unit_sold"]  # NEVER substitute harvest u.
    if named["Unit_sold"] is None:
        return []
    codes, categories = named["Unit_sold"]
    sold = pd.Series([categories[c] if c >= 0 else None for c in codes])
    unsupported = (_unit_sentinel_mask(pd.DataFrame({"u": sold}))
                   | sold.astype("string").str.strip().eq("").fillna(False).to_numpy(bool))
    quantity = _numeric(df["Quantity_sold"])
    eligible = np.isfinite(quantity) & (quantity > 0) & ~unsupported
    q = quantity
    value = (_numeric(df["Value_sold"]) if "Value_sold" in df.columns
             else np.full(len(df), np.nan))
    if column == "Sale_price":
        with np.errstate(divide="ignore", invalid="ignore", over="ignore"):
            q = value / quantity
        eligible &= np.isfinite(value) & (value > 0) & np.isfinite(q) & (q > 0)
    if not eligible.any():
        return []

    # Canonical crop axes plus the sold unit/state columns (data_info.yml).
    # Unlike the historical crop ladder's wave rung, the sold path must never
    # pool different denominators or substitute harvest condition for sale.
    base = [a for a in ("country", "t", "season", "Unit_sold", "Condition_sold")
            if named[a] is not None]
    keys = {a: named[a] for a in base + ["j"] if named[a] is not None}
    cols = base + (["j"] if "j" in keys else [])
    rungs = [(",".join(cols), cols)]
    if cols != base:
        rungs.append((",".join(base), base))
    references, rung = _cell_references(
        np.where(eligible, q, np.nan), keys, tuple(_TAILS[t] for t in tails),
        rungs=rungs)
    reports = []
    for tail, reference in zip(tails, references):
        with np.errstate(divide="ignore", invalid="ignore", over="ignore"):
            ratio = q / reference if tail == "high" else reference / q
        quantile, floor = _TAILS[tail]
        reports.extend(_reports_for(
            df, column, named, eligible & (ratio > QUANTITY_OUTLIER_K),
            ratio, q, reference, rung, country=country, table="crop_production",
            direction=tail, quantile=quantile, floor=floor))
    for report in reports:
        report["unit_column"] = "Unit_sold"
        report["unit_semantics"] = "as-reported"
        if column == "Sale_price":
            report["definition"] = "Value_sold / Quantity_sold"
        for offender in report["offenders"]:
            pos = offender["row_position"]
            offender["quantity_sold"] = float(quantity[pos])
            offender["value_sold"] = (float(value[pos]) if np.isfinite(value[pos])
                                      else None)
    return reports


def audit_quantities(df: Any, column: str, *, country: str, table: str,
                     tails: tuple[str, ...] = ("high",),
                     ) -> list[dict[str, Any]]:
    """Measure which values of *column* are implausible against their cell.

    Returns one report per wave (per ``t``; one report for the whole frame when
    the frame carries no ``t``), each NAMING up to every offending row it
    found -- household, plot, crop, unit, value, reference and ratio.  Returns
    ``[]`` when nothing fires, which is the case for 99.98% of the corpus's
    ``crop_production`` rows.

    Deliberately silent on: a frame without *column*; an empty frame; a row
    whose value is null or non-finite; and a row for which no rung of the
    ladder offers a cell (see :func:`_cell_references`).

    *tails* adds the lower tail (:func:`audit_near_zero`) on the same ladder,
    which is why they are one call: the sort is the cost, and a column that takes
    both pays for it once (GH #1493).

    On ``crop_production``, ``Quantity_sold`` uses explicit sold labels and
    ``Sale_price`` names the native ratio ``Value_sold / Quantity_sold``, not a
    served column. No sold unit is inferred from harvest labels. Missing unit
    or insufficient positive support returns no report, as on the other paths.
    """
    if table == "crop_production" and column in {"Quantity_sold", "Sale_price"}:
        return _audit_crop_sales(df, column, country=country, tails=tails)
    if not isinstance(df, pd.DataFrame) or df.empty or column not in df.columns:
        return []
    q = _numeric(df[column])
    if not np.isfinite(q).any():
        return []

    named = {axis: _axis(df, axis) for axis in ("t", "i", "plot", "j", "u")}
    keys = {axis: named[axis] for axis in ("t", "u", "j")
            if named[axis] is not None}
    rungs = None
    if table in {"livestock", "plot_inputs"}:
        # Ledger 1482-1485 §4/§5: a thin cell cannot borrow another species,
        # native unit, country or recall occasion's scale.  No conversion or
        # inferred period is introduced.  Keep missing labels as groups too.
        for axis in ("country", "period", "season", "visit", "input", "animal"):
            named[axis] = _axis(df, axis)
        if table == "livestock":
            if named["animal"] is None:
                return []  # EthiopiaRHS's household totals are not species counts.
            named["j"] = keys["j"] = named["animal"]
        keys.update({axis: named[axis]
                     for axis in ("country", "period", "season", "visit", "input")
                     if named[axis] is not None})
        base = [a for a in ("country", "t", "period", "season", "visit", "u")
                if a in keys]
        if table == "livestock":
            columns = [base + ["j"]]
        else:
            columns = [base + [a for a in ("input", "j") if a in keys],
                       base + [a for a in ("input",) if a in keys], base]
        rungs = []
        for cols in columns:
            name = ",".join("animal" if table == "livestock" and a == "j"
                            else a for a in cols) or "whole-frame"
            if name not in [seen for seen, _ in rungs]:
                rungs.append((name, cols))
    references, rung = _cell_references(q, keys, tuple(_TAILS[t] for t in tails),
                                        rungs=rungs)

    reports: list[dict[str, Any]] = []
    for tail, reference in zip(tails, references):
        with np.errstate(invalid="ignore", divide="ignore"):
            if tail == "high":
                ratio = q / reference
                flagged = ratio > QUANTITY_OUTLIER_K
            else:
                ratio = reference / q
                flagged = np.isfinite(q) & (q > 0) & (ratio > QUANTITY_OUTLIER_K)
        quantile, floor = _TAILS[tail]
        reports.extend(_reports_for(
            df, column, named, flagged, ratio, q, reference, rung,
            country=country, table=table, direction=tail,
            quantile=quantile, floor=floor))
    return reports


def audit_near_zero(df: Any, column: str, *, country: str, table: str
                    ) -> list[dict[str, Any]]:
    """The lower tail of :func:`audit_quantities` (GH #1493).

    Reports a strictly positive value more than :data:`QUANTITY_OUTLIER_K` times
    below the :data:`QUANTITY_LOW_REFERENCE_QUANTILE` of the positive values in
    its cell, on the same ladder with no floor.  ``ratio`` is reference over
    value.  Senegal 2018-19 holds three quantities near 1e-19, the residue of a
    float subtraction, and each puts its error into a price, but those are the
    rare case: most rows it names sit under a ``Value`` unit or a gram or
    millilitre unit, and why is not established.  It flags, and does not say a
    row is wrong: a small amount may be real.  A zero is never judged: it records
    none, not almost none.
    """
    return audit_quantities(df, column, country=country, table=table,
                            tails=("low",))


def _reports_for(df, column, named, flagged, ratio, q, reference, rung, *,
                 country, table, direction, quantile, floor):
    """One report per wave of the rows *flagged*, each naming every one."""
    if not flagged.any():
        return []

    positions = np.flatnonzero(flagged)

    by_wave: dict[Any, list[dict[str, Any]]] = {}
    country_axis = named.get("country")
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
        for axis in ("animal", "input", "period", "season", "visit"):
            if named.get(axis) is not None:
                offender[axis] = _label(named[axis], pos)
        if "Unit_sold" in named:
            # Retain native row identity, including source/state coordinates;
            # no household/crop collapse is performed by the diagnostic.
            for axis in ("Unit_sold", "harvest_u", "Condition_sold",
                         "condition_sold_native", "v", "source", "s", "production_obs"):
                coordinate = named.get(axis)
                label = None
                if coordinate is not None:
                    code = int(coordinate[0][pos])
                    if code >= 0 and pd.notna(coordinate[1][code]):
                        label = str(coordinate[1][code])
                offender[axis] = label
            offender["u"] = offender["Unit_sold"]
            offender["row_position"] = int(pos)
            row_key = df.index[pos]
            if not isinstance(df.index, pd.MultiIndex):
                row_key = (row_key,)
            offender["index"] = [
                [name, None if pd.isna(label) else str(label)]
                for name, label in zip(df.index.names, row_key)]
        wave = _label(named["t"], pos)
        scope = (_label(country_axis, pos), wave) if country_axis is not None else wave
        by_wave.setdefault(scope, []).append(offender)

    wave_sizes: dict[Any, int] = {}
    if named["t"] is not None:
        codes, categories = named["t"]
        sizes = np.bincount(codes + 1, minlength=len(categories) + 1)
        wave_sizes = {str(categories[k]): int(sizes[k + 1])
                      for k in range(len(categories))}

    reports: list[dict[str, Any]] = []
    scopes = (sorted(by_wave, key=lambda w: str(w)) if country_axis is not None
              else sorted(by_wave, key=lambda w: (w is None, w)))
    for scope in scopes:
        report_country, wave = scope if country_axis is not None else (country, scope)
        rows = int(wave_sizes.get(wave, len(df)))
        if country_axis is not None:
            # Count the same country/wave slice that supplied the reference;
            # a Feature-shaped operand must not inflate either denominator.
            pos = next(p for p in positions
                       if (_label(country_axis, p), _label(named["t"], p)) == scope)
            mask = country_axis[0] == country_axis[0][pos]
            if named["t"] is not None:
                mask = mask & (named["t"][0] == named["t"][0][pos])
            rows = int(mask.sum())
        offenders = sorted(by_wave[scope], key=lambda o: -o["ratio"])
        reports.append({
            "site": "built-table-quantity",
            "country": report_country,
            "table": table,
            "column": column,
            "direction": direction,
            "wave": wave,
            "rows": rows,
            "n_implausible": len(offenders),
            "max_ratio": offenders[0]["ratio"],
            "k": QUANTITY_OUTLIER_K,
            "reference_quantile": quantile,
            "min_cell": QUANTITY_MIN_CELL,
            "reference_floor": floor,
            "offenders": offenders,
        })
    return reports


# ---------------------------------------------------------------------------
# GH #1559 -- a SURVEY KILOGRAM against the mass its unit's SALE PRICES imply.
# ---------------------------------------------------------------------------

#: The rungs of the price-implied mass, finest first.  ``state`` is the sale's
#: ``Condition_sold`` on the price side and the harvest's ``condition`` on the
#: survey side, used only where BOTH are recorded; ``country`` and ``season`` join
#: every rung when the frame carries them (a ``Feature`` frame), and ``t`` always
#: does: a wave's prices never judge another wave's kilograms.  There is no region
#: rung, because ``Region`` lives in ``cluster_features`` and this runs on the read
#: path of a table that must not re-enter another one (``attach_recall``'s reason).
_SURVEY_KG_PRICE_RUNGS: tuple[tuple[str, bool], ...] = (
    ("t,j,state,u", True),
    ("t,j,u", False),
)


def _row_labels(axis: tuple[np.ndarray, np.ndarray] | None) -> np.ndarray | None:
    """Per-row labels of an :func:`_axis` result, ``None`` where missing."""
    if axis is None:
        return None
    codes, categories = axis
    out = np.full(len(codes), None, dtype=object)
    hit = codes >= 0
    out[hit] = categories[codes[hit]]
    out[pd.isna(out)] = None
    return out


def _key_text(labels: np.ndarray | None, n: int) -> np.ndarray:
    """Grouping text: stripped, case-folded; ``'\\x00'`` for a missing label.

    Case-folded because a sold unit and a harvest unit are compared by LABEL,
    and Niger's s16d sold labels arrive lower-case where s16c's do not
    (``CONTENTS.org``, "the SOLD unit is Unit_sold").
    """
    if labels is None:
        return np.full(n, "", dtype=object)
    series = pd.Series(labels, dtype=object)
    text = series.astype(str).str.strip().str.casefold().to_numpy(dtype=object)
    return np.where(series.isna().to_numpy(), "\x00", text)


def _stated_masses(labels: np.ndarray) -> np.ndarray:
    """Kilograms a unit label STATES, per row; ``NaN`` where it states none.

    The ruling's baseline is "kg sales plus sacks at stated mass": a label is
    read, never estimated -- :data:`~lsms_library.transformations.KNOWN_METRIC`'s
    mass tokens (``Kg``, ``Gramme``, ``Tonne``, ...) and what
    ``_parse_explicit_metric`` takes off a self-describing label (``Sac de 100
    kg``, ``Sac (50 Kg)``).  VOLUMES ARE NOT MASSES: litre spellings are dropped
    (``volume_as_mass=False``), as the food ladder's baseline drops them
    (``_BASELINE_UNIT_CONVERSION``).  A country's declared ``u_kg`` is not read:
    it is a table, not a label.
    """
    from .transformations import _FLUID_UNITS, KNOWN_METRIC, _parse_explicit_metric

    fluids = {u.lower() for u in _FLUID_UNITS}
    masses: dict[Any, float] = {}
    for label in pd.unique(pd.Series(labels, dtype=object).dropna()):
        text = str(label).strip()
        low = text.lower()
        kg = np.nan
        if low in fluids:
            kg = np.nan
        elif low in KNOWN_METRIC:
            kg = float(KNOWN_METRIC[low])
        else:
            parsed = _parse_explicit_metric(text, volume_as_mass=False)
            if parsed is not None and np.isfinite(parsed) and parsed > 0:
                kg = float(parsed)
        masses[label] = kg
    return pd.Series(labels, dtype=object).map(masses).to_numpy(
        dtype="float64", na_value=np.nan)


def _usable_units(labels: np.ndarray) -> np.ndarray:
    """Rows whose unit label names a unit at all.

    Not the missing-unit sentinel (``Unknown``, ``Manquant``, NA), not blank, and
    not a currency-denominated label: a price per ``Value`` is the amount over
    itself (GH #1129).
    """
    from .transformations import _is_currency_denominated, _unit_sentinel_mask

    series = pd.Series(labels, dtype=object)
    sentinel = _unit_sentinel_mask(pd.DataFrame({"u": series}))
    blank = series.astype(str).str.strip().eq("").to_numpy(dtype=bool)
    currency = _is_currency_denominated(series.fillna("").astype(str))
    return ~(np.asarray(sentinel, dtype=bool) | blank | currency)


def _known_state(labels: np.ndarray | None, n: int) -> np.ndarray:
    """Rows whose product state is recorded: not NA, not ``unknown_condition``."""
    if labels is None:
        return np.zeros(n, dtype=bool)
    text = pd.Series(_key_text(labels, n), dtype=object).astype(str)
    return (~text.isin(["\x00", ""]) & ~text.str.startswith("unknown")).to_numpy(dtype=bool)


def _cell_stats(keys: dict[str, np.ndarray], values: np.ndarray,
                rows: np.ndarray) -> pd.DataFrame:
    """``n``, ``median`` and ``spread`` of *values* over *rows*, per cell of *keys*.

    ``spread`` is ``p90/p10`` at ``n >= 10`` and ``max/min`` below (the food
    ladder's dispersion statistic).  Quantiles interpolate as ``Series.quantile``.
    """
    names = list(keys)
    frame = pd.DataFrame({k: keys[k][rows] for k in names})
    frame["_v"] = values[rows]
    grouped = frame.groupby(names, sort=False, dropna=False)["_v"]
    out = pd.DataFrame({"n": grouped.size(), "median": grouped.median(),
                        "lo": grouped.min(), "hi": grouped.max()})
    q = grouped.quantile([0.1, 0.9]).unstack()
    with np.errstate(divide="ignore", invalid="ignore"):
        decile = (q[0.9] / q[0.1]).reindex(out.index)
        extreme = out["hi"] / out["lo"]
    out["spread"] = np.where(out["n"] >= 10, decile, extreme)
    return out


def survey_kg_price_factors(df: Any) -> pd.DataFrame | None:
    """Per-row PRICE-IMPLIED kilograms per unit for ``crop_production`` (GH #1559).

    The measurement half of :func:`audit_survey_kg_prices`, public for tests and
    for an analyst who wants the per-row comparison rather than the offenders.
    Returns ``None`` when nothing can be judged (no ``Quantity_kg``, no sales, no
    ``u`` or ``Unit_sold``); otherwise a frame on *df*'s index, in its order:

    ``kg_survey``
        ``Quantity_kg / Quantity`` on rows with both finite and positive and a
        usable harvest unit that states NO mass, else NaN.  A stated-mass unit is
        never judged here: it is in the baseline, and
        ``transformations.harvest_kg_factors``' ``kg_survey_off_label`` judges it
        against its label, so the two screens never name the same row.
    ``kg_price_implied``
        median ``Value_sold / Quantity_sold`` of the cell's sales in that unit,
        over the median price per kilogram of its sales in a unit STATING a mass,
        read at that mass (:func:`_stated_masses`).  NaN where no rung passes its
        gates.
    ``kg_price_rung``
        ``'t,j,state,u'`` or ``'t,j,u'`` (:data:`_SURVEY_KG_PRICE_RUNGS`).
    ``kg_price_n_unit`` / ``kg_price_n_kg``
        sales behind each side of the ratio.
    ``kg_price_unit_price`` / ``kg_price_kg_price``
        the two medians.
    ``survey_over_implied``
        ``kg_survey / kg_price_implied``.
    ``kg_price_flag``
        ``'low'`` below :data:`SURVEY_KG_PRICE_BAND`, ``'high'`` above, else missing
        (read it with ``notna()``: pandas 3 may store the column as ``str``).

    A sale is a row with ``Quantity_sold > 0``, ``Value_sold > 0`` and a usable
    ``Unit_sold`` -- the sale's OWN unit, never the harvest's (GH #824).  The
    survey's sold kilograms (``Quantity_sold_kg``) are not read: on a sale in the
    harvest's unit they equal the harvest's kilograms per unit on most rows (Niger
    2021-22: 939 of 1,134), so they would judge the survey by itself.
    """
    if not isinstance(df, pd.DataFrame) or df.empty:
        return None
    if any(c not in df.columns
           for c in ("Quantity", "Quantity_kg", "Quantity_sold", "Value_sold")):
        return None
    n = len(df)
    total = _numeric(df["Quantity_kg"])
    quantity = _numeric(df["Quantity"])
    survey = np.isfinite(total) & (total > 0) & np.isfinite(quantity) & (quantity > 0)
    if not survey.any():
        return None
    harvest_u = _row_labels(_axis(df, "u"))
    sold_u = _row_labels(_axis(df, "Unit_sold"))
    if harvest_u is None or sold_u is None:
        return None

    sold_q = _numeric(df["Quantity_sold"])
    sold_v = _numeric(df["Value_sold"])
    with np.errstate(divide="ignore", invalid="ignore", over="ignore"):
        price = sold_v / sold_q
        kg_survey = total / quantity
    mass_sold = _stated_masses(sold_u)
    mass_harvest = _stated_masses(harvest_u)
    sale = (np.isfinite(sold_q) & (sold_q > 0) & np.isfinite(sold_v) & (sold_v > 0)
            & np.isfinite(price) & (price > 0) & _usable_units(sold_u))
    by_mass = sale & np.isfinite(mass_sold)
    by_unit = sale & ~np.isfinite(mass_sold)
    judged = survey & _usable_units(harvest_u) & ~np.isfinite(mass_harvest)
    with np.errstate(divide="ignore", invalid="ignore"):
        price_kg = np.where(by_mass, price / mass_sold, np.nan)

    implied = np.full(n, np.nan)
    rung = np.full(n, None, dtype=object)
    n_unit = np.full(n, np.nan)
    n_kg = np.full(n, np.nan)
    unit_price = np.full(n, np.nan)
    kg_price = np.full(n, np.nan)

    base = {a: _key_text(_row_labels(_axis(df, a)), n)
            for a in ("country", "t", "season", "j") if _axis(df, a) is not None}
    if not base:
        base = {"_frame": np.full(n, "", dtype=object)}
    sold_unit = _key_text(sold_u, n)
    harvest_unit = _key_text(harvest_u, n)
    sold_state = _key_text(_row_labels(_axis(df, "Condition_sold")), n)
    harvest_state = _key_text(_row_labels(_axis(df, "condition")), n)
    sold_known = _known_state(_row_labels(_axis(df, "Condition_sold")), n)
    harvest_known = _known_state(_row_labels(_axis(df, "condition")), n)

    if by_mass.any() and by_unit.any() and judged.any():
        for name, with_state in _SURVEY_KG_PRICE_RUNGS:
            todo = judged & np.isnan(implied)
            if with_state:
                todo &= harvest_known
                mass_rows, unit_rows = by_mass & sold_known, by_unit & sold_known
            else:
                mass_rows, unit_rows = by_mass, by_unit
            if not (todo.any() and mass_rows.any() and unit_rows.any()):
                continue
            keys = dict(base)
            if with_state:
                keys["state"] = sold_state
            baseline = _cell_stats(keys, price_kg, mass_rows)
            units = _cell_stats({**keys, "u": sold_unit}, price, unit_rows)
            baseline = baseline[(baseline["n"] >= SURVEY_KG_PRICE_MIN_BASELINE)
                                & (baseline["spread"] <= SURVEY_KG_PRICE_MAX_SPREAD)]
            units = units[(units["n"] >= SURVEY_KG_PRICE_MIN_REPORTS)
                          & (units["spread"] <= SURVEY_KG_PRICE_MAX_SPREAD)]
            if baseline.empty or units.empty:
                continue
            names = list(keys)
            cell = units.reset_index().merge(
                baseline.reset_index(), on=names, suffixes=("_u", "_kg"))
            if cell.empty:
                continue
            cell["implied"] = cell["median_u"] / cell["median_kg"]
            cell = cell.set_index(names + ["u"])
            want = {k: v for k, v in keys.items()}
            if with_state:
                want["state"] = harvest_state
            want["u"] = harvest_unit
            positions = np.flatnonzero(todo)
            lookup = pd.MultiIndex.from_arrays(
                [want[k][positions] for k in names + ["u"]], names=names + ["u"])
            got = cell.reindex(lookup)
            hit = got["implied"].notna().to_numpy()
            if not hit.any():
                continue
            take = positions[hit]
            implied[take] = got["implied"].to_numpy()[hit]
            rung[take] = name
            n_unit[take] = got["n_u"].to_numpy(dtype="float64")[hit]
            n_kg[take] = got["n_kg"].to_numpy(dtype="float64")[hit]
            unit_price[take] = got["median_u"].to_numpy()[hit]
            kg_price[take] = got["median_kg"].to_numpy()[hit]

    with np.errstate(divide="ignore", invalid="ignore"):
        ratio = np.where(judged, kg_survey, np.nan) / implied
    low, high = SURVEY_KG_PRICE_BAND
    flag = np.full(n, None, dtype=object)
    flag[np.isfinite(ratio) & (ratio < low)] = "low"
    flag[np.isfinite(ratio) & (ratio > high)] = "high"
    return pd.DataFrame({
        "kg_survey": np.where(judged, kg_survey, np.nan),
        "kg_price_implied": implied,
        "kg_price_rung": rung,
        "kg_price_n_unit": n_unit,
        "kg_price_n_kg": n_kg,
        "kg_price_unit_price": unit_price,
        "kg_price_kg_price": kg_price,
        "survey_over_implied": ratio,
        "kg_price_flag": flag,
    }, index=df.index)


def audit_survey_kg_prices(df: Any, *, country: str,
                           table: str = "crop_production") -> list[dict[str, Any]]:
    """Name every survey kilogram outside the band of its price-implied mass (GH #1559).

    REPORT ONLY, by the same contract as the rest of this module: it reads *df*
    and returns reports; it never touches a value.  One report per wave and
    direction (``'low'``: below 0.1x, ``'high'``: above 10x), each naming every
    offending row with the implied factor, both cell sizes, both medians and the
    rung, because a reader must be able to see what the row was judged against.
    ``[]`` for any table but ``crop_production``, for a frame with no survey
    kilograms, and where nothing fires.

    A flag, not a verdict.  The price-implied mass has measured biases of its own
    (``survey_kg_price_factors``; the GH #1559 design memo): survey kilograms run
    1.1-1.4x it at the median; a state the sale does not share moves a price per
    kilogram by about 2.4x (shelled against unshelled groundnut) and only the
    EHCVM waves record it; sacks are read at their stated mass, which the
    survey's own kilograms dispute; and the prices come from the fifth of
    households that sell.
    """
    if str(table) != "crop_production":
        return []
    factors = survey_kg_price_factors(df)
    if factors is None:
        return []
    flag = factors["kg_price_flag"].to_numpy(dtype=object)
    flagged = factors["kg_price_flag"].notna().to_numpy(dtype=bool)
    if not flagged.any():
        return []

    named = {a: _axis(df, a) for a in ("country", "t", "i", "plot", "j", "u", "condition")}
    judged = np.isfinite(factors["kg_price_implied"].to_numpy(dtype="float64")) \
        & np.isfinite(factors["kg_survey"].to_numpy(dtype="float64"))
    quantity = _numeric(df["Quantity"])
    total = _numeric(df["Quantity_kg"])
    cols = {c: factors[c].to_numpy() for c in factors.columns}
    n = len(df)
    country_of = (np.array([None if v is None else str(v)
                            for v in _row_labels(named["country"])], dtype=object)
                  if named["country"] is not None else np.full(n, country, dtype=object))
    wave_of = (np.array([None if v is None else str(v) for v in _row_labels(named["t"])],
                        dtype=object)
               if named["t"] is not None else np.full(n, None, dtype=object))

    by_scope: dict[tuple[Any, Any, str], list[dict[str, Any]]] = {}
    for pos in np.flatnonzero(flagged):
        direction = str(flag[pos])
        value = float(cols["kg_survey"][pos])
        implied = float(cols["kg_price_implied"][pos])
        fold = value / implied if direction == "high" else implied / value
        offender = {
            "i": _label(named["i"], pos),
            "plot": _label(named["plot"], pos),
            "j": _label(named["j"], pos),
            "u": _label(named["u"], pos),
            "condition": _label(named["condition"], pos),
            "value": value,
            "quantity": float(quantity[pos]),
            "quantity_kg": float(total[pos]),
            "implied": implied,
            "reference": implied,
            "survey_over_implied": float(cols["survey_over_implied"][pos]),
            "ratio": round(float(fold), 1),
            "rung": (None if pd.isna(cols["kg_price_rung"][pos])
                     else str(cols["kg_price_rung"][pos])),
            "n_unit_sales": int(cols["kg_price_n_unit"][pos]),
            "n_kg_sales": int(cols["kg_price_n_kg"][pos]),
            "unit_price": float(cols["kg_price_unit_price"][pos]),
            "kg_price": float(cols["kg_price_kg_price"][pos]),
            "row_position": int(pos),
        }
        by_scope.setdefault((country_of[pos], wave_of[pos], direction), []).append(offender)

    reports: list[dict[str, Any]] = []
    for (c, w, direction) in sorted(by_scope, key=lambda k: (str(k[0]), str(k[1]), k[2])):
        in_scope = (country_of == c) & (wave_of == w)
        offenders = sorted(by_scope[(c, w, direction)], key=lambda o: -o["ratio"])
        reports.append({
            "site": _SURVEY_KG_PRICE_SITE,
            "country": c,
            "table": "crop_production",
            "column": _SURVEY_KG_PRICE_COLUMN,
            "definition": "Quantity_kg / Quantity",
            "direction": direction,
            "wave": w,
            "rows": int(in_scope.sum()),
            "n_judged": int((judged & in_scope).sum()),
            "n_implausible": len(offenders),
            "max_ratio": offenders[0]["ratio"],
            "k": SURVEY_KG_PRICE_BAND[1],
            "band": list(SURVEY_KG_PRICE_BAND),
            "min_reports": SURVEY_KG_PRICE_MIN_REPORTS,
            "min_baseline": SURVEY_KG_PRICE_MIN_BASELINE,
            "max_spread": SURVEY_KG_PRICE_MAX_SPREAD,
            "offenders": offenders,
        })
    return reports


# ---------------------------------------------------------------------------
# Reporting -- one choke point, mirroring null_read_audit._emit_null_read_report
# ---------------------------------------------------------------------------

#: ``{(table, column): what the value is}`` for a screened column that holds a
#: KILOGRAM TOTAL while its cell is keyed on the row's harvest ``u`` (GH #1560,
#: red team F2).  Printed as ``u=Sac (100 Kg) 1,150,000``, a total of kilograms
#: sold read as 1,150,000 sacks.  For these the message names the unit
#: ``harvest_u`` and puts the value's own unit after the value and the reference.
#: The unit stays in the message because the reference IS that harvest-unit
#: cell's.  Wording only: which rows are named, and the structured report
#: (``u``, ``rung``, ``value``), do not depend on it.  ``Quantity_kg`` reads the
#: same way and is left as it was.
_KG_TOTAL_ON_HARVEST_U: dict[tuple[str, str], str] = {
    ("crop_production", "Quantity_sold_kg"): "kg sold",
}


def _format_offender(offender: dict[str, Any], direction: str = "high",
                     quantile: float = QUANTITY_REFERENCE_QUANTILE,
                     kg_total: str | None = None) -> str:
    parts = [f"i={offender['i']}"]
    if offender["plot"] is not None:
        parts.append(f"plot={offender['plot']}")
    if "animal" in offender:
        parts.append(f"animal={offender['animal']}")
    elif offender["j"] is not None:
        parts.append(offender["j"])
    for axis in ("input", "period", "season", "visit"):
        if axis in offender:
            parts.append(f"{axis}={offender[axis]}")
    if "Unit_sold" in offender:
        for axis in ("Unit_sold", "harvest_u", "Condition_sold", "v", "source", "s",
                     "production_obs"):
            if offender.get(axis) is not None:
                parts.append(f"{axis}={offender[axis]}")
    elif offender["u"] is not None:
        parts.append(f"{'harvest_u' if kg_total else 'u'}={offender['u']}")
    scale = (f"x{offender['ratio']:,.1f} its" if direction == "high"
             else f"1/{offender['ratio']:,.1f} of its")
    unit = f" {kg_total}" if kg_total else ""
    rung = offender["rung"]
    if kg_total and rung:
        rung = ",".join("harvest_u" if a == "u" else a for a in str(rung).split(","))
    parts.append(f"{offender['value']:,.10g}{unit} ({scale} "
                 f"cell's p{quantile * 100:.0f} of "
                 f"{offender['reference']:,.10g}{unit}, cell keyed on "
                 f"{rung})")
    return " ".join(parts)


def _format_survey_kg_price_report(report: dict[str, Any]) -> str:
    """The GH #1559 warning.  Its own text: the reference is a price ratio, not a p90."""
    where = f"{report['country']}/{report['table']}"
    if report.get("wave"):
        where = f"{where} {report['wave']}"
    offenders = report["offenders"]
    shown = offenders[:_MAX_NAMED]
    more = len(offenders) - len(shown)
    low, high = report["band"]
    side = (f"more than {high:g}x" if report["direction"] == "high"
            else f"less than {low:g}x")

    def one(o: dict[str, Any]) -> str:
        parts = [f"i={o['i']}"]
        if o["plot"] is not None:
            parts.append(f"plot={o['plot']}")
        if o["j"] is not None:
            parts.append(str(o["j"]))
        if o["u"] is not None:
            parts.append(f"u={o['u']}")
        if o.get("condition") is not None:
            parts.append(f"condition={o['condition']}")
        parts.append(
            f"{o['quantity_kg']:,.10g} kg on {o['quantity']:,.10g} = {o['value']:,.4g} kg per "
            f"unit against {o['implied']:,.4g} implied ({o['survey_over_implied']:,.3g}x; "
            f"{o['n_unit_sales']} sales in the unit, {o['n_kg_sales']} by mass; cell {o['rung']})")
        return " ".join(parts)

    listed = "; ".join(one(o) for o in shown)
    return (
        f"{where}: {report['n_implausible']:,} of the {report['n_judged']:,} row(s) this "
        f"screen can judge (of {report['rows']:,}) report a survey kilogram per unit "
        f"(Quantity_kg / Quantity) {side} the kilograms per unit the wave's own crop sales "
        f"imply (median price per unit sold, over the median price per kilogram of sales "
        f"in Kg or in a unit stating its mass, read at that mass; same wave and crop, and "
        f"the same product state where both record one; at least "
        f"{report['min_reports']} sales on each side, spread at most "
        f"{report['max_spread']:g}x): {listed}{f' (+{more} more)' if more else ''}. "
        f"NOTHING HAS BEEN CHANGED -- these rows are returned exactly as they are stored. "
        f"A flag, not a verdict: the price-implied mass has biases of its own (survey "
        f"kilograms run 1.1-1.4x it at the median, sacks are read at their stated mass, "
        f"a state the sale does not share moves a price per kilogram, and the prices are "
        f"the sellers'). A sack or a total keyed against a small unit's label has been "
        f"the answer in this corpus. Use quantity_reports() to get the rows "
        f"programmatically. Set {_QUANTITY_STRICT_ENV}=1 to make this fatal."
    )


def _format_quantity_report(report: dict[str, Any]) -> str:
    if report.get("site") == _SURVEY_KG_PRICE_SITE:
        return _format_survey_kg_price_report(report)
    where = f"{report['country']}/{report['table']}"
    if report.get("wave"):
        where = f"{where} {report['wave']}"
    offenders = report["offenders"]
    shown = offenders[:_MAX_NAMED]
    more = len(offenders) - len(shown)
    direction = report.get("direction", "high")
    kg_total = _KG_TOTAL_ON_HARVEST_U.get((report["table"], report["column"]))
    listed = "; ".join(
        _format_offender(o, direction, report["reference_quantile"], kg_total)
        for o in shown)
    if direction == "high":
        tail = (f"more than {report['k']:,.0f}x the "
                f"{report['reference_quantile'] * 100:.0f}th percentile")
        answers = ("a quantity keyed into a container field, a calendar year "
                   "keyed into a quantity field, and a missing-value sentinel "
                   "that was never stripped have each been the answer in this "
                   "corpus")
    else:
        tail = (f"more than {report['k']:,.0f}x below the "
                f"{report['reference_quantile'] * 100:.0f}th percentile")
        answers = ("most such rows in this corpus sit under a 'Value' unit or a "
                   "gram or millilitre unit, which reads like another scale or a "
                   "larger unit keyed under a small one, though that is not "
                   "established; a few are the residue of a floating-point "
                   "subtraction, and a small amount may be real")
    comparison = ("same wave, unit and crop where at least "
                  f"{report['min_cell']} such rows exist; zeros never set the scale")
    if report["table"] in {"livestock", "plot_inputs"}:
        comparison = (
            "country, wave, unit and any period/season/visit kept separate; "
            + ("species kept separate at every rung; " if report["table"] == "livestock"
               else "crop, then input may pool within those boundaries; ")
            + f"at least {report['min_cell']} positive rows; zeros never set the scale")
    if report["table"] == "crop_production" and report["column"] in {"Quantity_sold", "Sale_price"}:
        comparison = (
            "explicit Unit_sold and sold condition kept separate within country/"
            "wave/season; crop then the same sold-unit/state cell; "
            f"at least {report['min_cell']} positive finite rows")
        answers = ("Sale_price means Value_sold / Quantity_sold per Unit_sold, "
                   "not per harvest u or per kg; disagreeing unit labels "
                   "remain unreliable, and no sentinel or source correction is inferred")
    if kg_total is not None:
        comparison = (
            "each value is the TOTAL kilograms sold on its row, not a count of "
            "any unit; comparable rows share the wave, the row's HARVEST unit "
            "(harvest_u, not the sale's Unit_sold) and the crop where at least "
            f"{report['min_cell']} such rows exist; zeros never set the scale")
    return (
        f"{where}: {report['n_implausible']:,} of {report['rows']:,} row(s) "
        f"report a '{report['column']}' {tail} of comparable rows "
        f"with a positive value ({comparison}): "
        f"{listed}{f' (+{more} more)' if more else ''}. "
        f"NOTHING HAS BEEN CHANGED -- these rows are returned exactly as they "
        f"are stored, because clipping an implausible value would hide the "
        f"defect rather than fix it. Read this as a work queue: check the "
        f"wave's source column and its unit label ({answers}). "
        f"Use quantity_reports() to "
        f"get the rows programmatically. Set {_QUANTITY_STRICT_ENV}=1 to make "
        f"this fatal."
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
    category = (SurveyKgPriceWarning if report.get("site") == _SURVEY_KG_PRICE_SITE
                else QuantityImplausibleWarning)
    warnings.warn(msg, category, stacklevel=2)


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
    when that call warned.  ``food_prices`` also files its own ``Price`` findings
    under ``table='food_prices'`` (GH #1493).  A report's ``direction`` is
    ``'high'``, or ``'low'`` for :func:`audit_near_zero`.
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
    :func:`quantity_reports(table=...)` must look for it.  ``food_prices`` is
    screened in its own right as well, on ``Price`` (GH #1493), and files that
    under ``table='food_prices'``.
    """
    columns = _SCREENED_COLUMNS.get(str(table), ())
    if not columns:
        return df
    try:
        reports: list[dict[str, Any]] = []
        near_zero = _NEAR_ZERO_COLUMNS.get(str(table), ())
        for column in columns:
            tails = ("high", "low") if column in near_zero else ("high",)
            reports.extend(audit_quantities(df, column, country=country,
                                            table=table, tails=tails))
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
    # GH #1559: the survey's kilograms against their sale prices.  After the
    # distributional screens, so a frame's reports keep their old order, and in
    # its own guard, so a failure here cannot silence those screens' findings.
    try:
        reports.extend(audit_survey_kg_prices(df, country=country, table=table))
    except Exception as exc:                                   # noqa: BLE001
        warnings.warn(
            f"{country}/{table}: survey-kilogram price screen could not run "
            f"({type(exc).__name__}: {exc}). The table itself is unaffected, "
            f"but its survey kilograms are NOT known to agree with its sale prices.",
            SurveyKgPriceWarning, stacklevel=2)
    for report in reports:
        _record_quantity_report(report)
    return df


def _clear_quantity_reports() -> None:
    """Drop every filed report.  For tests only -- the ledger is process-wide
    and a test that asserts on it must not inherit another test's findings."""
    _QUANTITY_LEDGER.clear()
