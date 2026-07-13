# GH #323 — EthiopiaRHS: silent collapse of a non-unique declared index

Branch `fix/323-ethiopiarhs`, base `development` @ `d572d8a9`.

## §1 What the brief said, and where it was stale

The dispatched diagnosis said `_normalize_dataframe_index` applies
`groupby().first()` to EthiopiaRHS `food_acquired`, destroying 131 rows of real
Quantity/Expenditure, and prescribed (1) a source-level dedup and (2) declaring
`aggregation: sum`.

**Half of that was already fixed and the sign of the other half was inverted.**
`food_acquired` has been in `feature.py::_ADDITIVE_MEASURE_COLUMNS` since GH
`#501`/`#514` (`899b89c6`, `d8ff08d5`), so the collapse already **SUMs** rather
than `first()`s. Measured on the L2-wave parquets vs. the API:

| wave  | L2-wave Qty | API Qty | Δ |
|-------|-------------|---------|---|
| 1989  | 14020.6 | 14020.6 | 0 |
| 1994a | 116829.4 | 116829.4 | 0 |
| 1994b | 70860.2 | 70860.2 | 0 |
| 1997  | 86044.1 | 86044.1 | 0 |

Mass **preserved** ⇒ the sum path is live ⇒ the 131 "destroyed" rows were
already being folded in correctly. What the brief missed is the consequence:
**`sum` DOUBLE-COUNTS a row that was punched twice.**

## §2 What was actually broken

Instrument validated first on the mandated positives (Mali/2014-15
`household_roster` = 32,026 dup rows; Guyana/1992 `housing` = 311) before any
EthiopiaRHS number was trusted.

**Channel A — double-count (class-1, silently WRONG).** 90 rows across the five
roster waves are byte-identical in the canonical grain *and* in both measures.
The framework sums them, so they are counted twice: **514.9 Quantity and 343.1
Birr of pure overstatement.**

Smoking gun, end to end: `food89.dta` rows 865/866 are identical in *every*
source column (hh 20120, foodcode 34, qty 2.0, unit 9, value 0.5). The API
reported **Quantity 4.0 / Expenditure 1.0**. The source records one line.

The other 127 colliding rows genuinely DIFFER (1997 hh 10_93 produced Berbere
1.0 kg *and* 30.0 kg). Those are real repeat measurements and `sum` is right for
them. So dedup and sum are a **matched pair** — neither is correct alone.

**Channel B — NaN index keys silently DELETED (new; not in the brief).**
`groupby(level=..., observed=True)` defaults to `dropna=True`, so any row with a
NaN in an index level **vanishes** during the collapse. It only bites when the
index is non-unique (otherwise no groupby runs), which is why it hid inside
Channel A. EthiopiaRHS 1995 lost 11 rows (20.2 Qty, 23.7 Birr) this way — the
wide melt path filtered missing `i` and `u` but forgot `j`.

This model is **exactly predictive**: `rows − dup − nan_key` reproduces the
observed API row count for all five waves (2068 / 12971 / 13333 / 11956 / 14365).

## §3 The fix

**Class level** (`country.py`) — the part that matters for #323.
`SkunkWorks/grain_aggregation_policy.org` introduced an `aggregation:` block in
`data_scheme.yml`; ten countries declare one; **nothing read it.** It was
documentary prose while the code applied `.first()` regardless — the repo's own
"prose is not enforcement" failure mode. `_normalize_dataframe_index` now
**honours** it (`_declared_aggregation`), with precedence
declared → hardcoded additive map → `first`. An unknown reducer raises rather
than degrading to `first()`. Undeclared collapses stay loud, and
`LSMS_STRICT_INDEX=1` turns them into an error — the enforcement lever.

Proof it was inert on base: with `aggregation: {Quantity: sum}` declared and
rows 1.0 + 30.0, base returns **1.0**.

**Country level** (`EthiopiaRHS/_/ethiopiarhs.py`, `_/data_scheme.yml`).
`_drop_double_punched()` drops rows indistinguishable in the canonical grain,
*before* the framework sums; the wide path now also filters NaN `j`, converting
an accidental pandas deletion into an intentional, symmetric drop.
`aggregation: {Quantity: sum, Expenditure: sum}` is declared explicitly rather
than inherited from the hardcoded map.

## §4 Numbers

Cold rebuild (`cache clear --country EthiopiaRHS`; the L2-country parquet is
written POST-collapse, so a warm run shows the poisoned cache and appears to
pass).

| wave  | API rows before → after | Qty before → after | Exp before → after |
|-------|-------------------------|--------------------|--------------------|
| 1989  | 2068 → **2068** | 14020.6 → 14018.6 (−2.0) | 10094.8 → 10094.3 (−0.5) |
| 1994a | 12971 → **12971** | 116829.4 → 116745.5 (−83.9) | 50267.6 → 50243.5 (−24.1) |
| 1994b | 13333 → **13333** | 70860.2 → 70689.5 (−170.7) | 58759.7 → 58706.1 (−53.6) |
| 1995  | 11956 → **11956** | 56950.9 → 56791.6 (−159.3) | 44405.8 → 44300.8 (−105.0) |
| 1997  | 14365 → **14365** | 86044.1 → 85945.1 (−99.0) | 72176.8 → 72016.8 (−160.0) |

Row counts unchanged (the grain did not move); the mass falls by *exactly* the
double-counted amount. Byte-identical dupes 90 → **0**; the 127 real repeat
measurements are retained and summed; NaN-`j` 11 → **0**. 1989 Fenugreek now
reports **2.0 / 0.5**, matching source.

**Regression.** `_normalize_dataframe_index` was run over every affected cell
under old and new code, with the config tree *and* a frozen parquet snapshot
pinned (the shared cache is being mutated by other agents — Panama/Niger cells
appeared and vanished mid-run, which is what made a first, unpinned comparison
untrustworthy): **719 cells / 31 countries / 27,085,429 rows → 0 output
changes.** Includes the nine countries whose `interview_date: {visit: first}`
policy is now live; `first`-for-every-column is byte-identical to the old
`.first()` (NaN-skipping included, verified).

## §5 Judgment call, stated plainly

Treating the 90 byte-identical rows as double-punches rather than as genuine
repeat acquisitions of identical amount+price+unit+source is **not settleable
from the data** — the instrument records no axis on which the two rows differ.
I took the conservative reading and drop.

If that call is wrong, it **understates** by 90 rows (class-2, silently
missing). Summing them **overstates** (class-1, silently wrong). Class-2 is the
safer error, and for rows identical on every recorded axis, asserting "two
distinct events" is the stronger and less defensible claim. The 1989 case is the
tell: rows 865/866 are identical in *unrelated household-level fields too*,
which is a data-entry signature, not an economic one.

The load-bearing negative: **there is no missing level to name.** All 27 columns
of the q36 module were enumerated — no line / transaction / occasion / visit id.
`q36_1a` is a section flag (1.0/NaN/2.0), not a line number; adding it to the key
moves 1994a's dup count only 213 → 212. Unlike Guyana (where `SN` was the missing
level), `(t,i,j,u,s)` **is** the maximal grain the instrument supports, so a
synthetic row-order id would be a meaningless axis and would break cross-country
comparability.

## §6 What I did NOT fix, and why

**The class is only PARTLY closed. Do not close #323 on this branch.**

1. **Channel B is repo-wide and unfixed: 710,845 rows** with a NaN index key are
   silently deleted by `groupby(dropna=True)` across ~28 countries. The one-line
   fix (`dropna=False`) would *restore* those rows and thereby change many
   countries' outputs — it cannot be validated country-by-country from an
   EthiopiaRHS remit, and shipping it here would violate "prove you broke nothing
   else." **It needs its own issue.** I fixed only EthiopiaRHS's 11, at source.

2. **The `first()` default still stands for undeclared tables.** A repo-wide scan
   of L2-wave parquets on the full declared index finds **99 cells / 24 countries
   where colliding rows genuinely DIFFER (477,499 rows silently wrong)**. Making
   an undeclared collapse `raise` by default — which is the correct end state —
   breaks all 24 at cold build. This branch ships the *mechanism* (declare a
   policy) and the *lever* (`LSMS_STRICT_INDEX=1`); flipping the default requires
   declaring a policy for each of those countries first. Per-country damage:
   Burkina_Faso 184,308 · Mali 68,666 · Malawi 67,137 · Nigeria 58,782 · Uganda
   20,317 · Tanzania 20,102 · Tajikistan 11,540 · Albania 9,514 · South Africa
   8,454 · Kosovo 6,702 · India 6,194 · Guyana 4,545 · Liberia 2,954 · Kazakhstan
   1,861 · Niger 1,815 · Benin 1,302 · Cambodia 1,260 · China 758 · Togo 579 ·
   Senegal 208 · Ethiopia 207 · Serbia 163 · EthiopiaRHS 127 · CotedIvoire 4.
   (That scan does not separate *intended* level-drop aggregation from the bug;
   it is an upper bound on the cells needing a declared policy, not on the bug.)

## §7 Prior art consulted

`SkunkWorks/grain_aggregation_policy.org` (the `aggregation:` contract and its
"NO AGGREGATION IN CORE" direction — this change does not contradict it; it makes
the *interim* collapse honest), `feature.py::_ADDITIVE_MEASURE_COLUMNS` +
`_collapse_duplicate_index` (GH #501/#514 — reused as the fallback rather than
duplicated), CLAUDE.md cache tiers (why verification must be cold).
