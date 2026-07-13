# Prior-Art Ledger — GH #323, Kosovo

**Search tier used:** ripgrep + git (gitnexus not consulted; the change is confined to
one country's `_/` config tree plus a new test — no library symbol is edited).

## §1 Task, restated

`Country('Kosovo').housing()` and `.cluster_features()` were both being produced by
`_normalize_dataframe_index`'s `groupby().first()` collapse of a non-unique DECLARED
index (GH #323). The two cells have DIFFERENT root causes and different severities:

- **`2000/housing`** — INDEX_INCOMPLETE, and **silently WRONG (class-1)**.
  `DWELLING.dta` section 3a is a *roster over dwelling structures*: the questionnaire
  (`2000/Documentation/hheng.pdf` p.82) instructs the enumerator to "LIST ALL STRUCTURES
  BEFORE ASKING Q.2-6" and asks `s3a_q01` "Does your household use [STRUCTURE]?" for each
  of 8 structure types. So the source is 7,047 rows over 2,865 households, keyed
  `(hhid, s3a_q0a)`. Declared `(t, i)`, it reached the canonical index with **4,182
  duplicate tuples**. Because `GroupBy.first()` is **skipna per column** it did not pick
  a row — it *fabricated* one: `Type` came from roster row 0 (usually a structure the
  household answered "No" to) while `Rooms`/`Tenure` came from the first non-null, i.e.
  the actually-occupied structure. **524 of 2,850** single-occupancy households (18.4%)
  were published with a dwelling type they had explicitly denied. The per-column skipna
  is exactly why no sanity check ever tripped.
- **A second, independent class-1 bug in the same cell:** `Electricity: s3a_q01` is a
  **mis-mapped column** — `s3a_q01` is the occupancy screener, not electricity. All 2,865
  published values were meaningless (525 read "No" purely because roster row 0 was an
  unoccupied structure; the truth is 24).
- **`2000/cluster_features`** — INTENDED_AGGREGATION, benign but silent. `ID.dta` is
  household-level (2,880 rows); the table is `(t, v)` (360 PSUs x 8 HH). The YAML declared
  both `v: psu` and `i: hhid` under `idxvars`, so a household-grain frame was handed to a
  cluster-grain index and 2,520 rows were collapsed. The values were **right** (Region and
  Rural are constant within every PSU — verified, 0/360 violations), so this is a
  declaration/hygiene defect, not a correctness defect.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `_normalize_dataframe_index` | `lsms_library/country.py:4100` | reorders/drops index levels; collapses a non-unique declared index with `groupby().first()` (warns, GH #323) | partially | **not edited** — see §6 |
| `Country.cluster_features` | `lsms_library/country.py:1168-1201` | GH #161: collapses a leaked `i` level with `.first()` / `.mean()` — **silently, no warning** | no | avoided (script now emits `(t, v)` directly) |
| `_ADDITIVE_MEASURE_COLUMNS` | `lsms_library/feature.py:101` | the only *declared* reducer policy that the core actually applies (`food_acquired` -> sum) | yes | not applicable (housing has no additive measure) |
| `aggregation:` key | `CotedIvoire`, `Albania`, `Malawi`, `Niger`, `Senegal`, `Benin`, `Togo`, `Burkina_Faso`, `Guinea-Bissau` `_/data_scheme.yml` | **declared in 9 countries and READ BY ZERO LINES OF CODE** — it appears only in the `_skip` meta-key sets (`country.py:2387`, `diagnostics.py:174`) | n/a | declared *and* enforced in-script (see §4) |
| `run_make_target` | `lsms_library/country.py:2485` | `materialize: make` — runs `_/{table}.py` directly when no Makefile exists | yes | reuse (Kosovo has no `_/Makefile`; `interview_date.py` / `plot_features.py` already rely on this) |
| `df_data_grabber`, `get_dataframe`, `to_parquet`, `format_id` | `lsms_library/local_tools.py` | canonical source read / id formatting / parquet write | yes | reuse |

## §3 Definitions & conventions in force

- Script-path table convention: **absent** from the wave `data_info.yml`, present in
  `_/data_scheme.yml` with `materialize: make`, script at `{wave}/_/{table}.py` writing via
  `to_parquet()`. Precedent in-country: `Kosovo/2000/_/interview_date.py`, `plot_features.py`.
- "NO AGGREGATION IN CORE" — the composition path must return the maximal grain and never
  implicitly reduce it; all reduction is explicit. `SkunkWorks/grain_aggregation_policy.org`.
  The `groupby().first()` in `_normalize_dataframe_index` is a standing violation of that
  contract; this fix removes Kosovo from its blast radius rather than relying on it.
- `housing` is not in canonical `index_info` and not in `Join v from sample > skip_extra`,
  so `v` is joined at API time (`lsms_library/data_info.yml`). Index stays `(t, i)`.
- Occupancy screener / dwelling-type / walls / tenure / water / toilet / electricity code
  lists: `2000/Documentation/hheng.pdf` pp.82-83 (read directly; every label expansion in
  the new script is taken from there, none invented).

## §4 Invariants & assumptions

- **`(hhid, s3a_q0a)` is unique over all 7,047 DWELLING rows** (0 duplicates) — that is the
  true key. Do **not** use `s3a_q00`: it is the 8-char truncated Stata label and collides
  "Damaged house"/"Damaged apartment", giving 596 spurious duplicates.
- **`s3a_q01` is a pure screener**: all 4,168 'No' rows have `q02..q06` entirely NaN
  (verified by construction), and 2,876 of the 2,878 'Yes' rows carry detail.
- **Region/Rural are constant within a PSU** — the invariant that makes the
  `cluster_features` reduction safe. It held (0/360 violations) but was enforced *nowhere*;
  the code comment asserting it was prose. Now **enforced** (raises) in
  `2000/_/cluster_features.py` and guarded by a test.
- **The `aggregation:` block is documentation, not enforcement** (§2). Because nothing in
  the library reads it, the declared policy is enforced **in the build scripts**, and the
  `data_scheme.yml` comments say so explicitly. Declaring it without enforcing it would be
  exactly the "prose is not enforcement" failure this issue keeps reproducing.
- `format_id` must be applied to `hhid`/`psu` in the scripts (it is auto-applied to YAML
  `idxvars`, and the scripts bypass that path); otherwise `i` would not join `sample`.
- Moving a table from YAML to a script silently drops any inline `mapping:` the YAML
  carried. The Tenure label expansion (`Built pe` -> `Built personally`, ...) was carried
  over **verbatim**; a first pass omitted it and regressed 2,389 households' Tenure to the
  truncated Stata labels. Caught by BEFORE/AFTER value diff, not by any test.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| roster -> household reduction | **new (in-script)** | Needs a filter + argmax + sum; inexpressible in YAML, and the core's only declared reducer (`_ADDITIVE_MEASURE_COLUMNS`) is a sum over additive measures, which `Type`/`Tenure` are not. |
| `Rooms` reducer | **sum** | `s3a_q04` is "rooms in [STRUCTURE]" — per-structure. A household occupying two structures has the rooms of both. `first` is provably wrong (it returned 1.0 where the truth is 2.0). |
| `Type`/`Walls`/`Tenure` reducer | **primary structure = unique argmax of `s3a_q02`** ("how large is the part of the [STRUCTURE] your household uses") | The only ordering variable the instrument provides. |
| tie-breaking on that argmax | **refuse — emit `<NA>`** | 3 of the 13 multi-structure households (202, 11501, 12401) tie on max area. `idxmax()` would resolve by ROW ORDER — a positional guess dressed as a rule (the Guyana failure mode). Class-2 (missing) beats class-1 (wrong). `Rooms` (a tie-independent sum) and the AMENITIES columns are unaffected and are retained. |
| households with no occupied structure | **drop, LOUDLY** | 16307 (all 8 rows 'No') and 23801 (blank) have no dwelling. Warn with the ids rather than fabricate. |
| `Electricity` | **re-source from `AMENITIES.dta` `s3b_q07`** | The old mapping pointed at the occupancy screener. `s3b_q07` = "Does your household have access to electricity?", household-level, unique on hhid. |
| `Water` / `Toilet` / `Walls` | **add** | The `data_scheme.yml` / `data_info.yml` comment claiming "DWELLING.dta has no Roof/Floor/Walls/Water/Toilet module" was **FALSE**, and that false comment is *why* nobody found the electricity variable. Walls is `s3a_q03`; Water/Toilet are `s3b_q01`/`s3b_q05`. All label expansions taken from the questionnaire, none invented. |
| `cluster_features` reduction | **explicit + enforced** | Same 360 rows and byte-identical values as before, but now the homogeneity invariant that licenses the reduction is checked, and a violation raises instead of being `first()`-ed away. |

## §6 What this fix does NOT do (the CLASS)

`_normalize_dataframe_index` still collapses a non-unique declared index with
`groupby().first()` for every other country. This fix removes Kosovo from that path but
**does not close the class**. Making the core *refuse* to collapse without a declared
`aggregation:` is the right regression net, but it is a framework change that would
simultaneously break the ~13 other countries still in that path, so it is not this agent's
to land unilaterally (non-negotiable: every other country must stay byte-identical).

Two findings the framework fix will need, surfaced here:

1. **`aggregation:` is inert.** Nine countries declare it; **zero lines of code read it**
   (it lives only in the `_skip` meta-key sets). Any sibling fix-agent told to "declare it
   in `aggregation:`" will produce a *decorative* fix that leaks unless the core is taught
   to consume the key, or the policy is enforced in the build as it is here.
2. **`Country.cluster_features()` (country.py:1180-1189) collapses a leaked `i` level with
   `.first()` and does not even warn** — a second, wholly silent collapse path that the
   GH #323 warning in `_normalize_dataframe_index` does not cover. Any audit that greps for
   the #323 warning will miss every country affected by this one.

## §7 Evidence

Instrument validated on the known positives before any Kosovo number was trusted
(L2-**wave** parquet, full declared index): Mali/2014-15 household_roster = 32,026 dups,
Guyana/1992 housing = 311. Both reproduced exactly.

| | BEFORE | AFTER |
|---|---|---|
| `housing` source rows | 7,047 | 7,047 |
| `housing` duplicate `(t,i)` tuples | **4,182** | **0** |
| `housing()` API rows | 2,865 | 2,863 (2 dropped loudly: no occupied structure) |
| single-occupancy HHs with a **WRONG `Type`** | **524 / 2,850 (18.4%)** | **0 (100%)** |
| HHs published with a type they answered "No" to | **525** | **0** |
| HHs with a **wrong `Electricity`** | **545 / 2,865** | **0** |
| `Rooms` for the 13 multi-structure HHs | `first` (e.g. 1.0) | **sum** (e.g. 2.0), all 13 verified |
| tied-argmax HHs (202, 11501, 12401) | silently guessed | `<NA>` + loud warning |
| `cluster_features` duplicate `(t,v)` tuples | **2,520** | **0** |
| `cluster_features()` API rows / values | 360 | 360, **byte-identical** |
| GH #323 collapse warnings on a cold build | 1 | **0** |

Every other Kosovo table (`sample`, `household_roster`, `individual_education`, `assets`,
`interview_date`, `plot_features`) verified **byte-identical** (`DataFrame.equals`) across a
cold rebuild. No library code was touched, so no other country can be affected.
