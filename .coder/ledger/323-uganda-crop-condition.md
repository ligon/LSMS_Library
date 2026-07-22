# Prior-Art Ledger — #323 / #637: `condition` index level on Uganda `crop_production`

**Search tier used:** ripgrep + git floor (gitnexus not exercised; no MCP index write attempted).

## §1 Task, restated

Uganda's `crop_production` is a script-path (`materialize: make`) table built by
`lsms_library/countries/Uganda/_/uganda.py::crop_production_for_wave` from the UNPS
post-harvest modules AGSEC5A (season A) / AGSEC5B (season B).  Its declared index in
`Uganda/_/data_scheme.yml` is `(t, i, plot, j, u, season)`.  The UNPS harvest question
6 records, per plot-crop-season, *how much was harvested **and in what condition***
(green / fresh / dry-at-harvest / dried, crossed with the physical form: on the stalk,
with shell/cob, in the cob, in pods, as grain).  The condition is not in the declared
index, so two harvest records that differ **only** by condition — e.g. 240 kg dry
coffee and 100 kg fresh coffee off the same plot — land on the same index tuple and are
summed by the de-duplication block at the end of `crop_production_for_wave`.  Dry weight
is added to fresh weight.  This ledger covers adding `condition` as an index level,
sourced per vintage, on a shared vocabulary.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `crop_production_for_wave` | `Uganda/_/uganda.py:922` | builds one wave's canonical frame from AGSEC5A/5B + AGSEC4A; already loops over a per-season `conditions:` list of column-sets | via wave-script asserts | **extend** — the loop variable is already named `cond`; it just never emitted the condition label |
| `CROP_COLMAPS` | `Uganda/_/uganda.py:1099` | per-wave, per-season source column map | no | **extend** — add a `condition:` key per condition dict |
| `_harmonized_codes(table)` | `Uganda/_/uganda.py:517` | reads a `Code \| Preferred Label` org table from `Uganda/_/categorical_mapping.org` into `{int: str}` | no | **reuse** — same call shape as `_harvest_unit_map()` |
| `harvest_units` org table | `Uganda/_/categorical_mapping.org:779` | the harvest **unit** code scheme (`1=Kg`, `10=Sack (100 kgs)`, …) | no | **model to copy** for a sibling `harvest_conditions` table |
| `_enforce_canonical_spellings` | `lsms_library/country.py:3948` | rewrites variant→canonical values; **line 3962 handles INDEX levels** via `df.rename(index=…, level=col)` | `tests/test_schema_consistency.py` reads the same YAML | **reuse** — this is what makes a `Columns:` declaration reach an index level |
| `_load_canonical_spellings` | `lsms_library/country.py:3921` | inverts `Columns.<table>.<col>.spellings` into `{variant: canonical}` | — | reuse (read-only) |
| `Columns.plot_features.Tenure` | `lsms_library/data_info.yml:159` | the house model for a canonical **vocabulary declaration**: `type: str`, a prose `note:`, and `spellings:` with *empty* variant lists | — | **model to copy** |
| `_normalize_dataframe_index` | `lsms_library/country.py:4099` | **drops any index level not in `data_scheme.yml`'s `index:`**, then collapses duplicates | — | constraint: the `index:` line MUST be updated or the level is silently dropped at API time |
| `harvest_kg` | `lsms_library/transformations.py:1132` | `groupby(['t','i',plot,'j']).sum()` over whatever levels are present | — | **unaffected** — name-based groupby, and it sums, so splitting rows leaves the total identical |
| `_canonical_index_levels` / modal-shape exclusion | `lsms_library/feature.py:58`, `:487` | `Feature()` drops per-country frames whose `tuple(index.names)` differs from the modal shape | — | constraint: `crop_production` is **not** in `index_info`; Uganda is already modal-excluded |
| `Country._/crop_production.py` | `Uganda/_/crop_production.py` | concatenates wave parquets + `id_walk`; index-agnostic | — | no change needed (docstring only) |

## §3 Definitions & conventions in force

- **Canonical schema single source of truth**: `lsms_library/data_info.yml`;
  `tests/test_schema_consistency.py` reads it — never hardcode schema rules in tests
  (`CLAUDE.md`, "Canonical Schema").
- **`spellings` is an inverse dict** (canonical → accepted variants);
  "The canonical values are simply `spellings.keys()`" — `lsms_library/data_info.yml:71-76`.
- **Index-level value *enumerations* are not schema-able**: "Acquisition source `s` is an
  INDEX level, not a column. … Enforced in code (`lsms_library.transformations.S_VALUES`);
  data_info.yml has no schema for index-level value enumerations."
  — `lsms_library/data_info.yml:247-250`.  See §5 for how this and `_enforce_canonical_spellings`
  line 3962 are both true.
- **Countries extend, they do not force-fit**: "Countries may extend this list rather than
  force-fit a local category" — `lsms_library/data_info.yml:170` (`Tenure`), and the same
  stance for `TenureSystem` at `:188`.
- **`crop_production` is deliberately NOT registered in `index_info`**: "the plot-level ag
  features (plot_labor/crop_production/plot_inputs) are NOT here: their per-country index
  NAMES diverge (plot vs plot_id, crop vs j) and need harmonizing before registration --
  tracked separately." — `lsms_library/data_info.yml:32-34`.
- **Never write an unevidenced "no module here" claim** — `CLAUDE.md`, "Adjudicating
  `absent` cells".  Applied here to the `a5aq6b`/`a5aq6c` inversion comment: it was
  checked per wave against Stata metadata rather than trusted.
- **`sane` is not `blessed`** — `CLAUDE.md`, "Coverage Matrix".  No cell is blessed by
  this PR; no human has read the per-condition numbers in analysis.

## §4 Invariants & assumptions

- **The declared `index:` in `data_scheme.yml` is load-bearing.**  `_normalize_dataframe_index`
  (`country.py:4160`) drops undeclared index levels and then collapses duplicates.  Adding
  `condition` to the parquet without adding it to `index:` would be a silent no-op that
  still sums fresh onto dry.
- **The de-dup collapse uses `groupby(level=…)` with pandas' default `dropna=True`**
  (`uganda.py:1084`), so any row with `pd.NA` in an index level is **silently dropped**.
  This already loses 431 rows / 7.1 M native units in 2009-10 (NaN `plot`) — see §6.
  Consequence for this task: `condition` must carry a **sentinel**, never `pd.NA`, exactly
  as `u` already does (`df['u'] = … .where(df['u'].notna(), 'Unknown')`, `uganda.py:1078`).
- **`_harmonized_codes` returns `pd.NA` for a code whose `Preferred Label` is blank or for
  a code absent from the table** (`uganda.py:534`, plus the `.get(c, pd.NA)` at the call
  site).  So off-scheme codes must be sentinel-filled by the caller.
- **`uganda.py` resolves `categorical_mapping.org` relative to CWD** (`../../_/`), i.e. it
  assumes the wave-script working directory.  Any harness must `chdir` into a wave `_/`.
- **`u`, `plot`, `j` and now `condition` are all *object* index levels**; the collapse's
  `groupby` is on level names, so level ORDER in the tuple is not load-bearing for it —
  but it is for anyone slicing positionally.
- **`t` is a string wave label**; `season` is `'A'`/`'B'`.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| condition code → label decode | **reuse** `_harmonized_codes` + a new `harvest_conditions` org table | identical shape to `harvest_units`; one table serves every wave because the integer code scheme is bit-identical across vintages (see `CONTENTS.org`) |
| per-vintage source column | **extend** `CROP_COLMAPS` with a `condition:` key per condition dict | the `conditions:` list already exists for the wide 2019-20 slots |
| canonical cross-country vocabulary | **extend** `data_info.yml` `Columns:` with a `crop_production.condition` entry in the `Tenure` style | `_enforce_canonical_spellings` (`country.py:3962`) *does* reach index levels — verified in code and by an executed unit test, not assumed |
| index-level value **enforcement** (reject an off-vocabulary value) | **deferred** — needs an `S_VALUES`-style constant in `transformations.py`, which is core and out of scope | see §6 |
| `index_info` registration of `crop_production` | **not done** | blocked on cross-country level-NAME harmonisation (`plot` vs `plot_id`, `crop` vs `j`) across 14 countries; `data_info.yml:32-34` says so explicitly.  Uganda is already modal-excluded from `Feature('crop_production')` today, before and after this change. |
| reducer / `aggregation:` YAML | **rejected** | #323 D1: a new index level or nothing; `aggregation:` is dead config |

## §6 Open questions for the human

- **Enforcement follow-up (core).**  `spellings:` *declares* the vocabulary and rewrites
  known variants, but nothing rejects a value outside it.  A follow-up core PR should add
  either (a) a `CONDITION_VALUES` constant in `transformations.py` alongside `S_VALUES`, or
  (b) a generic index-level enumeration check driven from `data_info.yml` — which would
  also let `s` stop being special-cased.  (b) is the better shape; (a) is the precedent.
- **`crop_production` in `index_info`.**  Requires renaming `crop`→`j` (10 countries) and
  `plot`→`plot_id` (or the reverse) before registration, and a decision on whether Uganda's
  `season` and `condition` become canonical levels or get fabricated as NaN for the other
  13.  Separate PR; sized in the report.
- **Pre-existing NaN-key row loss in the collapse** (431 rows / 7.1 M units in 2009-10,
  43 rows / 962.5 in 2011-12).  Rows whose `plot` id is `pd.NA` vanish in
  `groupby(level=…).sum()`.  Not caused by, and not fixed by, this PR — it is a separate
  defect and is unchanged by it.
- **2009-10 sentinel quantities.**  2009-10's `Quantity` sums to 3.18e8 native units,
  ~300× every other wave, because `99999` is used as a missing sentinel and never stripped.
  Separate defect.
- **Second condition slot dropped for 2019-20 season B and 2018-19 season B.**
  `agsec5b.dta` carries `s5bq06a_2/b_2/c_2` labelled "(2019, condition2)" with 1 306
  non-null conditions; `CROP_COLMAPS['2019-20']['B']` reads only slot 1, so those harvest
  records are absent from the table entirely.  Deliberately NOT added here: it *adds mass*,
  which would destroy the "totals unchanged" invariant this PR is verified against.
  Separate issue.

---
### Measured outcome (cold, isolated `LSMS_DATA_DIR`, 2026-07-21)

| wave | dup groups before | after | rows before | rows after | Quantity before = after |
|---|---:|---:|---:|---:|---:|
| 2009-10 | 670 | 181 | 24 997 | 25 485 | 310,664,565.8 |
| 2010-11 | 608 | 81 | 20 442 | 20 970 | 708,149.5 |
| 2011-12 | 653 | 55 | 19 552 | 20 152 | 1,128,560.3 |
| 2013-14 | 589 | 61 | 17 053 | 17 585 | 1,098,713.5 |
| 2015-16 | 764 | 63 | 18 874 | 19 576 | 1,050,367.4 |
| 2018-19 | 0 | 0 | 14 194 | 14 194 | 1,180,031.7 |
| 2019-20 | 370 | 18 | 15 369 | 15 721 | 1,175,600.8 |
| **total** | **3 654** | **459** | **130 481** | **133 683** | **317,005,989.0** |

`Quantity_sold` (18,134,251.4) and `Value_sold` (9,066,365,499.8) totals are
likewise identical before and after.  The 130 481 "before" figure is independently
corroborated by the pre-change `Feature('crop_production')` run.

`Feature('crop_production')` is byte-identical before and after: 250 692 rows, the
same 8 kept countries, the same modal index, and the same 6 excluded frames
(`Ethiopia, EthiopiaRHS, Mali, Nigeria, Tanzania, Uganda`) — Uganda was already
excluded.

Negative control: on the pre-fix tree `tests/test_uganda_crop_condition.py` gives
6 failed / 3 errors / 1 passed, including `expected the dry (240) and fresh (100)
coffee records to stay separate, got quantities [340.0]`.  Post-fix, from a
physically cleared Uganda cache: 10 passed.

### Phase 3 — verification

- `crop_production_for_wave` (condition decode + sentinel) — **OK (anchored on §4)**: uses
  `_harmonized_codes` per §5, sentinel-fills per the `u` precedent at `uganda.py:1078`, so
  no new `pd.NA` index keys and no new silent row loss.
- `harvest_conditions` org table — **OK (anchored on §2)**: same `Code | Preferred Label`
  shape as `harvest_units`; codes are the 20 that carry Stata value labels, nothing invented.
- `data_info.yml Columns.crop_production.condition` — **OK (anchored on §3)**: `Tenure`
  shape, empty variant lists, prose `note:` carrying the "countries may extend" stance.
- `data_scheme.yml index:` — **OK (anchored on §4)**: required, else `_normalize_dataframe_index`
  drops the level.
- No `aggregation:` key added; no reducer; no edit under `lsms_library/*.py` except the
  config file `data_info.yml`.  `transformations.py` untouched.

---

## Phase 4 — post-review corrections (PR #649 adversarial review, 2026-07-21)

Verdict was APPROVE-WITH-NOTES: the data path survived every attack, three
comments were inaccurate, one test gap was real.  All measurements below were
re-derived independently (isolated `LSMS_DATA_DIR` with only `dvc-cache`
symlinked in, `LSMS_COUNTRIES_ROOT` pinned to the worktree and asserted,
Uganda cache physically removed, no `dvc` CLI) — not copied from the review.

### F1 (MEDIUM) — code 99's justification was false by ~100x

Old claim: "only 3 rows in the whole panel and none after 2011-12".
Measured: **381 raw source rows** carry code 99 with a reported measure and
**340 built rows** carry `other_condition`; **219 of the 340 are after
2011-12**.  Per wave (built): 1 / 0 / 120 / 32 / 32 / 32 / 123.

Vintage semantics confirmed from Stata metadata: 99 = `Others` (2011-12),
`Not Applicable` (2018-19, 2019-20), and **absent from the label set** in
2013-14 / 2015-16 (both ship 19 codes, no 99) though 32 rows per wave use it.

**Behaviour unchanged.**  The merge is kept and the *real* justification is
now written down: `t` is an index level so no wave-crossing tuple exists and
nothing is summed; 99 is the scheme's own residual slot in every vintage; and
splitting would need a third value for 2013-14/2015-16 where no wave says
what 99 means.  The alternative (split into `other_condition` /
`not_applicable_condition`) is recorded as a follow-up that moves 340 rows
and needs its own before/after.

### F2 (LOW) — the sentinel rate, not the off-scheme rate

`data_scheme.yml` said "~1%".  Measured sentinel share: **7 994 / 133 683 =
6.0% panel-wide, 23.2% in 2009-10**.  "~1%" is right only for *off-scheme
codes*: 297 = 1.1% of 2009-10, 192 = 0.9% of 2010-11, 492 panel-wide.
`CONTENTS.org` already had the correct figures, so this was the summary
drifting from the analysis; the summary now agrees.

### F3 (LOW) — `Source Label` is not verbatim

Of the 216 (code, wave, season) label pairs in the five labelled waves,
**60 match the org column literally, 156 do not**.  It is the 2018-20 wording
with whitespace collapsed, except **four codes — 24, 32, 42, 99** — which
take the longer 2009-16 phrasing; 99's text ("Others / Not Applicable")
appears in no wave at all.  Also documented: code 45's *Preferred* Label
`dried_grain` is an inference from grid position (every wave writes only
"Dry - grain"), covering 40 411 / 133 683 rows = 30%.

### F4 (MEDIUM) — a mis-wired colmap must fail loudly

Two independent guards, because they catch different mistakes.

1. **`uganda._require` raises `CropColmapError`** when a colmap NAMES a column
   the source lacks.  `None` stays the declared way to say "no such column".
   Measured no-op: exactly one named column failed to resolve panel-wide
   (2010-11's intercrop `flag: 'a4aq3'`, a column that does not exist in that
   file — fixed to `None`, itself a no-op), and a cold rebuild is
   byte-identical afterwards (133 683 rows; Quantity 317,005,989.0;
   Quantity_sold 18,134,251.4; Value_sold 9,066,365,499.8; `intercropped`
   non-null 75 625 on both sides; sorted frames `.equals()` True).
2. **Three tests**, thresholds set from measurement:
   `test_crop_colmap_columns_resolve_in_source` (S3-guarded),
   `test_sentinel_share_bounded_per_wave_season` (ceiling 40%; worst honest
   cell 25.7%), `test_condition_varies_within_every_wave_season` (floor 10
   distinct; measured minimum 18).

**Mutation proof** (Uganda cache physically cleared each run):

| mutation | before | after |
|---|---|---|
| baseline | 10 passed | **13 passed** |
| `condition: 'a5aq6b_TYPO'` (2018-19 A) | 10 passed | **1 failed, 4 passed, 8 errors** |
| `condition: 's5aq06f_1'` (existing wrong column) | 10 passed | **1 failed, 12 passed** |

The second mutation is the informative one: the build succeeds, the resolve
test passes, and the sentinel share is only **33.7%** — *under* the 40%
ceiling — so the variety test (2 distinct conditions vs a floor of 10) is what
catches it.  Neither invariant alone suffices; both are kept.

Also hardened: `test_fresh_and_dry_no_longer_collide`'s `skip`-on-empty is now
an assert, and the `crop_production` fixture re-raises a build failure when S3
credentials are present (still skips without them, so the data-free CI job is
unaffected).

### Found while doing F4, NOT raised by the review

`crop_production.intercropped` is wired to the **seed-use question** in every
wave that populates it (`a4aq3` / `a4aq16` / `s4aq16`, all labelled "did you
use any seed/seedlings?", {1: Yes, 2: No}), not to the cropping-system
question (`a4aq7` "Cropping system" {1: Pure Stand, 2: Inter cropped}, or
`a4aq8` / `s4aq08` "What type of crop stand was on the plot?" {1: Pure Stand,
2: Mixed Stand}).  Agreement between the wired flag and the true crop-stand
question is 48.5–52.5% — a coin flip.  Rewiring moves data, so it is filed as
a Known Issue in `Uganda/_/CONTENTS.org`, not fixed here.
