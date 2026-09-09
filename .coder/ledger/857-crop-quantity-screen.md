# Prior-Art Ledger — GH #857 / crop-quantity-screen

> Per-task ledger. Inherits `.coder/ledger/STANDING.md` (§0 baseline); cites it,
> `CLAUDE.md` and `lsms_library/data_info.yml` rather than re-copying.

**Search tier used:** ripgrep + git (floor). The `gitnexus` MCP server did not
connect this session (CONNECTION_CLOSED), so the substitutes named in CLAUDE.md
§"Code intelligence" were used: `rg` across `lsms_library/` for call sites,
`git log`/`git show` for the `harvest_kg` precedent. Declared, not skipped.

## §1 Task, restated

`crop_production` (a *registered*, item-grain table: index `(t, i, plot, j[, u,
…])`, columns `Quantity`, `u`, `Quantity_sold`, `Value_sold`, …) carries raw
harvest quantities with no plausibility screen anywhere. Tanzania 2020-21 alone
ships a 7,500,000 kg coconut row. GH #857 asks for a **report-only** count that
NAMES the offending rows — household, plot, crop, unit, wave — and never clips,
drops or NaNs them, with a threshold read off the corpus's own distribution
rather than a hard-coded ceiling. It is the `Quantity` twin of the
reported-`KgFactor` screen that landed on 2026-09-09 (`0d3a4241`, `cf1e0334`).

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `_screen_reported_factors` | `lsms_library/transformations.py:1714` | rejects implausible REPORTED `KgFactor`; counts under `reported_implausible`; never clips | `tests/test_crop_kg_factor.py` | **shape reused** (count, name, never clip) — not the code: it screens a *factor* against named containers, we screen a *quantity* with no external referent |
| `KG_FACTOR_MAX` / `KG_UNIT_TOLERANCE` / `_KG_FACTOR_YEAR_BAND` | `transformations.py:1690`/`:1701`/`:1708` | the three factor thresholds, each derived from the corpus's own unit tables | same | cited, **not** reused (different column, different unit) |
| `_survey_median_factors` | `transformations.py:1860` | fine→coarse group ladder `(country,t,u,condition)` → `(country,t,u)`, `min_reports` floor | same | **ladder reused** verbatim in shape: never pool across `t` ("a thin module does not borrow a thick one's licence") |
| `null_read_audit.audit_declared_columns` / `check_declared_columns` | `lsms_library/null_read_audit.py:229`/`:450` | Site B: warns when a required declared column is present-and-empty; own lever, own ledger, own accessor | `tests/test_null_read_guard.py` | **structure reused** — sibling module, same five parts |
| `Country._finalize_result` | `lsms_library/country.py:2098`, Site B call at `:3079` | the read-path pipeline; re-runs on every read, warm cache included; in `_EXCLUDED_CALLABLES` | integration surface | **extended** by one call, next to Site B |
| `_build_registry._EXCLUDED_CALLABLES` / `_EXCLUDED_CONSTANTS` | `lsms_library/_build_registry.py:114`/`:190` | keeps pure-reporting code out of the L2 cache fingerprint | `tests/test_null_read_guard.py::test_the_audit_module_is_not_folded_into_any_build_fingerprint` | **extended** with the new module + its ledger |
| `tanzania.plot_features_for_wave` Area clamp | `countries/Tanzania/_/tanzania.py:1046-1051` | drops parcels > 2500 acres to NaN | — | **anti-precedent**: it NULLS. #857 forbids copying either its behaviour or its number |
| `country._audit_index_collapse` / `_grain_strict` | `lsms_library/country.py` | GH #323's warn-by-default + own env lever + `grain_reports()` | grain tests | **structure reused** (three-lever family: grain / read / quantity) |

## §3 Definitions & conventions in force

- **"Count, never clip"** — `transformations.py:1730-1735`: "A rejected value is
  NEVER clipped or rescaled -- inventing a weight is the failure this whole
  design exists to avoid." The same sentence governs here, for the same reason.
- **No allowlist of known-bad cells** — `CLAUDE.md` §"Grain Collapse" and
  `null_read_audit.py`'s module docstring: "an allowlist is the same disease with
  a registry." A `(country, wave, i)` exclusion list — EPAR's remedy at
  `EPAR_UW_Tanzania_NPS_W5.do:578` — is therefore out of bounds.
- **Warn by default, own strict lever** — `null_read_audit._read_strict`
  docstring: "a guard that breaks a working corpus on the day it lands gets
  reverted, and a revert is how the bug class survives"; levers ratchet
  separately, with identical `{1,true,yes}` spelling.
- **`u` sentinels** — `U_UNKNOWN = 'Unknown'` and `_U_SENTINELS`
  (`transformations.py:1988`/`:2000`): "no unit was recorded" is not a unit. A
  cell keyed on such a `u` still has a distribution, so it is not excluded here
  — but see §6 on Uganda's `99999`.
- **Canonical schema** — `Quantity` is declared for `crop_production` in
  `lsms_library/data_info.yml`; this task changes no declaration.

## §4 Invariants & assumptions

- **Site B placement is what makes the signal survive the cache.** Unlike GH
  #323, the evidence here is *in* the parquet, and `_finalize_result` re-runs on
  every read (`country.py:3064-3078`), so no stamp-and-replay is needed.
- **`_finalize_result` is in `_EXCLUDED_CALLABLES`** (`_build_registry.py:127`),
  so a call added there must move **zero** cache hashes. Measured both sides
  (§Phase 3) — not assumed, because a moved hash triggers a rebuild descent and
  `_evict_hashless_wave_caches` would delete script-path wave parquets.
- **`observed=True`, `dropna=False` on every groupby.** `u` and `j` arrive as
  pandas categoricals from `.dta` (`CLAUDE.md` §Gotchas); an unobserved
  `t × u × j` cross-product would blow memory, and dropping NaN keys would
  silently exempt exactly the rows most likely to be defective.
- **`Quantity` may be a nullable masked dtype.** `Int64`/`Float64` raise or
  return `pd.NA` from a bare `>` against a float array — hit twice while
  measuring. Coerce with `pd.to_numeric(...).astype('float64')` first.
- **The crop / plot / unit level names differ by country**: `j` (Tanzania,
  Uganda, Ethiopia) vs `crop` (Malawi, Nigeria); `plot` vs `plot_id`; `u` is an
  index level in Uganda/Malawi/Ethiopia and a *column* in Tanzania/Nigeria. All
  three shapes are live in the corpus and were seen while measuring.
- **Never rebuild.** All measurement is `pd.read_parquet` on warm L2-country
  parquets. (Uganda's were removed by a concurrent process mid-session; see §6.)

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| the plausibility predicate | **new** | nothing in the corpus screens a quantity against its own distribution; `_screen_reported_factors` screens a factor against *named containers*, an external referent `Quantity` has none |
| the group ladder | **reuse** (shape) | `_survey_median_factors`' fine→coarse ladder with a minimum-count floor, unchanged in spirit |
| warn / strict / ledger / accessor | **reuse** (shape) | `null_read_audit`'s five parts, mirrored one-for-one so learning one is learning both |
| where it is called | **extend** | one call in `Country._finalize_result` beside Site B |
| the module | **new file** `lsms_library/quantity_audit.py` | `null_read_audit`'s docstring is entirely about EMPTINESS; folding a plausibility rule in would make that docstring lie. Sibling, cross-referenced, own lever |

### The site decision, and its cost (recorded BEFORE coding)

**Chosen: (a), a read-path audit site — "Site Q" — in a sibling module, called
from `Country._finalize_result`.** Rejected: (b), a query-time diagnostic in
`transformations.py`.

Why (a):
1. **(b) is a warning nobody reads.** `null_read_audit`'s own docstring gives the
   argument — "a warning nobody reads is exactly how #323 survived its first
   fix". A diagnostic an analyst must know to call will be called by the people
   who already suspect the problem.
2. **The defect is served warm.** The 7.5 M kg coconut is *in* the L2 parquet;
   every future read of Tanzania `crop_production` hands it to somebody. Site B's
   placement argument (`country.py:3069-3078`) applies verbatim: `_finalize_result`
   re-runs on every read, so the finding needs no stamp-and-replay.
3. **It costs no cache invalidation** — `_finalize_result` and the audit module's
   entry points are excluded from the build fingerprint. Measured, both sides.
4. The `harvest_kg` precedent is per-call, but `harvest_kg` is itself an
   analyst-called transform; the analogous placement for a *table* is the table's
   read path.

Cost of choosing (a), stated plainly:
- **Every reader of `crop_production` now pays for the audit and sees the
  warning**, including one who does not care. Measured by a spy *inside* the
  read (best of five, warm cache): **83 ms of a 2,157 ms Uganda read (4%)**,
  65 ms of 1,391 ms on Malawi, 9 ms of 643 ms on Tanzania. Two earlier readings
  of this line were wrong and are corrected rather than deleted: a first draft
  *guessed* 4 ms / 22 ms; the shipped d27b7f89 measured the audit in isolation
  at 43 ms / 257 ms and did not quote the read-path delta at all, which the red
  team rightly called out (it differenced two whole reads and got +411 ms on a
  machine that had a concurrent re-warm running — the spy is the number that
  survives load). The implementation was then rewritten (`_group_stats`,
  `_axis`) from 196 ms to 65 ms on the same Uganda parquet.
- **A country with genuinely fat-tailed harvests will warn, and there is no
  allowlist** — by design, and it is a real cost, not a rounding one. The
  measured corpus-wide count (§Phase 3) is what makes it a work queue rather
  than a firehose; if a future country pushes that into the hundreds, the answer
  is to fix the rows or split the rule, not to add a registry of excuses.
- **It is one more thing that can fire during an unrelated build.** Mitigated the
  way Site B is: the audit is wrapped so a failure inside it can never break a
  read.

Lever: **its own, `LSMS_QUANTITY_STRICT`.** `CLAUDE.md` says "a destroyed row and
an empty column are different concerns and must ratchet separately"; a value that
is present, non-null and *impossible* is a third concern. Folding it into
`LSMS_READ_STRICT` would gate that lever's CI adoption — already blocked on
Niger's wave-vs-country `optional:` granularity — on every Malawi bag-of-maize
outlier as well.

## §6 Open questions for the human

- **A repeated sentinel is mostly INVISIBLE to this screen, and the corpus now
  supplies both outcomes.** Uganda's 3,097 `Quantity == 99999` rows: **seven**
  fired, across **two** cells (2009-10 Millet / `u='Unknown'`, p90 2.8, and
  Sorghum / `Plastic Basin (15 lts)`, p90 12) — the two the sentinel did not
  saturate. An earlier draft of this ledger, the module docstring and AGENTS.md
  all said "nine, in one cell"; two of those nine were genuine Sugarcane rows
  (red team, 2026-09-09). GH #861 has since landed and dropped the rows at
  source, so Uganda now fires 25 and none is a sentinel. Niger is the same
  signature unfixed: eleven `999999` rows in 2011-12 that DO fire, because
  eleven rows cannot saturate a wave. Whether this screen sees a sentinel is an
  accident of how often it was keyed — which is why stripping one is a separate
  job with a separate issue.
- **Malawi's `50 kg Bag` rows of 2015 / 2016 / 2018 / 2019** are the
  year-keyed-into-a-quantity-field signature that `_KG_FACTOR_YEAR_BAND`
  documents for factors (`transformations.py:1708`). Observed, recorded, **not**
  screened: adding a second rule is scope creep here, and the ratio rule already
  catches four of them. Worth its own issue.
- **Tanzania's 4,000,000 kg Timber row is the binding constraint on the
  threshold** (ratio 173.3, the corpus's own 99.99th percentile of the ratio).
  Its cell has 37 rows and a genuinely fat tail; any less robust reference
  statistic hides it (§Phase 3). If a future revision loosens `K` past ~170 it
  stops firing — pin it.
- **The floor is a floor, not a cure.** `max(p90, 1.0)` removes a firing whose
  reference was *absurd* (0.6 of an unnamed unit); it does not remove one whose
  reference is merely *thin*. Four Uganda 2010-11 rows still fire at 105×–300×
  a reference of one unit, and the real defect there is that the `(t,u)` rung
  pools every crop sharing a junk unit label. Excluding the `U_UNKNOWN` /
  "Others specify" family from the `(t,u)` rung is the obvious next lever and
  is deliberately NOT taken here — it needs its own measurement.
- **Nothing reports how many rows went unjudged** (red team CONCERN C):
  30,876 corpus-wide have no reference at any rung, 12,891 of them Nigeria's
  (20.5% of that country's table). A country whose crop table is entirely thin
  reports exactly like a clean one. An `n_unjudged` field in each report would
  fix it; out of scope for this pass, and worth a line in the follow-up.
- **Should `food_acquired.Quantity` be screened too?** Deliberately out of scope:
  unmeasured, and 5.26 M rows on GhanaLSS alone. The `_SCREENED_COLUMNS` mapping
  is the extension point.

---
### Phase 3 — verification (filled at task end)

**The rule, and why each number.** A row of `crop_production` is reported when

```
Quantity > QUANTITY_OUTLIER_K * reference(row)
```

where `reference` is the **90th percentile of the row's comparison cell**, the
cell being the finest of `(country, t, u, j)` → `(country, t, u)` →
`(country, t)` that holds at least `QUANTITY_MIN_CELL = 30` non-null quantities
**and** whose 90th percentile is strictly positive. The ladder never pools across
`t`.

- **Why the 90th percentile and not the 99th or the 95th — measured, and the
  binding case is Tanzania's Timber cell** (`t=2020-21`, `u='kg'`, `j='Timber'`,
  n=37): `p99 = 2,632,000` (66% of the very row under test — the statistic is
  eaten by the outlier), `p95 = 70,800` (dragged up by the *second* outlier,
  200,000), `p90 = 23,088` (clean). A percentile at rank `q·(n−1)` is immune to
  the top `(1−q)·(n−1)` values, so at the `MIN_CELL` floor of 30 rows p99 is
  immune to nothing, p95 to 1.45 values, p90 to 2.9. Under p99 the 4,000,000 kg
  Timber row **does not fire at any K** that leaves the corpus quiet; under p90
  it fires at 173×.
- **Why `MIN_CELL = 30`**: a thin cell must not manufacture a reference. 30 is
  also where p90 first buys immunity to ~3 contaminating values.
- **Why `reference > 0`**: a cell whose 90th percentile is zero carries no scale,
  so it cannot support a plausibility judgement. Measured cost of omitting it:
  six Malawi rows of 2–30 units fired at ratio `inf` — a 2 kg cassava row named
  as implausible, which is exactly the false positive that discredits a guard.
- **Why `K = 100`, and the honest part: THERE IS NO GAP.** The null-read module
  could set `_NULL_FRACTION_TRIGGER` "in the gap" because one existed (corpus max
  28.3% vs known-bad ≥40%). Here the ratio distribution is *smooth*: over 277,277
  four-country rows with a reference it runs p90 = 1.0, p99 = 3.7, p99.9 = 20.0,
  p99.99 = 173.7, max = 11,029, with no discontinuity anywhere near the top. So
  `K = 100` is a **stated tolerance, not a discovered boundary**, and it is
  chosen as: two orders of magnitude above what nine in ten comparable rows
  report; five times the corpus's own 99.9th percentile of the ratio; below the
  Timber row's 173.3 so the row #857 names actually fires. The sensitivity is
  smooth and is recorded so the choice is re-checkable: K=50 → 94 rows, K=100 →
  49, K=150 → 34, K=300 → 21 (four countries, 293,868 rows).

**Measured firing counts** — **every** country holding a warm L2-country
`crop_production` parquet, via `pd.read_parquet` only; no rebuild, no
`Country()` call. The first version of `measure_firing.py` iterated a hardcoded
five-name list while its own README claimed it globbed `data_root()`, and five
countries were published as the corpus; it globs now (red team CONCERN 2A).

| country | rows | fired | audit ms | | country | rows | fired | audit ms |
|---|---|---|---|---|---|---|---|---|
| Benin | 9,056 | 2 | 5 | | Mali | 35,060 | 18 | 20 |
| Burkina_Faso | 15,593 | 1 | 6 | | Niger | 47,077 | 28 | 19 |
| CotedIvoire | 22,223 | 16 | 9 | | Nigeria | 62,844 | 9 | 35 |
| Ethiopia | 85,519 | 10 | 44 | | Senegal | 8,428 | 1 | 5 |
| EthiopiaRHS | 17,523 | 2 | 2 | | Tanzania | 14,126 | 14 | 10 |
| GhanaSPS | 25,109 | 7 | 10 | | Togo | 11,565 | 1 | 5 |
| Guinea-Bissau | 10,579 | 1 | 4 | | Uganda | 130,606 | 25 | 77 |
| Malawi | 131,379 | 16 | 61 | | **TOTAL** | **626,687** | **151** | |

**151 of 626,687 rows = 0.0241%.** The headline rows:

- **All four rows #857 names still fire** — Tanzania 2020-21 Coconuts 7,500,000
  (`9640-001-99`, plot 1, kg, ×11,029), Timber 4,000,000 (`2010-001`, plot 6,
  ×173.3 — still the binding constraint on `K` from above), Other Fruits
  2,400,000 (×3,189), Ripe Bananas 1,505,400 (×1,004).
- **Two larger rows in countries nobody had looked at**: Benin 2018-19 Coton
  **12,500,000 kg** (`i=220065`, plot 1_2, ×1,785.7) and CotedIvoire 2018-19
  Cocoa **1,500,000** (`i=600009`, ×750).
- **Niger 2011-12 ships eleven rows of exactly `999999`** (`u='Unknown'`; Mil,
  Sorgho, Niébé, Riz Paddy; ×14,285.7 on the `t` rung) — a second, unfixed
  sentinel family, 11 of Niger's 28 firings and the corpus's largest ratio.
  Recorded in `slurm_logs/gh857_quantity_screen/README.org` for filing.

**Uganda, before and after the two changes that hit it:**

| Uganda `crop_production` | rows | fired |
|---|---|---|
| d27b7f89, pre-#861 | 133,683 | 36 |
| post-#861 (sentinel rows dropped at source), no floor | 130,606 | 29 |
| post-#861, **with** `QUANTITY_REFERENCE_FLOOR` (shipped) | 130,606 | **25** |

The floor removed 4 of the 8 sub-unit-reference firings the red team found;
the surviving four (Rice 300, Ground Nuts 150, Sugarcane 150, Pumpkins 105,
all 2010-11 `u='Others specify'` against a reference of one unit) are a
UNIT-LABEL defect rather than a quantity defect and are left visible. Corpus
effect of the floor alone: 155 → 151.

**Sensitivity, re-measured on all fifteen** (`sensitivity_2026-09-09.txt`):
K=30 → 417, K=50 → 261, K=100 → 151, K=150 → 90, K=300 → 49. Ratio
distribution over the 595,811 rows that have a reference: p90 = 1.0,
p99 = 3.6, p99.9 = 21.0, p99.99 = 233.7, max = 14,285.7 — still **no gap**.

**Anchored verdicts.**

- `quantity_audit.audit_quantities` — **OK (anchored on §5)**: new predicate, no
  in-repo equivalent; ladder shape borrowed from `_survey_median_factors` (§2).
- `quantity_audit.check_quantities` / `quantity_reports` / `QuantityImplausible*`
  — **OK (§5)**: one-for-one mirror of `null_read_audit`'s five parts, which is
  the stated goal, not a reinvention.
- `Country._finalize_result` +1 call — **OK (§4)**: measured to move zero of 12
  table hashes and zero of 5 build fingerprints.
- Threshold constants — **OK (§3)**: report-only, never applied to a value; the
  "count, never clip" sentence at `transformations.py:1730` is honoured
  literally — the audit returns its input frame unchanged, by contract.
