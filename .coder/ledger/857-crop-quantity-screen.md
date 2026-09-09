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
  warning**, including one who does not care. Measured (best of three, warm
  parquets): 43 ms on Tanzania (14,126 rows), 257 ms on Uganda (133,683 rows —
  the corpus's largest `crop_production`). Same order as Site B's own 196 ms on
  the largest built table (`CLAUDE.md`). An earlier draft of this line guessed
  4 ms and 22 ms; it was wrong and is corrected rather than deleted.
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

- **Uganda's `99999` harvest sentinel (GH #861) is mostly INVISIBLE to this
  screen, and that is the correct behaviour to argue about.** Measured: Uganda's
  `crop_production` holds **3,097** rows with `Quantity == 99999`, of which
  **9** fire. The sentinel saturates most `(t, u, j)` cells it appears in, so in
  those cells 99999 *is* the 90th percentile and the rows are not distributional
  outliers by any honest reading; the nine that fire leaked into 2009-10 Millet /
  `u='Unknown'`, whose p90 is 2.8. A *repeated* sentinel is a different defect
  from a *lone* extreme; #861 owns it, and this country's count moves when it
  lands.
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

**Measured firing counts** — every country whose `crop_production` L2-country
parquet was warm under the configured `data_root()`; `pd.read_parquet` only, no
rebuild, no `Country()` call:

| country | rows | fired at K=100 | audit cost | the rows |
|---|---|---|---|---|
| Tanzania | 14,126 | 14 (2 waves) | 43 ms | **all four rows #857 names**: Coconuts 7,500,000 (`9640-001-99`, plot 1, kg, 2020-21, ×11,029); Timber 4,000,000 (`2010-001`, plot 6, kg, 2020-21, ×173.3); Other Fruits 2,400,000 (`9640-001-99`, plot 1, kg, ×3,189); Ripe Bananas 1,505,400 (`2677-001`, plot 1, kg, ×1,004). Plus Leafy Greens 800,000 in 2019-20 (×944) |
| Uganda | 133,683 | 36 (7 waves) | 257 ms | Sugarcane 360,000 (2018-19, `1083000903`, `u='Unknown'`, ×3,000); Tea 180,000 (2013-14, `H42707-04-01`, kg, ×600); Sugarcane 90,000 (2015-16, `H36701-04-01`, ×300); nine `99999` sentinel rows in 2009-10 (§6) |
| Malawi | 131,379 | 16 (2 waves) | 225 ms | the `50 kg Bag` / `Pail (Large)` family — Beans 48,000 bags (2019-20, `104071840057`, ×8,000) and the year-shaped 2015/2016 rows (§6) |
| Nigeria | 62,844 | 9 (3 waves) | 118 ms | Yam--roots 600,000 kg (2013Q1, `100043`, ×200); Cassava--roots 60,000 `Stalk` (2016Q1, `120100`, ×600) |
| Ethiopia | 85,519 | 10 (3 waves) | 165 ms | Maize 50,000 kg (2015-16, `04040501902092`, ×494) — one household supplies three of the five 2015-16 rows |
| **total** | **427,551** | **85** | | **0.0199% of rows** |

Uganda's parquets were cleared and rebuilt by a concurrent workstream twice
while this was measured; the table above is the reading taken after the final
rebuild, against `data_root()` rather than the legacy `~/.local/share` root.

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
