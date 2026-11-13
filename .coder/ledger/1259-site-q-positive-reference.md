# Prior-Art Ledger — GH #1259 / Site Q positive-only reference

> Per-task ledger. Inherits `.coder/ledger/STANDING.md` (§0) and the Site Q
> ledger `.coder/ledger/857-crop-quantity-screen.md`, whose §2-§5 still hold;
> this file records only what #1259 changes.

**Search tier used:** ripgrep + git (floor). The `gitnexus` MCP server did not
connect this session (CONNECTION_CLOSED); substitutes per CLAUDE.md §"Code
intelligence". Blast radius found by `rg`: `_cell_reference` has one caller
(`audit_quantities`); `audit_quantities` / `check_quantities` are called only
from `Country._finalize_result` (`country.py:4180`), the #857 sweep scripts and
`tests/test_quantity_screen.py`. Risk: LOW (report-only, read path, excluded
from every fingerprint).

## §1 Task, restated

Site Q (GH #857) judges `crop_production.Quantity`, `food_acquired.Quantity`
and `plot_features.Area` against the p90 of a comparison cell. The p90 and the
30-row cell floor counted every finite value, zeros included. PR #1263 serves
Tanzania's recorded "not harvested in the last 12 months" answers as explicit
`Quantity = 0, u = kg`, which put 309 zeros beside Timber's 37 harvests in
2020-21. The reference fell from 23,088 kg to 17 kg, and 16 ordinary harvests
were named. #1259 asks for a reference built from strictly positive values
only. It touches no data and no config.

## §2 What changed

| symbol | change |
|--------|--------|
| `quantity_audit._cell_reference` | masks `values <= 0` to `NaN` once, before the rung loop; `_group_stats` then sees only positive values, so both the quantile and the `QUANTITY_MIN_CELL` count are over measurements. `stat > 0` is kept, but it is now implied. |
| `quantity_audit._group_stats` | unchanged. It is still a faithful finite-value `Series.quantile`, and `test_group_quantile_matches_pandas`, which injects zeros, is untouched. |
| constants (`K`, quantile, floor, `MIN_CELL`), the ladder | unchanged (out of scope). |

## §3 Decision 1: which values count (measured)

The three readings run on the same ladder, K, quantile and floor
(`variants.py`):

| reading | crop (cold, 16 countries) | food (2026-10-19 cache) | plot (2026-10-19 cache) | Tanzania on #1263 |
|---|---|---|---|---|
| A: development (all finite values in both) | 129 | 2,004 | 348 | 30 |
| B: quantile over positives, floor counts all | 120 | 1,997 | 347 | 14 |
| **C: both positive (shipped)** | **120** | **1,998** | **347** | **14** |
| C with the floor at 0 | 120 | 2,000 | 355 | 14 |

B and C differ on one row in the corpus, and that row is the reason for C:
Malawi `food_acquired` 2016-17, `Maize Ufa Mgaiwa` with no unit. Its cell holds
45 zeros and a single 3,200.
- Under B, the 45 zeros carry the cell over the floor, and the row becomes its
  own reference (ratio 1).
- Under C, the cell holds 1 measurement, so the row drops to the wave rung
  (reference 10) and fires at 320×.

"A thin cell cannot manufacture a reference" is the module's own logic, and the
floor has to count measurements to enforce it.

## §4 Decision 2: ladder fall-through (measured)

Measured on the cold build of development `9a0ed5f3c`, `crop_production`:
- **3,394 rows move rung, 0 lose their reference, 0 newly fire.**
- 943 move `(t,u,j)` → `(t,u)`, across 10 crop cells. Each cell now holds
  fewer than 30 harvests: at most 29 positive, with a zero share of 3-90%. The
  3% cell is 29 harvests that a single zero had carried over the floor.
- 2,451 Uganda rows move `(t)` → `(t,u)`. Their unit cell is 93.8% zeros
  (2,582 finite, 161 positive), so its p90 was 0 and the ladder skipped it; it
  now has a scale.
- Every finite row still finds a reference somewhere. The 68,192 unjudged rows
  are exactly the rows with no finite quantity.

## §5 Corpus effect

On `crop_production` (cold), 9 rows leave and none arrive:
- Uganda 2010-11, four `Others specify` rows: the cell is 697 zeros of 775 and
  the reference goes 1.0 (the floor) → 92.7.
- Nigeria 2011Q1, four Maize/`Unknown` rows: 60 zeros + 7 harvests. The row
  drops from the `(t,u,j)` cell (1.2) to the `(t,u)` cell (890).
- Malawi 2016-17 Pigeon Pea 500: 106.4× → 100.0×.

PR #1263's Tanzania: 30 → 14, which is exactly development's 14-row set. All 16
excess rows leave: 2020-21 Timber 10, Citrus 1, Coconuts 1, Ripe Bananas 1, and
2019-20 Timber 3.
- Timber/kg 2020-21: reference 17 → 23,088 (`(t,u,j)`).
- Timber/kg 2019-20: reference 56 → 864. Its 13 harvests are under the floor,
  so it falls to the `(t,kg)` cell, where the issue had estimated 34,100.

Off `crop_production`, on the 2026-10-19 cache:
- `food_acquired`: 6 Ethiopia rows leave (below).
- `plot_features`: 1 Uganda row leaves (100.05× → 99.75×).

## §6 Open questions for the human

- **The Ethiopia `food_acquired` cost.** Six 2011-12 `u='Unknown'` slips
  (500,000 to 8,120,000) stop firing. Their `(t,u)` cell is 220 zeros and 32
  positives, and 6 of the 32 are the slips themselves, more than the 3.1 a p90
  over 32 can absorb. The zeros used to pull the p90 to 1,000; the positive p90
  is 698,000. This is the repeated-sentinel blind spot, no longer hidden by
  zeros. It is not a reason to count zeros, which is the collapse that names a
  2,000 kg Timber harvest. Applying #1259 to `crop_production` only would keep
  the six, but at the price of a table-specific rule. Not done here; flagged.
- **The floor no longer binds on `crop_production`.** It still binds on
  `food_acquired` (2 rows) and `plot_features.Area` (8 rows), where a hectare
  p90 below 1 is legitimate. Whether a one-*native*-unit floor is right for
  hectares predates #1259 and is not addressed here.
- **GhanaLSS `crop_production` has no finite `Quantity` (37,280 rows).** That is
  a Site B matter, recorded here only because the sweep surfaced it.

---
### Phase 3 — verification

- `_cell_reference`: **OK (anchored on 857 §5, "ladder reuse")**. The ladder and
  `_group_stats` are unchanged; only the value mask is new.
- Cache: a full census of development vs this branch moved **0/549**
  L2-country, **0/40** `build_transforms_fingerprint` and **0/2,109** L2-wave
  hashes. The stash probe (#857 pairs) moved **0/17**.
- Read cost (spy inside `_finalize_result`, alternating runs):
  - Uganda: 72-77 ms of about 1,040 ms.
  - Tanzania: 9-13 ms of 123-164 ms.
  - Same under both rules. An interleaved isolated A/B shows no difference
    (GhanaLSS `food_acquired` 2,886 vs 2,894 ms).
- Served data: `check_quantities` returns its input object, and the warm
  Quantity checksums are identical under both rules.
- Evidence (aggregates only):
  `slurm_logs/isa_backlog_2026-10-21/1259-site-q-positive-reference/`.
