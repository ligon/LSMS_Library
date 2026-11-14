# Red team: GH #1509 / #1068 / #1129 (food unit evidence)

All three jobs have exited.

**Verdict: CHANGES.** The changes needed are in the docs and the PR, not the code.

- **Reviewer.** Claude Opus 5.5 (`claude-opus-5-5`), an independent red team. I did not write the candidate, and I did not read the author's reports.
- **Candidate.** `fix/1509-1068-1129-food-unit-evidence-20261112` @ `0966d62ef3dc3ee60ba9a17f6ab90dd26043c15c`.
- **Base.** development @ `e5fdd2fa831c4d03a247d264be7f339da459e6b0`.
- **Rulings read.** `docs/isa-sweep-20261103:.sucoder/isa-decisions.org`, the 2026-11-12 entries for: GH1509 "Kg-factor evidence rows"; "GH1509 GhanaLSS consequence (amends)"; "GH1509 second (unit-level) non-purchase rung"; "GH1068 + GH1129".

## Bottom line

The code does what the rulings say. Every check I could make on real data agrees with it:
- Only purchases are evidence.
- Every row is still served.
- No fallback row sits in a cell that has purchase evidence.
- `unitvalue` and `units` drop exactly the rows the rulings name, and the counts match.
- Every `waves=` subset is a slice of the all-waves result.
- The hash footprint is GhanaLSS only.

The tests discriminate. All 9 source mutations I made were each caught by 2–40 tests, and the full non-S3 suite passes.

What blocks APPROVE is disclosure. The checked-in docs (AGENTS.md / CLAUDE.md) and the coordinator's expectation miss several measured movements:
- **GhanaSPS moves**, by +3.93% kg with 250k row factors changed. Nothing mentions it.
- **GhanaLSS `nutrition` moves**, by −18.7% Energy.
- **Uganda `nutrition` moves without a hash change**, so warm and cold caches diverge.
- **The ladder order is the author's choice.** The docs credit to the ruling that the purchase `unit` rung outranks the item-level fallback. That order costs GhanaLSS 290k kg.

## Findings

### F1. MEDIUM: GhanaSPS moves under #1509, and no doc or PR text says so

**Where:** `lsms_library/transformations.py:2922` (`priced = _purchase_evidence(df)`); disclosure gap at `AGENTS.md:759-763`.

**Evidence.** In one process on a cold GhanaSPS `food_acquired`, I ran the candidate transform against the base `transformations.py` (loaded as a sibling module); the rest of the package is identical between base and candidate.
- 79,742 priced non-purchase rows (53,840 produced, 25,902 in kind) used to enter the estimate.
- **250,258 of 407,929 rows change kg factor**: 187,132 purchased, 44,895 produced, 18,231 in kind.
  - 17,961 of them change by more than 2x.
  - 18,741 change rung: `item_unit->unit` 9,403 and `unit->item_unit` 9,338.
- `food_quantities(units='kgs')` goes 1,384,290 → 1,438,672 kg, **+3.93%**:

  | wave | change |
  |---|---|
  | 2009-10 | +4.05% |
  | 2013-14 | +4.74% |
  | 2017-18 | +3.43% |

- `food_prices()` (kgvalue) changes Price on 245,793 of 387,440 rows (median ratio 0.972). The row count is unchanged.
- No GhanaSPS row reaches either fallback rung, because every cell has purchases. No GhanaSPS hash moves; this is read-time only.

The coordinator's brief expected "everything else should equal base". The AGENTS.md #1509 paragraph lists the measured countries and omits GhanaSPS.

I also measured the other unmeasured food countries the same way: Tanzania, Nigeria, Mali, Senegal, CotedIvoire, Guatemala, Togo, Benin, Guinea-Bissau, EthiopiaRHS, Panama, Burkina Faso. Plus Ethiopia, Malawi and Niger end-to-end. All 15 have **0 kg-factor rows changed**. Peru has no `food_acquired` wiring and was not measured.

The in-process method is validated end-to-end: `movers.py` reproduces the Country-API kg totals for GhanaLSS (11,007,392 → 9,918,751) and Uganda (1,241,553 → 1,224,670) to the digit.

**Requested.** Add GhanaSPS (+3.93% kg, 250,258 factor rows, kgvalue 245,793 prices) to the AGENTS.md #1509 paragraph and to the PR. State that the other 15 countries measured do not move in kg.

### F2. MEDIUM: the ladder order is an unruled design choice, and AGENTS.md credits it to the ruling

**Where:** `transformations.py:3151-3166` (the `resolved` / `source` ladder: `unit` before `item_unit_nonpurchase`); `AGENTS.md:763` ("…a purchase rung serves at another factor …, which the ruling keeps above both fallbacks").

**Evidence.** The "Fallback rung, disclosed" ruling says a no-purchase cell "may fall back to non-purchase … evidence on a NEW, separately labelled rung". It does not say whether that rung sits above or below the purchase u-pooled `unit` rung.

The candidate puts `unit` first. On GhanaLSS:
- **79,773 produced rows** sit in cells with no purchase in any wave and do have an item-level non-purchase estimate. Each such estimate equals that row's base factor, on all 79,773 rows.
- They are served the purchase `unit` factor instead. It is u-pooled across items, at a median of 0.15x the cell's own estimate.
- Those rows carry **46,889 kg as served, against 337,368 kg** under the item fallback (and on base): −290,479 kg.
- By wave:

  | wave | rows |
  |---|---|
  | 1991-92 | 18,943 |
  | 1998-99 | 13,313 |
  | 2005-06 | 14,990 |
  | 2012-13 | 18,577 |
  | 2016-17 | 13,950 |

That is about 28% of the 1,046,612 kg lost on the `item_unit`/`item_unit_tight -> unit` transitions (318,389 rows). Split by `kg_cell_has_purchase` (job A, `out/analysis3.json`):

| rows | kg | what |
|---|---|---|
| 238,585 | −756,145 | cells *with* purchase evidence that failed an item floor; the ruling sends these to `unit` |
| 79,773 | −290,479 | no-purchase cells with an item-fallback estimate (above) |
| 31 | +11 | no-purchase cells with no item-fallback estimate |

No other measured country has rows in the second group.

The order is defensible as "purchases first", and the synthetic test `test_a_purchase_unit_rung_outranks_the_fallback` pins it. But the user has not ruled on it.

**Requested.** Reword `AGENTS.md:763` so the order is the implementation's choice, not the ruling's. Put the 79,773 rows / −290k kg in the PR, and ask the user to confirm the order.

### F3. MEDIUM (disclosure): `nutrition` moves; Uganda's moves without a hash change

**Where:** `lsms_library/countries/Uganda/_/nutrition.py:43` (`uga.food_quantities()`); `countries/GhanaLSS/_/nutrition.py` (imports `food_quantities_from_acquired`).

**Evidence.** I built both arms cold, and the households are the same in each:

| country | households | values changed | Energy | Protein | Carbohydrate | Fiber |
|---|---|---|---|---|---|---|
| GhanaLSS | 34,798 | Energy on 33,156 | −18.67% | −7.51% | −23.95% | −18.85% |
| Uganda | 23,777 | Energy on 22,848 | −1.07% | +0.40% | | |

- **GhanaLSS.** Its hash moves (22/555 tables), so caches rebuild. The table moves a lot, and the docs disclose only the hash.
- **Uganda.** The script calls the Country API, which `framework_imports_fingerprint` cannot see. No Uganda hash moves, so **a warm `var/nutrition.parquet` keeps serving the base numbers**, while a cold build serves the candidate's.
- **Ethiopia.** `nutrition` is frame-equal; Ethiopia's non-purchase rows carry no Expenditure.

**Requested.** State both in the PR. For Uganda, either say "clear Uganda `nutrition` after landing", or record it as a known stale-cache case under "None of this invalidates a cache" (`AGENTS.md:765`), which is false for Uganda `nutrition`.

### F4. LOW: Uganda's movement is understated in AGENTS.md

**Where:** `AGENTS.md:759` ("Uganda -1.37% (18 rows to `none`)").

**Evidence.** Candidate against base:
- kg 1,241,553 → 1,224,670 (−1.36%).
- **141,884 rows change factor**; 4,850 of them by more than 2x.
- `food_prices()` changes on 141,867 rows, and `kgprice` on 129,194.
- Partition: `item_unit` 59,738 → 55,760, `unit` 105,230 → 109,181, `unit_nonpurchase` 15, `none` +3.

"18 rows" reads as though 18 rows move.

**Requested.** Give the row counts.

### F5. LOW (decision): `unit_nonpurchase` eligibility is per `(j, u)` cell, not per label

**Where:** `transformations.py:3021` (`eligible = ~cell_purchased`) and `:3027`.

**Evidence: Uganda `Sack (unspecified)`.**
- The label has 10 rows, 1 of them a priced purchase (one item).
- The purchase u-pooled arm gives the label no factor.
- The purchased row, and one other row in the same `(j, Sack)` cell, go to `none` (`none_by_reason.cell_has_purchase = 2`).
- The 8 non-purchase rows in other items' Sack cells get the non-purchase u-pooled factor, **2.749 kg/sack** (base 3.556, −23%).
- So within one label, the cell that has purchase evidence gets no kilograms, and the cells without it do.

The other 4 labels on the rung (`139093`, `142090`, `142092`, `181087`) have no purchases at all. They look like raw unit codes leaking into `u`, which is a separate, existing defect.

This is the literal ruling ("same no-purchase-evidence condition"), and it is disclosed in code and docs. But at label grain it breaks the item-level principle that evidence failing a gate never reaches a fallback. The effect is tiny (8 rows, about 6 kg).

**Requested.** State it in the PR, and ask whether label-level eligibility was intended.

### F6. LOW: the ruling named one attrs key; the candidate adds a second

**Where:** `transformations.py:4088`.

**Evidence.** The #1068 ruling and its issue comment say `units` drops are "tallied in `attrs['price_rows_dropped']`". The candidate writes `attrs['quantity_rows_dropped']` for `food_quantities(units='units')`, which is reasonable: it is not a price table.

**Requested.** Name the deviation in the PR.

### F7. LOW: the `currency` and `unknown` buckets count different things

**Where:** `transformations.py:395` (`currency = on_currency & expected`) vs `:412`.

**Evidence.**
- In `unitvalue`, `currency` counts *every* expected row on a currency unit, including rows already unpriceable.
- Serbia: `currency = 3,772`, but only 5 served groups were actually removed. The other 3,767 were `nan` loss on base, where `lost_fraction` was 1.88%; it is now 0.
- GhanaLSS: `currency = 3,551,979` input rows, against 1,107,464 served groups removed.
- `unknown` counts only rows whose Price was servable. That asymmetry is in the docstring, but a reader can take `currency` to mean "rows this ruling removed".

**Requested.** Say so in docs or the PR. Optional: add a `currency_removed` count.

### F7b. LOW: CLAUDE.md quotes gate and wave-spread figures this change moves

**Where:** `AGENTS.md`, the GH #850 "baseline dispersion gate" and "wave provenance" paragraphs.

**Evidence.** These `kgf_attrs` move base → candidate; `kg_factor_declared_none` does not.

| attr | country | base | candidate |
|---|---|---|---|
| `kg_factor_baseline_gate.rows_refused` | Uganda | 13,571 | 2,834 |
| `kg_factor_baseline_gate.rows_refused` | GhanaLSS | 428,168 | 446,696 |
| `kg_factor_wave_spread.rows_over_threshold` | GhanaLSS | 286,141 | 0 |
| `kg_factor_wave_spread.rows_single_wave` | GhanaLSS | 646,539 of 1,064,287 | 538,583 of 538,583 |
| `kg_factor_wave_spread.rows_over_threshold` | Uganda | 24,523 | 22,622 |

GhanaLSS's purchase item rungs are now estimated in a single wave, because before 2016-17 its purchases are almost all `u='Value'`. CLAUDE.md still quotes Uganda 13,571 refused rows.

**Requested.** Update the quoted figures, or note that #1509 moved them.

### F8. NIT

- `feature.py:207-211`'s list of non-recomputed `kg_factor_*` attrs omits `kg_factor_evidence`.
- `tests/test_currency_denominated_units.py:544` `test_ghanalss_unitvalue_and_units_modes_are_unaffected` now asserts that they *are* affected. Rename it.
- `units` mode keeps `u='Value'` rows (GhanaLSS 1,107,464, Uganda 6,429, Serbia 5). That matches the ruling text, which scopes `units` to `Unknown`; the coordinator summary's item 4 is looser than the ruling.

### F9. INFO: cost

On GhanaLSS, base → candidate (job A; 7 per-wave subset calls per mode):

| call | base | candidate |
|---|---|---|
| `food_kg_factors` | 29.1 s | 61.1 s |
| `food_quantities(units='kgs')` | 72 s | 104 s |
| `food_prices()` | 64 s | 93 s |
| each kg-mode `waves=[w]` call | about 47–50 s | about 69–78 s |

This roughly matches the author's own disclosure (28 → 52 s, measured on other nodes).

## Answers to the brief's questions

1. **Purchase definition.**
   - Evidence is a row whose `s` is exactly `'purchased'` (`_purchase_rows`, `transformations.py:2575-2586`, using `isin`). NA, `inkind`, `other` and a misspelled `'Purchased'` are excluded; synthetic tests cover each, and a mutation removing the filter is caught by 40 tests.
   - Caveat: on a direct call, a misspelled purchase becomes **non-purchase** evidence for the fallbacks, because `_nonpurchase_evidence` masks only exact purchases. The Country path cannot reach this: `validate_acquisition_source` refuses NA and non-canonical `s`.
   - The filter is in the estimator only (`food_kg_factors`, line 2922). `conversion_to_kgs`, `_get_kg_factors` and the crop / harvest ladder are unchanged in the diff.
   - Every row is still served. `food_acquired` is frame-equal across arms, and `kgs` row counts are unchanged in every country. `kgvalue` row counts are unchanged too, except Uganda −3: three rows went from `unit` to `none`.
2. **Fallback eligibility.**
   - In code, eligibility is the absence of a *priced* purchase in the cell, over all rows and waves, judged on the purchase frame before any floor.
   - My own groupby over all 7 end-to-end countries finds **0 fallback rows in a cell with a priced purchase**, and 0 in a cell with *any* purchased row.
   - `kg_cell_has_purchase` agrees with my recount on every row, except 4 Niger rows. Those are size-pooled family judgements, by design.
   - Gated-out purchase cells: GhanaLSS has 17,066 rows in priced-purchase cells served `none`, and none of them is on a fallback. The `eligible_if_no_estimate` mutation (letting gated-out cells reach the fallback) is caught by 9 tests.
3. **The unit-level rung.**
   - It is consistent with the purchase side, where a dispersion-refused row falls to the ungated `unit` rung.
   - GhanaLSS: 23,156 of its 28,939 rows are item-fallback refusals (`refused_spread_to_unit_nonpurchase = 23,156`). All 28,939 come back at exactly their base factors, because none of the 7 labels has a priced purchase anywhere, so base's u factor was already non-purchase-only.
   - It is disclosed in the per-row column, the partition and `fallback.unit_rung`.
   - Uganda: see F5.
4. **Disclosure arithmetic.** Verified on all 7 countries:
   - partitions sum to the frame;
   - `priced_rows_read` / `priced_rows_excluded` match my recount by `s`;
   - `fallback.rows_served` and `unit_rung.rows_served` match the partition;
   - `none_by_reason` sums to `none`, and its currency / missing counts match mine.

   **`unitvalue`:** the candidate is frame-equal to base minus the `Value` / `Unknown` rows. The `currency` tally equals my count of expected currency rows (GhanaLSS 3,551,979; Uganda 6,429; Serbia 3,772). The `unknown` tally equals my count of priced `Unknown` rows:

   | GhanaLSS | Uganda | Malawi | Ethiopia | Burkina | Nigeria | Panama |
   |---|---|---|---|---|---|---|
   | 2,269 | 900 | 1,275 | 90 | 178 | 422 | 390 |

   **`units`:** the candidate is frame-equal to base minus the `Unknown` groups. The tally equals my count of `Unknown` rows with a quantity:

   | GhanaLSS | Uganda | Malawi | Ethiopia | Niger |
   |---|---|---|---|---|
   | 2,304 | 905 | 1,967 | 244 | 8 |

   **Feature():** a one-country call keeps `kg_factor_evidence`, `price_rows_dropped` and `quantity_rows_dropped`; a two-country call drops all of them, as it drops `kg_factor_sources`. That matches CLAUDE.md.
5. **#1487 subsets.** Every `waves=[w]` in `kgs` and `kgvalue` is frame-equal to the all-waves slice: 29 waves × 2 modes, over GhanaLSS 7, Uganda 8, Malawi 5, Ethiopia 5, Niger 2, Serbia 1, Cambodia 1. That includes the GhanaLSS waves served by the fallback rungs. Base passes the same check.
6. **`unitvalue`.**
   - GhanaLSS 1987-88, 1988-89 and 1991-92 serve 0 rows. 1,676,514 → 568,130.
   - No served `unitvalue` row has a currency unit, in any measured country, so there is no 1.0 constant.
   - `food_prices(units='unitvalue', waves=['1987-88'])` returns 0 rows with the declared index and `currency = 71,602`.
   - The 104,975 remaining Price==1.0 rows are physical units; the #1129 body excludes them.
   - Keeping `Value` rows in `units` follows the ruling text (see F8). A summed currency amount tagged `u='Value'` is not a ratio.
7. **Data.** Cold, base vs candidate, in separate private roots; table below.
8. **Hash footprint.** 22 of 555 `_table_cache_hash` and 154 of 2,321 `Wave._input_hash` move, all GhanaLSS; 0 of 41 `build_transforms_fingerprint`. The exception is Uganda `nutrition` (F3).
9. **Tests.**
   - Moved pins are each justified by the rulings and agree with my measurements: GhanaLSS 568,130 / 1,694,111 / 560,023 / kgprice 187,096 (1991-92 28,754); Panama 483,661; Burkina `unknown = 178`.
   - On base, the candidate's new evidence file fails 22 tests (2 pass as regression guards), the 2 fallback files fail at collection, and 4 of the modified tests fail.
   - **Mutations.** Nine source mutations, each caught; failing tests in parentheses: `no_purchase_filter` 40, `eligible_all` 7, `eligible_if_no_estimate` 9, `fallback_reads_all` 3, `no_unit_rung` 20, `no_item_fallback` 16, `unitvalue_keeps_value` 8, `unitvalue_keeps_unknown` 4, `units_keeps_unknown` 2.
   - The data-gated pins pass on a fresh cold candidate root (job C: 576 passed, 4 cache-gated skips) and on job A's candidate root (580 passed, 0 skipped).
   - The full non-S3 suite passes: 15,177 passed, 35 skipped, 4 xfailed.
   - Blind spot: no test pins GhanaSPS, nor any `nutrition` total. `tests/test_ghanalss_nutrition.py` passes with Energy −18.7%.

### Q7 table: cold, base → candidate

| country | kg (`kgs`) | % | `food_prices()` | `kgprice` | `unitvalue` | `units` |
|---|---|---|---|---|---|---|
| GhanaLSS | 11,007,392 → 9,918,751 | −9.890 | 560,023 = | 187,096 = | 1,676,514 → 568,130 | 1,695,046 → 1,694,111 |
| Uganda | 1,241,553 → 1,224,670 | −1.360 | 348,181 → 348,178 | 325,051 → 325,048 | 355,510 → 348,181 | 355,544 → 354,639 |
| Cambodia | 57,198 → 56,579 | −1.083 | 35,187 = | n/a | 35,187 = | 35,190 = |
| Serbia | 260,550 → 255,023 | −2.122 | 196,274 = | 196,274 = | 196,279 → 196,274 | 196,279 = |
| Malawi | 2,567,830 = | 0 | 680,372 = | n/a | 681,614 → 680,373 | 971,062 → 969,198 |
| Niger | 411,667 = | 0 | 165,029 = | n/a | 165,029 = | 239,878 → 239,870 |
| Ethiopia | 1,747,435 = | 0 | 263,627 = | n/a | 263,717 → 263,627 | 326,082 → 325,838 |
| GhanaSPS (transform) | 1,384,290 → 1,438,672 | **+3.929** | 387,440 = (245,793 prices change) | n/a | 387,440 = | 393,296 = |

**GhanaLSS kg by wave:**

| wave | base | candidate |
|---|---|---|
| 1991-92 | 282,371 | 235,220 |
| 1998-99 | 392,777 | 293,188 |
| 2005-06 | 815,248 | 663,604 |
| 2012-13 | 1,167,914 | 857,377 |
| 2016-17 | 8,349,082 | 7,869,362 |

**GhanaLSS partition:**

| rung | base | candidate |
|---|---|---|
| `item_unit` | 993,954 | 532,603 |
| `item_unit_tight` | 70,333 | 5,980 |
| `unit` | 605,938 | 894,613 |
| `item_unit_nonpurchase` | — | 208,090 (87 cells) |
| `unit_nonpurchase` | — | 28,939 (89 cells, 7 labels) |
| `none` | 3,571,848 | 3,571,848 |

Both fallback rungs come back at exactly the base factor on every row, to 1e-9.

**GhanaLSS `kg_factor_sources` per wave** (from `food_kg_factors` on the served `food_acquired`; every row of 1987-88 and 1988-89 is `none` in both arms). Base → candidate:

| wave | `item_unit` | `item_unit_tight` | `unit` | `item_unit_nonpurchase` | `unit_nonpurchase` |
|---|---|---|---|---|---|
| 1991-92 | 217,323 → 61,367 | 20,990 → 0 | 28,680 → 68,297 | 118,319 | 19,010 |
| 1998-99 | 72,942 → 37,245 | 13,279 → 0 | 10,513 → 36,696 | 19,170 | 3,623 |
| 2005-06 | 137,396 → 50,993 | 15,772 → 0 | 15,119 → 56,053 | 55,678 | 5,563 |
| 2012-13 | 141,003 → 63,254 | 10,936 → 0 | 18,673 → 91,692 | 14,923 | 743 |
| 2016-17 | 425,290 → 319,744 | 9,356 → 5,980 | 532,953 → 641,875 | 0 | 0 |

`metric` and `none` are unchanged in every wave. Per-wave partitions for every country are in `out/{base,cand}/<C>.json` under `kgf_sources_by_t`.

**GhanaSPS kg by wave:** 2009-10 366,675 → 381,536; 2013-14 349,513 → 366,091; 2017-18 668,102 → 691,044.

**`unitvalue` / `units` drops in countries outside the 6 (transform-level):**

| country | `unitvalue` | `units` |
|---|---|---|
| Nigeria | −422 | −590 |
| EthiopiaRHS | −200 | −112 |
| Panama | −7,185 | −443 |
| Burkina Faso | −178 | −270 |
| Senegal | 0 | −21 |

Every other country measured has no change in these two modes. Kilograms are unchanged in all of them.

## Jobs I ran

All jobs used `sbatch -A co_carleton -p savio4_htc -q carleton_htc4_normal -c 8 --mem=64G -t 3:00:00 --exclude=n0035.savio4`. Each one:
- cloned the mirror at the full SHA;
- mounted the venv with `bin/savio_venv.sh mount`;
- set `PYTHONPATH` to its clone and asserted the import identity and HEAD SHA;
- used a private `LSMS_DATA_DIR` under `/local/job<ID>/rt/root_*`, created empty by that job, with private `XDG_CACHE_HOME`, `TMPDIR`, `MPLCONFIGDIR` and `LSMS_ISSUES_LOG`. In job A each arm reused its one root, in sequence, for the hash probe, the measurement, the `Feature()` check and pytest; jobs B and C used one fresh root per invocation;
- asserted `data_root()` was private and not a forbidden root, via `scripts/guard.py`;
- pinned BLAS threads to 1 and set `LSMS_BUILD_WORKERS=1`;
- ran pytest with `--dist=loadfile` and wrote its output to a file.

There were no `dvc` calls, no shared-cache reads, and no repo or GitHub writes. On n0035 I only read result JSONs with system `python3` and ran `ast.parse` syntax checks on my scripts: no library import, no data builds.

| job | node | what |
|---|---|---|
| 41249259 (A) | n0054 | Base and candidate arms in parallel, each in its own root: the hash probe over all countries; the end-to-end measurement of GhanaLSS, Uganda, Cambodia, Serbia, Malawi, Niger and Ethiopia (`scripts/measure.py`); `Feature()` attrs; the comparison (`scripts/compare.py`); data-gated pytest; the transition split (`scripts/analysis3.py`). |
| 41249260 (B) | n4257 | The candidate's synthetic and related tests; 9 source mutations in a private clone, restored afterwards; the candidate's new and modified tests run on base code; the full non-S3 suite; the in-process base-vs-candidate transform comparison for 14 more countries (`scripts/extras.py`). |
| 41249733 (C) | n0083 | Data-gated pins on a fresh candidate root; per-wave drills for GhanaSPS, Uganda and GhanaLSS (`scripts/movers.py`); the ladder-order and kg-by-transition analysis (`scripts/analysis2.py`). |

All under `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/framework-1509-redteam/`:
- outputs: `out/{base,cand,extras,hash,pytest}/`, `out/compare.json`, `out/analysis2.json`;
- logs: `logs/`;
- scripts: `scripts/`.

## PR must-state items

1. **GhanaSPS moves:** +3.93% kg; 250,258 factor rows (17,961 by more than 2x); kgvalue Price on 245,793 rows; no rows on either fallback rung. 15 other countries measured do not move in kg.
2. **Ladder order:** the purchase `unit` rung outranks `item_unit_nonpurchase` by implementation choice, not by ruling. On GhanaLSS that costs 79,773 rows and −290,479 kg. Ask the user to confirm the order.
3. **`nutrition`:** GhanaLSS changes Energy −18.7%, Protein −7.5% and Carbohydrate −24% on 33k households, and its hash moves. Uganda changes Energy −1.07% with **no hash move**, so warm Uganda `nutrition` caches stay at base until cleared.
4. **Uganda:** −1.36% kg comes from 141,884 rows changing factor, not 18.
5. **Unit rung eligibility** is per `(j, u)` cell, so a label with failed purchase evidence (Uganda `Sack (unspecified)`) serves its no-purchase cells and leaves its purchase cell `none`. Ask whether that was intended.
6. **Attrs naming:** `quantity_rows_dropped`, not `price_rows_dropped`, for `units`.
7. **`currency` bucket semantics** in `unitvalue`, and the resulting drop in `lost_fraction` (Serbia 1.88% → 0).
8. **#1068 drops by country** (`unitvalue` / `units`):

   | country | `unitvalue` | `units` |
   |---|---|---|
   | Uganda | −7,329 | −905 |
   | Malawi | −1,241 | −1,864 |
   | Nigeria | −422 | −590 |
   | Ethiopia | −90 | −244 |
   | Panama | −7,185 | −443 |
   | Burkina Faso | −178 | −270 |
   | EthiopiaRHS | −200 | −112 |
   | Niger | 0 | −8 |
   | Senegal | 0 | −21 |

   GhanaSPS's 13,481 / 13,728 NaN-`u` rows now appear in the tally; base's groupby had already deleted them silently.
9. **Cost** (F9), and the Cambodia allocated-quantity caveat (already in `Cambodia/_/CONTENTS.org`).
10. **Movement, not correction:** no external unit weights were scored.

## Limits: what I did not verify

1. **No external answer key.** No external unit weights were scored, so I do not know whether any movement (GhanaLSS, GhanaSPS, Uganda) is a correction.
2. **GhanaSPS, and the 12 other extra countries, were compared at transform level only.** I ran `food_kg_factors` / `food_*_from_acquired` on the served `food_acquired` with the same `unit_kg` / `no_kg`, but not through the Country API. So `_finalize_result` post-steps and `waves=` subsets were not exercised for them.
3. **Peru** has no `food_acquired` wiring and was not measured.
4. **Uganda's stale `nutrition`** was inferred from the hash probe plus the cold-vs-cold difference. I did not reproduce it on a warm root.
5. **Data-gated pins:** job C's fresh root skipped 4 cache-gated tests (GhanaLSS / Malawi `food_acquired` not cached). Job A re-ran the same data-gated files on its job-fresh candidate root after the measurement had built GhanaLSS and Malawi: **580 passed, 0 skipped**, including the #1042 egg pins. Those 4 tests are therefore covered, though on a root that the measurement had already warmed.
6. **EHCVM size-pooled eligibility** (4 Niger rows) was checked only by agreement with the code's own family judgement.
7. **Mutations** covered the synthetic tests only. No mutation was run against the data-gated tests.
