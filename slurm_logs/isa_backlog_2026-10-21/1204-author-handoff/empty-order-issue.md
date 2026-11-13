# Uganda crop_production: barren-request column order depends on cache coverage and backend

The GH1204 routing repair exposes an existing constructor ordering rule as new observable Uganda behavior: a genuinely barren 2005-06 request has different column order depending on whether Default can serve a covered country cache or must construct a declared empty frame. Make uses the declared-empty route. All results have zero rows; this finding concerns the returned schema order.

Observed after clean runtime candidate `69bcbd5ff79883606008c67dd36e81580c3f497b` (the final author commit changes only its task ledger), against actual parent `491daf5654b9ee45332bec97e1296ae821ff46d5`.

An ordinary request is:

```python
ll.Country('Uganda').crop_production(waves=['2005-06'], currency=None)
```

The measured cases use separate authenticated private L1 roots and normal API calls, without cache bypass or artificial warm files:

| Route and prior calls | Returned column order |
|---|---|
| Candidate Default, cold crop cache | Declared empty |
| Candidate Default, 2018-19 singleton first | Declared empty |
| Candidate Default, all-wave call first | Native / covered cache |
| Candidate Make, cold or populated | Declared empty |
| Actual491 Default, each of those same population orders | Native / covered cache; the cold and coverage-short paths also have GH1204's wrong raw-cache end state |

Native / covered-cache order:

```text
Quantity, Quantity_sold, Value_sold, Unit_sold, Condition_sold,
harvest_month, intercropped, KgFactor
```

Declared-empty order:

```text
Quantity, Quantity_sold, Value_sold, harvest_month, intercropped,
Unit_sold, Condition_sold, KgFactor
```

The unchanged `lsms_library/country.py:_empty_declared_frame` at line1050 concatenates required scheme columns before optional scheme columns. `absent_subset_frame` at line5004 reaches it when the country has no builder and all requested waves lack the table. A covered Default L2-country read instead keeps the native column order. The complete framework file and both source excerpts are byte-identical at actual491 and the candidate; retiring Uganda's country builder newly exposes the constructor in these calls. The same Uganda observable behavior must therefore not be described as pre-existing at491.

Four full-frame strict comparisons remain FAIL in the author evidence: actual-parent/candidate Default after a supported request then barren, actual-parent/candidate Default cold barren, and candidate Default/Make barren after the two all-wave population orders. Complete empty axes, unused domains/codes/widths and recursive typed attrs pass. Separate per-name field checks prove exact dtype, storage, NA policy and masks, but are narrower diagnostics and do not convert those full strict failures into passes. No frame was reordered or normalized.

Expected behavior: choose and enforce a stable public column-order contract for an empty requested-wave result, across cold, covered-cache and Make paths, while keeping the GH1204 raw-coverage repair. This needs a separate framework/schema-order decision and regression coverage; the country routing repair deliberately makes no such edit.

The nonempty products are preserved: all 21 native table/footer pairs, all nonempty public/raw/Feature/harvest comparisons and five actual-parent Make all-wave controls pass strictly. The held native crop audit footers and grain ledgers are empty on both heads; nonempty audit transport is tested only by the targeted synthetic native-write regressions. This residual makes no science or data-loss claim.

Only aggregate evidence should be attached: `empty-order-residual.json`, the exact source diff and the author validation report. Raw operands, household IDs, full warnings and traces remain private. Root should deduplicate this draft before filing; it has not been sent to GitHub.

Sue
