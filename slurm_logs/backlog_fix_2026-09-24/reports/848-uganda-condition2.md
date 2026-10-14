**SCOPE DEVIATIONS** (files touched that the task did not name; none matches the stop list, nothing left uncommitted):
- `tests/test_crop_kg_factor.py`. The live pinned baseline lives here (rows, harvest_kg rows and sum, the full source dict). The triage's `test_uganda_crop_condition.py:59` is a provenance comment, not an assertion.
- `tests/test_gh842_uganda_harvest_unit.py`. Its 2018-19 season-B assertion of exactly zero `Unknown` units was premised on slot 1 alone. It now derives the allowed count from the raw file and asserts equality.
- `tests/test_uganda_sold_unit.py`. Four pinned sale-row counts moved. Nobody anticipated this file.
- `lsms_library/data_info.yml`, `lsms_library/countries/Uganda/_/data_scheme.yml` (comment only), `.coder/ledger/824-uganda-sold-unit.md`. The sold-unit test's own failure message directs updating every place that quotes the 40,083 count. The ledger got a dated bracket appended, not a rewrite.
- `lsms_library/countries/Uganda/2018-19/_/crop_production.py` and `2019-20/_/crop_production.py`. Docstrings only. The 2019-20 one said "KNOWN GAP: CROP_COLMAPS does not read" slot 2, which became false in a file the country rule tells the next reader to read first.

**1. Commit and branch.** One commit, `cc25a626c`, on `fix/848-uganda-condition2`. Pushed to `origin`; `git ls-remote` shows the same SHA.

**2. Verification.** Both reads were cold in the private root (the wave scripts ran; the after-read took 91.6 s). The data_scheme.yml comment landed after the probe, so the third pytest run forced a second independent cold rebuild, and the pinned numbers below were reproduced by it, not read back from the probe's parquet.

| quantity | before | after |
|---|---|---|
| rows | 130,606 | 133,331 |
| 2018-19 B rows | 7,041 | 8,463 (+1,422 = 1,436 slot-2 records − 14 collisions) |
| 2019-20 B rows | 7,372 | 8,675 (+1,303 = 1,307 − 4) |
| every other (t, season) | | identical |
| harvest_kg rows / sum kg | 78,532 / 28,680,848.60 | 78,571 / 29,093,322.41 |
| harvest_kg 2018-19 / 2019-20 kg | 5,904,803 / 3,668,441 | 6,152,896 / 3,832,822 |
| kg_factor_sources reported / survey_median / metric / none / implausible | 14,050 / 57 / 86,978 / 29,521 / 99 | 14,972 / 555 / 88,190 / 29,614 / 102 |
| KgFactor non-null | 14,149 | 15,074 |
| u='Unknown' 2018-19 B | 0 | 4 (slot-2 records with a measure and no unit code) |
| GrainConflictWarning groups corpus-wide | 11 | 12 (one 2018-19 B `Condition_sold` conflict between the slots) |
| sale rows knowing both units / agreement / largest disagreement cell | 40,083 / 0.9606 / 162 | 41,162 / 0.9614 / 165 |

Collisions were measured from the wave builder itself (old colmap vs new): on the 18 summed groups, `KgFactor` and `harvest_month` are slot 1's, six sold labels were completed from NA, one was blanked. Unit-vs-condition for `s5bq06b_2` / `s5bq06c_2` by vocabulary: 100% in `harvest_units` (69.4% / 64.2% unit-only) vs 100% in `harvest_conditions` (91.1% / 84.8% condition-only).

Test runs, written to files:
```
tests/test_uganda_crop_condition.py test_gh842_uganda_harvest_unit.py test_crop_kg_factor.py test_uganda_sold_unit.py test_uganda_99999.py test_uganda_intercropped.py test_gh637_uganda_plot_inputs_season.py test_quantity_screen.py test_863_transforms.py
224 passed, 8 skipped, 41 warnings in 72.69s (0:01:12)
tests/test_build_transform_hash.py test_feature_canonical_index.py test_label_selection.py test_id_format_id_canonical.py test_gh866_stale_country_parquet_not_laundered.py test_derivations.py test_schema_consistency.py test_panel_ids_persistence.py test_shipped_factors.py
555 passed, 15 skipped, 12 warnings in 390.24s (0:06:30)
```
All skips are "other country not warm here" in the private root. `find lsms_library/countries -name '*.parquet'` is empty.

**3. Cache cost.** Full Uganda re-warm of every table, from `uganda.py` alone; the data_scheme.yml comment adds nothing incremental. Paid only in the private root. `data_info.yml` is read-path and moves no hash.

**4. Surprises.** The triage's 133,683 baseline was two fixes stale (live pin 130,606, after #861). The triage's 2019-20 collision count of 3 is 4 on the served key, because keys pass through the code-to-label maps. A fourth pinned test (`test_uganda_sold_unit`) moved and nobody named it. Nothing contradicts CONTENTS.org; the idiosyncrasy that bears on the task is exactly as recorded there: 2018-19 AGSEC5B mixes naming vintages in one file (slot 1 legacy `a5bq6{b,c,d}`, slot 2 WB `s5bq06{b,c,d}_2`), and slot 2 is variable-labelled "second season of 2017" but is a condition slot by its June/July 2018 harvest dates. The `NullIndexKeyWarning` on `v` (926 rows) is an after-only count; I did not capture it before. Remaining quotes of 130,606 elsewhere are dated measurements in ledgers and the quantity_audit timing table, left as history.

**5. Decisions.** No `kg_factor` on either 2019-20 slot although `s5bq06d_2` exists there (921 non-null): that is #849's scope and the gh842 test pins zero KgFactor outside 2018-19. The `_11`/`_21` part-harvest slots stay unwired and the one-sentence note stands verbatim; only the 2018-19 sold-slot claim was removed from that paragraph. The gh842 sentinel assertion moved from a constant to a raw-derived count rather than to a looser threshold. The #824 "first experiment" is recorded in CONTENTS.org as run: slot 2 adds 18 collision groups and cannot explain the pre-existing 433.