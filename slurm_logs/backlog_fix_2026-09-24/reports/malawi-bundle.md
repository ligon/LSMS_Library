**SCOPE DEVIATIONS**

Files touched that the task did not name by path (none match the stop-list; all committed):
- `lsms_library/countries/Malawi/{2010-11,2013-14,2016-17,2019-20}/_/crop_production.py` — one added kwarg per sale call, needed to pass the S/U column into `_sale_block`.
- `tests/test_malawi_shipped_factors.py` — a new test class appended; no existing assertion changed.
- `lsms_library/countries/Malawi/_/CONTENTS.org` — the brief's country rule asks for this.

**1. Commits and branch**

Branch `fix/malawi-bundle`, pushed to `origin`; `git ls-remote` confirms the head.

| SHA | content |
|---|---|
| `1a040c61f` | code: sale-side S/U wired; Groundnut split; data_scheme JOIN claims corrected; six new tests |
| `13da05b11` | CONTENTS.org entries |

**2. Verification**

- Package identity assert: `/local/job39198807/clone/lsms_library/__init__.py` (ok).
- Gate tests before touching prose: `8 passed, 36 deselected in 6.63s` (the two pinned #854 tests unchanged, plus the six new ones).
- Full selection with the verbatim recipe (Malawi shipped-factors, cassava recode, declared spellings, build-transform hash, null-index guard, derivations, schema consistency): `454 passed, 37 warnings in 397.04s (0:06:37)`.
- Cold rebuild of `crop_production` and `food_acquired` in the private root through the library, no PYTHONSAFEPATH exported: both built; `find lsms_library/countries -name '*.parquet'` is 0.
- Harvest/sale shelled-unshelled basis, attached sales with a positive quantity, harvest rows unchanged (33,420 / 12,616 / 37,170 / 51,872):

| wave | attached before → after | both sides answered S/U: agree / contradict (before) | contradictions attached after | basis-mismatch sales suppressed / MWK |
|---|---|---|---|---|
| 2010-11 | 5,205 → 4,903 | 2,339 / 174 | 0 | 303 / 4,922,307 |
| 2013-14 | 1,774 → 1,715 | 895 / 48 | 0 | 59 / 969,200 |
| 2016-17 | 4,564 → 4,317 | 2,941 / 158 | 0 | 247 / 20,236,770 |
| 2019-20 | 7,131 → 6,628 | 4,504 / 262 | 0 | 503 / 37,981,880 |

Total 642 contradictions before (Groundnut 375, Rice 97, Maize 62, Soyabean 36), plus 465 "not applicable vs S/U" pairs. The one #854 ambiguity (2010-11 household 303041430013, Maize, 50 kg Bag, 2,500 MWK) now attaches to its shelled row; the ambiguity tally is 0 in every wave. The rebuilt parquet reproduces the in-process numbers exactly.
- Groundnut no longer receives vendor-snack rows. `food_acquired` rows with `j == 'Groundnut'` in 2016-17 / 2019-20: 188 / 225 → 0, now served as `Groundnut - Boiled (Vendor)` (188 / 225); 2004-05 / 2010-11 / 2013-14 unchanged at 4,031 / 3,937 / 1,937.
- JOIN claims measured on the rebuilt tables: 24 of 44 crop labels occur as a food label; 19 of 51 priced items occur as a food label and 6 as a crop label. The two comments now say so.

**3. Cache cost**

One Malawi cold re-warm: `malawi.py` and `data_scheme.yml` are in every Malawi table's hash, so all eleven Malawi tables rebuild on next read, not only the two I rebuilt. I rebuilt only `crop_production` and `food_acquired` in the private root; the other nine private-root parquets are stale-on-read. The live cache was not touched.

**4. Surprises and CONTENTS.org**

- The idiosyncrasy that bears on the task, from CONTENTS.org lines 201-217: a quantity column and its unit column are a pair and the sale module has its own; `ag_i02c` was "still unread" and the #854 gate was "defensive, not corrective". That entry was a deferral, not a claim the data contradicts, and this work closes it. Nothing found contradicts CONTENTS.org.
- New idiosyncrasy recorded: Module Q carries no `ag_q02c` before 2016-17, so every 2010-11 / 2013-14 perennial sale is `unknown_condition` on the sale side and attaches on the wildcard only. Where Q does ask, the answer is overwhelmingly "not applicable", which is compatible with Module P's unknown.
- Measured and recorded: 0 household-crop-unit keys mix an unsold all-null Module I/Q line with a real sale, so the zero-valued phantom sales cannot make a real sale ambiguous.
- The 2010-11 build log shows the pre-existing #869 loud-drop warning for one row with crop code 1039; not from this change.
- Stale prose not edited: the 2010-11 and 2013-14 wave-script docstrings still describe Module I as `qty=ag_i02a, value=ag_i03` without the S/U column. A docstring-only edit would move the wave hash again for no data reason; worth folding in at merge.

**5. Decisions taken**

- Compatibility rule: a sale attaches only where its condition equals the harvest row's or either side is `unknown_condition`. `shell_not_applicable` against `shelled`/`unshelled` counts as a contradiction, not a wildcard; that is the 465-row class the task did not anticipate.
- The contradiction class lives in a new `attrs['sale_basis_mismatch']` with its own warning, rather than widening the pinned `sale_suppressed` dict.
- The Aggregate Label for the raw nut in the three early waves moved from `Groundnut, Vendor/Boiled` to `Groundnut, Whole`. The task asked only for the Preferred Label split, but leaving the raw nut in the vendor bucket would have fixed the price comparison and left `labels='Aggregate'` wrong.
- Cassava: no Malawi mapping change. The two-code-scheme instance in the triage is Uganda's; Malawi keeps `Cassava Flour` and `Cassava Tubers` apart, its crop `Cassava` is deliberately neither, and its cross-wave cassava recode was already closed by `_PERENNIAL_RECODE` (#869). Recorded in the data_scheme comment and CONTENTS.org.
- `condition='ag_q02c'` is passed for Module Q in all four waves; an absent column resolves to `unknown_condition` by design.