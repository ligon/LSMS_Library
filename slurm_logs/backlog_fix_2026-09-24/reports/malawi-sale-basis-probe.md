# REPORT -- malawi-sale-basis-probe (Tier B diagnosis, read-only)

## SCOPE DEVIATIONS

None. Files touched: `slurm_logs/backlog_fix_2026-09-24/malawi_sale_basis/` (new: README.org, five probe scripts, `pdftext.py`, `out/` aggregates) and `lsms_library/countries/Malawi/_/CONTENTS.org` (one new section, as the brief's country rule asks). No library code, no tests, no baselines, no dependency files, no DVC. No stop-list match. The per-sale frame (household ids and MWK) is not committed; `build_sales.py` regenerates it in about three minutes. `pypdf` was installed into a private `--target` directory under the job's tmp, not into the venv.

## 1. Commits and branch

Branch `fix/malawi-sale-basis-probe` (on `fix/malawi-bundle` at `13da05b11`), pushed to `origin`; `git ls-remote` confirms head `7398de55f`. `git status --short` is empty.

| SHA | content |
|---|---|
| `1ff7f4f2e` | probe scripts, outputs, README with the proposed derivation; CONTENTS.org section |
| `43cbcd583` | proposal restricted to within-wave reference rungs (the cross-wave `(crop, u)` rung reached one sale a per-wave build cannot see): 18 / 479,300 became 17 / 477,800 MWK; `pdftext.py` reads `PYPDF_DIR`; the attached-count gap stated |
| `7398de55f` | text correction: all 17 price-attached sales are inferred unshelled (the "16 + 1 shelled" in the previous commit's message counted the excluded Pigeon Pea sale) |

## 2. Verification

- Package identity assert: `ok /local/job39200778/clone/lsms_library/__init__.py`.
- `build_sales.py` (exit 0). The frame is gated against the bundle. Basis-mismatch sales and MWK per wave: 303 / 4,922,307; 59 / 969,200; 247 / 20,236,770; 503 / 37,981,880. These match the bundle's own figures exactly. NA-vs-S/U: 127 / 11 / 89 / 238 = 465, matching the CONTENTS.org table exactly. Contradictions: 647 per served sale versus the bundle's 642 per harvest row.
- `separation.py`, `classify.py`, `tobacco_cotton.py`, `proposal_counts.py` (twice, with and without the cross-wave rung): all exit 0; stdout committed under `out/`.
- Pytest with the verbatim recipe on `tests/test_malawi_shipped_factors.py tests/test_malawi_cassava_recode.py tests/test_derivations.py`, log at `$STAGE/pytest.log`:

```
96 passed, 49 warnings in 443.75s (0:07:23)
```

  Zero FAILED or ERROR lines. The harness moved this run to the background on its own after its 120 s default; I waited for the summary line in the foreground before writing anything.
- `find lsms_library/countries -name '*.parquet'` returns 0 files.
- Nothing implemented, so no served number changed. If the proposed derivation were implemented (within-wave rungs only), attached sales 4,903 / 1,715 / 4,317 / 6,628 would become 4,904 / 1,715 / 4,319 / 6,642, the mismatch tallies 303 / 59 / 247 / 503 would become 302 / 59 / 245 / 489, and 477,800 MWK would be attached.

**The measurement, in numbers** (all quoted from `out/`):

- **Price identifies the basis for Groundnut only.** Over 14,791 agreeing sales (sale S/U equal to harvest S/U), a shelled unit sells at 1.64 / 2.16 / 1.67 / 2.19 times an unshelled one per 50 kg bag by wave (Mann-Whitney P(S>U) 0.80 / 0.94 / 0.78 / 0.83), 3.7 times per kilogramme and 4.4 times per large pail (P 0.95 to 0.98). Maize 0.95 (P 0.48), Soyabean 0.94 (0.42), Rice 1.00 (0.42 to 0.54), Pigeon Pea 1.00 to 1.40, Tobacco bale 1.00 (0.47). The same picture holds with the unit dropped and with the wave dropped, on cell-relative log price.
- **On Tobacco and Cotton "shelled" is the "not applicable" price**: 7 of 9 cells with at least 10 sales on each side have P between 0.47 and 0.59. The instrument explains it: both the IHS3 and IHS5 questionnaires say "FOR ALL APPLICABLE CROPS, MAKE SURE TO ASK WHETHER THE REPORTED VALUE IS SHELLED OR UNSHELLED", with no crop list and no ginned or cured option. The harvest side is 100% not-applicable in 2010-11 (Tobacco 1,411 / 0 / 0; Cotton 252 / 0 / 0) and 86 to 97% afterwards.
- **Validation, leave-one-out on 8,693 known-basis sales**: Groundnut recall 79% shelled and 80% unshelled, 87% on the 81% the k=0.5 margin calls unambiguous. Maize 55% (unshelled recall 0%), Rice 14% (unshelled recall 5%), Soyabean 55%, Tobacco 44%. The margin threshold alone is no safeguard: where the medians coincide nearly every sale is "unambiguous" (Soyabean 98% coverage at 55% accuracy). The cell-separation gate, P outside (0.25, 0.75), is the load-bearing one.
- **The 465 class** is 307 sales with a stated S/U on the sale and a not-applicable harvest row (22.63M MWK) plus 158 not-applicable sales against S/U harvest rows (18.02M). With the gates and within-wave rungs, **17 sales / 477,800 MWK attach** (1 / 0 / 2 / 14 by wave; all Groundnut, all inferred unshelled, all at the finest grain). **448 sales / 40.17M MWK stay suppressed**, including every Tobacco (109 / 24.76M) and Cotton (57 / 2.04M) sale. Without the separation gate, 47 / 3.41M would attach, 18 of them Tobacco on a reference that does not separate.
- **The 647 contradictions**: the price sides with the sale's stated basis 379 times against the harvest row's 207. On Groundnut it is 164 : 104 (sale shelled, harvest unshelled) and 83 : 25 (sale unshelled, harvest shelled), against a ceiling near 80% if the sale field were always right and a floor near 20% if the harvest were. The sale field is the more reliable one, but not reliably enough for a rule. Under the same gates 319 / 10.63M MWK would be re-attachable (217 to the sale basis, 102 to the harvest); not proposed.

## 3. Cache cost

These commits: none. CONTENTS.org is excluded from every input hash and `slurm_logs/` is outside the tree. The proposed derivation, if implemented: `derivations.yml` moves nothing and `crop_production` already declares `Derivation` as optional; but the function must run inside `assemble_crop_production` in `malawi.py`, which is in every Malawi table's hash, so one full Malawi cold re-warm of all 11 tables, the bill the bundle already pays once. Not paid here; nothing was rebuilt.

## 4. Surprises, and CONTENTS.org

- The ruling's framing, "a sale with no recorded basis", describes the minority of the class: 158 of 465. The majority have a recorded S/U on the sale and a not-applicable harvest row.
- Price cannot decide a basis where no physical basis exists. The money in the class is Tobacco and Cotton, so the price route recovers 1.2% of it.
- A NULL-plot perennial sale is silently excluded by the bundle's single-plot gate, because `nunique()` of a NULL plot id is 0, and it is counted in neither tally: one sale, 3,600 MWK (2016-17 Mango). Recorded in CONTENTS.org; not fixed.
- The proposal's reference ladder must not pool across waves, since a per-wave wave script sees one wave's Module I/Q. The cross-wave rung is kept in the scripts as a measurement only. The advisor caught this; corrected in the second commit.
- My "attached" column runs 4 / 16 / 1 / 11 above the bundle's. 13 of 2013-14's are sales on harvest rows with an unmapped crop code that the build drops after the attach; the rest are unreconciled and nothing here rests on them.
- Nothing contradicts CONTENTS.org. Its claim that Module Q has no S/U column before 2016-17 is now corroborated on the instrument itself: the IHS3 Module Q page's column header reads `QUANTITY UNIT` while Module I reads `QUANTITY UNIT S/U`, and the S/U code legend printed on the Module Q page is boilerplate. The IHS5 Module Q page reads `QUANTITY UNIT S/U`.
- The GitNexus and aristotle MCP servers did not connect; code reading was done with `grep` (declared substitution; no code was edited).

## 5. Decisions taken

- Counting rule: per served sale (the `_sale_block` key); the bundle counts per harvest row and the verdict per candidate pair (647 / 642 / 661).
- Reference population: agreeing sales only. The contradictions and the 465 class are excluded so they are not tested against a distribution they belong to.
- Coarser grains use cell-relative log price, never raw MWK, because the kwacha moved several-fold across waves and per-kilogramme prices sit beside per-bag prices.
- Leave-one-out validation on the sale's own-basis median and its cell offset.
- Proposal gates: at least 10 agreeing sales per basis in the reference cell; the cell separates at P outside (0.25, 0.75); margin above half the gap (the task's threshold); the inferred basis carried by exactly one harvest row; within-wave rungs only. The two rule changes the evidence bears on, treating not-applicable as a wildcard for no-shell crops and re-adjudicating contradictions by price, are written up with the money attached as decisions for @ligon, not as proposals.
- Non-positive prices (11 raw lines) are excluded from every price analysis and counted in the build summary.