SCOPE DEVIATIONS

- No unauthorized edits or external writes. All commits are local with hooks disabled; no pushes, DVC CLI, dependency/lockfile edits, DVC sidecar changes, global cache clears or baseline regeneration.
- Parent explicitly added historical #837 after the six original branches. Its recovered code was measured and rejected under the signed contract; the final branch retains the original commit in history but restores its three source files exactly to development, landing documentation and evidence only.
- Niger worktree moved from `countries/niger` to `countries/niger-followup` because pytest imported the root package named `niger`, shadowing the country module. Ordinary tests pass after the move; no assertion changed.
- Uganda's historical nutrition fixture remains untouched. A proposed two-field patch is committed as an unapplied `.patch`; the scientific hold is owned by the coordinator. Original #967 already contains nine reviewed food-manifest changes; this lane added none.
- Peru sample expectations were narrowly corrected under the workplan's explicit permission: no fabricated weight when the export weight is constant one, and only the three unwired Peru waves excluded from the existing coverage assertion. Other countries remain covered.

All worktrees are clean and frozen. Independent review by the runtime lane approved all six active heads, with Uganda's scientific hold retained. Its review is `control/runtime/COUNTRY_REVIEW.md`. Parent owns integration, publication, combined-suite validation and release.

## Frozen branches and evidence

Paths below are relative to `/local/job39919092/release-work/countries`. Each durable report is under its worktree's `slurm_logs/release_landing_2026-10-14/`.

| Branch | Full SHA | Worktree | Durable report | Status |
|---|---|---|---|---|
| landing/niger | bb51df7e769dbddce367698a827fef5c2584e52a | niger-followup | niger.org | ready |
| landing/nigeria824 | 34928ec68a1703b715dfe7f6373f120703e5b31f | nigeria824 | nigeria824.org | ready |
| landing/nigeriaw5 | b3d035afec96f41e1bb02e59765114bebcc0b199 | nigeriaw5 | nigeriaw5.org | ready |
| landing/perupanama | 9fc6aa2e0efa0360f9d20489fef87f96acd5670b | perupanama | perupanama.org | ready |
| landing/burkina | 45783af9e28276a84a89738fd3f8a9e9dfa43108 | burkina | burkina.org | ready |
| landing/uganda | 059373913186fb7c018e272d6eda07a15f979cdd | uganda | uganda967.org | evidence ready; nutrition direction hold |
| landing/cluster837 | 2877ab3878b336c34a51201ecd221b501de2fc81 | cluster837 | cluster837.org | documented rejection ready |

Each active branch began at its original origin head and merged explicit development `452302adcfdbf3af538ef06375df1ac6c5aaa8a0`; original fixes were preserved. Historical #837 began at `c8bf36d81`, merged that same base, and received an explicit restoring commit after negative-control evidence.

## Validation and substantive findings

**Niger:** 139 targeted tests passed, no skips, 19.63 s. Ten tables rebuilt from empty private L2: sample 20,231; roster 121,956; education 80,445; recent work 125,294; assets 94,478; livestock 30,230; crop 46,376; inputs 42,426; labor 53,041; food 239,877. Assets retains an existing duplicate index; the other nine indices are unique. Follow-up corrects the cold baseline citation and supplies accurate #824/#833 reconciliation drafts. Idiosyncrasy: ECVMA2 identity includes EXTENSION; split-off EXTENSION=2 is another household. Crop sale replication is by harvest LINE. Residuals remain seed-slot collisions, harvest-only household 27010 and unlinked split-off namespace collisions; no assertion was weakened.

**Nigeria #824:** 22 targeted tests passed, no skips, 12.25 s. Cold crop table 62,844 x 10; Unit_sold nonnull counts 3,486 / 3,430 / 0 / 0 / 0 by harvest round. Documentation distinguishes discarded positive-sale rows with both units (93/66) from all positive-sale drops (96/66), keys touching positive sales (125/81), and keys with multiple positive sales (78/43). Fixed misleading dedup comment and added explicit Unit_sold presence to the column-order regression. Idiosyncrasy: W3–W5 sales are household-crop without a plot link; W1–W2 main-buyer extraction omits other buyers. Drafts separately cover dedup loss, other-buyer sales and accurate #824 country progress; existing CI fix is cited.

**Nigeria W5:** all 34 cluster-identity tests passed, no skips, 120.85 s. Empty-L2 sample, cluster and community-price builds: cluster 7,070 rows across ten quarters; each W5 quarter has 911 clusters (518 actual plus 393 household-specific mover keys), 2024Q1 community prices 27,318 rows. Corrected v01 versus v02 claims throughout. Repository v01 lacks geovars, but catalog v02 supplies the geovars and other updates; existing open #978 owns acquisition. Idiosyncrasy: EA=0 movers need household-specific keys, not a shared LGA-0. Existing all-NA W5 GPS regression is temporary pending that acquisition.

**Peru/Panama:** 26 selected tests passed, eight existing weight/refreshment skips and one narrow Peru weight xfail, 22.21 s. All three new source-backed tests executed. Tightened Panama absolute-cent checks with `rel=0`; affected test rerun: one passed, no skips, 17.26 s. Peru cold sample 3,623, clusters 364, roster 19,285; all served roster rows join the padded sample cluster. Source REG01 export weight is exactly one; no domain expansion weights were fabricated. Idiosyncrasy: Peru 1994 household IDs encode fractional HOGAR and padded SEGMENTO. Panama food 570,942 rows; 1997 code-77 rows have no physical Quantity, raw expenditure sum is preserved at 356,079.56, while 46 capital-Value convention rows retain Q=E. Corrected two stray 7.7-row descriptions and construction-time amountless-row exclusion. Residuals: unwired Peru waves/features and Panama's existing null units; original branch's 412 amountless-row removal is disclosed, not enlarged.

**Burkina:** 34 targeted tests passed, no skips, 28.92 s. Independent empty-L2 before/after builds each have 765,816 rows. After precisely four documented label folds, indices and all non-Quantity columns agree per row. Exactly 449,033 zero quantities become NA; every other quantity is identical. P1 has 43,906 missing and 219 genuine clipped zeros; p2–p4 quantities are absent by instrument. Total 2014 expenditure 342,110,515 and quantity 4,208,737.600000024 are invariant. Added the requested money-only heuristic limitation and corrected count prose. Idiosyncrasy: four distinct seven-day recall visits, only the first has the quantity grid; embedded NEL in the crosswalk must not split a physical row. Accurate proposed PR title is in the report.

**Uganda #967:** current-development versus candidate replay independently rebuilds seven outputs and three price modes. All 7,359 previously blank-unit rows are joined to raw source and accounted for, quantities/expenditures preserved, unchanged physical factors and 15 x 113 FCT exact. Kg delta -849.299850578 decomposes into missing/zero -890.512708333, fish +7.741666667, residual labels +33.471191089. Nutrition loses 24 household-wave rows (four previously zero, twenty fabricated nonzero estimates), and 60 surviving vectors change. Energy delta -130,397.304616177. Fresh matrix replay maximum absolute difference is 2.9802322387695312e-08; independent food summation difference 1.1641532182693481e-10. These corrected tolerances are recorded; prior report's zero claim was corrected during independent review.

Uganda targeted checks: 85 passed, 41 explicitly unbuilt-manifest skips, one retained nutrition failure, 20.69 s. Historical nutrition fixture shape/hash: `[23801,15]` / `fc85fc2e2046f3c034d77b1b56da52dbafb9f38bd24f6f63331846677b6eee44`. Current development: same shape / `1131b358677a55ce4b3af6a5e63d8897f1e08381e76ac76b5b978d88ffdaf17e`. Candidate: `[23777,15]` / `12d25f6ec6d069af7d3cff703b5091bc4770944eaafacf2e87bc2f058f450e3a`. Other nutrition manifest fields agree. Explicit CONTENTS hold says “Do not regenerate the baseline until the direction is signed off.” This lane does not independently certify earlier #850 mass assumptions. Parent assigned separate historical replay review. Idiosyncrasy: curated conversion_to_kgs.json is discarded by canonicalization; served factors come from the runtime ladder. Unapplied two-field proposal: `uganda967/nutrition-manifest-proposed.patch`.

**Historical #837:** direct raw Ethiopia joins independently validate unique household keys, all admin conflicts zero, and cluster counts unchanged by a Region/District composite. W1 has 34 mixed-missing GPS groups, with 52 unmatched cover households; its 373 audit losses are not administrative disagreements. Current code already serves all 333 W1 fixes. Recovered hooks would blank exactly 364 Ethiopia GPS cells plus 119 Tanzania administrative cells, changing nothing else. They contradict Tanzania ledger §6.1 and signed GH #871 observed-row selection. Existing Tanzania suite: base 28 passed/26.19 s; WIP 27 passed plus one household-grain failure/26.26 s; restored source 28 passed/25.94 s, no skips. No redundant test added. Original three source edits restored exactly; country docs quote and correct the false Ethiopia inference and retain #837 source/scientific residuals. Idiosyncrasies: four GPS spellings/native household IDs in Ethiopia; Tanzania movers retain original clusters and geocode scheme depends on wave. Raw counts alone do not certify every source coordinate or geographic explanation.

## Cache scope and reproducibility

All Python used `/local/job39919092/venv_img/bin/python`, cwd and PYTHONPATH set to the task worktree, LSMS_COUNTRIES_ROOT set to that tree, package identity asserted. CPU affinity 8–15, BLAS/OMP/MKL threads one, LSMS_BUILD_WORKERS=1; pytest `-n 6 --dist=loadfile` logged directly to files. PYTHONSAFEPATH was not exported into wave scripts. `make api-audit` passed.

Each task owns its private `*-data` root; only its `dvc-cache` symlink points to `/global/scratch/fsa/fc_jevons/ligon/cache/lsms_library/dvc-cache`. No writable parquet links. All recorded cold builds started empty in L2. Separate baseline/restored roots were used where compared. Private frames/logs remain under `control/countries/<task>` and data roots; only aggregate evidence and scripts were committed. No in-tree parquets were introduced.

Original bundles invalidate their respective country outputs, not just newly declared waves. Niger final review measured all 22 declarations; Nigeria W5's change moves full Nigeria L2-country hashes. Uganda original country-module change moves all Uganda tables. No global/cache policy change was introduced. Documentation/test follow-ups add no cache invalidation; Burkina docstring is excluded from executable fingerprinting. Historical #837 final source matches development, so its documentation-only net change adds no cache invalidation.

## Remaining coordinator actions

- Integrate the seven frozen branch dispositions and run the combined suite on the integrated head.
- Resolve the explicit Uganda historical nutrition direction hold before applying the proposed two-field manifest patch.
- Publish the prepared residual issue updates and accurate PR descriptions; no GitHub writes were made by this lane.
- Keep #978 acquisition, #837 scientific/source questions, country residuals and existing warnings visible in release reporting.
