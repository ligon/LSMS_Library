# SCOPE DEVIATIONS

None from the approved branch follow-ups. `CLAUDE.md` is a symlink: labels documentation edits are recorded in `AGENTS.md`. The kinship follow-up also corrects the same false sibling-label spelling in Niger/Senegal `CONTENTS.org`, as reported to the coordinator before editing, consistent with the country-documentation instruction. No stop-list path edited, no dependency/lock/DVC/baseline changes, no push or GitHub write, no shared-cache clear. Parent additionally assigned historical #600 and an independent read-only #980 follow-up review.

# Ready branches

All six branches merge fixed development `452302adcfdbf3af538ef06375df1ac6c5aaa8a0` and have clean worktrees. Hooks were disabled per commit to prevent automatic mirror publication.

| branch | SHA | follow-up |
|---|---|---|
| landing/framework-small-2 | 7e7662fa9c265bcb20ea4cb2ce4edccb2babebb2 | independent review, merge only |
| landing/831-followup | c38803d3cab5a647224b93b88593839d88e92cdb | no-git guard tested before skip; drifting count removed |
| landing/ghana-docs-encoding | d831e5cb00269b22f3359aa06d01d081ed60f2c7 | 8/9 Guinea-Bissau, all 6 Togo; GhanaSPS interval endpoints explicit |
| landing/labels-relabel-787 | 8fa4d5daf0b7b9fdf95ecceacb9a1dedce51f24f | strict/warning/attrs, filtered rename map, numeric refusal documented |
| landing/grader-scale-830 | 1641e1fff3286bd75979849381af17ef4c9e167e | approved policy/census in own ledger commit; per-neighbour forms; blindspots |
| landing/kinship-974 | 8bb3d5aeb5d328e0108450d2bc36470fd003c4d0 | merge keeps #848 counts/comment and #931 ladder pin; accurate accents |

# Verification

Venv `/local/job39919092/venv_img/bin/python`; taskset CPUs16-23; OPENBLAS/OMP/MKL=1, LSMS_BUILD_WORKERS=1; PYTHONPATH and LSMS_COUNTRIES_ROOT explicitly point at each worktree; import identity asserted. Each pytest command uses `-n6 --dist=loadfile -q` and logs to this control directory. Tests ran sequentially, with only read-only single-process hash probes overlapping.

- framework-small-2: `test_dvc_caching`, `test_countries_root_override`, `test_gh905_vectorised_min_count_sum`, `test_categorical_mapping_resolution`, `test_catalog`, `test_gh909_panel_spine_coverage`: **106 passed, 1 skipped**, 75.71s. Existing LSMS_EXPECTED_ROOT-dependent subprocess probe skipped. Both fixture root caches and catalog cache clear on teardown; #915 ValueError remains tested; <=4 ULP remains asserted.
- 831: `test_gh831_cache_clear_intree_sources`, `test_cli`, `test_gh803_intree_parquet_never_read`, `test_gh831_no_runtime_writes_into_config_tree`, `test_build_transform_hash`: **63 passed**, 42.72s.
- Ghana docs: `test_no_c1_controls_in_curated_labels`, `test_categorical_mapping_resolution`, `test_schema_consistency`: **313 passed, 1 skipped**, 12.19s (same existing LSMS_EXPECTED_ROOT probe).
- Labels: `test_food_labels`, `test_label_selection`, `test_api_discoverability`, `test_feature`: **116 passed, 4 skipped**, 85.15s. Existing data/parameter gates; diagnostic, strict-mode, malformed-table, numeric-target and conservation tests pass.
- Grader: `test_coverage_matrix`, `test_build_transform_hash`: **98 passed**, 82.86s. Includes cold Uganda monetary positive control and new mixed-neighbour form regression; no test skip. Existing raw quantity warnings are retained.
- Kinship: `test_kinship_yml_hygiene`, `test_kinship_other_relative`, `test_crop_kg_factor::test_crop_ladder_is_blind_to_declared_unit_kg`: **21 passed**, 9.28s. #848 current counts/comments preserved verbatim in merge.
- Every new patch passes `git diff --check`. GitNexus unavailable; rg call-site searches and diff/test review substituted. Grader diagnostics are consumed only by coverage details and the audit harness; both were updated. Original algorithms, tolerance, support floor and tier assignment are unchanged.

# Cache scopes

Private roots are `/local/job39919092/release-work/framework/<key>-cache`; their only symlink is `dvc-cache` to `/global/scratch/fsa/fc_jevons/ligon/cache/lsms_library/dvc-cache`. No writable L2 symlinks. No global cache clear. Hash outputs and rerunnable hash_probe.py are in this directory.

Fresh measurements against fixed development using every Country.data_scheme table:

- Ghana encoding whole branch: GhanaLSS **22/22** hashes move, GhanaSPS0/13, Togo0/22, Guinea-Bissau0/21. Previous review's 21 was its older declaration count. New prose follow-up alone is hash-neutral.
- Kinship final whole branch: Niger **23/23**, Senegal **22/22**, Uganda0/28. The new mapping.py docstring text is hashed; the underlying kinship deletion was hash-neutral. Do not describe the final branch as no-cache-cost.
- framework-small-2 and 831 original independent verdicts measured Panama5/5, other probed countries unchanged. New fixture/doc-only 831 followup adds no hash movement.
- Labels and grader: read-path diagnostics/documentation; existing independent verdicts and build-registry tests establish no rebuild. No build-function edits in followups.

# Residual drafts for coordinator filing

## #831: sidecar-less in-tree read policy

The write side is fixed by #917 and this branch makes `cache clear` name both sidecar-backed in-tree copies and sidecar-less sources. `local_tools.get_dataframe` still serves a sidecar-less file inside countries_root before trying DVC, intended for newly prepared acquisitions. Decide whether to warn once per read naming the path, refuse unless an explicit LSMS_ALLOW_INTREE_SOURCE opt-in is set, or retain the behavior and rely on CLI reporting. The CLI change does not alter which bytes are served and must not close this policy half implicitly.

## #787: unfinished label semantics

The landed increment makes Preferred Label a no-op, refuses numeric targets and reports non-null unrenamed j values in warnings/attrs, with LSMS_LABELS_STRICT. Remaining work: (1) adjudicate Preferred Label -> multiple distinct targets (currently last-row-wins; Niger153, Senegal134, Burkina72 conflicts in review, plus Uganda Cassava flour); (2) curate each country's intended variants and missing mappings; (3) carry `labels_unmatched` through Feature's multi-country concat, using its existing attrs-carrier mechanism (warnings already survive); (4) correct now-false numeric-target examples in Guatemala/_/categorical_mapping.org:75 and Serbia/_/categorical_mapping.org:68 at those countries' next planned rewarm. Those hashed org inputs were intentionally not changed here. Preferred Label now preserves input row order instead of groupby-resorting; values are conserved.

## #830: warning-only first increment and remaining detection gaps

Maintainer-approved policy: fixed fivefold tolerance, warning-only, unchanged support floor and ISO-code boundary. The independent review's read-only census found four records across three of 87 warm monetary country-tables: GhanaLSS community_prices.Price 1998-99 (5.56x), GhanaLSS food_acquired.Price 1991-92 (0.16x), GhanaSPS crop_production.Value_sold 2009-10 (0.167x) and 2017-18 (6.0x). Review attributes GhanaLSS to inflation across seven years; GhanaSPS remains unadjudicated. This is cited review evidence, not a new release-worker census. Follow up persistent breaks (A,B vs C,D), unconfigured redenominations, community_prices comparison grain and gap/CPI-aware thresholds. `sane` still does not establish item coverage or correct unit labels. New diagnostics carry each neighbour's actual form and use form=mixed when they differ.

## GhanaSPS scale finding: season revenue vs buyer-channel revenue

Investigate crop_production.Value_sold across 2009-10 and 2017-18: reviewed per-crop medians vary about 3–7.5x across eight years, with a residual above the review's CPI comparison still unadjudicated. CONTENTS.org explicitly documents different instruments: season A83/A124 cedis + pesewas/100 in 2009-10; no plotid-linked sales in 2013-14 (NaN by design); nine buyer quantity×price channels in 2017-18. Compare matched concepts/populations before deciding whether any correction is justified. Do not treat the scale warning itself as proof of corruption.

## #974 / #945 / #931

#974 removes exactly adjudicated un-emitted keys and duplicate identical lines; reachable variant folding remains #945's canonical-form decision. Either close #974 with that fold explicitly rehomed to #945 or narrow its body accordingly. #945 alternatives remain normalizing lookup plus canonical keys (would move live Peru/Guatemala spellings) versus retaining emitted spellings. #931 crop-vs-food `u_kg` asymmetry remains deliberate pending a decision; any crop ladder declaration support must re-express Ethiopia's external quintal check in the same change, so it does not become a tautology.

## #905 and framework residuals

Use the archived `slurm_logs/release_landing_2026-10-14/framework-small-2-report.md` full #905 reducer-site list. No broad reducer sweep was attempted. The original full-suite warm pass does not resolve cold Albania concurrent sample-build behavior; preserve it as a separate race investigation. The #703 credentialed CI cache-location question likewise remains outside this fixture repair.

## Ghana docs residuals

#926 still covers load-bearing C1 key cells in Mali/Niger/Burkina/Uganda, ISO-8859 units.org files, and the transcoding policy for re-derivation. #812's unwired strata in Senegal/Mali/Niger/Burkina should remain a separately tracked allocation-analysis gap. GhanaSPS #880's MonthsAway docs now agree with the previously verified implementation.

# Country idiosyncrasies and contradictions

- Panama: legacy unittable.csv is a tracked source nothing reads by path; placing it under _/ makes the no-git positional exclusion truthful. Spanish quintal differs from metric quintal; no unit changes made.
- GhanaSPS: wave1 MonthsAway missingness reflects EA recording convention (17 EAs strictly between25% and75%,20 inclusive); three crop sales instruments differ and wave2 has no plotid-linked sales. No GhanaSPS wave contains two PP/PH rounds.
- Guinea-Bissau: SAB is urban-only and receives92 clusters; eight other regions receive42–50. Original new prose said 'Seven of nine' despite its own eight-row table, corrected.
- Togo: seasonal halves share one library wave; all six regional allocations lie72–98, contradicting new 'Five of six' wording, corrected.
- GhanaLSS: cp1252 punctuation in format118 labels required curated ASCII targets; whole-country config hashes move. Existing verified repair retained.
- Niger/Senegal: roster cp1252 repair re-title-cases Frère, Sœur; CONTENTS falsely said 'Served label is now =Frere, Soeur= with the ligature restored'. Corrected that spelling, naming the old contradiction in place. Their food mojibake key crosswalks remain load-bearing and untouched.

# Independent #980 follow-up review

APPROVE (read-only). Read original verdict, production conftest hook, three changed tests and new test_conftest_net.py. Pytester copies actual conftest into a temp root without checkout credentials; setup/call warning tests, -W error, collection-time skip and true-error preservation are discriminatory. Missing-file injection targets only absent.csv and delegates all other reads. Locality sentinel now verifies forwarding and public warning; precise housing rebuild marker is justified. Parent owns final execution evidence/publication.

# Worktrees

`/local/job39919092/release-work/framework/{framework-small-2,831-followup,ghana-docs-encoding,labels-relabel-787,grader-scale-830,kinship-974}`.

Historical #600 follow-up is in progress in the sibling `600-provenance` worktree; separate addendum will report its disposition.

# Historical #600 addendum (2026-10-14)

SCOPE DEVIATIONS: none; separately authorized by parent after six branches. Port changed current AGENTS.md instead of resurrecting the old regular CLAUDE.md. It preserved current CLAUDE.md symlink, repository-search logic, data_root import, and acquisition/read protections. No files were written into main.

- Branch `landing/600-provenance`, worktree `/local/job39919092/release-work/framework/600-provenance`.
- Ledger commit `9bcd3c55d`; code `5d4925a323af99ad5b766cfc5e0510d7fb655cf8`.
- Original `8afe909ae` is genuinely unfinished, not a squash artifact: current development has neither catalog_relations nor list-valued provenance. GitHub #600 remains open; read its body without posting.
- Original 2026-07-12 design ported onto fixed development. Multiple ids per SOURCE.org, separate CATALOG_COVERS, evidenced same-study vs derived relations, conservative unknown logical waves, preserved scalar catalog_id/boolean local API. No blind merge of obsolete CLAUDE.
- Independent review added two compatibility corrections: new dataclass fields follow all pre-existing fields (old positional idno/title constructors remain valid); aliases normalize recorded holdings as well as queried IDs, merging rather than losing wave lists. Both have discriminatory regressions.
- Initial targeted port:122 passed/41.00s. Final:`test_wave_provenance.py test_repository_discovery.py test_capability.py test_build_transform_hash.py -n6 --dist=loadfile -q`:**124 passed/41.35s**, log600-final-tests.log.
- Fixed-catalog replay against actual current SOURCE.org (no network): all seven rows report `no` on development. Port yields Malawi2939/3819=`yes` with respective2016-17/2019-20 folders; Tanzania76/1050/2252/2862=`covered` under2008-15; Nigeria5835=`derived` from four directories. latter five keep local=False. Stored600-base.json/600-final.json and rerunnable600_probe.py.
- Exact same probes find **0/103 country-table hashes changed**: Malawi0/26,Tanzania0/25,Nigeria0/24,Uganda0/28; all Country.waves identical. Private600-provenance-cache shares only the DVC blob symlink. No feature builds needed to prove this discovery-only patch.
- Read Malawi CONTENTS.org: Panel/Cross_Sectional halves and distinct identifier conventions are intentional. Read Tanzania CONTENTS.org opening design and WAITING#114: UPHI back-casts descendants, r_hhid names interviewed households, their grain inconsistency remains unresolved. The provenance relation records which release supplies these rows and does not adjudicate grain.
- Dated file/API counts in the restored ledger and three SOURCE.org records remain the original author's July12 evidence. A new live WB corpus census was NOT performed; the new replay verifies current software behavior against those fixed catalog identities.

Residual draft for #600/backfill policy: Successful re-stamps now preserve extra ids, covers, notes and validation. If a recorded primary id is absent from the caller-supplied catalog, existing backfill.resolve behavior instead produces UNKNOWN and replaces metadata; that pre-existing failure branch can still discard hand-recorded facts. Decide whether an absent/filtered catalog response should leave the existing record intact, or produce a review artifact separate from SOURCE.org. No broad backfill was run; the patch does not decide that policy. Avoid claiming preservation under every failure outcome or closing that residual without disposition.

Additional validation of fixed grader head: direct `_scale_findings` smoke on a synthetic mixed-form frame emits one warning finding, retains per-neighbour forms, and round-trips through JSON. This read-only smoke changed no ready ref.

Final hygiene: all seven owned worktrees clean, zero in-tree parquets; main tracked tree clean. Ready six refs remained fixed after coordinator acceptance.
