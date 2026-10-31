# GH1290: recorded agriculture-cover household identity fallback

## §1 Task and status

Public extraction of unchanged product 48c8ef36676eed4e51965f7eac4443ba1924c3dd onto development 899d62e1289a0fb8b5c9fbaa8995e540b452c2be. Two production files and the existing 43-case synthetic file are byte-identical to that reviewed product. The affected Uganda/core/schema paths have no development drift from its 57203c240baca199b8dfd248b61d967a59c63a49 base. No tests, data or native builds are replayed for extraction. Source and composed runtime evidence are distinct; original failures remain failed history.

## §2 Existing machinery

Search tier Git/rg/AST. Reuse mapping.housing_ids pair validation via a renamed AG projection, primary-cover authority, existing housing collision check, ordinary YAML left-merge, one household walk and sample-owned cluster join. Inherit STANDING.md, CLAUDE.md and data_info.yml. Existing tests/test_gh1290_uganda_housing_namespace.py has 43 guarded invented passes; a separate one-case nullable-key control passed once. No new crosswalk, sentinel, reducer, sample member or weight is constructed.

## §3 Definitions and source lineage

Merged 1321 established gsec9.hhid→gsec1.hh/HHID. Later 1358 independently admitted own-wave AG sample membership; membership alone never proves a link. The AG source provides recorded hh/HHID pairs. Actual full-cover overlap is 2,622 keys with 0 disagreements, housing overlap 2,620 rows with 0 disagreements. Observed housing roles are 3,303 primary / 6 AG / 3 unsupported across 3,312 rows. No count is an acceptance constant. #1357 excludes genuinely anonymous households; three unsupported recorded keys remain a different source-provenance residual.

## §4 Invariants

Primary target never overwritten. Missing primary may use only the unique exact same-release AG target; cross-cover disagreement, ambiguous relation or final collision refuses. Payload/order/population are housing-owned. No AG geographic/weight answer enters housing and no fabricated/approximate/prefix/shared-unknown identity is allowed. Unsupported keys remain with warning/missing v. The prior diagnostic-only AG scope was not a permanent prohibition;1358 changed only membership evidence.

## §5 Verification and composition limits

Three mandatory strict typed predicates passed: raw source/native writer CALL; prepared wave/country writer CALL; source-bound prepared composition/public Country. Two RETURN/storage diagnostics passed. Exactly 6 source get_dataframe/init/read/value-label calls completed; all closed inputs/source/children were verified. Independent source oracle does not make current finalizer or cluster transformations independent science oracles. Finalized sample is the authenticated all-wave D1 prerequisite, not rebuilt/current sample-cache acceptance.

Original actual ac34ecb0 failed after strict-return capture on an overbroad all-axis assertion. Saved-output diagnostic later passed full existing typed comparator on Country versus strict return: i/t 0 null, v 3 null outside the current declared/canonical housing union(t,i). Two metadata diagnostic startup failures are preserved, not relabelled passes. No product/strict-rule/key change followed.

Remaining-only finisher 43db383b uses five authenticated immutable prerequisites, new receiving epochs/snapshot and original footer/hash/coverage, with no writer or restamp. First Feature plus fresh warm Country/Feature all pass full axes/dtypes/masks/nonmissing values/order/typed attrs on 3,312 rows. Both L2 bytes/mode/mtime and five original/copy/source/manifest closures pass; children closed. Named Python hooks observe 0 source/native/writer activity, with ordinary tracked-config fingerprint events recorded separately. No historical missing cold-snapshot reconstruction, directory census or universal C-level IO claim. The 64.421-second finisher result is separately reviewed as 92cf30f63df1; source review b37abd0db53b and saved-attribution review 2210285c12ba.

## §6 Residuals

Three unsupported recorded housing keys remain a producer-provenance question. No other-wave / whole #1290 / #1120 / sample / Make / Uganda closure is inferred. Final platform publication is complete at b4371cb3e829b157bd542c7ca05e2570241bca51, task completed, Slurm 40679579 COMPLETED 0:0 in 6:56; no raw IDs, private artifacts, harnesses or execution operators belong in this public patch.
