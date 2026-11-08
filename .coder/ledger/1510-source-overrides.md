# GH1510 — per-file source override dependencies

Search tier: git/rg/AST floor. Base c964c6c4f25945dc14636fd094893b16b12180b7.

## §1 Task
Extend existing declaration traversal so the paths actually consumed by per-file override list members contribute to Wave and Country source hashes. Metadata-only work; no source builds or scientific rule changes.

## §2 Existing machinery
- local_tools._collect_file_paths_from_block:807: shared recursive collector, consumed by Wave._input_hash and _warm_dvc_cache_for_feature. Extend it, not a second extractor.
- Wave.column_mapping:1630: list mapping entry consumes next(iter(entry.items())); only the first path is used. Multiple list entries are supported; multiple paths in one entry are not all consumed.
- Wave._input_hash:1434 and Country._table_cache_hash:4500: existing declaration→source_fingerprint→hash chain; reuse. tests/test_cache_hash_invalidation.py covers sidecar and full hash primitives.
- local_tools.source_fingerprint:3043: prefers sidecar metadata; never need real raw files here.
- local_tools._warm_dvc_cache_for_feature:685: secondary collector caller, existing `_`-relative resolver and best-effort prewarming; test with fake remote/cache only, preserve resolver semantics.
- Existing1260 Malawi panel-geo and1292 Nigeria PH ledgers explain real override semantics. Inherit STANDING.md/CLAUDE.md/caching guide rather than duplicate IO/schema rules.

## §3 Definitions
The collector docstring promises 'every file: value' but currently yields only string list members. Burkina CONTENTS 'A gap in the hash, not fixed here' explicitly documents GH1510 (28 entries/9 files at that checkpoint). This task resolves that mechanical contradiction without altering extraction.

Country anchors: Burkina2014 has four passages; sample appends389 food-source households without inventing welfare weights. Malawi combines IHS/IHPS and panel current geography differs from its stayer-grounded baseline EA coordinates;3819 release is not replaceable by2939 on assumption. Nigeria logical PP/PH visits share folders; its WAITING recall record quotes both '12 months reference period' and 'PAST 6 MONTHS'. Neither wording authorizes a recall change. Retain WAITING1148/1226/48 and other scientific holds.

## §4 Invariants
Only consumed source paths are dependencies. Override variable names/values, including a variable named file, are not new file declarations. Match first-key semantics of existing Wave.column_mapping for a multi-key mapping; do not implement a new grammar. Wave.grab_data resolves names listed in dfs as sibling subblocks: a subframe explicitly named file/idxvars/myvars must be visited even though the same spelling otherwise denotes a variable/override payload. Preserve nested dfs, ordinary strings, ordering and duplicates. No LSMS_CACHE_SCHEMA bump, provider/decoder, DVC CLI, source/cache reads, data acquisition, dependency change or public push. Hash invalidation is not evidence of changed served data.

## §5 Decisions
Reuse existing fingerprints and hash composition; extend collector only. Skip recursion into file-entry override payloads and known myvars/idxvars maps (their keys are variables, not feature blocks), while recursing dfs and genuine generic blocks as before. Root explicitly approved the two additional negative variable-map controls; a whole YAML edit still legitimately changes the hash, but sidecars named by variable values do not. Independent ff4a review then reproduced a P2 for explicitly named dfs subframes; exclude payloads by declaration context rather than unconditionally by spelling. Four actual mapping/Wave/Country controls include main, myvars, idxvars and file. Preserve that failed intermediate head and its census; no broad CI was run on it. Add actual Wave/Country sidecar-change controls and fake-FS prewarm controls; no duplicate hashing machinery. Measure full registry/hash scope at base and frozen candidate using preload_panel_ids=False and actual Wave.file_path.

## §6 Open questions
No science ruling required. Root approved first-consumed-path and payload isolation explicitly. Report any separate prewarm resolver deficiency rather than changing it. Broad offline CI is additional frozen verification as resources permit; independent review/publication remains root-owned.


## Phase 3 — final anchored verification

Frozen implementation `065082e44ae2ae024823b93a6f9df78a09614f79`; all local
verification/collection actors are terminal. Detailed immutable receipts and
failed intermediate attempts live in `B/reports/framework-1510-local-20261107/`
(where B is the session burst root recorded by the coordinating task).

- **OK (§2/§5 reuse):** existing collector, source fingerprints, actual Wave and
  Country hashes are reused; no parallel extractor or cache-version lever.
- **OK (§3/§4 definition):** first consumed override paths are preserved;
  variable/override payloads are excluded by context. Explicit DFS sibling
  names `main`, `myvars`, `idxvars`, `file` retain their source dependencies.
  Intermediate ff4a's independently reproduced P2 and negative probes remain
  preserved, not rewritten as successes.
- **OK (§4/§5 tests):** 102 focused checks plus one separate fake-FS resolver
  witness = **103 distinct controls**; overlapping reruns are not added.
  Independent final review at exact065082 passes22 hermetic author controls
  and4 independently constructed actual namespace controls (not eight from
  the standalone phase-exit repeat), with no remaining introduced P1/P2.
  Verdict: `B/reports/review-1510-source-overrides-20261107/FINAL-VERDICT.md`.
- Strict `make api-audit STRICT=1` exits0. Frozen local offline CI command:
  `unshare -rn /local/job40825266/venv_img/bin/python -m pytest -n 2 --dist=loadfile --no-purge -q -m 'not requires_s3' tests`.
  At exact065082 it exits0: **11,464 passed,245 skipped,4 xfailed**,1034.89s;
  wrapper1037.61s, clean unchanged head, no source credentials and an empty
  allocation-local initial data root. A separate network-isolated
  `--collect-only -n 0` invocation executes no test bodies/fixtures and proves
  **17,117 total /11,713 selected /5,404 source-marked deselected**. The local
  filtered run does not establish source-marked or configured GitHub CI success.
- Full registered metadata scope:38 countries,450 declared country-table cells,
  1895 wave cells,35 declared-method build fingerprints,186 import closures;
  zero errors/denials and28 override entries in9 physical configs,0 unsupported.
  Explicit exclusions are11 unregistered country entries and3 non-country
  entries.18 None wave cells are Peru1985/1990/1991 ×6declared tables (missing
  wave `_` directories). The separately enumerated public runtime-derived
  surface553/2313/39 adds food_expenditures,food_prices,food_quantities and
  household_characteristics; it does not imply additional persisted caches.
- Measured invalidation is **GLOBAL:450/450 country,1877/1895 wave,35/35 build
  fingerprints move;0/186 import closures move**. The unchanged-callable trace
  is `Country._aggregate_wave_data -> _warm_dvc_cache_for_feature ->
  _collect_file_paths_from_block`. No source/served-data regression claim or
  cache rewarm is inferred. The successful operator permits only absent-file
  lookups and the exact unchanged tracked conversion-config CSV blob28619ec2;
  all actual raw/harmonized/provider/build/acquisition accesses remain refused.
- Existing bare-Data-path prewarm resolver miss is separately filed **GH1602**;
  fake-FS witness has no real client/download/cache activity. No resolver fix or
  actual prewarm benefit is bundled. Scientific/weight/recall/date policies
  and shared/master rewarm remain outside this task.

This final ledger-only documentation commit preserves065082 code history.
Library/configuration/test bytes must be proven EXACT065082 in the separate
`final-doc-only-proof.json` receipt; no new build input is introduced and no
full local/source rerun is needed. Root owns draft PR1601's configured GitHub
head checks, publication/merge and release coordination; those checks remain
separate acceptance from this local verification.
