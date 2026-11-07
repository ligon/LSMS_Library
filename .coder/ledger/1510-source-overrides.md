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
