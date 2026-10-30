# Prior-Art Ledger -- #1287 Tanzania country concatenators

Sections 1-6 preserve the earlier bounded phase; sections 7-9 govern this declaration extension.

Search tier: git + ripgrep + AST (floor). Initial source a846fb13f38a0afe6de0d60dd2789bec1fff072e.

## §1 Task
Refuse a country-script output when any explicitly enumerated supported wave folder cannot be read, rather than writing a partial table. No wave producer, round selector, household key, reducer or framework route changes.

## §2 Existing machinery
- Tanzania/_/{crop_production,livestock,people_last7days,plot_features,plot_inputs,plot_labor,community_cluster_xwalk}.py enumerate exactly 2019-20 and 2020-21. Both wave scripts exist for every table. Their broad reader catches currently continue, so one loaded sibling suffices to publish.
- Tanzania/_/{anthropometry,individual_education,food_coping,life_satisfaction,shocks}.py enumerate Waves.keys(): 2008-15, 2019-20, 2020-21. Each has a script in every folder. Existing data-root then relative-path attempts are preserved; exhaustion currently warns and permits partial output.
- Tanzania/_/Makefile: separate wave prerequisite rules; no Niger-style semicolon-chain repair is needed here. Framework aggregation is authoritative (#788/#1091); this patch is only the remaining country-script boundary, not a new framework completeness guarantee.
- tests/test_niger_concatenator_refusal.py: existing accepted country-refusal precedent; its source-block/full-script tests are reusable in form. Its synthetic parquet IO and Country route are not run here.
- Tanzania/_/tanzania.py:id_walk (single substitution per wave) is already the country script operation; retain it, test with an invented mapping. community_cluster_xwalk has no household walk.

## §3 Definitions
Inherit STANDING.md §§3-4, CLAUDE.md and canonical data_info.yml. Complete Tanzania CONTENTS.org including TODO/WAITING/LOGBOOK read first. UPHI back-casting and r_hhid interviewed-household grain are unresolved (#114); neither is repaired here. A physical folder is not a logical wave: wave_folder_map shares 2008-15 across four rounds. food_coping declares only rounds2/3/4 there; this is source documentation, not fresh C4 evidence. Requiring the declared folder's output does not require a round1 row. Modern Extended and Refresh samples remain disjoint.

## §4 Invariants
Preserve every folder list, fallback path, input frame, column, dtype, null mask, concat order, duplicate handling and id_walk. Missing/unreadable declared output must raise before to_parquet; keep original exception as cause. Existing data-root fallback may still recover an unreadable first candidate. No per-round nonempty requirement: declared support can honestly produce no rows. No raw/PDF/provider/cache/SQLite reads, ID generation or Country/Feature builds. Synthetic readers and a captured writer replace only IO leaves; execute original scripts and actual id_walk.

## §5 Reuse decision
Extend existing catches only: replace silent skipping with a contextual chained RuntimeError after existing attempts. No new helper, data schema or module edit. Twelve focused script edits avoid a Tanzania-module-wide build-input change; touched scripts are build inputs, but actual hash costs are not measured. Test actual scripts on invented frames, both missing-folder positions, empty and healthy cases, fallback success and refusal-before-write. Run unchanged tests on original source first.

## §6 Remaining boundary
Source completeness is not row completeness. A readable partial/empty parquet and lost logical round remain undetected. Five multi-round scripts retain their existing reducers and documented WAITING114 caveats. Default framework waves and public routing are outside this bounded country-script fix. Independent review and any real-source acceptance remain root-owned.

### Phase 3
Pending original red/healthy controls, candidate controls, and exact scoped diff review.


## §7 Exact issue criterion and previously unimplemented case (2026-10-28)

Authoritative issue body retrieved read-only from https://github.com/ligon/LSMS_Library/issues/1287.
Quoted verbatim below, including its distinction between declared and unwired waves:

> Found while extending Niger's missing-wave refusal (branch `fix/niger-concatenator-andand-20261023`, following #1241). Same family as #1281.
>
> **About 50 country-level concatenators in 14 countries silently skip a wave whose wave parquet is missing.** Among them are Ethiopia, Malawi, Mali, Tanzania and Uganda. Each reads `{wave}/_/<table>.parquet` for the waves it knows and drops the ones that are absent. A failed or unrun wave script therefore yields a country table that lacks that wave, with no error. Under `;`-chained Make recipes the failure is not even surfaced by `make`.
>
> **Measured on Niger before the fix** (cold build, make backend, one wave script broken on purpose): the four tables served 32,659 of 46,376, 20,466 of 30,230, 43,584 of 53,041 and 90,146 of 125,294 rows, each missing exactly that wave, with no warning.
>
> **Fix pattern** (#1241 and its Niger extension):
> 1. Chain the recipe's wave scripts and the concatenator with `&&`.
> 2. In the concatenator, refuse when a wave that **declares** the table produced no parquet. "Declares" must be read from the config: a wave that does not wire the table is a legitimate skip (e.g. Niger `plot_features` 2011-12 and 2014-15).
>
> `&&` alone is worse than `;`, because it also stops the later waves from building. The refusal is what makes it safe.
>
> **Scope:** per country, one re-warm each. Inventory first: which concatenators skip, and whether every skipped wave declares the table.
>
> Related: #1123, #1241, #1281.

The earlier 5b1b6110a extraction and its 99 candidate passes / 58 original failures / 41 healthy controls establish the bounded all-current-producers refusal. Those source/evidence packets remain immutable. They did not implement runtime config retirement: a genuinely unwired enumerated folder would still be read/refused. This is an unimplemented issue acceptance case, not a newly observed failure on the present 29 declarations.

Preparation status: runtime declaration gates and metadata-backed undeclared controls are being prepared in a NEW exact-d65 extraction. No new imports/tests/data stage has run. Root source/diff review precedes any synthetic execution.

## §8 Reuse and constructor/import audit

Reuse actual Country('Tanzania', preload_panel_ids=False), Country.__getitem__, Wave.data_scheme and Wave.wave_folder. Build a physical declared-folder set from the public logical waves, then gate each existing enumerated folder before either reader candidate. Never index Country by physical '2008-15'; four public logical waves share it. A declaration on any mapped logical Wave retains that physical output; no logical nonempty/round-1 requirement is introduced.

At exact d65 country.py:2527-2575 constructor catalog checks and resources read metadata only; explicit preload_panel_ids=False prevents panel generation. resources:2579 reads country YAML. Country.waves:2802 imports the country module and sets wave_folder_map; __getitem__:3103 validates via waves and constructs inert Wave:1186. Wave.data_scheme:1262 unions native script names and YAML keys. yaml_utils._construct_make only constructs metadata dictionaries, not Make jobs.

Country.waves executes Tanzania module top-level code, rather than being pure YAML discovery. At unchanged d65 the module's top level consists of imports, constants/function definitions and two sys.path appends; data/read/build helpers are function bodies, not invoked there. The earlier guarded 99 actual-script checks imported that same source without provider/raw/nativeDB access. The new source review calls no table/getter/build/finalizer/ID API; runtime guards must independently enforce this later. Repeated metadata imports are existing getter behavior, not a reason to edit the shared framework.

## §9 New discriminating controls (prepared, NOT RUN)

Keep the existing 99 cases and actual id_walk. Add actual Country/Wave declaration premises for each table on the real source config. Use supported private JSON-as-YAML country/wave metadata to represent either native-script or YAML declarations; retain the real six-wave/folder map, remove BOTH native/YAML declarations for retired folders, and assert actual Wave.data_scheme says absent.

For each enumerated table/folder and both declaration forms, run the actual compiler with invented IO leaves: no retired-folder read, exactly the remaining rows, no fabricated empty/missing wave, same real ID walk. Also cover all-declarations-retired preserving the preexisting no-input failure without any read/write. No Country/Wave implementation, compiler or id_walk stub is permitted; this is actual declaration API on synthetic metadata, not public Country.table data acceptance.

Static proposed count: existing99 +12 actual-config premises +58 one-folder-retirement cases +24 all-retired cases =193. Compiler executions:87+58+24=169;24 presence/metadata-only cases execute none. Prior5b scripts should retain the111 established/premise passes while exposing82 new retirement failures; exact original d65 would additionally retain its58 old refusal failures. These are expected controls, not executed results. No broad suite, Make/cache/native source, #905/#846 or scientific acceptance is authorized by this preparation.
