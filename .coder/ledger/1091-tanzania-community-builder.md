# Prior-Art Ledger - GH1091 Tanzania community builder diagnosis

Sue (GPT-6), 2026-10-21. Search tier: ripgrep + git floor.
Baseline: reviewed Tanzania PR1185 head
`e75be9c56fa6c318ce32cea5a8be6d448578bed1`.
Inherit `.coder/ledger/STANDING.md` sections 3-4, `CLAUDE.md`, and canonical
`lsms_library/data_info.yml`; do not duplicate or redefine their contracts.
This ledger is committed before new diagnosis functions or analysis.

## Section 1: Task, restated

Diagnose the Tanzania community-price country concatenator and actual Make
DAG, including normal default/Make all, supported-singleton, unsupported and
combined requests at target-cold, fresh-process warm and populated stages.
Produce a concrete minimal retirement proposal and discriminating test plan;
do not implement a candidate. GH788 settles framework authority. GH840/1086
settles the source variety/visit/observation representation. No science,
eligibility, unit basis, identifiers, grain or household crosswalk changes.

## Section 2: Existing machinery

| Symbol / file | Location at baseline | Existing behavior / test | Decision |
|---|---|---|---|
| Country concatenator | `Tanzania/_/community_prices.py` | Loops only 2019-20/2020-21 native parquets and concatenates; no source transform or ID walk | Diagnose retirement |
| Country Make block | `Tanzania/_/Makefile:145-149` | Sole-use discovery variables and explicit var target invoke country script | Audit all references and actual make -n |
| Independent wave rule | `Tanzania/_/Makefile:151-152` | Runs actual requested wave community script | Preserve |
| Generic rule | `Tanzania/_/Makefile:154-155` | `%.parquet: %.py CONTENTS.org tanzania.py` can affect legacy paths/discovery | Measure actual route; no regex inference |
| `_has_country_builder` | `country.py:4893` | Script existence, then real fresh-empty-root Make probe; GH1039 tests | Reuse |
| `_aggregate_wave_data` / `load_from_waves` | `country.py:4980/5249` | Native wave loading, ID walk/normalization, authoritative concatenation | Reuse |
| `_empty_declared_frame` | `country.py:1050` | Existing strict-subset no-builder empty contract | Measure; no normalized comparison |
| `_assert_built_required_columns` | `country.py:4817` | Existing post-finalize script-path column enforcement | Preserve |
| Native helper | `Tanzania/_/tanzania.py:1953` | Positive village price/quantity basis, resolved source item/unit, source-code variety, reported unit price | Preserve byte-exact |
| Both modern wave scripts | `Tanzania/{2019-20,2020-21}/_/community_prices.py` | Read own CM_SEC_F_ID, raw codes plus aligned item labels; sanctioned IO | Preserve |
| Sanctioned IO/hash/coverage | `local_tools.py`, `Country._table_cache_hash`, `Wave._input_hash` | Existing gates and native legacy script cache contracts; STANDING sections 2/4 | Reuse |
| Relevant tests/audit | `tests/test_community_prices_variety.py`, `tests/test_gh1039_make_backend_no_country_rule.py`, `bench/feature_audit/` | Existing source-code grain, Make routing and Feature checks | Reuse narrowly if prerequisites gated |
| Reviewed unit correction | ledger `1118-tanzania-plot-input-units.md`, source/API evidence under `slurm_logs/.../1118-Tanzania/` | PR1185 normalized 86+322 missing plot-input units, preserving 9698 rows | Verify held-source control; do not alter |

## Section 3: Definitions and country idiosyncrasies

Complete current Tanzania CONTENTS read: 2142 lines, Git blob
`9424a5042b9880d6c48aba17af0e8d3cadc45ccb`, including TODO/WAITING/LOGBOOK.
AGENTS/CLAUDE match the previously read complete guidance (SHA256
`c1205513db01fb743ec3c8b8c0ec69615a2bfcd9521fb7ed2d3195bd1a7e20dd`).
Read backlog-workflow, cross-country-features, multi-round-waves and
tanzania-panel-design; retain the current task's stricter no-bypass/no-acquisition
constraints over older skill examples suggesting cache clears or NO_CACHE.

- UPD4 back-casts split-off descendants into earlier waves. UPHI is the
  round4 panel identity; r_hhid is the interviewed round household. The
  many-to-many relationship is deliberate, and a unique key does not prove
  correct grain. The live WAITING #114 remains; no incidental key repair.
- Four logical waves share 2008-15/ via wave_folder_map. Use actual folders
  in scripts/caches; do not invent unsupported community sources there.
- 2014-15 pools Extended and Refresh panels; 2019-20 follows Extended,
  2020-21 follows Refresh plus 545 urban booster households. Zero household
  overlap between the latter waves is by design.
- Community v is native interview__key, not household sample cluster v.
  No i-bearing household join belongs on this table. Retain the native
  i==v compatibility shim until normal framework normalization, if produced
  by the existing helper; verify actual layers rather than assume it.
- Source item code variety preserves separately priced codes merged by j;
  Description retains each questionnaire's label. Price is cm_f063/cm_f062
  per native cm_f061 unit; district-capital prices are a different instrument
  and remain excluded. Positive price/quantity and resolved item/unit/key
  eligibility remains unchanged.
- CONTENTS says the geography-only join is "infeasible without a lossy,
  unverifiable crosswalk", then records an implemented confidence-tagged
  community_cluster_xwalk with deterministic 2020 matching. The schema's
  older region-only paragraph also differs from its adjacent implemented
  crosswalk section. Quote that contrast, do not silently erase either or
  rewrite the crosswalk. Native v remains authoritative for these prices.
- PR1185's 408-unit correction concerns plot_inputs, not community prices:
  86 in2019-20 and322 in2020-21, including19 no-amount records. Its existing
  conflicting-unit duplicate groups belong to #637, not this retirement.

## Section 4: Invariants and validation boundary

- Read-only production/config/tests. The only tracked edit authorized now is
  this task ledger. No candidate patch or candidate build is authorized.
- New private root only; never reuse or mutate the frozen Niger/1078/849/Mali
  source or evidence roots. Safe exports get a dedicated safe-exports/
  directory; never overwrite a private aggregate basename.
- Exact cwd/import/PYTHONPATH/countries/config/data identities; mounted image,
  one numeric/build/Make thread and at most two explicit loadfile workers.
  Active socket/write guards plus unshare -rn before any source decoding/API.
- Stage only held declared sources using MD5/SHA/size/mode/mtime and physical
  copy/nlink/inode gates, both DVC layouts. Terminal successful preparation
  precedes dependent probes. No acquisition, DVC CLI, shared-cache operation,
  bypass, stamp, in-tree raw/parquet, household join or xwalk rewrite.
- Compare complete native/walked/normalized/raw-country/public and CLI
  stages, with check_exact=True, rtol=atol=0, all axes/levels/codes/dtypes/
  masks/order and recursively typed full attrs. No attr stripping or numeric
  tolerance. Capture per-call cache receipts BEFORE assertions.
- Default and Make are separately measured. Do not preserve any actual
  wrong-wave Make output as an acceptance target, or promise blanket zero
  warm writes where native hashless scripts naturally rebuild.
- Healthy comparator L2 requires exact current hash/full coverage and
  physical provenance gate at this source pin; d602 Niger is not assumed
  compatible. Avoid unrelated food/FCT builds and broad test/corpus sweeps.
- Any expected candidate hash scope is explicitly a prediction until an
  approved real patch and actual census exist. No source-guess shortcut.
- Raw frames/IDs/coordinates/warnings/tracebacks stay private. An allowlisted
  aggregate wrapper exports only bounded counts/statuses; never cat/head/tail
  or rg detailed failure logs. Preserve failed launches and exact limits.

## Section 5: Reuse decision

Reuse existing authoritative wave aggregation, fresh Make discovery, required
column guards, source helpers and cache machinery. No new concatenator,
grain/ID design, selector or scientific transform is needed. Diagnostic
wrappers and independent source/count receipts are private analysis only.
The concrete eventual removal allowlist and baseline-negative tests will be
proposed after measurement, not inferred from Niger or Mali.

## Section 6: Open scope and stop

No new human authority/grain decision is requested: framework authority is
settled. Finish the read-only diagnosis/proposal and stop for ROOT review
before implementation. Source gaps or contradictions are evidence to report,
not permission to acquire, infer a fallback or extend scope.
No GitHub write, public merge or issue closure.

## Phase 3: Verification

Pending terminal preparation and bounded actual baseline measurements.
