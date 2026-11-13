# Prior-Art Ledger — #1287 Niger five country producers

Search tier: Git/ripgrep/AST floor; no GitNexus tool available. Inherits
STANDING.md. Public base d65e8b13683409ab0005f1a5590208cd7d94ae62.
Current dispatch is source preparation only: actual authority 0; no project
imports, tests, parser, producer child, native/API/Make actor or data access.

## §1 Task, restated

Replace the five hardcoded native-wave prerequisite/command lists in Niger's
country Make recipes with declaration-aware production. Product edits only
Niger/_/Makefile and a narrow producer helper. Retain country compilers and
science, native frame grain, and framework/cache policy (#1288). Compose with
open draft #1316; do not duplicate/adopt its branch. Its current source receipt
is requested through the shared steering channel; the rescued ledger cites
77fa5f9b4c7f69ab47384cf927938e5d2960850d, absent from this clone.

## §2 Existing machinery

| Symbol | Source | Behavior / test source (not executed here) | Decision |
|---|---|---|---|
| Country.waves | country.py:2803 | module, registry, folder discovery; initializes mapping | reuse |
| Country.__getitem__ | country.py:3103 | logical wave -> Wave with physical folder | reuse |
| Wave.data_scheme | country.py:1263 | native-script + YAML-key union, including null keys | reuse |
| _build_wave_script_target | country.py:249 | checked Make/direct-script runner; force=True preserves former unconditional native execution; tests/test_gh968_no_make_fallback.py, tests/test_build_subprocess_env.py | reuse |
| Wave.grab_data | country.py:1885 | YAML loader returns pre-finalize native wave frame; filters t to logical year | reuse |
| local_tools.to_parquet | local_tools.py:3309 | sanctioned writer; absolute_path avoids caller-stack ambiguity | reuse |
| yaml_utils.load_yaml | yaml_utils.py:43 | metadata parser understands !make; prepared tests reuse it for tagged country registry | reuse |
| _wave_grain_audit / wave_datetime_audit / _wave_build_drops | country.py:2334-2342 | YAML write provenance | reuse |
| five country recipes | Niger/_/Makefile:32-120 | four static script lists, checked && chaining | extend |
| concat_wave_parquets | Niger/_/niger.py:1765 | public base requires four scripts; draft #1316 owns declaration-aware compilation | unchanged integration dependency |

Rescued WIP inspected BEFORE design: SOURCE-ORIGIN.json and complete source.patch
at /global/scratch/fsa/fc_jevons/ligon/burst/isa-country-dispatch-20261029/rescued-source/Niger.
Original dispatch a52a7ab8071a4d698251c6c790ded919/job40605724,
HEAD cc7e5a1de00905a9816ba271f8ab3aaf602baf3e,
snapshot 1a5596fa60f5e9a53e7353b986759aa08e69dd5e. Native failed/exit143;
preservation unverified; publication incomplete; runtime_or_tests_accepted=false.
Selected source: ledger, Makefile, wave_producers.py, test_niger_producer_routing.py.
No accepted result accompanies them. WIP helper lacks physical-folder dedup;
new candidate must fix it. Reuse attributed source only; never inherit acceptance.

## §3 Definitions & conventions in force

Cite STANDING.md §§3-4, CLAUDE.md "Data Access", "Grain Collapse", and
lsms_library/data_info.yml. Native compiler inputs differ from normalized API
results: do not call Country's normalized table method to manufacture them.

Niger CONTENTS.org, "Every country concatenator refuses a missing wave":
"Every Niger wave has a wave script for all five concatenated tables";
"plot_features is different: 2011-12 and 2014-15 have no wave script".
Those are current facts, not a permanent four-wave contract. EHCVM vague is a
seasonal sample split; 2014 EXTENSION 2 is a separate household. Preserve both.
Existing documented organic/inorganic sentinel contradiction and ECVMA seed-slot
identity defect stay unresolved. No new survey claim is made from source alone.

## §4 Invariants & assumptions

Only Country-registered waves declaring the table may produce. Native scripts
retain priority in these five native Make recipes (even beside a null YAML
key); a genuine YAML-only declaration goes to grab_data. Discovery errors fail,
never imply retirement; finish discovery before the first producer. Process
physical native folders once in deterministic Country.waves order. YAML loaders
are logical-wave scoped: when logical waves share a folder, load each declared
logical contribution, concatenate without reduction, and serialize the physical
input once. Preserve all native Make rule prerequisites. First/late producer or
writer failure prevents country compiler execution through &&. No fabricated
fallback frame. Never finalize/reduce/walk ids, evict/clear/stamp caches, change
schema/fixtures/assertions, or touch science/crosswalks.

## §5 Concrete command/dependency design

EXTEND attributed WIP adapter: snapshot actual Country.waves, resolve through
Country.__getitem__, inspect Wave.data_scheme, collect all declarations and
metadata before production. Group contributors by resolved physical directory
in first-seen order. Native folder: call _build_wave_script_target once with
physical wave target and force=True. YAML-only folder: call grab_data for each
logical contributor, concatenate only if multiple, then to_parquet at physical
compiler path, including typed empties and LSMS_NO_CACHE. Use existing audit
records for each logical wave; no normalized Country call or invented output.

Country targets keep compiler/niger/category dependencies, add helper/registry,
and use secondary expansion for existing registered-wave metadata and declared
native drivers. A phony failing prerequisite exposes discovery errors otherwise
swallowed by Make's $(shell). Emit no partial list; escape dependency names.
Recipe: `$(PYTHON) wave_producers.py TABLE && $(PYTHON) TABLE.py`.
Native pattern rules remain byte-identical. Existing country-target freshness
and failed-Make fallback acceptance remain #1288, not repaired here. Shared
Makefile/helper input implies a Niger re-warm; actual hashes unmeasured.

NEW/EXTEND prepared tests use actual private Country/Wave metadata and invented
producer/IO leaves, never actual wave scripts or survey bytes. Cover native/YAML/
null union, retirement, new/unregistered/mapped/shared-folder waves, deterministic
order and one physical output, metadata failures before production, exact typed
frames including empty/duplicate/NA, audits, first/late runner/load/write failures,
and Make command/prerequisite/parser semantics using invented leaves only.
Do NOT execute these tests in this dispatch. Prepare a finite guarded plan for
root release, using receiving-node affinity and threads1. Existing154 controls
are retained historical evidence, not run or counted again.

## §6 Open questions / remaining gates

Current #1316 compiler source pin/receipt requested; composition test requires
separate root review. Full make api-audit is deferred under current prohibition
on project/helper children; only dry command inspection and NEW stdlib skill
frontmatter inventory are allowed. Root must release any project test/import,
Make parser/helper child, typed controls or actual actor. No project/API/scientific
acceptance, no claim of fourteen-country #1287 completion.

### Phase 3 — verification

- OK (anchored on §§2,4): discover reuses actual registration, indexing and
  declaration membership; its errors propagate before production. Source/AST
  checks only; project behavior unexecuted.
- OK (anchored on §§2,4,5): produce groups physical paths in first-seen order,
  keeps checked forced native runner, loads each logical YAML contribution and
  writes once without reduction. Grain/date audits select physical folders;
  drops select logical years, per existing accessor definitions.
- OK (anchored on §§4,5): five recipes chain producer and compiler with &&;
  native wave pattern rules unchanged. Make parser/discovery behavior is a
  separately guarded, unexecuted gate, not established by AST assertions.
- Prepared tests fix an inherited WIP test-source defect: safe_load cannot
  understand the current registry's !make tag; reuse yaml_utils.load_yaml.
- NEW stdlib source_controls.py passed 30 source/AST checks in 0.07 s, with
  zero project imports/tests/helper executions/children/data calls. This is
  source-policy evidence, not typed/runtime/API acceptance. Final source pins
  and any additional static result live with the dispatch evidence.
- No reinvention found in source search: checked runner, YAML loader, writer
  and audit accessors are reused. Grouping is narrow country orchestration.
- Current draft #1316 remains unverified: cited head absent from local Git;
  public PR fetch returned "Cache miss". Root must supply a current source
  receipt and review composition. Existing154 controls were not replayed.
  All project/typed/parser/helper/real-source/API/scientific gates remain
  unexecuted, actual authority 0; #1288 and fourteen-country completion remain
  outside this task.
