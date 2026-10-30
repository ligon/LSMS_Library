# Prior-Art Ledger — #1287 Ethiopia food country selector

**Search tier used:** ripgrep + Git + Python AST floor; GitNexus tools are
unavailable. Base: `d65e8b13683409ab0005f1a5590208cd7d94ae62`. Recovery
`9cac9ffc` is evidence only; draft #1315's nine compilers are not replayed.

## §1 Task, restated

Change only Ethiopia's country `_/food_acquired.py` selection to use
`Country('Ethiopia', preload_panel_ids=False).waves`, each actual Wave's
native-script/YAML/null declaration union, and physical `wave_folder`.
Read a declared folder once, skip only unwired contributions, and refuse a
declared unreadable input with contextual exception chaining before writing.
Keep the public squeeze, local walker, concat, food rename and writer semantics.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | decision |
|---|---|---|---|---|
| `Country.waves` | `lsms_library/country.py:2802` | authoritative logical waves; module, YAML or documentary discovery; folder map | existing Country tests; new focused metadata tests | reuse |
| `Country.__getitem__` | `lsms_library/country.py:3103` | constructs actual Wave with mapped physical folder | same | reuse |
| `Wave.data_scheme` | `lsms_library/country.py:1262` | unions native script stems and YAML keys, including null | same | reuse |
| country food assembly | `lsms_library/countries/Ethiopia/_/food_acquired.py:54` | historical `Waves` loop, squeeze, local walk, concat and rename | new focused invented-frame tests | extend selection only |
| `id_walk`, `fix_food_labels` | same file:30,44 | historical ordered walk and union-map rename | new focused metadata/invented-leaf tests | preserve byte-for-byte |
| `change_id` | `lsms_library/countries/Ethiopia/_/ethiopia.py:482` | normalizes/maps **j**, not i | existing scientific tests; not run here | preserve, replace only IO leaf in new tests |
| `harmonized_food_labels` | same file:42 | reads org food-label mapping | existing food tests; not run here | preserve, invented label leaf in new tests |
| food Make target | `lsms_library/countries/Ethiopia/_/Makefile:50` | runs the country compiler after wave prerequisites | no actual Make build permitted | preserve |

Call-site sweep: the local `id_walk` and `fix_food_labels` are used only by
this script; Make invokes it at country scope. Other Ethiopia tables/helpers
and shared library APIs are outside the patch. Risk: selector changes which
contributions reach existing science; no schema, unit, panel or label redesign.

## §3 Definitions & conventions in force

- Inherit `.coder/ledger/STANDING.md` §§3–4, `CLAUDE.md` and
  `lsms_library/data_info.yml`; no shared-schema changes.
- `CONTENTS.org`, food sources (§ food_acquired reads Q3/Q5/Q6): reported
  purchased/produced/inkind quantities; produced/inkind expenditure stays NA.
- `CONTENTS.org:848`: “2011-12 / 2013-14 / 2015-16 form one panel and
  2018-19 / 2021-22 form another.” Preserve panel boundaries.
- Aggregate-labels skill: Ethiopia has no Aggregate column; `labels=` behavior
  remains unchanged. PP/PH skill: physical folders and logical t are distinct.

## §4 Invariants & assumptions

- Public functions and postselection statements remain AST-identical; keep
  `.squeeze()` (including its existing Series behavior), no added normalization,
  aggregation, deduplication of rows, filtering of t, or household walking.
- Deduplicate physical input folders, preserving authoritative wave order.
  A stale output's existence is never a declaration. YAML null is a declaration.
- The local walk still receives logical t and the historical `Waves` mapping.
  New waves unsupported by that mapping are a separate scientific dependency,
  not authorization to redesign the walk.
- Preserve the documented/source discrepancy: `CONTENTS.org:2453` says “only
  a local =change_id=-based walk” in a household-ID discussion, but the public
  helper maps `j`. Report it, do not repair it here.
- Runtime/profile: private config/data/L1/L2 only; all threads 1; no actual
  raw/parsed payload, crosswalk JSON, decoder, provider, Country/Feature table
  build, Make or helper child. Newly authored finite tests only, <=600s/cohort,
  denied or invented IO leaves, exact source/JUnit and zero-real-data accounting.
- Do not modify existing fixtures/assertions/goldens/locks/dependencies. New
  test-local private metadata and invented frames are expressly authorized.

## §5 Reuse decision

| quantity | decision | reason |
|---|---|---|
| declared contributions and physical folders | reuse Country/Wave | same authoritative selection as draft #1315; no independent scanner |
| contextual failure | extend local read boundary | prevent partial writer; retain original exception as cause |
| scientific/concat/normalization/walker tail | reuse exact public source | no scientific or source-policy changes authorized |
| tests | new narrow file | omitted food selector is absent from draft #1315; use actual discovery and invented IO |

## §6 Open questions for the human

None blocks this narrow assignment. Actual-source, Make/API/cache/freshness,
fourteen-country #1287 and scientific acceptance remain separately held gates.

### Phase 3 — verification

Pending candidate source and finite data-free tests. This ledger is committed
before product edits; final anchored verification will be a separate commit.

### Phase 3a — first finite actor and test-premise correction

The frozen a7525f166 cohort actually collected 66 cases: 65 passed, and the
singleton public-squeeze test failed only at its `__cause__ is None` assertion.
All 66 compiler epochs closed, with zero guard denials and all recorded source,
runtime, configuration, metadata, profile and no-data closure gates true. The
one-shot transaction remains STOPPED / accepted 0; no automatic retry occurred.
Its durable inventory is
`ffa877b0bfe704d52990b4f9f58e8e44f25ab395f71cfe88fcd47919ef89770e`
(269 ordinary copies, seven font-cache files inventory-only).

The outer `KeyError` matching `j` was observed, as was a non-None cause. The
receipt does not serialize the inner exception's type or args. Source explains
the mistaken premise: unchanged `.squeeze()` produces a Series for one row;
unchanged `change_id` resets it then accesses `x['j']` (`ethiopia.py:491–495`).
Installed pandas `DataFrame.__getitem__` calls `columns.get_loc` (`frame.py:4378`),
which explicitly raises `KeyError(key) from err` (`indexes/base.py:3640–3648`).
This failure is outside the new reader-only RuntimeError boundary. Public d65
has the same squeeze and walker path; no baseline actor was replayed.

The separately isolated successor changes only this new test's chaining
premise: retain the required `KeyError('j')` and no-writer behavior, and require
the expected pandas `KeyError('j')` cause instead of requiring no cause. Exact
inner details are source-supported expectations awaiting a separately released
measurement. Product/helper bytes and every other test remain unchanged; the
singleton shape failure itself and the local j-versus-i walker limitation remain.
No successor test execution, raw data, Make/API/cache work or scientific
acceptance is implied. Original native exit143/publication-incomplete/
preservation-unverified provenance and original finite 65/1 failure are retained.
