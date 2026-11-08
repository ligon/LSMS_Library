# GH #1520 — source-free wave_folder_map initialization

Search tier: git + ripgrep + static AST (floor); no GitNexus tool is exposed.
Base: b8346cf1f953ac82517b1b7473c715a5655ca950. Inherits STANDING.md
§§3–4 and CLAUDE.md. Issue snapshot is the dispatch's authenticated
context/issue-1520.json (OPEN, no comments); no live GitHub operation.

## §1 Task

Make direct Country.wave_folder_map access resolve the existing metadata
without requiring a previous waves read. Preserve the resolver's source
precedence, ordering, aliasing, public assignment and later waves refreshes.
This is a bounded code/invented-fixture preparation, not country/API/source,
hash-census, scientific, CI or whole-issue acceptance.

## §2 Existing machinery

| Symbol / source | Behavior and existing coverage | Decision |
|---|---|---|
| Country.__init__, country.py:2587 | Seeds an empty public mapping; causes the direct-reader defect | Remove only placeholder initialization; no eager metadata or preload |
| Country.waves, country.py:2836 | Existing module import; module waves before Waves keys; YAML only when module file absent; sorted directory SOURCE.org/SOURCE fallback | Reuse unchanged as the sole metadata resolver |
| Country.resources / file_path, country.py:2607–2622 | Existing countries_root and YAML resolver | Reuse via strict invented providers; no new parser |
| Country.__getitem__, country.py:3137 | Validates through waves, transports logical label plus mapped physical folder to Wave | Reuse actual method with inert fake Wave |
| Country mapping readers, country.py:4768,5240,5526 | Cache eviction/hash source folders/Make target use map; no new callers | Scope analysis only; no execution |
| Country._aggregate_wave_data, country.py:5830 | Old comment says self[w] ensures map initialization | Update obsolete explanatory wording only |
| transformations.pp_ph_area_t, transformations.py:5629 | Forces waves read; groups map into existing round pairs | Keep duck-typed caller behavior; remove obsolete empty-map claim only |
| coverage_matrix.py:1174; coverage-matrix and 818 ledgers | Historical workaround reads waves before lookup | Keep historical ledgers; no grader change |
| 1602-prewarm-source-paths / 1287 Tanzania and Nigeria ledgers | Physical folders come from actual Country lookup; declarations differ from held files | Preserve all mapping/declaration/source policies |
| tests/test_gh1287_ethiopia_food_country_selector.py | Existing invented module waves/Waves/YAML/SOURCE authorities | Reuse precedence premises, not constructor/source allowance |

## §3 Definitions and country grounding

FULL Tanzania and Nigeria CONTENTS.org, including LOGBOOK/WAITING/TODO, read
before country metadata. Tanzania's four logical rounds share 2008-15; UPHI
backcasting and live WAITING #114 remain held. Its new #1497 explicitly says
"The existing v ruling remains separate from which household inherits canonical i."
Nigeria's logical PP/PH quarters share folders but keep own covers and codebooks.
The recorded contradiction "The same households are visited in both rounds"
is explicitly superseded by own-round membership evidence. Tanzania likewise
records "the country-level path above is retired" (#836) while preserving
older food descriptions. Neither tension authorizes any data/identity repair.
No new country idiosyncrasy is discovered or country file edited here.

## §4 Invariants

- Preserve the exact waves resolver, including the module-exists/elif-YAML
  distinction, lowercase waves precedence, sorted order and SOURCE fallbacks.
- Module map is assigned even without waves/Waves, and without map a module
  retains an existing explicit map. YAML's truthy Waves branch assigns its
  map or a fresh empty dict on each waves access. Do not merge/clone/freeze maps.
- Explicit public assignment keeps the exact object and bypasses lazy work;
  later waves still overwrites it where the original resolver did. Mutation
  after resolution remains mutation of that object. No waves caching.
- No new metadata parser, folder inference, constructor eager work, source
  loader, dependencies, fingerprint exclusions, schema/ID/quantity policy.
- Runtime: exact prepared MAIN interpreter/cwd/PYTHONPATH/countries root,
  private initially empty nonsymlink data/L1/config, false credential gate,
  LSMS_SKIP_AUTH=1, unshare -rn, BLAS/OMP/MKL/build=1, pytest -n2 loadfile.
- Reuse accepted-fast_guard.py byte-for-byte and add strict fixtures/guard
  assertions. No actual Country/Feature constructor or country module, Wave
  data getter/build, source getter/decoder/Make/DVC/network/cache activity.
- Freeze code/tests before one focused invented selection. Preserve parent
  negative controls, errors and routine fixture corrections truthfully. No
  guard exception, broadened access or unknown-outcome retries.

## §5 Reuse decision

New functools.cached_property is the missing interface: resolve through existing waves
only when there is no public mapping in the instance dictionary, then retain
the original map or an empty mutable fallback. Its non-data descriptor leaves
ordinary public assignment/deletion in the same instance dictionary slot;
no setter or private backing field is needed. Remove the constructor's empty
placeholder; constructor remains lazy. The waves source is unchanged.
This avoids a second resolver, waves caching or changed loader precedence.
No scientific ruling is needed. New invented controls are necessary to test
direct-first access, source authority, exceptions, aliasing/overrides and
getitem transport; use frozen parent methods as negative/compatibility proof.

## §6 Open questions / remaining gates

No implementation blocker established. Stop if precedence, assignment/mutation
semantics or an actual country/module/source execution becomes necessary.
Hash footprint UNMEASURED; metadata census/full suite/native/CI/review/merge
remain ROOT-owned and unrun. Frozen operator/source/runtime identities and
validation evidence will live in the dispatch evidence directory.

### Phase 3

Pending frozen implementation and one guarded invented-focused selection.
