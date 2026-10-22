# Prior-Art Ledger - GH1215 Uganda empty-column order

**Author:** Sue (GPT-6). **Phase:** static proposal only; no implementation is authorized.
**Source parent:** `a417b69d71c460ab33f6a494b4df5d67cd792de5`.
**Search tier:** direct source reads, ripgrep, git, and stdlib AST/text inspection.
No GitNexus or memory connector is available in this session.

## §1 Task, restated

Diagnose why the existing typed-empty country response can enumerate required
columns before optional columns while Uganda's native/covered-cache response
uses another order. Establish the existing contract and all affected callers,
then propose a bounded boundary and meaningful checks. This is a Tier C
schema-order proposal, not authority to change production, schema, or tests.

## §2 Existing machinery (initial grounding; source anchors to be resolved)

| machinery | source authority | role | coverage status | decision |
|---|---|---|---|---|
| `Country._empty_declared_frame` and every caller | `country.py`, named by the root brief | existing typed-empty constructor | inspect GH1099 tests before proposing changes | reuse/possibly extend |
| Required/optional declared-column enumeration | `data_info.yml`, country `data_scheme.yml`, `country.py` | schema membership and existing ordered declarations | inspect static definitions and tests | reuse |
| Raw-to-public projection and `_finalize_result` | `STANDING.md` §2/§4; `country.py` | post-read coercion, metadata, ID walk and sample join | existing integration surface; no runtime in this phase | reuse |
| Uganda native crop assembly | `Uganda/_/CONTENTS.org`, final GH1204 section | requested native waves on Default and Make | original 1204 strict residuals remain failures | reuse |

## §3 Definitions & conventions in force

- Inherit `STANDING.md` §3 and `CLAUDE.md`; canonical schema is
  `lsms_library/data_info.yml`. Do not invent a column-order contract.
- Uganda `CONTENTS.org`: "A shared axis NAME is still not a shared VOCABULARY."
  Crop `plot_id` contains native household/parcel/plot identity, while area IDs
  have a different vocabulary. Keep order diagnosis separate from ID repair.
- The native crop index retains `production_obs` after `season` (GH1198),
  condition, unit and actual source-season detail. A/B denote modules, not
  universally the calendar's first and second seasons.
- `KgFactor`, `Unit_sold`, and `Condition_sold` are optional ordinary columns;
  missing sold labels are NA-bearing rather than invented index sentinels.
- GH1204 documents 2005-06 as genuinely barren and the existing typed-empty
  response as preceding a country-cache rebuild/write.

## §4 Invariants & assumptions

- Inherit `STANDING.md` §4. No raw reads, package imports, API calls, tests,
  builds, data copies, acquisition, cache/venv operations, or runtime replay.
- Only this ledger and the assigned private prefix may be written.
- Preserve dtype/index names, optional membership, attrs, native grain and all
  pre-existing failure evidence. Stable column order alone is not full-frame
  equality or cold/warm equivalence.
- Quote contradictory documentation as evidence; do not silently replace it.
  The notes say Region is "complete for every household" and separately
  document three missing Regions in 2019-20. The notes also retain historical
  reducer/factor/cache statements and explicitly dated superseding sections.

## §5 Reuse decision

| quantity | decision | reason |
|---|---|---|
| Column identity/order authority | reuse | trace existing ordered declarations/writers before proposing policy |
| Empty dtype/index construction | reuse/possibly extend | existing constructor and GH1099 are the regression boundary |
| Metadata/Feature verification | reuse | propose bounded existing surfaces; no new public grain or oracle |
| Static census | new private analysis only | enumerate source declarations/callers without importing the package or parsing YAML tags |

## §6 Open questions for root

- Which ordering is already promised by contract/tests, and where do native
  writers intentionally differ from declaration order?
- Would preserving original declaration order be a coherent empty-only rule
  across all callers, including optional-only/no-column/index declarations?
- What bounded parent-failing controls and healthy metadata/Feature checks are
  needed before a separate implementation release?

### Phase 3 - verification

Pending static diagnosis. This initial grounding ledger is committed before
nontrivial source analysis; the final ledger will cite exact source anchors.
