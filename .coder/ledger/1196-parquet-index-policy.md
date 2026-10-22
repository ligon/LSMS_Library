# Prior-Art Ledger - GH1196 parquet index string policy

**Search tier used:** ripgrep + git + installed-source inspection; no memory or
knowledge-graph tool. This ledger precedes the concrete private patch proposal.
Stage 1 permits this ledger commit and private design only: no product, test or
configuration edit, package import, payload decode, test, API or source staging.

## §1 Task, restated

Propose a generic `local_tools.get_dataframe` parquet-read repair that restores
the pandas string missing-value policy recorded for each physical index field.
Niger `food_acquired` and Uganda `cluster_features` independently retain
`numpy_type="string"` in Arrow pandas metadata but are reconstructed as the
NaN-policy `str` dtype. Both complete public indexes are MultiIndex; the `u`
and `v` diagnostics describe their extracted Index levels. Preserve values,
missingness, complete index structure and typed attrs. Root must inspect the
actual proposed patch before any product/test/configuration edit or data work.

## §2 Existing machinery

Source anchors are release f8d236694c26fe9e709c5548f02c70f74ea72c33.

| Symbol / evidence | Anchor | Existing role | Decision |
|-------------------|--------|---------------|----------|
| `get_dataframe` | `lsms_library/local_tools.py:1686`, parquet dispatch near1837 | sanctioned IO, pandas/pyarrow parser and fallback errors | extend only the successful parquet-read boundary |
| `to_parquet` | `lsms_library/local_tools.py:3225` | GH645 null coercion, index preparation, JSON attrs, two atomic writer branches | reuse unchanged |
| `map_index` | `lsms_library/local_tools.py:3840` | legacy index mapping after decode | reuse unchanged |
| Country country-cache gate | `lsms_library/country.py:4434` | freshness/coverage before finalization | reuse unchanged; insufficient as the only repair boundary |
| Country route selection | `lsms_library/country.py:5800` | Default country-cache path versus Make wave path | preserve both paths |
| Wave cache gates / finalization | `lsms_library/wave.py`, `lsms_library/country.py` | native-wave reads and canonical post-read processing | reuse unchanged |
| GH645 regressions | `tests/test_gh645_to_parquet_null_coercion.py` | real nulls versus literal null spellings and mixed objects | retain meaningful controls |
| cache metadata/atomicity regressions | `tests/test_cache_hash_invalidation.py` | writer metadata and failed-write safety | retain controls; do not redesign writer |

Prior tracked diagnosis ledger:
`isa-campaign/.coder/ledger/1196-index-roundtrip.md`, initial
468d9733701e1b1402dcc4d428b980830e0ce298 and clarification
859846b0a85627b67fca64841d1dfa563ca949ab. The original initial wording is
preserved; the latter corrects full-frame versus extracted-level classes.

Frozen diagnosis root:
`/local/job40255479/control/isa-backlog-20261021/shared1196-diagnosis`.
`READY-v4.json` SHA256
08a2aa8392cd21b4b6d4308523334303ed7b43d42cf767ab1e1bf56b1958832a;
281-entry `final-private-manifest-v4.json` SHA256
368e4997fd99d0982c6db54992fa19e8f6bea82271b28a1e2ec91e2d1afcd24c.
Use `safe-exports/final/{DIAGNOSIS.org,PROPOSAL.md,ROUTE-COVERAGE.md}` and the
separate release-v4 qualifications; never mutate or execute into that root.

## §3 Definitions and conventions in force

- Inherit `STANDING.md` §3/§4, `CLAUDE.md` and canonical `data_info.yml`.
- Follow the sanctioned generic reader/writer and current-hash/full-coverage
  cache gates; no stamp, schema lever, exclusion or assumed-fresh override.
- Current Niger CONTENTS SHA256
  b1bf5794b242c794437995eb76742db763951397d8dfdb691d064af569ef67ee
  and Uganda CONTENTS SHA256
  774d5bc13c35ca5a6d9543766648f88dbc1efe508d73aa388d5ac6e81146256c
  match the complete diagnosis-stage readings, including TODO/WAITING/LOGBOOK.
- Niger uses `grappe` and the 2014 `(GRAPPE,MENAGE,EXTENSION)` convention;
  split-offs and `OriginalLabel` unit dictionaries retain their documented
  meanings. Quote the tension: CONTENTS:12 says "each grappe is visited in
  exactly one =vague=", while:160 says both seasonal visits cover "half of
  every EA". This repair does not resolve sampling documentation.
- Uganda retains native EA/GPS-bin/parish eras, district/parish identity and
  missing Region caveats. CONTENTS:253 says sample Region "is complete for
  every household";:270 records three 2019-20 households with missing Region.
  The settled CONTENTS:2317 ruling says the 2018 label is wrong and "it IS a
  condition slot"; do not reopen that season/condition decision.

## §4 Invariants and assumptions

- Restore only recognized recorded `string` versus `str` missing policy on
  already-string reconstructed index fields. Preserve the storage already
  selected by the reader; metadata does not justify inventing storage.
- Bind physical `index_columns` descriptors to index positions, including
  unnamed or duplicate logical names. Preserve names, level values/dtypes,
  unused reconstructed levels, codes/code dtypes, masks, order and data columns.
- Preserve exact recursively typed full attrs at matching stages. Existing
  writer JSON record-to-dict conversion is separate and remains unchanged.
- Preserve RangeIndex, absent/malformed/unknown metadata and parser-failure
  behavior; no blanket types_mapper, broad cast or new exception swallowing.
- The writer's reset/set preparation already prunes unused source levels;
  do not promise to recover information lost before Arrow in this fix.
- Default observed calls reach the Country read gate. Both observed tables
  lack `materialize: make`; Make takes the wave route and bypasses that gate.
  Four saved Uganda Make endpoints establish the actual symptom there.
  Niger Make runtime remains unmeasured; route coverage is source-only so far.
- No country decode, finalizer, reducer, scientific factor, identifier, unit,
  dependency, writer or atomicity change. GH645 distinctions remain exact.

## §5 Reuse decision

| Behavior | Decision | Reason |
|----------|----------|--------|
| generic IO/parser | extend reader only | per-field metadata exists before the index reconstruction loses its policy |
| Arrow/pandas dtype metadata | reuse | distinguishes `string`/`str` fields without country or value guesses |
| strict comparator | reuse with source pins | check_exact=True, rtol=atol=0, complete axes and recursively typed attrs |
| freshness/build routing | reuse | repair must cover generic direct/wave/country reads, including Uganda Make |
| fingerprint inventory | measure later | diagnosis simulation predicts549 API/39 framework/1362 wave changes; not an actual patch census |
| real source acceptance | stage later after approval | Default+Make Uganda and Default Niger; Niger Make if held prerequisites permit |

## §6 Open questions and authorization boundary

Root authorized stage1 on2026-10-22: this ledger first, followed by PRIVATE
`proposed.patch` and `DESIGN.md`, without applying the patch. No new survey or
missingness policy is proposed. Concrete helper/malformed-metadata behavior
and regression matrix are for root review before implementation. Future actual
hash inventories must precede any approved scoped source build; no corpus rebuild.

### Phase3 verification

Pending implementation authorization. No product patch or data result is
claimed by this ledger. The frozen diagnosis's absent source-memory operands,
writer JSON boundary, static-only fingerprint predictions and sealing/guard
qualifications remain explicit in its pinned reports.

Sue
