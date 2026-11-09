# Prior-Art Ledger — GH #1531 Guinea-Bissau GPS area unit diagnosis

**Source pin:** `6d9563f7aac1e65e10e98b002ae935f1bd84c266`.
**Search tier:** git + ripgrep + source inspection. No GitNexus tool is
available. This is a bounded country diagnosis, not a Feature audit or repair.
Inherited baseline: [STANDING.md](STANDING.md), especially §§3–5.

## §1 Task, restated

Trace Guinea-Bissau's own `s16aq47` GPS-area binding, farmer estimate/unit,
and measurement flag through the pinned declarations and helper. Inspect
headers/cards of the one admitted private 2018-19 s16a copy after sealing it;
2021-22 s16a is missing and may not be fetched or searched for. Establish
whether explicit source metadata supplies a GPS unit discriminator. Do not
infer units from magnitudes or the farmer's unit, classify mixed rows, change
the 1000 ha clamp, or construct a real table.

## §2 Existing machinery

| machinery | source anchor | role / existing verification | decision |
|---|---|---|---|
| `plot_features_for_wave` | `lsms_library/countries/Guinea-Bissau/_/guinea_bissau.py:56` | GPS-first farmer fallback; own native keys; static inspection here, no direct own helper test located | cite only; do not execute |
| wave driver `colmap` | `lsms_library/countries/Guinea-Bissau/2018-19/_/plot_features.py:26` | binds `s16aq47`, `s16aq45`, `s16aq09a/b`; full driver forbidden | reuse declarations as evidence |
| `i` | `lsms_library/countries/Guinea-Bissau/_/guinea_bissau.py:21` | own `(grappe, menage)` household formatting | preserve; no manufactured keys |
| `get_dataframe` | `lsms_library/local_tools.py` (STANDING §2/§4) | sanctioned numeric reader; may fetch/parse full data | UNRUN unless the charter's conditional numeric gate is satisfied |
| Stata header interface | mounted pandas `StataReader.variable_labels/value_labels` | one metadata parse captured 301 fields / 36 value-label sets; every recorded `lines_read` = 0 | reused; no `read`, `get_chunk`, iteration or `categories_only` |
| canonical plot schema | `lsms_library/data_info.yml:103,604–611` | `plot_id`, `Area` hectares, `AreaUnit` original survey unit | cite; no schema changes |
| derivation registry | `lsms_library/countries/Guinea-Bissau/_/derivations.yml` | two entries, food and community prices; no plot-area entry | report absence; do not register a new construction |

## §3 Definitions & conventions in force

- Country identity stays explicitly Guinea-Bissau; no other country's
  authority or plot syntax can determine this source's units.
- `CONTENTS.org`, “plot_features” (lines 428–452), defines native
  `plot_id = "{field_no}_{parcel_no}"` from `s16aq02` / `s16aq03` and
  identifies Portuguese value labels with integer-code mappings. `s16aq47`
  is an area, not a coordinate. These are actual country idiosyncrasies.
- Canonical `Area` is hectares (`lsms_library/data_info.yml:604–607`).
  The helper's claim that GPS is “already hectares” is a claim to check
  against this source's metadata, not an independent instrument authority.
- Preserve the exact question labels, typed value-label codes and links,
  storage types/formats and field positions before any interpretation.
- Own admitted 2018 source cards distinguish two GPS fields: bound `s16aq47`
  asks “16A.47. Quelle est la superficie de la parcelle selon les mesures GPS ?”
  with no unit or value-label link; unbound `s16aq47a` asks “16A.47a. Qual é a
  área da parcela (em m2) de acordo com as medidas GPS?”. The explicit m2
  authority belongs to the latter. It does not establish the former's unit,
  their relationship or row/vague applicability. Both are doubles.
- `S16AQ09B` is linked only to farmer-unit field `s16aq09b`: typed codes
  1 “Hectare (Ha)”, 2 “Metro quadrado (m2)”. `s16aq45` links to a different
  codebook, `S16AQ45`: 1 “Sim”, 2 “Não”, for GPS measured. Neither that flag
  nor reason-for-nonmeasurement `s16aq48` assigns a unit to bound `s16aq47`.

## §4 Invariants & assumptions

- STANDING §4 IO invariants apply; the dispatch charter makes the narrow
  exception for metadata-only headers on the sealed private copy. Never
  read the original shared DTA or invoke acquisition, DVC, Make data builds,
  Country/Feature/table APIs or native producers.
- Full own `CONTENTS.org` read, including its TODO declaration and all dated
  notes; no separate LOGBOOK/WAITING body is present. Its one-vague-per-grappe
  question is explicitly settled by the later community-price note; do not
  reopen it.
- Record, rather than silently reconcile, the text “Single EHCVM wave
  =2018-19=; there is no 2021-22 wave” (CONTENTS lines 435–436), the later
  “2021-22 held and registered, not wired” note, and the charter's
  “currently wired waves2018-19/2021-22”. At this exact pin the 2021-22
  `_/data_info.yml` contains only `Country` and `Wave`, and `git ls-tree`
  shows no 2021-22 plot driver. Diagnose both declared source identities,
  but do not invent a second-wave binding or edit the wiring.
- 2018 admission: MD5 `39a00532332e724fa9c50bf206c58334`, 6,531,988 bytes;
  private staged SHA256 `6c19d3be29bc07f3d29752e425c5a2afc5489f38c3ce674bb647e137429f1328`.
  2021 declaration: MD5 `00d948d16d402ebf8b60fa6c03763705`, 5,594,481 bytes,
  MISSING per supplied staging manifest. Do not fetch or broaden the hunt.
- Numeric read gate: first identify a unit/measurement flag and freeze the
  fields and exact unresolved question. If labels suffice, or no GPS unit
  discriminator exists, no numeric rows may be read. No ratios, thresholds,
  sample intersections, yields, trimming, medians or missing-to-zero/No.
- Keep raw identifiers/operands owner-only. Publish field names and metadata
  conclusions only. No product/config/schema/default/unit changes.
- Use the mounted interpreter, exact checkout/PYTHONPATH/countries root,
  private config/data/DVC site cache, absent credentials and single BLAS /
  OMP / MKL / build workers. Preserve HOME/XDG/CODEX/SDK guards. No subagents.

## §5 Reuse decision

| quantity | decision | reason |
|---|---|---|
| binding trace | reuse static source declarations | executing the producer is unnecessary and forbidden |
| units / measurement semantics | reuse own admitted Stata cards and committed own instrument text | units cannot be estimated from magnitude |
| metadata serialization | new external evidence only | preserve typed dictionaries with pickle or typed records; plain JSON object keys would coerce numeric codes |
| numerical discrimination | UNRUN; gate not satisfied | no identified flag assigns units to bound `s16aq47`; co-occurrence with the separate m2 field would not supply authority |
| repair / row classification | out of scope | diagnosis grant supplies no authority for a default change |

## §6 Open questions for the human

- The original charter incorrectly called both waves wired. This was reported
  in the absolute shared questions channel; the coordinator's subsequent
  “COORDINATOR SCOPE ERRATUM” explicitly accepts the finding and calls 2021
  an “unserved holding only”. No 2021 header or serving claim was invented.
- Exact missing authority: own 2018 household instrument section 16A Q47/Q47a
  and official export/cleaning definitions establishing each field's unit,
  relationship and applicability by vague. Declared own documents are
  `2018-19/Documentation/gnb_ehcvm1_qnr_household_excel_vague{1,2}.xlsx`;
  both sidecars declare MD5 `0f3d80b68800a577659f5ead1032cdf9`, 673,399 bytes.
  These are declarations, not document-byte or question-content verification;
  no instrument bytes are admitted in this packet. Do not infer two different
  fielded versions from their filenames. No routine approval is pending.

### Phase 3 — header-phase verification (frozen before scope extension)

The first new commit, `8a6ac2ad199b59df297546a7d9a9cc8e1172c597`, contains
only this ledger and was surfaced before source metadata reads. All five exact
required evidence filenames were initialized before long reading. The country
documentation freeze is separate commit
`62e450c130b4a5c07364258b41404092fb509e1c`; this final ledger update is its
own commit. Final SHA and detailed command records live in the external packet
(`report.md`, `RESULT.json`, `verification-plan.json`, `ledger-checkpoint.md`,
`source-byte-proof.json`). Private publication is by the prepared commit hook.

- `StataReader` metadata reuse — **OK (anchored on §§2–5)**. Exactly one
  admitted existing private copy, pre/post-all MD5/SHA256/size/mode/inode/mtime
  unchanged; shared original DTA never read. 301 fields and 36 value-label
  dictionaries preserved in the original typed pickle, including NumPy int32
  keys; typed JSON records preserve code bytes and dtypes. Header nobs 9,873
  does not mean rows read. `lines_read` stayed 0 at construction and after
  both metadata methods. Numeric `get_dataframe` calls: 0.
- Unit diagnosis — **OK (anchored on §§3–5)**. Explicit m2 authority exists
  for the distinct `s16aq47a`; unit authority for bound `s16aq47` remains
  missing in admitted metadata. No mixed-unit verdict or forced repair.
- Documentation update — **OK (anchored on §§3–4)**. Records actual field
  distinction, exact missing authority, and registered-but-unwired 2021;
  preserves prior claims as quoted history. No executable/config/schema/unit/
  default changes. No new production helper or tests; existing tests unchanged.
- Independent evidence controls — **OK (anchored on §§3–4)**.
  `PROJECT_PYTHON evidence/verify-evidence.py` exited 0: 11 controls including
  reconstruction of typed codes, distinct unit vs measurement codebooks,
  positive explicit-m2 field, zero reader counters, own native bindings, nine
  unchanged production/config files, post-all seals and bounded changed paths.
- Failures preserved: `make api-audit STRICT=1` exited 2 because Poetry is
  unavailable (recipe command error 127); authorized exact fallback
  `PROJECT_PYTHON scripts/api_surface_audit.py --strict` exited 0 (10 kwargs,
  15 listed skills, no untracked hashed build inputs). Initial header script
  exited 1 while rendering NumPy int32 to JSON, after preserving its typed
  pickle. Recovery rendered that pickle with typed records, exited 0, and
  did not reopen the DTA. No failed result was relabeled as passing.
- UNRUN: 2021 actual header (missing admitted source, no fetch/hunt);
  numeric rows (conditional gate unsatisfied); instrument document bytes
  (not admitted); real tables/native producer/Make data builds; scientific
  unit acceptance; fingerprint/performance measurement; full CI / warm cache
  acceptance. Runtime import identity passed with the prepared interpreter,
  exact cwd/PYTHONPATH/countries root and private roots/absent credentials.

No ledger contradiction or reinvention was found in the bounded diagnosis.
The helper's hectares assumption is unverified, not disproved by assigning the
separate m2 field's authority to it. Root retains independent integration,
review, CI, GitHub and merge responsibility.

### Authorized numeric extension checkpoint — 2026-11-09

After the header-only closure, the absolute steering channel added an explicit
ROOT grant: “characterizing DISTINCT explicitly m² s16aq47a itself, its
presence/missingness and s16aq45 flag/known farmer-unit relation”. This extends
the conditional gate; it does not transfer the m2 authority to `s16aq47`.
The completed header-only evidence (including zero reader counters and original
failures) is preserved in `evidence/header-phase-closure/` externally.

Before any numeric read, `numeric-read-decision.json` freezes one sanctioned
`get_dataframe(..., convert_categoricals=False)` call on the already-admitted
private 2018 copy. Retain every source row/column/index/position/dtype/attr and
all missing masks before analytic copies. Keep the source and retained originals
immutable and sealed. The question is the distinct explicit-m2 field's
presence/missingness by literal `vague`, GPS measured flag and farmer unit,
including unknown/missing flags unchanged. The analysis fields are `vague`,
`grappe`, `menage`, `s16aq02`, `s16aq03`, `s16aq45`, `s16aq47`, `s16aq47a`,
`s16aq09a`, `s16aq09b`, `s16aq48`; all other columns remain retained.

No sample intersection, ratios, thresholds, medians, p90, yield, trimming,
missing-to-zero/No, ambiguous-q47 unit inference or product/default/clamp edit.
Disclose the actual added read and measured parse wall time. Own instrument /
export authority remains the priority; coordinator is checking only own held
questionnaire declarations without acquisition. Any q47a-source proposal is for
ROOT review only, with its own explicit m2 authority and no q47 unit borrowing.
2021 remains missing/unserved/unread. Numeric extension verification is UNRUN
at this ledger-only checkpoint; no additional permission is pending.

### Own instrument extension checkpoint — 2026-11-09

The manager subsequently admitted ONE private read-only own2018 questionnaire
workbook, shared by both committed vague declarations: MD5
`0f3d80b68800a577659f5ead1032cdf9`, 673,399 bytes. The absolute steering
channel names the exact private copy and manifest (SHA256
`dc9aeace332700279ac97fa9ecde974a3850997202dc370f4399773b979c8aad`).
Inspect only the own16A GPS47/47a, measurement flag, farmer area/unit wording
and export identifiers, with pre/post seals and existing tooling. No download,
other-country authority, product/default/clamp edit or independent-two-forms
claim. Instrument reading is UNRUN at this checkpoint; no permission is pending.

The previously authorized SINGLE `get_dataframe` numeric call is complete,
exit0: all9,873 rows and301 columns, index/source positions/dtypes/attrs and
all301 missing masks retained in an immutable owner-only pickle before copies.
The admitted DTA and retained original are unchanged. Additional call wall time
was0.4138639532 seconds (not an internally instrumented parser-call count).
`s16aq47a` is present on1,191 rows, all literal vague2/measurement flag1 and all
also holding `s16aq47`; q47-only3,612, neither5,070. Flag2 and13 missing flags
are kept distinct. These are presence counts only, not q47 unit inference.
No header/cards/11-control rerun; the original zero-reader-counter header phase
remains preserved as history. Instrument and final supplementary verification
remain to be completed before a new closure.
