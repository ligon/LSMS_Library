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
| Stata header interface | mounted pandas `StataReader.variable_labels/value_labels` | charter explicitly admits metadata-only access; counter must remain zero | reuse; no `read`, `get_chunk`, iteration or `categories_only` |
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
| numerical discrimination | deferred, conditional | only if an identified metadata flag leaves a specific source question unresolved |
| repair / row classification | out of scope | diagnosis grant supplies no authority for a default change |

## §6 Open questions for the human

- Nonblocking discrepancy: the charter calls 2021-22 wired, but this pin has
  registration only. Record it in the absolute shared questions channel;
  proceed with the independent 2018 header diagnosis and 2021 static limitation.
- Any missing GPS unit authority will be named as an exact missing question /
  document, not filled with a guessed conversion. No routine approval is pending:
  the charter already authorizes bounded work after this surfaced checkpoint.

### Phase 3 — verification

UNRUN at the ledger-only checkpoint. All five required evidence files were
initialized before long reading. `make api-audit STRICT=1` exited 2 because
Poetry is unavailable (recipe command error 127); the authorized exact fallback
`PROJECT_PYTHON scripts/api_surface_audit.py --strict` exited 0: 10 documented
kwargs, 15 listed skills, no untracked hashed build inputs. No source metadata
or numeric rows have yet been read. Final verification will distinguish held
2018 metadata, missing 2021 metadata and every UNRUN scientific/integration gate.
