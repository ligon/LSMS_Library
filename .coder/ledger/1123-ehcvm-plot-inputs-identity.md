# Prior-Art Ledger - #1123 EHCVM plot_inputs identity (Niger, Senegal, CotedIvoire)

**Search tier used:** ripgrep + git floor. The gitnexus MCP server did not
connect this session (CLAUDE.md "Code intelligence" substitutes used).
Runtime baseline: development `9a0ed5f3c`. Burkina Faso's equivalent repair
(PR #1189, `2193be000`) is the governing precedent.

## §1 Task, restated

`plot_inputs` is a script-path table (`materialize: make`) declared at
`(t, i, input, crop, u)` in Niger, Senegal and CotedIvoire (each
`_/data_scheme.yml`). One native source line is one household x input slot of
the agricultural-input roster (`s16b` in EHCVM; `AS02C` in Niger's ECVMA
waves). Where two native lines land on one declared tuple,
`_normalize_dataframe_index` selects one observed row and the rest vanish
(GH #323). Diagnose every such collision from the held source; where the
cause is the Burkina one (a non-injective `harmonize_seed_crop`), restore the
identity and guard the wave output before writing. Preserve every native
record and its payload; never select among conflicting rows.

## §2 Existing machinery

| symbol | path:line | what it does | tested? | reuse / extend / new |
|---|---|---|---|---|
| Burkina seed-crop split | `Burkina_Faso/_/categorical_mapping.org:637-638` | `Semences d'autres céréales -> Autre céréale`, `Plants/boutures de tubercules -> Autre tubercule` | `tests/test_burkina_faso_plot_inputs_identity.py` | reuse labels verbatim |
| Benin/Togo split (origin) | `Benin/_/categorical_mapping.org:281-282`, commit `36170a57b` | same two cells | `tests/test_gh323_benin_togo.py` | reuse |
| Pre-write guard, count-only | `Burkina_Faso/2018-19/_/plot_inputs.py:140` | assert declared index unique immediately before `to_parquet`, aggregate message | write-spy tests | reuse form; Benin's form (`Benin/2018-19/_/plot_inputs.py:149`) prints keys, not reused |
| Unlabelled-code token | `Burkina_Faso/_/categorical_mapping.org` (`21 -> Unknown [Burkina_Faso 2018-19 s16bq01=21]`) | source-qualified identity fallback, used only because NECESSARY there | yes | NOT needed here (§5) |
| Niger seed/label helpers | `Niger/_/niger.py:626-700` | `_input_maps`, `_seed_crop_labels`, `_finish_plot_inputs` | indirectly | reuse unchanged (PR #1227 also edits niger.py) |
| Senegal/CIV inline helpers | `Senegal/2018-19/_/plot_inputs.py`, `CotedIvoire/2018-19/_/plot_inputs.py` | self-contained clones | indirectly | reuse unchanged |
| Core collapse/audit | `country.py:_normalize_dataframe_index`, `most_complete_row`, `_audit_index_collapse` | selects one observed row; reports the loss | yes | unchanged |
| Transforms | `transformations.py:nitrogen_kg` (5485), `seed_kg` (5615) | household-grain kg sums over `plot_inputs` | yes | unchanged; measured before/after |

## §3 Definitions in force

- #323 doctrine (CLAUDE.md "Grain Collapse"): duplicates on a declared index
  mean the identifier is broken or a level is missing; fix the index, never
  declare a reducer. `aggregation:` is dead config.
- `plot_inputs` grain per each country's `_/data_scheme.yml`: household x input
  (EHCVM has no plot), `(t, i, input, crop, u)`; `crop` carries the seed's crop
  from `harmonize_seed_crop`; non-seed rows carry `(not crop-specific)`.
- No `crop` / `input` table exists in these three countries'
  `categorical_mapping.org`, so the wave script's crop labels are final at read
  time; only `u` is relabelled on read.
- `Purchased` / `Quantity_purchased` are existing API bindings to `s16bq05` /
  `s16bq07a` (gift fields) in all three countries -- #1188's Burkina finding
  applies by inspection; out of scope here and NOT changed.

## §4 Invariants

- Every applied native line `(household, input slot)` is unique and non-null in
  all six source files (measured: 0 duplicate, 0 null).
- After the fix each EHCVM wave's declared index must be unique before
  `to_parquet` (guard), and the served table must keep every native line
  except the all-NA hollow rows removed by `_finalize_result`'s
  `dropna(how='all')` (Niger 2021-22: 4; zero elsewhere).
- Quantities, units, `input` classification, household ids and the core
  reducer are unchanged. Only `crop` moves, and only on code-15 rows
  (`Semences d'autres céréales`) and, in Niger 2021-22 / Senegal, code-19
  rows (`Plants/boutures de tubercules`).
- No guard in Niger 2011-12 / 2014-15 or in `niger.py:_finish_plot_inputs`:
  those ECVMA waves still collide (§6) and a guard would fail the build.
- `categorical_mapping.org` is a country build input: every table hash of the
  three countries moves (three whole-country re-warms); no framework
  fingerprint or other country should move.

## §5 Reuse decision

| country | cells changed | why (precedent) |
|---|---|---|
| Niger | `Semences d'autres céréales -> Autre céréale`; `Plants/boutures de tubercules -> Autre tubercule` | 2018-19 collides on slots (15,19); 2021-22 on (15,20) and (19,20). Benin/Burkina labels. |
| Senegal | same two | 2018-19 collides on (15,20) and (19,20). |
| CotedIvoire | `Semences d'autres céréales -> Autre céréale` only | all 9 collisions are (15,19); `Plants/boutures` has 0 rows (2018 label set stops at 19). Burkina kept unused cells unchanged. |

Niger 2018-19's unlabelled code 20 (382 applied rows) stays `Autre crop`: with
the two splits it collides with nothing (3 households carry both 19 and 20,
all in different units), so Burkina's NECESSARY third cell has no analogue.
Its map comment, which borrows 2021-22's code-20 meaning, is corrected to the
measured facts and the meaning is flagged as unverified.

## §6 Open questions for the human

- **Niger ECVMA (2011-12, 2014-15) is different in kind and NOT fixed.** The
  seven `AS02CQ02` seed slots (11-17) all carry the label `semences`; a
  household that names the same crop in two slots in the same unit lands
  twice on one key (2011-12: 67 groups / 73 lost lines; 2014-15: 39 / 45).
  The identity dropped is the slot ORDINAL, which no label vocabulary carries.
  Candidate repairs: add a slot level to the index (schema decision), a
  slot-qualified token on `crop` or `input` (vocabulary decision, collides
  with #1119), or leave it as reported loss. Niger CONTENTS calls this "a
  harmonisation-level collision on the input axis, not an identifier defect"
  (GH #614 / #637); CLAUDE.md's #323 doctrine says duplicates mean "a level is
  missing". Not adjudicated here.
- Niger 2018-19 code 20's meaning (no value label) is unknown; it keeps its
  legacy `Seed` / `Autre crop` binding.

---
### Phase 3 -- verification

(filled at task end)
