# Prior-Art Ledger — GH #1723 Burkina Faso 2021-22 crop display aliases

> Per-task ledger. Inherits `STANDING.md`; cites it, `CLAUDE.md`,
> `lsms_library/data_info.yml` and `Burkina_Faso/_/CONTENTS.org` rather than
> re-copying them. Base `47df62d49e614c9a56e319def7324145d138c3d9`.

**Search tier used:** ripgrep + git (floor). GitNexus MCP did not connect
(`npx` absent) and `gh` is not installed on the node; the declared substitutes
were used. The shared `prior-art-ledger` skill file was not reachable from this
worker's home, so this ledger follows `.coder/ledger/TEMPLATE.md` and the
`1119-burkina-seed-crop-labels.md` precedent.

## §1 Task, restated

Ruling @ligon 2026-11-25, "Align all four crop labels" (GH #1723). In Burkina
Faso 2021-22 `crop_production` (script path, `2021-22/_/crop_production.py`),
the `crop` index level of native codes 3, 8, 9 and 27 is served `Riz Paddy`,
`Niébé`, `Voandzou`, `Laitue` instead of the s16c value labels `Riz`,
`Niébé/Haricot`, `Voandzou (pois de terre)`, `Laitue (salade)`. Display only:
no code, join, quantity, sale, `Derivation` key, raw answer, seed input, unit
or other wave changes. Unit code 3 (`Plat Yoruba`) and the #1740 plot-input
units are out of scope.

## §2 Existing machinery (this task's area)

| symbol | path | what it does | tested? | reuse / extend / new |
|--------|------|--------------|---------|----------------------|
| `derive_crop_code_label` | `Burkina_Faso/2021-22/_/mapping.py` | the registered selection of one code's label from the s16c / s16d label sets (`crop-code-from-roster-text`) | `test_crop_label_rule` | reuse, untouched |
| `_crop_label_table` | same | `{code: served label}`, through `harmonize_food`; the only place a 2021-22 crop label is made | same | **extend**: alias step between selection and `harmonize_food` |
| `_crop_tables` / `_raw_crop_frames` | same | feed the builder and all four `inputs_` helpers from that one table | `test_inputs_join_to_the_keyed_served_rows` (data-gated) | reuse; inherits the alias |
| `CROP_CODES_FROM_TEXT`, code-65 rule | same | 55/58/64 from roster text, 65 from s16d's label | `test_crop_labels_from_the_roster_text` | reuse, untouched; guarded against aliasing |
| `harmonize_food` | `Burkina_Faso/_/categorical_mapping.org` | country food/crop label table; 2018-19 crops pass through it | `test_burkina_faso_seed_crop_labels.py` | reuse, **not edited**: its `Riz` row is a food key shared with `food_acquired` |
| `harmonize_seed_crop` | same | seed slot -> crop word (`Riz Paddy`, `Niébé`; GH #1119) | same | reuse, not edited |
| `2018-19/_/crop_production.py:_crop_labels` | 2018-19 builder | maps its own labels through `harmonize_food`, unmapped pass through | GH #824 tests | precedent for ordering (alias, then `harmonize_food`) |

Call-site sweep (`rg`): `_crop_label_table` is called once (`_crop_tables`);
`derive_crop_code_label` once; the three distinctive old words occur in no test
or library file outside this wave's `mapping.py` comment and `CONTENTS.org`.

## §3 Definitions & conventions in force

- "THE IDENTITY OF A CROP IS ITS CODE": `2021-22/_/mapping.py` block comment and
  `CONTENTS.org` "2021-22 crop_production" (2026-11-13), incl. the label table
  this alias is bound to.
- A derivation key names an operation that computes a value; a source label of
  the same code carries none (code 65 precedent, `_/derivations.yml`
  `crop-code-from-roster-text`). `CLAUDE.md` §"Derived Values".
- Seed crops are spelled as harvest crops are: `CONTENTS.org` GH #1119 entry.
- A refusal subclasses `Exception`, not a builtin: `CLAUDE.md` §"A Failure Must
  Not Read as Absence".

## §4 Invariants & assumptions

- The alias must never reach a join: the builder matches, attaches and stamps on
  the code and writes the label last (`crop_production.py`, pinned by a static
  source-order test).
- ASSUMED, not measured here: the held `s16c_me_bfa2021.dta` decodes 3, 8, 9, 27
  to byte-exactly the four left-hand words, and s16d labels them with the
  right-hand words (source: the 2026-11-13 `CONTENTS.org` table). If the first
  fails the build refuses (`CropAliasError`); it cannot silently serve the old
  word.
- ASSUMED: 2018-19 serves exactly `Riz Paddy`, `Niébé`, `Voandzou`, `Laitue`
  (`Niébé` and `Riz Paddy` are corroborated by the #1119 counts; `Voandzou` and
  `Laitue` rest on the issue table).
- `mapping.py` and the wave script are build inputs: one Burkina re-warm.
  `derivations.yml` and `CONTENTS.org` are not.

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| crop label selection | reuse `derive_crop_code_label` | registered function; ruling does not change selection |
| the four served words | **new** literal `CROP_DISPLAY_ALIASES` + `_crop_display_aliases` in the wave `mapping.py` | wave-scoped and code-bound; `harmonize_food` would leak to food tables and other waves; no framework rule added |
| refusal | new `CropAliasError(Exception)` | binding and many-to-one guard |
| derivation registry | no new key | no value computed; served word is a source label of the same code |

## §6 Open questions for the human

- Unit code 3: `Plat Yoruba` vs `Plat yorouba` (unit-only residual of #1723).
- Confirm "no registry key for a display alias" (flagged to the coordinator).

---
### Phase 3 — verification

- `CROP_DISPLAY_ALIASES` — OK (anchored on §1, §3): exactly the four ruled pairs.
- `_crop_display_aliases` — OK (§4): bound to code and decoded label; refuses a
  third word and any many-to-one fold; 55/58/64/65 excluded by guard.
- `_crop_label_table` — OK (§2): selection, alias, `harmonize_food`, in the
  2018-19 order; post-`harmonize_food` fold guard.
- `derive_crop_code_label`, `CROP_CODES_FROM_TEXT`, builder joins — unchanged.
- Verification run: source-free only, 78 passed / 18 data-gated skipped across
  `test_burkina_faso_2021_crop_production.py`, `test_derivations.py`,
  `test_burkina_faso_seed_crop_labels.py`,
  `test_gh1509_build_time_food_api_dependency.py`. No build, no source read, no
  hash census: all three are owed to an independent source stage.
