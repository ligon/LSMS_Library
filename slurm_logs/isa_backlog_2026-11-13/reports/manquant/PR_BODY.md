
## Togo / Senegal / Côte d'Ivoire / Niger: `Unknown` missing input unit, EHCVM purchase columns, Senegal seed crop names and `plot_features` refusal

Addresses #1118, #1119 and #1287. All three are partial; other countries remain in each.

### Changes since 1d89815f0 (for the delta review)

**Ruling (@ligon, 2026-11-13): "Extend to all four now (Recommended)".**

Senegal 2018-19, Côte d'Ivoire 2018-19, Niger 2018-19 and Niger 2021-22 now read `s16bq08` / `s16bq09a` / `s16bq09b` as the purchase, on the Togo template `408d02a24`. The gift answers (`s16bq05` / `s16bq07a` / `s16bq07b`), which `Purchased` / `Quantity_purchased` used to serve, go into `Received_as_gift` / `Quantity_received_as_gift` / `Unit_received_as_gift`. Each unit is decoded from its own question's value labels and served through `ehcvm.served_unit_labels`.

| commit | content |
|---|---|
| `fa9feefa79e9` | Senegal 2018-19: the wave script, the scheme comment, CONTENTS, and the `test_senegal_1483_s16b.py` re-pin |
| `55d60b6695aa` | Côte d'Ivoire 2018-19: the wave script, the scheme comment, CONTENTS. This closes the 2021-22 WAITING note. |
| `cbc3eb83c83d` | Niger 2018-19 and 2021-22: both wave scripts, the four columns declared `optional` in `_/data_scheme.yml`, CONTENTS |
| `14606914c8ac` | `tests/test_ehcvm_plot_inputs_identity.py` re-pinned for all four cells: the oracle reads the purchase and the gift, and the synthetic frames carry the new columns |
| `63aa569cad2d` | CONTENTS served measurements; `test_gh1500` re-pins Senegal's scheme digest (comments only) |

**Measured.** Cold, private roots, previous head `1d89815f0` against the candidate `14606914c` (burst job 41280722). Rows are conserved and the index is equal in every cell.

| cell | served rows | `Purchased` True, before → after | `Quantity_purchased` non-null | gift × purchase cross-tab (Non/Non, Non/Oui, Oui/Non, Oui/Oui) |
|---|---|---|---|---|
| Senegal 2018-19 | 9,916 | 536 → 4,956 | 533 → 4,951 | 4,504 / 4,876 / 456 / 80 |
| Côte d'Ivoire 2018-19 | 11,259 | 890 → 8,134 | 890 → 8,134 | 2,373 / 7,996 / 752 / 138 |
| Niger 2018-19 | 13,401 | 1,383 → 4,557 | 1,383 → 4,557 | 7,602 / 4,416 / 1,242 / 141 |
| Niger 2021-22 | 12,435 | 856 → 3,593 | 856 → 3,593 | 8,052 / 3,522 / 785 / 71 (+ 9 blank on both) |

- **The old values are preserved.** In all four cells the new gift columns equal the old purchase columns row for row.
- **Only the acquisition columns move.** No other wave changes; Niger's ECVMA waves serve the four new columns as NA.
- **Hollow lines unchanged.** Niger 2021-22 still drops its 4 hollow lines; 5 served lines keep both flags NA.
- **Kilograms and assembly.** `seed_kg` and `nitrogen_kg` are frame-equal, and so are Senegal's `crop_production` and `plot_features`. `Feature('plot_inputs')` equals the Country table on every value column.
- **Incomplete answers, served as filed:**
  - Senegal: 5 purchases have a unit but no quantity; 3 gifts have no quantity, 1 of them with a unit.
  - Niger 2021-22: 1 gift quantity has no unit.
- **Unit vocabulary.** The served `Unit_*` labels are `u` labels, except Niger 2021-22's `Charrette Bovine` / `Charrette Asine` (74 purchase lines), which the purchase question capitalises.
  - Not folded: it needs a `u` row in `_/categorical_mapping.org`, which is a build input of every Niger wave.
  - Senegal 2021-22's adapter serves raw `Kilogramme` / `Charrettée` beside 2018-19's `Kg` / `Charrette`; that is left to the #1483 adapter residual.
- **Niger's shared tail.** `niger._finish_plot_inputs` is not edited. The two EHCVM scripts attach the four columns after it, by position, with the line count asserted, so the ECVMA waves' hashes do not move.
- **Hash census at the head `63aa569ca` against `f138a3d61`:**

  | kind | moved |
  |---|---|
  | `build_transforms_fingerprint` | 0 / 42 |
  | `framework_imports_fingerprint` | 0 / 38 |
  | `Country._table_cache_hash` | 91 / 555 (Togo 22, Senegal 23, Côte d'Ivoire 21, **Niger 25 / 25**) |
  | `Wave._input_hash` | 160 / 2,321 (Togo 22, Senegal 46, Côte d'Ivoire 42, **Niger 50 / 100**: 2018-19 and 2021-22; 2011-12 and 2014-15 do not move) |

  Against `1d89815f0`, the delta moves Senegal 23 + 23, Côte d'Ivoire 21 + 21 and Niger 25 + 50, and nothing else. The head moves nothing against `14606914c`.
- **Tests.**
  - At `14606914c`: the identity, Senegal, Côte d'Ivoire and Togo tests, the #962 sweep, schema, build-hash, derivation and Feature-index tests, and all Niger test files. 1,930 passed and 1 failed; the failure was the `test_gh1500` digest, re-pinned in `63aa569ca`.
  - At the head: `test_gh1500`, `test_ehcvm_plot_inputs_identity` and `test_senegal_1483_s16b`, 137 passed.
  - No in-tree parquet was written.

### What the whole PR changes

**EHCVM `plot_inputs`: the purchase, not the gift.** This follows the rulings "Fix all three now" (Togo here; Benin and Guinea-Bissau in other streams) and "Extend to all four now" (Senegal 2018-19, Côte d'Ivoire 2018-19, Niger 2018-19 and 2021-22).
- `Purchased` / `Quantity_purchased` read `s16bq08` / `s16bq09a`, and `Unit_purchased` reads `s16bq09b`. The file's own labels show `s16bq05` / `s16bq07a` are the gift question.
- The gift answers go into the three gift columns.
- `Quantity_purchased` is in `Unit_purchased`, not in `u`. `u` still describes the quantity used.
- **Togo 2018:**
  - `Purchased` True goes 804 → 7,128. 725 gifts that were never bought had been served as purchases, and 7,049 purchases as not bought.
  - 6 purchase quantities and 1 gift quantity have no unit.
  - The 2018 quantity questions are worded differently from 2021-22's.
  - The old answers survive row for row in the new gift columns.

**#1118: a blank used unit is `Unknown`, not the minted `Manquant`.**

| cell | rows relabelled |
|---|---|
| Togo 2018 | 9 |
| Senegal 2018-19 | 0 |
| Côte d'Ivoire 2018-19 | 0 |
| Côte d'Ivoire 2021-22 | 4 (a site the issue's inventory missed) |

No `s16bq03b` value set has a missing-marker label, so there is no raw answer to keep in a column of its own.

**#1119, Senegal.** `harmonize_seed_crop` maps maize and cowpea seed to `Maïs` / `Niébé`, the labels `crop_production` serves (the Burkina #1203 pattern).
- 2,136 seed rows relabel across both waves.
- The join gain exists only in 2018-19, because 2021-22 has no `crop_production`: seed rows with a harvest row for the same `(t, i, crop)` go 0 → 723 of 735 for maize and 0 → 448 of 460 for cowpea.

**#1287, Senegal.** `_/plot_features.py` refuses a declared wave whose parquet is absent, instead of `except Exception: continue`.
- **Only the direct-run route changes.** Both Country routes already raised on a broken wave. Run directly, the base script silently wrote 2018-19 alone (7,767 rows).
- **Healthy builds are frame-equal to base** on the default route, the make route and the direct run.

**Docs.**
- **CONTENTS.org entries.** Each of the four countries has dated entries with the evidence and the costs.
- **Comments.** Comments that called the gift answers a purchase are corrected, as is the reversed Togo grappe 101 / menage 9 note, in the wave script, the test and `.coder/ledger/323-benin-togo.md`.

### Cost

| country | change | hashes moved |
|---|---|---|
| Togo | one re-warm | 22 table, 22 wave (2018) |
| Senegal | one re-warm | 23 table, 46 wave |
| Côte d'Ivoire | one re-warm | 21 table, 42 wave: 21 of them (2021-22) for its 4 relabelled rows, 21 (2018-19) for the purchase rebinding |
| Niger | one re-warm | 25 table, 50 wave (2018-19 and 2021-22) |

No `build_transforms_fingerprint` or `framework_imports_fingerprint` moves, and no other country moves.

### Not changed
- **Two stale Togo comments.** The 2021-22 `plot_inputs.py` docstring and the note at `_/categorical_mapping.org:199-201` would re-warm Togo 2021-22 to fix (22 wave hashes) for comments only. A Togo `plot_features` concatenator fix would move those hashes anyway, so they could ride along with it. CONTENTS records both.
- **Togo and Benin `plot_features` concatenators.** They still skip a declared wave since their 2021-22 wiring (#1287 re-opened). Drafted as a residual, outside this PR.
- **The Senegal 2021-22 adapter** (`_/s16b_2021.py`, #1483) serves 26 columns its scheme does not declare, and its own unit spellings. Residual to file against #1483.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
