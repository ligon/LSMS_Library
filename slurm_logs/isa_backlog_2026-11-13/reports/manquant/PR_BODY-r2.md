
## Togo / Senegal / Côte d'Ivoire: `Unknown` missing input unit, Togo 2018 purchase columns, Senegal seed crop names and `plot_features` refusal

Addresses #1118, #1119 and #1287. All three are partial: other countries remain in each.

### What changes

**Togo 2018 `plot_inputs`.** This implements the ruling of 2026-11-13, "Fix all three now (Recommended)". The file's own Stata labels show that `s16bq05` / `s16bq07a` are the gift question.
- **`Purchased` / `Quantity_purchased`** now read `s16bq08` / `s16bq09a` (the purchase).
- **The gift answers** fill `Received_as_gift` / `Quantity_received_as_gift`.
- **Each acquisition unit** fills its own `Unit_purchased` / `Unit_received_as_gift`, in the `u` vocabulary, as Togo 2021-22 and Burkina (GH #1188) do.
- **`Purchased` changes meaning.** True goes from 804 to 7,128. Of the old True lines, 725 were gifts that were never bought; 7,049 bought lines were served False.
- **The old answers survive** row for row in the new gift columns.
- **Units.** `Quantity_purchased` is in `Unit_purchased`, not in `u`. 6 purchase quantities and 1 gift quantity have no unit in the file and are served with an NA `Unit_*`.
- **Wording.** The 2018 quantity questions are worded differently from 2021-22's: "Selon vous, quantité … reçue" and "la quantité et la valeur totale … achetées", against "Quelle quantité … reçue" and "Quelle est la quantité … achetée".

**#1118: a blank used unit is `Unknown`, not the minted `Manquant`.**

| cell | rows relabelled |
|---|---|
| Togo 2018 | 9 |
| Senegal 2018-19 | 0 |
| Côte d'Ivoire 2018-19 | 0 |
| Côte d'Ivoire 2021-22 | **4**, a site the issue's inventory missed |

No `s16bq03b` value set has a missing-marker label, so there is no raw answer to keep in its own column.

**#1119, Senegal.** `harmonize_seed_crop` maps `Semences de maïs` to `Maïs` and `Semences de haricots/niébé` to `Niébé`, the labels `crop_production` serves (the Burkina #1203 pattern).
- **Both waves read the map**, so 2,136 seed rows relabel: 1,195 in 2018-19 and 941 in 2021-22.
- **The join gain exists only in 2018-19**, because 2021-22 has no `crop_production`. Seed rows with a harvest row for the same `(t, i, crop)` go 0 → 723 of 735 for maize and 0 → 448 of 460 for cowpea.

**#1287, Senegal.** `_/plot_features.py` replaces `except Exception: continue` with a refusal: a declared wave whose parquet is absent raises `FileNotFoundError` naming the wave. This is the Burkina form of the #1241 refusal.
- **Only the direct-run route changes.** Both Country routes already raised on a broken wave at base. Run directly, the base script silently wrote 2018-19 alone (7,767 rows); the candidate refuses.
- **Healthy builds are frame-equal to base** on the default route, the make route and the direct run.

**Docs.**
- **CONTENTS.** Each country's CONTENTS.org has a 2026-11-13 entry with the evidence and the cost.
- **Comments now true.** The Togo GH #323 example note had read the gift flag as the purchase. It is corrected in the 2018 wave script, `tests/test_gh323_benin_togo.py` and `.coder/ledger/323-benin-togo.md`. The Senegal 2018-19 comments no longer call the gift answers a purchase or the seed crop a food label.

### Not changed
- **Gift/purchase binding in four cells.** Senegal 2018-19, Côte d'Ivoire 2018-19, Niger 2018-19 and Niger 2021-22 still serve `s16bq05` / `s16bq07a` as the purchase. The ruling names only Guinea-Bissau, Togo and Benin. The binding is pinned by `tests/test_ehcvm_plot_inputs_identity.py:217-230` and `tests/test_senegal_1483_s16b.py:629-630`, and the counts are measured in CONTENTS. This needs a ruling.
- **Two stale Togo comments.** The 2021-22 `plot_inputs.py` docstring and the note at `_/categorical_mapping.org:199-201` are left alone. Each file is hashed into the 2021-22 `Wave._input_hash`, so fixing them would re-warm that wave for comments only. CONTENTS records both.
- **Togo and Benin `plot_features` concatenators.** They still skip a declared wave; drafted as a residual, outside this PR.

### Verification
- **Cold builds, base `f138a3d61` against the candidate, in private roots.**
  - Only the relabelled rows and the rewired Togo 2018 columns move.
  - Rows are conserved in all three countries: 29,772, 19,284 and 25,907.
  - `seed_kg` and `nitrogen_kg` are frame-equal, as are Senegal's `crop_production` and `plot_features`.
  - Country output equals `Feature('plot_inputs')` output in all three. In Senegal 2021-22 the adapter's undeclared `*_unlisted` flags differ in dtype, identically on base.
- **Hash census, base → head `1d89815f0`.** This is one cold re-warm each of Togo, Senegal and Côte d'Ivoire, and no other country moves.

  | kind | moved |
  |---|---|
  | `build_transforms_fingerprint` | 0 / 42 |
  | `framework_imports_fingerprint` | 0 / 38 |
  | `Country._table_cache_hash` | 66 / 555 (Togo 22, Senegal 23, Côte d'Ivoire 21) |
  | `Wave._input_hash` | 110 / 2,321 (below) |

  - **Togo:** the 22 wave hashes of 2018 only.
  - **Senegal:** 23 + 23, both waves.
  - **Côte d'Ivoire:** 21 + 21. The 2018-19 edit serves 0 rows and the 2021-22 edit 4; those 4 rows are the country's whole served effect.
- **Tests.**
  - **Full set at `4ab20ca27`:** 65 test files. 4,483 passed and 3 failed: one digest pin this branch moves (re-pinned) and two bugs in the new tests (fixed).
  - **Rerun of the changed files:** 75 passed.
  - **Seven more files:** 317 passed, 8 skipped.
  - **The pinned Senegal tests plus schema consistency at the head:** 431 passed.
  - **The independent red team's re-run** of 74 files: 4,821 passed, 0 failed.
- **New tests:** `test_togo_2018_plot_inputs_acquisition.py`, `test_senegal_seed_crop_labels.py`, `test_senegal_plot_concatenator_refusal.py` and `test_cotedivoire_plot_inputs_unit_sentinel.py`.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
