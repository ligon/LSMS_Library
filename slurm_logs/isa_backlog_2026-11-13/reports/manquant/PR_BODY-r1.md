
## Togo / Senegal / Cote d'Ivoire: `Unknown` missing input unit, Togo 2018 purchase columns, Senegal seed crop names and `plot_features` refusal

Addresses #1118, #1119 and #1287. All three are partial: other countries remain in each.

### What changes

**Togo 2018 `plot_inputs`**, per the ruling of 2026-11-13, "Fix all three now (Recommended)":
- `Purchased` / `Quantity_purchased` now read `s16bq08` / `s16bq09a` (the purchase), not `s16bq05` / `s16bq07a` (the gift question, by the file's own Stata labels).
- The gift answers fill `Received_as_gift` / `Quantity_received_as_gift`.
- Each acquisition unit fills its own `Unit_*` column, in the `u` vocabulary, as Togo 2021-22 and Burkina (GH #1188) do.
- Served: `Purchased` True goes 804 -> 7,128, and `Received_as_gift` NA -> 804. The new gift columns equal the old purchase columns row for row.

**#1118, missing unit.** A blank used unit is now `Unknown`, not the minted `Manquant`:
- Togo 2018: 9 rows.
- Senegal 2018-19: 0 rows.
- Cote d'Ivoire 2018-19: 0 rows. Cote d'Ivoire 2021-22: **4 rows**, a site the issue's inventory missed.
- No `s16bq03b` value set has a missing-marker label, so there is no raw answer to keep in its own column.

**#1119, Senegal.** `harmonize_seed_crop` maps `Semences de maïs` -> `Maïs` and `Semences de haricots/niébé` -> `Niébé`, the labels `crop_production` serves. This follows the Burkina #1203 pattern.
- 2,136 seed rows relabel (2018-19: 1,195; 2021-22: 941).
- The 2018-19 `(t, i, crop)` join to a harvest row goes 0 -> 723 of 735 (maize) and 0 -> 448 of 460 (cowpea).

**#1287, Senegal.** `_/plot_features.py` now refuses a declared wave whose parquet is absent (`FileNotFoundError` naming the wave), in the Burkina form of the #1241 refusal. It no longer uses `except Exception: continue`.
- **Measured:** both Country routes already raised on a broken wave at base. The script run directly at base silently wrote 2018-19 alone (7,767 rows). The candidate refuses.
- **Healthy builds are frame-equal to base** on the default route, the make route and the direct run.

**Docs.** Each country's CONTENTS.org has a 2026-11-13 entry with the evidence and the cost. Togo's GH #323 example note had read the gift flag as the purchase; it is corrected.

### Not changed
- **Senegal and Cote d'Ivoire 2018-19 gift bindings.** Same defect, but the ruling names only Guinea-Bissau, Togo and Benin. Measured, and left for a ruling.
- **Togo comments.** The Togo 2021-22 docstring and one `categorical_mapping.org` comment are now stale. Fixing them would re-warm 2021-22 for comments only.

### Verification
- **Cold builds.** Base `f138a3d61` against candidate, in private roots. Only the relabelled rows and the rewired Togo 2018 columns move. Rows are conserved. `seed_kg` and `nitrogen_kg` are frame-equal in all three countries. Country output equals Feature output; Senegal's 2021-22 `*_unlisted` flags differ in dtype, on base as on candidate.
- **Hash census, base -> head:**

  | kind | moved |
  |---|---|
  | `build_transforms_fingerprint` | 0 of 42 |
  | `framework_imports_fingerprint` | 0 of 38 |
  | `Country._table_cache_hash` | 66 of 555 (Togo 22, Senegal 23, Cote d'Ivoire 21) |
  | `Wave._input_hash` | 110 of 2,321 (Togo 2018; both Senegal waves; Cote d'Ivoire 2018-19 and 2021-22) |

  No other country moves. Cost: one re-warm each of Togo, Senegal and Cote d'Ivoire.
- **Tests.** The 65 test files for these countries, plus the schema, hash and derivation suites, ran at `4ab20ca27`: 4,483 passed, 3 failed. The 3 failures were one digest pin this branch moves (re-pinned) and two bugs in the new tests (fixed). Rerun at `fcda613b7`: 75 passed. Seven more files at the head: 317 passed, 8 skipped.
- **New tests:** `test_togo_2018_plot_inputs_acquisition.py`, `test_senegal_seed_crop_labels.py`, `test_senegal_plot_concatenator_refusal.py`, `test_cotedivoire_plot_inputs_unit_sentinel.py`.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
