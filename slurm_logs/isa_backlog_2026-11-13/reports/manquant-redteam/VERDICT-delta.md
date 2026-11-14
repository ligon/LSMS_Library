**Delta verdict: APPROVE.** Candidate `63aa569cad2d2d4d2d8f9f928a686dd443af931b`, compared with my previous candidate `fc40960d27b800045984735a38c68cf91c9b2194`, plus a hash census against `f138a3d61c203f892148592675a7ef72e160a322`. Every claim in the worker's "Changes since 1d89815f0" section reproduces, and I found nothing that blocks. Runs were cold, in private roots: job 41281242 for builds, census and tests, and job 41281443 for the older Niger waves. Outputs are in `reports/manquant-redteam/out/delta/`.

**1. The four cells**
- **Labels and coding.** In all four files `s16bq05` is "Avez-vous obtenu [INTRANT] sous forme de cadeau ou de don?" and `s16bq08` is "Avez-vous acheté [INTRANT]?". Both are coded 1 = Oui, 2 = Non, with no unlabelled codes (my first-round raw probe).
- **Old values kept.** The new gift columns equal the old purchase columns row for row, joined by key:

| cell | rows | `Purchased` True, before → after | gift × purchase (Non/Non, Non/Oui, Oui/Non, Oui/Oui) |
|---|---|---|---|
| Senegal 2018-19 | 9,916 | 536 → 4,956 | 4,504 / 4,876 / 456 / 80 |
| Côte d'Ivoire 2018-19 | 11,259 | 890 → 8,134 | 2,373 / 7,996 / 752 / 138 |
| Niger 2018-19 | 13,401 | 1,383 → 4,557 | 7,602 / 4,416 / 1,242 / 141 |
| Niger 2021-22 | 12,435 | 856 → 3,593 | 8,057 / 3,522 / 785 / 71, plus 5 lines with both flags blank |

- **Rows conserved.** Senegal 19,284, Côte d'Ivoire 25,907, Niger 43,620, and the key sets are equal on both sides.
- **Nothing else moves.** Only the acquisition columns change in the four cells. Every other wave has 0 changed cells.
- **Downstream.** `seed_kg` and `nitrogen_kg` are frame-equal, and so are Senegal's `crop_production` and `plot_features`. The Feature table equals the Country table in all three countries.
- **No sentinels.** No 9999-style code appears in `s16bq09a` or `s16bq07a`. The largest Niger purchase quantity is 6,000 in 2018-19 and 1,200 in 2021-22.

**2. Niger**
- **Positional attachment is safe today.** The shared tail in `niger.py` keeps row order: its fill helper copies the frame, its input filter becomes a no-op after the script's identical pre-filter, and it ends with a column selection and `set_index`.
  - Alignment is proven two ways. The keyed comparison above holds across 25,836 rows. And the column that goes through the tail (`Purchased`) agrees with the attached one (`Unit_purchased` is filled exactly where `Purchased` is True): 0 violations.
  - The re-pinned identity test also checks gift and purchase together against the raw file.
- **The length assert would not catch a reorder.** If someone later adds a sort inside the shared tail, the count stays the same and the columns misalign silently. Note-level hardening: also assert that `out.index.get_level_values('i')` and `'input'` equal the pre-tail `df['i']` and `df['input']`.
- **The older ECVMA waves are untouched.** Niger's wave hashes move for 2018-19 (25) and 2021-22 (25) only. 2011-12 and 2014-15 carry the four new columns as all-NA, with the right dtypes.
- **Declaring the four columns `optional` is correct.** The older waves never ask these questions, so NA there is the honest answer and not a hidden empty wave.

**3. The re-pinned tests.** Both re-pins are justified by the ruling.
- `test_ehcvm_plot_inputs_identity.py` is now stronger: its oracle also checks the gift flag and gift quantity. It does not check the `Unit_*` columns; my checks cover those.
- `test_senegal_1483_s16b.py` now pins the new binding in the source text.
- The `test_gh1500` scheme digest re-pin covers comment edits only.
- 180 files at the head, including every Niger-related test: 8,174 passed, 0 failed, 191 skipped, 8 xfailed. No in-tree parquet was written.

**4. Unit-spelling residue: acceptable as disclosed.**
- **Niger 2021-22.** 74 purchase lines are served as `Charrette Bovine` (62) and `Charrette Asine` (12), because the purchase question capitalises them, while `u` and the gift column serve them in lower case. Folding them needs a new row in Niger's `categorical_mapping.org`, which would move the older waves' hashes too; a fold inside the script would be an unregistered relabel. Recommend a residual.
- **Senegal 2021-22.** The adapter still serves `Kilogramme` / `Charrettée` where 2018-19 now serves `Kg` / `Charrette`, so one column uses two spellings across the country's waves. Fixing `_/s16b_2021.py` now would cost no extra hashes, since every Senegal hash already moves, but it would mean changing the #1483 adapter and its tests. Leaving it to the #1483 residual is fine, as long as the PR body states the spelling split.

**5. Hash census**

| kind | `f138a3d61` → `63aa569ca` | `fc40960d2` → `63aa569ca` |
|---|---|---|
| `build_transforms_fingerprint` | 0 of 42 | 0 |
| `framework_imports_fingerprint` | 0 of 38 | 0 |
| `Country._table_cache_hash` | 91 of 555 (Togo 22, Senegal 23, Côte d'Ivoire 21, Niger 25) | Niger 25, Senegal 23, Côte d'Ivoire 21 |
| `Wave._input_hash` | 160 of 2,321 (Togo 22, Senegal 46, Côte d'Ivoire 42, Niger 50) | Niger 50, Senegal 23, Côte d'Ivoire 21 |

Nothing moves outside these four countries.

**Notes**
- The superseded "Not changed" paragraph in Côte d'Ivoire's CONTENTS still says the binding "survives in four cells". It is marked superseded, so it is acceptable as history.
- The positional-assert hardening above can wait for the next edit of the Niger wave scripts.
