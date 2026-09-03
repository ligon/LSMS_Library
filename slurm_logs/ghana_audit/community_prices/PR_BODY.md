GH #562 **phase 3a** (refs #109): a `community_prices` feature for GhanaLSS, built from each wave's OWN market/community price survey. Phase 3b (the inverse price→quantity imputation) is not in this PR; `transformations.py` and the derived paths are untouched.

## What lands

- **`GhanaLSS/_/data_scheme.yml`**: `community_prices` at grain **`(t, v, j, u, obs)`**, reported columns `Price`, `NumberOfUnits`, `Description` (optional), `materialize: make`; `index_info` entry.
- **`GhanaLSS/_/glss_prices.py`** (new shared helper; `ghanalss.py` untouched): the column contract, `countries_root()`-resolved org-table readers (Trap 6 / #753), the `obs` enumeration rule, and the two shared wave builders.
- **`GhanaLSS/<wave>/_/community_prices.py`** ×6 + **`GhanaLSS/_/community_prices.py`** (concat), `_/Makefile` rules.
- **`harmonize_price_item`** tables (`Code | Label | Preferred Label | Food | Unit | Basis | Note`) appended to each wave's `categorical_mapping.org`; a `harmonize_price_unit` spelling table for 2016-17's free-text units.
- **`.coder/coverage/absent_verdicts.csv`**: 2005-06 `asked-not-distributed` (C1;C2;C4).
- **`CONTENTS.org`** "Community price survey" section; `tests/test_ghanalss_community_prices.py` (slow-marked, data-gated); audit scripts + `verify_run.log` under `slurm_logs/ghana_audit/community_prices/`.

## Per-wave source / geography / join (measured)

| wave | price file | form keyed on | price clusters | in `sample().v` | rows |
|---|---|---|---|---|---|
| 1987-88 | `PRICE.DAT` (comma-delimited behind a fixed-width `.DCT`) | LOCALITY / CLUSTER | 165 | 163/165 (163/176 hh clusters) | 16,582 |
| 1988-89 | `PRICE.DAT` (same form blob as 1987) | LOCALITY / CLUSTER | 112 | 106/112 (106/170) | 12,642 |
| 1991-92 | `Prices/G3PRICE.DTA` (per-unit value only) | REGION / DISTRICT / LOCALITY / EA | 256 | 256/256 (256/365) | 58,348 |
| 1998-99 | `Prices/G4PRICE.DTA` (per-unit value only) | same instrument | 253 | 251/253 (251/300) | 63,445 |
| 2005-06 | **none distributed** (`G5QPrice.pdf` exists) | EA / MARKET NUMBER | — | — | — |
| 2012-13 | `PRICES/price_sec{1,2}.dta` | REGION / DISTRICT / LOCALITY / EA / MARKET NUMBER | 1,015 | 1015/1015 (1015/1200) | 288,243 |
| 2016-17 | `g7price.dta` | REGION / DISTRICT / CLUSTER / MARKET NAME | 398 | 398/398 (398/1000) | 220,908 |

`v` is the survey's own cluster id, native in the index, on `sample().v`'s keyspace — no cluster id is fabricated. Corroboration: `G3PRICE.loc5` == `POV_GH.loc5` on all 256 clusters; the BID's "165 price questionnaires" is the 165 distinct `CLUST` in the 1987-88 file.

## Design choices (all documented in CONTENTS.org / data_scheme.yml)

- **`obs`** — every GLSS price form records up to three vendor observations per item (BID §2.3; GLSS3–7 forms). They are kept as rows, never `.first()`-ed or averaged; the one departure from the Malawi/Nigeria `(t, v, j, u)` precedent. `obs` > 3 only where the source holds more than one record per (cluster, item, unit): repeat visits (1988-89 clusters 2305/2310), brand lines (2016-17), the two mis-keyed 2016-17 clusters (70002, 70909), or several price items folding onto one `j` (Nescafe tin + sachet → `Coffee`). Share of rows at `obs` ≤ 3: 98/97/90/88/87/81 %.
- **`j`** — no wave's price code scheme is its `Code_9b` consumption scheme (0/644 in GLSS7, 5/102 in GLSS6), so items are decoded by name onto the wave's `harmonize_food` Preferred-Label axis; non-foods keep own labels. Share of price rows on the food axis: 49 / 47 / 59 / 57 / 71 / 52 %. **GLSS3's 117-code list is reconstructed**: all three "GLSS3" price-form files are the 123-item GLSS4 instrument; per-item median/count alignment shows GLSS3 = GLSS4 minus Sorghum, Pepper (sweet green), Live chicken (poultry) and the three sachet variants (exactly 123−117). Labelled as inference with its evidence table.
- **`u` / `NumberOfUnits`** — the form's stated basis per item (weighed KG, tablets, beer bottle, 6 yards, the 0.170 kg tin); GLSS3/4 distribute only the per-unit value `p` = PRICE/KG, so `NumberOfUnits` there is the form's basis. 2012-13 non-foods are priced on free text (`s2desc`) → `u='Other Unit'`, text in `Description`. The 1980s files' `PRICEnU`/`DEFL`/`PRICE` are calculated fields (BID §6.2) and are not stored.
- **Currency** — native cedi per wave, no conversion: maize per kg 71 (1987-88) → 94 (1991-92) → 480 (1998-99) → 0.77 GHS (2012-13) → 1.2 GHS (2016-17).

## Verification (from `verify_run.log`)

- `Country('GhanaLSS').community_prices()` builds end-to-end under `LSMS_GRAIN_STRICT=1 LSMS_READ_STRICT=1`: (660,168 × 3), index `(t, v, j, u, obs)` unique, **0** grain / null-read warnings on the table. `LSMS_READ_STRICT=1` does fire on `sample()` — `strata` 100 % null in 1987-88/1988-89 — a pre-existing GLSS1/2 fact (CONTENTS.org `sample` table shows `---`), not touched here.
- `pytest tests/test_schema_consistency.py -k GhanaLSS` from the main root with `LSMS_COUNTRIES_ROOT` = worktree: 8 passed. `tests/test_ghanalss_community_prices.py` (main root, `--rootdir=<main>`): 6 passed.
- `Feature('community_prices')` — measured, and it is the reason for the proposal below. `community_prices` has no global `index_info` entry, so assembly keeps the *modal* index shape and excludes the rest with a `UserWarning`: `(['GhanaLSS','Malawi'])` → (660,168 × 3) at `(country, t, v, j, u, obs, currency)`, **Malawi excluded**; `(['GhanaLSS','Nigeria'])` → Nigeria excluded; `(['GhanaLSS','Malawi','Nigeria'])` → (220,690 × 3) at `(country, t, v, j, u, currency)`, **GhanaLSS excluded**. Nothing is silently collapsed — the warning names the excluded frame — but a five-level GhanaLSS cannot stack with the four-level precedents until the canonical index says how.

## Not done / for the maintainer

- **No canonical block in `lsms_library/data_info.yml`** (not edited). Proposed: `community_prices: (t, v, j, u, obs)` under `Index Info: index_info`, with `community_prices` added to `fabricate_missing_levels` so the single-observation countries (Malawi, Nigeria, Tanzania, Ethiopia, Mali, Niger, EthiopiaRHS) gain `obs = <NA>` and stack with GhanaLSS instead of one side being excluded; the alternative — dropping `obs` here and `.first()`-ing two of three surveyed vendors — is a core aggregation the design rules out.
- The API relabels `Kilogram` → `Kg` through the global `lsms_library/categorical_mapping/u.org` (its only scope); the parquet keeps `Kilogram`. `Gram`/`Liter`/`Milliliter` are left as the country spells them, per that file's stated deferral.
- `_/unit_labels.org` lacks `Tablet`, `Capsule`, `Bottle` (and maps "Margarin tin" / "margarin tin" to two Preferred Labels) — not in this task's ownership.
- `_/glss_prices.py` is a helper module, not the country module, so it is **not** in the cache hash: an edit to it needs `lsms-library cache clear --country GhanaLSS`.
- 2005-06's price file is an acquisition item (ask GSS); `G5QPrice.pdf` shows the GLSS6 instrument was fielded.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
