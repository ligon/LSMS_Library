# Prior-Art Ledger — GH #562 phase 3a: GhanaLSS `community_prices`

> Per-task ledger. Living, git-tracked snapshot of the machinery, definitions
> and conventions that bear on THIS task. Inherits `STANDING.md`; cites
> `CLAUDE.md`, `lsms_library/data_info.yml` and `GhanaLSS/_/CONTENTS.org`
> rather than re-copying them.

**Search tier used:** ripgrep + git floor. The `gitnexus` MCP server timed out
at session start (CONNECT_TIMEOUT), so no call-graph queries were possible;
callers were located with `grep`/`sed`.
**Line anchors as of:** `9c21ab89` (development, 2026-09-02).

## §1 Task, restated

Add a **registered, script-path** (`materialize: make`) table `community_prices`
to `GhanaLSS/_/data_scheme.yml`, built per wave by a new
`GhanaLSS/<wave>/_/community_prices.py` from each wave's OWN market/community
price survey (`PRICE.DAT` 1987-88/1988-89, `Prices/G3PRICE.DTA` 1991-92,
`Prices/G4PRICE.DTA` 1998-99, `PRICES/price_sec{0,1,2}.dta` 2012-13,
`g7price.dta` 2016-17), concatenated by a country-level
`GhanaLSS/_/community_prices.py`. Grain is the survey's own: one row per
(wave `t`, price-survey cluster `v`, priced item `j`, unit `u`, vendor
observation `obs`). Columns are the REPORTED fields only (`Price`,
`NumberOfUnits`, `Description`); no per-unit price, no kg-standardisation, no
cross-cluster median, no household imputation (those are phase 3b,
`transformations.py`, NOT this task). 2005-06 ships no price file: adjudicate
in `.coder/coverage/absent_verdicts.csv`.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `Malawi/_/data_scheme.yml: community_prices` | `countries/Malawi/_/data_scheme.yml:380` | precedent block: `index: (t, v, j, u)`, `Price`, `NumberOfUnits`, `Available`, `materialize: make`; `v` NATIVE (no household `i`) | via build | **PATTERN REUSED**; GhanaLSS departs by adding `obs` (3 vendor observations per item are surveyed in every wave) |
| `Nigeria/_/data_scheme.yml: community_prices` | `countries/Nigeria/_/data_scheme.yml:469` | precedent: `(t, v, j, u)`, `Price` only; W1/W2 item codes resolved by NAME onto `harmonize_food` (`_W1W2_PRICE_ITEM`), W3-W5 by code | via build | **PATTERN REUSED** (the "names" mode: every GhanaLSS price list is its own code scheme) |
| `malawi._price_block` / `assemble_community_prices` | `countries/Malawi/_/malawi.py:2363,2421` | reshape one Module-CK file to long `(t,v,j,u)` rows; decodes item/unit codes through org tables | via build | **NOT reused as code** (Malawi-specific columns) — shape copied |
| `nigeria.community_prices_for_wave` | `countries/Nigeria/_/nigeria.py:1998` | same, Nigeria; collapses same-(t,v,j,u) duplicates with `.first()` | via build | **NOT reused**; GhanaLSS keeps every observation under `obs` instead of `.first()` (core never aggregates) |
| `Nigeria/_/community_prices.py`, `Malawi/_/community_prices.py` | country scripts | concat wave parquets → `../var/community_prices.parquet` via `to_parquet` | via build | **PATTERN REUSED** |
| `local_tools.map_index` | `lsms_library/local_tools.py:~1140` | legacy `j`→`i` swap; **now exempts** a `j`-without-`i` index when `v` is present (cluster-level item tables) | yes | reuse — the redundant `i=v` shim Malawi/Nigeria carry is no longer needed; verified at build |
| `local_tools.get_dataframe` `.DAT`/`.DCT` reader | `lsms_library/local_tools.py` (GH #704) | detects comma-delimited `.DAT` behind a fixed-width `.DCT`; honours `.` missing | yes | **REUSED** for `PRICE.DAT` |
| `local_tools.df_from_orgfile`, `get_categorical_mapping`, `code_label_map` | `local_tools.py:1445,1478,1621` | org-table readers; `get_categorical_mapping` searches cwd-relative `dirs` | yes | **REUSED**, but paths built from `paths.countries_root()` (Trap 6 / GH #753) |
| `ghanalss.appendix_i_cluster_attributes` | `countries/GhanaLSS/_/ghanalss.py:420` | the `countries_root()` pattern already used in this country | via build | **PATTERN REUSED** (helper lives in a NEW module `_/glss_prices.py`; `ghanalss.py` is imported by `food_acquired.py` and is not touched) |
| `GhanaLSS/2016-17/_/food_acquired.py` `decode_unit` / `unit_9b` | wave script | unit code → native label → `unit_labels.org` Preferred Label | via build | **PATTERN REUSED** for `g7price.dta` `unit{a,b,c}` (decoded from the price file's OWN value labels: they differ from `unit_9b` at code 72 and add code 75) |
| `GhanaLSS/_/unit_labels.org` (`unit_label`) | country org | native unit spelling → Preferred Label (`u` axis) | via build | **REUSED read-only** (not in ownership) |
| `GhanaLSS/<wave>/_/categorical_mapping.org: harmonize_food` | wave org tables | `Preferred Label` = the `j` axis `food_acquired` uses | via build | **REUSED** as the target of the per-wave `harmonize_price_item` tables |
| `feature._canonical_index_levels` | `lsms_library/feature.py:59` | `[]` for tables absent from global `index_info` → no canonical reshaping | yes | relevant to STEP-3 check 6; no canonical block exists (proposed in the report, `data_info.yml` NOT edited) |
| `country._audit_index_collapse` / `GrainCollapseWarning` | `lsms_library/country.py` | audits any declared-index collapse | yes | must stay silent: `obs` makes the declared index unique |
| `null_read_audit` Site B | `lsms_library/null_read_audit.py` | required declared column all-null in a wave slice → warning / `NullReadError` under `LSMS_READ_STRICT` | yes | `Description` declared `optional: true` (instrument-specific by design) |

## §3 Definitions & conventions in force

- **`v` is `sample().v`'s keyspace**: `CLUST`/`clust` per wave, string via `format_id` — `GhanaLSS/_/CONTENTS.org` §"Cluster (PSU) variable" (1xxx … 7xxxx) and §"`sample` feature source variables".
- **Reported fields only** in `community_prices`; per-unit price / kg / medians are transformations — Malawi/Nigeria `data_scheme.yml` comments; `SkunkWorks/grain_aggregation_policy.org` §3a (no aggregation in core), `CLAUDE.md` "Grain Collapse".
- **`j` axis** = the wave's `harmonize_food` `Preferred Label` (what `food_acquired.j` carries); non-food items keep their own label on the same axis — Malawi precedent.
- **`u` axis** = `GhanaLSS/_/unit_labels.org` `Preferred Label` — `CONTENTS.org` §"Unit handling".
- **Currency** is the wave's native cedi (pre-2007 GHC; post-2007 GHS) — `currency.py` labels it; no conversion here (brief).
- **`optional: true`** is country-grain and exempts a column from Site B — `CLAUDE.md` "The Silent All-Null Read".
- **`absent_verdicts.csv`** closing verdicts need C4 + evidence — `docs/guide/coverage.md` §"The four checks".
- **Config paths via `countries_root()`**, never `files('lsms_library')` — `CLAUDE.md` "Two independent roots"; `CONTENTS.org` Trap 6.

## §4 Invariants & assumptions

- Every GhanaLSS price form records **three vendor observations** per item (BID §2.3; GLSS3/4/5/6 "1ST/2ND/3RD OBSERVATION"; GLSS7 a/b/c) → the natural grain has an `obs` level. `obs` 1–3 is the form's slot; values >3 arise ONLY when the source holds more than one record for a (cluster, item, unit) (1988-89 clusters 2305/2310; 2016-17 brand rows; 1991-92 rows per (clust,item,time) up to 9); enumerated deterministically, counted per wave.
- **GLSS1/GLSS2 `.DAT` are comma-delimited with a header** despite the fixed-width `.DCT` — `CONTENTS.org` §"GLSS1/GLSS2 .DAT files are COMMA-SEPARATED"; reader handles it (GH #704).
- The 1987-88 and 1988-89 price questionnaire PDFs are the **same blob** (md5 `f4926b21…`); item 48 (1988-89 only) is not on the form.
- All three "GLSS3" price-form files (`pdf/G3QPrice.pdf`, `GHA_1991_GLSS_Price_Questionnaire_EN.pdf.pdf`, `G3QPrice.doc`) carry the **123-item GLSS4 list**; G3PRICE has 117 codes. GLSS3's own list is reconstructed by aligning per-item medians/counts with GLSS4 (offsets at 6/34/44 + three sachets = 6 missing). **Inference, labelled as such** in the org table (`Note`) and CONTENTS.org.
- **GLSS3/GLSS4 ship only the per-unit value** (`p` = PRICE/KG, e.g. 500/7); the weighed KG is not distributed → `NumberOfUnits` comes from the form's stated basis per item.
- **2012-13 prices are stamped per cluster but collected per market**: 1,015 clusters, 323 `(region, district, market)` keys, identical prices across clusters sharing a market; `clust` values 60001–61200 ⊂ `g6loc_edt.clust`.
- **2016-17 `clust` is nationally unique in the household cover** (1000 EAs, 0 spanning regions) but the price file stamps 11 `(clust, region)` pairs with a region the cover contradicts (clusters 70002, 70909 carry a sibling EA's rows). Kept native, named, not "fixed".
- 2012-13 non-food rows carry `u='Other Unit'` (free-text basis in `Description`) rather than `pd.NA`, because `map_index` renames a `pd.NA` `u` level to the string `'unit'` on every read (`local_tools.py`).
- The global `lsms_library/categorical_mapping/u.org` relabels `Kilogram` -> `Kg` at API time (its only scope); the parquet keeps `Kilogram`.
- `Feature('community_prices')` keeps the MODAL index shape and excludes the rest (`feature._harmonize_country_frame`, `_canonical_index_levels` is `[]` here): measured, Malawi is excluded from `(['GhanaLSS','Malawi'])` and GhanaLSS from `(['GhanaLSS','Malawi','Nigeria'])`, each with a `UserWarning`.  Motivates the §6 proposal.
- **Cache-hash coverage, measured** (`Country._table_cache_hash('community_prices', waves)`): a comment-only edit to `_/glss_prices.py`, to `1987-88/_/categorical_mapping.org`, to `1987-88/_/community_prices.py` or to `_/community_prices.py` each moves the hash -- the `_/*.py` glob and the `*.org` build inputs of `CLAUDE.md` "Automatic content-hash staleness" cover everything this feature decodes through.
- `LSMS_COUNTRIES_ROOT` + private `LSMS_DATA_DIR` (shared `dvc-cache` symlink) for every run; assert `'wt-glss-prices' in str(countries_root())`.
- `get_data_file()` walks the DVC index (`DVCFS.exists`) and hung >10 min on Lustre; documentation blobs were fetched lock-free through `_ensure_dvc_pulled` + the sidecar md5 (`slurm_logs/ghana_audit/community_prices/fetch_docs.py`).

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| grain `(t, v, j, u, obs)` | **extend** the `(t, v, j, u)` precedent with `obs` | the surveys record three vendors; `.first()` (Nigeria) or a mean would be a core aggregation |
| `Price`, `NumberOfUnits` | **reuse** precedent columns | reported fields only |
| `Description` (str, optional) | **new column** | 2012-13 non-food basis (`s2desc`) and 2016-17 brand (`itname`) / other-unit text are surveyed identity that would otherwise be lost |
| `j` decode | **new per-wave `harmonize_price_item` org tables** (Nigeria "names" mode) | no wave's price code scheme equals its `Code_9b` consumption scheme (0/644 in 2016-17, 5/102 in 2012-13) |
| `u` decode | **reuse** `unit_labels.org` mapping; per-item basis from the form for 1987-99 | shared `u` axis |
| `v` | **reuse** `format_id(clust)` | same keyspace as `sample().v` |
| wave→country concat | **reuse** Malawi/Nigeria script shape | |
| price→quantity imputation | **not built** (phase 3b) | out of scope by brief |

## §6 Open questions for the human

- Canonical `community_prices` block for `lsms_library/data_info.yml`: proposed in the report (`index_info: community_prices: (t, v, j, u, obs)` + `community_prices` in `fabricate_missing_levels`), not edited here.  Without it a cross-country call excludes one side (see §4).
- GLSS3 codes 5 (Rice imported vs Sorghum, n=50) and 49–51 (tin/chocolate set) are the weakest links of the reconstruction; a GSS-held GLSS3 price form would settle them.
- `_/unit_labels.org` maps `Margarin tin` and `margarin tin` to two different Preferred Labels (`Margarin tin` / `Margarine Tin`); not in ownership, reported.

---
### Phase 3 — verification (filled at task end)

- `GhanaLSS/_/glss_prices.py` (new helper) — OK (§2/§3): paths via `countries_root()`; reported columns only; `obs` enumeration rule as §4.
- `GhanaLSS/<wave>/_/community_prices.py` ×6 — OK (§4/§5): each wave's own file, own code scheme, own form basis.
- `GhanaLSS/_/community_prices.py` — OK (§2): concat only, index uniqueness asserted.
- `GhanaLSS/_/data_scheme.yml` block + `index_info` — OK (§3): mirrors the precedents plus `obs`; `Description` optional.
- `.coder/coverage/absent_verdicts.csv` 2005-06 row — OK (§3): C1;C2;C4 with evidence.
- No `transformations.py` / `data_info.yml` / `unit_labels.org` / `ghanalss.py` edits — by brief.
