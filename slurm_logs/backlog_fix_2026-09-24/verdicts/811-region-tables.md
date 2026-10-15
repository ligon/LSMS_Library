VERDICT: pass — every before/after figure in the three commits reproduces on a cold build in the private root, the new test fails on `origin/development` and passes on the branch, no row/index/audit report moves, the diff is exactly the seven named files, and the cache-cost statement is correct (verified: all 53 `(country, table)` hashes move).

## 1. Refutations (each claim → what I ran → result)

| claim | ran | result |
|---|---|---|
| GB `Region` served raw on development (`sab`, …, `BOLAMA_BIJAGOS`) while `strata` is normalised | `git worktree add /local/job$ID/base origin/development`; `Country(c, assume_cache_fresh=True).cluster_features()` / `.sample()` on base code against the seeded pre-fix parquets | **held**: Region = {sab 92, bafata 50, oio 48, gabu 48, cacheu 44, tombali 42, quinara 42, biombo 42, BOLAMA_BIJAGOS 42}; strata = {SAB, Bafata, …, Bolama-Bijagos} |
| GB fixed on a COLD build: 450 rows, unique, 9→9 labels, vocab == strata vocab, Rural 281/169 | `lsms-library cache clear --country Guinea-Bissau` against MY root (GB `var/` emptied), then `Country('Guinea-Bissau').cluster_features()` on branch code (`lsms_library.__file__` asserted at the clone) | **held**: SAB 92, Bafata 50, Oio 48, Gabu 48, Cacheu 44, Tombali 42, Quinara 42, Biombo 42, Bolama-Bijagos 42; Rural 281 / Urban 169; `set(Region) == set(sample.strata)` |
| GB stored wave parquet keeps raw lower-case, 5,351 rows | `pd.read_parquet($LSMS_DATA_DIR/Guinea-Bissau/2018-19/_/cluster_features.parquet)` | **held**: 5,351 rows, vocab raw; `var/cluster_features.parquet` 450 rows, also raw (read-path mapping) |
| Raw source mixes 8 lower-case names + 1 upper-case underscored | `get_dataframe('…/s00_me_gnb2018.dta')['s00q01']` | **held** (BOLAMA_BIJAGOS 502 of 5,351) |
| BF 22 → 13 labels, 2,035 rows, per-wave 13/13/13, pooled counts are sums of variants | branch read (stale hash → rebuild), development read | **held**: dev 22 labels; branch Centre 224, Hauts-Bassins 204, Boucle du Mouhoun 168, Centre-Est 168, Est 152, Centre-Ouest 151, Sud-Ouest 149, Centre-Nord 147, Nord 143, Centre-Sud 137, Sahel 132, Cascades 131, Plateau-Central 129; Σ = 2,035; 13/13/13 both sides |
| BF raw spellings per wave (2014 unhyphenated + `Cascade`; 2018-19 `Hauts Bassins`; 2021-22 `Boucle du Mouhoum` typo) | `get_dataframe` on `emc2014_p1_logement_27022015.dta`, `s00_me_bfa2018.dta`, `s00_me_bfa2021.dta` | **held** exactly as CONTENTS.org states (`Boucle du Mouhoum` 261 rows in 2021-22 only) |
| BF 2014 `strate` is a bare 1–26 code | `emc2014_welfare.dta['strate']` | **held**: int8, values 1..26 |
| SEN 14 → 14 labels, 1,194 rows, per-wave counts unchanged (Dakar 86/85, Thies 48/48, Sedhiou 34/34) | branch read vs development read | **held**, every per-wave count identical |
| SEN raw is lower-case + `SAINT-LOUIS` | raw `s00q01` both waves | **held** |
| SEN `sample` has no `strata` column | `sample().columns` | **held**: `[v, Rural, weight, panel_weight]` |
| "categorical_mapping.org is a build input, every cache hash of the country moves" | `Country._table_cache_hash(t, waves)` for every table, base vs branch | **held, and understated**: 53 of 53 tables moved (GB 17, BF 19, SEN 17) — not just the script-path ones |
| `test_region_and_strata_share_one_vocabulary` discriminates | copied the branch test file into the base worktree, ran with `PYTHONPATH=base`, separate scratch `LSMS_DATA_DIR` | **held**: 1 failed (the new test, on the vocab assert), 5 passed; on the branch 6 passed |
| "the (t, v) projection cannot move" | `grain_reports()` before/after | **held**: GB none/none; BF one report `2014: destroyed 1,034 of 10,800, 94 groups` — byte-identical on development (cold build in a scratch root) → pre-existing, not introduced |

## 2. Claimed vs measured

| cell | claimed | measured |
|---|---|---|
| GB rows / distinct Region / Rural | 450 / 9→9 / 281:169 | 450 / 9→9 / 281:169 |
| GB per-label | SAB 92 … Bolama-Bijagos 42 | identical |
| GB wave parquet rows | 5,351 raw | 5,351 raw |
| BF rows / distinct | 2,035 / 22→13 | 2,035 / 22→13 |
| BF Hauts-Bassins | 84+60+60 = 204 | 204 |
| BF Boucle du Mouhoun | 75+49+44 = 168 | 168 |
| BF per-wave distinct | 13/13/13 | 13/13/13 |
| SEN rows / distinct | 1,194 / 14→14 | 1,194 / 14→14 |
| SEN per-wave counts | unchanged | unchanged (all 28 cells) |
| cache hashes moved | "every <country> hash" | 53/53 |
| null_index / null_read / quantity reports | (not claimed) | unchanged before→after: GB none; BF only the pre-existing `sample.strata` 100%-null in 2018-19/2021-22; SEN none |

## 3. Regression (verbatim summary lines)

- `pytest tests/test_guinea_bissau_cluster_features.py tests/test_schema_consistency.py tests/test_build_transform_hash.py tests/test_categorical_merge.py tests/test_orgfile_missing_table.py tests/test_harmonize_method_auto_dispatch.py tests/test_grain_collapse_cluster_features.py tests/test_declared_spellings.py tests/test_burkina_faso_2014_passages.py -n 4 --dist=loadfile` (BLAS pinned, `LSMS_BUILD_WORKERS=1`): **`383 passed, 37 warnings in 90.58s`**
- `pytest tests/test_feature_canonical_index.py tests/test_ehcvm_7day.py -k "not (Benin or Togo or Mali or Niger or CotedIvoire or Cote)"`: **`118 passed, 9 warnings in 491.27s`**
- new test on development: **`1 failed, 5 passed in 9.06s`** (only the new test fails)
- `bench/feature_audit/scan.py --features cluster_features --countries Guinea-Bissau Burkina_Faso Senegal`: **`features: 1  records: 51  skips: 0 / by status: {'warn': 51}`** — 0 fail/error; the warns are ResourceWarnings from the hasher's unclosed file handles, `has_household_index` (cluster grain, expected), the population-universe pooling notice, and the pre-existing BF 2014 grain collapse.
- `Feature('cluster_features')(['Guinea-Bissau','Burkina_Faso','Senegal'])`: 3,679 rows, index unique, canonical vocab per country; `Region == 'Bafata'` → 50, `'Dakar'` → 171, `'Hauts-Bassins'` → 204. `market='Region'` on GB `household_roster` and BF `sample` yields the canonical `m` vocabulary.

## 4. Scope findings

- `git diff --stat origin/development...HEAD`: exactly the 7 files named (3× `_/categorical_mapping.org`, 3× `_/CONTENTS.org`, 1 test), 280 insertions, 0 deletions. No baseline/blessed/lockfile/dependency edit; no assertion weakened, skipped or xfailed; no `data_info.yml` change.
- `find lsms_library/countries -name '*.parquet'` → 0 before and after; no writes to the shared cache or blob store; `git status --short` empty in the clone; scratch worktree removed.
- CONTENTS.org: all three entries name the idiosyncrasy concretely and every factual claim in them checks against the raw `.dta` (see §1). The only inaccuracies are cosmetic counts in the commit messages ("seven"/"eleven" script-path tables — `grep -c` gives 7 and 13; irrelevant since ALL tables' hashes move, which the messages also say).
- Niger (16 → 8, the third sibling named in the triage) is **not** in this branch. The task brief named only GB/BF/SEN so this is not a deviation, but the issue's sweep is not complete without it.

## 5. Residual risks / what a human must decide

1. **Canonical spellings are editorial, in three places.** GB `SAB` stays UPPER (matches the existing `strata` table; the issue asked for title case). BF corrects the source typo `Boucle du Mouhoum` → `Mouhoun` and `Cascade` → `Cascades`. SEN is title-cased WITHOUT accents (`Thies`, `Kedougou`, `Sedhiou`; official names carry them). All three are documented as decisions; none is wrong, but if a cross-country `Region` vocabulary is ever pinned (e.g. matching CotedIvoire's accented labels), SEN and `SAB` will need revisiting.
2. **Cost is a full re-warm of three countries (53 tables), not "one Guinea-Bissau re-warm, one wave, small"** as the dispatch plan said. The commits state this correctly; the merger must budget it (BF food_acquired 2014 has four passages).
3. **Pre-existing, surfaced, not caused here**: BF `cluster_features/2014` destroys 1,034 of 10,800 rows (94 conflicting `(t, v)` groups) and both BF 2014/2018-19 drop their `df_geo` sub-df (`lat_dd_mod not in columns`) — identical on development. BF `sample.strata` is 100% null in the EHCVM waves (known, CONTENTS.org). Worth their own issues if none exists.
4. The seeded pre-fix parquets in the review root did not carry development's expected hash either (all 53 stamps ≠ base expectation), so on development they would also have rebuilt; this is a property of the seed, not the branch, and my "before" read used `assume_cache_fresh=True` on raw-valued parquets, which is valid.

## 6. Recommended follow-up commits

None required for merge. Optional: a sibling commit for Niger (`Niger/_/categorical_mapping.org:230` `#+NAME: Region`, add the lower-case variants; 16 → 8) to close #811's sweep.
