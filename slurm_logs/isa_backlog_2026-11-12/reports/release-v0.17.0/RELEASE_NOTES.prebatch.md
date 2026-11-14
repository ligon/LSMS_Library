# v0.17.0 (DRAFT, not released)

> Draft prepared 2026-11-12 by a read-only release-preparation agent (Claude Opus 5.5) for
> Sue. It is not yet a release record. Per the maintainer's 2026-11-12 decision ("One
> release after batch"), v0.17.0 is cut **after** the six-branch integration batch lands;
> that batch has its own placeholder section at the end of this file.

Changes since v0.16.0 (`097283459f80016b0f00a51b814a0cb964a42789`, 2026-10-21, the current
`master`), through development `e5fdd2fa831c4d03a247d264be7f339da459e6b0` (PR #1693).

**Scope, measured:**
- 2,447 commits, 527 of them merge commits.
- **355 merged PRs**, each verified by ancestry: its merge commit is an ancestor of
  `e5fdd2fa8` and not of `v0.16.0`. The brief cited "about 314"; the gap is mostly 19
  integration and squash merges whose commits do not say "Merge pull request" (e.g.
  #1680-#1687).
- By type: 267 code PRs, 71 documentation-only PRs and 17 CI/test-harness PRs. Of the
  documentation PRs, #1337 also revises Tanzania capability verdicts (coverage metadata, not
  served data), and #1684 edits comments and docstrings in hashed Mali files.
- 191 issues were closed by hand against this work, with no closing keywords. They are listed
  in `CLOSES.md` and `ALREADY_CLOSED.tsv`.

**Upgrade note.** The release moves served numbers in many countries; the next section lists
each move. Several framework PRs move every build fingerprint, so **expect every harmonized
cache to rebuild cold on first access**, at both L2 tiers, from the L1 source blobs. L1 blobs
remain reusable. See "Cache note".

Sizes below are quoted from each PR's own body (author and independent-review measurements,
mostly cold builds on private roots against the stated base). They are not re-measured here.
A PR's base is often an unreleased intermediate state, so where several PRs touch one cell
the sizes describe each step; the bullets give the net change against v0.16.0 where it is known.

## Behaviour changes that move served numbers

### Framework-wide

- **`id_walk` is a single-pass rename, applied exactly once** (#1282, issue #1245).
  - The old chain closure could send a household to another household's id.
  - Burkina Faso now links all 3,227 panel households. The old chain guard had dropped 80
    links and served 8 households under another household's id.
  - `IdWalkError` refuses an undecidable or mixed wave. Corpus-wide cold re-warm.
- **A missing `u` is `'Unknown'`, cold and warm alike** (#1251). Before, a cold build served it
  as NaN and a warm read as a minted `'unit'`.
  - Malawi `crop_production`: 8,902 rows.
  - Malawi `plot_inputs`: 6,075 rows.
  - Côte d'Ivoire `crop_production`: 6,358 rows.
  - Mali `food_acquired`: 1,534 rows.
  - Panama `food_acquired`: rows 570,939 → 570,942.
  - Corpus-wide re-warm.
- **Row order no longer depends on `PYTHONHASHSEED`** (#1273). `merge_on` was built from a set,
  so EHCVM rosters were sorted by cluster in some processes and by household in others.
  Corpus-wide re-warm.
- **Numeric text is summed, not concatenated, on the duplicate-row aggregation path** (#1636).
  `"2"` + `"3"` gives `5`, not `23.0`. Malformed non-missing text now raises. Corpus-wide
  re-warm.
- **Households with no market label are kept** (#1424). Under `market=` they are served with
  `m` NA and a `NullIndexKeyWarning` (`site='market-index'`), instead of being deleted.
  - Households affected, by `Region`: Albania 10,488, Tanzania 340, Uganda 129,
    Tajikistan 54.
  - By `District`: Tanzania 7,529, Uganda 2,208.
  - A consumer `groupby('m')` still drops them.
- **A failure no longer reads as absence** (#1589, #1599, #1300, #1303).
  - Builds that used to serve 0 rows, or a shortened country table, now raise the cause:
    - a failed wave producer;
    - a declared-wave `KeyError`;
    - a failed country Make recipe;
    - a corrupt crosswalk.
  - The coverage manifest records only waves whose build completed (#1599).
  - #1589 measured nothing served moving on 585 cold calls over 38 countries (59,116,645 rows
    both sides). The one call that changed is Armenia `household_characteristics(waves=['1996'])`:
    0 rows became `FileNotFoundError`.
- **Crop transforms retain `country`** (#1600). On pooled frames, reductions, kg inference and
  yield joins no longer sum across countries. Before, one shared unit label pooled independent
  5 kg and 20 kg basket estimates into 12.5 kg for both.
- **`tlu()` keeps supplied country identity** (#1620). Distinct households sharing local
  `(t, i)` keys are no longer pooled.

### Food quantities, units and kilograms

- **EHCVM `food_acquired.u` carries the recorded unit size** in all eight EHCVM countries
  (#1458, issue #1224). Examples: `Tas (Petit)`, `Tiya (Entier)`, `Bol (Grand)`. Every EHCVM table
  hash moves (147 of 147).
- **Unsized labels pool their base unit across sizes** (#1474, issue #1450). This is the
  fix the maintainer required before this release.
  - Mali 2014-15 kg: 12,927,468 after #1224, 12,562,371 now (12,563,052 before #1224).
  - Burkina Faso 2014 kg: 333,482 → 334,742.
  - Median absolute log error against EHCVM's `ehcvm_nsu` weights, Mali 2014-15:
    0.752 → 0.461.
- **The released EHCVM NSU unit-weight tables now supply `survey_kg`** for Burkina Faso, Mali,
  Niger and Senegal food quantities in 2018-19 and 2021-22 (#1597).
  - Each row carries a registered `Quantity_kg`, built from national row medians, Niger's
    latest weighing and label-based 2018 product joins.
  - Pre-EHCVM waves, size pooling and request-subset invariance are preserved.
  - The PR states no kg totals.
- **A `waves=` subset is a slice of the all-waves result** for the kilogram modes of
  `food_quantities` and `food_prices` (#1511, issue #1487).
  - 165 of 165 subsets are frame-equal to the all-waves slice.
  - Cost on a warm single-wave read: Uganda 3.7 → 6.2 s, Mali 6.0 → 10.6 s, GhanaLSS
    23 → 53 s; peak RSS rises 0.2-0.6 GB.
  - Follow-ups in #1542: a kilogram subset whose waves declare no food table skips the
    all-waves read.
- **Mali 2014-15:** the purchased-food expenditure sentinel is served as missing (#1634).
  Expenditure falls from 5,652,532,205 to 152,532,260 XOF, and 40 newly all-missing rows drop
  (90,925 → 90,885).
- **Mali 2018-19 food labels lose their padding** and reach `harmonize_food` (#1587).
  - `j` goes 138 → 125.
  - Household `(j, u)` cells with a price go 216 of 910 → 294 of 835.
- **Nigeria W4 food units carry the recorded size** (#1508). Before, 48,305 and 50,181 rows
  on eleven codes were all served as `small`.
- **Malawi:** no code-23 kg factor on `Unknown` rows (#1428). 49 rows move from `survey_kg` to
  `none`, and 7,143.49 kg to 0.
- **Senegal:** missing food-unit labels are served as `Unknown` (#1641).
- **Under `LSMS_NO_CACHE`, the YAML-path `food_acquired` is no longer degraded** (#1504).
  - Before, `Derivation` was lost in all 8 EHCVM countries and Senegal lost 21 rows. The result
    was stamped fresh.
  - One `{1,true,yes}` predicate now governs both the read gates and the YAML wave-cache writer
    (#1642).
- **New `period=` conversion** of `food_quantities` and `food_expenditures` to
  `'day'|'week'|'month'|'year'`, from each wave's declared recall exposure (#1671, #1271). It is
  opt-in; `period=None` is unchanged.
  - The recall records were corrected: Mali 2014-15 has two seven-day asks, and Burkina 2014
    quantities have one ask against four expenditure asks.
  - Coverage grew from 32 to 48 of 67 cells. Uganda `period='day'` now converts 7 waves
    (220,277 rows) instead of 1.
- **`median_price_valuation` groups prices within each survey wave by default** (#1399,
  issue #1254). `pool_waves=True` restores cross-wave pooling.
- **Sold quantities are valued in their own units** (#1355). The valuation uses `Unit_sold` or
  a screened `KgFactor_sold`, and falls back to the harvest `u` only when there is no sold label.
- **`numeraire=` conversion now uses interview dates in seven EHCVM countries** (#1595). It
  reads `'Interview start'`. FX falls 2.3-8.6% per wave (Niger 2018-19 −2.49%, Niger 2021-22
  −8.55%).

### Panels, samples and clusters

- **Nigeria panel crosswalk rebuilt** over all waves, interviewed households and own covers
  (#1449). `panel_ids` entries go 14,251 → 39,326. #1328 also builds `sample` and clusters
  from each round's own cover: round-households 49,770 → 49,089, and `cluster_features` has
  7,128 rows.
- **Ethiopia:**
  - W3's ESS1 households link to W1, and Panel II and W2-W1 are recorded (#1558). `panel_ids`
    entries go 4,981 → 13,688.
  - The make backend serves the committed crosswalk (#1470).
  - Households from recorded agriculture covers are retained (#1359). This adds +25 W2 and
    +27 W3 links.
  - W2's 3 livestock-only households join the panel map (#1581).
- **Burkina Faso:**
  - The 2018-19 targets in the 2021-22 crosswalk now use the served id (#1249). Of 3,227 panel
    households, 1,088 targets had been ids no table serves.
  - Household samples are separated, and the 2021 cross-section's 7,176 households join
    `sample` and `household_roster` (#1322), `cluster_features` and `interview_date` (#1418).
    Before, `market=` lookups dropped all 7,176.
  - The 2014 sample gains 389 households that the welfare file omits (#1522).
- **EHCVM `sample.strata` is region × milieu** in Senegal, Mali, Niger and Burkina Faso (#1572).
  - Before → after, strata per wave:
    - Senegal: no column → 28, 28.
    - Mali 2018-19, 2021-22: 0 → 20, 17.
    - Niger 2014-15, 2018-19, 2021-22: 0 → 25, 16, 15.
    - Burkina 2018-19, 2021-22: 0 → 26, 26.
  - Benin, Togo, Guinea-Bissau and Côte d'Ivoire get 2018 strata in #1693. The development
    tip's PR body still lists its cold/warm data acceptance as pending; see `CHECKLIST.md`.
  - Burkina 2014 strata labels landed in #1690.
- **Uganda:**
  - Early-wave EAs get one id each (#1232). Households keeping `v` across 2005-06 → 2009-10
    go 0/2,607 → 2,410/2,607.
  - `sample()` gains the design PSU `ea` (#1472, #1534). Non-null `ea` in 2019-20:
    2,823 → 2,868.
  - Early nonfood household ids now match the sample (#1325): 54,027 rows.
  - Each household's own `District` is served on `sample` (#1478).
- **Mali:**
  - The EAC-I id namespace now carries one prefix per wave, `EACI14_` and `EACI17_` (#1515,
    #1586). Ids shared across waves go 3,560 → 0.
  - Every 2021 household is namespaced before the panel mapback (#1638).
  - The new ids move served `i` in 2014-15 and 2017-18.
- **Tanzania `District`:**
  - It is named in 2008-09 and 2010-11 as a registered derivation (#1488). Non-null clusters:
    0 → 409 of 409, and 0 → 406.
  - Two 2010-11 clusters are named, and 2019-20 split-offs take their parent's cluster (#1498).
    2010-11 `District` reaches 408 of 409.
- **GPS:**
  - Malawi IHPS panel EAs get the stayers' fix: 101/102 missing → 0 (#1279). Malawi 2013-14:
    180 → 204 EAs (#1296).
  - Burkina Faso, net change: 2021-22 panel grappes now carry their 2018-19 GPS fix and 2014
    takes a modal province (#1465). Later PRs then withhold unreliable or copied GPS points. PR #1522
    withholds six carried fixes (525 → 519), and #1583 withholds the copied 2018-19 fixes on nine
    grappes (555 → 546) and stops carrying three more (519 → 516).

### Agriculture

- **`harvest_kg` keeps the crop axis** whichever name a country uses (#1492). kg totals are
  unchanged. Rows, before → after:
  - Benin 5,320 → 6,020.
  - Malawi 62,911 → 92,745.
  - Nigeria up as well: 8,365 of 19,007 plots hold 2 or more crops.
- **`harvest_kg` and `crop_diversity` exclude `unknown_plot` and report it** (#1543). Uganda
  `Harvest_kg` rows go 89,207 → 88,980.
- **A Farm-level sale is no longer counted as a further crop occurrence** (#1491).
- **Uganda reported kg factors carried at native production grain** (#1207). This newly
  populates 82,103 factors, and harvest totals go 29,095,413 → 42,026,695 kg.
- **Multi-plot household-crop sales are served on a `Farm-level` row** (#1567, #1568, #1473).
  - Niger 2014-15 `Value_sold`: 78,485,760 → 113,169,510 FCFA.
  - Nigeria W3/W4/W5 `Value_sold`: 0 → 196,085,180 / 226,761,273 / 716,443,945.
  - Exact source repeats are dropped first.
- **`crop_production` gains an optional `Quantity_kg` and a `survey_kg` rung**, for Niger and
  Mali 2021-22 (#1563). Positive Niger quantities with no kilogram go 7,157 → 86, 7,333 → 87
  and 9,798 → 0.
- **Mali 2021-22 harvest totals come from the `s16d` dispositions** (#1515, issue #1437). 3,360
  totals; `harvest_kg` 261,596 → 6,977,514 per the issue record.
- **Unique observation levels where the old index destroyed rows:**
  - Niger: 916 rows the core collapse destroyed are now served (#1420).
  - Ethiopia `production_obs`: 605 duplicated §9 records are dropped (#1477).
  - Nigeria `production_obs`: 822 previously discarded lines are served (#1550).
  - Tanzania `input_obs`: `plot_inputs` 9,709 → 9,815 rows (#1430).
- **Mali `plot_inputs`** serves the plot-level use answers: 113,735 → 179,817 rows (#1223).
  Missing units are served as `Unknown` (#1222).
- **Malawi `plot_inputs`:**
  - Seed purchases are gated on value (#1422). 2016-17 purchase rows go 741 → 5,627, and
    2019-20 go 748 → 6,345.
  - Seed acquisition quantities are preserved (#1694): 179,957 → 222,662 rows.
  - Instrument-covered applied totals are kilograms (#1193, #1238).
  - Input code 1 is Chitowe NPK 23:21:0+4S at 0.23 N (#1446). 26,563 of 37,076 plots move in
    `nitrogen_kg`.
  - The seed-purchase fan-out and the blank harvests served as 0 are fixed (#1269).
- **Uganda `plot_labor`:** a missing use-flag is not 'No' (#1255). 5,185 fabricated
  `PersonDays == 0` rows are removed: 169,877 → 164,692, then 164,777 after the wage-only ruling.
- **Burkina Faso `plot_inputs`:**
  - Distinct native slots are kept (#1189): 22,694 → 22,877 rows, recovering 2,409.58 kg.
  - Code 21 is groundnut seed (#1427): 602 rows.
- **Ethiopia:**
  - `crop_production` §11/§12 sales (#1257, #1274) serve `0 → NaN` cells where all inputs were
    blank. W3/W4/W5 `Quantity_sold` has 542/816/819 such cells.
  - A missing seed-purchase answer stays missing (#1425): W1 45,149 → 44,778 rows.
- **Niger ECVMA inputs** recover 73 + 45 lost occurrences (#1336).

### Community prices, shocks, livestock and other tables

- **Community prices:**
  - Mali restores all three readings (#1180): 155,931 rows, +115,713.
  - Nigeria recovers ten C8 units (#1192): 131,338 → 136,566 rows. W3 C8a/C8b are served as
    `obs` 1/2 (#1268).
  - Malawi serves a unit on every row (#1239). Before, 38,506 of 82,635 rows had no unit.
  - Niger maps 683 CS07 rice varieties to the household rice labels (#1426).
  - Make-backend wave requests are honored for Niger (#1200) and Tanzania (#1201).
- **Shocks, EHCVM:**
  - `AffectedConsumption` in 2018-19 and 2021-22 used to read the livestock herd-size question
    (`s14bq04d`, "Effectif du cheptel") (#1564, #1579).
  - Under the 2026-11-06 ruling it now records food purchases, and a new `AffectedFoodStock`
    records food stocks. This covers 22 waves in ten countries (#1596); served row counts are
    unchanged (271,973).
  - `HowCoped` keeps code 25 (#1564): `HowCoped0` NA goes Niger 35 → 0, Mali 72 → 0.
- **Côte d'Ivoire 2018-19 shocks** decode Stata `.d` "not concerned" answers (#1661). 13,778
  rows are served with the five effects as nullable booleans.
- **Malawi livestock:**
  - `Owned` is RaisedOrOwned (#1593, #1523): rows 39,121 → 39,126.
  - A blank head count stays blank (#1486): a few rows per wave.
- **Burkina Faso 2014 food** is served section C block by block (#1431). In-kind rows go
  0 → 1,468.
- **Burkina Faso `District`** has one spelling per province (#1242): 96 → 45.

## New data

### Waves: held, registered and not yet wired

- **Malawi 2024-25** (#1506, issue #1275). IHS-VI (WB 8507, 81 files) goes in
  `Data/Cross_Sectional/` and IHPS-V (WB 8520, 82 files) in `Data/Panel/`, through
  `add_wave(subdir=)`. Held and registered, with no tables wired. Wiring is in the pending batch.
- **EHCVM-2 2021-22: Benin (6272), Côte d'Ivoire (6273), Guinea-Bissau (6274), Togo (6279)**
  (#1506). Held, with BIDs, household questionnaires and `LICENSE.org`; not wired. Wiring is
  in the pending batch (issue #1697).
- **Liberia 2014-15 and 2016-17 (HIES)** and **Armenia 2001-2017 (ILCS)** are registered in
  `Country.waves`, with population records and no table wiring (#1514, #1580).
- **Held, deliberately not registered** (#1514): Nigeria `2018-19-NLSS/` (WB 3827),
  Tanzania `2014-15-EXT/` (WB 3455), KHDS, HBI, and the Tanzania W1-W3 geovariables (issue #449).
- **Release refreshes:**
  - Ethiopia 2021-22 ESPS-5 v03_M (#1284).
  - Nigeria 2023-24 GHS-Panel W5 v02_M, which adds household geovariables (#1285).
  - Malawi 2016-17/2019-20 Panel provenance corrected to catalog 3819 v06 (#1286).

### Tables and columns now served

- **Community prices:**
  - Niger 2018-19, 84,444 rows over 504 grappes (#1227).
  - Benin, Guinea-Bissau and Senegal 2018-19 (#1505): 222,296 / 80,202 / 126,624 rows.
  - Burkina Faso 2018-19 quotes with native identities (#1374).
  - Mali EHCVM quote slots (#1356).
  - Nigeria W1/W2 native varieties (#1378).
- **Crop production:**
  - Mali EHCVM 2018-19 (`s16c`, 7,992 rows) and 2021-22, with household-crop sales (#1466).
  - Ethiopia W1-W3 §12 tree/fruit/root sale module (#1257).
- **New columns:**
  - `Unit_sold`: Burkina 2018-19 (#1270) and Mali 2014-15/2017-18 (#1272).
  - `Value_harvested`: Nigeria (#1419).
  - `HeadCount12MonthsAgo`: Malawi livestock (#1421).
  - `condition` / `condition_native` on `crop_production`: six EHCVM countries (#1553) and
    Niger (#1471).
  - `condition_sold_native`: Niger (#1584).
  - `Condition_sold`: Burkina Faso and Mali (#1605).
- **Household plot inputs:**
  - Burkina Faso 2021-22 and Mali 2018-19/2021-22 household input applications (#1686, #1687).
  - Senegal 2021-22 `plot_inputs`, 9,368 rows (#1695).
- **Niger 2011-12:**
  - `cultivation_records` and `farm_cost_operands`, from sections 3B and 3D (#1658).
  - Native dry-season input observations (#1632).
  - The 2011-12 dry season, NPK 15-15-15 and ECVMA first-entry community prices (#1527).
- **Nigeria W3** post-harvest `food_coping` (#1670): 4,582 households, 41,238 strategy records.
- **Plot geography:**
  - Ethiopia plot geovariables in native units (#1352).
  - Burkina Faso 2021 parcel coordinates (#1194).
  - Nigeria W5 household reference coordinates (#1335).
- **Documents now managed through DVC:**
  - Ethiopia W5 household and agriculture questionnaires (#1685).
  - The Ethiopia W1 manual (#1667).
  - The Mali EAC-I 2017 BID (#1635).
  - Three original EHCVM sampling documents (#1640).

## New guards, warnings and levers

- **Failure is not absence** (#1589). The derived-table and legacy fall-throughs catch only the
  four generic builtins, and only where a legacy build exists. They emit
  `LegacyFallbackWarning` naming the cause. Also new:
  - `SampleUnavailableWarning` on the v-join and `market=`.
  - `errors.WaveWorkerError` for a worker exception that does not survive pickling.
  - Related: #1625 preserves wave failures when parent exception decoding fails.
- **Proven wave completion** (#1599). `lsms_cache_waves` records only completed waves, and
  producer, hook and script errors propagate.
  - #1300 refuses declared-wave `KeyError`s before caching.
  - #1303 propagates failed country Make recipes.
  - #1645 requires current native inputs for direct Make builds.
  - #1327 honors wave subsets after direct Make builds.
  - #1656 preserves explicit empty coverage.
- **Site Q (`LSMS_QUANTITY_STRICT`) widened:**
  - A lower tail for near-zero quantities, and an upper rule on `food_prices.Price` (#1544).
  - Plot-input quantities and species-specific livestock heads (#1618).
  - Crop-sale quantities and native sale prices (#1650).
  - The reference is taken from strictly positive values (#1266).
  - `kg_survey_off_label` names a survey kilogram total outside 0.1x-10x of its unit's stated
    mass (#1582), on 107 of 5,642 sized rows. It refuses nothing.
- **Site C (cluster identity):**
  - `sampling.yml` boundary records for Burkina Faso (#1246) and Niger (#1248), so
    `ClusterIdentityWarning` fires there.
  - `design_tag: not-stated`, with Albania 2008 as the first record (#1541).
  - The warning quotes the recorded boundary sizes (#1677).
- **Other new diagnostics:**
  - `ShippedFactorWarning` when unqualified shipped factors span countries (#1613).
  - TLU discloses unsupported species without changing values (#1614).
  - `PlotGrainMismatchWarning` when a non-empty fertilizer-rate result omits whole eligible
    country/wave cells (#1621).
  - Native and finalized dtype checks are distinguished (#1627, #1630).
  - Proven-missing Make targets stay quiet, and unwired tables are explained (#1676, #1678).
- **New typed refusals:**
  - `IdWalkError` (#1282).
  - `UnitSizeLabelError` (#1458).
  - `StratumError` (#1572).
  - `ShockLayoutError` (#1579).
  - `SplitOffClusterError` (#1498).
  - `OriginEaError` (#1534).
  - `EmcSampleError` (#1522).
  - Duplicate derivation registry keys are rejected (#1624).
- **Degraded sources and caches:**
  - Secondary inputs degrade on five explicit connection-failure classes (#1665).
  - A missing-credentials secondary source is handled (#1629, #1646).
  - Every missing source is required before healing a multi-file input (#1666).
  - Declared input interruptions are tracked (#1669).
  - Offline heal probes are bounded (#1673).
  - Recovery after local cache permission failures (#1663, #1683).
  - Stored grain diagnostics replay on `assume_cache_fresh` reads (#1674).
- **CI:**
  - A required full-data validation check for ready development PRs (#1680).
  - Credentialed output is captured before public logs (#1294).
  - Uganda invariance reports its coverage and can require complete comparisons (#1609).

## API changes and deprecations

- **New keyword arguments:**
  - `period=` on `food_quantities()` and `food_expenditures()` (#1671). Prices and other tables
    refuse it.
  - `pool_waves=` on `median_price_valuation` (#1399); the default changes to within-wave.
  - `area_t=` on `yield_kg` (#1548), for surveys that measure plots in another round.
  - An opt-in Nigeria W3 native harvest conversion helper (#1622).
- **New index levels and columns** (listed above):
  - `production_obs`: Ethiopia (#1477), Nigeria (#1550).
  - `input_obs`: Tanzania seed rows (#1430).
  - `condition` and `condition_native` (#1553, #1471).
  - `Quantity_kg` on `crop_production` (#1563).
  - `sample().ea`: Uganda (#1472).
  - `sample.strata`: EHCVM (#1572, #1693).
  - `AffectedFoodStock` (#1596).
  - Vocabulary registered in `data_info.yml`: `unknown_plot`, `unknown_crop` and the
    `(not crop-specific)` marker (#1295).
- **Feature assembly:**
  - An assembly no country survived keeps its `country` index (#1490).
  - Providers missing required household axes are excluded before shape voting (#1619).
- **Acquisition:**
  - `add_wave(subdir=)` for two-entry waves (#1293).
  - The `#+CATALOG_SUBDIR:` mapping, refresh-only re-acquisition, and per-entry
    `#+CATALOG_HELD:` / `#+CATALOG_LATEST:` lines (#1293, issue #1283).
  - Reacquired members are placed at their recorded sidecar paths (#1211).
- **Removed:**
  - The implicit exception for the retired `food_prices_quantities_and_expenditures` name
    (#1651). An undeclared request now fails the ordinary unknown-method check.
  - The retired food Make targets in Togo, Côte d'Ivoire and Afghanistan (#1655).
  - Malawi's obsolete raw derived-food registrations (#1208).
  - The duplicate country community-price builders in Mali, Niger and Tanzania (#1199, #1200,
    #1201).
- **Contract changes that can raise where 0 rows were served before:** #1589, #1599, #1300,
  #1303, and #1636 (malformed numeric text).
- **CFE demand inputs** aggregate expenditure sources explicitly, and the demand-estimation
  skill recipe is fixed (#1612).
- `trust_cache=` remains a deprecated alias of `assume_cache_fresh=`; no new deprecations were
  found in the PR bodies.

## Cache note

- **`LSMS_CACHE_SCHEMA` is unchanged at 5** (`lsms_library/local_tools.py`, both at `v0.16.0`
  and `e5fdd2fa8`). All invalidation in this release comes from content-hash and
  build-fingerprint movement, not from that lever.
- **Expect a corpus-wide cold re-warm on first use, at both L2 tiers.**
  - Each of the following PRs states that it moves every country-table hash, every non-null
    wave hash and every build fingerprint (per each PR body): #1197, #1214, #1220/#1221, #1233,
    #1251, #1273, #1282, #1303, #1309, #1601, #1629, #1636, #1643, #1651, #1661, #1663, #1665,
    #1669, #1673, #1676.
  - #1327 and #1372 change shared build fingerprints. #1599 changes all table fingerprints by
    intent, invalidating old incorrectly complete manifests.
  - `Wave.grab_data` folds `build_transforms_fingerprint` into `Wave._input_hash`, so
    L2-wave parquets are stale too. Nothing harmonized is reusable; L1 blobs are.
- **Country-scoped re-warms** are additionally stated for most country PRs: every Malawi,
  Mali, Niger, Nigeria, Ethiopia, Uganda, Tanzania and Burkina Faso table, plus Senegal,
  Côte d'Ivoire and Togo. These fold into the corpus-wide re-warm above.
- **Reported zero-hash changes** (no re-warm of their own): #1195, #1211, #1216, #1271, #1284,
  #1285, #1286, #1293, #1295, #1424, #1545, #1548, #1582, #1595, #1608, #1614, #1618, #1619,
  #1620, #1624, #1625, #1627, #1628, #1635, #1671, #1677. Also the `sampling.yml` records
  (#1246, #1248, #1541) and `CONTENTS.org`-only PRs.
- **Stale roots between upgrade and re-warm (Ethiopia, #1558):**
  - On the default backend, `check_panel_consistency` fails `id_walk_idempotent` on an old
    `var/household_roster.parquet`; one `household_roster()` read heals it.
  - On the make backend, `nutrition` raises `IdWalkError` until
    `lsms-library cache clear --country Ethiopia`.
  - Under `assume_cache_fresh=True`, the 49 relinked households are served under old ids.
- **Performance:** a warm single-wave kilogram-mode food read now reads all waves (#1511):
  GhanaLSS 23 → 53 s. A cold all-waves build is paid once per cache.
- **#1504** removes a path by which `LSMS_NO_CACHE=1` wrote a degraded `food_acquired` parquet
  stamped fresh. A cache written that way under v0.16.0 is invalidated by the corpus-wide hash
  move.

## Fixes, grouped by country

Every code PR in the range appears once, under its country (from the title), or under
"Multi-country and EHCVM" or "Framework". PRs already described above are repeated here for
completeness.

### Albania (1)

- #1541 Albania, sampling: add design_tag not-stated, first record 2008 (#1134)

### Burkina Faso (18)

- #1189 Burkina Faso: preserve distinct native agricultural input slots
- #1194 Burkina Faso: expose reported 2021 parcel coordinates
- #1203 Burkina Faso: align maize and cowpea seed crop labels
- #1219 Burkina Faso: route purchase and gift input fields correctly
- #1242 Burkina Faso cluster_features: one District spelling per province (96 -> 45)
- #1246 Burkina Faso: declare v continuity in sampling.yml (2014 -> 2018-19 warns; panel boundary stable)
- #1249 Burkina Faso panel crosswalk: 2018-19 targets use the served id (correct links 2,041 -> 3,147)
- #1270 Burkina Faso 2018-19 crop_production: carry the recorded sold unit as Unit_sold (#824)
- #1282 id_walk: single-pass rename applied exactly once; Burkina links all 3,227 panel households (corpus re-war
- #1307 Honor Burkina table declarations and refuse incomplete plot compilation
- #1322 Separate Burkina household samples and include the 2021 cross-section
- #1374 Add Burkina Faso 2018-19 community price quotes with native identities
- #1418 Burkina Faso 2021-22: cluster_features and interview_date read the cross-section cover (#1276)
- #1427 Burkina Faso 2018-19: serve input code 21 as groundnut seed (#1187)
- #1431 Burkina Faso 2014: serve food section C block by block, each in its own unit (#864) [held for decision]
- #1465 Burkina_Faso: cluster_features: 2014 modal province, 2018-19 GPS carried to panel grappes (#1060, #1057)
- #1522 Burkina Faso: withhold six carried GPS fixes, extend the 2014 sample (#1057, #1464)
- #1583 Burkina Faso: withhold the copied 2018-19 GPS fixes and stop carrying three (#1057)

### Ethiopia (20)

- #1181 preserve Unknown units on Ethiopia input-use records
- #1257 Ethiopia crop_production: read the §12 tree/fruit/root sale module (W1-W3), registered as a window deriva
- #1274 Ethiopia crop_production bundle (stacked on #1257): §11 sale fabrications, wave-keyed crop codes, W4/W5 w
- #1284 Ethiopia 2021-22: hold ESPS-5 v03_M (acquired; retire the duplicate plot-geovariables sidecar)
- #1315 Honor Ethiopia table declarations and refuse partial country compilation
- #1319 Select Ethiopia food inputs through Country/Wave declarations
- #1324 Distinguish missing Ethiopia harvest units from reported Other
- #1330 Preserve missing Ethiopia seed-purchase quantities during grouping
- #1352 expose Ethiopia plot geovariables in native units
- #1359 retain households from recorded agriculture covers
- #1365 mark structural plot inputs as not crop-specific
- #1371 walk country food household IDs on i
- #1375 Make nutrition builds offline with a versioned Ethiopia FCT snapshot
- #1376 Retire Ethiopia nutrition’s redundant food prerequisite and unused FCT target
- #1413 Ethiopia: count out-of-window interview dates with the build-time parser (#1056)
- #1425 Ethiopia: a missing seed-purchase answer stays missing (#1183)
- #1470 Ethiopia: make backend serves the committed crosswalk (#1447); account for agriculture-only households (#
- #1477 Ethiopia: give crop_production a production_obs level and drop §9 repeat records (#1461)
- #1558 Ethiopia: link W3's ESS1 households to W1 and record Panel II and W2-W1 in panel_ids (#1467)
- #1581 Ethiopia: add W2's 3 livestock-only households to the panel map (#1467)

### Malawi (29)

- #1175 Malawi: restore reported organic fertilizer quantities and units
- #1193 bind instrument-covered applied totals to kg
- #1208 Malawi: remove obsolete raw derived-food registrations
- #1213 Malawi: assemble food through canonical native waves on Make
- #1229 Malawi: recover 90 kg bag and dengu unit text on the harvest and sale sides
- #1238 Malawi plot_inputs 2013-14/2016-17: applied totals are kilograms (questionnaire-confirmed)
- #1239 Malawi community_prices: serve a unit on every row (form-fixed units for struck items; Unknown otherwise)
- #1258 Malawi Makefile: per-wave rules depend on their wave script (make backend rebuilt nothing on a script edi
- #1269 Malawi: seed-purchase fan-out, blank harvests served as 0, 2010-11 exact repeat records (#637 tail)
- #1279 Malawi IHPS panel EAs: acquire and wire the household geovariables (GPS 101/102 -> 0 missing); stayers' f
- #1286 Malawi 2016-17/2019-20: the Panel files are catalog 3819 v06, not 2939 (provenance correction)
- #1296 Malawi 2013-14: serve each IHPS EA its stayers' fix (#1260 rule, one shared implementation)
- #1313 Malawi: honor table declarations and refuse partial country compilation
- #1323 Preserve Malawi seed source observations across all four waves
- #1351 reuse held blobs in restricted plot-input reader
- #1368 Preserve Malawi missing-plot labor observations
- #1373 Preserve Malawi native crop codes in production grain
- #1377 Reuse held cached bytes for Malawi’s restricted plot-feature reader
- #1421 Malawi livestock: serve the 12-months-ago stock ag_r07 as HeadCount12MonthsAgo (#855)
- #1422 Malawi plot_inputs: seed purchases gated on value, free seed on its own (#1190); missing Module D amounts
- #1428 Malawi food units: no code-23 kg factor on Unknown rows (#1074); unlisted suffixed codes record no unit (
- #1446 Malawi: serve input code 1 as Chitowe NPK 23:21:0+4S at 0.23 N (#1106)
- #1486 Malawi livestock: a blank head count stays blank, and the rows this leaves empty are registered (#1435); 
- #1523 Malawi: IHS5 id namespace, seed purchase unit, cowpea label, livestock ownership (#1278, #1190, #1174, #1
- #1593 Malawi: livestock Owned is RaisedOrOwned; register the 1,355 excluded seed lines (#1435, #1174)
- #1607 Disclose Malawi’s existing DAP deletion through raw-input provenance
- #1653 Expose Malawi native sources to private build preparation
- #1681 Reuse cached source bytes in Malawi 2016–17 labor reader
- #1694 Malawi: preserve seed acquisition quantities in plot inputs (#1190)

### Mali (17)

- #1180 Mali: restore all three native community-price readings
- #1186 align Mali labor source casing with existing selectors
- #1199 Mali: retire the duplicate country community-price builder
- #1222 Mali plot_inputs: serve a missing unit as Unknown
- #1223 Mali plot_inputs: serve the plot-level use answers ('?' labels)
- #1231 Mali plot_inputs: not-crop-specific marker, nitrogen skip count, use-answer convention documented
- #1272 Mali crop_production: carry Unit_sold (2014-15, 2017-18); tree Quantity is per-month harvest, not the mon
- #1317 Honor Mali table declarations and refuse partial country compilation
- #1326 Preserve Mali tree sales as recorded under the revised window ruling
- #1392 Carry Mali field sold-unit factors with exact agreement and audit
- #1515 Mali: EAC-I id namespace, community units and Quantity, 2021-22 harvest total (#1494, #1453, #1437, #1179
- #1586 Mali: one EAC-I prefix per wave, EACI14_ and EACI17_ (#1494)
- #1587 Mali: strip the padding from 2018-19 food labels so they reach harmonize_food (#1513)
- #1611 Use accented Taoudénit consistently for Mali Region and strata
- #1634 Treat Mali 2014–15 purchased-food expenditure sentinel as missing
- #1635 Add Mali EAC-I 2017 basic information document to managed holdings
- #1638 Namespace every Mali 2021 household before approved panel mapback

### Niger (15)

- #1176 Niger: clear missing CS07 quantities while preserving recorded prices
- #1200 Niger: preserve community-price wave requests on both backends
- #1289 Niger: missing-wave refusal + && for crop_production, livestock, plot_labor, people_last7days (#1241 patt
- #1316 Honor Niger table declarations in six country compilers
- #1320 Build Niger country tables from declared wave producers
- #1336 Preserve Niger ECVMA input occurrences with recorded source-qualified slots
- #1417 Niger: 2011-12 wage_work reads the 30-day question ms04q01 (#882)
- #1420 Niger: serve crop_production native observations so the index is unique (#1360)
- #1426 Niger: map CS07 rice varieties to the household rice labels (#1090)
- #1471 Niger: serve the canonical condition level on crop_production (#1436)
- #1527 Niger: ECVMA first-entry community prices, NPK 15-15-15, the 2011-12 dry season and Condition_sold (#1087
- #1584 Niger: serve condition_sold_native, the 2018-19 sold state as recorded
- #1632 Preserve Niger 2011–12 native dry-season input observations
- #1652 Expose Niger native sources to build dependency discovery
- #1658 Expose Niger dry-season cultivation and payment records

### Nigeria (22)

- #1172 Nigeria: decode recorded W3 food unit labels
- #1178 Nigeria: decode food items using their recorded quarter
- #1192 Nigeria: recover ten recorded C8 community-price units
- #1268 Nigeria community_prices W3: serve both C8a/C8b locations as obs 1/2 (no longer first-wins)
- #1285 Nigeria 2023-24: hold GHS-Panel W5 v02_M (acquisition only; geovariables not yet wired)
- #1311 Nigeria: gate five country compilers on declared waves and propagate failures
- #1318 Date Nigeria's selected education observations at the post-harvest visit
- #1328 Build Nigeria sample and clusters from each round’s own cover
- #1335 Wire Nigeria W5 household reference coordinates through the existing round covers
- #1341 Normalize Nigeria retained plot-input units while preserving hollow-row eligibility
- #1370 Preserve source provenance for Nigeria input crop markers
- #1378 Preserve Nigeria W1/W2 native community-price varieties
- #1416 Nigeria: account for the W3 geovariables household on neither cover (#1261)
- #1419 Nigeria: carry the household's own harvest valuation as Value_harvested (#844)
- #1429 Nigeria: label the W4 purchased row's 30-day window as a registered identity derivation (#881)
- #1449 Nigeria: rebuild the panel crosswalk over all waves, interviewed households, own covers (#1264)
- #1473 Nigeria: serve W3-W5 household-crop sales on their harvesting row or a Farm-level row (#843)
- #1508 Nigeria: W4 unit sizes, interview result, unplaced community units, area-unit record (#1033, #1096, #1444
- #1550 Nigeria: serve every crop_production line under production_obs; sampling.yml notes count the crosswalk (#
- #1622 Add opt-in Nigeria W3 harvest conversion with complete native audits
- #1631 Curate Nigeria groundnut and shelled-maize Aggregate aliases
- #1670 Serve Nigeria W3 post-harvest food coping answers

### Senegal (2)

- #1641 Use Unknown for missing Senegal food-unit labels
- #1695 Add Senegal 2021 household plot inputs (#1483)

### Tanzania (12)

- #1185 normalize Tanzania plot-input units after duplicate reduction
- #1201 Tanzania: build community prices from requested waves
- #1263 Tanzania: plot_inputs keys on u (no incommensurable sums); crop_production mints no zero (#637 tail)
- #1308 Tanzania: stage scoped Section J food-price evidence
- #1312 Tanzania: refuse failed declared concatenator inputs and skip unwired folders
- #1314 Vectorize Tanzania livestock sums and preserve signed reducer behavior
- #1366 Tanzania: preserve crop marker provenance through input reduction
- #1430 Tanzania: seed rows keep their native crop observation via input_obs (#637)
- #1488 Tanzania: name District in 2008-09 and 2010-11 as a registered derivation (#1441)
- #1498 Tanzania: name two 2010-11 clusters and give 2019-20 split-offs their parent's cluster (#1441)
- #1615 Reconcile Tanzania split-off links and document inherited fallback cases
- #1682 Avoid numeric callbacks for singleton Tanzania input and labor groups

### Uganda (20)

- #1173 Uganda: preserve sibling cache provenance during country builds
- #1191 Uganda: exercise runtime-derived APIs with meaningful dtype and index tests
- #1202 Uganda: preserve native crop production observations
- #1207 Uganda: carry reported kg factors at native production grain
- #1216 Uganda: restore parcel-area matches after household ID walking
- #1217 Uganda: build crop production from requested native waves
- #1232 Uganda: one id per early-wave EA (7-char comm -> 8-digit frame EA via a linkage-derived table)
- #1255 Uganda plot_labor: a missing use-flag is not 'No' (5,185 fabricated zeros removed)
- #1310 Honor Uganda declarations and refuse partial country compilation
- #1321 Bridge Uganda 2015–16 housing IDs through the cover sheet
- #1325 Match Uganda early nonfood household IDs to the sample
- #1329 Disclose Uganda parish keys separately from sampling EAs
- #1350 distinguish structural and unrecorded crop identities
- #1358 retain own-cover households and missing-plot observations
- #1364 preserve missing-plot labor observations
- #1383 retain recorded agriculture links for unmatched housing rows
- #1472 Uganda: add the design PSU ea to sample(), inherited through the tracking key in 2018-20 (#689)
- #1478 Uganda: keep AGSEC1-only shocks; serve each household's own District on sample (#1434, #1441)
- #1534 Uganda: ea across all eight waves, with AGSEC1.ea; Aggregate split by state (#689, #1459, #833; notes on 
- #1654 Declare Uganda anthropometry inputs while retaining the source map

### Multi-country and EHCVM (27)

- #1227 Niger 2018-19: wire the EHCVM community price module into community_prices
- #1241 EHCVM plot_inputs: keep distinct native seed slots (Niger 2018-19/2021-22, Senegal, CotedIvoire)
- #1248 Niger: declare v continuity in sampling.yml (ECVMA -> EHCVM warns; both panels stable)
- #1265 panel_ids: Malawi and Nigeria commit their crosswalk as JSON (design A); dict- and frame-identical
- #1271 Recall windows from the questionnaires: 16 ISA food cells (Uganda, Malawi, Mali, Niger, Burkina) -- 0 has
- #1333 Preserve native Ethiopia and Malawi land keys in fertilizer rates
- #1356 preserve Mali EHCVM community price quote slots
- #1466 Mali: wire EHCVM crop_production, 2018-19 from s16c and 2021-22 with household-crop sales (#824)
- #1484 EHCVM: correct the kg-ladder prose, pin '100. Kg', record NPK and strata measurements (#1454, #1445, #812
- #1505 EHCVM 2018-19 community prices: wire Benin, Guinea-Bissau, Senegal; respell Burkina u (#1226)
- #1506 Hold and register Malawi 2024-25 and four EHCVM-2 waves; documents for five countries (#1275, #552, #1084
- #1507 Mali, Malawi: serve null-plot harvest rows on the canonical sentinels (#1476)
- #1514 Acquisitions: Liberia HIES, Armenia ILCS, NLSS, KHDS, HBI and Tanzania geovariables; nothing wired
- #1553 EHCVM: serve condition and condition_native on crop_production in six countries (#1436)
- #1563 crop_production: an optional Quantity_kg and a survey_kg rung, Niger and Mali 2021-22 (#1436)
- #1564 EHCVM 2021-22 shocks: AffectedConsumption reads the food stock; HowCoped keeps code 25 (#1556)
- #1567 Mali, Niger, Tanzania: serve multi-plot crop sales on one Farm-level row (#1457)
- #1568 Malawi, Ethiopia: serve every sale no harvest row holds on a Farm-level row (#1457)
- #1572 EHCVM: sample.strata is region x milieu in Senegal, Mali, Niger and Burkina Faso; Niger 2014-15 reads str
- #1579 EHCVM 2018-19 shocks: AffectedConsumption reads the food stock in each layout (#1556)
- #1595 conversion: _interview_dates reads 'Interview start', so seven EHCVM countries convert numeraire= on inte
- #1597 Use released EHCVM unit weights for 2018-19 and 2021-22 food quantities
- #1605 Serve Burkina Faso and Mali sold-crop condition answers
- #1640 Hold three original EHCVM sampling documents through DVC
- #1667 Manage Ethiopia W1 manual and document Mali strata authority gap
- #1686 Serve own household-input rosters for Burkina 2021 and Mali 2018/2021
- #1693 Wire documented 2018 strata for Benin, Togo, Guinea-Bissau and Côte d’Ivoire

### Framework (84)

- #1678 Explain unwired tables before impossible fallback builds (country.py; classed as docs by title, but it changes code)
- #1182 keep missing keys out of categorical label lookups
- #1195 framework: preserve categorical errors in derived food and roster methods (#1081)
- #1197 Preserve food producer provenance across cache eviction
- #1211 Place reacquired release members at their recorded sidecar paths
- #1212 Preserve recorded string index policy when reading parquet (#1196)
- #1214 Validate wave selections before cache eviction and derived food work
- #1220 Preserve declaration order in constructed empty tables
- #1221 Constructed empty tables carry declared types of optional fields
- #1233 Empty-frame follow-up to #1221: keep read-path dtypes out of the fingerprint; never cache a constructed e
- #1251 map_index: a missing u is 'Unknown', not a minted 'unit' -- cold and warm (corpus re-warm; pair with #123
- #1262 GPS census (#592): 11 of 18 empty waves fixed; the rest documented with catalog evidence (docs only, 0 ha
- #1266 Site Q: reference from strictly positive values only (zeros no longer collapse p90); 0 hashes
- #1273 Wave.grab_data: merge_on order no longer depends on PYTHONHASHSEED (roster row order) -- corpus re-warm, 
- #1293 add_wave(subdir=) for two-entry waves, CATALOG_SUBDIR mapping, refresh-only re-acquisition, and per-entry
- #1295 data_info.yml: register unknown_plot, unknown_crop and the (not crop-specific) marker (comments only)
- #1300 Refuse declared-wave KeyErrors before caching a partial country table
- #1302 Preserve typed empty datetime columns and their index metadata
- #1303 Propagate failed country Make recipes without cached-output fallback
- #1309 Preserve successful empty YAML wave results in Country builds
- #1327 Honor wave subsets after successful direct country Make builds
- #1355 value sold quantities using their own units
- #1372 compose all declared mapping stages
- #1399 Keep median-price valuations within each survey wave by default
- #1424 country: keep households with no market label and warn instead of dropping them
- #1458 ehcvm: carry the unit size in food_acquired u (#1224)
- #1474 food_kg_factors: unsized rows pool their base unit across sizes (#1450)
- #1490 Feature: an assembly no country survived keeps its country index (#1456)
- #1491 transformations: a Farm-level sale is not a further crop occurrence (#1469)
- #1492 transformations: harvest_kg keeps the crop axis whichever name a country uses (#1460)
- #1504 Wave.food_acquired: no reducer on the memory route; one LSMS_NO_CACHE predicate at the read gates (#1455)
- #1511 Food kg factors: a waves= subset is a slice of the all-waves result (#1487)
- #1542 Food dispatch and L2 gate: the four follow-ups from #1487's review (#1512)
- #1543 harvest_kg, crop_diversity: exclude unknown_plot and report it (#1460)
- #1544 quantity_audit: name a near-zero quantity and an impossible price (#1493)
- #1545 yield_kg: join a land key both features already share (#1460)
- #1548 yield_kg: an area_t kwarg for surveys that measure plots in another round (#1460)
- #1582 crop ladder: name a survey total far from its label, and serve it (#1436, #1559)
- #1589 A failure must not read as absence (#1546, #1516, #1524, #1532)
- #1596 separate food purchases from food stocks across surveyed waves
- #1599 Require proven wave completion and propagate producer failures
- #1600 Retain country through crop reductions, inference and yield joins
- #1601 Hash source paths consumed by per-file overrides
- #1608 Keep crop month answers as text for pooled Parquet export
- #1610 Align YAML source prewarming with Wave Data paths
- #1612 Aggregate expenditure sources explicitly before preparing CFE demand inputs
- #1613 Warn when unqualified shipped factors span multiple countries
- #1614 Disclose unsupported species in TLU estimates without changing values
- #1618 Screen reported input quantities and species-specific livestock heads
- #1619 Exclude providers missing required household axes before Feature shape voting
- #1620 Preserve supplied country identity when aggregating TLU
- #1621 Warn when nonempty fertilizer rates omit entire eligible country/wave cells
- #1623 Preserve sample source errors when market labeling lacks cluster identity
- #1624 Reject duplicate derivation registry keys
- #1625 Preserve wave failures when parent exception decoding fails
- #1627 Diagnostics: distinguish native and finalized dtype checks
- #1628 Resolve wave-folder mappings on direct access
- #1629 Handle missing credentials for secondary wave sources
- #1630 Recognize canonical datetimes in finalized dtype diagnostics
- #1633 Share crop-condition decoders while preserving instrument-specific answers
- #1636 Convert numeric text before duplicate-row aggregation
- #1642 Honor parsed LSMS_NO_CACHE values when writing YAML wave caches
- #1643 Keep panel crosswalk publishers out of runtime fallbacks (#1468)
- #1645 Require current native inputs for direct Make builds (#1288)
- #1646 Apply secondary-source soft-drop policy to unreachable endpoints (#1463)
- #1647 Track built unit mappings and unify cold-cache market labels
- #1650 Screen crop-sale quantities and native sale prices in Site Q
- #1651 Remove the retired food-table exception from implicit aggregation
- #1655 Remove retired food Make targets and stale guidance
- #1656 Preserve explicit empty wave coverage in parquet caches
- #1661 Decode source-specific Stata not-concerned shock responses
- #1662 Store livestock household IDs before country parquet serialization
- #1663 Recover degraded-source availability after local cache permission failures
- #1665 Degrade secondary inputs on five explicit connection failure classes
- #1666 Require every missing source before healing a multi-file input
- #1669 Track declared input interruptions in degraded caches
- #1671 Add declared-period conversion for food quantities and expenditures
- #1673 Bound repeated offline heal probes with a scoped connection-failure window
- #1674 Replay stored grain diagnostics on assumed-fresh cache reads
- #1676 Keep proven missing Make targets quiet during fallbacks
- #1677 Quote recorded boundary sizes in cluster identity warnings
- #1683 Keep DVC metadata private and recover unreadable managed caches
- #1687 Resolve ISA input coverage and cache ownership; add managed docs and PR data validation
- #1690 Integrate reviewed country repairs, 2014 BFA labels and bounded PR validation

## Docs

71 documentation-only PRs. Most record country idiosyncrasies, source authority and runtime
checkpoints in `countries/{C}/_/CONTENTS.org`; `CONTENTS.org` is excluded from every cache
hash. Highlights:
- The GPS census of 18 empty waves (#1262): 11 were fixed, and the rest are documented with
  catalog evidence.
- The Tanzania, Mali, Ethiopia, Burkina Faso and Uganda `CONTENTS.org` updates for issues
  #837/#846, #1179/#824, #660/#1055, #1187/#864 and #689/#845/#1290/#1401 (#1410-#1415).
- The country-compiler runtime checkpoints for #1287 (#1342-#1409).
- Corrections:
  - the historical Stata missing-marker diagnosis (#1660);
  - the Nigeria GHS panel cohorts (#1379);
  - the Farm-level crop-row semantics (#1644);
  - the generated skills after the retired food-helper removal (#1659).

Full list:

#1169 Record verified v0.16.0 publication and synchronize release history; #1280 tidy text made 
stale by the ISA batch merge order (cross-PR review); #1334 Record current Malawi plot-input unit 
census and no remaining served residual; #1337 Record Tanzania food-module evidence across six 
waves without closing coverage; #1338 Record Uganda other-table ID rendering check and its coverage 
limits; #1339 Document Malawi cross-sample identifier ambiguity and migration scope; #1340 Document 
Uganda partial-harvest overlap and unresolved accounting; #1342 Record bounded Niger livestock 
producer, compiler and API acceptance; #1343 record Ethiopia seed-instrument evidence and retention 
caveats; #1344 record actual crop compiler/API verification; #1345 qualify cowpea identity evidence 
and remaining decisions; #1346 ground W4 PH food qualifiers in official instruments; #1347 record 
reviewed nonfood compiler and API verification; #1348 record reviewed seed-purchase source 
attribution; #1349 record Tanzania livestock and Nigeria roster runtime checkpoints; #1353 record 
accepted v8 sample and roster checkpoint; #1354 record Tanzania and Malawi actual compiler 
checkpoints; #1361 record Niger remaining compiler runtime checkpoint; #1362 record coping and 
housing compiler checkpoints; #1363 record Tanzania and Nigeria compiler runtime checkpoints; #1369 
Document Mali 2021 price availability from official schemas; #1379 clarify Nigeria GHS panel 
cohorts and linkage scope; #1380 record seven compiler runtime checkpoints; #1381 record plot 
compiler runtime checkpoint; #1382 record food coping compiler runtime checkpoint; #1384 record 
Mali plot_features compiler runtime; #1385 record Uganda seven-wave labor compiler checkpoint; 
#1386 record Mali labor compiler runtime checkpoint; #1387 Document qualified Uganda plot-input 
compiler runtime checkpoint; #1388 Document Ethiopia plot compiler and consumer runtime scope; 
#1389 Document Uganda food-inadequacy compiler runtime checkpoint; #1390 checkpoint Mali plot 
inputs compiler and consumers; #1391 record qualified Ethiopia five-wave labor checkpoint; #1393 
checkpoint Ethiopia crop compiler and consumers; #1394 record Uganda assets compiler and consumer 
validation (#1287); #1395 record qualified Mali livestock compiler checkpoint; #1396 Document 
qualified Ethiopia livestock compiler and recovered delivery; #1397 checkpoint Uganda livestock 
compiler and consumers; #1398 Document qualified Mali people compiler and consumer checkpoint; 
#1400 Record Ethiopia food_coping compiler and consumer checkpoint; #1402 Document qualified 
Ethiopia community-prices checkpoint; #1403 Document qualified Uganda shocks compiler and consumer 
checkpoint; #1404 Document qualified Uganda anthropometry compiler and consumer checkpoint; #1405 
Record Ethiopia people_last7days compiler checkpoint; #1406 record qualified Ethiopia anthropometry 
checkpoint; #1407 record qualified Uganda interview-date checkpoint; #1408 Document qualified 
Uganda plot_features compiler checkpoint; #1409 Document Uganda food compiler and API verification; 
#1410 Tanzania CONTENTS.org: correct the #837 district-residue account and record #846 API facts; 
#1411 Mali CONTENTS.org: re-measured #1179 unit codes, #824 EHCVM crop source facts, #1224 
unit-size loss; #1412 Ethiopia CONTENTS.org: BID account of agriculture-only households (#660) and 
W5 visit structure (#1055); #1414 Burkina Faso CONTENTS.org: questionnaire evidence for input code 
21 (#1187) and 2014 food blocks (#864); #1415 Uganda CONTENTS.org: cover EA key (#689), price 
residual (#845), housing-key search (#1290), anthropometr; #1496 Records: Senegal's quantities near 
1e-19 (#1489), and evidence for #1464, #1462, #1437; #1580 AGENTS.md: Armenia is held and not 
wired; #1617 Record own-form conflicts and unresolved sold-weight cases in Niger and Mali; #1626 
date historical Nigeria measurements in framework docstrings; #1637 Document planned EHCVM 
stratification for four countries; #1639 Document the bounded Ethiopia W1 key-formatting 
investigation; #1644 Clarify that Farm-level crop rows can carry harvests (#1495); #1648 Update 
agriculture registration and source-coverage notes; #1649 Document Guinea-Bissau GPS field units 
and unresolved export relationship; #1657 Clarify Niger and Nigeria crop-source evidence; #1660 
Correct the historical Stata missing-marker diagnosis; #1664 Record Tanzania UPD evidence and 
remaining original-round fielding gap; #1668 Clarify printed harvest routing in Mali and Burkina 
Faso; #1672 Record recall validation and Uganda's food-item key authority; #1675 Record Ethiopia 
component authority and Niger released-gate evidence; #1679 Document Mali 2017 harvest-state authority gap; #1684 Clarify 
country input grain and unresolved source authority; #1685 Archive and document Ethiopia W5 
household and agriculture questionnaires

### CI and test harness

#1170 LSMS-ISA backlog: inventory, precise residuals and execution evidence; #1236 test(Burkina 
Faso): expect the nullable string u that #1196 preserves (red on development since #1212); #1294 
ci: capture credentialed build and test output before public logs; #1299 ci: execute private output 
tests in the Windows smoke step; #1301 ci: pin Windows Miniforge bootstrap to the observed working 
release; #1305 Use pandas to reduce the Ethiopia quintal test mask; #1306 Align worker verification 
guidance with private runtime and caches; #1332 Load Nigeria source-test oracle by sibling path; 
#1367 test: guard partial Uganda baselines and Nigeria sold-unit caches; #1423 Require pinned 
source and coordinator review in backlog sweep helpers; #1479 tests: re-pin 40 stale Uganda, Malawi 
and Mali pins so red runs carry signal (#1432); #1481 Uganda baseline: the entries tonight's 
reviewed merges moved (#1432); #1521 tests: clear the record loaders between tests (#1503); #1538 
FOR MAINTAINER APPROVAL: Uganda baseline, var/sample after the ea work; #1609 Report Uganda 
invariance coverage and require complete comparisons on demand; #1659 Refresh generated skills 
after retired food-helper removal; #1680 Require full data validation for ready development PRs

## Batch of 2026-11-12 (to be filled after merge)

> Placeholder. The maintainer decided on 2026-11-12: "One release after batch". Fill each
> entry with PR numbers, the served-number moves (countries, tables, sizes), the cache cost,
> and any issues the batch resolves. The issues already flagged as batch-dependent are listed
> in `CLOSES.md` under "Excluded".

- **Malawi 2024-25 wiring** (IHS-VI / IHPS-V; #1275, #1604; shocks ruling per #1556). PR: _TBD_.
  Served-number moves: _TBD_. Cache: _TBD_.
- **EHCVM-2 2021-22 wiring: Togo, Benin, Côte d'Ivoire** (#1697). PR: _TBD_. Moves: _TBD_.
  Cache: _TBD_.
- **Nigeria post-planting `community_prices`.** PR: _TBD_. Moves: _TBD_. Cache: _TBD_.
- **kg-factor evidence:** #1509 (purchase rows only in the price-ratio inference) and
  #1068/#1129 ("Drop and count" in the native-unit modes). PR: _TBD_. Moves: _TBD_.
  Cache: _TBD_ (a `transformations.py` edit moves GhanaLSS through `nutrition`).
- **Sold kilograms:** #1560 and #1571 (Niger and Mali). PR: _TBD_. Moves: _TBD_. Cache: _TBD_.
- **Served-wave corrections:** #1707 (Mali 2018-19 constructed rice rows), #1708 (Niger 2018-19
  computed last-purchase quantity), #1536 (Uganda 2009-10 `ea` fill, registered) and #1519
  (Senegal near-zero quantities). PR: _TBD_. Moves: _TBD_. Cache: _TBD_.
