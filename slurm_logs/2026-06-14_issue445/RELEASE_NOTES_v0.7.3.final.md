# LSMS Library v0.7.3

Released from `development` — **448 commits / 97 merged PRs since v0.7.2**.
This is a large, additive release: a new analyst-facing item-feature stack, a
new survey family (EthiopiaRHS), `plot_features` across LSMS-ISA, and broad
feature-coverage sweeps spanning ~30 countries. No breaking removals.

## Highlights

### WB-parity item-feature stack (new)
A new layer of disaggregated, item-level agricultural and welfare features that
brings the library to parity with the World Bank LSMS-ISA harmonized panel,
wired across **7 ISA countries** (Ethiopia, Malawi, Mali, Niger, Nigeria,
Tanzania, Uganda):
- `crop_production` `(t,i,plot,crop)`, `plot_inputs` `(t,i,plot,input)`,
  `plot_labor` `(t,i,plot,source)`, `livestock` `(t,i,animal)`,
  `anthropometry` `(t,i,pid)`, `people_last7days` `(t,i,pid)`, and
  `community_prices` `(t,v,j,u)`.
- **Transforms layer** — analyst-callable aggregates over the item features
  (valuations, indices) callable from the Country API.
- **Unit #0 label foundation** — migrated per-country `food_items.org` into a
  canonical, Code-first `harmonize_food` / `u` in `categorical_mapping.org`,
  fixing silent unit auto-application and unifying food/unit labels x7.
- `plot_features` attribute audit added reported item columns across the stack.

### New survey family: Ethiopian Rural Household Survey (EthiopiaRHS)
- 5 waves wired (#272, closes #271); 1989 income + transitive `panel_ids` (#273);
  bespoke 1989 assets and 1999 (R5) household_roster / livestock / income
  with a `_no_v_join` exemption (#277); 2004/2009 promoted as bespoke `(t,i)`
  aggregate waves.
- `plot_features` config + 1994a (R1) data (#278); design landed (#328).
- Bellemare-etal13 custom food-label scheme as a worked bespoke-`j` example
  (out-of-tree mechanism still tracked in #279).
- `Wave.data_scheme` tolerates an unwired stub wave with no `_/` dir instead of
  raising `FileNotFoundError` (#276, closes #274).

### `plot_features` rolled out across LSMS-ISA and beyond (closes #167)
Uganda (8 waves) plus Nigeria, Tanzania, Ethiopia, Mali, Senegal, Niger, Togo,
Benin, Burkina Faso, Guinea-Bissau, Malawi — and non-ISA: China, Cambodia,
Albania, Kosovo, Liberia, Tajikistan, Timor-Leste, GhanaLSS. Cross-country
`TenureSystem` spellings harmonized.

### Broad feature-coverage sweeps (~30 countries)
- **assets** — Guyana, Albania, Iraq, Kosovo, Liberia, Serbia, SouthAfrica,
  Guatemala, Kazakhstan (#319), with per-`(t,i,j)` instance aggregation; plus
  item-level recovery from DDI labels for India/Kazakhstan (#342).
- **housing** — ~15 countries (Roof/Floor/Walls/Water/Toilet/Tenure/Rooms/…).
- **individual_education** — ~15 countries (#320).
- **interview_date** — Tajikistan, Cambodia, Guyana, Pakistan, India, Kosovo,
  Iraq, Albania, Serbia, Liberia, SouthAfrica (#318); cross-country schema
  harmonization to canonical `Int_t` (closes #325).
- **subjective_well_being / life_satisfaction** — EHCVM §20 ladder (Benin,
  Burkina Faso, CotedIvoire, Mali, Niger, Senegal, Togo, Guinea-Bissau);
  Cantril/Overall/Finances (Iraq, Tajikistan, Timor-Leste, SouthAfrica,
  Kazakhstan, SerbiaMontenegro).
- **food security / coping** — FIES (8 EHCVM + Ghana/Malawi/Ethiopia),
  HFIAS (Tajikistan), `food_coping` + `months_food_inadequate` (Nigeria,
  Ethiopia, Tanzania, Malawi, Mali, Burkina Faso, Uganda, Liberia,
  Timor-Leste, India).
- **shocks** — Albania, Iraq, Liberia, Guatemala (canonical rosters).

### Cross-country label & unit harmonization
- Global `categorical_mapping/u.org` (kg core) with additive global↔country
  merge behind a per-table flag (#223 Layer 2); curated EHCVM `s07bq03b` unit
  codebook decoding Togo + Burkina.
- Exact per-row `Quantity_kg`; Malawi migrated to `Quantity_kg` with native
  unit labels preserved (#378).

### Catalog / API conveniences
- Top-level `ll.countries()` and `ll.features()`.

### Infrastructure
- Savio squashfs single-file venv fast path (`bin/savio_venv.sh`).
- `data_access`: auto-use S3 writer creds on push + unpushed-blob check;
  user-config path for writer creds + write-access docs.

## Framework fixes
- `roster_to_characteristics` applies a mover sentinel for NaN `v` instead of
  dropping mover households (#270, closes #268).
- `_add_market_index` dedups the cluster-fallback `v_lookup` (#267, closes #266).
- `_join_v_from_sample` warns when it would silently drop a wave (#265, closes #256).
- Warn on silent duplicate-index row drops (#323) and name `Feature()` index
  levels (#326); log (not swallow) a missing wave `_/` dir (#329).
- Bool columns coerced via `_coerce_to_boolean`, not `pd.to_numeric` (closes #386).
- Reserved `kg`/`Value` `u`-sentinels protected from country remap (closes #361).
- `interview_date` schema harmonized cross-country to `Int_t` (closes #325).
- Route `food_acquired.j` through `harmonize_food` for Nigeria + Tanzania (#443);
  Mali container-unit `u`-code leak fixed (#444).
- Replace removed-in-pandas-2.1 `groupby(axis=1)` with `T.groupby().T` (#261).
- Uganda 2013-14 AGSEC household ids canonicalized (int32 → H-form) so the
  v-join matches `sample()`.
- Cleared the first full cold-cache gate: Albania `waves` 1996 exclusion + Uganda
  invariance baseline regeneration (#445).

## Notes
- Version is derived from this git tag via poetry-dynamic-versioning.
- No breaking removals; `trust_cache` remains deprecated (removal still slated
  for v0.8.0).
- Known open backlog after this release: parallel-cold test robustness (#330),
  the `Country.waves` data_scheme footgun, data-gap issues (#107–#118, #140),
  and the enhancement backlog (#168, #170, #171, #218, #226, #279, #438, #439).
