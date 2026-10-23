# Prior-Art Ledger — GH #1059 Burkina Faso `cluster_features.District` spellings

**Search tier used:** ripgrep+git floor (the `gitnexus` MCP server failed to
connect this session; substitutes per CLAUDE.md "Code intelligence").

## §1 Task, restated
Burkina Faso `cluster_features` (index `(t, v)`, YAML path in all three waves:
2014 EMC `province`, 2018-19 / 2021-22 EHCVM `s00q02`) serves `District` as
the raw source string, so the pooled column carries 96 strings for 45
provinces (45 / 45 / 44 per wave).  Add a country-level `#+name: District`
table to `Burkina_Faso/_/categorical_mapping.org` so the read-path
auto-apply-by-name maps every spelling onto one canonical label per
province -- exactly as PR #971 (#811) did for `Region`.  Do NOT map one
province onto another: three shared 2018-19/2021-22 grappes name different
provinces, which is a cross-wave identity question to report, not fix.

## §2 Existing machinery (this task's area)

| symbol | path:line | what it does | tested? | reuse / extend / new |
|--------|-----------|--------------|---------|----------------------|
| `Country._apply_categorical_mappings` | `lsms_library/country.py:3502` | auto-applies a country table to any column / index level of the same name (case-insensitive), `replace` so unmatched values pass through | yes | reuse |
| `_categorical_key_column` | `lsms_library/country.py:485` | key = `#+key:` else first non-reserved column | yes | reuse (positional; no `#+key:`, as the Region table) |
| `Country._market_lookup` | `lsms_library/country.py:3025` | `market='District'` lookup; applies the same table by name | yes | reuse (covered by the same table) |
| `Country._valuation_geo` / `median_price_valuation` | `country.py:3222`, `transformations.py:7274` | District as a price-ladder rung, cells keyed with `_t` | yes | unaffected: per-wave 1:1 relabel |
| BF `#+name: Region` table | `Burkina_Faso/_/categorical_mapping.org:411-471` (PR #971, commit `b396bcb52`) | the precedent: `Alternate Spelling` / `Preferred Label`, identity rows, prose recording the decision | yes (no BF test; GB test in #971) | mirror |
| `tests/test_niger_region_labels.py` | | config-only + data-gated pattern for a spelling table | yes | mirror |

## §3 Definitions & conventions in force
- Automatic categorical mapping by column name, whole-table override at the
  country level: `CLAUDE.md` "Automatic categorical mappings" and
  "Categorical mappings resolve wave → country → global".  No global
  `District` table exists (`lsms_library/categorical_mapping/`), and
  `District` is not in `_ADDITIVE_CATEGORICAL_TABLES`.
- Canonical spelling for BF Region (#971): "the Preferred Label is the
  2021-22 spelling -- the fully hyphenated official form ... with =Mouhoum=
  corrected to =Mouhoun=" (`Burkina_Faso/_/categorical_mapping.org:432-434`;
  `CONTENTS.org`, Region entry; PR #971 body "Editorial spellings ... BF
  corrects the source typos").
- `CONTENTS.org` "THE THING TO KNOW": 2021-22 is a panel sub-sample within the
  2018-19 EAs (550 of 585 grappes).

## §4 Invariants & assumptions
- Per-wave distinct District is unchanged (45/45/44): the mapping is a
  bijection within each wave, so neither the `(t, v)` collapse (GH #1060, 2014)
  nor any per-wave grouping (valuation ladder cells include `_t`) can move.
- `categorical_mapping.org` is a build input (`Wave._input_hash`,
  `Country._table_cache_hash`): every Burkina_Faso hash moves, no other.
- The table keys are the served strings verbatim (generated from the measured
  values, so Unicode normalisation matches).

## §5 Reuse decision

| quantity | decision | reason |
|----------|----------|--------|
| District canonical labels | reuse (auto-apply by name) | identical mechanism to #971 Region |
| canonical spelling | extend #971's rule | 2021-22 is not uniformly official for District (drops Balé/Bazèga/Séno accents), so: official accented title-case form *as attested in the data*; all 45 labels occur verbatim in an EHCVM wave |

## §6 Open questions for the human
- `Komandjoari` (all three waves) vs the official `Komondjari` -- RESOLVED
  (@ligon, 2026-10-23): serve the official `Komondjari`; the data's spelling
  is an Alternate Spelling row.
- The three province disagreements (below): which wave's province is right?
  Bears on any 2018-19 → 2021-22 GPS carry-forward (GH #1050).

| grappe | Region | 2018-19 District (households) | 2021-22 District (households) | 2021-22 hh tracked from this grappe | 2018-19 GPS fix |
|---|---|---|---|---|---|
| 19 | Sahel | Séno (12/12) | Yagha (6/6) | 6/6 | yes |
| 365 | Sud-Ouest | Poni (12/12) | Noumbiel (6/6) | 6/6 | yes |
| 549 | Est | Kompienga (12/12) | Gourma (6/6) | 6/6 | no |

Evidence: `slurm_logs/isa_backlog_2026-10-21/1059-burkina-district/disagreements.json`.

---
### Phase 3 — verification

- `#+name: District` table — OK (anchored on §2 Region precedent, §3): 96
  rows = every served spelling, 45 Preferred Labels, identity rows, no
  `#+key:`, `Alternate Spelling` header as the Region table.
- Province-onto-province mapping — none (§1): `Yagha`/`Noumbiel`/`Gourma`
  self-map; the shared-grappe residue is exactly the three pairs (measured).
- `tests/test_burkina_faso_district_labels.py` — OK (§2 Niger pattern).
- Framework code — not touched.
- Measured (fix commit `595340db5` vs development `9a0ed5f3c`, cold builds;
  `slurm_logs/isa_backlog_2026-10-21/1059-burkina-district/REPRODUCE.md`):
  District 45/45/44 per wave before and after, pooled 96 -> 45; only
  `District` changes (954 of 2,035 rows); `sample` and
  `food_expenditures(basis='total', valuation='median_price')` frame-equal;
  shared-grappe residue 53 -> 3; 2014 grain report identical.  Hashes:
  Burkina_Faso only (24/549 country, 72/2,109 wave, 0/40 btf).
- Seen, out of scope, not investigated: 1,088 of the 3,132 `panel_ids.json`
  2021-22 -> 2018-19 targets are not a served 2018-19 `sample()` `i` on the
  base build (e.g. target `19126`, served `190126`).
