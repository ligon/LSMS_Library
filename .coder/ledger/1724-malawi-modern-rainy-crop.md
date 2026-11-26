# Malawi #1724 / #1275: 2024–25 reported rainy harvest and sales

**Search tier:** git and ripgrep. Base is protected-green #1809
`24ccbc1d92dbe5fa4068d0f996c2d316249b5023`, not the IHS5 code-49 draft.

## §1 Task

Serve the two 2024–25 releases' Module G13 reported rainy harvests and
positive Module I household-crop sales. Keep the IHS6 interview intact for
shared refreshment households and keep the distinct retained IHPS interviews.
The user approved rainy G and I only on 2026-11-26. Do not claim coverage of
dry-season or perennial harvest/sales.

## §2 Existing machinery

| Symbol | Authority and test | Decision |
|---|---|---|
| `ihps24_refreshed`, `CS_24` | Malawi `_/malawi.py`, #1604 cover tests | Reuse the existing refresh bridge, then guard both load cohorts against their own covers |
| `_harvest_block`, `assemble_crop_production` | Malawi `_/malawi.py`, #854/#637 tests and #1809 provenance tests | Reuse G reshaping and existing harvest-only assembly; give G no I sales to allocate |
| `_sale_block`, `_farm_level_sales` | Malawi `_/malawi.py`, `test_gh1457_malawi_farm_level_sales.py` | Reuse native sold unit/state/nulls and the structural Farm-level schema, without a ladder decision or its cause tally |
| `scan_script_data_refs`, `Country._make_declared_inputs` | `local_tools.py`, `country.py`, #1288 tests | Reuse literal source bindings and the native private Make contract |
| `crop_waves`, `_/crop_production.py` | Malawi country Make/compiler and #1287 | Add the new target; compiler already iterates declared waves |
| `_CROP_SALE_SOURCES`, `CROP_TEXT_WAVES` | Historical ladder/text replays in `_/malawi.py`, #1809 | Unchanged; modern rows bear neither historical derivation |

## §3 Definitions

See `STANDING.md` §§3–4, `lsms_library/data_info.yml` crop_production grain,
and Malawi `_/data_scheme.yml`. Native coordinates keep `reported:G:<code>`
or `reported:I:<code>`. On a Farm-level row, `u` and `condition` describe
no harvest. `Unit_sold` and `Condition_sold` carry I's own answers. Use the
existing unit/state vocabulary only where it describes the released codes.

## §4 Invariants and evidence

The 815 shared interviews are identified on Module A. Panel `case_id` is
a baseline ID for the 1,948 retained households, so never treat it as their
current key. Retained panel records use `y5_hhid` before framework walking.
The exact G+I+A projection is 36,405 unique G records (32,471 positive,
3,917 zero, 17 blank) and 8,786 positive I records from recorded Yes
responses. Of the positive G13 records, 25 report harvest not complete in
G07. Do not skip them or substitute expected quantity G08. The two G/I
files agree on shared native keys and numeric payload but 34/4 rows differ
in optional unit text. Historical text recovery would change 73/13
positive G/I records, so do not invoke it. No sample, weights, related
module, historical map, season or quantity conversion change.

Evidence receipt: `reconcile-1724-modern-harvest.QBEaXAk2` under the
November 25 ISA campaign, `source-receipt.tsv` and `tracker-note.md`.
Six admitted producer providers, relative to `2024-25/Data`:

| Source | MD5 | Bytes |
|---|---|---:|
| Cross_Sectional/hh_mod_a_filt.dta | 77291c794d64fb001df3266e9d7b417a | 2751262 |
| Panel/hh_mod_a_filt_24.dta | c4c79be4babe53ffc52d4df01723e9f7 | 892953 |
| Cross_Sectional/ag_mod_g.dta | 2bdfbfb250b1a6ef95cf254459cd1bda | 27991541 |
| Panel/ag_mod_g_24.dta | 5939cab283ee5349114fd106622d0b6d | 4938347 |
| Cross_Sectional/ag_mod_i.dta | 609f106b60f89f5966f68c334ca02e33 | 24556356 |
| Panel/ag_mod_i_24.dta | 6b842ff69f0772b68fd8fcb25711b0d9 | 4044306 |

## §5 Reuse decisions

Modern crop labels and ownership are wave-local. G and I retain distinct
native specify codes 48 and 49, and code 50 is directly reported Cowpea
(Khobwe), not a Module D derivative. Positive I sales go directly to the
canonical Farm-level row even where G has exactly one plot. It is not a
failure of the historical plot allocation ladder. Align the two row roles
on the existing country schema and use the existing native builders and
sanctioned IO. No unrelated source or scoped provenance rule is extended.

## §6 Open questions

Dry-season and perennial coverage, text and local-name roles, explicit
season, modern conversion evidence, and the separate 2019–20 code-49 draft
are outside the approved scope. The private full API and transform audit
is separate from the producer's source-free regression tests.

### Phase 3 — verified against the ledger

- Focused private, hard-offline source-free run: 230 passed, 43
  genuine-data cases deselected. Covers G/I read scope and source identity,
  shared versus retained full-interview precedence, zero/missing/incomplete
  G13 controls, distinct crop codes, in-band reported sale unit and
  condition, structural Farm-level sales even with one matching plot,
  and fail-closed invalid inputs.
- All six raw references resolve to their exact sidecars in the maintained
  Make input collector. No C/D/E/O/P/Q or other modern module is declared.
  GNU Make cold dry-run selects the same native recipe for 2024–25, and
  each of the six changed sidecars would independently rebuild the warm
  modern wave target. The same modern-sidecar probe leaves the 2019–20
  wave target up to date.
- Exact #1809 baseline versus product changes 26/26 Malawi country and
  156/156 Malawi wave cache fingerprints. The country Makefile is a tracked
  input to all of those keys. This is cache invalidation, not evidence of
  changed historical output. No shared pipeline code was changed. The
  coordinator verifies the full private held-source output and transforms.
