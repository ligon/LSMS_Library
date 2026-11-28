# #1518 — Nigeria W2 release-table harvest kilograms

## §1 Authorized task

On receiving base `93d74b06101cb5e428434abeb3ff28325d39014c`, implement the
2026-11-27 maintainer ruling “Serve the labeled W2 estimate” on retained
2013Q1 A3 harvest lines only. The November 25 held correspondence and the
accepted Nigeria plan are evidence and scope, not current API row counts.
The full public #1518 body and two comments match the sealed snapshot. Open-PR
searches for Nigeria, 1518, harvest and the lookup filename found no competing
W2 work. The #1622 and #1752 W3 rulings remain in force.

## §2 Existing machinery

Search tier: Git, ripgrep, source and AST. Follow [STANDING.md](STANDING.md).
Paths below are under Nigeria's `_/` unless qualified.

| Anchor at base | Reuse and existing tests |
| --- | --- |
| `crop_production.py:155–180`, `nigeria.py:1044–1347` | Country concatenator, logical PH quarter, `_crop_line_observations`, pure producer and PP attachment. `test_gh1433_nigeria_crop_lines.py`, `test_gh844_nigeria_harvest_value.py`. |
| `nigeria.py:4051`, `:4104–4192` | Existing release-factor multiplication and W3 crop/size/zone lookup + inverse inputs. Reuse math, not W3 lookup format, holds, geography or density. `test_gh1502_nigeria_food_conversion.py` and `test_nigeria_safe_harvest.py`. |
| `derivations.yml:1354+`, `Country.derivation_inputs` | Registry, callable and raw-key witness; API walks panel IDs exactly once. `test_derivations.py`. |
| `transformations.harvest_kg_factors`, `_sold_kg_factors`, `median_price_valuation` | Direct harvest total enters `survey_kg`. Sold denominators are a different path. `test_nigeria_sold_unit.py`, `test_nigeria_kg_factor.py`. |
| `local_tools.get_dataframe`, `Country._make_declared_inputs`, `Country._table_cache_hash` | Sanctioned source reads and existing fingerprint mechanisms. Test the actual metadata closure; distinguish ordinary L2 hash from managed-Make manifest. |

## §3 Definitions and country facts

Cite the [standing baseline](STANDING.md), `CLAUDE.md` and canonical
`lsms_library/data_info.yml` (`crop_production.Quantity_kg`, `KgFactor`,
`Unit_sold`). Read the Nigeria country record including the WAITING nonfood
recall entry. 2012Q3 planting and 2013Q1 harvest share a folder but are
different logical periods. The PP intercrop join uses native household/plot/
crop and does not identify a conversion. Some PH plots are independently
sourced despite the older “recorded only in the POST-PLANTING round” text.
`cropid` is a line ID, not `cropcode`, and public crop and unit labels can
collapse native distinctions. A3 sale Q11/Q12 and valuation Q18 are not Q6.

“W1--W3 are all-NaN because the column does not exist in the source” refers
to reported `KgFactor`, not shipped-table totals. “A factor belongs to the
quantity block it was asked beside.” The food note “a card weight fits a
fraction of a container and not a count of them” does not govern W2 A3.
Public W2 F99/V2935 codes Q3 Yes as 1; F99/V2941 card codes 1–4 are named
standard mass or volume. F118/V3303 labels only 1 as kilograms. The own BID
§7.27 illustration is Q8, gives inconsistent arithmetic and cannot verify
respondent Q6 mode or mass calibration.

## §4 Invariants

Use the exact A3 raw cropcode × sa3q6a2 match with a unique finite positive
release rate, explicitly marked kg=1, and finite positive Q6/product. No
public-label, cropid, food, zone, wave, density or fraction inference. An
absent/No Q3, missing/unknown/standard unit, unclassified basis or unmatched
pair never receives this assignment. Malformed lookup and duplicates must
stop, never produce a successful partial build. Keep the old footprint, sales,
factors, PP attach, labels and all other waves. Read-only source declarations
and invented runtime evidence only. The existing harvest rung name is not
the derivation identity.

## §5 Reuse decision

Reuse the country concatenator, pure producer, line-key machinery, shared
release-factor multiplication and public registry. Add W2-local validation,
native lookup and a registered witness. W3's region/card-conflict policies and
the food conversions' input formats and assumptions do not fit this CSV.

## §6 Review boundary

The W2 held correspondence must be run independently under its separate grant.
W1 output basis, code-0 basis, unmatched pairs and Q6 quantity convention remain
open. No change to the global schema or the plot-area denominator policy.

## Phase 3 — anchored verification

- `_crop_line_keys` and `crop_production_w2` — **OK (§§2–4)**: share
  exactly the old crop occurrence key, keep the existing W2 script-frame
  wiring and attach only a qualified harvest total and its key. An offline
  run of the separate exact-base and candidate scripts/helpers emitted the
  same 30 invented lines and types, with only four estimates and four
  derivation cells different. W1 and W3–W5, including a W3 Farm-level sale,
  compare exactly. Invented PP flags, reported values and sales are preserved.
- `_w2_crop_conversion_table`, `_w2_crop_release_trace`,
  `derive_w2_crop_release_kg` — **OK (§§2–5)**: own Q3/Q6 and unit-card
  gates, exact native-pair key, unique positive finite kg-marked factor and
  safe positive product. The release-rate multiplication is reused, not
  reimplemented. Duplicate/invalid/unavailable lookup tests fail the actual
  script. No label pooling, substitution, density or quantity-mode screen.
- `inputs_w2_crop_release_kg` and registry — **OK (§§2–4)**: invented
  public Country inverse inputs use the actual registry and shared ID walk.
  Reproduction equals all four expected totals. The existing `survey_kg`
  harvest rung is separate from the W2 derivation key. The same fixture
  exercises sold-factor admission and median-price valuation with both
  volume options. A W2 harvest estimate on a native sack sale does not
  create a supported sold denominator, and reported kg sales still resolve.
- Literal CSV declaration — **OK (§§2,4)**: actual managed-Make input
  manifest contains the declared sidecar fingerprint. The rollout producer
  bytes move the ordinary L2 hash. Country-script-only sidecar drift alone
  moves the managed-Make manifest, not the ordinary L2 Country hash. This is
  the inherited #1288 residual, not a new wave producer or cache rule.

The targeted suite of 52 source-free tests passed in the admitted pinned
offline runtime on `d28994afb061b619ac48efffe8554741f981c6ad`. This ledger
commit leaves all runtime bytes identical to that head. No held files,
provider, shared cache, full suite, real-data Country/Feature/Make/land
campaign, CI, or merge was run. The coordinator's independent finite raw
projection reproduces the historical match counts and reports that the
largest raw estimate contributes about 15% of the eligible raw mass. This
is not a served total, calibration, or a new exclusion.
