# #1058 Burkina parcel coordinates — private prior-art ledger draft

## 1. Scope and status

Read-only Tier B diagnosis at f8d236694c26fe9e709c5548f02c70f74ea72c33. No tracked ledger or implementation commit is authorized. If approved, commit `.coder/ledger/1058-burkina-parcel-gps.md` before runtime edits, carrying this grounding forward.

## 2. Existing machinery and reuse search

Search tier: git/ripgrep/AST, with a read-only profiling observer for actual hash inputs. No production callable replacement or new framework function.

| Existing mechanism | Anchor | Decision |
|---|---|---|
| Typed Stata source read | local_tools.get_dataframe(convert_categoricals=False); data_access.get_data_file for private metadata inspection | reuse |
| Native parcel mask and key | Burkina helper `plot_features_for_wave`, lines154–254; field/parcel nonnull mask; existing format_id | reuse without eligibility change |
| Wave-specific household key | 2018 script passes ehcvm_i; 2021 script passes _old_i/country_i | reuse exactly |
| Area/GPS-area/estimate, units and clamp | same helper, lines204–227 | preserve, q47 is not a coordinate |
| Tenure/soil/water maps and nullable types | same helper and country categorical_mapping.org | preserve |
| Optional existing coordinate columns | canonical data_info.yml tables.plot_features Latitude/Longitude float | reuse, no schema extension |
| Panel identity mapping/flag | local_tools.id_walk and Country load_from_waves/finalizer id_converted protocol | reuse tested mapper; raw country already walked |
| API-time cluster join | Country._join_v_from_sample | reuse, no v baked into wave data |
| Hashes, coverage and ordinary cache gates | Country._table_cache_hash, Wave._input_hash, local_tools.cache_freshness/read_parquet_cache_waves | reuse, no stamp or bypass |
| Cross-country proof | Feature(plot_features), bench/feature_audit scan phases1/2 | reuse, zero extra row loss |

## 3. Definitions in force

Inherit `.coder/ledger/STANDING.md` sections3/4, CLAUDE.md data access, index/attrs and cache conventions, and canonical `lsms_library/data_info.yml`. Harmonize the interface while retaining source detail. The plot axis remains plot_id. EHCVM vague is a documented sample split in the operating convention, not a new measurement axis.

## 4. Source-specific invariants and contradictions

Full Burkina/Mali/Niger/Senegal CONTENTS were read before source headers, including live tasks/drawers. The blanket “EHCVM s16a has no decimal-degree parcel GPS” conflicts with Burkina's dated correction and current 2021 metadata. q46 carries coordinates; q47 carries area. Native scripts and framework-written country cache are distinct key layers; the existing concatenator's pre-transformation cache description does not describe the default load_from_waves id_walk-before-write route. Preserve the existing mapper and attrs.

Held Burkina source: 8,467 rows, 7,350 native parcels, 1,117 placeholders, 4,180 paired fixes, no partial pairs. Ten GPS-measured parcels lack positions. Existing public all-wave table: 19,784 rows, with precisely seven all-six-payload-missing 2018 rows excluded. No native/post-walk parcel collision or public sample orphan. All old keys/payload/nulls/dtypes/attrs survive the private projection exactly. Other three held 2021 s16a files have no coordinate fields.

## 5. Proposed extension and validation boundary

Extend only the existing Burkina helper with two optional typed colmap fields, bind them only in 2021, declare existing canonical columns optional in the country, and correct the corresponding comments/CONTENTS. Reuse retained source positions without a merge or new key normalization. No area, input, source-grain, eligibility, screening, universe or sample policy change.

Actual baseline549/39 inventories match f8. Actual hash dependency capture places the country helper/scheme in all24 Burkina cells; anticipate24 invalidations,525 other-country cells unchanged, framework0. No candidate hash values are claimed. Twenty-one existing tests and14 private controls pass; one baseline omission assertion fails as expected. Normal cold default Country, held source/helper/wave projection, physical fresh-process warm and Senegal/Feature evidence are in this private task root.

## 6. Remaining work

Await root approval of PROPOSAL.md. Then ledger-first implementation, focused real production regression tests including partial coordinate masks and wrong-key/area mutants, paired fresh source builds, warm/Feature/comparator checks, actual hash scope, safe report and independent review. Current diagnosis neither changes code nor declares production resolved.

Sue.
