# Prior-Art Ledger — #1577 Mali current served strata

**Source:** `c8e378920968fde414767855186e64f5e36f54bb`.
**Search tier used:** ripgrep + git floor; no GitNexus tools are available.
Inherits [STANDING.md](STANDING.md). Country CONTENTS was read first, including
its dated qualifications; no earlier country-note prefix will be rewritten.

## §1 Task, restated

Trace the four current Mali `sample` YAML producers through their formatters,
derivation and read-time categorical mappings to explain what `strata` serves.
Supply an external four-wave source census for the held #1577 metadata design,
plus country-owned documentation of new source facts. This is source analysis,
not a current-data measurement or a second #1574 document/header investigation.

## §2 Existing machinery (this task's area)

| symbol / source | path:line | role / existing verification | reuse / extend / new |
|---|---|---|---|
| Four `sample` producers | `lsms_library/countries/Mali/{2014-15,2017-18}/_/data_info.yml:4`, `2018-19/_/data_info.yml:88`, `2021-22/_/data_info.yml:81` | exact current declarations; inspect text only | reuse |
| `strata` | `lsms_library/countries/Mali/2017-18/_/mapping.py:22` | `format_id` on native `strate`; retain code semantics | reuse |
| `derive_region_milieu_stratum`, `strata_label`, `stamp_region_milieu_stratum` | `lsms_library/ehcvm.py:821`, `:847`, `:852` | existing label construction and provenance stamp; `tests/test_gh812_ehcvm_strata.py` (not run here) | reuse |
| `sample` hooks | `lsms_library/countries/Mali/2018-19/_/mapping.py:48`, `2021-22/_/mapping.py:41` | stamp existing derivation | reuse |
| `strata` and `Region` tables | `lsms_library/countries/Mali/_/categorical_mapping.org` | existing spelling curation; #1576 ledger/tests are authority to inspect | reuse |
| Existing country evidence | `Mali/_/CONTENTS.org:2610`, `:3127`, `:3483`, `:3637` | #812 construction, #1576 spelling, closed #1574 raster/header findings | reuse |
| Held design question | supplied `issues/1577.json` | OPEN Tier C; placement/vocabulary not decided | reuse |

## §3 Definitions & conventions in force

- Canonical interface: STANDING §3 and `lsms_library/data_info.yml`; country
  declaration `Mali/_/data_scheme.yml:4-10` requires `strata: str`.
- `docs/guide/feature.md`, “strata Is Not One Vocabulary”: a code, a label,
  a construction and nothing already coexist; this task adopts no new enum.
- Country idiosyncrasy: EAC-I 2014/2017 differ from EHCVM 2018/2021. The
  2017 rural Lourd/Leger split is documented in `CONTENTS.org:206-236`.
- `CONTENTS.org:3641-3659`: the completed BID raster/header review did not
  identify the 51-code crosswalk; `strate` has literal label `codestrate`,
  no value-label-set reference and no value-label dictionaries. Reuse only.

## §4 Invariants & assumptions

- STANDING §4 and CLAUDE apply, narrowed by this task's explicit source-only
  grant: no LSMS import/runtime, native/header/row/PDF reads, tests, census
  execution, network/provider work or cache/config writes.
- Do not infer EAC-I2017 names/geography from EHCVM or an occupied-code pattern.
- Historical measurements remain attributed historical evidence, never fresh
  observed row counts. Absence from a producer is not whole-survey absence.
- Do not edit sampling.yml, weights, defaults, schema, mappings or producers.
  External JSON is an evidence attachment, not a proposed library schema.

## §5 Reuse decision

| requested output | decision | reason |
|---|---|---|
| Four-wave meaning of `sample.strata` | reuse source declarations and mapping chain | no transform or measurement needed |
| EAC-I2017 unknown semantics | reuse closed #1574 findings | no repeat acquisition or inspection permitted |
| Placement recommendation | reuse #1577's existing alternatives | hold vocabulary and hash-cost measurement for root |
| External evidence + country prose | new documentary attachment | missing exact current source trace; no loader consumes it |

## §6 Open questions for the human

No answer is needed for this bounded task. Root retains #1577 placement,
vocabulary and hash-cost choices. Own-release authority binding EAC-I2017
`strate` codes to all 51 design strata remains the missing operand (#1574).
For 2014, an own-wave authoritative stratum field or assignment rule must bind
the configured household/cluster keys to the 51-stratum design before wiring.
Neither a deflator nor a 2017 `grappe` back-cast supplies that authority.

### Phase 3 — verification

- **OK (§§2–5):** all four current producers are YAML. Their exact cover/weight
  merges are on `i`, `i`, `v`, `i`, respectively. 2014 declares no strata
  extraction; the string type declaration does not synthesize one. Country
  concat supplies missing cells in the pooled column (`country.py:6031-6061`).
- **OK (§§2–4):** 2017 `strata()` calls `format_id`, not a code-name lookup.
  The six country spelling keys match only region-milieu labels. The reused
  closed #1574 reports and current sidecar agree on MD5/size; neither the
  header result nor historical occupied-code/geographic patterns name codes.
- **OK (§§2–4):** both EHCVM waves use their own `s00q01`/`s00q04` labels via
  `strata_label`; the hook stamps only nonmissing strata. Numeric operands
  raise and missing/blank operands give NA. Country read-time curation uses
  the #1576 accent preference. The separate Rural mapping does not change
  `Urbain` in the stratum label. No native named design-stratum field is
  established; #812's registered BID rationale supports this construction.
- **OK (§§4–5):** external `Mali-served-strata.json` contains exactly four
  source-qualified records and explicit gaps; it proposes no library schema.
  Recommended future location is per-wave `_/sampling.yml` outside `panel`,
  after the held field/vocabulary decision. No hash-cost measurement is made.
- **OK (§4):** `REVIEW.json` confirms the entire original 3,897-line country
  prefix, 33 referenced source/sidecar files unchanged, and only this ledger
  plus the additive country note differing from c8. `git diff --check` passes.
  No code symbol changes, reinvention or definition contradiction found.
- **Operator failure preserved:** initial stdlib-only review helper failed
  before checks on Python 3.6's unsupported `capture_output` argument; the
  enclosing shell still published the country note. External v1/error receipt
  remain. V2 uses `PIPE`/`universal_newlines`, passes the same documentary
  checks and changes no scientific assertion or project runtime.
- **Not executed (§4):** LSMS import/runtime, tests/JUnit, raw/header/row,
  PDF/image, Country/Feature, provider/network and cache work. This source-only
  task does not claim current API/cache freshness or close #1574/#1577.
