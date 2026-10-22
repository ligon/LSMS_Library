# Prior-Art Ledger -- GH849 Uganda reported harvest factors

**Search tier:** git + ripgrep floor. Inherits [STANDING.md](STANDING.md),
CLAUDE.md and the canonical data_info.yml. This ledger precedes production
edits and new analysis. Parent:0854303bc5f60daa698b2927fc3b193822a4c090,
with independently accepted1198 observation runtime6e735/929149222.

## §1 Task, restated

Carry the twelve already diagnosed survey-reported kg-per-unit source fields
through Uganda CROP_COLMAPS at the preserved133761-observation native grain.
Add only currently absent kg_factor keys for A/B2010/2011/2013/2015 and both
actual A/B slots2019; keep the three2018 entries and2009 exact. Correct only
directly superseded unwired comments/docstrings and append measured country
notes. Existing pure builder, source identity, eligibility, units/conditions,
quantities/sales/stand/months, schema, factor dictionaries, screens, ladder,
Country/Feature and cache machinery remain unchanged. The old42.035M scalar
projection is rejected historical evidence, never the expected new total.

## §2 Existing machinery

| Machinery | Source | Existing test / decision |
|---|---|---|
| CROP_COLMAPS / crop_production_for_wave | Uganda/_/uganda.py:1230,1848 | test_uganda_crop_condition.py, test_crop_kg_factor.py; extend only12 mapping cells, reuse builder unchanged |
| _production_observations and source_fingerprint | Uganda/_/uganda.py:1230; local_tools.py:2954 | test_gh1198_uganda_production_observations.py; reuse native IDs/actual slots/release-row identity and source bindings unchanged |
| Builder factor coercion | Uganda/_/uganda.py:1522-1536 | existing pd.to_numeric(errors='coerce') and where(kgf>0,NaN); preserve pending brief clarification (§6) |
| _screen_reported_factors / _valid_factor | transformations.py:3668 | test_crop_kg_factor.py, test_shipped_factors.py; reuse finite-positive, Kg1percent, cap250 and calendar-year rules |
| _survey_median_factors | transformations.py:3861 | existing same-country/wave fine(u,condition) then coarse(u), minimum5, missing-unit excluded on both sides; reuse |
| harvest_kg_factors / harvest_kg / yield_kg | transformations.py:4282,4698,4888 | existing factor/shipped/transform tests; reuse row conversion before explicit analyst reduction and real-area controls |
| Country finalization / Feature alignment | country.py:3815; feature.py | test_feature_canonical_index.py and1198 source-backed proof; reuse typed attrs, ID walk/sample-v and honest unresolved sentinel |
| Sanctioned IO and content-hash gates | STANDING §2/§4 | get_dataframe/to_parquet; no manual stamp, clear, bypass, acquisition or DVC CLI |
| Frozen diagnosis and strict addendum | private Uganda/849 and849-exact-addendum | historical held-source/factor and exact saved-artifact evidence; re-gate copies, never promote unsafe scalar result |
| Accepted parent source/runtime proof | private review-Uganda-1198 and Uganda/1198-docs-final-review | full133761 native oracle, actual both backends/Feature/real yield; independent acceptance does not substitute for fresh current patch measurements |

## §3 Definitions and conventions in force

- KgFactor is the survey's kg per ONE native unit on the SAME observation,
  not a quantity, total, constructed rate or external shipped-table lookup:
  canonical data_info.yml Columns.crop_production.KgFactor and
  harvest-kg-shipped-factors ledger §3/§7.
- Core retains maximal native observations; analyst harvest_kg may aggregate
  only after each row's own Quantity times screened factor. See1198 ledger,
  CLAUDE.md grain contract and SkunkWorks/grain_aggregation_policy.org.
- Current complete Uganda CONTENTS carry is exact blob
  9dbb7afba39b772e2670608dd9040fd626ccab33 (3671 lines). Previous full6e3665
  reading plus the exact seven-line085 paragraph replacement and its separate
  independent documentation proof establish every TODO/WAITING/LOGBOOK entry.
- A/B names modules, not universal calendar order;2015A second2014/B first2015.
  Preserve2013 swapped labels,2018A missing harvest unit/dual conditions and
  B mixed legacy/WB fields, separate sold units/conditions and season rosters.
- The2018B slot2 calendar-label contradiction is already settled using actual
  harvest dates/vocabulary: CONTENTS1414-1434/2315-2324 and loader35-38.
  Independent saved-date counts corroborate it; no new calendar decision.
- Reported production IDs, actual wide condition slots and explicitly
  constructed release-order rows retain their1198 meanings. Preserve403 old
  conditional null-key exclusions and928 eligible null-v observations.
- Prior claims of 'small and in the right direction', old unregistered-feature
  wording and100percent parcel coverage are dated contradictions to quote,
  not invariants. The2015 sales x100 producer-release WAITING is separate.

## §4 Invariants and assumptions

- Exactly12 map additions only after actual held-source metadata/row pairing
  confirmation. No changes to helper AST, source references, identity, all
  other colmap cells, transformations, dictionaries, framework or schema.
- Every133761 key/production_obs, other value/dtype/null mask/order/axis/unused
  level/code and complete recursively typed attrs/_FIELDS remains exact at
  corresponding helper/native/raw/walked/public/Feature stages.
- Raw source values remain private and literal. Existing builder positive
  normalization and conversion plausibility screening are distinct stages;
  never silently claim a zero/negative survives if the existing builder masks
  it. Preserve positive over-cap/year/Kg-not-one carriers; count, never repair.
- Fresh549API/39framework/declared+API-wave metadata inventories precede
  dependent builds; count movement, do not imply every moved table rebuilt.
- Fresh physically private L1/L2, both DVC layouts, MD5/rawSHA/declared size or
  explicit absence, mode/mtime/distinct inode/nlink1/nonsymlink gates before
  decode/API/pickle. Mounted image, exact cwd/import/countries/config/data,
  active socket/write guard + unshare-rn, threads1, max2 loadfile workers.
- Both backends cold and fresh-process/new-object warm, actual matched currency
  arguments, healthy current/full-coverage comparator, subsets, CLI/artifact and
  scanner controls. Endpoints before assertions, unique logs/XML/artifacts.
- Preserve Make subset defect and same17 byte-identical0644-to0600+mtime
  changes as measured limits; do not claim zero writes. Preserve support v
  StringDtype NA-to-NaN difference with strict matched-stage comparisons.
- Preserve all original1198/849/addendum/1078/global/BF/Tanzania and73ad/085
  source/evidence roots. No raw records/IDs/coordinates/full warnings in Git.

## §5 Reuse decision

| Quantity | Decision | Reason |
|---|---|---|
| Reported field carriers | extend12 CROP_COLMAPS keys | verified source fields already supported by the unchanged pure builder |
| Raw-to-native factor pairing | reuse source-row/actual-slot identity |1198 solves the lost carrier across all records, including equal/partial reports |
| Screens, precedence, median pools, harvest/yield | reuse | existing tested methodology; no new estimator, heuristic or scientific exception |
| Source/Feature exact comparator and negative mutations | reuse reviewed strict pattern | explicit zero tolerances, unused MI levels/codes and complete typed attrs |
| Cold/warm/cache/CLI/scanner proof | reuse actual normal routes | current hashes/coverage, private source gates; no shortcut/freshness stamp |

## §6 Approved boundary and outstanding clarification

Root explicitly assigned this bounded writer after accepting1198 runtime and
085 documentary qualification; no new survey or scientific decision is needed.
Before production, confirm held fields again and report substantive surprises.
One concrete brief inconsistency was sent to root: the sentence requesting raw
zero/negative retention conflicts with unchanged builder1522-1536, which masks
nonpositive values. Proposed boundary is to keep that existing normalization,
retain exact raw operands privately and test/report it, while positive suspect
values remain served raw and conversion screens stay unchanged. Production
waits for root clarification; independent preparation can continue.

### Phase3 verification

Pending fresh source gates, exactly12-key AST proof, actual inventory/same-source
cold/warm/Feature/derived/source pairing, focused parent-negative controls,
scanner full occurrence accounting and independent review of the frozen head.
No author-side final PASS or GitHub publication/merge/closure is implied.

Sue (GPT-6).
