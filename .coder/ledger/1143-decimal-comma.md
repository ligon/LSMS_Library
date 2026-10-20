# Prior-Art Ledger - GH #1143 decimal commas in explicit metric labels

**Search tier used:** ripgrep + git (floor), GitHub issue body/comments; base
`976fc840d12ba40f1602b7dff58b81548632b2a9`. Inherits `STANDING.md`.

Existing unpublished prior art discovered when the ledger commit's publish
collided with `origin/fix/1143-decimal-comma`: commits `0020cfdac` (parser),
`15d2d567f` (tests), `7393a3074` (country notes), based on `238a4c15d`.
Parent preserved that branch, renamed this branch with the job suffix, and
cherry-picked only the parser/tests as `3e49c4bea` / `a91ae9107`, preserving
their attribution. Extend this existing implementation rather than duplicate
it; historical country notes are evidence to verify, not current measurements.

## §1 Task, restated

Correct `_parse_explicit_metric` so the metric rung reads an entire numeric
token, including EHCVM decimal commas, instead of its last digits. Reuse the
same parser for food and crop consumers. Measure Senegal, Benin and CotedIvoire
food quantities/prices on separate private cold caches, and measure actual
cache-fingerprint reach before landing.

## §2 Existing machinery

| symbol | path:line | behavior / test | decision |
|---|---|---|---|
| `_EXPLICIT_METRIC_PATTERNS` | `lsms_library/transformations.py:1601` | mass/volume patterns and scales; `tests/test_metric_fraction_parse.py`, `tests/test_volume_as_mass_kwarg.py` | extend number token and left boundary |
| `_metric_number` | `lsms_library/transformations.py:1625` | decimal point / fraction, zero-denominator error; `tests/test_metric_fraction_parse.py:73` | extend comma interpretation, reuse division |
| `_METRIC_NUMBER`, `_metric_scalar`, comma grammars | prior `0020cfdac`, now `3e49c4bea` | shared comma implementation; `tests/test_metric_decimal_comma.py` from `15d2d567f` | reuse and correct leading-zero/malformed-token gaps |
| `_parse_explicit_metric` | `lsms_library/transformations.py:1645` | first pattern's number times scale; catches invalid/zero denominator; doctests and tests above | reuse |
| `_seeded_kg_factors` | `lsms_library/transformations.py:1714` | declarations/known units outrank parser; positive finite factors; `tests/test_gh919_country_qualified_units.py` | reuse |
| `food_kg_factors` | `lsms_library/transformations.py:1959` | food factor ladder; `tests/test_food_kg_inference.py` | reuse |
| `food_quantities_from_acquired` / `food_prices_from_acquired` | `lsms_library/transformations.py:3015,3134` | runtime food API derivations | reuse for measurement |
| `_kg_factor_series` / `harvest_kg_factors` | `lsms_library/transformations.py:3538,4191` | crop ladder shares parser; `tests/test_crop_kg_factor.py` | reuse; regression check |
| `Country._table_cache_hash` / `_build_registry` | per `STANDING.md §2` | build fingerprint; `tests/test_build_transform_hash.py:440` only covers tagged build closure, not country imports | reuse actual country hashes; no claim of zero impact from tagged closure alone |

## §3 Definitions & conventions in force

- Authority is [GH #1143](https://github.com/ligon/LSMS_Library/issues/1143):
  "`\d+,\d{1,2}\b` is a decimal"; "`\d{1,3}(,\d{3})+` is a thousands
  separator, unless it starts with `0,`." No issue comments exist at grounding.
  Thus `1,500 kg` means 1500 kg; `0,125 kg` means 0.125 kg.
- Contrary to the initial dispatcher brief, current `_metric_number` has **no
  comma convention**: it calls `float()` directly. Parent acknowledged the
  issue's explicit rule as authority on 2026-10-20. Extend rather than claim an
  existing comma implementation.
- Existing SI scales and `volume_as_mass=False` remain authoritative in
  `_EXPLICIT_METRIC_PATTERNS`; this does not introduce density estimates.
- Existing range endpoint behavior is pinned at
  `tests/test_food_kg_inference.py:471`: `Gourd (1-5lts)` means parser endpoint
  5, not midpoint. Preserve it; no new range policy.
- Runtime derivations and sanctioned IO follow `STANDING.md §3/§4` and
  `CLAUDE.md`; no derived-table registration or new factor ladder.

## §4 Invariants & assumptions

- Never read a suffix of a malformed numeric token as a valid quantity.
  Preserve fraction syntax, decimal points, SI scales, and unit word
  boundaries (`20gallon` declines); `tests/test_metric_fraction_parse.py` and
  `tests/test_volume_as_mass_kwarg.py`.
- No mixed-separator or malformed-grouping convention is established by the
  issue; decline these rather than silently extracting trailing digits.
- Preserve numbered vocabulary prefixes (`1001. 12,5 cl`) and dual fraction
  labels (`Packet(1/4lt / 1/4kg)`); the recovered tests pin them. Refuse malformed
  scientific/whitespace suffixes while preserving existing inner container
  sizes in multiplier labels (`2x500g`), per the extended parser tests.
- Country documentation read first, including TODO/WAITING/LOGBOOK entries:
  Senegal's food crosswalk intentionally keys on mojibake strings; changing
  its keys without its reader loses rows (`Senegal/_/CONTENTS.org`, unit
  handling). Benin's removed `2018-2019` folder was an md5-identical staging
  duplicate, not a second wave (`Benin/_/CONTENTS.org`, Overview).
  CotedIvoire's historical CILSS food is not wired; the 2018-19 EHCVM source
  names use uppercase CIV (`CotedIvoire/_/CONTENTS.org:22,140`). GhanaLSS
  `nutrition.py` imports the food derivation, so parser changes can invalidate
  its country caches despite no tagged build closure change (`CLAUDE.md`,
  GH #1042 cache-impact note). Do not modify these country configs.
- Use separate private L2 roots before/after, with shared L1 links only;
  never clear or modify shared L2. Record aggregates, not household records.
- `_metric_number` zero denominator still raises and parser still declines.
  Declared factor precedence and positive-finite seeding remain unchanged.

## §5 Reuse decision

| quantity | decision | reason |
|---|---|---|
| Numeric token recognition | extend existing patterns | entire comma/fraction token must reach existing parser, never a suffix |
| Number interpretation | extend `_metric_number` | shared implementation; GH #1143 provides missing comma rules |
| Factor scales, food/crop ladders | reuse | existing tested machinery already computes required conversions |
| Food API evidence and hashes | reuse existing methods | compare actual outputs and hashes without reimplementing definitions |
| Regression cases | extend existing fraction parser tests | same suffix-matching failure; adds decimal/grouping/malformed coverage |

## §6 Open questions for the human

None requiring another user decision. Parent has confirmed the issue's comma
rule and preserving existing range endpoints. Parent surfaced and approved
this ledger and the prior-branch recovery on 2026-10-20. Mixed-separator/malformed strings decline;
changing broader multi-quantity/range semantics is out of scope.

### Phase 3 - verification

- `_METRIC_NUMBER` / `_metric_scalar` / `_metric_number` / parser prefix guard:
  **OK (anchored on §2/§3/§4)**. Extend recovered, tested machinery; comma
  grouping follows the issue; zero-prefix precision follows parent ruling;
  malformed tokens decline without breaking pinned range/fraction cases.
- Food/crop consumers: **OK (anchored on §2/§5)**. Reuse existing factor ladders
  and API derivations; 359 focused tests passed. No duplicate conversion path.
- Live evidence: **OK (anchored on §4)**. Private exact-base/current cold builds
  affect 783 Senegal + 126 Benin + 36 CotedIvoire quantity rows (945 total).
  All source frames and native quantities are unchanged; 692/68/23 default
  price rows change. See `slurm_logs/framework_campaign_2026-10-20/1143/`.
- Cache reach: **OK (anchored on §2/§4)**. Actual hashes move for 22 country
  tables and 154 wave cells, all GhanaLSS; zero tagged build fingerprints move.
  Do not describe this as a zero-cost read-time change.
- Country docs: **OK (anchored on §4)**. Record Senegal's 624 latent Demi
  walatt rows and the observable stale CotedIvoire no-u-table statement with
  evidence; no country mapping changes.
