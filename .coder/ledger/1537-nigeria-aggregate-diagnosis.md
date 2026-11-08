# Prior-Art Ledger — #1537 Nigeria Aggregate passthrough diagnosis

**Search tier:** ripgrep + git; no GitNexus tools exposed. Initial source
`b8346cf1f953ac82517b1b7473c715a5655ca950`, private branch
`dispatch/0e611a6fd55d4f688c2a956c80239370`. This is a source-free diagnosis
assignment. ROOT owns scientific decisions, independent review, data stages,
publication and merge. No production change is authorized by this ledger.

## §1 Task, restated

Trace the three native food names in the supplied open/zero-comment #1537
snapshot through Nigeria's committed quarter decoder, own curation and
`Country._relabel_j` after the food-price transform. Preserve default native
names and price values. Deliver a route/curation matrix, adversarial invented
controls and an exact minimal proposal for ROOT review. The issue's 121 labels,
11,649 rows and 2.4% describe its historical observation, not a new measurement.

## §2 Existing machinery

| Machinery | Anchor at initial source | Existing controls | Decision |
|---|---|---|---|
| Food-only decoder | `Nigeria/_/nigeria.py:641,680,723` | `tests/test_nigeria_food_item_decode.py` | reuse; quarter names and protected native names unchanged |
| Own label curation | `Nigeria/_/categorical_mapping.org:38` | decoder tests | reuse exact existing destinations only; no invented code |
| Canonical producer | `Nigeria/_/nigeria.py:3759,3805` | `test_nigeria_food_acquired_sources.py` | read only; no source/build invocation |
| Runtime price route | `country.py:6674,6851,6993`; `transformations.py:3592,3772` | food-price transform tests | reuse; price derived before relabel, never summed |
| Scalar relabel | `country.py:3898,3966,4079` | `test_food_labels.py`, `test_label_selection.py` | invoke unbound on an invented metadata provider only |
| Pass-through report | `country.py:8118` | existing label tests | reuse unchanged warnings, strict refusal and attrs |
| Contract B | `errors.py:9`; `feature.py:1168,1185` | Feature tests | preserve missing-curation degrade versus defect warning |
| Earlier decision | `.coder/ledger/1144-nigeria-food-item-rounds.md` §§1,4,6 | historical source-grounded evidence, not replayed | preserve entire ledger and recorded-specificity policy |

## §3 Definitions & conventions in force

Inherit `STANDING.md` §§3–4 and `CLAUDE.md` Derived Tables. Read the full
4057-line Nigeria `_/CONTENTS.org`, including its live WAITING recall caveat
at line 3505. `AGENTS.md` and `CLAUDE.md` are byte-identical at the source pin.
The parent food-acquired skill owns bucket design; its aggregate-labels
sub-skill owns Contract B. Cross-country skill/audit README were read for the
assembly/error contract; this assignment runs no Feature audit.

The load-bearing country quotation is `CONTENTS.org:18–23`:
"Do not add an attribute the source did not record" and "These three native
names are not new Aggregate buckets: an alternate-label request leaves an
uncurated name intact and reports it through the ordinary label-coverage
diagnostic." This is an intentional limit, not assumed stale documentation.
The current issue reports precisely that passthrough. A correction must reconcile
the two, not quietly reinstate the historical specific aliases.

## §4 Invariants & diagnosis matrix

| Native served j | Committed quarter/code identity | Own existing Aggregate route | Ambiguity / collision |
|---|---|---|---|
| `Groundnuts` | code 43, 2010Q3/2011Q1/2012Q3 | same row's Preferred `Groundnuts (unshelled)` → `Groundnuts`; code 44/shelled and crop 1060–1062 also → `Groundnuts` | shell state unknown, but all these own targets agree; incoming native spelling already equals target; diagnostic still fires because it is absent as a Preferred key |
| `Maize (shelled)` | code 21, 2012Q3/2013Q1 | same row's Preferred `Maize (shelled, on the cob)` → `Maize`; codes 12/20/22 and crop 1080 also → `Maize` | cob state unknown, but own target is independent of that state; rename can collide at full price grain with other maize j values |
| `Pepper` | code 76, 2010Q3/2011Q1/2012Q3 | same row's Preferred `Fresh Pepper` → `Fresh Pepper`; crop 2140 native `Pepper` also → `Fresh Pepper` | this follows a row whose specificity #1144 deliberately declined; separate own targets `Dry Pepper` (77/3030) and `Ground Pepper` (144) exist; no generic `Pepper` Aggregate bucket; choosing any adds an unsupported state |

Nigeria's `harmonize_food` has **no `Original Label` column**: its eight
quarter columns carry native names. Scalar `labels='Original'` therefore
cannot recover those names; `_relabel_j` keys only on `Preferred Label` and
does not read the quarter/code route. The separate legacy `aggregate_items.json`
is consumed by old `food_quantities.py`/`unitvalues.py`, not by `_relabel_j`.
The current guide already records #1537's dated passthrough observation.

Keep default/Preferred calls identical; preserve all payloads, dtypes, nulls,
index levels and untouched inputs. No producer/weights/quantities/units/kg
inference change. Do not deduplicate or sum Price after an Aggregate rename:
collisions remain visible as duplicate full keys. Unknown j values still pass
and warn; missing curation raises `LabelUnavailableError`; malformed/no-j
cases remain plain `KeyError`. No fingerprint exclusions/schema changes.

## §5 Reuse decision and minimal proposal

**Reuse** the existing scalar relabel mechanism. A review-only curation option
is two **label-only rows** in Nigeria's own `harmonize_food`:

| Code | Preferred Label | Aggregate Label | all quarter cells |
|---|---|---|---|
| blank | Groundnuts | Groundnuts | blank |
| blank | Maize (shelled) | Maize | blank |

This would certify the already identical Groundnuts destination and supply
Maize's missing Preferred key without asserting shell/cob state. Blank codes
are skipped by both numeric decoders. All existing rows remain unchanged.
This is an option, **not applied**; it changes curation and has whole-Nigeria
hash implications because the `.org` is a build input. A shared quarter-alias
fallback would widen behavior for every requested column/country and is rejected
as unnecessary scope. Pepper remains warned/preserved until ROOT rules.

New work is limited to source-free diagnostic controls and review evidence:
they establish mechanism and boundaries, not fielded population or science.
No real `Country`/`Feature` constructor or generated table method may run.
Use the prepared image, exact cwd/package/config realpaths, private roots,
false credential gate, source/provider/constructor/write guards and `unshare -rn`;
thread/build limits 1, pytest at most n2/loadfile, logs to files.

## §6 Open questions for ROOT

1. Does ROOT authorize the two label-only rows as a separate partial curation
   correction, or prefer a diagnosis-only packet? Current grant permits the
   latter and does not settle recuration against #1144.
2. Pepper: retain passthrough pending science, or provide an evidence-backed
   ruling? Existing own curation does not establish fresh/dry/ground identity.
   No generic bucket or fresh/dry alias is proposed here.
3. Full metadata-hash enumeration is a separate gated stage. Freeze code,
   prepare the exact approved external operator/wrapper and ask ROOT; no
   enumeration or real Country constructor occurs on the initial grant.

## Phase 3 — verification

Pending. No production symbol changed. Full source/API/Make/warm-cache checks
and the metadata hash census remain unrun. Record failures without overwriting
them; independent ROOT acceptance remains pending.
