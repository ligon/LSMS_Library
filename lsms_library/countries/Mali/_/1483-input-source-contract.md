# Mali EHCVM input sources: GH #1483

Source-only research, 2026-11-09, based on
`4381d701ffb06ab447a37c66362119d1c1e23009`. This records published source
contracts and decisions still needed; it does not declare a new wave loader.

Both EHCVM waves have household input costs in `s16b` and separate parcel
input questions in `s16a`. The three old “no agriculture-input block” claims
in `data_scheme.yml`, `mali.py` and `plot_inputs.py` confuse missing wiring
with missing survey content. Only the two EAC-I waves currently contribute
to `plot_inputs`. The comment corrections change no declaration or callable.
Because these files are hash inputs, even prose corrections are expected to
invalidate Mali builds; that footprint has **not** been measured here.

## Own-round authority

These are the official public variable lists, read separately for each wave:

| Wave / held release in SOURCE.org | Parcel dictionary | Household input dictionary |
|---|---|---|
| 2018-19 / MLI_2018_EHCVM_v02_M | [4295/F30, s16a](https://microdata.worldbank.org/catalog/4295/data-dictionary/F30?file_name=s16a_me_mli2018) | [4295/F31, s16b](https://microdata.worldbank.org/catalog/4295/data-dictionary/F31?file_name=s16b_me_mli2018) |
| 2021-22 / MLI_2021_EHCVM-2_v01_M | [6275/F29, s16a](https://microdata.worldbank.org/catalog/6275/data-dictionary/F29?file_name=s16a_me_mli2021) | [6275/F30, s16b](https://microdata.worldbank.org/catalog/6275/data-dictionary/F30?file_name=s16b_me_mli2021) |

The published lists contain respectively 240/19 and 299/17 variables.
They identify the published release schema; they do not authenticate the
fielded CAPI, native bytes, row eligibility or household/parcel uniqueness.
No native header or row was read for this investigation.

Both `s16b` lists have `grappe`, `menage`, `vague`, input identity `s16bq01`
and use answer `s16bq02`. Neither lists field/parcel IDs `s16aq02/03`.
**2018-19 also lists `s16aq09a/b` (area and its unit), but no field/parcel
ID.** The presence of an area field does not identify a parcel. 2021-22's
list has neither those area fields nor a parcel ID. Joining these rosters
to every household parcel would replicate household amounts.

## Amounts and their own units

The two own-round lists independently identify these roles:

| Answer | Field(s), both waves | Scope |
|---|---|---|
| Used quantity / unit | `s16bq03a/b` | household campaign input use |
| Gift answer / quantity / unit | `s16bq05`, `s16bq07a/b` | received as gift/donation |
| Purchase answer / quantity / unit / amount | `s16bq08`, `s16bq09a/b/c` | bought input |

Thus `q05/q07a` must not be copied into purchase columns. Three unit fields
remain distinct even though each own dictionary publishes the same five
labels. Each was checked separately: used/gift/purchased fields in 2018
[V1089](https://microdata.worldbank.org/catalog/4295/variable/F31/V1089?name=s16bq03b),
[V1094](https://microdata.worldbank.org/catalog/4295/variable/F31/V1094?name=s16bq07b),
[V1097](https://microdata.worldbank.org/catalog/4295/variable/F31/V1097?name=s16bq09b),
and 2021
[V1288](https://microdata.worldbank.org/catalog/6275/variable/F30/V1288?name=s16bq03b),
[V1294](https://microdata.worldbank.org/catalog/6275/variable/F30/V1294?name=s16bq07b),
[V1297](https://microdata.worldbank.org/catalog/6275/variable/F30/V1297?name=s16bq09b).
This does not establish that any given household used, received and bought
in the same unit. The published input-unit code 1 denotes grams and 2
kilograms, the reverse of the 2014 community-price grid's first two codes.
No existing EAC-I unit dictionary is a decoding authority for these fields.

Both `s16a` lists identify use answers `s16aq22` (animal waste), `s16aq25`
(household refuse/other), `s16aq27` (inorganic fertilizer), `s16aq30`
(phytosanitary). Parcel quantity/unit pairs include `s16aq24a/b` for manure,
`s16aq29a1/a2` for urea, `b1/b2` for phosphates, `c1/c2` for NPK/formule
unique, `d1/d2` for DAP **or other** chemical fertilizer, and `s16aq31a1/a2`,
`b1/b2`, `c1/c2`, `d1/d2` for the four phytosanitary slots. Do not infer
an NPK grade or pure DAP composition from these questions.

## Source identities change by wave

The own input-identity dictionaries
[2018 V1086](https://microdata.worldbank.org/catalog/4295/variable/F31/V1086?name=s16bq01)
and
[2021 V1285](https://microdata.worldbank.org/catalog/6275/variable/F30/V1285?name=s16bq01)
publish 20 and 22 categories. At the tail:

| Code | 2018 label | 2021 label |
|---|---|---|
| 19 | Plants/boutures de tubercules | Semences d'arichide |
| 20 | Autres semences | Plants/boutures de tubercules |
| 21 | not listed | Plants d'hévéa, de teck |
| 22 | not listed | Autres semences |

These are source dictionary facts, **not new curated mappings**. A shared
numeric seed map would misidentify tubers and peanuts. The 2021 public
category counts include the input menu, so they are not the issue's count
of applied NPK records. That count has not been remeasured.

## Reserved choices and bounded next operands

GH #1483 explicitly asks: “Decide whether to wire at plot grain.” ROOT must
settle household roster versus parcel application representation, treatment
of their overlap, source eligibility, input/seed labels and unit carriers.
A household's purchased amount cannot be spread over its plots without a
separate rule. The DAP catch-all and nitrogen composition remain GH #1445.
Mali's current finalizer encodes EAC-I missing codes, fixed synthetic input
slots and an existing duplicate reducer; it is not an EHCVM adapter.

Only after ROOT reviews a precise execution plan, the smallest native
comparison would use these four **declared sidecar identities**:

| Wave / source | MD5 | Declared bytes |
|---|---|---:|
| 2018-19 s16a | f509d90b4557ed049ac4d5cfb6455bb0 | 3,545,863 |
| 2018-19 s16b | 4c5a9f33e93b9f7655fdee856cbe8fc7 | 643,645 |
| 2021-22 s16a | c0c31ab14e9ab192e5d41e934665fc07 | 5,558,508 |
| 2021-22 s16b | 673cc63eb5afdf3dc45931d81e867538 | 3,722,722 |

Total declared bytes: 13,470,738. These were read from committed sidecars,
not located, fetched or authenticated as payloads. A native plan must first
name already held regular operands and obtain ROOT's execution release.
It would compare the named input fields, own value-label dictionaries,
explicit use answers, missingness and native key multiplicity. It would not
use Country/Feature, sample, weights or shared caches. Any later public API
plan needs its own full dependency and execution authorization.

Literal invented control **plans**, not fixtures written or tests run:

- Give one invented household two parcels, one household used amount 10,
  and parcel amounts 3 and 7. Reject a join that repeats 10 on both parcels;
  no expected public shape is chosen before ROOT's grain decision.
- Give a gift answer/amount of yes/3 grams and purchase answer/amount of
  no/missing, then reverse them. Separate source roles and their own units
  must survive; no zero or same-unit conversion is presumed.
- Use input codes 19 and 20 separately in each wave, with source labels
  above; reject a one-wave numeric map reused across both waves.
- Use an unanswered use flag beside an amount and a negative answer beside
  zero. Record the competing interpretations for ROOT; do not pick inclusion
  or rewrite missingness merely to make a test green.

The source-only dispatch has not run or collected tests, imported the
project, built a table or changed source eligibility, mappings or defaults.
