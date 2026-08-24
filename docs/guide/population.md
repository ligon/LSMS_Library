# The Population Record

Two "nationally representative" surveys can represent different populations.
The library now says so, per `(country, wave)`, and carries the record with the
data.

`Liberia/2018-19` is the **National Household Forest Survey**: 32 clusters,
98.8% rural, target population *forest-proximate communities*, in a ~50%-urban
country. It flowed into every cross-country `Feature()` call beside 33
general-purpose surveys. `Liberia/_/CONTENTS.org` already described the problem
correctly, in plain English, for months. It changed nothing — **prose in a
`CONTENTS.org` is not enforcement.**

Ethiopia is why this is load-bearing rather than an oddity. ESS W1 (2011-12) was
designed for *rural areas and small towns*: 503 urban households. By 2018-19 the
same panel has 3,655. A household fixed-effects model across W1→W5 pools a
universe that moved underneath it — and every one of those waves grades `sane`.

So: **the universe is a property of the `(country, wave)` cell**, not of the
country and not of a table.

## What it does, in three steps — and the fourth it deliberately skips

1. **Config** — `countries/{C}/_/population.yml`, keyed by wave.
2. **Surface** — the record rides on `df.attrs['population']`.
3. **Warn** — a `Feature()` call that pools materially different universes says
   so, naming them.

There is **no fencing**. GH #603 proposed excluding `specialized` frames from
`Feature()` by default; that was declined. A default that silently *drops* data
is the same disease as one that silently *pools* it, with the sign flipped — and
the countries most likely to be dropped by a "national-only" default are the
LSMS-ISA countries, which restrict their frames by design because they are
agriculture-focused. **`Feature()` returns everything it returned before.**

## Reading the record

```python
import lsms_library as ll

ll.Country('Ethiopia').population['2011-12'].universe_tag
# 'rural-and-small-town'

df = ll.Country('Ethiopia').household_roster()
df.attrs['population']['Ethiopia']['2011-12']['exclusions']
# the verbatim exclusion clauses from the ESS sampling appendix
```

`df.attrs['population']` has the same shape from `Country(...)` and from
`Feature(...)`:

```python
{country: {wave: {...record...}}}
```

`df.attrs['population_resolution']` says how the waves were matched: `exact`
(every `t` value matched a wave label), or one of the `all-waves (...)` forms
for a frame with no `t` axis or with `t` values that are not wave labels.

> **`attrs` survive an operation only when every input agrees.** Any
> disagreement — including one side having none — yields `{}`. That one rule
> covers `merge` and `concat` together, and makes single-input operations
> (`set_index`, `rename`, `dropna`, `groupby().first()`) obviously safe.
> `merge` with the *same* `attrs` on both sides preserves them; it is the
> disagreement that loses them, not the merge.
>
> Cross-country assembly is a `concat` over frames whose population records
> differ **on purpose** — one per country — so it lands in the `{}` case every
> time. That is why `Feature` captures each country's record before assembly
> and re-attaches it after; the re-attach is load-bearing, not belt-and-braces.
> If your own code merges these frames you have two valid fixes: copy `attrs`
> across explicitly, or give both frames the same record. See `CLAUDE.md`,
> "Panel ID Transitive Chains and the `attrs` Flag", for the measured table and
> the bug this hazard already caused.

## The three fields, and why they are inseparable

Every record carries `universe_tag`, `source_type` and `confidence`. They are
written together, returned together, and reported together, because the tag is
**not a quote**. The source document says so in capitals:

> THE TAG IS AN EDITORIAL READING. IT IS NOT A QUOTE, AND NO DOCUMENT USES IT.

| field | values | what it tells you |
|---|---|---|
| `universe_tag` | the ten below | an editorial one-phrase compression of what the documentation, taken as a whole, says the sample represents |
| `source_type` | `local-documentation` / `wb-catalog` / `not-found` | where the statement was found. 41 of 111 waves' statements exist **only** in World Bank catalog metadata, which the Bank can revise or blank |
| `confidence` | `high` / `medium` / `low` | `medium` = "a coverage or representativeness claim, not a universe declaration"; `low` = "this is sample-design text and **should not be laundered into a universe**" |

Quoting the tag without the other two turns a judgement into a fact. That is
the failure mode this design is built against, and it is the same one
`capability.py` guards for coverage verdicts and `provenance.py` guards for
catalog ids.

Records also carry the **verbatim** `population_statement` and `exclusions`, and
the extractor's `notes`. The exclusions matter most: they are what makes two
"nationally representative" surveys represent different populations.

## The ten tags

| tag | waves | meaning |
|---|---|---|
| `national-all-households` | 37 | the documentation **names** the population: all households / residents / private dwellings |
| `national-claimed` | 28 | only a coverage claim — "National", "nationally representative". No document names the population |
| `region-excluded` | 18 | national minus named regions (Tirana, Kosovo and Metohija, Likoma, Kidal, Tigray, Arlit, rural Borno, FATA, …) |
| `subnational-area` | 11 | a named sub-national area from the outset (rural Hebei/Liaoning; four UP/Bihar regions; 15 Ethiopian peasant associations; metropolitan Lima) |
| `not-stated` | 7 | no population statement exists |
| `panel-inherited` | 5 | defined only by reference to an earlier wave's sample |
| `mixed-national+panel` | 2 | one wave directory, two declared universes (Malawi IHS4/IHS5 + IHPS) |
| `rural-and-small-town` | 1 | Ethiopia ESS1 |
| `specialized` | 1 | Liberia NHFS — forest-proximate EAs, urban Montserrado excluded |
| `agricultural-households` | 1 | Nigeria 2012-13, per the WB catalog's own `Universe` field |

Plus one value the library writes and config may not: **`unrecorded`**, for a
wave with no entry. It is deliberately distinct from `not-stated` —
*"nobody looked"* is not *"we looked and the documentation says nothing"*.

## The warning

```
household_roster: this result pools 3 materially different population
universes. Nothing was dropped -- every country you asked for is in the result;
this is a comparability warning, not a filter.
  region-excluded [national minus named regions/districts] -- 3 wave(s), 1 country(ies): Ethiopia 2013-14, Ethiopia 2015-16, Ethiopia 2021-22
  national [national (documented or claimed)] -- 1 wave(s), 1 country(ies): Ethiopia 2018-19
  rural-and-small-town [rural areas and small towns only] -- 1 wave(s), 1 country(ies): Ethiopia 2011-12
  ...
```

It is a `PopulationHeterogeneityWarning`, fires **once per `Feature()` call**,
and never fires from a `Country(...)` call — a single-country call still gets
the record on `attrs`, but the decision to pool is the analyst's.

Silence it, if you have made the comparability judgement yourself:

```python
warnings.filterwarnings('ignore', category=ll.population.PopulationHeterogeneityWarning)
```

### What counts as "materially different"

Two or more distinct **comparability classes**, or one documented class plus at
least one wave whose universe is unknown.

- **`national-all-households` and `national-claimed` are one class**,
  `national`. The difference between them is *epistemic, not substantive* —
  whether any document names the population, versus only claiming national
  representativeness. Both are attempts to measure the same target population,
  so pooling them is not a comparability error. It is a provenance difference,
  and provenance is what `source_type` / `confidence` already record. **The two
  tags stay distinct everywhere else**: in config, in `attrs`, and in every
  report. 28 waves are `national-claimed`, including all eight Uganda waves and
  all four Côte d'Ivoire CILSS waves, and how well documented a universe is
  remains a real fact about those surveys.
- **`region-excluded` is its own class** and warns against `national`.
  "National minus Tigray" is not national: Ethiopia 2021-22's own weighting
  sentence puts Tigray outside the *represented* population. Malawi's Likoma
  exclusion (IHS2/IHS3) then inclusion (IHS4 onward) is a real break in the
  universe that a naive panel would silently absorb.
- **`not-stated` and `unrecorded` trigger the warning, but as UNKNOWN** — the
  message says comparability *cannot be verified*, not that the universes
  differ. Treating an undocumented universe as equivalent to a documented
  national one would be silence masquerading as knowledge.
- A pool of **only** unknowns says nothing: there is no comparability claim to
  make.

### How often it fires

Measured over `.coder/coverage/latest.csv` — every `(country, feature, wave)`
cell that actually builds — for the rule as shipped:

| call shape | fires |
|---|---|
| `Feature(f)()`, all countries | 31 of 38 features |
| `Feature(f)([one country])` | 194 of 489 (all of them multi-wave) |
| `Feature(f)([two countries])` | 81.2% of 4,967 |

An all-country call almost always warns, and that is not noise: such a call
genuinely pools nine distinct classes, from `specialized` to `subnational-area`.
The message names them, so what the user gets is a list of what is in their
pool, not a bare alarm.

## Adding or correcting a record

The `population.yml` files are **generated**. Edit
`slurm_logs/POPULATION_STATEMENTS_2026-07-21.org` — the verbatim evidence base —
and re-run:

```sh
python scripts/promote_population_records.py --check   # validate, write nothing
python scripts/promote_population_records.py           # write
```

The script refuses to write unless every entry parses, every entry agrees with
the document's own independent summary table, and the resulting tag /
`source_type` / `confidence` histograms equal the counts the document publishes
about itself.

### Waves the sweep does not cover

The sweep covered the 111 wave directories the library shipped on 2026-07-21.
Four of its rows describe waves the library cannot build (Afghanistan ×2 and
Albania 1996 have no `data_scheme.yml`; Benin `2018-2019` is a duplicate staging
directory). They are written to config as `status: inert` so the evidence
survives, and the loader never returns them.

Nine country directories were outside the sweep entirely because they ship no
`_/` config at all — Bosnia-Herzegovina, Brazil, Bulgaria, KenyaLPS, Kyrgyz
Republic, Nicaragua, Rwanda, Tanzania_Kegera, Harmonized_LSMS-ISA_Ag. When one
of them is wired up, its waves will surface as `unrecorded` until swept.
