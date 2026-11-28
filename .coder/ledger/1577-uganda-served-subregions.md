# Prior-Art Ledger — #1577 Uganda served subregions

**Search tier used:** ripgrep and git, plus the held Stata value-label
dictionaries and exact projected sample rows.

## §1 Task, restated

Classify Uganda's five later `sample().strata` columns in the existing
`served_strata` metadata. These columns describe the households' recorded
subregions. This does not settle whether these are sampling-design strata.

## §2 Existing machinery

| Symbol | Source | Role and decision |
|--------|--------|-------------------|
| `served_strata` | `lsms_library/sampling.py`, `STRATA_HOLDS` and `_check_served_strata` | Reuse the `native-label` record. The metadata is about what is served, not the sample design. |
| `sample.strata` | Uganda's five wave `_/data_info.yml` files, `sample` section | Reuse each wave's own labelled `sregion` or `subreg`. |
| `_adopt_alternate_cover` | `lsms_library/countries/Uganda/_/uganda.py:491` | In 2015-16 only, the existing branch serves the alternate cover's own label only for households missing from the primary cover. |
| `strata` spelling folds | `lsms_library/countries/Uganda/_/categorical_mapping.org` | Keep the existing display cleanup. |
| Native-tag tests | `tests/test_sampling.py`, `TestANativeRecordIsWhatTheConfigServes` and `test_the_tag_agrees_with_the_served_strata` | Reuse both checks and pin the alternate-cover branch separately. |

## §3 Definitions & conventions in force

Use the #1577 ruling "Registry + sampling.yml tag". The registry is for
constructed values. See `STANDING.md` §3 and the Uganda `_/CONTENTS.org`
sections "Strata" and "Household-level `ea`". The early six-label field
`stratum` is not interchangeable with the later subregion fields.

## §4 Invariants & assumptions

Per `STANDING.md` §4, identifiers and data must not move for this metadata
change. In particular, AGSEC1 extends membership without supplying a
stratum in 2011-12 and 2018-19. In 2015-16, the primary cover's blank
cannot be filled from the alternate cover. In 2018-19 and 2019-20, the
inherited origin EA is independent of the household's own subregion.
Documented EA non-nesting remains. No design method or weight changes.

## §5 Reuse decision

Use five instances of the existing `native-label` metadata. No change to
loader, registry, cache, vocabulary, precedence or taxonomy.

## §6 Open questions for the human

None for the served-value tag. The existing Uganda design and weight
investigation remains separate.

---
### Phase 3 — verification

- `served_strata` — OK (§2–§5). An exact held-raw projection of the five
  waves matches every pre-finalize stratum/missing value, including the
  72 alternate-cover households in 2015-16. Independent offline Country
  and Feature builds each return the same 15,743 rows as development,
  with identical dtypes and warnings. Only the new sampling records enter
  the five-wave attrs; all 252 Uganda cache fingerprints are unchanged.
- Alternate-cover regression — OK (§2, §4). The source-free test covers a
  conflicting alternate value, a blank primary value, and an alternate-only
  label. Run it with the native-tag and Uganda cover tests:
  `pytest tests/test_sampling.py tests/test_gh689_uganda_ea_psu.py
  tests/test_gh1441_uganda_market_labels.py -m 'not requires_s3'`.
