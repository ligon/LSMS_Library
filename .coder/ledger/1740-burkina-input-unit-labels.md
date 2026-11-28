# Prior-Art Ledger — #1740 Burkina Faso plot_inputs acquisition unit labels

**Search tier:** git + ripgrep. Base `47df62d49e614c9a56e319def7324145d138c3d9`. Inherits `STANDING.md` and `CLAUDE.md` (Data Access and Cache Behavior).

## §1 Task, restated

Burkina Faso's 2018-19 and 2021-22 household s16b builds serve the used-unit index `u` in the Country API vocabulary, but leave the two acquisition payload columns at the local-only spelling. Align only the already decoded `Unit_purchased` and `Unit_received_as_gift` to the existing served vocabulary. Do not change any code card, source measurement, index, table availability, or physical-unit/parcel/crop policy.

## §2 Existing machinery

| Symbol | Anchor | Evidence and decision |
|---|---|---|
| `served_unit_labels` and `_apply_build_categorical_mappings` | `lsms_library/ehcvm.py:464`, `lsms_library/country.py:1299` | Reuse the actual merged `u` read-time mapper. `tests/test_gh1454_served_unit_fingerprint.py` covers callable closure and passthrough. |
| Benin s16b purchase and gift build | `Benin/2018-19/_/plot_inputs.py:102`; `tests/test_benin_2018_plot_inputs.py:68` | Reuse the pattern of explicit normalization for the two payload columns, leaving the used `u` alone. Benin's raw formatting is not Burkina's. |
| 2018-19 own labelled source fields and `_unit_labels` | `Burkina_Faso/2018-19/_/plot_inputs.py:70,107-138` | Retain own Stata labels, local exact matching and nullable output. Covered by `tests/test_gh1188_burkina_input_acquisition_fields.py` and `tests/test_burkina_faso_plot_inputs_identity.py`. |
| 2021-22 own cards and injected wrapper | `Burkina_Faso/_/s16b_2021.py:48-66,83-188`, `Burkina_Faso/2021-22/_/plot_inputs.py:17` | Retain the pure adapter, all three source codebooks and raw operands. Enrich the wave's write callback as the existing sanctioned file boundary, without modifying the adapter's caller-supplied IO protocol. Covered by `tests/test_burkina_faso_1483_header_adapter_20261110.py`, `tests/test_burkina_faso_2021_wave_writer.py`. |
| Automatic categorical mapping | `lsms_library/country.py:3899,4397` | Mapping auto-applies to named columns/indexes. Both payload columns lack mappings by name, and only `u` is currently normalized. |
| Shared build inputs and fingerprint | `lsms_library/country.py:1450-1568` | Reuse cache invalidation; the two wave scripts are wave-wide hash inputs. Keep shared modules and both global and country Org mapping tables unchanged. |

## §3 Definitions & conventions in force

- `Burkina_Faso/_/CONTENTS.org`, #1188: "Each unit variable uses its own decoded labels and only exact curated formatter keys; equal numeric unit codes do not establish equal units." Used = q03a/b. Purchase = q08 and q09a/b. Gift = q05 and q07a/b. The 2021 own monetary q09c remains a separate raw field.
- `Burkina_Faso/_/CONTENTS.org`, #1483 integration qualification: the earlier unbuilt 2021 statement is historical and superseded. 2021 selects q02 = 1 from its panel file only. s16b is household by input, not parcel by input.
- `lsms_library/categorical_mapping/u.org` keeps global defaults to kilogram spelling variants. The local Burkina `u` table uses the same key and takes precedence on collisions. This task does not add any equivalence.
- `lsms_library/data_info.yml`, the Burkina `data_scheme.yml` and standing §§3-4 govern optional columns, native index and sanctioned IO.

## §4 Invariants & assumptions

- Preserve used `u` before and after finalization, all amounts, separately sourced flags, raw 2021 operands, NA masks, dtypes, attributes, source-native identities, public row counts and the used-unit sentinel from #1118. No fallback across acquisition roles, no filtering on a purchase/gift answer and no import of s16c/s16d harvest units.
- The admitted private files are only `2018-19/Data/s16b_me_bfa2018.dta` (`32eb603ade5ae97b1844c00a2923fd31`, 988,943 B) and `2021-22/Data/s16b_me_bfa2021.dta` (`ed8625abcf1d679669d7d231cf77f58c`, 20,891,535 B). Native producers plus the public categorical stage are testable with those files. The complete public Country call would also join all-wave sample and is outside this projection.
- Independently decoded own cards and actual producers agree: 22,877 / 12,223 applied rows, 16,196 / 2,337 used kilograms already served as `Kg`. Payload correction is `Kilogramme` to `Kg` for 5,197 / 1,122 purchase cells and 331 / 53 gift cells. All are affirmative and have positive amounts. Shared observed same-role labels have no cross-wave spelling disagreement.
- For example 2018-19 code 5 is `Charrettée` in the used card (3,889 rows), but `Charrette` in purchase (132) and gift (397). Preserve those exact label distinctions. Preserve 2021 sack sizes, Yorouba variants and unmapped qualified tokens.

## §5 Reuse decision

| Quantity | Decision | Reason |
|---|---|---|
| Canonical vocabulary | Reuse `served_unit_labels` | It runs the actual public mapping, including additive global defaults and Burkina precedence. |
| Application to payload columns | Extend both wave boundaries only | The framework label-by-name path correctly does not infer the semantic role from a different column name. No mapper, pure adapter, schema or config policy change. |
| Independent evidence | Reuse existing guarded script/adapter tests and compare own numeric and decoded answers | Numeric code equality cannot establish physical or reported unit identity. |

## §6 Open questions for the human

None for the measured mechanical spelling change. Any proposal to harmonize Burkina carts, sacks, parcel linkage or crop UML is a separate policy decision.

### Phase 3 — verification

- **OK (§§2–5), reuse.** Both wave boundaries call the real served `u` mapper only on non-null, already-decoded acquisition labels. No changes to either producer's source-code decoder, the pure 2021 adapter, local/global tables, or the read-time mapper.
- **OK (§§3–4), definitions.** Guarded cold reference/candidate producer runs in the verified image, offline, with private runtime state and the two MD5/size-verified sources above. Each unit role was compared row-for-row with its independent DTA value-label card. The 2021 committed cards match their corresponding provider cards. Frames and attrs are exact outside the two columns, and their NA masks and dtypes agree. The four measured changes above are the only differences. The full Country-to-sample path was not exercised.
- **OK (§4), regression.** 160 passed, 6 deselected (requires-S3) in the new both-wave unit test plus the Burkina #1188/identity/#1483/writer, Benin #1705, and shared #1454/EHCVM unit tests.
- **OK (§2), cache.** Metadata-only reference/candidate census: 25 of 25 Burkina country-table hashes and 50 of 75 wave-table hashes move under the existing conservative fingerprint. The changed wave hashes are 25 each for 2018-19 and 2021-22, including 18 and 14 actually declared wave-table pairs respectively. All 25 2014 wave hashes, 26 Burkina build-transform hashes, and the Burkina country-module import fingerprint stay fixed. Benin control: no change in 23 country, 46 wave, 24 build-transform hashes or the country-module import fingerprint. This is cache invalidation breadth, not output change breadth.
