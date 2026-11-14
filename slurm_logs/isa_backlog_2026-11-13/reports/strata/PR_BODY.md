
## sampling.yml: `served_strata`, what `sample().strata` holds per wave (GH #1577)

**Ruling** (@ligon, 2026-11-12): "Registry + sampling.yml tag". Constructed strata stay in the derivations registry, and a per-wave tag records the native-code, native-label and absent cases, under GH #1134's widening rule.

### What changes
- `lsms_library/sampling.py` adds a top-level block, `served_strata`, with its own provenance, like `panel`:
  - `holds`: `native-code` | `native-label` | `constructed` | `absent`;
  - `variable` for native values; `derivation` for `constructed`, a canonical `<country>::sample::<name>` key;
  - `note`, `source_type`, `confidence`, `locator`;
  - `SamplingRecord.strata_holds` reads the value. A `served_strata`-only entry is a record, so first waves can carry one.
- **First records:**

  | value | record |
  |---|---|
  | `native-code` | Mali 2017-18 |
  | `native-label` | Niger 2011-12 |
  | `constructed` | Mali 2018-19 |
  | `absent` | Mali 2014-15 |

- **19 records** in Mali, Niger, Burkina Faso, Togo, Côte d'Ivoire, Senegal (new file) and Uganda (2005-06 to 2010-11).
- **Not recorded by design:** library-decoded codes (Burkina 2014, Nigeria W5) and geographic variables served as `strata`. Both are questions for the maintainer.
- **Benin and Guinea-Bissau** are listed in `SERVED_STRATA_PENDING`, with proposed blocks for integration.
- `docs/guide/feature.md`: the strata table is updated for #1575 and #1573 and gains a `served_strata.holds` column. It documents `Country(c).sampling.get(wave)`: `[wave]` raises `KeyError` on an uncurated wave.
- Mali and Burkina CONTENTS.org get dated notes.

### Verification
- **Cache:** nothing measured moves a served value, a hash or a warning. The hash census `f138a3d61` → `79739f816` moved `build_transforms_fingerprint` 0/42, framework imports 0/38, `Country._table_cache_hash` 0/555, `Wave._input_hash` 0/2,321; the red team confirmed this independently.
- **Unit tests:** 794 passed, 0 failed (8 files, non-S3).
- **Full S3 run:** `test_sampling.py` and `test_gh1494` gave 416 passed, 0 failed, cold. This includes the 19 delivered checks that each tag matches the served frame.
- **Static check** (new, non-S3): each native record's `variable` is the wave's configured `strata` source and nothing relabels it.
- **Validator:** refuses unknown values and fields, missing provenance, `not-found`, mismatched `variable`/`derivation`, other-country or other-table keys, non-canonical keys, and `absent` naming anything.

### Must-know
1. Mali 2014-15's one-wave `sample()` column set depends on cache state, identically on base: cold has no `strata`, and after an all-wave call it is all NA. This is a pre-existing defect, filed as a residual.
2. `constructed` comes from the brief, beyond the ruling's literal text. The registry↔tag sync test obliges every future strata registration to tag its waves, or to be listed as pending.
3. Four values arrived in one PR, each with a verified first record. The maintainer should confirm that this reading of "one tag at a time" is acceptable.
4. **Attrs changes:**
   - Senegal frames now carry `attrs['sampling']`.
   - Mali 2014-15, Niger 2011-12, Togo 2018 and Côte d'Ivoire 2018-19 replace `design_tag='unrecorded'` placeholders with records whose `design_tag` is None.
5. **Integration:**
   - Merge the Benin and Guinea-Bissau `served_strata` blocks into the existing entries. Never paste duplicate wave keys: the loader keeps the last one silently.
   - Add Guinea-Bissau 2021-22, whose registry span is on the gnb branch.
   - Then remove both from `SERVED_STRATA_PENDING`.
   - Resolve the CONTENTS.org append conflicts. Mali: this branch, coping `9abab1855`, cropkg `49aea7436`. Burkina: this branch, coping, burkina `d4d063fa1`.

### Follow-ups
- **#1577:** the remaining cells.
- **Register strata constructions:** Ethiopia, GhanaAHIES, EthiopiaRHS.
- **Cache-state-dependent column set:** the Mali 2014-15 one-wave `sample()` defect.
- **Duplicate keys:** make `_SamplingLoader` refuse duplicate keys.
- **Still open:** #1134 and #1574.

Addresses #1577 and #1134 (partial).

🤖 Generated with [Claude Code](https://claude.com/claude-code)
