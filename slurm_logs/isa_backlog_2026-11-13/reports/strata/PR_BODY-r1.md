
## sampling.yml: `served_strata`, what `sample().strata` holds per wave (GH #1577)

**Ruling** (@ligon, 2026-11-12): "Registry + sampling.yml tag". Constructed strata stay in the derivations registry, and a per-wave tag records the native-code, native-label and absent cases, widened under GH #1134's rule (one value at a time, each with its first evidenced record).

### What changes
- `lsms_library/sampling.py` adds one top-level block, `served_strata`, with its own provenance like `panel`:
  - `holds`: `native-code` | `native-label` | `constructed` | `absent`;
  - `variable` for native values only, and `derivation`, a `<country>::sample::<name>` registry key, for `constructed` only;
  - `note`, `source_type`, `confidence`, `locator`;
  - `SamplingRecord.strata_holds` reads the value. An entry of `served_strata` alone is a record, so first waves can carry one.
- **First records:**

  | value | record |
  |---|---|
  | `native-code` | Mali 2017-18 (`strate`, no value labels) |
  | `native-label` | Niger 2011-12 |
  | `constructed` | Mali 2018-19 |
  | `absent` | Mali 2014-15 |

- **19 records** across Mali, Niger, Burkina Faso, Togo, Côte d'Ivoire, Senegal (new file, `served_strata` only) and Uganda (2005-06 to 2010-11).
- **Not recorded by design:** a code the library decodes through its own lookup (Burkina 2014, Nigeria W5), and region-type variables served as `strata`. Both are questions for the maintainer.
- **Benin and Guinea-Bissau** have registered constructions but no record here; their records are proposed for integration, and both countries are listed by name in `SERVED_STRATA_PENDING`.
- `docs/guide/feature.md`: the strata table is updated for #1575 and #1573 and gains a `served_strata.holds` column.
- Mali and Burkina CONTENTS.org get dated notes.

### Verification
- **Cache:** hash census `f138a3d61` → `0d62f49df` moved `build_transforms_fingerprint` 0/42, framework imports 0/38, `Country._table_cache_hash` 0/555, `Wave._input_hash` 0/2,321.
- **Unit tests:** 779 passed, 0 failed, across test_sampling, test_sample, test_schema_consistency, test_build_transform_hash, test_gh1494, test_gh812, test_feature_attrs_carriers and test_gh1503, all `-m "not requires_s3"`.
- **Delivered test:** `test_the_tag_agrees_with_the_served_strata` passed 19/19, cold, against what each wave's `sample()` serves. It checks:
  - `absent` means NA or no column;
  - `constructed` means the pointed key on every row with a stratum and on no other row;
  - `native-code` values are digit strings, and `native-label` values are not.
- **Counts:** a dump of distinct values per wave matches every count quoted in a locator.
- **Read path only:** Senegal frames now carry `attrs['sampling']`. Four first or uncurated waves get a record in place of the `unrecorded` placeholder. No data, weight, Site C warning or default changes.

### Follow-ups
- **#1577:** the remaining cells, which need a value census, a search record or a ruling.
- **New issue:** register the Ethiopia, GhanaAHIES and EthiopiaRHS strata constructions.
- **New issue:** a one-wave Mali 2014-15 `sample()` lacks its declared-required `strata` column.
- **Still open:** #1134 (no `design_tag` added) and #1574.

Addresses #1577 and #1134 (partial).

🤖 Generated with [Claude Code](https://claude.com/claude-code)
