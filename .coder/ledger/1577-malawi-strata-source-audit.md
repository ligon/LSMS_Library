# Prior-Art Ledger — #1577 Malawi strata operands

**Search tier:** git and ripgrep, country sampling notes, and exact held
Stata projections with the current declared mapping. Baseline
`47df62d49e614c9a56e319def7324145d138c3d9`.

## §1 Task, restated

Correct the Malawi country notes about the source of `sample().strata`.
Separate fields and mappings that the library serves from the definitions
of design strata in each survey's documentation. No production change.

## §2 Existing machinery

| Machinery | Source | Decision |
|-----------|--------|----------|
| Sample operands | Six Malawi wave `_/data_info.yml` sample blocks | Read the actual declared source column for each wave and component. |
| District lookup | Malawi `_/categorical_mapping.org`, table `district` | Preserve the build-time aliases and measure occupied category collisions. |
| IHS6/IHPS overlap | Malawi `2024-25/_/mapping.py`, `sample` | Preserve IHS6 precedence and keep the sample-weight rules. |
| Served strata vocabulary | `lsms_library/sampling.py`, issue #1577 | No tag in this patch. The test suite intentionally treats the explicit Malawi relabel as non-native today. |

## §3 Definitions & conventions in force

See `STANDING.md` §§3–4 and Malawi `_/CONTENTS.org`'s own design
citations. A codebook label, its served display value, and an actual
sampling stratum are different claims. Counts of labels do not count
distinct design strata.

## §4 Invariants & assumptions

Use only the nine declared held household covers for the source audit,
and read just the stated geography labels, the two older alternative
fields used for comparison, and the 2024 overlap key. No change to
values, mappings, weights, precedence, cache state, or vocabulary.

## §5 Reuse decision

Repair the current country description. Leave `sampling.yml` and
derivations alone while the broader classification question remains.

## §6 Open questions for the human

None for the factual description. The #1577 question about how to
label explicit source mappings, especially the pooled waves, remains.

---
### Phase 3 — verification

- **OK (§§2–4).** Nine exact held covers (13,368,535 bytes) matched
  declared DVC sizes and digests in private offline roots. The source
  columns and their label bindings were read independently and applied
  to the dictionaries obtained through the actual wave YAML mapper.
- **OK (§§3–4).** The 2004 and 2010 mappings preserve all 30 and 31
  occupied category distinctions, changing 1,680 and 1,149 rendered
  strings respectively. The selected 2013 district labels each span
  more than one code of the separate six-class `stratum` field.
  The 2016 panel has a within-cover many-to-one mapping (29 to 28);
  each of the three pooled waves has two many-to-one destinations
  over the union of both covers' native strings before overlap removal.
  These are not counts of the final served sample. The 2024 covers differ on 32
  native spellings among 815 overlaps, but none after the existing
  mapping. These checks characterize the source and mapping, not a
  fresh full Malawi sample build.
- **OK (§4).** All 182 Malawi sample and other table fingerprints
  in the country and wave inventory are unchanged. The 2024-25 overview
  uses the separately verified #1275 cover overlap and sample counts.
