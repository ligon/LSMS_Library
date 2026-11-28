# Prior-art ledger: Malawi stacked sample estimands (GH 1704)

Search tier: ripgrep + git, pinned development 47df62d49. Inherits STANDING.md.

## §1 Task
Compare the current pooled rural household share with the corresponding served IHS and retained-IHPS component estimates and denominators. No production weight change.

## §2 Existing machinery
- `Country.sample` wave YAML and Malawi's component identity and refreshment-cohort rule.
- `country._normalise_sample_weights` and `tests/test_weight_normalisation.py`: within-wave mean-one normalization leaves ratio estimates invariant.
- Malawi CONTENTS.org documents the existing sample and the separate 2024-25 raw-scale `ihps_weight` for the full IHPS cohort.

## §3 Definitions
See STANDING.md and Malawi CONTENTS.org, weights and decision 48. IHS cross-section, retained IHPS observations, and the full IHPS cohort including refreshment households are different estimands. The pooled current weight sum is not a national population total.

## §4 Invariants
Confirm component membership from the wave's identity construction, not from the presence of raw `ihps_weight` on overlap households. Keep matched denominators, missingness and production panel links intact.

## §5 Reuse decision
Call existing sample serving and use weighted shares with exactly the same served weights within each group. Also show the full 2024-25 IHPS share separately where supported.

## §6 Ruling
The maintainer selected exposing the interview source without changing weights on 2026-11-25. The source tag is assigned at cover extraction and does not use weight presence. The 815 copied refresh interviews remain labelled IHS, preserving their separate IHPS weight and identifier.

## Verification
Three private, separately populated six-input roots: the base and candidate default and Make produced identical ordered index, dtype and existing-value receipts after the new column was removed. All 815 IHS-tagged refresh interviews carry both their IHPS ID and weight. The targeted unit and namespace suite passed 52 cases with 24 data-gated tests deselected.
