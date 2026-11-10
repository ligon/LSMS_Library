# Prior-Art Ledger — #1288 Niger plot_inputs phantom reference

**Base:** c8e378920968fde414767855186e64f5e36f54bb.
**Search tier:** git + ripgrep + AST floor; no GitNexus tool available.

## §1 Task, restated

Clean up only the two `get_dataframe(base + 'ECVMA2_AS02CP1.dta', ...)`
operands in Niger/2014-15/_/plot_inputs.py, spelling the identical effective
full relative path literally. The real source is already discoverable from
the `_ecvma_input_observations` provenance argument. This removes an extra
false bare-leaf metadata reference; it is not missing-real-input closure or
a measured runtime data-loss fix. Leave the harmless base assignment.

## §2 Existing machinery

| Symbol / source | Anchor | Existing coverage / decision |
|---|---|---|
| `scan_script_data_refs` | local_tools.py:3559 | Literal AST walker; reuse unchanged. Covered by tests/test_gh1288_niger_livestock_source_literal.py. |
| `source_fingerprint` / `cached_file_hash` | local_tools.py:3538 / :3518 | Sidecar SHA256 first; reuse unchanged, no raw reads. |
| `Country._make_declared_inputs` | country.py:5701 | Existing finite scanner/collector; use unbound on minimal metadata fixture as the livestock regression does. No Country construction or build-root transaction. |
| Original getters / provenance | Niger/2014-15/_/plot_inputs.py:30 / :31 / :49 | Both resolve to `../Data/NER_2014_ECVMA-II_v02_M_STATA8/ECVMA2_AS02CP1.dta`, True then False. |
| Prior discovery ledger | 1288-niger-native-source-discovery.md §2 | Already excludes plot_inputs as a provenance-literal lookalike. |
| Completed predecessor | Niger1288-ComputedPlotSources/97ad5fbe12fd49c98ccd2280c3645ee0/evidence/REPORT.md and SOURCE-PROOF.json | 14 stdlib controls are qualified history, not rerun. Full source covered, extra bare leaf unresolved. |

The sole real source's committed sidecar SHA256 is
`01c06d0fc063fc39b66591c6d1a7fc08f6ae2fa15a98684d1380b240a0c90fdb`.
Original whole script SHA256:
`5dda581782197be20f27ed132ebfd452f04cdc9e11bdbdcb6e1a2307bbb1d21d`.

## §3 Definitions & conventions in force

Inherits STANDING.md §§3–4, CLAUDE.md Cache Behavior / Data Access and canonical
lsms_library/data_info.yml; none is edited. Read all 3,611 lines of
Niger/_/CONTENTS.org, including status/history (only TODO/WAITING occurrence
is the Org declaration). Relevant country caveats:

- GH #679: the household key is `(GRAPPE, MENAGE, EXTENSION)`; split-offs are
  distinct, including the documented cross-wave `20012` namespace collision.
- Earlier text: repeated ECVMA seed slots are “NOT fixed, a design question”.
  Later GH #1360 residual: “the ECVMA collapse is gone” and “both wave frames
  are unique on the declared index”. `input_obs` retains each recorded slot,
  scoped by household and authenticated sidecar; this task preserves it.
- Existing source/form, quantity-999, gift/purchase, dry-season/window,
  sold-kg/state and panel caveats stand. Quote: “Only the harvest is ruled.”
  No contradiction is silently treated as stale, and no scientific choice is made.

## §4 Invariants & assumptions

Change exactly two getter operands. Preserve original effective reader paths,
order, True-then-False category flags and all kwargs. Keep every other source
byte and processing AST, helper, provenance literal, ID, filter, slot and writer.
No framework/scanner/guard change, original CI test edit, source acquisition,
raw/native/PDF/header/API/cache/producer/provider/census or campaign replay.
Freeze whole prior/candidate source and exact getter/scanner/collector witnesses
before one focused PURE cohort. Use the actual imported scanner/collector on a
finite copied script+sidecar fixture; its one selected wave and empty Other set
are not a full country/transaction exposure. Ordinary verified MAIN image,
private empty roots, false credentials, nonroot user/net namespace, cwd and
PYTHONPATH/config identity, BLAS/build1, --rootdir and -n2 --dist=loadfile apply.
No source changes while the test actor is live.

Script text participates in conservative build fingerprints. Exact hash impact
is unmeasured; no zero-cost or scoped-only hash claim, exclusion or cache re-warm.

## §5 Reuse decision

Reuse scanner, source fingerprint and metadata collector exactly; new focused
regression is needed for this concrete phantom marker, ordered getter equivalence
and source inverse. Freeze c8 source; independent literal expected path and pinned
whole-source/sidecar SHA256 avoid an oracle derived only from candidate text.
Only ledger, selected script and one new test file are in the product scope.
External operator/evidence files do not become production machinery.

## §6 Open questions for the human

None blocks authorized reversible engineering or meaningful PURE checks. The
user's explicit headless charter supersedes the skill's pause-for-ledger step.
Root/integration lead reviews the completed source/evidence before composition;
whole #1288, runtime data equality and broader cache/transaction exposure remain
outside this task. No new country fact warrants a CONTENTS appendix.

### Phase 3 — verification

OK (§§2–5): the script differs from c8 by exactly the two authenticated getter
operands. Whole-source byte inverse and whole processing-AST inverse agree;
the provenance literal, base assignment and sole real sidecar remain exact.
No scientific helper, ID, slot, schema, original CI test or framework changed.

OK (§§2, 4–5): the new test reuses the actual imported scanner, fingerprint and
unbound preparation collector. The corrected child passes seven checks (0
failures/errors/skips; pytest 33.85 s, whole actor 61.50 s). Those checks cover
both original/candidate getter kwargs/order, scanner literals, preparation source
sets/manifests and real sidecar/provenance. All three final observers have zero
denials; import/configuration/namespace identities pass, roots have no data or
config files, source bindings remain fixed and the actor group closes normally.
`make api-audit` exits 0. Static scope and whitespace checks pass.

Original failures are preserved outside the tree: an empty L1 directory tripped
the first preflight before any actor; the first cohort stopped with `.F` because
the new getter fixture also selected later filtering assignments, then its XML
write was refused outside the ordinary operator log root. The terminal lost the
per-node exception summary/JUnit in that shutdown, so the first inverse pass is
identified by source order, with a separate final static byte/AST proof. A child
then failed before collection on report-adapter quoting under Python 3.11. The
affected fixture, XML destination and quote were corrected and frozen; no
product change or broad replay followed. The already-passed inverse test body
was not rerun. A clean final-file eight-node run is not claimed.

No contradiction or reinvention relative to §§2–5. Exact fingerprint impact,
full transaction/country exposure, runtime data equality and scientific
revalidation remain unmeasured. Root/integration-lead composition review and
configured CI are external pending acceptance, not worker-certified outcomes.
