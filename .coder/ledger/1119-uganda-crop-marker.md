# Uganda #1119 crop-axis vocabulary

## §1 Scope
Implement the supplied issue/ruling 6368100317 at public d378c7e6 only;
preserve eligibility and all other policies. Search tier: git/ripgrep.

## §2 Machinery
Reuse uganda.plot_inputs_for_wave, seven native plot_inputs.py drivers,
INPUT_COLMAPS and harmonize_crop. Existing season tests document conditional
collapse. Malawi _seed_crop_identity supplies the lossless numeric-token
precedent; no Malawi ownership, eligibility or input_obs schema is adopted.
tests/nigeria_round_oracle.py assert_numeric_exact supplies EA + NumPy exact
checking. Country finalization and Feature alignment are production composition,
not independent scientific oracles.

## §3 Definitions
Inherit STANDING.md, CLAUDE.md and canonical data_info.yml comments 250-266.
Structural noncrop is (not crop-specific); missing crop-specific is unknown_crop;
recorded unmapped passes through. unknown_plot and Farm-level are separate axes.

## §4 Invariants
Keep the old seed eligibility lookup separate from new classification. No
quantity/unit/flag/map/weight/plot/reducer/season changes. Native grain is
(t,i,plot_id,input,j,season); AGSEC4A seed is season A only. Conditional duplicate
aggregation and null-key loss remain. New labels can expose previously conflated
seed groups; account for any such conflict, do not invent source identifiers.

## §5 Reconciliation / reuse
Read 7631c13 and 8f75a4d source and ledger from supplied complete source.git.
Their broader D1/D2/D3 and crop-map extension are not adopted. Canonical
data_info.yml explicitly defines Uganda code zero without a value label as
unrecorded crop: reuse unknown_crop for that case; all other unmapped recorded
codes pass through. This supersedes the initial overly broad passthrough reading.
Extend only the public helper using branch provenance and original raw crop.
Retain literal labels and nonintegral codes; preserve mapped native names.

## §6 Residuals
#829 WAITING, #647 PART additivity and 2019 panel-weight questions remain separate.
No scientific acceptance inferred from production-transform composition.

## Verification
OK (§3): final canonical zero exception and recorded-unmapped passthrough are
separate; raw answers remain available. OK (§4): all seven actual native
frames and generated Country/Feature preserve exact non-j axes, order,
payloads, masks, dtypes and typed attrs; cold equals fresh warm. Twelve
synthetic tests include nextafter/tiny-negative calibration and an explicit
lossy-legacy-group refusal. Actual source membership is independently checked
against raw seed answers. Production assembly is not independent scientific
acceptance. An initial zero-passthrough interpretation and pre-reducer ordering
experiment failed review/checks and were corrected, with retained private
witnesses. Source-observation conflict handling remains a disclosed residual.

## Public extraction
Source-only extraction onto development ed72ce10de7bb5af7d5cc19fc4f5a8ec604b39d2.
Relative to tested d378c7e6, the only lsms_library change is Ethiopia CONTENTS;
Uganda, framework, canonical schema and numeric test helper remain byte-identical.
The product helper, tests and additive Uganda disclosure are copied exactly
from private reviewed 7a8ed9bdc; no operator archives or private ancestors.
Independent source/aggregate/byte review supports this scope: review SHA256
5c50b19a12451828c11dc23ba0a0e7446c74d58650396cd7099e1ee20e37e659.
101 ordinary members and 139 closed witness hashes authenticate; no reviewer
data/API replay or frame deserialization. Conflict refusal remains explicit;
other countries and umbrella #1119 remain open. Root owns issue reconciliation.
