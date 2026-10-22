This supersedes the helper recommendation in NEXT-SECOND-CRITIC-BRIEF.md.
Do NOT reuse fast-v2. Root's guarded synthetic proof at
../comparator-object-dtype-root/ROOT-SYNTHETIC-PROOF.json shows that pandas'
object EA dtype is NumpyEADtype, not np.dtype; the fast-v2 np.dtype condition
therefore skipped all typed object-scalar recursion. No real country mismatch
has been inferred from that synthetic failure.

Use exact_strict_fast_v3.py only after source pinning, review and fresh guarded
calibration in the critic's own root. It skips typed scalar iteration ONLY
for pd.StringDtype and preserves every other kind-O loop. The 42 controls in
calibrate-object-v3.py cover the original12 plus30 object-type combinations
across values,row/column axes,nested Series/DataFrame and scalar attrs.
All earlier helper versions and frozen releases are unchanged. The author
is preparing a separate corrected-object-v3 saved-artifact supplement for
every actual fast-v2 use. Original operand/memory/log gaps remain.

The proposed twelve-PR split still requires root's explicit assignment and
separate exact source/private metadata. This correction authorizes no API,
source/test replay,old-root write or GitHub action. Sue.
