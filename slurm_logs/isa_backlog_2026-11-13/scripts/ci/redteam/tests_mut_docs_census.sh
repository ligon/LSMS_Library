#!/bin/bash
# Red team ci stream: tests on A, B, base and the A+B merge tree; mutation checks
# of both candidates' tests; mkdocs --strict on A and base; hash census.
set -uo pipefail
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
R=$BATCH/reports/ci-redteam
A=72885852cfc9ab765759c044c6b23489f94f3d7a
B=41f1a44a170cda31a523d7fddb4b7b8ec3c5a6f7
BASE=f138a3d61c203f892148592675a7ef72e160a322
source $BATCH/tools/burst-env.sh ci-redteam $A $B $BASE || exit 9
export PATH="$(dirname "$PY"):$PATH"
unset EXPECT
J=$SLURM_JOB_ID; N=$SLURM_CPUS_ON_NODE
FILES="tests/test_gh932_pr_data_validation.py tests/test_private_ci_check.py tests/test_gh932_safe_ci_failure_locations.py"
runtests() {  # runtests <dir> <data> <label>
  ( cd "$1" && PYTHONPATH=$1 LSMS_DATA_DIR=$2 "$PY" -m pytest -n "$N" --dist=loadfile -q -p no:cacheprovider -rfEsx \
      $FILES > $OUT/pytest-$3-$J.txt 2>&1; echo "pytest $3 exit $?" >> $OUT/pytest-$3-$J.txt )
  echo "== pytest $3 @ $(git -C "$1" rev-parse --short=12 HEAD)"; tail -2 $OUT/pytest-$3-$J.txt
}
runtests "$SRC_1" "$DATA_1" A
runtests "$SRC_2" "$DATA_2" B
runtests "$SRC_3" "$DATA_3" base

echo "== combined tree (git merge-tree in a job-local clone; nothing touches any worktree or the mirror)"
C=$JOBLOCAL/combined
git clone --quiet --shared --no-checkout "$MIRROR" "$C"
TREE=$(git -C "$C" merge-tree --write-tree "$A" "$B" | head -1)
echo "merge-tree exit/tree: $TREE"
M=$(git -C "$C" -c user.name=redteam -c user.email=redteam@invalid commit-tree "$TREE" -p "$A" -p "$B" -m "redteam combined A+B (job-local)")
git -C "$C" checkout --quiet --detach "$M"
echo "combined vs A:"; git -C "$C" diff --stat "$A" "$M"
echo "combined vs B:"; git -C "$C" diff --stat "$B" "$M" | tail -1
mkdir -p $JOBLOCAL/private/data-comb
ident "$C" $JOBLOCAL/private/data-comb
runtests "$C" $JOBLOCAL/private/data-comb combined

mutate() {  # mutate <clone> <file> <label> <old> <new> <test-file> [-k expr]
  local dir=$1 f=$2 lab=$3 old=$4 new=$5 tf=$6; shift 6
  git -C "$dir" checkout --quiet -- "$f"
  "$PY" -I -c 'import sys; p, o, n = sys.argv[1:4]; s = open(p).read(); assert s.count(o) == 1, ("anchor", s.count(o), o); open(p, "w").write(s.replace(o, n, 1))' \
      "$dir/$f" "$old" "$new" || { echo "MUTANT $lab: anchor not found"; return; }
  ( cd "$dir" && PYTHONPATH=$dir "$PY" -m pytest -q -p no:cacheprovider "$tf" "$@" > $JOBLOCAL/mut-$lab.txt 2>&1 )
  local rc=$?
  echo "MUTANT $lab: pytest exit $rc ($( [ $rc -ne 0 ] && echo CAUGHT || echo 'NOT CAUGHT')): $(tail -1 $JOBLOCAL/mut-$lab.txt)"
  git -C "$dir" checkout --quiet -- "$f"
}
echo "== mutation checks, B (tests/test_gh932_pr_data_validation.py)"
MB=$JOBLOCAL/mutB; git clone --quiet --shared --no-checkout "$MIRROR" "$MB"; git -C "$MB" checkout --quiet --detach "$B"
WF=.github/workflows/pr-data-validation.yml; TB=tests/test_gh932_pr_data_validation.py
mutate "$MB" $WF B1-readd-edited-trigger "types: [opened, synchronize, reopened, ready_for_review, converted_to_draft]" "types: [opened, synchronize, reopened, ready_for_review, converted_to_draft, edited]" $TB
mutate "$MB" $WF B2-drop-always "if: \${{ always() }}" "if: \${{ needs.classify.result == 'success' }}" $TB
mutate "$MB" $WF B3-conditional-test-step "      - name: Run full test suite
" "      - name: Run full test suite
        if: github.event.pull_request.draft == false
" $TB
mutate "$MB" $WF B4-classifier-accepts-edited "'converted_to_draft'}:" "'converted_to_draft', 'edited'}:" $TB
mutate "$MB" $WF B5-guard-accepts-metadata "mode != 'validate'" "mode not in {'validate', 'metadata'}" $TB
mutate "$MB" $WF B6-dynamic-name "    name: pr-data-validation
" "    name: \${{ needs.classify.result == 'success' && 'pr-data-validation' || 'pr-data-refused' }}
" $TB
mutate "$MB" $WF B7-guard-main-swallows "                  raise SystemExit(str(error))
              print('Eligible" "                  print(str(error))
              print('Eligible" $TB
mutate "$MB" $WF B8-classifier-main-swallows "raise SystemExit('PR data request refused before checkout: ' + str(error))" "print('refused: ' + str(error)); raise SystemExit(0)" $TB
mutate "$MB" $WF B9-any-base "base.get('ref') != 'development'" "base.get('ref') not in ('development', 'master')" $TB
mutate "$MB" $WF B10-draft-allowed "if pr.get('draft') is not False:" "if pr.get('draft') is None:" $TB

echo "== mutation checks, A (tests/test_private_ci_check.py -k pin)"
MA=$JOBLOCAL/mutA; git clone --quiet --shared --no-checkout "$MIRROR" "$MA"; git -C "$MA" checkout --quiet --detach "$A"
PC=scripts/private_ci_check.py; TA=tests/test_private_ci_check.py
mutate "$MA" $PC A1-no-stage-restriction "if args.public_pins and stage != _PIN_STAGE:" "if False:" $TA -k pin
mutate "$MA" $PC A2-ambiguous-accepted "len(found[spec]) == 1" "len(found[spec]) >= 1" $TA -k pin
mutate "$MA" $PC A3-search-not-fullmatch "match = _PIN_RESULT.fullmatch(line)" "match = _PIN_RESULT.search(line)" $TA -k pin
mutate "$MA" $PC A4-unterminated-kept 'if not raw.endswith(b"\n"):' 'if len(raw) >= _MAX_PIN_LINE:' $TA -k pin
mutate "$MA" $PC A5-loose-digest "digest=(?P<digest>[0-9a-f]{12})" "digest=(?P<digest>\\S{1,64})" $TA -k pin
mutate "$MA" $PC A6-loose-expect "or not _PIN_ROWS.fullmatch(rows) or not _PIN_DIGEST.fullmatch(digest)):" "):" $TA -k pin
mutate "$MA" $PC A7-print-even-if-unusable "    if pins is None:
        return None
    found" "    if pins is None:
        pins = {'Uganda:shocks': (0, '000000000000')}
    found" $TA -k pin

echo "== synthetic: is the 48-bit digest additive (one added row -> that row's hash)?"
( cd "$SRC_1" && "$PY" - <<'EOF'
import numpy as np, pandas as pd
rng = np.random.default_rng(0)
df = pd.DataFrame({c: rng.integers(0, 50, 500).astype(str) for c in "abcdefghij"})
row = df.iloc[[0]].copy(); row["a"] = "999"
M = (1 << 48) - 1
def dig(x):  # windows_build_check.py:81-83
    x = x[sorted(x.columns)].sort_values(sorted(x.columns), kind="mergesort").reset_index(drop=True)
    return int(pd.util.hash_pandas_object(x.astype(str), index=False).sum()) & M
d0, d1, hr = dig(df), dig(pd.concat([df, row])), dig(row)
print("digest(T+r) - digest(T) == digest(r) mod 2^48:", (d1 - d0) & M == hr)
EOF
)

echo "== mkdocs build --strict (as ci.yml docs job), A and base"
MK=$JOBLOCAL/mkdocs-pkgs
"$PY" -m pip install --quiet --target "$MK" mkdocs-material 'mkdocstrings[python]' > $OUT/mkdocs-pip-$J.txt 2>&1 \
  && echo "mkdocs installed: $(PYTHONPATH=$MK "$PY" -c 'import mkdocs, material; print(mkdocs.__version__, material.__version__)' 2>&1 | tail -1)" \
  || echo "mkdocs install FAILED"
for pair in "A:$SRC_1" "base:$SRC_3"; do
  lab=${pair%%:*}; dir=${pair#*:}
  ( cd "$dir" && PYTHONPATH=$MK:$dir LSMS_SKIP_AUTH=1 LSMS_DATA_DIR=$DATA_1 "$PY" -m mkdocs build --strict \
      -d $JOBLOCAL/site-$lab > $OUT/mkdocs-$lab-$J.txt 2>&1; echo "mkdocs $lab exit $?" >> $OUT/mkdocs-$lab-$J.txt )
  echo "-- $lab:"; grep -E 'WARNING|ERROR|exit|not included|v0.17' $OUT/mkdocs-$lab-$J.txt | head -20
done
ls -la $JOBLOCAL/site-A/releases/v0.17.0/index.html 2>&1
grep -o 'releases/v0.17.0/' $JOBLOCAL/site-A/index.html 2>/dev/null | head -1 | sed 's/^/nav link on home page: /'

echo "== hash census base -> A, base -> B"
for i in 1 2 3; do
  s=SRC_$i; d=DATA_$i
  ( cd "${!s}" && PYTHONPATH=${!s} LSMS_DATA_DIR=${!d} "$PY" $BATCH/tools/hash_census.py $OUT/census-$i-$J.json ) \
    > $OUT/census-$i-$J.log 2>&1; tail -1 $OUT/census-$i-$J.log
done
"$PY" $BATCH/tools/hashdiff.py $OUT/census-3-$J.json $OUT/census-1-$J.json $OUT/hashdiff-base-A-$J.json
"$PY" $BATCH/tools/hashdiff.py $OUT/census-3-$J.json $OUT/census-2-$J.json $OUT/hashdiff-base-B-$J.json
echo "in-tree parquets: $(find $SRC_1/lsms_library/countries $SRC_2/lsms_library/countries $SRC_3/lsms_library/countries -name '*.parquet' | wc -l)"
