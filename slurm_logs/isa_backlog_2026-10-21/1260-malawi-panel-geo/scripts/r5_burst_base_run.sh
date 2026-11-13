set -u
git -C "$CLONE" checkout -q e0b1485ae && echo "base head: $(git -C "$CLONE" log --oneline -1)"
"$PY" -c 'import lsms_library,os; assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(os.environ["CLONE"])+os.sep)'
[ -s $JOBDIR/hash.json ] || "$PY" $S/r5_hash_census.py "$CLONE" $JOBDIR/hash.json > $JOBDIR/hash.log 2>&1
"$PY" $S/r5_build_all.py $JOBDIR/out > $JOBDIR/build_stdout.log 2>&1
tail -3 $JOBDIR/out/build.log
