set -u
echo "head: $(git -C "$CLONE" log --oneline -1)"
T="tests/test_gh1260_malawi_panel_geo.py tests/test_gh323_malawi_cluster_key.py"
run() { name=$1; dd=$2; shift 2
  mkdir -p $dd; [ -e $dd/dvc-cache ] || ln -s /global/scratch/fsa/fc_jevons/ligon/cache/lsms_library/dvc-cache $dd/dvc-cache
  LSMS_DATA_DIR=$dd "$PY" -m pytest -p no:cacheprovider -n $NCPU --dist=loadfile -rfEsxX "$@" $T > $JOBDIR/pytest_$name.txt 2>&1
  echo "$name: $(tail -1 $JOBDIR/pytest_$name.txt)"; }
run clean $JOBDIR/data-clean -q
"$PY" $S/r5_mutate.py lsms_library/countries/Malawi/_/malawi.py "    df.loc[panel & ~at_fix, coords] = np.nan\n" "    df.loc[panel & ~at_fix, ['Latitude']] = np.nan\n"
run mutA_short $JOBDIR/data-mutA --tb=short
run mutA_long $JOBDIR/data-mutA
git -C "$CLONE" checkout -- .
"$PY" $S/r5_mutate.py lsms_library/countries/Malawi/2013-14/_/mapping.py "def cluster_features(df):\n" "def _cluster_features_off(df):\n"
run mutB_short $JOBDIR/data-mutB --tb=short
run mutB_long $JOBDIR/data-mutB
git -C "$CLONE" checkout -- .
git -C "$CLONE" status --short
"$PY" $S/r5_leak_scan.py $JOBDIR/leak_lines_PRIVATE.txt $JOBDIR/pytest_*.txt > $JOBDIR/leak_scan.txt
cat $JOBDIR/leak_scan.txt
