set -u
# Mutation testing on the branch head: each mutant in its own worktree and own
# LSMS_DATA_DIR; crop_production pre-built, then the two test files +
# test_derivations.py.  Adapted from the red-team review (run_mut.sh).
S=$JOBDIR/scripts; MO=$JOBDIR/mut-$(git rev-parse --short HEAD); mkdir -p $MO
L1=/global/scratch/fsa/fc_jevons/ligon/cache/lsms_library/dvc-cache
MUTS="M0_control A_rescale_both A2_rescale_value A3_rescale_qty B_drop_sale_key B2_drop_harvest_key_on_sold B3_unsorted_join C_sale_key_all_trees C2_sale_key_on_zero D_inputs_drop_unit E_inputs_all_roster F_yml_drop_value_col G_unmask_value_code H_blank_edge_sales I_unmask_field_code J_unmask_tree_code K_inputs_drop_2017_unit L_harvest_inputs_drop_unit"
run_one() {
  m=$1; W=$TMPDIR/mut_$m; D=$TMPDIR/data-mut-$m
  git -C "$CLONE" worktree add -q --detach "$W" HEAD || { echo "$m worktree failed"; return; }
  if [ "$m" != M0_control ]; then "$PY" $S/mutate.py "$W" "$m" > $MO/$m.apply.txt 2>&1 || { echo "$m APPLY FAILED"; cat $MO/$m.apply.txt; return; }; fi
  (cd "$W" && git diff --stat > $MO/$m.diffstat.txt)
  mkdir -p "$D"; [ -e "$D/dvc-cache" ] || ln -s "$L1" "$D/dvc-cache"
  (
    cd "$W"
    export PYTHONPATH="$W" LSMS_COUNTRIES_ROOT="$W/lsms_library/countries" LSMS_DATA_DIR="$D"
    "$PY" -c '
import lsms_library, os, sys
w = os.path.realpath(os.environ["PYTHONPATH"]) + os.sep
sys.exit(0 if os.path.realpath(lsms_library.__file__).startswith(w) else "identity: %s" % lsms_library.__file__)' || { echo "$m IDENTITY FAILED"; exit 3; }
    "$PY" -c '
import warnings; warnings.simplefilter("ignore")
import lsms_library as ll
df = ll.Country("Mali").crop_production(); print("prebuilt", df.shape)
df.reset_index()[["i"]].astype({"i": "string"}).to_parquet("'$MO'/'$m'.ids.parquet")' > $MO/$m.prebuild.txt 2>&1
    "$PY" -m pytest -n 2 --dist=loadfile -p no:cacheprovider -rfE \
      tests/test_gh1267_mali_tree_quantity.py tests/test_gh824_mali_unit_sold.py tests/test_derivations.py \
      > $MO/$m.pytest.txt 2>&1
    echo "$m pytest rc=$? :: $(tail -1 $MO/$m.pytest.txt)"
  )
}
i=0
for m in $MUTS; do
  run_one $m &
  i=$((i+1)); if [ $((i % 8)) -eq 0 ]; then wait; fi
done
wait
echo "== id leak check $(date)"
"$PY" $S/idleak.py $MO/M0_control.ids.parquet $MO > $MO/idleak.txt 2>&1; cat $MO/idleak.txt
rm -f $MO/*.ids.parquet
echo "== done $(date)"
