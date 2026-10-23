#!/bin/sh
# GH #856 step 2 -- how the evidence in this directory was produced (2026-10-22).
# One process at a time, single-threaded; each checkout gets its own private
# LSMS_DATA_DIR whose dvc-cache is a symlink to the shared L1 blob cache.
PY=/local/job40390255/venv_img/bin/python
ENV="OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1"
BASE=/local/job40390255/worktrees/base-856        # detached at development 6ffeb3caf
BR=/local/job40390255/worktrees/856-clean         # fix/856-malawi-unit-text-20261022
BASEDEV=/local/job40390255/worktrees/base-856-dev # detached at 9a0ed5f3c, the branch's parent (hash census only)
D=$BR/slurm_logs/isa_backlog_2026-10-21/856-harvest-unit-text
P=/local/job40390255/data-private

# 1. cold builds of Country('Malawi').crop_production() + harvest_kg_factors
(cd $BASE && env $ENV PYTHONPATH=$BASE LSMS_DATA_DIR=$P/base-856 $PY $D/build_and_measure.py $BASE $P/base-856/compare-base)
#    harvest-only state (old branch, commit 8b637dee4) and the pre-ruling head
#    (old branch ab5da2d5a) were built into $P/lane-856/compare-A and
#    compare-final; the bamboo-ruling head bf29f212a into compare-bamboo
#    (root-bamboo); the current head (bamboo + unspecified-size + bag typos):
(cd $BR && env $ENV PYTHONPATH=$BR LSMS_DATA_DIR=$P/lane-856/root-r2 $PY $D/build_and_measure.py $BR $P/lane-856/compare-r2)
cp $P/lane-856/compare-r2/summary.json $D/summary_final.json   # warning text is cut before any i= listing

# 2. row-level comparison (pure pandas over the parquets above)
$PY -I $D/compare.py $P/base-856/compare-base $P/lane-856/compare-A $P/856-private/diff_base_harvest_only.json
$PY -I $D/compare.py $P/base-856/compare-base $P/lane-856/compare-final $P/856-private/diff_base_final.json
$PY -I $D/compare.py $P/base-856/compare-base $P/lane-856/compare-bamboo $P/856-private/diff_base_bamboo.json
$PY -I $D/compare.py $P/base-856/compare-base $P/lane-856/compare-r2 $P/856-private/diff_base_r2.json

# 3. raw texts behind every sale whose attachment moved; recovered / left strings
(cd $BR && env $ENV PYTHONPATH=$BR LSMS_DATA_DIR=$P/lane-856 $PY $D/detached_texts.py $P/856-private/diff_base_r2.json $P/856-private/moved_sale_texts_r2.json)

# 3b. sale-basis ladder replayed per wave (aggregate tallies the pinned
#     constants describe), on the branch's parent and on this head
(cd $BASEDEV && env $ENV PYTHONPATH=$BASEDEV LSMS_DATA_DIR=$P/base-856 $PY $D/ladder_tallies.py $BASEDEV $D/ladder_tallies_base.json)
(cd $BR && env $ENV PYTHONPATH=$BR LSMS_DATA_DIR=$P/lane-856 $PY $D/ladder_tallies.py $BR $D/ladder_tallies_branch.json)
#     (the quantity-screen and per-sale ladder comparisons behind the
#     CONTENTS.org caveats print row-level detail, so their scripts live in
#     $P/856-private/: qaudit.py, dec_diff.py)
(cd $BR && env $ENV PYTHONPATH=$BR LSMS_DATA_DIR=$P/lane-856 $PY $D/strings_856.py $D/strings_856.json)

# 4. cache-hash census on both checkouts, then the diff
HC=/local/job40390255/worktrees/1218/slurm_logs/isa_backlog_2026-10-21/1218-current-validation/hash_census.py
#    (against the branch's own parent, so development's later Mali change is not counted)
(cd $BASEDEV && env $ENV PYTHONPATH=$BASEDEV LSMS_DATA_DIR=$P/base-856 $PY $HC $BASEDEV $P/base-856/hash_base_dev.json)
(cd $BR && env $ENV PYTHONPATH=$BR LSMS_DATA_DIR=$P/lane-856 $PY $HC $BR $P/lane-856/hash_clean.json)
$PY -I $D/hash_diff.py $P/base-856/hash_base_dev.json $P/lane-856/hash_clean.json $D/hash_diff.json
