# Independent review commands -- Niger1091

These are the commands and private sources used for the frozen review. Do not rerun them into the frozen output root. A replay requires new physical source/output roots and root-prepared ignored metadata targets; no cache stamp/bypass or acquisition is required.

```sh
P=/local/job40255479/control/isa-backlog-20261021/review-Niger-1091
cd "$P"
# run.sh asserts cwd/PYTHONPATH/countries/config/data, mounted interpreter,
# guard + unshare -rn, credential absence and threads/build/Make=1.
bash run.sh base hash-base "$P/hash_inventory.py"
bash run.sh candidate hash-candidate "$P/hash_inventory.py"
bash run.sh candidate prepare "$P/diagnostic.py" "$P/prepare.py"
bash run.sh candidate tests-candidate "$P/diagnostic.py" "$P/test_launch.py"
bash run.sh base tests-base "$P/diagnostic.py" "$P/test_launch.py"
# Candidate114PASS; actual parent identical17tests:12FAIL5PASS0ERROR0SKIP.
for backend in default make; do
  for key in all 2011 2014 2018 2021 absent; do
    bash run.sh candidate "candidate-$backend-$key" "$P/diagnostic.py" "$P/probe.py" cold
    bash run.sh candidate "candidate-$backend-$key" "$P/diagnostic.py" "$P/probe.py" warm
  done
done
for backend in default make; do
  bash run.sh base "base-$backend-all" "$P/diagnostic.py" "$P/probe.py" cold
  bash run.sh base "base-$backend-all" "$P/diagnostic.py" "$P/probe.py" warm
done
# Oracle's initial resolution stop is retained. oracle-L1-terminal.json pins
# the separately verified six physical copies before the successful decode.
bash run.sh candidate oracle "$P/diagnostic.py" "$P/source_oracle.py"
for side in base candidate; do
  bash run.sh "$side" "$side-default-all" "$P/diagnostic.py" "$P/extra.py" cold
  bash run.sh "$side" "$side-default-all" "$P/diagnostic.py" "$P/extra.py" warm
done
bash run.sh candidate candidate-make-all "$P/diagnostic.py" "$P/make_extra.py"
# Each populated.py argument/backend uses the existing side-default-all root.
# Preserve stopped candidate Make prefix; populated_remaining.py executes ONLY
# its four unexecuted requests. Do not replay all/2011 to erase that limitation.
bash run.sh base base-default-all "$P/diagnostic.py" "$P/populated.py" default
bash run.sh base base-default-all "$P/diagnostic.py" "$P/populated.py" make
bash run.sh candidate candidate-default-all "$P/diagnostic.py" "$P/populated.py" default
bash run.sh candidate candidate-default-all "$P/diagnostic.py" "$P/populated_remaining.py"
for side in base candidate; do
  bash run.sh "$side" "$side-default-all" "$P/diagnostic.py" "$P/final_layers.py"
  bash run.sh "$side" "$side-default-all" "$P/diagnostic.py" "$P/scanner.py"
done
for backend in default make; do
  bash run.sh base "base-$backend-absent" "$P/diagnostic.py" "$P/parent_absent.py" prepare "$backend"
  bash run.sh base "base-$backend-absent" "$P/diagnostic.py" "$P/parent_absent.py" cold "$backend"
  bash run.sh base "base-$backend-absent" "$P/diagnostic.py" "$P/parent_absent.py" warm "$backend"
done
bash run.sh candidate checker "$P/diagnostic.py" "$P/check_artifacts.py"
bash run.sh candidate warningcheck "$P/diagnostic.py" "$P/warnings_scanner_check.py"
bash run.sh candidate freeze "$P/diagnostic.py" "$P/freeze_verify.py"
```

The chronological commands differed where a stop was preserved and only an unexecuted stage resumed. The immutable diagnostic JSONs, source/copy receipts and per-probe frames identify those boundaries. The direct actual-source oracle uses sanctioned get_dataframe and original label dictionaries, independently reconstructing46043 observations,30late slots and76exact quantity masks. Existing source-memory/serialized, declared-empty/populated, PopulationRecord/dict and GH1196 u differences are explicit.

Actual assertion sources: check_artifacts.py, exact.py, parent_absent.py, warnings_scanner_check.py and freeze_verify.py. Full logs/messages/frames remain private. Seven exact-comparator negative controls fail as required; no equality normalization is used. No public/Git write, broad suite, corpus build or old-root mutation is part of this review.

Signed: Sue.
