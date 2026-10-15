One initial read-only CotedIvoire parquet comparison used the designated venv
but omitted taskset and explicit BLAS pinning. It did not call a Country API or
write any cache/source. The confirming sorted-frame and row-multiset comparison
was rerun with taskset 0-7 and BLAS/OMP/MKL=1; results are in order-comparison.json.
All corpus builds, primary shared/private snapshot reads, and planned Feature
builds use the pinned launcher. No shared L2 mutation or source edit occurred.
