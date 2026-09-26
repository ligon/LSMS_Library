import pathlib, sys
waves = sorted(p.read_text(encoding="utf-8").strip() for p in pathlib.Path("..").glob("*/_/things.parquet"))
pathlib.Path(sys.argv[1]).write_text(",".join(waves), encoding="utf-8")
print("country ->", sys.argv[1], waves)
