import pathlib, sys
out = pathlib.Path(__file__).with_suffix(".parquet")
out.write_text(f"{pathlib.Path.cwd().parent.name}\n", encoding="utf-8")
print("wrote", out, "with", sys.executable)
