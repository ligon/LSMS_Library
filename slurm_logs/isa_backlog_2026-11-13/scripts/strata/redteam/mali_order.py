"""Does Mali sample(waves=['2014-15']) carry a `strata` column?  Depends on call order?
Run in a fresh, empty LSMS_DATA_DIR.  usage: mali_order.py"""
import subprocess
import warnings

warnings.simplefilter("ignore")
import lsms_library as ll  # noqa: E402

print("head", subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip())
for step, waves in (("1 cold, one wave first", ["2014-15"]),
                    ("2 all waves", None),
                    ("3 one wave again, after the all-wave build", ["2014-15"]),
                    ("4 two waves 2014-15 + 2017-18", ["2014-15", "2017-18"])):
    s = ll.Country("Mali").sample(waves=waves) if waves else ll.Country("Mali").sample()
    flat = s.reset_index()
    has = "strata" in flat.columns
    print(f"{step}: rows={len(flat)} strata_column={has} "
          f"non_null={int(flat['strata'].notna().sum()) if has else None} "
          f"columns={sorted(map(str, flat.columns))}")
