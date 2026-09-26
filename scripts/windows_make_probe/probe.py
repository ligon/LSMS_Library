"""Probe: can GNU make + Git-for-Windows' sh drive a country-shaped Makefile,
invoked from Python the way Country.run_make_target does?  Prints a table of
variants; never fails the job (it is a measurement)."""
import os, shutil, subprocess, sys, tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
make = shutil.which("make") or shutil.which("mingw32-make")
git = shutil.which("git")
git_usr_bin = Path(git).resolve().parents[1] / "usr" / "bin" if git else None
print("python     :", sys.executable)
print("make       :", make)
print("git        :", git, "-> usr/bin", git_usr_bin, git_usr_bin and git_usr_bin.exists())
print("sh on PATH :", shutil.which("sh"))
print("find on PATH:", shutil.which("find"))
print("HOME set   :", "HOME" in os.environ)
if make:
    print(subprocess.run([make, "--version"], capture_output=True, text=True).stdout.splitlines()[0])

def attempt(label, data_root, target_style, shell_mode):
    work = Path(tempfile.mkdtemp(prefix="mkprobe"))
    shutil.copytree(HERE / "Fakeland", work / "Fakeland")
    env = os.environ.copy()
    env["PATH"] = os.path.dirname(sys.executable) + os.pathsep + env["PATH"]
    cmd = [make, "-s"]
    if shell_mode in ("git-sh", "git-sh+path") and git_usr_bin:
        cmd.append(f"SHELL={(git_usr_bin / 'sh.exe').as_posix()}")
    if shell_mode == "git-sh+path" and git_usr_bin:
        env["PATH"] = os.path.dirname(sys.executable) + os.pathsep + str(git_usr_bin) + os.pathsep + env["PATH"]
    dr = Path(data_root)
    env["LSMS_DATA_DIR"] = dr.as_posix() if target_style == "posix" else str(dr)
    target = dr / "Fakeland" / "var" / "things.parquet"
    cmd.append(target.as_posix() if target_style == "posix" else str(target))
    try:
        r = subprocess.run(cmd, cwd=work / "Fakeland" / "_", env=env, capture_output=True, text=True, timeout=120)
        ok = target.exists() and target.read_text(encoding="utf-8") == "2019-20,2021-22"
        msg = (r.stdout + r.stderr).strip().replace("\n", " | ")[:300]
        print(f"{'PASS' if ok else 'FAIL'}  {label:55s} rc={r.returncode}  {msg}")
    except Exception as e:  # noqa: BLE001
        print(f"FAIL  {label:55s} {type(e).__name__}: {e}")

if not make:
    print("NO MAKE ON PATH -- stopping"); sys.exit(0)
base = Path(tempfile.mkdtemp(prefix="lsmsdata"))
spaced = Path(tempfile.mkdtemp(prefix="lsms data "))
modes = ["default"] + (["git-sh", "git-sh+path"] if git_usr_bin and git_usr_bin.exists() else [])
for shell_mode in modes:
    for style in ("native", "posix"):
        attempt(f"shell={shell_mode} paths={style}", base / shell_mode / style, style, shell_mode)
    attempt(f"shell={shell_mode} paths=posix root-with-SPACE", spaced / shell_mode, "posix", shell_mode)
