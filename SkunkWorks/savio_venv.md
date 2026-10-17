# Savio node-local venv: squashfs image (the default)

This documents **the** venv mechanism on Savio, driven by `bin/savio_venv.sh`.
It was opt-in until 2026-10 (GH #960); the adopt-or-build tar-pipe in
`.venv.lustre/README_WHY_THIS_EXISTS.md` used to be the default and is now a
last-resort fallback only, because `/local/jobNNN` is reaped at job end (see
"Why") and so there is never a venv on the node to adopt.

This whole page is **Savio-specific**. On login nodes, laptops, and non-HPC
clusters use a normal in-tree `.venv/`.

## Why

The venv is ~926 MB / ~33k tiny files. Two problems on Savio:

1. **Lustre MDS load.** Reading 33k files off Lustre on every import/pytest
   hammers the metadata server (a shared-cluster social cost). The fix is to
   keep the venv on node-local SSD, but…
2. **Node-local scratch is per-job-ephemeral.** `/local/jobNNN` is created and
   **reaped** per job by the SPANK plugin `spank_private_tmpshm.so` (runs as
   root; `/local` itself is `root:root`, so we can't make a stable path there).
   So a venv built under `/local/jobNNN` is deleted when the job ends.

A squashfs image solves both: it's **one file** on Lustre (one inode, no MDS
storm) that we **mount fresh per job** onto node-local — no extraction, tiny
node-local footprint, and nothing to survive the reaper.

## The three artifacts (all gitignored)

| Path | Role | Scope | Writable? |
|------|------|-------|-----------|
| `SHARE/.venv.lustre/` | canonical **master** venv on Lustre | global | yes — via `update` only |
| `SHARE/.venv.sqfs` | read-only **image** built from the master (~230 MB, 1 inode) | global | no |
| `SHARE/.venv.sqfs.prev` | the image `update` replaced — the rollback | global | no |
| `/local/jobNNN/venv_img` | this job's **mount** of the image | per job | no (read-only) |
| `<clone>/.venv` → the mount | convenience symlink | **per clone** | — |

`SHARE` is the shared mirror (`~/mirrors/LSMS_Library`).  The script finds it
from wherever it is run: `$LSMS_VENV_SHARE` if set, else the checkout itself if
it holds `.venv.sqfs`, else the checkout's `origin` if that is a local directory
holding one — which is what a sucoder `/local` clone has (GH #958).

**The `.venv` symlink is per-clone state and is never written in `SHARE`.** It
used to be: `mount` repointed the mirror's `.venv`, so with jobs on N nodes the
last writer won and the other N−1 dangled.  Run from the mirror, `mount` now
leaves `.venv` alone and prints the mount path to use directly.

**The master carries no editable install of `lsms_library`** (GH #957), so the
image is the dependency closure and nothing else; a checkout is selected with
`PYTHONPATH=<checkout>`.  `update` refuses to image a master that has
reacquired one (`poetry install` without `--no-root` does exactly that).

## Update the venv (when deps change)

Because the mounted venv is **read-only**, you can't `pip install` into it.
Update the writable master, then regenerate the image:

```bash
# install/upgrade deps into the master AND re-image in one step:
bin/savio_venv.sh update "somepkg==1.2.3"        # any pip args
# or just re-image after editing the master yourself:
bin/savio_venv.sh update
```

`update` holds an exclusive `flock` on `SHARE/.venv.lock` throughout (the
master is shared Lustre state; `flock` works there, not under NFS `$HOME`), then
(1) optionally `pip install`s its args into `.venv.lustre`, (2) refuses a master
carrying an editable install, (3) verifies the master imports `lsms_library`
from the checkout the script lives in (`PYTHONPATH`, `PYTHONSAFEPATH`), (4)
rewrites console-script shebangs to the self-locating form (below), (5) stamps
`VENV_GENERATION` into the master, (6) `mksquashfs … -no-xattrs` to a temp file,
(7) hardlinks the outgoing image to `.venv.sqfs.prev` and atomically `mv`s the
new one over `.venv.sqfs`. Jobs already running keep their old mount (old inode)
until they re-mount; **new** `mount` calls pick up the new image.

**Two nodes can therefore run different generations at once**, legitimately.
`mount` prints the generation it mounted and `bin/savio_venv.sh status` shows
both the image's and this job's mount's.  `VENV_GENERATION` records the UTC
build time, host, the checkout and commit it was built from, and the sha256 of
`poetry.lock` and of `pip freeze`.

Rollback: `mv SHARE/.venv.sqfs.prev SHARE/.venv.sqfs` (then re-mount).

> For a dependency change that must reach `pyproject.toml`/`poetry.lock`, edit
> those and `poetry install` into the master as usual, then run
> `bin/savio_venv.sh update` to re-image. The image is a build artifact — it is
> never committed (gitignored); `pyproject.toml`/`poetry.lock` remain the
> tracked source of truth for what the venv contains.

## Use the venv in a job

```bash
bin/savio_venv.sh mount      # mounts the image, verifies it, points THIS clone's .venv at it
export PYTHONPATH="$PWD"     # select this checkout; nothing else will (GH #957)
.venv/bin/python -m pytest …  # or .venv/bin/pytest -- both work (see below)
bin/savio_venv.sh status     # image + mount generation
bin/savio_venv.sh umount     # optional; the job's /local is reaped at job end anyway
```

`mount` mounts at `/local/$SLURM_JOB_ID/venv_img` (deliberately distinct from
the adopt-or-build tar-pipe's `/local/$SLURM_JOB_ID/venv`, so the two paths
never collide).  It verifies the import *through the mount path* before
touching any symlink.

**Console scripts are self-locating** (GH #958 defect 5).  pip writes an
absolute shebang naming the interpreter it ran under, so the image's
`bin/pytest` and friends used to name either the mirror's `.venv` (dangling on
every node but the last to mount) or the Lustre master's own python (which
*works*, silently, importing every package over Lustre from whatever the master
holds today rather than from the mounted image).  `update` rewrites every one
to the trampoline pip itself uses for relocatable scripts:

```sh
#!/bin/sh
'''exec' "$(dirname -- "$(realpath -- "$0")")"/python "$0" "$@"
' '''
```

which execs the `python` beside the script, so a script runs under whichever
mount it was invoked through.  `.venv/bin/python -m <module>` remains fine.

### Dispatching a job to another node

The burst job cannot see this node's `/local` clone.  Use the same recipe an
interactive node runs — clone the branch into the job's own `/local`, mount
the image from there, select the clone:

```bash
git clone --branch "$BRANCH" ~/mirrors/LSMS_Library "/local/job$SLURM_JOB_ID/mirrors/LSMS_Library"
cd "/local/job$SLURM_JOB_ID/mirrors/LSMS_Library"
bin/savio_venv.sh mount && export PYTHONPATH="$PWD"
```

Do **not** point a job at the mirror's working tree for anything but
`development`: pushing a branch does not check it out, so the mirror's tree
stays on `development` and the job tests that while believing it tests your
branch.  And do not `cp -a .venv.lustre` into the job (the README's old prolog):
that is a ~33k-file Lustre metadata storm.

## How the mount works (and the one wrinkle)

- The mounter is apptainer's bundled `/usr/libexec/apptainer/bin/squashfuse_ll`
  (no separate install). It's built against **libfuse3** and execs
  `fusermount3`, but this host only ships the **setuid libfuse2** `fusermount`.
  The fuse2/fuse3 mount handshake is compatible, so the script drops a
  `fusermount3 → /usr/bin/fusermount` **shim** on `PATH`. No admin/root needed
  (`/dev/fuse` is world-RW; `fusermount` is setuid-root).
- **Fallbacks** (handled by the script): if FUSE is unavailable, it extracts the
  image with `unsquashfs -d` (still one Lustre inode read). Only if `.venv.sqfs`
  is missing entirely, fall back to the adopt-or-build tar-pipe — whose ADOPT
  step will find nothing, since `/local` job directories are reaped.

## Requirements / assumptions

- `apptainer` present (for `squashfuse_ll`), `/dev/fuse` readable/writable, and
  a setuid `fusermount`. All true on Savio savio2/savio3 as of 2026-06, on
  savio4 (n0035, 2026-09-21) and on savio3 again (n0019, 2026-10-17).
- `mksquashfs` / `unsquashfs` on `PATH` (present on Savio).
- `mksquashfs` here lacks `zstd`; the script uses `gzip`.
- `squashfs-tools` is 4.3, which has no `unsquashfs -cat`; `status` extracts the
  one `VENV_GENERATION` file to a temp directory instead.

## Endgame (the real fix for cross-node persistence)

Even this still rebuilds/mounts per node (the image lives on Lustre; the mount
is per job). True persistent node-local state would need HPC support to
provision a per-user dir excluded from the `spank_private_tmpshm` reaper — file
a ticket if the churn ever warrants it.
