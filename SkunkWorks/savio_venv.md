# Savio node-local venv: squashfs image (opt-in fast path)

This documents the **opt-in** single-file venv mechanism on Savio. The
**default** recovery recipe is still the adopt-or-build tar-pipe in
`.venv.lustre/README_WHY_THIS_EXISTS.md` (summarized in `CLAUDE.md`); this
squashfs path is a faster, MDS-friendlier alternative driven by
`bin/savio_venv.sh`.

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

| Path | Role | Writable? |
|------|------|-----------|
| `.venv.lustre/` | canonical **master** venv on Lustre | yes — install deps here |
| `.venv.sqfs` | read-only **image** built from the master (~255 MB, 1 inode) | no |
| `.venv` → `/local/jobNNN/venv_img` | per-job **mount** of the image | no (read-only) |

## Update the venv (when deps change)

Because the mounted venv is **read-only**, you can't `pip install` into it.
Update the writable master, then regenerate the image:

```bash
# install/upgrade deps into the master AND re-image in one step:
bin/savio_venv.sh update "somepkg==1.2.3"        # any pip args
# or just re-image after editing the master yourself:
bin/savio_venv.sh update
```

`update` (1) optionally `pip install`s its args into `.venv.lustre`, (2)
verifies the master imports `lsms_library`, (3) `mksquashfs … -no-xattrs` to a
temp file, (4) atomically `mv`s it over `.venv.sqfs`. Jobs already running keep
their old mount (old inode) until they re-mount; **new** `mount` calls pick up
the new image.

> For a dependency change that must reach `pyproject.toml`/`poetry.lock`, edit
> those and `poetry install` into the master as usual, then run
> `bin/savio_venv.sh update` to re-image. The image is a build artifact — it is
> never committed (gitignored); `pyproject.toml`/`poetry.lock` remain the
> tracked source of truth for what the venv contains.

## Use the venv in a job

```bash
bin/savio_venv.sh mount      # mounts .venv.sqfs and points .venv at it
.venv/bin/python -m pytest …  # invoke via `python -m` (see note)
bin/savio_venv.sh umount     # optional; the job's /local is reaped at job end anyway
```

`mount` mounts at `/local/$SLURM_JOB_ID/venv_img` (deliberately distinct from
the adopt-or-build tar-pipe's `/local/$SLURM_JOB_ID/venv`, so the two paths
never collide) and repoints the `.venv` symlink there.

Always invoke as `.venv/bin/python -m <module>`: console-script shebangs
(`bin/pytest`) point at the stable `.venv` symlink, and `pyvenv.cfg`'s `home =`
points at the base system python, so going through the interpreter is robust
regardless of the mount path.

## How the mount works (and the one wrinkle)

- The mounter is apptainer's bundled `/usr/libexec/apptainer/bin/squashfuse_ll`
  (no separate install). It's built against **libfuse3** and execs
  `fusermount3`, but this host only ships the **setuid libfuse2** `fusermount`.
  The fuse2/fuse3 mount handshake is compatible, so the script drops a
  `fusermount3 → /usr/bin/fusermount` **shim** on `PATH`. No admin/root needed
  (`/dev/fuse` is world-RW; `fusermount` is setuid-root).
- **Fallbacks** (handled by the script): if FUSE is unavailable, it extracts the
  image with `unsquashfs -d` (still one Lustre inode read). If `.venv.sqfs` is
  missing entirely, fall back to the default adopt-or-build tar-pipe.

## Requirements / assumptions

- `apptainer` present (for `squashfuse_ll`), `/dev/fuse` readable/writable, and
  a setuid `fusermount`. All true on Savio savio2/savio3 as of 2026-06.
- `mksquashfs` / `unsquashfs` on `PATH` (present on Savio).
- `mksquashfs` here lacks `zstd`; the script uses `gzip`.

## Endgame (the real fix for cross-node persistence)

Even this still rebuilds/mounts per node (the image lives on Lustre; the mount
is per job). True persistent node-local state would need HPC support to
provision a per-user dir excluded from the `spank_private_tmpshm` reaper — file
a ticket if the churn ever warrants it.
