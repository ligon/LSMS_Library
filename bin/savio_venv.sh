#!/bin/bash
# savio_venv.sh — manage the single-file (squashfs) node-local venv on Savio.
#
# Model (see SkunkWorks/savio_venv.md):
#   .venv.lustre/   canonical WRITABLE venv on Lustre (master; gitignored)
#   .venv.sqfs      read-only single-file image built from the master (gitignored,
#                   ~255 MB, ONE Lustre inode instead of ~33k -> no MDS storm)
#   .venv -> mount  per-job symlink to a squashfuse mount of .venv.sqfs
#
# This is the OPT-IN fast path. The default recovery recipe remains the
# adopt-or-build tar-pipe in .venv.lustre/README_WHY_THIS_EXISTS.md.
#
# Subcommands:
#   update [pip-args...]   (re)build .venv.sqfs from .venv.lustre. With pip-args,
#                          installs them into the master first, then re-images.
#   mount                  mount .venv.sqfs at /local/$SLURM_JOB_ID/venv and point
#                          .venv at it (FUSE; falls back to unsquashfs extract).
#   umount                 unmount this job's mount.
set -uo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MASTER="$REPO/.venv.lustre"
IMG="$REPO/.venv.sqfs"
SQUASHFUSE="/usr/libexec/apptainer/bin/squashfuse_ll"   # apptainer-bundled (fuse3)

die() { echo "savio_venv: $*" >&2; exit 1; }

cmd_update() {
    [ -d "$MASTER" ] || die "no writable master at $MASTER (build one first; see SkunkWorks/savio_venv.md)"
    if [ "$#" -gt 0 ]; then
        echo ">> installing into master: $*"
        "$MASTER/bin/python" -m pip install "$@" || die "pip install into master failed"
    fi
    # The master must NOT carry an editable install of this project (GH #957).
    # `poetry install` re-adds one by default, so check rather than trust: an
    # image built from a master carrying the .pth silently re-imposes ONE
    # checkout on every consumer of the image, on every node.
    local sp
    sp="$(echo "$MASTER"/lib/python*/site-packages)"
    if [ -e "$sp/lsms_library.pth" ] || compgen -G "$sp/lsms_library-*.dist-info" >/dev/null; then
        die "master carries an editable install of lsms_library (.pth and/or dist-info in $sp).
     That is what GH #957 removed; imaging it would re-impose one checkout on every
     consumer.  Remove those, or reinstall with: poetry install --no-root"
    fi
    # Import check needs PYTHONPATH now, for the same reason.  Do NOT drop the
    # check and do NOT rely on cwd: the old bare `import lsms_library` passed
    # only because callers happened to `cd "$REPO"` first, which is the very
    # accident GH #957 exists to remove.
    echo ">> sanity-check master imports (PYTHONPATH=$REPO)"
    PYTHONPATH="$REPO" "$MASTER/bin/python" -c "
import lsms_library, os
want, got = os.path.realpath('$REPO') + os.sep, os.path.realpath(lsms_library.__file__)
assert got.startswith(want), 'imported %s, expected under %s' % (got, want)
" || die "master venv does not import lsms_library from $REPO — fix deps before imaging"
    command -v mksquashfs >/dev/null || die "mksquashfs not found"
    local tmp="$IMG.tmp.$$"
    echo ">> building image (gzip) -> $IMG"
    # -no-xattrs: skip the per-file `lustre.lov` xattr (squashfs can't store it; a
    # venv needs no xattrs) — otherwise mksquashfs warns once per file (~33k lines).
    mksquashfs "$MASTER" "$tmp" -comp gzip -noappend -no-progress -no-xattrs >/dev/null || { rm -f "$tmp"; die "mksquashfs failed"; }
    mv -f "$tmp" "$IMG"        # atomic swap; running mounts keep the old inode until remount
    echo ">> done: $(du -h "$IMG" | cut -f1)  $IMG"
}

cmd_mount() {
    [ -f "$IMG" ] || die "no image at $IMG — run: $0 update"
    [ -n "${SLURM_JOB_ID:-}" ] || die "must run inside a Slurm allocation (SLURM_JOB_ID unset)"
    local mnt="/local/job$SLURM_JOB_ID/venv_img"
    if mount 2>/dev/null | grep -q " $mnt "; then
        echo ">> already mounted at $mnt"
    elif [ -c /dev/fuse ] && [ -x "$SQUASHFUSE" ]; then
        # squashfuse_ll is fuse3 and execs `fusermount3`; this host only has the
        # setuid fuse2 `fusermount`. The fuse2/fuse3 mount handshake is compatible,
        # so a fusermount3 -> fusermount shim makes it work with no admin/install.
        local shim="/local/job$SLURM_JOB_ID/.fuseshim"
        mkdir -p "$shim"; ln -sf /usr/bin/fusermount "$shim/fusermount3"
        mkdir -p "$mnt"
        echo ">> mounting (squashfuse_ll + fusermount3 shim) -> $mnt"
        PATH="$shim:$PATH" "$SQUASHFUSE" "$IMG" "$mnt" || die "squashfuse mount failed"
    else
        # FUSE unavailable -> extract the single image to node-local (still 1 Lustre inode).
        command -v unsquashfs >/dev/null || die "no FUSE and no unsquashfs — cannot materialize venv"
        echo ">> FUSE unavailable; extracting image -> $mnt (unsquashfs)"
        unsquashfs -f -d "$mnt" "$IMG" >/dev/null || die "unsquashfs failed"
    fi
    # VERIFY FIRST, THEN REPOINT.  The old order repointed $REPO/.venv -- a
    # symlink shared by every job using this mirror -- and only then checked the
    # import, so a failed check left the repoint in place (and exited nonzero).
    # Verify through the mount path directly, which needs no symlink at all.
    PYTHONPATH="$REPO" "$mnt/bin/python" -c "
import lsms_library, os
want, got = os.path.realpath('$REPO') + os.sep, os.path.realpath(lsms_library.__file__)
assert got.startswith(want), 'imported %s, expected under %s' % (got, want)
" || die "venv mounted at $mnt but import failed; $REPO/.venv left untouched"
    # NOTE (GH #958): this symlink is per-clone state kept in a SHARED location.
    # Two jobs on different nodes overwrite each other's value, and console
    # scripts are shebanged through it.  Retiring it is #958's decision; until
    # then, repointing it is at least no longer done before verification.
    ln -sfn "$mnt" "$REPO/.venv"
    echo ">> .venv -> $mnt  (verified import OK)"
}

cmd_umount() {
    [ -n "${SLURM_JOB_ID:-}" ] || die "SLURM_JOB_ID unset"
    local mnt="/local/job$SLURM_JOB_ID/venv_img"
    fusermount -u "$mnt" 2>/dev/null && echo ">> unmounted $mnt" || echo ">> nothing mounted at $mnt (or it was an extract, not a mount)"
}

case "${1:-}" in
    update) shift; cmd_update "$@" ;;
    mount)  cmd_mount ;;
    umount) cmd_umount ;;
    *) echo "usage: $0 {update [pip-args...] | mount | umount}" >&2; exit 2 ;;
esac
