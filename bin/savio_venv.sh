#!/bin/bash
# savio_venv.sh -- manage the single-file (squashfs) node-local venv on Savio.
#
# Model (see SkunkWorks/savio_venv.md):
#   SHARE/.venv.lustre/  canonical WRITABLE venv on Lustre (master; gitignored)
#   SHARE/.venv.sqfs     read-only single-file image built from the master
#                        (gitignored, ~230 MB, ONE Lustre inode instead of ~33k)
#   SHARE/.venv.sqfs.prev  the previous image, kept as a rollback by `update`
#   REPO/.venv -> mount  per-CLONE symlink to this job's squashfuse mount
#
# Two locations, deliberately distinct (GH #958 defect 1):
#   REPO   the checkout this script lives in -- the code that must be imported,
#          and the owner of the per-clone .venv symlink.
#   SHARE  where the image and master live.  Resolution order:
#            1. $LSMS_VENV_SHARE, if set;
#            2. REPO itself, if it holds .venv.sqfs (you are in the mirror);
#            3. REPO's `origin`, if it is a local path holding .venv.sqfs
#               (a sucoder /local clone of the mirror).
#
# Subcommands:
#   update [pip-args...]   (re)build SHARE/.venv.sqfs from SHARE/.venv.lustre,
#                          under an flock.  With pip-args, installs them into
#                          the master first.  Rewrites console-script shebangs to
#                          a self-locating form and stamps a generation id.
#   mount                  mount the image at /local/job$SLURM_JOB_ID/venv_img and
#                          point REPO/.venv at it -- unless REPO is SHARE (the
#                          shared mirror), whose .venv is never written (#958).
#   umount                 unmount this job's mount.
#   status                 print SHARE, the image generation, and this job's mount.
set -uo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SQUASHFUSE="/usr/libexec/apptainer/bin/squashfuse_ll"   # apptainer-bundled (fuse3)
GEN_FILE="VENV_GENERATION"                              # stamped inside the image

die() { echo "savio_venv: $*" >&2; exit 1; }

resolve_share() {
    if [ -n "${LSMS_VENV_SHARE:-}" ]; then
        SHARE="$(cd "$LSMS_VENV_SHARE" 2>/dev/null && pwd)" \
            || die "LSMS_VENV_SHARE=$LSMS_VENV_SHARE is not a directory"
        return
    fi
    if [ -f "$REPO/.venv.sqfs" ] && [ ! -L "$REPO/.venv.sqfs" ]; then
        SHARE="$REPO"; return
    fi
    local origin
    origin="$(git -C "$REPO" remote get-url origin 2>/dev/null || true)"
    if [ -n "$origin" ] && [ -d "$origin" ] && [ -f "$origin/.venv.sqfs" ]; then
        SHARE="$(cd "$origin" && pwd)"; return
    fi
    die "cannot find .venv.sqfs: not in $REPO, not in origin (${origin:-none}).
     Set LSMS_VENV_SHARE to the directory holding .venv.sqfs and .venv.lustre."
}

same_dir() { [ "$(realpath "$1")" = "$(realpath "$2")" ]; }

# Identity check: the interpreter must import lsms_library from REPO, not from
# anywhere else.  PYTHONSAFEPATH keeps cwd off sys.path[0] (otherwise running
# from inside REPO satisfies the check whatever the venv contains, GH #957);
# sys.exit rather than assert, so PYTHONOPTIMIZE cannot strip it.
check_imports_repo() {
    LSMS_WANT_ROOT="$REPO" PYTHONSAFEPATH=1 PYTHONPATH="$REPO" "$1" -c '
import lsms_library, os, sys
want = os.path.realpath(os.environ["LSMS_WANT_ROOT"]).rstrip(os.sep) + os.sep
got = os.path.realpath(lsms_library.__file__)
if not got.startswith(want):
    sys.exit("imported %s, expected under %s" % (got, want))
'
}

# GH #958 defect 5: pip writes an ABSOLUTE shebang naming the interpreter it ran
# under -- the Lustre master, or (for scripts installed before the master was
# renamed) the mirror's .venv symlink, which dangles on every node but the last
# one to mount.  Rewrite each to the relocatable form pip/distlib itself emits:
# a /bin/sh trampoline that execs the python sitting next to the script, so the
# script runs under whichever mount it is invoked from.  Idempotent.
rewrite_shebangs() {
    local bin="$1" f first n=0
    for f in "$bin"/*; do
        [ -f "$f" ] && [ ! -L "$f" ] || continue
        IFS= read -r first < "$f" || continue
        case "$first" in
            '#!'*/python|'#!'*/python3|'#!'*/python3.[0-9]*) ;;
            *) continue ;;
        esac
        {
            printf '%s\n' '#!/bin/sh'
            printf '%s\n' "'''exec' \"\$(dirname -- \"\$(realpath -- \"\$0\")\")\"/python \"\$0\" \"\$@\""
            printf '%s\n' "' '''"
            tail -n +2 "$f"
        } > "$f.tmp.$$" && chmod --reference="$f" "$f.tmp.$$" && mv -f "$f.tmp.$$" "$f" && n=$((n+1))
    done
    echo ">> rewrote $n console-script shebang(s) to the self-locating form"
}

cmd_update() {
    resolve_share
    local master="$SHARE/.venv.lustre" img="$SHARE/.venv.sqfs" lock="$SHARE/.venv.lock"
    [ -d "$master" ] || die "no writable master at $master (build one first; see SkunkWorks/savio_venv.md)"
    command -v mksquashfs >/dev/null || die "mksquashfs not found"
    # GH #958 defect 3: the master is shared Lustre state; two concurrent updates
    # corrupt it.  flock works on Lustre (it does NOT under $HOME/NFS), so hold an
    # exclusive lock across the whole mutate-then-image sequence.
    exec 9>"$lock" || die "cannot open lock file $lock"
    flock -n 9 || { echo ">> waiting for another update to release $lock"; flock 9; }
    if [ "$#" -gt 0 ]; then
        echo ">> installing into master: $*"
        "$master/bin/python" -m pip install "$@" || die "pip install into master failed"
    fi
    # The master must NOT carry an editable install of this project (GH #957).
    # `poetry install` re-adds one by default, so check rather than trust: an
    # image built from a master carrying the .pth silently re-imposes ONE
    # checkout on every consumer of the image, on every node.
    # ITERATE the glob and FAIL CLOSED: a word-split glob yields a nonexistent
    # path on zero or many matches, and the guard would pass in exactly the
    # states it exists for.
    local sp found=0
    for sp in "$master"/lib/python*/site-packages; do
        [ -d "$sp" ] || continue
        found=1
        if [ -e "$sp/lsms_library.pth" ] || compgen -G "$sp/lsms_library-*.dist-info" >/dev/null; then
            die "master carries an editable install of lsms_library in $sp.
     That is what GH #957 removed; imaging it would re-impose ONE checkout on every
     consumer of this image, on every node.  Remove it with:
         rm -f  $sp/lsms_library.pth
         rm -rf $sp/lsms_library-*.dist-info
         rm -f  $master/bin/lsms-library
     (\`poetry install --no-root\` only SKIPS installing the root -- it does not
     uninstall one that is already there.)"
        fi
    done
    [ "$found" = 1 ] || die "no lib/python*/site-packages under $master -- refusing to image an unrecognisable venv"
    echo ">> sanity-check master imports (PYTHONPATH=$REPO)"
    check_imports_repo "$master/bin/python" \
        || die "master venv does not import lsms_library from $REPO -- fix deps before imaging"
    rewrite_shebangs "$master/bin"
    # GH #958 defect 4: two nodes can legitimately run different image
    # generations at once (running mounts keep the old inode).  Stamp one.
    local lock_sha freeze_sha gen
    lock_sha="$(sha256sum "$REPO/poetry.lock" 2>/dev/null | cut -c1-12)"
    freeze_sha="$("$master/bin/python" -m pip freeze 2>/dev/null | sha256sum | cut -c1-12)"
    gen="$(date -u +%Y%m%dT%H%M%SZ)"
    {
        echo "generation: $gen"
        echo "built_on: $(hostname) by ${USER:-?}"
        echo "built_from_repo: $REPO @ $(git -C "$REPO" rev-parse --short HEAD 2>/dev/null || echo '?')"
        echo "repo_poetry_lock_sha256: ${lock_sha:-none}   # the lock in the building checkout, NOT proof the master was installed from it"
        echo "pip_freeze_sha256: $freeze_sha"
    } > "$master/$GEN_FILE"
    local tmp="$img.tmp.$$"
    echo ">> building image (gzip) -> $img"
    # -no-xattrs: skip the per-file `lustre.lov` xattr (squashfs can't store it; a
    # venv needs no xattrs) -- otherwise mksquashfs warns once per file.
    mksquashfs "$master" "$tmp" -comp gzip -noappend -no-progress -no-xattrs >/dev/null \
        || { rm -f "$tmp"; die "mksquashfs failed"; }
    # Keep the outgoing image as the rollback: hardlink BEFORE the rename, so
    # there is no window in which $img is absent.  ONE generation is kept, so
    # the previous .prev is dropped -- say so, naming it.  (2026-10-17: the
    # first run of this replaced the 2026-09-07 pre-#957 image without a word;
    # it was the only copy.  Copy .prev aside first if it matters.)
    if [ -f "$img.prev" ] && [ "$(stat -c %i "$img.prev")" != "$(stat -c %i "$img")" ]; then
        echo ">> dropping the older rollback $img.prev ($(du -h "$img.prev" | cut -f1), $(stat -c %y "$img.prev" | cut -d. -f1))"
    fi
    if [ -f "$img" ]; then ln -f "$img" "$img.prev" || die "could not keep $img.prev"; fi
    mv -f "$tmp" "$img"        # atomic swap; running mounts keep the old inode until remount
    echo ">> done: $(du -h "$img" | cut -f1)  $img  (generation $gen; rollback: $img.prev)"
}

cmd_mount() {
    resolve_share
    local img="$SHARE/.venv.sqfs"
    [ -f "$img" ] || die "no image at $img -- run: $0 update"
    [ -n "${SLURM_JOB_ID:-}" ] || die "must run inside a Slurm allocation (SLURM_JOB_ID unset)"
    local mnt="/local/job$SLURM_JOB_ID/venv_img"
    if mount 2>/dev/null | grep -q " $mnt "; then
        echo ">> already mounted at $mnt"
    elif [ -x "$mnt/bin/python" ]; then
        echo ">> already extracted at $mnt"
    elif [ -c /dev/fuse ] && [ -x "$SQUASHFUSE" ]; then
        # squashfuse_ll is fuse3 and execs `fusermount3`; this host only has the
        # setuid fuse2 `fusermount`. The fuse2/fuse3 mount handshake is compatible,
        # so a fusermount3 -> fusermount shim makes it work with no admin/install.
        local shim="/local/job$SLURM_JOB_ID/.fuseshim"
        mkdir -p "$shim"; ln -sf /usr/bin/fusermount "$shim/fusermount3"
        mkdir -p "$mnt"
        echo ">> mounting $img (squashfuse_ll + fusermount3 shim) -> $mnt"
        PATH="$shim:$PATH" "$SQUASHFUSE" "$img" "$mnt" || die "squashfuse mount failed"
    else
        # FUSE unavailable -> extract the single image to node-local (still 1 Lustre inode).
        command -v unsquashfs >/dev/null || die "no FUSE and no unsquashfs -- cannot materialize venv"
        echo ">> FUSE unavailable; extracting image -> $mnt (unsquashfs)"
        unsquashfs -f -d "$mnt" "$img" >/dev/null || die "unsquashfs failed"
    fi
    # VERIFY FIRST, THEN REPOINT, through the mount path directly.
    check_imports_repo "$mnt/bin/python" \
        || die "venv mounted at $mnt but import from $REPO failed; $REPO/.venv left untouched"
    if [ -f "$mnt/$GEN_FILE" ]; then
        echo ">> image $(grep '^generation:' "$mnt/$GEN_FILE")"
    else
        echo ">> image generation: unstamped (built before GH #958's stamp)"
    fi
    # GH #958 defect 2: the .venv symlink is PER-CLONE state.  In the shared
    # mirror it would be last-writer-wins across nodes, so it is never written
    # there; use the mount path directly instead.
    if same_dir "$REPO" "$SHARE"; then
        echo ">> $REPO is the shared mirror: NOT repointing its .venv (GH #958)."
        echo ">> use the mount directly:  $mnt/bin/python"
        return
    fi
    ln -sfn "$mnt" "$REPO/.venv"
    echo ">> .venv -> $mnt  (verified import from $REPO)"
}

cmd_umount() {
    [ -n "${SLURM_JOB_ID:-}" ] || die "SLURM_JOB_ID unset"
    local mnt="/local/job$SLURM_JOB_ID/venv_img"
    fusermount -u "$mnt" 2>/dev/null && echo ">> unmounted $mnt" || echo ">> nothing mounted at $mnt (or it was an extract, not a mount)"
}

cmd_status() {
    resolve_share
    echo "repo:  $REPO"
    echo "share: $SHARE"
    local img="$SHARE/.venv.sqfs"
    if [ -f "$img" ]; then
        echo "image: $img  ($(du -h "$img" | cut -f1), $(stat -c %y "$img" | cut -d. -f1))"
        if command -v unsquashfs >/dev/null; then
            # Extract the one file (squashfs-tools 4.3 on Savio has no -cat).
            local t; t="$(mktemp -d "${TMPDIR:-/tmp}/savio_venv.XXXXXX")"
            if unsquashfs -f -d "$t/x" "$img" "$GEN_FILE" >/dev/null 2>&1 && [ -f "$t/x/$GEN_FILE" ]; then
                sed 's/^/  /' "$t/x/$GEN_FILE"
            else
                echo "  (unstamped: built before GH #958's stamp)"
            fi
            rm -rf "$t"
        fi
    else
        echo "image: none"
    fi
    if [ -n "${SLURM_JOB_ID:-}" ]; then
        local mnt="/local/job$SLURM_JOB_ID/venv_img"
        if [ -x "$mnt/bin/python" ]; then
            echo "mount: $mnt"
            [ -f "$mnt/$GEN_FILE" ] && grep '^generation:' "$mnt/$GEN_FILE" | sed 's/^/  /'
        else
            echo "mount: none for job $SLURM_JOB_ID"
        fi
    fi
    if [ -L "$REPO/.venv" ]; then echo ".venv: -> $(readlink "$REPO/.venv")"; fi
}

case "${1:-}" in
    update) shift; cmd_update "$@" ;;
    mount)  cmd_mount ;;
    umount) cmd_umount ;;
    status) cmd_status ;;
    *) echo "usage: $0 {update [pip-args...] | mount | umount | status}" >&2; exit 2 ;;
esac
